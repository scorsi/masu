//! Schema-specific wire checks before prost discards unknown fields and duplicate map keys.

use super::{Error, MAX_QUERIES, Result};
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
enum Kind {
    Request,
    Range,
    Query,
    SeriesQuery,
    Up,
    Discover,
    Response,
    Result,
    Error,
    SeriesFrame,
    Series,
    Table,
    Counts,
    CountGroup,
    Budget,
    Timestamp,
    Duration,
    Strings,
    QueryEntry,
    ResultEntry,
    Struct,
    StructEntry,
    Value,
    List,
}

#[derive(Clone, Copy)]
enum Field {
    String,
    Varint,
    Double,
    PackedDouble,
    PackedInt,
    Message(Kind),
}

pub(super) fn request(bytes: &[u8]) -> Result<BTreeSet<String>> {
    let mut unsupported = BTreeSet::new();
    if inspect(bytes, Kind::Request, 0, &mut unsupported)? {
        return Err(Error::InvalidRequest);
    }
    Ok(unsupported)
}
pub(super) fn response(bytes: &[u8]) -> Result<()> {
    if inspect(bytes, Kind::Response, 0, &mut BTreeSet::new())? {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn message(bytes: &[u8]) -> Result<&[u8]> {
    let mut bytes = bytes;
    let len = usize::try_from(decode_varint(&mut bytes).map_err(|_| Error::InvalidRequest)?)
        .map_err(|_| Error::InvalidRequest)?;
    bytes.get(..len).ok_or(Error::InvalidRequest)
}

fn key(mut bytes: &[u8]) -> Result<String> {
    let mut key = String::new();
    while !bytes.is_empty() {
        let (tag, ty) = decode_key(&mut bytes).map_err(|_| Error::InvalidRequest)?;
        let value = bytes;
        skip_field(ty, tag, &mut bytes, DecodeContext::default())
            .map_err(|_| Error::InvalidRequest)?;
        if tag == 1 {
            if ty != WireType::LengthDelimited {
                return Err(Error::InvalidRequest);
            }
            key = std::str::from_utf8(message(value)?)
                .map_err(|_| Error::InvalidRequest)?
                .into();
        }
    }
    Ok(key)
}

fn inspect(
    mut bytes: &[u8],
    kind: Kind,
    depth: usize,
    unsupported: &mut BTreeSet<String>,
) -> Result<bool> {
    if depth > 64 {
        return Err(Error::InvalidRequest);
    }
    let mut unknown = false;
    let mut keys: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    let mut query_unknown = false;
    let mut singular = BTreeSet::new();
    let entry_key = if matches!(kind, Kind::QueryEntry) {
        Some(key(bytes)?)
    } else {
        None
    };
    while !bytes.is_empty() {
        let (tag, ty) = decode_key(&mut bytes).map_err(|_| Error::InvalidRequest)?;
        let raw = bytes;
        skip_field(ty, tag, &mut bytes, DecodeContext::default())
            .map_err(|_| Error::InvalidRequest)?;
        let field = match (kind, tag) {
            (Kind::Request, 1) => Field::Message(Kind::Range),
            (Kind::Request, 2) => Field::Message(Kind::QueryEntry),
            (Kind::Response, 1) => Field::Message(Kind::ResultEntry),
            (Kind::Response, 2) => Field::Message(Kind::Budget),
            (Kind::Range, 1 | 2) => Field::Message(Kind::Timestamp),
            (Kind::Query, 1) => Field::Message(Kind::SeriesQuery),
            (Kind::Query, 2) => Field::Message(Kind::Up),
            (Kind::Query, 3) => Field::Message(Kind::Discover),
            (Kind::SeriesQuery, 1..=3) | (Kind::Up, 1 | 2 | 4) | (Kind::Discover, 1..=3) => {
                Field::String
            }
            (Kind::SeriesQuery, 4) => Field::Message(Kind::Strings),
            (Kind::SeriesQuery, 5) => Field::Message(Kind::Duration),
            (Kind::SeriesQuery, 6 | 7) | (Kind::Up, 3) => Field::Varint,
            (Kind::QueryEntry | Kind::ResultEntry | Kind::Strings | Kind::StructEntry, 1) => {
                Field::String
            }
            (Kind::QueryEntry, 2) => Field::Message(Kind::Query),
            (Kind::ResultEntry, 2) => Field::Message(Kind::Result),
            (Kind::Strings, 2) => Field::String,
            (Kind::Result, 1) => Field::Message(Kind::SeriesFrame),
            (Kind::Result, 2) => Field::Message(Kind::Table),
            (Kind::Result, 3) => Field::Message(Kind::Counts),
            (Kind::Result, 4) => Field::Message(Kind::Error),
            (Kind::Result, 5) | (Kind::Error, 1 | 2) => Field::String,
            (Kind::SeriesFrame, 1) => Field::Message(Kind::Series),
            (Kind::Series, 1) | (Kind::CountGroup, 1) => Field::Message(Kind::Strings),
            (Kind::Series, 2 | 4) | (Kind::Counts, 1) => Field::Message(Kind::Timestamp),
            (Kind::Series, 3) | (Kind::Counts, 2) => Field::Message(Kind::Duration),
            (Kind::Series, 5) => Field::PackedDouble,
            (Kind::Table, 1) | (Kind::Value, 5) => Field::Message(Kind::Struct),
            (Kind::Counts, 3) => Field::Message(Kind::CountGroup),
            (Kind::CountGroup, 2) => Field::PackedInt,
            (Kind::Budget, 1..=3) | (Kind::Timestamp | Kind::Duration, 1 | 2) => Field::Varint,
            (Kind::Struct, 1) => Field::Message(Kind::StructEntry),
            (Kind::StructEntry, 2) | (Kind::List, 1) => Field::Message(Kind::Value),
            (Kind::Value, 1 | 4) => Field::Varint,
            (Kind::Value, 2) => Field::Double,
            (Kind::Value, 3) => Field::String,
            (Kind::Value, 6) => Field::Message(Kind::List),
            _ => {
                unknown = true;
                continue;
            }
        };
        // shortcut: reject singular-field merges; extend only with logical-message duplicate tracking.
        let repeated = matches!(
            (kind, tag),
            (Kind::Request, 2)
                | (Kind::Response, 1)
                | (Kind::SeriesQuery, 4)
                | (Kind::SeriesFrame, 1)
                | (Kind::Series, 1 | 4 | 5)
                | (Kind::Table, 1)
                | (Kind::Counts, 3)
                | (Kind::CountGroup, 1 | 2)
                | (Kind::Struct, 1)
                | (Kind::List, 1)
        );
        let slot = if matches!(kind, Kind::Query | Kind::Value)
            || matches!((kind, tag), (Kind::Result, 1..=4))
        {
            0
        } else {
            tag
        };
        if !repeated && !singular.insert(slot) {
            return Err(Error::InvalidRequest);
        }
        let expected = match field {
            Field::Varint => WireType::Varint,
            Field::Double => WireType::SixtyFourBit,
            Field::PackedDouble if ty == WireType::SixtyFourBit => ty,
            Field::PackedInt if ty == WireType::Varint => ty,
            _ => WireType::LengthDelimited,
        };
        if ty != expected {
            return Err(Error::InvalidRequest);
        }
        if matches!((kind, tag), (Kind::SeriesQuery, 6)) {
            let mut value = raw;
            if !(0..=5)
                .contains(&(decode_varint(&mut value).map_err(|_| Error::InvalidRequest)? as i32))
            {
                unknown = true;
            }
        }
        if let Field::Message(child) = field {
            let value = message(raw)?;
            if matches!(
                child,
                Kind::Strings | Kind::QueryEntry | Kind::ResultEntry | Kind::StructEntry
            ) {
                let entries = keys.entry(tag).or_default();
                if !entries.insert(key(value)?) {
                    return Err(Error::InvalidRequest);
                }
                if matches!(child, Kind::QueryEntry | Kind::ResultEntry)
                    && entries.len() > MAX_QUERIES
                {
                    return Err(Error::InvalidRequest);
                }
            }
            let child_unknown = inspect(value, child, depth + 1, unsupported)?;
            if matches!((kind, tag), (Kind::QueryEntry, 2)) {
                query_unknown |= child_unknown;
            } else {
                unknown |= child_unknown;
            }
        } else if matches!(field, Field::PackedDouble) && ty == WireType::LengthDelimited {
            if message(raw)?.len() % 8 != 0 {
                return Err(Error::InvalidRequest);
            }
        } else if matches!(field, Field::PackedInt) && ty == WireType::LengthDelimited {
            let mut value = message(raw)?;
            while !value.is_empty() {
                decode_varint(&mut value).map_err(|_| Error::InvalidRequest)?;
            }
        }
    }
    if let Some(key) = entry_key
        && query_unknown
    {
        unsupported.insert(key);
    }
    Ok(unknown)
}
