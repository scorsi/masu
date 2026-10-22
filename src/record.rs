//! Scrape-record rules (docs/storage-format.md, ADR 0003): what masu checks on each pushed
//! record before storing it. The errors are the exact `PushRejected.reason` strings, published
//! with their examples in fixtures/records/ (ADR 0009).

use serde::de::{Deserialize, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::fmt;

/// A stored record is at most 4 MiB.
pub const MAX_STORED_BYTES: usize = 4 * 1024 * 1024;
/// `,"recv_ts":"2026-10-19T08:14:00.391022117Z"`, inserted by masu before the final `}`.
pub const RECV_TS_MEMBER_BYTES: usize = 43;
/// The smallest valid record: every required member, shortest values (test `smallest_record`).
pub const MIN_RECORD_BYTES: usize = 154;
/// The largest record an agent may push.
pub const MAX_PUSHED_BYTES: usize = MAX_STORED_BYTES - RECV_TS_MEMBER_BYTES;

/// The envelope fields the ingestion path matches against the request (ADR 0003).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub ts_ns: i64,
    pub node: String,
    pub stream: String,
    pub seq: u64,
}

/// Checks one pushed record; `Err` holds the `PushRejected.reason`.
///
/// One reason per record, the first rule broken in this order: size, UTF-8, framing, JSON,
/// object, duplicate member, `recv_ts`, then the envelope fields in the order of
/// storage-format.md, then `raw_b64`.
pub fn check(record: &[u8]) -> Result<Envelope, String> {
    if record.len() > MAX_PUSHED_BYTES {
        return Err("record-too-large".into());
    }
    if std::str::from_utf8(record).is_err() {
        return Err("not-utf8".into());
    }
    // Stored as one NDJSON line, with recv_ts spliced in before the final byte.
    if record.first() != Some(&b'{')
        || record.last() != Some(&b'}')
        || record.iter().any(|&b| b == b'\n' || b == b'\r')
    {
        return Err("framing".into());
    }
    let Ok(value) = serde_json::from_slice::<Value>(record) else {
        return Err("not-json".into());
    };
    // Unreachable after framing (valid JSON starting with `{` is an object); kept for the map.
    let Some(obj) = value.as_object() else {
        return Err("not-object".into());
    };
    // serde_json keeps the last of duplicated members: a second pass finds them.
    if let Ok(FirstDuplicate(Some(name))) = serde_json::from_slice(record) {
        return Err(format!("duplicate-field:{name}"));
    }
    if obj.contains_key("recv_ts") {
        return Err("reserved-field:recv_ts".into());
    }
    field(obj, "v", |v| v.as_u64().filter(|&n| n == 1))?;
    // A date outside the i64 nanoseconds of the store is not a `ts` (`ts-format`).
    let ts_ns = field(obj, "ts", |v| v.as_str().and_then(crate::time::parse_ns))?;
    let node = field(obj, "node", |v| v.as_str().filter(|s| is_node(s)))?;
    field(obj, "service", Value::as_str)?;
    let stream = field(obj, "stream", |v| v.as_str().filter(|s| is_stream(s)))?;
    let seq = field(obj, "seq", Value::as_u64)?;
    let up = field(obj, "up", Value::as_bool)?;
    field(obj, "dur_us", Value::as_u64)?;
    let error = if up {
        field(obj, "content_type", Value::as_str)?;
        field(obj, "body", Value::as_str)?;
        None
    } else {
        Some(field(obj, "error", Value::as_str)?)
    };
    if obj.contains_key("raw_b64") {
        field(obj, "raw_b64", Value::as_str)?;
        if !error.is_some_and(|e| e.starts_with("rejected:")) {
            return Err("unexpected-field:raw_b64".into());
        }
    }
    Ok(Envelope {
        ts_ns,
        node: node.into(),
        stream: stream.into(),
        seq,
    })
}

/// `missing-field:<name>` when absent, `<name>-format` when `get` refuses the value (wrong JSON
/// type or wrong format).
fn field<'a, T>(
    obj: &'a Map<String, Value>,
    name: &str,
    get: impl Fn(&'a Value) -> Option<T>,
) -> Result<T, String> {
    let v = obj
        .get(name)
        .ok_or_else(|| format!("missing-field:{name}"))?;
    get(v).ok_or_else(|| format!("{name}-format"))
}

/// The first top-level member name seen twice (after unescaping), if any.
struct FirstDuplicate(Option<String>);

impl<'de> Deserialize<'de> for FirstDuplicate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Members;
        impl<'de> Visitor<'de> for Members {
            type Value = FirstDuplicate;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<FirstDuplicate, A::Error> {
                let mut seen = HashSet::new();
                let mut first = None;
                // Read to the end: serde_json checks the closing `}` after the visitor.
                while let Some(name) = map.next_key::<String>()? {
                    map.next_value::<IgnoredAny>()?;
                    if first.is_none() && !seen.insert(name.clone()) {
                        first = Some(name);
                    }
                }
                Ok(FirstDuplicate(first))
            }
        }
        d.deserialize_map(Members)
    }
}

/// 32 lowercase hex digits: the 16-byte `stream_id`.
fn is_stream(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// UTC RFC 3339 with exactly 9 fractional digits and `Z`: `2026-10-19T08:14:00.000213904Z`.
pub fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 30 || !s.is_ascii() {
        return false;
    }
    let shape = [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'.'),
        (29, b'Z'),
    ];
    if shape.iter().any(|&(i, c)| b[i] != c) {
        return false;
    }
    let num = |from: usize, to: usize| -> Option<u32> {
        b[from..to]
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| s[from..to].parse().ok())?
    };
    let (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(sec), Some(_)) = (
        num(0, 4),
        num(5, 7),
        num(8, 10),
        num(11, 13),
        num(14, 16),
        num(17, 19),
        num(20, 29),
    ) else {
        return false;
    };
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    (1..=12).contains(&mo) && d >= 1 && d <= days[mo as usize - 1] && h < 24 && mi < 60 && sec < 60
}

/// `[a-z0-9][a-z0-9-]{0,62}`: the node is a directory of the archive.
pub fn is_node(s: &str) -> bool {
    (1..=63).contains(&s.len())
        && !s.starts_with('-')
        && s.bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        assert!(is_timestamp("2026-10-19T08:14:00.000213904Z"));
        assert!(is_timestamp("2028-02-29T23:59:59.999999999Z"));
        for bad in [
            "2026-10-19T08:14:00.000213Z",
            "2026-10-19T08:14:00.000213904+00:00",
            "2026-02-29T08:14:00.000213904Z",
            "2026-13-19T08:14:00.000213904Z",
            "2026-10-19T24:14:00.000213904Z",
            "2026-10-19T08:14:60.000213904Z",
            "2026-10-19 08:14:00.000213904Z",
            "+026-10-19T08:14:00.000213904Z",
        ] {
            assert!(!is_timestamp(bad), "{bad}");
        }
    }

    #[test]
    fn recv_ts_member_size() {
        let member = format!(",\"recv_ts\":\"{}\"", "2026-10-19T08:14:00.391022117Z");
        assert_eq!(member.len(), RECV_TS_MEMBER_BYTES);
    }

    #[test]
    fn smallest_record() {
        let r = format!(
            r#"{{"v":1,"ts":"2026-10-19T08:14:00.000213904Z","node":"a","service":"","stream":"{}","seq":0,"up":false,"dur_us":0,"error":""}}"#,
            "0".repeat(32)
        );
        assert!(check(r.as_bytes()).is_ok());
        assert_eq!(r.len(), MIN_RECORD_BYTES);
    }
}
