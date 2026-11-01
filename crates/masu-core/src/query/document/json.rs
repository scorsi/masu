//! The document's ProtoJSON mapping. prost-types deliberately supplies no serde mapping.

use super::{Error, Result};
use crate::pb;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::io::{self, Write};
use std::sync::LazyLock;

// RawValue keeps integer exponents exact; serde_json still owns all JSON lexing.
struct RawObject<'a>(BTreeMap<String, &'a serde_json::value::RawValue>);
impl<'de> Deserialize<'de> for RawObject<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = RawObject<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object without duplicate members")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut out = BTreeMap::new();
                while let Some(key) = a.next_key::<String>()? {
                    if out.contains_key(&key) {
                        return Err(de::Error::custom("duplicate member"));
                    }
                    out.insert(key, a.next_value()?);
                }
                Ok(RawObject(out))
            }
        }
        d.deserialize_map(V)
    }
}

fn integer(s: &str) -> Option<u64> {
    let s = s.strip_prefix('+').unwrap_or(s);
    let (mantissa, exponent) = s.split_once(['e', 'E']).map_or((s, "0"), |v| v);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{fraction}")
        .trim_start_matches('0')
        .to_string();
    if digits.is_empty() {
        return exponent.parse::<i64>().ok().map(|_| 0);
    }
    let scale = exponent
        .parse::<i64>()
        .ok()?
        .checked_sub(i64::try_from(fraction.len()).ok()?)?;
    if scale < 0 {
        let remove = usize::try_from(scale.unsigned_abs()).ok()?;
        if remove >= digits.len() || !digits[digits.len() - remove..].bytes().all(|b| b == b'0') {
            return None;
        }
        digits.truncate(digits.len() - remove);
    } else {
        let zeros = usize::try_from(scale).ok()?;
        if zeros > 20 || digits.len().checked_add(zeros)? > 20 {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', zeros));
    }
    digits.parse().ok()
}

fn parse(bytes: &[u8]) -> Result<Value> {
    fn value(raw: &serde_json::value::RawValue, depth: usize) -> Result<Value> {
        if depth > 64 {
            return Err(Error::InvalidRequest);
        }
        let text = raw.get();
        match text.as_bytes().first() {
            Some(b'{') => {
                let RawObject(fields) =
                    serde_json::from_str(text).map_err(|_| Error::InvalidRequest)?;
                fields
                    .into_iter()
                    .map(|(k, v)| Ok((k, value(v, depth + 1)?)))
                    .collect::<Result<Map<_, _>>>()
                    .map(Value::Object)
            }
            Some(b'[') => {
                let rows: Vec<&serde_json::value::RawValue> =
                    serde_json::from_str(text).map_err(|_| Error::InvalidRequest)?;
                rows.into_iter()
                    .map(|v| value(v, depth + 1))
                    .collect::<Result<Vec<_>>>()
                    .map(Value::Array)
            }
            _ => serde_json::from_str(text).map_err(|_| Error::InvalidRequest),
        }
    }
    let raw: &serde_json::value::RawValue =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
    value(raw, 0)
}
fn guard(raw: &serde_json::value::RawValue, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(Error::InvalidRequest);
    }
    match raw.get().as_bytes().first() {
        Some(b'{') => {
            let RawObject(fields) =
                serde_json::from_str(raw.get()).map_err(|_| Error::InvalidRequest)?;
            for v in fields.values() {
                guard(v, depth + 1)?;
            }
        }
        Some(b'[') => {
            let rows: Vec<&serde_json::value::RawValue> =
                serde_json::from_str(raw.get()).map_err(|_| Error::InvalidRequest)?;
            for v in rows {
                guard(v, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn raw_object(
    raw: &serde_json::value::RawValue,
) -> Result<BTreeMap<String, &serde_json::value::RawValue>> {
    let RawObject(obj) = serde_json::from_str(raw.get()).map_err(|_| Error::InvalidRequest)?;
    Ok(obj)
}
fn unknown_raw_query(raw: &serde_json::value::RawValue) -> Result<bool> {
    let obj = raw_object(raw)?;
    if obj
        .keys()
        .any(|k| !["series", "up", "discover"].contains(&k.as_str()))
    {
        return Ok(true);
    }
    for (operator, raw) in obj {
        let names: &[&str] = match operator.as_str() {
            "series" => &[
                "node", "service", "name", "labels", "step", "reduce", "rate",
            ],
            "up" => &["node", "service", "limit", "after"],
            _ => &["node", "service", "prefix"],
        };
        let Ok(body) = raw_object(raw) else { continue };
        if body.keys().any(|k| !names.contains(&k.as_str())) {
            return Ok(true);
        }
        if operator == "series"
            && let Some(step) = body.get("step")
            && let Ok(step) = raw_object(step)
            && step.keys().any(|k| k != "seconds" && k != "nanos")
        {
            return Ok(true);
        }
    }
    Ok(false)
}
fn exact_uint(value: &mut Value, raw: &serde_json::value::RawValue) {
    // Only typed uint fields use their numeric lexeme; doubles and Struct data are untouched.
    if raw.get().as_bytes().first().is_some_and(u8::is_ascii_digit)
        && let Some(n) = integer(raw.get())
    {
        *value = n.into();
    }
}
fn request_uints(value: &mut Value, raw: &serde_json::value::RawValue) -> Result<()> {
    let obj = raw_object(raw)?;
    if let Some(up) = obj.get("up")
        && let Ok(up) = raw_object(up)
        && let Some(limit) = up.get("limit")
    {
        exact_uint(&mut value["up"]["limit"], limit);
    }
    Ok(())
}
fn response_uints(value: &mut Value, raw: &serde_json::value::RawValue) -> Result<()> {
    let obj = raw_object(raw)?;
    if let Some(budget) = obj.get("budget")
        && let Ok(budget) = raw_object(budget)
    {
        for (key, raw) in budget {
            if [
                "recordsRead",
                "records_read",
                "bytesRead",
                "bytes_read",
                "elapsedMs",
                "elapsed_ms",
            ]
            .contains(&key.as_str())
            {
                exact_uint(&mut value["budget"][&key], raw);
            }
        }
    }
    if let Some(results) = obj.get("results")
        && let Ok(results) = raw_object(results)
    {
        for (name, result) in results {
            let Ok(result) = raw_object(result) else {
                continue;
            };
            let Some(counts) = result.get("counts") else {
                continue;
            };
            let Ok(counts) = raw_object(counts) else {
                continue;
            };
            let Some(groups) = counts.get("groups") else {
                continue;
            };
            let Ok(groups) =
                serde_json::from_str::<Vec<&serde_json::value::RawValue>>(groups.get())
            else {
                continue;
            };
            for (i, group) in groups.into_iter().enumerate() {
                let Ok(group) = raw_object(group) else {
                    continue;
                };
                let Some(counts) = group.get("counts") else {
                    continue;
                };
                let Ok(counts) =
                    serde_json::from_str::<Vec<&serde_json::value::RawValue>>(counts.get())
                else {
                    continue;
                };
                for (j, count) in counts.into_iter().enumerate() {
                    exact_uint(
                        &mut value["results"][&name]["counts"]["groups"][i]["counts"][j],
                        count,
                    );
                }
            }
        }
    }
    Ok(())
}
fn object(v: &Value) -> Result<&Map<String, Value>> {
    static EMPTY: LazyLock<Map<String, Value>> = LazyLock::new(Map::new);
    if v.is_null() {
        return Ok(&EMPTY);
    }
    v.as_object().ok_or(Error::InvalidRequest)
}
fn fields<'a>(v: &'a Value, names: &[&str]) -> Result<&'a Map<String, Value>> {
    let obj = object(v)?;
    if obj.keys().any(|k| !names.contains(&k.as_str())) {
        return Err(Error::InvalidRequest);
    }
    Ok(obj)
}
fn get<'a>(v: &'a Map<String, Value>, name: &str) -> &'a Value {
    v.get(name).unwrap_or(&Value::Null)
}
fn alias<'a>(v: &'a Map<String, Value>, a: &str, b: &str) -> Result<&'a Value> {
    if v.contains_key(a) && v.contains_key(b) {
        return Err(Error::InvalidRequest);
    }
    Ok(v.get(a).or_else(|| v.get(b)).unwrap_or(&Value::Null))
}
fn string(v: &Value) -> Result<String> {
    if v.is_null() {
        Ok(String::new())
    } else {
        v.as_str().map(String::from).ok_or(Error::InvalidRequest)
    }
}
fn boolean(v: &Value) -> Result<bool> {
    if v.is_null() {
        Ok(false)
    } else {
        v.as_bool().ok_or(Error::InvalidRequest)
    }
}
fn uint(v: &Value) -> Result<u64> {
    if v.is_null() {
        return Ok(0);
    }
    v.as_u64()
        .or_else(|| v.as_str().and_then(integer))
        .or_else(|| v.as_number().and_then(|n| integer(&n.to_string())))
        .ok_or(Error::InvalidRequest)
}
fn array(v: &Value) -> Result<&[Value]> {
    if v.is_null() {
        Ok(&[])
    } else {
        v.as_array().map(Vec::as_slice).ok_or(Error::InvalidRequest)
    }
}
fn strings(v: &Value) -> Result<HashMap<String, String>> {
    object(v)?
        .iter()
        .map(|(k, v)| {
            if v.is_null() {
                return Err(Error::InvalidRequest);
            }
            Ok((k.clone(), string(v)?))
        })
        .collect()
}
fn timestamp(v: &Value) -> Result<prost_types::Timestamp> {
    let t = v
        .as_str()
        .ok_or(Error::InvalidRequest)?
        .parse()
        .map_err(|_| Error::InvalidRequest)?;
    super::timestamp_ns(&t)?;
    Ok(t)
}
fn duration(v: &Value) -> Result<prost_types::Duration> {
    let d = v
        .as_str()
        .ok_or(Error::InvalidRequest)?
        .parse()
        .map_err(|_| Error::InvalidRequest)?;
    super::duration_ns(&d)?;
    Ok(d)
}
fn optional<T>(v: &Value, parse: impl FnOnce(&Value) -> Result<T>) -> Result<Option<T>> {
    if v.is_null() {
        Ok(None)
    } else {
        parse(v).map(Some)
    }
}
fn double(v: &Value) -> Result<f64> {
    if let Some(v) = v.as_f64() {
        return Ok(v);
    }
    match v.as_str() {
        Some("NaN") => Ok(f64::NAN),
        Some("Infinity") => Ok(f64::INFINITY),
        Some("-Infinity") => Ok(f64::NEG_INFINITY),
        Some(s) => s
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .ok_or(Error::InvalidRequest),
        None => Err(Error::InvalidRequest),
    }
}

fn unknown_query(v: &Value) -> Result<bool> {
    let obj = object(v)?;
    if obj
        .keys()
        .any(|k| !["series", "up", "discover"].contains(&k.as_str()))
    {
        return Ok(true);
    }
    for (operator, body) in obj {
        let names: &[&str] = match operator.as_str() {
            "series" => &[
                "node", "service", "name", "labels", "step", "reduce", "rate",
            ],
            "up" => &["node", "service", "limit", "after"],
            "discover" => &["node", "service", "prefix"],
            _ => return Ok(true),
        };
        let Ok(body) = object(body) else {
            continue;
        };
        if body.keys().any(|k| !names.contains(&k.as_str())) {
            return Ok(true);
        }
        if operator == "series" {
            if let Some(step) = body.get("step").and_then(Value::as_object)
                && step.keys().any(|k| k != "seconds" && k != "nanos")
            {
                return Ok(true);
            }
            let reduce = get(body, "reduce");
            if let Some(name) = reduce.as_str() {
                if pb::Reduce::from_str_name(name).is_none() {
                    return Ok(true);
                }
            } else if let Some(number) = reduce.as_i64()
                && i32::try_from(number)
                    .ok()
                    .and_then(|n| pb::Reduce::try_from(n).ok())
                    .is_none()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn query(v: &Value) -> Result<pb::Query> {
    let obj = object(v)?;
    if obj.values().filter(|v| !v.is_null()).count() > 1 {
        return Err(Error::InvalidRequest);
    }
    let body = match obj.iter().find(|(_, v)| !v.is_null()) {
        None => None,
        Some((op, value)) => {
            let q = object(value)?;
            Some(match op.as_str() {
                "series" => {
                    let reduce = get(q, "reduce");
                    let reduce = if reduce.is_null() {
                        0
                    } else if let Some(name) = reduce.as_str() {
                        pb::Reduce::from_str_name(name).ok_or(Error::InvalidRequest)? as i32
                    } else {
                        i32::try_from(reduce.as_i64().ok_or(Error::InvalidRequest)?)
                            .map_err(|_| Error::InvalidRequest)?
                    };
                    pb::query::Body::Series(pb::SeriesQuery {
                        node: string(get(q, "node"))?,
                        service: string(get(q, "service"))?,
                        name: string(get(q, "name"))?,
                        labels: strings(get(q, "labels"))?,
                        step: optional(get(q, "step"), duration)?,
                        reduce,
                        rate: boolean(get(q, "rate"))?,
                    })
                }
                "up" => pb::query::Body::Up(pb::UpQuery {
                    node: string(get(q, "node"))?,
                    service: string(get(q, "service"))?,
                    limit: u32::try_from(uint(get(q, "limit"))?)
                        .map_err(|_| Error::InvalidRequest)?,
                    after: string(get(q, "after"))?,
                }),
                "discover" => pb::query::Body::Discover(pb::DiscoverQuery {
                    node: string(get(q, "node"))?,
                    service: string(get(q, "service"))?,
                    prefix: string(get(q, "prefix"))?,
                }),
                _ => return Err(Error::InvalidRequest),
            })
        }
    };
    Ok(pb::Query { body })
}

pub(super) fn request(
    bytes: &[u8],
) -> Result<(pb::QueryRequest, BTreeSet<String>, BTreeSet<String>)> {
    let raw: &serde_json::value::RawValue =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
    guard(raw, 0)?;
    let obj = raw_object(raw)?;
    if obj.keys().any(|k| k != "range" && k != "queries") {
        return Err(Error::InvalidRequest);
    }
    let range = match obj.get("range") {
        None => None,
        Some(raw) => {
            let v = parse(raw.get().as_bytes())?;
            optional(&v, |v| {
                let r = fields(v, &["from", "to"])?;
                Ok(pb::Range {
                    from: optional(get(r, "from"), timestamp)?,
                    to: optional(get(r, "to"), timestamp)?,
                })
            })?
        }
    };
    let raw_queries = obj.get("queries").ok_or(Error::InvalidRequest)?;
    let mut unsupported = BTreeSet::new();
    let mut invalid = BTreeSet::new();
    let mut queries = BTreeMap::new();
    for (name, raw) in raw_object(raw_queries)? {
        let q = match unknown_raw_query(raw) {
            Ok(true) => {
                unsupported.insert(name.clone());
                pb::Query::default()
            }
            _ => match parse(raw.get().as_bytes()).and_then(|mut v| {
                request_uints(&mut v, raw)?;
                Ok(v)
            }) {
                Ok(v) => match unknown_query(&v) {
                    Ok(true) => {
                        unsupported.insert(name.clone());
                        pb::Query::default()
                    }
                    Ok(false) => match query(&v) {
                        Ok(q) => q,
                        Err(_) => {
                            invalid.insert(name.clone());
                            pb::Query::default()
                        }
                    },
                    Err(_) => {
                        invalid.insert(name.clone());
                        pb::Query::default()
                    }
                },
                Err(_) => {
                    invalid.insert(name.clone());
                    pb::Query::default()
                }
            },
        };
        queries.insert(name, q);
    }
    Ok((pb::QueryRequest { range, queries }, unsupported, invalid))
}

fn query_value(query: &pb::Query) -> Value {
    match &query.body {
        None => json!({}),
        Some(pb::query::Body::Series(q)) => {
            let mut v = json!({"node":q.node,"service":q.service,"name":q.name,"labels":q.labels,"rate":q.rate});
            v["reduce"] = pb::Reduce::try_from(q.reduce)
                .map(|r| Value::from(r.as_str_name()))
                .unwrap_or_else(|_| Value::from(q.reduce));
            if let Some(step) = &q.step {
                v["step"] = step.to_string().into();
            }
            json!({"series":v})
        }
        Some(pb::query::Body::Up(q)) => {
            json!({"up":{"node":q.node,"service":q.service,"limit":q.limit,"after":q.after}})
        }
        Some(pb::query::Body::Discover(q)) => {
            json!({"discover":{"node":q.node,"service":q.service,"prefix":q.prefix}})
        }
    }
}
pub(super) fn request_value(q: &pb::QueryRequest) -> Value {
    let r = q.range.as_ref().unwrap();
    let queries: Map<String, Value> = q
        .queries
        .iter()
        .map(|(k, v)| (k.clone(), query_value(v)))
        .collect();
    json!({"range":{"from":r.from.as_ref().unwrap().to_string(),"to":r.to.as_ref().unwrap().to_string()},"queries":queries})
}

fn series(v: &Value) -> Result<pb::Series> {
    if v.is_null() {
        return Err(Error::InvalidRequest);
    }
    let o = fields(v, &["labels", "start", "step", "ts", "values"])?;
    Ok(pb::Series {
        labels: strings(get(o, "labels"))?,
        start: optional(get(o, "start"), timestamp)?,
        step: optional(get(o, "step"), duration)?,
        ts: array(get(o, "ts"))?
            .iter()
            .map(timestamp)
            .collect::<Result<_>>()?,
        values: array(get(o, "values"))?
            .iter()
            .map(double)
            .collect::<Result<_>>()?,
    })
}
fn result(v: &Value) -> Result<pb::QueryResult> {
    let o = fields(v, &["series", "table", "counts", "error", "cursor"])?;
    let cases: Vec<_> = ["series", "table", "counts", "error"]
        .into_iter()
        .filter(|k| !get(o, k).is_null())
        .collect();
    if cases.len() != 1 {
        return Err(Error::InvalidRequest);
    }
    let case = cases[0];
    let value = get(o, case);
    let body = match case {
        "series" => {
            let f = fields(value, &["series"])?;
            pb::query_result::Body::Series(pb::SeriesFrame {
                series: array(get(f, "series"))?
                    .iter()
                    .map(series)
                    .collect::<Result<_>>()?,
            })
        }
        "table" => {
            let f = fields(value, &["rows"])?;
            pb::query_result::Body::Table(pb::TableFrame {
                rows: array(get(f, "rows"))?
                    .iter()
                    .map(struct_from_json)
                    .collect::<Result<_>>()?,
            })
        }
        "counts" => {
            let f = fields(value, &["start", "step", "groups"])?;
            let groups = array(get(f, "groups"))?
                .iter()
                .map(|v| {
                    if v.is_null() {
                        return Err(Error::InvalidRequest);
                    }
                    let g = fields(v, &["by", "counts"])?;
                    Ok(pb::CountGroup {
                        by: strings(get(g, "by"))?,
                        counts: array(get(g, "counts"))?
                            .iter()
                            .map(|v| {
                                if v.is_null() {
                                    Err(Error::InvalidRequest)
                                } else {
                                    uint(v)
                                }
                            })
                            .collect::<Result<_>>()?,
                    })
                })
                .collect::<Result<_>>()?;
            pb::query_result::Body::Counts(pb::CountsFrame {
                start: optional(get(f, "start"), timestamp)?,
                step: optional(get(f, "step"), duration)?,
                groups,
            })
        }
        _ => {
            let e = fields(value, &["reason", "message"])?;
            pb::query_result::Body::Error(pb::QueryError {
                reason: string(get(e, "reason"))?,
                message: string(get(e, "message"))?,
            })
        }
    };
    Ok(pb::QueryResult {
        body: Some(body),
        cursor: string(get(o, "cursor"))?,
    })
}

pub(super) fn response(bytes: &[u8]) -> Result<pb::QueryResponse> {
    let mut v = parse(bytes)?;
    let raw: &serde_json::value::RawValue =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
    response_uints(&mut v, raw)?;
    let o = fields(&v, &["results", "budget"])?;
    let results = object(get(o, "results"))?
        .iter()
        .map(|(k, v)| Ok((k.clone(), result(v)?)))
        .collect::<Result<_>>()?;
    let budget = optional(get(o, "budget"), |v| {
        let b = fields(
            v,
            &[
                "recordsRead",
                "records_read",
                "bytesRead",
                "bytes_read",
                "elapsedMs",
                "elapsed_ms",
            ],
        )?;
        Ok(pb::Budget {
            records_read: uint(alias(b, "recordsRead", "records_read")?)?,
            bytes_read: uint(alias(b, "bytesRead", "bytes_read")?)?,
            elapsed_ms: uint(alias(b, "elapsedMs", "elapsed_ms")?)?,
        })
    })?;
    Ok(pb::QueryResponse { results, budget })
}
fn struct_from_json(v: &Value) -> Result<prost_types::Struct> {
    if !v.is_object() {
        return Err(Error::InvalidRequest);
    }
    Ok(prost_types::Struct {
        fields: object(v)?
            .iter()
            .map(|(k, v)| Ok((k.clone(), value_from_json(v)?)))
            .collect::<Result<_>>()?,
    })
}
fn value_from_json(v: &Value) -> Result<prost_types::Value> {
    use prost_types::value::Kind;
    let kind = match v {
        Value::Null => Kind::NullValue(0),
        Value::Bool(v) => Kind::BoolValue(*v),
        Value::Number(v) => Kind::NumberValue(v.as_f64().ok_or(Error::InvalidRequest)?),
        Value::String(v) => Kind::StringValue(v.clone()),
        Value::Object(_) => Kind::StructValue(struct_from_json(v)?),
        Value::Array(v) => Kind::ListValue(prost_types::ListValue {
            values: v.iter().map(value_from_json).collect::<Result<_>>()?,
        }),
    };
    Ok(prost_types::Value { kind: Some(kind) })
}
pub(super) fn validate_struct(s: &prost_types::Struct, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(Error::InvalidRequest);
    }
    for v in s.fields.values() {
        validate_value(v, depth + 1)?;
    }
    Ok(())
}
fn validate_value(v: &prost_types::Value, depth: usize) -> Result<()> {
    use prost_types::value::Kind;
    if depth > 64 {
        return Err(Error::InvalidRequest);
    }
    match v.kind.as_ref().ok_or(Error::InvalidRequest)? {
        Kind::NumberValue(n) if !n.is_finite() => Err(Error::InvalidRequest),
        Kind::NullValue(n) if *n != 0 => Err(Error::InvalidRequest),
        Kind::StructValue(s) => validate_struct(s, depth + 1),
        Kind::ListValue(l) => {
            for v in &l.values {
                validate_value(v, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn encode(value: &Value, limit: usize, error: Error) -> Result<Vec<u8>> {
    struct Limited {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit - self.bytes.len() {
                return Err(io::Error::other("document too large"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut out = Limited {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut out, value).map_err(|_| error)?;
    Ok(out.bytes)
}

// These adapters borrow message contents, so measuring/encoding a batch does not clone it.
struct SeriesJson<'a>(&'a pb::Series);
struct ResultJson<'a>(&'a pb::QueryResult);
struct ResponseJson<'a>(&'a pb::QueryResponse);
struct StructJson<'a>(&'a prost_types::Struct);
struct ValueJson<'a>(&'a prost_types::Value);
struct DoubleJson(f64);
struct TimestampList<'a>(&'a [prost_types::Timestamp]);
struct DoubleList<'a>(&'a [f64]);
struct UIntList<'a>(&'a [u64]);

impl serde::Serialize for DoubleJson {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        if self.0.is_nan() {
            s.serialize_str("NaN")
        } else if self.0 == f64::INFINITY {
            s.serialize_str("Infinity")
        } else if self.0 == f64::NEG_INFINITY {
            s.serialize_str("-Infinity")
        } else {
            s.serialize_f64(self.0)
        }
    }
}
impl serde::Serialize for TimestampList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for t in self.0 {
            out.serialize_element(&t.to_string())?;
        }
        out.end()
    }
}
impl serde::Serialize for DoubleList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for n in self.0 {
            out.serialize_element(&DoubleJson(*n))?;
        }
        out.end()
    }
}
impl serde::Serialize for UIntList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for n in self.0 {
            out.serialize_element(&n.to_string())?;
        }
        out.end()
    }
}
impl serde::Serialize for SeriesJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("labels", &self.0.labels)?;
        if let Some(t) = &self.0.start {
            out.serialize_entry("start", &t.to_string())?;
        }
        if let Some(t) = &self.0.step {
            out.serialize_entry("step", &t.to_string())?;
        }
        out.serialize_entry("ts", &TimestampList(&self.0.ts))?;
        out.serialize_entry("values", &DoubleList(&self.0.values))?;
        out.end()
    }
}
impl serde::Serialize for StructJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(self.0.fields.len()))?;
        for (k, v) in &self.0.fields {
            out.serialize_entry(k, &ValueJson(v))?;
        }
        out.end()
    }
}
impl serde::Serialize for ValueJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use prost_types::value::Kind;
        use serde::ser::SerializeSeq;
        match self
            .0
            .kind
            .as_ref()
            .ok_or_else(|| serde::ser::Error::custom("missing Value kind"))?
        {
            Kind::NullValue(_) => s.serialize_unit(),
            Kind::BoolValue(v) => s.serialize_bool(*v),
            Kind::NumberValue(v) => s.serialize_f64(*v),
            Kind::StringValue(v) => s.serialize_str(v),
            Kind::StructValue(v) => serde::Serialize::serialize(&StructJson(v), s),
            Kind::ListValue(v) => {
                let mut out = s.serialize_seq(Some(v.values.len()))?;
                for v in &v.values {
                    out.serialize_element(&ValueJson(v))?;
                }
                out.end()
            }
        }
    }
}
struct SeriesList<'a>(&'a [pb::Series]);
struct RowList<'a>(&'a [prost_types::Struct]);
struct GroupList<'a>(&'a [pb::CountGroup]);
impl serde::Serialize for SeriesList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for v in self.0 {
            out.serialize_element(&SeriesJson(v))?;
        }
        out.end()
    }
}
impl serde::Serialize for RowList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for v in self.0 {
            out.serialize_element(&StructJson(v))?;
        }
        out.end()
    }
}
impl serde::Serialize for GroupList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for v in self.0 {
            out.serialize_element(&CountGroupJson(v))?;
        }
        out.end()
    }
}
struct SeriesFrameJson<'a>(&'a pb::SeriesFrame);
struct TableJson<'a>(&'a pb::TableFrame);
struct CountsJson<'a>(&'a pb::CountsFrame);
struct CountGroupJson<'a>(&'a pb::CountGroup);
struct ErrorJson<'a>(&'a pb::QueryError);
struct BudgetJson<'a>(&'a pb::Budget);
struct ResultsJson<'a>(&'a BTreeMap<String, pb::QueryResult>);
impl serde::Serialize for SeriesFrameJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry("series", &SeriesList(&self.0.series))?;
        out.end()
    }
}
impl serde::Serialize for TableJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry("rows", &RowList(&self.0.rows))?;
        out.end()
    }
}
impl serde::Serialize for CountsJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(3))?;
        out.serialize_entry("start", &self.0.start.as_ref().unwrap().to_string())?;
        out.serialize_entry("step", &self.0.step.as_ref().unwrap().to_string())?;
        out.serialize_entry("groups", &GroupList(&self.0.groups))?;
        out.end()
    }
}
impl serde::Serialize for CountGroupJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(2))?;
        out.serialize_entry("by", &self.0.by)?;
        out.serialize_entry("counts", &UIntList(&self.0.counts))?;
        out.end()
    }
}
impl serde::Serialize for ErrorJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(2))?;
        out.serialize_entry("reason", &self.0.reason)?;
        out.serialize_entry("message", &self.0.message)?;
        out.end()
    }
}
impl serde::Serialize for BudgetJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(3))?;
        out.serialize_entry("recordsRead", &self.0.records_read.to_string())?;
        out.serialize_entry("bytesRead", &self.0.bytes_read.to_string())?;
        out.serialize_entry("elapsedMs", &self.0.elapsed_ms.to_string())?;
        out.end()
    }
}
impl serde::Serialize for ResultsJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0 {
            out.serialize_entry(k, &ResultJson(v))?;
        }
        out.end()
    }
}
impl serde::Serialize for ResultJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(None)?;
        match self.0.body.as_ref().unwrap() {
            pb::query_result::Body::Series(v) => {
                out.serialize_entry("series", &SeriesFrameJson(v))?
            }
            pb::query_result::Body::Table(v) => out.serialize_entry("table", &TableJson(v))?,
            pb::query_result::Body::Counts(v) => out.serialize_entry("counts", &CountsJson(v))?,
            pb::query_result::Body::Error(v) => out.serialize_entry("error", &ErrorJson(v))?,
        }
        if !self.0.cursor.is_empty() {
            out.serialize_entry("cursor", &self.0.cursor)?;
        }
        out.end()
    }
}
impl serde::Serialize for ResponseJson<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("results", &ResultsJson(&self.0.results))?;
        if let Some(v) = &self.0.budget {
            out.serialize_entry("budget", &BudgetJson(v))?;
        }
        out.end()
    }
}

pub(super) fn measured<T: serde::Serialize>(value: &T) -> Result<u64> {
    struct Count(u64);
    impl Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("size overflow"))?;
            if self.0 > super::MAX_RESPONSE_BYTES as u64 {
                return Err(io::Error::other("document too large"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_err(|_| Error::ResponseLimit)?;
    Ok(count.0)
}
pub(super) fn series_size(v: &pb::Series) -> Result<u64> {
    measured(&SeriesJson(v))
}
pub(super) fn result_size(v: &pb::QueryResult) -> Result<u64> {
    measured(&ResultJson(v))
}
pub(super) fn table_row_size(v: &prost_types::Struct) -> Result<u64> {
    measured(&StructJson(v))
}
pub(super) fn response_size(v: &pb::QueryResponse) -> Result<u64> {
    measured(&ResponseJson(v))
}
pub(super) fn encode_response(v: &pb::QueryResponse) -> Result<Vec<u8>> {
    struct Limited(Vec<u8>);
    impl Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > super::MAX_RESPONSE_BYTES - self.0.len() {
                return Err(io::Error::other("document too large"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut out = Limited(Vec::new());
    serde_json::to_writer(&mut out, &ResponseJson(v)).map_err(|_| Error::ResponseLimit)?;
    Ok(out.0)
}
