//! Scrape-record rules (docs/storage-format.md, ADR 0003): what masu checks on each pushed
//! record before storing it. The errors are the exact `PushRejected.reason` strings, published
//! with their examples in fixtures/records/ (ADR 0009).

use serde_json::{Map, Value};

/// A stored record is at most 4 MiB.
pub const MAX_STORED_BYTES: usize = 4 * 1024 * 1024;
/// `,"recv_ts":"2026-10-19T08:14:00.391022117Z"`, inserted by masu before the final `}`.
pub const RECV_TS_MEMBER_BYTES: usize = 43;
/// The largest record an agent may push.
pub const MAX_PUSHED_BYTES: usize = MAX_STORED_BYTES - RECV_TS_MEMBER_BYTES;

/// Checks one pushed record; `Err` holds the `PushRejected.reason`.
///
/// One reason per record, the first rule broken in this order: size, JSON, object, framing,
/// `recv_ts`, then the envelope fields in the order of storage-format.md.
pub fn check(record: &[u8]) -> Result<(), String> {
    if record.len() > MAX_PUSHED_BYTES {
        return Err("record-too-large".into());
    }
    // shortcut: a duplicated key keeps its last value (serde_json); reject duplicates if hikyaku
    // or a store ever reads records with a first-wins parser.
    let Ok(value) = serde_json::from_slice::<Value>(record) else {
        return Err("not-json".into());
    };
    let Some(obj) = value.as_object() else {
        return Err("not-object".into());
    };
    // Stored as one NDJSON line, with recv_ts spliced in before the final byte.
    if record.first() != Some(&b'{')
        || record.last() != Some(&b'}')
        || record.iter().any(|&b| b == b'\n' || b == b'\r')
    {
        return Err("framing".into());
    }
    if obj.contains_key("recv_ts") {
        return Err("reserved-field:recv_ts".into());
    }
    if field(obj, "v", Value::as_u64)? != 1 {
        return Err("unsupported-version".into());
    }
    if !is_timestamp(field(obj, "ts", Value::as_str)?) {
        return Err("ts-format".into());
    }
    if !is_node(field(obj, "node", Value::as_str)?) {
        return Err("node-format".into());
    }
    field(obj, "service", Value::as_str)?;
    let stream = field(obj, "stream", Value::as_str)?;
    if stream.len() != 32
        || !stream
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err("stream-format".into());
    }
    field(obj, "seq", Value::as_u64)?;
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
    Ok(())
}

fn field<'a, T>(
    obj: &'a Map<String, Value>,
    name: &str,
    get: impl Fn(&'a Value) -> Option<T>,
) -> Result<T, String> {
    let v = obj
        .get(name)
        .ok_or_else(|| format!("missing-field:{name}"))?;
    get(v).ok_or_else(|| format!("field-type:{name}"))
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
fn is_node(s: &str) -> bool {
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
}
