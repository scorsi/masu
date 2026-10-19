//! Own logs: JSON lines on stderr in nisshi's log schema v1 (ADR 0002): `v`, `ts`, `level`,
//! `event`, optional `msg`, `err` only with `warn`/`error`, then the event's fields. No
//! reserved field (`node`, `service`, `stream`, `seq`, `source`, `recv_ts`, `_*`) is ever used
//! as a field name here: `seq-gap` uses `gap_stream`, `from_seq`, `to_seq` as nisshi does.

use serde_json::{Map, Value, json};

pub fn info(event: &str, fields: Value) {
    eprintln!(
        "{}",
        line("info", event, None, fields, crate::time::now_ns())
    );
}

pub fn warn(event: &str, err: &str, fields: Value) {
    eprintln!(
        "{}",
        line("warn", event, Some(err), fields, crate::time::now_ns())
    );
}

pub fn error(event: &str, err: &str, fields: Value) {
    eprintln!(
        "{}",
        line("error", event, Some(err), fields, crate::time::now_ns())
    );
}

fn line(level: &str, event: &str, err: Option<&str>, fields: Value, now_ns: i64) -> String {
    let mut m = Map::new();
    m.insert("v".into(), json!(1));
    m.insert("ts".into(), json!(crate::time::format_ns(now_ns)));
    m.insert("level".into(), json!(level));
    m.insert("event".into(), json!(event));
    if let Some(err) = err {
        m.insert("err".into(), json!(err));
    }
    if let Value::Object(extra) = fields {
        m.extend(extra);
    }
    Value::Object(m).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-checked against nisshi's docs/log-schema.md (serde_json keeps insertion order only
    /// with `preserve_order`; the schema does not care about member order, these lines were
    /// checked member by member).
    #[test]
    fn schema_lines() {
        let now = crate::time::parse_ns("2026-10-19T08:14:03.512004117Z").unwrap();
        let got: Value = serde_json::from_str(&line(
            "info",
            "serve-start",
            None,
            json!({"version": "0.1.0"}),
            now,
        ))
        .unwrap();
        assert_eq!(
            got,
            json!({"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"serve-start","version":"0.1.0"})
        );
        let got: Value = serde_json::from_str(&line(
            "warn",
            "seq-gap",
            Some("first_seq 21 > hwm 10 + 1"),
            json!({"gap_stream": "7f3c", "from_seq": 11, "to_seq": 20}),
            now,
        ))
        .unwrap();
        assert_eq!(
            got,
            json!({"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"warn","event":"seq-gap","err":"first_seq 21 > hwm 10 + 1","gap_stream":"7f3c","from_seq":11,"to_seq":20})
        );
        // `err` never appears on an info line; `v` is the integer 1; `ts` has 9 digits.
        let text = line("info", "seal", None, json!({}), now);
        assert!(!text.contains("\"err\""));
        assert!(text.contains("\"v\":1,") || text.contains("\"v\":1}"));
        assert!(text.contains("\"ts\":\"2026-10-19T08:14:03.512004117Z\""));
        assert!(!text.contains('\n'));
    }
}
