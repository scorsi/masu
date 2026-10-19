//! Every line of fixtures/records/records.ndjson through masu's record check: same verdict,
//! same `PushRejected.reason` (ADR 0009; the line format is in fixtures/records/README.md).

use masu::pb::PushRejected;
use masu::record;
use prost::Message;
use serde_json::Value;

fn input(line: &Value) -> Vec<u8> {
    if let Some(s) = line["input"].as_str() {
        assert!(line.get("generate").is_none());
        return s.as_bytes().to_vec();
    }
    let g = &line["generate"];
    let fill = g["fill"].as_str().unwrap();
    let count = g["count"].as_u64().unwrap() as usize;
    [
        g["prefix"].as_str().unwrap(),
        &fill.repeat(count),
        g["suffix"].as_str().unwrap(),
    ]
    .concat()
    .into_bytes()
}

#[test]
fn records_fixtures() {
    let file = include_str!("../fixtures/records/records.ndjson");
    let mut reasons = std::collections::BTreeSet::new();
    for (i, text) in file.lines().enumerate() {
        let line: Value = serde_json::from_str(text).unwrap();
        let record = input(&line);
        let valid = line["valid"].as_bool().unwrap();
        let reason = line["reason"].as_str().unwrap();
        let got = record::check(&record);
        let want = if valid {
            Ok(())
        } else {
            Err(reason.to_string())
        };
        assert_eq!(got, want, "line {}", i + 1);
        assert!(
            !valid || reason.is_empty(),
            "line {}: valid with a reason",
            i + 1
        );
        reasons.insert(reason.to_string());
    }
    // The milestone's list (M0): each one exercised.
    for r in [
        "missing-field:ts",
        "ts-format",
        "missing-field:body",
        "reserved-field:recv_ts",
        "not-object",
        "record-too-large",
    ] {
        assert!(reasons.contains(r), "no fixture for {r}");
    }
}

/// The 400 body carries the reason: record-level with an index, request-level without.
#[test]
fn push_rejected_round_trip() {
    for index in [Some(3), None] {
        let rejected = PushRejected {
            record_index: index,
            reason: "ts-format".into(),
        };
        let decoded = PushRejected::decode(rejected.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, rejected);
    }
}
