//! Every line of fixtures/records/records.ndjson through masu's record check: same verdict,
//! same `PushRejected.reason` (ADR 0009; the line format is in fixtures/records/README.md).

use masu::pb::PushRejected;
use masu::record;
use prost::Message;
use serde_json::Value;

fn input(line: &Value) -> Vec<u8> {
    let forms = ["input", "input_b64", "generate"];
    let present = forms.iter().filter(|f| line.get(**f).is_some()).count();
    assert_eq!(present, 1, "exactly one of {forms:?}");
    if let Some(s) = line["input"].as_str() {
        return s.as_bytes().to_vec();
    }
    if let Some(s) = line["input_b64"].as_str() {
        return base64(s);
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

/// Standard base64 with padding (RFC 4648 section 4); panics on anything else.
fn base64(s: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    assert!(s.len().is_multiple_of(4), "base64 length");
    let mut out = Vec::new();
    for chunk in s.as_bytes().chunks(4) {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        assert!(pad <= 2, "base64 padding");
        let mut n = 0u32;
        for &c in &chunk[..4 - pad] {
            let d = ALPHABET
                .iter()
                .position(|&a| a == c)
                .expect("base64 alphabet");
            n = n << 6 | d as u32;
        }
        n <<= 6 * pad;
        out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
    }
    out
}

#[test]
fn base64_decodes() {
    assert_eq!(base64("/w=="), [0xff]);
    assert_eq!(base64("eyJ2IjoxfQ=="), br#"{"v":1}"#);
    assert_eq!(base64("YWJj"), b"abc");
    assert_eq!(base64("YWI="), b"ab");
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
    // The milestone's list (M0) and the owner decisions of 2026-10-19: each one exercised.
    // `not-object` has no fixture: after `framing`, valid JSON is always an object.
    for r in [
        "missing-field:ts",
        "ts-format",
        "missing-field:body",
        "reserved-field:recv_ts",
        "record-too-large",
        "not-utf8",
        "framing",
        "not-json",
        "duplicate-field:v",
        "v-format",
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
