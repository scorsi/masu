use masu_core::pb;
use masu_core::query::document::{self, DecodedQuery, Encoding, Error};
use prost::Message;
use prost_types::{Duration, Timestamp};
use std::collections::BTreeMap;

fn range() -> pb::Range {
    pb::Range {
        from: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 1,
        }),
        to: Some(Timestamp {
            seconds: 1_700_000_001,
            nanos: 2,
        }),
    }
}
fn request() -> pb::QueryRequest {
    pb::QueryRequest {
        range: Some(range()),
        queries: BTreeMap::from([(
            "tile".into(),
            pb::Query {
                body: Some(pb::query::Body::Series(pb::SeriesQuery {
                    node: "node-a".into(),
                    service: "backup".into(),
                    name: "backup_timestamp_seconds".into(),
                    step: Some(Duration {
                        seconds: 0,
                        nanos: 250_000_001,
                    }),
                    reduce: pb::Reduce::Last as i32,
                    ..Default::default()
                })),
            },
        )]),
    }
}
fn field(tag: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, &mut out);
    prost::encoding::encode_varint(body.len() as u64, &mut out);
    out.extend_from_slice(body);
    out
}
fn varint(tag: u32, value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, &mut out);
    prost::encoding::encode_varint(value, &mut out);
    out
}
fn wire_query(query: &[u8]) -> Vec<u8> {
    let mut out = field(1, &range().encode_to_vec());
    let mut entry = field(1, b"tile");
    entry.extend(field(2, query));
    out.extend(field(2, &entry));
    out
}
fn known(query: DecodedQuery) -> pb::Query {
    match query {
        DecodedQuery::Known(q) => q,
        _ => panic!("expected known query"),
    }
}

#[test]
fn mirrored_envelope_cases() {
    let cases: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/query-document/requests.json"
    ))
    .unwrap();
    for case in cases.as_array().unwrap() {
        let result = document::decode_request(
            case["document"].as_str().unwrap().as_bytes(),
            Encoding::Json,
        );
        let actual = match result {
            Err(e) => e.reason(),
            Ok(q) => {
                if matches!(q.queries["tile"], DecodedQuery::Unsupported) {
                    "unsupported"
                } else {
                    "ok"
                }
            }
        };
        assert_eq!(
            actual,
            case["expected"].as_str().unwrap(),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn both_encodings_keep_nanoseconds_operators_and_enum_names() {
    let mut q = request();
    q.queries.insert(
        "up".into(),
        pb::Query {
            body: Some(pb::query::Body::Up(pb::UpQuery {
                node: "node-a".into(),
                service: "backup".into(),
                limit: 10,
                after: "opaque".into(),
            })),
        },
    );
    q.queries.insert(
        "discovery".into(),
        pb::Query {
            body: Some(pb::query::Body::Discover(pb::DiscoverQuery {
                node: "node-a".into(),
                service: "backup".into(),
                prefix: "backup_".into(),
            })),
        },
    );
    for encoding in [Encoding::Protobuf, Encoding::Json] {
        let bytes = document::encode_request(&q, encoding).unwrap();
        if matches!(encoding, Encoding::Json) {
            assert!(std::str::from_utf8(&bytes).unwrap().contains("\"LAST\""));
        }
        let decoded = document::decode_request(&bytes, encoding).unwrap();
        assert_eq!(decoded.range, q.range.clone().unwrap());
        for (key, value) in q.queries.clone() {
            assert_eq!(
                known(
                    decoded
                        .queries
                        .get(&key)
                        .map(|v| match v {
                            DecodedQuery::Known(q) => DecodedQuery::Known(q.clone()),
                            _ => panic!(),
                        })
                        .unwrap()
                ),
                value
            );
        }
    }
}

#[test]
fn unknown_members_are_isolated_at_every_query_depth() {
    let known = request().queries["tile"].encode_to_vec();
    for unknown in [
        field(100, &[]),
        field(
            1,
            &[pb::SeriesQuery::default().encode_to_vec(), varint(100, 1)].concat(),
        ),
        field(1, &field(5, &varint(100, 1))),
        field(
            1,
            &field(
                4,
                &[field(1, b"x"), field(2, b"y"), varint(100, 1)].concat(),
            ),
        ),
    ] {
        let bytes = wire_query(&[known.clone(), unknown.clone()].concat());
        // Distinct known oneof bodies are invalid in the strict wire subset, so put the
        // unknown child in its own query instead of merging two singular bodies.
        let bytes = if unknown_first_is_series(&unknown) {
            wire_query(&unknown)
        } else {
            bytes
        };
        assert!(matches!(
            document::decode_request(&bytes, Encoding::Protobuf)
                .unwrap()
                .queries["tile"],
            DecodedQuery::Unsupported
        ));
    }
    let mut q=serde_json::to_value(serde_json::json!({"range":{"from":"2026-11-01T00:00:00Z","to":"2026-11-01T00:01:00Z"},"queries":{"good":{"up":{"node":"n","service":"s"}},"bad":{"series":{"node":"n","future":true}},"future":{"series":false,"zzz":{}},"step":{"series":{"step":{"future":1}}}}})).unwrap();
    let decoded =
        document::decode_request(&serde_json::to_vec(&q).unwrap(), Encoding::Json).unwrap();
    assert!(matches!(decoded.queries["good"], DecodedQuery::Known(_)));
    for key in ["bad", "future", "step"] {
        assert!(matches!(decoded.queries[key], DecodedQuery::Unsupported));
    }
    q["unknown"] = true.into();
    assert_eq!(
        document::decode_request(&serde_json::to_vec(&q).unwrap(), Encoding::Json).unwrap_err(),
        Error::InvalidRequest
    );
}
fn unknown_first_is_series(bytes: &[u8]) -> bool {
    bytes.first() == Some(&10)
}

#[test]
fn unknown_envelope_and_wkt_fields_fail_the_request() {
    let mut bytes = document::encode_request(&request(), Encoding::Protobuf).unwrap();
    bytes.extend(varint(100, 1));
    assert_eq!(
        document::decode_request(&bytes, Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    let ts = [range().from.unwrap().encode_to_vec(), varint(100, 1)].concat();
    let mut bytes = field(
        1,
        &[
            field(1, &ts),
            field(2, &range().to.unwrap().encode_to_vec()),
        ]
        .concat(),
    );
    bytes.extend(field(2, &[field(1, b"tile"), field(2, &[])].concat()));
    assert_eq!(
        document::decode_request(&bytes, Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
}

#[test]
fn duplicate_maps_and_singular_message_merges_cannot_hide_keys() {
    let mut bytes = wire_query(&[]);
    bytes.extend(field(2, &[field(1, b"tile"), field(2, &[])].concat()));
    assert_eq!(
        document::decode_request(&bytes, Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    let label = field(4, &[field(1, b"x"), field(2, b"y")].concat());
    let duplicate = field(1, &[label.clone(), label.clone()].concat());
    assert_eq!(
        document::decode_request(&wire_query(&duplicate), Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    let merged = [field(1, &label), field(1, &label)].concat();
    assert_eq!(
        document::decode_request(&wire_query(&merged), Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    let mut entry = field(1, b"tile");
    entry.extend(field(2, &field(1, &label)));
    entry.extend(field(2, &field(1, &label)));
    let bytes = [field(1, &range().encode_to_vec()), field(2, &entry)].concat();
    assert_eq!(
        document::decode_request(&bytes, Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
}

#[test]
fn query_parameter_errors_do_not_poison_the_batch_and_unknown_wins() {
    let q = serde_json::json!({"range":{"from":"2026-11-01T00:00:00Z","to":"2026-11-01T00:01:00Z"},"queries":{"good":{"up":{"node":"n","service":"s"}},"bad":{"series":{"step":"not-a-duration"}},"future":{"series":{"step":"bad","future":1}}}});
    let d = document::decode_request(&serde_json::to_vec(&q).unwrap(), Encoding::Json).unwrap();
    assert!(matches!(d.queries["good"], DecodedQuery::Known(_)));
    assert!(matches!(d.queries["bad"], DecodedQuery::Invalid));
    assert!(matches!(d.queries["future"], DecodedQuery::Unsupported));
    let invalid = field(1, &field(5, &varint(2, 2_000_000_000)));
    assert!(matches!(
        document::decode_request(&wire_query(&invalid), Encoding::Protobuf)
            .unwrap()
            .queries["tile"],
        DecodedQuery::Invalid
    ));
    let future = field(
        1,
        &field(5, &[varint(2, 2_000_000_000), varint(100, 1)].concat()),
    );
    assert!(matches!(
        document::decode_request(&wire_query(&future), Encoding::Protobuf)
            .unwrap()
            .queries["tile"],
        DecodedQuery::Unsupported
    ));
}

#[test]
fn range_domains_and_resource_limits_are_distinct() {
    let mut q = request();
    q.range.as_mut().unwrap().to.as_mut().unwrap().seconds += 31 * 86_400;
    assert_eq!(
        document::encode_request(&q, Encoding::Protobuf).unwrap_err(),
        Error::ScanLimit
    );
    q.range.as_mut().unwrap().to.as_mut().unwrap().seconds = 1;
    assert_eq!(
        document::encode_request(&q, Encoding::Json).unwrap_err(),
        Error::InvalidRequest
    );
    q = request();
    q.range.as_mut().unwrap().from.as_mut().unwrap().nanos = -1;
    assert_eq!(
        document::decode_request(&q.encode_to_vec(), Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    q = request();
    for n in 0..64 {
        q.queries.insert(n.to_string(), pb::Query::default());
    }
    assert_eq!(
        document::decode_request(&q.encode_to_vec(), Encoding::Protobuf).unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        document::decode_request(&vec![0; document::MAX_REQUEST_BYTES + 1], Encoding::Json)
            .unwrap_err(),
        Error::RequestTooLarge
    );
    q = request();
    if let Some(pb::query::Body::Series(s)) = &mut q.queries.get_mut("tile").unwrap().body {
        s.name = "\n".repeat(33_000);
    }
    assert_eq!(
        document::encode_request(&q, Encoding::Json).unwrap_err(),
        Error::RequestTooLarge
    );
}

fn response() -> pb::QueryResponse {
    let t = range().from.unwrap();
    let series = pb::Series {
        labels: std::collections::HashMap::from([("peer".into(), "n\"\\\n".into())]),
        ts: vec![t.clone(); 4],
        values: vec![1.25, f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
        ..Default::default()
    };
    let row = prost_types::Struct {
        fields: BTreeMap::from([
            (
                "ts".into(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(t.to_string())),
                },
            ),
            (
                "nested".into(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NullValue(0)),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NumberValue(3.5)),
                                },
                            ],
                        },
                    )),
                },
            ),
        ]),
    };
    pb::QueryResponse {
        results: BTreeMap::from([
            (
                "series".into(),
                pb::QueryResult {
                    body: Some(pb::query_result::Body::Series(pb::SeriesFrame {
                        series: vec![series],
                    })),
                    ..Default::default()
                },
            ),
            (
                "up".into(),
                pb::QueryResult {
                    body: Some(pb::query_result::Body::Table(pb::TableFrame {
                        rows: vec![row],
                    })),
                    cursor: "opaque".into(),
                },
            ),
            (
                "counts".into(),
                pb::QueryResult {
                    body: Some(pb::query_result::Body::Counts(pb::CountsFrame {
                        start: Some(t),
                        step: Some(Duration {
                            seconds: 1,
                            nanos: 1,
                        }),
                        groups: vec![pb::CountGroup {
                            by: Default::default(),
                            counts: vec![0, u64::MAX],
                        }],
                    })),
                    ..Default::default()
                },
            ),
            (
                "error".into(),
                pb::QueryResult {
                    body: Some(pb::query_result::Body::Error(pb::QueryError {
                        reason: "unsupported".into(),
                        message: "Operator not available".into(),
                    })),
                    ..Default::default()
                },
            ),
        ]),
        budget: Some(pb::Budget {
            records_read: u64::MAX,
            bytes_read: 100,
            elapsed_ms: 1,
        }),
    }
}

#[test]
fn all_response_frames_roundtrip_both_encodings() {
    let r = response();
    for encoding in [Encoding::Protobuf, Encoding::Json] {
        let bytes = document::encode_response(&r, encoding).unwrap();
        let decoded = document::decode_response(&bytes, encoding).unwrap();
        assert_eq!(decoded.budget, r.budget);
        assert_eq!(decoded.results["counts"], r.results["counts"]);
        assert_eq!(decoded.results["up"], r.results["up"]);
        assert_eq!(decoded.results["error"], r.results["error"]);
        let pb::query_result::Body::Series(f) = decoded.results["series"].body.as_ref().unwrap()
        else {
            panic!()
        };
        let s = &f.series[0];
        assert_eq!(s.ts.len(), 4);
        assert_eq!(s.values[0], 1.25);
        assert!(s.values[1].is_nan());
        assert_eq!(s.values[2], f64::INFINITY);
        assert_eq!(s.values[3], f64::NEG_INFINITY);
        if matches!(encoding, Encoding::Json) {
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(text.contains("\"NaN\""));
            assert!(text.contains("\"18446744073709551615\""));
            assert!(text.contains("1.000000001s"));
        }
    }
}

#[test]
fn protojson_uints_accept_exact_decimal_exponents_and_reject_fraction_overflow() {
    let input =
        String::from_utf8(document::encode_response(&response(), Encoding::Json).unwrap()).unwrap();
    for value in [
        "1e3",
        "\"1e3\"",
        "\"184467440737095516150e-1\"",
        "184467440737095516150e-1",
    ] {
        let input = input.replace("\"18446744073709551615\"", value);
        let r = document::decode_response(input.as_bytes(), Encoding::Json).unwrap();
        let expected = if value.contains("1e3") {
            1000
        } else {
            u64::MAX
        };
        assert_eq!(r.budget.unwrap().records_read, expected);
    }
    for value in [
        "1.5",
        "\"1.5\"",
        "\"18446744073709551616\"",
        "18446744073709551616e0",
        "\"1e100000\"",
    ] {
        let input = input.replace("\"18446744073709551615\"", value);
        assert!(
            document::decode_response(input.as_bytes(), Encoding::Json).is_err(),
            "{value}"
        );
    }
}

#[test]
fn response_rejects_unknowns_duplicates_bad_shapes_and_null_array_items() {
    let r = response();
    let bytes = document::encode_response(&r, Encoding::Json).unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["results"]["series"]["series"]["series"][0]["future"] = true.into();
    assert_eq!(
        document::decode_response(&serde_json::to_vec(&v).unwrap(), Encoding::Json).unwrap_err(),
        Error::InvalidResponse
    );
    let input = String::from_utf8(bytes).unwrap().replace(
        "\"recordsRead\":",
        "\"records_read\":\"1\",\"recordsRead\":",
    );
    assert!(document::decode_response(input.as_bytes(), Encoding::Json).is_err());
    for path in ["series", "counts"] {
        let mut v: serde_json::Value =
            serde_json::from_slice(&document::encode_response(&r, Encoding::Json).unwrap())
                .unwrap();
        if path == "series" {
            v["results"]["series"]["series"]["series"][0] = serde_json::Value::Null;
        } else {
            v["results"]["counts"]["counts"]["groups"][0] = serde_json::Value::Null;
        }
        assert!(
            document::decode_response(&serde_json::to_vec(&v).unwrap(), Encoding::Json).is_err()
        );
    }
    let mut bad = r.clone();
    if let Some(pb::query_result::Body::Series(f)) =
        &mut bad.results.get_mut("series").unwrap().body
    {
        f.series[0].ts.pop();
    }
    assert_eq!(
        document::encode_response(&bad, Encoding::Protobuf).unwrap_err(),
        Error::InvalidResponse
    );
    let mut wire = r.encode_to_vec();
    wire.extend(varint(100, 1));
    assert_eq!(
        document::decode_response(&wire, Encoding::Protobuf).unwrap_err(),
        Error::InvalidResponse
    );
}

#[test]
fn response_and_depth_limits_are_checked_before_decoding_or_output() {
    assert_eq!(
        document::decode_response(
            &vec![0; document::MAX_RESPONSE_BYTES + 1],
            Encoding::Protobuf
        )
        .unwrap_err(),
        Error::ResponseLimit
    );
    let mut r = response();
    r.results.get_mut("error").unwrap().body =
        Some(pb::query_result::Body::Error(pb::QueryError {
            reason: "unsupported".into(),
            message: "\n".repeat(document::MAX_RESPONSE_BYTES / 2 + 1),
        }));
    assert_eq!(
        document::encode_response(&r, Encoding::Json).unwrap_err(),
        Error::ResponseLimit
    );
    let mut nested = serde_json::Value::Null;
    for _ in 0..70 {
        nested = serde_json::json!([nested]);
    }
    let input=serde_json::to_string(&serde_json::json!({"range":{"from":"2026-11-01T00:00:00Z","to":"2026-11-01T00:01:00Z"},"queries":{"a":{"future":nested}}})).unwrap();
    assert_eq!(
        document::decode_request(input.as_bytes(), Encoding::Json).unwrap_err(),
        Error::InvalidRequest
    );
}

#[test]
fn malformed_truncated_wire_never_panics() {
    let q = request().encode_to_vec();
    for n in 0..q.len() {
        let _ = document::decode_request(&q[..n], Encoding::Protobuf);
    }
    for input in [
        &[0xff][..],
        &[0x0a, 0xff, 0xff],
        &[0x00],
        &[
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1,
        ],
    ] {
        assert!(document::decode_request(input, Encoding::Protobuf).is_err());
    }
}

#[test]
fn unknown_numeric_payload_does_not_poison_a_known_neighbour() {
    let input=br#"{"range":{"from":"2026-11-01T00:00:00Z","to":"2026-11-01T00:01:00Z"},"queries":{"good":{"up":{"node":"n","service":"s"}},"future":{"newOperator":{"big":1e400}}}}"#;
    let q = document::decode_request(input, Encoding::Json).unwrap();
    assert!(matches!(q.queries["good"], DecodedQuery::Known(_)));
    assert!(matches!(q.queries["future"], DecodedQuery::Unsupported));
}

#[test]
fn doubles_keep_negative_zero_and_struct_numbers_are_not_strings() {
    let mut r = response();
    if let Some(pb::query_result::Body::Series(f)) = &mut r.results.get_mut("series").unwrap().body
    {
        f.series[0].values[0] = -0.0;
    }
    if let Some(pb::query_result::Body::Table(f)) = &mut r.results.get_mut("up").unwrap().body {
        f.rows[0].fields.insert(
            "zero".into(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(-0.0)),
            },
        );
    }
    for encoding in [Encoding::Json, Encoding::Protobuf] {
        let bytes = document::encode_response(&r, encoding).unwrap();
        let decoded = document::decode_response(&bytes, encoding).unwrap();
        let Some(pb::query_result::Body::Series(f)) = &decoded.results["series"].body else {
            panic!()
        };
        assert_eq!(f.series[0].values[0].to_bits(), (-0.0f64).to_bits());
        let Some(pb::query_result::Body::Table(f)) = &decoded.results["up"].body else {
            panic!()
        };
        let Some(prost_types::value::Kind::NumberValue(n)) = f.rows[0].fields["zero"].kind else {
            panic!()
        };
        assert!(n.is_sign_negative());
    }
    let input = String::from_utf8(document::encode_response(&r, Encoding::Json).unwrap())
        .unwrap()
        .replace("3.5", "1e3");
    let decoded = document::decode_response(input.as_bytes(), Encoding::Json).unwrap();
    let Some(pb::query_result::Body::Table(f)) = &decoded.results["up"].body else {
        panic!()
    };
    let Some(prost_types::value::Kind::ListValue(l)) = &f.rows[0].fields["nested"].kind else {
        panic!()
    };
    assert_eq!(
        l.values[1].kind,
        Some(prost_types::value::Kind::NumberValue(1000.0))
    );
}

#[test]
fn timestamp_offsets_and_wkt_domains_keep_nanoseconds() {
    let input=br#"{"range":{"from":"2026-11-01T01:00:00.000000001+01:00","to":"2026-11-01T00:00:00.000000002Z"},"queries":{"a":{}}}"#;
    let decoded = document::decode_request(input, Encoding::Json).unwrap();
    assert_eq!(decoded.range.from.unwrap().nanos, 1);
    assert_eq!(decoded.range.to.unwrap().nanos, 2);
    let input=br#"{"range":{"from":"0001-01-01T00:00:00Z","to":"0001-01-01T00:00:00.000000001Z"},"queries":{"a":{}}}"#;
    assert!(document::decode_request(input, Encoding::Json).is_ok());
    let input=br#"{"range":{"from":"0000-01-01T00:00:00Z","to":"0000-01-01T00:00:00.000000001Z"},"queries":{"a":{}}}"#;
    assert_eq!(
        document::decode_request(input, Encoding::Json).unwrap_err(),
        Error::InvalidRequest
    );
}

#[test]
fn nested_response_wkt_unknowns_map_duplicates_and_bad_packed_values_are_rejected() {
    let counts = [
        field(1, &range().from.unwrap().encode_to_vec()),
        field(
            2,
            &[
                Duration {
                    seconds: 1,
                    nanos: 0,
                }
                .encode_to_vec(),
                varint(100, 1),
            ]
            .concat(),
        ),
    ]
    .concat();
    let entry = [field(1, b"counts"), field(2, &field(3, &counts))].concat();
    assert_eq!(
        document::decode_response(&field(1, &entry), Encoding::Protobuf).unwrap_err(),
        Error::InvalidResponse
    );
    let bad_packed = field(5, &[0; 7]);
    let entry = [
        field(1, b"series"),
        field(2, &field(1, &field(1, &bad_packed))),
    ]
    .concat();
    assert_eq!(
        document::decode_response(&field(1, &entry), Encoding::Protobuf).unwrap_err(),
        Error::InvalidResponse
    );
    let label = [field(1, b"x"), field(2, b"y")].concat();
    let duplicates = [field(1, &label), field(1, &label)].concat();
    let entry = [
        field(1, b"series"),
        field(2, &field(1, &field(1, &duplicates))),
    ]
    .concat();
    assert_eq!(
        document::decode_response(&field(1, &entry), Encoding::Protobuf).unwrap_err(),
        Error::InvalidResponse
    );
}
