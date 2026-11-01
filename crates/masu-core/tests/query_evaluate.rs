use masu_core::pb;
use masu_core::query::budget::{Budget, Error as BudgetError, Limits};
use masu_core::query::document::{self, DecodedRequest, Encoding};
use masu_core::query::evaluate::evaluate;
use masu_core::seal::Scan;
use masu_core::store::{FORMAT, State};
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Archive {
    root: PathBuf,
    state: State,
}

impl Archive {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "masu-query-evaluate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("FORMAT"), FORMAT).unwrap();
        let archive = Self {
            root,
            state: State {
                v: 1,
                ..State::default()
            },
        };
        archive.save_state();
        archive
    }

    fn save_state(&self) {
        fs::write(
            self.root.join("state.json"),
            serde_json::to_vec(&self.state).unwrap(),
        )
        .unwrap();
    }

    fn data(&self, node: &str, day: &str) -> PathBuf {
        self.root.join(format!("archive/{node}/{day}.ndjson.zst"))
    }

    fn write(&mut self, node: &str, day: &str, records: &[Value], sealed: bool) -> u64 {
        let dir = self.root.join("archive").join(node);
        fs::create_dir_all(&dir).unwrap();
        let mut bytes = Vec::new();
        let mut scan = Scan::default();
        for record in records {
            let line = format!("{record}\n");
            scan.line(line.as_bytes()).unwrap();
            bytes.extend_from_slice(line.as_bytes());
        }
        let compressed = zstd::encode_all(bytes.as_slice(), 3).unwrap();
        fs::write(self.data(node, day), &compressed).unwrap();
        let key = format!("{node}/{day}");
        if sealed {
            let manifest = scan.manifest(node, day, "2026-11-02T00:10:00.000000000Z".into());
            fs::write(
                dir.join(format!("{day}.manifest.json")),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            self.state.files.remove(&key);
        } else {
            self.state.files.insert(key, compressed.len() as u64);
        }
        self.save_state();
        compressed.len() as u64
    }
}

impl Drop for Archive {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn scrape(node: &str, second: i64, seq: u64, body: &str) -> Value {
    let start = masu_core::time::parse_ns("2026-11-01T00:00:00.000000000Z").unwrap();
    json!({
        "v": 1, "node": node, "service": "svc",
        "ts": masu_core::time::format_ns(start + second * 1_000_000_000),
        "recv_ts": masu_core::time::format_ns(start + (43_200 + seq as i64) * 1_000_000_000),
        "stream": if node == "node-b" { "22222222222222222222222222222222" } else { "11111111111111111111111111111111" }, "seq": seq,
        "up": true, "dur_us": 1, "content_type": "text/plain; version=0.0.4", "body": body
    })
}

fn request(from: &str, to: &str, queries: Value) -> DecodedRequest {
    document::decode_request(
        &serde_json::to_vec(&json!({"range": {"from": from, "to": to}, "queries": queries}))
            .unwrap(),
        Encoding::Json,
    )
    .unwrap()
}

fn minute(queries: Value) -> DecodedRequest {
    request("2026-11-01T00:00:00Z", "2026-11-01T00:01:00Z", queries)
}

fn series<'a>(response: &'a pb::QueryResponse, name: &str) -> &'a pb::SeriesFrame {
    match response.results[name].body.as_ref().unwrap() {
        pb::query_result::Body::Series(frame) => frame,
        other => panic!("Expected series for {name}, got {other:?}"),
    }
}

fn error<'a>(response: &'a pb::QueryResponse, name: &str) -> &'a str {
    match response.results[name].body.as_ref().unwrap() {
        pb::query_result::Body::Error(error) => &error.reason,
        other => panic!("Expected query error for {name}, got {other:?}"),
    }
}

fn table<'a>(response: &'a pb::QueryResponse, name: &str) -> &'a pb::TableFrame {
    match response.results[name].body.as_ref().unwrap() {
        pb::query_result::Body::Table(frame) => frame,
        other => panic!("Expected table for {name}, got {other:?}"),
    }
}

#[test]
fn query_select_batch_shares_archive_and_sample_work_without_poisoning_other_queries() {
    let mut archive = Archive::new();
    let records = [
        scrape(
            "node-a",
            0,
            1,
            "# TYPE svc_value gauge\nsvc_value{site=\"west\",node=\"sample\",service=\"other\"} 1\nsvc_other 10\n",
        ),
        scrape(
            "node-a",
            10,
            2,
            "# TYPE svc_value gauge\nsvc_value{site=\"west\",node=\"sample\",service=\"other\"} 2\nsvc_other 20\n",
        ),
    ];
    let compressed = archive.write("node-a", "2026-11-01", &records, false);
    let query = minute(json!({
        "raw": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}},
        "last": {"series": {"node": "node-a", "service": "svc", "name": "svc_value", "labels": {"site": "west"}, "reduce": "LAST"}},
        "health": {"up": {"node": "node-a", "service": "svc", "limit": 1}},
        "names": {"discover": {"node": "node-a", "service": "svc", "prefix": "svc_"}},
        "future": {"futureOperator": {}},
        "bad-node": {"up": {"node": "../escape", "service": "svc"}}
    }));
    let mut budget = Budget::default();
    let result = evaluate(&archive.root, &query, Encoding::Json, &mut budget).unwrap();
    assert_eq!(result.results.len(), 6);
    assert_eq!(series(&result, "raw").series[0].values, [1.0, 2.0]);
    assert_eq!(series(&result, "raw").series[0].labels["node"], "sample");
    assert_eq!(series(&result, "raw").series[0].labels["service"], "other");
    assert_eq!(series(&result, "last").series[0].values, [2.0]);
    assert_eq!(error(&result, "future"), "unsupported");
    assert_eq!(error(&result, "bad-node"), "invalid-query");
    assert!(!result.results["health"].cursor.is_empty());
    assert_eq!(budget.usage.records, 2);
    assert_eq!(
        budget.usage.samples, 4,
        "a successful scrape is parsed once for the entire batch"
    );
    assert_eq!(budget.usage.compressed_bytes, compressed);
    let after = &result.results["health"].cursor;
    let continuation = minute(json!({
        "health": {"up": {"node": "node-a", "service": "svc", "limit": 1, "after": after}}
    }));
    let next = evaluate(
        &archive.root,
        &continuation,
        Encoding::Json,
        &mut Budget::default(),
    )
    .unwrap();
    let rows = &table(&next, "health").rows;
    assert_eq!(rows.len(), 1);
    let Some(prost_types::value::Kind::StringValue(ts)) = rows[0].fields["ts"].kind.as_ref() else {
        panic!("up timestamp must remain a string");
    };
    assert_eq!(
        ts.parse::<prost_types::Timestamp>().unwrap().seconds,
        masu_core::time::parse_ns("2026-11-01T00:00:10.000000000Z").unwrap() / 1_000_000_000
    );
    assert!(next.results["health"].cursor.is_empty());
    let changed = minute(json!({
        "health": {"up": {"node": "node-a", "service": "other", "limit": 1, "after": after}}
    }));
    let result = evaluate(
        &archive.root,
        &changed,
        Encoding::Json,
        &mut Budget::default(),
    )
    .unwrap();
    assert_eq!(error(&result, "health"), "invalid-query");
}

#[test]
fn query_select_missing_sealed_days_only_affect_queries_whose_range_and_service_touch_them() {
    let mut archive = Archive::new();
    let mut old = scrape("node-a", 0, 1, "svc_value 1\n");
    old["ts"] = "2026-10-31T00:00:00.000000000Z".into();
    old["recv_ts"] = "2026-10-31T12:00:00.000000000Z".into();
    archive.write("node-a", "2026-10-31", &[old], true);
    fs::remove_file(archive.data("node-a", "2026-10-31")).unwrap();
    archive.write(
        "node-a",
        "2026-11-01",
        &[scrape("node-a", 0, 2, "svc_value 2\n")],
        false,
    );
    let queries = json!({
        "value": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}},
        "other-service": {"series": {"node": "node-a", "service": "other", "name": "other_value"}}
    });
    let mut budget = Budget::default();
    let result = evaluate(
        &archive.root,
        &minute(queries.clone()),
        Encoding::Json,
        &mut budget,
    )
    .unwrap();
    assert_eq!(series(&result, "value").series[0].values, [2.0]);
    assert_eq!(budget.usage.records, 1);
    let wider = request("2026-10-31T00:00:00Z", "2026-11-01T00:01:00Z", queries);
    let result = evaluate(
        &archive.root,
        &wider,
        Encoding::Json,
        &mut Budget::default(),
    )
    .unwrap();
    assert_eq!(error(&result, "value"), "archive-incomplete");
    assert!(series(&result, "other-service").series.is_empty());
}

#[test]
fn query_select_late_integrity_failure_cannot_publish_an_earlier_successful_frame() {
    let mut archive = Archive::new();
    archive.write(
        "node-a",
        "2026-11-01",
        &[scrape("node-a", 0, 1, "svc_value 1\n")],
        false,
    );
    let bad = scrape("node-b", 0, 1, "svc_value 2\n");
    archive.write("node-b", "2026-11-01", &[bad], true);
    let altered = format!("{}\n", scrape("node-b", 0, 1, "svc_value 99\n"));
    fs::write(
        archive.data("node-b", "2026-11-01"),
        zstd::encode_all(altered.as_bytes(), 3).unwrap(),
    )
    .unwrap();
    let query = minute(json!({
        "first": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}},
        "corrupt": {"series": {"node": "node-b", "service": "svc", "name": "svc_value"}}
    }));
    let mut budget = Budget::default();
    assert!(evaluate(&archive.root, &query, Encoding::Json, &mut budget).is_err());
    assert_eq!(
        budget.usage.records, 2,
        "the later hash failure must discard already accumulated results"
    );
}

#[test]
fn query_select_aggregate_result_budget_fails_the_batch_before_append() {
    let mut archive = Archive::new();
    archive.write(
        "node-a",
        "2026-11-01",
        &[
            scrape("node-a", 0, 1, "svc_value 1\n"),
            scrape("node-a", 10, 2, "svc_value 2\n"),
        ],
        false,
    );
    let query = minute(json!({
        "one": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}},
        "two": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}}
    }));
    let mut budget = Budget::new(Limits {
        result_items: 3,
        ..Limits::default()
    });
    assert!(evaluate(&archive.root, &query, Encoding::Json, &mut budget).is_err());
    assert_eq!(budget.check(), Err(BudgetError::ResponseLimit));
    assert!(budget.usage.result_items <= 3);
}

#[test]
fn query_select_response_byte_budget_is_shared_with_frame_construction() {
    let mut archive = Archive::new();
    archive.write(
        "node-a",
        "2026-11-01",
        &[scrape("node-a", 0, 1, "svc_value{site=\"west\"} 1\n")],
        false,
    );
    let query = minute(json!({
        "value": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}}
    }));
    let mut budget = Budget::new(Limits {
        response_bytes: 1,
        ..Limits::default()
    });
    assert!(evaluate(&archive.root, &query, Encoding::Json, &mut budget).is_err());
    assert_eq!(budget.check(), Err(BudgetError::ResponseLimit));
    assert!(budget.usage.response_bytes <= 1);
}

#[test]
fn query_select_many_inputs_can_reduce_to_a_small_exactly_counted_response() {
    let mut archive = Archive::new();
    let records: Vec<_> = (0..12_000)
        .map(|second| {
            scrape(
                "node-a",
                second,
                second as u64 + 1,
                &format!("# TYPE svc_value gauge\nsvc_value {}\n", second % 10),
            )
        })
        .collect();
    archive.write("node-a", "2026-11-01", &records, false);
    drop(records);
    let query = request(
        "2026-11-01T00:00:00Z",
        "2026-11-01T04:00:00Z",
        json!({
            "last": {"series": {"node": "node-a", "service": "svc", "name": "svc_value", "reduce": "LAST"}},
            "average": {"series": {"node": "node-a", "service": "svc", "name": "svc_value", "reduce": "AVG"}}
        }),
    );
    for encoding in [Encoding::Protobuf, Encoding::Json] {
        let mut budget = Budget::new(Limits {
            response_bytes: 4096,
            ..Limits::default()
        });
        let result = evaluate(&archive.root, &query, encoding, &mut budget).unwrap();
        assert_eq!(series(&result, "last").series[0].values, [9.0]);
        assert!((series(&result, "average").series[0].values[0] - 4.5).abs() < 1e-10);
        assert_eq!(budget.usage.records, 12_000);
        assert_eq!(budget.usage.samples, 12_000);
        let bytes = document::encode_response(&result, encoding).unwrap();
        assert_eq!(budget.usage.response_bytes, bytes.len() as u64);
        assert!(bytes.len() < 4096);
    }
}

#[test]
fn query_select_label_and_error_bytes_use_the_selected_encoding_without_double_counting() {
    let text = "\"\\\n".repeat(2000);
    for error_row in [false, true] {
        let mut archive = Archive::new();
        let record = if error_row {
            let mut record = scrape("node-a", 0, 1, "");
            record["up"] = false.into();
            record["error"] = text.clone().into();
            record.as_object_mut().unwrap().remove("body");
            record.as_object_mut().unwrap().remove("content_type");
            record
        } else {
            let escaped = text
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            scrape(
                "node-a",
                0,
                1,
                &format!("svc_value{{site=\"{escaped}\"}} 1\n"),
            )
        };
        archive.write("node-a", "2026-11-01", &[record], false);
        let query = minute(if error_row {
            json!({"value": {"up": {"node": "node-a", "service": "svc"}}})
        } else {
            json!({"value": {"series": {"node": "node-a", "service": "svc", "name": "svc_value"}}})
        });
        let limits = Limits {
            response_bytes: 10_000,
            ..Limits::default()
        };
        let mut budget = Budget::new(limits);
        let result = evaluate(&archive.root, &query, Encoding::Protobuf, &mut budget).unwrap();
        let bytes = document::encode_response(&result, Encoding::Protobuf).unwrap();
        assert_eq!(budget.usage.response_bytes, bytes.len() as u64);
        assert!(bytes.len() < 10_000);
        let mut budget = Budget::new(limits);
        assert!(evaluate(&archive.root, &query, Encoding::Json, &mut budget).is_err());
        assert_eq!(budget.check(), Err(BudgetError::ResponseLimit));
    }
}

#[test]
fn query_rate_uses_scrape_order_and_the_partial_last_bucket_duration() {
    let mut archive = Archive::new();
    archive.write(
        "node-a",
        "2026-11-01",
        &[
            scrape(
                "node-a",
                20,
                1,
                "# TYPE svc_counter_total counter\nsvc_counter_total 2\n",
            ),
            scrape(
                "node-a",
                0,
                2,
                "# TYPE svc_counter_total counter\nsvc_counter_total 10\n",
            ),
            scrape(
                "node-a",
                10,
                3,
                "# TYPE svc_counter_total counter\nsvc_counter_total 15\n",
            ),
        ],
        false,
    );
    let query = request(
        "2026-11-01T00:00:00Z",
        "2026-11-01T00:00:25Z",
        json!({
            "rate": {"series": {"node": "node-a", "service": "svc", "name": "svc_counter_total", "step": "10s", "rate": true}},
            "raw": {"series": {"node": "node-a", "service": "svc", "name": "svc_counter_total"}}
        }),
    );
    let mut budget = Budget::default();
    let result = evaluate(&archive.root, &query, Encoding::Json, &mut budget).unwrap();
    let rates = &series(&result, "rate").series[0].values;
    assert_eq!(rates.len(), 3);
    assert!(rates[0].is_nan());
    assert_eq!(rates[1], 0.5);
    assert_eq!(rates[2], 0.4);
    assert_eq!(series(&result, "raw").series[0].values, [10.0, 15.0, 2.0]);
    assert!(series(&result, "raw").series[0].labels.is_empty());
    assert_eq!(budget.usage.records, 3);
    assert_eq!(budget.usage.samples, 3);
}
