//! Rates over actual ingested records, including late delivery and a shared raw query.
use masu_core::{
    metrics::Metrics,
    pb,
    query::{
        budget::Budget,
        document::{self, Encoding},
        evaluate::evaluate,
    },
    store::{Reply, Store},
    time,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
const START: i64 = 1_793_491_200_000_000_000;
struct Archive {
    root: PathBuf,
    store: Store,
    seq: u64,
}
impl Archive {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "masu-query-rate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let store = Store::open(&root, 0, Arc::new(Metrics::new())).unwrap();
        Self {
            root,
            store,
            seq: 0,
        }
    }
    fn push(&mut self, second: i64, value: &str) {
        self.seq += 1;
        let body = format!("# TYPE svc_total counter\nsvc_total {value}\n");
        let record = json!({"v":1,"ts":time::format_ns(START+second*1_000_000_000),"node":"node-a","service":"svc","stream":"07070707070707070707070707070707","seq":self.seq,"up":true,"dur_us":1,"content_type":"text/plain; version=0.0.4","body":body});
        let request = pb::PushRequest {
            node: "node-a".into(),
            stream_id: vec![7u8; 16].into(),
            first_seq: self.seq,
            records: vec![record.to_string().into_bytes().into()],
            status: None,
        };
        assert_eq!(
            self.store.push("node-a", &request),
            Reply::Ok {
                acked_seq: self.seq
            }
        );
    }
    fn evaluate(&self, to: i64, step: i64, with_raw: bool) -> (pb::QueryResponse, Budget) {
        let mut queries = json!({"rate":{"series":{"node":"node-a","service":"svc","name":"svc_total","rate":true,"step":format!("{step}s")}}});
        if with_raw {
            queries["raw"] = json!({"series":{"node":"node-a","service":"svc","name":"svc_total"}});
        }
        let request = json!({"range":{"from":time::format_ns(START),"to":time::format_ns(START+to*1_000_000_000)},"queries":queries});
        let request =
            document::decode_request(&serde_json::to_vec(&request).unwrap(), Encoding::Json)
                .unwrap();
        let mut budget = Budget::default();
        let response = evaluate(&self.root, &request, Encoding::Json, &mut budget).unwrap();
        (response, budget)
    }
}
impl Drop for Archive {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn series<'a>(response: &'a pb::QueryResponse, name: &str) -> &'a pb::Series {
    let Some(pb::query_result::Body::Series(frame)) = &response.results[name].body else {
        panic!("query did not return a series")
    };
    assert_eq!(frame.series.len(), 1);
    &frame.series[0]
}

#[test]
fn query_rate_late_arrivals_and_duplicate_scrape_timestamps_are_sorted_once_for_the_batch() {
    let mut a = Archive::new();
    a.push(40, "140");
    a.push(20, "100");
    a.push(40, "150");
    let (response, budget) = a.evaluate(60, 60, true);
    assert!((series(&response, "rate").values[0] - 50. / 60.).abs() < 1e-12);
    assert_eq!(series(&response, "raw").values, [100., 150.]);
    assert_eq!(budget.usage.records, 3);
    assert_eq!(budget.usage.samples, 3);
}
#[test]
fn query_rate_resets_and_partial_final_buckets_use_the_defined_duration() {
    let mut a = Archive::new();
    for (second, value) in [(22, "8"), (7, "20"), (12, "3"), (2, "10")] {
        a.push(second, value);
    }
    let (response, _) = a.evaluate(25, 10, false);
    assert_eq!(series(&response, "rate").values, [1., 0.3, 1.]);
}
#[test]
fn query_rate_nan_transitions_only_poison_the_buckets_containing_their_increments() {
    let mut a = Archive::new();
    for (second, value) in [(2, "0"), (7, "NaN"), (12, "3"), (22, "8")] {
        a.push(second, value);
    }
    let (response, _) = a.evaluate(30, 10, false);
    let values = &series(&response, "rate").values;
    assert!(values[0].is_nan());
    assert!(values[1].is_nan());
    assert_eq!(values[2], 0.5);
}
