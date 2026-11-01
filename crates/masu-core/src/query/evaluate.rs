//! One archive traversal per node for a batch; query failures never become partial successes.
use super::{
    budget::{Budget, Charge},
    document::{DecodedQuery, DecodedRequest, Encoding},
    snapshot,
};
use crate::{
    exposition::{self, Family, Kind},
    pb, record,
    seal::Manifest,
    time,
};
use prost::Message;
use prost_types::{Struct, Value, value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
mod series;
use series::SeriesEvaluator;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StoredRecord {
    pub ts: i128,
    pub recv_ts: i128,
    pub stream: String,
    pub seq: u64,
}
#[derive(Debug)]
pub(super) enum Failure {
    Query(&'static str),
    Budget(super::budget::Error),
}
impl From<super::budget::Error> for Failure {
    fn from(e: super::budget::Error) -> Self {
        Self::Budget(e)
    }
}
#[derive(Debug)]
pub enum Error {
    Archive(snapshot::Error),
    Budget(super::budget::Error),
    InvalidRequest,
}
impl Error {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid-request",
            Self::Budget(super::budget::Error::ResponseLimit) => "response-limit",
            Self::Budget(super::budget::Error::ScanLimit) => "scan-limit",
            Self::Budget(super::budget::Error::Timeout | super::budget::Error::Cancelled) => {
                "query-timeout"
            }
            Self::Archive(snapshot::Error::Budget(e)) => match e {
                super::budget::Error::ResponseLimit => "response-limit",
                super::budget::Error::ScanLimit => "scan-limit",
                super::budget::Error::Timeout | super::budget::Error::Cancelled => "query-timeout",
            },
            Self::Archive(snapshot::Error::Io(_)) => "archive-io",
            Self::Archive(snapshot::Error::Corrupt(_)) => "archive-corrupt",
            Self::Archive(snapshot::Error::Unavailable) => "snapshot-unavailable",
            Self::Archive(snapshot::Error::ArchiveIncomplete) => "archive-incomplete",
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}
impl std::error::Error for Error {}
impl From<snapshot::Error> for Error {
    fn from(e: snapshot::Error) -> Self {
        Self::Archive(e)
    }
}
impl From<super::budget::Error> for Error {
    fn from(e: super::budget::Error) -> Self {
        Self::Budget(e)
    }
}

struct Prepared {
    name: String,
    node: String,
    service: String,
    state: State,
}
enum State {
    Series(SeriesEvaluator),
    Up(Up),
    Discover(Discover),
    Failed(&'static str),
}
struct Up {
    encoding: Encoding,
    more: bool,
    limit: usize,
    after: Option<StoredRecord>,
    binding: String,
    rows: BTreeMap<StoredRecord, Struct>,
}
struct Discover {
    encoding: Encoding,
    prefix: String,
    rows: Vec<Discovery>,
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Discovery {
    name: String,
    labels: Vec<(String, String)>,
    kind: &'static str,
}

fn ns(t: &prost_types::Timestamp) -> Option<i128> {
    if !(-62_135_596_800..=253_402_300_799).contains(&t.seconds)
        || !(0..1_000_000_000).contains(&t.nanos)
    {
        return None;
    }
    Some(i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos))
}
fn valid_source(node: &str, service: &str) -> bool {
    record::is_node(node) && !service.is_empty() && service.len() <= 128
}
fn error_result(reason: &str) -> pb::QueryResult {
    pb::QueryResult {
        body: Some(pb::query_result::Body::Error(pb::QueryError {
            reason: reason.into(),
            message: String::new(),
        })),
        cursor: String::new(),
    }
}
fn text(s: impl Into<String>) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(s.into())),
    }
}
fn boolean(b: bool) -> Value {
    Value {
        kind: Some(value::Kind::BoolValue(b)),
    }
}
fn object(fields: impl IntoIterator<Item = (String, Value)>) -> Struct {
    Struct {
        fields: fields.into_iter().collect(),
    }
}
// Work buffers have a separate internal cap: they are not bytes in the returned response.
fn reserve(budget: &mut Budget, bytes: usize) -> Result<(), Failure> {
    budget.charge(Charge::ResultItem, 1)?;
    budget.charge(Charge::Workspace, bytes as u64)?;
    Ok(())
}
fn json_len<T: serde::Serialize>(v: &T) -> u64 {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(b.len() as u64);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut n = Count(0);
    serde_json::to_writer(&mut n, v)
        .expect("serializing strings and integer budget counters is infallible");
    n.0
}
fn delimited_len(n: u64) -> u64 {
    1 + prost::encoding::encoded_len_varint(n) as u64 + n
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Gauge => "gauge",
        Kind::Counter => "counter",
        Kind::Unknown => "unknown",
        Kind::Histogram => "histogram",
        Kind::GaugeHistogram => "gaugehistogram",
        Kind::Summary => "summary",
        Kind::StateSet => "stateset",
        Kind::Info => "info",
    }
}

impl Prepared {
    fn new(
        name: &str,
        query: &DecodedQuery,
        range: &pb::Range,
        encoding: Encoding,
    ) -> Result<Self, Error> {
        let mut p = Self {
            name: name.into(),
            node: String::new(),
            service: String::new(),
            state: State::Failed("invalid-query"),
        };
        let q = match query {
            DecodedQuery::Unsupported => {
                p.state = State::Failed("unsupported");
                return Ok(p);
            }
            DecodedQuery::Invalid => return Ok(p),
            DecodedQuery::Known(q) => q,
        };
        let result = match &q.body {
            Some(pb::query::Body::Series(q)) => {
                p.node = q.node.clone();
                p.service = q.service.clone();
                if q.name.len() > 256
                    || !exposition::is_metric_name(&q.name)
                    || q.labels.keys().any(|k| !exposition::is_label_name(k))
                {
                    return Ok(p);
                }
                SeriesEvaluator::new(q.clone(), *range, encoding).map(State::Series)
            }
            Some(pb::query::Body::Up(q)) => {
                p.node = q.node.clone();
                p.service = q.service.clone();
                Up::new(q, range, encoding).map(State::Up)
            }
            Some(pb::query::Body::Discover(q)) => {
                p.node = q.node.clone();
                p.service = q.service.clone();
                if q.prefix.len() > 256
                    || (!q.prefix.is_empty() && !exposition::is_metric_name(&q.prefix))
                {
                    return Ok(p);
                }
                Ok(State::Discover(Discover {
                    encoding,
                    prefix: q.prefix.clone(),
                    rows: Vec::new(),
                }))
            }
            None => return Ok(p),
        };
        if !valid_source(&p.node, &p.service) {
            return Ok(p);
        }
        p.state = match result {
            Ok(s) => s,
            Err(Failure::Query(reason)) => State::Failed(reason),
            Err(Failure::Budget(e)) => return Err(e.into()),
        };
        Ok(p)
    }
    fn active(&self) -> bool {
        !matches!(self.state, State::Failed(_))
    }
    fn observe(
        &mut self,
        record: &StoredRecord,
        raw: &serde_json::Value,
        families: &[Family],
        budget: &mut Budget,
    ) -> Result<(), super::budget::Error> {
        let result = match &mut self.state {
            State::Series(s) => s.observe(record, families, budget),
            State::Up(s) => s.observe(record, raw, budget),
            State::Discover(s) => s.observe(families, budget),
            State::Failed(_) => return Ok(()),
        };
        match result {
            Ok(()) => Ok(()),
            Err(Failure::Query(r)) => {
                self.state = State::Failed(r);
                Ok(())
            }
            Err(Failure::Budget(e)) => Err(e),
        }
    }
    fn finish(self, budget: &mut Budget) -> Result<pb::QueryResult, Error> {
        let result = match self.state {
            State::Failed(reason) => return Ok(error_result(reason)),
            State::Series(s) => s.finish(budget).map(|s| pb::QueryResult {
                body: Some(pb::query_result::Body::Series(s)),
                cursor: String::new(),
            }),
            State::Up(s) => s.finish(budget),
            State::Discover(s) => s.finish(budget),
        };
        match result {
            Ok(r) => Ok(r),
            Err(Failure::Query(r)) => Ok(error_result(r)),
            Err(Failure::Budget(e)) => Err(e.into()),
        }
    }
}
impl Up {
    fn new(q: &pb::UpQuery, range: &pb::Range, encoding: Encoding) -> Result<Self, Failure> {
        if q.limit > 10_000 {
            return Err(Failure::Query("invalid-query"));
        }
        let mut binding_query = q.clone();
        binding_query.after.clear();
        binding_query.limit = 0;
        let mut hash = Sha256::new();
        hash.update(b"masu.up.cursor.v1\0");
        let bytes = range.encode_to_vec();
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        hash.update(binding_query.encode_to_vec());
        let binding = crate::store::hex(&hash.finalize());
        let after = if q.after.is_empty() {
            None
        } else {
            Some(decode_cursor(&q.after, &binding)?)
        };
        Ok(Self {
            encoding,
            more: false,
            limit: if q.limit == 0 { 1000 } else { q.limit as usize },
            after,
            binding,
            rows: BTreeMap::new(),
        })
    }
    fn observe(
        &mut self,
        key: &StoredRecord,
        raw: &serde_json::Value,
        budget: &mut Budget,
    ) -> Result<(), Failure> {
        if self.after.as_ref().is_some_and(|a| key <= a) || self.rows.contains_key(key) {
            return Ok(());
        }
        if self.rows.len() == self.limit {
            self.more = true;
            if self
                .rows
                .last_key_value()
                .is_some_and(|(last, _)| key >= last)
            {
                return Ok(());
            }
            self.rows.pop_last();
        }
        let error = if raw["up"].as_bool() == Some(false) {
            Some(raw["error"].as_str().unwrap())
        } else {
            None
        };
        reserve(budget, error.map_or(0, str::len).saturating_add(512))?;
        let mut row = object([
            ("ts".into(), text(time::format_ns(key.ts as i64))),
            ("up".into(), boolean(raw["up"].as_bool().unwrap())),
        ]);
        if let Some(error) = error {
            row.fields.insert("error".into(), text(error));
        }
        self.rows.insert(key.clone(), row);
        Ok(())
    }
    fn finish(self, budget: &mut Budget) -> Result<pb::QueryResult, Failure> {
        let cursor = if self.more {
            self.rows
                .last_key_value()
                .map(|(key, _)| encode_cursor(key, &self.binding))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let mut rows = Vec::new();
        for row in self.rows.into_values() {
            let bytes = super::document::table_row_encoded_len(&row, self.encoding)
                .map_err(|_| Failure::Query("invalid-response"))?;
            budget.charge(Charge::Response, bytes)?;
            rows.push(row);
        }
        Ok(pb::QueryResult {
            body: Some(pb::query_result::Body::Table(pb::TableFrame { rows })),
            cursor,
        })
    }
}
fn encode_cursor(k: &StoredRecord, binding: &str) -> String {
    format!("1.{binding}.{}.{}.{}.{}", k.ts, k.recv_ts, k.stream, k.seq)
}
fn decode_cursor(cursor: &str, binding: &str) -> Result<StoredRecord, Failure> {
    if cursor.len() > 256 {
        return Err(Failure::Query("invalid-query"));
    }
    let p: Vec<_> = cursor.split('.').collect();
    if p.len() != 6
        || p[0] != "1"
        || p[1] != binding
        || p[4].len() != 32
        || !p[4]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Failure::Query("invalid-query"));
    }
    let k = StoredRecord {
        ts: p[2].parse().map_err(|_| Failure::Query("invalid-query"))?,
        recv_ts: p[3].parse().map_err(|_| Failure::Query("invalid-query"))?,
        stream: p[4].into(),
        seq: p[5].parse().map_err(|_| Failure::Query("invalid-query"))?,
    };
    if encode_cursor(&k, binding) != cursor {
        return Err(Failure::Query("invalid-query"));
    }
    Ok(k)
}
impl Discover {
    fn observe(&mut self, families: &[Family], budget: &mut Budget) -> Result<(), Failure> {
        for family in families {
            for sample in &family.samples {
                budget.check()?;
                if !sample.name.starts_with(&self.prefix) {
                    continue;
                }
                let mut labels: Vec<_> = sample
                    .labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                labels.sort_unstable();
                let kind = kind_name(family.kind);
                let index = match self.rows.binary_search_by(|d| {
                    d.name
                        .as_str()
                        .cmp(&sample.name)
                        .then_with(|| {
                            d.labels
                                .iter()
                                .map(|(k, v)| (k.as_str(), v.as_str()))
                                .cmp(labels.iter().copied())
                        })
                        .then_with(|| d.kind.cmp(kind))
                }) {
                    Ok(_) => continue,
                    Err(i) => i,
                };
                if self.rows.len() >= 10_000 {
                    return Err(Failure::Query("result-limit"));
                }
                reserve(
                    budget,
                    sample.name.len()
                        + sample.labels.len() * 96
                        + kind.len()
                        + labels.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
                )?;
                let d = Discovery {
                    name: sample.name.clone(),
                    labels: labels
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect(),
                    kind,
                };
                self.rows.insert(index, d);
            }
        }
        Ok(())
    }
    fn finish(self, budget: &mut Budget) -> Result<pb::QueryResult, Failure> {
        let mut rows = Vec::new();
        for d in self.rows {
            let row = object([
                ("name".into(), text(d.name)),
                ("type".into(), text(d.kind)),
                (
                    "labels".into(),
                    Value {
                        kind: Some(value::Kind::StructValue(object(
                            d.labels.into_iter().map(|(k, v)| (k, text(v))),
                        ))),
                    },
                ),
            ]);
            let bytes = super::document::table_row_encoded_len(&row, self.encoding)
                .map_err(|_| Failure::Query("invalid-response"))?;
            budget.charge(Charge::Response, bytes)?;
            rows.push(row);
        }
        Ok(pb::QueryResult {
            body: Some(pb::query_result::Body::Table(pb::TableFrame { rows })),
            cursor: String::new(),
        })
    }
}
fn touches(manifest: &Manifest, service: &str, from: i128, to: i128) -> bool {
    manifest.services.contains_key(service)
        && manifest
            .min_ts
            .as_deref()
            .and_then(time::parse_ns)
            .is_some_and(|min| i128::from(min) < to)
        && manifest
            .max_ts
            .as_deref()
            .and_then(time::parse_ns)
            .is_some_and(|max| i128::from(max) >= from)
}

pub fn evaluate(
    root: &Path,
    request: &DecodedRequest,
    encoding: Encoding,
    budget: &mut Budget,
) -> Result<pb::QueryResponse, Error> {
    let from = request
        .range
        .from
        .as_ref()
        .and_then(ns)
        .ok_or(Error::InvalidRequest)?;
    let to = request
        .range
        .to
        .as_ref()
        .and_then(ns)
        .ok_or(Error::InvalidRequest)?;
    if from >= to || request.queries.is_empty() || request.queries.len() > 64 {
        return Err(Error::InvalidRequest);
    }
    if to - from > 31 * 86_400_000_000_000i128 {
        return Err(super::budget::Error::ScanLimit.into());
    }
    if matches!(encoding, Encoding::Json) {
        budget.charge(Charge::Response, 14)?;
    }
    let mut prepared = Vec::new();
    let mut nodes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (name, query) in &request.queries {
        budget.check()?;
        let p = Prepared::new(name, query, &request.range, encoding)?;
        if p.active() {
            nodes
                .entry(p.node.clone())
                .or_default()
                .push(prepared.len());
        }
        prepared.push(p);
    }
    for (node, indices) in nodes {
        let mut snapshot = snapshot::Snapshot::capture(root, &node, budget)?;
        for day in &mut snapshot.days {
            let relevant: Vec<_> = indices
                .iter()
                .copied()
                .filter(|&i| {
                    prepared[i].active()
                        && day
                            .manifest
                            .as_ref()
                            .is_none_or(|m| touches(m, &prepared[i].service, from, to))
                })
                .collect();
            if relevant.is_empty() {
                continue;
            }
            if day.is_missing() {
                for i in relevant {
                    prepared[i].state = State::Failed("archive-incomplete");
                }
                continue;
            }
            day.scan(budget, |raw, budget| {
                let ts = time::parse_ns(raw["ts"].as_str().unwrap()).unwrap();
                if i128::from(ts) < from || i128::from(ts) >= to {
                    return Ok(());
                }
                let matching: Vec<_> = relevant
                    .iter()
                    .copied()
                    .filter(|&i| {
                        prepared[i].active()
                            && prepared[i].service == raw["service"].as_str().unwrap()
                    })
                    .collect();
                if matching.is_empty() {
                    return Ok(());
                }
                let families = if raw["up"].as_bool().unwrap() {
                    let format = exposition::Format::from_content_type(
                        raw["content_type"].as_str().unwrap(),
                    );
                    match exposition::parse(raw["body"].as_str().unwrap(), format) {
                        Ok(families) => {
                            budget.charge(
                                Charge::Sample,
                                families.iter().map(|f| f.samples.len() as u64).sum(),
                            )?;
                            families
                        }
                        Err(_) => {
                            for i in matching {
                                prepared[i].state = State::Failed("invalid-exposition");
                            }
                            return Ok(());
                        }
                    }
                } else {
                    Vec::new()
                };
                let record = StoredRecord {
                    ts: i128::from(ts),
                    recv_ts: i128::from(time::parse_ns(raw["recv_ts"].as_str().unwrap()).unwrap()),
                    stream: raw["stream"].as_str().unwrap().into(),
                    seq: raw["seq"].as_u64().unwrap(),
                };
                for i in matching {
                    prepared[i].observe(&record, raw, &families, budget)?;
                }
                Ok(())
            })?;
        }
    }
    let mut results = BTreeMap::new();
    for p in prepared {
        budget.check()?;
        let name = p.name.clone();
        let before = budget.usage.response_bytes;
        let result = p.finish(budget)?;
        let children = budget.usage.response_bytes - before;
        let body = super::document::result_encoded_len(&result, encoding)
            .map_err(|_| Error::InvalidRequest)?;
        let entry = match encoding {
            Encoding::Json => json_len(&name) + 1 + body + u64::from(!results.is_empty()),
            Encoding::Protobuf => {
                delimited_len(delimited_len(name.len() as u64) + delimited_len(body))
            }
        };
        budget.charge(Charge::Response, entry.saturating_sub(children))?;
        results.insert(name, result);
    }
    let stats = pb::Budget {
        records_read: budget.usage.records,
        bytes_read: budget
            .usage
            .compressed_bytes
            .saturating_add(budget.usage.metadata_bytes),
        elapsed_ms: budget.elapsed_ms(),
    };
    let overhead = match encoding {
        Encoding::Protobuf => delimited_len(stats.encoded_len() as u64),
        Encoding::Json => {
            10 + json_len(
                &serde_json::json!({"recordsRead":stats.records_read.to_string(),"bytesRead":stats.bytes_read.to_string(),"elapsedMs":stats.elapsed_ms.to_string()}),
            )
        }
    };
    budget.charge(Charge::Response, overhead)?;
    let response = pb::QueryResponse {
        results,
        budget: Some(stats),
    };
    let actual = super::document::response_encoded_len(&response, encoding)
        .map_err(|_| Error::InvalidRequest)?;
    budget.charge(
        Charge::Response,
        actual.saturating_sub(budget.usage.response_bytes),
    )?;
    if actual > super::document::MAX_RESPONSE_BYTES as u64 {
        return Err(super::budget::Error::ResponseLimit.into());
    }
    budget.check()?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::super::{
        budget::Limits,
        document::{self, Encoding},
    };
    use super::*;
    use crate::{
        seal::Scan,
        store::{FORMAT, State as StoreState},
    };
    use serde_json::{Value as Json, json};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    const START: i64 = 1_793_491_200_000_000_000;
    struct Fixture {
        root: PathBuf,
        state: StoreState,
    }
    impl Fixture {
        fn new() -> Self {
            static ID: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "masu-evaluate-{}-{}",
                std::process::id(),
                ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).unwrap();
            fs::write(root.join("FORMAT"), FORMAT).unwrap();
            let mut f = Self {
                root,
                state: StoreState {
                    v: 1,
                    ..StoreState::default()
                },
            };
            f.persist();
            f
        }
        fn persist(&mut self) {
            fs::write(
                self.root.join("state.json"),
                serde_json::to_vec(&self.state).unwrap(),
            )
            .unwrap();
        }
        fn file(&mut self, node: &str, day: &str, records: &[Json], sealed: bool) -> PathBuf {
            let dir = self.root.join("archive").join(node);
            fs::create_dir_all(&dir).unwrap();
            let mut bytes = Vec::new();
            let mut scan = Scan::default();
            for r in records {
                let line = format!("{r}\n");
                scan.line(line.as_bytes()).unwrap();
                bytes.extend_from_slice(line.as_bytes());
            }
            let bytes = zstd::encode_all(bytes.as_slice(), 3).unwrap();
            let data = dir.join(format!("{day}.ndjson.zst"));
            fs::write(&data, &bytes).unwrap();
            let key = format!("{node}/{day}");
            if sealed {
                let m = scan.manifest(node, day, "2026-11-02T00:10:00.000000000Z".into());
                fs::write(
                    dir.join(format!("{day}.manifest.json")),
                    serde_json::to_vec(&m).unwrap(),
                )
                .unwrap();
                self.state.files.remove(&key);
            } else {
                self.state.files.insert(key, bytes.len() as u64);
            }
            self.persist();
            data
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    fn record(node: &str, sec: i64, seq: u64, body: Result<&str, &str>) -> Json {
        let mut r = json!({"v":1,"node":node,"service":"azukari","ts":time::format_ns(START+sec*1_000_000_000),"recv_ts":"2026-11-01T10:00:00.000000000Z","stream":"11111111111111111111111111111111","seq":seq,"up":body.is_ok(),"dur_us":1});
        match body {
            Ok(b) => {
                r["body"] = b.into();
                r["content_type"] = "text/plain".into();
            }
            Err(e) => r["error"] = e.into(),
        };
        r
    }
    fn request(queries: Json) -> DecodedRequest {
        let bytes=serde_json::to_vec(&json!({"range":{"from":"2026-11-01T00:00:00Z","to":"2026-11-02T00:00:00Z"},"queries":queries})).unwrap();
        document::decode_request(&bytes, Encoding::Json).unwrap()
    }
    fn up(node: &str, limit: u32, after: &str) -> Json {
        json!({"up":{"node":node,"service":"azukari","limit":limit,"after":after}})
    }
    fn discover(node: &str) -> Json {
        json!({"discover":{"node":node,"service":"azukari"}})
    }
    fn series(node: &str) -> Json {
        json!({"series":{"node":node,"service":"azukari","name":"azukari_value"}})
    }
    fn rows(r: &pb::QueryResult) -> &[Struct] {
        let Some(pb::query_result::Body::Table(t)) = &r.body else {
            panic!("not table: {r:?}")
        };
        &t.rows
    }
    fn reason(r: &pb::QueryResult) -> &str {
        let Some(pb::query_result::Body::Error(e)) = &r.body else {
            panic!("not error: {r:?}")
        };
        &e.reason
    }
    fn string(v: &Value) -> &str {
        let Some(value::Kind::StringValue(s)) = &v.kind else {
            panic!("not string")
        };
        s
    }
    #[test]
    fn query_select_batch_traverses_each_node_and_parses_each_record_once() {
        let mut f = Fixture::new();
        let a = f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Ok("azukari_value 1\n"))],
            false,
        );
        let b = f.file(
            "node-b",
            "2026-11-01",
            &[record("node-b", 2, 1, Ok("azukari_value 2\n"))],
            false,
        );
        let req = request(
            json!({"up1":up("node-a",0,""),"up2":up("node-a",0,""),"discover":discover("node-a"),"s1":series("node-a"),"s2":series("node-a"),"other":up("node-b",0,"")}),
        );
        let mut budget = Budget::default();
        let result = evaluate(&f.root, &req, Encoding::Json, &mut budget).unwrap();
        assert_eq!(result.results["up1"], result.results["up2"]);
        assert_eq!(result.results["s1"], result.results["s2"]);
        assert_eq!(budget.usage.records, 2);
        assert_eq!(budget.usage.samples, 2);
        assert_eq!(
            budget.usage.compressed_bytes,
            fs::metadata(a).unwrap().len() + fs::metadata(b).unwrap().len()
        );
        assert_eq!(rows(&result.results["other"]).len(), 1);
    }
    #[test]
    fn query_select_up_sorts_unordered_arrivals_and_uses_half_open_bounds() {
        let mut f = Fixture::new();
        f.file(
            "node-a",
            "2026-11-01",
            &[
                record("node-a", 2, 1, Ok("m 1\n")),
                record("node-a", 1, 2, Ok("m 2\n")),
                record("node-a", 3, 3, Ok("m 3\n")),
            ],
            false,
        );
        let mut req = request(json!({"up":up("node-a",0,"")}));
        req.range.from = Some(prost_types::Timestamp {
            seconds: START / 1_000_000_000 + 1,
            nanos: 0,
        });
        req.range.to = Some(prost_types::Timestamp {
            seconds: START / 1_000_000_000 + 3,
            nanos: 0,
        });
        let mut budget = Budget::default();
        let r = evaluate(&f.root, &req, Encoding::Json, &mut budget).unwrap();
        let data = rows(&r.results["up"]);
        assert_eq!(data.len(), 2);
        assert_eq!(
            string(&data[0].fields["ts"]),
            time::format_ns(START + 1_000_000_000)
        );
        assert_eq!(
            string(&data[1].fields["ts"]),
            time::format_ns(START + 2_000_000_000)
        );
        assert_eq!(budget.usage.records, 3);
        assert_eq!(budget.usage.samples, 2);
    }
    #[test]
    fn query_select_unknown_invalid_and_bad_exposition_are_isolated() {
        let mut f = Fixture::new();
        f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Ok("bad metric syntax\n"))],
            false,
        );
        f.file(
            "node-b",
            "2026-11-01",
            &[record("node-b", 1, 1, Ok("m 1\n"))],
            false,
        );
        let req = request(
            json!({"up":up("node-a",0,""),"series":series("node-a"),"discover":discover("node-a"),"good":up("node-b",0,""),"invalid":up("../node-a",0,""),"future":{"future":{}},"badlimit":up("node-b",10001,"")}),
        );
        let r = evaluate(&f.root, &req, Encoding::Json, &mut Budget::default()).unwrap();
        for name in ["up", "series", "discover"] {
            assert_eq!(reason(&r.results[name]), "invalid-exposition");
        }
        for name in ["invalid", "badlimit"] {
            assert_eq!(reason(&r.results[name]), "invalid-query");
        }
        assert_eq!(reason(&r.results["future"]), "unsupported");
        assert_eq!(rows(&r.results["good"]).len(), 1);
    }
    #[test]
    fn query_select_up_preserves_failed_scrapes_without_inventing_samples() {
        let mut f = Fixture::new();
        f.file(
            "node-a",
            "2026-11-01",
            &[
                record("node-a", 1, 1, Err("timeout")),
                record("node-a", 2, 2, Ok("other_value 1\n")),
            ],
            false,
        );
        let req = request(
            json!({"up":up("node-a",0,""),"series":series("node-a"),"discover":discover("node-a")}),
        );
        let mut b = Budget::default();
        let r = evaluate(&f.root, &req, Encoding::Json, &mut b).unwrap();
        assert_eq!(rows(&r.results["up"]).len(), 2);
        assert_eq!(
            string(&rows(&r.results["up"])[0].fields["error"]),
            "timeout"
        );
        assert_eq!(b.usage.samples, 1);
        assert_eq!(rows(&r.results["discover"]).len(), 1);
        let Some(pb::query_result::Body::Series(s)) = &r.results["series"].body else {
            panic!()
        };
        assert!(s.series.is_empty());
    }
    #[test]
    fn query_select_up_cursor_is_filter_bound_and_late_data_requires_rereading() {
        let mut f = Fixture::new();
        let original = [
            record("node-a", 3, 1, Ok("m 1\n")),
            record("node-a", 2, 2, Ok("m 2\n")),
        ];
        f.file("node-a", "2026-11-01", &original, false);
        let req = request(json!({"up":up("node-a",1,"")}));
        let r = evaluate(&f.root, &req, Encoding::Json, &mut Budget::default()).unwrap();
        let cursor = &r.results["up"].cursor;
        assert!(!cursor.is_empty());
        let mut added = original.to_vec();
        added.push(record("node-a", 1, 3, Ok("m 3\n")));
        f.file("node-a", "2026-11-01", &added, false);
        let next = request(json!({"up":up("node-a",1,cursor)}));
        let r = evaluate(&f.root, &next, Encoding::Json, &mut Budget::default()).unwrap();
        assert_eq!(rows(&r.results["up"]).len(), 1);
        assert_eq!(
            string(&rows(&r.results["up"])[0].fields["ts"]),
            time::format_ns(START + 3_000_000_000)
        );
        let reread = evaluate(&f.root, &req, Encoding::Json, &mut Budget::default()).unwrap();
        assert_eq!(
            string(&rows(&reread.results["up"])[0].fields["ts"]),
            time::format_ns(START + 1_000_000_000)
        );
        let mut changed = next;
        changed.range.to.as_mut().unwrap().seconds -= 1;
        assert_eq!(
            reason(
                &evaluate(&f.root, &changed, Encoding::Json, &mut Budget::default())
                    .unwrap()
                    .results["up"]
            ),
            "invalid-query"
        );
        let wrong = request(json!({"up":up("node-b",1,cursor),"bad":up("node-a",1,"garbage")}));
        let r = evaluate(&f.root, &wrong, Encoding::Json, &mut Budget::default()).unwrap();
        assert_eq!(reason(&r.results["up"]), "invalid-query");
        assert_eq!(reason(&r.results["bad"]), "invalid-query");
    }
    #[test]
    fn query_select_discovery_deduplicates_and_orders_name_labels_then_type() {
        let mut f = Fixture::new();
        let counter = "# TYPE m_total counter\nm_total{b=\"x\",a=\"y\"} 1\n";
        let gauge = "# TYPE m_total gauge\nm_total{a=\"y\",b=\"x\"} 2\n";
        f.file(
            "node-a",
            "2026-11-01",
            &[
                record("node-a", 1, 1, Ok(gauge)),
                record("node-a", 2, 2, Ok(counter)),
                record("node-a", 3, 3, Ok(counter)),
            ],
            false,
        );
        let r = evaluate(
            &f.root,
            &request(json!({"d":discover("node-a")})),
            Encoding::Json,
            &mut Budget::default(),
        )
        .unwrap();
        let rs = rows(&r.results["d"]);
        assert_eq!(rs.len(), 2);
        assert_eq!(string(&rs[0].fields["type"]), "counter");
        assert_eq!(string(&rs[1].fields["type"]), "gauge");
    }
    #[test]
    fn query_select_missing_retained_days_only_affect_relevant_queries() {
        let mut f = Fixture::new();
        let mut gone = record("node-a", 1, 1, Ok("m 1\n"));
        gone["recv_ts"] = "2026-10-30T12:00:00.000000000Z".into();
        let data = f.file("node-a", "2026-10-30", &[gone], true);
        fs::remove_file(data).unwrap();
        let mut other = record("node-a", 2, 2, Ok("m 2\n"));
        other["service"] = "annai".into();
        f.file("node-a", "2026-11-01", &[other], false);
        let r=evaluate(&f.root,&request(json!({"missing":up("node-a",0,""),"good":{"up":{"node":"node-a","service":"annai"}},"absent":up("node-b",0,"")})),Encoding::Json,&mut Budget::default()).unwrap();
        assert_eq!(reason(&r.results["missing"]), "archive-incomplete");
        assert_eq!(rows(&r.results["good"]).len(), 1);
        assert!(rows(&r.results["absent"]).is_empty());
    }
    #[test]
    fn query_select_retained_missing_day_outside_range_is_pruned_without_reading() {
        let mut f = Fixture::new();
        let mut old = record("node-a", -172800, 1, Ok("m 1\n"));
        old["recv_ts"] = "2026-10-30T12:00:00.000000000Z".into();
        let data = f.file("node-a", "2026-10-30", &[old], true);
        fs::remove_file(data).unwrap();
        let live = f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 2, Ok("m 1\n"))],
            false,
        );
        let mut b = Budget::default();
        let r = evaluate(
            &f.root,
            &request(json!({"up":up("node-a",0,"")})),
            Encoding::Json,
            &mut b,
        )
        .unwrap();
        assert_eq!(rows(&r.results["up"]).len(), 1);
        assert_eq!(b.usage.records, 1);
        assert_eq!(b.usage.compressed_bytes, fs::metadata(live).unwrap().len());
    }
    #[test]
    fn query_select_corrupt_manifest_discards_every_accumulated_result() {
        let mut f = Fixture::new();
        f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Ok("m 1\n"))],
            true,
        );
        let path = f.root.join("archive/node-a/2026-11-01.manifest.json");
        let mut m: Json = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        m["content_sha256"] = "0".repeat(64).into();
        fs::write(path, serde_json::to_vec(&m).unwrap()).unwrap();
        let mut b = Budget::default();
        let error = evaluate(
            &f.root,
            &request(json!({"up":up("node-a",0,"")})),
            Encoding::Json,
            &mut b,
        )
        .unwrap_err();
        assert_eq!(error.reason(), "archive-corrupt");
        assert!(b.usage.result_items > 0);
    }
    #[test]
    fn query_select_request_budgets_stop_before_oversized_row_allocation() {
        let mut f = Fixture::new();
        let long = "x".repeat(1 << 20);
        f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Err(&long))],
            false,
        );
        let req = request(json!({"up":up("node-a",0,"")}));
        let mut b = Budget::new(Limits {
            response_bytes: 1024,
            ..Limits::default()
        });
        assert_eq!(
            evaluate(&f.root, &req, Encoding::Json, &mut b)
                .unwrap_err()
                .reason(),
            "response-limit"
        );
        assert_eq!(b.usage.records, 1);
        let mut b = Budget::new(Limits {
            result_items: 0,
            ..Limits::default()
        });
        assert_eq!(
            evaluate(&f.root, &req, Encoding::Json, &mut b)
                .unwrap_err()
                .reason(),
            "response-limit"
        );
        let mut b = Budget::new(Limits {
            records: 0,
            ..Limits::default()
        });
        assert_eq!(
            evaluate(&f.root, &req, Encoding::Json, &mut b)
                .unwrap_err()
                .reason(),
            "scan-limit"
        );
    }
    #[test]
    fn query_select_large_plain_row_uses_actual_response_size_not_escape_worst_case() {
        let mut f = Fixture::new();
        let error = "x".repeat(1 << 20);
        f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Err(&error))],
            false,
        );
        let req = request(json!({"up":up("node-a",0,"")}));
        for encoding in [Encoding::Json, Encoding::Protobuf] {
            let mut b = Budget::new(Limits {
                response_bytes: 2 << 20,
                ..Limits::default()
            });
            let r = evaluate(&f.root, &req, encoding, &mut b).unwrap();
            assert_eq!(
                string(&rows(&r.results["up"])[0].fields["error"]).len(),
                error.len()
            );
            assert!(b.usage.response_bytes < 2 << 20);
            assert!(b.usage.response_bytes > 1 << 20);
        }
        let mut b = Budget::new(Limits {
            workspace_bytes: 0,
            ..Limits::default()
        });
        assert_eq!(
            evaluate(&f.root, &req, Encoding::Json, &mut b)
                .unwrap_err()
                .reason(),
            "scan-limit"
        );
    }
    #[test]
    fn query_select_response_counter_matches_the_actual_encoding() {
        let mut f = Fixture::new();
        f.file(
            "node-a",
            "2026-11-01",
            &[
                record("node-a", 1, 1, Err("quoted\"\nerror")),
                record("node-a", 2, 2, Ok("m{label=\"escaped\\\"value\"} 3\n")),
            ],
            false,
        );
        let req = request(
            json!({"up":up("node-a",1,""),"discovery":discover("node-a"),"missing":up("node-b",0,""),"series":{"series":{"node":"node-a","service":"azukari","name":"m"}},"escaped\"\nname":{"future":{}}}),
        );
        for encoding in [Encoding::Json, Encoding::Protobuf] {
            let mut b = Budget::default();
            let r = evaluate(&f.root, &req, encoding, &mut b).unwrap();
            let actual = document::encode_response(&r, encoding).unwrap();
            assert_eq!(b.usage.response_bytes, actual.len() as u64, "{encoding:?}");
            assert!(b.usage.workspace_bytes > 0);
        }
    }
    #[test]
    fn query_select_discovery_limit_is_per_query() {
        let mut f = Fixture::new();
        let body = (0..10001).map(|i| format!("m_{i} 1\n")).collect::<String>();
        f.file(
            "node-a",
            "2026-11-01",
            &[record("node-a", 1, 1, Ok(&body))],
            false,
        );
        let r = evaluate(
            &f.root,
            &request(json!({"d":discover("node-a"),"up":up("node-a",0,"")})),
            Encoding::Json,
            &mut Budget::default(),
        )
        .unwrap();
        assert_eq!(reason(&r.results["d"]), "result-limit");
        assert_eq!(rows(&r.results["up"]).len(), 1);
    }
}
