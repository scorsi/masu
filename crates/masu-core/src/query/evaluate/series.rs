//! Per-series selection and math over one shared parse of each scrape.
use super::{Failure, StoredRecord};
use crate::{
    exposition::{Family, Kind, Labels},
    pb,
    query::{
        budget::{Budget, Charge},
        document::{self, Encoding},
    },
};
use std::collections::BTreeMap;

const MAX_SERIES: usize = 1_000;
const MAX_POINTS: usize = 11_000;
const NS: i128 = 1_000_000_000;
type Result<T> = std::result::Result<T, Failure>;

pub(super) struct SeriesEvaluator {
    query: pb::SeriesQuery,
    from: i128,
    to: i128,
    start: prost_types::Timestamp,
    step: Option<i128>,
    reduce: pb::Reduce,
    raw: bool,
    encoding: Encoding,
    series: Vec<SeriesData>,
}
struct SeriesData {
    labels: Labels,
    points: BTreeMap<i128, Point>,
}
struct Point {
    received: i128,
    stream: String,
    seq: u64,
    value: f64,
}

impl SeriesEvaluator {
    pub(super) fn new(
        query: pb::SeriesQuery,
        range: pb::Range,
        encoding: Encoding,
    ) -> Result<Self> {
        let from = range.from.ok_or(Failure::Query("invalid-query"))?;
        let to = range.to.ok_or(Failure::Query("invalid-query"))?;
        let from_ns = i128::from(from.seconds) * NS + i128::from(from.nanos);
        let to_ns = i128::from(to.seconds) * NS + i128::from(to.nanos);
        if from_ns >= to_ns {
            return Err(Failure::Query("invalid-query"));
        }
        let reduce =
            pb::Reduce::try_from(query.reduce).map_err(|_| Failure::Query("unsupported"))?;
        let step = match query.step {
            None => None,
            Some(d) => {
                if d.seconds < 0 || !(0..1_000_000_000).contains(&d.nanos) {
                    return Err(Failure::Query("invalid-query"));
                }
                let step = i128::from(d.seconds) * NS + i128::from(d.nanos);
                if step <= 0 {
                    return Err(Failure::Query("invalid-query"));
                }
                if ((to_ns - from_ns - 1) / step + 1) > MAX_POINTS as i128 {
                    return Err(Failure::Query("result-limit"));
                }
                Some(step)
            }
        };
        if query.rate && (step.is_none() || reduce != pb::Reduce::Unspecified) {
            return Err(Failure::Query("invalid-query"));
        }
        Ok(Self {
            raw: step.is_none() && reduce == pb::Reduce::Unspecified,
            query,
            from: from_ns,
            to: to_ns,
            start: from,
            step,
            reduce,
            encoding,
            series: Vec::new(),
        })
    }

    pub(super) fn observe(
        &mut self,
        record: &StoredRecord,
        families: &[Family],
        budget: &mut Budget,
    ) -> Result<()> {
        if record.ts < self.from || record.ts >= self.to {
            return Ok(());
        }
        for family in families {
            budget.check()?;
            for sample in &family.samples {
                budget.check()?;
                if sample.name != self.query.name {
                    continue;
                }
                if !matches!(family.kind, Kind::Gauge | Kind::Counter | Kind::Unknown) {
                    return Err(Failure::Query("unsupported"));
                }
                let counting = family.kind == Kind::Counter
                    && (sample.name == family.name
                        || sample.name.strip_suffix("_total") == Some(family.name.as_str()));
                if self.query.rate && !counting {
                    return Err(Failure::Query("invalid-query"));
                }
                if self
                    .query
                    .labels
                    .iter()
                    .any(|(key, value)| !sample.labels.iter().any(|(k, v)| k == key && v == value))
                {
                    continue;
                }
                // The scratch key borrows strings; persistent keys are charged before cloning.
                let scratch =
                    (sample.labels.len() as u64)
                        .saturating_mul(std::mem::size_of::<(&str, &str)>() as u64);
                if scratch > budget.remaining(Charge::Workspace) {
                    budget.charge(Charge::Workspace, scratch)?;
                }
                let mut labels: Vec<_> = sample
                    .labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                labels.sort_unstable();
                let found = self.series.binary_search_by(|s| {
                    s.labels
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .cmp(labels.iter().copied())
                });
                let index = match found {
                    Ok(index) => index,
                    Err(index) => {
                        if self.series.len() >= MAX_SERIES {
                            return Err(Failure::Query("result-limit"));
                        }
                        let bytes = labels.iter().fold(256u64, |sum, (k, v)| {
                            sum.saturating_add((k.len() + v.len()) as u64)
                                .saturating_add(128)
                        });
                        budget.charge(Charge::Workspace, bytes)?;
                        let labels = labels
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        self.series.insert(
                            index,
                            SeriesData {
                                labels,
                                points: BTreeMap::new(),
                            },
                        );
                        index
                    }
                };
                let data = &mut self.series[index];
                if let Some(point) = data.points.get_mut(&record.ts) {
                    if (record.recv_ts, record.stream.as_str(), record.seq)
                        > (point.received, point.stream.as_str(), point.seq)
                    {
                        point.received = record.recv_ts;
                        point.stream.clone_from(&record.stream);
                        point.seq = record.seq;
                        point.value = sample.value;
                    }
                } else {
                    if self.raw && data.points.len() >= MAX_POINTS {
                        return Err(Failure::Query("result-limit"));
                    }
                    budget.charge(Charge::ResultItem, 1)?;
                    budget.charge(
                        Charge::Workspace,
                        (record.stream.len() as u64).saturating_add(128),
                    )?;
                    data.points.insert(
                        record.ts,
                        Point {
                            received: record.recv_ts,
                            stream: record.stream.clone(),
                            seq: record.seq,
                            value: sample.value,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self, budget: &mut Budget) -> Result<pb::SeriesFrame> {
        let mut series = Vec::new();
        for data in self.series {
            budget.check()?;
            let (ts, values, start, step) = if let Some(step) = self.step {
                let count = usize::try_from((self.to - self.from - 1) / step + 1)
                    .map_err(|_| Failure::Query("result-limit"))?;
                reserve_values(budget, count, 8)?;
                let mut values = vec![f64::NAN; count];
                if self.query.rate {
                    let mut previous: Option<f64> = None;
                    let mut bucket = None;
                    let mut sum = Sum::default();
                    for (ts, point) in data.points {
                        budget.check()?;
                        if let Some(prev) = previous {
                            let index = ((ts - self.from) / step) as usize;
                            if bucket != Some(index) {
                                if let Some(index) = bucket {
                                    values[index] = sum.divided_by(bucket_seconds(
                                        self.from, self.to, step, index,
                                    ));
                                }
                                bucket = Some(index);
                                sum = Sum::default();
                            }
                            if point.value < prev {
                                sum.add(point.value);
                            } else {
                                let delta = point.value - prev;
                                if delta.is_infinite()
                                    && point.value.is_finite()
                                    && prev.is_finite()
                                {
                                    sum.add(point.value);
                                    sum.add(-prev);
                                } else {
                                    sum.add(delta);
                                }
                            }
                        }
                        previous = Some(point.value);
                    }
                    if let Some(index) = bucket {
                        values[index] =
                            sum.divided_by(bucket_seconds(self.from, self.to, step, index));
                    }
                } else {
                    let mut bucket = None;
                    let mut aggregate = Aggregate::default();
                    let reduce = if self.reduce == pb::Reduce::Unspecified {
                        pb::Reduce::Last
                    } else {
                        self.reduce
                    };
                    for (ts, point) in data.points {
                        budget.check()?;
                        let index = ((ts - self.from) / step) as usize;
                        if bucket != Some(index) {
                            if let Some(index) = bucket {
                                values[index] = aggregate.value(reduce);
                            }
                            bucket = Some(index);
                            aggregate = Aggregate::default();
                        }
                        aggregate.add(point.value);
                    }
                    if let Some(index) = bucket {
                        values[index] = aggregate.value(reduce);
                    }
                }
                (Vec::new(), values, Some(self.start), self.query.step)
            } else if self.raw {
                reserve_values(budget, data.points.len(), 24)?;
                let mut ts = Vec::with_capacity(data.points.len());
                let mut values = Vec::with_capacity(data.points.len());
                for (time, point) in data.points {
                    budget.check()?;
                    ts.push(timestamp(time));
                    values.push(point.value);
                }
                (ts, values, None, None)
            } else {
                reserve_values(budget, 1, 24)?;
                let (ts, value) = if self.reduce == pb::Reduce::Last {
                    let (ts, point) = data.points.last_key_value().unwrap();
                    (*ts, point.value)
                } else {
                    let mut aggregate = Aggregate::default();
                    for point in data.points.values() {
                        budget.check()?;
                        aggregate.add(point.value);
                    }
                    (self.from, aggregate.value(self.reduce))
                };
                (vec![timestamp(ts)], vec![value], None, None)
            };
            let result = pb::Series {
                labels: data.labels.into_iter().collect(),
                ts,
                values,
                start,
                step,
            };
            let bytes = document::series_encoded_len(&result, self.encoding)
                .map_err(|_| Failure::Budget(crate::query::budget::Error::ResponseLimit))?;
            budget.charge(Charge::Response, bytes)?;
            series.push(result);
        }
        Ok(pb::SeriesFrame { series })
    }
}

fn reserve_values(budget: &mut Budget, count: usize, bytes: u64) -> Result<()> {
    budget.charge(Charge::ResultItem, count as u64)?;
    budget.charge(Charge::Workspace, (count as u64).saturating_mul(bytes))?;
    Ok(())
}
fn timestamp(ns: i128) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: (ns.div_euclid(NS)) as i64,
        nanos: ns.rem_euclid(NS) as i32,
    }
}
fn bucket_seconds(from: i128, to: i128, step: i128, index: usize) -> f64 {
    ((from + step * (index as i128 + 1)).min(to) - (from + step * index as i128)) as f64 / NS as f64
}

/// Scale before summing, and divide before restoring the scale: a finite average or rate
/// must not overflow just because its un-divided sum would.
#[derive(Default)]
struct Sum {
    scale: f64,
    scaled: f64,
    positive_inf: bool,
    negative_inf: bool,
    nan: bool,
}
impl Sum {
    fn add(&mut self, value: f64) {
        if value.is_nan() {
            self.nan = true;
        } else if value == f64::INFINITY {
            self.positive_inf = true;
        } else if value == f64::NEG_INFINITY {
            self.negative_inf = true;
        } else if value != 0.0 {
            let scale = value.abs();
            if scale > self.scale {
                self.scaled *= self.scale / scale;
                self.scale = scale;
            }
            self.scaled += value / self.scale;
        }
    }
    fn divided_by(&self, divisor: f64) -> f64 {
        if self.nan || self.positive_inf && self.negative_inf {
            f64::NAN
        } else if self.positive_inf {
            f64::INFINITY
        } else if self.negative_inf {
            f64::NEG_INFINITY
        } else {
            let sum = self.scaled * self.scale;
            if sum.is_finite() && (sum != 0.0 || self.scaled == 0.0) {
                sum / divisor
            } else {
                (self.scaled / divisor) * self.scale
            }
        }
    }
}
#[derive(Default)]
struct Aggregate {
    count: u64,
    last: f64,
    min: Option<f64>,
    max: Option<f64>,
    sum: Sum,
}
impl Aggregate {
    fn add(&mut self, value: f64) {
        self.last = value;
        if !value.is_nan() {
            self.count += 1;
            self.sum.add(value);
            self.min = Some(self.min.map_or(value, |v| v.min(value)));
            self.max = Some(self.max.map_or(value, |v| v.max(value)));
        }
    }
    fn value(&self, reduce: pb::Reduce) -> f64 {
        match reduce {
            pb::Reduce::Last => self.last,
            pb::Reduce::Min => self.min.unwrap_or(f64::NAN),
            pb::Reduce::Max => self.max.unwrap_or(f64::NAN),
            pb::Reduce::Avg => {
                if self.count == 0 {
                    f64::NAN
                } else {
                    self.sum.divided_by(self.count as f64)
                }
            }
            pb::Reduce::Count => self.count as f64,
            pb::Reduce::Unspecified => unreachable!("raw points do not use a reducer"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exposition::{self, Format},
        query::budget::{Error as BudgetError, Limits},
    };
    fn range(to: i64) -> pb::Range {
        pb::Range {
            from: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            to: Some(prost_types::Timestamp {
                seconds: to,
                nanos: 0,
            }),
        }
    }
    fn query(reduce: pb::Reduce, step: Option<i64>, rate: bool) -> pb::SeriesQuery {
        pb::SeriesQuery {
            node: "n".into(),
            service: "svc".into(),
            name: "metric".into(),
            reduce: reduce as i32,
            step: step.map(|seconds| prost_types::Duration { seconds, nanos: 0 }),
            rate,
            ..Default::default()
        }
    }
    fn families(kind: &str, samples: &str) -> Vec<Family> {
        exposition::parse(
            &format!("# TYPE metric {kind}\n{samples}"),
            Format::Prometheus,
        )
        .unwrap()
    }
    fn record(ts: i64, seq: u64) -> StoredRecord {
        StoredRecord {
            ts: i128::from(ts) * NS,
            recv_ts: i128::from(seq) * NS,
            seq,
            stream: "a".repeat(32),
        }
    }
    fn add(e: &mut SeriesEvaluator, ts: i64, seq: u64, kind: &str, value: &str, b: &mut Budget) {
        e.observe(
            &record(ts, seq),
            &families(kind, &format!("metric {value}\n")),
            b,
        )
        .unwrap();
    }
    fn failure(result: Result<impl Sized>, reason: &str) {
        assert!(matches!(result,Err(Failure::Query(r))if r==reason));
    }
    fn close(actual: f64, want: f64) {
        assert!(
            (actual - want).abs() <= want.abs().max(1.0) * 1e-12,
            "{actual} != {want}"
        );
    }

    #[test]
    fn query_select_raw_orders_scrape_times_and_uses_latest_tie_verbatim_labels() {
        let mut q = query(pb::Reduce::Unspecified, None, false);
        q.labels.insert("peer".into(), "a".into());
        let mut e = SeriesEvaluator::new(q, range(60), Encoding::Json).unwrap();
        let mut b = Budget::default();
        for (ts, seq, value) in [(40, 1, "4"), (20, 2, "2"), (40, 3, "5")] {
            e.observe(
                &record(ts, seq),
                &families(
                    "gauge",
                    &format!(
                        "metric{{peer=\"a\",node=\"source\"}} {value}\nmetric{{peer=\"b\"}} 99\n"
                    ),
                ),
                &mut b,
            )
            .unwrap();
        }
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series.len(), 1);
        assert_eq!(f.series[0].values, [2., 5.]);
        assert_eq!(
            f.series[0].ts.iter().map(|t| t.seconds).collect::<Vec<_>>(),
            [20, 40]
        );
        assert_eq!(f.series[0].labels.len(), 2);
        assert_eq!(f.series[0].labels["node"], "source");
        assert!(!f.series[0].labels.contains_key("service"));
    }
    #[test]
    fn query_select_last_keeps_sample_time_and_ties_use_receive_stream_then_seq() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Last, None, false),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for (recv, stream, seq, value) in [
            (10, "a", 1, "1"),
            (10, "b", 1, "2"),
            (10, "b", 2, "3"),
            (9, "z", 9, "99"),
        ] {
            let r = StoredRecord {
                ts: 20 * NS,
                recv_ts: recv * NS,
                stream: stream.repeat(32),
                seq,
            };
            e.observe(&r, &families("gauge", &format!("metric {value}\n")), &mut b)
                .unwrap();
        }
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series[0].ts[0].seconds, 20);
        assert_eq!(f.series[0].values, [3.]);
    }
    #[test]
    fn query_select_buckets_are_half_open_and_clip_the_last_interval() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(10), false),
            range(25),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for (ts, value) in [(0, "1"), (9, "2"), (10, "3"), (24, "4"), (25, "99")] {
            add(&mut e, ts, ts as u64 + 1, "gauge", value, &mut b);
        }
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series[0].values, [2., 3., 4.]);
        assert!(f.series[0].ts.is_empty());
        assert_eq!(f.series[0].start.unwrap().seconds, 0);
        assert_eq!(f.series[0].step.unwrap().seconds, 10);
    }
    #[test]
    fn query_select_reducers_define_nan_empty_buckets_and_infinities() {
        for (reduce, want) in [
            (pb::Reduce::Min, 1.),
            (pb::Reduce::Max, 3.),
            (pb::Reduce::Avg, 2.),
            (pb::Reduce::Count, 2.),
            (pb::Reduce::Last, f64::NAN),
        ] {
            let mut e =
                SeriesEvaluator::new(query(reduce, Some(10), false), range(20), Encoding::Json)
                    .unwrap();
            let mut b = Budget::default();
            for (ts, value) in [(1, "1"), (2, "3"), (3, "NaN")] {
                add(&mut e, ts, ts as u64, "gauge", value, &mut b);
            }
            let values = e.finish(&mut b).unwrap().series.remove(0).values;
            if want.is_nan() {
                assert!(values[0].is_nan());
            } else {
                assert_eq!(values[0], want);
            }
            assert!(values[1].is_nan());
        }
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Avg, None, false),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        add(&mut e, 1, 1, "gauge", "+Inf", &mut b);
        add(&mut e, 2, 2, "gauge", "-Inf", &mut b);
        assert!(e.finish(&mut b).unwrap().series[0].values[0].is_nan());
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Count, Some(10), false),
            range(20),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        add(&mut e, 1, 1, "gauge", "NaN", &mut b);
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series[0].values[0], 0.);
        assert!(f.series[0].values[1].is_nan());
    }
    #[test]
    fn query_select_avg_scales_finite_sums_before_dividing() {
        for values in [["1e308", "1e308"], ["1e308", "-1e308"]] {
            let mut e = SeriesEvaluator::new(
                query(pb::Reduce::Avg, None, false),
                range(60),
                Encoding::Json,
            )
            .unwrap();
            let mut b = Budget::default();
            add(&mut e, 1, 1, "gauge", values[0], &mut b);
            add(&mut e, 2, 2, "gauge", values[1], &mut b);
            let v = e.finish(&mut b).unwrap().series[0].values[0];
            close(
                v,
                if values[1].starts_with('-') {
                    0.
                } else {
                    1e308
                },
            );
        }
    }
    #[test]
    fn query_rate_sorts_inputs_uses_bucket_duration_and_reset_increments() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(10), true),
            range(25),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for (ts, value) in [(22, "8"), (7, "20"), (12, "3"), (2, "10")] {
            add(&mut e, ts, ts as u64, "counter", value, &mut b);
        }
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series[0].values, [1., 0.3, 1.]);
    }
    #[test]
    fn query_rate_divides_increments_by_bucket_width_not_sample_span() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(60), true),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        add(&mut e, 40, 1, "counter", "140", &mut b);
        add(&mut e, 20, 2, "counter", "100", &mut b);
        close(e.finish(&mut b).unwrap().series[0].values[0], 40. / 60.);
    }
    #[test]
    fn query_rate_has_no_warmup_outside_range_and_empty_buckets_are_nan() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(10), true),
            range(40),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        add(&mut e, -1, 1, "counter", "0", &mut b);
        add(&mut e, 2, 2, "counter", "10", &mut b);
        add(&mut e, 22, 3, "counter", "20", &mut b);
        let v = e.finish(&mut b).unwrap().series.remove(0).values;
        assert!(v[0].is_nan());
        assert!(v[1].is_nan());
        assert_eq!(v[2], 1.);
        assert!(v[3].is_nan());
    }
    #[test]
    fn query_rate_nonfinite_values_follow_consecutive_ieee_increments() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(10), true),
            range(30),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for (ts, value) in [(2, "0"), (7, "NaN"), (12, "3"), (22, "8")] {
            add(&mut e, ts, ts as u64, "counter", value, &mut b);
        }
        let v = e.finish(&mut b).unwrap().series.remove(0).values;
        assert!(v[0].is_nan());
        assert!(v[1].is_nan());
        assert_eq!(v[2], 0.5);
    }
    #[test]
    fn query_rate_avoids_overflow_in_a_representable_final_rate() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(60), true),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for (ts, value) in [(1, "0"), (10, "1e308"), (20, "0"), (30, "1e308")] {
            add(&mut e, ts, ts as u64, "counter", value, &mut b);
        }
        close(
            e.finish(&mut b).unwrap().series[0].values[0],
            (2. / 60.) * 1e308,
        );
    }
    #[test]
    fn query_rate_rejects_invalid_combinations_and_uses_counting_sample_metadata() {
        failure(
            SeriesEvaluator::new(
                query(pb::Reduce::Last, Some(10), true),
                range(60),
                Encoding::Json,
            ),
            "invalid-query",
        );
        failure(
            SeriesEvaluator::new(
                query(pb::Reduce::Unspecified, None, true),
                range(60),
                Encoding::Json,
            ),
            "invalid-query",
        );
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, Some(10), true),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        failure(
            e.observe(&record(1, 1), &families("gauge", "metric 1\n"), &mut b),
            "invalid-query",
        );
        let om = exposition::parse(
            "# TYPE requests counter\nrequests_total 10\nrequests_created 100\n# EOF\n",
            Format::OpenMetrics,
        )
        .unwrap();
        let mut q = query(pb::Reduce::Unspecified, Some(10), true);
        q.name = "requests_created".into();
        let mut e = SeriesEvaluator::new(q, range(60), Encoding::Json).unwrap();
        failure(e.observe(&record(1, 1), &om, &mut b), "invalid-query");
        let mut q = query(pb::Reduce::Unspecified, Some(10), true);
        q.name = "metric_created".into();
        let mut e = SeriesEvaluator::new(q, range(60), Encoding::Json).unwrap();
        let prom = exposition::parse(
            "# TYPE metric_created counter\nmetric_created 10\n",
            Format::Prometheus,
        )
        .unwrap();
        e.observe(&record(1, 1), &prom, &mut b).unwrap();
    }
    #[test]
    fn query_select_output_limits_do_not_limit_reduced_scrape_count() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Last, None, false),
            range(20_000),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        let f = families("gauge", "metric 1\n");
        for ts in 0..11_001 {
            e.observe(&record(ts, ts as u64), &f, &mut b).unwrap();
        }
        let f = e.finish(&mut b).unwrap();
        assert_eq!(f.series[0].ts[0].seconds, 11_000);
        assert_eq!(f.series[0].values.len(), 1);
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Unspecified, None, false),
            range(20_000),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        let f = families("gauge", "metric 1\n");
        for ts in 0..11_000 {
            e.observe(&record(ts, ts as u64), &f, &mut b).unwrap();
        }
        failure(
            e.observe(&record(11_000, 11_000), &f, &mut b),
            "result-limit",
        );
        let mut q = query(pb::Reduce::Last, Some(1), false);
        q.step = Some(prost_types::Duration {
            seconds: 0,
            nanos: 1,
        });
        failure(
            SeriesEvaluator::new(q, range(1), Encoding::Json),
            "result-limit",
        );
    }
    #[test]
    fn query_select_cardinality_and_global_item_and_byte_budgets_precede_retention() {
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Last, None, false),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::default();
        for n in 0..1_000 {
            e.observe(
                &record(1, n),
                &families("gauge", &format!("metric{{peer=\"{n}\"}} 1\n")),
                &mut b,
            )
            .unwrap();
        }
        failure(
            e.observe(
                &record(1, 1_001),
                &families("gauge", "metric{peer=\"overflow\"} 1\n"),
                &mut b,
            ),
            "result-limit",
        );
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Last, None, false),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::new(Limits {
            result_items: 0,
            ..Limits::default()
        });
        assert!(matches!(
            e.observe(&record(1, 1), &families("gauge", "metric 1\n"), &mut b),
            Err(Failure::Budget(BudgetError::ResponseLimit))
        ));
        let mut e = SeriesEvaluator::new(
            query(pb::Reduce::Last, None, false),
            range(60),
            Encoding::Json,
        )
        .unwrap();
        let mut b = Budget::new(Limits {
            workspace_bytes: 128,
            ..Limits::default()
        });
        assert!(matches!(
            e.observe(
                &record(1, 1),
                &families(
                    "gauge",
                    &format!("metric{{peer=\"{}\"}} 1\n", "x".repeat(100))
                ),
                &mut b
            ),
            Err(Failure::Budget(BudgetError::ScanLimit))
        ));
        assert!(e.series.is_empty());
    }
}
