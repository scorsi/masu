//! HTTPS query client. No archive access or enrollment/identity changes.
use masu_core::{
    pb,
    query::document::{self, Encoding},
};
use prost_types::{Duration, Timestamp};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    process::ExitCode,
    time::SystemTime,
};
mod transport;

type Result<T> = std::result::Result<T, String>;
pub fn run(args: &[&str]) -> ExitCode {
    let result = (|| {
        let (request, query, json) = prepare(args)?;
        let config = transport::Config::from_env()?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "cannot start query runtime")?;
        let bytes = rt.block_on(transport::request(config, &request))?;
        let response = document::decode_response(&bytes, Encoding::Json)
            .map_err(|e| format!("query response: {e}"))?;
        if response.results.len() != 1 || !response.results.contains_key("query") {
            return Err("query response names do not match request".into());
        }
        let result = &response.results["query"];
        let plan = if matches!(result.body, Some(pb::query_result::Body::Error(_))) {
            None
        } else {
            Some(validate(result, &query, request.range.as_ref().unwrap())?)
        };
        Ok::<_, String>((bytes, response, query, json, plan))
    })();
    let (bytes, response, query, json, plan) = match result {
        Ok(v) => v,
        Err(e) => {
            eprintln!("masu: {e}");
            return ExitCode::from(2);
        }
    };
    let result = &response.results["query"];
    if let Some(pb::query_result::Body::Error(error)) = &result.body {
        if error.reason.len() > 128 || error.reason.bytes().any(|b| !b.is_ascii_graphic()) {
            eprintln!("masu: invalid query error reason");
            return ExitCode::from(2);
        }
        if json && std::io::stdout().lock().write_all(&bytes).is_err() {
            eprintln!("masu: cannot write stdout");
            return ExitCode::from(2);
        }
        eprintln!("masu: query: {}", error.reason);
        return ExitCode::from(3);
    }
    let write = if json {
        std::io::stdout().lock().write_all(&bytes)
    } else {
        write_text(
            result,
            &query,
            &plan.unwrap(),
            &mut std::io::BufWriter::new(std::io::stdout().lock()),
        )
    };
    match write {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("masu: stdout: {e}");
            ExitCode::from(2)
        }
    }
}

fn prepare(args: &[&str]) -> Result<(pb::QueryRequest, pb::SeriesQuery, bool)> {
    let now: Timestamp = SystemTime::now().into();
    let mut fields = BTreeMap::new();
    let mut labels = BTreeMap::new();
    let mut json = false;
    let mut rate = false;
    let mut it = args.iter();
    while let Some(&flag) = it.next() {
        if flag == "--json" {
            if json {
                return Err("duplicate --json".into());
            }
            json = true;
            continue;
        }
        if flag == "--rate" {
            if rate {
                return Err("duplicate --rate".into());
            }
            rate = true;
            continue;
        }
        if !matches!(
            flag,
            "--node"
                | "--service"
                | "--metric"
                | "--label"
                | "--from"
                | "--to"
                | "--step"
                | "--reduce"
        ) {
            return Err(format!("unknown query option: {flag}"));
        }
        let value = *it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        if flag == "--label" {
            let (k, v) = value.split_once('=').ok_or("--label requires key=value")?;
            if !label_name(k) || labels.insert(k.to_string(), v.to_string()).is_some() {
                return Err("invalid or duplicate label".into());
            }
        } else if fields.insert(flag, value).is_some() {
            return Err(format!("duplicate {flag}"));
        }
    }
    let required = |key| {
        fields
            .get(key)
            .copied()
            .ok_or_else(|| format!("{key} is required"))
    };
    let node = required("--node")?;
    let service = required("--service")?;
    let metric = required("--metric")?;
    if !masu_core::record::is_node(node)
        || service.is_empty()
        || service.len() > 128
        || metric.len() > 256
        || !metric_name(metric)
    {
        return Err("invalid node, service or metric".into());
    }
    let from = parse_time(required("--from")?, now)?;
    let to = parse_time(required("--to")?, now)?;
    let step = fields.get("--step").map(|s| duration(s)).transpose()?;
    let reduce = match fields
        .get("--reduce")
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        None => pb::Reduce::Unspecified,
        Some("last") => pb::Reduce::Last,
        Some("min") => pb::Reduce::Min,
        Some("max") => pb::Reduce::Max,
        Some("avg") => pb::Reduce::Avg,
        Some("count") => pb::Reduce::Count,
        _ => return Err("--reduce: expected last|min|max|avg|count".into()),
    };
    if rate && (step.is_none() || reduce != pb::Reduce::Unspecified) {
        return Err("--rate needs --step and cannot combine with --reduce".into());
    }
    let query = pb::SeriesQuery {
        node: node.into(),
        service: service.into(),
        name: metric.into(),
        labels: labels.into_iter().collect(),
        step,
        reduce: reduce.into(),
        rate,
    };
    let request = pb::QueryRequest {
        range: Some(pb::Range {
            from: Some(from),
            to: Some(to),
        }),
        queries: BTreeMap::from([(
            "query".into(),
            pb::Query {
                body: Some(pb::query::Body::Series(query.clone())),
            },
        )]),
    };
    document::encode_request(&request, Encoding::Json).map_err(|e| format!("query: {e}"))?;
    Ok((request, query, json))
}
fn label_name(s: &str) -> bool {
    let mut c = s.bytes();
    c.next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && c.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn metric_name(s: &str) -> bool {
    let mut c = s.bytes();
    c.next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b':')
        && c.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
}
fn span_ns(s: &str) -> Result<i128> {
    let units = [
        ("ns", 1i128),
        ("us", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60_000_000_000),
        ("h", 3_600_000_000_000),
        ("d", 86_400_000_000_000),
    ];
    let (number, multiplier) = units
        .iter()
        .find_map(|(unit, m)| s.strip_suffix(unit).map(|n| (n, *m)))
        .ok_or("duration requires integer ns|us|ms|s|m|h|d")?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err("duration requires an unsigned integer".into());
    }
    number
        .parse::<i128>()
        .ok()
        .and_then(|v| v.checked_mul(multiplier))
        .ok_or("duration out of range".into())
}
fn duration(s: &str) -> Result<Duration> {
    let ns = span_ns(s)?;
    if ns == 0 {
        return Err("step must be positive".into());
    }
    let seconds = i64::try_from(ns / 1_000_000_000).map_err(|_| "duration out of range")?;
    if seconds > 315_576_000_000 {
        return Err("duration out of range".into());
    }
    Ok(Duration {
        seconds,
        nanos: (ns % 1_000_000_000) as i32,
    })
}
fn parse_time(s: &str, now: Timestamp) -> Result<Timestamp> {
    if let Some(offset) = s.strip_prefix('-') {
        let delta = span_ns(offset)?;
        let ns = i128::from(now.seconds) * 1_000_000_000 + i128::from(now.nanos) - delta;
        let seconds =
            i64::try_from(ns.div_euclid(1_000_000_000)).map_err(|_| "time out of range")?;
        if !(-62_135_596_800..=253_402_300_799).contains(&seconds) {
            return Err("time out of range".into());
        }
        Ok(Timestamp {
            seconds,
            nanos: ns.rem_euclid(1_000_000_000) as i32,
        })
    } else {
        s.parse::<Timestamp>()
            .map_err(|_| "expected RFC3339 timestamp or negative duration offset".into())
    }
}
struct TextPlan {
    names: Vec<String>,
    points: Vec<(Timestamp, usize, usize)>,
}
fn point_time(series: &pb::Series, index: usize) -> Result<Timestamp> {
    if let Some(step) = series.step {
        let start = series.start.ok_or("stepped series has no start")?;
        let n = i128::from(start.seconds) * 1_000_000_000
            + i128::from(start.nanos)
            + (index as i128) * (i128::from(step.seconds) * 1_000_000_000 + i128::from(step.nanos));
        Ok(Timestamp {
            seconds: i64::try_from(n.div_euclid(1_000_000_000))
                .map_err(|_| "response timestamp overflow")?,
            nanos: n.rem_euclid(1_000_000_000) as i32,
        })
    } else {
        series
            .ts
            .get(index)
            .copied()
            .ok_or("series timestamps missing".into())
    }
}
fn ns(t: Timestamp) -> i128 {
    i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos)
}
fn validate(
    result: &pb::QueryResult,
    query: &pb::SeriesQuery,
    range: &pb::Range,
) -> Result<TextPlan> {
    let Some(pb::query_result::Body::Series(frame)) = &result.body else {
        return Err("expected a series response".into());
    };
    if !result.cursor.is_empty() || frame.series.len() > 1000 {
        return Err("invalid series response cardinality or cursor".into());
    }
    let (from, to) = (ns(range.from.unwrap()), ns(range.to.unwrap()));
    let mut names = Vec::new();
    let mut points = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (si, series) in frame.series.iter().enumerate() {
        let mut key: Vec<_> = series
            .labels
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        key.sort_unstable();
        if !seen.insert(key)
            || series.labels.keys().any(|k| !label_name(k))
            || query
                .labels
                .iter()
                .any(|(k, v)| series.labels.get(k) != Some(v))
        {
            return Err("response does not match requested labels".into());
        }
        if series.step != query.step {
            return Err("response step does not match query".into());
        }
        if let Some(step) = query.step {
            let span = ns(Timestamp {
                seconds: step.seconds,
                nanos: step.nanos,
            });
            if series.start != range.from
                || span <= 0
                || series.values.len() as i128 != (to - from - 1) / span + 1
            {
                return Err("response buckets do not match query range".into());
            }
        } else if query.reduce != pb::Reduce::Unspecified as i32 && series.values.len() != 1 {
            return Err("reduced response must have one value per series".into());
        }
        if series.values.len() > 11000 || points.len() + series.values.len() > 2_000_000 {
            return Err("response exceeds point limit".into());
        }
        let mut labels = series
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        labels.sort();
        names.push(masu_core::read::series(&query.name, &labels));
        let mut previous = None;
        for i in 0..series.values.len() {
            let ts = point_time(series, i)?;
            let value = ns(ts);
            if value < from || value >= to || previous.is_some_and(|p| p > value) {
                return Err("response timestamps are outside the range or unordered".into());
            }
            previous = Some(value);
            points.push((ts, si, i));
        }
    }
    // Sort indices, never cloned label strings or complete rendered lines per point.
    points.sort_by(|a, b| {
        (a.0.seconds, a.0.nanos, &names[a.1]).cmp(&(b.0.seconds, b.0.nanos, &names[b.1]))
    });
    Ok(TextPlan { names, points })
}
fn write_text(
    result: &pb::QueryResult,
    query: &pb::SeriesQuery,
    plan: &TextPlan,
    out: &mut impl Write,
) -> std::io::Result<()> {
    let Some(pb::query_result::Body::Series(frame)) = &result.body else {
        unreachable!("validated series frame")
    };
    for (ts, series, point) in &plan.points {
        writeln!(
            out,
            "{} {} {} {} {}",
            canonical_time(*ts),
            query.node,
            query.service,
            plan.names[*series],
            frame.series[*series].values[*point]
        )?;
    }
    out.flush()
}

fn canonical_time(ts: Timestamp) -> String {
    let text = ts.to_string();
    let base = text.strip_suffix('Z').unwrap_or(&text);
    if let Some((whole, fraction)) = base.split_once('.') {
        format!("{whole}.{fraction:0<9}Z")
    } else {
        format!("{base}.000000000Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> (pb::QueryRequest, pb::SeriesQuery, bool) {
        prepare(&[
            "--node",
            "node-a",
            "--service",
            "annai",
            "--metric",
            "annai_total",
            "--from",
            "2026-11-01T00:00:00Z",
            "--to",
            "2026-11-02T00:00:00Z",
        ])
        .unwrap()
    }
    fn result(series: Vec<pb::Series>) -> pb::QueryResult {
        pb::QueryResult {
            body: Some(pb::query_result::Body::Series(pb::SeriesFrame { series })),
            cursor: String::new(),
        }
    }
    fn sample(labels: &[(&str, &str)], times: &[i64]) -> pb::Series {
        pb::Series {
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ts: times
                .iter()
                .map(|s| Timestamp {
                    seconds: 1_793_491_200 + s,
                    nanos: 0,
                })
                .collect(),
            values: times.iter().map(|s| *s as f64).collect(),
            ..pb::Series::default()
        }
    }
    #[test]
    fn query_client_parses_time_offsets_units_and_options_strictly() {
        let now = Timestamp {
            seconds: 3600,
            nanos: 123,
        };
        assert_eq!(
            parse_time("-1h", now).unwrap(),
            Timestamp {
                seconds: 0,
                nanos: 123
            }
        );
        assert_eq!(parse_time("-0s", now).unwrap(), now);
        assert_eq!(
            duration("250ms").unwrap(),
            Duration {
                seconds: 0,
                nanos: 250_000_000
            }
        );
        assert!(duration("0s").is_err());
        assert!(duration("-1s").is_err());
        assert!(duration("1.5m").is_err());
        assert_eq!(
            parse_time("2026-11-01T01:00:00+01:00", now)
                .unwrap()
                .to_string(),
            "2026-11-01T00:00:00Z"
        );
        for args in [
            vec!["--root", "/tmp"],
            vec!["--node", "../bad"],
            vec!["--label", "x=a", "--label", "x=b"],
        ] {
            assert!(prepare(&args).is_err());
        }
        assert!(metric_name("a:1"));
        assert!(!metric_name("1a"));
        assert!(!label_name("bad:name"));
        let (_, query, json) = request();
        assert!(!json);
        assert_eq!(query.name, "annai_total");
    }
    #[test]
    fn query_client_text_is_chronological_canonical_and_validated_before_writing() {
        let (req, q, _) = request();
        let frame = result(vec![
            sample(&[("z", "quoted\"\n"), ("a", "v")], &[3, 5]),
            sample(&[("z", "other")], &[1]),
        ]);
        let plan = validate(&frame, &q, req.range.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        write_text(&frame, &q, &plan, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(
            out.lines()
                .next()
                .unwrap()
                .starts_with("2026-11-01T00:00:01.000000000Z node-a annai")
        );
        assert!(out.contains(r#"annai_total{a="v",z="quoted\"\n"}"#));
        let invalid = result(vec![sample(&[], &[3, 1])]);
        assert!(validate(&invalid, &q, req.range.as_ref().unwrap()).is_err());
        let duplicate = result(vec![
            sample(&[("a", "b")], &[1]),
            sample(&[("a", "b")], &[2]),
        ]);
        assert!(validate(&duplicate, &q, req.range.as_ref().unwrap()).is_err());
    }
    #[test]
    fn query_client_large_labels_are_stored_once_and_streamed_by_index() {
        let (req, q, _) = request();
        let label = "x".repeat(1 << 20);
        let ts = (0..11000).collect::<Vec<_>>();
        let frame = result(vec![sample(&[("label", &label)], &ts)]);
        let plan = validate(&frame, &q, req.range.as_ref().unwrap()).unwrap();
        assert_eq!(plan.names.len(), 1);
        assert_eq!(plan.points.len(), 11000);
        assert!(std::mem::size_of_val(plan.points.as_slice()) < 1 << 20);
        struct Count(u64);
        impl Write for Count {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0 += b.len() as u64;
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(0);
        write_text(&frame, &q, &plan, &mut count).unwrap();
        assert!(count.0 > 10_000_000_000);
    }
}
