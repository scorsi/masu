//! `masu validate --metrics`: the failure list of docs/metric-convention.md (ADR 0005).

use crate::exposition::{self, Format, Kind};
use std::collections::HashSet;
use std::fmt;

/// Series budget of one service.
pub const MAX_SERIES: usize = 500;
const FORBIDDEN_LABELS: &[&str] = &[
    "ip",
    "addr",
    "client",
    "qname",
    "domain",
    "host",
    "path",
    "url",
    "user",
    "id",
    "request_id",
];
const RESERVED_LABELS: &[&str] = &["node", "service"];
const NON_BASE_UNITS: &[&str] = &["_ms", "_milliseconds", "_kb", "_percent"];
/// Runtime metrics are allowed as the runtime gives them; they still count in the budget.
const RUNTIME_PREFIXES: &[&str] = &["process_", "go_"];
/// Sample suffixes stripped before the unit check (`foo_ms_total` is in milliseconds).
const SAMPLE_SUFFIXES: &[&str] = &[
    "_total", "_created", "_bucket", "_sum", "_count", "_gsum", "_gcount", "_info",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Rule {
    ParseError,
    ForbiddenLabel,
    ReservedLabel,
    CounterWithoutTotal,
    NonBaseUnit,
    SeriesBudget,
}

impl Rule {
    pub fn id(self) -> &'static str {
        match self {
            Rule::ParseError => "parse-error",
            Rule::ForbiddenLabel => "forbidden-label",
            Rule::ReservedLabel => "reserved-label",
            Rule::CounterWithoutTotal => "counter-without-total",
            Rule::NonBaseUnit => "non-base-unit",
            Rule::SeriesBudget => "series-budget",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub rule: Rule,
    pub line: usize,
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "line {}: {}: {}",
            self.line,
            self.rule.id(),
            self.message
        )
    }
}

/// Every violation of the convention, in line order; empty when the exposition conforms.
pub fn validate(body: &[u8], format: Format) -> Vec<Violation> {
    let text = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(e) => {
            let line = body[..e.valid_up_to()]
                .iter()
                .filter(|&&b| b == b'\n')
                .count()
                + 1;
            return vec![violation(Rule::ParseError, line, "not UTF-8".into())];
        }
    };
    let families = match exposition::parse(text, format) {
        Ok(f) => f,
        Err(e) => return vec![violation(Rule::ParseError, e.line, e.message)],
    };
    let mut out = Vec::new();
    for f in &families {
        if RUNTIME_PREFIXES.iter().any(|p| f.name.starts_with(p)) {
            continue;
        }
        // One violation per family and rule (and label), at its first sample.
        let mut seen = HashSet::new();
        let mut report = |rule: Rule, key: &str, line: usize, message: String| {
            if seen.insert((rule, key.to_string())) {
                out.push(violation(rule, line, message));
            }
        };
        for s in &f.samples {
            for (label, _) in &s.labels {
                if FORBIDDEN_LABELS.contains(&label.as_str()) {
                    let m = format!("`{}` has the forbidden label `{label}`", s.name);
                    report(Rule::ForbiddenLabel, label, s.line, m);
                } else if RESERVED_LABELS.contains(&label.as_str()) {
                    let m = format!("`{}` has the reserved label `{label}`", s.name);
                    report(Rule::ReservedLabel, label, s.line, m);
                }
            }
            if f.kind == Kind::Counter
                && !s.name.ends_with("_total")
                && !s.name.ends_with("_created")
            {
                let m = format!("counter `{}` does not end with `_total`", s.name);
                report(Rule::CounterWithoutTotal, "", s.line, m);
            }
            let base = SAMPLE_SUFFIXES
                .iter()
                .find_map(|suffix| s.name.strip_suffix(suffix))
                .unwrap_or(&s.name);
            if let Some(unit) = NON_BASE_UNITS.iter().find(|u| base.ends_with(*u)) {
                let m = format!("`{}` is in `{}`, not a base unit", s.name, &unit[1..]);
                report(Rule::NonBaseUnit, base, s.line, m);
            }
        }
    }
    if let Some(s) = families.iter().flat_map(|f| &f.samples).nth(MAX_SERIES) {
        let total: usize = families.iter().map(|f| f.samples.len()).sum();
        let m = format!("{total} series, the budget is {MAX_SERIES}");
        out.push(violation(Rule::SeriesBudget, s.line, m));
    }
    out.sort_by_key(|v| v.line);
    out
}

fn violation(rule: Rule, line: usize, message: String) -> Violation {
    Violation {
        rule,
        line,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(s: &str) -> Vec<Rule> {
        validate(s.as_bytes(), Format::Prometheus)
            .into_iter()
            .map(|v| v.rule)
            .collect()
    }

    #[test]
    fn rules_apply() {
        assert!(rules("svc_up 1\n").is_empty());
        assert_eq!(
            rules("svc_a_total{client=\"x\"} 1\nsvc_b_total{client=\"y\"} 1\n"),
            [Rule::ForbiddenLabel, Rule::ForbiddenLabel]
        );
        assert_eq!(rules("svc_a_total{node=\"x\"} 1\n"), [Rule::ReservedLabel]);
        assert_eq!(
            rules("# TYPE svc_a counter\nsvc_a 1\n"),
            [Rule::CounterWithoutTotal]
        );
        assert_eq!(rules("svc_wait_ms_total 1\n"), [Rule::NonBaseUnit]);
        assert_eq!(rules("svc_a 1\nsvc_a 2\n"), [Rule::ParseError]);
        // Runtime metrics are exempt from the naming rules.
        assert!(rules("go_thing_ms{id=\"1\"} 1\n").is_empty());
        assert_eq!(validate(b"a 1\n\xff\n", Format::Prometheus)[0].line, 2);
    }

    #[test]
    fn budget() {
        let ok: String = (0..MAX_SERIES)
            .map(|i| format!("svc_x{{n=\"{i}\"}} 1\n"))
            .collect();
        assert!(rules(&ok).is_empty());
        let over = format!("{ok}svc_y 1\n");
        let v = validate(over.as_bytes(), Format::Prometheus);
        assert_eq!(v[0].rule, Rule::SeriesBudget);
        assert_eq!(v[0].line, MAX_SERIES + 1);
    }
}
