//! Hand-written parser for the Prometheus text format 0.0.4 and OpenMetrics 1.0 text (ADR 0002).
//!
//! Besides the grammar it enforces the structure metric-convention.md asks of a hand-written
//! endpoint: HELP/TYPE once and before the samples, the samples of a family together, no
//! duplicate series, and consistent histograms (cumulative buckets ending with `+Inf` equal to
//! `_count`, `_sum` and `_count` present).

use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Prometheus,
    OpenMetrics,
}

impl Format {
    /// The format announced by a response `Content-Type`.
    pub fn from_content_type(content_type: &str) -> Format {
        if content_type
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("application/openmetrics-text")
        {
            Format::OpenMetrics
        } else {
            Format::Prometheus
        }
    }

    /// The format of a file: OpenMetrics ends with `# EOF`.
    pub fn sniff(body: &[u8]) -> Format {
        let body = body.trim_ascii_end();
        if body == b"# EOF" || body.ends_with(b"\n# EOF") {
            Format::OpenMetrics
        } else {
            Format::Prometheus
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    Summary,
    StateSet,
    Info,
    /// `untyped` in 0.0.4, `unknown` in OpenMetrics, or no TYPE line.
    Unknown,
}

impl Kind {
    fn parse(s: &str, format: Format) -> Option<Kind> {
        use Format::*;
        Some(match (s, format) {
            ("counter", _) => Kind::Counter,
            ("gauge", _) => Kind::Gauge,
            ("histogram", _) => Kind::Histogram,
            ("summary", _) => Kind::Summary,
            ("untyped", Prometheus) | ("unknown", OpenMetrics) => Kind::Unknown,
            ("gaugehistogram", OpenMetrics) => Kind::GaugeHistogram,
            ("stateset", OpenMetrics) => Kind::StateSet,
            ("info", OpenMetrics) => Kind::Info,
            _ => return None,
        })
    }

    /// The sample-name suffixes a family of this kind owns.
    fn suffixes(self, format: Format) -> &'static [&'static str] {
        use Format::*;
        match (self, format) {
            (Kind::Counter, Prometheus) => &[""],
            (Kind::Counter, OpenMetrics) => &["_total", "_created"],
            (Kind::Histogram, Prometheus) => &["_bucket", "_sum", "_count"],
            (Kind::Histogram, OpenMetrics) => &["_bucket", "_sum", "_count", "_created"],
            (Kind::GaugeHistogram, _) => &["_bucket", "_gsum", "_gcount"],
            (Kind::Summary, Prometheus) => &["", "_sum", "_count"],
            (Kind::Summary, OpenMetrics) => &["", "_sum", "_count", "_created"],
            (Kind::Info, _) => &["_info"],
            (Kind::Gauge | Kind::StateSet | Kind::Unknown, _) => &[""],
        }
    }
}

/// Every suffix any kind owns, to find a sample's family by name.
const ALL_SUFFIXES: &[&str] = &[
    "", "_total", "_created", "_bucket", "_sum", "_count", "_gsum", "_gcount", "_info",
];

/// Label names and values, in exposition order.
pub type Labels = Vec<(String, String)>;

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: Labels,
    pub value: f64,
    /// Milliseconds in 0.0.4, seconds in OpenMetrics, as written.
    pub timestamp: Option<f64>,
    pub line: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Family {
    pub name: String,
    pub kind: Kind,
    pub help: Option<String>,
    pub unit: Option<String>,
    pub samples: Vec<Sample>,
    typed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for ParseError {}

pub fn parse(text: &str, format: Format) -> Result<Vec<Family>, ParseError> {
    let mut p = Parser {
        format,
        families: Vec::new(),
        by_name: HashMap::new(),
        series: HashSet::new(),
    };
    let Some(body) = text.strip_suffix('\n') else {
        if text.is_empty() && format == Format::Prometheus {
            return Ok(Vec::new());
        }
        return Err(ParseError {
            line: text.split('\n').count(),
            message: "the body does not end with a newline".into(),
        });
    };
    let mut eof = false;
    let mut last = 0;
    for (i, line) in body.split('\n').enumerate() {
        let n = i + 1;
        last = n;
        let error = |message: &str| ParseError {
            line: n,
            message: message.into(),
        };
        if eof {
            return Err(error("content after `# EOF`"));
        }
        if format == Format::OpenMetrics && line == "# EOF" {
            eof = true;
            continue;
        }
        let line = match format {
            Format::Prometheus => line.trim_start_matches([' ', '\t']),
            Format::OpenMetrics => line,
        };
        if line.is_empty() {
            if format == Format::OpenMetrics {
                return Err(error("empty line"));
            }
            continue;
        }
        let result = match line.strip_prefix('#') {
            Some(rest) => p.comment(rest),
            None => p.sample(line, n),
        };
        result.map_err(|m| error(&m))?;
    }
    if format == Format::OpenMetrics && !eof {
        return Err(ParseError {
            line: last,
            message: "missing `# EOF`".into(),
        });
    }
    for f in &p.families {
        match f.kind {
            Kind::Histogram => check_histogram(f, "_sum", "_count")?,
            Kind::GaugeHistogram => check_histogram(f, "_gsum", "_gcount")?,
            _ => {}
        }
    }
    Ok(p.families)
}

struct Parser {
    format: Format,
    families: Vec<Family>,
    by_name: HashMap<String, usize>,
    series: HashSet<(String, Labels)>,
}

impl Parser {
    fn comment(&mut self, rest: &str) -> Result<(), String> {
        let om = self.format == Format::OpenMetrics;
        let (keyword, rest) = token(rest.trim_start_matches([' ', '\t']));
        if !(matches!(keyword, "HELP" | "TYPE") || (om && keyword == "UNIT")) {
            return if om {
                Err(format!("unexpected comment `#{rest}` in OpenMetrics"))
            } else {
                Ok(()) // a plain comment in 0.0.4
            };
        }
        let (name, text) = token(rest);
        if !is_metric_name(name) {
            return Err(format!("`# {keyword}` without a valid metric name"));
        }
        let i = self.family_for_metadata(name)?;
        let format = self.format;
        let f = &mut self.families[i];
        match keyword {
            "HELP" => {
                if f.help.is_some() {
                    return Err(format!("second HELP line for `{name}`"));
                }
                f.help = Some(unescape(text, om));
            }
            "TYPE" => {
                if f.typed {
                    return Err(format!("second TYPE line for `{name}`"));
                }
                f.kind = Kind::parse(text.trim_end(), format)
                    .ok_or_else(|| format!("unknown type `{}` for `{name}`", text.trim_end()))?;
                f.typed = true;
            }
            _ => {
                if f.unit.is_some() {
                    return Err(format!("second UNIT line for `{name}`"));
                }
                if !text.is_empty() && !name.ends_with(&format!("_{text}")) {
                    return Err(format!("`{name}` does not end with its unit `{text}`"));
                }
                f.unit = Some(text.into());
            }
        }
        Ok(())
    }

    fn family_for_metadata(&mut self, name: &str) -> Result<usize, String> {
        if let Some(&i) = self.by_name.get(name) {
            if i + 1 != self.families.len() || !self.families[i].samples.is_empty() {
                return Err(format!(
                    "metadata for `{name}` after its samples or after another family"
                ));
            }
            return Ok(i);
        }
        Ok(self.new_family(name))
    }

    fn new_family(&mut self, name: &str) -> usize {
        self.families.push(Family {
            name: name.into(),
            kind: Kind::Unknown,
            help: None,
            unit: None,
            samples: Vec::new(),
            typed: false,
        });
        self.by_name.insert(name.into(), self.families.len() - 1);
        self.families.len() - 1
    }

    fn family_for_sample(&mut self, name: &str) -> Result<usize, String> {
        let format = self.format;
        let owner = ALL_SUFFIXES.iter().find_map(|suffix| {
            let base = name.strip_suffix(suffix)?;
            let &i = self.by_name.get(base)?;
            self.families[i]
                .kind
                .suffixes(format)
                .contains(suffix)
                .then_some(i)
        });
        match owner {
            Some(i) if i + 1 == self.families.len() => Ok(i),
            Some(i) => Err(format!(
                "samples of family `{}` are not together",
                self.families[i].name
            )),
            None => Ok(self.new_family(name)),
        }
    }

    fn sample(&mut self, line: &str, n: usize) -> Result<(), String> {
        let sample = parse_sample(line, self.format, n)?;
        let i = self.family_for_sample(&sample.name)?;
        let mut key = sample.labels.clone();
        key.sort();
        if !self.series.insert((sample.name.clone(), key)) {
            return Err(format!("duplicate series `{}`", sample.name));
        }
        self.families[i].samples.push(sample);
        Ok(())
    }
}

/// Splits off the first token; the rest starts after the blanks that follow it.
fn token(s: &str) -> (&str, &str) {
    match s.find([' ', '\t']) {
        Some(i) => (&s[..i], s[i..].trim_start_matches([' ', '\t'])),
        None => (s, ""),
    }
}

fn is_metric_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn is_label_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// HELP text: `\\` and `\n` (and `\"` in OpenMetrics) are escapes; other backslashes are kept.
fn unescape(s: &str, om: bool) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('"') if om => out.push('"'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// A sample value: decimal floats, and `+Inf`, `-Inf`, `NaN` spelled exactly so.
pub fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        // Rust also reads `inf`, `infinity`, `nan` in any case: the formats do not.
        _ if s
            .bytes()
            .any(|b| b.is_ascii_alphabetic() && b != b'e' && b != b'E') =>
        {
            None
        }
        _ => s.parse().ok(),
    }
}

fn parse_sample(line: &str, format: Format, n: usize) -> Result<Sample, String> {
    let om = format == Format::OpenMetrics;
    let end = line
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(line.len());
    let name = &line[..end];
    if !is_metric_name(name) {
        return Err("expected a metric name".into());
    }
    let mut rest = &line[end..];
    let mut labels = Vec::new();
    let before_labels = if om {
        rest
    } else {
        rest.trim_start_matches([' ', '\t'])
    };
    if before_labels.starts_with('{') {
        let (parsed, after) = parse_labels(before_labels, om)?;
        labels = parsed;
        rest = after;
    }
    let Some(rest) = rest.strip_prefix([' ', '\t']) else {
        return Err(format!("expected a value after `{name}`"));
    };
    let (rest, exemplar) = match rest.split_once(" # ") {
        Some((r, e)) if om => (r, Some(e)),
        _ => (rest, None),
    };
    let mut tokens = rest.split([' ', '\t']).filter(|t| !t.is_empty() || om);
    let value_token = tokens.next().unwrap_or("");
    let value = parse_value(value_token).ok_or_else(|| format!("invalid value `{value_token}`"))?;
    let timestamp = match tokens.next() {
        None => None,
        Some(t) if om => Some(
            parse_value(t)
                .filter(|v| v.is_finite())
                .ok_or_else(|| format!("invalid timestamp `{t}`"))?,
        ),
        Some(t) => Some(
            t.parse::<i64>()
                .map_err(|_| format!("invalid timestamp `{t}`"))? as f64,
        ),
    };
    if let Some(t) = tokens.next() {
        return Err(format!("unexpected `{t}` after the value"));
    }
    if let Some(e) = exemplar {
        let (_, after) = parse_labels(e, true).map_err(|m| format!("exemplar: {m}"))?;
        let mut parts = after.strip_prefix(' ').unwrap_or("").split(' ');
        let v = parts.next().unwrap_or("");
        parse_value(v).ok_or_else(|| format!("exemplar: invalid value `{v}`"))?;
        if let Some(ts) = parts.next() {
            parse_value(ts).ok_or_else(|| format!("exemplar: invalid timestamp `{ts}`"))?;
        }
        if parts.next().is_some() {
            return Err("exemplar: unexpected content".into());
        }
    }
    Ok(Sample {
        name: name.into(),
        labels,
        value,
        timestamp,
        line: n,
    })
}

/// Parses `{a="b",…}` at the start of `s`; returns the labels and what follows `}`.
fn parse_labels(s: &str, om: bool) -> Result<(Labels, &str), String> {
    let blanks = |s: &str| -> usize {
        if om {
            0
        } else {
            s.len() - s.trim_start_matches([' ', '\t']).len()
        }
    };
    if !s.starts_with('{') {
        return Err("expected `{`".into());
    }
    let mut labels: Labels = Vec::new();
    let mut i = 1; // after `{`
    let mut after_comma = false;
    loop {
        i += blanks(&s[i..]);
        let rest = &s[i..];
        if let Some(after) = rest.strip_prefix('}') {
            if after_comma && om {
                return Err("trailing comma in the label set".into());
            }
            return Ok((labels, after));
        }
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let name = &rest[..end];
        if !is_label_name(name) {
            return Err("expected a label name".into());
        }
        i += end;
        i += blanks(&s[i..]);
        if !s[i..].starts_with("=") {
            return Err(format!("expected `=` after label `{name}`"));
        }
        i += 1;
        i += blanks(&s[i..]);
        if !s[i..].starts_with('"') {
            return Err(format!("expected `\"` for label `{name}`"));
        }
        i += 1;
        let mut value = String::new();
        let mut chars = s[i..].char_indices();
        loop {
            match chars.next() {
                None => return Err(format!("unterminated value of label `{name}`")),
                Some((j, '"')) => {
                    i += j + 1;
                    break;
                }
                Some((_, '\\')) => match chars.next() {
                    Some((_, '\\')) => value.push('\\'),
                    Some((_, '"')) => value.push('"'),
                    Some((_, 'n')) => value.push('\n'),
                    _ => return Err(format!("invalid escape in the value of label `{name}`")),
                },
                Some((_, c)) => value.push(c),
            }
        }
        if labels.iter().any(|(k, _)| k == name) {
            return Err(format!("label `{name}` twice"));
        }
        labels.push((name.into(), value));
        i += blanks(&s[i..]);
        match s[i..].chars().next() {
            Some(',') => {
                i += 1;
                after_comma = true;
            }
            Some('}') => after_comma = false,
            _ => return Err("expected `,` or `}` in the label set".into()),
        }
    }
}

struct Group {
    labels: Labels,
    line: usize,
    buckets: Vec<(f64, f64, usize)>,
    sum: bool,
    count: Option<f64>,
}

/// Buckets, sum and count of a histogram (`_sum`, `_count`) or gauge histogram (`_gsum`,
/// `_gcount`). Buckets and count are counts: neither NaN nor negative.
fn check_histogram(f: &Family, sum: &str, count: &str) -> Result<(), ParseError> {
    let mut groups: Vec<Group> = Vec::new();
    for s in &f.samples {
        let mut labels: Vec<_> = s
            .labels
            .iter()
            .filter(|(k, _)| k != "le")
            .cloned()
            .collect();
        labels.sort();
        let g = match groups.iter().position(|g| g.labels == labels) {
            Some(i) => &mut groups[i],
            None => {
                groups.push(Group {
                    labels,
                    line: s.line,
                    buckets: Vec::new(),
                    sum: false,
                    count: None,
                });
                groups.last_mut().unwrap()
            }
        };
        let error = |message: String| ParseError {
            line: s.line,
            message,
        };
        let suffix = &s.name[f.name.len()..];
        if (suffix == "_bucket" || suffix == count) && (s.value.is_nan() || s.value < 0.0) {
            return Err(error(format!("`{}` is not a count: {}", s.name, s.value)));
        }
        match suffix {
            "_bucket" => {
                let le = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == "le")
                    .ok_or_else(|| error(format!("`{}` without `le`", s.name)))?;
                let bound = parse_value(&le.1)
                    .filter(|b| !b.is_nan())
                    .ok_or_else(|| error(format!("invalid `le=\"{}\"`", le.1)))?;
                if let Some(&(prev_le, prev_v, _)) = g.buckets.last() {
                    if bound <= prev_le {
                        return Err(error(format!("`le` not ascending in `{}`", f.name)));
                    }
                    if s.value < prev_v {
                        return Err(error(format!("buckets of `{}` not cumulative", f.name)));
                    }
                }
                g.buckets.push((bound, s.value, s.line));
            }
            x if x == sum => g.sum = true,
            x if x == count => g.count = Some(s.value),
            _ => {}
        }
    }
    for g in &groups {
        let error = |message: String| ParseError {
            line: g.line,
            message: format!("histogram `{}`: {message}", f.name),
        };
        let Some(&(_, inf, _)) = g.buckets.last().filter(|b| b.0 == f64::INFINITY) else {
            return Err(error("no `le=\"+Inf\"` bucket last".into()));
        };
        let Some(count) = g.count else {
            return Err(error(format!("no `{count}`")));
        };
        if !g.sum {
            return Err(error(format!("no `{sum}`")));
        }
        if count != inf {
            return Err(error(format!(
                "`le=\"+Inf\"` bucket ({inf}) differs from `_count` ({count})"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prom(s: &str) -> Result<Vec<Family>, ParseError> {
        parse(s, Format::Prometheus)
    }

    #[test]
    fn families_and_samples() {
        let f = prom(concat!(
            "# HELP a_total Things.\n",
            "# TYPE a_total counter\n",
            "a_total{x=\"1\",y=\"q\\\"\\\\\\n\"} 3 1700000000000\n",
            "\n",
            "# random comment\n",
            "b 1e+06\n",
            "  c{} -Inf\n",
        ))
        .unwrap();
        assert_eq!(f.len(), 3);
        assert_eq!(f[0].kind, Kind::Counter);
        assert_eq!(f[0].help.as_deref(), Some("Things."));
        assert_eq!(f[0].samples[0].labels[1].1, "q\"\\\n");
        assert_eq!(f[0].samples[0].timestamp, Some(1.7e12));
        assert_eq!(f[1].samples[0].value, 1e6);
        assert_eq!(f[2].samples[0].value, f64::NEG_INFINITY);
        assert_eq!(f[2].samples[0].line, 7);
    }

    #[test]
    fn openmetrics() {
        let f = parse(
            concat!(
                "# TYPE foo_seconds counter\n",
                "# UNIT foo_seconds seconds\n",
                "# HELP foo_seconds Time.\n",
                "foo_seconds_total{a=\"b\"} 1.5 1520879607.789 # {trace_id=\"x\"} 1 1520879607.7\n",
                "foo_seconds_created{a=\"b\"} 1520430000.123\n",
                "# TYPE i info\n",
                "i_info{version=\"1\"} 1\n",
                "# EOF\n",
            ),
            Format::OpenMetrics,
        )
        .unwrap();
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].samples.len(), 2);
        assert_eq!(f[0].unit.as_deref(), Some("seconds"));
    }

    #[test]
    fn errors() {
        let bad = [
            ("a 1", "newline"),
            ("a\n", "value"),
            ("a 1 2 3\n", "unexpected"),
            ("a inf\n", "invalid value"),
            ("a{b=\"\\x\"} 1\n", "escape"),
            ("a{b=\"1\",b=\"2\"} 1\n", "twice"),
            ("a 1\na 1\n", "duplicate"),
            ("a 1\nb 1\na{x=\"1\"} 1\n", "not together"),
            ("# TYPE a gauge\n# TYPE a gauge\n", "second TYPE"),
            ("a 1\n# TYPE a gauge\n", "after its samples"),
            ("# TYPE a stateset\n", "unknown type"),
        ];
        for (input, want) in bad {
            let e = prom(input).unwrap_err();
            assert!(e.message.contains(want), "{input:?}: {e}");
        }
        let om_bad = [
            ("a 1\n", "missing `# EOF`"),
            ("a 1\n# EOF\nb 1\n", "after `# EOF`"),
            ("a{b=\"1\",} 1\n# EOF\n", "trailing comma"),
            ("# hello\n# EOF\n", "unexpected comment"),
            ("\n# EOF\n", "empty line"),
            (
                "# TYPE a_bytes gauge\n# UNIT a_bytes seconds\n# EOF\n",
                "unit",
            ),
        ];
        for (input, want) in om_bad {
            let e = parse(input, Format::OpenMetrics).unwrap_err();
            assert!(e.message.contains(want), "{input:?}: {e}");
        }
    }

    #[test]
    fn histograms() {
        let ok = concat!(
            "# TYPE h histogram\n",
            "h_bucket{p=\"a\",le=\"0.1\"} 1\n",
            "h_bucket{p=\"a\",le=\"+Inf\"} 2\n",
            "h_sum{p=\"a\"} 0.3\n",
            "h_count{p=\"a\"} 2\n",
        );
        prom(ok).unwrap();
        for (broken, want) in [
            (
                ok.replace("le=\"0.1\"} 1", "le=\"0.1\"} 3"),
                "not cumulative",
            ),
            (ok.replace("0.1", "+Inf"), "duplicate"),
            (ok.replace("+Inf", "0.05"), "not ascending"),
            (
                ok.replace("h_count{p=\"a\"} 2", "h_count{p=\"a\"} 5"),
                "differs",
            ),
            (ok.replace("h_sum{p=\"a\"} 0.3\n", ""), "no `_sum`"),
        ] {
            let e = prom(&broken).unwrap_err();
            assert!(e.message.contains(want), "{broken:?}: {e}");
        }
    }

    #[test]
    fn sniff() {
        assert_eq!(Format::sniff(b"a 1\n# EOF\n"), Format::OpenMetrics);
        assert_eq!(Format::sniff(b"a 1\n"), Format::Prometheus);
        assert_eq!(
            Format::from_content_type("application/openmetrics-text; version=1.0.0"),
            Format::OpenMetrics
        );
    }
}
