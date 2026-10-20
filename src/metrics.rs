//! Own metrics (`masu_*` of docs/metric-convention.md), served on the loopback listener.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// `(name, type, help)`; `masu_disk_free_bytes` is read from the filesystem at render time.
const FAMILIES: &[(&str, &str, &str)] = &[
    ("masu_build_info", "gauge", "Build information."),
    (
        "masu_records_total",
        "counter",
        "Records committed, by pushing node.",
    ),
    (
        "masu_duplicate_records_total",
        "counter",
        "Records dropped as already committed.",
    ),
    (
        "masu_seq_gaps_total",
        "counter",
        "Batches that started above hwm + 1.",
    ),
    (
        "masu_push_requests_total",
        "counter",
        "Push requests, by response code.",
    ),
    (
        "masu_disk_free_bytes",
        "gauge",
        "Free bytes on the data directory's filesystem.",
    ),
    (
        "masu_seals_total",
        "counter",
        "Day files sealed, by result.",
    ),
];

#[derive(Default)]
pub struct Metrics {
    /// `(family, labels)` → value; labels rendered as `{a="x",b="y"}` or empty.
    counters: Mutex<BTreeMap<(usize, String), u64>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let m = Metrics::default();
        m.add("masu_duplicate_records_total", &[], 0);
        m.add("masu_seq_gaps_total", &[], 0);
        for code in ["200", "400", "401", "403", "413", "500", "507"] {
            m.add("masu_push_requests_total", &[("code", code)], 0);
        }
        for result in ["ok", "error"] {
            m.add("masu_seals_total", &[("result", result)], 0);
        }
        m
    }

    pub fn inc(&self, name: &str, labels: &[(&str, &str)]) {
        self.add(name, labels, 1);
    }

    pub fn add(&self, name: &str, labels: &[(&str, &str)], n: u64) {
        let family = FAMILIES
            .iter()
            .position(|f| f.0 == name)
            .expect("unknown metric family");
        let mut labels: Vec<String> = labels
            .iter()
            .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
            .collect();
        labels.sort();
        let key = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", labels.join(","))
        };
        *self
            .counters
            .lock()
            .unwrap()
            .entry((family, key))
            .or_insert(0) += n;
    }

    /// Prometheus text 0.0.4.
    pub fn render(&self, disk_free_bytes: u64) -> String {
        let mut counters = self.counters.lock().unwrap().clone();
        counters.insert((5, String::new()), disk_free_bytes);
        counters.insert(
            (0, format!("{{version=\"{}\"}}", env!("CARGO_PKG_VERSION"))),
            1,
        );
        let mut out = String::new();
        for (i, (name, kind, help)) in FAMILIES.iter().enumerate() {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
            for ((_, labels), value) in counters.range((i, String::new())..(i + 1, String::new())) {
                out.push_str(&format!("{name}{labels} {value}\n"));
            }
        }
        out
    }
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convention;
    use crate::exposition::Format;

    #[test]
    fn renders_a_conforming_exposition() {
        let m = Metrics::new();
        m.inc("masu_records_total", &[("source_node", "node-a")]);
        m.add("masu_records_total", &[("source_node", "node-a")], 9);
        m.inc("masu_push_requests_total", &[("code", "200")]);
        let text = m.render(42);
        assert!(text.contains("masu_records_total{source_node=\"node-a\"} 10\n"));
        assert!(text.contains("masu_push_requests_total{code=\"200\"} 1\n"));
        assert!(text.contains("masu_seq_gaps_total 0\n"));
        assert!(text.contains("masu_disk_free_bytes 42\n"));
        let violations = convention::validate(text.as_bytes(), Format::Prometheus);
        assert!(violations.is_empty(), "{violations:?}");
    }
}
