//! `masu validate` against the fixtures and the convention's own examples (ADR 0005, 0009).

use masu_core::convention::{self, Rule};
use masu_core::exposition::{self, Format};
use std::path::{Path, PathBuf};

fn fixtures(dir: &str) -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/metrics")
        .join(dir);
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    files
}

/// `.om` files are OpenMetrics, the others Prometheus text 0.0.4.
fn format(path: &Path) -> Format {
    match path.extension().and_then(|e| e.to_str()) {
        Some("om") => Format::OpenMetrics,
        _ => Format::Prometheus,
    }
}

#[test]
fn valid_fixtures_pass() {
    for path in fixtures("valid") {
        let v = convention::validate(&std::fs::read(&path).unwrap(), format(&path));
        assert!(v.is_empty(), "{}: {v:?}", path.display());
    }
}

/// `invalid/<rule>[.<case>].<ext>` breaks exactly that rule.
#[test]
fn each_invalid_fixture_breaks_its_rule() {
    let mut rules_seen = Vec::new();
    for path in fixtures("invalid") {
        let name = path.file_name().unwrap().to_str().unwrap();
        let rule = name.split('.').next().unwrap();
        let v = convention::validate(&std::fs::read(&path).unwrap(), format(&path));
        assert!(!v.is_empty(), "{name}: accepted");
        for x in &v {
            assert_eq!(x.rule.id(), rule, "{name}: {x}");
        }
        rules_seen.push(v[0].rule);
    }
    for rule in [
        Rule::ParseError,
        Rule::ForbiddenLabel,
        Rule::ReservedLabel,
        Rule::CounterWithoutTotal,
        Rule::NonBaseUnit,
        Rule::SeriesBudget,
    ] {
        assert!(rules_seen.contains(&rule), "no fixture for {}", rule.id());
    }
}

/// A real node_exporter scrape (1.11.1, darwin, node name replaced by `node-a`) parses.
/// node_exporter is not a house service: of the convention it breaks only the series budget
/// and the `id` label of `node_os_info`.
#[test]
fn node_exporter_scrape() {
    let path = fixtures("node-exporter").remove(0);
    let body = std::fs::read(&path).unwrap();
    let families =
        exposition::parse(std::str::from_utf8(&body).unwrap(), Format::Prometheus).unwrap();
    assert_eq!(families.len(), 97);
    let v = convention::validate(&body, Format::sniff(&body));
    let mut rules: Vec<_> = v.iter().map(|x| x.rule).collect();
    rules.dedup();
    assert_eq!(rules.len(), 2, "{v:?}");
    assert!(v.iter().any(|x| x.rule == Rule::SeriesBudget));
    assert!(
        v.iter()
            .any(|x| x.rule == Rule::ForbiddenLabel && x.message.contains("node_os_info"))
    );
}

/// Every exposition example of docs/metric-convention.md passes. The block of the stack's own
/// metrics lists names, not samples: each line becomes a sample (first alternative of `a|b`,
/// value 0), and the `nisshi_…` placeholder is skipped.
#[test]
fn convention_examples_pass() {
    let doc = include_str!("../../../docs/metric-convention.md");
    let blocks: Vec<&str> = doc.split("```").skip(1).step_by(2).collect();
    assert!(blocks.len() >= 5, "examples not found");
    for block in blocks {
        let body = block.strip_prefix('\n').unwrap_or(block);
        let text = if body.lines().any(|l| l.starts_with("hikyaku_")) {
            body.lines()
                .filter(|l| !l.contains('…'))
                .map(|l| {
                    let mut l = l.to_string();
                    while let Some(i) = l.find('|') {
                        let end = l[i..].find('"').unwrap() + i;
                        l.replace_range(i..end, "");
                    }
                    format!("{l} 0\n")
                })
                .collect()
        } else {
            body.to_string()
        };
        let v = convention::validate(text.as_bytes(), Format::Prometheus);
        assert!(v.is_empty(), "{text}\n{v:?}");
    }
}
