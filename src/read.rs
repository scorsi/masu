//! `masu read` and `masu verify` (ADR 0008): read the archive files directly, no server.

use crate::exposition::{self, Format};
use crate::seal::{Manifest, Scan, differences, next_line, open_day};
use crate::store::{node_dirs, read_dir_sorted};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// `masu read` refuses a query past this many output rows (they are sorted in memory).
const MAX_ROWS: usize = 1_000_000;

pub struct Query {
    pub node: String,
    pub service: Option<String>,
    /// Exact name or prefix of the sample name.
    pub metric: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub raw: bool,
}

/// Prints `ts node service series value` lines sorted by `ts` (`!error` for a failed scrape,
/// `!parse-error:…` for a body the exposition parser refuses), or the matching records with `--raw`.
/// A damaged record (not JSON, envelope missing) is an error, never skipped.
pub fn read(root: &Path, q: &Query, out: &mut impl Write) -> io::Result<()> {
    read_rows(root, q, out, MAX_ROWS)
}

fn read_rows(root: &Path, q: &Query, out: &mut impl Write, max_rows: usize) -> io::Result<()> {
    let dir = root.join("archive").join(&q.node);
    let mut rows: Vec<(String, String)> = Vec::new();
    for file in read_dir_sorted(&dir)? {
        let Some(day) = data_day(&file) else {
            continue;
        };
        // A manifest's ts range prunes the day; an open day is always read.
        if let Some(m) = manifest(&dir.join(format!("{day}.manifest.json")))
            .map_err(|e| io::Error::other(format!("{}/{day}: {e}", q.node)))?
            && (m
                .min_ts
                .as_deref()
                .zip(q.to.as_deref())
                .is_some_and(|(min, to)| min > to)
                || m.max_ts
                    .as_deref()
                    .zip(q.from.as_deref())
                    .is_some_and(|(max, from)| max < from))
        {
            continue;
        }
        let mut lines = open_day(&file)?;
        let mut line = Vec::new();
        let mut n = 0;
        let damaged = |n: u64, e: &dyn std::fmt::Display| {
            io::Error::other(format!("{}/{day}: record {n}: {e}", q.node))
        };
        while next_line(&mut lines, &mut line).map_err(|e| damaged(n + 1, &e))? {
            n += 1;
            let v: Value = serde_json::from_slice(&line).map_err(|e| damaged(n, &e))?;
            let (Some(ts), Some(service), Some(up)) =
                (v["ts"].as_str(), v["service"].as_str(), v["up"].as_bool())
            else {
                return Err(damaged(n, &"envelope without ts, service or up"));
            };
            if q.from.as_deref().is_some_and(|f| ts < f) || q.to.as_deref().is_some_and(|t| ts > t)
            {
                continue;
            }
            if q.service.as_deref().is_some_and(|s| s != service) {
                continue;
            }
            if rows.len() >= max_rows {
                return Err(io::Error::other(format!(
                    "more than {max_rows} rows: narrow the query (--from, --to, --service, --metric)"
                )));
            }
            if q.raw {
                rows.push((ts.into(), String::from_utf8_lossy(&line).trim_end().into()));
                continue;
            }
            let head = format!("{ts} {} {service}", q.node);
            if !up {
                rows.push((
                    ts.into(),
                    format!("{head} !{}", v["error"].as_str().unwrap_or("error")),
                ));
                continue;
            }
            let format = Format::from_content_type(v["content_type"].as_str().unwrap_or(""));
            match exposition::parse(v["body"].as_str().unwrap_or(""), format) {
                Err(e) => rows.push((ts.into(), format!("{head} !parse-error:{e}"))),
                Ok(families) => {
                    for s in families.iter().flat_map(|f| &f.samples) {
                        if q.metric.as_deref().is_some_and(|m| !s.name.starts_with(m)) {
                            continue;
                        }
                        // shortcut: a sample's own timestamp is ignored, the record's ts is printed;
                        // revisit when the engine re-ingests the archive.
                        rows.push((
                            ts.into(),
                            format!("{head} {} {}", series(&s.name, &s.labels), s.value),
                        ));
                    }
                }
            }
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, row) in rows {
        writeln!(out, "{row}")?;
    }
    Ok(())
}

/// A day's manifest; `None` when the day is not sealed. Any other read error is an error.
fn manifest(path: &Path) -> io::Result<Option<Manifest>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| io::Error::other(format!("manifest: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io::Error::other(format!("manifest: {e}"))),
    }
}

fn series(name: &str, labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return name.into();
    }
    let labels: Vec<String> = labels
        .iter()
        .map(|(k, v)| {
            format!(
                "{k}=\"{}\"",
                v.replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
            )
        })
        .collect();
    format!("{name}{{{}}}", labels.join(","))
}

/// Checks `FORMAT`, every data file (decompression, every manifest field recomputed) and
/// `(stream, seq)` uniqueness over the whole archive. Returns the error lines; empty means the
/// archive is sound.
pub fn verify(root: &Path, out: &mut impl Write) -> io::Result<Vec<String>> {
    let mut errors = Vec::new();
    if fs::read_to_string(root.join("FORMAT")).ok().as_deref() != Some(crate::store::FORMAT) {
        errors.push("FORMAT: missing or not `masu-archive 1`".to_string());
    }
    let mut seen = Seen::default();
    for node in node_dirs(root)? {
        let node_name = node.file_name().unwrap().to_string_lossy().into_owned();
        for file in read_dir_sorted(&node)? {
            let Some(day) = data_day(&file) else {
                continue;
            };
            let name = format!("{node_name}/{day}");
            let manifest_path = node.join(format!("{day}.manifest.json"));
            match verify_file(&file, &node_name, day, &manifest_path, &mut seen) {
                Ok(records) => writeln!(out, "{name}: ok ({records} records)")?,
                Err(e) => errors.push(format!("{name}: {e}")),
            }
        }
    }
    for e in &errors {
        writeln!(out, "{e}")?;
    }
    Ok(errors)
}

fn verify_file(
    file: &Path,
    node: &str,
    day: &str,
    manifest_path: &Path,
    seen: &mut Seen,
) -> Result<u64, String> {
    let manifest = manifest(manifest_path).map_err(|e| e.to_string())?;
    let mut scan = Scan::default();
    let mut lines = open_day(file).map_err(|e| e.to_string())?;
    let mut line = Vec::new();
    let mut records = 0u64;
    while next_line(&mut lines, &mut line).map_err(|e| format!("record {}: {e}", records + 1))? {
        records += 1;
        let v = scan.line(&line).map_err(|e| e.to_string())?;
        if let (Some(stream), Some(seq)) = (v["stream"].as_str(), v["seq"].as_u64())
            && !seen.insert(stream, seq)
        {
            return Err(format!(
                "record {records}: duplicate (stream {stream}, seq {seq})"
            ));
        }
    }
    if let Some(m) = manifest {
        let diff = differences(&scan.manifest(node, day, m.sealed_at.clone()), &m);
        if !diff.is_empty() {
            return Err(diff);
        }
    }
    Ok(records)
}

/// Every `(stream, seq)` seen, as disjoint inclusive ranges per stream: a stream's consecutive
/// seqs cost one entry, not one per record.
#[derive(Default)]
struct Seen(HashMap<String, BTreeMap<u64, u64>>);

impl Seen {
    /// `false` when `(stream, seq)` was already seen.
    fn insert(&mut self, stream: &str, seq: u64) -> bool {
        if !self.0.contains_key(stream) {
            self.0.insert(stream.into(), BTreeMap::new());
        }
        let ranges = self.0.get_mut(stream).unwrap();
        let below = ranges.range(..=seq).next_back().map(|(&a, &b)| (a, b));
        if below.is_some_and(|(_, end)| seq <= end) {
            return false;
        }
        let start = match below {
            Some((start, end)) if end + 1 == seq => start,
            _ => seq,
        };
        let end = match seq.checked_add(1).and_then(|next| ranges.remove(&next)) {
            Some(end) => end,
            None => seq,
        };
        ranges.insert(start, end);
        true
    }
}

fn data_day(file: &Path) -> Option<&str> {
    file.file_name()?.to_str()?.strip_suffix(".ndjson.zst")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Reply;
    use crate::store::tests::{STREAM, open, request, tempdir};

    #[test]
    fn read_and_verify() {
        let root = tempdir("read");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 5)),
            Reply::Ok { acked_seq: 5 }
        );
        let q = Query {
            node: "node-a".into(),
            service: Some("annai".into()),
            metric: Some("annai_queries_total".into()),
            from: Some("2026-10-19T08:15:00.000000000Z".into()),
            to: Some("2026-10-19T08:16:00.000000000Z".into()),
            raw: false,
        };
        let mut out = Vec::new();
        read(&root, &q, &mut out).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "2026-10-19T08:15:00.000000000Z node-a annai annai_queries_total{proto=\"udp\"} 20\n\
             2026-10-19T08:15:30.000000000Z node-a annai annai_queries_total{proto=\"udp\"} 30\n\
             2026-10-19T08:16:00.000000000Z node-a annai annai_queries_total{proto=\"udp\"} 40\n"
        );
        let mut out = Vec::new();
        read(
            &root,
            &Query {
                raw: true,
                metric: None,
                ..q
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 3);
        assert!(out.starts_with(b"{\"v\":1,"));

        let mut out = Vec::new();
        assert!(
            verify(&root, &mut out).unwrap().is_empty(),
            "{}",
            String::from_utf8_lossy(&out)
        );
        // A duplicated (stream, seq) is an error.
        let key = s.state.files.keys().next().unwrap().clone();
        let path = s.data_path(&key);
        let dup = fs::read(&path).unwrap();
        fs::write(s.data_path("node-a/2020-01-01"), &dup).unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("duplicate"), "{errors:?}");
        fs::remove_file(s.data_path("node-a/2020-01-01")).unwrap();
        // A manifest whose hash does not match is an error.
        fs::write(
            s.manifest_path(&key),
            serde_json::to_vec(&Manifest {
                v: 1,
                node: "node-a".into(),
                day: "x".into(),
                records: 5,
                uncompressed_bytes: 0,
                content_sha256: "00".into(),
                min_ts: None,
                max_ts: None,
                services: Default::default(),
                streams: vec![],
                sealed_at: String::new(),
            })
            .unwrap(),
        )
        .unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert!(errors[0].contains("content_sha256"), "{errors:?}");
        let _ = fs::remove_dir_all(root);
    }

    fn query(node: &str) -> Query {
        Query {
            node: node.into(),
            service: None,
            metric: None,
            from: None,
            to: None,
            raw: true,
        }
    }

    /// F12: verify recomputes the exact bytes and every manifest field, and an unreadable
    /// manifest is an error, not an absent one.
    #[test]
    fn verify_checks_bytes_and_manifest() {
        let root = tempdir("verify-strict");
        let dir = root.join("archive/node-a");
        fs::create_dir_all(&dir).unwrap();
        fs::write(root.join("FORMAT"), crate::store::FORMAT).unwrap();
        let rec = |seq: u64| {
            String::from_utf8(crate::store::tests::record("node-a", &STREAM, seq).to_vec()).unwrap()
        };
        let content = format!("{}\n{}\n", rec(1), rec(2));
        let data = dir.join("2026-10-19.ndjson.zst");
        fs::write(&data, zstd::encode_all(content.as_bytes(), 3).unwrap()).unwrap();
        let mut scan = Scan::default();
        for l in content.split_inclusive('\n') {
            scan.line(l.as_bytes()).unwrap();
        }
        let good = scan.manifest(
            "node-a",
            "2026-10-19",
            "2026-10-20T00:10:00.000000000Z".into(),
        );
        let mpath = dir.join("2026-10-19.manifest.json");
        fs::write(&mpath, serde_json::to_vec(&good).unwrap()).unwrap();
        assert!(verify(&root, &mut Vec::new()).unwrap().is_empty());
        // Same records without the last LF.
        fs::write(
            &data,
            zstd::encode_all(content.trim_end().as_bytes(), 3).unwrap(),
        )
        .unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert!(errors[0].contains("without LF"), "{errors:?}");
        fs::write(&data, zstd::encode_all(content.as_bytes(), 3).unwrap()).unwrap();
        // A wrong time range, hash right.
        let mut bad = serde_json::to_value(&good).unwrap();
        bad["min_ts"] = "2020-01-01T00:00:00.000000000Z".into();
        fs::write(&mpath, serde_json::to_vec(&bad).unwrap()).unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert!(errors[0].contains("min_ts"), "{errors:?}");
        // An unreadable manifest.
        fs::remove_file(&mpath).unwrap();
        fs::create_dir(&mpath).unwrap();
        assert_eq!(verify(&root, &mut Vec::new()).unwrap().len(), 1);
        // F09, F16: a frame over the 8 MiB window, a line over 4 MiB.
        fs::remove_dir(&mpath).unwrap();
        let mut enc = zstd::Encoder::new(Vec::new(), 1).unwrap();
        enc.set_parameter(zstd::stream::raw::CParameter::WindowLog(27))
            .unwrap();
        enc.write_all(content.as_bytes()).unwrap();
        fs::write(&data, enc.finish().unwrap()).unwrap();
        assert_eq!(verify(&root, &mut Vec::new()).unwrap().len(), 1);
        let long = "x".repeat(crate::record::MAX_STORED_BYTES + 10);
        fs::write(&data, zstd::encode_all(long.as_bytes(), 3).unwrap()).unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert!(errors[0].contains("longer than a record"), "{errors:?}");
        let _ = fs::remove_dir_all(root);
    }

    /// F13: a broken record or envelope is an error, never skipped.
    #[test]
    fn read_refuses_damaged_records() {
        let root = tempdir("read-damaged");
        let dir = root.join("archive/node-a");
        fs::create_dir_all(&dir).unwrap();
        let rec = |seq: u64| {
            String::from_utf8(crate::store::tests::record("node-a", &STREAM, seq).to_vec()).unwrap()
        };
        let data = dir.join("2026-10-19.ndjson.zst");
        for broken in ["{not json", r#"{"v":1,"stream":"x"}"#] {
            let content = format!("{}\n{broken}\n{}\n", rec(1), rec(3));
            fs::write(&data, zstd::encode_all(content.as_bytes(), 3).unwrap()).unwrap();
            let err = read(&root, &query("node-a"), &mut Vec::new()).unwrap_err();
            assert!(err.to_string().contains("record 2"), "{err}");
        }
        // The row budget.
        fs::write(
            &data,
            zstd::encode_all(format!("{}\n{}\n", rec(1), rec(2)).as_bytes(), 3).unwrap(),
        )
        .unwrap();
        assert!(read_rows(&root, &query("node-a"), &mut Vec::new(), 2).is_ok());
        assert!(read_rows(&root, &query("node-a"), &mut Vec::new(), 1).is_err());
        let _ = fs::remove_dir_all(root);
    }

    /// F16: seqs kept as ranges; duplicates found across and inside ranges.
    #[test]
    fn seen_ranges() {
        let mut seen = Seen::default();
        for seq in [1, 2, 3, 5, 4, 10, u64::MAX] {
            assert!(seen.insert("s", seq), "{seq}");
        }
        assert_eq!(seen.0["s"].len(), 3);
        for seq in [1, 3, 5, 10, u64::MAX] {
            assert!(!seen.insert("s", seq), "{seq}");
        }
        assert!(seen.insert("t", 3));
    }
}
