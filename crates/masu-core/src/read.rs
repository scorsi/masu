//! Direct `masu-store read/verify` (ADRs 0008/0015), without recovery or archive mutation.

use crate::exposition::{self, Format};
use crate::query::{
    budget::{Budget, Limits},
    snapshot::Snapshot,
};
#[cfg(test)]
use crate::seal::Scan;
use crate::store::node_dirs;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Direct `read` refuses a query past this many output rows (they are sorted in memory).
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

fn direct_budget() -> Budget {
    Budget::new(Limits {
        metadata_bytes: u64::MAX,
        files: u64::MAX,
        directory_entries_per_node: u64::MAX,
        compressed_bytes: u64::MAX,
        decompressed_bytes: u64::MAX,
        records: u64::MAX,
        wall_time: std::time::Duration::MAX,
        ..Limits::default()
    })
}
fn read_rows(root: &Path, q: &Query, out: &mut impl Write, max_rows: usize) -> io::Result<()> {
    let mut budget = direct_budget();
    let mut snapshot =
        Snapshot::capture_for_read(root, &q.node, &mut budget).map_err(io::Error::other)?;
    let mut rows = Vec::new();
    for day in &mut snapshot.days {
        if day.is_missing() {
            continue;
        }
        if day.manifest.as_ref().is_some_and(|m| {
            m.min_ts
                .as_deref()
                .zip(q.to.as_deref())
                .is_some_and(|(min, to)| min > to)
                || m.max_ts
                    .as_deref()
                    .zip(q.from.as_deref())
                    .is_some_and(|(max, from)| max < from)
        }) {
            continue;
        }
        let mut n = 0u64;
        day.scan_lines(&mut budget, |value, line, _| {
            n += 1;
            collect_rows(q, value, line, &mut rows, max_rows)
                .map_err(|e| crate::query::snapshot::Error::Corrupt(format!("record {n}: {e}")))
        })
        .map_err(|e| io::Error::other(format!("{}/{}: {e}", q.node, day.day)))?;
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, row) in rows {
        writeln!(out, "{row}")?;
    }
    Ok(())
}
fn collect_rows(
    q: &Query,
    v: &Value,
    line: &[u8],
    rows: &mut Vec<(String, String)>,
    max_rows: usize,
) -> io::Result<()> {
    let (Some(ts), Some(service), Some(up)) =
        (v["ts"].as_str(), v["service"].as_str(), v["up"].as_bool())
    else {
        return Err(io::Error::other("envelope without ts, service or up"));
    };
    if q.from.as_deref().is_some_and(|f| ts < f) || q.to.as_deref().is_some_and(|t| ts > t) {
        return Ok(());
    }
    if q.service.as_deref().is_some_and(|s| s != service) {
        return Ok(());
    }
    // The budget is checked before every row, each sample of a record included.
    let mut emit = |row: String| {
        if rows.len() >= max_rows {
            return Err(io::Error::other(format!(
                "more than {max_rows} rows: narrow the query (--from, --to, --service, --metric)"
            )));
        }
        rows.push((ts.to_string(), row));
        Ok(())
    };
    if q.raw {
        emit(String::from_utf8_lossy(line).trim_end().into())?;
        return Ok(());
    }
    let head = format!("{ts} {} {service}", q.node);
    if !up {
        emit(format!(
            "{head} !{}",
            v["error"].as_str().unwrap_or("error")
        ))?;
        return Ok(());
    }
    let format = Format::from_content_type(v["content_type"].as_str().unwrap_or(""));
    match exposition::parse(v["body"].as_str().unwrap_or(""), format) {
        Err(e) => emit(format!("{head} !parse-error:{e}"))?,
        Ok(families) => {
            for s in families.iter().flat_map(|f| &f.samples) {
                if q.metric.as_deref().is_some_and(|m| !s.name.starts_with(m)) {
                    continue;
                }
                // shortcut: a sample's own timestamp is ignored, the record's ts is printed;
                // revisit when the engine re-ingests the archive.
                emit(format!("{head} {} {}", series(&s.name, &s.labels), s.value))?;
            }
        }
    }
    Ok(())
}

pub fn series(name: &str, labels: &[(String, String)]) -> String {
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
        let error = "FORMAT: missing or not `masu-archive 1`".to_string();
        writeln!(out, "{error}")?;
        return Ok(vec![error]);
    }
    let mut seen = Seen::default();
    let mut budget = direct_budget();
    let _root_guard =
        crate::query::snapshot::ReadGuard::acquire(root, &mut budget).map_err(io::Error::other)?;
    for node in node_dirs(root)? {
        let node_name = node.file_name().unwrap().to_string_lossy().into_owned();
        let mut snapshot = match Snapshot::capture_for_read(root, &node_name, &mut budget) {
            Ok(s) => s,
            Err(e) => {
                errors.push(format!("{node_name}: {e}"));
                continue;
            }
        };
        for day in &mut snapshot.days {
            if day.is_missing() {
                continue;
            }
            let name = format!("{node_name}/{}", day.day);
            let mut records = 0u64;
            let result = day.scan(&mut budget, |v, _| {
                records += 1;
                let stream = v["stream"].as_str().unwrap();
                let seq = v["seq"].as_u64().unwrap();
                if !seen.insert(stream, seq) {
                    return Err(crate::query::snapshot::Error::Corrupt(format!(
                        "record {records}: duplicate (stream {stream}, seq {seq})"
                    )));
                }
                Ok(())
            });
            match result {
                Ok(()) => writeln!(out, "{name}: ok ({records} records)")?,
                Err(e) => errors.push(format!("{name}: {e}")),
            }
        }
    }
    for error in &errors {
        writeln!(out, "{error}")?;
    }
    Ok(errors)
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
        let decoded = zstd::decode_all(fs::File::open(&path).unwrap()).unwrap();
        let mut duplicate = Vec::new();
        for line in decoded.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let mut value: Value = serde_json::from_slice(line).unwrap();
            value["recv_ts"] = "2020-01-01T00:00:00.000000000Z".into();
            duplicate.extend_from_slice(format!("{value}\n").as_bytes());
        }
        let dup = zstd::encode_all(duplicate.as_slice(), 3).unwrap();
        fs::write(s.data_path("node-a/2020-01-01"), &dup).unwrap();
        s.state
            .files
            .insert("node-a/2020-01-01".into(), dup.len() as u64);
        s.write_state().unwrap();
        let errors = verify(&root, &mut Vec::new()).unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("duplicate"), "{errors:?}");
        fs::remove_file(s.data_path("node-a/2020-01-01")).unwrap();
        s.state.files.remove("node-a/2020-01-01");
        s.write_state().unwrap();
        // A manifest whose hash does not match is an error.
        let decoded = zstd::decode_all(fs::File::open(s.data_path(&key)).unwrap()).unwrap();
        let mut scan = Scan::default();
        for line in decoded.split_inclusive(|&b| b == b'\n') {
            scan.line(line).unwrap();
        }
        let mut manifest = scan.manifest(
            "node-a",
            key.split_once('/').unwrap().1,
            "2026-11-02T00:10:00.000000000Z".into(),
        );
        manifest.content_sha256 = "0".repeat(64);
        fs::write(
            s.manifest_path(&key),
            serde_json::to_vec(&manifest).unwrap(),
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
            let mut v: Value =
                serde_json::from_slice(&crate::store::tests::record("node-a", &STREAM, seq))
                    .unwrap();
            v["recv_ts"] = "2026-10-19T10:00:00.000000000Z".into();
            v.to_string()
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
        fs::write(root.join("FORMAT"), crate::store::FORMAT).unwrap();
        let dir = root.join("archive/node-a");
        fs::create_dir_all(&dir).unwrap();
        let rec = |seq: u64| {
            let mut v: Value =
                serde_json::from_slice(&crate::store::tests::record("node-a", &STREAM, seq))
                    .unwrap();
            v["recv_ts"] = "2026-10-19T10:00:00.000000000Z".into();
            v.to_string()
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
        // Validation item 6: a record of three samples is three rows; a record with no matching
        // sample adds none.
        let three = rec(1).replace(
            r#"annai_queries_total{proto=\"udp\"} 10\n"#,
            r#"annai_queries_total{proto=\"udp\"} 10\nannai_queries_total{proto=\"tcp\"} 1\nannai_queries_total{proto=\"dot\"} 2\n"#,
        );
        let other = rec(2).replace("annai_queries_total", "annai_other_total");
        fs::write(
            &data,
            zstd::encode_all(format!("{three}\n{other}\n").as_bytes(), 3).unwrap(),
        )
        .unwrap();
        let samples = Query {
            raw: false,
            metric: Some("annai_queries_total".into()),
            ..query("node-a")
        };
        let mut out = Vec::new();
        read_rows(&root, &samples, &mut out, 3).unwrap();
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 3);
        let err = read_rows(&root, &samples, &mut Vec::new(), 2).unwrap_err();
        assert!(err.to_string().contains("more than 2 rows"), "{err}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direct_read_and_verify_ignore_uncommitted_live_tails_without_mutation() {
        let root = tempdir("direct-live");
        let mut store = open(&root);
        assert_eq!(
            store.push("node-a", &request("node-a", &STREAM, 1, 2)),
            Reply::Ok { acked_seq: 2 }
        );
        let key = store.state.files.keys().next().unwrap().clone();
        let data = store.data_path(&key);
        let state = fs::read(root.join("state.json")).unwrap();
        let committed = fs::read(&data).unwrap();
        let mut tailed = committed.clone();
        tailed.extend(&committed);
        tailed.extend([0x28, 0xb5]);
        fs::write(&data, &tailed).unwrap();
        let mut out = Vec::new();
        read(&root, &query("node-a"), &mut out).unwrap();
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 2);
        assert!(verify(&root, &mut Vec::new()).unwrap().is_empty());
        assert_eq!(fs::read(&data).unwrap(), tailed);
        assert_eq!(fs::read(root.join("state.json")).unwrap(), state);
        fs::remove_file(root.join("state.json")).unwrap();
        assert!(
            read(&root, &query("node-a"), &mut Vec::new()).is_err(),
            "an active writer without state must never become an offline reader"
        );
        drop(store);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn direct_restored_read_holds_a_shared_lock_and_never_rebuilds_state() {
        let root = tempdir("direct-offline");
        let mut store = open(&root);
        store.push("node-a", &request("node-a", &STREAM, 1, 2));
        let key = store.state.files.keys().next().unwrap().clone();
        let data = store.data_path(&key);
        let before = fs::read(&data).unwrap();
        drop(store);
        fs::remove_file(root.join("state.json")).unwrap();
        let snapshot = Snapshot::capture_for_read(&root, "node-a", &mut direct_budget()).unwrap();
        let probe = fs::File::open(root.join("FORMAT")).unwrap();
        assert!(probe.try_lock().is_err());
        drop(snapshot);
        probe.try_lock().unwrap();
        drop(probe);
        let mut out = Vec::new();
        read(&root, &query("node-a"), &mut out).unwrap();
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 2);
        assert!(verify(&root, &mut Vec::new()).unwrap().is_empty());
        assert!(!root.join("state.json").exists());
        assert_eq!(fs::read(data).unwrap(), before);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn direct_verify_empty_offline_root_still_excludes_a_writer() {
        let root = tempdir("verify-empty-lock");
        fs::write(root.join("FORMAT"), crate::store::FORMAT).unwrap();
        let writer = fs::File::open(root.join("FORMAT")).unwrap();
        writer.try_lock().unwrap();
        let mut out = Vec::new();
        assert!(verify(&root, &mut out).is_err());
        assert!(out.is_empty());
        drop(writer);
        assert!(verify(&root, &mut out).unwrap().is_empty());
        let probe = fs::File::open(root.join("FORMAT")).unwrap();
        probe.try_lock().unwrap();
        assert!(!root.join("state.json").exists());
        drop(probe);
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
