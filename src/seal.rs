//! Sealing, compaction and retention (ADR 0006, docs/storage-format.md "Manifest").

use crate::store::{Store, fsync_dir, read_dir_sorted};
use crate::time;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

const SEAL_LEVEL: i32 = 9;
/// 8 MiB window: readable by any zstd with the default limits.
const WINDOW_LOG: u32 = 23;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub v: u32,
    pub node: String,
    pub day: String,
    pub records: u64,
    pub uncompressed_bytes: u64,
    pub content_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ts: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ts: Option<String>,
    pub services: BTreeMap<String, u64>,
    pub streams: Vec<StreamRange>,
    pub sealed_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamRange {
    pub stream: String,
    pub first_seq: u64,
    pub last_seq: u64,
}

/// Seals every open file of a past UTC day, then applies retention. Idempotent: a sealed file
/// leaves `state.files`, so a second run finds nothing to do.
pub fn run(store: &Arc<Mutex<Store>>, retention_days: u32) {
    let today = time::day_of_ns(time::now_ns());
    let (keys, root) = {
        let s = store.lock().unwrap();
        let keys: Vec<String> = s
            .state
            .files
            .keys()
            .filter(|k| {
                k.split_once('/')
                    .is_some_and(|(_, day)| day < today.as_str())
            })
            .cloned()
            .collect();
        (keys, s.root().to_path_buf())
    };
    for key in keys {
        match seal_one(store, &key) {
            Ok(m) => {
                store
                    .lock()
                    .unwrap()
                    .metrics
                    .inc("masu_seals_total", &[("result", "ok")]);
                crate::log::info(
                    "seal",
                    serde_json::json!({"file": key, "records": m.records, "uncompressed_bytes": m.uncompressed_bytes}),
                );
            }
            Err(e) => {
                store
                    .lock()
                    .unwrap()
                    .metrics
                    .inc("masu_seals_total", &[("result", "error")]);
                crate::log::error("seal", &e.to_string(), serde_json::json!({"file": key}));
            }
        }
    }
    if retention_days > 0
        && let Err(e) = retention(&root, &today, retention_days)
    {
        crate::log::error("retention", &e.to_string(), serde_json::json!({}));
    }
}

/// Scan, recompress to a temporary file, verify, rename, manifest, drop from the state. Only the
/// last three steps hold the store lock; nothing writes a past day's file meanwhile.
fn seal_one(store: &Arc<Mutex<Store>>, key: &str) -> io::Result<Manifest> {
    let (node, day) = key.split_once('/').unwrap();
    let (path, manifest_path) = {
        let s = store.lock().unwrap();
        (s.data_path(key), s.manifest_path(key))
    };
    let tmp = path.with_extension("zst.tmp");
    let mut scan = Scan::default();
    let mut encoder = zstd::Encoder::new(File::create(&tmp)?, SEAL_LEVEL)?;
    encoder.set_parameter(zstd::stream::raw::CParameter::WindowLog(WINDOW_LOG))?;
    let mut lines = BufReader::new(zstd::Decoder::new(File::open(&path)?)?);
    let mut line = Vec::new();
    loop {
        line.clear();
        if lines.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        scan.line(&line)?;
        encoder.write_all(&line)?;
    }
    let out = encoder.finish()?;
    out.sync_all()?;
    // Decompress again: the sealed bytes must hash like the original.
    let mut check = Sha256::new();
    let mut decoder = zstd::Decoder::new(File::open(&tmp)?)?;
    let mut buf = vec![0u8; 1 << 16];
    let mut n_bytes = 0u64;
    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        n_bytes += n as u64;
        check.update(&buf[..n]);
    }
    let content_sha256 = crate::store::hex(&scan.hash.finalize());
    if crate::store::hex(&check.finalize()) != content_sha256 || n_bytes != scan.bytes {
        let _ = fs::remove_file(&tmp);
        return Err(io::Error::other("recompressed file does not match"));
    }
    let manifest = Manifest {
        v: 1,
        node: node.into(),
        day: day.into(),
        records: scan.records,
        uncompressed_bytes: scan.bytes,
        content_sha256,
        min_ts: scan.min_ts,
        max_ts: scan.max_ts,
        services: scan.services,
        streams: scan
            .streams
            .into_iter()
            .map(|(stream, (first_seq, last_seq))| StreamRange {
                stream,
                first_seq,
                last_seq,
            })
            .collect(),
        sealed_at: time::format_ns(time::now_ns()),
    };

    let mut s = store.lock().unwrap();
    fs::rename(&tmp, &path)?;
    let dir = path.parent().unwrap();
    fsync_dir(dir)?;
    let mtmp = manifest_path.with_extension("json.tmp");
    let mut f = File::create(&mtmp)?;
    f.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(&mtmp, &manifest_path)?;
    fsync_dir(dir)?;
    s.state.files.remove(key);
    s.write_state()?;
    Ok(manifest)
}

#[derive(Default)]
struct Scan {
    hash: Sha256,
    bytes: u64,
    records: u64,
    min_ts: Option<String>,
    max_ts: Option<String>,
    services: BTreeMap<String, u64>,
    streams: BTreeMap<String, (u64, u64)>,
}

impl Scan {
    fn line(&mut self, line: &[u8]) -> io::Result<()> {
        self.hash.update(line);
        self.bytes += line.len() as u64;
        self.records += 1;
        let v: Value = serde_json::from_slice(line).map_err(|e| {
            io::Error::other(format!("record {}: not valid JSON: {e}", self.records))
        })?;
        if let Some(ts) = v["ts"].as_str() {
            if self.min_ts.as_deref().is_none_or(|m| ts < m) {
                self.min_ts = Some(ts.into());
            }
            if self.max_ts.as_deref().is_none_or(|m| ts > m) {
                self.max_ts = Some(ts.into());
            }
        }
        if let Some(service) = v["service"].as_str() {
            *self.services.entry(service.into()).or_insert(0) += 1;
        }
        if let (Some(stream), Some(seq)) = (v["stream"].as_str(), v["seq"].as_u64()) {
            let e = self.streams.entry(stream.into()).or_insert((seq, seq));
            e.0 = e.0.min(seq);
            e.1 = e.1.max(seq);
        }
        Ok(())
    }
}

/// Deletes the data file of every sealed day older than `retention_days`; manifests stay.
fn retention(root: &Path, today: &str, retention_days: u32) -> io::Result<()> {
    let today = time::parse_day(today).unwrap();
    for node in read_dir_sorted(&root.join("archive"))? {
        for file in read_dir_sorted(&node)? {
            let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let Some(day) = name.strip_suffix(".manifest.json") else {
                continue;
            };
            let Some(day_n) = time::parse_day(day) else {
                continue;
            };
            let data = node.join(format!("{day}.ndjson.zst"));
            if today - day_n > i64::from(retention_days) && data.exists() {
                fs::remove_file(&data)?;
                fsync_dir(&node)?;
                crate::log::info(
                    "retention-delete",
                    serde_json::json!({"file": format!("{}/{day}", node.file_name().unwrap().to_string_lossy())}),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::{STREAM, lines, open, request, tempdir};
    use crate::store::{Reply, hex};

    #[test]
    fn seal_is_idempotent_and_retention_keeps_manifests() {
        let root = tempdir("seal");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 20)),
            Reply::Ok { acked_seq: 20 }
        );
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 21, 20)),
            Reply::Ok { acked_seq: 40 }
        );
        let before = lines(&s, "node-a");
        let key = s.state.files.keys().next().unwrap().clone();
        // Rename the open day to yesterday, as a day change would leave it.
        let yesterday = time::day_of_ns(time::now_ns() - time::DAY_SECS * time::NS);
        let old_key = format!("node-a/{yesterday}");
        fs::rename(s.data_path(&key), s.data_path(&old_key)).unwrap();
        let len = s.state.files.remove(&key).unwrap();
        s.state.files.insert(old_key.clone(), len);
        s.write_state().unwrap();
        let open_size = fs::metadata(s.data_path(&old_key)).unwrap().len();
        let store = Arc::new(Mutex::new(s));

        run(&store, 400);
        let s = store.lock().unwrap();
        assert!(s.state.files.is_empty());
        let m: Manifest =
            serde_json::from_slice(&fs::read(s.manifest_path(&old_key)).unwrap()).unwrap();
        assert_eq!(m.records, 40);
        assert_eq!(m.services["annai"], 40);
        assert_eq!(m.streams[0].stream, hex(&STREAM));
        assert_eq!((m.streams[0].first_seq, m.streams[0].last_seq), (1, 40));
        assert_eq!(m.min_ts.as_deref(), Some("2026-10-19T08:14:30.000000000Z"));
        assert_eq!(m.day, yesterday);
        assert_eq!(lines(&s, "node-a"), before);
        let joined = before.join("\n") + "\n";
        assert_eq!(m.uncompressed_bytes, joined.len() as u64);
        assert_eq!(m.content_sha256, hex(&Sha256::digest(joined.as_bytes())));
        assert!(fs::metadata(s.data_path(&old_key)).unwrap().len() < open_size);
        let metrics = s.metrics.render(0);
        assert!(metrics.contains("masu_seals_total{result=\"ok\"} 1\n"));
        drop(s);

        // Second run: nothing to seal, nothing deleted (yesterday is within 400 days).
        run(&store, 400);
        let s = store.lock().unwrap();
        assert!(
            s.metrics
                .render(0)
                .contains("masu_seals_total{result=\"ok\"} 1\n")
        );
        assert!(s.data_path(&old_key).exists());
        drop(s);

        // Retention of 0 keeps everything; 1 day deletes yesterday's data, keeps the manifest.
        run(&store, 0);
        assert!(store.lock().unwrap().data_path(&old_key).exists());
        {
            let s = store.lock().unwrap();
            let far = "node-a/2020-01-01";
            fs::copy(s.data_path(&old_key), s.data_path(far)).unwrap();
            fs::copy(s.manifest_path(&old_key), s.manifest_path(far)).unwrap();
        }
        run(&store, 1);
        let s = store.lock().unwrap();
        assert!(
            s.data_path(&old_key).exists(),
            "yesterday is 1 day old: kept"
        );
        assert!(!s.data_path("node-a/2020-01-01").exists());
        assert!(s.manifest_path("node-a/2020-01-01").exists());
        assert!(s.manifest_path(&old_key).exists());
        drop(s);

        // A line that is not JSON stops the seal of that file.
        {
            let s = store.lock().unwrap();
            let bad = "node-a/2021-01-01";
            fs::write(
                s.data_path(bad),
                zstd::encode_all(&b"{\"v\":1}\nnot json\n"[..], 3).unwrap(),
            )
            .unwrap();
            drop(s);
            let mut s = store.lock().unwrap();
            let len = fs::metadata(s.data_path(bad)).unwrap().len();
            s.state.files.insert(bad.into(), len);
            s.write_state().unwrap();
        }
        run(&store, 0);
        let s = store.lock().unwrap();
        assert!(s.state.files.contains_key("node-a/2021-01-01"));
        assert!(!s.manifest_path("node-a/2021-01-01").exists());
        assert!(
            s.metrics
                .render(0)
                .contains("masu_seals_total{result=\"error\"} 1\n")
        );
        drop(s);
        let _ = fs::remove_dir_all(root);
    }
}
