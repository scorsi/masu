//! Sealing, compaction and retention (ADR 0006, docs/storage-format.md "Manifest").

use crate::store::{Store, fsync_dir, node_dirs, read_dir_sorted};
use crate::time;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

const SEAL_LEVEL: i32 = 9;
/// 8 MiB window: readable by any zstd with the default limits.
const WINDOW_LOG: u32 = 23;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
        if let Some(why) = s.broken() {
            crate::log::error(
                "seal",
                "skipped: the writer is stopped",
                serde_json::json!({"why": why}),
            );
            return;
        }
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

/// Scan, recompress to a temporary file, verify, then under the store lock: manifest, rename,
/// drop from the state. The manifest comes first: from then on the day file, original or
/// recompressed, matches it, which is how a restart recognises an interrupted seal. Pushes to
/// the day are refused for the whole seal (`Store::sealing`).
fn seal_one(store: &Arc<Mutex<Store>>, key: &str) -> io::Result<Manifest> {
    let (path, manifest_path) = {
        let mut s = store.lock().unwrap();
        s.sealing.insert(key.into());
        (s.data_path(key), s.manifest_path(key))
    };
    let result = seal_file(store, key, &path, &manifest_path);
    store.lock().unwrap().sealing.remove(key);
    result
}

fn seal_file(
    store: &Arc<Mutex<Store>>,
    key: &str,
    path: &Path,
    manifest_path: &Path,
) -> io::Result<Manifest> {
    let (node, day) = key.split_once('/').unwrap();
    let tmp = path.with_extension("zst.tmp");
    let mut scan = Scan::default();
    let mut encoder = zstd::Encoder::new(File::create(&tmp)?, SEAL_LEVEL)?;
    encoder.set_parameter(zstd::stream::raw::CParameter::WindowLog(WINDOW_LOG))?;
    let mut lines = open_day(path)?;
    let mut line = Vec::new();
    while next_line(&mut lines, &mut line)? {
        scan.line(&line)?;
        encoder.write_all(&line)?;
    }
    let out = encoder.finish()?;
    out.sync_all()?;
    let manifest = scan.manifest(node, day, time::format_ns(time::now_ns()));
    // Decompress again: the sealed bytes must scan like the original.
    if scan_file(&tmp)?.manifest(node, day, manifest.sealed_at.clone()) != manifest {
        let _ = fs::remove_file(&tmp);
        return Err(io::Error::other("recompressed file does not match"));
    }

    let mut s = store.lock().unwrap();
    let dir = path.parent().unwrap();
    let mtmp = manifest_path.with_extension("json.tmp");
    let mut f = File::create(&mtmp)?;
    f.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(&mtmp, manifest_path)?;
    fsync_dir(dir)?;
    s.step("seal-manifest")?;
    fs::rename(&tmp, path)?;
    fsync_dir(dir)?;
    s.step("seal-rename")?;
    s.state.files.remove(key);
    s.write_state()?;
    Ok(manifest)
}

/// The decompressed lines of a day file, with the archive's window limit (8 MiB, storage-format.md):
/// a frame asking for more is refused before its buffer is allocated.
pub fn open_day(path: &Path) -> io::Result<BufReader<zstd::Decoder<'static, BufReader<File>>>> {
    let mut d = zstd::Decoder::new(File::open(path)?)?;
    d.window_log_max(WINDOW_LOG)?;
    Ok(BufReader::new(d))
}

/// The next line of a decompressed day file into `line`, LF included; `false` at the end. A
/// line longer than a stored record, or a last line without LF, is an error (storage-format.md).
pub fn next_line(r: &mut impl BufRead, line: &mut Vec<u8>) -> io::Result<bool> {
    line.clear();
    let n =
        Read::take(&mut *r, crate::record::MAX_STORED_BYTES as u64 + 1).read_until(b'\n', line)?;
    if n == 0 {
        return Ok(false);
    }
    if line.last() != Some(&b'\n') {
        return Err(io::Error::other(if n > crate::record::MAX_STORED_BYTES {
            "line longer than a record (4 MiB)"
        } else {
            "last line without LF"
        }));
    }
    Ok(true)
}

/// Scans a whole day file (what its manifest records).
pub fn scan_file(path: &Path) -> io::Result<Scan> {
    let mut scan = Scan::default();
    let mut lines = open_day(path)?;
    let mut line = Vec::new();
    while next_line(&mut lines, &mut line)? {
        scan.line(&line)?;
    }
    Ok(scan)
}

/// An unsealed day file (a restored `.partial`): its length and what it holds. It must be
/// whole zstd frames of records: a frame that does not decode, or an incomplete last frame, is
/// an error and nothing is truncated. A cut frame cannot be told from a corrupt size in an
/// earlier frame pointing past the end, which would cut valid frames after it (review R2);
/// the backup copies whole frames, so a sound `.partial` never ends inside one. The file is
/// streamed, frame by frame, never read whole.
pub fn scan_open(path: &Path) -> io::Result<(u64, Scan)> {
    let len = fs::metadata(path)?.len();
    let mut file = BufReader::new(File::open(path)?);
    let mut end = 0;
    while end < len {
        let frame = zstd::Decoder::with_buffer(&mut file).and_then(|d| {
            let mut d = d.single_frame();
            d.window_log_max(WINDOW_LOG)?;
            io::copy(&mut d, &mut io::sink())
        });
        match frame {
            Ok(_) => end = file.stream_position()?,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::other(format!(
                    "incomplete frame at byte {end} of {len}: the file must end on a complete zstd frame (docs/storage-format.md, Backup and restore)"
                )));
            }
            Err(e) => {
                return Err(io::Error::other(format!(
                    "corrupt frame at byte {end} of {len}: {e}"
                )));
            }
        }
    }
    let mut d = zstd::Decoder::new(File::open(path)?.take(end))?;
    d.window_log_max(WINDOW_LOG)?;
    let mut lines = BufReader::new(d);
    let mut scan = Scan::default();
    let mut line = Vec::new();
    while next_line(&mut lines, &mut line)? {
        scan.line(&line)?;
    }
    Ok((end, scan))
}

pub fn read_manifest(path: &Path) -> io::Result<Manifest> {
    serde_json::from_slice(&fs::read(path)?).map_err(|e| io::Error::other(format!("manifest: {e}")))
}

/// The manifest, after checking that `data` holds exactly what it records.
pub fn check_sealed(data: &Path, manifest: &Path) -> io::Result<Manifest> {
    let m = read_manifest(manifest)?;
    let got = scan_file(data)?.manifest(&m.node, &m.day, m.sealed_at.clone());
    match differences(&got, &m) {
        diff if diff.is_empty() => Ok(m),
        diff => Err(io::Error::other(format!(
            "data differs from the manifest: {diff}"
        ))),
    }
}

/// The top-level manifest fields that differ, e.g. `content_sha256 <got> != manifest <want>`.
pub fn differences(got: &Manifest, want: &Manifest) -> String {
    let (got, want) = (
        serde_json::to_value(got).unwrap(),
        serde_json::to_value(want).unwrap(),
    );
    let keys: std::collections::BTreeSet<&String> = got
        .as_object()
        .unwrap()
        .keys()
        .chain(want.as_object().unwrap().keys())
        .collect();
    keys.into_iter()
        .filter(|k| got[k.as_str()] != want[k.as_str()])
        .map(|k| format!("{k} {} != manifest {}", got[k.as_str()], want[k.as_str()]))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Default)]
pub struct Scan {
    hash: Sha256,
    bytes: u64,
    records: u64,
    min_ts: Option<String>,
    max_ts: Option<String>,
    services: BTreeMap<String, u64>,
    /// Stream → (first, last) seq.
    pub streams: BTreeMap<String, (u64, u64)>,
    /// The records' `node`s (one, the file's, in a sound archive).
    pub nodes: std::collections::BTreeSet<String>,
}

impl Scan {
    /// One line, LF included; its parsed record.
    pub fn line(&mut self, line: &[u8]) -> io::Result<Value> {
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
        if let Some(node) = v["node"].as_str()
            && !self.nodes.contains(node)
        {
            self.nodes.insert(node.into());
        }
        if let Some(service) = v["service"].as_str() {
            *self.services.entry(service.into()).or_insert(0) += 1;
        }
        if let (Some(stream), Some(seq)) = (v["stream"].as_str(), v["seq"].as_u64()) {
            let e = self.streams.entry(stream.into()).or_insert((seq, seq));
            e.0 = e.0.min(seq);
            e.1 = e.1.max(seq);
        }
        Ok(v)
    }

    pub fn manifest(&self, node: &str, day: &str, sealed_at: String) -> Manifest {
        Manifest {
            v: 1,
            node: node.into(),
            day: day.into(),
            records: self.records,
            uncompressed_bytes: self.bytes,
            content_sha256: crate::store::hex(&self.hash.clone().finalize()),
            min_ts: self.min_ts.clone(),
            max_ts: self.max_ts.clone(),
            services: self.services.clone(),
            streams: self
                .streams
                .iter()
                .map(|(stream, &(first_seq, last_seq))| StreamRange {
                    stream: stream.clone(),
                    first_seq,
                    last_seq,
                })
                .collect(),
            sealed_at,
        }
    }
}

/// Deletes the data file of every sealed day older than `retention_days`; manifests stay.
fn retention(root: &Path, today: &str, retention_days: u32) -> io::Result<()> {
    let today = time::parse_day(today).unwrap();
    for node in node_dirs(root)? {
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
    use std::path::PathBuf;

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

    /// The open day of a store with records 1–40 in two frames, renamed to yesterday as a day
    /// change would leave it.
    fn yesterday_store(name: &str) -> (PathBuf, Arc<Mutex<Store>>, String, Vec<String>) {
        let root = tempdir(name);
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 20)),
            Reply::Ok { acked_seq: 20 }
        );
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 21, 20)),
            Reply::Ok { acked_seq: 40 }
        );
        let key = s.state.files.keys().next().unwrap().clone();
        let yesterday = time::day_of_ns(time::now_ns() - time::DAY_SECS * time::NS);
        let old_key = format!("node-a/{yesterday}");
        fs::rename(s.data_path(&key), s.data_path(&old_key)).unwrap();
        let len = s.state.files.remove(&key).unwrap();
        s.state.files.insert(old_key.clone(), len);
        s.write_state().unwrap();
        let before = lines(&s, "node-a");
        (root, Arc::new(Mutex::new(s)), old_key, before)
    }

    /// F01: a seal interrupted after its manifest, after the rename, or by a failed state write
    /// leaves a day that the next start recognises as sealed, byte for byte.
    #[test]
    fn interrupted_seal_recovers() {
        for fault in ["seal-manifest", "seal-rename", "state-write"] {
            let (root, store, key, before) = yesterday_store(fault);
            let original = fs::read(store.lock().unwrap().data_path(&key)).unwrap();
            store.lock().unwrap().faults = vec![fault];
            run(&store, 0);
            assert!(store.lock().unwrap().state.files.contains_key(&key) || fault == "state-write");
            drop(store);
            let s = open(&root);
            assert!(!s.state.files.contains_key(&key), "{fault}");
            assert_eq!(lines(&s, "node-a"), before, "{fault}");
            if fault == "seal-manifest" {
                assert_eq!(fs::read(s.data_path(&key)).unwrap(), original);
            }
            assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 40);
            check_sealed(&s.data_path(&key), &s.manifest_path(&key)).unwrap();
            assert!(
                crate::read::verify(&root, &mut Vec::new())
                    .unwrap()
                    .is_empty()
            );
            drop(s);
            let _ = fs::remove_dir_all(root);
        }
    }

    /// F05: a push the store clock puts on a sealed day, or on a day being sealed, gets 503
    /// and changes nothing.
    #[test]
    fn sealed_day_is_never_written() {
        let root = tempdir("sealed-day");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 5)),
            Reply::Ok { acked_seq: 5 }
        );
        let key = s.state.files.keys().next().unwrap().clone();
        s.sealing.insert(key.clone());
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 6, 5)),
            Reply::Unavailable(_)
        ));
        s.sealing.clear();
        let store = Arc::new(Mutex::new(s));
        // As if the clock had gone back onto a day sealed meanwhile.
        seal_one(&store, &key).unwrap();
        let mut s = store.lock().unwrap();
        let (data, manifest) = (
            fs::read(s.data_path(&key)).unwrap(),
            fs::read(s.manifest_path(&key)).unwrap(),
        );
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 6, 5)),
            Reply::Unavailable(_)
        ));
        assert_eq!(fs::read(s.data_path(&key)).unwrap(), data);
        assert_eq!(fs::read(s.manifest_path(&key)).unwrap(), manifest);
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 5);
        // A duplicate stores nothing: still answered.
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 5)),
            Reply::Ok { acked_seq: 5 }
        );
        drop(s);
        let _ = fs::remove_dir_all(root);
    }

    /// Restore (cross-review 2.1): the backed-up sealed days in an empty data directory give
    /// back each stream's hwm and owner; a manifest that does not match its data refuses.
    #[test]
    fn restore_rebuilds_the_streams() {
        let (root, store, key, before) = yesterday_store("restore");
        run(&store, 0);
        drop(store);
        let restored = tempdir("restored");
        for (from, to) in [
            ("FORMAT", "FORMAT"),
            (&format!("archive/{key}.ndjson.zst")[..], ""),
            (&format!("archive/{key}.manifest.json")[..], ""),
        ] {
            let to = if to.is_empty() { from } else { to };
            fs::create_dir_all(restored.join(to).parent().unwrap()).unwrap();
            fs::copy(root.join(from), restored.join(to)).unwrap();
        }
        let mut s = open(&restored);
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 40);
        assert_eq!(s.state.streams[&hex(&STREAM)].node, "node-a");
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 11, 10)),
            Reply::Ok { acked_seq: 40 }
        );
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 41, 2)),
            Reply::Ok { acked_seq: 42 }
        );
        assert_eq!(lines(&s, "node-a").len(), before.len() + 2);
        drop(s);

        // The restored data does not match its manifest: no start.
        fs::remove_file(restored.join("state.json")).unwrap();
        let data = restored.join(format!("archive/{key}.ndjson.zst"));
        fs::write(&data, zstd::encode_all(&b"{\"v\":1}\n"[..], 3).unwrap()).unwrap();
        assert!(Store::open(&restored, 0, Arc::new(crate::metrics::Metrics::new())).is_err());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(restored);
    }
}
