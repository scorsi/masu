//! The archive writer: layout, commit and recovery of docs/storage-format.md, the response table
//! of ADR 0003. One `Store` per data directory, used under a mutex by the server.

use crate::metrics::Metrics;
use crate::pb::{AgentStatus, PushRequest};
use crate::record::{self, Envelope};
use crate::time;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const FORMAT: &str = "masu-archive 1\n";
/// zstd level of the open day's frames (one per commit); the seal uses level 9.
const OPEN_LEVEL: i32 = 3;

/// `state.json`: what a restart needs to truncate the open files and resume the streams.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub v: u32,
    /// `<node>/<YYYY-MM-DD>` → committed length of the open day file.
    pub files: BTreeMap<String, u64>,
    /// Hex `stream_id` → position.
    pub streams: BTreeMap<String, StreamState>,
    /// Last `AgentStatus` per node.
    pub agents: BTreeMap<String, Agent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamState {
    pub node: String,
    pub hwm: u64,
    pub last_receive_ns: i64,
    pub last_record_ns: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Agent {
    pub version: String,
    pub spool_bytes: u64,
    pub spool_limit_bytes: u64,
    pub paused: bool,
    pub oldest_unacked_ns: i64,
}

/// `AgentStatus.version` is kept up to this many bytes.
pub const MAX_VERSION_BYTES: usize = 64;

impl From<&AgentStatus> for Agent {
    fn from(s: &AgentStatus) -> Agent {
        Agent {
            // Bounded: it is kept in state.json and copied on every commit (review F15).
            version: s.version[..s.version.floor_char_boundary(MAX_VERSION_BYTES)].into(),
            spool_bytes: s.spool_bytes,
            spool_limit_bytes: s.spool_limit_bytes,
            paused: s.paused,
            oldest_unacked_ns: s.oldest_unacked_ns,
        }
    }
}

/// The outcome of a push, as the response table of ADR 0003 names it.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    /// 200
    Ok { acked_seq: u64 },
    /// 400
    Rejected {
        record_index: Option<u32>,
        reason: String,
    },
    /// 403: the stream belongs to another node (ADR 0011); `PushRejected{reason}`, no index.
    Forbidden(String),
    /// 507: free space below the reserve, or a write error (rolled back).
    Insufficient(String),
    /// 503: the store cannot take this push now (its clock puts it on a sealed day, or a failed
    /// rollback stopped the writer); nothing changed, the agent keeps its spool and retries.
    Unavailable(String),
}

fn rejected(record_index: Option<u32>, reason: &str) -> Reply {
    Reply::Rejected {
        record_index,
        reason: reason.into(),
    }
}

pub struct Store {
    root: PathBuf,
    pub state: State,
    pub disk_reserve: u64,
    pub metrics: Arc<Metrics>,
    /// Day keys whose seal is running: no push may append to them (ADR 0006 amendment).
    pub sealing: BTreeSet<String>,
    /// Why the writer stopped (a rollback failed): every push gets 503 until a restart recovers.
    broken: Option<String>,
    /// `FORMAT`, locked for the life of the store: one writer per data directory.
    _lock: File,
    /// Test hook: the named steps fail as I/O errors (crash and fault injection).
    #[cfg(test)]
    pub faults: Vec<&'static str>,
}

impl Store {
    /// Opens (or creates) the data directory and recovers it (storage-format.md, "Commit").
    pub fn open(root: &Path, disk_reserve: u64, metrics: Arc<Metrics>) -> io::Result<Store> {
        fs::create_dir_all(root.join("archive"))?;
        let format = root.join("FORMAT");
        match fs::read_to_string(&format) {
            Ok(s) if s == FORMAT => {}
            Ok(s) => {
                return Err(io::Error::other(format!(
                    "{}: unsupported format `{}`",
                    format.display(),
                    s.trim()
                )));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut f = File::create(&format)?;
                f.write_all(FORMAT.as_bytes())?;
                f.sync_all()?;
            }
            Err(e) => return Err(e),
        }
        // `archive/` and `FORMAT` durable before anything is written below them.
        fsync_dir(root)?;
        let lock = File::open(&format)?;
        lock.try_lock().map_err(|_| {
            io::Error::other(format!(
                "{}: another masu writer holds this data directory",
                root.display()
            ))
        })?;
        let state = match fs::read(root.join("state.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => rebuild(root)?,
            Err(e) => return Err(e),
        };
        let _ = fs::remove_file(root.join("state.json.tmp"));
        let mut store = Store {
            root: root.to_path_buf(),
            state,
            disk_reserve,
            metrics,
            sealing: BTreeSet::new(),
            broken: None,
            _lock: lock,
            #[cfg(test)]
            faults: Vec::new(),
        };
        store.recover()?;
        store.publish_streams();
        Ok(store)
    }

    /// `masu_streams{source_node}`: no cap on streams, the count is the signal (owner 2026-10-22).
    fn publish_streams(&self) {
        let mut per_node: BTreeMap<&str, u64> = BTreeMap::new();
        for s in self.state.streams.values() {
            *per_node.entry(&s.node).or_default() += 1;
        }
        for (node, n) in per_node {
            self.metrics
                .set("masu_streams", &[("source_node", node)], n);
        }
    }

    /// Every open file back to its committed length; a day file that is neither open nor
    /// sealed is truncated to zero (an interrupted first commit) and tracked as open. An open
    /// file shorter than its committed length, or missing, refuses the start: acknowledged
    /// records are gone and the `hwm` would acknowledge them again.
    fn recover(&mut self) -> io::Result<()> {
        let mut seen = Vec::new();
        for node in node_dirs(&self.root)? {
            let Some(node_name) = node.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            for file in read_dir_sorted(&node)? {
                let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let Some(day) = name.strip_suffix(".ndjson.zst") else {
                    continue;
                };
                let key = format!("{node_name}/{day}");
                let manifest = self.manifest_path(&key);
                let committed = match self.state.files.get(&key) {
                    // A seal interrupted after its manifest: the file, original or recompressed,
                    // must hold exactly what the manifest says; it is sealed, never resized.
                    Some(_) if manifest.exists() => {
                        crate::seal::check_sealed(&file, &manifest).map_err(|e| {
                            io::Error::other(format!("{key}: interrupted seal: {e}"))
                        })?;
                        crate::log::info("recover-seal", serde_json::json!({"file": key}));
                        continue;
                    }
                    Some(&len) => len,
                    None if manifest.exists() => continue,
                    None => 0,
                };
                let actual = fs::metadata(&file)?.len();
                if actual < committed {
                    return Err(io::Error::other(format!(
                        "{key}: {actual} bytes but {committed} committed: acknowledged records are missing"
                    )));
                }
                if actual != committed {
                    crate::log::warn(
                        "recover-truncate",
                        "uncommitted bytes after the last commit",
                        serde_json::json!({"file": key, "from": actual, "to": committed}),
                    );
                    let f = OpenOptions::new().write(true).open(&file)?;
                    f.set_len(committed)?;
                    f.sync_all()?;
                }
                self.state.files.insert(key.clone(), committed);
                seen.push(key);
            }
        }
        for (key, &len) in &self.state.files {
            if len > 0 && !seen.contains(key) && !self.manifest_path(key).exists() {
                return Err(io::Error::other(format!(
                    "{key}: committed file missing: acknowledged records are gone"
                )));
            }
        }
        // Empty or sealed keys left over are forgotten.
        self.state.files.retain(|k, _| seen.contains(k));
        self.write_state()
    }

    /// Why the writer stopped, if it did (seal and push skip a broken store).
    pub fn broken(&self) -> Option<&str> {
        self.broken.as_deref()
    }

    /// Test hook: `Err` when the step was asked to fail.
    pub(crate) fn step(&self, _name: &str) -> io::Result<()> {
        #[cfg(test)]
        if self.faults.contains(&_name) {
            return Err(io::Error::other(format!("injected fault: {_name}")));
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn data_path(&self, key: &str) -> PathBuf {
        self.root.join("archive").join(format!("{key}.ndjson.zst"))
    }

    pub fn manifest_path(&self, key: &str) -> PathBuf {
        self.root
            .join("archive")
            .join(format!("{key}.manifest.json"))
    }

    pub fn disk_free(&self) -> u64 {
        crate::disk::free_bytes(&self.root).unwrap_or(0)
    }

    /// The response table of ADR 0003 for one request of the authenticated `node`.
    pub fn push(&mut self, node: &str, req: &PushRequest) -> Reply {
        if let Some(why) = &self.broken {
            return Reply::Unavailable(format!("writer stopped: {why}"));
        }
        if req.node != node {
            return rejected(None, "node-mismatch");
        }
        if req.stream_id.len() != 16 {
            return rejected(None, "stream-id-format");
        }
        if req.first_seq == 0 {
            return rejected(None, "first-seq-zero");
        }
        let stream = hex(&req.stream_id);
        // A stream belongs to the node that first pushed it (ADR 0011).
        if self
            .state
            .streams
            .get(&stream)
            .is_some_and(|s| s.node != node)
        {
            return Reply::Forbidden("stream-owner".into());
        }
        let mut envelopes: Vec<Envelope> = Vec::with_capacity(req.records.len());
        for (i, bytes) in req.records.iter().enumerate() {
            let env = match record::check(bytes) {
                Ok(env) => env,
                Err(reason) => return rejected(Some(i as u32), &reason),
            };
            let reason = if env.node != req.node {
                "node-mismatch"
            } else if env.stream != stream {
                "stream-mismatch"
            } else if req.first_seq.checked_add(i as u64) != Some(env.seq) {
                // `first_seq + i` past u64::MAX is no record's seq.
                "seq-mismatch"
            } else {
                envelopes.push(env);
                continue;
            };
            return rejected(Some(i as u32), reason);
        }

        let hwm = self.state.streams.get(&stream).map_or(0, |s| s.hwm);
        let n = req.records.len() as u64;
        // Every record matched `first_seq + i`: no overflow.
        let last_seq = req.first_seq + n.saturating_sub(1);
        if last_seq <= hwm || n == 0 {
            self.metrics.add("masu_duplicate_records_total", &[], n);
            return Reply::Ok { acked_seq: hwm };
        }
        let skip = hwm.saturating_sub(req.first_seq - 1).min(n) as usize;
        let now = time::now_ns();
        let key = format!("{node}/{}", time::day_of_ns(now));
        // A sealed day never changes: a store clock behind it refuses until it is right again.
        if self.sealing.contains(&key) || self.manifest_path(&key).exists() {
            return Reply::Unavailable(format!(
                "{key} is sealed: the store clock is behind its sealed days"
            ));
        }
        let free = self.disk_free();
        if free < self.disk_reserve {
            return Reply::Insufficient(format!(
                "free space {free} below the reserve {}",
                self.disk_reserve
            ));
        }
        if skip > 0 {
            self.metrics
                .add("masu_duplicate_records_total", &[], skip as u64);
        }

        let member = format!(",\"recv_ts\":\"{}\"}}\n", time::format_ns(now));
        let records = &req.records[skip..];
        // One zstd frame per commit, streamed to the file: no second copy of the batch.
        let write = |file: &mut File| -> io::Result<()> {
            let mut enc = zstd::Encoder::new(file, OPEN_LEVEL)?;
            for bytes in records {
                enc.write_all(&bytes[..bytes.len() - 1])?;
                enc.write_all(member.as_bytes())?;
            }
            enc.finish().map(drop)
        };
        let last_record_ns = envelopes[skip..].iter().map(|e| e.ts_ns).max().unwrap_or(0);
        let stored = (n as usize - skip) as u64;
        let mut next = StreamState {
            node: node.into(),
            hwm: last_seq,
            last_receive_ns: now,
            last_record_ns,
        };
        if let Some(prev) = self.state.streams.get(&stream) {
            next.last_record_ns = next.last_record_ns.max(prev.last_record_ns);
        }
        if let Err(e) = self.commit(&key, write, |state| {
            state.streams.insert(stream.clone(), next.clone());
            if let Some(status) = &req.status {
                state.agents.insert(node.into(), status.into());
            }
        }) {
            return Reply::Insufficient(format!("commit {key}: {e}"));
        }
        self.publish_streams();
        self.metrics
            .add("masu_records_total", &[("source_node", node)], stored);
        if req.first_seq > hwm + 1 {
            self.metrics.inc("masu_seq_gaps_total", &[]);
            crate::log::warn(
                "seq-gap",
                &format!("first_seq {} > hwm {hwm} + 1", req.first_seq),
                serde_json::json!({"gap_stream": stream, "from_seq": hwm + 1, "to_seq": req.first_seq - 1}),
            );
        }
        Reply::Ok {
            acked_seq: last_seq,
        }
    }

    /// Steps 1–2 of storage-format.md "Commit": append and fsync (with the directories of a new
    /// file), then `state.json` through a temporary file. An error before the new state is
    /// renamed in leaves the file at its committed length and the state unchanged; after the
    /// rename the new state may already be on disk, so its data stays. A failed rollback stops
    /// the writer.
    fn commit(
        &mut self,
        key: &str,
        write: impl FnOnce(&mut File) -> io::Result<()>,
        update: impl FnOnce(&mut State),
    ) -> io::Result<()> {
        let path = self.data_path(key);
        let dir = path.parent().unwrap().to_path_buf();
        fs::create_dir_all(&dir)?;
        let committed = self.state.files.get(key).copied();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        let mut published = false;
        let result = (|| {
            if committed.is_none() {
                // The new file's name, and a new node directory's, durable before the 200.
                fsync_dir(&dir)?;
                fsync_dir(&self.root.join("archive"))?;
            }
            file.seek(SeekFrom::Start(committed.unwrap_or(0)))?;
            write(&mut file)?;
            let len = file.stream_position()?;
            file.sync_data()?;
            self.step("append")?;
            let mut next = State {
                v: 1,
                files: self.state.files.clone(),
                streams: self.state.streams.clone(),
                agents: self.state.agents.clone(),
            };
            next.files.insert(key.into(), len);
            update(&mut next);
            let prev = std::mem::replace(&mut self.state, next);
            self.publish_state().map_err(|(e, renamed)| {
                published = renamed;
                if !renamed {
                    self.state = prev;
                }
                e
            })
        })();
        if result.is_err() && !published {
            let rollback = self
                .step("rollback")
                .and_then(|()| file.set_len(committed.unwrap_or(0)))
                .and_then(|()| file.sync_all());
            if let Err(e) = rollback {
                let why = format!("rollback of {key} failed: {e}");
                crate::log::error("writer-stopped", &why, serde_json::json!({}));
                self.broken = Some(why);
            }
        }
        result
    }

    /// `state.json.tmp`, fsync, rename, fsync the directory.
    pub fn write_state(&self) -> io::Result<()> {
        self.publish_state().map_err(|(e, _)| e)
    }

    /// [`Store::write_state`]; the flag tells whether the rename happened (the new state may be
    /// on disk).
    fn publish_state(&self) -> Result<(), (io::Error, bool)> {
        let tmp = self.root.join("state.json.tmp");
        let before = |e| (e, false);
        let mut f = File::create(&tmp).map_err(before)?;
        f.write_all(&serde_json::to_vec(&self.state).map_err(|e| before(e.into()))?)
            .map_err(before)?;
        f.sync_all().map_err(before)?;
        self.step("state-write").map_err(before)?;
        fs::rename(&tmp, self.root.join("state.json")).map_err(before)?;
        let after = |e| (e, true);
        self.step("state-fsync-dir").map_err(after)?;
        fsync_dir(&self.root).map_err(after)
    }
}

/// The state of a data directory without `state.json` (a restore, or a first start): each
/// stream's `hwm` and owner from the manifests of the sealed days, each checked against its
/// data file when that file is still there, and from the complete frames of every unsealed day
/// file (a restored `.partial`), which stays open at that length.
fn rebuild(root: &Path) -> io::Result<State> {
    let mut state = State {
        v: 1,
        ..State::default()
    };
    let (mut sealed, mut open) = (0, 0);
    for node in read_dir_sorted(&root.join("archive"))? {
        let Some(node_name) = node.file_name().and_then(|n| n.to_str()).map(String::from) else {
            continue;
        };
        for file in read_dir_sorted(&node)? {
            let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let fail =
                |day: &str, e: io::Error| io::Error::other(format!("{node_name}/{day}: {e}"));
            let ranges: Vec<(String, u64)> = if let Some(day) = name.strip_suffix(".manifest.json")
            {
                let data = node.join(format!("{day}.ndjson.zst"));
                let m = if data.exists() {
                    crate::seal::check_sealed(&data, &file)
                } else {
                    crate::seal::read_manifest(&file)
                }
                .map_err(|e| fail(day, e))?;
                sealed += 1;
                m.streams
                    .into_iter()
                    .map(|r| (r.stream, r.last_seq))
                    .collect()
            } else if let Some(day) = name.strip_suffix(".ndjson.zst")
                && !node.join(format!("{day}.manifest.json")).exists()
            {
                let (len, scan) = crate::seal::scan_open(&file).map_err(|e| fail(day, e))?;
                state.files.insert(format!("{node_name}/{day}"), len);
                open += 1;
                scan.streams
                    .into_iter()
                    .map(|(s, (_, last))| (s, last))
                    .collect()
            } else {
                continue;
            };
            for (stream, last_seq) in ranges {
                let s = state.streams.entry(stream).or_insert(StreamState {
                    node: node_name.clone(),
                    hwm: 0,
                    last_receive_ns: 0,
                    last_record_ns: 0,
                });
                s.hwm = s.hwm.max(last_seq);
            }
        }
    }
    if sealed + open > 0 {
        crate::log::info(
            "state-rebuilt",
            serde_json::json!({"sealed_days": sealed, "open_days": open, "streams": state.streams.len()}),
        );
    }
    Ok(state)
}

pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// The `archive/<node>` directories; a stray file there (backup marker, `*.tmp`) is not a node.
pub fn node_dirs(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut v = read_dir_sorted(&root.join("archive"))?;
    v.retain(|p| p.is_dir());
    Ok(v)
}

pub fn read_dir_sorted(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(it) => it.map(|e| e.map(|e| e.path())).collect::<io::Result<_>>()?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    v.sort();
    Ok(v)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use prost::bytes::Bytes;

    pub const STREAM: [u8; 16] = [0xaa; 16];

    pub fn record(node: &str, stream: &[u8], seq: u64) -> Bytes {
        let ts = time::format_ns(1_792_397_640_000_000_000 + seq as i64 * 30 * time::NS);
        format!(
            r##"{{"v":1,"ts":"{ts}","node":"{node}","service":"annai","stream":"{}","seq":{seq},"up":true,"dur_us":812,"content_type":"text/plain; version=0.0.4","body":"# TYPE annai_queries_total counter\nannai_queries_total{{proto=\"udp\"}} {}\n"}}"##,
            hex(stream),
            seq * 10
        )
        .into()
    }

    pub fn request(node: &str, stream: &[u8], first_seq: u64, count: u64) -> PushRequest {
        PushRequest {
            node: node.into(),
            stream_id: Bytes::copy_from_slice(stream),
            first_seq,
            records: (0..count)
                .map(|i| record(node, stream, first_seq + i))
                .collect(),
            status: Some(AgentStatus {
                version: "0.1.0".into(),
                spool_bytes: 10,
                spool_limit_bytes: 100,
                paused: false,
                oldest_unacked_ns: 0,
            }),
        }
    }

    pub fn tempdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("masu-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub fn open(root: &Path) -> Store {
        Store::open(root, 0, Arc::new(Metrics::new())).unwrap()
    }

    /// The stored records of a node, decompressed, as lines.
    pub fn lines(store: &Store, node: &str) -> Vec<String> {
        let mut out = Vec::new();
        for f in read_dir_sorted(&store.root.join("archive").join(node)).unwrap() {
            if f.extension().is_some_and(|e| e == "zst") {
                let bytes = zstd::decode_all(File::open(f).unwrap()).unwrap();
                out.extend(
                    String::from_utf8(bytes)
                        .unwrap()
                        .lines()
                        .map(str::to_string),
                );
            }
        }
        out
    }

    fn seqs(store: &Store, node: &str) -> Vec<u64> {
        lines(store, node)
            .iter()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["seq"]
                    .as_u64()
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn response_table() {
        let root = tempdir("table");
        let mut s = open(&root);
        let ok = |acked_seq| Reply::Ok { acked_seq };
        assert_eq!(s.push("node-a", &request("node-a", &STREAM, 1, 10)), ok(10));
        // Exact duplicate: nothing stored, acked unchanged.
        assert_eq!(s.push("node-a", &request("node-a", &STREAM, 1, 10)), ok(10));
        // Overlap: only 11–15 stored.
        assert_eq!(s.push("node-a", &request("node-a", &STREAM, 6, 10)), ok(15));
        // Gap: stored, counted.
        assert_eq!(s.push("node-a", &request("node-a", &STREAM, 21, 5)), ok(25));
        assert_eq!(
            seqs(&s, "node-a"),
            (1..=15).chain(21..=25).collect::<Vec<_>>()
        );
        let metrics = s.metrics.render(0);
        assert!(metrics.contains("masu_seq_gaps_total 1\n"), "{metrics}");
        assert!(metrics.contains("masu_duplicate_records_total 15\n"));
        assert!(metrics.contains("masu_records_total{source_node=\"node-a\"} 20\n"));
        // A second stream has its own hwm.
        assert_eq!(
            s.push("node-a", &request("node-a", &[0xbb; 16], 1, 5)),
            ok(5)
        );

        // Request-level 400s, in order, without index.
        assert_eq!(
            s.push("node-b", &request("node-a", &STREAM, 1, 1)),
            rejected(None, "node-mismatch")
        );
        assert_eq!(
            s.push("node-a", &request("node-a", &[1, 2, 3], 1, 1)),
            rejected(None, "stream-id-format")
        );
        let mut req = request("node-a", &STREAM, 0, 1);
        req.records = vec![record("node-a", &STREAM, 1)];
        assert_eq!(s.push("node-a", &req), rejected(None, "first-seq-zero"));

        // Record-level 400s with the first invalid index; nothing of the batch stored.
        let mut req = request("node-a", &STREAM, 26, 5);
        req.records[3] = Bytes::from_static(b"{\"v\":1}");
        assert_eq!(
            s.push("node-a", &req),
            rejected(Some(3), "missing-field:ts")
        );
        let mut req = request("node-a", &STREAM, 26, 5);
        req.records[2] = record("node-b", &STREAM, 28);
        assert_eq!(s.push("node-a", &req), rejected(Some(2), "node-mismatch"));
        let mut req = request("node-a", &STREAM, 26, 5);
        req.records[1] = record("node-a", &[0xbb; 16], 27);
        assert_eq!(s.push("node-a", &req), rejected(Some(1), "stream-mismatch"));
        let mut req = request("node-a", &STREAM, 26, 5);
        req.records[4] = record("node-a", &STREAM, 31);
        assert_eq!(s.push("node-a", &req), rejected(Some(4), "seq-mismatch"));
        // A record's own rules come before its request match.
        let mut req = request("node-a", &STREAM, 26, 5);
        req.records[0] = record("node-b", &STREAM, 99);
        req.records[1] = Bytes::from_static(b"[]");
        assert_eq!(s.push("node-a", &req), rejected(Some(0), "node-mismatch"));
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 25);
        assert_eq!(seqs(&s, "node-a").len(), 25);

        // Disk reserve: 507, nothing stored.
        s.disk_reserve = u64::MAX;
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 26, 5)),
            Reply::Insufficient(_)
        ));
        s.disk_reserve = 0;
        assert_eq!(s.push("node-a", &request("node-a", &STREAM, 26, 5)), ok(30));
        assert_eq!(s.state.agents["node-a"].spool_bytes, 10);

        // Stored = pushed bytes with recv_ts spliced before the final brace, whose day is the file's day.
        let pushed = record("node-a", &STREAM, 30);
        let line = lines(&s, "node-a").pop().unwrap();
        let (head, tail) = line.split_at(pushed.len() - 1);
        assert_eq!(head.as_bytes(), &pushed[..pushed.len() - 1]);
        assert!(tail.starts_with(",\"recv_ts\":\"") && tail.ends_with("\"}"));
        let day = &tail[12..22];
        assert!(s.state.files.contains_key(&format!("node-a/{day}")));
        assert_eq!(tail.len(), record::RECV_TS_MEMBER_BYTES + 1);
        let _ = fs::remove_dir_all(root);
    }

    /// A crash after each step of the commit: the next open recovers to the last commit.
    #[test]
    fn commit_and_recovery() {
        let root = tempdir("recovery");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 3)),
            Reply::Ok { acked_seq: 3 }
        );
        let key = s.state.files.keys().next().unwrap().clone();
        let path = s.data_path(&key);
        let committed = fs::metadata(&path).unwrap().len();
        drop(s);

        // Crash during step 1: bytes appended, state not written.
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"garbage frame")
            .unwrap();
        let s = open(&root);
        assert_eq!(fs::metadata(&path).unwrap().len(), committed);
        assert_eq!(seqs(&s, "node-a"), vec![1, 2, 3]);
        drop(s);

        // Crash during step 2: a temporary state file is left; it is ignored and removed.
        fs::write(root.join("state.json.tmp"), b"{").unwrap();
        let s = open(&root);
        assert!(!root.join("state.json.tmp").exists());
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 3);
        drop(s);

        // A day file the state never saw (first commit interrupted) is truncated to zero.
        let other = root.join("archive/node-b/2026-10-18.ndjson.zst");
        fs::create_dir_all(other.parent().unwrap()).unwrap();
        fs::write(&other, b"partial").unwrap();
        let s = open(&root);
        assert_eq!(fs::metadata(&other).unwrap().len(), 0);
        assert_eq!(s.state.files["node-b/2026-10-18"], 0);
        drop(s);

        // After recovery the stream resumes exactly where it was acked.
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 2, 4)),
            Reply::Ok { acked_seq: 5 }
        );
        assert_eq!(seqs(&s, "node-a"), vec![1, 2, 3, 4, 5]);
        let _ = fs::remove_dir_all(root);
    }

    /// F02: an error before the state rename rolls back; after it, the data stays; a failed
    /// rollback stops the writer until a restart recovers.
    #[test]
    fn commit_faults() {
        let root = tempdir("faults");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 3)),
            Reply::Ok { acked_seq: 3 }
        );
        let key = s.state.files.keys().next().unwrap().clone();
        let path = s.data_path(&key);
        let len = |p: &Path| fs::metadata(p).unwrap().len();
        let committed = len(&path);

        s.faults = vec!["state-write"];
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 4, 3)),
            Reply::Insufficient(_)
        ));
        assert_eq!(len(&path), committed);
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 3);

        // The state is renamed in, its directory fsync fails: data and new state are kept.
        s.faults = vec!["state-fsync-dir"];
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 4, 3)),
            Reply::Insufficient(_)
        ));
        assert!(len(&path) > committed);
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 6);
        drop(s);
        let mut s = open(&root);
        // The agent retries the batch it got no 200 for: acknowledged, stored once.
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 4, 3)),
            Reply::Ok { acked_seq: 6 }
        );
        assert_eq!(seqs(&s, "node-a"), (1..=6).collect::<Vec<_>>());

        // A failed rollback stops the writer: 503 until a restart.
        let committed = len(&path);
        s.faults = vec!["append", "rollback"];
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 7, 3)),
            Reply::Insufficient(_)
        ));
        assert!(s.broken().is_some());
        s.faults.clear();
        assert!(matches!(
            s.push("node-a", &request("node-a", &STREAM, 7, 3)),
            Reply::Unavailable(_)
        ));
        drop(s);
        let mut s = open(&root);
        assert_eq!(len(&path), committed);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 7, 3)),
            Reply::Ok { acked_seq: 9 }
        );
        assert_eq!(seqs(&s, "node-a"), (1..=9).collect::<Vec<_>>());
        let _ = fs::remove_dir_all(root);
    }

    /// F04: a committed file shorter than its committed length, or gone, refuses the start
    /// and changes nothing. F11: one writer per data directory.
    #[test]
    fn lost_acknowledged_data_refuses_start() {
        let root = tempdir("lost");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 3)),
            Reply::Ok { acked_seq: 3 }
        );
        let err = Store::open(&root, 0, Arc::new(Metrics::new()))
            .err()
            .unwrap();
        assert!(err.to_string().contains("another masu writer"), "{err}");
        let path = s.data_path(s.state.files.keys().next().unwrap());
        drop(s);
        let full = fs::read(&path).unwrap();
        fs::write(&path, &full[..full.len() - 5]).unwrap();
        let err = Store::open(&root, 0, Arc::new(Metrics::new()))
            .err()
            .unwrap();
        assert!(err.to_string().contains("missing"), "{err}");
        assert_eq!(fs::metadata(&path).unwrap().len(), full.len() as u64 - 5);
        fs::remove_file(&path).unwrap();
        let err = Store::open(&root, 0, Arc::new(Metrics::new()))
            .err()
            .unwrap();
        assert!(err.to_string().contains("gone"), "{err}");
        fs::write(&path, &full).unwrap();
        assert_eq!(seqs(&open(&root), "node-a"), vec![1, 2, 3]);
        let _ = fs::remove_dir_all(root);
    }

    /// F10: `first_seq + i` past u64::MAX is a `seq-mismatch`, nothing stored, no panic.
    #[test]
    fn seq_overflow() {
        let root = tempdir("overflow");
        let mut s = open(&root);
        let rec = |seq: u64| -> Bytes {
            String::from_utf8(record("node-a", &STREAM, 1).to_vec())
                .unwrap()
                .replace("\"seq\":1,", &format!("\"seq\":{seq},"))
                .into()
        };
        let mut req = request("node-a", &STREAM, u64::MAX, 0);
        req.records = vec![rec(u64::MAX), rec(0), rec(1)];
        assert_eq!(s.push("node-a", &req), rejected(Some(1), "seq-mismatch"));
        req.records.truncate(1);
        assert_eq!(
            s.push("node-a", &req),
            Reply::Ok {
                acked_seq: u64::MAX
            }
        );
        assert_eq!(
            s.push("node-a", &req),
            Reply::Ok {
                acked_seq: u64::MAX
            }
        );
        let _ = fs::remove_dir_all(root);
    }

    /// ADR 0011: another node pushing an existing stream gets 403 `stream-owner`, before any
    /// record check or deduplication, also after a restart; the owner's stream is untouched.
    #[test]
    fn stream_owner() {
        let root = tempdir("owner");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 10)),
            Reply::Ok { acked_seq: 10 }
        );
        let forbidden = Reply::Forbidden("stream-owner".into());
        let mut req = request("node-b", &STREAM, 11, 10);
        assert_eq!(s.push("node-b", &req), forbidden);
        // Before the record checks.
        req.records[0] = Bytes::from_static(b"[]");
        assert_eq!(s.push("node-b", &req), forbidden);
        // After the request-level 400s.
        assert_eq!(
            s.push("node-b", &request("node-a", &STREAM, 11, 1)),
            rejected(None, "node-mismatch")
        );
        drop(s);
        let mut s = open(&root);
        assert_eq!(
            s.push("node-b", &request("node-b", &STREAM, 11, 10)),
            forbidden
        );
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 10);
        assert!(lines(&s, "node-b").is_empty());
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 11, 10)),
            Reply::Ok { acked_seq: 20 }
        );
        assert_eq!(seqs(&s, "node-a"), (1..=20).collect::<Vec<_>>());
        // node-b's own stream is its own.
        assert_eq!(
            s.push("node-b", &request("node-b", &[0xcc; 16], 1, 1)),
            Reply::Ok { acked_seq: 1 }
        );
        let _ = fs::remove_dir_all(root);
    }

    /// F15: `AgentStatus.version` is kept to 64 bytes, on a character boundary.
    #[test]
    fn agent_version_is_bounded() {
        let status = AgentStatus {
            version: "é".repeat(100),
            ..Default::default()
        };
        assert_eq!(Agent::from(&status).version, "é".repeat(32));
    }

    /// Restore of an open day backed up as `.partial` (its committed prefix, maybe cut inside
    /// a frame): the complete frames rebuild the streams and stay open; the cut tail goes.
    #[test]
    fn restore_partial_open_day() {
        let root = tempdir("partial");
        let mut s = open(&root);
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 1, 5)),
            Reply::Ok { acked_seq: 5 }
        );
        let complete = fs::metadata(s.data_path(s.state.files.keys().next().unwrap()))
            .unwrap()
            .len();
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 6, 5)),
            Reply::Ok { acked_seq: 10 }
        );
        let key = s.state.files.keys().next().unwrap().clone();
        let data = fs::read(s.data_path(&key)).unwrap();
        drop(s);
        let restored = tempdir("partial-restored");
        fs::copy(root.join("FORMAT"), restored.join("FORMAT")).unwrap();
        let path = restored.join(format!("archive/{key}.ndjson.zst"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // Copied while the second frame was being written: half of it.
        let cut = complete as usize + (data.len() - complete as usize) / 2;
        fs::write(&path, &data[..cut]).unwrap();
        let mut s = open(&restored);
        assert_eq!(s.state.files[&key], complete);
        assert_eq!(fs::metadata(&path).unwrap().len(), complete);
        assert_eq!(s.state.streams[&hex(&STREAM)].hwm, 5);
        assert_eq!(s.state.streams[&hex(&STREAM)].node, "node-a");
        assert_eq!(
            s.push("node-a", &request("node-a", &STREAM, 3, 8)),
            Reply::Ok { acked_seq: 10 }
        );
        assert_eq!(seqs(&s, "node-a"), (1..=10).collect::<Vec<_>>());
        assert_eq!(
            s.push("node-b", &request("node-b", &STREAM, 11, 1)),
            Reply::Forbidden("stream-owner".into())
        );
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(restored);
    }
}
