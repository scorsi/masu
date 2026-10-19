//! The archive writer: layout, commit and recovery of docs/storage-format.md, the response table
//! of ADR 0003. One `Store` per data directory, used under a mutex by the server.

use crate::metrics::Metrics;
use crate::pb::{AgentStatus, PushRequest};
use crate::record::{self, Envelope};
use crate::time;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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

impl From<&AgentStatus> for Agent {
    fn from(s: &AgentStatus) -> Agent {
        Agent {
            version: s.version.clone(),
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
    /// 507: free space below the reserve, or a write error (rolled back).
    Insufficient(String),
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => fs::write(&format, FORMAT)?,
            Err(e) => return Err(e),
        }
        let state = match fs::read(root.join("state.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => State {
                v: 1,
                ..State::default()
            },
            Err(e) => return Err(e),
        };
        let _ = fs::remove_file(root.join("state.json.tmp"));
        let mut store = Store {
            root: root.to_path_buf(),
            state,
            disk_reserve,
            metrics,
        };
        store.recover()?;
        Ok(store)
    }

    /// Every open file back to its committed length; a day file that is neither open nor
    /// sealed is truncated to zero (an interrupted first commit) and tracked as open.
    fn recover(&mut self) -> io::Result<()> {
        let mut seen = Vec::new();
        for node in read_dir_sorted(&self.root.join("archive"))? {
            let Some(node_name) = node.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            for file in read_dir_sorted(&node)? {
                let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let Some(day) = name.strip_suffix(".ndjson.zst") else {
                    continue;
                };
                let key = format!("{node_name}/{day}");
                let committed = match self.state.files.get(&key) {
                    Some(&len) => len,
                    None if self.manifest_path(&key).exists() => continue,
                    None => 0,
                };
                let actual = fs::metadata(&file)?.len();
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
        // An open file that disappeared (a deleted day) is forgotten.
        self.state.files.retain(|k, _| seen.contains(k));
        self.write_state()
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
            } else if env.seq != req.first_seq + i as u64 {
                "seq-mismatch"
            } else {
                envelopes.push(env);
                continue;
            };
            return rejected(Some(i as u32), reason);
        }

        let hwm = self.state.streams.get(&stream).map_or(0, |s| s.hwm);
        let n = req.records.len() as u64;
        let last_seq = req.first_seq + n.saturating_sub(1);
        if last_seq <= hwm || n == 0 {
            self.metrics.add("masu_duplicate_records_total", &[], n);
            return Reply::Ok { acked_seq: hwm };
        }
        let skip = hwm.saturating_sub(req.first_seq - 1).min(n) as usize;
        if skip > 0 {
            self.metrics
                .add("masu_duplicate_records_total", &[], skip as u64);
        }
        let free = self.disk_free();
        if free < self.disk_reserve {
            return Reply::Insufficient(format!(
                "free space {free} below the reserve {}",
                self.disk_reserve
            ));
        }

        let now = time::now_ns();
        let recv_ts = time::format_ns(now);
        let mut lines = Vec::new();
        for bytes in &req.records[skip..] {
            lines.extend_from_slice(&bytes[..bytes.len() - 1]);
            lines.extend_from_slice(format!(",\"recv_ts\":\"{recv_ts}\"}}\n").as_bytes());
        }
        let frame = match zstd::encode_all(&lines[..], OPEN_LEVEL) {
            Ok(f) => f,
            Err(e) => return Reply::Insufficient(format!("zstd: {e}")),
        };
        let key = format!("{node}/{}", time::day_of_ns(now));
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
        if let Err(e) = self.commit(&key, &frame, |state| {
            state.streams.insert(stream.clone(), next.clone());
            if let Some(status) = &req.status {
                state.agents.insert(node.into(), status.into());
            }
        }) {
            return Reply::Insufficient(format!("commit {key}: {e}"));
        }
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

    /// Steps 1–2 of storage-format.md "Commit": append and fsync, then `state.json` through a
    /// temporary file. On any error the file is back at its committed length and the state
    /// unchanged.
    fn commit(
        &mut self,
        key: &str,
        frame: &[u8],
        update: impl FnOnce(&mut State),
    ) -> io::Result<()> {
        let path = self.data_path(key);
        fs::create_dir_all(path.parent().unwrap())?;
        let committed = self.state.files.get(key).copied().unwrap_or(0);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        let result = (|| {
            file.seek(SeekFrom::Start(committed))?;
            file.write_all(frame)?;
            file.sync_data()?;
            let mut next = State {
                v: 1,
                files: self.state.files.clone(),
                streams: self.state.streams.clone(),
                agents: self.state.agents.clone(),
            };
            next.files
                .insert(key.into(), committed + frame.len() as u64);
            update(&mut next);
            let prev = std::mem::replace(&mut self.state, next);
            if let Err(e) = self.write_state() {
                self.state = prev;
                return Err(e);
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = file.set_len(committed);
            let _ = file.sync_all();
        }
        result
    }

    /// `state.json.tmp`, fsync, rename, fsync the directory.
    pub fn write_state(&self) -> io::Result<()> {
        let tmp = self.root.join("state.json.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(&self.state)?)?;
        f.sync_all()?;
        fs::rename(&tmp, self.root.join("state.json"))?;
        fsync_dir(&self.root)
    }
}

pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
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
}
