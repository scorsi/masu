//! Read-only committed archive views. Capturing never opens the writer or repairs its state.
use super::budget::{Budget, Charge};
use crate::{
    record,
    seal::{Manifest, Scan},
    store::{FORMAT, State},
    time,
};
use serde_json::Value;
use std::{
    cell::RefCell,
    collections::BTreeSet,
    fs::{self, File},
    io::{self, BufReader, Read},
    path::Path,
    rc::Rc,
};

#[derive(Debug)]
pub enum Error {
    Budget(super::budget::Error),
    Io(io::Error),
    Corrupt(String),
    Unavailable,
    ArchiveIncomplete,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget(e) => e.fmt(f),
            Self::Io(e) => write!(f, "archive-io: {e}"),
            Self::Corrupt(e) => write!(f, "archive-corrupt: {e}"),
            Self::Unavailable => f.write_str("snapshot-unavailable"),
            Self::ArchiveIncomplete => f.write_str("archive-incomplete"),
        }
    }
}
impl std::error::Error for Error {}
impl From<super::budget::Error> for Error {
    fn from(e: super::budget::Error) -> Self {
        Self::Budget(e)
    }
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub struct Snapshot {
    pub node: String,
    pub days: Vec<Day>,
}
pub struct Day {
    pub day: String,
    pub manifest: Option<Manifest>,
    node: String,
    source: Option<(File, u64)>,
    missing: bool,
}
impl Snapshot {
    pub fn capture(root: &Path, node: &str, budget: &mut Budget) -> Result<Self, Error> {
        capture_with(root, node, budget, &mut |_| {})
    }
    /// Callers must discard accumulated results if scanning any required day fails.
    pub fn scan(
        &mut self,
        budget: &mut Budget,
        mut visit: impl FnMut(&Value, &mut Budget) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for day in &mut self.days {
            day.scan(budget, &mut visit)?;
        }
        Ok(())
    }
}
impl Day {
    /// Retention keeps the manifest; the evaluator can use its range/service index to decide
    /// which queries are incomplete without silently treating a deleted day as empty.
    pub fn is_missing(&self) -> bool {
        self.missing
    }
    /// One forward pass per pinned descriptor, including manifest hash/field verification.
    pub fn scan(
        &mut self,
        budget: &mut Budget,
        mut visit: impl FnMut(&Value, &mut Budget) -> Result<(), Error>,
    ) -> Result<(), Error> {
        budget.check()?;
        if self.missing {
            return Err(Error::ArchiveIncomplete);
        }
        let (file, length) = self.source.take().ok_or(Error::Unavailable)?;
        if length == 0 && self.manifest.is_none() {
            return Ok(());
        }
        let shared = Rc::new(RefCell::new(budget));
        let input = CountRead {
            inner: file.take(length),
            budget: shared.clone(),
            kind: Charge::Compressed,
        };
        let mut decoder = zstd::Decoder::new(input).map_err(read_error)?;
        decoder.window_log_max(23).map_err(read_error)?;
        let decoded = CountRead {
            inner: decoder,
            budget: shared.clone(),
            kind: Charge::Decompressed,
        };
        let mut reader = BufReader::new(decoded);
        let mut scan = Scan::default();
        let mut line = Vec::new();
        while crate::seal::next_line(&mut reader, &mut line).map_err(read_error)? {
            shared.borrow_mut().charge(Charge::Record, 1)?;
            let bytes = line.strip_suffix(b"\n").unwrap();
            let envelope = record::check_stored(bytes).map_err(Error::Corrupt)?;
            if envelope.node != self.node {
                return Err(Error::Corrupt("record node differs from directory".into()));
            }
            let value = scan.line(&line).map_err(read_error)?;
            if &value["recv_ts"].as_str().unwrap()[..10] != self.day {
                return Err(Error::Corrupt(
                    "record receive day differs from directory".into(),
                ));
            }
            shared.borrow().check()?;
            visit(&value, &mut shared.borrow_mut())?;
            shared.borrow().check()?;
        }
        if let Some(want) = &self.manifest {
            let got = scan.manifest(&self.node, &self.day, want.sealed_at.clone());
            let diff = crate::seal::differences(&got, want);
            if !diff.is_empty() {
                return Err(Error::Corrupt(diff));
            }
        }
        shared.borrow().check()?;
        Ok(())
    }
}

struct CountRead<'a, R> {
    inner: R,
    budget: Rc<RefCell<&'a mut Budget>>,
    kind: Charge,
}
impl<R: Read> Read for CountRead<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.budget.borrow().check().map_err(io::Error::other)?;
        let max = buf
            .len()
            .min(self.budget.borrow().remaining(self.kind).saturating_add(1) as usize);
        let n = self.inner.read(&mut buf[..max])?;
        self.budget
            .borrow_mut()
            .charge(self.kind, n as u64)
            .map_err(io::Error::other)?;
        Ok(n)
    }
}
fn read_error(e: io::Error) -> Error {
    if let Some(e) = e
        .get_ref()
        .and_then(|e| e.downcast_ref::<super::budget::Error>())
    {
        Error::Budget(*e)
    } else {
        Error::Corrupt(e.to_string())
    }
}

/// Reads incrementally under the aggregate metadata limit, before parsing or allocating a
/// whole metadata file. Missing manifests are normal; missing live state is not.
fn metadata(path: &Path, budget: &mut Budget) -> Result<Option<Vec<u8>>, Error> {
    budget.charge(Charge::File, 1)?;
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut out = Vec::new();
    loop {
        budget.check()?;
        let mut chunk = [0; 8192];
        let max = chunk
            .len()
            .min(budget.remaining(Charge::Metadata).saturating_add(1) as usize);
        let n = file.read(&mut chunk[..max])?;
        if n == 0 {
            break;
        }
        budget.charge(Charge::Metadata, n as u64)?;
        out.extend_from_slice(&chunk[..n]);
    }
    Ok(Some(out))
}
// serde's map types overwrite duplicate keys; reject them before interpreting metadata.
fn validate_json_keys(bytes: &[u8]) -> Result<(), Error> {
    struct UniqueKeys;
    impl<'de> serde::Deserialize<'de> for UniqueKeys {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = UniqueKeys;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("JSON without duplicate keys")
                }
                fn visit_map<M: serde::de::MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> Result<UniqueKeys, M::Error> {
                    let mut keys = BTreeSet::new();
                    while let Some(key) = map.next_key::<String>()? {
                        if !keys.insert(key) {
                            return Err(serde::de::Error::custom("duplicate metadata key"));
                        }
                        map.next_value::<UniqueKeys>()?;
                    }
                    Ok(UniqueKeys)
                }
                fn visit_seq<S: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: S,
                ) -> Result<UniqueKeys, S::Error> {
                    while seq.next_element::<UniqueKeys>()?.is_some() {}
                    Ok(UniqueKeys)
                }
                fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
                fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
                fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
                fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
                fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<UniqueKeys, E> {
                    Ok(UniqueKeys)
                }
            }
            d.deserialize_any(Visitor)
        }
    }
    let _: UniqueKeys = serde_json::from_slice(bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
    Ok(())
}
fn valid_day(day: &str) -> bool {
    day.len() == 10 && record::is_timestamp(&format!("{day}T00:00:00.000000000Z"))
}
fn validate_manifest(m: &Manifest, node: &str, day: &str) -> Result<(), Error> {
    let timestamp = |s: &str| time::parse_ns(s).is_some();
    if m.v != 1
        || m.node != node
        || m.day != day
        || !timestamp(&m.sealed_at)
        || m.content_sha256.len() != 64
        || !m
            .content_sha256
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || m.min_ts.as_deref().is_some_and(|t| !timestamp(t))
        || m.max_ts.as_deref().is_some_and(|t| !timestamp(t))
        || (m.records == 0) != (m.min_ts.is_none() && m.max_ts.is_none())
        || (m.records > 0 && (m.min_ts.is_none() || m.max_ts.is_none()))
        || m.min_ts > m.max_ts
        || m.services.values().try_fold(0u64, |a, b| a.checked_add(*b)) != Some(m.records)
    {
        return Err(Error::Corrupt("invalid manifest".into()));
    }
    let mut streams = BTreeSet::new();
    for stream in &m.streams {
        if stream.stream.len() != 32
            || !stream
                .stream
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            || stream.first_seq > stream.last_seq
            || !streams.insert(&stream.stream)
        {
            return Err(Error::Corrupt("invalid manifest stream".into()));
        }
    }
    Ok(())
}
fn capture_with(
    root: &Path,
    node: &str,
    budget: &mut Budget,
    hook: &mut impl FnMut(&str),
) -> Result<Snapshot, Error> {
    if !record::is_node(node) {
        return Err(Error::Corrupt("invalid node".into()));
    }
    if metadata(&root.join("FORMAT"), budget)?.as_deref() != Some(FORMAT.as_bytes()) {
        return Err(Error::Corrupt("missing or unsupported FORMAT".into()));
    }
    for attempt in 0..2 {
        match capture_once(root, node, budget, hook) {
            Err(Error::Unavailable) if attempt == 0 => continue,
            result => return result,
        }
    }
    unreachable!()
}
fn capture_once(
    root: &Path,
    node: &str,
    budget: &mut Budget,
    hook: &mut impl FnMut(&str),
) -> Result<Snapshot, Error> {
    let state_path = root.join("state.json");
    let before = metadata(&state_path, budget)?.ok_or(Error::Unavailable)?;
    validate_json_keys(&before)?;
    let state: State =
        serde_json::from_slice(&before).map_err(|e| Error::Corrupt(e.to_string()))?;
    if state.v != 1 {
        return Err(Error::Corrupt("unsupported state version".into()));
    }
    let mut days = BTreeSet::new();
    for key in state.files.keys() {
        let (n, day) = key
            .split_once('/')
            .ok_or_else(|| Error::Corrupt("invalid state file key".into()))?;
        if !record::is_node(n) || !valid_day(day) {
            return Err(Error::Corrupt("invalid state file key".into()));
        }
        if n == node {
            days.insert(day.to_owned());
        }
    }
    let dir = root.join("archive").join(node);
    match fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries {
                budget.directory_entry(node)?;
                let entry = entry?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| Error::Corrupt("non-UTF8 archive entry".into()))?;
                if let Some(day) = name
                    .strip_suffix(".manifest.json")
                    .or_else(|| name.strip_suffix(".ndjson.zst"))
                {
                    if !valid_day(day) {
                        return Err(Error::Corrupt("invalid archive day".into()));
                    }
                    days.insert(day.into());
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut pinned = Vec::new();
    for day in days {
        budget.check()?;
        let manifest_path = dir.join(format!("{day}.manifest.json"));
        let manifest_bytes = metadata(&manifest_path, budget)?;
        let manifest = manifest_bytes
            .as_deref()
            .map(|bytes| {
                validate_json_keys(bytes)?;
                let m: Manifest =
                    serde_json::from_slice(bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
                validate_manifest(&m, node, &day)?;
                Ok::<_, Error>(m)
            })
            .transpose()?;
        let committed = state.files.get(&format!("{node}/{day}")).copied();
        if manifest.is_none() && committed.is_none() {
            continue;
        }
        budget.charge(Charge::File, 1)?;
        hook("before-open");
        let source = match File::open(dir.join(format!("{day}.ndjson.zst"))) {
            Ok(file) => {
                let actual = file.metadata()?.len();
                let length = if manifest.is_some() {
                    actual
                } else {
                    committed.unwrap()
                };
                if actual < length {
                    return Err(Error::Unavailable);
                }
                Some((file, length))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound && manifest.is_some() => None,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::Unavailable),
            Err(e) => return Err(e.into()),
        };
        // A manifest published across this open may have replaced the file with a compacted
        // inode; restart before an old compressed length can ever be applied to it.
        hook("after-open");
        if metadata(&manifest_path, budget)? != manifest_bytes {
            return Err(Error::Unavailable);
        }
        pinned.push(Day {
            day,
            node: node.into(),
            manifest,
            missing: source.is_none(),
            source,
        });
    }
    if metadata(&state_path, budget)?.as_deref() != Some(before.as_slice()) {
        return Err(Error::Unavailable);
    }
    budget.check()?;
    Ok(Snapshot {
        node: node.into(),
        days: pinned,
    })
}

#[cfg(test)]
mod tests {
    use super::super::budget::{Error as BudgetError, Limits};
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    const NODE: &str = "node-a";
    const DAY: &str = "2026-11-01";
    struct Fixture {
        root: PathBuf,
        line: Vec<u8>,
        original: Vec<u8>,
    }
    impl Fixture {
        fn new() -> Self {
            static ID: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "masu-query-{}-{}",
                std::process::id(),
                ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(root.join("archive/node-a")).unwrap();
            fs::write(root.join("FORMAT"), FORMAT).unwrap();
            let value = serde_json::json!({"v":1,"ts":"2026-10-31T23:00:00.000000000Z",
                "recv_ts":"2026-11-01T10:00:00.000000000Z", "node":NODE,"service":"azukari",
                "stream":"11111111111111111111111111111111", "seq":1,"up":true,"dur_us":1,
                "content_type":"text/plain", "body":"# TYPE azukari_last_snapshot_timestamp_seconds gauge\nazukari_last_snapshot_timestamp_seconds 1793520000\n"});
            let line = format!("{value}\n").into_bytes();
            let original = zstd::encode_all(line.as_slice(), 3).unwrap();
            let f = Self {
                root,
                line,
                original,
            };
            fs::write(f.data(), &f.original).unwrap();
            f.state(f.original.len() as u64);
            f
        }
        fn data(&self) -> PathBuf {
            self.root.join(format!("archive/{NODE}/{DAY}.ndjson.zst"))
        }
        fn manifest_path(&self) -> PathBuf {
            self.root
                .join(format!("archive/{NODE}/{DAY}.manifest.json"))
        }
        fn state(&self, len: u64) {
            let mut state = State {
                v: 1,
                ..State::default()
            };
            state.files.insert(format!("{NODE}/{DAY}"), len);
            let tmp = self.root.join("state.new");
            fs::write(&tmp, serde_json::to_vec(&state).unwrap()).unwrap();
            fs::rename(tmp, self.root.join("state.json")).unwrap();
        }
        fn manifest(&self) -> Manifest {
            let mut s = Scan::default();
            s.line(&self.line).unwrap();
            s.manifest(NODE, DAY, "2026-11-02T00:10:00.000000000Z".into())
        }
        fn publish(&self) {
            fs::write(
                self.manifest_path(),
                serde_json::to_vec(&self.manifest()).unwrap(),
            )
            .unwrap();
        }
        fn capture(&self) -> Snapshot {
            Snapshot::capture(&self.root, NODE, &mut Budget::default()).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    fn rows(snapshot: &mut Snapshot, budget: &mut Budget) -> Result<Vec<Value>, Error> {
        let mut out = Vec::new();
        snapshot.scan(budget, |v, _| {
            out.push(v.clone());
            Ok(())
        })?;
        Ok(out)
    }
    #[test]
    fn query_snapshot_committed_prefix_ignores_complete_and_partial_uncommitted_tail() {
        let f = Fixture::new();
        let mut file = fs::OpenOptions::new().append(true).open(f.data()).unwrap();
        file.write_all(&f.original).unwrap();
        file.write_all(&[0x28, 0xb5]).unwrap();
        let before = fs::read(f.data()).unwrap();
        let mut b = Budget::default();
        let mut s = Snapshot::capture(&f.root, NODE, &mut b).unwrap();
        assert_eq!(rows(&mut s, &mut b).unwrap().len(), 1);
        assert_eq!(b.usage.records, 1);
        assert_eq!(b.usage.compressed_bytes, f.original.len() as u64);
        assert_eq!(fs::read(f.data()).unwrap(), before);
        assert!(
            Snapshot::capture(&f.root, "absent", &mut Budget::default())
                .unwrap()
                .days
                .is_empty()
        );
        fs::remove_file(f.root.join("state.json")).unwrap();
        assert!(matches!(
            Snapshot::capture(&f.root, NODE, &mut Budget::default()),
            Err(Error::Unavailable)
        ));
    }
    #[test]
    fn query_snapshot_empty_committed_and_untracked_files_are_not_visible() {
        let f = Fixture::new();
        f.state(0);
        assert!(
            rows(&mut f.capture(), &mut Budget::default())
                .unwrap()
                .is_empty()
        );
        fs::write(
            f.root.join("state.json"),
            br#"{"v":1,"files":{},"streams":{},"agents":{}}"#,
        )
        .unwrap();
        assert!(f.capture().days.is_empty());
    }
    #[test]
    fn query_snapshot_metadata_duplicate_maps_are_refused_before_use() {
        let f = Fixture::new();
        let state = format!(
            r#"{{"v":1,"files":{{"node-a/2026-11-01":0,"node-a/2026-11-01":{}}},"streams":{{}},"agents":{{}}}}"#,
            f.original.len()
        );
        fs::write(f.root.join("state.json"), state).unwrap();
        assert!(matches!(
            Snapshot::capture(&f.root, NODE, &mut Budget::default()),
            Err(Error::Corrupt(_))
        ));
        f.state(f.original.len() as u64);
        let manifest = serde_json::to_string(&f.manifest())
            .unwrap()
            .replace("\"azukari\":1", "\"azukari\":0,\"azukari\":1");
        fs::write(f.manifest_path(), manifest).unwrap();
        assert!(matches!(
            Snapshot::capture(&f.root, NODE, &mut Budget::default()),
            Err(Error::Corrupt(_))
        ));
    }
    #[test]
    fn query_snapshot_manifest_before_rename_and_replaced_inode() {
        for phase in ["before-open", "after-open"] {
            let f = Fixture::new();
            // The old compressed prefix is much longer than the new compacted representation.
            let mut original = f.original.clone();
            original.extend(&f.original);
            fs::write(f.data(), &original).unwrap();
            f.state(original.len() as u64);
            let mut both = f.line.clone();
            both.extend(&f.line);
            let mut scan = Scan::default();
            scan.line(&f.line).unwrap();
            scan.line(&f.line).unwrap();
            let manifest = scan.manifest(NODE, DAY, "2026-11-02T00:10:00.000000000Z".into());
            let mut changed = false;
            let mut hook = |at: &str| {
                if at == phase && !changed {
                    changed = true;
                    fs::write(f.manifest_path(), serde_json::to_vec(&manifest).unwrap()).unwrap();
                    let tmp = f.data().with_extension("new");
                    fs::write(&tmp, zstd::encode_all(both.as_slice(), 9).unwrap()).unwrap();
                    fs::rename(tmp, f.data()).unwrap();
                    // State deliberately still has the OLD compressed length, as during sealing.
                }
            };
            let mut b = Budget::default();
            let mut s = capture_with(&f.root, NODE, &mut b, &mut hook).unwrap();
            assert!(changed);
            assert!(s.days[0].manifest.is_some());
            assert_eq!(rows(&mut s, &mut b).unwrap().len(), 2);
            assert!(b.usage.files >= 9, "retry must not reset costs");
        }
    }
    #[test]
    fn query_snapshot_pins_sealed_descriptor_across_retention_and_reports_missing_data() {
        let f = Fixture::new();
        f.publish();
        let mut s = f.capture();
        fs::remove_file(f.data()).unwrap();
        assert_eq!(rows(&mut s, &mut Budget::default()).unwrap().len(), 1);
        assert!(!s.days[0].is_missing());
        let mut s = f.capture();
        assert!(s.days[0].is_missing());
        assert!(matches!(
            s.scan(&mut Budget::default(), |_, _| Ok(())),
            Err(Error::ArchiveIncomplete)
        ));
    }
    #[test]
    fn query_snapshot_missing_day_can_be_inspected_and_skipped_independently() {
        let f = Fixture::new();
        let mut old = f.manifest();
        old.day = "2026-10-30".into();
        old.min_ts = Some("2026-10-29T00:00:00.000000000Z".into());
        old.max_ts = old.min_ts.clone();
        fs::write(
            f.root.join("archive/node-a/2026-10-30.manifest.json"),
            serde_json::to_vec(&old).unwrap(),
        )
        .unwrap();
        let mut budget = Budget::default();
        let mut snapshot = Snapshot::capture(&f.root, NODE, &mut budget).unwrap();
        let missing = &snapshot.days[0];
        assert!(missing.is_missing());
        let manifest = missing.manifest.as_ref().unwrap();
        assert_eq!(
            manifest.max_ts.as_deref(),
            Some("2026-10-29T00:00:00.000000000Z")
        );
        assert_eq!(manifest.services["azukari"], 1);
        let mut count = 0;
        snapshot.days[1]
            .scan(&mut budget, |_, _| {
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(budget.usage.compressed_bytes, f.original.len() as u64);
    }
    #[test]
    fn query_snapshot_rejects_large_windows_lines_and_incomplete_committed_frames() {
        let f = Fixture::new();
        let mut enc = zstd::Encoder::new(Vec::new(), 1).unwrap();
        enc.set_parameter(zstd::stream::raw::CParameter::WindowLog(27))
            .unwrap();
        enc.write_all(&f.line).unwrap();
        let large_window = enc.finish().unwrap();
        let long_line =
            zstd::encode_all("x".repeat(record::MAX_STORED_BYTES + 2).as_bytes(), 1).unwrap();
        let truncated = f.original[..f.original.len() - 1].to_vec();
        for data in [large_window, long_line, truncated] {
            fs::write(f.data(), &data).unwrap();
            f.state(data.len() as u64);
            assert!(matches!(
                rows(&mut f.capture(), &mut Budget::default()),
                Err(Error::Corrupt(_))
            ));
        }
    }
    #[test]
    fn query_snapshot_never_returns_a_mixed_state_and_retries_only_once() {
        let f = Fixture::new();
        let mut flips = 0;
        let mut hook = |phase: &str| {
            if phase == "after-open" {
                flips += 1;
                f.state(if flips % 2 == 1 {
                    0
                } else {
                    f.original.len() as u64
                });
            }
        };
        let mut b = Budget::default();
        assert!(matches!(
            capture_with(&f.root, NODE, &mut b, &mut hook),
            Err(Error::Unavailable)
        ));
        assert_eq!(flips, 2);
        assert!(
            b.usage.metadata_bytes > fs::metadata(f.root.join("state.json")).unwrap().len() * 2
        );
    }
    #[test]
    fn query_snapshot_refuses_corruption_and_bad_stored_envelopes() {
        let f = Fixture::new();
        f.publish();
        let mut manifest = f.manifest();
        manifest.content_sha256 = "0".repeat(64);
        fs::write(f.manifest_path(), serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(matches!(
            rows(&mut f.capture(), &mut Budget::default()),
            Err(Error::Corrupt(_))
        ));
        fs::remove_file(f.manifest_path()).unwrap();
        for field in [
            "recv_ts", "ts", "up", "service", "stream", "seq", "dur_us", "body",
        ] {
            let mut value: Value = serde_json::from_slice(&f.line).unwrap();
            value.as_object_mut().unwrap().remove(field);
            let bytes = zstd::encode_all(format!("{value}\n").as_bytes(), 3).unwrap();
            fs::write(f.data(), &bytes).unwrap();
            f.state(bytes.len() as u64);
            assert!(
                matches!(
                    rows(&mut f.capture(), &mut Budget::default()),
                    Err(Error::Corrupt(_))
                ),
                "{field}"
            );
        }
        fs::write(f.data(), [0, 1, 2, 3]).unwrap();
        f.state(4);
        assert!(matches!(
            rows(&mut f.capture(), &mut Budget::default()),
            Err(Error::Corrupt(_))
        ));
        f.publish();
        let mut m = f.manifest();
        m.node = "node-b".into();
        fs::write(f.manifest_path(), serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(matches!(
            Snapshot::capture(&f.root, NODE, &mut Budget::default()),
            Err(Error::Corrupt(_))
        ));
    }
    #[test]
    fn query_snapshot_callback_shares_the_request_result_budget() {
        let f = Fixture::new();
        let mut bytes = f.original.clone();
        bytes.extend(&f.original);
        fs::write(f.data(), &bytes).unwrap();
        f.state(bytes.len() as u64);
        let mut budget = Budget::new(Limits {
            result_items: 1,
            ..Limits::default()
        });
        let mut snapshot = f.capture();
        let mut appended = 0;
        assert!(matches!(
            snapshot.scan(&mut budget, |_, budget| {
                budget.charge(Charge::Sample, 1)?;
                budget.charge(Charge::ResultItem, 1)?;
                appended += 1;
                Ok(())
            }),
            Err(Error::Budget(BudgetError::ResponseLimit))
        ));
        assert_eq!(appended, 1);
        assert_eq!(budget.usage.samples, 2);
        assert_eq!(budget.check(), Err(BudgetError::ResponseLimit));
    }
    #[test]
    fn query_snapshot_charges_metadata_before_parsing_and_stops_on_cancellation() {
        let f = Fixture::new();
        let mut b = Budget::new(Limits {
            metadata_bytes: 1,
            ..Limits::default()
        });
        assert!(matches!(
            Snapshot::capture(&f.root, NODE, &mut b),
            Err(Error::Budget(BudgetError::ScanLimit))
        ));
        let mut b = Budget::new(Limits {
            decompressed_bytes: 1,
            ..Limits::default()
        });
        let mut s = f.capture();
        assert!(matches!(
            s.scan(&mut b, |_, _| panic!("over-budget record emitted")),
            Err(Error::Budget(BudgetError::ScanLimit))
        ));
        let mut all = f.original.clone();
        all.extend(&f.original);
        fs::write(f.data(), &all).unwrap();
        f.state(all.len() as u64);
        let mut b = Budget::default();
        let cancelled = b.cancellation();
        let mut calls = 0;
        assert!(matches!(
            f.capture().scan(&mut b, |_, _| {
                calls += 1;
                cancelled.store(true, Ordering::Relaxed);
                Ok(())
            }),
            Err(Error::Budget(BudgetError::Cancelled))
        ));
        assert_eq!(calls, 1);
        assert_eq!(b.usage.records, 1);
    }
}
