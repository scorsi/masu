//! `masu read` and `masu verify` (ADR 0008): read the archive files directly, no server.

use crate::exposition::{self, Format};
use crate::seal::Manifest;
use crate::store::{hex, read_dir_sorted};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

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
pub fn read(root: &Path, q: &Query, out: &mut impl Write) -> io::Result<()> {
    let dir = root.join("archive").join(&q.node);
    let mut rows: Vec<(String, String)> = Vec::new();
    for file in read_dir_sorted(&dir)? {
        let Some(day) = data_day(&file) else {
            continue;
        };
        // A manifest's ts range prunes the day; an open day is always read.
        if let Ok(bytes) = fs::read(dir.join(format!("{day}.manifest.json")))
            && let Ok(m) = serde_json::from_slice::<Manifest>(&bytes)
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
        for line in lines(&file)? {
            let line = line?;
            let v: Value = match serde_json::from_slice(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let ts = v["ts"].as_str().unwrap_or("");
            if q.from.as_deref().is_some_and(|f| ts < f) || q.to.as_deref().is_some_and(|t| ts > t)
            {
                continue;
            }
            let service = v["service"].as_str().unwrap_or("");
            if q.service.as_deref().is_some_and(|s| s != service) {
                continue;
            }
            if q.raw {
                rows.push((ts.into(), String::from_utf8_lossy(&line).trim_end().into()));
                continue;
            }
            let head = format!("{ts} {} {service}", q.node);
            if v["up"] != Value::Bool(true) {
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

fn data_day(file: &Path) -> Option<&str> {
    file.file_name()?.to_str()?.strip_suffix(".ndjson.zst")
}

fn lines(file: &Path) -> io::Result<impl Iterator<Item = io::Result<Vec<u8>>>> {
    let reader = BufReader::new(zstd::Decoder::new(File::open(file)?)?);
    Ok(reader.split(b'\n'))
}

/// Checks `FORMAT`, every data file (decompression, manifest hash and count) and `(stream, seq)`
/// uniqueness over the whole archive. Returns the error lines; empty means the archive is sound.
pub fn verify(root: &Path, out: &mut impl Write) -> io::Result<Vec<String>> {
    let mut errors = Vec::new();
    if fs::read_to_string(root.join("FORMAT")).ok().as_deref() != Some(crate::store::FORMAT) {
        errors.push("FORMAT: missing or not `masu-archive 1`".to_string());
    }
    let mut seen: HashSet<(String, u64)> = HashSet::new();
    for node in read_dir_sorted(&root.join("archive"))? {
        for file in read_dir_sorted(&node)? {
            let Some(day) = data_day(&file) else {
                continue;
            };
            let name = format!("{}/{day}", node.file_name().unwrap().to_string_lossy());
            match verify_file(&file, &node.join(format!("{day}.manifest.json")), &mut seen) {
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
    manifest: &Path,
    seen: &mut HashSet<(String, u64)>,
) -> Result<u64, String> {
    let mut hash = Sha256::new();
    let mut records = 0u64;
    let mut bytes = 0u64;
    for line in lines(file).map_err(|e| e.to_string())? {
        let line = line.map_err(|e| format!("zstd: {e}"))?;
        hash.update(&line);
        hash.update(b"\n");
        bytes += line.len() as u64 + 1;
        records += 1;
        let v: Value =
            serde_json::from_slice(&line).map_err(|e| format!("record {records}: {e}"))?;
        if let (Some(stream), Some(seq)) = (v["stream"].as_str(), v["seq"].as_u64())
            && !seen.insert((stream.into(), seq))
        {
            return Err(format!(
                "record {records}: duplicate (stream {stream}, seq {seq})"
            ));
        }
    }
    if let Ok(text) = fs::read(manifest) {
        let m: Manifest = serde_json::from_slice(&text).map_err(|e| format!("manifest: {e}"))?;
        let got = hex(&hash.finalize());
        if got != m.content_sha256 {
            return Err(format!(
                "content_sha256 {got} != manifest {}",
                m.content_sha256
            ));
        }
        if records != m.records || bytes != m.uncompressed_bytes {
            return Err(format!(
                "{records} records / {bytes} bytes != manifest {} / {}",
                m.records, m.uncompressed_bytes
            ));
        }
    }
    Ok(records)
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
}
