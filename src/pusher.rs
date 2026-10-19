//! Test pusher (ADR 0009, test-only binary behind the `pusher` feature): runs one scenario of
//! docs/delivery-scenarios.md, or every line of fixtures/records/records.ndjson, against a
//! running masu, and exits non-zero on the first failed expectation.
//!
//! ```text
//! masu-pusher scenario <file.json> --url https://localhost:9472 --ca <ca.pem> --token <t> \
//!   --other-token <t> --node node-a --other-node node-b --data-dir <root> \
//!   --metrics http://127.0.0.1:9473/metrics [--restart <cmd>] [--fill <cmd>] [--free <cmd>] \
//!   [--dump <file>]
//! masu-pusher records <records.ndjson> --url ... --ca ... --token <t> --node node-a
//! ```
//!
//! `--restart`, `--fill` and `--free` are run with `sh -c` for the `restart` and `disk` steps.

use hyper::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HOST};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use masu::pb::{AgentStatus, PushRejected, PushRequest, PushResponse};
use masu::server::{Full, read_body};
use masu::store::hex;
use masu::time;
use prost::Message;
use prost::bytes::Bytes;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

const STREAM_A: [u8; 16] = [0xaa; 16];
const STREAM_B: [u8; 16] = [0xbb; 16];
/// `ts` of seq 1; each seq adds 30 s, so `masu read --from/--to` can select an interval.
const TS_BASE_NS: i64 = 1_792_397_640_000_000_000;

struct Args {
    mode: String,
    file: PathBuf,
    opts: HashMap<String, String>,
}

impl Args {
    fn parse() -> Option<Args> {
        let mut it = std::env::args().skip(1);
        let mode = it.next()?;
        let file = PathBuf::from(it.next()?);
        let mut opts = HashMap::new();
        while let Some(flag) = it.next() {
            opts.insert(flag.strip_prefix("--")?.to_string(), it.next()?);
        }
        Some(Args { mode, file, opts })
    }

    fn get(&self, name: &str) -> &str {
        self.opts
            .get(name)
            .unwrap_or_else(|| panic!("--{name} is required"))
    }
}

struct Client {
    host: String,
    port: u16,
    tls: TlsConnector,
}

impl Client {
    fn new(url: &str, ca: &str) -> Client {
        let rest = url
            .strip_prefix("https://")
            .expect("--url must be https://host:port");
        let (host, port) = rest
            .rsplit_once(':')
            .expect("--url must be https://host:port");
        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(fs::File::open(ca).unwrap()))
        {
            roots.add(cert.unwrap()).unwrap();
        }
        let cfg =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        Client {
            host: host.into(),
            port: port.parse().unwrap(),
            tls: TlsConnector::from(Arc::new(cfg)),
        }
    }

    async fn push(&self, token: Option<&str>, req: &PushRequest) -> (StatusCode, Vec<u8>) {
        let body = zstd::encode_all(&req.encode_to_vec()[..], 3).unwrap();
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/push")
            .header(HOST, &self.host)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "zstd");
        if let Some(t) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {t}"));
        }
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .unwrap();
        let name = ServerName::try_from(self.host.clone()).unwrap();
        let tls = self.tls.connect(name, tcp).await.unwrap();
        send(TokioIo::new(tls), request.body(Full::from(body)).unwrap()).await
    }

    /// Waits until the store accepts connections again (after a restart).
    async fn wait_up(&self) {
        for _ in 0..300 {
            if TcpStream::connect((self.host.as_str(), self.port))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("store did not come back");
    }
}

async fn send<I>(io: TokioIo<I>, req: Request<Full>) -> (StatusCode, Vec<u8>)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    let response = sender.send_request(req).await.unwrap();
    let status = response.status();
    (
        status,
        read_body(response.into_body(), 1 << 20).await.unwrap(),
    )
}

/// `masu_seq_gaps_total` from the plain-HTTP metrics URL.
async fn gaps(url: &str) -> u64 {
    let rest = url.strip_prefix("http://").unwrap();
    let authority = rest.split('/').next().unwrap();
    let tcp = TcpStream::connect(authority).await.unwrap();
    let req = Request::builder()
        .method("GET")
        .uri(&rest[authority.len()..])
        .header(HOST, authority)
        .body(Full::from(Vec::new()))
        .unwrap();
    let (status, body) = send(TokioIo::new(tcp), req).await;
    assert_eq!(status, 200, "metrics");
    String::from_utf8(body)
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("masu_seq_gaps_total "))
        .expect("masu_seq_gaps_total")
        .parse()
        .unwrap()
}

fn record(node: &str, stream: &[u8], seq: u64, invalid: bool) -> Bytes {
    let ts = if invalid {
        "bad".to_string()
    } else {
        time::format_ns(TS_BASE_NS + seq as i64 * 30 * time::NS)
    };
    format!(
        r##"{{"v":1,"ts":"{ts}","node":"{node}","service":"annai","stream":"{}","seq":{seq},"up":true,"dur_us":812,"content_type":"text/plain; version=0.0.4","body":"# HELP annai_queries_total Queries answered.\n# TYPE annai_queries_total counter\nannai_queries_total{{proto=\"udp\"}} {}\n"}}"##,
        hex(stream),
        seq * 10
    )
    .into()
}

fn status() -> Option<AgentStatus> {
    Some(AgentStatus {
        version: "test".into(),
        spool_bytes: 1,
        spool_limit_bytes: 2,
        paused: false,
        oldest_unacked_ns: 0,
    })
}

fn sh(cmd: &str) {
    let st = Command::new("sh").arg("-c").arg(cmd).status().unwrap();
    assert!(st.success(), "`{cmd}` failed: {st}");
}

/// `"1-10,21-30"` → the set of seqs; `""` → empty.
fn ranges(spec: &str) -> HashSet<u64> {
    spec.split(',')
        .filter(|r| !r.is_empty())
        .flat_map(|r| {
            let (a, b) = r.split_once('-').unwrap_or((r, r));
            a.parse::<u64>().unwrap()..=b.parse::<u64>().unwrap()
        })
        .collect()
}

/// What the store's files hold for a node, read directly: seqs per stream, duplicates, and
/// every record checked against what was pushed (`recv_ts` spliced in, its day = file day).
fn stored(
    data_dir: &str,
    node: &str,
    pushed: &HashSet<Vec<u8>>,
) -> (BTreeMap<String, Vec<u64>>, u64) {
    let mut seqs: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut seen = HashSet::new();
    let mut duplicates = 0;
    let dir = PathBuf::from(data_dir).join("archive").join(node);
    for file in masu::store::read_dir_sorted(&dir).unwrap() {
        let name = file.file_name().unwrap().to_str().unwrap();
        let Some(day) = name.strip_suffix(".ndjson.zst") else {
            continue;
        };
        let bytes = zstd::decode_all(fs::File::open(&file).unwrap()).unwrap();
        for line in bytes.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let v: Value = serde_json::from_slice(line).unwrap();
            let recv_ts = v["recv_ts"].as_str().expect("recv_ts");
            assert!(masu::record::is_timestamp(recv_ts), "recv_ts format");
            assert_eq!(&recv_ts[..10], day, "recv_ts day is the file's day");
            let member = format!(",\"recv_ts\":\"{recv_ts}\"}}");
            let original = [
                line.strip_suffix(member.as_bytes())
                    .expect("recv_ts is the last member"),
                b"}",
            ]
            .concat();
            assert!(
                pushed.contains(&original),
                "stored record was not pushed: {}",
                String::from_utf8_lossy(line)
            );
            let stream = v["stream"].as_str().unwrap().to_string();
            let seq = v["seq"].as_u64().unwrap();
            if !seen.insert((stream.clone(), seq)) {
                duplicates += 1;
            }
            seqs.entry(stream).or_default().push(seq);
        }
    }
    (seqs, duplicates)
}

async fn scenario(args: &Args, client: &Client) {
    let text = fs::read_to_string(&args.file).unwrap();
    let scenario: Value = serde_json::from_str(&text).unwrap();
    let node = args.get("node");
    let other_node = args.opts.get("other-node").map_or("node-b", String::as_str);
    let mut pushed: HashSet<Vec<u8>> = HashSet::new();
    for (i, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
        let n = i + 1;
        if let Some(push) = step.get("push") {
            let token = match push["token"].as_str().unwrap_or("valid") {
                "valid" => Some(args.get("token")),
                "none" => None,
                "unknown" => Some("unknown-token-0000"),
                "other-node" => Some(args.get("other-token")),
                t => panic!("step {n}: token {t}"),
            };
            let req_node = match push["node"].as_str().unwrap_or("self") {
                "self" => node,
                "other" => other_node,
                x => panic!("step {n}: node {x}"),
            };
            let stream: &[u8] = match push["stream"].as_str().unwrap_or("a") {
                "a" => &STREAM_A,
                "b" => &STREAM_B,
                x => panic!("step {n}: stream {x}"),
            };
            let first_seq = push["first_seq"].as_u64().unwrap();
            let count = push["count"].as_u64().unwrap();
            let invalid_at = push["invalid_at"].as_u64();
            let records: Vec<Bytes> = (0..count)
                .map(|i| record(req_node, stream, first_seq + i, invalid_at == Some(i)))
                .collect();
            let req = PushRequest {
                node: req_node.into(),
                stream_id: Bytes::copy_from_slice(stream),
                first_seq,
                records: records.clone(),
                status: status(),
            };
            let (status, body) = client.push(token, &req).await;
            if status == 200 {
                pushed.extend(records.iter().map(|r| r.to_vec()));
                // `--dump <file>`: the accepted records, one per line, for the test's own oracle.
                if let Some(dump) = args.opts.get("dump") {
                    let mut f = fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(dump)
                        .unwrap();
                    for r in &records {
                        std::io::Write::write_all(&mut f, &[&r[..], b"\n"].concat()).unwrap();
                    }
                }
            }
            let expect = &step["expect"];
            if let Some(want) = expect["status"].as_u64() {
                assert_eq!(status.as_u16() as u64, want, "step {n}: status");
            }
            if let Some(want) = expect["acked_seq"].as_u64() {
                let resp = PushResponse::decode(&body[..]).unwrap();
                assert_eq!(resp.acked_seq, want, "step {n}: acked_seq");
            }
            if expect.get("record_index").is_some() || expect.get("reason").is_some() {
                let rejected = PushRejected::decode(&body[..]).expect("PushRejected body");
                match expect.get("record_index") {
                    Some(Value::Null) => assert_eq!(
                        rejected.record_index, None,
                        "step {n}: record_index must be absent"
                    ),
                    Some(v) => assert_eq!(
                        rejected.record_index,
                        Some(v.as_u64().unwrap() as u32),
                        "step {n}: record_index"
                    ),
                    None => {}
                }
                if let Some(reason) = expect["reason"].as_str() {
                    assert_eq!(rejected.reason, reason, "step {n}: reason");
                }
            }
            println!("step {n}: push first_seq={first_seq} count={count} -> {status}");
        } else if let Some(check) = step.get("check") {
            let (seqs, duplicates) = stored(args.get("data-dir"), node, &pushed);
            if let Some(spec) = check["stored_seqs"].as_str() {
                let got: HashSet<u64> = seqs
                    .get(&hex(&STREAM_A))
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                assert_eq!(got, ranges(spec), "step {n}: stored_seqs");
            }
            if let Some(want) = check["duplicates"].as_u64() {
                assert_eq!(duplicates, want, "step {n}: duplicates");
            }
            if let Some(want) = check["gaps"].as_u64() {
                assert_eq!(gaps(args.get("metrics")).await, want, "step {n}: gaps");
            }
            println!("step {n}: check ok ({seqs:?})");
        } else if step.get("restart").is_some() {
            sh(args.get("restart"));
            client.wait_up().await;
            println!("step {n}: restarted");
        } else if let Some(disk) = step.get("disk") {
            if disk["fill"].as_bool() == Some(true) {
                sh(args.get("fill"));
            }
            if disk["free"].as_bool() == Some(true) {
                sh(args.get("free"));
            }
            println!("step {n}: disk");
        } else {
            panic!("step {n}: unknown step {step}");
        }
    }
    println!("{}: ok", scenario["name"].as_str().unwrap_or("scenario"));
}

/// Every fixture line at index 1 of a two-record batch, after a valid record: a valid line is
/// accepted (200), an invalid one is refused with index 1 and the fixture's reason.
async fn records(args: &Args, client: &Client) {
    let text = fs::read_to_string(&args.file).unwrap();
    let node = args.get("node");
    let token = Some(args.get("token"));
    for (i, line) in text.lines().enumerate() {
        let n = i + 1;
        let fixture: Value = serde_json::from_str(line).unwrap();
        let input = fixture_input(&fixture);
        let parsed: Option<Value> = serde_json::from_slice(&input).ok();
        let stream = parsed
            .as_ref()
            .and_then(|v| v["stream"].as_str())
            .filter(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(|s| {
                (0..16)
                    .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
                    .collect::<Vec<u8>>()
            })
            .unwrap_or_else(|| STREAM_A.to_vec());
        let seq = parsed
            .as_ref()
            .and_then(|v| v["seq"].as_u64())
            .filter(|&s| s >= 1)
            .unwrap_or(2);
        let first_seq = seq - 1;
        let req = PushRequest {
            node: node.into(),
            stream_id: Bytes::from(stream.clone()),
            first_seq,
            records: vec![record(node, &stream, first_seq, false), Bytes::from(input)],
            status: None,
        };
        let (status, body) = client.push(token, &req).await;
        if fixture["valid"].as_bool().unwrap() {
            assert_eq!(
                status,
                200,
                "line {n}: valid fixture refused: {:?}",
                PushRejected::decode(&body[..])
            );
        } else {
            assert_eq!(status, 400, "line {n}");
            let rejected = PushRejected::decode(&body[..]).unwrap();
            assert_eq!(rejected.record_index, Some(1), "line {n}: record_index");
            assert_eq!(
                rejected.reason,
                fixture["reason"].as_str().unwrap(),
                "line {n}: reason"
            );
        }
    }
    println!("records: {} lines ok", text.lines().count());
}

/// The record bytes of a fixture line (fixtures/records/README.md).
fn fixture_input(line: &Value) -> Vec<u8> {
    if let Some(s) = line["input"].as_str() {
        return s.as_bytes().to_vec();
    }
    if let Some(s) = line["input_b64"].as_str() {
        return base64_decode(s);
    }
    let g = &line["generate"];
    [
        g["prefix"].as_str().unwrap(),
        &g["fill"]
            .as_str()
            .unwrap()
            .repeat(g["count"].as_u64().unwrap() as usize),
        g["suffix"].as_str().unwrap(),
    ]
    .concat()
    .into_bytes()
}

fn base64_decode(s: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    for chunk in s.as_bytes().chunks(4) {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        let mut n = 0u32;
        for &c in &chunk[..4 - pad] {
            n = n << 6 | ALPHABET.iter().position(|&a| a == c).unwrap() as u32;
        }
        n <<= 6 * pad;
        out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
    }
    out
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let Some(args) = Args::parse() else {
        eprintln!(
            "usage: masu-pusher scenario|records <file> --url <https://host:port> --ca <pem> --token <t> --node <n> ..."
        );
        return ExitCode::from(2);
    };
    let client = Client::new(args.get("url"), args.get("ca"));
    match args.mode.as_str() {
        "scenario" => scenario(&args, &client).await,
        "records" => records(&args, &client).await,
        m => panic!("unknown mode {m}"),
    }
    ExitCode::SUCCESS
}
