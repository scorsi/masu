//! `masu serve`: the HTTPS ingestion API (ADR 0003, 0007), `/v1/status` (ADR 0008) and the
//! loopback `/metrics` listener, over hyper and rustls (ADR 0002).

use crate::config::{Config, Identity};
use crate::metrics::Metrics;
use crate::pb::{PushRejected, PushRequest, PushResponse};
use crate::store::{Reply, Store};
use hyper::body::{Body, Bytes, Frame, Incoming, SizeHint};
use hyper::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

/// ADR 0003: at most 8 MiB compressed, 64 MiB decompressed.
pub const MAX_COMPRESSED: usize = 8 << 20;
pub const MAX_DECOMPRESSED: usize = 64 << 20;
const PROTOBUF: &str = "application/x-protobuf";

/// A one-chunk response body (no `http-body-util` in ADR 0002).
pub struct Full(Option<Bytes>);

impl From<Vec<u8>> for Full {
    fn from(v: Vec<u8>) -> Full {
        Full(Some(Bytes::from(v)))
    }
}

impl Body for Full {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.get_mut().0.take().map(|b| Ok(Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |b| b.len() as u64))
    }
}

/// Collects a request body, `Err(())` past `limit` bytes or on a transport error.
pub async fn read_body(mut body: Incoming, limit: usize) -> Result<Vec<u8>, ()> {
    let mut out = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        if let Ok(data) = frame.map_err(|_| ())?.into_data() {
            if out.len() + data.len() > limit {
                return Err(());
            }
            out.extend_from_slice(&data);
        }
    }
    Ok(out)
}

pub struct App {
    pub store: Arc<Mutex<Store>>,
    pub metrics: Arc<Metrics>,
    pub root: PathBuf,
    pub disk_reserve: u64,
    tokens: Vec<([u8; 32], Identity)>,
}

impl App {
    pub fn new(store: Arc<Mutex<Store>>, metrics: Arc<Metrics>, cfg: &Config) -> App {
        let tokens = cfg
            .tokens
            .iter()
            .map(|(hex, id)| {
                let mut hash = [0u8; 32];
                for (i, b) in hash.iter_mut().enumerate() {
                    *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
                }
                (hash, id.clone())
            })
            .collect();
        App {
            store,
            metrics,
            root: cfg.data_dir.clone(),
            disk_reserve: cfg.disk_reserve,
            tokens,
        }
    }

    /// The identity of `Authorization: Bearer <token>`: its SHA-256 compared in constant time
    /// with every configured hash (ADR 0007).
    fn identity(&self, req: &Request<Incoming>) -> Option<Identity> {
        let token = req
            .headers()
            .get(AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?;
        let digest = Sha256::digest(token.as_bytes());
        let mut found = None;
        for (hash, id) in &self.tokens {
            let diff = hash
                .iter()
                .zip(digest.iter())
                .fold(0u8, |d, (a, b)| d | (a ^ b));
            if diff == 0 {
                found = Some(id.clone());
            }
        }
        found
    }
}

fn respond(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response<Full> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Full::from(body))
        .unwrap()
}

fn rejected(record_index: Option<u32>, reason: &str) -> Response<Full> {
    let body = PushRejected {
        record_index,
        reason: reason.into(),
    };
    respond(StatusCode::BAD_REQUEST, PROTOBUF, body.encode_to_vec())
}

pub async fn handle(app: Arc<App>, req: Request<Incoming>) -> Result<Response<Full>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/v1/push") => {
            let r = push(&app, req).await;
            app.metrics
                .inc("masu_push_requests_total", &[("code", r.status().as_str())]);
            r
        }
        (&Method::GET, "/v1/status") => status(&app, &req),
        _ => respond(StatusCode::NOT_FOUND, "text/plain", Vec::new()),
    };
    Ok(response)
}

async fn push(app: &Arc<App>, req: Request<Incoming>) -> Response<Full> {
    let Some(Identity::Node(node)) = app.identity(&req) else {
        crate::log::warn(
            "auth-failed",
            "no or bad credentials",
            serde_json::json!({"path": "/v1/push"}),
        );
        return respond(StatusCode::UNAUTHORIZED, "text/plain", Vec::new());
    };
    let header = |name| req.headers().get(name).and_then(|v| v.to_str().ok());
    if header(CONTENT_LENGTH).and_then(|v| v.parse::<usize>().ok()) > Some(MAX_COMPRESSED) {
        return respond(StatusCode::PAYLOAD_TOO_LARGE, "text/plain", Vec::new());
    }
    if !header(CONTENT_ENCODING).is_some_and(|v| v.eq_ignore_ascii_case("zstd")) {
        return rejected(None, "content-encoding");
    }
    let Ok(compressed) = read_body(req.into_body(), MAX_COMPRESSED).await else {
        return respond(StatusCode::PAYLOAD_TOO_LARGE, "text/plain", Vec::new());
    };
    let mut raw = Vec::new();
    let decoded = zstd::Decoder::new(&compressed[..])
        .and_then(|d| d.take(MAX_DECOMPRESSED as u64 + 1).read_to_end(&mut raw));
    if decoded.is_err() {
        return rejected(None, "body-zstd");
    }
    if raw.len() > MAX_DECOMPRESSED {
        return respond(StatusCode::PAYLOAD_TOO_LARGE, "text/plain", Vec::new());
    }
    let Ok(request) = PushRequest::decode(Bytes::from(raw)) else {
        return rejected(None, "body-protobuf");
    };
    let store = app.store.clone();
    let reply = tokio::task::spawn_blocking(move || store.lock().unwrap().push(&node, &request))
        .await
        .unwrap_or_else(|e| Reply::Insufficient(format!("push task: {e}")));
    match reply {
        Reply::Ok { acked_seq } => respond(
            StatusCode::OK,
            PROTOBUF,
            PushResponse { acked_seq }.encode_to_vec(),
        ),
        Reply::Rejected {
            record_index,
            reason,
        } => {
            crate::log::warn(
                "push-rejected",
                &reason,
                serde_json::json!({"record_index": record_index}),
            );
            rejected(record_index, &reason)
        }
        Reply::Insufficient(err) => {
            crate::log::error("push-failed", &err, serde_json::json!({}));
            respond(StatusCode::INSUFFICIENT_STORAGE, "text/plain", Vec::new())
        }
    }
}

/// `StatusResponse` in the protojson mapping: lowerCamelCase, 64-bit integers as strings,
/// bytes as base64 (ADR 0008).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusJson {
    streams: Vec<StreamJson>,
    agents: BTreeMap<String, AgentJson>,
    disk_free_bytes: String,
    disk_reserve_bytes: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamJson {
    node: String,
    stream_id: String,
    acked_seq: String,
    last_receive_ns: String,
    last_record_ns: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentJson {
    version: String,
    spool_bytes: String,
    spool_limit_bytes: String,
    paused: bool,
    oldest_unacked_ns: String,
}

fn status(app: &Arc<App>, req: &Request<Incoming>) -> Response<Full> {
    if app.identity(req) != Some(Identity::Reader) {
        crate::log::warn(
            "auth-failed",
            "no or bad credentials",
            serde_json::json!({"path": "/v1/status"}),
        );
        return respond(StatusCode::UNAUTHORIZED, "text/plain", Vec::new());
    }
    let s = app.store.lock().unwrap();
    let body = StatusJson {
        streams: s
            .state
            .streams
            .iter()
            .map(|(hex, st)| StreamJson {
                node: st.node.clone(),
                stream_id: base64(&unhex(hex)),
                acked_seq: st.hwm.to_string(),
                last_receive_ns: st.last_receive_ns.to_string(),
                last_record_ns: st.last_record_ns.to_string(),
            })
            .collect(),
        agents: s
            .state
            .agents
            .iter()
            .map(|(node, a)| {
                (
                    node.clone(),
                    AgentJson {
                        version: a.version.clone(),
                        spool_bytes: a.spool_bytes.to_string(),
                        spool_limit_bytes: a.spool_limit_bytes.to_string(),
                        paused: a.paused,
                        oldest_unacked_ns: a.oldest_unacked_ns.to_string(),
                    },
                )
            })
            .collect(),
        disk_free_bytes: s.disk_free().to_string(),
        disk_reserve_bytes: app.disk_reserve.to_string(),
    };
    respond(
        StatusCode::OK,
        "application/json",
        serde_json::to_vec(&body).unwrap(),
    )
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap_or(0))
        .collect()
}

/// Standard base64 with padding (RFC 4648 section 4), as protojson encodes `bytes`.
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | u32::from(b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

pub async fn metrics_handle(
    app: Arc<App>,
    req: Request<Incoming>,
) -> Result<Response<Full>, Infallible> {
    Ok(match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let free = crate::disk::free_bytes(&app.root).unwrap_or(0);
            respond(
                StatusCode::OK,
                "text/plain; version=0.0.4; charset=utf-8",
                app.metrics.render(free).into_bytes(),
            )
        }
        _ => respond(StatusCode::NOT_FOUND, "text/plain", Vec::new()),
    })
}

pub fn tls_config(cfg: &Config) -> Result<Arc<ServerConfig>, String> {
    let open = |p: &PathBuf| {
        File::open(p)
            .map(BufReader::new)
            .map_err(|e| format!("{}: {e}", p.display()))
    };
    let certs = rustls_pemfile::certs(&mut open(&cfg.tls.cert_file)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cert_file: {e}"))?;
    let key = rustls_pemfile::private_key(&mut open(&cfg.tls.key_file)?)
        .map_err(|e| format!("key_file: {e}"))?
        .ok_or("key_file: no private key")?;
    let mut tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| e.to_string())?;
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(tls))
}

/// `sd_notify(READY=1)` by hand over `NOTIFY_SOCKET` (`Type=notify`).
fn notify_ready() {
    let Ok(path) = std::env::var("NOTIFY_SOCKET") else {
        return;
    };
    let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else {
        return;
    };
    let result = match path.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        Some(name) => {
            use std::os::linux::net::SocketAddrExt;
            match std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()) {
                Ok(addr) => sock.send_to_addr(b"READY=1", &addr),
                Err(e) => Err(e),
            }
        }
        #[cfg(not(target_os = "linux"))]
        Some(_) => Err(std::io::Error::other("abstract socket")),
        None => sock.send_to(b"READY=1", &path),
    };
    if let Err(e) = result {
        crate::log::warn("sd-notify", &e.to_string(), serde_json::json!({}));
    }
}

/// Seconds until the next 00:10 UTC.
fn until_seal(now_ns: i64) -> u64 {
    let sod = (now_ns / crate::time::NS).rem_euclid(crate::time::DAY_SECS);
    let target = 10 * 60;
    (if sod < target {
        target - sod
    } else {
        crate::time::DAY_SECS - sod + target
    }) as u64
}

pub fn serve(cfg: Config) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async {
        let metrics = Arc::new(Metrics::new());
        let store = Store::open(&cfg.data_dir, cfg.disk_reserve, metrics.clone())
            .map_err(|e| format!("{}: {e}", cfg.data_dir.display()))?;
        let store = Arc::new(Mutex::new(store));
        let tls = TlsAcceptor::from(tls_config(&cfg)?);
        let app = Arc::new(App::new(store.clone(), metrics, &cfg));

        for addr in &cfg.listen {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| format!("listen {addr}: {e}"))?;
            let (tls, app) = (tls.clone(), app.clone());
            tokio::spawn(async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        continue;
                    };
                    let (tls, app) = (tls.clone(), app.clone());
                    tokio::spawn(async move {
                        let stream = match tls.accept(tcp).await {
                            Ok(s) => s,
                            Err(e) => {
                                crate::log::warn(
                                    "tls-handshake",
                                    &e.to_string(),
                                    serde_json::json!({}),
                                );
                                return;
                            }
                        };
                        let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req| handle(app.clone(), req)),
                            )
                            .await;
                    });
                }
            });
        }
        if let Some(addr) = &cfg.listen_metrics {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| format!("listen_metrics {addr}: {e}"))?;
            let app = app.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        continue;
                    };
                    let app = app.clone();
                    tokio::spawn(async move {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(
                                TokioIo::new(tcp),
                                service_fn(move |req| metrics_handle(app.clone(), req)),
                            )
                            .await;
                    });
                }
            });
        }

        let retention_days = cfg.retention_days;
        tokio::spawn(async move {
            loop {
                let store = store.clone();
                let _ =
                    tokio::task::spawn_blocking(move || crate::seal::run(&store, retention_days))
                        .await;
                tokio::time::sleep(std::time::Duration::from_secs(until_seal(
                    crate::time::now_ns(),
                )))
                .await;
            }
        });

        crate::log::info(
            "serve-start",
            serde_json::json!({"version": env!("CARGO_PKG_VERSION"), "listen": cfg.listen}),
        );
        notify_ready();
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| e.to_string())?;
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        crate::log::info("serve-stop", serde_json::json!({}));
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encodes() {
        assert_eq!(base64(&[0xff]), "/w==");
        assert_eq!(base64(b"{\"v\":1}"), "eyJ2IjoxfQ==");
        assert_eq!(base64(b"abc"), "YWJj");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(&[0xaa; 16]), "qqqqqqqqqqqqqqqqqqqqqg==");
    }

    #[test]
    fn seal_schedule() {
        let ns = |s: &str| crate::time::parse_ns(s).unwrap();
        assert_eq!(until_seal(ns("2026-10-19T00:00:00.000000000Z")), 600);
        assert_eq!(until_seal(ns("2026-10-19T00:10:00.000000000Z")), 86_400);
        assert_eq!(until_seal(ns("2026-10-19T23:59:50.000000000Z")), 610);
    }

    #[test]
    fn protojson_shape() {
        let body = StatusJson {
            streams: vec![StreamJson {
                node: "node-a".into(),
                stream_id: base64(&[0xaa; 16]),
                acked_seq: "10".into(),
                last_receive_ns: "1792397640000213904".into(),
                last_record_ns: "0".into(),
            }],
            agents: BTreeMap::new(),
            disk_free_bytes: "1".into(),
            disk_reserve_bytes: "2".into(),
        };
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"streams":[{"node":"node-a","streamId":"qqqqqqqqqqqqqqqqqqqqqg==","ackedSeq":"10","lastReceiveNs":"1792397640000213904","lastRecordNs":"0"}],"agents":{},"diskFreeBytes":"1","diskReserveBytes":"2"}"#
        );
    }
}
