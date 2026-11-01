//! HTTPS/1 query boundary; work slots cover collection, scan and Hyper's buffered response.
use crate::config::{Config, Identity};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Body, Bytes, Frame, Incoming, SizeHint},
    header::{
        AUTHORIZATION, CACHE_CONTROL, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
    },
};
use hyper_util::rt::{TokioIo, TokioTimer};
use masu_core::query::{
    budget::{Budget, Limits},
    document::{self, Encoding},
    evaluate,
};
use sha2::{Digest, Sha256};
use std::{
    convert::Infallible,
    fs::File,
    io::BufReader,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinHandle,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_CONNECTIONS: usize = 32;
const PROTOBUF: &str = "application/x-protobuf";
const JSON: &str = "application/json";

struct App {
    root: PathBuf,
    tokens: Vec<([u8; 32], bool)>,
    work: Arc<Semaphore>,
    request_timeout: Duration,
}
impl App {
    fn new(cfg: &Config) -> Self {
        let tokens = cfg
            .tokens
            .iter()
            .map(|(hex, id)| {
                let mut hash = [0; 32];
                for (i, byte) in hash.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
                }
                (hash, matches!(id, Identity::Reader))
            })
            .collect();
        Self {
            root: cfg.data_dir.clone(),
            tokens,
            work: Arc::new(Semaphore::new(2)),
            request_timeout: QUERY_TIMEOUT,
        }
    }
    fn identity(&self, request: &Request<Incoming>) -> Option<bool> {
        if request.headers().get_all(AUTHORIZATION).iter().count() != 1 {
            return None;
        }
        let token = request
            .headers()
            .get(AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")
            .filter(|t| !t.is_empty())?;
        let digest = Sha256::digest(token.as_bytes());
        let mut found = None;
        for (hash, reader) in &self.tokens {
            let diff = hash
                .iter()
                .zip(digest.iter())
                .fold(0u8, |d, (a, b)| d | (a ^ b));
            if diff == 0 {
                found = Some(*reader);
            }
        }
        found
    }
}

struct Full(Option<Bytes>);
impl Body for Full {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
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
struct OwnedResponse {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for OwnedResponse {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
fn response(status: StatusCode, content_type: &'static str, bytes: Bytes) -> Response<Full> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, "no-store")
        .header(CONNECTION, "close")
        .body(Full(Some(bytes)))
        .unwrap()
}
fn failure(status: StatusCode, reason: &'static str) -> Response<Full> {
    let body = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        Vec::new()
    } else {
        serde_json::to_vec(&serde_json::json!({"error":{"reason":reason,"message":"The query request could not be completed."}})).unwrap()
    };
    response(status, JSON, Bytes::from(body))
}
struct Completed {
    status: StatusCode,
    content_type: &'static str,
    bytes: Bytes,
}
impl Completed {
    fn failure(status: StatusCode, reason: &'static str, permit: OwnedSemaphorePermit) -> Self {
        let mut result = failure(status, reason);
        let bytes = result.body_mut().0.take().unwrap();
        Self {
            status,
            content_type: JSON,
            bytes: Bytes::from_owner(OwnedResponse {
                bytes: bytes.to_vec(),
                _permit: permit,
            }),
        }
    }
    fn response(self) -> Response<Full> {
        response(self.status, self.content_type, self.bytes)
    }
}

/// The connection owns the blocking JoinHandle even if Hyper drops its service future.
#[derive(Default)]
struct ConnectionWork {
    cancellation: Mutex<Option<Arc<AtomicBool>>>,
    job: Mutex<Option<JoinHandle<()>>>,
}
impl ConnectionWork {
    fn cancel(&self) {
        if let Some(flag) = self.cancellation.lock().unwrap().as_ref() {
            flag.store(true, Ordering::Relaxed);
        }
    }
    async fn finish(&self) {
        self.cancel();
        let job = self.job.lock().unwrap().take();
        if let Some(job) = job {
            let _ = job.await;
        }
    }
}
impl Drop for ConnectionWork {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn encoding(request: &Request<Incoming>) -> Option<Encoding> {
    if request.headers().get_all(CONTENT_TYPE).iter().count() != 1 {
        return None;
    }
    match request
        .headers()
        .get(CONTENT_TYPE)?
        .to_str()
        .ok()?
        .split(';')
        .next()?
        .trim()
    {
        value if value.eq_ignore_ascii_case(JSON) => Some(Encoding::Json),
        value if value.eq_ignore_ascii_case(PROTOBUF) => Some(Encoding::Protobuf),
        _ => None,
    }
}
async fn collect(mut body: Incoming) -> Result<Vec<u8>, StatusCode> {
    let mut bytes = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        if let Ok(data) = frame.map_err(|_| StatusCode::BAD_REQUEST)?.into_data() {
            if data.len() > document::MAX_REQUEST_BYTES - bytes.len() {
                return Err(StatusCode::PAYLOAD_TOO_LARGE);
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}
fn status(reason: &str) -> StatusCode {
    match reason {
        "invalid-request" => StatusCode::BAD_REQUEST,
        "request-too-large" => StatusCode::PAYLOAD_TOO_LARGE,
        "scan-limit" | "response-limit" => StatusCode::UNPROCESSABLE_ENTITY,
        "query-timeout" | "snapshot-unavailable" | "busy" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
fn execute(
    app: Arc<App>,
    bytes: Vec<u8>,
    encoding: Encoding,
    mut budget: Budget,
    permit: OwnedSemaphorePermit,
) -> Completed {
    let mut run = || -> Result<Vec<u8>, &'static str> {
        budget.check().map_err(|_| "query-timeout")?;
        let request = document::decode_request(&bytes, encoding).map_err(|e| e.reason())?;
        budget.check().map_err(|_| "query-timeout")?;
        let result = evaluate::evaluate(&app.root, &request, encoding, &mut budget)
            .map_err(|e| e.reason())?;
        let encoded = document::encode_response(&result, encoding).map_err(|e| e.reason())?;
        budget.check().map_err(|_| "query-timeout")?;
        Ok(encoded)
    };
    match run() {
        Ok(bytes) => Completed {
            status: StatusCode::OK,
            content_type: match encoding {
                Encoding::Json => JSON,
                Encoding::Protobuf => PROTOBUF,
            },
            bytes: Bytes::from_owner(OwnedResponse {
                bytes,
                _permit: permit,
            }),
        },
        Err(reason) => Completed::failure(status(reason), reason, permit),
    }
}

async fn handle(
    app: Arc<App>,
    work: Arc<ConnectionWork>,
    request: Request<Incoming>,
) -> Result<Response<Full>, Infallible> {
    let deadline = tokio::time::Instant::now() + app.request_timeout;
    let budget = Budget::new(Limits {
        wall_time: app.request_timeout,
        ..Limits::default()
    });
    if request.uri().path() != "/v1/query" {
        return Ok(failure(StatusCode::NOT_FOUND, "not-found"));
    }
    if request.method() != Method::POST {
        let mut r = failure(StatusCode::METHOD_NOT_ALLOWED, "method-not-allowed");
        r.headers_mut()
            .insert(hyper::header::ALLOW, "POST".parse().unwrap());
        return Ok(r);
    }
    match app.identity(&request) {
        Some(true) => {}
        Some(false) => return Ok(failure(StatusCode::FORBIDDEN, "forbidden")),
        None => return Ok(failure(StatusCode::UNAUTHORIZED, "unauthorized")),
    }
    let Some(encoding) = encoding(&request) else {
        return Ok(failure(StatusCode::UNSUPPORTED_MEDIA_TYPE, "content-type"));
    };
    if request.headers().get_all(CONTENT_ENCODING).iter().any(|v| {
        v.to_str()
            .ok()
            .is_none_or(|v| !v.eq_ignore_ascii_case("identity"))
    }) {
        return Ok(failure(StatusCode::UNSUPPORTED_MEDIA_TYPE, "content-type"));
    }
    if request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > document::MAX_REQUEST_BYTES as u64)
    {
        return Ok(failure(StatusCode::PAYLOAD_TOO_LARGE, "request-too-large"));
    }
    let Ok(permit) = app.work.clone().try_acquire_owned() else {
        return Ok(failure(StatusCode::SERVICE_UNAVAILABLE, "busy"));
    };
    *work.cancellation.lock().unwrap() = Some(budget.cancellation());
    let bytes = match tokio::time::timeout_at(deadline, collect(request.into_body())).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(code)) => {
            return Ok(failure(
                code,
                if code == StatusCode::PAYLOAD_TOO_LARGE {
                    "request-too-large"
                } else {
                    "invalid-request"
                },
            ));
        }
        Err(_) => return Ok(failure(StatusCode::SERVICE_UNAVAILABLE, "query-timeout")),
    };
    let (send, mut receive) = oneshot::channel();
    let job = tokio::task::spawn_blocking(move || {
        let completed = execute(app, bytes, encoding, budget, permit);
        let _ = send.send(completed);
    });
    *work.job.lock().unwrap() = Some(job);
    let completed = match tokio::time::timeout_at(deadline, &mut receive).await {
        Ok(Ok(completed)) => completed,
        Ok(Err(_)) => return Ok(failure(StatusCode::INTERNAL_SERVER_ERROR, "internal-error")),
        Err(_) => {
            work.cancel();
            // Keep the connection's JoinHandle installed; outer cleanup joins it on disconnect too.
            let _ = receive.await;
            return Ok(failure(StatusCode::SERVICE_UNAVAILABLE, "query-timeout"));
        }
    };
    Ok(completed.response())
}
async fn serve_connection<I>(io: I, app: Arc<App>, work: Arc<ConnectionWork>)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service_work = work.clone();
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(QUERY_TIMEOUT)
        .max_buf_size(64 << 10)
        .keep_alive(false)
        .serve_connection(
            TokioIo::new(io),
            hyper::service::service_fn(move |request| {
                handle(app.clone(), service_work.clone(), request)
            }),
        );
    // Header time and query/body/admission time are each bounded; slow output cannot pin a slot forever.
    let _ = tokio::time::timeout(CONNECTION_TIMEOUT, connection).await;
    work.finish().await;
}
fn tls_config(cfg: &Config) -> Result<Arc<ServerConfig>, &'static str> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        File::open(&cfg.tls.cert_file).map_err(|_| "TLS certificate unreadable")?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|_| "invalid TLS certificate")?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        File::open(&cfg.tls.key_file).map_err(|_| "TLS key unreadable")?,
    ))
    .map_err(|_| "invalid TLS key")?
    .ok_or("TLS key missing")?;
    let mut tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| "invalid TLS configuration")?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|_| "invalid TLS certificate or key")?;
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(tls))
}
pub fn serve(cfg: Config) -> Result<(), &'static str> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| "runtime failed")?;
    runtime.block_on(async {
        let tls = TlsAcceptor::from(tls_config(&cfg)?);
        let app = Arc::new(App::new(&cfg));
        let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        for address in &cfg.listen {
            let listener = TcpListener::bind(address)
                .await
                .map_err(|_| "listener failed")?;
            let (tls, app, connections) = (tls.clone(), app.clone(), connections.clone());
            tokio::spawn(async move {
                loop {
                    let Ok(permit) = connections.clone().acquire_owned().await else {
                        return;
                    };
                    let Ok((tcp, _)) = listener.accept().await else {
                        continue;
                    };
                    let (tls, app) = (tls.clone(), app.clone());
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Ok(Ok(stream)) =
                            tokio::time::timeout(QUERY_TIMEOUT, tls.accept(tcp)).await
                        {
                            serve_connection(stream, app, Arc::new(ConnectionWork::default()))
                                .await;
                        }
                    });
                }
            });
        }
        masu_core::log::info(
            "query-start",
            serde_json::json!({"version":env!("CARGO_PKG_VERSION")}),
        );
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|_| "signal setup failed")?;
        tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
        masu_core::log::info("query-stop", serde_json::json!({}));
        Ok(())
    })
}

#[cfg(test)]
mod tests;
