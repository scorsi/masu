use hyper::{
    Request, Response,
    body::{Body, Bytes, Frame, Incoming, SizeHint},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    fs,
    io::BufReader,
    path::{Path, PathBuf},
    pin::Pin,
    process::{Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, ServerConfig},
};

const SUCCESS: &str = r#"{"results":{"query":{"series":{"series":[{"labels":{"zone":"a"},"ts":["2026-11-01T12:00:00Z"],"values":[12]}]}}}}"#;
const ERROR: &str =
    r#"{"results":{"query":{"error":{"reason":"result-limit","message":"narrow the range"}}}}"#;
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    root: Temporary,
    tls: Arc<ServerConfig>,
}
impl Fixture {
    fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let root = Temporary(std::env::temp_dir().join(format!(
            "masu-cli-https-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        )));
        fs::create_dir_all(&root.0).unwrap();
        let openssl = |args: &[&str]| {
            assert!(
                Command::new("openssl")
                    .args(args)
                    .current_dir(&root.0)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .unwrap()
                    .success()
            );
        };
        openssl(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=masu-test-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ]);
        openssl(&[
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ]);
        fs::write(root.0.join("leaf.ext"),"subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
        openssl(&[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "2",
            "-extfile",
            "leaf.ext",
        ]);
        fs::write(root.0.join("token"), "test-reader-token\n").unwrap();
        let certs = rustls_pemfile::certs(&mut BufReader::new(
            fs::File::open(root.0.join("server.pem")).unwrap(),
        ))
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap();
        let key = rustls_pemfile::private_key(&mut BufReader::new(
            fs::File::open(root.0.join("server.key")).unwrap(),
        ))
        .unwrap()
        .unwrap();
        let tls =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap();
        Self {
            root,
            tls: Arc::new(tls),
        }
    }
    fn client(
        &self,
        case: Case,
        host: &str,
        json: bool,
        ca: Option<&Path>,
        extra: &[&str],
    ) -> Output {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let tls = self.tls.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let serve = async {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let Ok(tls) = TlsAcceptor::from(tls).accept(tcp).await else {
                        return;
                    };
                    let service = service_fn(move |mut req: Request<Incoming>| {
                        let case = case.clone();
                        async move {
                            assert_eq!(req.uri().path(), "/v1/query");
                            assert_eq!(req.method(), "POST");
                            assert_eq!(req.headers()["authorization"], "Bearer test-reader-token");
                            while std::future::poll_fn(|cx| Pin::new(req.body_mut()).poll_frame(cx))
                                .await
                                .is_some()
                            {}
                            if case.delay {
                                return std::future::pending::<Result<Response<Full>, Infallible>>()
                                    .await;
                            }
                            let mut response = Response::builder()
                                .status(case.status)
                                .header("content-type", case.content_type)
                                .header("connection", "close");
                            if let Some(length) = case.length {
                                response = response.header("content-length", length);
                            } else if !case.omit_length {
                                response = response.header("content-length", case.body.len());
                            }
                            if case.gzip {
                                response = response.header("content-encoding", "gzip");
                            }
                            if case.status == 303 {
                                response = response
                                    .header("location", "https://redirect.invalid/v1/query");
                            }
                            Ok::<_, Infallible>(
                                response.body(Full(Some(case.body.into()))).unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                };
                tokio::select!{_=tokio::time::timeout(Duration::from_secs(12),serve)=>{},_=stopped=>{}}

            });
        });
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_masu"));
        cmd.args([
            "query",
            "--node",
            "node-a",
            "--service",
            "annai",
            "--metric",
            "annai_total",
            "--from",
            "2026-11-01T00:00:00Z",
            "--to",
            "2026-11-02T00:00:00Z",
        ])
        .args(extra)
        .env("MASU_QUERY_URL", format!("https://{host}:{port}"))
        .env("MASU_TOKEN_FILE", self.root.0.join("token"));
        if let Some(ca) = ca {
            cmd.env("MASU_CA_FILE", ca);
        } else {
            cmd.env_remove("MASU_CA_FILE")
                .env("NIX_SSL_CERT_FILE", self.root.0.join("ca.pem"));
        }
        if json {
            cmd.arg("--json");
        }
        let out = cmd.output().unwrap();
        let _ = stop.send(());
        thread.join().unwrap();
        out
    }
}
#[derive(Clone)]
struct Case {
    status: u16,
    body: Vec<u8>,
    content_type: &'static str,
    length: Option<usize>,
    gzip: bool,
    delay: bool,
    omit_length: bool,
}
impl Case {
    fn body(body: &str) -> Self {
        Self {
            status: 200,
            body: body.as_bytes().to_vec(),
            content_type: "application/json",
            length: None,
            gzip: false,
            delay: false,
            omit_length: false,
        }
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
        Poll::Ready(self.get_mut().0.take().map(|v| Ok(Frame::data(v))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
#[test]
fn query_client_https_success_and_query_errors_preserve_exit_and_stdout_contract() {
    let f = Fixture::new();
    let ca = f.root.0.join("ca.pem");
    let out = f.client(Case::body(SUCCESS), "localhost", false, Some(&ca), &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        out.stdout,
        b"2026-11-01T12:00:00.000000000Z node-a annai annai_total{zone=\"a\"} 12\n"
    );
    let out = f.client(Case::body(SUCCESS), "localhost", true, None, &[]);
    assert!(out.status.success());
    assert_eq!(out.stdout, SUCCESS.as_bytes());
    for json in [false, true] {
        let out = f.client(Case::body(ERROR), "localhost", json, Some(&ca), &[]);
        assert_eq!(out.status.code(), Some(3));
        assert_eq!(out.stdout, if json { ERROR.as_bytes() } else { b"" });
    }
}
#[test]
fn query_client_https_failures_never_emit_stdout_or_reader_credentials() {
    let f = Fixture::new();
    let ca = f.root.0.join("ca.pem");
    let mut cases = Vec::new();
    let mut redirect = Case::body(SUCCESS);
    redirect.status = 303;
    cases.push(redirect);
    let mut denied = Case::body(SUCCESS);
    denied.status = 401;
    cases.push(denied);
    let mut wrong_type = Case::body(SUCCESS);
    wrong_type.content_type = "text/plain";
    cases.push(wrong_type);
    let mut gzip = Case::body(SUCCESS);
    gzip.gzip = true;
    cases.push(gzip);
    let mut oversized = Case::body(SUCCESS);
    oversized.length = Some((16 << 20) + 1);
    cases.push(oversized);
    let mut truncated = Case::body("{\"results\":");
    truncated.length = Some(4096);
    cases.push(truncated);
    let mut chunked = Case::body("");
    chunked.omit_length = true;
    chunked.body = vec![b'x'; (16 << 20) + 1];
    cases.push(chunked);
    cases.push(Case::body("{not json"));
    cases.push(Case::body(&SUCCESS.replace("\"query\":", "\"other\":")));
    cases.push(Case::body(
        &SUCCESS.replace("2026-11-01T12:00:00Z", "2026-11-05T12:00:00Z"),
    ));
    for case in cases {
        for json in [false, true] {
            let out = f.client(case.clone(), "localhost", json, Some(&ca), &[]);
            assert_eq!(
                out.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(out.stdout.is_empty());
            if case.omit_length {
                assert!(String::from_utf8_lossy(&out.stderr).contains("exceeds 16 MiB"));
            }
            assert!(!String::from_utf8_lossy(&out.stderr).contains("test-reader-token"));
        }
    }
    let out = f.client(Case::body(SUCCESS), "127.0.0.1", false, Some(&ca), &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    let out = f.client(
        Case::body(SUCCESS),
        "localhost",
        false,
        Some(&ca),
        &["--label", "zone=b"],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    let foreign = Fixture::new();
    let out = f.client(
        Case::body(SUCCESS),
        "localhost",
        false,
        Some(&foreign.root.0.join("ca.pem")),
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("TLS"));
    fs::write(f.root.0.join("empty-ca"), "").unwrap();
    let out = f.client(
        Case::body(SUCCESS),
        "localhost",
        false,
        Some(&f.root.0.join("empty-ca")),
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    for token in [vec![b'x'; 4097], b"token\r\n".to_vec(), Vec::new()] {
        fs::write(f.root.0.join("token"), token).unwrap();
        let out = f.client(Case::body(SUCCESS), "localhost", false, Some(&ca), &[]);
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
    }
}
#[test]
fn query_client_deadline_covers_waiting_for_response() {
    let f = Fixture::new();
    let ca = f.root.0.join("ca.pem");
    let mut case = Case::body(SUCCESS);
    case.delay = true;
    let start = std::time::Instant::now();
    let out = f.client(case, "localhost", false, Some(&ca), &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("timed out"));
    assert!(start.elapsed() < Duration::from_secs(15));
}
#[test]
fn query_client_legacy_commands_have_specific_migration_help() {
    for (command, target) in [
        ("read", "masu-store read --root"),
        ("verify", "masu-store verify --root"),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_masu"))
            .args([command, "/not-a-guessed-url"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains(target));
    }
}
