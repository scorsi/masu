use super::*;
use prost::Message;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::atomic::AtomicUsize,
};

fn app() -> Arc<App> {
    let hash = |token: &str| {
        Sha256::digest(token.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let cfg=Config::parse(&format!("listen=[\"127.0.0.1:0\"]\ndata_dir=\"/nonexistent-masu-query-test-archive\"\n[tls]\ncert_file=\"/nonexistent-cert\"\nkey_file=\"/nonexistent-key\"\n[tokens]\n\"{}\"=\"reader\"\n\"{}\"=\"node:node-a\"\n",hash("reader-token"),hash("node-token"))).unwrap();
    Arc::new(App::new(&cfg))
}
async fn exchange(app: Arc<App>, bytes: Vec<u8>) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        serve_connection(stream, app, Arc::new(ConnectionWork::default())).await;
    });
    let response = tokio::task::spawn_blocking(move || send(addr, &bytes))
        .await
        .unwrap();
    server.await.unwrap();
    response
}
fn send(addr: SocketAddr, bytes: &[u8]) -> Vec<u8> {
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let _ = socket.write_all(bytes);
    let mut response = Vec::new();
    if let Err(error) = socket.read_to_end(&mut response) {
        assert!(
            !response.is_empty() && error.kind() == std::io::ErrorKind::ConnectionReset,
            "{error}"
        );
    }
    response
}
fn request(token: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes=format!("POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",body.len()).into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
fn code(response: &[u8]) -> u16 {
    std::str::from_utf8(response)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
fn body(response: &[u8]) -> &[u8] {
    let at = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    &response[at + 4..]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_auth_precedes_content_type_body_collection_and_decode() {
    for (token, want) in [("bad-token", 401), ("node-token", 403)] {
        let bytes=format!("POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: invalid\r\nContent-Length: 65537\r\n\r\n").into_bytes();
        let response = exchange(app(), bytes).await;
        assert_eq!(code(&response), want);
        assert!(body(&response).is_empty());
        assert!(
            String::from_utf8_lossy(&response)
                .to_lowercase()
                .contains("connection: close")
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_routes_media_and_body_caps_fail_closed_with_static_errors() {
    let requests=[
        (b"GET /v1/status HTTP/1.1\r\nHost: localhost\r\n\r\n".to_vec(),404),
        (b"GET /v1/query HTTP/1.1\r\nHost: localhost\r\n\r\n".to_vec(),405),
        (request("reader-token","text/plain",b""),415),
        (request("reader-token",JSON,b"malformed-do-not-echo"),400),
        (b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer reader-token\r\nContent-Type: application/json\r\nContent-Length: 65537\r\n\r\n".to_vec(),413),
    ];
    for (bytes, want) in requests {
        let response = exchange(app(), bytes).await;
        assert_eq!(code(&response), want);
        let text = String::from_utf8_lossy(&response);
        assert!(text.to_lowercase().contains("cache-control: no-store"));
        assert!(!text.contains("do-not-echo"));
        assert!(!text.contains("nonexistent"));
    }
    let mut chunked=b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer reader-token\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n10001\r\n".to_vec();
    chunked.extend(vec![b'x'; 65_537]);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    assert_eq!(code(&exchange(app(), chunked).await), 413);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_returns_the_request_encoding_and_scopes_unknown_operators() {
    let query=br#"{"range":{"from":"2026-11-01T00:00:00.000000001Z","to":"2026-11-01T00:00:01Z"},"queries":{"future":{"newOperator":{"large":1e400}}}}"#;
    let json = exchange(app(), request("reader-token", JSON, query)).await;
    assert_eq!(code(&json), 200);
    let response = document::decode_response(body(&json), Encoding::Json).unwrap();
    assert!(
        matches!(response.results["future"].body.as_ref(),Some(masu_core::pb::query_result::Body::Error(e))if e.reason=="unsupported")
    );
    let q = masu_core::pb::QueryRequest {
        range: Some(
            document::decode_request(query, Encoding::Json)
                .unwrap()
                .range,
        ),
        queries: std::collections::BTreeMap::from([(
            "empty".into(),
            masu_core::pb::Query::default(),
        )]),
    };
    let response = exchange(app(), request("reader-token", PROTOBUF, &q.encode_to_vec())).await;
    assert_eq!(code(&response), 200);
    let headers = String::from_utf8_lossy(
        &response[..response.windows(4).position(|w| w == b"\r\n\r\n").unwrap()],
    );
    assert!(headers.contains(PROTOBUF));
    assert!(document::decode_response(body(&response), Encoding::Protobuf).is_ok());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_busy_and_body_deadline_never_wait_for_unread_upload() {
    let busy_app = app();
    let first = busy_app.work.clone().acquire_owned().await.unwrap();
    let second = busy_app.work.clone().acquire_owned().await.unwrap();
    let incomplete=b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer reader-token\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n".to_vec();
    let response = exchange(busy_app.clone(), incomplete.clone()).await;
    assert_eq!(code(&response), 503);
    assert!(String::from_utf8_lossy(body(&response)).contains("busy"));
    drop(first);
    drop(second);
    let mut app = app();
    Arc::get_mut(&mut app).unwrap().request_timeout = Duration::from_millis(50);
    let response = exchange(app.clone(), incomplete).await;
    assert_eq!(code(&response), 503);
    assert!(String::from_utf8_lossy(body(&response)).contains("query-timeout"));
    assert_eq!(app.work.available_permits(), 2);
}
#[tokio::test]
async fn query_http_work_permit_lives_until_the_last_hyper_bytes_clone() {
    let work = Arc::new(Semaphore::new(2));
    let other = work.clone().acquire_owned().await.unwrap();
    let permit = work.clone().acquire_owned().await.unwrap();
    let data = Bytes::from_owner(OwnedResponse {
        bytes: vec![1, 2, 3],
        _permit: permit,
    });
    let mut full = Full(Some(data));
    let frame = std::future::poll_fn(|cx| Pin::new(&mut full).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let bytes = frame.into_data().unwrap();
    let clone = bytes.clone();
    drop(full);
    drop(bytes);
    assert_eq!(work.available_permits(), 0);
    assert_eq!(clone.as_ref(), [1, 2, 3]);
    drop(clone);
    assert_eq!(work.available_permits(), 1);
    drop(other);
    assert_eq!(work.available_permits(), 2);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_connection_cleanup_cancels_and_joins_the_blocking_worker() {
    let work = ConnectionWork::default();
    let flag = Arc::new(AtomicBool::new(false));
    *work.cancellation.lock().unwrap() = Some(flag.clone());
    let done = Arc::new(AtomicUsize::new(0));
    let worker_done = done.clone();
    let worker_flag = flag.clone();
    let (started, ready) = oneshot::channel();
    let handle = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        while !worker_flag.load(Ordering::Relaxed) {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        worker_done.store(1, Ordering::Release);
    });
    *work.job.lock().unwrap() = Some(handle);
    ready.await.unwrap();
    work.finish().await;
    assert!(flag.load(Ordering::Relaxed));
    assert_eq!(done.load(Ordering::Acquire), 1);
    assert!(work.job.lock().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_http_peer_disconnect_cancels_and_joins_installed_work() {
    let work = Arc::new(ConnectionWork::default());
    let flag = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    *work.cancellation.lock().unwrap() = Some(flag.clone());
    let worker_flag = flag.clone();
    let worker_done = done.clone();
    let (started, ready) = oneshot::channel();
    *work.job.lock().unwrap() = Some(tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        while !worker_flag.load(Ordering::Relaxed) {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        worker_done.store(true, Ordering::Release);
    }));
    ready.await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connection_work = work.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        serve_connection(stream, app(), connection_work).await;
    });
    let peer = tokio::net::TcpStream::connect(address).await.unwrap();
    drop(peer);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(flag.load(Ordering::Relaxed));
    assert!(done.load(Ordering::Acquire));
    assert!(work.job.lock().unwrap().is_none());
}
