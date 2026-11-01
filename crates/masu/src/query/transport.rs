use super::*;
use hyper::{
    Request,
    body::{Body, Bytes, Frame, Incoming, SizeHint},
    header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST},
};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    fs::File,
    io::BufReader,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};

pub(super) struct Config {
    authority: String,
    host: String,
    port: u16,
    token: String,
    ca: PathBuf,
}
impl Config {
    pub(super) fn from_env() -> Result<Self> {
        let env = |name| std::env::var(name).map_err(|_| format!("{name} is required"));
        let url = env("MASU_QUERY_URL")?;
        let (authority, host, port) = origin(&url)?;
        let mut token = String::new();
        File::open(env("MASU_TOKEN_FILE")?)
            .map_err(|_| "cannot read reader token file")?
            .take(4097)
            .read_to_string(&mut token)
            .map_err(|_| "invalid reader token file")?;
        if token.len() > 4096 {
            return Err("reader token exceeds 4 KiB".into());
        }
        if token.ends_with('\n') {
            token.pop();
        }
        if token.is_empty()
            || token
                .bytes()
                .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        {
            return Err("invalid reader token file".into());
        }
        let ca = std::env::var_os("MASU_CA_FILE")
            .or_else(|| std::env::var_os("NIX_SSL_CERT_FILE"))
            .or_else(|| std::env::var_os("SSL_CERT_FILE"))
            .map(PathBuf::from)
            .or_else(|| {
                ["/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem"]
                    .iter()
                    .map(PathBuf::from)
                    .find(|p| p.is_file())
            })
            .ok_or("system CA bundle unavailable; set MASU_CA_FILE")?;
        Ok(Self {
            authority,
            host,
            port,
            token,
            ca,
        })
    }
}
fn origin(url: &str) -> Result<(String, String, u16)> {
    let rest = url
        .strip_prefix("https://")
        .ok_or("MASU_QUERY_URL must be an HTTPS origin")?;
    if rest.is_empty()
        || rest.bytes().any(|b| {
            b.is_ascii_control()
                || b.is_ascii_whitespace()
                || matches!(b, b'/' | b'?' | b'#' | b'@' | b'\\' | b'%')
        })
    {
        return Err("MASU_QUERY_URL must contain only an HTTPS host and optional port".into());
    }
    let uri = url
        .parse::<hyper::Uri>()
        .map_err(|_| "invalid HTTPS origin")?;
    let authority = uri.authority().ok_or("missing query host")?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    if host.is_empty() {
        return Err("missing query host".into());
    }
    let port_text = if rest.starts_with('[') {
        let (_, tail) = rest.split_once(']').ok_or("invalid IPv6 query origin")?;
        if tail.is_empty() {
            None
        } else {
            Some(tail.strip_prefix(':').ok_or("invalid query port")?)
        }
    } else {
        if rest.bytes().filter(|&b| b == b':').count() > 1 {
            return Err("IPv6 origins require brackets".into());
        }
        rest.split_once(':').map(|(_, port)| port)
    };
    let port = match port_text {
        None => 443,
        Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => p
            .parse::<u16>()
            .ok()
            .filter(|&p| p > 0)
            .ok_or("invalid query port")?,
        Some(_) => return Err("invalid query port".into()),
    };
    ServerName::try_from(host.to_owned()).map_err(|_| "invalid query hostname")?;
    Ok((authority.to_string(), host.into(), port))
}
struct Full(Option<Bytes>);
impl Body for Full {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.get_mut().0.take().map(|b| Ok(Frame::data(b))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |b| b.len() as u64))
    }
}
pub(super) async fn request(config: Config, request: &pb::QueryRequest) -> Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(10), exchange(config, request))
        .await
        .map_err(|_| "query request timed out".to_string())?
}
async fn exchange(config: Config, request: &pb::QueryRequest) -> Result<Vec<u8>> {
    let mut roots = RootCertStore::empty();
    let pem = File::open(&config.ca).map_err(|_| "cannot read query CA bundle")?;
    for cert in rustls_pemfile::certs(&mut BufReader::new(pem)) {
        roots
            .add(cert.map_err(|_| "invalid query CA bundle")?)
            .map_err(|_| "invalid query CA certificate")?;
    }
    if roots.is_empty() {
        return Err("query CA bundle contains no certificates".into());
    }
    let tls = ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| "cannot configure TLS")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = TcpStream::connect((config.host.as_str(), config.port))
        .await
        .map_err(|_| "cannot connect to query service")?;
    let tls = TlsConnector::from(Arc::new(tls))
        .connect(
            ServerName::try_from(config.host).map_err(|_| "invalid query hostname")?,
            tcp,
        )
        .await
        .map_err(|_| "query TLS certificate or connection verification failed")?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .map_err(|_| "query HTTP handshake failed")?;
    let connection = tokio::spawn(connection);
    let result = async {
        let bytes = document::encode_request(request, Encoding::Json).map_err(|e| e.to_string())?;
        let req = Request::builder()
            .method("POST")
            .uri("/v1/query")
            .header(HOST, config.authority)
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, format!("Bearer {}", config.token))
            .body(Full(Some(bytes.into())))
            .map_err(|_| "invalid query request headers")?;
        let response = sender
            .send_request(req)
            .await
            .map_err(|_| "query HTTP request failed")?;
        if response.status() != hyper::StatusCode::OK {
            return Err(format!(
                "query service returned HTTP {}",
                response.status().as_u16()
            ));
        }
        let header = |name| response.headers().get(name).and_then(|v| v.to_str().ok());
        if header(CONTENT_TYPE).is_none_or(|v| {
            !v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/json")
        }) {
            return Err("unexpected query response content type".into());
        }
        if header(CONTENT_ENCODING).is_some_and(|v| !v.eq_ignore_ascii_case("identity")) {
            return Err("unexpected query response content encoding".into());
        }
        if header(CONTENT_LENGTH).is_some_and(|v| {
            v.parse::<usize>()
                .ok()
                .is_none_or(|n| n > document::MAX_RESPONSE_BYTES)
        }) {
            return Err("invalid or oversized query response length".into());
        }
        read_body(response.into_body()).await
    }
    .await;
    connection.abort();
    result
}
async fn read_body(mut body: Incoming) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|_| "truncated or invalid query response")?;
        if let Ok(data) = frame.into_data() {
            if data.len() > document::MAX_RESPONSE_BYTES - bytes.len() {
                return Err("query response exceeds 16 MiB".into());
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_client_origin_rejects_paths_credentials_and_unsafe_forms() {
        for url in [
            "http://example.test",
            "https://example.test/",
            "https://user@example.test",
            "https://example.test?q=x",
            "https://example.test#x",
            "https://example.test:0",
            "https://example.test:99999",
            "https://example.test:+443",
            "https://example.test:",
            "https://::1",
            "https://example.test\\x",
            "https://example.test%2fx",
        ] {
            assert!(origin(url).is_err(), "{url}");
        }
        assert_eq!(origin("https://example.test:9443").unwrap().2, 9443);
        assert_eq!(origin("https://[::1]:9443").unwrap().1, "::1");
    }
}
