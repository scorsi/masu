//! masu command line. M0: `masu validate --metrics <url|file>` (ADR 0005).

use masu::convention;
use masu::exposition::Format;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: masu validate --metrics <http://host:port/path | file>";
/// Far above the agent's 2 MiB body cap; only guards memory.
const MAX_RESPONSE_BYTES: u64 = 64 << 20;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["validate", "--metrics", target] => validate(target),
        ["--version"] => {
            println!("masu {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// Exit 0: conforms; 1: violations, one per line on stderr; 2: cannot read the target.
fn validate(target: &str) -> ExitCode {
    let (body, format) = match load(target) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("masu: {target}: {e}");
            return ExitCode::from(2);
        }
    };
    let violations = convention::validate(&body, format);
    if violations.is_empty() {
        println!("{target}: ok");
        return ExitCode::SUCCESS;
    }
    for v in &violations {
        eprintln!("{target}: {v}");
    }
    ExitCode::FAILURE
}

fn load(target: &str) -> Result<(Vec<u8>, Format), String> {
    if let Some(rest) = target.strip_prefix("http://") {
        return fetch(rest);
    }
    if target.starts_with("https://") {
        return Err("https is not supported: /metrics is served in plain HTTP on loopback".into());
    }
    let body = std::fs::read(target).map_err(|e| e.to_string())?;
    let format = Format::sniff(&body);
    Ok((body, format))
}

/// One HTTP/1.1 GET with `Connection: close`: the endpoints are on loopback (metric-convention.md).
fn fetch(rest: &str) -> Result<(Vec<u8>, Format), String> {
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let host_port = if authority.ends_with(']') || !authority.contains(':') {
        format!("{authority}:80")
    } else {
        authority.to_string()
    };
    let timeout = Duration::from_secs(10);
    let addr = host_port
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .next()
        .ok_or("no address")?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    stream
        .take(MAX_RESPONSE_BYTES)
        .read_to_end(&mut response)
        .map_err(|e| e.to_string())?;
    parse_response(&response)
}

fn parse_response(response: &[u8]) -> Result<(Vec<u8>, Format), String> {
    let end = find(response, b"\r\n\r\n").ok_or("malformed HTTP response")?;
    let head = std::str::from_utf8(&response[..end]).map_err(|_| "malformed HTTP response")?;
    let mut body = response[end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or("");
    if status.split(' ').nth(1) != Some("200") {
        return Err(format!("HTTP `{status}`"));
    }
    let mut format = Format::Prometheus;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-type" => format = Format::from_content_type(value),
            "content-encoding" if !value.eq_ignore_ascii_case("identity") => {
                return Err(format!("unexpected Content-Encoding `{value}`"));
            }
            "transfer-encoding" if value.to_ascii_lowercase().contains("chunked") => {
                body = dechunk(&body)?;
            }
            _ => {}
        }
    }
    Ok((body, format))
}

fn dechunk(mut rest: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let eol = find(rest, b"\r\n").ok_or("malformed chunk")?;
        let size = std::str::from_utf8(&rest[..eol])
            .ok()
            .and_then(|s| usize::from_str_radix(s.split(';').next()?.trim(), 16).ok())
            .ok_or("malformed chunk size")?;
        rest = &rest[eol + 2..];
        if size == 0 {
            return Ok(out);
        }
        if rest.len() < size + 2 {
            return Err("truncated chunk".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses() {
        let chunked = b"HTTP/1.1 200 OK\r\nContent-Type: application/openmetrics-text; version=1.0.0\r\nTransfer-Encoding: chunked\r\n\r\n4\r\na 1\n\r\n6\r\n# EOF\n\r\n0\r\n\r\n";
        let (body, format) = parse_response(chunked).unwrap();
        assert_eq!(body, b"a 1\n# EOF\n");
        assert_eq!(format, Format::OpenMetrics);

        let plain = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\na 1\n";
        assert_eq!(
            parse_response(plain).unwrap(),
            (b"a 1\n".to_vec(), Format::Prometheus)
        );

        assert!(parse_response(b"HTTP/1.1 404 Not Found\r\n\r\n").is_err());
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\nx").is_err());
    }
}
