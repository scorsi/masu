//! masu command line: `serve`, `read`, `verify`, `check-config` (M1) and `validate` (M0).

use masu::convention;
use masu::exposition::Format;
use masu::read::Query;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: masu serve <config.toml>
       masu check-config <config.toml>
       masu read <root> --node <name> [--service <s>] [--metric <name or prefix>] [--from <ts>] [--to <ts>] [--raw]
       masu verify <root>
       masu validate --metrics <http://host:port/path | file>";
/// Far above the agent's 2 MiB body cap; only guards memory.
const MAX_RESPONSE_BYTES: u64 = 64 << 20;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["validate", "--metrics", target] => validate(target),
        ["serve", config] => match load_config(config).and_then(masu::server::serve) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                masu::log::error("serve", &e, serde_json::json!({}));
                ExitCode::FAILURE
            }
        },
        ["check-config", config] => match load_config(config) {
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("masu: {config}: {e}");
                ExitCode::FAILURE
            }
        },
        ["verify", root] => match masu::read::verify(Path::new(root), &mut std::io::stdout()) {
            Ok(errors) if errors.is_empty() => ExitCode::SUCCESS,
            Ok(_) => ExitCode::FAILURE,
            Err(e) => {
                eprintln!("masu: {root}: {e}");
                ExitCode::from(2)
            }
        },
        ["read", root, ref rest @ ..] => read(root, rest),
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

fn load_config(path: &str) -> Result<masu::config::Config, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    masu::config::Config::parse(&text).map_err(|e| format!("{path}: {e}"))
}

fn read(root: &str, args: &[&str]) -> ExitCode {
    let mut q = Query {
        node: String::new(),
        service: None,
        metric: None,
        from: None,
        to: None,
        raw: false,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        if *flag == "--raw" {
            q.raw = true;
            continue;
        }
        let Some(value) = it.next() else {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        };
        let value = value.to_string();
        match *flag {
            "--node" => q.node = value,
            "--service" => q.service = Some(value),
            "--metric" => q.metric = Some(value),
            "--from" => q.from = Some(value),
            "--to" => q.to = Some(value),
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let ts_ok = |t: &Option<String>| t.as_deref().is_none_or(masu::record::is_timestamp);
    if q.node.is_empty() || !ts_ok(&q.from) || !ts_ok(&q.to) {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut out = std::io::BufWriter::new(std::io::stdout());
    match masu::read::read(Path::new(root), &q, &mut out).and_then(|()| out.flush()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("masu: {root}: {e}");
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
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut response)
        .map_err(|e| e.to_string())?;
    if response.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(format!("response larger than {MAX_RESPONSE_BYTES} bytes"));
    }
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
    let (mut chunked, mut length) = (false, None);
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
            "transfer-encoding" if value.to_ascii_lowercase().contains("chunked") => chunked = true,
            "content-length" => {
                length = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| "malformed Content-Length")?,
                )
            }
            _ => {}
        }
    }
    if chunked {
        body = dechunk(&body)?;
    } else if let Some(n) = length
        && body.len() != n
    {
        return Err(format!("{} body bytes, Content-Length {n}", body.len()));
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
        let end = size.checked_add(2).ok_or("malformed chunk size")?;
        if rest.len() < end {
            return Err("truncated chunk".into());
        }
        if &rest[size..end] != b"\r\n" {
            return Err("chunk without CRLF".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[end..];
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

        // F17: none of these panics or validates.
        for bad in [
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\nx"[..],
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\na 1\nXX0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nsvc_up 1\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: x\r\n\r\nsvc_up 1\n",
        ] {
            assert!(
                parse_response(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}
