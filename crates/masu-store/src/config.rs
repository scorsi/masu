//! `masu-store serve` configuration: the TOML the NixOS module renders with `pkgs.formats.toml`
//! (milestone M1, ADR 0002: `deny_unknown_fields`).

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Addresses of the HTTPS ingestion listener (`host:port`).
    pub listen: Vec<String>,
    /// Loopback address of the plain-HTTP `/metrics` listener; absent disables it.
    pub listen_metrics: Option<String>,
    pub data_dir: PathBuf,
    pub tls: Tls,
    /// Hex SHA-256 of a bearer token → `node:<name>` or `reader` (ADR 0007).
    #[serde(default)]
    pub tokens: BTreeMap<String, Identity>,
    /// Sealed days older than this lose their data file (manifest kept); 0 keeps everything.
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// Bytes of free space below which pushes get 507 (ADR 0006).
    #[serde(default = "default_disk_reserve")]
    pub disk_reserve: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    Node(String),
    Reader,
}

impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.strip_prefix("node:") {
            Some(node) if masu_core::record::is_node(node) => Ok(Identity::Node(node.into())),
            None if s == "reader" => Ok(Identity::Reader),
            _ => Err(serde::de::Error::custom(format!(
                "identity `{s}`: expected `node:<name>` or `reader`"
            ))),
        }
    }
}

fn default_retention_days() -> u32 {
    400
}

fn default_disk_reserve() -> u64 {
    2 << 30
}

impl Config {
    pub fn parse(text: &str) -> Result<Config, String> {
        let cfg: Config = toml::from_str(text).map_err(|e| e.to_string())?;
        for hash in cfg.tokens.keys() {
            if hash.len() != 64 || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
                return Err(format!("tokens: `{hash}` is not a lowercase hex SHA-256"));
            }
        }
        if cfg.listen.is_empty() {
            return Err("listen: at least one address".into());
        }
        // `/metrics` has no authentication: loopback only (metric convention).
        if let Some(addr) = &cfg.listen_metrics
            && !addr
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|a| a.ip().is_loopback())
        {
            return Err(format!(
                "listen_metrics: `{addr}` is not a loopback address and port"
            ));
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"
listen = ["[::]:9472"]
listen_metrics = "127.0.0.1:9473"
data_dir = "/var/lib/masu"
[tls]
cert_file = "/run/credentials/masu.service/cert"
key_file = "/run/credentials/masu.service/key"
[tokens]
"a3f1c2d4e5b6a7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70" = "node:node-a"
"b3f1c2d4e5b6a7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70" = "reader"
"#;

    #[test]
    fn parses_and_defaults() {
        let cfg = Config::parse(OK).unwrap();
        assert_eq!(cfg.retention_days, 400);
        assert_eq!(cfg.disk_reserve, 2 << 30);
        assert_eq!(cfg.tokens.len(), 2);
        assert_eq!(
            cfg.tokens.values().next(),
            Some(&Identity::Node("node-a".into()))
        );
    }

    #[test]
    fn rejects() {
        assert!(Config::parse(&format!("{OK}\nretention_dayz = 1")).is_err());
        assert!(Config::parse(&OK.replace("key_file", "key")).is_err());
        assert!(Config::parse(&OK.replace("\"reader\"", "\"admin\"")).is_err());
        assert!(Config::parse(&OK.replace("node:node-a", "node:Node_A")).is_err());
        assert!(Config::parse(&OK.replace("b3f1c2d4e5b6a7f8091a2b3c4d5e6f70", "zz")).is_err());
        assert!(Config::parse(&OK.replace("[\"[::]:9472\"]", "[]")).is_err());
        for addr in [
            "0.0.0.0:9473",
            "[::]:9473",
            "192.0.2.1:9473",
            "localhost:9473",
        ] {
            assert!(
                Config::parse(&OK.replace("127.0.0.1:9473", addr)).is_err(),
                "{addr}"
            );
        }
        assert!(Config::parse(&OK.replace("127.0.0.1:9473", "[::1]:9473")).is_ok());
    }
}
