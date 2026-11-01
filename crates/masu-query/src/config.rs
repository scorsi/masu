//! The query daemon owns its configuration; no ingestion or retention options.
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Vec<String>,
    pub data_dir: PathBuf,
    pub tls: Tls,
    #[serde(default)]
    pub tokens: BTreeMap<String, Identity>,
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
        let value = String::deserialize(d)?;
        match value.strip_prefix("node:") {
            Some(node) if masu_core::record::is_node(node) => Ok(Self::Node(node.into())),
            None if value == "reader" => Ok(Self::Reader),
            _ => Err(serde::de::Error::custom(
                "identity must be reader or node:<name>",
            )),
        }
    }
}
impl Config {
    pub fn parse(text: &str) -> Result<Self, &'static str> {
        let cfg: Self = toml::from_str(text).map_err(|_| "invalid TOML configuration")?;
        if cfg.listen.is_empty() || cfg.listen.iter().any(|a| a.trim().is_empty()) {
            return Err("listen must contain an address");
        }
        if cfg.data_dir.as_os_str().is_empty()
            || cfg.tls.cert_file.as_os_str().is_empty()
            || cfg.tls.key_file.as_os_str().is_empty()
        {
            return Err("archive and TLS paths must not be empty");
        }
        if cfg.tokens.keys().any(|h| {
            h.len() != 64
                || !h
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }) {
            return Err("token hashes must be lowercase SHA-256 hex");
        }
        Ok(cfg)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    const OK: &str = "listen = [\"127.0.0.1:0\"]\ndata_dir = \"/archive\"\n[tls]\ncert_file = \"/credentials/cert\"\nkey_file = \"/credentials/key\"\n[tokens]\n\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" = \"reader\"\n\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\" = \"node:node-a\"\n";
    #[test]
    fn query_config_checks_its_own_schema_without_opening_credentials_or_archive() {
        let c = Config::parse(OK).unwrap();
        assert_eq!(c.tokens.len(), 2);
        assert_eq!(c.tokens.values().next(), Some(&Identity::Reader));
    }
    #[test]
    fn query_config_refuses_unknown_fields_bad_identities_and_hashes_without_echoing_input() {
        for input in [
            format!("extra_secret = \"do-not-print\"\n{OK}"),
            OK.replace("key_file", "private_key"),
            OK.replace("\"reader\"", "\"do-not-print\""),
            OK.replace("node:node-a", "node:Node_A"),
            OK.replace("aaaaaaaa", "AAAAAAAA"),
            OK.replace("[\"127.0.0.1:0\"]", "[]"),
            OK.replace("data_dir = \"/archive\"", "data_dir = \"\""),
        ] {
            let error = Config::parse(&input).unwrap_err();
            assert!(!error.contains("do-not-print"));
        }
    }
}
