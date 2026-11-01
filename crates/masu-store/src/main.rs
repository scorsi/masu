//! `masu-store`, the datastore (ADR 0012): the only writer of a store root. It owns its
//! configuration: `check-config` validates a file without starting.

mod config;
mod server;

use std::{io::Write, path::Path, process::ExitCode};

const USAGE: &str = "usage: masu-store serve <config.toml>
       masu-store check-config <config.toml>
       masu-store read --root <root> --node <name> [--service <s>] [--metric <prefix>] [--from <ts>] [--to <ts>] [--raw]
       masu-store verify --root <root>
       masu-store --version";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["serve", config] => match load_config(config).and_then(server::serve) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                masu_core::log::error("serve", &e, serde_json::json!({}));
                ExitCode::FAILURE
            }
        },
        ["check-config", config] => match load_config(config) {
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("masu-store: {config}: {e}");
                ExitCode::FAILURE
            }
        },
        ["read", ref rest @ ..] => read(rest),
        ["verify", "--root", root] => {
            match masu_core::read::verify(Path::new(root), &mut std::io::stdout()) {
                Ok(errors) if errors.is_empty() => ExitCode::SUCCESS,
                Ok(_) => ExitCode::FAILURE,
                Err(e) => {
                    eprintln!("masu-store: {root}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ["--version"] => {
            println!("masu-store {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn load_config(path: &str) -> Result<config::Config, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    config::Config::parse(&text).map_err(|e| format!("{path}: {e}"))
}

fn read(args: &[&str]) -> ExitCode {
    let result = (|| {
        let mut q = masu_core::read::Query {
            node: String::new(),
            service: None,
            metric: None,
            from: None,
            to: None,
            raw: false,
        };
        let mut root = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut it = args.iter();
        while let Some(&flag) = it.next() {
            if !seen.insert(flag) {
                return Err("duplicate read option".to_string());
            }
            if flag == "--raw" {
                q.raw = true;
                continue;
            }
            let value = it.next().ok_or("missing read option value")?.to_string();
            match flag {
                "--root" => root = Some(value),
                "--node" => q.node = value,
                "--service" => q.service = Some(value),
                "--metric" => q.metric = Some(value),
                "--from" => q.from = Some(value),
                "--to" => q.to = Some(value),
                _ => return Err("unknown read option".into()),
            }
        }
        if !masu_core::record::is_node(&q.node)
            || q.from
                .as_deref()
                .is_some_and(|t| !masu_core::record::is_timestamp(t))
            || q.to
                .as_deref()
                .is_some_and(|t| !masu_core::record::is_timestamp(t))
        {
            return Err("invalid read node or timestamp".into());
        }
        let root = root.ok_or("--root is required")?;
        let mut out = std::io::BufWriter::new(std::io::stdout());
        masu_core::read::read(Path::new(&root), &q, &mut out)
            .and_then(|()| out.flush())
            .map_err(|e| format!("{root}: {e}"))
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("masu-store: {e}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
