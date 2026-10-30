//! `masu-store`, the datastore (ADR 0012): the only writer of a store root. It owns its
//! configuration: `check-config` validates a file without starting.

mod config;
mod server;

use std::process::ExitCode;

const USAGE: &str = "usage: masu-store serve <config.toml>
       masu-store check-config <config.toml>
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
