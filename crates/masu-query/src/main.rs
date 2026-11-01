//! A separate read-only HTTPS query daemon (ADRs 0013–0014).
mod config;
mod server;
use std::process::ExitCode;
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["--version"] => {
            println!("masu-query {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        ["check-config", path] => load(path).map(|_| ()),
        ["serve", path] => load(path).and_then(server::serve),
        _ => {
            eprintln!(
                "usage: masu-query serve <config.toml>\n       masu-query check-config <config.toml>\n       masu-query --version"
            );
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(reason) => {
            masu_core::log::error("query-start-failed", reason, serde_json::json!({}));
            ExitCode::FAILURE
        }
    }
}
fn load(path: &str) -> Result<config::Config, &'static str> {
    let text = std::fs::read_to_string(path).map_err(|_| "configuration unreadable")?;
    config::Config::parse(&text)
}
