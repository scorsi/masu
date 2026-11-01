use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
struct ConfigFile(PathBuf);
impl ConfigFile {
    fn new(text: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "masu-query-config-{}-{}.toml",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, text).unwrap();
        Self(path)
    }
}
impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
#[test]
fn query_command_checks_config_without_credentials_or_store_and_never_echoes_invalid_input() {
    let config = ConfigFile::new(
        "listen=[\"127.0.0.1:0\"]\ndata_dir=\"/missing-archive\"\n[tls]\ncert_file=\"/missing-cert\"\nkey_file=\"/missing-key\"\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_masu-query"))
        .arg("check-config")
        .arg(&config.0)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    let invalid = ConfigFile::new("secret=\"must-not-be-printed\"");
    let output = Command::new(env!("CARGO_BIN_EXE_masu-query"))
        .arg("check-config")
        .arg(&invalid.0)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(!error.contains("must-not-be-printed"));
    assert!(!error.contains(invalid.0.to_str().unwrap()));
    assert!(error.contains("query-start-failed"));
}
#[test]
fn query_command_has_an_independent_version_and_usage_exit() {
    let output = Command::new(env!("CARGO_BIN_EXE_masu-query"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("masu-query {}\n", env!("CARGO_PKG_VERSION"))
    );
    let output = Command::new(env!("CARGO_BIN_EXE_masu-query"))
        .arg("verify")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
}
