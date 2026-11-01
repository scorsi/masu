use masu_core::{read, record};
use std::fs;

#[test]
fn query_json_features_preserve_ingestion_and_read_numeric_contract() {
    let base = r#"{"v":1,"ts":"2026-11-01T00:00:00.000000000Z","node":"node-a","service":"annai","stream":"11111111111111111111111111111111","seq":1,"up":true,"dur_us":1,"content_type":"text/plain","body":"annai_up 1\n"}"#;
    for field in ["seq", "dur_us"] {
        for number in ["1e3", "1.0", "18446744073709551616"] {
            let input = base.replace(&format!("\"{field}\":1"), &format!("\"{field}\":{number}"));
            assert_eq!(
                record::check(input.as_bytes()).unwrap_err(),
                format!("{field}-format")
            );
        }
        let input = base.replace(
            &format!("\"{field}\":1"),
            &format!("\"{field}\":{}", u64::MAX),
        );
        assert!(record::check(input.as_bytes()).is_ok());
    }
    let oversized_number = format!("{},\"extra\":1e400}}", base.strip_suffix('}').unwrap());
    assert_eq!(
        record::check(oversized_number.as_bytes()).unwrap_err(),
        "not-json"
    );

    struct Temporary(std::path::PathBuf);
    impl Drop for Temporary {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let root = Temporary(
        std::env::temp_dir().join(format!("masu-query-record-compat-{}", std::process::id())),
    );
    let dir = root.0.join("archive/node-a");
    fs::create_dir_all(&dir).unwrap();
    fs::write(root.0.join("FORMAT"), masu_core::store::FORMAT).unwrap();
    let data = dir.join("2026-11-01.ndjson.zst");
    let query = read::Query {
        node: "node-a".into(),
        service: None,
        metric: None,
        from: None,
        to: None,
        raw: true,
    };
    let exact = format!(
        "{}\n",
        base.replace("\"seq\":1", &format!("\"seq\":{}", u64::MAX))
    );
    let exact = exact.trim_end().strip_suffix('}').unwrap().to_string()
        + ",\"recv_ts\":\"2026-11-01T00:00:01.000000000Z\"}\n";
    fs::write(&data, zstd::encode_all(exact.as_bytes(), 3).unwrap()).unwrap();
    let mut out = Vec::new();
    read::read(&root.0, &query, &mut out).unwrap();
    assert_eq!(out, exact.as_bytes());
    fs::write(
        &data,
        zstd::encode_all(format!("{oversized_number}\n").as_bytes(), 3).unwrap(),
    )
    .unwrap();
    let mut out = Vec::new();
    assert!(read::read(&root.0, &query, &mut out).is_err());
    assert!(out.is_empty());
}
