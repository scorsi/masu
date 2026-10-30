fn main() -> std::io::Result<()> {
    // `bytes` fields decode as `Bytes` slices of the body: no copy of each record.
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["../../proto/masu/v1/ingest.proto"], &["../../proto"])
}
