fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["proto/masu/v1/ingest.proto"], &["proto"])
}
