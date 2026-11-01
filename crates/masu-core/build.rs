fn main() -> std::io::Result<()> {
    // `bytes` fields decode as `Bytes` slices of the body: no copy of each record.
    prost_build::Config::new()
        .bytes(["."])
        .btree_map([
            ".masu.v1.QueryRequest.queries",
            ".masu.v1.QueryResponse.results",
        ])
        .compile_protos(
            &[
                "../../proto/masu/v1/ingest.proto",
                "../../proto/masu/v1/query.proto",
            ],
            &["../../proto"],
        )
}
