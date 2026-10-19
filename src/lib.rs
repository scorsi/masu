//! masu: metric store. The contracts other repositories test against (exposition parser, metric
//! convention validator, scrape-record rules) and the archive (store, seal, read, server).

pub mod config;
pub mod convention;
pub mod disk;
pub mod exposition;
pub mod log;
pub mod metrics;
pub mod read;
pub mod record;
pub mod seal;
pub mod server;
pub mod store;
pub mod time;

/// The ingestion API (ADR 0003), generated from `proto/masu/v1/ingest.proto` by build.rs.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/masu.v1.rs"));
}
