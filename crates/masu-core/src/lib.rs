//! masu: metric store. The contracts other repositories test against (exposition parser, metric
//! convention validator, scrape-record rules) and the archive (store, seal, read, verify).
//! Shared by the binaries `masu` (data CLI) and `masu-store` (ADR 0012).

pub mod convention;
pub mod disk;
pub mod exposition;
pub mod log;
pub mod metrics;
pub mod query;
pub mod read;
pub mod record;
pub mod seal;
pub mod store;
pub mod time;

/// The ingestion and query documents (ADRs 0003/0013), generated from `proto/masu/v1/`.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/masu.v1.rs"));
}
