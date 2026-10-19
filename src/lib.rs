//! masu: metric store. M0 holds the contracts other repositories test against: the exposition
//! parser, the metric convention validator and the scrape-record rules.

pub mod convention;
pub mod exposition;
pub mod record;

/// The ingestion API (ADR 0003), generated from `proto/masu/v1/ingest.proto` by build.rs.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/masu.v1.rs"));
}
