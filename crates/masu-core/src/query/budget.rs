//! Request-wide query budgets, charged before growing buffers or doing more work.
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub const MAX_REQUEST_BYTES: usize = 64 << 10;
pub const MAX_RESPONSE_BYTES: u64 = 16 << 20;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub metadata_bytes: u64,
    pub files: u64,
    pub directory_entries_per_node: u64,
    pub compressed_bytes: u64,
    pub decompressed_bytes: u64,
    pub records: u64,
    pub samples: u64,
    pub result_items: u64,
    pub response_bytes: u64,
    pub wall_time: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            metadata_bytes: 4 << 20,
            files: 1024,
            directory_entries_per_node: 4096,
            compressed_bytes: 128 << 20,
            decompressed_bytes: 512 << 20,
            records: 500_000,
            samples: 5_000_000,
            result_items: 2_000_000,
            response_bytes: MAX_RESPONSE_BYTES,
            wall_time: Duration::from_secs(10),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum Charge {
    Metadata,
    File,
    Compressed,
    Decompressed,
    Record,
    Sample,
    ResultItem,
    Response,
}
#[derive(Default, Debug)]
pub struct Usage {
    pub metadata_bytes: u64,
    pub files: u64,
    pub compressed_bytes: u64,
    pub decompressed_bytes: u64,
    pub records: u64,
    pub samples: u64,
    pub result_items: u64,
    pub response_bytes: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ScanLimit,
    ResponseLimit,
    Cancelled,
    Timeout,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ScanLimit => "scan-limit",
            Self::ResponseLimit => "response-limit",
            Self::Cancelled => "query-cancelled",
            Self::Timeout => "query-timeout",
        })
    }
}
impl std::error::Error for Error {}

pub struct Budget {
    limits: Limits,
    pub usage: Usage,
    started: Instant,
    cancelled: Arc<AtomicBool>,
    directory_entries: BTreeMap<String, u64>,
    exhausted: Option<Error>,
}
impl Default for Budget {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}
impl Budget {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            usage: Usage::default(),
            started: Instant::now(),
            cancelled: Arc::new(AtomicBool::new(false)),
            directory_entries: BTreeMap::new(),
            exhausted: None,
        }
    }
    /// The caller can cancel a blocking scan without waiting for its async wrapper.
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
    pub fn check(&self) -> Result<(), Error> {
        if let Some(error) = self.exhausted {
            return Err(error);
        }
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(Error::Cancelled);
        }
        if self.started.elapsed() >= self.limits.wall_time {
            return Err(Error::Timeout);
        }
        Ok(())
    }
    pub fn charge(&mut self, kind: Charge, amount: u64) -> Result<(), Error> {
        self.check()?;
        let (used, limit, error) = match kind {
            Charge::Metadata => (
                &mut self.usage.metadata_bytes,
                self.limits.metadata_bytes,
                Error::ScanLimit,
            ),
            Charge::File => (&mut self.usage.files, self.limits.files, Error::ScanLimit),
            Charge::Compressed => (
                &mut self.usage.compressed_bytes,
                self.limits.compressed_bytes,
                Error::ScanLimit,
            ),
            Charge::Decompressed => (
                &mut self.usage.decompressed_bytes,
                self.limits.decompressed_bytes,
                Error::ScanLimit,
            ),
            Charge::Record => (
                &mut self.usage.records,
                self.limits.records,
                Error::ScanLimit,
            ),
            Charge::Sample => (
                &mut self.usage.samples,
                self.limits.samples,
                Error::ScanLimit,
            ),
            Charge::ResultItem => (
                &mut self.usage.result_items,
                self.limits.result_items,
                Error::ResponseLimit,
            ),
            Charge::Response => (
                &mut self.usage.response_bytes,
                self.limits.response_bytes,
                Error::ResponseLimit,
            ),
        };
        match used.checked_add(amount).filter(|n| *n <= limit) {
            Some(n) => {
                *used = n;
                Ok(())
            }
            None => {
                self.exhausted = Some(error);
                Err(error)
            }
        }
    }
    pub fn directory_entry(&mut self, node: &str) -> Result<(), Error> {
        self.check()?;
        let n = self.directory_entries.entry(node.into()).or_default();
        if *n >= self.limits.directory_entries_per_node {
            self.exhausted = Some(Error::ScanLimit);
            return Err(Error::ScanLimit);
        }
        *n += 1;
        Ok(())
    }
    pub fn remaining(&self, kind: Charge) -> u64 {
        let (limit, used) = match kind {
            Charge::Metadata => (self.limits.metadata_bytes, self.usage.metadata_bytes),
            Charge::Compressed => (self.limits.compressed_bytes, self.usage.compressed_bytes),
            Charge::Decompressed => (
                self.limits.decompressed_bytes,
                self.usage.decompressed_bytes,
            ),
            Charge::File => (self.limits.files, self.usage.files),
            Charge::Record => (self.limits.records, self.usage.records),
            Charge::Sample => (self.limits.samples, self.usage.samples),
            Charge::ResultItem => (self.limits.result_items, self.usage.result_items),
            Charge::Response => (self.limits.response_bytes, self.usage.response_bytes),
        };
        limit.saturating_sub(used)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_budgets_each_ceiling_and_sticky_refusal() {
        let limits = Limits::default();
        for (kind, ceiling) in [
            (Charge::Metadata, limits.metadata_bytes),
            (Charge::File, limits.files),
            (Charge::Compressed, limits.compressed_bytes),
            (Charge::Decompressed, limits.decompressed_bytes),
            (Charge::Record, limits.records),
            (Charge::Sample, limits.samples),
            (Charge::ResultItem, limits.result_items),
            (Charge::Response, limits.response_bytes),
        ] {
            let mut b = Budget::default();
            b.charge(kind, ceiling).unwrap();
            assert!(b.charge(kind, 1).is_err());
            assert!(b.charge(kind, 0).is_err());
            let mut b = Budget::default();
            assert!(b.charge(kind, u64::MAX).is_err());
        }
        let mut b = Budget::new(Limits {
            result_items: 2,
            ..Limits::default()
        });
        b.charge(Charge::ResultItem, 2).unwrap();
        assert_eq!(b.charge(Charge::ResultItem, 1), Err(Error::ResponseLimit));
        assert_eq!(b.usage.result_items, 2);
    }
    #[test]
    fn query_budgets_cancel_deadline_and_per_node_entries() {
        let b = Budget::default();
        b.cancellation().store(true, Ordering::Relaxed);
        assert_eq!(b.check(), Err(Error::Cancelled));
        let b = Budget::new(Limits {
            wall_time: Duration::ZERO,
            ..Limits::default()
        });
        assert_eq!(b.check(), Err(Error::Timeout));
        let mut b = Budget::new(Limits {
            directory_entries_per_node: 1,
            ..Limits::default()
        });
        b.directory_entry("node-a").unwrap();
        b.directory_entry("node-b").unwrap();
        assert_eq!(b.directory_entry("node-a"), Err(Error::ScanLimit));
    }
}
