# ADR 0004 — Storage: raw scrape archive

Status: Accepted — 2026-10-19

Dashboard/engine coupling amended by [ADR 0014](0014-masu-query.md) on 2026-11-01:
bounded archive queries may serve dashboards before the engine; implementation is pending.

## Context

The archive must be readable without masu's code, lossless, and the source from which the
future engine is built. Volume is small: a few nodes, a few thousand series.

## Options considered

- **Engine blocks now (Gorilla)**: the engine is the later phase (0001); nobody can read
  them with standard tools.
- **Parquet**: open and columnar, but needs a fixed schema for histograms, summaries and label
  sets, drops HELP/TYPE/UNIT unless modelled; heavy writer crates.
- **Samples as NDJSON** (one line per sample): readable, but 20–50× more lines and label
  sets repeated on each.
- **Raw scrapes**: store exactly what `/metrics` returned, with the scrape time, as NDJSON.
  Lossless by construction (types, HELP, units, histograms, exemplars); the exposition format
  is an open standard. Consecutive scrapes are almost identical: zstd removes the repetition
  (measured 67× on a sealed file, resources.md).

## Decision

- Raw scrapes, one record per scrape attempt (envelope in
  [storage-format](../storage-format.md)): failed scrapes are records too (`up:false`,
  `error`), so missing data is explicit.
- Layout `<root>/<node>/<YYYY-MM-DD>.ndjson.zst`, day = UTC receive day, a zstd frame per
  commit, sealed daily (0006). Same reasons as nisshi's ADR 0005: append-only, a late
  delivery never reopens a sealed day.

## Consequences

- Reading one series over a long range decompresses every scrape of that target: fine for a
  CLI, not for dashboards. Dashboards wait for the engine.
- The engine reads this archive to build its blocks, and can rebuild them after a format
  change.
