# ADR 0001 — Scope and boundaries

Status: Accepted — 2026-10-19

## Context

Nothing is stored today. A baseline cannot be rebuilt afterwards, so collection must start
before any query engine exists. The time-series engine (Gorilla-style compression,
downsampling, range queries) is a learning goal of its own and deserves time; dashboards
([yagura](../../../yagura)) and alert rules ([hanshou](../../../hanshou)) will query it later.
Logs live in [nisshi](../../../nisshi), with no shared code.

## Options considered

- **Build the TSDB first, collect later**: the months spent on it are months of baseline lost.
- **Prometheus from nixpkgs now, own TSDB later**: Prometheus's TSDB blocks are readable by its
  tools only, and its retention would have to be kept until re-ingestion; and scraping from a
  central point does not reach the public edge node, nor survive an outage.
- **Archive raw scrapes now, build the engine from the archive later**.

## Decision

masu has two phases in one product:

- **Archive (minimal base)**: ingestion API (0003), raw scrape storage (0004), sealing and
  retention (0006), `masu read`, `/v1/status` (0008), the metric convention (0005).
- **Engine (later, own ADRs and milestones)**: blocks built from the archive, query API. The
  archive stays the source of truth: the engine can be rebuilt from it at any time, and its
  format can change freely.

## Consequences

- The archive format is a contract (storage-format.md); the engine's format is internal.
- hikyaku depends on masu (protos, end-to-end tests); masu tests itself with a test pusher.
