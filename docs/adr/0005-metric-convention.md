# ADR 0005 — Metric convention and its publication

Status: Accepted — 2026-10-19

## Context

Services in five languages will expose `/metrics`. Cardinality must stay bounded: no source
IP, domain or request id as a label. Names must stay comparable across services and years for
the future engine and dashboards.

## Decision

- The convention is [metric-convention.md](../metric-convention.md): Prometheus naming
  (`<service>_<what>_<unit>`, base units, `_total` counters), labels from closed sets only, a
  forbidden-label list, a budget of 500 series per service, `/metrics` on loopback only.
- **Enforcement in two places**: `masu validate --metrics <url|file>` in each service's
  nixosTest (masu as a test-only input), and the agent's `max_series` refusal at scrape time
  (hikyaku ADR 0003).
- Changing the meaning or unit of an existing metric means a new name; old names stay
  interpretable in the archive.
- **Harmonisation by validation, not by library** (same reasoning as nisshi ADR 0004): no
  shared metrics library per language; conformity is proven by `masu validate` on each
  service's real `/metrics` in its nixosTest. metric-convention.md lists the exposition
  pitfalls a hand-written endpoint must avoid, with a reference histogram. When togi exists, its
  reusable nixosTest helper runs this validation for a service that declares its endpoints. A
  shared library comes only when several services of one language rewrite the same code, and
  never in togi (development tooling, not runtime code).

## Consequences

- A service repository takes masu as an input for tests only.
