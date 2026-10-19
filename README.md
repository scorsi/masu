# masu (枡)

> Metric store of the sekkeizu observability stack: receives scrapes from
> [hikyaku](../hikyaku), archives them raw and losslessly today, becomes the time-series engine
> later. Owns the metric naming convention. Rust.

*枡 (masu): the wooden measuring box of Edo markets, the standard every quantity was measured
against.*

**Status:** designed (ADRs 2026-10-19), no code yet.

## Role

- **Now**: ingestion API (`masu.v1`), raw scrape archive (zstd NDJSON, one file per node and
  receive day), `masu read` (samples filtered by node, service, metric and time), the
  **metric convention** and `masu validate`.
- **Later** (own milestones, not in the minimal base): the time-series engine (Gorilla-style
  blocks, downsampling, range queries) built from the raw archive, and a query API for
  [yagura](../yagura) and [hanshou](../hanshou).

## Not its job

Logs ([nisshi](../nisshi)), dashboards, alerting, host metrics ([mihari](../mihari)).

## Is every node still delivering?

Silent-node detection is deliberately outside the minimal base; it comes with the rest of the
observability stack. Until then, by hand, with a `reader` token:

```sh
curl -s --cacert masu-ca.pem -H "Authorization: Bearer $(cat reader-token)" \
  https://<store>:9472/v1/status \
  | jq -r '.streams[] | [.node, (.lastReceiveNs | tonumber / 1e9 | todate)] | @tsv'
```

A node whose last receive is minutes old while its agent should be pushing is silent; the
agent's `heartbeat` events (every 5 minutes) make that true for logs too. `.agents` shows each
agent's spool state (`paused`, oldest unacked record).

## Documents

- [ADRs](docs/adr/README.md), [metric convention](docs/metric-convention.md),
  [storage format](docs/storage-format.md), [ingestion API](proto/masu/v1/ingest.proto),
  [milestones](docs/milestones.md), [resources](docs/resources.md)

## Relationships

| Direction | Project | What |
|---|---|---|
| fed by | hikyaku | `masu.v1` ingestion API |
| convention used by | every house service | `/metrics` on loopback, `masu validate` in their tests |
| read by | yagura, hanshou, kenbun (later) | the raw archive now, a query API later |

## Conventions

Nix flake (crane), NixOS module, `nixosTest`. Docs in English. Public repository: no host
names, domains or addresses of any deployment.
