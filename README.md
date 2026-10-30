# masu (枡)

> Metric store of the sekkeizu observability stack: receives scrapes from
> [hikyaku](../hikyaku), archives them raw and losslessly today, becomes the time-series engine
> later. Owns the metric naming convention. Rust.

*枡 (masu): the wooden measuring box of Edo markets, the standard every quantity was measured
against.*

**Status:** M1 (archive: `masu-store serve`, seal and retention, `masu read`, `masu verify`,
`services.masu`); mTLS is M2.

## Role

- **Now**: ingestion API (`masu.v1`), raw scrape archive (zstd NDJSON, one file per node and
  receive day), `masu read` (samples filtered by node, service, metric and time), the
  **metric convention** and `masu validate`.
- **Later** (own milestones, not in the minimal base): the time-series engine (Gorilla-style
  blocks, downsampling, range queries) built from the raw archive, and a query API for
  [yagura](../yagura) and [hanshou](../hanshou).

## Binaries

`masu` is the data CLI (`validate`, `read`, `verify`); `masu-store` is the datastore
(`serve`, `check-config`, `--version`); both build on the `masu-core` library (ADR 0012).

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
agent's spool state (`paused`, oldest unacked record) as of its **last successful push**: it is
refreshed only when a push is stored, so read it together with that node's `lastReceiveNs`,
never as a live probe. A silent agent keeps showing its last, possibly healthy, status.

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

## Testing hooks

- `MASU_TEST_NOW=<ts>` (same format as `ts`) shifts `masu-store serve`'s clock to that instant at
  start-up; it keeps running from there. Test only: the nixosTests use it to seal a day and to
  run retention without waiting for midnight.
- `masu-pusher` (crate `crates/masu-pusher`, `pkgs.masu.pusher`) runs a scenario of
  `fixtures/delivery/` or every line of `fixtures/records/records.ndjson` against a running
  store; not shipped in the package.
