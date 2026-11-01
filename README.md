# masu (枡)

> Metric store of the sekkeizu observability stack: receives scrapes from
> [hikyaku](../hikyaku), archives them raw and losslessly today, becomes the time-series engine
> later. Owns the metric naming convention. Rust.

*枡 (masu): the wooden measuring box of Edo markets, the standard every quantity was measured
against.*

**Status:** archive, bounded query daemon and remote data CLI implemented. The package ships
`masu`, `masu-store` and `masu-query`; mTLS remains M2. No deployment is implied.

## Role

- **Now**: ingestion API (`masu.v1`), raw scrape archive (zstd NDJSON, one file per node and
  receive day), bounded batched series/health/discovery queries over HTTPS, direct
  `masu-store read` exports, the **metric convention** and `masu validate`.
- **Later** (own milestones, not in the minimal base): the time-series engine (Gorilla-style
  blocks) built from the raw archive, replacing bounded scans behind the same query contract
  for [yagura](../yagura) and [hansho](../hansho).

## Binaries

| Binary | Commands |
|---|---|
| `masu` | `validate --metrics`, authenticated HTTPS `query` |
| `masu-store` | `serve`, `check-config`, direct `read --root`, `verify --root`, `--version` |
| `masu-query` | read-only `serve`, `check-config`, `--version` |

ADRs [0013](docs/adr/0013-query-document.md), [0014](docs/adr/0014-masu-query.md) and
[0015](docs/adr/0015-remote-data-cli.md) define the query document, operators and CLI split.
The daemon accepts one bounded batch over `POST /v1/query`, protobuf or ProtoJSON. It reads
committed archive snapshots once per relevant file, shares parsed scrapes across the batch,
and evaluates raw/stepped series, reductions, rates, scrape health and discovery. See
[the query contract](docs/query-document.md) for limits, paging and failure semantics.
The engine, other stores' query daemons and Yagura consumers are separate work.

## Remote queries

The client needs an HTTPS **origin** (no credentials, path, query or fragment), a reader
**token file**, and optionally a PEM CA file. Without `MASU_CA_FILE`, it uses
`NIX_SSL_CERT_FILE`, then `SSL_CERT_FILE`, then the platform CA bundle. Both certificate
chain and hostname are verified. Redirects are refused; there is no insecure mode.

```sh
export MASU_QUERY_URL=https://masu-query.example.test
export MASU_TOKEN_FILE=/run/secrets/masu-reader
export MASU_CA_FILE=/etc/masu-ca.pem
masu query --node node-a --service azukari \
  --metric azukari_last_snapshot_timestamp_seconds \
  --from -1d --to -0s --reduce last
```

`--metric` is an exact sample name; repeated `--label key=value` flags are equality matchers
(a series may have additional labels). Take actual metric names and labels from the
exporter's contract. Times accept RFC 3339, including fractional seconds and UTC offsets,
or negative integer offsets from one captured current time. Duration units are
`ns`, `us`, `ms`, `s`, `m`, `h`, `d`; `--step` is a positive integer duration. `--from` is
inclusive and `--to` exclusive. `--reduce last|min|max|avg|count` and `--rate` follow ADR 0014;
`--rate` needs `--step` and cannot combine with `--reduce`.

Text output is `ts node service series value`, sorted chronologically across series, with
canonical label ordering and nine fractional UTC timestamp digits. Labels are escaped as in
direct reads. `--json` prints the validated ProtoJSON response exactly as received. The
response is bounded to 16 MiB before decoding; validation completes before stdout is written.
HTTP/TLS/timeout/encoding/selection failures exit **2** with empty stdout. A per-query error
inside HTTP 200 exits **3**: text stdout is empty; `--json` prints that validated error response.
The connect/response deadline is ten seconds. Only series queries have CLI flags; batched
health/discovery clients use the query document API.

## Direct archive tools

```sh
masu-store read --root /var/lib/masu --node node-a --service annai \
  --metric annai_queries --from 2026-11-01T00:00:00.000000000Z \
  --to 2026-11-01T01:00:00.000000000Z
masu-store read --root /path/to/restored-backup --node node-a --raw
masu-store verify --root /path/to/restored-backup
```

Direct reads retain the previous optional flags, literal metric **prefix**, inclusive
`--from`/`--to`, output ordering, failed/parse-error marker lines, and one-million-row limit.
Their timestamps use the archive's nine-fractional-digit UTC form. `--raw` retains the stored
NDJSON bytes (trailing whitespace normalized and LF appended), and ignores `--metric` as
before. These export the retained files; manifest-only days removed by retention are skipped,
whereas a query needing such a day returns `archive-incomplete`.

Live reads and verification see committed prefixes, excluding complete or partial uncommitted
tails. A restored root works without a server or `state.json`: a shared read-only FORMAT lock
excludes an active writer/rebuild for the operation. No state is rebuilt and no file is
truncated, recovered or otherwise repaired. Verification retains exit **0** for a sound
archive, **1** for integrity errors and **2** when it cannot read the archive safely.
`masu read` and `masu verify` now print command-specific migration help and exit 2.
`masu validate --metrics` and `lib.validateMetrics` are unchanged.

## NixOS query service and client

`services.masu.query` is independent of `services.masu.enable`, so it can read a provisioned
store root without starting its writer. Configure `enable`, `listen`, `dataDir`,
`tls.certFile`, `tls.keyFile`, and token SHA-256 hashes in `tokens`. The service has read-only
archive access and opens no firewall port.

`programs.masu.enable` installs the package for client use, without enabling either daemon.
Set `queryUrl`, `tokenFile`, and optional `caFile`; the module supplies the three environment
variables above. Token values stay in files, not command arguments or Nix store values.
No deployment or credential provisioning is performed by these options automatically.

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
| read by | yagura, hansho, kenbun (later) | the raw archive now, a query API later |

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
