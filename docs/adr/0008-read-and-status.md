# ADR 0008 — Reading and status

Status: Accepted — 2026-10-19

## Decision

- `masu read <root> --node <n> [--service <s>] [--metric <name or prefix>] --from --to`: reads
  the archive files directly (works on a backup copy), prunes days with manifests, parses the
  bodies and prints `ts node service series value` lines sorted by `ts`; `--raw` prints the
  matching records as NDJSON instead. Failed scrapes print as `ts node service !error`.
- `masu verify <root>`: content hashes, `(stream, seq)` uniqueness.
- `GET /v1/status` (identity `reader`): `StatusResponse` of
  [ingest.proto](../../proto/masu/v1/ingest.proto) in the protojson mapping (lowerCamelCase,
  64-bit integers as strings, as nisshi's Go `protojson`; serde structs with
  `rename_all = "camelCase"`, no extra crate): per stream the last receive time
  and newest `ts`, the last agent status per node, free disk.
- Own metrics on a loopback listener: `masu_*` of the metric convention.

## Consequences

- Silent-node detection is glue over `/v1/status` built later on another component; masu
  only exposes what it needs.
