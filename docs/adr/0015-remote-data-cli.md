# ADR 0015 — The data CLI queries the daemon; the datastore reads files

Status: Accepted — 2026-11-01 (owner decision). Supersedes [0012](0012-binaries.md) in part.

## Context

ADR 0012 (accepted 2026-10-30) gives `masu` the direct `read` and `verify` over archive files
and no configuration. The first draft of the query proposal recorded an owner decision of the
same day in the other direction: `masu` talks HTTPS to the querier, `masu-store` alone reads
files directly, including restored backups. The two texts conflict; this ADR records the split
confirmed by the owner, and keeps it apart from the query contract
([0013](0013-query-document.md), [0014](0014-masu-query.md)).

## Options considered

- **Keep 0012 as is**: `masu read` reads files; a remote user needs a shell on the store host.
- **A compatibility `/v1/read` export** rendering today's `read` output over HTTP: a second
  time grammar, inclusive bounds, an 8 MiB cap, a prefix filter the query document does not
  have; a dead end kept alive for one command.
- **`masu query` over `/v1/query`, direct reads move to `masu-store`**: chosen. No second
  endpoint; the CLI is a client of the one contract.

## Decision

- **`masu-store read --root <root> ...`** and **`masu-store verify --root <root>`** take the
  direct entrypoints with today's flags, output, sort order and exit codes, over `masu-core`.
  A restored root works without a running daemon or state: only complete frames are read, no
  recovery or mutation; when state is absent, a shared read-only FORMAT lock excludes an
  active writer before the root is treated as offline. Live direct reads use the same
  committed-prefix rules as the daemon (ADR 0014). `--raw` and prefix reads stay here.
- **`masu query`** replaces `masu read`: `--node --service --metric [--label k=v]...
  --from --to [--step] [--reduce] [--rate] [--json]`, one `SeriesQuery` over `POST /v1/query`,
  text output in today's `ts node service series value` lines (same renderer), `--json` the
  protojson response. Times accept RFC 3339 or `-1h` offsets, resolved by the CLI. `masu read`
  prints a migration error naming `masu-store read --root`, `masu verify` one naming
  `masu-store verify --root`, never a guessed URL or an empty result. `masu validate
  --metrics` and `lib.validateMetrics` are unchanged.
- **Client configuration**: `MASU_QUERY_URL` (HTTPS origin, no credentials, path or query),
  `MASU_TOKEN_FILE` (at most 4 KiB, trailing LF removed; never an argument or a Nix store
  value), optional `MASU_CA_FILE` (otherwise system trust). One source, no precedence scheme.
  The response is read under a byte ceiling (the daemon's 16 MiB) before decoding, then
  validated before anything is written to stdout; a non-200, TLS error, timeout, oversized
  body or unexpected content type writes stderr and exits 2 with zero stdout. A 200 whose
  `QueryResult` carries an `error` (`unsupported`, `result-limit`, `archive-incomplete`…)
  writes the reason to stderr and exits 3: in text mode with zero stdout, with `--json` after
  printing the response as received, since it is the data. A test covers each exit path.
  Ten seconds for connect and response.
- **Ship together**: no release where a capability has no replacement. The package holds the
  three binaries; the module offers the querier service and a client-only installation that
  does not enable the datastore, with URL and file paths, never token values. In-repository
  callers, fixtures and backup commands move in the same change; other repositories' callers
  are listed before release and migrated under their own scope.

## Consequences

- `masu` reads one thing besides data: three client environment variables. ADR 0012's "it
  reads no configuration" is amended to that.
- Large or raw exports are a store-host operation (`masu-store read`), bounded by the existing
  one-million-row limit, not by the daemon's budgets.
- The owner confirmed this split on 2026-11-01. Today's commands remain unchanged until
  the complete replacements ship together.
