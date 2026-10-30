# ADR 0012 — Three binaries: data CLI, datastore, query daemon

Status: Accepted — 2026-10-30 (owner decision)

## Context

One `masu` binary validated `/metrics` expositions, read and verified stores, and ran the store
server from a configuration file. Other repositories call the validator in their tests
(`lib.validateMetrics`), operators call `read` and `verify` on a store or a backup, and a query
daemon for yagura and hansho will read the same files with its own configuration. A binary
that mixes data tools with a server's configuration grows options that only one of its uses
needs. nisshi made the same split (nisshi ADR 0016).

## Options considered

- **One binary with subcommands**: one package, but the data tools carry the server's
  configuration and dependencies, and a third role makes it larger again.
- **One binary per role over one library**: the exposition parser, the metric convention, the
  scrape-record rules, the format and the commit, seal and retention logic live once, in a
  library; each binary owns its configuration.

## Decision

A Cargo workspace:

- **`masu-core`** (library): the single implementation of the exposition parser, the metric
  convention and its validator, the scrape-record rules, the storage format, commit, recovery,
  seal, retention, `read` and `verify`.
- **`masu`** (data CLI): `validate --metrics`, `read`, `verify`, with the arguments, output and
  exit codes of before. It reads no configuration; it knows only data and its format.
  `lib.validateMetrics` still names it.
- **`masu-store`** (datastore): the only writer of a store root: ingestion, sealing,
  retention, `/v1/status`, `/metrics`. `masu-store serve <config.toml>` serves,
  `masu-store check-config <config.toml>` checks a configuration and exits non-zero with a
  message when it is invalid, `masu-store --version` prints the version. The NixOS module
  runs `check-config` on the rendered file at build time.
- **`masu-query`** (planned, not built): a read-only query daemon over the files, with its own
  configuration and `check-config`. The name is reserved for it.

Each binary validates its own configuration. `masu-pusher` (ADR 0009) is a fourth crate, for
tests only, not shipped.

## Consequences

- `masu serve` and `masu check-config` are gone: `services.masu` runs `masu-store`, with the
  same options and defaults; the package holds both binaries.
- The HTTP API, the archive format, `/metrics` and the log lines do not change.
