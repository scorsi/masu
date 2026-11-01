# masu — milestones

Each milestone ends with passing tests (ADR 0009). The stack-wide order (with hikyaku, nisshi
and the services) is in the deployment repository. M0–M2 are the archive phase (minimal base);
the engine phase gets its own ADRs and milestones when it starts.

## M0 — Convention and validator

- Repository on Forgejo with GitHub mirror; flake (flake-parts, treefmt, crane); Cargo workspace
  limited to ADR 0002; `proto/` with `buf` (Rust generated at build time by `prost-build`, as
  in kekkai).
- Exposition parser (Prometheus text 0.0.4, OpenMetrics 1.0 text) and
  `masu validate --metrics <url|file>` with the rules of the metric convention.
- `fixtures/delivery/` and `docs/delivery-scenarios.md` copied from nisshi M0 (mirror, ADR 0009).
- `fixtures/records/records.ndjson` (ADR 0009) with exact reasons: missing envelope field,
  bad `ts`, `up:true` without `body`, `recv_ts` present, not an object, over 4 MiB.
- Flake outputs: `packages.masu`, `lib.validateMetrics` (path of the binary for other repos'
  tests), `lib.fixtures`, `nixosModules.default`.
- **Done when**: `nix flake check` green on darwin and Linux; the parser accepts a real
  node_exporter scrape and the convention's examples; the validator rejects a fixture per
  rule. This unblocks the services' instrumentation and hikyaku's scrape-record builder.

## M1 — Archive

- `masu-store serve` (ADR 0012): config TOML, unknown keys rejected (`listen`, `listen_metrics`, `data_dir`, `tls.cert_file`,
  `tls.key_file`, `tokens` = map hex SHA-256 → identity, `retention_days` (400, `0` = keep),
  `disk_reserve`).
- `POST /v1/push` with the table of ADR 0003, `GET /v1/status`, own `/metrics` on loopback.
- Commit and recovery, seal and compaction, retention keeping manifests.
- `masu read`, `masu verify`.
- NixOS module `services.masu` (config rendered with `pkgs.formats.toml`): `enable`, `package`, `listen` (required, list), `dataDir`
  (default `/var/lib/masu`, fixed path for persistence), `tls.{certFile, keyFile}`
  (`LoadCredential`), `tokens.<sha256 hex> = "node:<name>" | "reader"`, `retentionDays`
  (400; `null` rendered as `0`), `diskReserve` (2 GiB); static user `masu`, `MemoryMax=128M`, `Type=notify`,
  hardening as genkan; no firewall opening.
- **Tests (nixosTest with the test pusher)**:
  - every scenario of `fixtures/delivery/` (mirror of nisshi's, nisshi ADR 0012);
  - every record fixture through the ingestion path, with index and reason;
  - after the disk-full scenario: `masu verify` passes and `zstd -t` on every file;
  - every stored record ends with a `recv_ts` whose day is the file's day, and is otherwise
    byte-identical to what was pushed;
  - seal with a faked clock: `content_sha256` checked with `zstdcat | sha256sum` and `jq`;
  - `zstdcat | jq` on the open day and a sealed day;
  - `masu read --metric annai_queries_total` returns the pushed samples of an interval for a
    node.
- **Done when**: all M1 tests pass and resources.md holds a 24 h measurement on a real node
  (raw, open-day and sealed sizes).

## M2 — mTLS (after hanko issues SVIDs)

- `auth.methods`, SPIFFE ID mapping (ADR 0007).
- **Done when**: nixosTest with a test CA standing in for hanko-ca: certificate accepted,
  expired or foreign certificate refused, token and certificate nodes side by side.

## Planned — Query document, masu-query and remote CLI (before the engine)

[ADR 0013](adr/0013-query-document.md) (the batched query contract shared with nisshi and
nenpyo), [ADR 0014](adr/0014-masu-query.md) (`masu-query` operators, trust boundary, budgets)
and [ADR 0015](adr/0015-remote-data-cli.md) (`masu query` over HTTPS, direct reads in
`masu-store`) are **Accepted — 2026-11-01**, with implementation in progress. They unblock
Yagura Y1 (latest backup timestamp, first charts) on bounded one-pass scans of the
archive, behind the contract the engine serves later. ADR 0014 sequences the feature checks
(`query-select`, `query-rate`, `query-snapshot`, `query-budgets`, `vm-query`).

The first library slice implements the document codec, committed archive snapshots and
request-wide budgets. Its focused checks are `query-document`, `query-snapshot` and
`query-budgets`; it includes no evaluator, daemon, remote CLI or deployment.
