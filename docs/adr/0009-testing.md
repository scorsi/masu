# ADR 0009 — Testing

Status: Accepted — 2026-10-19

## Decision

- **Rust tests** (`cargo test` through crane in `nix flake check`): exposition parser against a
  corpus (real node_exporter output, OpenMetrics examples, malformed inputs), validator rules,
  the response table of 0003, commit and recovery with a crash injected at every step, seal
  idempotence.
- **nixosTests** are the definition of done, with standard tools as the oracle (`zstdcat`, `jq`,
  `sha256sum`, `curl`). masu tests itself with a test pusher (a small binary in the
  repository, not shipped); end-to-end tests with the agent live in hikyaku.
- **Delivery scenarios are mirrored data** (nisshi ADR 0012): `fixtures/delivery/` and
  `docs/delivery-scenarios.md` are byte-identical copies of nisshi's; masu's pusher runs every
  scenario of its own copy. No dependency on nisshi, not even for tests; the mirror rule is in
  `CLAUDE.md`.
- **Record fixtures**: `fixtures/records/records.ndjson`, exported by the flake
  (`lib.fixtures`), same line format as nisshi's (`{"input", "valid", "reason"}`); `reason` is
  the exact `PushRejected.reason`. masu's ingestion and hikyaku's scrape-record builder both run
  them.
- A breakage seen on real hosts becomes a nixosTest before it is fixed.
