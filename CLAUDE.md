# masu — notes for AI assistants

Read [`docs/adr/`](docs/adr/) before making an architectural choice. [`docs/milestones.md`](docs/milestones.md)
is the plan: its acceptance criteria are the definition of done for a milestone.

## Mirrored with nisshi (`~/repositories/nisshi`)

These files exist, byte-identical, in both repositories (masu ADR 0009, nisshi ADR 0012).
Neither repository depends on the other, so nothing enforces it but this rule:

- `fixtures/delivery/` (every file)
- `docs/delivery-scenarios.md`

A change to one of them is made in **both** repositories, together with the store-side change
it implies (Rust here, Go in nisshi), before either is committed. Check with
`sha256sum fixtures/delivery/* docs/delivery-scenarios.md` in both: same output.

The delivery semantics themselves (ADR 0003: streams, `hwm`, the response table,
`PushRejected`, `recv_ts`) mirror nisshi ADR 0003; a change to one is proposed for the other.

## Contracts owned here

- `docs/metric-convention.md` and `masu validate` are the metric rules for every house service.
- `fixtures/records/` are the scrape-record rules; hikyaku runs them too: a change here is a
  change to hikyaku's record builder (`~/repositories/hikyaku`).
- `proto/masu/v1/` is the ingestion API; hikyaku compiles it.
- `docs/storage-format.md` (the archive) is a public contract and the source the future engine
  is built from: a change is a new format version, never an in-place change.

## Conventions

- Crates limited to ADR 0002; any other crate is an ADR.
- Binaries (ADR 0012): `masu-core` (library), `masu` (data CLI), `masu-store` (datastore),
  `masu-pusher` (tests only). `masu-query` is reserved for the future query daemon.
- Check with `nix develop -c cargo test --workspace`,
  `nix develop -c cargo clippy --workspace --all-targets -- -D warnings`.
- Format with `nix fmt`.
- Public repository: no host names, domains, addresses or keys of any deployment.
- Code, comments, docs and commits in English.
