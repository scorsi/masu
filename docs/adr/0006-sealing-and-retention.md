# ADR 0006 — Sealing, compaction and retention

Status: Accepted — 2026-10-19

## Context

The open day is one zstd frame per commit, a scrape or two each: about 12× on scrapes. A whole
day recompressed reaches 50–70× (resources.md). History is the priority. The integrity
requirement of the stack is on logs (nisshi ADR 0007); metrics need corruption detection, not a
tamper-evident chain.

## Decision

- **Seal** every day file of a past UTC day, at 00:10 UTC and at start-up (idempotent): scan
  (count, `min_ts`/`max_ts`, services, stream ranges, SHA-256), recompress into a temporary
  file (zstd level 9, 8 MiB window), decompress again and compare the hash, rename over the
  original, write the manifest (temporary + rename), drop the file from `state.json`. A line
  that is not valid JSON stops the seal of that file (`masu_seals_total{result="error"}`).
- **No hash chain**: the manifest's content hash detects corruption; `masu verify` checks it.
  A chain can be added by a new manifest version if the analysis layer needs it.
- **Retention**: default **400 days**, `0` keeps everything (TOML has no null); deletes data files of sealed
  days, keeps manifests. Until the engine exists, raw scrapes are the only copy: the
  deployment may set `0`.
- **Disk pressure never deletes data.** Below `disk_reserve` (default 2 GiB) pushes get 507.
- No downsampling in the archive: that is the engine's job.

## Consequences

- Disk estimates in [resources](../resources.md).
- Sealed files never change: incremental backups stay cheap.
