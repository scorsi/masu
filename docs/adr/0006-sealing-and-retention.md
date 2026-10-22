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

## Amendment — 2026-10-22

Status: Accepted — 2026-10-22 (owner decision, after the code review of 2026-10-21).

- **A sealed day is never reopened nor rewritten.** If the store's clock puts a push on a day
  that is sealed, or whose seal is running, masu refuses the whole push with **503**, logs a
  `push-failed` error event and changes nothing on disk. The agent keeps its spool and retries;
  pushes succeed again once the clock is right. A push made only of duplicates stores nothing
  and is still answered 200.
- **Seal order.** Scan, recompress to a temporary file and check it, then under the store lock:
  write the manifest (temporary + rename, directory `fsync`), rename the recompressed file over
  the original (directory `fsync`), drop the file from `state.json`. A start that finds a day
  still in `state.json` with a manifest checks that the file, original or recompressed, holds
  exactly what the manifest records and takes the day as sealed; it never resizes it.
- **Recovery refuses lost data.** An open file shorter than its committed length, or missing,
  stops the start with an error instead of being padded or forgotten: its `hwm` would
  acknowledge records the store no longer has.
- **Backup and restore** (owner decision 2026-10-22: backups twice a day, RPO about 12 h).
  Backups hold `FORMAT`, the sealed days (data files and manifests) and, for each open day, the
  prefix of its data file up to its committed length, as `<day>.ndjson.zst.partial`; never
  `state.json` (it may acknowledge records the backup does not hold). To restore: copy the
  backed-up files into an empty data directory, rename each `.partial` to `.ndjson.zst`, start
  masu. A start without `state.json` rebuilds each stream's `hwm` and owner node (ADR 0011)
  before it accepts pushes, logging one `state-rebuilt` event: from every manifest (largest
  `last_seq`; checked against its data file when the file is there, a mismatch stops the
  start) and from the complete frames of every day file without a manifest, which stays open at
  that length (a truncated last frame is cut off). Records pushed after the backup and lost
  with the disk come back as a `seq-gap`; records the backup holds are never stored twice.
  With `state.json` present, recovery is unchanged: a day file absent from it and without a
  manifest is an interrupted first commit, truncated to zero.
