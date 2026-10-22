# ADR 0011 — A stream belongs to the node that first pushed it

Status: Accepted — 2026-10-22 (owner decision, after the code review of 2026-10-21)

Amends [0003](0003-ingestion-api.md) (response table) and [0010](0010-forbidden-for-wrong-role.md)
(a second 403 case).

## Context

Streams are keyed by `stream_id` alone. The identity checks bind `PushRequest.node` and every
record's `node` to the bearer token, but nothing bound an existing stream to its node: a node
that learnt another node's `stream_id` (they are not secrets: `/v1/status`, the archives and
their backups show them) could push records of its own on that stream with higher `seq`s and
advance its `hwm`. The owner's next records, now below that `hwm`, would be acknowledged and
dropped, and the agent would delete them from its spool.

## Options considered

- **Key streams by `(node, stream_id)`**: isolates the nodes, but `(stream, seq)` would no
  longer be unique over the archive (storage-format.md), and nisshi keeps `stream_id` global.
- **Refuse another node's push on an existing stream**: one check, the archive contract
  unchanged.

## Decision

A stream belongs to the node that first pushed it (`StreamState.node` in `state.json`). A push
on an existing stream by another authenticated node is refused with **403** and the body
`PushRejected{reason: "stream-owner"}` without `record_index`, nothing stored, nothing changed.

The check runs under the store lock, after the request-level 400s (`node-mismatch`,
`stream-id-format`, `first-seq-zero`) and before any record check or deduplication.

The owner survives restarts (`state.json`) and the restore of the sealed days (the start
without `state.json` rebuilds each stream's owner from the manifests' directories, ADR 0006
amendment).

## Consequences

- hikyaku treats the 403 as it treats the role 403 (stop, retry after 300 s): only a
  misconfigured or hostile node gets it.
- A delivery scenario `fixtures/delivery/stream-owner.json` (two node tokens; node B pushes
  node A's stream: 403 `stream-owner`, A's `hwm` unchanged, A's next push stored once) is
  shared with nisshi (masu ADR 0009, nisshi ADR 0012).
