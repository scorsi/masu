# ADR 0003 — Ingestion API and delivery semantics

Status: Accepted — 2026-10-19

Superseded in part by [0010](0010-forbidden-for-wrong-role.md) (403 for a valid token without the role).
Amended by [0011](0011-stream-owner.md) (stream owner) and the 0006 amendment (503).

## Context

Agents push (the public edge node cannot be scraped from inside). Nothing may be lost
silently, nothing may appear twice. masu stores raw scrapes (0004), not samples.

## Options considered

- **Prometheus remote-write**: carries samples, not the raw scrape (HELP, TYPE, units,
  histogram shape would be rebuilt), and has no stream sequence to deduplicate on.
- **OTLP metrics**: the exposition format would be converted into OTLP points and back; large
  proto set.
- **gRPC**: a gRPC stack for one request/response.
- **House protobuf over HTTPS POST**: one endpoint, `curl`-debuggable.

## Decision

`POST /v1/push`, body = `PushRequest` ([ingest.proto](../../proto/masu/v1/ingest.proto)),
`Content-Type: application/x-protobuf`, `Content-Encoding: zstd`, HTTPS, HTTP/1.1 only
(amendment 2026-10-22, below). At most 8 MiB compressed, 64 MiB decompressed; zstd window at most 8 MiB (a larger
one is `body-zstd`, amendment 2026-10-22).

Resources (amendment 2026-10-22, review F07, F08): one push is processed at a time (body,
decode, commit; the seal takes the same turn), others wait at most 25 s (under hikyaku's 30 s
request timeout) and then get **503** with nothing read or stored: the agent backs off and
retries (amendment 2026-10-22; a long seal or a stuck `fsync` holds the slot, and the
server's deadlines do not end a waiting handler). The seal itself waits its turn without a
deadline. At most 32 connections; 10 s for
the TLS handshake and for the request headers, 30 s for the body, 10 minutes per connection.
`AgentStatus.version` is kept to 64 bytes.

Early refusals (amendment 2026-10-22, nisshi validation review N1): an answer sent before the
body is read (the 503 of the push slot, 401, 403, 413 on `Content-Length`, 400
`content-encoding`) never waits for that body. hyper, once the handler has dropped an unread
body, makes one non-blocking read of it and otherwise stops reading, disables keep-alive and
closes the connection after the response: a declared body that never arrives delays neither
the answer nor the close (test `push_admission_deadline`). masu adds no `Connection: close`
of its own.

HTTP/1.1 only (amendment 2026-10-22, validation of the review fixes): hyper bounds the time
of HTTP/1 request headers, but has no such bound for an HTTP/2 preface or HEADERS frame, so
an HTTP/2 connection could hold one of the 32 slots for its whole 10 minutes. ALPN offers
`http/1.1` only; an HTTP/2 preface is answered as a malformed HTTP/1 request and the
connection closed. hikyaku speaks HTTP/1.1. HTTP/2 comes back with a bound on its preface and
headers.

Streams and sequence numbers as in nisshi's API: masu keeps per stream the highest committed
`seq` (`hwm`), committed atomically with the data.

| Request | masu does | Response |
|---|---|---|
| `first_seq > hwm + 1` | stores the records, counts `masu_seq_gaps_total`, logs a `seq-gap` event (`warn`) | 200 |
| overlaps `hwm` | drops records with `seq <= hwm`, stores the rest | 200, `acked_seq` |
| all `seq <= hwm` | stores nothing, counts `masu_duplicate_records_total` | 200, `acked_seq = hwm` |
| no or bad credentials | nothing | 401 |
| a record invalid: not a JSON object, envelope invalid under the published rules (storage-format, fixtures), a store field present (`recv_ts`), `node`/`stream`/`seq` not matching the request | nothing (whole batch) | 400, `PushRejected{record_index, reason}` of the first invalid record |
| the request itself invalid: `node` ≠ authenticated node, malformed `stream_id`, `first_seq` = 0 | nothing | 400, `PushRejected{reason}` without index |
| the stream exists and belongs to another node (checked after the request-level 400s, before the records) | nothing | 403, `PushRejected{reason: "stream-owner"}` without index (ADR 0011) |
| free space below `disk_reserve`, or a write error | rolls back to last commit | 507 |
| a write error after the new `state.json` is renamed in (its directory `fsync` failed): it may already be on disk | keeps the data and the new state (no rollback), stops the writer | 507 (ADR 0006 amendment) |
| the store clock puts the push on a sealed day, or the writer stopped (a failed rollback, or the case above) | nothing | 503 (ADR 0006 amendment) |
| body too large: over 8 MiB compressed, over 64 MiB decompressed, or more records than 64 MiB of valid records can hold (each is at least 154 bytes) | nothing | 413 |
| body not received within 30 s | nothing | 408 |
| the push slot not free within 25 s (amendment 2026-10-22) | nothing, body not read | 503 |

A batch is all or nothing. Delivery is at least once on the wire, exactly once on disk.
Records are stored as received, plus `recv_ts` (commit time, same format as `ts`) appended as
the last member of each object.

A record-level 400 never blocks a stream: `record_index` lets the agent replace that record
and continue (hikyaku ADR 0005), as with nisshi (nisshi ADR 0003). The record validation rules
are published as fixtures (0009) and run by hikyaku's tests.

masu does not parse the scrape bodies at ingestion: a malformed exposition is stored as received
and flagged by `masu read` and the engine.

## Consequences

- Same client logic in hikyaku for both stores; the two protos are separate contracts that
  happen to have the same shape and may diverge.
