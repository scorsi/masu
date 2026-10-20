# ADR 0003 — Ingestion API and delivery semantics

Status: Accepted — 2026-10-19

Superseded in part by [0010](0010-forbidden-for-wrong-role.md) (403 for a valid token without the role).

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
`Content-Type: application/x-protobuf`, `Content-Encoding: zstd`, HTTPS (HTTP/2 when
negotiated). At most 8 MiB compressed, 64 MiB decompressed.

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
| free space below `disk_reserve`, or a write error | rolls back to last commit | 507 |
| body too large | nothing | 413 |

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
