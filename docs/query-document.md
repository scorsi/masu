# Query document

The shared query envelope of ADR 0013. Each store defines its `Query` operators separately;
masu's operators are in ADR 0014. This document and `fixtures/query-document/` are intended
to be mirrored byte-identically when the other stores adopt the contract. This change adds
only the masu copy.

## Transport and envelope

`POST /v1/query` accepts `application/x-protobuf` or `application/json` (ProtoJSON) and returns
the same encoding. HTTPS, HTTP/1.1, reader identity, certificate and hostname verification,
no redirects, `Cache-Control: no-store`. Authentication precedes decoding; bodies and tokens
are never logged. The HTTP listener lives in the separate `masu-query` daemon; the core
codec and evaluator have no network access.

A request has a required `range` (`from` inclusive, `to` exclusive, `from < to`) and 1–64
named `queries`. Times are protobuf Timestamp values, JSON RFC 3339 strings with nanoseconds
preserved; durations use protobuf Duration / its JSON string mapping. Relative times belong
to a client, not the document. An answer has exactly the same result names as the request;
the client checks those names against its own request.

Each result contains exactly one series, table, counts or error body. Series carry verbatim
labels and either explicit timestamps, or a start and positive step. Tables contain the
store's documented JSON objects. Counts carry a start, positive step and equally sized
count groups. The optional budget describes records and bytes read and elapsed milliseconds.

Doubles follow the Prometheus model, including ProtoJSON `"NaN"`, `"Infinity"` and
`"-Infinity"`. uint64 values use decimal JSON strings; both numeric and quoted integral
exponent forms are accepted without rounding uint64 values through a double. Struct numbers
remain doubles, including their known precision limit above 2^53. Standard generated
protobuf encoders work without a JSON dependency; the custom JSON mapping uses serde_json's
lexer and prost-types' Timestamp/Duration conversion.

## Unknown fields, duplicates and validation

An unknown member anywhere inside one Query, including a parameter added to a known
operator, makes only that query unsupported. Its known prefix is never executable. A
malformed known query parameter is isolated as invalid-query. Unknown fields elsewhere in
the envelope fail the request. Duplicate map keys fail the request, including duplicate JSON
members in an unknown query. The codec distinguishes known, unsupported and invalid queries
before evaluation.

The protobuf guard runs before prost: prost normally drops unknown fields and merges map
entries. The guard checks known maps before those entries are collected. As a deliberately
strict subset of otherwise valid protobuf encodings, repeated singular fields and repeated
oneof bodies are refused, rather than merged; that prevents duplicate keys from hiding
across merged submessages. This is a codec restriction, not protobuf or prost behavior.
Usual generated encoders emit each singular field once. Future support for merged encodings
requires duplicate tracking over the complete logical merged message.

JSON null may stand for an unset field; it is refused as an element of repeated message or
scalar fields (Struct's JSON data may itself contain null). JSON depth and protobuf message
depth are bounded. Malformed wire values, truncated messages, invalid well-known types and
inconsistent response frames fail without exposing their contents in error text.

## Errors and resource bounds

Whole-request errors: invalid-request (400), request-too-large (413), content-type (415),
scan-limit / response-limit (422), archive-corrupt / archive-io (500), and busy /
snapshot-unavailable / query-timeout (503). Their HTTP error shape is
`{"error":{"reason":"...","message":"..."}}`; 401/403 are empty.
A usable request returns HTTP 200 with any query-specific error in that query's result.
Messages contain no tokens, paths or raw records.

Per-store budgets apply before allocating a complete result or sending a partial frame.
Masu currently bounds request and response documents at 64 KiB and 16 MiB, 64 named queries,
and a 31-day range. An invalid or inverted range is invalid-request; exceeding the range
budget is scan-limit. The core encoder bounds the serialized output before returning it;
the decoder checks input length before parsing. The HTTP layer bounds collection of incoming
bytes before decoding and authenticates the reader before collecting the body. Two work
permits cover collection, evaluation and buffered response bytes until their last network
consumer releases them. A query has a ten-second wall deadline; the connection also has a
twenty-second ceiling for headers and output.

The evaluator measures the selected response encoding exactly; response bytes count only
returned output, once. A separate initial 128 MiB request-wide workspace reservation bounds
staged candidates, labels and result assembly before allocation. Exhausting this work guard
is scan-limit even when a reduction would return a small response. It is distinct from the
16 MiB serialized response ceiling. Integrity or request-wide budget failures discard the
batch rather than exposing accumulated frames as successful results.

## Paging and evolution

An operator may support `limit` and `after`. Its cursor is bound to the query filters and
range and advances in that operator's stable total order. It is a position, not a frozen
snapshot: late data before the position requires re-reading the range from its beginning.
Series and counts do not page; their point and bucket ceilings apply instead.
New operators are Query oneof cases; an older server marks only that query unsupported.
The batch is evaluated over one traversal of the selected files, not one scan per query.

## Example (masu operator)

```json
{
  "range": {"from": "2026-11-01T00:00:00Z", "to": "2026-11-01T01:00:00Z"},
  "queries": {
    "backup": {"series": {"node": "node-a", "service": "backup", "name": "backup_last_snapshot_timestamp_seconds", "reduce": "LAST"}}
  }
}
```

```json
{
  "results": {
    "backup": {"series": {"series": [{"labels": {}, "ts": ["2026-11-01T00:59:00.000000001Z"], "values": [1793494740]}]}}
  },
  "budget": {"recordsRead": "120", "bytesRead": "4096", "elapsedMs": "2"}
}
```
