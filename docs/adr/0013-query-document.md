# ADR 0013 — Query document: one batched request contract for the stores

Status: Accepted — 2026-11-01 (owner decision)

## Context

yagura needs from masu the latest value of a series, series discovery, range queries with a
step, downsampling and counter rates, and scrape health; from nisshi filtered records and a
histogram; from nenpyo events and counts (`yagura/docs/store-query-needs.md`). A dashboard
refresh composes dozens of such queries; hansho evaluates rules and kenbun reads history with
the same questions. yagura owns every view and has no query language of its own, only the
stores' structured APIs (yagura ADR 0002). masu, nisshi and nenpyo share no code (ADR 0001),
so what they share is a contract, as the delivery fixtures already are (ADR 0009).

The archive stores raw scrapes (ADR 0004): answering one series over a range decompresses
every scrape of that target. Twenty tiles answered by twenty requests are twenty scans.

## Options considered

- **One HTTP route per need** (`/v1/latest`, `/v1/read`, `/v1/range`…): each route gets its
  own time grammar and tie rules; a dashboard is N requests and N scans; the first route is a
  dead end once the next one exists.
- **GraphQL**: selects fields of an object graph, which this data is not; step, downsampling
  and rates would still be arguments in a second language; a large crate set in three stores.
- **A text query language now** (PromQL-like): a parser, a grammar and an error model per
  store before any value is shown; metrics, logs and events never fit one language.
- **gRPC**: HTTP/2, which ADR 0003 excluded from the stores; tonic and its tree in three
  dependency registers; its one advantage, server streaming, is not a need (polling with a
  cursor is).
- **A protobuf query document over HTTPS POST, as `/v1/push`**: one request carries a batch
  of named queries; the store answers all of them in one pass; the same messages describe the
  envelope in the three stores. Chosen.

## Decision

Every query daemon (`masu-query`, `nisshi-query`, `nenpyo-query`) serves one endpoint:

- `POST /v1/query`, body `QueryRequest` of `proto/<store>/v1/query.proto`,
  `Content-Type: application/x-protobuf` or `application/json` (protojson mapping, as
  `/v1/status` in ADR 0008); the response uses the request's encoding. HTTPS, HTTP/1.1 only
  (ADR 0003), identity `reader`; 401/403 before the body is decoded (ADR 0010);
  `Cache-Control: no-store`; bodies and tokens never logged. No content-encoding negotiation
  in this version. `GET /v1/status` is unchanged.
- The envelope messages are the same text in the three `query.proto`; only `Query` (the
  store's operators) differs. `docs/query-document.md` (this contract with its examples) and
  `fixtures/query-document/` (envelope cases: malformed range, too many queries, unknown
  field, per-query error shape) are mirrored byte-identical in masu, nisshi and nenpyo, with
  the rule of `fixtures/delivery/`.

```proto
import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/struct.proto";

message QueryRequest {
  Range range = 1;                  // required
  map<string, Query> queries = 2;   // 1..64 named queries; Query is the store's
}
message Range {
  google.protobuf.Timestamp from = 1;  // inclusive
  google.protobuf.Timestamp to = 2;    // exclusive; from < to
}
message QueryResponse {
  map<string, QueryResult> results = 1;  // one entry per request query, same names
  Budget budget = 2;                     // what the request cost, informative
}
message QueryResult {
  oneof body {
    SeriesFrame series = 1;
    TableFrame table = 2;
    CountsFrame counts = 3;
    QueryError error = 4;
  }
  string cursor = 5;  // set when more rows remain; opaque; pass back as the query's `after`
}
message QueryError { string reason = 1; string message = 2; }

message SeriesFrame { repeated Series series = 1; }
message Series {
  map<string, string> labels = 1;          // the store's labels, verbatim
  google.protobuf.Timestamp start = 2;     // with `step`: values[i] is bucket [start+i*step, start+(i+1)*step)
  google.protobuf.Duration step = 3;
  repeated google.protobuf.Timestamp ts = 4;  // without step: one per value
  repeated double values = 5;              // NaN = no sample in that bucket
}
message TableFrame { repeated google.protobuf.Struct rows = 1; }  // the store's documented objects
message CountsFrame {
  google.protobuf.Timestamp start = 1;
  google.protobuf.Duration step = 2;
  repeated CountGroup groups = 3;
}
message CountGroup { map<string, string> by = 1; repeated uint64 counts = 2; }
message Budget { uint64 records_read = 1; uint64 bytes_read = 2; uint64 elapsed_ms = 3; }
```

Rules of the envelope, the same in every store:

- **Time**: `google.protobuf.Timestamp` (RFC 3339 in JSON, nanoseconds kept); every query is
  bounded by the request's `[from, to)`. No relative offsets in the API: the client computes
  them (the data CLIs keep `-1h`). Compared as integers, never as JavaScript numbers.
- **Values**: `double`, the Prometheus data model; protojson renders non-finite values as the
  strings `"NaN"`, `"Infinity"`, `"-Infinity"`. No string lexemes, no exact integers above
  2^53.
- **Unknown fields** are scoped. An unknown field or member anywhere in the subtree of one
  `Query` (an unknown `oneof` case, or a new parameter of a known operator) makes that query
  `unsupported` and touches no other query; an unknown field or member anywhere else in the
  envelope fails the whole request with 400. **Duplicate keys**
  fail the request: map entries are decoded as repeated entries and a repeated key is
  refused (a plain decoder keeps the last one); the JSON decoder refuses a repeated member.
- **Errors in two layers.** The request fails as a whole only when the envelope is unusable
  or the daemon cannot serve: 400 `invalid-request` (range, query count, unknown field,
  encoding), 413 `request-too-large`, 415 `content-type`, 422 `scan-limit` (the shared scan
  exceeded a budget) / `response-limit` (the results together exceed a budget; narrow the
  range or the batch), 500 `archive-corrupt` / `archive-io`,
  503 `busy` / `snapshot-unavailable` / `query-timeout`, with
  `{"error":{"reason":"...","message":"..."}}` (empty 401/403). Everything else is HTTP 200
  with a `QueryError` **in that query's result**: `invalid-query`, `unsupported`,
  `result-limit`, `archive-incomplete`, and the store's own reasons. One failing tile does
  not fail the dashboard. Messages never carry tokens, paths or raw data.
- **Paging**: a query body that lists rows may carry `limit` and `after`; the result carries
  `cursor` while rows remain; each operator documents its stable total order. A cursor is
  bound to the filters of the query that produced it: `after` with other filters or another
  range is `invalid-query`. A cursor is a position in the order, not a frozen view: a record
  delivered late with a `ts` before that position is missed by the continuation; a client
  that needs completeness re-reads the range from its start, a live view does so on its
  polling period. Series and counts do not page: they are bounded by points and
  buckets. `google.protobuf.Struct` holds numbers as doubles: an integer above 2^53 in a row
  loses precision, a known limit of the table frame.
- **Budgets** per request (wall time, bytes read, records decoded, concurrent requests,
  values and rows accumulated across all results) and per query (series, points, rows,
  buckets) are part of each store's query ADR, with their initial ceilings. Result budgets
  are counted while frames are built, never checked on a finished response: the request's
  aggregate ceiling is 422 `response-limit`. A budget hit is reported with the reasons above,
  never by a truncated or partial frame.
- **One pass**: a daemon evaluates every query of a request over one traversal of the files
  intersecting the range. This is why queries are batched; it is a requirement on the
  implementation, not a hint.
- **Evolution**: an operator is a new `oneof` case of `Query` and a frame a new case of
  `QueryResult`; an old client keeps working, a new query against an old store gets
  `unsupported` for that query alone (the unknown-field rule above). A text query language,
  if one comes, compiles to `Query` in the client and changes nothing here.

## Consequences

- yagura has one client core (envelope, auth, errors, paging) and one typed `Query` per
  store; a tile is a request with one query, a dashboard a request with many. hansho stores
  `Query` messages as rule inputs; kenbun batches wide ranges with a coarse step. Both get
  the store's results, never their own arithmetic.
- `prost-types` (well-known types) is approved as a direct dependency: it is already
  transitive through `prost-build`. The masu dependency register records it; the Cargo
  change belongs to implementation. Other stores record it when adopting the contract.
- Each store's operators are its own ADR: masu [0014](0014-masu-query.md), nisshi ADR 0017,
  nenpyo ADR 0008. This ADR accepts the envelope, not any ceiling or operator.
