# ADR 0014 — masu-query: operators, trust boundary and budgets

Status: Accepted — 2026-11-01 (owner decision)

## Context

yagura Y1 needs a backup timestamp gauge first, then charts; hansho and kenbun follow. ADR
0012 reserves `masu-query`; ADR 0013 fixes the request envelope. ADR 0004 said dashboards
wait for the engine: this ADR lets them start on bounded scans of the archive, behind the
contract the engine will serve later.

## Options considered

- **Wait for the TSDB**: postpones the first useful value for months.
- **A latest-only endpoint**: smallest, but a shape the range query would not reuse.
- **The envelope's operators over a bounded one-pass scan**: chosen. More I/O than an index;
  explicit refusal when a budget cannot establish a complete answer.

## Decision

### Process and trust boundary

`masu-query serve <config.toml>` is a separate read-only daemon on the store host over
`masu-core`; `check-config` rejects unknown keys. Configuration: archive root, HTTPS
listeners, TLS certificate and key paths, token-hash-to-identity map, in the store's
conventions; no port or deployment here. The process has read and traverse access only; it
never opens `Store`, never recovers, seals, truncates or retains, never touches HWM, state or
FORMAT. Only `POST /v1/query` is exposed; anything else is 404/405. Clients verify the CA
chain and hostname, accept a configured CA file, refuse redirects, have no insecure switch.
mTLS comes with hanko. No new crate beyond `prost-types` (ADR 0013).

### Operators

```proto
message Query {
  oneof body {
    SeriesQuery series = 1;
    UpQuery up = 2;
    DiscoverQuery discover = 3;
  }
}
message SeriesQuery {
  string node = 1;                     // required, node grammar
  string service = 2;                  // required, exact, <= 128 bytes, a record filter
  string name = 3;                     // required, exact sample name, <= 256 bytes
  map<string, string> labels = 4;      // equality matchers; a series with more labels still matches
  google.protobuf.Duration step = 5;   // optional
  Reduce reduce = 6;                   // REDUCE_UNSPECIFIED = raw points (needs no step)
  bool rate = 7;                       // counters only; needs step
}
enum Reduce { REDUCE_UNSPECIFIED = 0; LAST = 1; MIN = 2; MAX = 3; AVG = 4; COUNT = 5; }
message UpQuery {
  string node = 1;
  string service = 2;
  uint32 limit = 3;   // default 1000, at most 10 000
  string after = 4;   // cursor of a previous result
}
message DiscoverQuery { string node = 1; string service = 2; string prefix = 3; }  // no paging
```

- **Clock**: the record's scrape `ts`, never `recv_ts`, never a sample's own exposition
  timestamp. Late deliveries may land in any receive day: selection is by manifests'
  `min_ts`/`max_ts` and service, never by filename.
- **`series`** returns one `Series` per distinct label set matching the matchers, the
  sample's labels verbatim: nothing is added or rewritten, the request already names the
  node and service, and the convention's reserved labels are the validator's job. No
  aggregation across series. Without `step` and without `reduce`: the raw points in
  `[from, to)`, in `ts` order. With `step` (`> 0`): `ceil(range / step)` buckets from `from`,
  the last one cut at `to`, `reduce` applied per bucket (`LAST` when unspecified), `NaN` for
  an empty bucket. With `reduce` and no `step`: the whole range reduced to one point; for
  `LAST` its `ts` is the sample's `ts` (a backup gauge reads as "value at that time"),
  otherwise `from`. Ties at the same `ts` are broken by greatest `(recv_ts, stream, seq)`.
- **Non-finite samples**: a stored `NaN` is ignored by `MIN`, `MAX`, `AVG` and `COUNT`
  (`COUNT` counts the non-`NaN` samples); `LAST` returns it as is; `+Inf`/`-Inf` take part
  in every reduction. The same rule binds the future engine.
- **`rate`**: for a `counter` family's counting sample (`_total` in OpenMetrics, the bare
  name in Prometheus), each bucket's value is the sum of the increments between consecutive
  samples whose later sample falls in the bucket, divided by the bucket's duration in
  seconds; a decrease is a reset and the later value is the increment; no extrapolation.
  A bucket whose samples have no earlier sample in the range is `NaN`. `rate` without
  `step`, with a `reduce`, on a non-counter family or on a `_created` sample is
  `invalid-query`; `_created` is readable as a raw series. No other arithmetic.
- **Families**: `gauge`, `counter`, `unknown` (`untyped` reported as `unknown`), from parsed
  TYPE metadata, never a suffix guess. A `series` whose name belongs to a histogram, summary,
  info or stateset family is `unsupported` in this version. A failed scrape or a successful
  scrape without the series contributes nothing; nothing is invented.
- **`up`** returns a `TableFrame` of `{ts, up, error}` rows per scrape record of that
  node/service, `error` present when `up` is false, in `(ts, recv_ts, stream, seq)` order,
  paged by `limit`/`after`. The scrape health yagura draws as markers.
- **`discover`** returns a `TableFrame` of distinct `{name, type, labels}` seen in the range
  for that node/service, `name` starting with `prefix` (empty prefix: all), sorted by `name`,
  then the labels in canonical form, then `type`; above the row ceiling it is `result-limit`.
- A successful scrape whose exposition cannot be parsed makes every query touching it fail
  with `invalid-exposition`, never disappear silently.

### Consistent reads and completeness

Live queries read a committed-prefix view: a bounded snapshot of `state.json`, validated
manifests and opened descriptors. Unsealed files are read to their state-recorded committed
compressed length, never an appending tail; a file absent from state and without a manifest
contributes nothing. An old state length is never applied to a newly compacted inode;
inconsistent state/manifest/path/descriptor observations retry the snapshot once, then 503
`snapshot-unavailable`. Opened sealed descriptors survive rename and unlink, so a pinned file
finishes after retention; a required file missing before it is pinned cannot.

FORMAT, manifests (node, day, schema), record envelopes and decompression are validated; a
sealed file used for an answer matches its manifest's content hash. Malformed metadata is
never trusted for pruning. A retained manifest whose data is missing and intersects the range
makes the queries touching it `archive-incomplete`. 200 means a complete scan of the
**retained** archive, not coverage of wall-clock history. The daemon never verifies, rebuilds
or repairs.

### Budgets

Initial ceilings, part of the contract until measured:

| Per request | Ceiling |
|---|---:|
| Request bytes / queries | 64 KiB / 64 |
| Range | 31 days |
| Snapshot metadata bytes | 4 MiB |
| Files opened / directory entries per node | 1024 / 4096 |
| Compressed / decompressed bytes read | 128 MiB / 512 MiB |
| Records decoded / samples parsed | 500,000 / 5,000,000 |
| Wall time, including admission | 10 s |
| Concurrent requests per daemon | 2 |
| Values and rows accumulated across all results | 2,000,000 |
| Response bytes | 16 MiB |

| Per query | Ceiling |
|---|---:|
| Series per `series` result | 1,000 |
| Points per series | 11,000 |
| Rows per `up` / `discover` result | 100,000 / 10,000 |

Counters include skipped records and failed attempts. Every read, decompress and parse stage
checks cancellation and budgets; a timed-out wrapper over a running blocking scan is not
enough. Result ceilings are counted as values and rows are appended, never on a finished
response (64 queries of 1,000 series of 11,000 points would be 5.6 GB of doubles). Frame
sizes are never trusted for allocation; the archive's 8 MiB zstd window and 4 MiB line limits
apply. A per-request scan budget hit is 422 `scan-limit`, an aggregate result one 422
`response-limit`; a per-query one is that query's `result-limit`; a frame is never
truncated.

### Sequence and acceptance

This accepted design does not authorize deployment. Implement in small commits, with Kajiba
feature checks on Linux before publishing:

1. `masu-core`: snapshot reader and one-pass evaluator, no network. Checks `query-select`
   (matchers, cardinality, raw/step/reduce, partial last bucket, non-finite samples, ties,
   late arrival, empty and failed scrapes, `up` paging, `discover` order), `query-rate`
   (formula, resets, single-sample buckets, `_created`, `reduce` refusal, non-counter
   refusal, families), `query-snapshot` (append, partial
   tail, seal/rename, unlink, restart, corrupt metadata/frame/hash, missing state),
   `query-budgets` (every ceiling, the aggregate result ceiling hit during accumulation,
   cancellation, no frame on refusal).
2. `masu-query` daemon and HTTPS reader contract. Check `vm-query`: real ingested open and
   sealed days, both encodings, batch with one failing query, 401/403, TLS hostname and
   foreign CA refusal, limits, concurrent pushes, no mutation of the archive.
3. Only then: a separate yagura slice (Y1) with the real backup series and thresholds.

## Consequences

- ADR 0004's "dashboards wait for the engine" is amended: dashboards use
  `masu-query` within its budgets; the engine replaces the scan behind the same contract.
  storage-format's "readers ignore `state.json`" is amended for committed live snapshots.
- If measurements show the scan too slow for ordinary dashboards, the next step is a cache
  of decoded series per sealed file keyed by the manifest hash (sealed files are immutable),
  proposed separately; then the engine.
- The owner accepted the initial ceilings, scrape-clock semantics, family restriction and
  equality-matcher semantics on 2026-11-01. Implementation and measurements remain pending.
