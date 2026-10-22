# Archive format — `masu-archive 1`

Enough to read or re-ingest masu's archive without masu's code. ADRs 0004 and 0006.

## Layout

```
<root>/
  FORMAT                              one line: "masu-archive 1"
  state.json                          writer state (Commit); readers ignore it
  archive/<node>/<YYYY-MM-DD>.ndjson.zst
  archive/<node>/<YYYY-MM-DD>.manifest.json   present once the day is sealed
```

- `<node>` matches `[a-z0-9][a-z0-9-]{0,62}`.
- `<YYYY-MM-DD>` is the UTC day masu **received** the records.
- `archive/` leaves room for the engine's own directory next to it.

## Data files

- A concatenation of complete zstd frames (RFC 8878), no dictionary, window ≤ 8 MiB.
- Decompressed: NDJSON, UTF-8, one record per line, each line ends with `\n`.
- The open day may end with a partial frame while a write is in progress.

## Record

```json
{"v":1,"ts":"2026-10-19T08:14:00.000213904Z","node":"node-a","service":"annai","stream":"7f3c11e0a9b84d2c8e6f5a4b3c2d1e0f","seq":5121,"up":true,"dur_us":812,"content_type":"text/plain; version=0.0.4","body":"# HELP annai_queries_total Queries answered.\n# TYPE annai_queries_total counter\nannai_queries_total{proto=\"udp\",qtype=\"A\",rcode=\"NOERROR\"} 18231\n","recv_ts":"2026-10-19T08:14:00.391022117Z"}
{"v":1,"ts":"2026-10-19T08:14:30.000198311Z","node":"node-a","service":"annai","stream":"7f3c11e0a9b84d2c8e6f5a4b3c2d1e0f","seq":5122,"up":false,"dur_us":5000112,"error":"timeout","recv_ts":"2026-10-19T08:14:35.400120003Z"}
```

| Field | Type | Meaning |
|---|---|---|
| `v` | integer | `1` |
| `ts` | string | scrape start, UTC RFC 3339, exactly 9 fractional digits, `Z` |
| `node` | string | scraped node |
| `service` | string | target name |
| `stream`, `seq` | string, integer | agent stream and position; `(stream, seq)` unique |
| `recv_ts` | string | commit time at masu, same format as `ts`, inserted by masu before the final `}` of the pushed bytes (never re-serialized); its UTC day is the file's day |
| `up` | bool | scrape succeeded |
| `dur_us` | integer | scrape duration |
| `content_type` | string | response `Content-Type`, when `up` |
| `body` | string | the exposition text exactly as received, when `up` |
| `error` | string | when not `up`: `timeout`, `connection-refused`, `http-<code>`, `series-limit`, `body-too-large` (the body, JSON-escaped as it appears in the record, is over 2 MiB), `skipped-spool-full`, `rejected:<store reason>`, or the error text |
| `raw_b64` | string | only with `error:"rejected:…"`: the bytes of the record masu refused (hikyaku ADR 0005) |

masu checks only the whole record: at most 4 MiB, as pushed (`raw_b64` is not part of it). The
agent (hikyaku ADR 0013) caps a scrape body at 2 MiB measured on its JSON-escaped form, the
bytes that sit inside `"body":"…"`, so that a refused record still fits once base64-encoded. The record rules are published as fixtures in `fixtures/records/`
(ADR 0009).

Re-ingestion: parse `body` as Prometheus text 0.0.4 or OpenMetrics 1.0 according to
`content_type`; a sample without its own timestamp takes `ts`; add labels `node` and
`service` from the record.

## Manifest

```json
{
  "v": 1,
  "node": "node-a",
  "day": "2026-10-19",
  "records": 17280,
  "uncompressed_bytes": 346112000,
  "content_sha256": "5d1e…",
  "min_ts": "2026-10-18T23:59:30.000101000Z",
  "max_ts": "2026-10-19T23:59:30.000177000Z",
  "services": {"node": 2880, "annai": 2880, "hikyaku": 2880},
  "streams": [{"stream": "7f3c…", "first_seq": 1, "last_seq": 17280}],
  "sealed_at": "2026-10-20T00:10:04.000000000Z"
}
```

`content_sha256` is the SHA-256 of the decompressed bytes (detects corruption; not chained,
see ADR 0006).

## Commit (writer)

1. Append the new frames to the day file, `fsync`.
2. Write `state.json.tmp` (committed length of each open file, `hwm` and last times per
   stream), `fsync`, rename, `fsync` the directory.
3. Only then answer 200.

A commit that creates a day file (or a node directory) also `fsync`s the directories above it
before step 2. The data directory is locked (`FORMAT`, `flock`) by its one writer.

At start, each open file is truncated to its committed length; a day file absent from
`state.json` and without a manifest is truncated to zero. An open file shorter than its
committed length, or missing, stops the start (acknowledged records are gone). A day still in
`state.json` that has a manifest is an interrupted seal: its file must match the manifest, and
the day is then sealed (ADR 0006 amendment).

## Backup and restore (writer)

Backups hold `FORMAT`, the sealed data files and their manifests, and, for each open day, the
prefix of its data file up to its committed length in `state.json`, named
`archive/<node>/<YYYY-MM-DD>.ndjson.zst.partial` in the backup tree. Never `state.json` itself.

To restore: copy `FORMAT`, the sealed days and their manifests into an empty data directory,
rename each `.partial` to `.ndjson.zst`, start masu. Leave out everything else the backup tree
holds: the `LAST_RUN` marker at its root and any stray `*.tmp`. masu ignores a regular file
directly under `archive/` and any file under `archive/<node>/` that is not
`<YYYY-MM-DD>.ndjson.zst` or `<YYYY-MM-DD>.manifest.json`, and never reads one at the root
other than `FORMAT` and `state.json`. Without `state.json`, masu rebuilds each
stream's `hwm` and owner node before accepting pushes (ADR 0006 amendment):

- from every manifest (largest `last_seq`), checked against its data file when present and
  against its path (`node`, `day`);
- from every day file without a manifest: its complete zstd frames are scanned and the day
  stays open at their length; a last frame cut by the end of the file is cut off, a frame that
  does not decode stops the start.

A stream found under two nodes stops the start.

Records pushed after the backup and lost come back as a `seq-gap`; records the backup holds
are never stored twice. With `state.json` present, a day file absent from it and without a
manifest is still an interrupted first commit, truncated to zero.

## Reading with standard tools

```sh
zstdcat archive/node-a/2026-10-19.ndjson.zst | jq -r 'select(.service == "annai" and .up) | .ts + " " + .body' | grep '^annai_queries_total'
zstdcat archive/node-a/2026-10-19.ndjson.zst | jq -c 'select(.up | not) | {ts, service, error}'
zstdcat archive/node-a/2026-10-19.ndjson.zst | sha256sum    # = manifest content_sha256
```
