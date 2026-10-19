# Scrape-record fixtures

`records.ndjson` is the executable form of the record rules of
[storage-format.md](../../docs/storage-format.md) and ADR 0003: one pushed record per line, the
verdict masu gives it, and the exact `PushRejected.reason` when it refuses it. masu's tests
(`checks.<system>.records-fixtures`) and hikyaku's scrape-record builder both run every line
(ADR 0009). A rule change is a fixture change first.

## Line format

One JSON object per line:

```json
{"input": "<the record bytes, as a JSON string>", "valid": true, "reason": ""}
{"input": "[1,2]", "valid": false, "reason": "not-object"}
```

- `input`: the bytes of `PushRequest.records[i]`, UTF-8, exactly as pushed (no trailing newline
  unless the fixture is about one).
- `valid`: whether masu stores it.
- `reason`: `""` when valid, else the exact `PushRejected.reason`.

A record too large to sit literally in the file has `generate` instead of `input`:

```json
{"generate": {"prefix": "{\"v\":1,…,\"body\":\"", "fill": "x", "count": 4194052, "suffix": "\"}"}, "valid": false, "reason": "record-too-large"}
```

The record is `prefix + fill × count + suffix` (`fill` repeated `count` times), as UTF-8 bytes.
Every line has exactly one of `input` and `generate`.

## Sizes

A stored record is at most 4 MiB (4 194 304 bytes). masu appends
`,"recv_ts":"YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"` (43 bytes) before the final `}`, so a pushed
record is at most **4 194 261 bytes**. The two generated lines sit on each side of that limit.

## Reasons

masu reports the first rule broken, in this order:

| Reason | Rule |
|---|---|
| `record-too-large` | more than 4 194 261 bytes |
| `not-json` | not a single UTF-8 JSON value |
| `not-object` | JSON, but not an object |
| `framing` | the bytes do not start with `{` and end with `}`, or contain CR or LF (masu stores one record per line and inserts `recv_ts` before the last byte) |
| `reserved-field:recv_ts` | `recv_ts` present: a store field |
| `missing-field:<f>` | an envelope field absent: `v`, `ts`, `node`, `service`, `stream`, `seq`, `up`, `dur_us` in this order; then `content_type` and `body` when `up` is true, `error` when it is false |
| `field-type:<f>` | that field of the wrong JSON type (`v`, `seq`, `dur_us`: non-negative integers; `up`: boolean; the others: strings) |
| `unsupported-version` | `v` is not `1` (checked right after `v`) |
| `ts-format` | `ts` not UTC RFC 3339 with exactly 9 fractional digits and `Z`, or not a real date and time (checked right after `ts`) |
| `node-format` | `node` not `[a-z0-9][a-z0-9-]{0,62}` (checked right after `node`) |
| `stream-format` | `stream` not 32 lowercase hex digits (the 16-byte `stream_id`) (checked right after `stream`) |
| `unexpected-field:raw_b64` | `raw_b64` present without `error` starting with `rejected:` |

Fields not listed are not checked: a newer agent may add one. Checks that need the request
(`node`, `stream`, `seq` against `PushRequest`, e.g. `seq-mismatch`, `node-mismatch` in
ingest.proto) belong to the ingestion path (milestone M1) and are not in this file.
