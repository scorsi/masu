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
- `input_b64`: the same bytes, standard base64 with padding (RFC 4648 section 4), for records
  that cannot sit in a JSON string (not valid UTF-8). nisshi's fixtures use the same name and
  meaning.
- `valid`: whether masu stores it.
- `reason`: `""` when valid, else the exact `PushRejected.reason`.

A record too large to sit literally in the file has `generate` instead of `input`:

```json
{"generate": {"prefix": "{\"v\":1,…,\"body\":\"", "fill": "x", "count": 4194052, "suffix": "\"}"}, "valid": false, "reason": "record-too-large"}
```

The record is `prefix + fill × count + suffix` (`fill` repeated `count` times), as UTF-8 bytes.
Every line has exactly one of `input`, `input_b64` and `generate`.

## Sizes

A stored record is at most 4 MiB (4 194 304 bytes). masu appends
`,"recv_ts":"YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"` (43 bytes) before the final `}`, so a pushed
record is at most **4 194 261 bytes**. The two generated lines sit on each side of that limit.

## Reasons

masu reports the first rule broken, in this order. The vocabulary is the same as nisshi's,
except `unexpected-field:raw_b64` (scrape records only).

| # | Reason | Rule |
|---|---|---|
| 1 | `record-too-large` | more than 4 194 261 bytes |
| 2 | `not-utf8` | not valid UTF-8 |
| 3 | `framing` | the bytes do not start with `{` and end with `}`, or contain CR or LF (masu stores one record per line and inserts `recv_ts` before the last byte) |
| 4 | `not-json` | not a single valid JSON value |
| 5 | `not-object` | valid JSON, but not an object |
| 6 | `duplicate-field:<f>` | a top-level member name present twice; `<f>` is the first name repeated, after unescaping (`"\u0076"` is `v`) |
| 7 | `reserved-field:recv_ts` | `recv_ts` present: a store field |
| 8 | `missing-field:<f>`, `<f>-format` | the envelope fields `v`, `ts`, `node`, `service`, `stream`, `seq`, `up`, `dur_us` in this order, then `content_type` and `body` when `up` is true, `error` when it is false: `missing-field:<f>` when absent, `<f>-format` when of the wrong JSON type or format (below) |
| 9 | `raw_b64-format`, `unexpected-field:raw_b64` | `raw_b64` present: not a string, or without `error` starting with `rejected:` |

Field formats (`<f>-format`):

| Field | Valid |
|---|---|
| `v` | the integer `1` |
| `ts` | a string, UTC RFC 3339 with exactly 9 fractional digits and `Z`, a real date and time that fits signed 64-bit Unix nanoseconds (1677-09-21T00:12:43.145224192Z to 2262-04-11T23:47:16.854775807Z) |
| `node` | a string matching `[a-z0-9][a-z0-9-]{0,62}` |
| `stream` | a string of 32 lowercase hex digits (the 16-byte `stream_id`) |
| `seq`, `dur_us` | a non-negative integer |
| `up` | a boolean |
| `service`, `content_type`, `body`, `error` | a string |

`not-object` has no fixture: once `framing` holds, the bytes start with `{`, and a valid JSON
value starting with `{` is an object. masu keeps the check; the reason exists for nisshi's
vocabulary.

Duplicates are checked at the top level only: a nested object is not an envelope field.

Fields not listed are not checked: a newer agent may add one.

## Request checks (ingestion path, milestone M1)

Checks that need the `PushRequest` are not in this file; they are made by `POST /v1/push`
(ADR 0003), with the same 400 `PushRejected` body, in this order (the first failing check is
answered). Authentication comes first: 401 without a known token, 403 for a `reader` token
(ADR 0010). A body that is not zstd is refused, never read as plain protobuf.

| Reason | Level | Rule |
|---|---|---|
| `content-encoding` | request (no `record_index`) | `Content-Encoding` is not `zstd` |
| `body-zstd` | request (no `record_index`) | the body is not a valid zstd frame |
| `body-protobuf` | request (no `record_index`) | the decompressed body is not a `PushRequest` |
| `node-mismatch` | request (no `record_index`) | `PushRequest.node` is not the authenticated node |
| `stream-id-format` | request (no `record_index`) | `stream_id` is not 16 bytes |
| `first-seq-zero` | request (no `record_index`) | `first_seq` is 0 |
| `stream-owner` (**403**) | request (no `record_index`) | the stream exists and belongs to another node (ADR 0011) |
| `node-mismatch` | record (`record_index`) | the record's `node` is not `PushRequest.node` |
| `stream-mismatch` | record (`record_index`) | the record's `stream` is not the hex of `stream_id` |
| `seq-mismatch` | record (`record_index`) | the record's `seq` is not `first_seq + i` |
