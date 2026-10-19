# Delivery scenarios

Shared by nisshi's and masu's test pushers (nisshi ADR 0012, masu ADR 0009). **Mirrored**:
this file and `fixtures/delivery/` are byte-identical in both repositories (see `CLAUDE.md`).
One JSON file per scenario in `fixtures/delivery/`.

## Format

```json
{
  "name": "overlap-after-lost-ack",
  "steps": [
    {"push": {"first_seq": 1, "count": 10}, "expect": {"status": 200, "acked_seq": 10}},
    {"push": {"first_seq": 6, "count": 10}, "expect": {"status": 200, "acked_seq": 15}},
    {"check": {"stored_seqs": "1-15", "duplicates": 0}}
  ]
}
```

| Step | Fields | Meaning |
|---|---|---|
| `push` | `first_seq`, `count`, optional `token` (`valid` default, `none`, `unknown`, `other-node`), `node` (`self` default, `other`), `invalid_at` (index of a record the pusher makes invalid), `stream` (`a` default, `b`: a second stream of the same node) | one `POST /v1/push` |
| `expect` | `status`, `acked_seq`, `record_index`, `reason` | response checks; absent fields are not checked, `null` means the field must be absent from the response |
| `check` | `stored_seqs` (ranges, `""` = none), `duplicates`, `gaps` (count of gap reports) | reads the store's files with standard tools, after a store-side flush |
| `restart` | `kill: true` | `kill -9` the store and start it again |
| `disk` | `fill: true` / `free: true` | fills or frees the store's test filesystem |

## Scenarios (initial set)

| File | Covers |
|---|---|
| `in-order.json` | batches in sequence, `acked_seq` advances |
| `exact-duplicate.json` | the same batch twice: stored once, `acked_seq` unchanged |
| `overlap-after-lost-ack.json` | overlapping resend: only the new part stored |
| `gap.json` | `first_seq > hwm + 1`: stored, one gap reported |
| `two-streams.json` | two streams of one node, independent `hwm` |
| `auth.json` | no token, unknown token: 401, nothing stored |
| `other-node.json` | records of another node: 400 without index (request level) |
| `invalid-record.json` | `invalid_at: 3`: 400, `record_index: 3`, nothing of the batch stored; the same batch with a valid record 3 is accepted |
| `kill-mid-stream.json` | pushes, `kill -9`, resend from the last acked seq: no loss, no duplicate |
| `disk-full.json` | `fill`: 507, nothing stored; `free`: same batch accepted |

Each store's "gap reported" check: nisshi counts `seq-gap` records, masu reads
`masu_seq_gaps_total`.
