# ADR 0010 — 403 for a valid token without the role

Status: accepted (owner decision 2026-10-20)

Supersedes in part: the `no or bad credentials → 401` row of [0003](0003-ingestion-api.md), the
role rules of [0007](0007-authentication.md) and the `reader` identity of
[0008](0008-read-and-status.md), for the response code only.

## Context

M1 answered 401 to every refused request, including a known token used on an endpoint its
identity may not call (a `reader` pushing, a node reading `/v1/status`). nisshi and masu must
answer the same way; the owner chose to tell the two cases apart.

## Options considered

- **401 for everything** (M1): one code, but a misconfigured agent with a valid token cannot
  tell "wrong token" from "wrong role".
- **401 / 403** (HTTP semantics): 401 when no identity is established, 403 when it is and lacks
  the role.

## Decision

| Request | Response |
|---|---|
| no `Authorization`, malformed, or unknown token | 401 (unchanged) |
| `reader` token on `POST /v1/push` | 403 |
| `node:<x>` token on `GET /v1/status` (and any reader-only endpoint) | 403 |
| `node:<x>` token pushing `PushRequest.node` ≠ `<x>` | 400 `node-mismatch` (unchanged) |

Both 401 and 403 have an empty body and are logged as an `auth-failed` event. A push refused
with 403 is counted in `masu_push_requests_total{code="403"}`.

## Consequences

- Same rule in nisshi (owner decision 2026-10-20).
- `fixtures/delivery/` is unchanged (it only has missing and unknown tokens); the 403 cases are
  asserted by masu's own nixosTest.
