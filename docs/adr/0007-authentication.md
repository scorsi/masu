# ADR 0007 — Authentication before and after hanko

Status: Accepted — 2026-10-19

Superseded in part by [0010](0010-forbidden-for-wrong-role.md) (403 for a valid token without the role).

## Context

Same need and same constraints as nisshi (nisshi ADR 0008): refuse unknown agents, bind a
batch to a node, no dependency on the monitored overlay, hanko certificates and SVIDs later.

## Decision

The same scheme as nisshi ADR 0008, implemented in masu:

- **Now**: TLS + per-node bearer token; config maps hex `sha256(token)` → `node:<name>` or
  `reader`; constant-time comparison; `node:<name>` pushes only `node = <name>` records;
  `reader` only reads `/v1/status`. Self-signed server certificate pinned by agents.
- **Step 1**: certificate from hanko, pin dropped.
- **Step 2**: `auth.methods = [ "token", "mtls" ]`, SPIFFE ID
  `spiffe://<trust-domain>/node/<name>` → `node:<name>`, nodes switch one by one, then tokens
  removed.

## Consequences

- One token per node may be accepted by both stores (each holds only its hash); the deployment
  decides whether to share it.
