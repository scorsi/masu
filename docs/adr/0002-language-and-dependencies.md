# ADR 0002 — Language and dependencies

Status: Accepted — 2026-10-19

## Context

The archive phase is I/O: an HTTPS server, protobuf, zstd, JSON. The engine phase is the
opposite: bit-level compression (delta-of-delta, XOR), memory layout of in-memory heads,
scans over millions of samples, where control over allocation and no GC pauses matter, and
where the learning lies. Choosing a language for the archive alone would mean rewriting masu
for the engine.

## Options considered

| | For | Against |
|---|---|---|
| Rust | control of memory layout, no GC, bit-twiddling without unsafety, iterators that compile to tight loops; kekkai's language; good crates for the plumbing | more dependencies than Go for the archive phase |
| Go | stdlib covers the archive phase | GC and less control for the engine; escape analysis to fight on hot paths |
| Zig | full control, annai's language | no TLS server, no zstd compressor, no protobuf: the archive phase becomes a library project |

## Decision

Rust, built with crane. Direct dependencies:

| Crate | Why |
|---|---|
| `tokio` (`rt-multi-thread` limited to 2 workers, `net`, `fs`, `time`, `signal`, `macros`) | runtime |
| `hyper` (`server`, `http1`), `hyper-util` | HTTP server (HTTP/1.1 only, ADR 0003 amendment) |
| `rustls` (`ring` provider), `tokio-rustls`, `rustls-pemfile` | TLS, no C toolchain for crypto |
| `prost` | ingestion API |
| `prost-types` (approved by ADR 0013; implementation pending) | query protocol well-known types |
| `zstd` (vendored libzstd) | archive files; mature encoder with numeric levels |
| `serde` (`derive`), `serde_json` | records, manifests, state, status |
| `toml` | configuration file, rendered by the module with `pkgs.formats.toml`, read with `#[serde(deny_unknown_fields)]` |
| `sha2` | manifest content hash |

The exposition-format parser (for `masu read`, `masu validate` and later the engine) is
hand-written: it is part of the engine's learning core and small. Argument parsing is
hand-written, as kekkai. Own logs are JSON lines in nisshi's schema written through
`serde_json`, no `tracing`. Any other crate needs a new ADR.

## Consequences

- Static binaries per platform (ADR 0012); the engine phase adds no runtime.
- The archive phase is more code than in Go; accepted for one language across both phases.
