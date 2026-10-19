# Resource budget

Estimates; replaced by the 24 h measurement of M1.

## Measured (2026-10-19)

node_exporter on macOS, 60 scrapes 2 s apart, 53 KB per scrape, 3.2 MB as NDJSON records:

| Encoding | Size | Ratio |
|---|---|---|
| one zstd -3 frame per 2 scrapes (open-day style) | 261 KB | 12× |
| whole file, zstd -9 (seal) | 48 KB | 67× (17 MB RSS) |
| whole file, zstd -19 | 43 KB | 75× (57 MB RSS: not worth it) |

At 30 s more values change between scrapes: the budget assumes **10× open, 40× sealed**.

## Volume

| Node | Targets | Raw per day (30 s) |
|---|---|---|
| internal server | node_exporter (≈ 100 KB), annai, genkan, hikyaku, masu, nisshi (≈ 5 KB each) | ≈ 360 MB |
| public edge VPS | node_exporter, annai, hikyaku | ≈ 320 MB |
| macOS workstation | node_exporter (53 KB), hikyaku | ≈ 170 MB |
| **total** | | **≈ 850 MB** |

## Disk

| | Per day | 30 days | 90 days | 400 days (default retention) |
|---|---|---|---|---|
| sealed | 21 MB | 0.65 GB | 1.9 GB | 8.5 GB |

Plus the open day (≈ 85 MB) and the free-space reserve (2 GiB).

## Memory and CPU

`masu serve` ≤ 64 MiB RSS in the archive phase (`MemoryMax=128M`); the seal streams through
libzstd level 9 (≈ 20 MB). CPU idle except the nightly seal (tens of seconds for 850 MB raw).
The engine phase gets its own budget.
