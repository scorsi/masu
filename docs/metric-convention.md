# Metric convention

Applies to every house service's `/metrics`. Checked by `masu validate --metrics` in each
service's tests. ADR 0005.

## Endpoint

- Prometheus text format 0.0.4 or OpenMetrics 1.0 text, on `GET /metrics`.
- **Loopback listener only** (`127.0.0.1` or `[::1]`), address set by the service's NixOS module
  (`services.<svc>.metrics.listen`, `null` disables). The local agent is the only scraper.
- Hand-written exposition is fine: no client library is required.

## Names

- `<service>_<what>_<unit>`, `snake_case`: `annai_query_duration_seconds`.
- Base units only: `seconds`, `bytes`, `ratio` (0–1). No `ms`, `kb`, `percent`.
- Counters end in `_total`. Histograms in seconds or bytes, buckets fixed in the code.
- `<service>_build_info{version="…"} 1` on every service.
- Runtime metrics (`process_*`, `go_*`) allowed as the runtime gives them.

## Labels

- Allowed: values from a **closed set** known at build or config time: protocol, rcode, qtype
  (known types, else `other`), HTTP status class (`2xx`…), method (known, else `other`),
  configured names (listener, route, upstream, peer, zone, key).
- Forbidden: source or client IP, domain or query name, URL path or host header, user,
  request or connection id, error text. These go into logs (nisshi's log schema).
- Reserved: `node` and `service` (added from the archive record at re-ingestion).
- Budget: at most 500 series per service. hikyaku refuses a scrape above `max_series`
  (default 2000).

`masu validate --metrics` fails on: a forbidden label name (`ip`, `addr`, `client`, `qname`,
`domain`, `host`, `path`, `url`, `user`, `id`, `request_id`), a reserved label, a counter
without `_total`, a non-base unit suffix (`_ms`, `_milliseconds`, `_kb`, `_percent`), more than
500 series, a parse error.

## Writing the endpoint by hand

No shared library (ADR 0005); the exposition format is simple but has traps that
`masu validate` catches and that are cheaper to avoid:

- `# HELP` and `# TYPE` once per metric family, before its samples; all samples of a family
  together.
- Histogram buckets are **cumulative**, `le` ascending, ending with `le="+Inf"` equal to
  `_count`; `_sum` and `_count` present. Bucket bounds in the base unit (`0.005`, not `5`).
- Label values escaped: `\\`, `\"`, `\n`. With closed label sets this rarely matters, but a
  configured name may contain a quote.
- Counters only go up from process start; never reset them on reload.
- Floats printed so that they parse back exactly (`%g`-style is fine, no locale separators);
  `+Inf`, `-Inf`, `NaN` spelled as such.
- Content type `text/plain; version=0.0.4; charset=utf-8`, body ending with `\n`.

Reference histogram:

```
# HELP annai_query_duration_seconds Time to answer a query.
# TYPE annai_query_duration_seconds histogram
annai_query_duration_seconds_bucket{proto="udp",le="0.0001"} 9120
annai_query_duration_seconds_bucket{proto="udp",le="0.001"} 18002
annai_query_duration_seconds_bucket{proto="udp",le="0.01"} 18220
annai_query_duration_seconds_bucket{proto="udp",le="+Inf"} 18231
annai_query_duration_seconds_sum{proto="udp"} 4.112
annai_query_duration_seconds_count{proto="udp"} 18231
```

## annai

```
annai_build_info{version="0.4.0",mode="soto"} 1
annai_queries_total{proto="udp",qtype="A",rcode="NOERROR"} 18231
annai_query_duration_seconds_bucket{proto="udp",le="0.001"} 18002
annai_cache_entries 412
annai_cache_lookups_total{result="hit"} 9012
annai_upstream_queries_total{upstream="9.9.9.9:53",result="timeout"} 98
annai_updates_total{key="hanko",result="refused"} 2
annai_dynamic_records 1
```

`upstream` and `key` are configured values: closed sets.

## genkan

```
genkan_build_info{version="0.3.0"} 1
genkan_requests_total{listener="edge",route="forgejo",method="GET",code="2xx"} 4210
genkan_request_duration_seconds_bucket{listener="edge",route="forgejo",le="0.05"} 4102
genkan_response_bytes_total{listener="edge",route="forgejo"} 81233120
genkan_upstream_errors_total{route="forgejo"} 3
genkan_tls_handshake_errors_total{listener="edge"} 12
genkan_certificate_expiry_timestamp_seconds{certificate="edge"} 1799020800
```

## kekkai

```
kekkai_build_info{version="0.4.0"} 1
kekkai_peer_path{peer="node-b",path="direct"} 1
kekkai_handshakes_total{peer="node-b",result="ok"} 41
kekkai_rx_bytes_total{peer="node-b"} 912331
kekkai_tx_bytes_total{peer="node-b"} 1203312
kekkai_dropped_packets_total{reason="firewall"} 77
```

`peer` = configured peer name; `reason` = kekkai-core's `DropReason`.

## The stack's own metrics

```
hikyaku_spool_bytes{signal="logs"}
hikyaku_spool_limit_bytes
hikyaku_spool_paused{signal="logs"}
hikyaku_unacked_records{signal="logs"}
hikyaku_oldest_unacked_timestamp_seconds{signal="logs"}
hikyaku_scrapes_total{target="annai",result="ok|error|series-limit|skipped"}
hikyaku_push_requests_total{signal="logs",code="200|400|401|403|413|507|5xx|network"}
hikyaku_journald_suppressed_lines_total{unit="annai"}
hikyaku_journal_gaps_total
hikyaku_rejected_records_total{signal="logs",reason="ts-format"}
masu_records_total{source_node="node-a"}
masu_duplicate_records_total
masu_seq_gaps_total
masu_push_requests_total{code="200"}
masu_disk_free_bytes
masu_seals_total{result="ok|error"}
nisshi_records_total{source_node="node-a"}
nisshi_…                                   (nisshi ADR 0010)
```

`source_node` (a configured closed set) is not `node`, which is reserved for the scraped node.
