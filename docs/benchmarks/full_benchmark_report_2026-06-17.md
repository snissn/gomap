# TreeDB Full Benchmark Report, 2026-06-17

This summarizes the local full benchmark artifact generated from:

- artifact root: `/mnt/fast4tb/tmp/gomap_full_benchmark_report_20260617_083348`
- HTML report: `/mnt/fast4tb/tmp/gomap_full_benchmark_report_20260617_083348/deep_report.html`
- generated at: `2026-06-17T18:51:43Z`
- benchmark commit: `534fc6eda3c710828cc54e862934a2841eafa977`
- branch at capture: `main`
- Go: `go1.26.0 linux/amd64`
- host: `Linux mikers-B560-DS3H-AC-Y1 6.8.0-124-generic x86_64`
- run tier: `pr`
- exact top-level invocation: `scripts/treedb_benchmark_run_report.sh`

The repository may have moved past the benchmark commit. Treat these as dated
run evidence, not as live-head benchmark results.

## Scope

Top-level run parameters:

| Setting | Value |
| --- | --- |
| Raw keys | `200000` |
| Collection docs | `100000` |
| Mongo docs | `100000` |
| Index counts | `0 1 2` |
| Mongo mode | Docker |
| Mongo image | `mongo:8` |
| Mongo compact | `true` |
| Mongo readers | `1 2 4 8 16 32` |
| Mongo writers | `1 2 4 8 16 32` |

## Collection Insert And Compaction

`docs/sec` rows are benchmark-timed post-insert measurements from
`collections_sqlite_canonical_1m/indexes_*/benchmark_summary.md`. Maintenance
rows are excluded from throughput ranking. Template-v1 compacted-size
comparisons use TreeDB `exhaustive_compact` and SQLite `VACUUM`. BSON and JSON
compacted B/doc rows below use the diagnostic `full_leafgen_pack_gc` phase.

| Indexes | Engine / format | Docs/sec | Compacted B/doc |
| ---: | --- | ---: | ---: |
| 0 | TreeDB template-v1 | 1,876,877 | 15.5 |
| 0 | TreeDB BSON | 1,207,146 | 18.1 |
| 0 | TreeDB JSON | 1,356,668 | 18.3 |
| 0 | SQLite native columns | 628,931 | 89.3 |
| 0 | SQLite JSON | 799,361 | 128.2 |
| 1 | TreeDB template-v1 | 1,049,098 | 20.2 |
| 1 | TreeDB BSON | 946,074 | 23.5 |
| 1 | TreeDB JSON | 670,241 | 28.2 |
| 1 | SQLite native columns | 511,509 | 132.5 |
| 1 | SQLite JSON | 451,671 | 201.1 |
| 2 | TreeDB template-v1 | 746,269 | 22.8 |
| 2 | TreeDB BSON | 653,595 | 25.7 |
| 2 | TreeDB JSON | 527,148 | 35.2 |
| 2 | SQLite native columns | 385,654 | 156.7 |
| 2 | SQLite JSON | 333,444 | 231.7 |

For the README two-index headline, TreeDB template-v1 reached `746,269`
docs/sec at `22.8 B/doc`; SQLite native columns reached `385,654` docs/sec at
`156.7 B/doc`.

## Mongo API Full Sweep

Source:
`mongo_gateway_full_sweep_1m_expanded/report.md`.

This is a BSON `driver-command-raw` full phase sweep through the TreeDB Mongo
gateway against MongoDB 8. The run has `range_index=true`, so the load rows
include the additional `age_1` range index even when the displayed
secondary-index count is `0`.

| Secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB load / MongoDB | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 549,311 | 454,224 | 1.21x | 68.66 | 213.38 |
| 1 | 422,113 | 255,638 | 1.65x | 69.38 | 216.94 |
| 2 | 448,003 | 206,858 | 2.17x | 69.66 | 217.52 |

Concurrent `_id` read sweep:

| Secondary indexes | Readers | TreeDB ops/sec | MongoDB ops/sec | TreeDB / MongoDB | TreeDB p95 us | MongoDB p95 us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 17,176 | 8,441 | 2.03x | 71.0 | 142 |
| 0 | 16 | 137,313 | 34,396 | 3.99x | 181 | 795 |
| 0 | 32 | 138,433 | 38,978 | 3.55x | 438 | 1,588 |
| 1 | 1 | 16,838 | 8,706 | 1.93x | 73.0 | 133 |
| 1 | 16 | 121,289 | 34,727 | 3.49x | 202 | 781 |
| 1 | 32 | 126,581 | 37,763 | 3.35x | 489 | 1,657 |
| 2 | 1 | 17,012 | 8,791 | 1.94x | 72.0 | 132 |
| 2 | 16 | 129,211 | 34,566 | 3.74x | 192 | 783 |
| 2 | 32 | 137,808 | 37,346 | 3.69x | 441 | 1,677 |

Concurrent indexed age range read sweep:

| Secondary indexes | Readers | TreeDB ops/sec | MongoDB ops/sec | TreeDB / MongoDB | TreeDB p95 us | MongoDB p95 us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 17,018 | 6,722 | 2.53x | 70.0 | 172 |
| 0 | 16 | 45,522 | 25,210 | 1.81x | 1,197 | 1,012 |
| 0 | 32 | 47,929 | 26,361 | 1.82x | 1,824 | 2,172 |
| 1 | 1 | 16,984 | 6,768 | 2.51x | 70.0 | 169 |
| 1 | 16 | 42,368 | 24,925 | 1.70x | 1,256 | 1,036 |
| 1 | 32 | 46,698 | 26,198 | 1.78x | 1,840 | 2,163 |
| 2 | 1 | 17,070 | 6,888 | 2.48x | 70.0 | 164 |
| 2 | 16 | 45,683 | 25,232 | 1.81x | 1,208 | 1,014 |
| 2 | 32 | 49,224 | 26,290 | 1.87x | 1,734 | 2,122 |

Concurrent `_id` update sweep:

| Secondary indexes | Writers | TreeDB ops/sec | MongoDB ops/sec | TreeDB / MongoDB | TreeDB p95 us | MongoDB p95 us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 14,777 | 7,405 | 2.00x | 80.0 | 161 |
| 0 | 16 | 92,205 | 33,426 | 2.76x | 219 | 773 |
| 0 | 32 | 110,701 | 31,745 | 3.49x | 362 | 1,917 |
| 1 | 1 | 14,749 | 7,525 | 1.96x | 81.0 | 157 |
| 1 | 16 | 90,802 | 33,406 | 2.72x | 221 | 774 |
| 1 | 32 | 109,583 | 35,182 | 3.11x | 368 | 1,669 |
| 2 | 1 | 14,839 | 7,669 | 1.93x | 80.0 | 148 |
| 2 | 16 | 91,746 | 32,606 | 2.81x | 216 | 801 |
| 2 | 32 | 107,110 | 34,690 | 3.09x | 371 | 1,679 |

## Mongo API Client-Mode Load Matrix

Source:
`mongo_client_mode_load_matrix_1m/report.md`.

This matrix keeps the phase to load-only insert throughput and varies TreeDB
client/protocol shape. It is not the same workload as the older June 4
client-shape read table in the README.

| Secondary indexes | TreeDB config | TreeDB load docs/sec | MongoDB baseline docs/sec | TreeDB / MongoDB | TreeDB physical MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | `treedb_bson_driver_command_raw` | 593,225 | 195,749 | 3.03x | 5.20 |
| 0 | `treedb_bson_raw_wire_tcp` | 650,653 | 195,749 | 3.32x | 5.20 |
| 1 | `treedb_bson_driver_command_raw` | 477,542 | 228,241 | 2.09x | 6.16 |
| 1 | `treedb_bson_raw_wire_tcp` | 493,712 | 228,241 | 2.16x | 6.16 |
| 2 | `treedb_bson_driver_command_raw` | 461,853 | 183,054 | 2.52x | 6.54 |
| 2 | `treedb_bson_raw_wire_tcp` | 495,168 | 183,054 | 2.71x | 6.54 |

## Raw Engine Profile Sweep

Source:
`raw_engine_full_matrix/*/benchprof_results.md`.

The raw-engine sweep used `200000` keys, `128` byte values, `batchsize=8000`,
`range-queries=200`, `range-span=100`, `val-pattern=zero`, and the native
fastpath.

| Profile / checkpoint mode | Sequential write | Random write | Batch write | Batch steady | Random read | Parallel read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `fast`, checkpoint between tests | 3,345,855 | 3,256,197 | 14,861,026 | 2,184,606 | 1,095,563 | 10,607,186 |
| `fast`, no checkpoint between tests | 3,313,791 | 3,004,559 | 7,109,104 | 611,019 | 2,513,715 | 23,781,123 |
| `wal_on_fast`, checkpoint between tests | 2,269,434 | 2,372,889 | 14,230,203 | 2,234,787 | 1,132,842 | 11,137,272 |
| `wal_on_fast`, no checkpoint between tests | 2,236,893 | 2,021,236 | 6,067,371 | 735,668 | 2,496,625 | 33,392,778 |

The full artifact has the complete raw-engine table, checkpoint timings,
profile files, and codec counters. In the `wal_on_fast` checkpoint-between-tests
run, value-log auto-compression selected LZ4 for almost all outer-leaf frames:
`65,217` LZ4 frames, `372,260,338` raw bytes, and `34,855,993` stored bytes
for a stored ratio of `0.093633`.

## Notes

- The HTML report and raw artifacts remain outside the repository because they
  include large profile outputs and generated files.
- MongoDB `dbStats.dataSize` is logical document size; physical `du` is the
  preferred local disk comparison in Docker mode.
- TreeDB full-sweep storage includes the range index where `range_index=true`;
  the load-only client-mode matrix omits that range index.
- Wall ops/sec includes the full benchmark phase loop. Sampled ops/sec, where
  reported, isolates the timed driver/gateway call inside the phase.
