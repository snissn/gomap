# Mongo Gateway Benchmark Comparison - 2026-04-29

- generated_at: `2026-04-29T08:31:58Z`
- matrix: `docs/benchmarks/mongo_gateway_compare_2026-04-29/matrix.tsv`
- comparison cells: `6`
- targets: `treedb`, `mongo`

## Highlights

- Largest TreeDB ops/sec lead: `load_insert_many` at 1000 docs / 0 indexes, 66735 ops/sec vs 18756 ops/sec (3.56x TreeDB / MongoDB).
- Largest MongoDB ops/sec lead: `age_range_limit_10` at 10000 docs / 2 indexes, 11.2 ops/sec vs 5058 ops/sec (<0.01x TreeDB / MongoDB).
- Largest cell disk: at 10000 docs / 2 indexes, TreeDB checkpoint files used 12.25 MiB (1284.5 B/doc), TreeDB physical du was 12.28 MiB, MongoDB dbStats dataSize was 2.79 MiB, MongoDB dbStats totalSize was 16.00 KiB, and MongoDB physical du was 300.12 MiB (31469.6 B/doc).

## Disk Summary

| docs | indexes | TreeDB checkpoint | TreeDB checkpoint bytes/doc | TreeDB physical du | TreeDB physical bytes/doc | MongoDB dbStats dataSize | MongoDB dbStats totalSize | MongoDB physical du | MongoDB physical bytes/doc | TreeDB / MongoDB dbStats totalSize | TreeDB / MongoDB physical |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 | 0 | 1.75 MiB | 1835.4 B | 1.78 MiB | 1867.8 B | 288.09 KiB | 8.00 KiB | 300.11 MiB | 314687.5 B | 224.05x | <0.01x |
| 1000 | 1 | 3.50 MiB | 3670.4 B | 3.53 MiB | 3702.8 B | 288.09 KiB | 12.00 KiB | 300.11 MiB | 314691.6 B | 298.70x | 0.01x |
| 1000 | 2 | 4.50 MiB | 4719.0 B | 4.53 MiB | 4751.4 B | 288.09 KiB | 16.00 KiB | 300.12 MiB | 314695.7 B | 288.02x | 0.02x |
| 10000 | 0 | 5.75 MiB | 603.0 B | 5.78 MiB | 606.2 B | 2.79 MiB | 8.00 KiB | 300.11 MiB | 31468.7 B | 736.05x | 0.02x |
| 10000 | 1 | 9.75 MiB | 1022.4 B | 9.78 MiB | 1025.6 B | 2.79 MiB | 12.00 KiB | 300.11 MiB | 31469.2 B | 832.03x | 0.03x |
| 10000 | 2 | 12.25 MiB | 1284.5 B | 12.28 MiB | 1287.8 B | 2.79 MiB | 16.00 KiB | 300.12 MiB | 31469.6 B | 784.02x | 0.04x |

## Ops/Sec Summary

| docs | indexes | phase | TreeDB ops/sec | MongoDB ops/sec | TreeDB / MongoDB | TreeDB p95 us | MongoDB p95 us |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | 0 | `load_insert_many` | 66735 | 18756 | 3.56x | 7688 | 48846 |
| 1000 | 0 | `id_find_one` | 12472 | 8016 | 1.56x | 93.0 | 143 |
| 1000 | 0 | `email_find_one` | 117 | 3887 | 0.03x | 8771 | 373 |
| 1000 | 0 | `age_range_limit_10` | 107 | 4986 | 0.02x | 9561 | 272 |
| 1000 | 0 | `id_update_set` | 7284 | 7707 | 0.95x | 207 | 138 |
| 1000 | 1 | `load_insert_many` | 60716 | 84062 | 0.72x | 8335 | 5888 |
| 1000 | 1 | `id_find_one` | 12217 | 7355 | 1.66x | 94.0 | 198 |
| 1000 | 1 | `email_find_one` | 11550 | 6414 | 1.80x | 103 | 208 |
| 1000 | 1 | `age_range_limit_10` | 108 | 4330 | 0.02x | 9614 | 295 |
| 1000 | 1 | `id_update_set` | 5959 | 7449 | 0.80x | 213 | 158 |
| 1000 | 2 | `load_insert_many` | 60024 | 74833 | 0.80x | 8323 | 6706 |
| 1000 | 2 | `id_find_one` | 11660 | 8175 | 1.43x | 101 | 137 |
| 1000 | 2 | `email_find_one` | 11513 | 6785 | 1.70x | 100 | 162 |
| 1000 | 2 | `age_range_limit_10` | 108 | 4790 | 0.02x | 9568 | 244 |
| 1000 | 2 | `id_update_set` | 5475 | 7626 | 0.72x | 251 | 143 |
| 10000 | 0 | `load_insert_many` | 65804 | 78844 | 0.83x | 9062 | 3660 |
| 10000 | 0 | `id_find_one` | 12195 | 8074 | 1.51x | 98.0 | 141 |
| 10000 | 0 | `email_find_one` | 12.2 | 759 | 0.02x | 84363 | 2612 |
| 10000 | 0 | `age_range_limit_10` | 11.2 | 4878 | <0.01x | 90531 | 261 |
| 10000 | 0 | `id_update_set` | 7343 | 7745 | 0.95x | 190 | 147 |
| 10000 | 1 | `load_insert_many` | 65000 | 91588 | 0.71x | 8237 | 5498 |
| 10000 | 1 | `id_find_one` | 12116 | 8244 | 1.47x | 95.0 | 137 |
| 10000 | 1 | `email_find_one` | 11533 | 7157 | 1.61x | 101 | 156 |
| 10000 | 1 | `age_range_limit_10` | 11.2 | 5021 | <0.01x | 90927 | 224 |
| 10000 | 1 | `id_update_set` | 5639 | 7000 | 0.81x | 259 | 152 |
| 10000 | 2 | `load_insert_many` | 64766 | 81605 | 0.79x | 7966 | 6012 |
| 10000 | 2 | `id_find_one` | 11996 | 7821 | 1.53x | 95.0 | 148 |
| 10000 | 2 | `email_find_one` | 11424 | 7053 | 1.62x | 102 | 160 |
| 10000 | 2 | `age_range_limit_10` | 11.2 | 5058 | <0.01x | 92244 | 224 |
| 10000 | 2 | `id_update_set` | 4695 | 7247 | 0.65x | 282 | 150 |

## Raw Inputs

| docs | indexes | target | raw json |
| ---: | ---: | --- | --- |
| 1000 | 0 | treedb | `raw/treedb_docs_1000_idx_0.json` |
| 1000 | 0 | mongo | `raw/mongo_docs_1000_idx_0.json` |
| 1000 | 1 | treedb | `raw/treedb_docs_1000_idx_1.json` |
| 1000 | 1 | mongo | `raw/mongo_docs_1000_idx_1.json` |
| 1000 | 2 | treedb | `raw/treedb_docs_1000_idx_2.json` |
| 1000 | 2 | mongo | `raw/mongo_docs_1000_idx_2.json` |
| 10000 | 0 | treedb | `raw/treedb_docs_10000_idx_0.json` |
| 10000 | 0 | mongo | `raw/mongo_docs_10000_idx_0.json` |
| 10000 | 1 | treedb | `raw/treedb_docs_10000_idx_1.json` |
| 10000 | 1 | mongo | `raw/mongo_docs_10000_idx_1.json` |
| 10000 | 2 | treedb | `raw/treedb_docs_10000_idx_2.json` |
| 10000 | 2 | mongo | `raw/mongo_docs_10000_idx_2.json` |

## Notes

- TreeDB disk bytes come from `treedb_disk_after_checkpoint.total_bytes`, with `treedb_disk_after_load.total_bytes` as a fallback.
- TreeDB physical bytes come from the matrix runner's `du` measurement of the isolated TreeDB directory.
- MongoDB `dbStats.dataSize` is uncompressed logical document size, not disk usage.
- MongoDB `dbStats.totalSize` is reported separately because it can diverge sharply from the isolated data-directory `du` measurement on small WiredTiger workloads.
- MongoDB physical bytes are the preferred local disk comparison when the matrix runner has an isolated data directory, such as Docker mode.
- Ops/sec values are produced by the shared MongoDB Go driver workload in `cmd/mongo_gateway_bench`.
