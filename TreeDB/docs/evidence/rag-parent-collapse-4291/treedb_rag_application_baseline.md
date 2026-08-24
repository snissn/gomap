# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `53c2b5f3ed5d64ad2f90699d6dbed9b62eaa499a`
- binary SHA-256: `3eb6f8c93b39ea955fa3808e58f6843971772f2bc0aac9aa063bd10840434155`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_53c2b5f3e -out-dir /tmp/gomap-4291-artifacts-53c -dir /tmp/gomap-4291-db-53c -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision 53c2b5f3ed5d64ad2f90699d6dbed9b62eaa499a -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 final candidate`

## Independent semantic evidence

- model: `sentence-transformers/all-MiniLM-L6-v2`
- revision: `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`
- license: `Apache-2.0`
- dimensions: `384`
- preprocessing: SentenceTransformer.encode(normalize_embeddings=True); model tokenizer; max_seq_length=256
- corpus license: MIT (gomap repository fixture)
- generation: `python3 TreeDB/cmd/treedb_rag_benchmark/testdata/generate_semantic_vectors.py --inputs TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_inputs.json --output TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_vectors.json (sentence-transformers==5.4.1, transformers==5.8.0, torch==2.11.0)`

## Actual source ingestion (`IngestSources`)

Five fresh-DB rows include embedding, index publication, and checkpoint in end-to-end source docs/s.

| rep | sources | chunks | end-to-end s | source docs/s | chunk docs/s | B/source | allocs/source | storage bytes | reopen |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 18 | 54 | 0.071129 | 253.06 | 759.18 | 2339303 | 4563 | 3743358 | true |
| 1 | 18 | 54 | 0.042383 | 424.69 | 1274.08 | 1833301 | 4428 | 3743269 | true |
| 2 | 18 | 54 | 0.041215 | 436.73 | 1310.20 | 1826732 | 4426 | 3743359 | true |
| 3 | 18 | 54 | 0.038441 | 468.26 | 1404.77 | 1831670 | 4422 | 3743269 | true |
| 4 | 18 | 54 | 0.045455 | 395.99 | 1187.98 | 1823699 | 4421 | 3743269 | true |

Median/p95 docs/s: **424.69 / 461.95**. Median/p95 B/source: **1831670 / 2238102**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **488.40**, B/source <= **1648503**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5747.6 | 0.167 | 0.189 | 0.333 | 0.5556 | 0.8333 | 1.0000 | 221978 | 1705.6 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5534.3 | 0.173 | 0.196 | 0.324 | 0.5556 | 1.0000 | 1.0000 | 230896 | 1775.9 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 18344.2 | 0.052 | 0.058 | 0.123 | 0.5556 | 0.8333 | 1.0000 | 132882 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 17550.9 | 0.054 | 0.061 | 0.129 | 0.5556 | 1.0000 | 1.0000 | 141908 | 474.5 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 6341.5 | 0.154 | 0.168 | 0.202 | 0.5556 | 0.6667 | 1.0000 | 158979 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 5926.0 | 0.164 | 0.180 | 0.214 | 0.5556 | 0.8333 | 1.0000 | 184726 | 1706.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24542.9 | 0.038 | 0.047 | 0.062 | 0.5556 | 0.6667 | 1.0000 | 69930 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19512.4 | 0.049 | 0.055 | 0.062 | 0.5556 | 0.8333 | 1.0000 | 95955 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 7431.5 | 0.131 | 0.143 | 0.171 | 0.5556 | 0.8889 | 1.0000 | 111739 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 7137.6 | 0.137 | 0.148 | 0.170 | 0.5556 | 0.9444 | 1.0000 | 137088 | 1504.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 69679.9 | 0.013 | 0.016 | 0.020 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 45476.9 | 0.019 | 0.026 | 0.058 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 16792.0 | 0.197 | 0.475 | 0.728 | 0.5556 | 0.8333 | 1.0000 | 222234 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 16360.6 | 0.204 | 0.441 | 0.748 | 0.5556 | 1.0000 | 1.0000 | 231021 | 1776.0 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 44841.2 | 0.068 | 0.179 | 0.317 | 0.5556 | 0.8333 | 1.0000 | 133026 | 403.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 42843.4 | 0.074 | 0.176 | 0.321 | 0.5556 | 1.0000 | 1.0000 | 142089 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 19783.9 | 0.165 | 0.402 | 0.592 | 0.5556 | 0.6667 | 1.0000 | 159396 | 1603.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 19259.6 | 0.173 | 0.385 | 0.582 | 0.5556 | 0.8333 | 1.0000 | 184914 | 1707.1 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 74274.1 | 0.038 | 0.108 | 0.181 | 0.5556 | 0.6667 | 1.0000 | 70080 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 56054.6 | 0.048 | 0.138 | 0.278 | 0.5556 | 0.8333 | 1.0000 | 96142 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 23740.5 | 0.140 | 0.308 | 0.421 | 0.5556 | 0.8889 | 1.0000 | 111746 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 22450.0 | 0.147 | 0.344 | 0.488 | 0.5556 | 0.9444 | 1.0000 | 137096 | 1504.2 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 210895.5 | 0.012 | 0.023 | 0.038 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 127827.9 | 0.019 | 0.058 | 0.089 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1636.8 | 0.598 | 0.656 | 1.082 | 0.5556 | 0.8333 | 1.0000 | 506901 | 4046.1 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1592.5 | 0.609 | 0.671 | 1.129 | 0.5556 | 1.0000 | 1.0000 | 514888 | 4118.6 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2854.3 | 0.344 | 0.378 | 0.620 | 0.5556 | 0.6667 | 1.0000 | 256752 | 2790.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2745.5 | 0.361 | 0.387 | 0.525 | 0.5556 | 0.8333 | 1.0000 | 282806 | 2897.7 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1031.2 | 0.949 | 1.050 | 1.352 | 0.5556 | 0.8889 | 1.0000 | 598736 | 3885.1 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5246.0 | 0.641 | 1.383 | 1.954 | 0.5556 | 0.8333 | 1.0000 | 506015 | 4048.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4897.9 | 0.692 | 1.403 | 2.492 | 0.5556 | 1.0000 | 1.0000 | 515242 | 4121.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 10140.2 | 0.343 | 0.657 | 0.869 | 0.5556 | 0.6667 | 1.0000 | 257053 | 2793.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 9698.9 | 0.354 | 0.727 | 1.041 | 0.5556 | 0.8333 | 1.0000 | 283359 | 2901.8 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3310.6 | 1.087 | 1.772 | 2.249 | 0.5556 | 0.8889 | 1.0000 | 598485 | 3886.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 581.5 | 1.632 | 2.055 | 2.509 | 0.5556 | 0.7222 | 1.0000 | 824621 | 8669.3 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 591.1 | 1.643 | 1.919 | 2.430 | 0.5556 | 0.8333 | 1.0000 | 828548 | 8747.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17807.6 | 0.053 | 0.064 | 0.096 | 0.5556 | 0.7222 | 1.0000 | 134282 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16818.4 | 0.056 | 0.067 | 0.120 | 0.5556 | 0.8333 | 1.0000 | 143803 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 603.2 | 1.614 | 1.922 | 2.179 | 0.5556 | 0.6667 | 1.0000 | 760233 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 597.2 | 1.631 | 1.960 | 2.165 | 0.5556 | 0.8333 | 1.0000 | 780848 | 8665.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 25272.1 | 0.038 | 0.044 | 0.048 | 0.5556 | 0.6667 | 1.0000 | 69912 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19253.6 | 0.050 | 0.058 | 0.079 | 0.5556 | 0.8333 | 1.0000 | 95961 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 608.5 | 1.601 | 1.934 | 2.183 | 0.5556 | 0.6667 | 1.0000 | 715729 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 604.8 | 1.607 | 1.955 | 2.236 | 0.5556 | 0.8333 | 1.0000 | 741016 | 8463.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 60143.5 | 0.015 | 0.017 | 0.021 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41582.1 | 0.022 | 0.028 | 0.036 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1981.7 | 1.712 | 3.153 | 5.082 | 0.5556 | 0.7222 | 1.0000 | 825106 | 8669.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2100.2 | 1.710 | 2.712 | 4.009 | 0.5556 | 0.8333 | 1.0000 | 828795 | 8747.9 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43416.4 | 0.073 | 0.180 | 0.328 | 0.5556 | 0.7222 | 1.0000 | 134441 | 408.2 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39716.5 | 0.081 | 0.180 | 0.347 | 0.5556 | 0.8333 | 1.0000 | 143965 | 487.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2210.4 | 1.656 | 2.528 | 3.052 | 0.5556 | 0.6667 | 1.0000 | 761628 | 8563.3 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2181.1 | 1.672 | 2.608 | 3.222 | 0.5556 | 0.8333 | 1.0000 | 781335 | 8666.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 81774.0 | 0.038 | 0.069 | 0.141 | 0.5556 | 0.6667 | 1.0000 | 70083 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 59705.5 | 0.048 | 0.128 | 0.213 | 0.5556 | 0.8333 | 1.0000 | 96119 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2230.9 | 1.630 | 2.531 | 3.442 | 0.5556 | 0.6667 | 1.0000 | 715752 | 8359.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2242.3 | 1.633 | 2.463 | 3.064 | 0.5556 | 0.8333 | 1.0000 | 741060 | 8463.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 183699.0 | 0.015 | 0.030 | 0.051 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 119555.2 | 0.021 | 0.062 | 0.098 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 420.2 | 2.265 | 2.829 | 3.451 | 0.5556 | 0.7222 | 1.0000 | 1135459 | 11022.3 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 432.5 | 2.238 | 2.668 | 3.752 | 0.5556 | 0.8333 | 1.0000 | 1137931 | 11102.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 532.1 | 1.831 | 2.173 | 2.404 | 0.5556 | 0.6667 | 1.0000 | 859752 | 9751.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 528.9 | 1.844 | 2.187 | 2.415 | 0.5556 | 0.8333 | 1.0000 | 880379 | 9856.9 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 373.7 | 2.595 | 3.126 | 3.373 | 0.5556 | 0.6667 | 1.0000 | 1236509 | 10855.6 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1556.0 | 2.348 | 3.716 | 4.370 | 0.5556 | 0.7222 | 1.0000 | 1133863 | 11024.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1539.6 | 2.342 | 3.809 | 5.194 | 0.5556 | 0.8333 | 1.0000 | 1137066 | 11104.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1967.4 | 1.880 | 2.717 | 3.021 | 0.5556 | 0.6667 | 1.0000 | 861084 | 9751.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1916.6 | 1.908 | 2.832 | 3.330 | 0.5556 | 0.8333 | 1.0000 | 880554 | 9858.6 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1345.4 | 2.750 | 3.934 | 4.701 | 0.5556 | 0.6667 | 1.0000 | 1236752 | 10857.0 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 20 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable`: 4 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 4 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated`: 68 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+http_score_only_route_unavailable`: 20 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+http_vector_parent_collapse_unavailable`: 4 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 4 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable`: 136 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+http_score_only_route_unavailable`: 40 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+http_vector_parent_collapse_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.

## Exact controls

- `hashing_regression`: 54 vectors, chunk recall@10 0.5556, parent recall@10 0.8889, nDCG@10 1.0000; offline exhaustive cosine over hash-bound final vectors; excluded from product QPS and fallback counters.
- `semantic_minilm`: 54 vectors, chunk recall@10 0.5556, parent recall@10 0.6667, nDCG@10 1.0000; offline exhaustive cosine over hash-bound final vectors; excluded from product QPS and fallback counters.

## Lifecycle and durability

- `hashing_regression`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; IngestSources publishes each source replacement as one dependency-closed durable root selection.
- `semantic_minilm`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; IngestSources publishes each source replacement as one dependency-closed durable root selection.

## Frozen structural/noise policy

- cross-tenant results = 0
- cross-workspace results = 0
- full-document-scan fallbacks = 0
- score-only document fetches = 0
- fetch rows <= TopK documents
- fresh DB; five repetitions; median is decision statistic; p95 disclosed; >10% unaffected QPS or p99 regression blocks; quality/work/projection digests must match
