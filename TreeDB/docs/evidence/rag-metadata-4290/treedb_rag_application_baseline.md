# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `241e5a2afcadc19d63309b348a1797ffc2c6a8f1`
- binary SHA-256: `5c7c7eda9f337ef9a00c0e2b2b0a2ae84dc20132a4b5b11abcc4e7d25aa90577`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_241e5a2af -out-dir /tmp/gomap-4290-artifacts-241 -dir /tmp/gomap-4290-db-241 -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision 241e5a2afcadc19d63309b348a1797ffc2c6a8f1 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4290 corrected candidate`

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
| 0 | 18 | 54 | 0.068683 | 262.07 | 786.22 | 2339526 | 4807 | 3748800 | true |
| 1 | 18 | 54 | 0.044780 | 401.96 | 1205.89 | 1854961 | 4683 | 3748827 | true |
| 2 | 18 | 54 | 0.042902 | 419.56 | 1258.68 | 1845995 | 4676 | 3748833 | true |
| 3 | 18 | 54 | 0.036810 | 489.00 | 1467.01 | 1840752 | 4672 | 3748827 | true |
| 4 | 18 | 54 | 0.042447 | 424.06 | 1272.17 | 1852888 | 4676 | 3748827 | true |

Median/p95 docs/s: **419.56 / 476.01**. Median/p95 B/source: **1852888 / 2242613**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **482.49**, B/source <= **1667599**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4545.5 | 0.209 | 0.234 | 0.383 | 1.0000 | 1.0000 | 1.0000 | 253335 | 2274.2 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4742.1 | 0.205 | 0.218 | 0.338 | 0.5556 | 0.8333 | 1.0000 | 277713 | 2166.4 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18595.8 | 0.051 | 0.058 | 0.120 | 1.0000 | 1.0000 | 1.0000 | 107766 | 510.5 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18284.7 | 0.052 | 0.058 | 0.082 | 0.5556 | 0.8333 | 1.0000 | 132841 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 4842.6 | 0.201 | 0.216 | 0.242 | 1.0000 | 1.0000 | 1.0000 | 221648 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5055.3 | 0.193 | 0.208 | 0.256 | 0.5556 | 0.6667 | 1.0000 | 215650 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 20096.0 | 0.048 | 0.053 | 0.058 | 1.0000 | 1.0000 | 1.0000 | 75514 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24625.7 | 0.039 | 0.048 | 0.067 | 0.5556 | 0.6667 | 1.0000 | 69906 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5544.5 | 0.175 | 0.190 | 0.223 | 1.0000 | 1.0000 | 1.0000 | 183327 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5761.9 | 0.170 | 0.182 | 0.221 | 0.5556 | 0.8889 | 1.0000 | 168560 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 52705.9 | 0.017 | 0.021 | 0.034 | 1.0000 | 1.0000 | 1.0000 | 37513 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 70163.5 | 0.013 | 0.016 | 0.019 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13000.3 | 0.253 | 0.552 | 0.943 | 1.0000 | 1.0000 | 1.0000 | 253465 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 14363.5 | 0.237 | 0.527 | 0.803 | 0.5556 | 0.8333 | 1.0000 | 277977 | 2166.6 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 47112.3 | 0.066 | 0.155 | 0.280 | 1.0000 | 1.0000 | 1.0000 | 107924 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 44818.7 | 0.067 | 0.175 | 0.351 | 0.5556 | 0.8333 | 1.0000 | 133015 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 14828.2 | 0.213 | 0.544 | 0.776 | 1.0000 | 1.0000 | 1.0000 | 221807 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 16474.2 | 0.205 | 0.473 | 0.674 | 0.5556 | 0.6667 | 1.0000 | 216239 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 58510.2 | 0.047 | 0.140 | 0.232 | 1.0000 | 1.0000 | 1.0000 | 75680 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 74136.1 | 0.039 | 0.102 | 0.162 | 0.5556 | 0.6667 | 1.0000 | 70063 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 17796.3 | 0.186 | 0.424 | 0.691 | 1.0000 | 1.0000 | 1.0000 | 183398 | 1972.3 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 18914.5 | 0.180 | 0.401 | 0.539 | 0.5556 | 0.8889 | 1.0000 | 168573 | 1862.0 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 117630.1 | 0.019 | 0.066 | 0.124 | 1.0000 | 1.0000 | 1.0000 | 37548 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 213280.1 | 0.012 | 0.022 | 0.036 | 0.5556 | 0.8889 | 1.0000 | 22488 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1434.6 | 0.661 | 0.752 | 1.639 | 1.0000 | 1.0000 | 1.0000 | 557956 | 5370.8 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1473.3 | 0.662 | 0.735 | 1.277 | 0.5556 | 0.8333 | 1.0000 | 583597 | 5257.0 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2301.5 | 0.425 | 0.464 | 0.659 | 1.0000 | 1.0000 | 1.0000 | 360857 | 4480.6 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2278.2 | 0.430 | 0.467 | 0.737 | 0.5556 | 0.6667 | 1.0000 | 353369 | 4509.3 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1205.4 | 0.820 | 0.873 | 1.015 | 1.0000 | 1.0000 | 1.0000 | 375083 | 3830.2 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1216.6 | 0.813 | 0.865 | 1.103 | 0.5556 | 0.8889 | 1.0000 | 383329 | 3824.1 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4455.1 | 0.774 | 1.461 | 2.196 | 1.0000 | 1.0000 | 1.0000 | 558861 | 5374.1 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4573.7 | 0.736 | 1.564 | 2.277 | 0.5556 | 0.8333 | 1.0000 | 583377 | 5259.8 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7585.1 | 0.451 | 0.926 | 1.269 | 1.0000 | 1.0000 | 1.0000 | 361233 | 4483.7 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7801.1 | 0.450 | 0.864 | 1.132 | 0.5556 | 0.6667 | 1.0000 | 355031 | 4512.6 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 4174.8 | 0.861 | 1.446 | 1.753 | 1.0000 | 1.0000 | 1.0000 | 375770 | 3832.2 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 4220.0 | 0.856 | 1.325 | 1.773 | 0.5556 | 0.8889 | 1.0000 | 383609 | 3825.9 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 577.9 | 1.685 | 1.981 | 2.565 | 1.0000 | 1.0000 | 1.0000 | 832939 | 9222.1 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 580.5 | 1.679 | 2.023 | 2.341 | 0.5556 | 0.7222 | 1.0000 | 854682 | 9119.3 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17666.2 | 0.051 | 0.073 | 0.146 | 1.0000 | 1.0000 | 1.0000 | 109035 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17992.9 | 0.053 | 0.060 | 0.089 | 0.5556 | 0.7222 | 1.0000 | 134258 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 585.4 | 1.671 | 1.980 | 2.245 | 1.0000 | 1.0000 | 1.0000 | 797683 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 581.6 | 1.664 | 2.044 | 2.320 | 0.5556 | 0.6667 | 1.0000 | 790272 | 9012.6 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 19597.4 | 0.049 | 0.055 | 0.074 | 1.0000 | 1.0000 | 1.0000 | 75514 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24921.7 | 0.038 | 0.045 | 0.051 | 0.5556 | 0.6667 | 1.0000 | 69901 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 589.2 | 1.654 | 1.992 | 2.273 | 1.0000 | 1.0000 | 1.0000 | 760984 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 593.5 | 1.643 | 1.983 | 2.250 | 0.5556 | 0.6667 | 1.0000 | 744002 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 48408.3 | 0.018 | 0.025 | 0.042 | 1.0000 | 1.0000 | 1.0000 | 38773 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 59887.9 | 0.015 | 0.018 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2056.9 | 1.758 | 2.761 | 3.769 | 1.0000 | 1.0000 | 1.0000 | 833119 | 9222.3 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2063.3 | 1.758 | 2.717 | 3.262 | 0.5556 | 0.7222 | 1.0000 | 855148 | 9119.9 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 44241.7 | 0.073 | 0.159 | 0.254 | 1.0000 | 1.0000 | 1.0000 | 109160 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 44067.5 | 0.068 | 0.159 | 0.389 | 0.5556 | 0.7222 | 1.0000 | 134391 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2120.3 | 1.716 | 2.655 | 3.416 | 1.0000 | 1.0000 | 1.0000 | 798124 | 9165.7 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2130.4 | 1.707 | 2.624 | 3.407 | 0.5556 | 0.6667 | 1.0000 | 791617 | 9013.3 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 58632.3 | 0.048 | 0.125 | 0.301 | 1.0000 | 1.0000 | 1.0000 | 75667 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 70027.5 | 0.040 | 0.113 | 0.143 | 0.5556 | 0.6667 | 1.0000 | 70060 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2165.8 | 1.690 | 2.526 | 3.156 | 1.0000 | 1.0000 | 1.0000 | 761400 | 8920.1 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2179.6 | 1.677 | 2.511 | 3.136 | 0.5556 | 0.6667 | 1.0000 | 744048 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 108603.3 | 0.022 | 0.071 | 0.145 | 1.0000 | 1.0000 | 1.0000 | 38828 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 196179.2 | 0.014 | 0.021 | 0.035 | 0.5556 | 0.6667 | 1.0000 | 23768 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 416.6 | 2.335 | 2.715 | 3.908 | 1.0000 | 1.0000 | 1.0000 | 1163807 | 12330.3 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 414.5 | 2.358 | 2.775 | 3.135 | 0.5556 | 0.7222 | 1.0000 | 1186723 | 12222.6 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 502.1 | 1.938 | 2.339 | 2.525 | 1.0000 | 1.0000 | 1.0000 | 938402 | 11429.3 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 495.6 | 1.955 | 2.376 | 2.605 | 0.5556 | 0.6667 | 1.0000 | 930403 | 11458.7 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 150.6 | 6.561 | 7.206 | 7.577 | 1.0000 | 1.0000 | 1.0000 | 1791566 | 4110.7 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 149.5 | 6.598 | 7.270 | 7.713 | 0.5556 | 0.6667 | 1.0000 | 1802724 | 4106.2 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1466.9 | 2.459 | 4.009 | 5.570 | 1.0000 | 1.0000 | 1.0000 | 1162731 | 12332.1 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1465.1 | 2.503 | 4.034 | 4.898 | 0.5556 | 0.7222 | 1.0000 | 1184315 | 12224.2 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1834.7 | 1.993 | 3.000 | 3.632 | 1.0000 | 1.0000 | 1.0000 | 940155 | 11432.3 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1797.9 | 2.052 | 3.024 | 3.652 | 0.5556 | 0.6667 | 1.0000 | 932135 | 11459.9 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 549.3 | 6.973 | 8.520 | 9.630 | 1.0000 | 1.0000 | 1.0000 | 1796001 | 4113.6 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 556.3 | 6.973 | 8.476 | 9.485 | 0.5556 | 0.6667 | 1.0000 | 1804490 | 4107.4 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+parent_collapse_unavailable`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+parent_collapse_unavailable+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `parent_collapse_unavailable`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `parent_collapse_unavailable+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.

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
