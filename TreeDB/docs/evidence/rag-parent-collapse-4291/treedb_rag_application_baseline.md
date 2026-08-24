# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `4f96d46078a08e3bf705c87d2ead793ca48ea5b0`
- binary SHA-256: `ff801b145d71c7b87c68dd91f86d1959e03fb409e7b066d4c397ad0ae98a87e4`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_4f96d4607 -out-dir /tmp/gomap-4291-artifacts -dir /tmp/gomap-4291-db -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision 4f96d46078a08e3bf705c87d2ead793ca48ea5b0 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 candidate`

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
| 0 | 18 | 54 | 0.073134 | 246.12 | 738.37 | 2309287 | 4555 | 3743358 | true |
| 1 | 18 | 54 | 0.039911 | 451.01 | 1353.02 | 1830070 | 4426 | 3743358 | true |
| 2 | 18 | 54 | 0.041297 | 435.87 | 1307.60 | 1828248 | 4424 | 3743269 | true |
| 3 | 18 | 54 | 0.035436 | 507.96 | 1523.88 | 1825114 | 4421 | 3743269 | true |
| 4 | 18 | 54 | 0.046593 | 386.33 | 1158.98 | 1838274 | 4424 | 3743269 | true |

Median/p95 docs/s: **435.87 / 496.57**. Median/p95 B/source: **1830070 / 2215084**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **501.25**, B/source <= **1647063**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5653.4 | 0.169 | 0.186 | 0.249 | 0.5556 | 0.8333 | 1.0000 | 221894 | 1705.5 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5523.0 | 0.173 | 0.190 | 0.251 | 0.5556 | 1.0000 | 1.0000 | 230853 | 1775.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 16994.7 | 0.054 | 0.065 | 0.170 | 0.5556 | 0.8333 | 1.0000 | 132867 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 16533.9 | 0.056 | 0.068 | 0.148 | 0.5556 | 1.0000 | 1.0000 | 141947 | 474.5 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6373.8 | 0.153 | 0.167 | 0.187 | 0.5556 | 0.6667 | 1.0000 | 158979 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5971.7 | 0.163 | 0.176 | 0.209 | 0.5556 | 0.8333 | 1.0000 | 184734 | 1706.9 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24448.9 | 0.039 | 0.047 | 0.067 | 0.5556 | 0.6667 | 1.0000 | 69930 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 19819.1 | 0.049 | 0.055 | 0.088 | 0.5556 | 0.8333 | 1.0000 | 95962 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7485.0 | 0.131 | 0.141 | 0.156 | 0.5556 | 0.8889 | 1.0000 | 111738 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7176.6 | 0.136 | 0.145 | 0.160 | 0.5556 | 0.9444 | 1.0000 | 137089 | 1504.2 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 70141.4 | 0.013 | 0.016 | 0.019 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 50156.5 | 0.019 | 0.021 | 0.030 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15996.8 | 0.200 | 0.430 | 0.950 | 0.5556 | 0.8333 | 1.0000 | 222046 | 1705.7 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15164.0 | 0.212 | 0.495 | 0.741 | 0.5556 | 1.0000 | 1.0000 | 231008 | 1775.9 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 45401.7 | 0.073 | 0.150 | 0.298 | 0.5556 | 0.8333 | 1.0000 | 132996 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 38488.2 | 0.078 | 0.232 | 0.380 | 0.5556 | 1.0000 | 1.0000 | 142070 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 18611.3 | 0.168 | 0.398 | 0.594 | 0.5556 | 0.6667 | 1.0000 | 159392 | 1603.5 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 19057.7 | 0.174 | 0.400 | 0.584 | 0.5556 | 0.8333 | 1.0000 | 184958 | 1707.1 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 72388.2 | 0.039 | 0.106 | 0.168 | 0.5556 | 0.6667 | 1.0000 | 70121 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 61608.9 | 0.047 | 0.120 | 0.238 | 0.5556 | 0.8333 | 1.0000 | 96177 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 23005.4 | 0.142 | 0.331 | 0.449 | 0.5556 | 0.8889 | 1.0000 | 111745 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 22125.5 | 0.147 | 0.337 | 0.506 | 0.5556 | 0.9444 | 1.0000 | 137099 | 1504.3 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 212606.0 | 0.012 | 0.021 | 0.030 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 139108.0 | 0.018 | 0.046 | 0.081 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1613.0 | 0.604 | 0.656 | 1.076 | 0.5556 | 0.8333 | 1.0000 | 505365 | 4045.5 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1600.1 | 0.605 | 0.656 | 0.889 | 0.5556 | 1.0000 | 1.0000 | 514162 | 4118.3 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2670.6 | 0.370 | 0.397 | 0.528 | 0.5556 | 0.6667 | 1.0000 | 276020 | 3298.2 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2755.7 | 0.360 | 0.386 | 0.533 | 0.5556 | 0.8333 | 1.0000 | 282371 | 2897.6 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1029.8 | 0.953 | 1.047 | 1.372 | 0.5556 | 0.8889 | 1.0000 | 598168 | 3885.0 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1827.2 | 0.536 | 0.584 | 0.861 | 0.5556 | 0.9444 | 1.0000 | 399868 | 3439.7 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5145.8 | 0.657 | 1.336 | 2.245 | 0.5556 | 0.8333 | 1.0000 | 506284 | 4048.9 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5304.5 | 0.650 | 1.280 | 2.007 | 0.5556 | 1.0000 | 1.0000 | 514199 | 4121.1 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9095.1 | 0.380 | 0.751 | 1.005 | 0.5556 | 0.6667 | 1.0000 | 276759 | 3301.6 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9774.3 | 0.351 | 0.712 | 0.998 | 0.5556 | 0.8333 | 1.0000 | 282962 | 2901.5 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3391.0 | 1.062 | 1.729 | 2.093 | 0.5556 | 0.8889 | 1.0000 | 598568 | 3886.2 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 6265.0 | 0.543 | 1.120 | 1.691 | 0.5556 | 0.9444 | 1.0000 | 398946 | 3441.9 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 591.9 | 1.638 | 1.944 | 2.470 | 0.5556 | 0.7222 | 1.0000 | 824499 | 8669.1 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 593.5 | 1.639 | 1.906 | 2.328 | 0.5556 | 0.8333 | 1.0000 | 828508 | 8747.7 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17660.6 | 0.054 | 0.064 | 0.134 | 0.5556 | 0.7222 | 1.0000 | 134252 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 15761.1 | 0.056 | 0.090 | 0.168 | 0.5556 | 0.8333 | 1.0000 | 143737 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 599.0 | 1.624 | 1.944 | 2.157 | 0.5556 | 0.6667 | 1.0000 | 760226 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 598.7 | 1.630 | 1.942 | 2.253 | 0.5556 | 0.8333 | 1.0000 | 780828 | 8665.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24833.6 | 0.039 | 0.047 | 0.052 | 0.5556 | 0.6667 | 1.0000 | 69912 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 20139.7 | 0.048 | 0.053 | 0.068 | 0.5556 | 0.8333 | 1.0000 | 95974 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 595.7 | 1.605 | 1.975 | 2.603 | 0.5556 | 0.6667 | 1.0000 | 715726 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 611.8 | 1.596 | 1.868 | 2.174 | 0.5556 | 0.8333 | 1.0000 | 741016 | 8463.0 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60421.7 | 0.015 | 0.018 | 0.022 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 42618.8 | 0.022 | 0.026 | 0.036 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2084.0 | 1.727 | 2.685 | 3.508 | 0.5556 | 0.7222 | 1.0000 | 824686 | 8669.3 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2097.8 | 1.716 | 2.722 | 3.762 | 0.5556 | 0.8333 | 1.0000 | 828722 | 8747.8 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 43968.1 | 0.069 | 0.149 | 0.432 | 0.5556 | 0.7222 | 1.0000 | 134402 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 38961.4 | 0.081 | 0.206 | 0.346 | 0.5556 | 0.8333 | 1.0000 | 143899 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2162.2 | 1.659 | 2.619 | 3.540 | 0.5556 | 0.6667 | 1.0000 | 761504 | 8563.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2159.2 | 1.676 | 2.525 | 3.237 | 0.5556 | 0.8333 | 1.0000 | 781364 | 8666.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 78836.4 | 0.039 | 0.083 | 0.152 | 0.5556 | 0.6667 | 1.0000 | 70083 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 66234.3 | 0.047 | 0.098 | 0.237 | 0.5556 | 0.8333 | 1.0000 | 96119 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2240.9 | 1.630 | 2.527 | 3.293 | 0.5556 | 0.6667 | 1.0000 | 715755 | 8359.8 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2212.9 | 1.639 | 2.512 | 3.129 | 0.5556 | 0.8333 | 1.0000 | 741056 | 8463.5 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 168221.0 | 0.015 | 0.037 | 0.062 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 121571.6 | 0.021 | 0.059 | 0.105 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 429.9 | 2.267 | 2.660 | 3.494 | 0.5556 | 0.7222 | 1.0000 | 1133376 | 11021.4 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 432.1 | 2.242 | 2.634 | 3.314 | 0.5556 | 0.8333 | 1.0000 | 1137113 | 11102.1 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 521.3 | 1.859 | 2.262 | 2.537 | 0.5556 | 0.6667 | 1.0000 | 878883 | 10258.2 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 527.6 | 1.844 | 2.242 | 2.480 | 0.5556 | 0.8333 | 1.0000 | 879852 | 9856.6 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 372.8 | 2.604 | 3.143 | 3.426 | 0.5556 | 0.6667 | 1.0000 | 1236770 | 10855.7 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 449.3 | 2.152 | 2.642 | 2.982 | 0.5556 | 0.8333 | 1.0000 | 1029647 | 10411.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1528.6 | 2.332 | 3.827 | 5.488 | 0.5556 | 0.7222 | 1.0000 | 1132864 | 11023.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1530.1 | 2.376 | 3.635 | 5.696 | 0.5556 | 0.8333 | 1.0000 | 1136634 | 11104.0 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1892.6 | 1.919 | 2.987 | 3.579 | 0.5556 | 0.6667 | 1.0000 | 879710 | 10259.1 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1920.9 | 1.918 | 2.816 | 3.285 | 0.5556 | 0.8333 | 1.0000 | 881819 | 9859.4 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1337.1 | 2.759 | 3.955 | 4.686 | 0.5556 | 0.6667 | 1.0000 | 1241916 | 10857.8 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1652.5 | 2.189 | 3.403 | 4.144 | 0.5556 | 0.8333 | 1.0000 | 1028848 | 10412.9 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable`: 144 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+http_score_only_route_unavailable`: 48 rows; `*main.capabilityError`; zero results; fail closed.

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
