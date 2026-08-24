# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `594575f03f5f0bac7d847b48a1329ee610458fae`
- binary SHA-256: `c406eb49c87ed0740a967b17408b278da81fcadc7dd8ef2b5ab19aeaa21fe364`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_594575f03 -out-dir /tmp/gomap-4291-artifacts-594 -dir /tmp/gomap-4291-db-594 -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision 594575f03f5f0bac7d847b48a1329ee610458fae -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 optimized candidate`

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
| 0 | 18 | 54 | 0.070576 | 255.04 | 765.13 | 2326714 | 4558 | 3743358 | true |
| 1 | 18 | 54 | 0.050344 | 357.54 | 1072.61 | 1820502 | 4428 | 3743378 | true |
| 2 | 18 | 54 | 0.041316 | 435.67 | 1307.01 | 1820380 | 4425 | 3743202 | true |
| 3 | 18 | 54 | 0.038741 | 464.63 | 1393.88 | 1831335 | 4424 | 3743269 | true |
| 4 | 18 | 54 | 0.042444 | 424.09 | 1272.26 | 1819620 | 4421 | 3743269 | true |

Median/p95 docs/s: **424.09 / 458.83**. Median/p95 B/source: **1820502 / 2227638**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **487.70**, B/source <= **1638452**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5792.9 | 0.167 | 0.185 | 0.236 | 0.5556 | 0.8333 | 1.0000 | 221968 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5551.1 | 0.173 | 0.193 | 0.322 | 0.5556 | 1.0000 | 1.0000 | 230881 | 1775.9 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18285.9 | 0.052 | 0.059 | 0.131 | 0.5556 | 0.8333 | 1.0000 | 132923 | 403.9 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 17327.0 | 0.055 | 0.062 | 0.145 | 0.5556 | 1.0000 | 1.0000 | 141901 | 474.4 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6329.6 | 0.154 | 0.168 | 0.227 | 0.5556 | 0.6667 | 1.0000 | 158978 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5978.0 | 0.163 | 0.174 | 0.190 | 0.5556 | 0.8333 | 1.0000 | 184741 | 1706.9 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24573.7 | 0.039 | 0.047 | 0.062 | 0.5556 | 0.6667 | 1.0000 | 69937 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 19236.4 | 0.048 | 0.060 | 0.116 | 0.5556 | 0.8333 | 1.0000 | 95956 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7519.3 | 0.130 | 0.139 | 0.152 | 0.5556 | 0.8889 | 1.0000 | 111737 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7143.4 | 0.137 | 0.146 | 0.161 | 0.5556 | 0.9444 | 1.0000 | 137088 | 1504.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 69690.7 | 0.013 | 0.016 | 0.019 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 48409.7 | 0.019 | 0.022 | 0.036 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 17061.0 | 0.196 | 0.431 | 0.764 | 0.5556 | 0.8333 | 1.0000 | 222198 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15496.8 | 0.212 | 0.484 | 0.735 | 0.5556 | 1.0000 | 1.0000 | 231034 | 1776.0 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 42698.0 | 0.072 | 0.187 | 0.320 | 0.5556 | 0.8333 | 1.0000 | 133059 | 403.9 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 43723.7 | 0.071 | 0.189 | 0.289 | 0.5556 | 1.0000 | 1.0000 | 142095 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 19846.5 | 0.165 | 0.392 | 0.556 | 0.5556 | 0.6667 | 1.0000 | 159398 | 1603.4 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 18204.4 | 0.174 | 0.417 | 0.733 | 0.5556 | 0.8333 | 1.0000 | 184951 | 1707.1 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 77077.8 | 0.038 | 0.097 | 0.199 | 0.5556 | 0.6667 | 1.0000 | 70100 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 59103.4 | 0.048 | 0.137 | 0.229 | 0.5556 | 0.8333 | 1.0000 | 96171 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 24551.9 | 0.140 | 0.267 | 0.444 | 0.5556 | 0.8889 | 1.0000 | 111748 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 23083.8 | 0.146 | 0.318 | 0.444 | 0.5556 | 0.9444 | 1.0000 | 137099 | 1504.3 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 166777.9 | 0.013 | 0.042 | 0.058 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 131720.4 | 0.019 | 0.054 | 0.100 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1630.2 | 0.600 | 0.659 | 1.011 | 0.5556 | 0.8333 | 1.0000 | 507245 | 4046.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1598.3 | 0.609 | 0.658 | 0.946 | 0.5556 | 1.0000 | 1.0000 | 514791 | 4118.6 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2683.1 | 0.369 | 0.397 | 0.466 | 0.5556 | 0.6667 | 1.0000 | 275994 | 3298.3 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2735.9 | 0.362 | 0.383 | 0.531 | 0.5556 | 0.8333 | 1.0000 | 282666 | 2897.6 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1040.5 | 0.949 | 1.020 | 1.289 | 0.5556 | 0.8889 | 1.0000 | 598252 | 3884.9 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5321.1 | 0.642 | 1.283 | 2.240 | 0.5556 | 0.8333 | 1.0000 | 506222 | 4048.5 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5173.8 | 0.655 | 1.421 | 2.141 | 0.5556 | 1.0000 | 1.0000 | 515269 | 4121.5 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9617.9 | 0.364 | 0.688 | 0.941 | 0.5556 | 0.6667 | 1.0000 | 276557 | 3301.5 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9829.6 | 0.352 | 0.658 | 0.997 | 0.5556 | 0.8333 | 1.0000 | 283069 | 2901.8 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3308.9 | 1.068 | 1.843 | 2.239 | 0.5556 | 0.8889 | 1.0000 | 598942 | 3886.9 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 595.4 | 1.630 | 2.002 | 2.334 | 0.5556 | 0.7222 | 1.0000 | 824587 | 8669.3 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 595.1 | 1.636 | 1.906 | 2.358 | 0.5556 | 0.8333 | 1.0000 | 828545 | 8747.7 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17882.7 | 0.053 | 0.061 | 0.101 | 0.5556 | 0.7222 | 1.0000 | 134289 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17031.3 | 0.056 | 0.062 | 0.090 | 0.5556 | 0.8333 | 1.0000 | 143788 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 606.2 | 1.613 | 1.891 | 2.150 | 0.5556 | 0.6667 | 1.0000 | 760177 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 597.4 | 1.634 | 1.967 | 2.198 | 0.5556 | 0.8333 | 1.0000 | 780861 | 8665.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24851.7 | 0.039 | 0.045 | 0.053 | 0.5556 | 0.6667 | 1.0000 | 69926 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 18624.8 | 0.050 | 0.070 | 0.118 | 0.5556 | 0.8333 | 1.0000 | 95960 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 608.6 | 1.601 | 1.939 | 2.193 | 0.5556 | 0.6667 | 1.0000 | 715718 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 610.8 | 1.603 | 1.890 | 2.112 | 0.5556 | 0.8333 | 1.0000 | 741019 | 8463.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60901.5 | 0.015 | 0.018 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 42782.9 | 0.022 | 0.025 | 0.039 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2119.2 | 1.710 | 2.558 | 3.461 | 0.5556 | 0.7222 | 1.0000 | 825102 | 8669.9 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2083.7 | 1.725 | 2.746 | 3.757 | 0.5556 | 0.8333 | 1.0000 | 828817 | 8748.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 41774.0 | 0.074 | 0.171 | 0.349 | 0.5556 | 0.7222 | 1.0000 | 134435 | 408.2 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 41356.5 | 0.077 | 0.182 | 0.359 | 0.5556 | 0.8333 | 1.0000 | 143952 | 487.9 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2221.1 | 1.655 | 2.437 | 3.222 | 0.5556 | 0.6667 | 1.0000 | 761508 | 8563.1 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2166.0 | 1.671 | 2.607 | 3.219 | 0.5556 | 0.8333 | 1.0000 | 781355 | 8666.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 86716.5 | 0.037 | 0.048 | 0.090 | 0.5556 | 0.6667 | 1.0000 | 70070 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 62335.7 | 0.048 | 0.124 | 0.231 | 0.5556 | 0.8333 | 1.0000 | 96160 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2220.4 | 1.635 | 2.573 | 3.140 | 0.5556 | 0.6667 | 1.0000 | 715749 | 8359.7 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2217.8 | 1.637 | 2.531 | 3.295 | 0.5556 | 0.8333 | 1.0000 | 741051 | 8463.5 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 191890.9 | 0.015 | 0.021 | 0.031 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 129916.2 | 0.021 | 0.041 | 0.107 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 437.1 | 2.236 | 2.618 | 2.959 | 0.5556 | 0.7222 | 1.0000 | 1135327 | 11022.3 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 430.9 | 2.258 | 2.642 | 3.587 | 0.5556 | 0.8333 | 1.0000 | 1137675 | 11102.3 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 520.3 | 1.866 | 2.253 | 2.475 | 0.5556 | 0.6667 | 1.0000 | 879021 | 10258.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 529.8 | 1.841 | 2.195 | 2.427 | 0.5556 | 0.8333 | 1.0000 | 880428 | 9857.0 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 374.0 | 2.598 | 3.089 | 3.368 | 0.5556 | 0.6667 | 1.0000 | 1236028 | 10855.8 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1548.2 | 2.346 | 3.689 | 4.525 | 0.5556 | 0.7222 | 1.0000 | 1133604 | 11023.9 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1547.7 | 2.329 | 3.779 | 5.416 | 0.5556 | 0.8333 | 1.0000 | 1136724 | 11104.1 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1873.5 | 1.938 | 3.034 | 3.632 | 0.5556 | 0.6667 | 1.0000 | 879006 | 10259.1 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1919.0 | 1.914 | 2.803 | 3.340 | 0.5556 | 0.8333 | 1.0000 | 880783 | 9859.0 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1338.9 | 2.755 | 4.019 | 4.586 | 0.5556 | 0.6667 | 1.0000 | 1237189 | 10857.1 |

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
