# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `99929cdeb2ae2ec1e411236c853eb36942075d72`
- harness revision: `e3de4a6f1e7de8450081c7357a9ff5575cf847bd`
- binary SHA-256: `88b4f1485307ff1b4cd00ec2d1769df94e49b61d479772d5c2121ff8b0848d7e`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_e3de4a6f1 -out-dir /Users/michaelseiler/orca/workspaces/gomap/4289-rag-baseline/TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23 -dir /tmp/gomap-4289-rag-baseline-db-e3de4a6f1-go126 -product-base-sha 99929cdeb2ae2ec1e411236c853eb36942075d72 -harness-revision e3de4a6f1e7de8450081c7357a9ff5575cf847bd -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1`

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
| 0 | 18 | 54 | 0.130111 | 138.34 | 415.03 | 2666104 | 6196 | 7168626 | true |
| 1 | 18 | 54 | 0.076785 | 234.42 | 703.26 | 2164257 | 6066 | 7168557 | true |
| 2 | 18 | 54 | 0.076337 | 235.80 | 707.39 | 2165365 | 6057 | 7168367 | true |
| 3 | 18 | 54 | 0.075318 | 238.99 | 716.96 | 2163009 | 6058 | 7168711 | true |
| 4 | 18 | 54 | 0.060703 | 296.52 | 889.57 | 2161028 | 6050 | 7168557 | true |

Median/p95 docs/s: **235.80 / 285.02**. Median/p95 B/source: **2164257 / 2565956**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **271.17**, B/source <= **1947831**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4465.7 | 0.170 | 0.433 | 0.820 | 0.5556 | 0.8333 | 1.0000 | 221941 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 15857.0 | 0.051 | 0.125 | 0.204 | 0.5556 | 0.8333 | 1.0000 | 132879 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5616.1 | 0.158 | 0.270 | 0.502 | 0.5556 | 0.6667 | 1.0000 | 158939 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 23984.4 | 0.037 | 0.055 | 0.122 | 0.5556 | 0.6667 | 1.0000 | 69905 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 6406.1 | 0.134 | 0.281 | 0.396 | 0.5556 | 0.8889 | 1.0000 | 111707 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 65865.5 | 0.013 | 0.023 | 0.045 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13036.2 | 0.235 | 0.568 | 0.907 | 0.5556 | 0.8333 | 1.0000 | 222108 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 35831.6 | 0.079 | 0.254 | 0.446 | 0.5556 | 0.8333 | 1.0000 | 133021 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 15124.1 | 0.189 | 0.514 | 0.766 | 0.5556 | 0.6667 | 1.0000 | 159326 | 1603.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 42648.4 | 0.039 | 0.141 | 0.458 | 0.5556 | 0.6667 | 1.0000 | 70061 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 18605.7 | 0.160 | 0.384 | 0.519 | 0.5556 | 0.8889 | 1.0000 | 111715 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 195476.0 | 0.012 | 0.033 | 0.053 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1456.6 | 0.623 | 1.017 | 1.801 | 0.5556 | 0.8333 | 1.0000 | 507545 | 4045.8 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2351.1 | 0.385 | 0.643 | 0.971 | 0.5556 | 0.6667 | 1.0000 | 276030 | 3298.2 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 847.7 | 1.049 | 1.971 | 2.563 | 0.5556 | 0.8889 | 1.0000 | 601705 | 3885.1 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 1903.3 | 1.489 | 7.228 | 16.134 | 0.5556 | 0.8333 | 1.0000 | 507535 | 4050.8 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7476.7 | 0.455 | 0.960 | 1.390 | 0.5556 | 0.6667 | 1.0000 | 276537 | 3301.7 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 2474.6 | 1.458 | 2.630 | 3.553 | 0.5556 | 0.8889 | 1.0000 | 601194 | 3886.4 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 525.9 | 1.760 | 2.527 | 3.439 | 0.5556 | 0.7222 | 1.0000 | 824761 | 8669.6 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17191.0 | 0.052 | 0.075 | 0.152 | 0.5556 | 0.7222 | 1.0000 | 134288 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 349.9 | 2.077 | 6.230 | 17.284 | 0.5556 | 0.6667 | 1.0000 | 760208 | 8562.6 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 15805.7 | 0.039 | 0.129 | 0.379 | 0.5556 | 0.6667 | 1.0000 | 69910 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 377.0 | 1.942 | 7.163 | 13.782 | 0.5556 | 0.6667 | 1.0000 | 715695 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 58649.8 | 0.015 | 0.021 | 0.042 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 1920.0 | 1.820 | 3.395 | 4.700 | 0.5556 | 0.7222 | 1.0000 | 825171 | 8670.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 36664.4 | 0.084 | 0.195 | 0.458 | 0.5556 | 0.7222 | 1.0000 | 134409 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 1264.3 | 2.227 | 6.668 | 15.436 | 0.5556 | 0.6667 | 1.0000 | 761451 | 8563.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 46316.1 | 0.044 | 0.168 | 0.342 | 0.5556 | 0.6667 | 1.0000 | 70061 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2025.5 | 1.736 | 3.105 | 4.261 | 0.5556 | 0.6667 | 1.0000 | 715717 | 8359.7 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 161230.6 | 0.015 | 0.046 | 0.071 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 365.2 | 2.586 | 3.543 | 4.668 | 0.5556 | 0.7222 | 1.0000 | 1136617 | 11022.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 334.4 | 2.375 | 5.046 | 10.585 | 0.5556 | 0.6667 | 1.0000 | 879274 | 10258.5 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 334.1 | 2.840 | 3.940 | 6.532 | 0.5556 | 0.6667 | 1.0000 | 1239487 | 10856.2 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1354.5 | 2.681 | 4.473 | 5.473 | 0.5556 | 0.7222 | 1.0000 | 1134122 | 11024.5 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1535.5 | 2.327 | 4.242 | 6.409 | 0.5556 | 0.6667 | 1.0000 | 879842 | 10259.7 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1015.9 | 3.591 | 6.210 | 9.073 | 0.5556 | 0.6667 | 1.0000 | 1244037 | 10858.3 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 12 rows; `*main.capabilityError`; zero results; fail closed.
- `parent_collapse_unavailable`: 36 rows; `*main.capabilityError`; zero results; fail closed.
- `parent_collapse_unavailable+http_score_only_route_unavailable`: 12 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated`: 36 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+http_score_only_route_unavailable`: 12 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+parent_collapse_unavailable`: 72 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+multi_field_filter_unavailable+parent_collapse_unavailable+http_score_only_route_unavailable`: 24 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+parent_collapse_unavailable`: 36 rows; `*main.capabilityError`; zero results; fail closed.
- `source_metadata_not_propagated+parent_collapse_unavailable+http_score_only_route_unavailable`: 12 rows; `*main.capabilityError`; zero results; fail closed.

## Exact controls

- `hashing_regression`: 54 vectors, chunk recall@10 0.5556, parent recall@10 0.8889, nDCG@10 1.0000; offline exhaustive cosine over hash-bound final vectors; excluded from product QPS and fallback counters.
- `semantic_minilm`: 54 vectors, chunk recall@10 0.5556, parent recall@10 0.6667, nDCG@10 1.0000; offline exhaustive cosine over hash-bound final vectors; excluded from product QPS and fallback counters.

## Lifecycle and durability

- `hashing_regression`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; source ingestion storage publication remains commit-ambiguous until #4284.
- `semantic_minilm`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; source ingestion storage publication remains commit-ambiguous until #4284.

## Frozen structural/noise policy

- cross-tenant results = 0
- cross-workspace results = 0
- full-document-scan fallbacks = 0
- score-only document fetches = 0
- fetch rows <= TopK documents
- fresh DB; five repetitions; median is decision statistic; p95 disclosed; >10% unaffected QPS or p99 regression blocks; quality/work/projection digests must match
