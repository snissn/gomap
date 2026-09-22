# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `99929cdeb2ae2ec1e411236c853eb36942075d72`
- harness revision: `43e9568e0059806b9a7f735a5e383800880d1865`
- binary SHA-256: `84cdf838df15dbc3254dbc9d0d2c13955b6ecf4b88140eeaee735869c2a18f5c`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_43e9568e0 -out-dir /Users/michaelseiler/orca/workspaces/gomap/4289-rag-baseline/TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23 -dir /tmp/gomap-4289-rag-baseline-db-43e9568e0-go126 -product-base-sha 99929cdeb2ae2ec1e411236c853eb36942075d72 -harness-revision 43e9568e0059806b9a7f735a5e383800880d1865 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1`

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
| 0 | 18 | 54 | 0.112375 | 160.18 | 480.54 | 2679898 | 6199 | 7168659 | true |
| 1 | 18 | 54 | 0.077515 | 232.21 | 696.64 | 2165451 | 6072 | 7168557 | true |
| 2 | 18 | 54 | 0.063604 | 283.00 | 849.01 | 2163595 | 6053 | 7168557 | true |
| 3 | 18 | 54 | 0.061696 | 291.75 | 875.25 | 2157374 | 6049 | 7168557 | true |
| 4 | 18 | 54 | 0.060548 | 297.29 | 891.86 | 2158564 | 6049 | 7168557 | true |

Median/p95 docs/s: **283.00 / 296.18**. Median/p95 B/source: **2163595 / 2577008**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5848.6 | 0.166 | 0.179 | 0.239 | 0.5556 | 0.8333 | 1.0000 | 221923 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18279.3 | 0.052 | 0.059 | 0.073 | 0.5556 | 0.8333 | 1.0000 | 132848 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6421.0 | 0.152 | 0.164 | 0.202 | 0.5556 | 0.6667 | 1.0000 | 158939 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24967.6 | 0.038 | 0.048 | 0.063 | 0.5556 | 0.6667 | 1.0000 | 69912 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7572.5 | 0.129 | 0.139 | 0.180 | 0.5556 | 0.8889 | 1.0000 | 111705 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 69990.0 | 0.013 | 0.016 | 0.027 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15766.0 | 0.197 | 0.493 | 0.765 | 0.5556 | 0.8333 | 1.0000 | 222120 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 43630.0 | 0.071 | 0.181 | 0.325 | 0.5556 | 0.8333 | 1.0000 | 133001 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 20422.5 | 0.164 | 0.366 | 0.531 | 0.5556 | 0.6667 | 1.0000 | 159354 | 1603.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 80923.8 | 0.037 | 0.075 | 0.180 | 0.5556 | 0.6667 | 1.0000 | 70054 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 22021.3 | 0.141 | 0.370 | 0.693 | 0.5556 | 0.8889 | 1.0000 | 111711 | 1400.5 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 201076.0 | 0.012 | 0.033 | 0.047 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1634.5 | 0.595 | 0.669 | 1.014 | 0.5556 | 0.8333 | 1.0000 | 508421 | 4047.2 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2698.3 | 0.362 | 0.415 | 0.594 | 0.5556 | 0.6667 | 1.0000 | 275959 | 3298.1 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1015.2 | 0.962 | 1.125 | 1.333 | 0.5556 | 0.8889 | 1.0000 | 601258 | 3884.8 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5389.9 | 0.642 | 1.303 | 1.977 | 0.5556 | 0.8333 | 1.0000 | 507141 | 4049.2 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9328.7 | 0.369 | 0.743 | 1.025 | 0.5556 | 0.6667 | 1.0000 | 276826 | 3301.7 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3335.0 | 1.081 | 1.751 | 2.156 | 0.5556 | 0.8889 | 1.0000 | 601566 | 3886.6 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 584.8 | 1.637 | 2.095 | 2.449 | 0.5556 | 0.7222 | 1.0000 | 824660 | 8669.5 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17451.5 | 0.053 | 0.071 | 0.153 | 0.5556 | 0.7222 | 1.0000 | 134243 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 579.9 | 1.626 | 2.129 | 2.613 | 0.5556 | 0.6667 | 1.0000 | 760206 | 8562.6 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24734.9 | 0.038 | 0.047 | 0.068 | 0.5556 | 0.6667 | 1.0000 | 69896 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 604.9 | 1.599 | 1.963 | 2.253 | 0.5556 | 0.6667 | 1.0000 | 715691 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60905.4 | 0.015 | 0.017 | 0.020 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2059.1 | 1.740 | 2.746 | 3.508 | 0.5556 | 0.7222 | 1.0000 | 825091 | 8670.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 39909.8 | 0.081 | 0.189 | 0.315 | 0.5556 | 0.7222 | 1.0000 | 134449 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2252.0 | 1.649 | 2.381 | 2.835 | 0.5556 | 0.6667 | 1.0000 | 761503 | 8563.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 75632.0 | 0.038 | 0.095 | 0.201 | 0.5556 | 0.6667 | 1.0000 | 70047 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2216.3 | 1.634 | 2.532 | 3.187 | 0.5556 | 0.6667 | 1.0000 | 715725 | 8359.8 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 194782.8 | 0.014 | 0.017 | 0.032 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 410.0 | 2.286 | 3.132 | 4.556 | 0.5556 | 0.7222 | 1.0000 | 1135699 | 11022.4 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 508.6 | 1.853 | 2.372 | 3.536 | 0.5556 | 0.6667 | 1.0000 | 879258 | 10258.7 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 373.4 | 2.589 | 3.128 | 3.466 | 0.5556 | 0.6667 | 1.0000 | 1238880 | 10855.8 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1485.3 | 2.387 | 4.092 | 5.563 | 0.5556 | 0.7222 | 1.0000 | 1133871 | 11023.6 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1872.0 | 1.939 | 2.973 | 3.952 | 0.5556 | 0.6667 | 1.0000 | 880089 | 10259.5 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1326.1 | 2.792 | 4.003 | 4.870 | 0.5556 | 0.6667 | 1.0000 | 1243254 | 10857.3 |

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
