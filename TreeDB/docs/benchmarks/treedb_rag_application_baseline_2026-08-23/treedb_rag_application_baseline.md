# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v2`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `99929cdeb2ae2ec1e411236c853eb36942075d72`
- harness revision: `e0eb90f15a4a3de2cafed54509235dbeea96bd83`
- binary SHA-256: `dc95599f0b32649ed298ef6a48fdd4d9e96ca32064bb27254977b97a0e0dbe59`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `d5d66991cfbd9a79acea6879b04bb20786179ce05ae3338c9fcb3bafba593057`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_e0eb90f15 -out-dir TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23 -dir /tmp/gomap-4289-rag-baseline-db-e0eb90f15-go126 -product-base-sha 99929cdeb2ae2ec1e411236c853eb36942075d72 -harness-revision e0eb90f15a4a3de2cafed54509235dbeea96bd83 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1`

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
| 0 | 18 | 54 | 0.120701 | 149.13 | 447.39 | 2673394 | 6192 | 7168367 | true |
| 1 | 18 | 54 | 0.073804 | 243.89 | 731.67 | 2164360 | 6068 | 7168738 | true |
| 2 | 18 | 54 | 0.058299 | 308.75 | 926.25 | 2157516 | 6048 | 7168557 | true |
| 3 | 18 | 54 | 0.056481 | 318.69 | 956.07 | 2159368 | 6052 | 7168557 | true |
| 4 | 18 | 54 | 0.052352 | 343.83 | 1031.49 | 2162690 | 6053 | 7168557 | true |

Median/p95 docs/s: **308.75 / 338.80**. Median/p95 B/source: **2162690 / 2571587**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **355.06**, B/source <= **1946421**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5313.7 | 0.169 | 0.276 | 0.524 | 0.5556 | 0.8333 | 1.0000 | 221979 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 16275.6 | 0.051 | 0.103 | 0.215 | 0.5556 | 0.8333 | 1.0000 | 132871 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6216.2 | 0.154 | 0.175 | 0.332 | 0.5556 | 0.6667 | 1.0000 | 158939 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24133.1 | 0.037 | 0.047 | 0.111 | 0.5556 | 0.6667 | 1.0000 | 69905 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5260.7 | 0.133 | 0.369 | 0.941 | 0.5556 | 0.8889 | 1.0000 | 111707 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 68114.4 | 0.013 | 0.017 | 0.035 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15237.1 | 0.204 | 0.548 | 0.899 | 0.5556 | 0.8333 | 1.0000 | 222151 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 35440.8 | 0.089 | 0.227 | 0.456 | 0.5556 | 0.8333 | 1.0000 | 133007 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 17712.9 | 0.169 | 0.431 | 0.694 | 0.5556 | 0.6667 | 1.0000 | 159340 | 1603.4 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 81863.5 | 0.037 | 0.069 | 0.181 | 0.5556 | 0.6667 | 1.0000 | 70075 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 19472.8 | 0.148 | 0.413 | 0.684 | 0.5556 | 0.8889 | 1.0000 | 111715 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 156004.7 | 0.013 | 0.047 | 0.066 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1315.4 | 0.623 | 1.223 | 2.245 | 0.5556 | 0.8333 | 1.0000 | 508786 | 4046.4 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2508.0 | 0.369 | 0.581 | 0.862 | 0.5556 | 0.6667 | 1.0000 | 275735 | 3298.1 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 838.3 | 1.022 | 1.726 | 3.000 | 0.5556 | 0.8889 | 1.0000 | 601343 | 3885.1 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4645.4 | 0.715 | 1.612 | 2.366 | 0.5556 | 0.8333 | 1.0000 | 507037 | 4049.3 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 8182.0 | 0.398 | 0.963 | 1.574 | 0.5556 | 0.6667 | 1.0000 | 277003 | 3301.6 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 2219.0 | 1.411 | 3.190 | 6.652 | 0.5556 | 0.8889 | 1.0000 | 601754 | 3886.8 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 476.4 | 1.844 | 3.163 | 7.740 | 0.5556 | 0.7222 | 1.0000 | 824772 | 8669.6 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 14126.1 | 0.056 | 0.134 | 0.195 | 0.5556 | 0.7222 | 1.0000 | 134271 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 585.1 | 1.630 | 2.058 | 2.996 | 0.5556 | 0.6667 | 1.0000 | 760179 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 23232.8 | 0.038 | 0.052 | 0.121 | 0.5556 | 0.6667 | 1.0000 | 69902 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 554.4 | 1.633 | 2.326 | 3.805 | 0.5556 | 0.6667 | 1.0000 | 715700 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 57408.7 | 0.016 | 0.021 | 0.031 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 1676.7 | 2.128 | 3.811 | 4.772 | 0.5556 | 0.7222 | 1.0000 | 825102 | 8670.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 27461.6 | 0.112 | 0.315 | 0.766 | 0.5556 | 0.7222 | 1.0000 | 134416 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2068.7 | 1.755 | 2.724 | 3.488 | 0.5556 | 0.6667 | 1.0000 | 761465 | 8563.3 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 73442.4 | 0.039 | 0.102 | 0.193 | 0.5556 | 0.6667 | 1.0000 | 70068 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 1607.7 | 2.010 | 3.948 | 7.737 | 0.5556 | 0.6667 | 1.0000 | 715729 | 8359.8 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 191476.8 | 0.014 | 0.023 | 0.051 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 360.7 | 2.592 | 4.115 | 5.237 | 0.5556 | 0.7222 | 1.0000 | 1136383 | 11022.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 498.2 | 1.907 | 2.499 | 3.392 | 0.5556 | 0.6667 | 1.0000 | 879294 | 10258.4 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 276.0 | 3.091 | 4.980 | 12.506 | 0.5556 | 0.6667 | 1.0000 | 1239923 | 10855.9 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 869.8 | 3.955 | 7.056 | 10.951 | 0.5556 | 0.7222 | 1.0000 | 1134280 | 11023.7 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1739.0 | 2.081 | 3.470 | 4.251 | 0.5556 | 0.6667 | 1.0000 | 880346 | 10259.2 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 813.1 | 4.186 | 8.834 | 21.194 | 0.5556 | 0.6667 | 1.0000 | 1241020 | 10857.7 |

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
