# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `f34b10cd057bd85477f3b54b23ca39b53983da8d`
- binary SHA-256: `8f8995c9077a069541fbf78ea6a55aecea93c196a8662e3ad59f4a69ba2e4523`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_f34b10cd0 -out-dir /tmp/gomap-4290-artifacts-f34 -dir /tmp/gomap-4290-db-f34 -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision f34b10cd057bd85477f3b54b23ca39b53983da8d -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4290 candidate`

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
| 0 | 18 | 54 | 0.063943 | 281.50 | 844.51 | 2348039 | 4812 | 3748783 | true |
| 1 | 18 | 54 | 0.040344 | 446.17 | 1338.50 | 1849740 | 4680 | 3748790 | true |
| 2 | 18 | 54 | 0.042368 | 424.85 | 1274.55 | 1850447 | 4682 | 3748833 | true |
| 3 | 18 | 54 | 0.040513 | 444.31 | 1332.92 | 1850904 | 4677 | 3748827 | true |
| 4 | 18 | 54 | 0.041099 | 437.97 | 1313.91 | 1841477 | 4674 | 3748827 | true |

Median/p95 docs/s: **437.97 / 445.79**. Median/p95 B/source: **1850447 / 2248612**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **503.66**, B/source <= **1665402**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4590.7 | 0.208 | 0.231 | 0.352 | 0.7407 | 0.8889 | 0.8093 | 277658 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4732.8 | 0.205 | 0.222 | 0.373 | 0.5556 | 0.8333 | 1.0000 | 277721 | 2166.4 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18032.7 | 0.051 | 0.070 | 0.139 | 0.7407 | 0.8889 | 0.8093 | 132860 | 403.9 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18062.4 | 0.052 | 0.063 | 0.123 | 0.5556 | 0.8333 | 1.0000 | 132867 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5076.6 | 0.192 | 0.210 | 0.284 | 0.8889 | 0.8889 | 0.9256 | 215681 | 2064.2 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5079.5 | 0.192 | 0.209 | 0.277 | 0.5556 | 0.6667 | 1.0000 | 215672 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24771.2 | 0.039 | 0.046 | 0.061 | 0.8889 | 0.8889 | 0.9256 | 69889 | 301.4 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24342.6 | 0.038 | 0.049 | 0.105 | 0.5556 | 0.6667 | 1.0000 | 69898 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5785.3 | 0.169 | 0.181 | 0.205 | 0.6667 | 0.8889 | 0.6591 | 168558 | 1861.8 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5815.4 | 0.168 | 0.179 | 0.222 | 0.5556 | 0.8889 | 1.0000 | 168560 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 71554.6 | 0.013 | 0.015 | 0.018 | 0.6667 | 0.8889 | 0.6591 | 22486 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 68019.9 | 0.013 | 0.017 | 0.023 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13494.7 | 0.245 | 0.534 | 0.885 | 0.7407 | 0.8889 | 0.8093 | 277828 | 2166.4 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13966.0 | 0.238 | 0.545 | 0.836 | 0.5556 | 0.8333 | 1.0000 | 277963 | 2166.7 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 42176.2 | 0.073 | 0.191 | 0.310 | 0.7407 | 0.8889 | 0.8093 | 133055 | 403.9 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 43792.5 | 0.069 | 0.192 | 0.333 | 0.5556 | 0.8333 | 1.0000 | 133048 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 16187.6 | 0.204 | 0.471 | 0.703 | 0.8889 | 0.8889 | 0.9256 | 215905 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 11107.1 | 0.243 | 0.588 | 2.064 | 0.5556 | 0.6667 | 1.0000 | 216205 | 2064.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 77903.4 | 0.038 | 0.099 | 0.186 | 0.8889 | 0.8889 | 0.9256 | 70075 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 72943.3 | 0.039 | 0.107 | 0.210 | 0.5556 | 0.6667 | 1.0000 | 70067 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 19078.7 | 0.179 | 0.372 | 0.541 | 0.6667 | 0.8889 | 0.6591 | 168581 | 1862.1 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 18932.1 | 0.178 | 0.392 | 0.573 | 0.5556 | 0.8889 | 1.0000 | 168578 | 1862.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 185079.0 | 0.012 | 0.036 | 0.057 | 0.6667 | 0.8889 | 0.6591 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 210855.6 | 0.012 | 0.024 | 0.044 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1411.7 | 0.685 | 0.754 | 1.236 | 0.7407 | 0.8889 | 0.8093 | 582455 | 5256.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1425.0 | 0.684 | 0.753 | 1.247 | 0.5556 | 0.8333 | 1.0000 | 584086 | 5257.1 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2147.8 | 0.454 | 0.516 | 0.855 | 0.8889 | 0.8889 | 0.9256 | 353642 | 4508.4 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2189.1 | 0.450 | 0.489 | 0.691 | 0.5556 | 0.6667 | 1.0000 | 353800 | 4509.5 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 947.8 | 1.038 | 1.128 | 1.450 | 0.6667 | 0.8889 | 0.6591 | 681335 | 5148.6 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 955.1 | 1.032 | 1.116 | 1.398 | 0.5556 | 0.8889 | 1.0000 | 681507 | 5148.7 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4549.9 | 0.740 | 1.504 | 2.533 | 0.7407 | 0.8889 | 0.8093 | 582092 | 5258.3 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4812.1 | 0.719 | 1.425 | 2.036 | 0.5556 | 0.8333 | 1.0000 | 582912 | 5259.6 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7734.8 | 0.447 | 0.890 | 1.159 | 0.8889 | 0.8889 | 0.9256 | 354994 | 4512.7 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7882.9 | 0.442 | 0.888 | 1.082 | 0.5556 | 0.6667 | 1.0000 | 355215 | 4512.0 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 2930.3 | 1.195 | 2.170 | 2.876 | 0.6667 | 0.8889 | 0.6591 | 681469 | 5150.3 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3069.3 | 1.157 | 1.948 | 2.380 | 0.5556 | 0.8889 | 1.0000 | 681448 | 5150.5 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 576.4 | 1.682 | 2.036 | 2.649 | 0.8519 | 1.0000 | 0.8742 | 854536 | 9119.1 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 578.1 | 1.682 | 2.063 | 2.340 | 0.5556 | 0.7222 | 1.0000 | 854731 | 9119.4 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 18037.2 | 0.053 | 0.060 | 0.122 | 0.8519 | 1.0000 | 0.8742 | 134244 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17665.2 | 0.053 | 0.062 | 0.156 | 0.5556 | 0.7222 | 1.0000 | 134252 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 580.0 | 1.675 | 2.032 | 2.271 | 0.8889 | 0.8889 | 0.9256 | 790281 | 9012.5 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 589.3 | 1.659 | 1.966 | 2.205 | 0.5556 | 0.6667 | 1.0000 | 790294 | 9012.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24141.6 | 0.040 | 0.048 | 0.054 | 0.8889 | 0.8889 | 0.9256 | 69887 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 25062.9 | 0.039 | 0.045 | 0.053 | 0.5556 | 0.6667 | 1.0000 | 69874 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 584.5 | 1.647 | 1.970 | 2.267 | 0.7037 | 0.7778 | 0.7652 | 744003 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 593.0 | 1.648 | 1.962 | 2.188 | 0.5556 | 0.6667 | 1.0000 | 744007 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60621.7 | 0.015 | 0.021 | 0.033 | 0.7037 | 0.7778 | 0.7652 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 62445.1 | 0.015 | 0.017 | 0.021 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2030.3 | 1.762 | 2.981 | 3.913 | 0.8519 | 1.0000 | 0.8742 | 854748 | 9119.3 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2052.9 | 1.762 | 2.740 | 3.255 | 0.5556 | 0.7222 | 1.0000 | 855177 | 9120.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 39381.7 | 0.078 | 0.175 | 0.393 | 0.8519 | 1.0000 | 0.8742 | 134424 | 408.2 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 43209.9 | 0.074 | 0.171 | 0.335 | 0.5556 | 0.7222 | 1.0000 | 134436 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2119.2 | 1.711 | 2.619 | 3.598 | 0.8889 | 0.8889 | 0.9256 | 790861 | 9013.4 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2138.4 | 1.702 | 2.588 | 3.220 | 0.5556 | 0.6667 | 1.0000 | 791620 | 9013.3 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 79862.8 | 0.038 | 0.072 | 0.173 | 0.8889 | 0.8889 | 0.9256 | 70059 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 77475.2 | 0.039 | 0.098 | 0.177 | 0.5556 | 0.6667 | 1.0000 | 70053 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2182.2 | 1.673 | 2.565 | 3.292 | 0.7037 | 0.7778 | 0.7652 | 744047 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2193.6 | 1.675 | 2.513 | 3.082 | 0.5556 | 0.6667 | 1.0000 | 744047 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 160685.2 | 0.015 | 0.047 | 0.059 | 0.7037 | 0.7778 | 0.7652 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 142523.0 | 0.015 | 0.048 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 411.4 | 2.364 | 2.735 | 4.006 | 0.8519 | 1.0000 | 0.8742 | 1184375 | 12221.6 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 419.1 | 2.324 | 2.769 | 3.193 | 0.5556 | 0.7222 | 1.0000 | 1186081 | 12222.4 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 496.9 | 1.954 | 2.371 | 2.525 | 0.8889 | 0.8889 | 0.9256 | 930344 | 11457.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 497.9 | 1.951 | 2.356 | 2.576 | 0.5556 | 0.6667 | 1.0000 | 929943 | 11458.4 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 361.7 | 2.677 | 3.197 | 3.538 | 0.7037 | 0.7778 | 0.7652 | 1290808 | 12107.7 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 359.4 | 2.694 | 3.237 | 3.523 | 0.5556 | 0.6667 | 1.0000 | 1291404 | 12107.4 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1463.1 | 2.466 | 4.112 | 5.772 | 0.8519 | 1.0000 | 0.8742 | 1183420 | 12223.7 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1512.0 | 2.398 | 3.887 | 4.670 | 0.5556 | 0.7222 | 1.0000 | 1185346 | 12224.0 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1815.6 | 2.015 | 3.024 | 3.784 | 0.8889 | 0.8889 | 0.9256 | 930564 | 11459.8 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1821.4 | 2.036 | 2.945 | 3.454 | 0.5556 | 0.6667 | 1.0000 | 931309 | 11459.9 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1289.3 | 2.869 | 4.178 | 5.128 | 0.7037 | 0.7778 | 0.7652 | 1294931 | 12109.8 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1291.5 | 2.866 | 4.135 | 4.812 | 0.5556 | 0.6667 | 1.0000 | 1295143 | 12109.2 |

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
