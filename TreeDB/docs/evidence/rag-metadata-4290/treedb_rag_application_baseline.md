# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `9cf3c53c432b1a5027263bb8d1b227709f3538fc`
- binary SHA-256: `8663cc17046d4717bf8e537371b86f493ddb98c4a4234d1406eaa8e2dcfda553`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_9cf3c53c4 -out-dir /tmp/gomap-4290-artifacts-9cf -dir /tmp/gomap-4290-db-9cf -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision 9cf3c53c432b1a5027263bb8d1b227709f3538fc -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4290 final candidate`

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
| 0 | 18 | 54 | 0.069810 | 257.84 | 773.53 | 2355704 | 4806 | 3748790 | true |
| 1 | 18 | 54 | 0.045194 | 398.28 | 1194.85 | 1839286 | 4681 | 3748827 | true |
| 2 | 18 | 54 | 0.042445 | 424.07 | 1272.22 | 1850420 | 4677 | 3748827 | true |
| 3 | 18 | 54 | 0.043314 | 415.57 | 1246.70 | 1855144 | 4673 | 3748827 | true |
| 4 | 18 | 54 | 0.035952 | 500.67 | 1502.01 | 1849823 | 4676 | 3748827 | true |

Median/p95 docs/s: **415.57 / 485.35**. Median/p95 B/source: **1850420 / 2255592**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **477.90**, B/source <= **1665378**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4593.0 | 0.208 | 0.230 | 0.274 | 1.0000 | 1.0000 | 1.0000 | 253308 | 2274.2 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4645.1 | 0.207 | 0.222 | 0.290 | 0.5556 | 0.8333 | 1.0000 | 277644 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 17663.8 | 0.053 | 0.061 | 0.094 | 1.0000 | 1.0000 | 1.0000 | 107779 | 510.5 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 17151.5 | 0.053 | 0.065 | 0.135 | 0.5556 | 0.8333 | 1.0000 | 132819 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 4854.8 | 0.200 | 0.219 | 0.297 | 1.0000 | 1.0000 | 1.0000 | 221633 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5086.6 | 0.192 | 0.209 | 0.288 | 0.5556 | 0.6667 | 1.0000 | 215667 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 20212.4 | 0.047 | 0.055 | 0.073 | 1.0000 | 1.0000 | 1.0000 | 75521 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24514.8 | 0.039 | 0.048 | 0.072 | 0.5556 | 0.6667 | 1.0000 | 69898 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5600.4 | 0.173 | 0.188 | 0.271 | 1.0000 | 1.0000 | 1.0000 | 183332 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5781.2 | 0.169 | 0.183 | 0.226 | 0.5556 | 0.8889 | 1.0000 | 168559 | 1861.8 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 53651.9 | 0.017 | 0.021 | 0.029 | 1.0000 | 1.0000 | 1.0000 | 37499 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 68948.7 | 0.013 | 0.017 | 0.021 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13856.3 | 0.244 | 0.478 | 0.815 | 1.0000 | 1.0000 | 1.0000 | 253427 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13663.0 | 0.239 | 0.478 | 0.973 | 0.5556 | 0.8333 | 1.0000 | 277824 | 2166.4 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 47534.3 | 0.067 | 0.150 | 0.261 | 1.0000 | 1.0000 | 1.0000 | 107856 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 43413.1 | 0.075 | 0.164 | 0.335 | 0.5556 | 0.8333 | 1.0000 | 132952 | 403.8 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 14967.3 | 0.213 | 0.500 | 0.752 | 1.0000 | 1.0000 | 1.0000 | 221935 | 2217.0 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 16786.8 | 0.203 | 0.438 | 0.620 | 0.5556 | 0.6667 | 1.0000 | 216196 | 2064.7 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 63451.1 | 0.047 | 0.120 | 0.190 | 1.0000 | 1.0000 | 1.0000 | 75672 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 75097.1 | 0.038 | 0.108 | 0.195 | 0.5556 | 0.6667 | 1.0000 | 70054 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 17901.1 | 0.186 | 0.427 | 0.557 | 1.0000 | 1.0000 | 1.0000 | 183528 | 1972.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 19211.1 | 0.178 | 0.377 | 0.559 | 0.5556 | 0.8889 | 1.0000 | 168575 | 1862.0 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 132674.0 | 0.019 | 0.053 | 0.110 | 1.0000 | 1.0000 | 1.0000 | 37542 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 194758.1 | 0.012 | 0.035 | 0.054 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1419.1 | 0.685 | 0.747 | 0.932 | 1.0000 | 1.0000 | 1.0000 | 557606 | 5370.7 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1417.6 | 0.685 | 0.739 | 1.202 | 0.5556 | 0.8333 | 1.0000 | 582207 | 5256.4 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2212.7 | 0.447 | 0.477 | 0.652 | 1.0000 | 1.0000 | 1.0000 | 360843 | 4480.6 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2184.5 | 0.452 | 0.483 | 0.763 | 0.5556 | 0.6667 | 1.0000 | 353570 | 4509.4 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1573.3 | 0.621 | 0.682 | 1.056 | 1.0000 | 1.0000 | 1.0000 | 469754 | 4666.2 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 941.2 | 1.040 | 1.152 | 1.497 | 0.5556 | 0.8889 | 1.0000 | 681223 | 5148.6 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4595.1 | 0.738 | 1.475 | 2.564 | 1.0000 | 1.0000 | 1.0000 | 557754 | 5373.6 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4568.9 | 0.729 | 1.482 | 2.872 | 0.5556 | 0.8333 | 1.0000 | 582539 | 5259.2 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7618.9 | 0.451 | 0.940 | 1.222 | 1.0000 | 1.0000 | 1.0000 | 361265 | 4484.3 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7794.6 | 0.447 | 0.872 | 1.152 | 0.5556 | 0.6667 | 1.0000 | 354581 | 4512.2 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 5400.4 | 0.632 | 1.329 | 1.985 | 1.0000 | 1.0000 | 1.0000 | 469036 | 4668.9 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3105.3 | 1.147 | 1.947 | 2.374 | 0.5556 | 0.8889 | 1.0000 | 682615 | 5150.9 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 573.1 | 1.687 | 2.041 | 2.466 | 1.0000 | 1.0000 | 1.0000 | 832915 | 9222.1 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 579.4 | 1.679 | 1.995 | 2.421 | 0.5556 | 0.7222 | 1.0000 | 854524 | 9119.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 18095.8 | 0.053 | 0.059 | 0.077 | 1.0000 | 1.0000 | 1.0000 | 108999 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17338.5 | 0.054 | 0.068 | 0.088 | 0.5556 | 0.7222 | 1.0000 | 134227 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 581.6 | 1.674 | 2.000 | 2.331 | 1.0000 | 1.0000 | 1.0000 | 797650 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 586.4 | 1.661 | 2.009 | 2.272 | 0.5556 | 0.6667 | 1.0000 | 790272 | 9012.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 20036.2 | 0.048 | 0.053 | 0.071 | 1.0000 | 1.0000 | 1.0000 | 75507 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 25129.9 | 0.038 | 0.045 | 0.056 | 0.5556 | 0.6667 | 1.0000 | 69894 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 596.3 | 1.641 | 1.940 | 2.140 | 1.0000 | 1.0000 | 1.0000 | 760979 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 592.0 | 1.645 | 2.003 | 2.246 | 0.5556 | 0.6667 | 1.0000 | 744005 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 48398.2 | 0.019 | 0.024 | 0.031 | 1.0000 | 1.0000 | 1.0000 | 38779 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 59857.5 | 0.015 | 0.021 | 0.024 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2017.2 | 1.760 | 2.937 | 4.338 | 1.0000 | 1.0000 | 1.0000 | 833027 | 9222.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2043.9 | 1.758 | 2.851 | 3.812 | 0.5556 | 0.7222 | 1.0000 | 854709 | 9119.4 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 43333.5 | 0.073 | 0.159 | 0.252 | 1.0000 | 1.0000 | 1.0000 | 109149 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 44777.5 | 0.074 | 0.166 | 0.302 | 0.5556 | 0.7222 | 1.0000 | 134377 | 408.1 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2141.9 | 1.708 | 2.544 | 3.114 | 1.0000 | 1.0000 | 1.0000 | 798358 | 9165.7 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2109.0 | 1.709 | 2.676 | 3.773 | 0.5556 | 0.6667 | 1.0000 | 791656 | 9013.4 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 59145.8 | 0.047 | 0.139 | 0.221 | 1.0000 | 1.0000 | 1.0000 | 75685 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 78015.6 | 0.038 | 0.099 | 0.168 | 0.5556 | 0.6667 | 1.0000 | 70025 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2164.1 | 1.682 | 2.621 | 3.215 | 1.0000 | 1.0000 | 1.0000 | 761262 | 8920.0 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2155.9 | 1.687 | 2.574 | 3.238 | 0.5556 | 0.6667 | 1.0000 | 744046 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 115487.0 | 0.025 | 0.076 | 0.128 | 1.0000 | 1.0000 | 1.0000 | 38827 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 165875.6 | 0.014 | 0.036 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 413.5 | 2.337 | 2.722 | 3.630 | 1.0000 | 1.0000 | 1.0000 | 1162669 | 12329.8 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 415.0 | 2.332 | 2.766 | 3.759 | 0.5556 | 0.7222 | 1.0000 | 1183981 | 12221.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 496.7 | 1.951 | 2.372 | 2.550 | 1.0000 | 1.0000 | 1.0000 | 938293 | 11429.5 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 500.6 | 1.947 | 2.338 | 2.539 | 0.5556 | 0.6667 | 1.0000 | 930021 | 11458.6 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 434.2 | 2.228 | 2.741 | 2.973 | 1.0000 | 1.0000 | 1.0000 | 1072811 | 11624.9 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 360.8 | 2.693 | 3.239 | 3.489 | 0.5556 | 0.6667 | 1.0000 | 1294307 | 12108.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1481.3 | 2.425 | 3.951 | 5.872 | 1.0000 | 1.0000 | 1.0000 | 1162413 | 12332.4 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1465.9 | 2.453 | 4.124 | 5.434 | 0.5556 | 0.7222 | 1.0000 | 1183245 | 12223.4 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1817.4 | 2.025 | 2.951 | 3.473 | 1.0000 | 1.0000 | 1.0000 | 939711 | 11431.6 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1831.0 | 1.999 | 3.003 | 3.428 | 0.5556 | 0.6667 | 1.0000 | 931752 | 11460.0 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1527.6 | 2.359 | 3.899 | 4.621 | 1.0000 | 1.0000 | 1.0000 | 1071815 | 11626.3 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1271.5 | 2.918 | 4.203 | 4.899 | 0.5556 | 0.6667 | 1.0000 | 1294061 | 12108.9 |

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
