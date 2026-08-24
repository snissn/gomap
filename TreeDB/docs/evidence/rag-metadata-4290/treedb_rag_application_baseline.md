# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `ab277539635c89d1e75ea88bb74aba99d25a9bd3`
- binary SHA-256: `bee1d1942bb05769e11c80d324bd37119f42b769d2ce14c8770889db4bdae08c`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_ab2775396 -out-dir /tmp/gomap-4290-artifacts-ab -dir /tmp/gomap-4290-db-ab -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision ab277539635c89d1e75ea88bb74aba99d25a9bd3 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4290 indexed candidate`

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
| 0 | 18 | 54 | 0.070064 | 256.91 | 770.73 | 2347332 | 4807 | 3748783 | true |
| 1 | 18 | 54 | 0.044184 | 407.39 | 1222.17 | 1857811 | 4684 | 3748827 | true |
| 2 | 18 | 54 | 0.042323 | 425.30 | 1275.90 | 1846853 | 4679 | 3748802 | true |
| 3 | 18 | 54 | 0.038422 | 468.48 | 1405.45 | 1844936 | 4676 | 3748827 | true |
| 4 | 18 | 54 | 0.035302 | 509.88 | 1529.64 | 1850476 | 4674 | 3748827 | true |

Median/p95 docs/s: **425.30 / 501.60**. Median/p95 B/source: **1850476 / 2249427**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **489.09**, B/source <= **1665429**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4584.0 | 0.209 | 0.229 | 0.270 | 1.0000 | 1.0000 | 1.0000 | 253315 | 2274.2 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 4641.0 | 0.207 | 0.223 | 0.403 | 0.5556 | 0.8333 | 1.0000 | 277668 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18412.4 | 0.050 | 0.067 | 0.126 | 1.0000 | 1.0000 | 1.0000 | 107744 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 17199.1 | 0.053 | 0.063 | 0.134 | 0.5556 | 0.8333 | 1.0000 | 132799 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 4865.5 | 0.200 | 0.216 | 0.323 | 1.0000 | 1.0000 | 1.0000 | 221627 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5081.9 | 0.192 | 0.208 | 0.288 | 0.5556 | 0.6667 | 1.0000 | 215643 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 20267.7 | 0.048 | 0.053 | 0.063 | 1.0000 | 1.0000 | 1.0000 | 75508 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24851.0 | 0.038 | 0.046 | 0.058 | 0.5556 | 0.6667 | 1.0000 | 69898 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5601.1 | 0.174 | 0.188 | 0.239 | 1.0000 | 1.0000 | 1.0000 | 183324 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 5745.0 | 0.170 | 0.183 | 0.219 | 0.5556 | 0.8889 | 1.0000 | 168562 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 52450.0 | 0.017 | 0.021 | 0.027 | 1.0000 | 1.0000 | 1.0000 | 37500 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 69589.6 | 0.013 | 0.016 | 0.020 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13666.0 | 0.243 | 0.539 | 0.743 | 1.0000 | 1.0000 | 1.0000 | 253425 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 13436.2 | 0.247 | 0.583 | 0.951 | 0.5556 | 0.8333 | 1.0000 | 277790 | 2166.3 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 42315.2 | 0.074 | 0.163 | 0.292 | 1.0000 | 1.0000 | 1.0000 | 107891 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 33316.6 | 0.090 | 0.238 | 0.626 | 0.5556 | 0.8333 | 1.0000 | 132974 | 403.8 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 16724.0 | 0.210 | 0.401 | 0.697 | 1.0000 | 1.0000 | 1.0000 | 221880 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 16229.2 | 0.205 | 0.492 | 0.686 | 0.5556 | 0.6667 | 1.0000 | 216132 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 59653.4 | 0.047 | 0.133 | 0.277 | 1.0000 | 1.0000 | 1.0000 | 75674 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 77633.3 | 0.038 | 0.094 | 0.167 | 0.5556 | 0.6667 | 1.0000 | 70083 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 16765.0 | 0.191 | 0.456 | 0.620 | 1.0000 | 1.0000 | 1.0000 | 183489 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 18854.1 | 0.180 | 0.371 | 0.544 | 0.5556 | 0.8889 | 1.0000 | 168581 | 1862.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 114813.5 | 0.021 | 0.068 | 0.109 | 1.0000 | 1.0000 | 1.0000 | 37508 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 219516.1 | 0.012 | 0.020 | 0.028 | 0.5556 | 0.8889 | 1.0000 | 22488 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1407.4 | 0.687 | 0.751 | 1.119 | 1.0000 | 1.0000 | 1.0000 | 557968 | 5370.7 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1407.3 | 0.688 | 0.764 | 1.258 | 0.5556 | 0.8333 | 1.0000 | 582411 | 5256.4 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2217.9 | 0.447 | 0.475 | 0.709 | 1.0000 | 1.0000 | 1.0000 | 360591 | 4480.5 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2157.1 | 0.454 | 0.492 | 0.813 | 0.5556 | 0.6667 | 1.0000 | 353711 | 4509.3 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1578.3 | 0.620 | 0.677 | 1.033 | 1.0000 | 1.0000 | 1.0000 | 469622 | 4666.2 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 942.9 | 1.039 | 1.156 | 1.458 | 0.5556 | 0.8889 | 1.0000 | 680865 | 5148.5 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4501.2 | 0.749 | 1.523 | 2.376 | 1.0000 | 1.0000 | 1.0000 | 557869 | 5373.5 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4616.8 | 0.739 | 1.528 | 2.232 | 0.5556 | 0.8333 | 1.0000 | 582627 | 5259.3 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7713.6 | 0.441 | 0.912 | 1.211 | 1.0000 | 1.0000 | 1.0000 | 361331 | 4484.4 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 7578.8 | 0.449 | 0.933 | 1.158 | 0.5556 | 0.6667 | 1.0000 | 354257 | 4512.4 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 5468.3 | 0.631 | 1.268 | 1.809 | 1.0000 | 1.0000 | 1.0000 | 469112 | 4668.6 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3084.6 | 1.159 | 1.951 | 2.315 | 0.5556 | 0.8889 | 1.0000 | 682487 | 5150.7 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 579.2 | 1.676 | 1.981 | 2.501 | 1.0000 | 1.0000 | 1.0000 | 832880 | 9222.0 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 576.3 | 1.683 | 2.074 | 2.519 | 0.5556 | 0.7222 | 1.0000 | 854568 | 9119.2 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17924.3 | 0.053 | 0.061 | 0.115 | 1.0000 | 1.0000 | 1.0000 | 109020 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17395.4 | 0.054 | 0.067 | 0.093 | 0.5556 | 0.7222 | 1.0000 | 134248 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 585.6 | 1.670 | 1.959 | 2.245 | 1.0000 | 1.0000 | 1.0000 | 797685 | 9164.9 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 590.1 | 1.656 | 1.939 | 2.241 | 0.5556 | 0.6667 | 1.0000 | 790285 | 9012.6 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 19989.0 | 0.048 | 0.053 | 0.068 | 1.0000 | 1.0000 | 1.0000 | 75534 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 24377.8 | 0.039 | 0.046 | 0.067 | 0.5556 | 0.6667 | 1.0000 | 69888 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 590.6 | 1.650 | 1.956 | 2.251 | 1.0000 | 1.0000 | 1.0000 | 760954 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 592.2 | 1.642 | 1.943 | 2.258 | 0.5556 | 0.6667 | 1.0000 | 744003 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 46932.6 | 0.019 | 0.026 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 38786 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 58890.3 | 0.015 | 0.019 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2017.7 | 1.763 | 2.938 | 4.388 | 1.0000 | 1.0000 | 1.0000 | 832994 | 9222.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 1994.7 | 1.771 | 2.977 | 4.160 | 0.5556 | 0.7222 | 1.0000 | 854774 | 9119.4 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 42529.1 | 0.074 | 0.166 | 0.314 | 1.0000 | 1.0000 | 1.0000 | 109164 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 41591.2 | 0.076 | 0.174 | 0.312 | 0.5556 | 0.7222 | 1.0000 | 134385 | 408.1 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2127.0 | 1.715 | 2.594 | 3.165 | 1.0000 | 1.0000 | 1.0000 | 798276 | 9165.6 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2118.1 | 1.710 | 2.710 | 3.232 | 0.5556 | 0.6667 | 1.0000 | 791795 | 9013.3 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 62038.8 | 0.047 | 0.116 | 0.221 | 1.0000 | 1.0000 | 1.0000 | 75674 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 76051.1 | 0.038 | 0.095 | 0.154 | 0.5556 | 0.6667 | 1.0000 | 70046 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2131.7 | 1.694 | 2.643 | 3.332 | 1.0000 | 1.0000 | 1.0000 | 761360 | 8920.1 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2158.7 | 1.680 | 2.617 | 3.379 | 0.5556 | 0.6667 | 1.0000 | 744048 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 127234.1 | 0.019 | 0.056 | 0.103 | 1.0000 | 1.0000 | 1.0000 | 38815 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 177059.8 | 0.015 | 0.031 | 0.051 | 0.5556 | 0.6667 | 1.0000 | 23768 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 408.6 | 2.368 | 2.799 | 3.361 | 1.0000 | 1.0000 | 1.0000 | 1162560 | 12329.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 410.9 | 2.361 | 2.822 | 3.764 | 0.5556 | 0.7222 | 1.0000 | 1184275 | 12221.6 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 501.0 | 1.942 | 2.322 | 2.547 | 1.0000 | 1.0000 | 1.0000 | 938640 | 11429.3 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 488.1 | 1.957 | 2.467 | 3.078 | 0.5556 | 0.6667 | 1.0000 | 930090 | 11458.5 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 431.3 | 2.240 | 2.758 | 3.213 | 1.0000 | 1.0000 | 1.0000 | 1073166 | 11625.2 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 360.8 | 2.688 | 3.232 | 3.458 | 0.5556 | 0.6667 | 1.0000 | 1289456 | 12107.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1469.0 | 2.455 | 3.977 | 5.499 | 1.0000 | 1.0000 | 1.0000 | 1162499 | 12332.4 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1472.1 | 2.440 | 4.058 | 6.065 | 0.5556 | 0.7222 | 1.0000 | 1183405 | 12223.2 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1849.2 | 1.986 | 2.959 | 3.483 | 1.0000 | 1.0000 | 1.0000 | 938639 | 11430.9 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1827.7 | 2.014 | 2.956 | 3.499 | 0.5556 | 0.6667 | 1.0000 | 931034 | 11460.0 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1555.9 | 2.294 | 3.814 | 4.995 | 1.0000 | 1.0000 | 1.0000 | 1071947 | 11625.9 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1270.2 | 2.938 | 4.177 | 4.796 | 0.5556 | 0.6667 | 1.0000 | 1295076 | 12109.8 |

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
