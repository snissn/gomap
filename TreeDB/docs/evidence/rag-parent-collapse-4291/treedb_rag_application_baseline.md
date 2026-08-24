# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `a316c6db6428c21f7203998b41ac6c1f11b53e7e`
- binary SHA-256: `056fcb573bbfe435fccd2edc54afecd910b0efb43bc51852abe648fac6751bee`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_a316c6db6 -out-dir /tmp/gomap-4291-artifacts-a316 -dir /tmp/gomap-4291-db-a316 -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision a316c6db6428c21f7203998b41ac6c1f11b53e7e -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 corrected candidate`

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
| 0 | 18 | 54 | 0.063999 | 281.25 | 843.76 | 2327150 | 4558 | 3743358 | true |
| 1 | 18 | 54 | 0.039465 | 456.10 | 1368.31 | 1836571 | 4429 | 3743269 | true |
| 2 | 18 | 54 | 0.042494 | 423.59 | 1270.77 | 1835963 | 4426 | 3743358 | true |
| 3 | 18 | 54 | 0.038442 | 468.23 | 1404.70 | 1825719 | 4420 | 3743358 | true |
| 4 | 18 | 54 | 0.041399 | 434.79 | 1304.37 | 1824353 | 4421 | 3743269 | true |

Median/p95 docs/s: **434.79 / 465.81**. Median/p95 B/source: **1835963 / 2229034**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **500.01**, B/source <= **1652366**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5122.7 | 0.171 | 0.261 | 0.768 | 0.5556 | 0.8333 | 1.0000 | 221898 | 1705.5 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5533.9 | 0.172 | 0.193 | 0.311 | 0.5556 | 1.0000 | 1.0000 | 230840 | 1775.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 16909.7 | 0.055 | 0.067 | 0.137 | 0.5556 | 0.8333 | 1.0000 | 132846 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 16560.0 | 0.057 | 0.070 | 0.135 | 0.5556 | 1.0000 | 1.0000 | 141889 | 474.4 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6376.1 | 0.154 | 0.165 | 0.198 | 0.5556 | 0.6667 | 1.0000 | 158965 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5868.5 | 0.166 | 0.183 | 0.199 | 0.5556 | 0.8333 | 1.0000 | 184734 | 1706.9 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24787.3 | 0.038 | 0.046 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 69930 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 19843.2 | 0.049 | 0.055 | 0.072 | 0.5556 | 0.8333 | 1.0000 | 95962 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7286.9 | 0.131 | 0.149 | 0.287 | 0.5556 | 0.8889 | 1.0000 | 111738 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 6290.0 | 0.140 | 0.277 | 0.461 | 0.5556 | 0.9444 | 1.0000 | 137081 | 1504.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 73454.0 | 0.012 | 0.016 | 0.020 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 50066.8 | 0.019 | 0.022 | 0.033 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15721.3 | 0.210 | 0.437 | 0.866 | 0.5556 | 0.8333 | 1.0000 | 222016 | 1705.6 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 16418.2 | 0.205 | 0.424 | 1.244 | 0.5556 | 1.0000 | 1.0000 | 231021 | 1775.9 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 41199.3 | 0.075 | 0.266 | 0.326 | 0.5556 | 0.8333 | 1.0000 | 132997 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 37003.7 | 0.082 | 0.181 | 0.321 | 0.5556 | 1.0000 | 1.0000 | 142033 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 20161.9 | 0.166 | 0.381 | 0.501 | 0.5556 | 0.6667 | 1.0000 | 159335 | 1603.4 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 18769.5 | 0.173 | 0.439 | 0.654 | 0.5556 | 0.8333 | 1.0000 | 184965 | 1707.1 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 70763.9 | 0.039 | 0.111 | 0.204 | 0.5556 | 0.6667 | 1.0000 | 70079 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 63162.5 | 0.047 | 0.115 | 0.223 | 0.5556 | 0.8333 | 1.0000 | 96142 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 19161.9 | 0.159 | 0.387 | 0.526 | 0.5556 | 0.8889 | 1.0000 | 111752 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 16277.0 | 0.170 | 0.480 | 0.756 | 0.5556 | 0.9444 | 1.0000 | 137088 | 1504.2 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 161646.4 | 0.013 | 0.045 | 0.069 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 106128.8 | 0.021 | 0.064 | 0.107 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1676.4 | 0.576 | 0.655 | 0.812 | 0.5556 | 0.8333 | 1.0000 | 505293 | 4045.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1625.0 | 0.588 | 0.662 | 1.395 | 0.5556 | 1.0000 | 1.0000 | 514806 | 4118.4 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2925.1 | 0.328 | 0.388 | 0.762 | 0.5556 | 0.6667 | 1.0000 | 256856 | 2790.9 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2887.8 | 0.340 | 0.361 | 0.421 | 0.5556 | 0.8333 | 1.0000 | 282308 | 2897.5 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1736.6 | 0.521 | 1.031 | 1.688 | 0.5556 | 0.8889 | 1.0000 | 374467 | 3333.5 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1623.9 | 0.529 | 0.785 | 2.758 | 0.5556 | 0.9444 | 1.0000 | 398349 | 3439.1 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 3815.7 | 0.849 | 2.087 | 4.737 | 0.5556 | 0.8333 | 1.0000 | 505469 | 4048.6 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4320.8 | 0.774 | 1.818 | 3.384 | 0.5556 | 1.0000 | 1.0000 | 514585 | 4121.9 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 10133.9 | 0.341 | 0.628 | 0.962 | 0.5556 | 0.6667 | 1.0000 | 257776 | 2794.9 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 8652.9 | 0.402 | 0.785 | 1.154 | 0.5556 | 0.8333 | 1.0000 | 283678 | 2903.1 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 5153.6 | 0.679 | 1.324 | 1.936 | 0.5556 | 0.8889 | 1.0000 | 373864 | 3335.6 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 5776.3 | 0.559 | 1.388 | 1.892 | 0.5556 | 0.9444 | 1.0000 | 399049 | 3441.5 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 593.8 | 1.635 | 1.908 | 2.312 | 0.5556 | 0.7222 | 1.0000 | 824423 | 8669.0 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 583.0 | 1.655 | 2.037 | 2.542 | 0.5556 | 0.8333 | 1.0000 | 828490 | 8747.6 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17123.6 | 0.054 | 0.065 | 0.103 | 0.5556 | 0.7222 | 1.0000 | 134248 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 16058.4 | 0.058 | 0.069 | 0.087 | 0.5556 | 0.8333 | 1.0000 | 143743 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 602.2 | 1.623 | 1.933 | 2.191 | 0.5556 | 0.6667 | 1.0000 | 760223 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 597.2 | 1.623 | 1.981 | 2.324 | 0.5556 | 0.8333 | 1.0000 | 780837 | 8665.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 25159.3 | 0.038 | 0.045 | 0.062 | 0.5556 | 0.6667 | 1.0000 | 69913 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 19897.4 | 0.048 | 0.053 | 0.072 | 0.5556 | 0.8333 | 1.0000 | 95968 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 610.1 | 1.595 | 1.948 | 2.141 | 0.5556 | 0.6667 | 1.0000 | 715722 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 612.0 | 1.593 | 1.805 | 2.207 | 0.5556 | 0.8333 | 1.0000 | 740974 | 8462.9 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60280.3 | 0.015 | 0.020 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 42345.7 | 0.022 | 0.026 | 0.034 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2080.2 | 1.717 | 2.764 | 4.119 | 0.5556 | 0.7222 | 1.0000 | 824626 | 8669.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2044.2 | 1.736 | 2.782 | 4.356 | 0.5556 | 0.8333 | 1.0000 | 828621 | 8747.7 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 41928.3 | 0.076 | 0.158 | 0.284 | 0.5556 | 0.7222 | 1.0000 | 134422 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 39085.9 | 0.084 | 0.208 | 0.394 | 0.5556 | 0.8333 | 1.0000 | 143893 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2187.2 | 1.663 | 2.544 | 3.448 | 0.5556 | 0.6667 | 1.0000 | 761561 | 8563.1 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2151.0 | 1.672 | 2.612 | 3.380 | 0.5556 | 0.8333 | 1.0000 | 781472 | 8666.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 74725.0 | 0.039 | 0.105 | 0.213 | 0.5556 | 0.6667 | 1.0000 | 70084 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 64311.1 | 0.047 | 0.116 | 0.189 | 0.5556 | 0.8333 | 1.0000 | 96146 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2222.4 | 1.641 | 2.471 | 3.139 | 0.5556 | 0.6667 | 1.0000 | 715746 | 8359.7 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2160.5 | 1.652 | 2.797 | 3.660 | 0.5556 | 0.8333 | 1.0000 | 740987 | 8463.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 167829.0 | 0.015 | 0.038 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 118803.0 | 0.022 | 0.056 | 0.099 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 435.2 | 2.226 | 2.593 | 3.676 | 0.5556 | 0.7222 | 1.0000 | 1132452 | 11020.9 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 432.2 | 2.233 | 2.643 | 3.728 | 0.5556 | 0.8333 | 1.0000 | 1137407 | 11102.0 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 536.9 | 1.818 | 2.157 | 2.403 | 0.5556 | 0.6667 | 1.0000 | 859584 | 9750.9 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 529.6 | 1.832 | 2.228 | 2.497 | 0.5556 | 0.8333 | 1.0000 | 880696 | 9857.1 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 452.1 | 2.139 | 2.632 | 2.993 | 0.5556 | 0.6667 | 1.0000 | 1003444 | 10303.8 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 446.4 | 2.135 | 2.674 | 3.775 | 0.5556 | 0.8333 | 1.0000 | 1027441 | 10410.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1537.7 | 2.323 | 3.807 | 6.371 | 0.5556 | 0.7222 | 1.0000 | 1132198 | 11023.3 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1483.8 | 2.361 | 4.171 | 6.989 | 0.5556 | 0.8333 | 1.0000 | 1136354 | 11103.8 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1909.6 | 1.934 | 2.861 | 3.395 | 0.5556 | 0.6667 | 1.0000 | 860393 | 9752.0 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1949.3 | 1.893 | 2.738 | 3.098 | 0.5556 | 0.8333 | 1.0000 | 880388 | 9858.7 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1622.3 | 2.237 | 3.565 | 4.285 | 0.5556 | 0.6667 | 1.0000 | 1002577 | 10305.4 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1581.7 | 2.283 | 3.718 | 4.754 | 0.5556 | 0.8333 | 1.0000 | 1027539 | 10412.4 |

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
