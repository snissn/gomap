# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `e9911721c2e03ae10ef12d84249de139f8334426`
- harness revision: `b2684a4edc76291e80a4ff2868023d0487a235fd`
- binary SHA-256: `a0542317162f9abd830238366b20ce95f9168b81a7f7531d7d1393a4f311cb14`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_b2684a4ed -out-dir /tmp/gomap-4291-artifacts-b268 -dir /tmp/gomap-4291-db-b268 -product-base-sha e9911721c2e03ae10ef12d84249de139f8334426 -harness-revision b2684a4edc76291e80a4ff2868023d0487a235fd -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 canonical candidate`

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
| 0 | 18 | 54 | 0.067257 | 267.63 | 802.89 | 2317555 | 4556 | 3743378 | true |
| 1 | 18 | 54 | 0.050432 | 356.91 | 1070.74 | 1826798 | 4435 | 3743269 | true |
| 2 | 18 | 54 | 0.037487 | 480.17 | 1440.50 | 1819503 | 4423 | 3743269 | true |
| 3 | 18 | 54 | 0.043370 | 415.04 | 1245.11 | 1826587 | 4423 | 3743269 | true |
| 4 | 18 | 54 | 0.039312 | 457.88 | 1373.63 | 1829829 | 4421 | 3743202 | true |

Median/p95 docs/s: **415.04 / 475.71**. Median/p95 B/source: **1826798 / 2220010**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **477.29**, B/source <= **1644118**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5810.6 | 0.167 | 0.183 | 0.226 | 0.5556 | 0.8333 | 1.0000 | 221977 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5469.6 | 0.173 | 0.195 | 0.440 | 0.5556 | 1.0000 | 1.0000 | 230858 | 1775.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18179.4 | 0.051 | 0.059 | 0.140 | 0.5556 | 0.8333 | 1.0000 | 132888 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 17415.3 | 0.055 | 0.062 | 0.126 | 0.5556 | 1.0000 | 1.0000 | 141928 | 474.5 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6316.5 | 0.154 | 0.170 | 0.221 | 0.5556 | 0.6667 | 1.0000 | 158972 | 1603.0 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 5963.3 | 0.163 | 0.177 | 0.209 | 0.5556 | 0.8333 | 1.0000 | 184728 | 1706.9 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24355.3 | 0.039 | 0.047 | 0.073 | 0.5556 | 0.6667 | 1.0000 | 69930 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 19655.6 | 0.049 | 0.057 | 0.070 | 0.5556 | 0.8333 | 1.0000 | 95962 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7488.3 | 0.131 | 0.140 | 0.177 | 0.5556 | 0.8889 | 1.0000 | 111738 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7092.1 | 0.137 | 0.150 | 0.176 | 0.5556 | 0.9444 | 1.0000 | 137089 | 1504.2 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 70334.0 | 0.013 | 0.015 | 0.018 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 47688.3 | 0.020 | 0.022 | 0.030 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 17360.3 | 0.195 | 0.447 | 0.644 | 0.5556 | 0.8333 | 1.0000 | 222151 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 15891.2 | 0.206 | 0.462 | 0.734 | 0.5556 | 1.0000 | 1.0000 | 231057 | 1776.0 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 43872.4 | 0.071 | 0.178 | 0.344 | 0.5556 | 0.8333 | 1.0000 | 133059 | 403.9 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 39790.7 | 0.077 | 0.201 | 0.331 | 0.5556 | 1.0000 | 1.0000 | 142109 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 20512.3 | 0.164 | 0.360 | 0.527 | 0.5556 | 0.6667 | 1.0000 | 159400 | 1603.4 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 19367.2 | 0.174 | 0.393 | 0.538 | 0.5556 | 0.8333 | 1.0000 | 184978 | 1707.1 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 70693.4 | 0.039 | 0.106 | 0.211 | 0.5556 | 0.6667 | 1.0000 | 70079 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 58877.6 | 0.048 | 0.137 | 0.260 | 0.5556 | 0.8333 | 1.0000 | 96136 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 24460.7 | 0.139 | 0.268 | 0.414 | 0.5556 | 0.8889 | 1.0000 | 111747 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 23160.4 | 0.147 | 0.329 | 0.432 | 0.5556 | 0.9444 | 1.0000 | 137094 | 1504.2 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 192858.8 | 0.012 | 0.034 | 0.053 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 122326.4 | 0.019 | 0.060 | 0.103 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1629.6 | 0.598 | 0.671 | 1.066 | 0.5556 | 0.8333 | 1.0000 | 507667 | 4046.4 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1609.6 | 0.606 | 0.654 | 1.085 | 0.5556 | 1.0000 | 1.0000 | 514351 | 4118.5 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2653.6 | 0.371 | 0.398 | 0.579 | 0.5556 | 0.6667 | 1.0000 | 275771 | 3298.0 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2779.5 | 0.355 | 0.381 | 0.578 | 0.5556 | 0.8333 | 1.0000 | 282135 | 2897.5 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 1024.6 | 0.955 | 1.075 | 1.393 | 0.5556 | 0.8889 | 1.0000 | 598489 | 3884.9 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5450.5 | 0.635 | 1.228 | 1.732 | 0.5556 | 0.8333 | 1.0000 | 506056 | 4048.3 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 5073.6 | 0.660 | 1.469 | 2.223 | 0.5556 | 1.0000 | 1.0000 | 515158 | 4121.2 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9527.3 | 0.360 | 0.714 | 1.004 | 0.5556 | 0.6667 | 1.0000 | 276711 | 3301.4 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 9939.2 | 0.356 | 0.656 | 0.947 | 0.5556 | 0.8333 | 1.0000 | 283419 | 2901.5 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3325.1 | 1.085 | 1.816 | 2.319 | 0.5556 | 0.8889 | 1.0000 | 598006 | 3886.4 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 597.9 | 1.630 | 1.963 | 2.203 | 0.5556 | 0.7222 | 1.0000 | 824609 | 8669.3 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 595.2 | 1.639 | 1.875 | 2.334 | 0.5556 | 0.8333 | 1.0000 | 828588 | 8747.8 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17672.7 | 0.053 | 0.064 | 0.094 | 0.5556 | 0.7222 | 1.0000 | 134283 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17097.4 | 0.056 | 0.065 | 0.135 | 0.5556 | 0.8333 | 1.0000 | 143773 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 599.5 | 1.622 | 1.962 | 2.268 | 0.5556 | 0.6667 | 1.0000 | 760217 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 600.9 | 1.630 | 1.910 | 2.172 | 0.5556 | 0.8333 | 1.0000 | 780817 | 8665.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 25509.2 | 0.037 | 0.045 | 0.053 | 0.5556 | 0.6667 | 1.0000 | 69919 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 19660.1 | 0.049 | 0.058 | 0.074 | 0.5556 | 0.8333 | 1.0000 | 95961 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 613.9 | 1.594 | 1.873 | 2.173 | 0.5556 | 0.6667 | 1.0000 | 715720 | 8359.3 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 611.4 | 1.596 | 1.898 | 2.118 | 0.5556 | 0.8333 | 1.0000 | 741016 | 8463.0 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 59927.2 | 0.015 | 0.019 | 0.024 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 41576.6 | 0.022 | 0.028 | 0.038 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2095.5 | 1.727 | 2.654 | 3.139 | 0.5556 | 0.7222 | 1.0000 | 825084 | 8670.0 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 2080.3 | 1.736 | 2.737 | 3.955 | 0.5556 | 0.8333 | 1.0000 | 828791 | 8748.0 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 40618.0 | 0.075 | 0.185 | 0.329 | 0.5556 | 0.7222 | 1.0000 | 134456 | 408.2 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 40587.2 | 0.078 | 0.183 | 0.359 | 0.5556 | 0.8333 | 1.0000 | 143939 | 487.9 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2212.2 | 1.655 | 2.450 | 3.128 | 0.5556 | 0.6667 | 1.0000 | 761499 | 8563.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 2146.7 | 1.669 | 2.706 | 3.624 | 0.5556 | 0.8333 | 1.0000 | 781452 | 8666.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 79927.0 | 0.038 | 0.077 | 0.147 | 0.5556 | 0.6667 | 1.0000 | 70077 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 49288.0 | 0.052 | 0.144 | 0.226 | 0.5556 | 0.8333 | 1.0000 | 96153 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2078.9 | 1.654 | 2.979 | 4.556 | 0.5556 | 0.6667 | 1.0000 | 715759 | 8359.8 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 2172.3 | 1.646 | 2.685 | 3.653 | 0.5556 | 0.8333 | 1.0000 | 741045 | 8463.4 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 184885.9 | 0.014 | 0.025 | 0.038 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 122997.1 | 0.021 | 0.060 | 0.094 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 437.5 | 2.235 | 2.592 | 3.033 | 0.5556 | 0.7222 | 1.0000 | 1135772 | 11022.7 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 428.7 | 2.269 | 2.684 | 3.469 | 0.5556 | 0.8333 | 1.0000 | 1137969 | 11102.6 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 525.1 | 1.855 | 2.216 | 2.460 | 0.5556 | 0.6667 | 1.0000 | 879133 | 10258.4 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 524.4 | 1.848 | 2.243 | 2.506 | 0.5556 | 0.8333 | 1.0000 | 880826 | 9857.0 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 374.2 | 2.595 | 3.118 | 3.387 | 0.5556 | 0.6667 | 1.0000 | 1236873 | 10855.6 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1567.1 | 2.303 | 3.717 | 4.340 | 0.5556 | 0.7222 | 1.0000 | 1134115 | 11023.8 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1560.6 | 2.297 | 3.798 | 5.148 | 0.5556 | 0.8333 | 1.0000 | 1136891 | 11104.1 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1916.6 | 1.903 | 2.836 | 3.501 | 0.5556 | 0.6667 | 1.0000 | 880275 | 10259.6 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1897.1 | 1.922 | 2.868 | 3.456 | 0.5556 | 0.8333 | 1.0000 | 881098 | 9859.4 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1318.1 | 2.786 | 4.109 | 5.253 | 0.5556 | 0.6667 | 1.0000 | 1237983 | 10857.0 |

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
