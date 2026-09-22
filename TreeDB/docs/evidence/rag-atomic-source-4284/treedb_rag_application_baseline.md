# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `cc5b6babf0b7227af90ba7aca0add4bc21c54caa`
- harness revision: `37b257565ff8a6f6d43bb8c8397649467204d621`
- binary SHA-256: `1ebd5b67e668017b2a99ab39023671ddbe5f01cff57a272e495255e327418097`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_37b257565 -out-dir /tmp/gomap-4284-final2-artifacts-37b257565 -dir /tmp/gomap-4284-final2-db-37b257565 -product-base-sha cc5b6babf0b7227af90ba7aca0add4bc21c54caa -harness-revision 37b257565ff8a6f6d43bb8c8397649467204d621 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; final exact candidate repeat`

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
| 0 | 18 | 54 | 0.077733 | 231.56 | 694.68 | 2320490 | 4550 | 3743269 | true |
| 1 | 18 | 54 | 0.049112 | 366.51 | 1099.53 | 1830443 | 4425 | 3743269 | true |
| 2 | 18 | 54 | 0.044304 | 406.29 | 1218.86 | 1835442 | 4425 | 3743269 | true |
| 3 | 18 | 54 | 0.045529 | 395.35 | 1186.05 | 1826265 | 4424 | 3743266 | true |
| 4 | 18 | 54 | 0.043969 | 409.38 | 1228.13 | 1832542 | 4424 | 3743269 | true |

Median/p95 docs/s: **395.35 / 408.76**. Median/p95 B/source: **1832542 / 2223480**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **454.65**, B/source <= **1649288**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | projection | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | fetch_topk | 5565.8 | 0.169 | 0.203 | 0.509 | 0.5556 | 0.8333 | 1.0000 | 221989 | 1705.7 |
| hashing_regression | direct_collection | 1 | hybrid | score_only | 18204.5 | 0.051 | 0.065 | 0.126 | 0.5556 | 0.8333 | 1.0000 | 132856 | 403.8 |
| hashing_regression | direct_collection | 1 | text_only | fetch_topk | 6155.5 | 0.156 | 0.180 | 0.279 | 0.5556 | 0.6667 | 1.0000 | 158938 | 1602.9 |
| hashing_regression | direct_collection | 1 | text_only | score_only | 24281.0 | 0.038 | 0.048 | 0.108 | 0.5556 | 0.6667 | 1.0000 | 69885 | 301.5 |
| hashing_regression | direct_collection | 1 | vector_only | fetch_topk | 7331.6 | 0.131 | 0.150 | 0.202 | 0.5556 | 0.8889 | 1.0000 | 111705 | 1400.5 |
| hashing_regression | direct_collection | 1 | vector_only | score_only | 71232.7 | 0.013 | 0.016 | 0.021 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | direct_collection | 4 | hybrid | fetch_topk | 16035.4 | 0.198 | 0.462 | 0.864 | 0.5556 | 0.8333 | 1.0000 | 222168 | 1705.8 |
| hashing_regression | direct_collection | 4 | hybrid | score_only | 37879.9 | 0.074 | 0.222 | 0.537 | 0.5556 | 0.8333 | 1.0000 | 133014 | 403.9 |
| hashing_regression | direct_collection | 4 | text_only | fetch_topk | 19632.3 | 0.165 | 0.367 | 0.666 | 0.5556 | 0.6667 | 1.0000 | 159320 | 1603.4 |
| hashing_regression | direct_collection | 4 | text_only | score_only | 73364.7 | 0.039 | 0.111 | 0.222 | 0.5556 | 0.6667 | 1.0000 | 70054 | 301.5 |
| hashing_regression | direct_collection | 4 | vector_only | fetch_topk | 21720.6 | 0.147 | 0.355 | 0.458 | 0.5556 | 0.8889 | 1.0000 | 111715 | 1400.6 |
| hashing_regression | direct_collection | 4 | vector_only | score_only | 208210.3 | 0.012 | 0.024 | 0.048 | 0.5556 | 0.8889 | 1.0000 | 22486 | 98.1 |
| hashing_regression | http_service | 1 | hybrid | fetch_topk | 1553.9 | 0.620 | 0.746 | 1.213 | 0.5556 | 0.8333 | 1.0000 | 507219 | 4045.9 |
| hashing_regression | http_service | 1 | text_only | fetch_topk | 2590.7 | 0.377 | 0.416 | 0.706 | 0.5556 | 0.6667 | 1.0000 | 276185 | 3298.2 |
| hashing_regression | http_service | 1 | vector_only | fetch_topk | 982.7 | 0.984 | 1.171 | 1.580 | 0.5556 | 0.8889 | 1.0000 | 599877 | 3884.9 |
| hashing_regression | http_service | 4 | hybrid | fetch_topk | 4574.0 | 0.734 | 1.597 | 2.295 | 0.5556 | 0.8333 | 1.0000 | 506825 | 4049.3 |
| hashing_regression | http_service | 4 | text_only | fetch_topk | 8826.6 | 0.389 | 0.765 | 1.111 | 0.5556 | 0.6667 | 1.0000 | 276781 | 3301.8 |
| hashing_regression | http_service | 4 | vector_only | fetch_topk | 3034.0 | 1.148 | 2.082 | 2.536 | 0.5556 | 0.8889 | 1.0000 | 599961 | 3886.6 |
| semantic_minilm | direct_collection | 1 | hybrid | fetch_topk | 571.4 | 1.669 | 2.145 | 2.578 | 0.5556 | 0.7222 | 1.0000 | 824599 | 8669.4 |
| semantic_minilm | direct_collection | 1 | hybrid | score_only | 17317.0 | 0.052 | 0.074 | 0.179 | 0.5556 | 0.7222 | 1.0000 | 134257 | 408.1 |
| semantic_minilm | direct_collection | 1 | text_only | fetch_topk | 581.2 | 1.653 | 2.071 | 2.516 | 0.5556 | 0.6667 | 1.0000 | 760149 | 8562.5 |
| semantic_minilm | direct_collection | 1 | text_only | score_only | 23090.6 | 0.037 | 0.070 | 0.153 | 0.5556 | 0.6667 | 1.0000 | 69910 | 301.4 |
| semantic_minilm | direct_collection | 1 | vector_only | fetch_topk | 568.8 | 1.640 | 2.091 | 3.319 | 0.5556 | 0.6667 | 1.0000 | 715694 | 8359.4 |
| semantic_minilm | direct_collection | 1 | vector_only | score_only | 60745.9 | 0.015 | 0.021 | 0.028 | 0.5556 | 0.6667 | 1.0000 | 23766 | 98.1 |
| semantic_minilm | direct_collection | 4 | hybrid | fetch_topk | 1892.4 | 1.866 | 3.171 | 4.277 | 0.5556 | 0.7222 | 1.0000 | 825032 | 8669.9 |
| semantic_minilm | direct_collection | 4 | hybrid | score_only | 37360.9 | 0.081 | 0.193 | 0.427 | 0.5556 | 0.7222 | 1.0000 | 134416 | 408.2 |
| semantic_minilm | direct_collection | 4 | text_only | fetch_topk | 1994.1 | 1.735 | 3.205 | 4.202 | 0.5556 | 0.6667 | 1.0000 | 761472 | 8563.2 |
| semantic_minilm | direct_collection | 4 | text_only | score_only | 71955.4 | 0.039 | 0.111 | 0.217 | 0.5556 | 0.6667 | 1.0000 | 70054 | 301.5 |
| semantic_minilm | direct_collection | 4 | vector_only | fetch_topk | 1704.3 | 1.826 | 4.249 | 8.133 | 0.5556 | 0.6667 | 1.0000 | 715734 | 8359.9 |
| semantic_minilm | direct_collection | 4 | vector_only | score_only | 162783.0 | 0.015 | 0.044 | 0.067 | 0.5556 | 0.6667 | 1.0000 | 23767 | 98.1 |
| semantic_minilm | http_service | 1 | hybrid | fetch_topk | 414.6 | 2.319 | 2.932 | 3.697 | 0.5556 | 0.7222 | 1.0000 | 1136158 | 11022.4 |
| semantic_minilm | http_service | 1 | text_only | fetch_topk | 495.9 | 1.933 | 2.444 | 2.920 | 0.5556 | 0.6667 | 1.0000 | 879781 | 10258.6 |
| semantic_minilm | http_service | 1 | vector_only | fetch_topk | 332.0 | 2.801 | 3.889 | 6.218 | 0.5556 | 0.6667 | 1.0000 | 1239147 | 10855.7 |
| semantic_minilm | http_service | 4 | hybrid | fetch_topk | 1328.4 | 2.719 | 4.647 | 6.005 | 0.5556 | 0.7222 | 1.0000 | 1134535 | 11024.2 |
| semantic_minilm | http_service | 4 | text_only | fetch_topk | 1741.1 | 2.087 | 3.336 | 4.485 | 0.5556 | 0.6667 | 1.0000 | 879912 | 10259.3 |
| semantic_minilm | http_service | 4 | vector_only | fetch_topk | 1175.5 | 3.084 | 5.016 | 6.184 | 0.5556 | 0.6667 | 1.0000 | 1241948 | 10857.5 |

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

- `hashing_regression`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; IngestSources publishes each source replacement as one dependency-closed durable root selection.
- `semantic_minilm`: re-ingest=true update=`src-billing-alpha-red-new` delete=`src-lifecycle-beta-blue-old` cold reopen=true text/vector/scalar parity=true/true/true; IngestSources publishes each source replacement as one dependency-closed durable root selection.

## Frozen structural/noise policy

- cross-tenant results = 0
- cross-workspace results = 0
- full-document-scan fallbacks = 0
- score-only document fetches = 0
- fetch rows <= TopK documents
- fresh DB; five repetitions; median is decision statistic; p95 disclosed; >10% unaffected QPS or p99 regression blocks; quality/work/projection digests must match
