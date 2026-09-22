# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `d8ad352965493f6a31a1d50ac70f5d783103c454`
- harness revision: `30239761b1e7b90cf66fadf894921757f347c9b1`
- binary SHA-256: `994e7b98e477a3b219dd4226648d7a3af316f364005837653b3a081253b724be`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_30239761b -out-dir /tmp/gomap-rag-m3-abba-a1 -dir /tmp/gomap-rag-m3-abba-a1-db -product-base-sha d8ad352965493f6a31a1d50ac70f5d783103c454 -harness-revision 30239761b1e7b90cf66fadf894921757f347c9b1 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 ABBA A1 control`

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
| 0 | 18 | 54 | 0.065316 | 275.58 | 826.75 | 2341640 | 4809 | 3748833 | true |
| 1 | 18 | 54 | 0.043519 | 413.61 | 1240.83 | 1853247 | 4682 | 3748802 | true |
| 2 | 18 | 54 | 0.041043 | 438.56 | 1315.69 | 1862973 | 4677 | 3748802 | true |
| 3 | 18 | 54 | 0.047467 | 379.21 | 1137.64 | 1849708 | 4677 | 3748827 | true |
| 4 | 18 | 54 | 0.039889 | 451.25 | 1353.75 | 1840392 | 4676 | 3748827 | true |

Median/p95 docs/s: **413.61 / 448.71**. Median/p95 B/source: **1853247 / 2245907**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final, repeated, and comparison-control artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4560.0 | 0.209 | 0.233 | 0.421 | 1.0000 | 1.0000 | 1.0000 | 253312 | 2274.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4523.7 | 0.215 | 0.236 | 0.375 | 0.6667 | 1.0000 | 0.7767 | 259046 | 2323.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4541.5 | 0.208 | 0.252 | 0.534 | 0.5556 | 0.8333 | 1.0000 | 277720 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4526.4 | 0.212 | 0.231 | 0.283 | 0.5556 | 1.0000 | 1.0000 | 286529 | 2236.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18042.7 | 0.052 | 0.060 | 0.088 | 1.0000 | 1.0000 | 1.0000 | 107765 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17415.0 | 0.055 | 0.063 | 0.099 | 0.6667 | 1.0000 | 0.7767 | 113570 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17695.6 | 0.053 | 0.062 | 0.119 | 0.5556 | 0.8333 | 1.0000 | 132851 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16699.2 | 0.056 | 0.063 | 0.205 | 0.5556 | 1.0000 | 1.0000 | 141878 | 474.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4816.0 | 0.202 | 0.217 | 0.337 | 1.0000 | 1.0000 | 1.0000 | 221668 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4761.7 | 0.205 | 0.222 | 0.272 | 0.6667 | 1.0000 | 0.7767 | 221789 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5068.1 | 0.192 | 0.207 | 0.345 | 0.5556 | 0.6667 | 1.0000 | 215709 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4093.5 | 0.204 | 0.231 | 0.615 | 0.5556 | 0.8333 | 1.0000 | 241382 | 2167.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20166.9 | 0.047 | 0.055 | 0.060 | 1.0000 | 1.0000 | 1.0000 | 75546 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19468.2 | 0.050 | 0.055 | 0.062 | 0.6667 | 1.0000 | 0.7767 | 78917 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24608.9 | 0.038 | 0.048 | 0.071 | 0.5556 | 0.6667 | 1.0000 | 69937 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19414.8 | 0.049 | 0.058 | 0.066 | 0.5556 | 0.8333 | 1.0000 | 95969 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5534.7 | 0.175 | 0.189 | 0.314 | 1.0000 | 1.0000 | 1.0000 | 183342 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5410.1 | 0.179 | 0.197 | 0.271 | 0.6667 | 1.0000 | 0.7767 | 187774 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5688.0 | 0.170 | 0.190 | 0.260 | 0.5556 | 0.8889 | 1.0000 | 168593 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5516.7 | 0.177 | 0.189 | 0.273 | 0.5556 | 0.9444 | 1.0000 | 193920 | 1965.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51738.4 | 0.018 | 0.022 | 0.031 | 1.0000 | 1.0000 | 1.0000 | 37538 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 47722.4 | 0.020 | 0.022 | 0.038 | 0.6667 | 1.0000 | 0.7767 | 42247 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 73036.1 | 0.012 | 0.016 | 0.022 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 47452.9 | 0.019 | 0.024 | 0.034 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 14022.4 | 0.247 | 0.449 | 0.852 | 1.0000 | 1.0000 | 1.0000 | 253460 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 12153.9 | 0.256 | 0.544 | 1.028 | 0.6667 | 1.0000 | 0.7767 | 259157 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 10787.7 | 0.262 | 0.824 | 2.802 | 0.5556 | 0.8333 | 1.0000 | 277849 | 2166.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 12500.9 | 0.257 | 0.575 | 1.004 | 0.5556 | 1.0000 | 1.0000 | 286700 | 2236.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 44764.8 | 0.071 | 0.178 | 0.263 | 1.0000 | 1.0000 | 1.0000 | 107902 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45620.4 | 0.070 | 0.157 | 0.283 | 0.6667 | 1.0000 | 0.7767 | 113709 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43837.3 | 0.074 | 0.171 | 0.359 | 0.5556 | 0.8333 | 1.0000 | 132978 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39274.0 | 0.082 | 0.185 | 0.376 | 0.5556 | 1.0000 | 1.0000 | 142076 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15724.3 | 0.214 | 0.481 | 0.663 | 1.0000 | 1.0000 | 1.0000 | 221889 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15138.3 | 0.217 | 0.500 | 0.748 | 0.6667 | 1.0000 | 0.7767 | 222053 | 2252.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16172.9 | 0.205 | 0.478 | 0.724 | 0.5556 | 0.6667 | 1.0000 | 216303 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15868.4 | 0.213 | 0.494 | 0.739 | 0.5556 | 0.8333 | 1.0000 | 241709 | 2168.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 62486.8 | 0.047 | 0.118 | 0.196 | 1.0000 | 1.0000 | 1.0000 | 75711 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60292.6 | 0.049 | 0.113 | 0.241 | 0.6667 | 1.0000 | 0.7767 | 79097 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 64209.1 | 0.040 | 0.116 | 0.253 | 0.5556 | 0.6667 | 1.0000 | 70093 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 58517.7 | 0.048 | 0.136 | 0.287 | 0.5556 | 0.8333 | 1.0000 | 96149 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18086.6 | 0.185 | 0.422 | 0.603 | 1.0000 | 1.0000 | 1.0000 | 183519 | 1972.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17662.2 | 0.189 | 0.402 | 0.581 | 0.6667 | 1.0000 | 0.7767 | 187829 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 15733.1 | 0.192 | 0.491 | 0.692 | 0.5556 | 0.8889 | 1.0000 | 168607 | 1862.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18601.7 | 0.185 | 0.382 | 0.576 | 0.5556 | 0.9444 | 1.0000 | 193935 | 1965.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 109269.6 | 0.019 | 0.073 | 0.117 | 1.0000 | 1.0000 | 1.0000 | 37565 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 117571.0 | 0.022 | 0.057 | 0.111 | 0.6667 | 1.0000 | 0.7767 | 42268 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 157361.8 | 0.013 | 0.044 | 0.068 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 123225.2 | 0.019 | 0.052 | 0.102 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1395.4 | 0.691 | 0.758 | 1.332 | 1.0000 | 1.0000 | 1.0000 | 558685 | 5370.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1393.4 | 0.692 | 0.755 | 0.969 | 0.6667 | 1.0000 | 0.7767 | 563327 | 5425.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1406.6 | 0.692 | 0.758 | 1.208 | 0.5556 | 0.8333 | 1.0000 | 582234 | 5256.3 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1375.5 | 0.700 | 0.777 | 1.313 | 0.5556 | 1.0000 | 1.0000 | 590529 | 5329.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2309.3 | 0.424 | 0.459 | 0.693 | 1.0000 | 1.0000 | 1.0000 | 343242 | 4164.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2289.5 | 0.428 | 0.469 | 0.638 | 0.6667 | 1.0000 | 0.7767 | 343290 | 4202.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2431.7 | 0.405 | 0.445 | 0.615 | 0.5556 | 0.6667 | 1.0000 | 336393 | 4003.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2336.6 | 0.419 | 0.458 | 0.706 | 0.5556 | 0.8333 | 1.0000 | 362156 | 4109.8 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1568.2 | 0.624 | 0.679 | 1.069 | 1.0000 | 1.0000 | 1.0000 | 469662 | 4666.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 948.3 | 1.036 | 1.151 | 1.501 | 0.5556 | 0.8889 | 1.0000 | 680744 | 5148.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4414.8 | 0.761 | 1.504 | 2.738 | 1.0000 | 1.0000 | 1.0000 | 558034 | 5373.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4179.2 | 0.769 | 1.909 | 3.538 | 0.6667 | 1.0000 | 0.7767 | 563047 | 5428.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4363.9 | 0.777 | 1.667 | 2.549 | 0.5556 | 0.8333 | 1.0000 | 582429 | 5259.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4516.2 | 0.742 | 1.445 | 2.839 | 0.5556 | 1.0000 | 1.0000 | 590379 | 5331.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7700.1 | 0.448 | 0.894 | 1.243 | 1.0000 | 1.0000 | 1.0000 | 343875 | 4167.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7620.0 | 0.453 | 0.904 | 1.320 | 0.6667 | 1.0000 | 0.7767 | 344238 | 4206.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7507.8 | 0.457 | 0.947 | 1.418 | 0.5556 | 0.6667 | 1.0000 | 337119 | 4007.2 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7449.1 | 0.455 | 0.951 | 1.283 | 0.5556 | 0.8333 | 1.0000 | 363189 | 4114.2 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5209.7 | 0.649 | 1.294 | 2.169 | 1.0000 | 1.0000 | 1.0000 | 469201 | 4669.0 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2855.2 | 1.220 | 2.301 | 2.769 | 0.5556 | 0.8889 | 1.0000 | 683290 | 5150.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 569.4 | 1.699 | 2.048 | 2.503 | 1.0000 | 1.0000 | 1.0000 | 832860 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 565.0 | 1.704 | 2.043 | 2.481 | 0.6667 | 1.0000 | 0.7767 | 829680 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 572.5 | 1.696 | 2.055 | 2.514 | 0.5556 | 0.7222 | 1.0000 | 854592 | 9119.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 558.4 | 1.713 | 2.192 | 2.834 | 0.5556 | 0.8333 | 1.0000 | 858658 | 9197.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 17758.7 | 0.053 | 0.062 | 0.112 | 1.0000 | 1.0000 | 1.0000 | 109038 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 16814.5 | 0.056 | 0.067 | 0.130 | 0.6667 | 1.0000 | 0.7767 | 114837 | 560.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17438.1 | 0.054 | 0.064 | 0.137 | 0.5556 | 0.7222 | 1.0000 | 134238 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16648.8 | 0.057 | 0.067 | 0.145 | 0.5556 | 0.8333 | 1.0000 | 143743 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 557.9 | 1.718 | 2.193 | 3.016 | 1.0000 | 1.0000 | 1.0000 | 797760 | 9164.9 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 566.8 | 1.717 | 2.063 | 2.345 | 0.6667 | 1.0000 | 0.7767 | 792166 | 9202.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 583.2 | 1.670 | 2.004 | 2.267 | 0.5556 | 0.6667 | 1.0000 | 790285 | 9012.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 580.4 | 1.677 | 2.039 | 2.289 | 0.5556 | 0.8333 | 1.0000 | 809193 | 9115.3 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19547.9 | 0.049 | 0.056 | 0.082 | 1.0000 | 1.0000 | 1.0000 | 75546 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 18877.5 | 0.051 | 0.060 | 0.073 | 0.6667 | 1.0000 | 0.7767 | 78931 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 25515.0 | 0.037 | 0.045 | 0.065 | 0.5556 | 0.6667 | 1.0000 | 69919 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19667.3 | 0.049 | 0.055 | 0.090 | 0.5556 | 0.8333 | 1.0000 | 95968 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 586.7 | 1.659 | 2.021 | 2.353 | 1.0000 | 1.0000 | 1.0000 | 760991 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 580.2 | 1.663 | 2.071 | 2.660 | 0.6667 | 1.0000 | 0.7767 | 762039 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 588.1 | 1.658 | 1.997 | 2.253 | 0.5556 | 0.6667 | 1.0000 | 744027 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 588.8 | 1.660 | 1.945 | 2.222 | 0.5556 | 0.8333 | 1.0000 | 769331 | 8912.8 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47130.0 | 0.019 | 0.026 | 0.031 | 1.0000 | 1.0000 | 1.0000 | 38797 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 41026.7 | 0.022 | 0.030 | 0.038 | 0.6667 | 1.0000 | 0.7767 | 43544 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 59963.5 | 0.015 | 0.020 | 0.026 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41099.7 | 0.022 | 0.032 | 0.051 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1986.7 | 1.779 | 2.837 | 4.605 | 1.0000 | 1.0000 | 1.0000 | 833033 | 9222.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1943.6 | 1.786 | 3.121 | 5.296 | 0.6667 | 1.0000 | 0.7767 | 829751 | 9270.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2014.5 | 1.764 | 2.911 | 4.542 | 0.5556 | 0.7222 | 1.0000 | 854723 | 9119.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1872.1 | 1.845 | 3.596 | 4.726 | 0.5556 | 0.8333 | 1.0000 | 858790 | 9197.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 44110.3 | 0.072 | 0.162 | 0.289 | 1.0000 | 1.0000 | 1.0000 | 109195 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 40971.8 | 0.076 | 0.172 | 0.362 | 0.6667 | 1.0000 | 0.7767 | 114974 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 42925.2 | 0.077 | 0.161 | 0.350 | 0.5556 | 0.7222 | 1.0000 | 134416 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38296.5 | 0.080 | 0.180 | 0.396 | 0.5556 | 0.8333 | 1.0000 | 143920 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2091.5 | 1.728 | 2.695 | 3.314 | 1.0000 | 1.0000 | 1.0000 | 798093 | 9165.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1991.4 | 1.765 | 2.995 | 3.911 | 0.6667 | 1.0000 | 0.7767 | 792597 | 9202.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2107.9 | 1.706 | 2.783 | 3.318 | 0.5556 | 0.6667 | 1.0000 | 791659 | 9013.3 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2084.9 | 1.719 | 2.683 | 3.454 | 0.5556 | 0.8333 | 1.0000 | 809660 | 9115.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 64638.0 | 0.049 | 0.092 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 75702 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 55510.4 | 0.053 | 0.144 | 0.235 | 0.6667 | 1.0000 | 0.7767 | 79068 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 77157.8 | 0.039 | 0.094 | 0.153 | 0.5556 | 0.6667 | 1.0000 | 70070 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 56554.7 | 0.048 | 0.133 | 0.237 | 0.5556 | 0.8333 | 1.0000 | 96120 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2134.7 | 1.692 | 2.645 | 3.473 | 1.0000 | 1.0000 | 1.0000 | 761238 | 8919.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2092.3 | 1.694 | 2.984 | 4.177 | 0.6667 | 1.0000 | 0.7767 | 762245 | 8969.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2174.8 | 1.678 | 2.651 | 3.312 | 0.5556 | 0.6667 | 1.0000 | 744060 | 8809.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2108.9 | 1.694 | 2.824 | 3.567 | 0.5556 | 0.8333 | 1.0000 | 769383 | 8913.3 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 110888.3 | 0.022 | 0.068 | 0.114 | 1.0000 | 1.0000 | 1.0000 | 38831 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 107127.7 | 0.022 | 0.071 | 0.129 | 0.6667 | 1.0000 | 0.7767 | 43579 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 170731.9 | 0.015 | 0.033 | 0.057 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 109376.4 | 0.022 | 0.069 | 0.132 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 401.5 | 2.412 | 2.876 | 3.451 | 1.0000 | 1.0000 | 1.0000 | 1162399 | 12329.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 401.5 | 2.403 | 2.842 | 3.978 | 0.6667 | 1.0000 | 0.7767 | 1159275 | 12384.3 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 410.0 | 2.364 | 2.829 | 3.650 | 0.5556 | 0.7222 | 1.0000 | 1184151 | 12221.4 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 403.2 | 2.381 | 2.962 | 4.083 | 0.5556 | 0.8333 | 1.0000 | 1188116 | 12302.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 496.8 | 1.950 | 2.390 | 2.646 | 1.0000 | 1.0000 | 1.0000 | 920305 | 11113.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 490.9 | 1.970 | 2.441 | 2.654 | 0.6667 | 1.0000 | 0.7767 | 915069 | 11153.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 497.7 | 1.938 | 2.408 | 2.730 | 0.5556 | 0.6667 | 1.0000 | 912928 | 10952.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 501.8 | 1.936 | 2.364 | 2.616 | 0.5556 | 0.8333 | 1.0000 | 931439 | 11057.7 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 422.7 | 2.285 | 2.807 | 3.129 | 1.0000 | 1.0000 | 1.0000 | 1071839 | 11624.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 348.4 | 2.794 | 3.383 | 3.655 | 0.5556 | 0.6667 | 1.0000 | 1289400 | 12107.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1339.3 | 2.502 | 5.622 | 8.780 | 1.0000 | 1.0000 | 1.0000 | 1162315 | 12331.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1400.1 | 2.532 | 4.349 | 9.545 | 0.6667 | 1.0000 | 0.7767 | 1158893 | 12387.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1406.7 | 2.563 | 4.175 | 6.487 | 0.5556 | 0.7222 | 1.0000 | 1183343 | 12223.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1434.2 | 2.494 | 4.122 | 6.619 | 0.5556 | 0.8333 | 1.0000 | 1187874 | 12304.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1803.1 | 2.016 | 3.132 | 3.634 | 1.0000 | 1.0000 | 1.0000 | 921800 | 11115.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1809.3 | 2.027 | 3.080 | 3.634 | 0.6667 | 1.0000 | 0.7767 | 915355 | 11156.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1847.9 | 1.997 | 2.946 | 3.333 | 0.5556 | 0.6667 | 1.0000 | 912793 | 10952.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1824.8 | 1.988 | 3.066 | 3.861 | 0.5556 | 0.8333 | 1.0000 | 932525 | 11059.9 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1542.2 | 2.357 | 3.802 | 4.720 | 1.0000 | 1.0000 | 1.0000 | 1071955 | 11626.5 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1214.2 | 3.086 | 4.549 | 5.122 | 0.5556 | 0.6667 | 1.0000 | 1295217 | 12110.1 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 40 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable`: 136 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+http_score_only_route_unavailable`: 40 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+http_vector_parent_collapse_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.
- `multi_field_filter_unavailable+http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 8 rows; `*main.capabilityError`; zero results; fail closed.

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
