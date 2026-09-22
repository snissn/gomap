# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `3b3235ea1e83eb75d589b5379b05888b739b6b08`
- harness revision: `8fc41fac0b8cbebc213b6c1ff0759cac85be99cf`
- binary SHA-256: `c03900a3b2b8d452a6e798320b9b27445bd3758992c02437826db83d768e367e`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_8fc41fac0 -out-dir /tmp/gomap-rag-m3-abba-b1 -dir /tmp/gomap-rag-m3-abba-b1-db -product-base-sha 3b3235ea1e83eb75d589b5379b05888b739b6b08 -harness-revision 8fc41fac0b8cbebc213b6c1ff0759cac85be99cf -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 ABBA B1 candidate`

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
| 0 | 18 | 54 | 0.068139 | 264.16 | 792.49 | 2348612 | 4805 | 3748799 | true |
| 1 | 18 | 54 | 0.050688 | 355.12 | 1065.35 | 1855105 | 4680 | 3748822 | true |
| 2 | 18 | 54 | 0.044464 | 404.82 | 1214.45 | 1835512 | 4676 | 3748827 | true |
| 3 | 18 | 54 | 0.046394 | 387.98 | 1163.94 | 1849600 | 4678 | 3748827 | true |
| 4 | 18 | 54 | 0.044471 | 404.76 | 1214.27 | 1850772 | 4680 | 3748827 | true |

Median/p95 docs/s: **387.98 / 404.81**. Median/p95 B/source: **1850772 / 2249910**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final and repeated artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4566.9 | 0.209 | 0.229 | 0.386 | 1.0000 | 1.0000 | 1.0000 | 253408 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4438.7 | 0.216 | 0.237 | 0.409 | 0.6667 | 1.0000 | 0.7767 | 259140 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4314.3 | 0.213 | 0.254 | 0.722 | 1.0000 | 1.0000 | 1.0000 | 426248 | 2290.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4375.1 | 0.216 | 0.256 | 0.397 | 0.6667 | 1.0000 | 0.7751 | 429391 | 2323.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4541.5 | 0.203 | 0.263 | 0.790 | 1.0000 | 1.0000 | 1.0000 | 497325 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5908.2 | 0.156 | 0.242 | 0.327 | 0.6667 | 1.0000 | 0.7654 | 455794 | 1634.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4585.6 | 0.210 | 0.230 | 0.363 | 0.5556 | 0.8333 | 1.0000 | 277776 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4459.9 | 0.213 | 0.261 | 0.458 | 0.5556 | 1.0000 | 1.0000 | 286652 | 2237.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 19008.2 | 0.051 | 0.058 | 0.071 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17125.5 | 0.055 | 0.070 | 0.138 | 0.6667 | 1.0000 | 0.7767 | 113639 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 15835.7 | 0.053 | 0.100 | 0.245 | 1.0000 | 1.0000 | 1.0000 | 284462 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15274.8 | 0.056 | 0.079 | 0.286 | 0.6667 | 1.0000 | 0.7751 | 287555 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15792.1 | 0.053 | 0.071 | 0.217 | 1.0000 | 1.0000 | 1.0000 | 369675 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15085.4 | 0.053 | 0.206 | 0.272 | 0.6667 | 1.0000 | 0.7654 | 369617 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 16837.6 | 0.054 | 0.066 | 0.161 | 0.5556 | 0.8333 | 1.0000 | 132980 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16161.4 | 0.057 | 0.068 | 0.272 | 0.5556 | 1.0000 | 1.0000 | 141984 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4793.2 | 0.202 | 0.224 | 0.316 | 1.0000 | 1.0000 | 1.0000 | 221761 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4781.0 | 0.203 | 0.222 | 0.315 | 0.6667 | 1.0000 | 0.7767 | 221900 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4789.5 | 0.202 | 0.220 | 0.445 | 1.0000 | 1.0000 | 1.0000 | 401387 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5253.7 | 0.175 | 0.214 | 0.417 | 0.6667 | 1.0000 | 0.7751 | 385279 | 2032.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6119.6 | 0.144 | 0.197 | 0.358 | 1.0000 | 1.0000 | 1.0000 | 449022 | 1758.7 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7799.1 | 0.112 | 0.151 | 0.413 | 0.6667 | 1.0000 | 0.7654 | 416364 | 1357.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5053.2 | 0.192 | 0.209 | 0.239 | 0.5556 | 0.6667 | 1.0000 | 215824 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4757.8 | 0.202 | 0.226 | 0.376 | 0.5556 | 0.8333 | 1.0000 | 241481 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19951.7 | 0.049 | 0.055 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 75642 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19250.1 | 0.051 | 0.057 | 0.070 | 0.6667 | 1.0000 | 0.7767 | 79027 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19478.0 | 0.048 | 0.052 | 0.159 | 1.0000 | 1.0000 | 1.0000 | 258523 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19072.9 | 0.049 | 0.054 | 0.130 | 0.6667 | 1.0000 | 0.7751 | 259792 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19667.0 | 0.047 | 0.055 | 0.134 | 1.0000 | 1.0000 | 1.0000 | 350675 | 518.2 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19272.5 | 0.047 | 0.056 | 0.179 | 0.6667 | 1.0000 | 0.7654 | 350612 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24441.3 | 0.039 | 0.046 | 0.052 | 0.5556 | 0.6667 | 1.0000 | 70051 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19767.8 | 0.049 | 0.055 | 0.074 | 0.5556 | 0.8333 | 1.0000 | 96090 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5554.0 | 0.175 | 0.192 | 0.214 | 1.0000 | 1.0000 | 1.0000 | 183441 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4802.3 | 0.180 | 0.321 | 0.526 | 0.6667 | 1.0000 | 0.7767 | 187861 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5278.6 | 0.180 | 0.200 | 0.315 | 1.0000 | 1.0000 | 1.0000 | 370172 | 2020.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4760.1 | 0.185 | 0.304 | 0.610 | 0.6667 | 1.0000 | 0.7751 | 372889 | 2053.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5470.3 | 0.170 | 0.200 | 0.419 | 1.0000 | 1.0000 | 1.0000 | 450779 | 1908.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7113.6 | 0.126 | 0.183 | 0.321 | 0.6667 | 1.0000 | 0.7654 | 409637 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5746.9 | 0.169 | 0.184 | 0.246 | 0.5556 | 0.8889 | 1.0000 | 168719 | 1862.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5441.4 | 0.178 | 0.197 | 0.267 | 0.5556 | 0.9444 | 1.0000 | 194043 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 49750.7 | 0.018 | 0.023 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 37614 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45850.3 | 0.020 | 0.026 | 0.033 | 0.6667 | 1.0000 | 0.7767 | 42330 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 37670.9 | 0.022 | 0.033 | 0.093 | 1.0000 | 1.0000 | 1.0000 | 228140 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 36721.5 | 0.024 | 0.031 | 0.083 | 0.6667 | 1.0000 | 0.7751 | 230781 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32250.5 | 0.026 | 0.041 | 0.119 | 1.0000 | 1.0000 | 1.0000 | 324133 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31287.3 | 0.027 | 0.041 | 0.147 | 0.6667 | 1.0000 | 0.7654 | 324077 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 70987.7 | 0.012 | 0.017 | 0.021 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46505.5 | 0.020 | 0.025 | 0.030 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 12998.7 | 0.250 | 0.546 | 0.931 | 1.0000 | 1.0000 | 1.0000 | 253531 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13379.9 | 0.246 | 0.479 | 0.979 | 0.6667 | 1.0000 | 0.7767 | 259271 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 11039.3 | 0.284 | 0.690 | 2.284 | 1.0000 | 1.0000 | 1.0000 | 426354 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 11616.4 | 0.265 | 0.555 | 1.685 | 0.6667 | 1.0000 | 0.7751 | 429505 | 2323.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12271.5 | 0.268 | 0.490 | 0.996 | 1.0000 | 1.0000 | 1.0000 | 497436 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14560.5 | 0.219 | 0.433 | 1.026 | 0.6667 | 1.0000 | 0.7654 | 455941 | 1634.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 14081.5 | 0.247 | 0.436 | 0.860 | 0.5556 | 0.8333 | 1.0000 | 277917 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13598.0 | 0.250 | 0.481 | 0.953 | 0.5556 | 1.0000 | 1.0000 | 286808 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 48565.7 | 0.070 | 0.139 | 0.183 | 1.0000 | 1.0000 | 1.0000 | 107988 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 47690.8 | 0.071 | 0.142 | 0.177 | 0.6667 | 1.0000 | 0.7767 | 113775 | 561.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 27808.1 | 0.109 | 0.249 | 0.699 | 1.0000 | 1.0000 | 1.0000 | 284565 | 533.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 27781.9 | 0.104 | 0.260 | 0.679 | 0.6667 | 1.0000 | 0.7751 | 287679 | 566.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 22931.0 | 0.132 | 0.275 | 1.020 | 1.0000 | 1.0000 | 1.0000 | 369789 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 23535.7 | 0.134 | 0.241 | 0.530 | 0.6667 | 1.0000 | 0.7654 | 369718 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 38536.6 | 0.080 | 0.164 | 0.351 | 0.5556 | 0.8333 | 1.0000 | 133098 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38421.6 | 0.082 | 0.276 | 0.380 | 0.5556 | 1.0000 | 1.0000 | 142147 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15838.8 | 0.212 | 0.475 | 0.680 | 1.0000 | 1.0000 | 1.0000 | 222036 | 2216.8 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 14927.5 | 0.217 | 0.518 | 0.754 | 0.6667 | 1.0000 | 0.7767 | 222087 | 2252.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14637.6 | 0.220 | 0.524 | 0.786 | 1.0000 | 1.0000 | 1.0000 | 401694 | 2244.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 16011.6 | 0.206 | 0.498 | 0.704 | 0.6667 | 1.0000 | 0.7751 | 385576 | 2032.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 17230.3 | 0.190 | 0.483 | 0.641 | 1.0000 | 1.0000 | 1.0000 | 449377 | 1759.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20214.8 | 0.159 | 0.423 | 0.631 | 0.6667 | 1.0000 | 0.7654 | 416660 | 1357.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16515.1 | 0.204 | 0.462 | 0.583 | 0.5556 | 0.6667 | 1.0000 | 216414 | 2065.7 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15975.6 | 0.212 | 0.492 | 0.750 | 0.5556 | 0.8333 | 1.0000 | 241763 | 2169.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 58111.4 | 0.047 | 0.139 | 0.223 | 1.0000 | 1.0000 | 1.0000 | 75800 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 57999.0 | 0.049 | 0.126 | 0.254 | 0.6667 | 1.0000 | 0.7767 | 79165 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 44380.9 | 0.073 | 0.193 | 0.350 | 1.0000 | 1.0000 | 1.0000 | 258766 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40063.3 | 0.075 | 0.218 | 0.515 | 0.6667 | 1.0000 | 0.7751 | 260004 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 29724.9 | 0.109 | 0.334 | 0.491 | 1.0000 | 1.0000 | 1.0000 | 350894 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 32815.7 | 0.108 | 0.249 | 0.355 | 0.6667 | 1.0000 | 0.7654 | 350858 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 62810.6 | 0.041 | 0.119 | 0.236 | 0.5556 | 0.6667 | 1.0000 | 70222 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 58997.6 | 0.048 | 0.127 | 0.315 | 0.5556 | 0.8333 | 1.0000 | 96291 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 15693.2 | 0.190 | 0.576 | 0.868 | 1.0000 | 1.0000 | 1.0000 | 183554 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 16036.7 | 0.192 | 0.453 | 0.706 | 0.6667 | 1.0000 | 0.7767 | 187940 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 14331.3 | 0.208 | 0.576 | 1.022 | 1.0000 | 1.0000 | 1.0000 | 370242 | 2020.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 14598.6 | 0.209 | 0.481 | 1.274 | 0.6667 | 1.0000 | 0.7751 | 372958 | 2053.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 13942.8 | 0.207 | 0.473 | 1.900 | 1.0000 | 1.0000 | 1.0000 | 450879 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 16689.1 | 0.170 | 0.372 | 2.247 | 0.6667 | 1.0000 | 0.7654 | 409675 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 19113.4 | 0.179 | 0.370 | 0.581 | 0.5556 | 0.8889 | 1.0000 | 168730 | 1863.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18228.5 | 0.186 | 0.384 | 0.597 | 0.5556 | 0.9444 | 1.0000 | 194058 | 1966.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 134022.5 | 0.019 | 0.051 | 0.102 | 1.0000 | 1.0000 | 1.0000 | 37675 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 120087.2 | 0.020 | 0.058 | 0.105 | 0.6667 | 1.0000 | 0.7767 | 42404 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 44046.5 | 0.039 | 0.398 | 0.912 | 1.0000 | 1.0000 | 1.0000 | 228242 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42067.4 | 0.040 | 0.388 | 1.065 | 0.6667 | 1.0000 | 0.7751 | 230936 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32253.4 | 0.111 | 0.215 | 0.462 | 1.0000 | 1.0000 | 1.0000 | 324235 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30960.7 | 0.114 | 0.241 | 0.468 | 0.6667 | 1.0000 | 0.7654 | 324218 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 208261.1 | 0.013 | 0.024 | 0.043 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 122720.6 | 0.019 | 0.043 | 0.082 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1391.3 | 0.700 | 0.765 | 0.941 | 1.0000 | 1.0000 | 1.0000 | 557497 | 5372.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1381.4 | 0.704 | 0.796 | 1.254 | 0.6667 | 1.0000 | 0.7767 | 563377 | 5427.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1367.3 | 0.702 | 0.783 | 1.313 | 1.0000 | 1.0000 | 1.0000 | 732167 | 5410.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1311.8 | 0.692 | 0.869 | 1.989 | 0.6667 | 1.0000 | 0.7751 | 734000 | 5394.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1427.9 | 0.664 | 0.818 | 1.432 | 1.0000 | 1.0000 | 1.0000 | 791123 | 4981.2 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1583.0 | 0.581 | 0.796 | 1.707 | 0.6667 | 1.0000 | 0.7654 | 715079 | 3844.3 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1406.7 | 0.694 | 0.756 | 1.223 | 0.5556 | 0.8333 | 1.0000 | 582577 | 5256.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1392.2 | 0.697 | 0.770 | 1.225 | 0.5556 | 1.0000 | 1.0000 | 590437 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2213.9 | 0.448 | 0.471 | 0.710 | 1.0000 | 1.0000 | 1.0000 | 344001 | 4166.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2191.5 | 0.450 | 0.480 | 0.676 | 0.6667 | 1.0000 | 0.7767 | 345583 | 4205.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2186.8 | 0.450 | 0.481 | 0.812 | 1.0000 | 1.0000 | 1.0000 | 526900 | 4216.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2347.8 | 0.407 | 0.482 | 0.831 | 0.6667 | 1.0000 | 0.7751 | 496212 | 3769.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2646.7 | 0.347 | 0.440 | 0.827 | 1.0000 | 1.0000 | 1.0000 | 544341 | 3211.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3284.7 | 0.287 | 0.354 | 0.725 | 0.6667 | 1.0000 | 0.7654 | 491509 | 2397.3 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2296.3 | 0.430 | 0.460 | 0.721 | 0.5556 | 0.6667 | 1.0000 | 336442 | 4003.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2215.5 | 0.445 | 0.477 | 0.756 | 0.5556 | 0.8333 | 1.0000 | 362291 | 4110.8 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1305.8 | 0.643 | 1.479 | 2.490 | 1.0000 | 1.0000 | 1.0000 | 470323 | 4668.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1468.2 | 0.640 | 0.791 | 1.331 | 1.0000 | 1.0000 | 1.0000 | 658353 | 4738.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1553.5 | 0.620 | 0.682 | 1.060 | 1.0000 | 1.0000 | 1.0000 | 726376 | 4457.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 936.0 | 1.045 | 1.143 | 1.524 | 0.5556 | 0.8889 | 1.0000 | 680731 | 5148.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4320.3 | 0.760 | 1.686 | 2.934 | 1.0000 | 1.0000 | 1.0000 | 558024 | 5375.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4252.4 | 0.761 | 1.727 | 3.559 | 0.6667 | 1.0000 | 0.7767 | 565819 | 5430.9 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 3996.9 | 0.814 | 2.073 | 2.586 | 1.0000 | 1.0000 | 1.0000 | 732286 | 5414.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4046.7 | 0.804 | 2.271 | 2.929 | 0.6667 | 1.0000 | 0.7751 | 734612 | 5398.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4337.4 | 0.733 | 1.977 | 2.769 | 1.0000 | 1.0000 | 1.0000 | 790808 | 4983.7 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5078.7 | 0.674 | 1.618 | 2.310 | 0.6667 | 1.0000 | 0.7654 | 715383 | 3847.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4461.5 | 0.758 | 1.608 | 2.317 | 0.5556 | 0.8333 | 1.0000 | 582122 | 5259.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4466.1 | 0.762 | 1.516 | 2.472 | 0.5556 | 1.0000 | 1.0000 | 590756 | 5332.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7821.8 | 0.443 | 0.894 | 1.190 | 1.0000 | 1.0000 | 1.0000 | 344757 | 4170.2 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7724.4 | 0.446 | 0.886 | 1.246 | 0.6667 | 1.0000 | 0.7767 | 346253 | 4208.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7405.7 | 0.452 | 1.021 | 1.373 | 1.0000 | 1.0000 | 1.0000 | 527500 | 4220.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8306.2 | 0.419 | 0.847 | 1.175 | 0.6667 | 1.0000 | 0.7751 | 497301 | 3772.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 8877.0 | 0.391 | 0.822 | 1.180 | 1.0000 | 1.0000 | 1.0000 | 545727 | 3216.2 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 10765.1 | 0.316 | 0.710 | 1.043 | 0.6667 | 1.0000 | 0.7654 | 492560 | 2403.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 8136.4 | 0.423 | 0.847 | 1.177 | 0.5556 | 0.6667 | 1.0000 | 337730 | 4007.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7790.8 | 0.441 | 0.921 | 1.246 | 0.5556 | 0.8333 | 1.0000 | 363567 | 4114.0 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4959.1 | 0.675 | 1.416 | 2.381 | 1.0000 | 1.0000 | 1.0000 | 470347 | 4670.9 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4759.5 | 0.691 | 1.553 | 2.811 | 1.0000 | 1.0000 | 1.0000 | 658916 | 4742.3 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4465.1 | 0.740 | 1.618 | 2.500 | 1.0000 | 1.0000 | 1.0000 | 726874 | 4461.5 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3076.7 | 1.152 | 1.940 | 2.515 | 0.5556 | 0.8889 | 1.0000 | 681058 | 5150.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 564.2 | 1.700 | 2.143 | 2.505 | 1.0000 | 1.0000 | 1.0000 | 832915 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 562.1 | 1.704 | 2.105 | 2.818 | 0.6667 | 1.0000 | 0.7767 | 829722 | 9270.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 566.9 | 1.691 | 2.066 | 2.842 | 1.0000 | 1.0000 | 1.0000 | 1002369 | 9243.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 568.7 | 1.689 | 2.026 | 2.395 | 0.6667 | 1.0000 | 0.7751 | 1005397 | 9275.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 613.5 | 1.552 | 1.942 | 2.631 | 1.0000 | 1.0000 | 1.0000 | 1029900 | 8402.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 898.2 | 1.059 | 1.291 | 1.662 | 0.6667 | 1.0000 | 0.7654 | 816750 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 572.8 | 1.687 | 2.099 | 2.555 | 0.5556 | 0.7222 | 1.0000 | 854613 | 9120.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 558.6 | 1.720 | 2.164 | 2.405 | 0.5556 | 0.8333 | 1.0000 | 858707 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18208.8 | 0.053 | 0.062 | 0.075 | 1.0000 | 1.0000 | 1.0000 | 109132 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17686.1 | 0.055 | 0.063 | 0.073 | 0.6667 | 1.0000 | 0.7767 | 114898 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16558.9 | 0.055 | 0.075 | 0.125 | 1.0000 | 1.0000 | 1.0000 | 285740 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 16246.2 | 0.056 | 0.083 | 0.138 | 0.6667 | 1.0000 | 0.7751 | 288785 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15843.3 | 0.054 | 0.113 | 0.160 | 1.0000 | 1.0000 | 1.0000 | 370960 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15577.0 | 0.054 | 0.116 | 0.147 | 0.6667 | 1.0000 | 0.7654 | 370909 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17687.7 | 0.054 | 0.065 | 0.077 | 0.5556 | 0.7222 | 1.0000 | 134358 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 17073.5 | 0.057 | 0.066 | 0.078 | 0.5556 | 0.8333 | 1.0000 | 143849 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 579.6 | 1.675 | 2.051 | 2.315 | 1.0000 | 1.0000 | 1.0000 | 797801 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 577.9 | 1.683 | 2.042 | 2.241 | 0.6667 | 1.0000 | 0.7767 | 792215 | 9202.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 581.3 | 1.670 | 2.033 | 2.377 | 1.0000 | 1.0000 | 1.0000 | 973516 | 9197.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 666.1 | 1.398 | 1.805 | 2.099 | 0.6667 | 1.0000 | 0.7751 | 900722 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 831.4 | 1.042 | 1.563 | 1.839 | 1.0000 | 1.0000 | 1.0000 | 867101 | 6623.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1191.5 | 0.728 | 1.126 | 1.390 | 0.6667 | 1.0000 | 0.7654 | 679832 | 4600.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 579.5 | 1.685 | 2.028 | 2.417 | 0.5556 | 0.6667 | 1.0000 | 790431 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 571.6 | 1.695 | 2.112 | 2.403 | 0.5556 | 0.8333 | 1.0000 | 809260 | 9116.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20078.4 | 0.048 | 0.054 | 0.059 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19258.7 | 0.049 | 0.057 | 0.071 | 0.6667 | 1.0000 | 0.7767 | 79012 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 18633.7 | 0.050 | 0.056 | 0.126 | 1.0000 | 1.0000 | 1.0000 | 258506 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18324.7 | 0.050 | 0.059 | 0.153 | 0.6667 | 1.0000 | 0.7751 | 259803 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19058.8 | 0.048 | 0.055 | 0.158 | 1.0000 | 1.0000 | 1.0000 | 350622 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18634.5 | 0.049 | 0.061 | 0.177 | 0.6667 | 1.0000 | 0.7654 | 350593 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 23060.6 | 0.040 | 0.056 | 0.099 | 0.5556 | 0.6667 | 1.0000 | 70041 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19489.7 | 0.049 | 0.055 | 0.092 | 0.5556 | 0.8333 | 1.0000 | 96096 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 592.2 | 1.645 | 1.960 | 2.305 | 1.0000 | 1.0000 | 1.0000 | 761101 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 581.8 | 1.666 | 1.998 | 2.377 | 0.6667 | 1.0000 | 0.7767 | 762118 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 585.2 | 1.653 | 1.987 | 2.509 | 1.0000 | 1.0000 | 1.0000 | 946059 | 8972.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 581.7 | 1.654 | 2.056 | 2.597 | 0.6667 | 1.0000 | 0.7751 | 950544 | 9005.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 629.3 | 1.526 | 1.873 | 2.593 | 1.0000 | 1.0000 | 1.0000 | 982600 | 8162.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 889.0 | 1.042 | 1.486 | 2.298 | 0.6667 | 1.0000 | 0.7654 | 771218 | 5564.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 590.4 | 1.648 | 1.980 | 2.324 | 0.5556 | 0.6667 | 1.0000 | 744143 | 8810.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 592.5 | 1.644 | 1.918 | 2.230 | 0.5556 | 0.8333 | 1.0000 | 769446 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 45653.2 | 0.021 | 0.026 | 0.030 | 1.0000 | 1.0000 | 1.0000 | 38893 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44463.2 | 0.021 | 0.027 | 0.031 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 37223.9 | 0.023 | 0.036 | 0.080 | 1.0000 | 1.0000 | 1.0000 | 229404 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 34409.7 | 0.026 | 0.033 | 0.096 | 0.6667 | 1.0000 | 0.7751 | 232080 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30855.2 | 0.028 | 0.047 | 0.121 | 1.0000 | 1.0000 | 1.0000 | 325382 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30479.9 | 0.028 | 0.047 | 0.126 | 0.6667 | 1.0000 | 0.7654 | 325332 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 60542.4 | 0.015 | 0.018 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38842.6 | 0.024 | 0.031 | 0.048 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1967.2 | 1.782 | 3.047 | 5.036 | 1.0000 | 1.0000 | 1.0000 | 833051 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1972.6 | 1.777 | 2.905 | 5.462 | 0.6667 | 1.0000 | 0.7767 | 829877 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1922.6 | 1.780 | 3.041 | 6.097 | 1.0000 | 1.0000 | 1.0000 | 1002452 | 9243.4 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1986.4 | 1.782 | 2.723 | 3.911 | 0.6667 | 1.0000 | 0.7751 | 1005474 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2183.0 | 1.609 | 2.539 | 3.747 | 1.0000 | 1.0000 | 1.0000 | 1029981 | 8402.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2966.5 | 1.118 | 2.016 | 4.138 | 0.6667 | 1.0000 | 0.7654 | 816831 | 5804.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1808.1 | 1.790 | 3.050 | 7.508 | 0.5556 | 0.7222 | 1.0000 | 854763 | 9120.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1955.8 | 1.797 | 2.922 | 5.457 | 0.5556 | 0.8333 | 1.0000 | 858846 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 50070.9 | 0.068 | 0.128 | 0.156 | 1.0000 | 1.0000 | 1.0000 | 109261 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46496.1 | 0.074 | 0.142 | 0.180 | 0.6667 | 1.0000 | 0.7767 | 115033 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 30990.5 | 0.106 | 0.248 | 0.511 | 1.0000 | 1.0000 | 1.0000 | 285843 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 30462.7 | 0.107 | 0.240 | 0.515 | 0.6667 | 1.0000 | 0.7751 | 288970 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 23808.7 | 0.131 | 0.219 | 0.430 | 1.0000 | 1.0000 | 1.0000 | 371076 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 22737.4 | 0.129 | 0.225 | 0.682 | 0.6667 | 1.0000 | 0.7654 | 371012 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 46049.0 | 0.076 | 0.145 | 0.173 | 0.5556 | 0.7222 | 1.0000 | 134514 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 42553.3 | 0.082 | 0.159 | 0.195 | 0.5556 | 0.8333 | 1.0000 | 144011 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2110.6 | 1.720 | 2.677 | 3.353 | 1.0000 | 1.0000 | 1.0000 | 798268 | 9165.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2082.6 | 1.716 | 2.912 | 3.780 | 0.6667 | 1.0000 | 0.7767 | 792636 | 9202.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2109.1 | 1.711 | 2.740 | 3.578 | 1.0000 | 1.0000 | 1.0000 | 973976 | 9198.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2424.0 | 1.481 | 2.439 | 3.108 | 0.6667 | 1.0000 | 0.7751 | 901223 | 8059.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2896.7 | 1.128 | 2.250 | 2.912 | 1.0000 | 1.0000 | 1.0000 | 867441 | 6624.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4189.1 | 0.794 | 1.619 | 2.129 | 0.6667 | 1.0000 | 0.7654 | 680114 | 4600.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2133.7 | 1.707 | 2.620 | 3.722 | 0.5556 | 0.6667 | 1.0000 | 791772 | 9014.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2049.7 | 1.716 | 3.009 | 4.036 | 0.5556 | 0.8333 | 1.0000 | 809783 | 9116.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 59863.3 | 0.048 | 0.125 | 0.202 | 1.0000 | 1.0000 | 1.0000 | 75777 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 54075.9 | 0.051 | 0.145 | 0.262 | 0.6667 | 1.0000 | 0.7767 | 79169 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 39182.1 | 0.080 | 0.214 | 0.430 | 1.0000 | 1.0000 | 1.0000 | 258713 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41115.1 | 0.075 | 0.205 | 0.499 | 0.6667 | 1.0000 | 0.7751 | 259962 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30014.2 | 0.111 | 0.285 | 0.563 | 1.0000 | 1.0000 | 1.0000 | 350843 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30074.5 | 0.112 | 0.281 | 0.601 | 0.6667 | 1.0000 | 0.7654 | 350803 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 68054.0 | 0.040 | 0.114 | 0.180 | 0.5556 | 0.6667 | 1.0000 | 70219 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 56392.3 | 0.048 | 0.134 | 0.343 | 0.5556 | 0.8333 | 1.0000 | 96267 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2126.0 | 1.694 | 2.736 | 3.506 | 1.0000 | 1.0000 | 1.0000 | 761336 | 8919.9 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2130.5 | 1.692 | 2.839 | 4.031 | 0.6667 | 1.0000 | 0.7767 | 762264 | 8969.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2088.0 | 1.697 | 3.006 | 4.131 | 1.0000 | 1.0000 | 1.0000 | 946256 | 8972.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1757.7 | 2.054 | 3.555 | 5.277 | 0.6667 | 1.0000 | 0.7751 | 950588 | 9005.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2266.2 | 1.546 | 2.741 | 4.016 | 1.0000 | 1.0000 | 1.0000 | 982642 | 8162.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3246.3 | 1.044 | 1.981 | 3.615 | 0.6667 | 1.0000 | 0.7654 | 771273 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2152.2 | 1.683 | 2.656 | 3.538 | 0.5556 | 0.6667 | 1.0000 | 744178 | 8810.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2079.7 | 1.699 | 3.014 | 3.614 | 0.5556 | 0.8333 | 1.0000 | 769471 | 8914.0 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 119588.9 | 0.020 | 0.059 | 0.091 | 1.0000 | 1.0000 | 1.0000 | 38920 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 113438.4 | 0.022 | 0.063 | 0.089 | 0.6667 | 1.0000 | 0.7767 | 43660 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 41974.3 | 0.039 | 0.392 | 1.282 | 1.0000 | 1.0000 | 1.0000 | 229500 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 38954.6 | 0.042 | 0.408 | 1.063 | 0.6667 | 1.0000 | 0.7751 | 232161 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30345.6 | 0.116 | 0.213 | 0.542 | 1.0000 | 1.0000 | 1.0000 | 325504 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 28961.1 | 0.121 | 0.265 | 0.677 | 0.6667 | 1.0000 | 0.7654 | 325462 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 183478.9 | 0.015 | 0.027 | 0.044 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 106887.2 | 0.023 | 0.071 | 0.096 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 405.4 | 2.358 | 2.892 | 3.774 | 1.0000 | 1.0000 | 1.0000 | 1162225 | 12331.2 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 404.2 | 2.387 | 2.793 | 3.117 | 0.6667 | 1.0000 | 0.7767 | 1159251 | 12386.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 406.3 | 2.370 | 2.801 | 3.544 | 1.0000 | 1.0000 | 1.0000 | 1333431 | 12377.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 405.1 | 2.388 | 2.832 | 3.526 | 0.6667 | 1.0000 | 0.7751 | 1333751 | 12358.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 437.1 | 2.202 | 2.601 | 3.340 | 1.0000 | 1.0000 | 1.0000 | 1346797 | 11246.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 576.8 | 1.669 | 1.985 | 2.240 | 0.6667 | 1.0000 | 0.7654 | 1099338 | 8025.0 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 372.1 | 2.478 | 3.507 | 7.464 | 0.5556 | 0.7222 | 1.0000 | 1183709 | 12221.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 397.1 | 2.407 | 2.934 | 4.512 | 0.5556 | 0.8333 | 1.0000 | 1187780 | 12302.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 498.4 | 1.940 | 2.404 | 2.710 | 1.0000 | 1.0000 | 1.0000 | 921355 | 11114.9 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 498.6 | 1.944 | 2.368 | 2.688 | 0.6667 | 1.0000 | 0.7767 | 916390 | 11155.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 497.0 | 1.933 | 2.382 | 2.810 | 1.0000 | 1.0000 | 1.0000 | 1099736 | 11169.9 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 563.4 | 1.673 | 2.194 | 2.615 | 0.6667 | 1.0000 | 0.7751 | 1012599 | 9796.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 698.2 | 1.267 | 1.886 | 2.176 | 1.0000 | 1.0000 | 1.0000 | 963039 | 8076.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 979.3 | 0.913 | 1.313 | 1.604 | 0.6667 | 1.0000 | 0.7654 | 754504 | 5640.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 494.5 | 1.957 | 2.419 | 2.731 | 0.5556 | 0.6667 | 1.0000 | 912195 | 10952.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 498.1 | 1.943 | 2.378 | 2.688 | 0.5556 | 0.8333 | 1.0000 | 931566 | 11058.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 428.3 | 2.253 | 2.795 | 3.288 | 1.0000 | 1.0000 | 1.0000 | 1072745 | 11626.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 419.4 | 2.288 | 2.849 | 3.512 | 1.0000 | 1.0000 | 1.0000 | 1259302 | 11701.0 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 458.5 | 2.086 | 2.621 | 3.220 | 1.0000 | 1.0000 | 1.0000 | 1283762 | 10724.3 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 354.1 | 2.722 | 3.316 | 3.746 | 0.5556 | 0.6667 | 1.0000 | 1287859 | 12106.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1451.4 | 2.445 | 4.213 | 7.418 | 1.0000 | 1.0000 | 1.0000 | 1162430 | 12333.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1295.6 | 2.859 | 4.588 | 6.876 | 0.6667 | 1.0000 | 0.7767 | 1159277 | 12387.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1442.6 | 2.486 | 4.308 | 5.779 | 1.0000 | 1.0000 | 1.0000 | 1333023 | 12380.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1389.4 | 2.526 | 4.525 | 7.776 | 0.6667 | 1.0000 | 0.7751 | 1334036 | 12361.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1512.1 | 2.317 | 4.315 | 5.937 | 1.0000 | 1.0000 | 1.0000 | 1347037 | 11249.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1996.8 | 1.729 | 3.390 | 5.421 | 0.6667 | 1.0000 | 0.7654 | 1099502 | 8027.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1378.1 | 2.574 | 4.543 | 6.519 | 0.5556 | 0.7222 | 1.0000 | 1183422 | 12224.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1392.6 | 2.533 | 4.411 | 7.683 | 0.5556 | 0.8333 | 1.0000 | 1187786 | 12305.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1805.8 | 2.015 | 3.139 | 3.822 | 1.0000 | 1.0000 | 1.0000 | 922389 | 11117.4 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1817.6 | 2.018 | 3.061 | 3.652 | 0.6667 | 1.0000 | 0.7767 | 917650 | 11157.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1759.4 | 2.071 | 3.292 | 4.030 | 1.0000 | 1.0000 | 1.0000 | 1100854 | 11172.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1987.2 | 1.913 | 3.053 | 3.792 | 0.6667 | 1.0000 | 0.7751 | 1013026 | 9798.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2403.7 | 1.517 | 2.575 | 3.307 | 1.0000 | 1.0000 | 1.0000 | 963943 | 8078.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3429.3 | 1.077 | 1.880 | 2.304 | 0.6667 | 1.0000 | 0.7654 | 755017 | 5642.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1782.2 | 2.040 | 3.194 | 4.010 | 0.5556 | 0.6667 | 1.0000 | 914447 | 10955.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1802.9 | 2.022 | 3.124 | 3.647 | 0.5556 | 0.8333 | 1.0000 | 932294 | 11060.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1553.0 | 2.320 | 3.786 | 4.622 | 1.0000 | 1.0000 | 1.0000 | 1072227 | 11628.5 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1505.9 | 2.379 | 4.069 | 5.334 | 1.0000 | 1.0000 | 1.0000 | 1259009 | 11702.9 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1575.9 | 2.251 | 3.826 | 6.708 | 1.0000 | 1.0000 | 1.0000 | 1283043 | 10726.5 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1274.9 | 2.881 | 4.212 | 5.014 | 0.5556 | 0.6667 | 1.0000 | 1289636 | 12108.2 |

## Unsupported capability evidence

- `http_score_only_route_unavailable`: 80 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable`: 16 rows; `*main.capabilityError`; zero results; fail closed.
- `http_vector_parent_collapse_unavailable+http_score_only_route_unavailable`: 16 rows; `*main.capabilityError`; zero results; fail closed.

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
