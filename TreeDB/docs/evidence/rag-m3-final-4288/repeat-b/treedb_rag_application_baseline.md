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
- command: `/tmp/treedb_rag_benchmark_8fc41fac0 -out-dir /tmp/gomap-rag-m3-finalperf-b -dir /tmp/gomap-rag-m3-finalperf-b-db -product-base-sha 3b3235ea1e83eb75d589b5379b05888b739b6b08 -harness-revision 8fc41fac0b8cbebc213b6c1ff0759cac85be99cf -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 final performance B`

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
| 0 | 18 | 54 | 0.059756 | 301.23 | 903.68 | 2350087 | 4813 | 3748802 | true |
| 1 | 18 | 54 | 0.044686 | 402.81 | 1208.43 | 1853028 | 4681 | 3748827 | true |
| 2 | 18 | 54 | 0.043312 | 415.59 | 1246.77 | 1835719 | 4674 | 3748802 | true |
| 3 | 18 | 54 | 0.039266 | 458.41 | 1375.23 | 1863000 | 4679 | 3748827 | true |
| 4 | 18 | 54 | 0.042723 | 421.32 | 1263.96 | 1836030 | 4673 | 3748827 | true |

Median/p95 docs/s: **415.59 / 450.99**. Median/p95 B/source: **1853028 / 2252670**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final and repeated artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4645.4 | 0.209 | 0.234 | 0.364 | 1.0000 | 1.0000 | 1.0000 | 253408 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4349.3 | 0.218 | 0.286 | 0.387 | 0.6667 | 1.0000 | 0.7767 | 259115 | 2323.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4442.6 | 0.214 | 0.256 | 0.382 | 1.0000 | 1.0000 | 1.0000 | 426231 | 2290.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4404.9 | 0.213 | 0.247 | 0.632 | 0.6667 | 1.0000 | 0.7751 | 429382 | 2323.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4672.1 | 0.200 | 0.281 | 0.698 | 1.0000 | 1.0000 | 1.0000 | 497320 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5836.7 | 0.156 | 0.241 | 0.654 | 0.6667 | 1.0000 | 0.7654 | 455797 | 1634.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4553.8 | 0.210 | 0.238 | 0.395 | 0.5556 | 0.8333 | 1.0000 | 277784 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4477.4 | 0.214 | 0.249 | 0.392 | 0.5556 | 1.0000 | 1.0000 | 286650 | 2237.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18315.1 | 0.053 | 0.060 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17365.0 | 0.055 | 0.065 | 0.100 | 0.6667 | 1.0000 | 0.7767 | 113639 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16150.7 | 0.053 | 0.107 | 0.160 | 1.0000 | 1.0000 | 1.0000 | 284486 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15544.9 | 0.055 | 0.070 | 0.274 | 0.6667 | 1.0000 | 0.7751 | 287569 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 16051.6 | 0.053 | 0.093 | 0.225 | 1.0000 | 1.0000 | 1.0000 | 369695 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14743.4 | 0.053 | 0.137 | 0.259 | 0.6667 | 1.0000 | 0.7654 | 369617 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17245.5 | 0.053 | 0.063 | 0.264 | 0.5556 | 0.8333 | 1.0000 | 133005 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16210.9 | 0.057 | 0.070 | 0.192 | 0.5556 | 1.0000 | 1.0000 | 141997 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4801.9 | 0.203 | 0.222 | 0.321 | 1.0000 | 1.0000 | 1.0000 | 221764 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4784.2 | 0.204 | 0.219 | 0.345 | 0.6667 | 1.0000 | 0.7767 | 221870 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4786.1 | 0.201 | 0.219 | 0.394 | 1.0000 | 1.0000 | 1.0000 | 401368 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5244.3 | 0.176 | 0.216 | 0.513 | 0.6667 | 1.0000 | 0.7751 | 385282 | 2032.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6176.3 | 0.142 | 0.195 | 0.356 | 1.0000 | 1.0000 | 1.0000 | 449043 | 1758.8 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7862.1 | 0.112 | 0.147 | 0.323 | 0.6667 | 1.0000 | 0.7654 | 416373 | 1357.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5058.2 | 0.193 | 0.212 | 0.329 | 0.5556 | 0.6667 | 1.0000 | 215832 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4806.8 | 0.203 | 0.218 | 0.359 | 0.5556 | 0.8333 | 1.0000 | 241503 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20217.5 | 0.048 | 0.053 | 0.061 | 1.0000 | 1.0000 | 1.0000 | 75648 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19496.6 | 0.050 | 0.054 | 0.058 | 0.6667 | 1.0000 | 0.7767 | 79020 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19428.2 | 0.048 | 0.053 | 0.144 | 1.0000 | 1.0000 | 1.0000 | 258522 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18825.9 | 0.050 | 0.054 | 0.139 | 0.6667 | 1.0000 | 0.7751 | 259827 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19688.3 | 0.047 | 0.050 | 0.207 | 1.0000 | 1.0000 | 1.0000 | 350640 | 518.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19261.7 | 0.048 | 0.053 | 0.175 | 0.6667 | 1.0000 | 0.7654 | 350620 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24401.1 | 0.039 | 0.047 | 0.066 | 0.5556 | 0.6667 | 1.0000 | 70051 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 18816.9 | 0.049 | 0.075 | 0.129 | 0.5556 | 0.8333 | 1.0000 | 96083 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5593.5 | 0.174 | 0.187 | 0.277 | 1.0000 | 1.0000 | 1.0000 | 183456 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5495.9 | 0.177 | 0.191 | 0.264 | 0.6667 | 1.0000 | 0.7767 | 187870 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5315.9 | 0.179 | 0.200 | 0.346 | 1.0000 | 1.0000 | 1.0000 | 370196 | 2020.6 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5219.7 | 0.183 | 0.209 | 0.330 | 0.6667 | 1.0000 | 0.7751 | 372868 | 2053.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5534.9 | 0.170 | 0.199 | 0.327 | 1.0000 | 1.0000 | 1.0000 | 450804 | 1908.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7336.6 | 0.125 | 0.182 | 0.400 | 0.6667 | 1.0000 | 0.7654 | 409601 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5774.2 | 0.169 | 0.182 | 0.259 | 0.5556 | 0.8889 | 1.0000 | 168718 | 1862.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5539.7 | 0.176 | 0.189 | 0.245 | 0.5556 | 0.9444 | 1.0000 | 194046 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52316.5 | 0.018 | 0.021 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 37641 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45891.8 | 0.020 | 0.024 | 0.027 | 0.6667 | 1.0000 | 0.7767 | 42336 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 39178.8 | 0.022 | 0.032 | 0.094 | 1.0000 | 1.0000 | 1.0000 | 228099 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 36734.7 | 0.024 | 0.033 | 0.094 | 0.6667 | 1.0000 | 0.7751 | 230801 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32469.2 | 0.027 | 0.040 | 0.125 | 1.0000 | 1.0000 | 1.0000 | 324106 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31699.7 | 0.027 | 0.040 | 0.135 | 0.6667 | 1.0000 | 0.7654 | 324049 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 67620.0 | 0.013 | 0.017 | 0.022 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46702.0 | 0.020 | 0.023 | 0.037 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 12659.1 | 0.260 | 0.573 | 0.930 | 1.0000 | 1.0000 | 1.0000 | 253515 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13681.1 | 0.246 | 0.435 | 1.046 | 0.6667 | 1.0000 | 0.7767 | 259241 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 12630.0 | 0.262 | 0.489 | 0.794 | 1.0000 | 1.0000 | 1.0000 | 426356 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 12136.5 | 0.256 | 0.570 | 0.888 | 0.6667 | 1.0000 | 0.7751 | 429501 | 2323.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12844.4 | 0.254 | 0.446 | 0.737 | 1.0000 | 1.0000 | 1.0000 | 497411 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14072.9 | 0.228 | 0.495 | 1.328 | 0.6667 | 1.0000 | 0.7654 | 455935 | 1634.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 14142.3 | 0.246 | 0.497 | 0.875 | 0.5556 | 0.8333 | 1.0000 | 277929 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13698.3 | 0.250 | 0.435 | 0.879 | 0.5556 | 1.0000 | 1.0000 | 286819 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 50051.2 | 0.063 | 0.140 | 0.253 | 1.0000 | 1.0000 | 1.0000 | 107988 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 47565.3 | 0.071 | 0.138 | 0.182 | 0.6667 | 1.0000 | 0.7767 | 113775 | 561.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 28886.6 | 0.102 | 0.239 | 0.642 | 1.0000 | 1.0000 | 1.0000 | 284579 | 533.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 27865.5 | 0.103 | 0.264 | 0.906 | 0.6667 | 1.0000 | 0.7751 | 287666 | 566.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 24062.6 | 0.130 | 0.255 | 0.507 | 1.0000 | 1.0000 | 1.0000 | 369762 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 23010.8 | 0.140 | 0.274 | 0.754 | 0.6667 | 1.0000 | 0.7654 | 369745 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43234.7 | 0.078 | 0.155 | 0.296 | 0.5556 | 0.8333 | 1.0000 | 133091 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41518.2 | 0.078 | 0.171 | 0.369 | 0.5556 | 1.0000 | 1.0000 | 142147 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15566.2 | 0.214 | 0.489 | 0.704 | 1.0000 | 1.0000 | 1.0000 | 222046 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15842.7 | 0.214 | 0.464 | 0.715 | 0.6667 | 1.0000 | 0.7767 | 222070 | 2252.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14475.6 | 0.221 | 0.571 | 0.821 | 1.0000 | 1.0000 | 1.0000 | 401711 | 2244.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 16315.1 | 0.205 | 0.503 | 0.678 | 0.6667 | 1.0000 | 0.7751 | 385587 | 2032.4 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 16516.8 | 0.194 | 0.516 | 0.775 | 1.0000 | 1.0000 | 1.0000 | 449345 | 1759.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20148.4 | 0.159 | 0.441 | 0.656 | 0.6667 | 1.0000 | 0.7654 | 416724 | 1357.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16473.4 | 0.205 | 0.440 | 0.639 | 0.5556 | 0.6667 | 1.0000 | 216424 | 2065.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15625.4 | 0.214 | 0.469 | 0.658 | 0.5556 | 0.8333 | 1.0000 | 241789 | 2169.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 57129.0 | 0.048 | 0.137 | 0.224 | 1.0000 | 1.0000 | 1.0000 | 75814 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 61126.4 | 0.049 | 0.112 | 0.209 | 0.6667 | 1.0000 | 0.7767 | 79172 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 41650.6 | 0.076 | 0.212 | 0.353 | 1.0000 | 1.0000 | 1.0000 | 258781 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41134.0 | 0.076 | 0.210 | 0.387 | 0.6667 | 1.0000 | 0.7751 | 260031 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32286.0 | 0.107 | 0.284 | 0.391 | 1.0000 | 1.0000 | 1.0000 | 350927 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31009.0 | 0.108 | 0.289 | 0.471 | 0.6667 | 1.0000 | 0.7654 | 350920 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 77096.1 | 0.039 | 0.082 | 0.174 | 0.5556 | 0.6667 | 1.0000 | 70235 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 61804.6 | 0.047 | 0.124 | 0.266 | 0.5556 | 0.8333 | 1.0000 | 96285 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18394.3 | 0.184 | 0.395 | 0.597 | 1.0000 | 1.0000 | 1.0000 | 183539 | 1972.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17871.5 | 0.188 | 0.375 | 0.650 | 0.6667 | 1.0000 | 0.7767 | 187917 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 15255.3 | 0.197 | 0.538 | 0.941 | 1.0000 | 1.0000 | 1.0000 | 370263 | 2020.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 14969.4 | 0.204 | 0.512 | 1.253 | 0.6667 | 1.0000 | 0.7751 | 372964 | 2053.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12914.3 | 0.232 | 0.533 | 1.472 | 1.0000 | 1.0000 | 1.0000 | 450867 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 16111.7 | 0.180 | 0.426 | 1.801 | 0.6667 | 1.0000 | 0.7654 | 409656 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 19046.3 | 0.179 | 0.385 | 0.636 | 0.5556 | 0.8889 | 1.0000 | 168727 | 1862.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18272.5 | 0.186 | 0.399 | 0.550 | 0.5556 | 0.9444 | 1.0000 | 194053 | 1966.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 136106.0 | 0.018 | 0.052 | 0.088 | 1.0000 | 1.0000 | 1.0000 | 37661 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 129840.5 | 0.020 | 0.049 | 0.113 | 0.6667 | 1.0000 | 0.7767 | 42384 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 44275.8 | 0.039 | 0.377 | 1.007 | 1.0000 | 1.0000 | 1.0000 | 228249 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42934.5 | 0.039 | 0.287 | 1.164 | 0.6667 | 1.0000 | 0.7751 | 230938 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32202.9 | 0.110 | 0.243 | 0.412 | 1.0000 | 1.0000 | 1.0000 | 324221 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31599.0 | 0.113 | 0.211 | 0.422 | 0.6667 | 1.0000 | 0.7654 | 324184 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 207816.3 | 0.012 | 0.023 | 0.044 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 135248.0 | 0.019 | 0.043 | 0.103 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1403.4 | 0.695 | 0.764 | 1.263 | 1.0000 | 1.0000 | 1.0000 | 558397 | 5372.5 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1384.1 | 0.699 | 0.760 | 0.879 | 0.6667 | 1.0000 | 0.7767 | 564297 | 5427.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1390.6 | 0.695 | 0.762 | 1.345 | 1.0000 | 1.0000 | 1.0000 | 733025 | 5410.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1370.3 | 0.696 | 0.796 | 1.196 | 0.6667 | 1.0000 | 0.7751 | 734487 | 5394.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1459.1 | 0.652 | 0.722 | 1.569 | 1.0000 | 1.0000 | 1.0000 | 791451 | 4981.1 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1608.2 | 0.592 | 0.679 | 1.201 | 0.6667 | 1.0000 | 0.7654 | 715899 | 3844.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1399.5 | 0.692 | 0.768 | 1.412 | 0.5556 | 0.8333 | 1.0000 | 584026 | 5257.2 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1402.8 | 0.693 | 0.748 | 1.415 | 0.5556 | 1.0000 | 1.0000 | 590860 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2296.7 | 0.425 | 0.469 | 0.673 | 1.0000 | 1.0000 | 1.0000 | 344580 | 4166.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2293.4 | 0.426 | 0.469 | 0.638 | 0.6667 | 1.0000 | 0.7767 | 345696 | 4205.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2279.2 | 0.425 | 0.471 | 0.841 | 1.0000 | 1.0000 | 1.0000 | 526705 | 4216.6 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2503.6 | 0.379 | 0.449 | 0.730 | 0.6667 | 1.0000 | 0.7751 | 496530 | 3769.3 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2680.7 | 0.345 | 0.437 | 0.753 | 1.0000 | 1.0000 | 1.0000 | 545083 | 3211.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3411.8 | 0.284 | 0.362 | 0.695 | 0.6667 | 1.0000 | 0.7654 | 491187 | 2397.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2379.0 | 0.412 | 0.452 | 0.688 | 0.5556 | 0.6667 | 1.0000 | 336630 | 4004.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2300.6 | 0.424 | 0.476 | 0.678 | 0.5556 | 0.8333 | 1.0000 | 362349 | 4110.8 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1559.2 | 0.628 | 0.686 | 1.076 | 1.0000 | 1.0000 | 1.0000 | 471317 | 4668.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1508.8 | 0.639 | 0.697 | 1.227 | 1.0000 | 1.0000 | 1.0000 | 659763 | 4738.5 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1559.8 | 0.617 | 0.678 | 1.040 | 1.0000 | 1.0000 | 1.0000 | 727274 | 4457.4 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 937.8 | 1.044 | 1.153 | 1.552 | 0.5556 | 0.8889 | 1.0000 | 682217 | 5148.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4214.7 | 0.790 | 1.952 | 2.854 | 1.0000 | 1.0000 | 1.0000 | 558192 | 5375.1 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4470.8 | 0.754 | 1.554 | 2.722 | 0.6667 | 1.0000 | 0.7767 | 566213 | 5430.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4315.3 | 0.757 | 1.591 | 2.661 | 1.0000 | 1.0000 | 1.0000 | 732609 | 5413.9 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4359.2 | 0.764 | 1.815 | 2.679 | 0.6667 | 1.0000 | 0.7751 | 734776 | 5397.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4212.4 | 0.750 | 2.035 | 2.720 | 1.0000 | 1.0000 | 1.0000 | 791742 | 4984.1 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5499.0 | 0.628 | 1.438 | 2.198 | 0.6667 | 1.0000 | 0.7654 | 715950 | 3847.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4473.9 | 0.754 | 1.556 | 2.382 | 0.5556 | 0.8333 | 1.0000 | 582955 | 5259.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4595.4 | 0.747 | 1.506 | 2.358 | 0.5556 | 1.0000 | 1.0000 | 591211 | 5332.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7677.9 | 0.446 | 0.953 | 1.380 | 1.0000 | 1.0000 | 1.0000 | 345174 | 4170.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 6983.1 | 0.474 | 1.005 | 1.537 | 0.6667 | 1.0000 | 0.7767 | 345979 | 4210.2 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7439.2 | 0.456 | 0.940 | 1.275 | 1.0000 | 1.0000 | 1.0000 | 527021 | 4221.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8129.0 | 0.423 | 0.892 | 1.173 | 0.6667 | 1.0000 | 0.7751 | 497158 | 3773.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 8881.3 | 0.396 | 0.839 | 1.232 | 1.0000 | 1.0000 | 1.0000 | 546021 | 3216.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 10606.6 | 0.323 | 0.732 | 0.969 | 0.6667 | 1.0000 | 0.7654 | 491816 | 2402.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 8221.5 | 0.425 | 0.790 | 1.130 | 0.5556 | 0.6667 | 1.0000 | 338136 | 4008.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7727.8 | 0.449 | 0.848 | 1.141 | 0.5556 | 0.8333 | 1.0000 | 363563 | 4114.3 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5238.0 | 0.642 | 1.270 | 2.050 | 1.0000 | 1.0000 | 1.0000 | 470323 | 4670.5 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4832.1 | 0.666 | 1.607 | 2.576 | 1.0000 | 1.0000 | 1.0000 | 658802 | 4741.0 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5180.4 | 0.635 | 1.407 | 2.291 | 1.0000 | 1.0000 | 1.0000 | 726872 | 4460.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2999.9 | 1.177 | 2.015 | 2.279 | 0.5556 | 0.8889 | 1.0000 | 682973 | 5150.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 568.5 | 1.693 | 2.052 | 2.413 | 1.0000 | 1.0000 | 1.0000 | 832929 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 565.7 | 1.699 | 2.099 | 2.414 | 0.6667 | 1.0000 | 0.7767 | 829729 | 9270.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 572.1 | 1.683 | 2.051 | 2.478 | 1.0000 | 1.0000 | 1.0000 | 1002355 | 9243.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 570.3 | 1.694 | 2.086 | 2.323 | 0.6667 | 1.0000 | 0.7751 | 1005376 | 9275.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 625.3 | 1.533 | 1.890 | 2.238 | 1.0000 | 1.0000 | 1.0000 | 1029870 | 8402.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 894.0 | 1.062 | 1.353 | 1.735 | 0.6667 | 1.0000 | 0.7654 | 816758 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 570.8 | 1.694 | 2.079 | 2.481 | 0.5556 | 0.7222 | 1.0000 | 854649 | 9120.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 571.0 | 1.689 | 1.982 | 2.387 | 0.5556 | 0.8333 | 1.0000 | 858738 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18318.5 | 0.053 | 0.061 | 0.076 | 1.0000 | 1.0000 | 1.0000 | 109119 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17408.4 | 0.056 | 0.064 | 0.082 | 0.6667 | 1.0000 | 0.7767 | 114897 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16234.9 | 0.056 | 0.085 | 0.127 | 1.0000 | 1.0000 | 1.0000 | 285734 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 16147.2 | 0.056 | 0.073 | 0.255 | 0.6667 | 1.0000 | 0.7751 | 288799 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15347.8 | 0.054 | 0.116 | 0.276 | 1.0000 | 1.0000 | 1.0000 | 370959 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15691.2 | 0.054 | 0.069 | 0.233 | 0.6667 | 1.0000 | 0.7654 | 370875 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17825.2 | 0.054 | 0.064 | 0.075 | 0.5556 | 0.7222 | 1.0000 | 134392 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 17148.7 | 0.057 | 0.065 | 0.076 | 0.5556 | 0.8333 | 1.0000 | 143848 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 577.0 | 1.683 | 2.059 | 2.373 | 1.0000 | 1.0000 | 1.0000 | 797806 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 571.9 | 1.685 | 2.115 | 2.378 | 0.6667 | 1.0000 | 0.7767 | 792211 | 9202.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 583.8 | 1.671 | 1.942 | 2.308 | 1.0000 | 1.0000 | 1.0000 | 973504 | 9197.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 660.2 | 1.413 | 1.869 | 2.223 | 0.6667 | 1.0000 | 0.7751 | 900727 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 819.4 | 1.054 | 1.601 | 1.972 | 1.0000 | 1.0000 | 1.0000 | 867048 | 6623.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1183.9 | 0.732 | 1.126 | 1.349 | 0.6667 | 1.0000 | 0.7654 | 679790 | 4600.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 579.4 | 1.674 | 2.035 | 2.381 | 0.5556 | 0.6667 | 1.0000 | 790408 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 575.6 | 1.683 | 2.052 | 2.400 | 0.5556 | 0.8333 | 1.0000 | 809288 | 9116.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19354.2 | 0.047 | 0.080 | 0.129 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19607.9 | 0.049 | 0.056 | 0.080 | 0.6667 | 1.0000 | 0.7767 | 79019 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 20221.6 | 0.044 | 0.054 | 0.126 | 1.0000 | 1.0000 | 1.0000 | 258533 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19059.1 | 0.047 | 0.058 | 0.143 | 0.6667 | 1.0000 | 0.7751 | 259810 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19156.6 | 0.047 | 0.054 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 350643 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18727.3 | 0.048 | 0.061 | 0.181 | 0.6667 | 1.0000 | 0.7654 | 350609 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24824.9 | 0.038 | 0.045 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 70047 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19416.9 | 0.049 | 0.055 | 0.079 | 0.5556 | 0.8333 | 1.0000 | 96089 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 598.7 | 1.631 | 1.890 | 2.282 | 1.0000 | 1.0000 | 1.0000 | 761007 | 8919.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 577.8 | 1.653 | 2.073 | 2.862 | 0.6667 | 1.0000 | 0.7767 | 762132 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 590.9 | 1.645 | 1.912 | 2.427 | 1.0000 | 1.0000 | 1.0000 | 946109 | 8972.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 574.4 | 1.658 | 2.042 | 3.144 | 0.6667 | 1.0000 | 0.7751 | 950519 | 9005.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 640.5 | 1.506 | 1.813 | 2.371 | 1.0000 | 1.0000 | 1.0000 | 982569 | 8162.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 948.7 | 1.019 | 1.232 | 1.550 | 0.6667 | 1.0000 | 0.7654 | 771228 | 5564.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 583.8 | 1.664 | 1.987 | 2.351 | 0.5556 | 0.6667 | 1.0000 | 744151 | 8810.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 587.3 | 1.652 | 1.988 | 2.324 | 0.5556 | 0.8333 | 1.0000 | 769442 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 46519.0 | 0.020 | 0.026 | 0.029 | 1.0000 | 1.0000 | 1.0000 | 38899 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43696.5 | 0.021 | 0.026 | 0.032 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 35043.0 | 0.024 | 0.037 | 0.106 | 1.0000 | 1.0000 | 1.0000 | 229397 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 33061.6 | 0.026 | 0.044 | 0.095 | 0.6667 | 1.0000 | 0.7751 | 232087 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30208.9 | 0.028 | 0.041 | 0.141 | 1.0000 | 1.0000 | 1.0000 | 325403 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29251.6 | 0.029 | 0.052 | 0.125 | 0.6667 | 1.0000 | 0.7654 | 325293 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 59076.6 | 0.016 | 0.020 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39780.6 | 0.023 | 0.031 | 0.042 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1938.3 | 1.824 | 3.148 | 4.531 | 1.0000 | 1.0000 | 1.0000 | 833034 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1854.9 | 1.897 | 3.107 | 4.692 | 0.6667 | 1.0000 | 0.7767 | 829851 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1914.2 | 1.831 | 3.054 | 4.938 | 1.0000 | 1.0000 | 1.0000 | 1002446 | 9243.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1931.3 | 1.840 | 3.016 | 4.409 | 0.6667 | 1.0000 | 0.7751 | 1005494 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2137.8 | 1.646 | 2.822 | 4.867 | 1.0000 | 1.0000 | 1.0000 | 1029962 | 8402.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2954.4 | 1.127 | 2.045 | 4.054 | 0.6667 | 1.0000 | 0.7654 | 816826 | 5804.2 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2021.9 | 1.819 | 2.819 | 3.976 | 0.5556 | 0.7222 | 1.0000 | 854717 | 9120.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1945.6 | 1.833 | 3.037 | 5.377 | 0.5556 | 0.8333 | 1.0000 | 858789 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 48388.9 | 0.070 | 0.141 | 0.182 | 1.0000 | 1.0000 | 1.0000 | 109262 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45382.8 | 0.075 | 0.145 | 0.184 | 0.6667 | 1.0000 | 0.7767 | 115040 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 31592.5 | 0.101 | 0.214 | 0.531 | 1.0000 | 1.0000 | 1.0000 | 285851 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 31653.1 | 0.106 | 0.250 | 0.415 | 0.6667 | 1.0000 | 0.7751 | 288908 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 21624.0 | 0.137 | 0.242 | 0.510 | 1.0000 | 1.0000 | 1.0000 | 371022 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 22431.0 | 0.138 | 0.296 | 0.630 | 0.6667 | 1.0000 | 0.7654 | 371005 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 47528.3 | 0.074 | 0.139 | 0.178 | 0.5556 | 0.7222 | 1.0000 | 134494 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 43072.1 | 0.083 | 0.153 | 0.188 | 0.5556 | 0.8333 | 1.0000 | 144012 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2131.5 | 1.715 | 2.666 | 3.285 | 1.0000 | 1.0000 | 1.0000 | 798133 | 9165.3 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2103.0 | 1.725 | 2.697 | 3.569 | 0.6667 | 1.0000 | 0.7767 | 792685 | 9202.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2091.5 | 1.722 | 2.792 | 3.465 | 1.0000 | 1.0000 | 1.0000 | 973988 | 9198.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2397.4 | 1.496 | 2.537 | 3.370 | 0.6667 | 1.0000 | 0.7751 | 901182 | 8059.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2856.3 | 1.155 | 2.333 | 3.216 | 1.0000 | 1.0000 | 1.0000 | 867441 | 6624.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4237.6 | 0.785 | 1.535 | 2.177 | 0.6667 | 1.0000 | 0.7654 | 680132 | 4600.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2134.4 | 1.706 | 2.732 | 3.167 | 0.5556 | 0.6667 | 1.0000 | 791762 | 9014.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2094.1 | 1.726 | 2.753 | 3.602 | 0.5556 | 0.8333 | 1.0000 | 809794 | 9116.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 59289.7 | 0.048 | 0.125 | 0.210 | 1.0000 | 1.0000 | 1.0000 | 75804 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 57999.9 | 0.050 | 0.130 | 0.233 | 0.6667 | 1.0000 | 0.7767 | 79169 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 40255.0 | 0.080 | 0.226 | 0.425 | 1.0000 | 1.0000 | 1.0000 | 258687 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40036.3 | 0.079 | 0.232 | 0.458 | 0.6667 | 1.0000 | 0.7751 | 259963 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31021.7 | 0.109 | 0.275 | 0.505 | 1.0000 | 1.0000 | 1.0000 | 350852 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31497.6 | 0.109 | 0.261 | 0.556 | 0.6667 | 1.0000 | 0.7654 | 350776 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 75952.4 | 0.039 | 0.103 | 0.172 | 0.5556 | 0.6667 | 1.0000 | 70212 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 56074.4 | 0.048 | 0.088 | 0.226 | 0.5556 | 0.8333 | 1.0000 | 96254 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2127.8 | 1.709 | 2.737 | 3.411 | 1.0000 | 1.0000 | 1.0000 | 761262 | 8919.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2119.1 | 1.704 | 2.805 | 4.138 | 0.6667 | 1.0000 | 0.7767 | 762257 | 8969.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2072.5 | 1.726 | 2.942 | 4.093 | 1.0000 | 1.0000 | 1.0000 | 946279 | 8972.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2079.3 | 1.719 | 2.698 | 5.005 | 0.6667 | 1.0000 | 0.7751 | 950631 | 9005.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2264.3 | 1.571 | 2.698 | 3.993 | 1.0000 | 1.0000 | 1.0000 | 982617 | 8162.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3247.5 | 1.054 | 2.063 | 3.370 | 0.6667 | 1.0000 | 0.7654 | 771228 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2135.3 | 1.701 | 2.727 | 3.406 | 0.5556 | 0.6667 | 1.0000 | 744175 | 8810.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2091.0 | 1.708 | 2.978 | 3.519 | 0.5556 | 0.8333 | 1.0000 | 769472 | 8914.0 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 137013.6 | 0.019 | 0.047 | 0.060 | 1.0000 | 1.0000 | 1.0000 | 38907 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 120359.6 | 0.022 | 0.054 | 0.077 | 0.6667 | 1.0000 | 0.7767 | 43653 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 41466.5 | 0.041 | 0.500 | 0.825 | 1.0000 | 1.0000 | 1.0000 | 229458 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 39771.5 | 0.041 | 0.518 | 1.145 | 0.6667 | 1.0000 | 0.7751 | 232203 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 28400.6 | 0.118 | 0.264 | 0.800 | 1.0000 | 1.0000 | 1.0000 | 325498 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 27632.0 | 0.125 | 0.250 | 0.699 | 0.6667 | 1.0000 | 0.7654 | 325421 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 181936.4 | 0.015 | 0.024 | 0.038 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 108464.4 | 0.023 | 0.065 | 0.120 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 414.1 | 2.339 | 2.769 | 3.195 | 1.0000 | 1.0000 | 1.0000 | 1161770 | 12331.0 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 407.2 | 2.372 | 2.780 | 3.353 | 0.6667 | 1.0000 | 0.7767 | 1159106 | 12385.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 403.2 | 2.389 | 2.793 | 3.268 | 1.0000 | 1.0000 | 1.0000 | 1333411 | 12377.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 406.3 | 2.351 | 2.769 | 3.102 | 0.6667 | 1.0000 | 0.7751 | 1333709 | 12358.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 433.0 | 2.206 | 2.630 | 3.358 | 1.0000 | 1.0000 | 1.0000 | 1346623 | 11246.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 568.6 | 1.675 | 2.088 | 3.005 | 0.6667 | 1.0000 | 0.7654 | 1099438 | 8025.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 409.0 | 2.368 | 2.823 | 3.584 | 0.5556 | 0.7222 | 1.0000 | 1184153 | 12222.2 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 406.5 | 2.383 | 2.836 | 3.357 | 0.5556 | 0.8333 | 1.0000 | 1187577 | 12302.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 488.0 | 1.963 | 2.493 | 2.986 | 1.0000 | 1.0000 | 1.0000 | 920926 | 11114.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 497.1 | 1.949 | 2.372 | 2.724 | 0.6667 | 1.0000 | 0.7767 | 917244 | 11155.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 496.4 | 1.946 | 2.398 | 2.662 | 1.0000 | 1.0000 | 1.0000 | 1099389 | 11169.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 562.8 | 1.680 | 2.176 | 2.457 | 0.6667 | 1.0000 | 0.7751 | 1012760 | 9796.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 680.7 | 1.295 | 1.958 | 2.283 | 1.0000 | 1.0000 | 1.0000 | 962490 | 8075.9 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 957.3 | 0.932 | 1.389 | 1.674 | 0.6667 | 1.0000 | 0.7654 | 754935 | 5640.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 500.3 | 1.934 | 2.384 | 2.632 | 0.5556 | 0.6667 | 1.0000 | 912304 | 10952.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 494.6 | 1.952 | 2.405 | 2.726 | 0.5556 | 0.8333 | 1.0000 | 931789 | 11058.5 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 427.6 | 2.249 | 2.815 | 3.177 | 1.0000 | 1.0000 | 1.0000 | 1073263 | 11626.8 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 403.7 | 2.264 | 2.905 | 4.672 | 1.0000 | 1.0000 | 1.0000 | 1259238 | 11701.0 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 450.2 | 2.120 | 2.693 | 3.432 | 1.0000 | 1.0000 | 1.0000 | 1283384 | 10724.1 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 357.1 | 2.704 | 3.287 | 3.657 | 0.5556 | 0.6667 | 1.0000 | 1286142 | 12106.5 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1380.3 | 2.565 | 4.236 | 6.953 | 1.0000 | 1.0000 | 1.0000 | 1162734 | 12334.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1388.8 | 2.587 | 4.577 | 6.489 | 0.6667 | 1.0000 | 0.7767 | 1159451 | 12388.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1431.3 | 2.501 | 3.797 | 10.201 | 1.0000 | 1.0000 | 1.0000 | 1332867 | 12380.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1375.7 | 2.571 | 4.417 | 7.671 | 0.6667 | 1.0000 | 0.7751 | 1333956 | 12361.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1473.9 | 2.353 | 4.340 | 6.659 | 1.0000 | 1.0000 | 1.0000 | 1347275 | 11249.6 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1957.8 | 1.764 | 3.445 | 5.331 | 0.6667 | 1.0000 | 0.7654 | 1099488 | 8027.5 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1418.3 | 2.538 | 4.358 | 6.215 | 0.5556 | 0.7222 | 1.0000 | 1183729 | 12224.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1390.4 | 2.549 | 4.217 | 7.227 | 0.5556 | 0.8333 | 1.0000 | 1188116 | 12306.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1782.6 | 2.047 | 3.130 | 3.770 | 1.0000 | 1.0000 | 1.0000 | 922438 | 11117.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1827.3 | 1.986 | 3.104 | 3.866 | 0.6667 | 1.0000 | 0.7767 | 917306 | 11157.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1802.5 | 2.021 | 3.055 | 3.781 | 1.0000 | 1.0000 | 1.0000 | 1101134 | 11172.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1979.4 | 1.920 | 2.945 | 3.779 | 0.6667 | 1.0000 | 0.7751 | 1012549 | 9798.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2408.0 | 1.495 | 2.599 | 3.376 | 1.0000 | 1.0000 | 1.0000 | 963175 | 8078.3 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3289.4 | 1.122 | 2.040 | 2.648 | 0.6667 | 1.0000 | 0.7654 | 754738 | 5642.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1791.1 | 2.014 | 3.180 | 4.169 | 0.5556 | 0.6667 | 1.0000 | 912702 | 10953.8 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1839.1 | 1.999 | 2.991 | 3.433 | 0.5556 | 0.8333 | 1.0000 | 931706 | 11060.7 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1519.4 | 2.365 | 3.815 | 4.959 | 1.0000 | 1.0000 | 1.0000 | 1072602 | 11628.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1512.7 | 2.378 | 4.094 | 4.982 | 1.0000 | 1.0000 | 1.0000 | 1259712 | 11703.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1593.8 | 2.224 | 3.819 | 5.484 | 1.0000 | 1.0000 | 1.0000 | 1282804 | 10725.9 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1243.4 | 2.955 | 4.428 | 5.413 | 0.5556 | 0.6667 | 1.0000 | 1288967 | 12108.7 |

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
