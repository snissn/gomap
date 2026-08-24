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
- command: `/tmp/treedb_rag_benchmark_8fc41fac0 -out-dir /tmp/gomap-rag-m3-finalperf-a -dir /tmp/gomap-rag-m3-finalperf-a-db -product-base-sha 3b3235ea1e83eb75d589b5379b05888b739b6b08 -harness-revision 8fc41fac0b8cbebc213b6c1ff0759cac85be99cf -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 final performance A`

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
| 0 | 18 | 54 | 0.064283 | 280.01 | 840.04 | 2338316 | 4814 | 3748783 | true |
| 1 | 18 | 54 | 0.048474 | 371.33 | 1114.00 | 1848096 | 4681 | 3748827 | true |
| 2 | 18 | 54 | 0.044571 | 403.85 | 1211.56 | 1842836 | 4675 | 3748827 | true |
| 3 | 18 | 54 | 0.041482 | 433.92 | 1301.77 | 1836162 | 4677 | 3748827 | true |
| 4 | 18 | 54 | 0.039644 | 454.04 | 1362.13 | 1848736 | 4676 | 3748827 | true |

Median/p95 docs/s: **403.85 / 450.02**. Median/p95 B/source: **1848096 / 2240400**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final and repeated artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4552.7 | 0.209 | 0.240 | 0.403 | 1.0000 | 1.0000 | 1.0000 | 253423 | 2274.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4362.3 | 0.219 | 0.243 | 0.341 | 0.6667 | 1.0000 | 0.7767 | 259123 | 2323.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4237.5 | 0.216 | 0.323 | 0.740 | 1.0000 | 1.0000 | 1.0000 | 426256 | 2290.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4348.8 | 0.215 | 0.240 | 0.273 | 0.6667 | 1.0000 | 0.7751 | 429380 | 2323.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4651.0 | 0.199 | 0.232 | 0.328 | 1.0000 | 1.0000 | 1.0000 | 497317 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5970.7 | 0.154 | 0.238 | 0.305 | 0.6667 | 1.0000 | 0.7654 | 455779 | 1634.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4525.5 | 0.211 | 0.236 | 0.369 | 0.5556 | 0.8333 | 1.0000 | 277796 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4539.6 | 0.213 | 0.229 | 0.295 | 0.5556 | 1.0000 | 1.0000 | 286643 | 2237.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18235.3 | 0.054 | 0.060 | 0.068 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17680.1 | 0.055 | 0.063 | 0.081 | 0.6667 | 1.0000 | 0.7767 | 113651 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16196.1 | 0.054 | 0.106 | 0.162 | 1.0000 | 1.0000 | 1.0000 | 284496 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15176.9 | 0.056 | 0.094 | 0.274 | 0.6667 | 1.0000 | 0.7751 | 287569 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15819.8 | 0.053 | 0.096 | 0.209 | 1.0000 | 1.0000 | 1.0000 | 369686 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14340.1 | 0.054 | 0.113 | 0.246 | 0.6667 | 1.0000 | 0.7654 | 369625 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17143.7 | 0.053 | 0.063 | 0.278 | 0.5556 | 0.8333 | 1.0000 | 132973 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16016.0 | 0.057 | 0.070 | 0.283 | 0.5556 | 1.0000 | 1.0000 | 142009 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4780.0 | 0.204 | 0.222 | 0.273 | 1.0000 | 1.0000 | 1.0000 | 221763 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4769.4 | 0.205 | 0.220 | 0.265 | 0.6667 | 1.0000 | 0.7767 | 221883 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4760.3 | 0.203 | 0.221 | 0.372 | 1.0000 | 1.0000 | 1.0000 | 401394 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5237.1 | 0.177 | 0.214 | 0.449 | 0.6667 | 1.0000 | 0.7751 | 385279 | 2032.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6114.0 | 0.143 | 0.195 | 0.349 | 1.0000 | 1.0000 | 1.0000 | 449007 | 1758.7 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7685.0 | 0.114 | 0.155 | 0.303 | 0.6667 | 1.0000 | 0.7654 | 416374 | 1357.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5022.4 | 0.194 | 0.210 | 0.306 | 0.5556 | 0.6667 | 1.0000 | 215819 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4724.0 | 0.205 | 0.225 | 0.437 | 0.5556 | 0.8333 | 1.0000 | 241503 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19891.0 | 0.048 | 0.056 | 0.085 | 1.0000 | 1.0000 | 1.0000 | 75635 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19619.0 | 0.049 | 0.054 | 0.065 | 0.6667 | 1.0000 | 0.7767 | 79014 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19781.5 | 0.047 | 0.052 | 0.152 | 1.0000 | 1.0000 | 1.0000 | 258523 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18769.9 | 0.051 | 0.055 | 0.118 | 0.6667 | 1.0000 | 0.7751 | 259812 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19420.7 | 0.047 | 0.053 | 0.152 | 1.0000 | 1.0000 | 1.0000 | 350641 | 518.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19349.2 | 0.048 | 0.056 | 0.165 | 0.6667 | 1.0000 | 0.7654 | 350605 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24359.6 | 0.039 | 0.048 | 0.058 | 0.5556 | 0.6667 | 1.0000 | 70079 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19524.6 | 0.049 | 0.056 | 0.071 | 0.5556 | 0.8333 | 1.0000 | 96090 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5469.8 | 0.177 | 0.194 | 0.353 | 1.0000 | 1.0000 | 1.0000 | 183455 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5432.3 | 0.179 | 0.194 | 0.268 | 0.6667 | 1.0000 | 0.7767 | 187862 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5300.9 | 0.181 | 0.201 | 0.312 | 1.0000 | 1.0000 | 1.0000 | 370171 | 2020.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5121.9 | 0.185 | 0.216 | 0.503 | 0.6667 | 1.0000 | 0.7751 | 372883 | 2053.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5400.3 | 0.170 | 0.190 | 0.643 | 1.0000 | 1.0000 | 1.0000 | 450798 | 1908.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 6583.0 | 0.127 | 0.198 | 0.603 | 0.6667 | 1.0000 | 0.7654 | 409601 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5665.1 | 0.171 | 0.190 | 0.249 | 0.5556 | 0.8889 | 1.0000 | 168719 | 1862.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5492.9 | 0.177 | 0.195 | 0.217 | 0.5556 | 0.9444 | 1.0000 | 194044 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51796.9 | 0.018 | 0.023 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 37620 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44916.6 | 0.020 | 0.027 | 0.038 | 0.6667 | 1.0000 | 0.7767 | 42336 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 39368.3 | 0.022 | 0.033 | 0.087 | 1.0000 | 1.0000 | 1.0000 | 228106 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 35291.8 | 0.024 | 0.034 | 0.101 | 0.6667 | 1.0000 | 0.7751 | 230782 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32494.0 | 0.026 | 0.043 | 0.106 | 1.0000 | 1.0000 | 1.0000 | 324113 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 32695.5 | 0.027 | 0.039 | 0.098 | 0.6667 | 1.0000 | 0.7654 | 324062 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 67617.7 | 0.013 | 0.017 | 0.021 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 47071.9 | 0.020 | 0.025 | 0.039 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 12977.8 | 0.256 | 0.552 | 0.880 | 1.0000 | 1.0000 | 1.0000 | 253520 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13235.2 | 0.253 | 0.594 | 0.915 | 0.6667 | 1.0000 | 0.7767 | 259267 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 11948.2 | 0.279 | 0.534 | 0.937 | 1.0000 | 1.0000 | 1.0000 | 426360 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 12291.2 | 0.254 | 0.424 | 0.755 | 0.6667 | 1.0000 | 0.7751 | 429485 | 2323.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12221.7 | 0.264 | 0.479 | 0.813 | 1.0000 | 1.0000 | 1.0000 | 497411 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 13174.2 | 0.236 | 0.524 | 1.051 | 0.6667 | 1.0000 | 0.7654 | 455889 | 1634.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 12490.0 | 0.259 | 0.549 | 0.998 | 0.5556 | 0.8333 | 1.0000 | 277903 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13652.1 | 0.249 | 0.484 | 0.889 | 0.5556 | 1.0000 | 1.0000 | 286772 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 50403.4 | 0.067 | 0.126 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 107981 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45831.0 | 0.073 | 0.152 | 0.207 | 0.6667 | 1.0000 | 0.7767 | 113788 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 27214.2 | 0.107 | 0.254 | 0.543 | 1.0000 | 1.0000 | 1.0000 | 284579 | 533.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 28290.1 | 0.108 | 0.298 | 0.814 | 0.6667 | 1.0000 | 0.7751 | 287693 | 566.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 23473.5 | 0.131 | 0.280 | 0.721 | 1.0000 | 1.0000 | 1.0000 | 369735 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 22217.9 | 0.135 | 0.269 | 0.659 | 0.6667 | 1.0000 | 0.7654 | 369698 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43841.7 | 0.078 | 0.161 | 0.295 | 0.5556 | 0.8333 | 1.0000 | 133098 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41677.7 | 0.078 | 0.157 | 0.365 | 0.5556 | 1.0000 | 1.0000 | 142134 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 14497.2 | 0.218 | 0.541 | 0.781 | 1.0000 | 1.0000 | 1.0000 | 222004 | 2216.8 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15692.3 | 0.215 | 0.504 | 0.684 | 0.6667 | 1.0000 | 0.7767 | 222147 | 2252.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14528.1 | 0.220 | 0.557 | 0.833 | 1.0000 | 1.0000 | 1.0000 | 401722 | 2244.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 15161.7 | 0.209 | 0.549 | 0.864 | 0.6667 | 1.0000 | 0.7751 | 385606 | 2032.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 17073.8 | 0.191 | 0.493 | 0.726 | 1.0000 | 1.0000 | 1.0000 | 449436 | 1759.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19900.8 | 0.162 | 0.425 | 0.605 | 0.6667 | 1.0000 | 0.7654 | 416691 | 1357.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16571.0 | 0.205 | 0.448 | 0.633 | 0.5556 | 0.6667 | 1.0000 | 216414 | 2065.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15446.7 | 0.216 | 0.497 | 0.672 | 0.5556 | 0.8333 | 1.0000 | 241791 | 2169.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63089.6 | 0.047 | 0.110 | 0.177 | 1.0000 | 1.0000 | 1.0000 | 75814 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 61690.6 | 0.048 | 0.107 | 0.267 | 0.6667 | 1.0000 | 0.7767 | 79185 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 42691.7 | 0.076 | 0.219 | 0.400 | 1.0000 | 1.0000 | 1.0000 | 258728 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41754.4 | 0.075 | 0.221 | 0.388 | 0.6667 | 1.0000 | 0.7751 | 260023 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31908.1 | 0.108 | 0.270 | 0.395 | 1.0000 | 1.0000 | 1.0000 | 350935 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29214.4 | 0.108 | 0.338 | 0.551 | 0.6667 | 1.0000 | 0.7654 | 350946 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 74672.5 | 0.039 | 0.093 | 0.186 | 0.5556 | 0.6667 | 1.0000 | 70228 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 52820.7 | 0.049 | 0.148 | 0.251 | 0.5556 | 0.8333 | 1.0000 | 96284 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18118.1 | 0.186 | 0.426 | 0.659 | 1.0000 | 1.0000 | 1.0000 | 183538 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17490.2 | 0.194 | 0.377 | 0.627 | 0.6667 | 1.0000 | 0.7767 | 187914 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 14811.4 | 0.204 | 0.559 | 1.046 | 1.0000 | 1.0000 | 1.0000 | 370267 | 2020.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 14489.8 | 0.209 | 0.515 | 1.639 | 0.6667 | 1.0000 | 0.7751 | 372945 | 2053.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12469.8 | 0.216 | 0.650 | 2.046 | 1.0000 | 1.0000 | 1.0000 | 450895 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 16553.7 | 0.178 | 0.323 | 0.717 | 0.6667 | 1.0000 | 0.7654 | 409656 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18604.2 | 0.180 | 0.415 | 0.629 | 0.5556 | 0.8889 | 1.0000 | 168740 | 1863.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 17564.6 | 0.188 | 0.459 | 0.643 | 0.5556 | 0.9444 | 1.0000 | 194067 | 1966.8 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 124158.8 | 0.020 | 0.058 | 0.110 | 1.0000 | 1.0000 | 1.0000 | 37675 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 126085.8 | 0.020 | 0.052 | 0.117 | 0.6667 | 1.0000 | 0.7767 | 42391 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43553.4 | 0.040 | 0.460 | 0.832 | 1.0000 | 1.0000 | 1.0000 | 228263 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42955.7 | 0.040 | 0.379 | 1.060 | 0.6667 | 1.0000 | 0.7751 | 230931 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31707.6 | 0.111 | 0.234 | 0.463 | 1.0000 | 1.0000 | 1.0000 | 324248 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31160.8 | 0.114 | 0.247 | 0.464 | 0.6667 | 1.0000 | 0.7654 | 324219 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 205042.4 | 0.012 | 0.025 | 0.048 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 139114.2 | 0.019 | 0.038 | 0.088 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1440.6 | 0.675 | 0.752 | 1.271 | 1.0000 | 1.0000 | 1.0000 | 558190 | 5372.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1410.7 | 0.684 | 0.774 | 1.328 | 0.6667 | 1.0000 | 0.7767 | 564003 | 5427.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1402.2 | 0.684 | 0.810 | 1.405 | 1.0000 | 1.0000 | 1.0000 | 732650 | 5410.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1404.4 | 0.684 | 0.775 | 1.393 | 0.6667 | 1.0000 | 0.7751 | 734517 | 5394.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1452.1 | 0.656 | 0.750 | 1.567 | 1.0000 | 1.0000 | 1.0000 | 791342 | 4981.0 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1613.1 | 0.581 | 0.682 | 1.646 | 0.6667 | 1.0000 | 0.7654 | 715442 | 3844.3 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1391.0 | 0.679 | 0.811 | 1.708 | 0.5556 | 0.8333 | 1.0000 | 583358 | 5257.0 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1417.7 | 0.675 | 0.753 | 1.225 | 0.5556 | 1.0000 | 1.0000 | 591054 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2306.4 | 0.425 | 0.461 | 0.662 | 1.0000 | 1.0000 | 1.0000 | 344611 | 4166.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2275.0 | 0.429 | 0.476 | 0.772 | 0.6667 | 1.0000 | 0.7767 | 345466 | 4205.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2267.7 | 0.428 | 0.478 | 0.841 | 1.0000 | 1.0000 | 1.0000 | 526736 | 4216.6 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2464.9 | 0.385 | 0.455 | 0.826 | 0.6667 | 1.0000 | 0.7751 | 496710 | 3769.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2782.9 | 0.328 | 0.424 | 0.759 | 1.0000 | 1.0000 | 1.0000 | 544831 | 3211.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3414.5 | 0.270 | 0.333 | 0.664 | 0.6667 | 1.0000 | 0.7654 | 491505 | 2397.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2369.9 | 0.412 | 0.455 | 0.675 | 0.5556 | 0.6667 | 1.0000 | 336633 | 4004.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2307.3 | 0.423 | 0.468 | 0.773 | 0.5556 | 0.8333 | 1.0000 | 362725 | 4110.9 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1600.5 | 0.608 | 0.676 | 0.923 | 1.0000 | 1.0000 | 1.0000 | 470782 | 4668.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1556.5 | 0.619 | 0.692 | 1.085 | 1.0000 | 1.0000 | 1.0000 | 658731 | 4738.1 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1606.3 | 0.598 | 0.657 | 1.072 | 1.0000 | 1.0000 | 1.0000 | 727148 | 4457.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 941.9 | 1.037 | 1.148 | 1.589 | 0.5556 | 0.8889 | 1.0000 | 681777 | 5148.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4124.7 | 0.794 | 2.050 | 2.928 | 1.0000 | 1.0000 | 1.0000 | 558514 | 5375.9 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4289.2 | 0.759 | 1.887 | 2.718 | 0.6667 | 1.0000 | 0.7767 | 566433 | 5430.7 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4204.3 | 0.769 | 1.982 | 2.885 | 1.0000 | 1.0000 | 1.0000 | 732737 | 5414.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4029.4 | 0.790 | 2.087 | 3.450 | 0.6667 | 1.0000 | 0.7751 | 734612 | 5397.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4315.6 | 0.771 | 1.861 | 2.493 | 1.0000 | 1.0000 | 1.0000 | 791668 | 4984.7 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5259.8 | 0.653 | 1.531 | 2.234 | 0.6667 | 1.0000 | 0.7654 | 716246 | 3848.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4591.5 | 0.747 | 1.518 | 2.280 | 0.5556 | 0.8333 | 1.0000 | 582568 | 5259.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4322.7 | 0.778 | 1.594 | 2.643 | 0.5556 | 1.0000 | 1.0000 | 591335 | 5332.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7398.3 | 0.449 | 0.957 | 1.360 | 1.0000 | 1.0000 | 1.0000 | 344767 | 4170.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7755.5 | 0.443 | 0.917 | 1.335 | 0.6667 | 1.0000 | 0.7767 | 345140 | 4208.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7334.8 | 0.463 | 0.970 | 1.260 | 1.0000 | 1.0000 | 1.0000 | 528004 | 4221.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8256.1 | 0.423 | 0.852 | 1.250 | 0.6667 | 1.0000 | 0.7751 | 497358 | 3773.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 8482.6 | 0.400 | 0.911 | 1.335 | 1.0000 | 1.0000 | 1.0000 | 545199 | 3215.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 10270.1 | 0.335 | 0.745 | 1.028 | 0.6667 | 1.0000 | 0.7654 | 491513 | 2402.5 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7854.5 | 0.440 | 0.871 | 1.163 | 0.5556 | 0.6667 | 1.0000 | 337915 | 4008.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7701.9 | 0.446 | 0.908 | 1.331 | 0.5556 | 0.8333 | 1.0000 | 363445 | 4115.0 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5069.1 | 0.661 | 1.408 | 2.075 | 1.0000 | 1.0000 | 1.0000 | 470178 | 4670.9 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4799.9 | 0.696 | 1.607 | 2.377 | 1.0000 | 1.0000 | 1.0000 | 659773 | 4742.2 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5023.7 | 0.651 | 1.535 | 2.234 | 1.0000 | 1.0000 | 1.0000 | 727227 | 4460.6 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3008.3 | 1.177 | 2.043 | 2.407 | 0.5556 | 0.8889 | 1.0000 | 683741 | 5150.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 571.3 | 1.694 | 2.032 | 2.477 | 1.0000 | 1.0000 | 1.0000 | 832961 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 570.3 | 1.691 | 1.983 | 2.371 | 0.6667 | 1.0000 | 0.7767 | 829753 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 564.4 | 1.695 | 2.108 | 2.457 | 1.0000 | 1.0000 | 1.0000 | 1002387 | 9243.3 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 465.6 | 1.734 | 3.912 | 8.698 | 0.6667 | 1.0000 | 0.7751 | 1005401 | 9275.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 621.3 | 1.552 | 1.871 | 2.324 | 1.0000 | 1.0000 | 1.0000 | 1029870 | 8402.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 897.7 | 1.060 | 1.282 | 1.703 | 0.6667 | 1.0000 | 0.7654 | 816757 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 570.1 | 1.699 | 2.029 | 2.543 | 0.5556 | 0.7222 | 1.0000 | 854630 | 9120.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 567.7 | 1.704 | 2.016 | 2.455 | 0.5556 | 0.8333 | 1.0000 | 858705 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18317.1 | 0.053 | 0.061 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 109119 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17441.6 | 0.056 | 0.064 | 0.079 | 0.6667 | 1.0000 | 0.7767 | 114898 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16358.0 | 0.054 | 0.073 | 0.253 | 1.0000 | 1.0000 | 1.0000 | 285747 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15284.0 | 0.055 | 0.070 | 0.264 | 0.6667 | 1.0000 | 0.7751 | 288794 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15313.0 | 0.053 | 0.092 | 0.257 | 1.0000 | 1.0000 | 1.0000 | 370973 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15788.0 | 0.054 | 0.116 | 0.241 | 0.6667 | 1.0000 | 0.7654 | 370889 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17609.4 | 0.054 | 0.067 | 0.091 | 0.5556 | 0.7222 | 1.0000 | 134378 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16478.6 | 0.057 | 0.078 | 0.103 | 0.5556 | 0.8333 | 1.0000 | 143849 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 576.5 | 1.686 | 2.063 | 2.351 | 1.0000 | 1.0000 | 1.0000 | 797785 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 583.0 | 1.673 | 1.963 | 2.324 | 0.6667 | 1.0000 | 0.7767 | 792263 | 9202.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 570.8 | 1.699 | 2.095 | 2.365 | 1.0000 | 1.0000 | 1.0000 | 973531 | 9197.3 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 669.9 | 1.390 | 1.779 | 2.069 | 0.6667 | 1.0000 | 0.7751 | 900687 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 812.9 | 1.060 | 1.606 | 1.883 | 1.0000 | 1.0000 | 1.0000 | 867096 | 6623.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1206.9 | 0.716 | 1.077 | 1.405 | 0.6667 | 1.0000 | 0.7654 | 679806 | 4600.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 577.9 | 1.682 | 2.044 | 2.306 | 0.5556 | 0.6667 | 1.0000 | 790415 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 553.3 | 1.712 | 2.224 | 2.942 | 0.5556 | 0.8333 | 1.0000 | 809237 | 9116.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19960.4 | 0.048 | 0.054 | 0.060 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19409.7 | 0.050 | 0.056 | 0.079 | 0.6667 | 1.0000 | 0.7767 | 79012 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 18617.0 | 0.050 | 0.055 | 0.145 | 1.0000 | 1.0000 | 1.0000 | 258498 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18290.2 | 0.050 | 0.058 | 0.168 | 0.6667 | 1.0000 | 0.7751 | 259790 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 18817.3 | 0.048 | 0.054 | 0.164 | 1.0000 | 1.0000 | 1.0000 | 350636 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18768.6 | 0.048 | 0.058 | 0.165 | 0.6667 | 1.0000 | 0.7654 | 350613 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24396.6 | 0.039 | 0.047 | 0.054 | 0.5556 | 0.6667 | 1.0000 | 70047 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19340.6 | 0.049 | 0.056 | 0.076 | 0.5556 | 0.8333 | 1.0000 | 96082 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 592.6 | 1.648 | 1.923 | 2.214 | 1.0000 | 1.0000 | 1.0000 | 761090 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 582.5 | 1.666 | 1.971 | 2.463 | 0.6667 | 1.0000 | 0.7767 | 762124 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 583.6 | 1.660 | 1.992 | 2.515 | 1.0000 | 1.0000 | 1.0000 | 946079 | 8972.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 586.5 | 1.656 | 1.958 | 2.333 | 0.6667 | 1.0000 | 0.7751 | 950537 | 9005.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 646.0 | 1.499 | 1.800 | 2.101 | 1.0000 | 1.0000 | 1.0000 | 982562 | 8162.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 942.2 | 1.022 | 1.237 | 1.605 | 0.6667 | 1.0000 | 0.7654 | 771229 | 5564.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 586.8 | 1.657 | 1.996 | 2.349 | 0.5556 | 0.6667 | 1.0000 | 744146 | 8810.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 584.6 | 1.666 | 2.018 | 2.321 | 0.5556 | 0.8333 | 1.0000 | 769441 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 45593.0 | 0.020 | 0.027 | 0.033 | 1.0000 | 1.0000 | 1.0000 | 38899 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43093.6 | 0.021 | 0.028 | 0.036 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 36115.0 | 0.024 | 0.037 | 0.100 | 1.0000 | 1.0000 | 1.0000 | 229418 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 33366.0 | 0.025 | 0.036 | 0.139 | 0.6667 | 1.0000 | 0.7751 | 232108 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31077.9 | 0.028 | 0.043 | 0.130 | 1.0000 | 1.0000 | 1.0000 | 325369 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29181.8 | 0.029 | 0.041 | 0.134 | 0.6667 | 1.0000 | 0.7654 | 325320 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 61559.3 | 0.015 | 0.017 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38533.0 | 0.023 | 0.039 | 0.057 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2018.1 | 1.775 | 2.799 | 3.846 | 1.0000 | 1.0000 | 1.0000 | 833029 | 9221.9 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2003.7 | 1.772 | 2.735 | 5.231 | 0.6667 | 1.0000 | 0.7767 | 829852 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1912.9 | 1.780 | 3.314 | 6.746 | 1.0000 | 1.0000 | 1.0000 | 1002434 | 9243.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1739.5 | 1.821 | 3.935 | 8.272 | 0.6667 | 1.0000 | 0.7751 | 1005490 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2114.1 | 1.637 | 2.706 | 4.676 | 1.0000 | 1.0000 | 1.0000 | 1029995 | 8402.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3006.3 | 1.111 | 1.968 | 4.766 | 0.6667 | 1.0000 | 0.7654 | 816830 | 5804.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2014.3 | 1.784 | 2.728 | 4.547 | 0.5556 | 0.7222 | 1.0000 | 854721 | 9120.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1960.7 | 1.803 | 2.913 | 5.287 | 0.5556 | 0.8333 | 1.0000 | 858806 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47573.7 | 0.071 | 0.142 | 0.178 | 1.0000 | 1.0000 | 1.0000 | 109275 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46723.4 | 0.072 | 0.143 | 0.191 | 0.6667 | 1.0000 | 0.7767 | 115033 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 33338.8 | 0.101 | 0.233 | 0.772 | 1.0000 | 1.0000 | 1.0000 | 285870 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 32667.3 | 0.102 | 0.212 | 0.616 | 0.6667 | 1.0000 | 0.7751 | 288956 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 23677.5 | 0.133 | 0.269 | 0.632 | 1.0000 | 1.0000 | 1.0000 | 371035 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 23553.1 | 0.132 | 0.253 | 0.618 | 0.6667 | 1.0000 | 0.7654 | 370958 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 45411.8 | 0.072 | 0.137 | 0.214 | 0.5556 | 0.7222 | 1.0000 | 134507 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 44995.6 | 0.078 | 0.147 | 0.183 | 0.5556 | 0.8333 | 1.0000 | 143991 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2134.6 | 1.711 | 2.643 | 3.048 | 1.0000 | 1.0000 | 1.0000 | 798273 | 9165.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2113.3 | 1.716 | 2.792 | 3.432 | 0.6667 | 1.0000 | 0.7767 | 792630 | 9202.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2104.4 | 1.720 | 2.724 | 3.392 | 1.0000 | 1.0000 | 1.0000 | 974002 | 9198.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2366.7 | 1.535 | 2.600 | 3.246 | 0.6667 | 1.0000 | 0.7751 | 901130 | 8059.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2922.8 | 1.118 | 2.131 | 2.916 | 1.0000 | 1.0000 | 1.0000 | 867519 | 6624.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4148.5 | 0.770 | 1.685 | 2.294 | 0.6667 | 1.0000 | 0.7654 | 680127 | 4600.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2103.1 | 1.707 | 2.806 | 3.745 | 0.5556 | 0.6667 | 1.0000 | 791721 | 9014.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2100.6 | 1.723 | 2.728 | 3.335 | 0.5556 | 0.8333 | 1.0000 | 809702 | 9116.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 56011.9 | 0.048 | 0.121 | 0.205 | 1.0000 | 1.0000 | 1.0000 | 75804 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60088.0 | 0.049 | 0.118 | 0.189 | 0.6667 | 1.0000 | 0.7767 | 79176 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 39649.0 | 0.078 | 0.216 | 0.485 | 1.0000 | 1.0000 | 1.0000 | 258701 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 39881.9 | 0.078 | 0.214 | 0.567 | 0.6667 | 1.0000 | 0.7751 | 260003 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31281.1 | 0.109 | 0.230 | 0.688 | 1.0000 | 1.0000 | 1.0000 | 350844 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29901.8 | 0.110 | 0.254 | 0.718 | 0.6667 | 1.0000 | 0.7654 | 350870 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 72339.6 | 0.039 | 0.106 | 0.202 | 0.5556 | 0.6667 | 1.0000 | 70198 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 52810.1 | 0.050 | 0.151 | 0.276 | 0.5556 | 0.8333 | 1.0000 | 96255 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2122.3 | 1.695 | 2.744 | 3.464 | 1.0000 | 1.0000 | 1.0000 | 761356 | 8919.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1929.3 | 1.722 | 3.932 | 5.324 | 0.6667 | 1.0000 | 0.7767 | 762238 | 8969.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2125.9 | 1.698 | 2.733 | 4.013 | 1.0000 | 1.0000 | 1.0000 | 946280 | 8972.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2080.4 | 1.704 | 2.832 | 4.641 | 0.6667 | 1.0000 | 0.7751 | 950653 | 9005.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2305.0 | 1.537 | 2.721 | 3.874 | 1.0000 | 1.0000 | 1.0000 | 982723 | 8162.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3289.8 | 1.049 | 1.896 | 2.925 | 0.6667 | 1.0000 | 0.7654 | 771243 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2150.7 | 1.693 | 2.702 | 3.347 | 0.5556 | 0.6667 | 1.0000 | 744183 | 8810.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2167.6 | 1.691 | 2.638 | 3.259 | 0.5556 | 0.8333 | 1.0000 | 769486 | 8914.2 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 134675.9 | 0.019 | 0.050 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 38913 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 123105.7 | 0.021 | 0.052 | 0.070 | 0.6667 | 1.0000 | 0.7767 | 43640 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43066.6 | 0.039 | 0.471 | 1.112 | 1.0000 | 1.0000 | 1.0000 | 229500 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42359.2 | 0.041 | 0.414 | 1.440 | 0.6667 | 1.0000 | 0.7751 | 232148 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 28252.5 | 0.117 | 0.276 | 0.655 | 1.0000 | 1.0000 | 1.0000 | 325505 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29171.9 | 0.120 | 0.231 | 0.707 | 0.6667 | 1.0000 | 0.7654 | 325441 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 184024.9 | 0.015 | 0.025 | 0.038 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 104511.9 | 0.024 | 0.070 | 0.112 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 407.4 | 2.363 | 2.816 | 3.340 | 1.0000 | 1.0000 | 1.0000 | 1161595 | 12331.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 396.3 | 2.404 | 2.936 | 3.970 | 0.6667 | 1.0000 | 0.7767 | 1158943 | 12385.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 401.9 | 2.402 | 2.851 | 3.596 | 1.0000 | 1.0000 | 1.0000 | 1333013 | 12377.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 395.1 | 2.439 | 2.904 | 3.554 | 0.6667 | 1.0000 | 0.7751 | 1334127 | 12358.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 434.4 | 2.200 | 2.711 | 3.552 | 1.0000 | 1.0000 | 1.0000 | 1346789 | 11246.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 562.3 | 1.667 | 2.126 | 4.183 | 0.6667 | 1.0000 | 0.7654 | 1099348 | 8025.0 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 407.4 | 2.377 | 2.831 | 3.351 | 0.5556 | 0.7222 | 1.0000 | 1183356 | 12221.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 410.7 | 2.349 | 2.766 | 3.109 | 0.5556 | 0.8333 | 1.0000 | 1187782 | 12302.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 488.2 | 1.938 | 2.367 | 2.777 | 1.0000 | 1.0000 | 1.0000 | 920967 | 11114.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 491.6 | 1.963 | 2.410 | 2.720 | 0.6667 | 1.0000 | 0.7767 | 916705 | 11155.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 497.7 | 1.945 | 2.417 | 2.720 | 1.0000 | 1.0000 | 1.0000 | 1098876 | 11169.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 562.6 | 1.688 | 2.199 | 2.535 | 0.6667 | 1.0000 | 0.7751 | 1012792 | 9796.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 684.2 | 1.299 | 1.940 | 2.223 | 1.0000 | 1.0000 | 1.0000 | 962991 | 8076.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 959.0 | 0.935 | 1.371 | 1.661 | 0.6667 | 1.0000 | 0.7654 | 754561 | 5640.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 479.6 | 1.970 | 2.530 | 4.640 | 0.5556 | 0.6667 | 1.0000 | 912322 | 10952.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 496.1 | 1.949 | 2.385 | 2.655 | 0.5556 | 0.8333 | 1.0000 | 931496 | 11058.5 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 421.3 | 2.275 | 2.864 | 3.271 | 1.0000 | 1.0000 | 1.0000 | 1072758 | 11626.7 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 420.9 | 2.264 | 2.838 | 4.145 | 1.0000 | 1.0000 | 1.0000 | 1260160 | 11701.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 455.6 | 2.097 | 2.630 | 3.114 | 1.0000 | 1.0000 | 1.0000 | 1283638 | 10724.3 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 356.1 | 2.723 | 3.320 | 3.592 | 0.5556 | 0.6667 | 1.0000 | 1288015 | 12107.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1452.4 | 2.463 | 3.724 | 7.273 | 1.0000 | 1.0000 | 1.0000 | 1162149 | 12333.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1466.1 | 2.456 | 4.065 | 5.809 | 0.6667 | 1.0000 | 0.7767 | 1159508 | 12388.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1439.8 | 2.458 | 4.308 | 6.567 | 1.0000 | 1.0000 | 1.0000 | 1332782 | 12379.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1398.3 | 2.502 | 4.588 | 7.297 | 0.6667 | 1.0000 | 0.7751 | 1334244 | 12361.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1498.1 | 2.335 | 4.275 | 6.132 | 1.0000 | 1.0000 | 1.0000 | 1346940 | 11248.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2002.1 | 1.704 | 3.455 | 5.023 | 0.6667 | 1.0000 | 0.7654 | 1099730 | 8027.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1428.7 | 2.517 | 4.025 | 6.493 | 0.5556 | 0.7222 | 1.0000 | 1183250 | 12223.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1465.8 | 2.444 | 3.977 | 5.811 | 0.5556 | 0.8333 | 1.0000 | 1187248 | 12304.4 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1815.2 | 1.996 | 3.170 | 3.814 | 1.0000 | 1.0000 | 1.0000 | 922546 | 11116.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1774.5 | 2.055 | 3.156 | 3.816 | 0.6667 | 1.0000 | 0.7767 | 917037 | 11157.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1776.7 | 2.053 | 3.161 | 3.845 | 1.0000 | 1.0000 | 1.0000 | 1100136 | 11172.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1935.5 | 1.921 | 3.220 | 4.148 | 0.6667 | 1.0000 | 0.7751 | 1013993 | 9798.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2472.0 | 1.452 | 2.481 | 3.090 | 1.0000 | 1.0000 | 1.0000 | 962779 | 8078.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3393.3 | 1.053 | 1.933 | 2.628 | 0.6667 | 1.0000 | 0.7654 | 754967 | 5642.3 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1565.5 | 2.321 | 4.091 | 5.197 | 0.5556 | 0.6667 | 1.0000 | 912104 | 10953.8 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1843.5 | 1.979 | 2.989 | 3.673 | 0.5556 | 0.8333 | 1.0000 | 932104 | 11060.7 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1559.1 | 2.317 | 3.816 | 4.824 | 1.0000 | 1.0000 | 1.0000 | 1072085 | 11628.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1502.9 | 2.379 | 4.037 | 5.539 | 1.0000 | 1.0000 | 1.0000 | 1258922 | 11703.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1563.2 | 2.262 | 3.903 | 5.750 | 1.0000 | 1.0000 | 1.0000 | 1283018 | 10726.3 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1275.2 | 2.896 | 4.274 | 5.181 | 0.5556 | 0.6667 | 1.0000 | 1289642 | 12108.7 |

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
