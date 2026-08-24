# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `3b3235ea1e83eb75d589b5379b05888b739b6b08`
- harness revision: `3b3235ea1e83eb75d589b5379b05888b739b6b08`
- binary SHA-256: `e3a6defec36c0e391d8fda125676fa135ff916a5ab999681a144f7e715a4688e`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_3b3235ea1 -out-dir /tmp/gomap-rag-m3-final-a -dir /tmp/gomap-rag-m3-final-a-db -product-base-sha 3b3235ea1e83eb75d589b5379b05888b739b6b08 -harness-revision 3b3235ea1e83eb75d589b5379b05888b739b6b08 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 final A`

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
| 0 | 18 | 54 | 0.065188 | 276.12 | 828.37 | 2327716 | 4810 | 3748790 | true |
| 1 | 18 | 54 | 0.041990 | 428.67 | 1286.02 | 1842685 | 4682 | 3748827 | true |
| 2 | 18 | 54 | 0.037876 | 475.23 | 1425.70 | 1846929 | 4677 | 3748827 | true |
| 3 | 18 | 54 | 0.038221 | 470.94 | 1412.83 | 1856038 | 4679 | 3748827 | true |
| 4 | 18 | 54 | 0.040426 | 445.26 | 1335.78 | 1862734 | 4676 | 3748827 | true |

Median/p95 docs/s: **445.26 / 474.37**. Median/p95 B/source: **1856038 / 2234719**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **512.05**, B/source <= **1670434**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4624.7 | 0.208 | 0.221 | 0.261 | 1.0000 | 1.0000 | 1.0000 | 253384 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4446.9 | 0.216 | 0.237 | 0.373 | 0.6667 | 1.0000 | 0.7767 | 259124 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4085.7 | 0.216 | 0.334 | 0.790 | 1.0000 | 1.0000 | 1.0000 | 426258 | 2290.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4450.2 | 0.215 | 0.249 | 0.371 | 0.6667 | 1.0000 | 0.7751 | 429375 | 2323.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4553.0 | 0.200 | 0.299 | 0.378 | 1.0000 | 1.0000 | 1.0000 | 497334 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5934.0 | 0.152 | 0.184 | 0.599 | 0.6667 | 1.0000 | 0.7654 | 455811 | 1634.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4544.2 | 0.210 | 0.236 | 0.376 | 0.5556 | 0.8333 | 1.0000 | 277771 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4353.1 | 0.215 | 0.309 | 0.453 | 0.5556 | 1.0000 | 1.0000 | 286635 | 2237.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18393.1 | 0.053 | 0.060 | 0.068 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17771.2 | 0.055 | 0.061 | 0.070 | 0.6667 | 1.0000 | 0.7767 | 113642 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16042.2 | 0.054 | 0.105 | 0.229 | 1.0000 | 1.0000 | 1.0000 | 284461 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15863.5 | 0.054 | 0.110 | 0.267 | 0.6667 | 1.0000 | 0.7751 | 287568 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 16404.4 | 0.052 | 0.106 | 0.261 | 1.0000 | 1.0000 | 1.0000 | 369659 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15527.7 | 0.053 | 0.108 | 0.234 | 0.6667 | 1.0000 | 0.7654 | 369617 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17119.5 | 0.054 | 0.064 | 0.160 | 0.5556 | 0.8333 | 1.0000 | 132988 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16312.1 | 0.056 | 0.068 | 0.286 | 0.5556 | 1.0000 | 1.0000 | 142006 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4817.2 | 0.202 | 0.221 | 0.252 | 1.0000 | 1.0000 | 1.0000 | 221770 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4760.6 | 0.204 | 0.226 | 0.325 | 0.6667 | 1.0000 | 0.7767 | 221893 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4754.0 | 0.202 | 0.224 | 0.449 | 1.0000 | 1.0000 | 1.0000 | 401391 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5189.1 | 0.176 | 0.217 | 0.447 | 0.6667 | 1.0000 | 0.7751 | 385285 | 2032.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6114.4 | 0.143 | 0.195 | 0.412 | 1.0000 | 1.0000 | 1.0000 | 449041 | 1758.8 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7732.3 | 0.113 | 0.151 | 0.339 | 0.6667 | 1.0000 | 0.7654 | 416387 | 1357.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5058.0 | 0.192 | 0.209 | 0.316 | 0.5556 | 0.6667 | 1.0000 | 215842 | 2065.1 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4831.3 | 0.202 | 0.218 | 0.294 | 0.5556 | 0.8333 | 1.0000 | 241503 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20072.5 | 0.048 | 0.056 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 75635 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19131.0 | 0.050 | 0.057 | 0.107 | 0.6667 | 1.0000 | 0.7767 | 79027 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19519.1 | 0.048 | 0.054 | 0.154 | 1.0000 | 1.0000 | 1.0000 | 258543 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18830.0 | 0.050 | 0.054 | 0.146 | 0.6667 | 1.0000 | 0.7751 | 259807 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19785.9 | 0.047 | 0.053 | 0.152 | 1.0000 | 1.0000 | 1.0000 | 350633 | 518.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19469.8 | 0.048 | 0.054 | 0.149 | 0.6667 | 1.0000 | 0.7654 | 350612 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24244.1 | 0.039 | 0.048 | 0.067 | 0.5556 | 0.6667 | 1.0000 | 70072 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19566.9 | 0.049 | 0.055 | 0.096 | 0.5556 | 0.8333 | 1.0000 | 96104 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5605.3 | 0.174 | 0.187 | 0.212 | 1.0000 | 1.0000 | 1.0000 | 183432 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5438.1 | 0.178 | 0.196 | 0.285 | 0.6667 | 1.0000 | 0.7767 | 187879 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5281.3 | 0.180 | 0.201 | 0.374 | 1.0000 | 1.0000 | 1.0000 | 370174 | 2020.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5168.0 | 0.183 | 0.206 | 0.320 | 0.6667 | 1.0000 | 0.7751 | 372890 | 2053.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5562.0 | 0.169 | 0.193 | 0.376 | 1.0000 | 1.0000 | 1.0000 | 450791 | 1908.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7159.8 | 0.125 | 0.149 | 0.440 | 0.6667 | 1.0000 | 0.7654 | 409601 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5770.8 | 0.169 | 0.181 | 0.219 | 0.5556 | 0.8889 | 1.0000 | 168719 | 1862.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5562.3 | 0.175 | 0.189 | 0.221 | 0.5556 | 0.9444 | 1.0000 | 194045 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51365.3 | 0.018 | 0.022 | 0.033 | 1.0000 | 1.0000 | 1.0000 | 37627 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45113.2 | 0.020 | 0.026 | 0.035 | 0.6667 | 1.0000 | 0.7767 | 42323 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 39338.7 | 0.022 | 0.033 | 0.082 | 1.0000 | 1.0000 | 1.0000 | 228133 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 36535.3 | 0.024 | 0.033 | 0.087 | 0.6667 | 1.0000 | 0.7751 | 230816 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32369.3 | 0.026 | 0.035 | 0.133 | 1.0000 | 1.0000 | 1.0000 | 324114 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31451.2 | 0.027 | 0.039 | 0.144 | 0.6667 | 1.0000 | 0.7654 | 324050 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 65914.4 | 0.013 | 0.019 | 0.025 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46520.3 | 0.020 | 0.023 | 0.033 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 13127.8 | 0.253 | 0.490 | 0.691 | 1.0000 | 1.0000 | 1.0000 | 253550 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13425.5 | 0.251 | 0.542 | 0.882 | 0.6667 | 1.0000 | 0.7767 | 259248 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 11908.0 | 0.272 | 0.572 | 0.969 | 1.0000 | 1.0000 | 1.0000 | 426352 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 12485.7 | 0.249 | 0.469 | 0.809 | 0.6667 | 1.0000 | 0.7751 | 429500 | 2323.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 11531.7 | 0.267 | 0.599 | 0.982 | 1.0000 | 1.0000 | 1.0000 | 497439 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14086.7 | 0.227 | 0.432 | 1.263 | 0.6667 | 1.0000 | 0.7654 | 455886 | 1634.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 13144.7 | 0.247 | 0.558 | 0.945 | 0.5556 | 0.8333 | 1.0000 | 277926 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13104.4 | 0.254 | 0.533 | 0.915 | 0.5556 | 1.0000 | 1.0000 | 286789 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52739.7 | 0.065 | 0.120 | 0.163 | 1.0000 | 1.0000 | 1.0000 | 107981 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 42208.0 | 0.079 | 0.162 | 0.189 | 0.6667 | 1.0000 | 0.7767 | 113775 | 561.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 30454.9 | 0.098 | 0.181 | 0.638 | 1.0000 | 1.0000 | 1.0000 | 284572 | 533.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 27856.0 | 0.107 | 0.280 | 0.698 | 0.6667 | 1.0000 | 0.7751 | 287679 | 566.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 23511.2 | 0.130 | 0.268 | 0.668 | 1.0000 | 1.0000 | 1.0000 | 369742 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 24791.1 | 0.129 | 0.226 | 0.554 | 0.6667 | 1.0000 | 0.7654 | 369712 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 41737.0 | 0.075 | 0.252 | 0.308 | 0.5556 | 0.8333 | 1.0000 | 133105 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39715.7 | 0.080 | 0.281 | 0.386 | 0.5556 | 1.0000 | 1.0000 | 142134 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15999.5 | 0.211 | 0.477 | 0.692 | 1.0000 | 1.0000 | 1.0000 | 221944 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15303.8 | 0.217 | 0.490 | 0.741 | 0.6667 | 1.0000 | 0.7767 | 222130 | 2252.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14632.4 | 0.219 | 0.550 | 0.787 | 1.0000 | 1.0000 | 1.0000 | 401645 | 2244.8 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 15584.6 | 0.208 | 0.514 | 0.835 | 0.6667 | 1.0000 | 0.7751 | 385622 | 2032.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 16716.1 | 0.191 | 0.524 | 0.840 | 1.0000 | 1.0000 | 1.0000 | 449303 | 1759.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20195.9 | 0.159 | 0.412 | 0.621 | 0.6667 | 1.0000 | 0.7654 | 416649 | 1357.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16554.5 | 0.203 | 0.454 | 0.638 | 0.5556 | 0.6667 | 1.0000 | 216326 | 2065.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15196.8 | 0.214 | 0.513 | 0.748 | 0.5556 | 0.8333 | 1.0000 | 241767 | 2169.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63326.4 | 0.046 | 0.100 | 0.218 | 1.0000 | 1.0000 | 1.0000 | 75820 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60879.9 | 0.049 | 0.125 | 0.198 | 0.6667 | 1.0000 | 0.7767 | 79179 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 42883.4 | 0.074 | 0.212 | 0.361 | 1.0000 | 1.0000 | 1.0000 | 258780 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41510.5 | 0.077 | 0.220 | 0.360 | 0.6667 | 1.0000 | 0.7751 | 260064 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32686.4 | 0.105 | 0.267 | 0.399 | 1.0000 | 1.0000 | 1.0000 | 350892 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31397.8 | 0.108 | 0.270 | 0.450 | 0.6667 | 1.0000 | 0.7654 | 350933 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 76304.7 | 0.039 | 0.089 | 0.202 | 0.5556 | 0.6667 | 1.0000 | 70215 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 61637.5 | 0.048 | 0.122 | 0.227 | 0.5556 | 0.8333 | 1.0000 | 96284 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18543.7 | 0.183 | 0.405 | 0.615 | 1.0000 | 1.0000 | 1.0000 | 183542 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17777.1 | 0.188 | 0.419 | 0.631 | 0.6667 | 1.0000 | 0.7767 | 187968 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 13946.6 | 0.213 | 0.577 | 1.066 | 1.0000 | 1.0000 | 1.0000 | 370271 | 2020.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 14884.2 | 0.200 | 0.540 | 1.085 | 0.6667 | 1.0000 | 0.7751 | 372946 | 2053.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 14279.9 | 0.202 | 0.523 | 1.600 | 1.0000 | 1.0000 | 1.0000 | 450874 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18601.4 | 0.176 | 0.308 | 0.663 | 0.6667 | 1.0000 | 0.7654 | 409666 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 19188.0 | 0.178 | 0.392 | 0.548 | 0.5556 | 0.8889 | 1.0000 | 168730 | 1863.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18274.3 | 0.187 | 0.419 | 0.591 | 0.5556 | 0.9444 | 1.0000 | 194073 | 1966.8 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 126146.2 | 0.019 | 0.061 | 0.141 | 1.0000 | 1.0000 | 1.0000 | 37648 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 121839.3 | 0.020 | 0.055 | 0.114 | 0.6667 | 1.0000 | 0.7767 | 42384 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 44440.0 | 0.039 | 0.378 | 0.825 | 1.0000 | 1.0000 | 1.0000 | 228229 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 43815.0 | 0.040 | 0.320 | 1.168 | 0.6667 | 1.0000 | 0.7751 | 230978 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32137.6 | 0.111 | 0.215 | 0.476 | 1.0000 | 1.0000 | 1.0000 | 324262 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31138.9 | 0.113 | 0.215 | 0.500 | 0.6667 | 1.0000 | 0.7654 | 324224 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 195623.5 | 0.013 | 0.031 | 0.051 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 128557.1 | 0.019 | 0.061 | 0.093 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1408.8 | 0.691 | 0.743 | 1.412 | 1.0000 | 1.0000 | 1.0000 | 557582 | 5372.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1389.2 | 0.699 | 0.787 | 1.261 | 0.6667 | 1.0000 | 0.7767 | 563376 | 5427.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1390.8 | 0.696 | 0.759 | 1.299 | 1.0000 | 1.0000 | 1.0000 | 732042 | 5410.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1360.9 | 0.702 | 0.793 | 1.370 | 0.6667 | 1.0000 | 0.7751 | 733754 | 5394.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1413.6 | 0.670 | 0.749 | 1.220 | 1.0000 | 1.0000 | 1.0000 | 790976 | 4981.1 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1606.6 | 0.590 | 0.710 | 1.422 | 0.6667 | 1.0000 | 0.7654 | 715327 | 3844.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1395.4 | 0.693 | 0.756 | 1.447 | 0.5556 | 0.8333 | 1.0000 | 583117 | 5257.0 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1395.8 | 0.695 | 0.755 | 1.194 | 0.5556 | 1.0000 | 1.0000 | 590588 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2228.9 | 0.444 | 0.472 | 0.694 | 1.0000 | 1.0000 | 1.0000 | 343988 | 4166.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2199.9 | 0.450 | 0.478 | 0.726 | 0.6667 | 1.0000 | 0.7767 | 344903 | 4204.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2094.5 | 0.450 | 0.543 | 0.875 | 1.0000 | 1.0000 | 1.0000 | 527090 | 4216.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2377.3 | 0.405 | 0.468 | 0.793 | 0.6667 | 1.0000 | 0.7751 | 496960 | 3769.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2646.9 | 0.348 | 0.446 | 0.735 | 1.0000 | 1.0000 | 1.0000 | 544787 | 3211.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3339.2 | 0.280 | 0.349 | 0.669 | 0.6667 | 1.0000 | 0.7654 | 490997 | 2397.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2299.8 | 0.429 | 0.465 | 0.682 | 0.5556 | 0.6667 | 1.0000 | 336377 | 4003.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2258.3 | 0.440 | 0.472 | 0.755 | 0.5556 | 0.8333 | 1.0000 | 362316 | 4110.8 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1572.7 | 0.623 | 0.677 | 0.944 | 1.0000 | 1.0000 | 1.0000 | 470727 | 4668.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1517.2 | 0.637 | 0.705 | 1.222 | 1.0000 | 1.0000 | 1.0000 | 658088 | 4738.1 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1557.5 | 0.617 | 0.682 | 1.298 | 1.0000 | 1.0000 | 1.0000 | 726478 | 4457.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 943.2 | 1.040 | 1.149 | 1.495 | 0.5556 | 0.8889 | 1.0000 | 680377 | 5148.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4194.2 | 0.794 | 1.843 | 3.091 | 1.0000 | 1.0000 | 1.0000 | 557990 | 5375.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4469.9 | 0.746 | 1.790 | 2.709 | 0.6667 | 1.0000 | 0.7767 | 565666 | 5431.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4302.8 | 0.770 | 1.801 | 2.383 | 1.0000 | 1.0000 | 1.0000 | 732102 | 5414.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4124.2 | 0.776 | 2.026 | 3.178 | 0.6667 | 1.0000 | 0.7751 | 734418 | 5397.9 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4285.5 | 0.779 | 1.809 | 2.479 | 1.0000 | 1.0000 | 1.0000 | 791224 | 4984.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5383.5 | 0.629 | 1.487 | 2.189 | 0.6667 | 1.0000 | 0.7654 | 715604 | 3847.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4437.6 | 0.756 | 1.697 | 2.478 | 0.5556 | 0.8333 | 1.0000 | 582370 | 5259.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4389.6 | 0.765 | 1.641 | 2.503 | 0.5556 | 1.0000 | 1.0000 | 590430 | 5332.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7592.9 | 0.447 | 0.911 | 1.199 | 1.0000 | 1.0000 | 1.0000 | 345195 | 4170.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7640.4 | 0.457 | 0.865 | 1.234 | 0.6667 | 1.0000 | 0.7767 | 346095 | 4209.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7697.4 | 0.441 | 0.920 | 1.234 | 1.0000 | 1.0000 | 1.0000 | 527542 | 4219.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8228.9 | 0.426 | 0.834 | 1.140 | 0.6667 | 1.0000 | 0.7751 | 497935 | 3774.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 9012.4 | 0.391 | 0.848 | 1.123 | 1.0000 | 1.0000 | 1.0000 | 545467 | 3216.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 9963.9 | 0.337 | 0.768 | 1.021 | 0.6667 | 1.0000 | 0.7654 | 492086 | 2403.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 8344.1 | 0.418 | 0.814 | 1.095 | 0.5556 | 0.6667 | 1.0000 | 337364 | 4007.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7812.6 | 0.437 | 0.899 | 1.204 | 0.5556 | 0.8333 | 1.0000 | 363480 | 4114.5 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5326.4 | 0.633 | 1.331 | 2.066 | 1.0000 | 1.0000 | 1.0000 | 469539 | 4670.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5031.3 | 0.661 | 1.430 | 2.263 | 1.0000 | 1.0000 | 1.0000 | 658388 | 4740.6 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5146.4 | 0.643 | 1.481 | 2.122 | 1.0000 | 1.0000 | 1.0000 | 726272 | 4460.4 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2982.0 | 1.183 | 2.009 | 2.499 | 0.5556 | 0.8889 | 1.0000 | 682233 | 5150.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 573.9 | 1.689 | 1.992 | 2.433 | 1.0000 | 1.0000 | 1.0000 | 832950 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 569.2 | 1.696 | 2.008 | 2.443 | 0.6667 | 1.0000 | 0.7767 | 829730 | 9270.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 564.9 | 1.692 | 2.078 | 2.611 | 1.0000 | 1.0000 | 1.0000 | 1002315 | 9243.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 506.1 | 1.728 | 2.956 | 6.116 | 0.6667 | 1.0000 | 0.7751 | 1005380 | 9275.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 613.4 | 1.541 | 1.907 | 3.074 | 1.0000 | 1.0000 | 1.0000 | 1029900 | 8402.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 903.5 | 1.056 | 1.278 | 1.511 | 0.6667 | 1.0000 | 0.7654 | 816750 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 567.7 | 1.694 | 2.069 | 2.499 | 0.5556 | 0.7222 | 1.0000 | 854660 | 9120.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 567.3 | 1.702 | 2.061 | 2.415 | 0.5556 | 0.8333 | 1.0000 | 858745 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18079.6 | 0.054 | 0.063 | 0.073 | 1.0000 | 1.0000 | 1.0000 | 109146 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17593.5 | 0.055 | 0.064 | 0.074 | 0.6667 | 1.0000 | 0.7767 | 114897 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16492.8 | 0.054 | 0.071 | 0.251 | 1.0000 | 1.0000 | 1.0000 | 285760 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15421.4 | 0.055 | 0.073 | 0.297 | 0.6667 | 1.0000 | 0.7751 | 288807 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15576.0 | 0.053 | 0.076 | 0.112 | 1.0000 | 1.0000 | 1.0000 | 370945 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15559.0 | 0.054 | 0.112 | 0.223 | 0.6667 | 1.0000 | 0.7654 | 370875 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17685.4 | 0.054 | 0.064 | 0.082 | 0.5556 | 0.7222 | 1.0000 | 134358 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16908.9 | 0.057 | 0.068 | 0.083 | 0.5556 | 0.8333 | 1.0000 | 143849 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 575.8 | 1.682 | 2.103 | 2.374 | 1.0000 | 1.0000 | 1.0000 | 797806 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 576.8 | 1.681 | 2.087 | 2.400 | 0.6667 | 1.0000 | 0.7767 | 792197 | 9202.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 577.8 | 1.683 | 2.049 | 2.390 | 1.0000 | 1.0000 | 1.0000 | 973487 | 9197.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 666.8 | 1.395 | 1.815 | 2.133 | 0.6667 | 1.0000 | 0.7751 | 900742 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 816.5 | 1.064 | 1.609 | 1.818 | 1.0000 | 1.0000 | 1.0000 | 867062 | 6623.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1212.7 | 0.712 | 1.076 | 1.330 | 0.6667 | 1.0000 | 0.7654 | 679829 | 4600.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 585.5 | 1.663 | 2.000 | 2.263 | 0.5556 | 0.6667 | 1.0000 | 790398 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 576.1 | 1.677 | 2.101 | 2.414 | 0.5556 | 0.8333 | 1.0000 | 809242 | 9116.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20052.9 | 0.047 | 0.054 | 0.065 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19340.3 | 0.050 | 0.057 | 0.081 | 0.6667 | 1.0000 | 0.7767 | 79019 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19041.7 | 0.048 | 0.055 | 0.177 | 1.0000 | 1.0000 | 1.0000 | 258526 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18647.7 | 0.050 | 0.058 | 0.138 | 0.6667 | 1.0000 | 0.7751 | 259802 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19293.6 | 0.048 | 0.059 | 0.143 | 1.0000 | 1.0000 | 1.0000 | 350628 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18865.5 | 0.048 | 0.057 | 0.164 | 0.6667 | 1.0000 | 0.7654 | 350595 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24524.9 | 0.039 | 0.046 | 0.056 | 0.5556 | 0.6667 | 1.0000 | 70041 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19381.8 | 0.049 | 0.055 | 0.068 | 0.5556 | 0.8333 | 1.0000 | 96089 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 593.0 | 1.646 | 1.944 | 2.273 | 1.0000 | 1.0000 | 1.0000 | 761068 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 585.9 | 1.655 | 2.032 | 2.364 | 0.6667 | 1.0000 | 0.7767 | 762131 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 589.1 | 1.653 | 1.934 | 2.412 | 1.0000 | 1.0000 | 1.0000 | 946098 | 8972.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 586.5 | 1.655 | 1.932 | 2.307 | 0.6667 | 1.0000 | 0.7751 | 950509 | 9005.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 644.2 | 1.501 | 1.781 | 2.236 | 1.0000 | 1.0000 | 1.0000 | 982555 | 8162.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 955.2 | 1.008 | 1.148 | 1.534 | 0.6667 | 1.0000 | 0.7654 | 771207 | 5564.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 585.4 | 1.651 | 2.042 | 2.320 | 0.5556 | 0.6667 | 1.0000 | 744145 | 8810.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 591.3 | 1.645 | 1.984 | 2.262 | 0.5556 | 0.8333 | 1.0000 | 769442 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47121.3 | 0.020 | 0.026 | 0.032 | 1.0000 | 1.0000 | 1.0000 | 38899 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44173.2 | 0.021 | 0.027 | 0.032 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 36743.2 | 0.023 | 0.034 | 0.105 | 1.0000 | 1.0000 | 1.0000 | 229404 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 34398.2 | 0.026 | 0.032 | 0.103 | 0.6667 | 1.0000 | 0.7751 | 232087 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30515.1 | 0.028 | 0.050 | 0.138 | 1.0000 | 1.0000 | 1.0000 | 325403 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 28945.8 | 0.029 | 0.056 | 0.142 | 0.6667 | 1.0000 | 0.7654 | 325320 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 58106.5 | 0.016 | 0.019 | 0.024 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39818.0 | 0.023 | 0.029 | 0.038 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2036.9 | 1.754 | 2.769 | 4.122 | 1.0000 | 1.0000 | 1.0000 | 833068 | 9221.9 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2004.9 | 1.778 | 2.874 | 4.037 | 0.6667 | 1.0000 | 0.7767 | 829837 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1938.0 | 1.785 | 2.982 | 5.901 | 1.0000 | 1.0000 | 1.0000 | 1002417 | 9243.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1928.6 | 1.794 | 3.006 | 4.693 | 0.6667 | 1.0000 | 0.7751 | 1005496 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2160.6 | 1.602 | 2.465 | 3.330 | 1.0000 | 1.0000 | 1.0000 | 1029975 | 8402.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3096.8 | 1.104 | 1.945 | 4.142 | 0.6667 | 1.0000 | 0.7654 | 816824 | 5804.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1993.2 | 1.777 | 3.028 | 4.377 | 0.5556 | 0.7222 | 1.0000 | 854773 | 9120.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1988.2 | 1.787 | 2.884 | 4.434 | 0.5556 | 0.8333 | 1.0000 | 858850 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 48589.7 | 0.070 | 0.139 | 0.188 | 1.0000 | 1.0000 | 1.0000 | 109261 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 49008.2 | 0.070 | 0.127 | 0.177 | 0.6667 | 1.0000 | 0.7767 | 115047 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 34558.9 | 0.098 | 0.184 | 0.428 | 1.0000 | 1.0000 | 1.0000 | 285850 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 28464.6 | 0.106 | 0.243 | 0.642 | 0.6667 | 1.0000 | 0.7751 | 288938 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 23668.8 | 0.131 | 0.232 | 0.506 | 1.0000 | 1.0000 | 1.0000 | 371055 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 23191.0 | 0.134 | 0.237 | 0.600 | 0.6667 | 1.0000 | 0.7654 | 370999 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 48022.8 | 0.073 | 0.137 | 0.173 | 0.5556 | 0.7222 | 1.0000 | 134494 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41179.4 | 0.086 | 0.159 | 0.204 | 0.5556 | 0.8333 | 1.0000 | 143998 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2087.9 | 1.722 | 2.853 | 3.805 | 1.0000 | 1.0000 | 1.0000 | 798238 | 9165.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2094.5 | 1.725 | 2.706 | 3.397 | 0.6667 | 1.0000 | 0.7767 | 792637 | 9202.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2072.1 | 1.719 | 2.916 | 3.705 | 1.0000 | 1.0000 | 1.0000 | 974031 | 9198.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2404.6 | 1.499 | 2.466 | 3.070 | 0.6667 | 1.0000 | 0.7751 | 901102 | 8059.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2943.1 | 1.111 | 2.177 | 2.899 | 1.0000 | 1.0000 | 1.0000 | 867509 | 6624.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4098.9 | 0.832 | 1.701 | 2.328 | 0.6667 | 1.0000 | 0.7654 | 680075 | 4600.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2118.5 | 1.712 | 2.692 | 3.429 | 0.5556 | 0.6667 | 1.0000 | 791660 | 9014.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2125.8 | 1.710 | 2.728 | 3.538 | 0.5556 | 0.8333 | 1.0000 | 809698 | 9116.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 50254.0 | 0.051 | 0.144 | 0.201 | 1.0000 | 1.0000 | 1.0000 | 75797 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 62183.8 | 0.049 | 0.116 | 0.176 | 0.6667 | 1.0000 | 0.7767 | 79176 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 40392.0 | 0.075 | 0.198 | 0.525 | 1.0000 | 1.0000 | 1.0000 | 258687 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 43157.2 | 0.075 | 0.158 | 0.443 | 0.6667 | 1.0000 | 0.7751 | 259975 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30868.4 | 0.110 | 0.248 | 0.634 | 1.0000 | 1.0000 | 1.0000 | 350804 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31236.2 | 0.109 | 0.230 | 0.596 | 0.6667 | 1.0000 | 0.7654 | 350884 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 75080.7 | 0.039 | 0.098 | 0.165 | 0.5556 | 0.6667 | 1.0000 | 70184 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 56695.1 | 0.049 | 0.141 | 0.225 | 0.5556 | 0.8333 | 1.0000 | 96254 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2142.2 | 1.688 | 2.716 | 3.294 | 1.0000 | 1.0000 | 1.0000 | 761353 | 8919.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2174.5 | 1.681 | 2.597 | 3.810 | 0.6667 | 1.0000 | 0.7767 | 762247 | 8969.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2084.1 | 1.704 | 2.866 | 4.271 | 1.0000 | 1.0000 | 1.0000 | 946241 | 8972.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2070.3 | 1.702 | 3.006 | 5.382 | 0.6667 | 1.0000 | 0.7751 | 950664 | 9005.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2234.5 | 1.546 | 2.887 | 4.751 | 1.0000 | 1.0000 | 1.0000 | 982720 | 8162.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3302.3 | 1.047 | 1.915 | 3.086 | 0.6667 | 1.0000 | 0.7654 | 771235 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2168.6 | 1.685 | 2.598 | 3.195 | 0.5556 | 0.6667 | 1.0000 | 744175 | 8810.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2134.9 | 1.693 | 2.725 | 3.547 | 0.5556 | 0.8333 | 1.0000 | 769476 | 8914.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 140336.8 | 0.019 | 0.047 | 0.057 | 1.0000 | 1.0000 | 1.0000 | 38927 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 113639.7 | 0.022 | 0.063 | 0.091 | 0.6667 | 1.0000 | 0.7767 | 43640 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 42147.9 | 0.039 | 0.479 | 1.070 | 1.0000 | 1.0000 | 1.0000 | 229445 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41341.6 | 0.041 | 0.392 | 1.075 | 0.6667 | 1.0000 | 0.7751 | 232168 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 29670.3 | 0.116 | 0.232 | 0.572 | 1.0000 | 1.0000 | 1.0000 | 325511 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 28496.1 | 0.122 | 0.255 | 0.707 | 0.6667 | 1.0000 | 0.7654 | 325435 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 175088.7 | 0.015 | 0.032 | 0.056 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 126194.9 | 0.022 | 0.050 | 0.108 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 408.8 | 2.358 | 2.766 | 3.192 | 1.0000 | 1.0000 | 1.0000 | 1162060 | 12331.2 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 407.7 | 2.362 | 2.770 | 3.169 | 0.6667 | 1.0000 | 0.7767 | 1158879 | 12385.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 401.0 | 2.407 | 2.850 | 3.625 | 1.0000 | 1.0000 | 1.0000 | 1333135 | 12377.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 392.7 | 2.445 | 3.005 | 4.320 | 0.6667 | 1.0000 | 0.7751 | 1333538 | 12358.4 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 434.3 | 2.207 | 2.718 | 3.558 | 1.0000 | 1.0000 | 1.0000 | 1346972 | 11246.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 586.5 | 1.630 | 1.957 | 2.967 | 0.6667 | 1.0000 | 0.7654 | 1099067 | 8024.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 409.3 | 2.368 | 2.751 | 3.823 | 0.5556 | 0.7222 | 1.0000 | 1183595 | 12222.0 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 405.3 | 2.386 | 2.804 | 3.158 | 0.5556 | 0.8333 | 1.0000 | 1187617 | 12302.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 498.5 | 1.943 | 2.378 | 2.682 | 1.0000 | 1.0000 | 1.0000 | 922004 | 11115.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 498.7 | 1.944 | 2.337 | 2.703 | 0.6667 | 1.0000 | 0.7767 | 916716 | 11155.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 493.7 | 1.962 | 2.423 | 2.685 | 1.0000 | 1.0000 | 1.0000 | 1098680 | 11169.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 563.8 | 1.663 | 2.169 | 2.565 | 0.6667 | 1.0000 | 0.7751 | 1012778 | 9796.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 658.7 | 1.305 | 1.960 | 2.593 | 1.0000 | 1.0000 | 1.0000 | 963064 | 8076.0 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 950.9 | 0.938 | 1.385 | 1.797 | 0.6667 | 1.0000 | 0.7654 | 754826 | 5640.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 505.2 | 1.923 | 2.317 | 2.712 | 0.5556 | 0.6667 | 1.0000 | 912413 | 10952.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 497.1 | 1.949 | 2.395 | 2.630 | 0.5556 | 0.8333 | 1.0000 | 931723 | 11058.5 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 427.0 | 2.255 | 2.826 | 3.248 | 1.0000 | 1.0000 | 1.0000 | 1072706 | 11626.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 419.5 | 2.283 | 2.859 | 3.885 | 1.0000 | 1.0000 | 1.0000 | 1259180 | 11701.1 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 454.2 | 2.099 | 2.663 | 3.373 | 1.0000 | 1.0000 | 1.0000 | 1283804 | 10724.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 356.5 | 2.707 | 3.329 | 3.738 | 0.5556 | 0.6667 | 1.0000 | 1288657 | 12107.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1458.5 | 2.437 | 4.004 | 6.410 | 1.0000 | 1.0000 | 1.0000 | 1162353 | 12334.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1439.5 | 2.486 | 4.123 | 6.304 | 0.6667 | 1.0000 | 0.7767 | 1159458 | 12388.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1424.2 | 2.483 | 4.439 | 6.615 | 1.0000 | 1.0000 | 1.0000 | 1332889 | 12379.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1430.1 | 2.465 | 4.414 | 6.056 | 0.6667 | 1.0000 | 0.7751 | 1334087 | 12360.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1530.9 | 2.291 | 4.297 | 5.960 | 1.0000 | 1.0000 | 1.0000 | 1346771 | 11249.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2006.3 | 1.699 | 3.507 | 5.012 | 0.6667 | 1.0000 | 0.7654 | 1099632 | 8027.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1454.9 | 2.480 | 4.035 | 5.786 | 0.5556 | 0.7222 | 1.0000 | 1183433 | 12224.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1453.3 | 2.442 | 4.028 | 7.529 | 0.5556 | 0.8333 | 1.0000 | 1187582 | 12305.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1826.5 | 1.996 | 3.094 | 3.695 | 1.0000 | 1.0000 | 1.0000 | 922370 | 11117.4 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1794.3 | 2.017 | 3.156 | 3.853 | 0.6667 | 1.0000 | 0.7767 | 917846 | 11158.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1808.3 | 2.005 | 3.090 | 3.742 | 1.0000 | 1.0000 | 1.0000 | 1100429 | 11171.8 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2026.2 | 1.900 | 2.894 | 3.534 | 0.6667 | 1.0000 | 0.7751 | 1013704 | 9798.3 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2489.5 | 1.425 | 2.487 | 2.971 | 1.0000 | 1.0000 | 1.0000 | 964336 | 8078.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3363.1 | 1.076 | 2.026 | 2.796 | 0.6667 | 1.0000 | 0.7654 | 754421 | 5642.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1850.4 | 1.968 | 3.023 | 3.525 | 0.5556 | 0.6667 | 1.0000 | 912739 | 10953.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1817.0 | 2.019 | 3.051 | 3.538 | 0.5556 | 0.8333 | 1.0000 | 931494 | 11060.6 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1569.7 | 2.285 | 3.797 | 5.032 | 1.0000 | 1.0000 | 1.0000 | 1072128 | 11628.2 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1551.1 | 2.314 | 3.812 | 5.408 | 1.0000 | 1.0000 | 1.0000 | 1259227 | 11703.3 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1602.3 | 2.168 | 3.886 | 5.962 | 1.0000 | 1.0000 | 1.0000 | 1283086 | 10726.7 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1270.2 | 2.902 | 4.281 | 5.249 | 0.5556 | 0.6667 | 1.0000 | 1287602 | 12108.3 |

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
