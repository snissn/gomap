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
- command: `/tmp/treedb_rag_benchmark_8fc41fac0 -out-dir /tmp/gomap-rag-m3-abba-b2 -dir /tmp/gomap-rag-m3-abba-b2-db -product-base-sha 3b3235ea1e83eb75d589b5379b05888b739b6b08 -harness-revision 8fc41fac0b8cbebc213b6c1ff0759cac85be99cf -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 ABBA B2 candidate`

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
| 0 | 18 | 54 | 0.062084 | 289.93 | 869.79 | 2344408 | 4808 | 3748783 | true |
| 1 | 18 | 54 | 0.048393 | 371.95 | 1115.86 | 1851908 | 4680 | 3748827 | true |
| 2 | 18 | 54 | 0.040694 | 442.33 | 1326.99 | 1843996 | 4676 | 3748827 | true |
| 3 | 18 | 54 | 0.043516 | 413.64 | 1240.93 | 1855174 | 4674 | 3748827 | true |
| 4 | 18 | 54 | 0.043269 | 416.00 | 1247.99 | 1834404 | 4677 | 3748827 | true |

Median/p95 docs/s: **413.64 / 437.06**. Median/p95 B/source: **1851908 / 2246561**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final and repeated artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4546.6 | 0.210 | 0.244 | 0.353 | 1.0000 | 1.0000 | 1.0000 | 253408 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4435.9 | 0.217 | 0.250 | 0.351 | 0.6667 | 1.0000 | 0.7767 | 259137 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4460.2 | 0.214 | 0.253 | 0.396 | 1.0000 | 1.0000 | 1.0000 | 426231 | 2290.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4387.5 | 0.217 | 0.275 | 0.381 | 0.6667 | 1.0000 | 0.7751 | 429384 | 2323.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4760.4 | 0.200 | 0.277 | 0.424 | 1.0000 | 1.0000 | 1.0000 | 497334 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5833.7 | 0.157 | 0.245 | 0.557 | 0.6667 | 1.0000 | 0.7654 | 455798 | 1634.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4577.6 | 0.210 | 0.243 | 0.381 | 0.5556 | 0.8333 | 1.0000 | 277785 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4478.5 | 0.214 | 0.236 | 0.284 | 0.5556 | 1.0000 | 1.0000 | 286640 | 2237.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18442.9 | 0.053 | 0.060 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17597.5 | 0.055 | 0.066 | 0.091 | 0.6667 | 1.0000 | 0.7767 | 113639 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 15892.4 | 0.053 | 0.090 | 0.276 | 1.0000 | 1.0000 | 1.0000 | 284489 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15664.3 | 0.056 | 0.110 | 0.193 | 0.6667 | 1.0000 | 0.7751 | 287548 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15956.8 | 0.053 | 0.084 | 0.247 | 1.0000 | 1.0000 | 1.0000 | 369672 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15372.7 | 0.053 | 0.113 | 0.266 | 0.6667 | 1.0000 | 0.7654 | 369582 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 16956.1 | 0.053 | 0.067 | 0.278 | 0.5556 | 0.8333 | 1.0000 | 132999 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16242.1 | 0.057 | 0.070 | 0.284 | 0.5556 | 1.0000 | 1.0000 | 142001 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4844.4 | 0.201 | 0.218 | 0.270 | 1.0000 | 1.0000 | 1.0000 | 221754 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4758.4 | 0.204 | 0.226 | 0.289 | 0.6667 | 1.0000 | 0.7767 | 221877 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4777.0 | 0.202 | 0.222 | 0.455 | 1.0000 | 1.0000 | 1.0000 | 401387 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5222.4 | 0.176 | 0.215 | 0.446 | 0.6667 | 1.0000 | 0.7751 | 385256 | 2032.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6134.9 | 0.143 | 0.194 | 0.393 | 1.0000 | 1.0000 | 1.0000 | 449034 | 1758.7 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7744.3 | 0.113 | 0.152 | 0.354 | 0.6667 | 1.0000 | 0.7654 | 416370 | 1357.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5031.3 | 0.192 | 0.211 | 0.406 | 0.5556 | 0.6667 | 1.0000 | 215817 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4811.3 | 0.202 | 0.220 | 0.319 | 0.5556 | 0.8333 | 1.0000 | 241496 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20004.0 | 0.048 | 0.055 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 75641 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19663.7 | 0.047 | 0.059 | 0.110 | 0.6667 | 1.0000 | 0.7767 | 79014 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19396.0 | 0.047 | 0.061 | 0.166 | 1.0000 | 1.0000 | 1.0000 | 258530 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19194.6 | 0.049 | 0.053 | 0.144 | 0.6667 | 1.0000 | 0.7751 | 259805 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19634.3 | 0.047 | 0.055 | 0.185 | 1.0000 | 1.0000 | 1.0000 | 350640 | 518.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19374.6 | 0.047 | 0.056 | 0.147 | 0.6667 | 1.0000 | 0.7654 | 350594 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24697.6 | 0.038 | 0.047 | 0.063 | 0.5556 | 0.6667 | 1.0000 | 70072 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19705.6 | 0.049 | 0.053 | 0.089 | 0.5556 | 0.8333 | 1.0000 | 96090 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5555.2 | 0.174 | 0.191 | 0.222 | 1.0000 | 1.0000 | 1.0000 | 183447 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5424.9 | 0.178 | 0.193 | 0.289 | 0.6667 | 1.0000 | 0.7767 | 187870 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5258.2 | 0.181 | 0.201 | 0.369 | 1.0000 | 1.0000 | 1.0000 | 370172 | 2020.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5192.0 | 0.184 | 0.212 | 0.344 | 0.6667 | 1.0000 | 0.7751 | 372897 | 2053.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5542.5 | 0.169 | 0.195 | 0.331 | 1.0000 | 1.0000 | 1.0000 | 450776 | 1908.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7207.7 | 0.125 | 0.189 | 0.387 | 0.6667 | 1.0000 | 0.7654 | 409609 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5728.7 | 0.170 | 0.185 | 0.230 | 0.5556 | 0.8889 | 1.0000 | 168719 | 1862.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5505.4 | 0.177 | 0.193 | 0.209 | 0.5556 | 0.9444 | 1.0000 | 194043 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52162.1 | 0.017 | 0.022 | 0.034 | 1.0000 | 1.0000 | 1.0000 | 37627 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46960.2 | 0.020 | 0.024 | 0.035 | 0.6667 | 1.0000 | 0.7767 | 42330 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 39052.3 | 0.022 | 0.035 | 0.086 | 1.0000 | 1.0000 | 1.0000 | 228126 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 35796.3 | 0.024 | 0.035 | 0.098 | 0.6667 | 1.0000 | 0.7751 | 230836 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32490.6 | 0.026 | 0.042 | 0.127 | 1.0000 | 1.0000 | 1.0000 | 324126 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31672.5 | 0.026 | 0.046 | 0.135 | 0.6667 | 1.0000 | 0.7654 | 324069 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 65920.7 | 0.014 | 0.019 | 0.023 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46711.7 | 0.020 | 0.024 | 0.031 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 13400.8 | 0.251 | 0.557 | 0.944 | 1.0000 | 1.0000 | 1.0000 | 253520 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13220.7 | 0.253 | 0.618 | 0.941 | 0.6667 | 1.0000 | 0.7767 | 259262 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 12450.0 | 0.265 | 0.505 | 0.975 | 1.0000 | 1.0000 | 1.0000 | 426358 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 12181.3 | 0.259 | 0.488 | 1.006 | 0.6667 | 1.0000 | 0.7751 | 429491 | 2323.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12478.8 | 0.259 | 0.438 | 0.524 | 1.0000 | 1.0000 | 1.0000 | 497427 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 12887.1 | 0.240 | 0.544 | 1.905 | 0.6667 | 1.0000 | 0.7654 | 455920 | 1634.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 12908.2 | 0.251 | 0.553 | 0.972 | 0.5556 | 0.8333 | 1.0000 | 277902 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13158.0 | 0.254 | 0.491 | 0.685 | 0.5556 | 1.0000 | 1.0000 | 286780 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51529.6 | 0.065 | 0.127 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 107982 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43737.4 | 0.076 | 0.157 | 0.201 | 0.6667 | 1.0000 | 0.7767 | 113788 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 27188.2 | 0.111 | 0.234 | 0.660 | 1.0000 | 1.0000 | 1.0000 | 284585 | 533.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 28514.9 | 0.098 | 0.254 | 0.902 | 0.6667 | 1.0000 | 0.7751 | 287693 | 566.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 24484.5 | 0.130 | 0.286 | 1.107 | 1.0000 | 1.0000 | 1.0000 | 369755 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20958.7 | 0.137 | 0.291 | 0.835 | 0.6667 | 1.0000 | 0.7654 | 369687 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43951.0 | 0.076 | 0.160 | 0.266 | 0.5556 | 0.8333 | 1.0000 | 133098 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38387.1 | 0.081 | 0.278 | 0.387 | 0.5556 | 1.0000 | 1.0000 | 142147 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15751.1 | 0.214 | 0.483 | 0.715 | 1.0000 | 1.0000 | 1.0000 | 222038 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15523.3 | 0.214 | 0.492 | 0.903 | 0.6667 | 1.0000 | 0.7767 | 222150 | 2252.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14697.7 | 0.223 | 0.532 | 0.760 | 1.0000 | 1.0000 | 1.0000 | 401654 | 2244.8 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 15256.8 | 0.210 | 0.516 | 0.759 | 0.6667 | 1.0000 | 0.7751 | 385586 | 2032.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15369.9 | 0.210 | 0.525 | 0.775 | 1.0000 | 1.0000 | 1.0000 | 449415 | 1759.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20098.7 | 0.162 | 0.433 | 0.621 | 0.6667 | 1.0000 | 0.7654 | 416671 | 1357.4 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 17137.1 | 0.202 | 0.434 | 0.645 | 0.5556 | 0.6667 | 1.0000 | 216338 | 2065.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15034.3 | 0.216 | 0.482 | 0.831 | 0.5556 | 0.8333 | 1.0000 | 241788 | 2169.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 52418.5 | 0.050 | 0.144 | 0.274 | 1.0000 | 1.0000 | 1.0000 | 75820 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 54436.5 | 0.049 | 0.115 | 0.253 | 0.6667 | 1.0000 | 0.7767 | 79206 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 39910.8 | 0.076 | 0.243 | 0.408 | 1.0000 | 1.0000 | 1.0000 | 258774 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40643.0 | 0.075 | 0.219 | 0.457 | 0.6667 | 1.0000 | 0.7751 | 260071 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32570.3 | 0.108 | 0.245 | 0.396 | 1.0000 | 1.0000 | 1.0000 | 350921 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31612.4 | 0.109 | 0.268 | 0.400 | 0.6667 | 1.0000 | 0.7654 | 350887 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 75884.4 | 0.039 | 0.084 | 0.194 | 0.5556 | 0.6667 | 1.0000 | 70214 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 58299.0 | 0.048 | 0.125 | 0.282 | 0.5556 | 0.8333 | 1.0000 | 96270 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18529.7 | 0.184 | 0.428 | 0.624 | 1.0000 | 1.0000 | 1.0000 | 183474 | 1972.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17642.3 | 0.188 | 0.385 | 0.600 | 0.6667 | 1.0000 | 0.7767 | 187915 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 14072.4 | 0.213 | 0.561 | 1.039 | 1.0000 | 1.0000 | 1.0000 | 370247 | 2020.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 14401.1 | 0.214 | 0.537 | 0.968 | 0.6667 | 1.0000 | 0.7751 | 372970 | 2053.3 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 12364.3 | 0.230 | 0.585 | 1.884 | 1.0000 | 1.0000 | 1.0000 | 450912 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 16615.8 | 0.169 | 0.389 | 2.728 | 0.6667 | 1.0000 | 0.7654 | 409658 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18457.8 | 0.180 | 0.428 | 0.588 | 0.5556 | 0.8889 | 1.0000 | 168733 | 1863.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 17352.8 | 0.191 | 0.439 | 0.586 | 0.5556 | 0.9444 | 1.0000 | 194061 | 1966.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 129063.1 | 0.018 | 0.058 | 0.104 | 1.0000 | 1.0000 | 1.0000 | 37662 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 117521.2 | 0.020 | 0.065 | 0.119 | 0.6667 | 1.0000 | 0.7767 | 42377 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43520.4 | 0.040 | 0.388 | 0.970 | 1.0000 | 1.0000 | 1.0000 | 228248 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 41980.6 | 0.040 | 0.377 | 1.228 | 0.6667 | 1.0000 | 0.7751 | 230937 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31999.6 | 0.111 | 0.227 | 0.419 | 1.0000 | 1.0000 | 1.0000 | 324235 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30497.0 | 0.114 | 0.251 | 0.595 | 0.6667 | 1.0000 | 0.7654 | 324225 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 205064.3 | 0.013 | 0.026 | 0.047 | 0.5556 | 0.8889 | 1.0000 | 22648 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 144434.2 | 0.019 | 0.032 | 0.090 | 0.5556 | 0.9444 | 1.0000 | 47946 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1397.3 | 0.694 | 0.758 | 1.427 | 1.0000 | 1.0000 | 1.0000 | 557633 | 5372.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1380.1 | 0.705 | 0.772 | 1.449 | 0.6667 | 1.0000 | 0.7767 | 563481 | 5427.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1372.1 | 0.702 | 0.793 | 1.450 | 1.0000 | 1.0000 | 1.0000 | 732377 | 5410.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1357.5 | 0.705 | 0.800 | 1.504 | 0.6667 | 1.0000 | 0.7751 | 734080 | 5394.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1416.4 | 0.671 | 0.757 | 1.443 | 1.0000 | 1.0000 | 1.0000 | 790892 | 4981.0 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1569.3 | 0.594 | 0.703 | 1.964 | 0.6667 | 1.0000 | 0.7654 | 715271 | 3844.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1389.2 | 0.696 | 0.768 | 1.252 | 0.5556 | 0.8333 | 1.0000 | 582819 | 5256.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1380.3 | 0.697 | 0.771 | 1.345 | 0.5556 | 1.0000 | 1.0000 | 590415 | 5329.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2214.0 | 0.446 | 0.476 | 0.700 | 1.0000 | 1.0000 | 1.0000 | 344114 | 4166.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2186.0 | 0.450 | 0.484 | 0.818 | 0.6667 | 1.0000 | 0.7767 | 344672 | 4204.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2183.6 | 0.447 | 0.492 | 0.840 | 1.0000 | 1.0000 | 1.0000 | 526809 | 4216.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2363.2 | 0.406 | 0.472 | 0.776 | 0.6667 | 1.0000 | 0.7751 | 497021 | 3769.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2677.5 | 0.346 | 0.439 | 0.765 | 1.0000 | 1.0000 | 1.0000 | 544920 | 3211.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3394.9 | 0.281 | 0.349 | 0.668 | 0.6667 | 1.0000 | 0.7654 | 491229 | 2397.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2294.1 | 0.431 | 0.459 | 0.640 | 0.5556 | 0.6667 | 1.0000 | 336621 | 4004.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2227.3 | 0.443 | 0.474 | 0.706 | 0.5556 | 0.8333 | 1.0000 | 362893 | 4110.9 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1567.2 | 0.626 | 0.678 | 0.972 | 1.0000 | 1.0000 | 1.0000 | 470758 | 4668.1 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1516.7 | 0.637 | 0.704 | 1.228 | 1.0000 | 1.0000 | 1.0000 | 657964 | 4738.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1562.6 | 0.618 | 0.675 | 1.256 | 1.0000 | 1.0000 | 1.0000 | 726394 | 4457.4 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 936.6 | 1.047 | 1.159 | 1.503 | 0.5556 | 0.8889 | 1.0000 | 680751 | 5148.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4317.9 | 0.760 | 1.756 | 2.793 | 1.0000 | 1.0000 | 1.0000 | 557881 | 5374.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4241.0 | 0.773 | 1.820 | 2.705 | 0.6667 | 1.0000 | 0.7767 | 565438 | 5430.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4279.7 | 0.773 | 1.938 | 2.656 | 1.0000 | 1.0000 | 1.0000 | 731971 | 5413.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4173.8 | 0.778 | 2.051 | 2.578 | 0.6667 | 1.0000 | 0.7751 | 734119 | 5397.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4268.1 | 0.740 | 2.032 | 2.911 | 1.0000 | 1.0000 | 1.0000 | 791381 | 4984.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4781.6 | 0.681 | 1.651 | 2.352 | 0.6667 | 1.0000 | 0.7654 | 715501 | 3848.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4177.4 | 0.790 | 1.815 | 3.083 | 0.5556 | 0.8333 | 1.0000 | 582361 | 5260.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4487.5 | 0.767 | 1.474 | 2.379 | 0.5556 | 1.0000 | 1.0000 | 590523 | 5332.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7802.7 | 0.439 | 0.881 | 1.262 | 1.0000 | 1.0000 | 1.0000 | 345149 | 4170.5 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7578.4 | 0.449 | 0.947 | 1.309 | 0.6667 | 1.0000 | 0.7767 | 345817 | 4208.3 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7425.8 | 0.454 | 0.970 | 1.215 | 1.0000 | 1.0000 | 1.0000 | 527853 | 4220.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 7844.7 | 0.439 | 0.906 | 1.233 | 0.6667 | 1.0000 | 0.7751 | 497283 | 3774.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 8794.4 | 0.401 | 0.860 | 1.132 | 1.0000 | 1.0000 | 1.0000 | 545365 | 3217.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 10215.7 | 0.332 | 0.745 | 0.953 | 0.6667 | 1.0000 | 0.7654 | 491786 | 2402.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 8090.0 | 0.428 | 0.847 | 1.189 | 0.5556 | 0.6667 | 1.0000 | 337841 | 4007.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7656.5 | 0.446 | 0.886 | 1.306 | 0.5556 | 0.8333 | 1.0000 | 362690 | 4114.0 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5040.2 | 0.663 | 1.383 | 1.878 | 1.0000 | 1.0000 | 1.0000 | 470282 | 4671.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4789.3 | 0.680 | 1.599 | 2.488 | 1.0000 | 1.0000 | 1.0000 | 658392 | 4740.9 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4905.7 | 0.651 | 1.681 | 2.450 | 1.0000 | 1.0000 | 1.0000 | 726347 | 4460.5 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2951.1 | 1.220 | 2.004 | 2.424 | 0.5556 | 0.8889 | 1.0000 | 681211 | 5150.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 565.6 | 1.702 | 2.094 | 2.443 | 1.0000 | 1.0000 | 1.0000 | 832952 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 563.8 | 1.702 | 2.098 | 2.502 | 0.6667 | 1.0000 | 0.7767 | 829760 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 563.8 | 1.699 | 2.106 | 2.457 | 1.0000 | 1.0000 | 1.0000 | 1002345 | 9243.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 558.4 | 1.705 | 2.107 | 2.971 | 0.6667 | 1.0000 | 0.7751 | 1005406 | 9275.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 612.4 | 1.561 | 1.904 | 2.223 | 1.0000 | 1.0000 | 1.0000 | 1029930 | 8402.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 871.3 | 1.078 | 1.473 | 1.961 | 0.6667 | 1.0000 | 0.7654 | 816773 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 564.8 | 1.696 | 2.097 | 2.512 | 0.5556 | 0.7222 | 1.0000 | 854651 | 9120.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 565.4 | 1.707 | 2.115 | 2.415 | 0.5556 | 0.8333 | 1.0000 | 858723 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18255.5 | 0.052 | 0.064 | 0.107 | 1.0000 | 1.0000 | 1.0000 | 109119 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17734.3 | 0.055 | 0.064 | 0.071 | 0.6667 | 1.0000 | 0.7767 | 114898 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16496.8 | 0.056 | 0.091 | 0.122 | 1.0000 | 1.0000 | 1.0000 | 285733 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15162.5 | 0.055 | 0.074 | 0.268 | 0.6667 | 1.0000 | 0.7751 | 288821 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15281.8 | 0.054 | 0.112 | 0.236 | 1.0000 | 1.0000 | 1.0000 | 370966 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15684.7 | 0.053 | 0.077 | 0.265 | 0.6667 | 1.0000 | 0.7654 | 370889 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17772.0 | 0.054 | 0.064 | 0.080 | 0.5556 | 0.7222 | 1.0000 | 134372 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 17093.6 | 0.057 | 0.066 | 0.079 | 0.5556 | 0.8333 | 1.0000 | 143849 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 576.6 | 1.682 | 2.082 | 2.396 | 1.0000 | 1.0000 | 1.0000 | 797802 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 577.0 | 1.688 | 2.070 | 2.365 | 0.6667 | 1.0000 | 0.7767 | 792213 | 9202.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 574.8 | 1.687 | 2.070 | 2.423 | 1.0000 | 1.0000 | 1.0000 | 973498 | 9197.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 662.5 | 1.416 | 1.809 | 2.148 | 0.6667 | 1.0000 | 0.7751 | 900714 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 810.3 | 1.066 | 1.631 | 1.915 | 1.0000 | 1.0000 | 1.0000 | 867106 | 6623.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1182.8 | 0.736 | 1.124 | 1.371 | 0.6667 | 1.0000 | 0.7654 | 679841 | 4600.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 582.4 | 1.670 | 2.021 | 2.378 | 0.5556 | 0.6667 | 1.0000 | 790408 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 576.2 | 1.686 | 2.075 | 2.338 | 0.5556 | 0.8333 | 1.0000 | 809298 | 9116.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20344.9 | 0.047 | 0.054 | 0.065 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19364.9 | 0.049 | 0.057 | 0.072 | 0.6667 | 1.0000 | 0.7767 | 79019 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19040.6 | 0.049 | 0.057 | 0.121 | 1.0000 | 1.0000 | 1.0000 | 258512 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18405.3 | 0.048 | 0.059 | 0.218 | 0.6667 | 1.0000 | 0.7751 | 259788 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 17267.3 | 0.045 | 0.114 | 0.274 | 1.0000 | 1.0000 | 1.0000 | 350610 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18594.8 | 0.048 | 0.060 | 0.201 | 0.6667 | 1.0000 | 0.7654 | 350595 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24697.3 | 0.038 | 0.046 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 70047 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19482.8 | 0.049 | 0.056 | 0.087 | 0.5556 | 0.8333 | 1.0000 | 96082 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 589.8 | 1.650 | 1.976 | 2.244 | 1.0000 | 1.0000 | 1.0000 | 761082 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 580.6 | 1.677 | 2.015 | 2.396 | 0.6667 | 1.0000 | 0.7767 | 762116 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 576.3 | 1.675 | 2.087 | 2.375 | 1.0000 | 1.0000 | 1.0000 | 946104 | 8972.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 574.5 | 1.666 | 2.053 | 2.973 | 0.6667 | 1.0000 | 0.7751 | 950542 | 9005.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 637.5 | 1.508 | 1.895 | 2.285 | 1.0000 | 1.0000 | 1.0000 | 982548 | 8162.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 941.4 | 1.024 | 1.258 | 1.568 | 0.6667 | 1.0000 | 0.7654 | 771214 | 5564.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 588.3 | 1.652 | 2.000 | 2.290 | 0.5556 | 0.6667 | 1.0000 | 744142 | 8810.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 583.2 | 1.662 | 2.016 | 2.360 | 0.5556 | 0.8333 | 1.0000 | 769444 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 45275.0 | 0.021 | 0.026 | 0.032 | 1.0000 | 1.0000 | 1.0000 | 38893 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44757.3 | 0.021 | 0.026 | 0.029 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 36659.5 | 0.023 | 0.034 | 0.106 | 1.0000 | 1.0000 | 1.0000 | 229391 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 33337.4 | 0.026 | 0.039 | 0.118 | 0.6667 | 1.0000 | 0.7751 | 232088 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30320.5 | 0.028 | 0.047 | 0.141 | 1.0000 | 1.0000 | 1.0000 | 325382 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30126.1 | 0.029 | 0.043 | 0.121 | 0.6667 | 1.0000 | 0.7654 | 325291 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 60053.8 | 0.015 | 0.019 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 40464.2 | 0.023 | 0.029 | 0.037 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1978.3 | 1.773 | 2.810 | 5.974 | 1.0000 | 1.0000 | 1.0000 | 833072 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1973.0 | 1.802 | 2.805 | 4.443 | 0.6667 | 1.0000 | 0.7767 | 829821 | 9270.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1942.7 | 1.790 | 2.885 | 5.752 | 1.0000 | 1.0000 | 1.0000 | 1002432 | 9243.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1949.0 | 1.813 | 2.963 | 4.852 | 0.6667 | 1.0000 | 0.7751 | 1005462 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2082.9 | 1.653 | 2.787 | 5.184 | 1.0000 | 1.0000 | 1.0000 | 1030011 | 8402.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2985.7 | 1.125 | 1.896 | 4.156 | 0.6667 | 1.0000 | 0.7654 | 816846 | 5804.2 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1944.2 | 1.810 | 2.964 | 4.429 | 0.5556 | 0.7222 | 1.0000 | 854768 | 9120.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1960.8 | 1.807 | 3.004 | 4.207 | 0.5556 | 0.8333 | 1.0000 | 858849 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 46950.3 | 0.073 | 0.140 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 109261 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46928.7 | 0.071 | 0.145 | 0.178 | 0.6667 | 1.0000 | 0.7767 | 115033 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 28197.6 | 0.105 | 0.242 | 0.918 | 1.0000 | 1.0000 | 1.0000 | 285858 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 31780.6 | 0.109 | 0.208 | 0.378 | 0.6667 | 1.0000 | 0.7751 | 288915 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 22991.9 | 0.132 | 0.252 | 0.584 | 1.0000 | 1.0000 | 1.0000 | 371015 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 23464.2 | 0.133 | 0.257 | 0.638 | 0.6667 | 1.0000 | 0.7654 | 370992 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 47865.5 | 0.074 | 0.135 | 0.173 | 0.5556 | 0.7222 | 1.0000 | 134507 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 45132.8 | 0.078 | 0.150 | 0.185 | 0.5556 | 0.8333 | 1.0000 | 143998 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2084.7 | 1.732 | 2.853 | 3.455 | 1.0000 | 1.0000 | 1.0000 | 798280 | 9165.4 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2075.8 | 1.723 | 2.874 | 3.773 | 0.6667 | 1.0000 | 0.7767 | 792550 | 9202.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2057.5 | 1.725 | 2.938 | 3.535 | 1.0000 | 1.0000 | 1.0000 | 973951 | 9198.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2338.8 | 1.563 | 2.655 | 3.449 | 0.6667 | 1.0000 | 0.7751 | 901190 | 8059.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2836.9 | 1.210 | 2.388 | 3.265 | 1.0000 | 1.0000 | 1.0000 | 867530 | 6624.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4158.9 | 0.787 | 1.704 | 2.301 | 0.6667 | 1.0000 | 0.7654 | 680142 | 4601.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2082.6 | 1.719 | 2.817 | 3.627 | 0.5556 | 0.6667 | 1.0000 | 791745 | 9014.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2084.9 | 1.726 | 2.850 | 3.563 | 0.5556 | 0.8333 | 1.0000 | 809641 | 9116.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63110.3 | 0.047 | 0.121 | 0.200 | 1.0000 | 1.0000 | 1.0000 | 75778 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 58518.3 | 0.050 | 0.129 | 0.223 | 0.6667 | 1.0000 | 0.7767 | 79176 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 41011.1 | 0.074 | 0.236 | 0.425 | 1.0000 | 1.0000 | 1.0000 | 258693 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 23288.7 | 0.098 | 0.457 | 1.975 | 0.6667 | 1.0000 | 0.7751 | 259983 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30660.7 | 0.109 | 0.279 | 0.500 | 1.0000 | 1.0000 | 1.0000 | 350810 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31245.7 | 0.110 | 0.269 | 0.519 | 0.6667 | 1.0000 | 0.7654 | 350837 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 67038.8 | 0.040 | 0.111 | 0.174 | 0.5556 | 0.6667 | 1.0000 | 70212 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 53658.8 | 0.049 | 0.134 | 0.257 | 0.5556 | 0.8333 | 1.0000 | 96275 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2146.8 | 1.688 | 2.687 | 3.484 | 1.0000 | 1.0000 | 1.0000 | 761374 | 8919.9 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2076.2 | 1.704 | 2.865 | 4.007 | 0.6667 | 1.0000 | 0.7767 | 762194 | 8969.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2019.4 | 1.721 | 3.173 | 4.367 | 1.0000 | 1.0000 | 1.0000 | 946218 | 8972.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2050.1 | 1.716 | 3.075 | 4.834 | 0.6667 | 1.0000 | 0.7751 | 950657 | 9005.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2255.6 | 1.553 | 2.662 | 4.972 | 1.0000 | 1.0000 | 1.0000 | 982705 | 8162.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3178.4 | 1.046 | 2.057 | 3.206 | 0.6667 | 1.0000 | 0.7654 | 771264 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2148.5 | 1.687 | 2.776 | 3.407 | 0.5556 | 0.6667 | 1.0000 | 744181 | 8810.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2134.0 | 1.692 | 2.765 | 3.463 | 0.5556 | 0.8333 | 1.0000 | 769484 | 8914.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 133218.2 | 0.019 | 0.050 | 0.074 | 1.0000 | 1.0000 | 1.0000 | 38913 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 130489.4 | 0.021 | 0.048 | 0.063 | 0.6667 | 1.0000 | 0.7767 | 43660 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43789.4 | 0.039 | 0.414 | 1.089 | 1.0000 | 1.0000 | 1.0000 | 229492 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40929.1 | 0.041 | 0.471 | 1.142 | 0.6667 | 1.0000 | 0.7751 | 232168 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 29375.9 | 0.115 | 0.232 | 0.688 | 1.0000 | 1.0000 | 1.0000 | 325499 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29247.1 | 0.121 | 0.244 | 0.647 | 0.6667 | 1.0000 | 0.7654 | 325380 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 177712.8 | 0.015 | 0.030 | 0.058 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 109586.3 | 0.023 | 0.065 | 0.100 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 402.0 | 2.401 | 2.838 | 3.199 | 1.0000 | 1.0000 | 1.0000 | 1162035 | 12331.2 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 401.4 | 2.409 | 2.845 | 3.449 | 0.6667 | 1.0000 | 0.7767 | 1159562 | 12386.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 398.5 | 2.410 | 2.866 | 3.774 | 1.0000 | 1.0000 | 1.0000 | 1333033 | 12377.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 396.0 | 2.421 | 2.886 | 3.993 | 0.6667 | 1.0000 | 0.7751 | 1334249 | 12358.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 416.9 | 2.269 | 3.189 | 4.545 | 1.0000 | 1.0000 | 1.0000 | 1346551 | 11246.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 570.2 | 1.675 | 2.079 | 2.736 | 0.6667 | 1.0000 | 0.7654 | 1099267 | 8025.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 401.4 | 2.398 | 2.839 | 3.317 | 0.5556 | 0.7222 | 1.0000 | 1183730 | 12221.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 401.3 | 2.416 | 2.840 | 3.540 | 0.5556 | 0.8333 | 1.0000 | 1187489 | 12302.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 495.0 | 1.960 | 2.374 | 2.778 | 1.0000 | 1.0000 | 1.0000 | 921973 | 11115.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 491.9 | 1.964 | 2.441 | 2.777 | 0.6667 | 1.0000 | 0.7767 | 917053 | 11155.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 492.3 | 1.967 | 2.423 | 2.703 | 1.0000 | 1.0000 | 1.0000 | 1099700 | 11169.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 558.4 | 1.703 | 2.177 | 2.585 | 0.6667 | 1.0000 | 0.7751 | 1012622 | 9796.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 674.0 | 1.321 | 1.949 | 2.278 | 1.0000 | 1.0000 | 1.0000 | 963361 | 8076.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 951.4 | 0.938 | 1.373 | 1.644 | 0.6667 | 1.0000 | 0.7654 | 754535 | 5640.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 496.8 | 1.953 | 2.387 | 2.609 | 0.5556 | 0.6667 | 1.0000 | 912681 | 10952.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 493.3 | 1.958 | 2.435 | 2.697 | 0.5556 | 0.8333 | 1.0000 | 931601 | 11058.5 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 418.1 | 2.305 | 2.871 | 3.287 | 1.0000 | 1.0000 | 1.0000 | 1073022 | 11626.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 410.8 | 2.321 | 2.965 | 4.092 | 1.0000 | 1.0000 | 1.0000 | 1259482 | 11701.1 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 451.4 | 2.104 | 2.669 | 3.467 | 1.0000 | 1.0000 | 1.0000 | 1283684 | 10724.3 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 353.8 | 2.729 | 3.364 | 3.886 | 0.5556 | 0.6667 | 1.0000 | 1288464 | 12107.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1408.0 | 2.492 | 4.323 | 7.041 | 1.0000 | 1.0000 | 1.0000 | 1162431 | 12333.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1415.6 | 2.526 | 4.492 | 6.352 | 0.6667 | 1.0000 | 0.7767 | 1159451 | 12388.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1408.4 | 2.520 | 4.440 | 6.430 | 1.0000 | 1.0000 | 1.0000 | 1332762 | 12379.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1392.1 | 2.533 | 4.516 | 6.800 | 0.6667 | 1.0000 | 0.7751 | 1333946 | 12361.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1131.2 | 3.048 | 6.910 | 9.973 | 1.0000 | 1.0000 | 1.0000 | 1346888 | 11249.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2005.3 | 1.750 | 3.220 | 4.815 | 0.6667 | 1.0000 | 0.7654 | 1099707 | 8027.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1452.8 | 2.457 | 3.821 | 7.861 | 0.5556 | 0.7222 | 1.0000 | 1183344 | 12224.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1419.0 | 2.496 | 4.448 | 6.528 | 0.5556 | 0.8333 | 1.0000 | 1187515 | 12305.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1768.8 | 2.040 | 3.207 | 3.992 | 1.0000 | 1.0000 | 1.0000 | 922562 | 11117.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1783.0 | 2.028 | 3.254 | 4.028 | 0.6667 | 1.0000 | 0.7767 | 917296 | 11158.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1754.2 | 2.094 | 3.186 | 3.717 | 1.0000 | 1.0000 | 1.0000 | 1100061 | 11171.8 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2020.8 | 1.903 | 2.890 | 3.555 | 0.6667 | 1.0000 | 0.7751 | 1013538 | 9798.4 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2437.4 | 1.492 | 2.500 | 3.124 | 1.0000 | 1.0000 | 1.0000 | 963323 | 8078.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3371.9 | 1.104 | 2.000 | 2.577 | 0.6667 | 1.0000 | 0.7654 | 754592 | 5642.3 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1810.7 | 2.014 | 3.141 | 3.827 | 0.5556 | 0.6667 | 1.0000 | 913070 | 10954.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1792.7 | 2.009 | 3.165 | 3.879 | 0.5556 | 0.8333 | 1.0000 | 932424 | 11060.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1504.1 | 2.398 | 3.904 | 4.797 | 1.0000 | 1.0000 | 1.0000 | 1072404 | 11628.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1472.9 | 2.389 | 4.213 | 6.071 | 1.0000 | 1.0000 | 1.0000 | 1259158 | 11703.1 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1602.7 | 2.197 | 3.834 | 5.512 | 1.0000 | 1.0000 | 1.0000 | 1283226 | 10726.1 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1171.9 | 3.131 | 5.022 | 5.827 | 0.5556 | 0.6667 | 1.0000 | 1289240 | 12108.9 |

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
