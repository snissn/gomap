# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `363a550c368baa23b6ef7c4078f734bd236375a1`
- harness revision: `3224fc55306e3b2d4f661549956f6ef9e453084e`
- binary SHA-256: `2d15acc883d543ddfaf3480c94ff3bfed44ccddf7abc2a8785496dd7dc6d470a`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_3224fc553 -out-dir /tmp/gomap-4292-artifacts-322 -dir /tmp/gomap-4292-db-322 -product-base-sha 363a550c368baa23b6ef7c4078f734bd236375a1 -harness-revision 3224fc55306e3b2d4f661549956f6ef9e453084e -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4292 optimized candidate`

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
| 0 | 18 | 54 | 0.072369 | 248.72 | 746.17 | 2332950 | 4807 | 3748827 | true |
| 1 | 18 | 54 | 0.043446 | 414.31 | 1242.93 | 1842632 | 4676 | 3748827 | true |
| 2 | 18 | 54 | 0.040300 | 446.65 | 1339.95 | 1851911 | 4675 | 3748827 | true |
| 3 | 18 | 54 | 0.042401 | 424.52 | 1273.55 | 1857587 | 4678 | 3748833 | true |
| 4 | 18 | 54 | 0.041368 | 435.11 | 1305.34 | 1834912 | 4674 | 3748827 | true |

Median/p95 docs/s: **424.52 / 444.34**. Median/p95 B/source: **1851911 / 2237877**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **488.19**, B/source <= **1666720**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4582.4 | 0.209 | 0.231 | 0.394 | 1.0000 | 1.0000 | 1.0000 | 253385 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4455.5 | 0.216 | 0.240 | 0.393 | 0.6667 | 1.0000 | 0.7767 | 259133 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4428.0 | 0.214 | 0.252 | 0.446 | 1.0000 | 1.0000 | 1.0000 | 426238 | 2290.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4349.3 | 0.216 | 0.252 | 0.390 | 0.6667 | 1.0000 | 0.7751 | 429365 | 2323.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4725.5 | 0.198 | 0.244 | 0.706 | 1.0000 | 1.0000 | 1.0000 | 497325 | 2148.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5511.5 | 0.158 | 0.271 | 0.639 | 0.6667 | 1.0000 | 0.7654 | 455819 | 1634.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4549.6 | 0.209 | 0.226 | 0.400 | 0.5556 | 0.8333 | 1.0000 | 277770 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4565.3 | 0.213 | 0.238 | 0.371 | 0.5556 | 1.0000 | 1.0000 | 286640 | 2237.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18354.0 | 0.053 | 0.061 | 0.077 | 1.0000 | 1.0000 | 1.0000 | 107839 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17744.7 | 0.055 | 0.062 | 0.067 | 0.6667 | 1.0000 | 0.7767 | 113639 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 15434.2 | 0.053 | 0.075 | 0.245 | 1.0000 | 1.0000 | 1.0000 | 284509 | 533.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 15126.6 | 0.056 | 0.076 | 0.280 | 0.6667 | 1.0000 | 0.7751 | 287553 | 566.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15595.3 | 0.053 | 0.099 | 0.235 | 1.0000 | 1.0000 | 1.0000 | 369660 | 561.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 16352.6 | 0.053 | 0.105 | 0.144 | 0.6667 | 1.0000 | 0.7654 | 369625 | 566.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 16920.3 | 0.054 | 0.067 | 0.275 | 0.5556 | 0.8333 | 1.0000 | 133010 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16323.3 | 0.057 | 0.081 | 0.138 | 0.5556 | 1.0000 | 1.0000 | 141990 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4788.8 | 0.202 | 0.223 | 0.322 | 1.0000 | 1.0000 | 1.0000 | 221733 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4774.4 | 0.203 | 0.221 | 0.367 | 0.6667 | 1.0000 | 0.7767 | 221872 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4815.2 | 0.201 | 0.214 | 0.397 | 1.0000 | 1.0000 | 1.0000 | 401377 | 2244.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5290.2 | 0.175 | 0.211 | 0.430 | 0.6667 | 1.0000 | 0.7751 | 385270 | 2032.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6144.0 | 0.142 | 0.195 | 0.389 | 1.0000 | 1.0000 | 1.0000 | 449048 | 1758.8 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7834.6 | 0.113 | 0.148 | 0.332 | 0.6667 | 1.0000 | 0.7654 | 416356 | 1357.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5117.6 | 0.192 | 0.204 | 0.282 | 0.5556 | 0.6667 | 1.0000 | 215818 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4837.9 | 0.201 | 0.216 | 0.255 | 0.5556 | 0.8333 | 1.0000 | 241473 | 2168.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20035.8 | 0.049 | 0.055 | 0.067 | 1.0000 | 1.0000 | 1.0000 | 75642 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19669.5 | 0.049 | 0.054 | 0.069 | 0.6667 | 1.0000 | 0.7767 | 79027 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19535.7 | 0.048 | 0.054 | 0.123 | 1.0000 | 1.0000 | 1.0000 | 258530 | 487.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19090.4 | 0.050 | 0.055 | 0.126 | 0.6667 | 1.0000 | 0.7751 | 259820 | 506.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19491.0 | 0.047 | 0.055 | 0.157 | 1.0000 | 1.0000 | 1.0000 | 350654 | 518.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19459.8 | 0.048 | 0.053 | 0.148 | 0.6667 | 1.0000 | 0.7654 | 350633 | 521.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24271.4 | 0.039 | 0.048 | 0.054 | 0.5556 | 0.6667 | 1.0000 | 70072 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19420.6 | 0.049 | 0.057 | 0.101 | 0.5556 | 0.8333 | 1.0000 | 96097 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5588.3 | 0.174 | 0.189 | 0.281 | 1.0000 | 1.0000 | 1.0000 | 183463 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5459.5 | 0.178 | 0.192 | 0.272 | 0.6667 | 1.0000 | 0.7767 | 187870 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5250.4 | 0.180 | 0.198 | 0.489 | 1.0000 | 1.0000 | 1.0000 | 370203 | 2020.6 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5248.0 | 0.183 | 0.204 | 0.315 | 0.6667 | 1.0000 | 0.7751 | 372904 | 2053.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5498.4 | 0.170 | 0.203 | 0.555 | 1.0000 | 1.0000 | 1.0000 | 450807 | 1908.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7254.6 | 0.125 | 0.151 | 0.439 | 0.6667 | 1.0000 | 0.7654 | 409600 | 1395.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5788.8 | 0.169 | 0.179 | 0.218 | 0.5556 | 0.8889 | 1.0000 | 168722 | 1862.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5507.7 | 0.176 | 0.193 | 0.237 | 0.5556 | 0.9444 | 1.0000 | 194046 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51964.8 | 0.018 | 0.021 | 0.030 | 1.0000 | 1.0000 | 1.0000 | 37627 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44627.0 | 0.021 | 0.025 | 0.035 | 0.6667 | 1.0000 | 0.7767 | 42330 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 38317.1 | 0.022 | 0.034 | 0.104 | 1.0000 | 1.0000 | 1.0000 | 228113 | 262.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 35571.9 | 0.024 | 0.035 | 0.102 | 0.6667 | 1.0000 | 0.7751 | 230836 | 294.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 32291.9 | 0.027 | 0.040 | 0.114 | 1.0000 | 1.0000 | 1.0000 | 324126 | 322.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31992.4 | 0.027 | 0.042 | 0.107 | 0.6667 | 1.0000 | 0.7654 | 324015 | 327.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 66900.9 | 0.013 | 0.018 | 0.025 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46962.3 | 0.020 | 0.023 | 0.028 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 13327.1 | 0.249 | 0.493 | 0.969 | 1.0000 | 1.0000 | 1.0000 | 253526 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 12815.2 | 0.253 | 0.534 | 0.901 | 0.6667 | 1.0000 | 0.7767 | 259269 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 11975.8 | 0.267 | 0.609 | 1.940 | 1.0000 | 1.0000 | 1.0000 | 426325 | 2290.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 10670.0 | 0.270 | 0.752 | 1.516 | 0.6667 | 1.0000 | 0.7751 | 429504 | 2323.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 13014.6 | 0.251 | 0.422 | 0.829 | 1.0000 | 1.0000 | 1.0000 | 497407 | 2148.9 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 14415.6 | 0.220 | 0.416 | 0.727 | 0.6667 | 1.0000 | 0.7654 | 455907 | 1634.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 13195.0 | 0.248 | 0.557 | 1.028 | 0.5556 | 0.8333 | 1.0000 | 277916 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13335.6 | 0.250 | 0.547 | 0.952 | 0.5556 | 1.0000 | 1.0000 | 286807 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51478.6 | 0.066 | 0.131 | 0.172 | 1.0000 | 1.0000 | 1.0000 | 107988 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 47097.4 | 0.071 | 0.143 | 0.178 | 0.6667 | 1.0000 | 0.7767 | 113781 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 29632.6 | 0.105 | 0.254 | 0.681 | 1.0000 | 1.0000 | 1.0000 | 284599 | 533.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 26648.9 | 0.108 | 0.270 | 1.647 | 0.6667 | 1.0000 | 0.7751 | 287665 | 566.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 24200.0 | 0.131 | 0.225 | 0.700 | 1.0000 | 1.0000 | 1.0000 | 369789 | 561.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 24142.5 | 0.128 | 0.246 | 0.741 | 0.6667 | 1.0000 | 0.7654 | 369712 | 566.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 40590.7 | 0.077 | 0.264 | 0.344 | 0.5556 | 0.8333 | 1.0000 | 133091 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39601.6 | 0.081 | 0.245 | 0.323 | 0.5556 | 1.0000 | 1.0000 | 142141 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15556.0 | 0.214 | 0.498 | 0.619 | 1.0000 | 1.0000 | 1.0000 | 222024 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 16146.1 | 0.213 | 0.483 | 0.692 | 0.6667 | 1.0000 | 0.7767 | 222123 | 2252.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 13545.9 | 0.224 | 0.616 | 0.825 | 1.0000 | 1.0000 | 1.0000 | 401791 | 2244.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 16248.9 | 0.205 | 0.518 | 0.736 | 0.6667 | 1.0000 | 0.7751 | 385593 | 2032.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 17678.4 | 0.189 | 0.471 | 0.667 | 1.0000 | 1.0000 | 1.0000 | 449372 | 1759.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20474.5 | 0.160 | 0.408 | 0.663 | 0.6667 | 1.0000 | 0.7654 | 416724 | 1357.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 17079.4 | 0.202 | 0.417 | 0.657 | 0.5556 | 0.6667 | 1.0000 | 216330 | 2065.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15608.2 | 0.217 | 0.486 | 0.629 | 0.5556 | 0.8333 | 1.0000 | 241747 | 2169.4 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 62368.6 | 0.047 | 0.127 | 0.240 | 1.0000 | 1.0000 | 1.0000 | 75820 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 63687.8 | 0.049 | 0.110 | 0.197 | 0.6667 | 1.0000 | 0.7767 | 79199 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 42447.3 | 0.077 | 0.197 | 0.347 | 1.0000 | 1.0000 | 1.0000 | 258714 | 487.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 37137.9 | 0.083 | 0.242 | 0.359 | 0.6667 | 1.0000 | 0.7751 | 260085 | 506.9 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31969.8 | 0.107 | 0.261 | 0.408 | 1.0000 | 1.0000 | 1.0000 | 350886 | 518.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31493.4 | 0.109 | 0.253 | 0.379 | 0.6667 | 1.0000 | 0.7654 | 350899 | 522.0 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 74366.5 | 0.038 | 0.108 | 0.227 | 0.5556 | 0.6667 | 1.0000 | 70229 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 55830.0 | 0.049 | 0.139 | 0.311 | 0.5556 | 0.8333 | 1.0000 | 96291 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18772.7 | 0.183 | 0.391 | 0.600 | 1.0000 | 1.0000 | 1.0000 | 183552 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17989.6 | 0.187 | 0.386 | 0.666 | 0.6667 | 1.0000 | 0.7767 | 187920 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 15485.9 | 0.203 | 0.511 | 0.856 | 1.0000 | 1.0000 | 1.0000 | 370264 | 2020.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 15171.2 | 0.201 | 0.443 | 0.936 | 0.6667 | 1.0000 | 0.7751 | 372962 | 2053.2 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 14617.1 | 0.199 | 0.495 | 1.938 | 1.0000 | 1.0000 | 1.0000 | 450900 | 1909.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 17159.2 | 0.176 | 0.313 | 0.888 | 0.6667 | 1.0000 | 0.7654 | 409684 | 1395.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18861.3 | 0.179 | 0.391 | 0.593 | 0.5556 | 0.8889 | 1.0000 | 168732 | 1863.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18684.1 | 0.185 | 0.395 | 0.539 | 0.5556 | 0.9444 | 1.0000 | 194059 | 1966.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 124065.0 | 0.019 | 0.059 | 0.098 | 1.0000 | 1.0000 | 1.0000 | 37648 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 117878.1 | 0.020 | 0.053 | 0.143 | 0.6667 | 1.0000 | 0.7767 | 42377 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 41707.7 | 0.040 | 0.372 | 0.787 | 1.0000 | 1.0000 | 1.0000 | 228208 | 262.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42886.1 | 0.040 | 0.373 | 1.101 | 0.6667 | 1.0000 | 0.7751 | 230905 | 294.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 31783.7 | 0.112 | 0.216 | 0.448 | 1.0000 | 1.0000 | 1.0000 | 324275 | 322.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 30858.5 | 0.114 | 0.233 | 0.494 | 0.6667 | 1.0000 | 0.7654 | 324198 | 327.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 217055.4 | 0.012 | 0.023 | 0.030 | 0.5556 | 0.8889 | 1.0000 | 22647 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 138349.2 | 0.019 | 0.039 | 0.097 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1402.9 | 0.692 | 0.766 | 1.359 | 1.0000 | 1.0000 | 1.0000 | 557775 | 5372.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1371.0 | 0.706 | 0.768 | 1.188 | 0.6667 | 1.0000 | 0.7767 | 563835 | 5427.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1373.5 | 0.697 | 0.758 | 1.120 | 1.0000 | 1.0000 | 1.0000 | 731956 | 5410.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1357.0 | 0.705 | 0.804 | 1.425 | 0.6667 | 1.0000 | 0.7751 | 733926 | 5394.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1408.4 | 0.670 | 0.767 | 1.765 | 1.0000 | 1.0000 | 1.0000 | 790898 | 4981.1 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1576.5 | 0.588 | 0.686 | 1.236 | 0.6667 | 1.0000 | 0.7654 | 715087 | 3844.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1408.2 | 0.685 | 0.736 | 0.841 | 0.5556 | 0.8333 | 1.0000 | 582617 | 5256.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1387.2 | 0.696 | 0.770 | 1.259 | 0.5556 | 1.0000 | 1.0000 | 590580 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2221.4 | 0.445 | 0.474 | 0.617 | 1.0000 | 1.0000 | 1.0000 | 344644 | 4166.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2201.6 | 0.449 | 0.476 | 0.698 | 0.6667 | 1.0000 | 0.7767 | 345010 | 4204.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2198.1 | 0.448 | 0.480 | 0.781 | 1.0000 | 1.0000 | 1.0000 | 526661 | 4216.6 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2360.0 | 0.406 | 0.472 | 0.778 | 0.6667 | 1.0000 | 0.7751 | 496750 | 3769.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2654.6 | 0.346 | 0.441 | 0.771 | 1.0000 | 1.0000 | 1.0000 | 545107 | 3211.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3408.1 | 0.281 | 0.352 | 0.615 | 0.6667 | 1.0000 | 0.7654 | 491346 | 2397.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2295.9 | 0.432 | 0.456 | 0.729 | 0.5556 | 0.6667 | 1.0000 | 336863 | 4004.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2222.8 | 0.444 | 0.475 | 0.659 | 0.5556 | 0.8333 | 1.0000 | 362467 | 4111.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1562.1 | 0.624 | 0.698 | 1.012 | 1.0000 | 1.0000 | 1.0000 | 470856 | 4668.1 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1511.8 | 0.638 | 0.699 | 1.251 | 1.0000 | 1.0000 | 1.0000 | 658201 | 4738.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1550.3 | 0.619 | 0.680 | 1.162 | 1.0000 | 1.0000 | 1.0000 | 726321 | 4457.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 951.0 | 1.033 | 1.126 | 1.466 | 0.5556 | 0.8889 | 1.0000 | 681181 | 5148.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4464.2 | 0.762 | 1.625 | 2.544 | 1.0000 | 1.0000 | 1.0000 | 558002 | 5375.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4282.4 | 0.782 | 1.717 | 2.666 | 0.6667 | 1.0000 | 0.7767 | 565611 | 5430.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4236.5 | 0.784 | 1.769 | 2.586 | 1.0000 | 1.0000 | 1.0000 | 732155 | 5414.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4269.5 | 0.765 | 1.889 | 2.702 | 0.6667 | 1.0000 | 0.7751 | 734327 | 5398.1 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4380.5 | 0.744 | 1.847 | 2.768 | 1.0000 | 1.0000 | 1.0000 | 791330 | 4984.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5118.5 | 0.665 | 1.436 | 2.246 | 0.6667 | 1.0000 | 0.7654 | 715544 | 3847.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4381.8 | 0.756 | 1.634 | 2.503 | 0.5556 | 0.8333 | 1.0000 | 582319 | 5259.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4604.4 | 0.741 | 1.540 | 2.093 | 0.5556 | 1.0000 | 1.0000 | 590809 | 5332.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7916.7 | 0.439 | 0.852 | 1.171 | 1.0000 | 1.0000 | 1.0000 | 345006 | 4169.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7773.0 | 0.444 | 0.846 | 1.156 | 0.6667 | 1.0000 | 0.7767 | 346445 | 4208.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7320.5 | 0.458 | 0.994 | 1.345 | 1.0000 | 1.0000 | 1.0000 | 528718 | 4220.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8340.4 | 0.421 | 0.835 | 1.147 | 0.6667 | 1.0000 | 0.7751 | 498112 | 3773.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 9075.2 | 0.392 | 0.789 | 1.087 | 1.0000 | 1.0000 | 1.0000 | 545524 | 3217.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 10913.1 | 0.313 | 0.706 | 0.999 | 0.6667 | 1.0000 | 0.7654 | 492548 | 2402.5 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 8350.0 | 0.420 | 0.828 | 1.166 | 0.5556 | 0.6667 | 1.0000 | 337373 | 4007.2 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7867.2 | 0.442 | 0.836 | 1.143 | 0.5556 | 0.8333 | 1.0000 | 363130 | 4114.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5351.7 | 0.639 | 1.269 | 1.822 | 1.0000 | 1.0000 | 1.0000 | 469789 | 4670.2 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5088.5 | 0.652 | 1.437 | 2.216 | 1.0000 | 1.0000 | 1.0000 | 658649 | 4741.5 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4987.8 | 0.647 | 1.552 | 2.628 | 1.0000 | 1.0000 | 1.0000 | 726346 | 4460.4 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3089.7 | 1.157 | 1.908 | 2.296 | 0.5556 | 0.8889 | 1.0000 | 682005 | 5150.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 573.2 | 1.691 | 2.023 | 2.361 | 1.0000 | 1.0000 | 1.0000 | 832946 | 9221.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 571.9 | 1.691 | 1.953 | 2.366 | 0.6667 | 1.0000 | 0.7767 | 829754 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 574.4 | 1.677 | 1.964 | 2.330 | 1.0000 | 1.0000 | 1.0000 | 1002338 | 9243.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 567.5 | 1.689 | 2.026 | 2.398 | 0.6667 | 1.0000 | 0.7751 | 1005428 | 9275.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 621.1 | 1.542 | 1.880 | 2.193 | 1.0000 | 1.0000 | 1.0000 | 1029900 | 8402.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 904.7 | 1.050 | 1.282 | 1.530 | 0.6667 | 1.0000 | 0.7654 | 816757 | 5804.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 573.7 | 1.682 | 2.019 | 2.404 | 0.5556 | 0.7222 | 1.0000 | 854602 | 9119.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 569.3 | 1.698 | 2.077 | 2.423 | 0.5556 | 0.8333 | 1.0000 | 858702 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18399.5 | 0.053 | 0.061 | 0.070 | 1.0000 | 1.0000 | 1.0000 | 109119 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17545.0 | 0.056 | 0.063 | 0.077 | 0.6667 | 1.0000 | 0.7767 | 114898 | 560.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 16260.3 | 0.056 | 0.069 | 0.169 | 1.0000 | 1.0000 | 1.0000 | 285740 | 533.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 16501.8 | 0.056 | 0.073 | 0.127 | 0.6667 | 1.0000 | 0.7751 | 288803 | 565.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 15478.0 | 0.054 | 0.086 | 0.267 | 1.0000 | 1.0000 | 1.0000 | 370966 | 561.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 15633.3 | 0.055 | 0.111 | 0.133 | 0.6667 | 1.0000 | 0.7654 | 370868 | 566.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17798.7 | 0.054 | 0.064 | 0.074 | 0.5556 | 0.7222 | 1.0000 | 134358 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16965.2 | 0.057 | 0.067 | 0.086 | 0.5556 | 0.8333 | 1.0000 | 143848 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 580.3 | 1.676 | 2.057 | 2.274 | 1.0000 | 1.0000 | 1.0000 | 797798 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 584.6 | 1.670 | 1.963 | 2.291 | 0.6667 | 1.0000 | 0.7767 | 792194 | 9202.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 587.0 | 1.666 | 1.939 | 2.294 | 1.0000 | 1.0000 | 1.0000 | 973524 | 9197.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 668.0 | 1.390 | 1.802 | 2.164 | 0.6667 | 1.0000 | 0.7751 | 900727 | 8058.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 821.5 | 1.054 | 1.590 | 1.921 | 1.0000 | 1.0000 | 1.0000 | 867078 | 6623.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1209.4 | 0.719 | 1.074 | 1.382 | 0.6667 | 1.0000 | 0.7654 | 679804 | 4600.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 590.0 | 1.658 | 1.932 | 2.206 | 0.5556 | 0.6667 | 1.0000 | 790399 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 583.3 | 1.671 | 1.945 | 2.405 | 0.5556 | 0.8333 | 1.0000 | 809264 | 9116.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20420.5 | 0.047 | 0.054 | 0.060 | 1.0000 | 1.0000 | 1.0000 | 75633 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19381.9 | 0.050 | 0.056 | 0.068 | 0.6667 | 1.0000 | 0.7767 | 79019 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 19188.7 | 0.049 | 0.054 | 0.147 | 1.0000 | 1.0000 | 1.0000 | 258506 | 487.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18255.9 | 0.049 | 0.061 | 0.149 | 0.6667 | 1.0000 | 0.7751 | 259816 | 506.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19211.8 | 0.047 | 0.060 | 0.135 | 1.0000 | 1.0000 | 1.0000 | 350635 | 518.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 18954.3 | 0.048 | 0.055 | 0.142 | 0.6667 | 1.0000 | 0.7654 | 350593 | 521.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24622.4 | 0.039 | 0.046 | 0.056 | 0.5556 | 0.6667 | 1.0000 | 70040 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19330.5 | 0.049 | 0.055 | 0.087 | 0.5556 | 0.8333 | 1.0000 | 96096 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 595.1 | 1.637 | 1.924 | 2.229 | 1.0000 | 1.0000 | 1.0000 | 761077 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 591.3 | 1.643 | 1.926 | 2.293 | 0.6667 | 1.0000 | 0.7767 | 762112 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 586.2 | 1.656 | 2.004 | 2.427 | 1.0000 | 1.0000 | 1.0000 | 946103 | 8972.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 585.2 | 1.655 | 2.026 | 2.333 | 0.6667 | 1.0000 | 0.7751 | 950500 | 9005.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 642.8 | 1.501 | 1.801 | 2.264 | 1.0000 | 1.0000 | 1.0000 | 982564 | 8162.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 943.1 | 1.019 | 1.201 | 1.606 | 0.6667 | 1.0000 | 0.7654 | 771213 | 5564.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 588.8 | 1.651 | 1.998 | 2.331 | 0.5556 | 0.6667 | 1.0000 | 744150 | 8810.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 595.9 | 1.642 | 1.900 | 2.209 | 0.5556 | 0.8333 | 1.0000 | 769440 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 48305.4 | 0.019 | 0.025 | 0.029 | 1.0000 | 1.0000 | 1.0000 | 38899 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 44004.4 | 0.022 | 0.026 | 0.030 | 0.6667 | 1.0000 | 0.7767 | 43612 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 35346.4 | 0.024 | 0.035 | 0.104 | 1.0000 | 1.0000 | 1.0000 | 229384 | 262.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 33449.2 | 0.026 | 0.035 | 0.099 | 0.6667 | 1.0000 | 0.7751 | 232080 | 295.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 29179.1 | 0.028 | 0.049 | 0.143 | 1.0000 | 1.0000 | 1.0000 | 325382 | 322.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 29078.6 | 0.029 | 0.052 | 0.155 | 0.6667 | 1.0000 | 0.7654 | 325320 | 327.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 59634.0 | 0.016 | 0.017 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41025.6 | 0.023 | 0.030 | 0.037 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2030.6 | 1.755 | 2.736 | 4.391 | 1.0000 | 1.0000 | 1.0000 | 833047 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2053.9 | 1.757 | 2.599 | 4.260 | 0.6667 | 1.0000 | 0.7767 | 829871 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1967.8 | 1.777 | 3.016 | 5.612 | 1.0000 | 1.0000 | 1.0000 | 1002469 | 9243.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2006.1 | 1.766 | 2.601 | 3.978 | 0.6667 | 1.0000 | 0.7751 | 1005481 | 9275.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2161.7 | 1.615 | 2.512 | 3.519 | 1.0000 | 1.0000 | 1.0000 | 1029964 | 8402.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2989.2 | 1.115 | 1.957 | 5.032 | 0.6667 | 1.0000 | 0.7654 | 816858 | 5804.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2022.8 | 1.774 | 2.819 | 4.005 | 0.5556 | 0.7222 | 1.0000 | 854777 | 9120.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1904.0 | 1.798 | 3.422 | 4.900 | 0.5556 | 0.8333 | 1.0000 | 858833 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51291.3 | 0.067 | 0.122 | 0.153 | 1.0000 | 1.0000 | 1.0000 | 109268 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45684.6 | 0.074 | 0.149 | 0.192 | 0.6667 | 1.0000 | 0.7767 | 115033 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 32513.0 | 0.105 | 0.206 | 0.332 | 1.0000 | 1.0000 | 1.0000 | 285863 | 533.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 28376.5 | 0.105 | 0.295 | 0.588 | 0.6667 | 1.0000 | 0.7751 | 288928 | 565.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 22202.9 | 0.137 | 0.271 | 1.008 | 1.0000 | 1.0000 | 1.0000 | 371035 | 561.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 21664.4 | 0.130 | 0.237 | 0.550 | 0.6667 | 1.0000 | 0.7654 | 370957 | 566.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 45497.7 | 0.076 | 0.149 | 0.191 | 0.5556 | 0.7222 | 1.0000 | 134507 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 45613.4 | 0.079 | 0.141 | 0.179 | 0.5556 | 0.8333 | 1.0000 | 144005 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2119.8 | 1.711 | 2.713 | 3.331 | 1.0000 | 1.0000 | 1.0000 | 798286 | 9165.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2097.0 | 1.717 | 2.809 | 3.521 | 0.6667 | 1.0000 | 0.7767 | 792647 | 9202.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1926.8 | 1.746 | 3.426 | 5.472 | 1.0000 | 1.0000 | 1.0000 | 973950 | 9197.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2400.8 | 1.479 | 2.473 | 3.399 | 0.6667 | 1.0000 | 0.7751 | 901121 | 8059.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2893.6 | 1.129 | 2.196 | 2.966 | 1.0000 | 1.0000 | 1.0000 | 867482 | 6624.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4209.2 | 0.777 | 1.591 | 2.203 | 0.6667 | 1.0000 | 0.7654 | 680121 | 4601.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2137.4 | 1.704 | 2.689 | 3.489 | 0.5556 | 0.6667 | 1.0000 | 791731 | 9014.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2120.7 | 1.709 | 2.774 | 3.321 | 0.5556 | 0.8333 | 1.0000 | 809717 | 9116.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63662.2 | 0.047 | 0.103 | 0.197 | 1.0000 | 1.0000 | 1.0000 | 75797 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 61420.4 | 0.049 | 0.114 | 0.184 | 0.6667 | 1.0000 | 0.7767 | 79163 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 42400.3 | 0.074 | 0.219 | 0.425 | 1.0000 | 1.0000 | 1.0000 | 258694 | 487.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 39502.6 | 0.078 | 0.235 | 0.496 | 0.6667 | 1.0000 | 0.7751 | 259984 | 506.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30855.9 | 0.110 | 0.256 | 0.578 | 1.0000 | 1.0000 | 1.0000 | 350823 | 518.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 31307.0 | 0.109 | 0.244 | 0.561 | 0.6667 | 1.0000 | 0.7654 | 350843 | 521.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 76233.8 | 0.038 | 0.086 | 0.210 | 0.5556 | 0.6667 | 1.0000 | 70212 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 59092.0 | 0.048 | 0.135 | 0.251 | 0.5556 | 0.8333 | 1.0000 | 96255 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2158.4 | 1.686 | 2.661 | 3.539 | 1.0000 | 1.0000 | 1.0000 | 761258 | 8919.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2103.7 | 1.693 | 2.842 | 4.145 | 0.6667 | 1.0000 | 0.7767 | 762225 | 8969.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2051.2 | 1.702 | 3.067 | 4.413 | 1.0000 | 1.0000 | 1.0000 | 946228 | 8972.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2071.7 | 1.701 | 2.916 | 4.584 | 0.6667 | 1.0000 | 0.7751 | 950641 | 9005.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2288.1 | 1.537 | 2.641 | 4.954 | 1.0000 | 1.0000 | 1.0000 | 982698 | 8162.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3322.0 | 1.041 | 1.779 | 4.258 | 0.6667 | 1.0000 | 0.7654 | 771250 | 5564.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2149.3 | 1.678 | 2.725 | 3.353 | 0.5556 | 0.6667 | 1.0000 | 744180 | 8810.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2138.0 | 1.690 | 2.747 | 3.420 | 0.5556 | 0.8333 | 1.0000 | 769486 | 8914.2 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 141701.1 | 0.019 | 0.046 | 0.059 | 1.0000 | 1.0000 | 1.0000 | 38920 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 126223.3 | 0.021 | 0.053 | 0.075 | 0.6667 | 1.0000 | 0.7767 | 43653 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43330.5 | 0.039 | 0.450 | 1.038 | 1.0000 | 1.0000 | 1.0000 | 229486 | 262.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 42311.6 | 0.041 | 0.391 | 1.406 | 0.6667 | 1.0000 | 0.7751 | 232162 | 295.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 30164.2 | 0.116 | 0.225 | 0.583 | 1.0000 | 1.0000 | 1.0000 | 325471 | 322.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 28266.7 | 0.121 | 0.233 | 0.769 | 0.6667 | 1.0000 | 0.7654 | 325421 | 327.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 177245.1 | 0.015 | 0.031 | 0.053 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 122053.3 | 0.022 | 0.055 | 0.111 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 401.4 | 2.380 | 2.875 | 3.801 | 1.0000 | 1.0000 | 1.0000 | 1162712 | 12331.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 404.8 | 2.377 | 2.816 | 3.420 | 0.6667 | 1.0000 | 0.7767 | 1158973 | 12385.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 406.2 | 2.375 | 2.780 | 3.030 | 1.0000 | 1.0000 | 1.0000 | 1332945 | 12377.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 405.9 | 2.374 | 2.791 | 3.261 | 0.6667 | 1.0000 | 0.7751 | 1333660 | 12358.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 441.7 | 2.172 | 2.582 | 3.152 | 1.0000 | 1.0000 | 1.0000 | 1347073 | 11246.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 591.6 | 1.629 | 1.920 | 2.565 | 0.6667 | 1.0000 | 0.7654 | 1099506 | 8025.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 406.1 | 2.382 | 2.812 | 3.362 | 0.5556 | 0.7222 | 1.0000 | 1183029 | 12221.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 413.8 | 2.336 | 2.714 | 3.060 | 0.5556 | 0.8333 | 1.0000 | 1187624 | 12302.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 499.7 | 1.939 | 2.351 | 2.671 | 1.0000 | 1.0000 | 1.0000 | 922075 | 11115.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 498.9 | 1.944 | 2.349 | 2.645 | 0.6667 | 1.0000 | 0.7767 | 915919 | 11155.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 500.7 | 1.941 | 2.323 | 2.691 | 1.0000 | 1.0000 | 1.0000 | 1099410 | 11169.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 560.7 | 1.698 | 2.166 | 2.508 | 0.6667 | 1.0000 | 0.7751 | 1012546 | 9796.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 677.2 | 1.315 | 1.960 | 2.360 | 1.0000 | 1.0000 | 1.0000 | 963063 | 8076.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 961.0 | 0.927 | 1.344 | 1.677 | 0.6667 | 1.0000 | 0.7654 | 754529 | 5640.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 501.5 | 1.932 | 2.347 | 2.608 | 0.5556 | 0.6667 | 1.0000 | 912238 | 10952.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 498.8 | 1.943 | 2.362 | 2.644 | 0.5556 | 0.8333 | 1.0000 | 931915 | 11058.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 430.0 | 2.239 | 2.778 | 3.238 | 1.0000 | 1.0000 | 1.0000 | 1073093 | 11626.5 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 426.9 | 2.249 | 2.752 | 3.694 | 1.0000 | 1.0000 | 1.0000 | 1260083 | 11701.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 452.9 | 2.107 | 2.653 | 3.191 | 1.0000 | 1.0000 | 1.0000 | 1283223 | 10724.2 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 358.9 | 2.690 | 3.276 | 3.738 | 0.5556 | 0.6667 | 1.0000 | 1288829 | 12107.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1440.3 | 2.455 | 4.163 | 6.520 | 1.0000 | 1.0000 | 1.0000 | 1162272 | 12334.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1451.4 | 2.439 | 4.248 | 6.680 | 0.6667 | 1.0000 | 0.7767 | 1159427 | 12387.9 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1469.2 | 2.424 | 4.310 | 6.097 | 1.0000 | 1.0000 | 1.0000 | 1333039 | 12380.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1421.1 | 2.502 | 4.289 | 7.074 | 0.6667 | 1.0000 | 0.7751 | 1333880 | 12360.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1559.6 | 2.242 | 4.037 | 6.178 | 1.0000 | 1.0000 | 1.0000 | 1346916 | 11249.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2022.5 | 1.710 | 3.229 | 4.445 | 0.6667 | 1.0000 | 0.7654 | 1099571 | 8027.5 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1464.1 | 2.448 | 4.040 | 6.067 | 0.5556 | 0.7222 | 1.0000 | 1183587 | 12224.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1505.2 | 2.397 | 3.691 | 6.258 | 0.5556 | 0.8333 | 1.0000 | 1187619 | 12305.0 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1808.6 | 2.025 | 3.121 | 3.586 | 1.0000 | 1.0000 | 1.0000 | 922700 | 11116.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1787.1 | 2.033 | 3.279 | 3.882 | 0.6667 | 1.0000 | 0.7767 | 917077 | 11157.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1802.7 | 2.039 | 3.051 | 3.674 | 1.0000 | 1.0000 | 1.0000 | 1101796 | 11172.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2084.0 | 1.826 | 2.784 | 3.427 | 0.6667 | 1.0000 | 0.7751 | 1013666 | 9798.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2442.5 | 1.456 | 2.618 | 3.499 | 1.0000 | 1.0000 | 1.0000 | 963412 | 8077.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3481.4 | 1.020 | 1.853 | 2.432 | 0.6667 | 1.0000 | 0.7654 | 754952 | 5642.4 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1810.1 | 2.028 | 3.102 | 3.473 | 0.5556 | 0.6667 | 1.0000 | 913603 | 10954.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1805.0 | 2.009 | 3.207 | 3.860 | 0.5556 | 0.8333 | 1.0000 | 932444 | 11060.3 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1541.5 | 2.330 | 3.730 | 4.851 | 1.0000 | 1.0000 | 1.0000 | 1072557 | 11628.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1518.4 | 2.355 | 3.899 | 5.499 | 1.0000 | 1.0000 | 1.0000 | 1259034 | 11703.3 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1679.7 | 2.109 | 3.626 | 6.019 | 1.0000 | 1.0000 | 1.0000 | 1283511 | 10726.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1259.0 | 2.909 | 4.465 | 5.315 | 0.5556 | 0.6667 | 1.0000 | 1291218 | 12108.8 |

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
