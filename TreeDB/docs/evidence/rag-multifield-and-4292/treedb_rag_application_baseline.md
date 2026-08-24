# TreeDB retained RAG application baseline (#4289)

Authority: `M1_RETAINED_BASELINE`; schema: `treedb_rag_application_baseline/v3`. This is the repaired M1 application baseline, not the historical C1 claim.

## Exact bindings

- product base: `363a550c368baa23b6ef7c4078f734bd236375a1`
- harness revision: `f13a7686bb6efb020772f49f4fcd8fe206ecab76`
- binary SHA-256: `8c4dc8289f2abd309f659cbf2df8c149e10394953eb50b32cecdac817641ff4d`
- fixture SHA-256: `df71c11510a64b09a4b991a17a9062c8090dddf2d0a1e993b19bebd37d4c5db2`
- config SHA-256: `1bac1adb8f5bfd7037ae0e832656d448c1461c21bd302d1287d987a3a7bb2a0e`
- semantic vectors SHA-256: `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`
- hashing regression SHA-256: `2cb6a7f2b28b5335a717f4e4f601ffff65f1f21220504a0d88733e514146240f`
- Go/host: `go1.26.0` `darwin/arm64` `Michaels-Laptop.local`
- command: `/tmp/treedb_rag_benchmark_f13a7686b -out-dir /tmp/gomap-4292-artifacts -dir /tmp/gomap-4292-db -product-base-sha 363a550c368baa23b6ef7c4078f734bd236375a1 -harness-revision f13a7686bb6efb020772f49f4fcd8fe206ecab76 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4292 candidate`

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
| 0 | 18 | 54 | 0.066214 | 271.84 | 815.53 | 2342546 | 4810 | 3748800 | true |
| 1 | 18 | 54 | 0.044480 | 404.68 | 1214.03 | 1851102 | 4683 | 3748827 | true |
| 2 | 18 | 54 | 0.037215 | 483.67 | 1451.01 | 1846921 | 4678 | 3748827 | true |
| 3 | 18 | 54 | 0.039407 | 456.77 | 1370.32 | 1852746 | 4674 | 3748827 | true |
| 4 | 18 | 54 | 0.044902 | 400.87 | 1202.61 | 1846088 | 4673 | 3748827 | true |

Median/p95 docs/s: **404.68 / 478.29**. Median/p95 B/source: **1851102 / 2244586**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **465.38**, B/source <= **1665992**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents. Declared bounded scalar-intersection or parent-collapse exhaustions are scored with nonrelevant empty ranks through TopK; any other short ranking fails closed.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4662.8 | 0.209 | 0.230 | 0.335 | 1.0000 | 1.0000 | 1.0000 | 253432 | 2276.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4379.2 | 0.216 | 0.237 | 0.293 | 0.6667 | 1.0000 | 0.7767 | 259170 | 2325.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4680.3 | 0.211 | 0.229 | 0.256 | 1.0000 | 1.0000 | 1.0000 | 233651 | 2296.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4702.9 | 0.209 | 0.228 | 0.248 | 0.6667 | 1.0000 | 0.7751 | 236785 | 2329.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5179.4 | 0.190 | 0.209 | 0.223 | 1.0000 | 1.0000 | 1.0000 | 208257 | 2159.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 6747.5 | 0.145 | 0.161 | 0.183 | 0.6667 | 1.0000 | 0.7654 | 166726 | 1645.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4582.4 | 0.211 | 0.239 | 0.321 | 0.5556 | 0.8333 | 1.0000 | 277762 | 2167.2 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4510.4 | 0.215 | 0.241 | 0.382 | 0.5556 | 1.0000 | 1.0000 | 286651 | 2237.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 17967.5 | 0.054 | 0.062 | 0.070 | 1.0000 | 1.0000 | 1.0000 | 107871 | 512.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17577.8 | 0.056 | 0.063 | 0.074 | 0.6667 | 1.0000 | 0.7767 | 113671 | 563.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 19462.8 | 0.049 | 0.058 | 0.068 | 1.0000 | 1.0000 | 1.0000 | 91870 | 539.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 18785.1 | 0.051 | 0.059 | 0.071 | 0.6667 | 1.0000 | 0.7751 | 94966 | 572.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19971.4 | 0.049 | 0.056 | 0.062 | 1.0000 | 1.0000 | 1.0000 | 80587 | 572.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19703.3 | 0.049 | 0.056 | 0.061 | 0.6667 | 1.0000 | 0.7654 | 80516 | 577.7 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17850.4 | 0.054 | 0.064 | 0.081 | 0.5556 | 0.8333 | 1.0000 | 132958 | 404.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 15696.7 | 0.058 | 0.071 | 0.282 | 0.5556 | 1.0000 | 1.0000 | 142011 | 475.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4798.2 | 0.202 | 0.222 | 0.365 | 1.0000 | 1.0000 | 1.0000 | 221804 | 2218.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4716.9 | 0.204 | 0.229 | 0.361 | 0.6667 | 1.0000 | 0.7767 | 221910 | 2253.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 4815.0 | 0.200 | 0.220 | 0.416 | 1.0000 | 1.0000 | 1.0000 | 208758 | 2250.2 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5360.4 | 0.174 | 0.212 | 0.242 | 0.6667 | 1.0000 | 0.7751 | 192647 | 2037.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 6269.6 | 0.143 | 0.196 | 0.242 | 1.0000 | 1.0000 | 1.0000 | 159878 | 1769.5 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 8067.8 | 0.113 | 0.150 | 0.185 | 0.6667 | 1.0000 | 0.7654 | 127242 | 1367.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5079.3 | 0.192 | 0.206 | 0.273 | 0.5556 | 0.6667 | 1.0000 | 215813 | 2065.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4807.9 | 0.202 | 0.221 | 0.323 | 0.5556 | 0.8333 | 1.0000 | 241507 | 2169.0 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19850.6 | 0.048 | 0.055 | 0.071 | 1.0000 | 1.0000 | 1.0000 | 75667 | 455.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19398.7 | 0.050 | 0.055 | 0.074 | 0.6667 | 1.0000 | 0.7767 | 79059 | 494.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 21100.1 | 0.046 | 0.050 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 65926 | 493.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19811.5 | 0.049 | 0.056 | 0.069 | 0.6667 | 1.0000 | 0.7751 | 67202 | 512.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 21428.1 | 0.045 | 0.051 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 61553 | 529.1 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 21069.6 | 0.046 | 0.052 | 0.054 | 0.6667 | 1.0000 | 0.7654 | 61517 | 532.8 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24343.0 | 0.039 | 0.047 | 0.057 | 0.5556 | 0.6667 | 1.0000 | 70065 | 302.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19597.6 | 0.049 | 0.056 | 0.096 | 0.5556 | 0.8333 | 1.0000 | 96097 | 406.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5578.2 | 0.174 | 0.189 | 0.211 | 1.0000 | 1.0000 | 1.0000 | 183474 | 1974.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5470.5 | 0.178 | 0.193 | 0.248 | 0.6667 | 1.0000 | 0.7767 | 187902 | 2023.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5476.5 | 0.177 | 0.193 | 0.273 | 1.0000 | 1.0000 | 1.0000 | 177590 | 2026.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 5404.1 | 0.180 | 0.197 | 0.249 | 0.6667 | 1.0000 | 0.7751 | 180296 | 2059.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5758.1 | 0.165 | 0.184 | 0.435 | 1.0000 | 1.0000 | 1.0000 | 161697 | 1919.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 7753.5 | 0.126 | 0.139 | 0.170 | 0.6667 | 1.0000 | 0.7654 | 120515 | 1406.7 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5749.7 | 0.169 | 0.182 | 0.222 | 0.5556 | 0.8889 | 1.0000 | 168720 | 1862.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5558.0 | 0.175 | 0.189 | 0.245 | 0.5556 | 0.9444 | 1.0000 | 194045 | 1966.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 51221.1 | 0.018 | 0.023 | 0.041 | 1.0000 | 1.0000 | 1.0000 | 37673 | 210.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43458.6 | 0.021 | 0.026 | 0.035 | 0.6667 | 1.0000 | 0.7767 | 42375 | 260.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43568.5 | 0.021 | 0.026 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 35526 | 268.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40327.8 | 0.023 | 0.029 | 0.050 | 0.6667 | 1.0000 | 0.7751 | 38187 | 300.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 37500.6 | 0.025 | 0.031 | 0.041 | 1.0000 | 1.0000 | 1.0000 | 35022 | 333.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 36146.1 | 0.026 | 0.031 | 0.044 | 0.6667 | 1.0000 | 0.7654 | 34958 | 338.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 67636.0 | 0.012 | 0.019 | 0.048 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 46748.4 | 0.020 | 0.025 | 0.032 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 13067.9 | 0.248 | 0.650 | 0.995 | 1.0000 | 1.0000 | 1.0000 | 253550 | 2276.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13348.1 | 0.251 | 0.478 | 0.880 | 0.6667 | 1.0000 | 0.7767 | 259302 | 2325.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 15749.5 | 0.234 | 0.352 | 0.534 | 1.0000 | 1.0000 | 1.0000 | 233738 | 2296.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 15133.7 | 0.238 | 0.423 | 0.548 | 0.6667 | 1.0000 | 0.7751 | 236892 | 2329.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 16917.5 | 0.215 | 0.351 | 0.519 | 1.0000 | 1.0000 | 1.0000 | 208371 | 2159.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 21353.9 | 0.168 | 0.296 | 0.392 | 0.6667 | 1.0000 | 0.7654 | 166847 | 1645.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 14011.4 | 0.246 | 0.452 | 0.626 | 0.5556 | 0.8333 | 1.0000 | 277892 | 2167.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13035.6 | 0.255 | 0.533 | 1.066 | 0.5556 | 1.0000 | 1.0000 | 286814 | 2237.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 49645.2 | 0.068 | 0.139 | 0.176 | 1.0000 | 1.0000 | 1.0000 | 108013 | 512.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45762.6 | 0.073 | 0.150 | 0.197 | 0.6667 | 1.0000 | 0.7767 | 113820 | 563.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 50232.5 | 0.066 | 0.129 | 0.190 | 1.0000 | 1.0000 | 1.0000 | 92006 | 539.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 49916.7 | 0.067 | 0.132 | 0.186 | 0.6667 | 1.0000 | 0.7751 | 95086 | 572.1 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 50310.9 | 0.067 | 0.134 | 0.177 | 1.0000 | 1.0000 | 1.0000 | 80709 | 572.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 48675.6 | 0.068 | 0.140 | 0.200 | 0.6667 | 1.0000 | 0.7654 | 80625 | 577.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 44725.7 | 0.074 | 0.158 | 0.225 | 0.5556 | 0.8333 | 1.0000 | 133097 | 404.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41669.7 | 0.080 | 0.164 | 0.314 | 0.5556 | 1.0000 | 1.0000 | 142133 | 475.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15865.1 | 0.213 | 0.482 | 0.666 | 1.0000 | 1.0000 | 1.0000 | 221995 | 2218.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15775.0 | 0.215 | 0.472 | 0.642 | 0.6667 | 1.0000 | 0.7767 | 222143 | 2254.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 14385.5 | 0.219 | 0.548 | 0.732 | 1.0000 | 1.0000 | 1.0000 | 208900 | 2250.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 17149.9 | 0.196 | 0.467 | 0.649 | 0.6667 | 1.0000 | 0.7751 | 192847 | 2038.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 20703.3 | 0.153 | 0.368 | 0.552 | 1.0000 | 1.0000 | 1.0000 | 160088 | 1769.8 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 25384.2 | 0.128 | 0.300 | 0.484 | 0.6667 | 1.0000 | 0.7654 | 127402 | 1368.0 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16445.6 | 0.205 | 0.474 | 0.636 | 0.5556 | 0.6667 | 1.0000 | 216395 | 2065.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 16519.9 | 0.212 | 0.457 | 0.629 | 0.5556 | 0.8333 | 1.0000 | 241835 | 2169.4 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63183.7 | 0.047 | 0.117 | 0.199 | 1.0000 | 1.0000 | 1.0000 | 75839 | 455.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 62570.5 | 0.049 | 0.100 | 0.200 | 0.6667 | 1.0000 | 0.7767 | 79204 | 494.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 54209.7 | 0.050 | 0.145 | 0.221 | 1.0000 | 1.0000 | 1.0000 | 66085 | 493.2 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 58794.3 | 0.048 | 0.133 | 0.266 | 0.6667 | 1.0000 | 0.7751 | 67340 | 512.8 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 57860.7 | 0.050 | 0.120 | 0.236 | 1.0000 | 1.0000 | 1.0000 | 61664 | 529.2 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 56396.5 | 0.051 | 0.130 | 0.237 | 0.6667 | 1.0000 | 0.7654 | 61650 | 532.8 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 78264.0 | 0.038 | 0.089 | 0.189 | 0.5556 | 0.6667 | 1.0000 | 70200 | 302.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 64686.2 | 0.047 | 0.103 | 0.222 | 0.5556 | 0.8333 | 1.0000 | 96297 | 406.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18395.8 | 0.183 | 0.406 | 0.560 | 1.0000 | 1.0000 | 1.0000 | 183608 | 1974.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17756.6 | 0.188 | 0.389 | 0.691 | 0.6667 | 1.0000 | 0.7767 | 187937 | 2023.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 18120.1 | 0.187 | 0.361 | 0.602 | 1.0000 | 1.0000 | 1.0000 | 177675 | 2026.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 18087.4 | 0.190 | 0.333 | 0.540 | 0.6667 | 1.0000 | 0.7751 | 180345 | 2059.2 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 18616.4 | 0.180 | 0.393 | 0.584 | 1.0000 | 1.0000 | 1.0000 | 161774 | 1919.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 27293.7 | 0.129 | 0.212 | 0.368 | 0.6667 | 1.0000 | 0.7654 | 120562 | 1406.8 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18718.4 | 0.178 | 0.276 | 0.523 | 0.5556 | 0.8889 | 1.0000 | 168728 | 1863.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18294.1 | 0.185 | 0.387 | 0.675 | 0.5556 | 0.9444 | 1.0000 | 194062 | 1966.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 125824.2 | 0.020 | 0.057 | 0.118 | 1.0000 | 1.0000 | 1.0000 | 37700 | 210.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 112440.5 | 0.020 | 0.065 | 0.107 | 0.6667 | 1.0000 | 0.7767 | 42403 | 260.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 101453.5 | 0.024 | 0.076 | 0.142 | 1.0000 | 1.0000 | 1.0000 | 35553 | 268.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 101440.7 | 0.027 | 0.071 | 0.141 | 0.6667 | 1.0000 | 0.7751 | 38255 | 300.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 85621.3 | 0.035 | 0.087 | 0.138 | 1.0000 | 1.0000 | 1.0000 | 35050 | 333.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 87213.4 | 0.035 | 0.090 | 0.147 | 0.6667 | 1.0000 | 0.7654 | 35006 | 338.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 182513.1 | 0.013 | 0.026 | 0.064 | 0.5556 | 0.8889 | 1.0000 | 22646 | 99.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 128413.1 | 0.019 | 0.057 | 0.074 | 0.5556 | 0.9444 | 1.0000 | 47945 | 202.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1390.3 | 0.694 | 0.763 | 0.887 | 1.0000 | 1.0000 | 1.0000 | 557357 | 5374.3 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1378.9 | 0.703 | 0.784 | 1.400 | 0.6667 | 1.0000 | 0.7767 | 563711 | 5429.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1404.9 | 0.690 | 0.739 | 1.171 | 1.0000 | 1.0000 | 1.0000 | 539528 | 5416.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1418.0 | 0.690 | 0.736 | 1.218 | 0.6667 | 1.0000 | 0.7751 | 541298 | 5400.6 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1459.8 | 0.667 | 0.744 | 1.191 | 1.0000 | 1.0000 | 1.0000 | 501579 | 4992.0 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1652.0 | 0.583 | 0.655 | 0.756 | 0.6667 | 1.0000 | 0.7654 | 426001 | 3855.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1402.1 | 0.695 | 0.767 | 0.979 | 0.5556 | 0.8333 | 1.0000 | 582553 | 5256.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1389.3 | 0.695 | 0.756 | 1.269 | 0.5556 | 1.0000 | 1.0000 | 590536 | 5329.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2212.7 | 0.447 | 0.476 | 0.646 | 1.0000 | 1.0000 | 1.0000 | 344419 | 4168.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2209.0 | 0.448 | 0.476 | 0.648 | 0.6667 | 1.0000 | 0.7767 | 345434 | 4206.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2206.3 | 0.447 | 0.481 | 0.785 | 1.0000 | 1.0000 | 1.0000 | 333357 | 4222.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2374.0 | 0.406 | 0.470 | 0.643 | 0.6667 | 1.0000 | 0.7751 | 303247 | 3774.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2676.1 | 0.347 | 0.441 | 0.658 | 1.0000 | 1.0000 | 1.0000 | 254190 | 3221.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3620.0 | 0.274 | 0.352 | 0.430 | 0.6667 | 1.0000 | 0.7654 | 200954 | 2407.4 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2281.2 | 0.433 | 0.461 | 0.634 | 0.5556 | 0.6667 | 1.0000 | 336637 | 4004.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2233.5 | 0.444 | 0.467 | 0.693 | 0.5556 | 0.8333 | 1.0000 | 362671 | 4111.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1576.4 | 0.623 | 0.665 | 1.016 | 1.0000 | 1.0000 | 1.0000 | 470218 | 4670.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1531.4 | 0.635 | 0.698 | 0.945 | 1.0000 | 1.0000 | 1.0000 | 465397 | 4744.0 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1600.8 | 0.609 | 0.657 | 1.049 | 1.0000 | 1.0000 | 1.0000 | 437226 | 4468.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 946.0 | 1.038 | 1.139 | 1.448 | 0.5556 | 0.8889 | 1.0000 | 681394 | 5148.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4471.1 | 0.761 | 1.643 | 2.350 | 1.0000 | 1.0000 | 1.0000 | 558131 | 5377.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4495.7 | 0.755 | 1.588 | 2.323 | 0.6667 | 1.0000 | 0.7767 | 565386 | 5432.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 4659.7 | 0.740 | 1.486 | 2.262 | 1.0000 | 1.0000 | 1.0000 | 539135 | 5419.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 4703.5 | 0.736 | 1.603 | 2.660 | 0.6667 | 1.0000 | 0.7751 | 541540 | 5403.9 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 4715.6 | 0.722 | 1.537 | 2.223 | 1.0000 | 1.0000 | 1.0000 | 502002 | 4995.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 5352.5 | 0.632 | 1.549 | 2.360 | 0.6667 | 1.0000 | 0.7654 | 426462 | 3858.8 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4494.4 | 0.747 | 1.648 | 2.445 | 0.5556 | 0.8333 | 1.0000 | 582388 | 5259.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4697.7 | 0.744 | 1.477 | 2.235 | 0.5556 | 1.0000 | 1.0000 | 590648 | 5332.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7775.4 | 0.444 | 0.893 | 1.166 | 1.0000 | 1.0000 | 1.0000 | 344918 | 4172.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7754.3 | 0.444 | 0.891 | 1.216 | 0.6667 | 1.0000 | 0.7767 | 346287 | 4210.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 7731.4 | 0.447 | 0.880 | 1.165 | 1.0000 | 1.0000 | 1.0000 | 333661 | 4226.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 8437.7 | 0.424 | 0.765 | 1.289 | 0.6667 | 1.0000 | 0.7751 | 303316 | 3778.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 9723.5 | 0.373 | 0.686 | 1.044 | 1.0000 | 1.0000 | 1.0000 | 254935 | 3226.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 9337.4 | 0.374 | 0.760 | 1.069 | 0.6667 | 1.0000 | 0.7654 | 201703 | 2413.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7767.0 | 0.441 | 0.912 | 1.256 | 0.5556 | 0.6667 | 1.0000 | 337432 | 4007.0 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 8063.3 | 0.431 | 0.862 | 1.128 | 0.5556 | 0.8333 | 1.0000 | 363254 | 4114.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5143.5 | 0.665 | 1.294 | 1.838 | 1.0000 | 1.0000 | 1.0000 | 469508 | 4672.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 5178.8 | 0.651 | 1.276 | 2.148 | 1.0000 | 1.0000 | 1.0000 | 465575 | 4746.8 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 5639.5 | 0.615 | 1.127 | 1.904 | 1.0000 | 1.0000 | 1.0000 | 437063 | 4470.8 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3043.3 | 1.167 | 1.994 | 2.584 | 0.5556 | 0.8889 | 1.0000 | 680732 | 5149.9 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 575.0 | 1.687 | 1.991 | 2.334 | 1.0000 | 1.0000 | 1.0000 | 832953 | 9223.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 571.8 | 1.690 | 1.950 | 2.295 | 0.6667 | 1.0000 | 0.7767 | 829754 | 9272.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 576.8 | 1.679 | 2.035 | 2.353 | 1.0000 | 1.0000 | 1.0000 | 809774 | 9249.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 569.2 | 1.698 | 2.032 | 2.384 | 0.6667 | 1.0000 | 0.7751 | 812825 | 9281.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 629.6 | 1.532 | 1.849 | 2.173 | 1.0000 | 1.0000 | 1.0000 | 740851 | 8413.5 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 921.1 | 1.048 | 1.267 | 1.483 | 0.6667 | 1.0000 | 0.7654 | 527665 | 5815.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 571.5 | 1.692 | 2.072 | 2.448 | 0.5556 | 0.7222 | 1.0000 | 854600 | 9120.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 571.1 | 1.692 | 2.064 | 2.377 | 0.5556 | 0.8333 | 1.0000 | 858710 | 9198.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18212.6 | 0.054 | 0.061 | 0.069 | 1.0000 | 1.0000 | 1.0000 | 109151 | 512.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17537.3 | 0.055 | 0.064 | 0.073 | 0.6667 | 1.0000 | 0.7767 | 114930 | 562.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 19335.0 | 0.050 | 0.059 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 93150 | 539.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 17596.5 | 0.052 | 0.074 | 0.126 | 0.6667 | 1.0000 | 0.7751 | 96208 | 571.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 19885.2 | 0.049 | 0.056 | 0.061 | 1.0000 | 1.0000 | 1.0000 | 81860 | 572.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 19480.0 | 0.050 | 0.057 | 0.064 | 0.6667 | 1.0000 | 0.7654 | 81796 | 577.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17898.3 | 0.054 | 0.062 | 0.076 | 0.5556 | 0.7222 | 1.0000 | 134358 | 409.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16958.5 | 0.058 | 0.066 | 0.074 | 0.5556 | 0.8333 | 1.0000 | 143849 | 488.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 586.6 | 1.665 | 1.957 | 2.369 | 1.0000 | 1.0000 | 1.0000 | 797831 | 9166.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 583.6 | 1.671 | 1.985 | 2.316 | 0.6667 | 1.0000 | 0.7767 | 792223 | 9204.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 578.4 | 1.677 | 2.020 | 2.426 | 1.0000 | 1.0000 | 1.0000 | 780864 | 9203.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 667.1 | 1.398 | 1.803 | 2.099 | 0.6667 | 1.0000 | 0.7751 | 708093 | 8064.7 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 817.3 | 1.057 | 1.609 | 1.912 | 1.0000 | 1.0000 | 1.0000 | 577989 | 6634.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 1199.1 | 0.722 | 1.093 | 1.361 | 0.6667 | 1.0000 | 0.7654 | 390684 | 4611.3 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 581.0 | 1.670 | 2.006 | 2.407 | 0.5556 | 0.6667 | 1.0000 | 790408 | 9013.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 579.1 | 1.680 | 2.009 | 2.367 | 0.5556 | 0.8333 | 1.0000 | 809252 | 9116.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20094.7 | 0.048 | 0.055 | 0.073 | 1.0000 | 1.0000 | 1.0000 | 75672 | 455.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19398.2 | 0.050 | 0.057 | 0.077 | 0.6667 | 1.0000 | 0.7767 | 79044 | 494.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 20350.7 | 0.048 | 0.053 | 0.074 | 1.0000 | 1.0000 | 1.0000 | 65911 | 493.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 19703.4 | 0.049 | 0.057 | 0.064 | 0.6667 | 1.0000 | 0.7751 | 67207 | 512.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 20781.3 | 0.045 | 0.053 | 0.065 | 1.0000 | 1.0000 | 1.0000 | 61538 | 529.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 20316.0 | 0.047 | 0.054 | 0.064 | 0.6667 | 1.0000 | 0.7654 | 61523 | 532.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24059.7 | 0.039 | 0.046 | 0.054 | 0.5556 | 0.6667 | 1.0000 | 70041 | 302.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19240.4 | 0.049 | 0.056 | 0.097 | 0.5556 | 0.8333 | 1.0000 | 96096 | 406.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 594.8 | 1.640 | 1.913 | 2.283 | 1.0000 | 1.0000 | 1.0000 | 761095 | 8921.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 593.3 | 1.645 | 1.877 | 2.292 | 0.6667 | 1.0000 | 0.7767 | 762141 | 8971.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 587.3 | 1.650 | 2.002 | 2.512 | 1.0000 | 1.0000 | 1.0000 | 753506 | 8978.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 582.7 | 1.661 | 1.993 | 2.466 | 0.6667 | 1.0000 | 0.7751 | 757949 | 9011.6 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 652.4 | 1.488 | 1.750 | 2.058 | 1.0000 | 1.0000 | 1.0000 | 693525 | 8173.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 962.8 | 1.008 | 1.112 | 1.421 | 0.6667 | 1.0000 | 0.7654 | 482156 | 5575.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 589.3 | 1.654 | 1.972 | 2.285 | 0.5556 | 0.6667 | 1.0000 | 744145 | 8810.0 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 589.3 | 1.652 | 1.970 | 2.267 | 0.5556 | 0.8333 | 1.0000 | 769444 | 8913.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 46864.1 | 0.020 | 0.026 | 0.032 | 1.0000 | 1.0000 | 1.0000 | 38931 | 210.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43935.3 | 0.021 | 0.027 | 0.031 | 0.6667 | 1.0000 | 0.7767 | 43644 | 260.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 43927.1 | 0.022 | 0.025 | 0.029 | 1.0000 | 1.0000 | 1.0000 | 36791 | 268.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 40054.1 | 0.024 | 0.028 | 0.032 | 0.6667 | 1.0000 | 0.7751 | 39487 | 301.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 38093.5 | 0.025 | 0.031 | 0.033 | 1.0000 | 1.0000 | 1.0000 | 36287 | 333.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 37495.2 | 0.026 | 0.030 | 0.034 | 0.6667 | 1.0000 | 0.7654 | 36223 | 338.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 60910.6 | 0.015 | 0.017 | 0.023 | 0.5556 | 0.6667 | 1.0000 | 23926 | 99.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39227.9 | 0.023 | 0.031 | 0.047 | 0.5556 | 0.8333 | 1.0000 | 49235 | 202.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1992.0 | 1.761 | 2.930 | 4.962 | 1.0000 | 1.0000 | 1.0000 | 833108 | 9224.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1977.1 | 1.785 | 2.844 | 4.008 | 0.6667 | 1.0000 | 0.7767 | 829863 | 9272.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2016.9 | 1.763 | 2.848 | 4.404 | 1.0000 | 1.0000 | 1.0000 | 809839 | 9249.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2003.1 | 1.775 | 2.900 | 4.318 | 0.6667 | 1.0000 | 0.7751 | 812905 | 9281.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2287.7 | 1.595 | 2.336 | 3.665 | 1.0000 | 1.0000 | 1.0000 | 740926 | 8413.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3243.7 | 1.097 | 1.769 | 3.765 | 0.6667 | 1.0000 | 0.7654 | 527802 | 5815.2 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1989.0 | 1.781 | 2.996 | 4.475 | 0.5556 | 0.7222 | 1.0000 | 854779 | 9120.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1922.0 | 1.795 | 3.182 | 5.734 | 0.5556 | 0.8333 | 1.0000 | 858817 | 9198.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47575.6 | 0.072 | 0.147 | 0.174 | 1.0000 | 1.0000 | 1.0000 | 109293 | 512.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45870.2 | 0.074 | 0.143 | 0.191 | 0.6667 | 1.0000 | 0.7767 | 115066 | 562.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 51147.6 | 0.064 | 0.126 | 0.186 | 1.0000 | 1.0000 | 1.0000 | 93293 | 539.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 45578.6 | 0.074 | 0.149 | 0.193 | 0.6667 | 1.0000 | 0.7751 | 96358 | 571.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 50896.3 | 0.066 | 0.132 | 0.177 | 1.0000 | 1.0000 | 1.0000 | 81990 | 572.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 49791.2 | 0.067 | 0.135 | 0.186 | 0.6667 | 1.0000 | 0.7654 | 81906 | 577.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 45443.8 | 0.076 | 0.152 | 0.184 | 0.5556 | 0.7222 | 1.0000 | 134507 | 409.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 44041.3 | 0.079 | 0.148 | 0.176 | 0.5556 | 0.8333 | 1.0000 | 144005 | 488.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2106.7 | 1.719 | 2.856 | 3.289 | 1.0000 | 1.0000 | 1.0000 | 798396 | 9167.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2114.2 | 1.723 | 2.694 | 3.389 | 0.6667 | 1.0000 | 0.7767 | 792683 | 9204.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 2101.6 | 1.717 | 2.753 | 3.363 | 1.0000 | 1.0000 | 1.0000 | 781361 | 9203.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2418.3 | 1.476 | 2.413 | 3.164 | 0.6667 | 1.0000 | 0.7751 | 708582 | 8065.3 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2956.7 | 1.117 | 2.122 | 2.738 | 1.0000 | 1.0000 | 1.0000 | 578271 | 6634.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 4357.9 | 0.738 | 1.475 | 2.080 | 0.6667 | 1.0000 | 0.7654 | 390911 | 4611.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2124.1 | 1.706 | 2.878 | 3.444 | 0.5556 | 0.6667 | 1.0000 | 791758 | 9014.2 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2124.1 | 1.720 | 2.667 | 3.280 | 0.5556 | 0.8333 | 1.0000 | 809745 | 9116.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 65379.0 | 0.047 | 0.095 | 0.192 | 1.0000 | 1.0000 | 1.0000 | 75822 | 455.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 59170.8 | 0.050 | 0.125 | 0.223 | 0.6667 | 1.0000 | 0.7767 | 79201 | 494.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | disabled | 60694.1 | 0.049 | 0.110 | 0.230 | 1.0000 | 1.0000 | 1.0000 | 66082 | 493.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 60973.7 | 0.048 | 0.110 | 0.191 | 0.6667 | 1.0000 | 0.7751 | 67358 | 512.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 56661.5 | 0.053 | 0.132 | 0.231 | 1.0000 | 1.0000 | 1.0000 | 61661 | 529.1 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 59880.1 | 0.049 | 0.094 | 0.186 | 0.6667 | 1.0000 | 0.7654 | 61666 | 532.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 65343.9 | 0.041 | 0.112 | 0.168 | 0.5556 | 0.6667 | 1.0000 | 70212 | 302.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 61063.8 | 0.048 | 0.123 | 0.246 | 0.5556 | 0.8333 | 1.0000 | 96247 | 406.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2146.2 | 1.688 | 2.657 | 3.563 | 1.0000 | 1.0000 | 1.0000 | 761466 | 8921.9 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2101.2 | 1.699 | 2.774 | 4.176 | 0.6667 | 1.0000 | 0.7767 | 762323 | 8971.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 2125.4 | 1.694 | 2.693 | 3.756 | 1.0000 | 1.0000 | 1.0000 | 753705 | 8978.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2097.7 | 1.704 | 2.799 | 4.252 | 0.6667 | 1.0000 | 0.7751 | 758005 | 9011.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2328.6 | 1.529 | 2.531 | 3.943 | 1.0000 | 1.0000 | 1.0000 | 693612 | 8173.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3487.9 | 1.032 | 1.666 | 2.513 | 0.6667 | 1.0000 | 0.7654 | 482160 | 5575.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2177.3 | 1.681 | 2.681 | 3.249 | 0.5556 | 0.6667 | 1.0000 | 744176 | 8810.4 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2048.9 | 1.699 | 3.077 | 4.097 | 0.5556 | 0.8333 | 1.0000 | 769489 | 8914.2 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 136139.8 | 0.019 | 0.050 | 0.065 | 1.0000 | 1.0000 | 1.0000 | 38952 | 210.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 132445.4 | 0.021 | 0.046 | 0.060 | 0.6667 | 1.0000 | 0.7767 | 43685 | 260.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | disabled | 113956.3 | 0.026 | 0.060 | 0.079 | 1.0000 | 1.0000 | 1.0000 | 36832 | 268.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red | enabled_cap_2 | 107947.9 | 0.027 | 0.065 | 0.094 | 0.6667 | 1.0000 | 0.7751 | 39528 | 301.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 91306.2 | 0.032 | 0.083 | 0.110 | 1.0000 | 1.0000 | 1.0000 | 36321 | 333.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 94896.7 | 0.032 | 0.073 | 0.098 | 0.6667 | 1.0000 | 0.7654 | 36257 | 338.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 133928.9 | 0.016 | 0.051 | 0.065 | 0.5556 | 0.6667 | 1.0000 | 23927 | 99.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 130584.6 | 0.021 | 0.041 | 0.102 | 0.5556 | 0.8333 | 1.0000 | 49236 | 202.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 409.4 | 2.355 | 2.794 | 3.330 | 1.0000 | 1.0000 | 1.0000 | 1162119 | 12333.3 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 404.3 | 2.382 | 2.823 | 3.243 | 0.6667 | 1.0000 | 0.7767 | 1159035 | 12387.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 411.2 | 2.352 | 2.746 | 3.199 | 1.0000 | 1.0000 | 1.0000 | 1140543 | 12383.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 403.1 | 2.392 | 2.829 | 3.491 | 0.6667 | 1.0000 | 0.7751 | 1141452 | 12364.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 437.8 | 2.195 | 2.631 | 3.075 | 1.0000 | 1.0000 | 1.0000 | 1057716 | 11257.8 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 584.0 | 1.651 | 1.958 | 2.580 | 0.6667 | 1.0000 | 0.7654 | 810348 | 8036.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 405.7 | 2.381 | 2.846 | 3.583 | 0.5556 | 0.7222 | 1.0000 | 1183334 | 12221.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 410.3 | 2.356 | 2.769 | 3.748 | 0.5556 | 0.8333 | 1.0000 | 1187927 | 12302.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 500.6 | 1.942 | 2.304 | 2.647 | 1.0000 | 1.0000 | 1.0000 | 921001 | 11116.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 496.7 | 1.952 | 2.385 | 2.711 | 0.6667 | 1.0000 | 0.7767 | 917098 | 11157.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 495.4 | 1.951 | 2.375 | 2.706 | 1.0000 | 1.0000 | 1.0000 | 906798 | 11175.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 563.9 | 1.666 | 2.156 | 2.530 | 0.6667 | 1.0000 | 0.7751 | 819157 | 9801.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 686.6 | 1.282 | 1.892 | 2.338 | 1.0000 | 1.0000 | 1.0000 | 673307 | 8086.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 966.7 | 0.917 | 1.336 | 1.554 | 0.6667 | 1.0000 | 0.7654 | 464678 | 5650.9 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 501.1 | 1.935 | 2.367 | 2.632 | 0.5556 | 0.6667 | 1.0000 | 912951 | 10952.9 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 496.1 | 1.947 | 2.367 | 2.665 | 0.5556 | 0.8333 | 1.0000 | 931773 | 11058.4 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 427.5 | 2.245 | 2.816 | 3.462 | 1.0000 | 1.0000 | 1.0000 | 1073120 | 11628.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 426.8 | 2.245 | 2.800 | 3.477 | 1.0000 | 1.0000 | 1.0000 | 1067106 | 11707.2 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 457.3 | 2.098 | 2.648 | 2.974 | 1.0000 | 1.0000 | 1.0000 | 994192 | 10735.1 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 356.2 | 2.702 | 3.329 | 3.887 | 0.5556 | 0.6667 | 1.0000 | 1287067 | 12106.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1445.7 | 2.457 | 4.231 | 7.022 | 1.0000 | 1.0000 | 1.0000 | 1162190 | 12335.5 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1441.4 | 2.444 | 4.190 | 7.088 | 0.6667 | 1.0000 | 0.7767 | 1159479 | 12390.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1459.2 | 2.470 | 4.004 | 5.866 | 1.0000 | 1.0000 | 1.0000 | 1140318 | 12386.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 1452.7 | 2.464 | 4.072 | 6.715 | 0.6667 | 1.0000 | 0.7751 | 1141370 | 12367.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1634.2 | 2.196 | 3.669 | 4.923 | 1.0000 | 1.0000 | 1.0000 | 1058080 | 11260.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 2138.7 | 1.675 | 2.750 | 4.849 | 0.6667 | 1.0000 | 0.7654 | 810546 | 8038.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1457.4 | 2.460 | 4.015 | 5.514 | 0.5556 | 0.7222 | 1.0000 | 1183293 | 12224.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1441.2 | 2.469 | 4.147 | 6.417 | 0.5556 | 0.8333 | 1.0000 | 1187563 | 12304.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1844.7 | 1.975 | 3.017 | 3.487 | 1.0000 | 1.0000 | 1.0000 | 921535 | 11118.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1807.1 | 2.036 | 3.002 | 3.673 | 0.6667 | 1.0000 | 0.7767 | 918182 | 11159.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | disabled | 1802.1 | 2.008 | 3.223 | 3.964 | 1.0000 | 1.0000 | 1.0000 | 908136 | 11177.5 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red | enabled_cap_2 | 2096.1 | 1.856 | 2.685 | 3.384 | 0.6667 | 1.0000 | 0.7751 | 820579 | 9804.1 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 2487.4 | 1.428 | 2.472 | 2.990 | 1.0000 | 1.0000 | 1.0000 | 673131 | 8088.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | enabled_cap_2 | 3625.9 | 0.955 | 1.666 | 2.160 | 0.6667 | 1.0000 | 0.7654 | 464336 | 5652.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1797.8 | 2.019 | 3.158 | 4.052 | 0.5556 | 0.6667 | 1.0000 | 912884 | 10953.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1805.7 | 2.014 | 3.146 | 3.844 | 0.5556 | 0.8333 | 1.0000 | 932006 | 11060.6 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1559.5 | 2.315 | 3.664 | 4.793 | 1.0000 | 1.0000 | 1.0000 | 1073090 | 11630.8 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red | disabled | 1537.0 | 2.339 | 3.706 | 5.616 | 1.0000 | 1.0000 | 1.0000 | 1066624 | 11709.2 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha_workspace_red_updated_ge_2024 | disabled | 1658.0 | 2.136 | 3.640 | 5.144 | 1.0000 | 1.0000 | 1.0000 | 993460 | 10737.2 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1282.5 | 2.877 | 4.256 | 4.892 | 0.5556 | 0.6667 | 1.0000 | 1290396 | 12109.3 |

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
