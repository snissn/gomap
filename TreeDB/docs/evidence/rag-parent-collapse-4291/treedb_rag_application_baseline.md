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
- command: `/tmp/treedb_rag_benchmark_30239761b -out-dir /tmp/gomap-4291-artifacts-302 -dir /tmp/gomap-4291-db-302 -product-base-sha d8ad352965493f6a31a1d50ac70f5d783103c454 -harness-revision 30239761b1e7b90cf66fadf894921757f347c9b1 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; #4291 integrated metadata candidate`

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
| 0 | 18 | 54 | 0.070252 | 256.22 | 768.66 | 2365898 | 4810 | 3748802 | true |
| 1 | 18 | 54 | 0.048002 | 374.98 | 1124.95 | 1843383 | 4682 | 3748827 | true |
| 2 | 18 | 54 | 0.039101 | 460.35 | 1381.04 | 1841428 | 4673 | 3748827 | true |
| 3 | 18 | 54 | 0.043313 | 415.58 | 1246.75 | 1844557 | 4679 | 3748790 | true |
| 4 | 18 | 54 | 0.040467 | 444.81 | 1334.42 | 1857211 | 4676 | 3748827 | true |

Median/p95 docs/s: **415.58 / 457.24**. Median/p95 B/source: **1844557 / 2264160**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **477.92**, B/source <= **1660102**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4639.6 | 0.210 | 0.233 | 0.341 | 1.0000 | 1.0000 | 1.0000 | 253304 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4557.8 | 0.213 | 0.231 | 0.362 | 0.6667 | 1.0000 | 0.7767 | 259027 | 2323.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4624.2 | 0.209 | 0.228 | 0.384 | 0.5556 | 0.8333 | 1.0000 | 277678 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4478.5 | 0.215 | 0.239 | 0.283 | 0.5556 | 1.0000 | 1.0000 | 286543 | 2236.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18070.9 | 0.053 | 0.063 | 0.113 | 1.0000 | 1.0000 | 1.0000 | 107778 | 510.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17135.5 | 0.056 | 0.064 | 0.085 | 0.6667 | 1.0000 | 0.7767 | 113565 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17961.5 | 0.053 | 0.061 | 0.111 | 0.5556 | 0.8333 | 1.0000 | 132850 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16419.6 | 0.056 | 0.069 | 0.153 | 0.5556 | 1.0000 | 1.0000 | 141884 | 474.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4834.5 | 0.201 | 0.218 | 0.286 | 1.0000 | 1.0000 | 1.0000 | 221666 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4765.9 | 0.204 | 0.226 | 0.282 | 0.6667 | 1.0000 | 0.7767 | 221791 | 2251.9 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5058.5 | 0.193 | 0.209 | 0.279 | 0.5556 | 0.6667 | 1.0000 | 215706 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4832.3 | 0.201 | 0.219 | 0.336 | 0.5556 | 0.8333 | 1.0000 | 241353 | 2167.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20128.1 | 0.048 | 0.056 | 0.080 | 1.0000 | 1.0000 | 1.0000 | 75539 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19893.3 | 0.048 | 0.054 | 0.064 | 0.6667 | 1.0000 | 0.7767 | 78917 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24432.5 | 0.039 | 0.047 | 0.059 | 0.5556 | 0.6667 | 1.0000 | 69930 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19639.5 | 0.049 | 0.054 | 0.094 | 0.5556 | 0.8333 | 1.0000 | 95962 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5618.8 | 0.173 | 0.186 | 0.208 | 1.0000 | 1.0000 | 1.0000 | 183359 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5361.3 | 0.179 | 0.196 | 0.257 | 0.6667 | 1.0000 | 0.7767 | 187766 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5754.0 | 0.169 | 0.185 | 0.269 | 0.5556 | 0.8889 | 1.0000 | 168592 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5559.3 | 0.175 | 0.188 | 0.261 | 0.5556 | 0.9444 | 1.0000 | 193921 | 1965.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52818.3 | 0.017 | 0.020 | 0.032 | 1.0000 | 1.0000 | 1.0000 | 37531 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45319.1 | 0.020 | 0.025 | 0.032 | 0.6667 | 1.0000 | 0.7767 | 42240 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 69066.2 | 0.013 | 0.016 | 0.021 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 47266.0 | 0.020 | 0.023 | 0.034 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 13774.3 | 0.244 | 0.502 | 0.665 | 1.0000 | 1.0000 | 1.0000 | 253421 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13819.2 | 0.245 | 0.523 | 0.813 | 0.6667 | 1.0000 | 0.7767 | 259150 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 13366.6 | 0.247 | 0.553 | 0.988 | 0.5556 | 0.8333 | 1.0000 | 277821 | 2166.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13139.2 | 0.250 | 0.510 | 1.127 | 0.5556 | 1.0000 | 1.0000 | 286699 | 2236.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 48299.0 | 0.067 | 0.151 | 0.255 | 1.0000 | 1.0000 | 1.0000 | 107908 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43363.0 | 0.072 | 0.148 | 0.322 | 0.6667 | 1.0000 | 0.7767 | 113701 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 44385.2 | 0.073 | 0.158 | 0.285 | 0.5556 | 0.8333 | 1.0000 | 132997 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 42056.6 | 0.076 | 0.175 | 0.346 | 0.5556 | 1.0000 | 1.0000 | 142042 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15919.9 | 0.212 | 0.483 | 0.705 | 1.0000 | 1.0000 | 1.0000 | 221919 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15800.0 | 0.215 | 0.469 | 0.693 | 0.6667 | 1.0000 | 0.7767 | 222044 | 2252.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16569.9 | 0.205 | 0.459 | 0.678 | 0.5556 | 0.6667 | 1.0000 | 216265 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15978.2 | 0.213 | 0.454 | 0.638 | 0.5556 | 0.8333 | 1.0000 | 241666 | 2168.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 65080.7 | 0.046 | 0.087 | 0.215 | 1.0000 | 1.0000 | 1.0000 | 75711 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60846.2 | 0.049 | 0.126 | 0.194 | 0.6667 | 1.0000 | 0.7767 | 79083 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 79284.5 | 0.038 | 0.078 | 0.207 | 0.5556 | 0.6667 | 1.0000 | 70093 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 50789.2 | 0.050 | 0.142 | 0.310 | 0.5556 | 0.8333 | 1.0000 | 96163 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 16235.3 | 0.192 | 0.443 | 0.635 | 1.0000 | 1.0000 | 1.0000 | 183434 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 18090.1 | 0.188 | 0.407 | 0.622 | 0.6667 | 1.0000 | 0.7767 | 187810 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 19068.4 | 0.179 | 0.378 | 0.554 | 0.5556 | 0.8889 | 1.0000 | 168605 | 1862.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 18358.0 | 0.187 | 0.399 | 0.546 | 0.5556 | 0.9444 | 1.0000 | 193933 | 1965.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 100418.2 | 0.022 | 0.082 | 0.119 | 1.0000 | 1.0000 | 1.0000 | 37579 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 123928.5 | 0.022 | 0.053 | 0.106 | 0.6667 | 1.0000 | 0.7767 | 42315 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 187681.7 | 0.013 | 0.038 | 0.060 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 136341.0 | 0.019 | 0.046 | 0.088 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1406.8 | 0.688 | 0.752 | 1.249 | 1.0000 | 1.0000 | 1.0000 | 558755 | 5370.7 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1397.2 | 0.689 | 0.756 | 1.183 | 0.6667 | 1.0000 | 0.7767 | 562985 | 5425.8 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1395.3 | 0.690 | 0.779 | 1.403 | 0.5556 | 0.8333 | 1.0000 | 582861 | 5256.5 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1414.9 | 0.689 | 0.733 | 1.122 | 0.5556 | 1.0000 | 1.0000 | 590214 | 5328.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2213.4 | 0.447 | 0.471 | 0.597 | 1.0000 | 1.0000 | 1.0000 | 342884 | 4164.3 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2218.7 | 0.446 | 0.473 | 0.655 | 0.6667 | 1.0000 | 0.7767 | 343356 | 4202.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2291.1 | 0.432 | 0.461 | 0.718 | 0.5556 | 0.6667 | 1.0000 | 336006 | 4002.9 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2232.2 | 0.443 | 0.469 | 0.740 | 0.5556 | 0.8333 | 1.0000 | 361836 | 4109.6 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1571.6 | 0.624 | 0.676 | 1.034 | 1.0000 | 1.0000 | 1.0000 | 469682 | 4666.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 945.5 | 1.038 | 1.139 | 1.468 | 0.5556 | 0.8889 | 1.0000 | 681205 | 5148.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4582.7 | 0.737 | 1.670 | 2.496 | 1.0000 | 1.0000 | 1.0000 | 557875 | 5373.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4545.9 | 0.755 | 1.436 | 2.210 | 0.6667 | 1.0000 | 0.7767 | 563152 | 5428.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4624.7 | 0.738 | 1.379 | 3.069 | 0.5556 | 0.8333 | 1.0000 | 582658 | 5259.2 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4417.4 | 0.766 | 1.542 | 2.202 | 0.5556 | 1.0000 | 1.0000 | 590902 | 5332.1 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7906.2 | 0.441 | 0.868 | 1.154 | 1.0000 | 1.0000 | 1.0000 | 343497 | 4167.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7895.2 | 0.440 | 0.841 | 1.112 | 0.6667 | 1.0000 | 0.7767 | 344377 | 4206.9 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7670.8 | 0.438 | 0.905 | 1.211 | 0.5556 | 0.6667 | 1.0000 | 337118 | 4006.4 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7884.7 | 0.435 | 0.895 | 1.113 | 0.5556 | 0.8333 | 1.0000 | 363464 | 4113.8 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5375.3 | 0.632 | 1.249 | 1.866 | 1.0000 | 1.0000 | 1.0000 | 469037 | 4668.6 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 3114.9 | 1.146 | 1.932 | 2.336 | 0.5556 | 0.8889 | 1.0000 | 681849 | 5150.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 577.5 | 1.686 | 1.989 | 2.431 | 1.0000 | 1.0000 | 1.0000 | 832905 | 9222.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 572.0 | 1.693 | 2.013 | 2.373 | 0.6667 | 1.0000 | 0.7767 | 829695 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 575.0 | 1.684 | 2.055 | 2.472 | 0.5556 | 0.7222 | 1.0000 | 854587 | 9119.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 575.2 | 1.685 | 2.002 | 2.696 | 0.5556 | 0.8333 | 1.0000 | 858657 | 9197.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18493.0 | 0.050 | 0.061 | 0.110 | 1.0000 | 1.0000 | 1.0000 | 109038 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 16975.3 | 0.056 | 0.066 | 0.077 | 0.6667 | 1.0000 | 0.7767 | 114830 | 560.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17574.9 | 0.054 | 0.064 | 0.077 | 0.5556 | 0.7222 | 1.0000 | 134252 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16707.0 | 0.057 | 0.067 | 0.083 | 0.5556 | 0.8333 | 1.0000 | 143729 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 584.5 | 1.669 | 2.007 | 2.251 | 1.0000 | 1.0000 | 1.0000 | 797727 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 586.2 | 1.667 | 1.930 | 2.248 | 0.6667 | 1.0000 | 0.7767 | 792093 | 9202.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 583.0 | 1.664 | 2.051 | 2.355 | 0.5556 | 0.6667 | 1.0000 | 790324 | 9012.6 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 588.4 | 1.662 | 1.951 | 2.186 | 0.5556 | 0.8333 | 1.0000 | 809143 | 9115.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19934.3 | 0.048 | 0.055 | 0.072 | 1.0000 | 1.0000 | 1.0000 | 75539 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19195.8 | 0.050 | 0.056 | 0.082 | 0.6667 | 1.0000 | 0.7767 | 78924 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24842.4 | 0.039 | 0.046 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 69926 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19624.2 | 0.049 | 0.054 | 0.075 | 0.5556 | 0.8333 | 1.0000 | 95968 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 597.8 | 1.638 | 1.912 | 2.232 | 1.0000 | 1.0000 | 1.0000 | 760991 | 8919.5 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 576.2 | 1.649 | 1.991 | 3.294 | 0.6667 | 1.0000 | 0.7767 | 762042 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 590.8 | 1.645 | 1.978 | 2.326 | 0.5556 | 0.6667 | 1.0000 | 744031 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 591.4 | 1.649 | 1.952 | 2.263 | 0.5556 | 0.8333 | 1.0000 | 769326 | 8912.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47070.5 | 0.020 | 0.025 | 0.032 | 1.0000 | 1.0000 | 1.0000 | 38810 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 41846.2 | 0.022 | 0.028 | 0.040 | 0.6667 | 1.0000 | 0.7767 | 43551 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 58813.0 | 0.016 | 0.019 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41860.4 | 0.022 | 0.028 | 0.036 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2027.2 | 1.760 | 2.839 | 4.537 | 1.0000 | 1.0000 | 1.0000 | 833076 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1982.6 | 1.785 | 2.850 | 3.976 | 0.6667 | 1.0000 | 0.7767 | 829776 | 9270.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2026.3 | 1.778 | 2.772 | 3.697 | 0.5556 | 0.7222 | 1.0000 | 854823 | 9119.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1990.1 | 1.784 | 2.854 | 4.331 | 0.5556 | 0.8333 | 1.0000 | 858829 | 9197.9 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 44553.0 | 0.071 | 0.157 | 0.282 | 1.0000 | 1.0000 | 1.0000 | 109195 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 37804.4 | 0.085 | 0.180 | 0.305 | 0.6667 | 1.0000 | 0.7767 | 114953 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 42098.8 | 0.077 | 0.171 | 0.377 | 0.5556 | 0.7222 | 1.0000 | 134383 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39863.1 | 0.078 | 0.164 | 0.461 | 0.5556 | 0.8333 | 1.0000 | 143920 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2099.5 | 1.720 | 2.697 | 3.765 | 1.0000 | 1.0000 | 1.0000 | 798204 | 9165.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2085.0 | 1.720 | 2.771 | 3.389 | 0.6667 | 1.0000 | 0.7767 | 792685 | 9202.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2140.5 | 1.700 | 2.644 | 3.182 | 0.5556 | 0.6667 | 1.0000 | 791753 | 9013.4 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2081.8 | 1.723 | 2.800 | 3.330 | 0.5556 | 0.8333 | 1.0000 | 809711 | 9115.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 59319.1 | 0.048 | 0.131 | 0.225 | 1.0000 | 1.0000 | 1.0000 | 75710 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 51019.3 | 0.052 | 0.159 | 0.293 | 0.6667 | 1.0000 | 0.7767 | 79083 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 81647.3 | 0.038 | 0.069 | 0.153 | 0.5556 | 0.6667 | 1.0000 | 70076 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 63131.7 | 0.048 | 0.119 | 0.208 | 0.5556 | 0.8333 | 1.0000 | 96120 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2139.8 | 1.683 | 2.696 | 3.699 | 1.0000 | 1.0000 | 1.0000 | 761241 | 8919.9 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2141.4 | 1.686 | 2.784 | 3.767 | 0.6667 | 1.0000 | 0.7767 | 762241 | 8969.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2163.5 | 1.677 | 2.600 | 3.441 | 0.5556 | 0.6667 | 1.0000 | 744075 | 8809.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2143.7 | 1.692 | 2.661 | 3.381 | 0.5556 | 0.8333 | 1.0000 | 769367 | 8913.2 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 140788.8 | 0.019 | 0.045 | 0.054 | 1.0000 | 1.0000 | 1.0000 | 38824 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 114237.8 | 0.021 | 0.060 | 0.127 | 0.6667 | 1.0000 | 0.7767 | 43585 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 174412.0 | 0.015 | 0.031 | 0.058 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 113422.8 | 0.022 | 0.064 | 0.092 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 412.9 | 2.341 | 2.745 | 3.150 | 1.0000 | 1.0000 | 1.0000 | 1162577 | 12329.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 405.2 | 2.382 | 2.820 | 3.165 | 0.6667 | 1.0000 | 0.7767 | 1159080 | 12384.1 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 409.6 | 2.351 | 2.831 | 3.968 | 0.5556 | 0.7222 | 1.0000 | 1184324 | 12221.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 412.2 | 2.343 | 2.744 | 3.420 | 0.5556 | 0.8333 | 1.0000 | 1188531 | 12302.3 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 451.0 | 1.958 | 2.468 | 6.557 | 1.0000 | 1.0000 | 1.0000 | 920889 | 11113.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 497.9 | 1.946 | 2.403 | 2.611 | 0.6667 | 1.0000 | 0.7767 | 915429 | 11153.8 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 502.1 | 1.932 | 2.355 | 2.542 | 0.5556 | 0.6667 | 1.0000 | 913014 | 10952.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 499.8 | 1.940 | 2.397 | 2.628 | 0.5556 | 0.8333 | 1.0000 | 931784 | 11057.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 432.3 | 2.229 | 2.753 | 3.242 | 1.0000 | 1.0000 | 1.0000 | 1072261 | 11624.9 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 358.5 | 2.693 | 3.286 | 3.596 | 0.5556 | 0.6667 | 1.0000 | 1290538 | 12107.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1480.6 | 2.415 | 3.869 | 5.785 | 1.0000 | 1.0000 | 1.0000 | 1161903 | 12331.6 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1480.0 | 2.415 | 3.909 | 6.331 | 0.6667 | 1.0000 | 0.7767 | 1158858 | 12386.2 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1470.2 | 2.468 | 3.930 | 5.586 | 0.5556 | 0.7222 | 1.0000 | 1183366 | 12223.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1479.3 | 2.441 | 3.990 | 5.934 | 0.5556 | 0.8333 | 1.0000 | 1187795 | 12304.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1876.6 | 1.960 | 2.866 | 3.424 | 1.0000 | 1.0000 | 1.0000 | 921450 | 11115.3 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1828.5 | 2.016 | 2.960 | 3.547 | 0.6667 | 1.0000 | 0.7767 | 916348 | 11155.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1855.3 | 1.982 | 2.934 | 3.567 | 0.5556 | 0.6667 | 1.0000 | 913278 | 10953.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1818.0 | 2.029 | 2.993 | 3.638 | 0.5556 | 0.8333 | 1.0000 | 932542 | 11060.1 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1563.8 | 2.306 | 3.839 | 4.855 | 1.0000 | 1.0000 | 1.0000 | 1071704 | 11626.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1308.4 | 2.817 | 4.042 | 4.553 | 0.5556 | 0.6667 | 1.0000 | 1291484 | 12108.9 |

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
