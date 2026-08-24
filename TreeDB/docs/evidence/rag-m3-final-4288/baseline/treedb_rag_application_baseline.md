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
- command: `/tmp/treedb_rag_benchmark_30239761b -out-dir /tmp/gomap-rag-m3-base-repeat -dir /tmp/gomap-rag-m3-base-repeat-db -product-base-sha d8ad352965493f6a31a1d50ac70f5d783103c454 -harness-revision 30239761b1e7b90cf66fadf894921757f347c9b1 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 pre-filter baseline repeat`

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
| 0 | 18 | 54 | 0.064155 | 280.57 | 841.72 | 2359487 | 4809 | 3748791 | true |
| 1 | 18 | 54 | 0.048283 | 372.80 | 1118.40 | 1844676 | 4680 | 3748833 | true |
| 2 | 18 | 54 | 0.037438 | 480.80 | 1442.40 | 1851798 | 4675 | 3748827 | true |
| 3 | 18 | 54 | 0.038928 | 462.39 | 1387.18 | 1842341 | 4675 | 3748827 | true |
| 4 | 18 | 54 | 0.037442 | 480.74 | 1442.22 | 1844336 | 4676 | 3748827 | true |

Median/p95 docs/s: **462.39 / 480.79**. Median/p95 B/source: **1844676 / 2257949**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **531.75**, B/source <= **1660209**. historical regime did not reproduce on the retained application fixture; freeze an attainable 15% throughput gain and 10% allocation reduction

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4677.8 | 0.208 | 0.229 | 0.352 | 1.0000 | 1.0000 | 1.0000 | 253312 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4553.5 | 0.213 | 0.231 | 0.254 | 0.6667 | 1.0000 | 0.7767 | 259050 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4634.6 | 0.207 | 0.225 | 0.382 | 0.5556 | 0.8333 | 1.0000 | 277722 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4536.6 | 0.212 | 0.234 | 0.273 | 0.5556 | 1.0000 | 1.0000 | 286564 | 2236.6 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 17931.1 | 0.053 | 0.061 | 0.112 | 1.0000 | 1.0000 | 1.0000 | 107758 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17306.1 | 0.055 | 0.063 | 0.090 | 0.6667 | 1.0000 | 0.7767 | 113545 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17755.1 | 0.053 | 0.060 | 0.164 | 0.5556 | 0.8333 | 1.0000 | 132891 | 403.9 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16632.4 | 0.056 | 0.061 | 0.128 | 0.5556 | 1.0000 | 1.0000 | 141898 | 474.4 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4847.8 | 0.201 | 0.214 | 0.246 | 1.0000 | 1.0000 | 1.0000 | 221667 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4727.8 | 0.204 | 0.229 | 0.324 | 0.6667 | 1.0000 | 0.7767 | 221839 | 2252.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5080.2 | 0.192 | 0.209 | 0.284 | 0.5556 | 0.6667 | 1.0000 | 215730 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4820.1 | 0.202 | 0.221 | 0.345 | 0.5556 | 0.8333 | 1.0000 | 241371 | 2167.9 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19954.4 | 0.047 | 0.055 | 0.063 | 1.0000 | 1.0000 | 1.0000 | 75546 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19565.6 | 0.050 | 0.055 | 0.065 | 0.6667 | 1.0000 | 0.7767 | 78931 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24967.7 | 0.038 | 0.046 | 0.053 | 0.5556 | 0.6667 | 1.0000 | 69923 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19873.1 | 0.048 | 0.056 | 0.067 | 0.5556 | 0.8333 | 1.0000 | 95969 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5560.5 | 0.175 | 0.193 | 0.262 | 1.0000 | 1.0000 | 1.0000 | 183369 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5449.6 | 0.178 | 0.194 | 0.213 | 0.6667 | 1.0000 | 0.7767 | 187752 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5767.8 | 0.169 | 0.183 | 0.226 | 0.5556 | 0.8889 | 1.0000 | 168592 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5586.7 | 0.175 | 0.186 | 0.217 | 0.5556 | 0.9444 | 1.0000 | 193920 | 1965.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52678.4 | 0.017 | 0.022 | 0.027 | 1.0000 | 1.0000 | 1.0000 | 37524 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 45076.4 | 0.020 | 0.026 | 0.032 | 0.6667 | 1.0000 | 0.7767 | 42234 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 69698.1 | 0.013 | 0.016 | 0.020 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 47684.6 | 0.019 | 0.024 | 0.032 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 14133.2 | 0.239 | 0.482 | 0.920 | 1.0000 | 1.0000 | 1.0000 | 253460 | 2274.2 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13302.0 | 0.245 | 0.502 | 0.990 | 0.6667 | 1.0000 | 0.7767 | 259163 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 12875.9 | 0.250 | 0.547 | 0.967 | 0.5556 | 0.8333 | 1.0000 | 277871 | 2166.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13646.1 | 0.250 | 0.458 | 0.859 | 0.5556 | 1.0000 | 1.0000 | 286682 | 2236.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47151.6 | 0.068 | 0.166 | 0.266 | 1.0000 | 1.0000 | 1.0000 | 107896 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46151.5 | 0.070 | 0.160 | 0.285 | 0.6667 | 1.0000 | 0.7767 | 113708 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 43705.5 | 0.074 | 0.182 | 0.304 | 0.5556 | 0.8333 | 1.0000 | 132990 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 37585.5 | 0.080 | 0.238 | 0.416 | 0.5556 | 1.0000 | 1.0000 | 142035 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 15538.5 | 0.214 | 0.516 | 0.835 | 1.0000 | 1.0000 | 1.0000 | 221881 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15671.3 | 0.213 | 0.504 | 0.712 | 0.6667 | 1.0000 | 0.7767 | 222030 | 2252.2 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 15640.6 | 0.205 | 0.487 | 0.715 | 0.5556 | 0.6667 | 1.0000 | 216201 | 2064.6 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15807.1 | 0.212 | 0.501 | 0.785 | 0.5556 | 0.8333 | 1.0000 | 241649 | 2168.3 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 61437.3 | 0.047 | 0.112 | 0.224 | 1.0000 | 1.0000 | 1.0000 | 75710 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60443.8 | 0.049 | 0.128 | 0.215 | 0.6667 | 1.0000 | 0.7767 | 79089 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 77900.4 | 0.038 | 0.079 | 0.185 | 0.5556 | 0.6667 | 1.0000 | 70086 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 61746.5 | 0.047 | 0.125 | 0.229 | 0.5556 | 0.8333 | 1.0000 | 96156 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 17613.2 | 0.187 | 0.453 | 0.617 | 1.0000 | 1.0000 | 1.0000 | 183459 | 1972.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17762.4 | 0.190 | 0.395 | 0.701 | 0.6667 | 1.0000 | 0.7767 | 187804 | 2021.9 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18967.2 | 0.179 | 0.401 | 0.573 | 0.5556 | 0.8889 | 1.0000 | 168612 | 1862.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 17864.3 | 0.184 | 0.435 | 0.633 | 0.5556 | 0.9444 | 1.0000 | 193937 | 1965.7 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 124753.2 | 0.020 | 0.057 | 0.123 | 1.0000 | 1.0000 | 1.0000 | 37586 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 112696.6 | 0.020 | 0.064 | 0.137 | 0.6667 | 1.0000 | 0.7767 | 42281 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 216622.2 | 0.012 | 0.022 | 0.032 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 128914.6 | 0.019 | 0.056 | 0.122 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1403.0 | 0.690 | 0.764 | 1.273 | 1.0000 | 1.0000 | 1.0000 | 558895 | 5370.6 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1394.0 | 0.694 | 0.760 | 1.293 | 0.6667 | 1.0000 | 0.7767 | 563846 | 5425.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1410.6 | 0.691 | 0.757 | 1.255 | 0.5556 | 0.8333 | 1.0000 | 582888 | 5256.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1392.8 | 0.696 | 0.773 | 1.303 | 0.5556 | 1.0000 | 1.0000 | 591271 | 5329.1 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 2218.7 | 0.446 | 0.476 | 0.728 | 1.0000 | 1.0000 | 1.0000 | 343072 | 4164.5 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2208.6 | 0.448 | 0.479 | 0.725 | 0.6667 | 1.0000 | 0.7767 | 343233 | 4202.7 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2262.2 | 0.437 | 0.467 | 0.750 | 0.5556 | 0.6667 | 1.0000 | 336089 | 4003.0 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2225.9 | 0.444 | 0.472 | 0.679 | 0.5556 | 0.8333 | 1.0000 | 362254 | 4109.9 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1560.2 | 0.626 | 0.688 | 1.124 | 1.0000 | 1.0000 | 1.0000 | 470085 | 4666.3 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 927.0 | 1.047 | 1.199 | 1.594 | 0.5556 | 0.8889 | 1.0000 | 681750 | 5148.3 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4477.3 | 0.756 | 1.587 | 2.633 | 1.0000 | 1.0000 | 1.0000 | 558408 | 5373.1 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4369.3 | 0.776 | 1.571 | 2.514 | 0.6667 | 1.0000 | 0.7767 | 563670 | 5429.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4376.7 | 0.755 | 1.756 | 2.540 | 0.5556 | 0.8333 | 1.0000 | 583396 | 5259.0 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4313.7 | 0.781 | 1.642 | 2.621 | 0.5556 | 1.0000 | 1.0000 | 591158 | 5331.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 8004.3 | 0.436 | 0.811 | 1.174 | 1.0000 | 1.0000 | 1.0000 | 344045 | 4167.8 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7814.0 | 0.440 | 0.844 | 1.309 | 0.6667 | 1.0000 | 0.7767 | 344139 | 4206.5 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7765.1 | 0.433 | 0.931 | 1.221 | 0.5556 | 0.6667 | 1.0000 | 336586 | 4005.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7443.9 | 0.460 | 0.908 | 1.273 | 0.5556 | 0.8333 | 1.0000 | 363492 | 4114.3 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5185.8 | 0.648 | 1.335 | 1.975 | 1.0000 | 1.0000 | 1.0000 | 469481 | 4668.1 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2999.9 | 1.188 | 2.018 | 2.555 | 0.5556 | 0.8889 | 1.0000 | 682927 | 5150.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 568.5 | 1.690 | 2.100 | 2.426 | 1.0000 | 1.0000 | 1.0000 | 832884 | 9222.0 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 571.1 | 1.695 | 2.033 | 2.342 | 0.6667 | 1.0000 | 0.7767 | 829699 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 576.4 | 1.686 | 2.027 | 2.533 | 0.5556 | 0.7222 | 1.0000 | 854544 | 9119.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 575.5 | 1.689 | 1.996 | 2.450 | 0.5556 | 0.8333 | 1.0000 | 858665 | 9197.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18011.6 | 0.053 | 0.060 | 0.101 | 1.0000 | 1.0000 | 1.0000 | 109052 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17074.5 | 0.056 | 0.065 | 0.109 | 0.6667 | 1.0000 | 0.7767 | 114824 | 560.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17281.1 | 0.055 | 0.066 | 0.139 | 0.5556 | 0.7222 | 1.0000 | 134257 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16761.8 | 0.057 | 0.069 | 0.143 | 0.5556 | 0.8333 | 1.0000 | 143750 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 579.7 | 1.677 | 2.074 | 2.257 | 1.0000 | 1.0000 | 1.0000 | 797742 | 9164.9 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 583.1 | 1.670 | 1.987 | 2.263 | 0.6667 | 1.0000 | 0.7767 | 792108 | 9202.1 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 582.7 | 1.669 | 2.027 | 2.334 | 0.5556 | 0.6667 | 1.0000 | 790308 | 9012.5 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 582.0 | 1.674 | 2.005 | 2.289 | 0.5556 | 0.8333 | 1.0000 | 809149 | 9115.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 19946.6 | 0.048 | 0.055 | 0.072 | 1.0000 | 1.0000 | 1.0000 | 75539 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19169.9 | 0.050 | 0.057 | 0.085 | 0.6667 | 1.0000 | 0.7767 | 78924 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 25548.7 | 0.037 | 0.045 | 0.065 | 0.5556 | 0.6667 | 1.0000 | 69919 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19900.0 | 0.049 | 0.054 | 0.089 | 0.5556 | 0.8333 | 1.0000 | 95954 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 591.1 | 1.645 | 2.002 | 2.263 | 1.0000 | 1.0000 | 1.0000 | 760963 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 591.5 | 1.646 | 1.900 | 2.348 | 0.6667 | 1.0000 | 0.7767 | 762020 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 592.3 | 1.644 | 1.957 | 2.301 | 0.5556 | 0.6667 | 1.0000 | 744027 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 594.1 | 1.645 | 1.948 | 2.177 | 0.5556 | 0.8333 | 1.0000 | 769327 | 8912.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47601.3 | 0.020 | 0.026 | 0.030 | 1.0000 | 1.0000 | 1.0000 | 38797 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43742.7 | 0.021 | 0.027 | 0.038 | 0.6667 | 1.0000 | 0.7767 | 43531 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 60726.9 | 0.015 | 0.018 | 0.022 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 41291.9 | 0.023 | 0.028 | 0.034 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2043.1 | 1.755 | 2.717 | 4.130 | 1.0000 | 1.0000 | 1.0000 | 832969 | 9222.0 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2030.3 | 1.762 | 2.789 | 4.238 | 0.6667 | 1.0000 | 0.7767 | 829754 | 9270.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2049.3 | 1.762 | 2.835 | 3.952 | 0.5556 | 0.7222 | 1.0000 | 854749 | 9119.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2017.5 | 1.774 | 2.817 | 4.259 | 0.5556 | 0.8333 | 1.0000 | 858768 | 9197.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 43237.5 | 0.073 | 0.168 | 0.322 | 1.0000 | 1.0000 | 1.0000 | 109182 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 40772.2 | 0.079 | 0.182 | 0.287 | 0.6667 | 1.0000 | 0.7767 | 114960 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 42843.8 | 0.076 | 0.179 | 0.296 | 0.5556 | 0.7222 | 1.0000 | 134402 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 39650.2 | 0.081 | 0.175 | 0.323 | 0.5556 | 0.8333 | 1.0000 | 143887 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2130.5 | 1.716 | 2.595 | 3.500 | 1.0000 | 1.0000 | 1.0000 | 798221 | 9165.7 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2094.2 | 1.722 | 2.736 | 3.469 | 0.6667 | 1.0000 | 0.7767 | 792752 | 9202.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2115.4 | 1.711 | 2.665 | 3.517 | 0.5556 | 0.6667 | 1.0000 | 791752 | 9013.3 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2085.5 | 1.720 | 2.790 | 3.655 | 0.5556 | 0.8333 | 1.0000 | 809584 | 9115.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 58145.4 | 0.048 | 0.132 | 0.241 | 1.0000 | 1.0000 | 1.0000 | 75689 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 60568.1 | 0.049 | 0.118 | 0.262 | 0.6667 | 1.0000 | 0.7767 | 79062 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 80333.2 | 0.038 | 0.085 | 0.171 | 0.5556 | 0.6667 | 1.0000 | 70077 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 60882.2 | 0.048 | 0.126 | 0.226 | 0.5556 | 0.8333 | 1.0000 | 96133 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2134.5 | 1.690 | 2.759 | 3.382 | 1.0000 | 1.0000 | 1.0000 | 761371 | 8920.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2114.9 | 1.695 | 2.855 | 4.027 | 0.6667 | 1.0000 | 0.7767 | 762214 | 8969.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2135.6 | 1.686 | 2.732 | 3.346 | 0.5556 | 0.6667 | 1.0000 | 744082 | 8809.7 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2078.2 | 1.702 | 2.843 | 3.886 | 0.5556 | 0.8333 | 1.0000 | 769375 | 8913.3 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 136329.3 | 0.019 | 0.048 | 0.066 | 1.0000 | 1.0000 | 1.0000 | 38824 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 115122.3 | 0.023 | 0.054 | 0.108 | 0.6667 | 1.0000 | 0.7767 | 43586 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 181942.3 | 0.015 | 0.027 | 0.050 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 125564.9 | 0.021 | 0.048 | 0.097 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 406.5 | 2.377 | 2.834 | 3.602 | 1.0000 | 1.0000 | 1.0000 | 1162160 | 12329.5 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 403.2 | 2.394 | 2.834 | 3.402 | 0.6667 | 1.0000 | 0.7767 | 1158722 | 12384.0 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 414.9 | 2.335 | 2.742 | 3.900 | 0.5556 | 0.7222 | 1.0000 | 1183954 | 12221.3 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 410.7 | 2.354 | 2.783 | 3.204 | 0.5556 | 0.8333 | 1.0000 | 1188232 | 12302.2 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 501.3 | 1.934 | 2.380 | 2.616 | 1.0000 | 1.0000 | 1.0000 | 921165 | 11113.4 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 495.8 | 1.952 | 2.404 | 2.635 | 0.6667 | 1.0000 | 0.7767 | 915241 | 11153.7 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 504.3 | 1.925 | 2.341 | 2.554 | 0.5556 | 0.6667 | 1.0000 | 912432 | 10952.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 497.0 | 1.944 | 2.406 | 2.622 | 0.5556 | 0.8333 | 1.0000 | 931707 | 11057.7 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 424.8 | 2.241 | 2.849 | 3.267 | 1.0000 | 1.0000 | 1.0000 | 1072452 | 11624.8 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 361.6 | 2.669 | 3.262 | 3.632 | 0.5556 | 0.6667 | 1.0000 | 1288480 | 12107.1 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1479.7 | 2.415 | 4.100 | 5.866 | 1.0000 | 1.0000 | 1.0000 | 1161772 | 12331.6 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1450.5 | 2.478 | 4.064 | 5.844 | 0.6667 | 1.0000 | 0.7767 | 1158769 | 12386.8 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1491.4 | 2.420 | 3.815 | 5.798 | 0.5556 | 0.7222 | 1.0000 | 1183635 | 12223.3 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1454.8 | 2.438 | 4.125 | 6.801 | 0.5556 | 0.8333 | 1.0000 | 1187425 | 12304.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1814.5 | 2.013 | 3.094 | 3.587 | 1.0000 | 1.0000 | 1.0000 | 920694 | 11115.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1854.6 | 1.969 | 2.915 | 3.286 | 0.6667 | 1.0000 | 0.7767 | 916464 | 11155.9 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1851.7 | 1.976 | 2.933 | 3.410 | 0.5556 | 0.6667 | 1.0000 | 913444 | 10953.6 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1782.3 | 2.038 | 3.143 | 3.804 | 0.5556 | 0.8333 | 1.0000 | 932258 | 11060.4 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1537.7 | 2.328 | 3.838 | 5.000 | 1.0000 | 1.0000 | 1.0000 | 1071398 | 11626.7 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1280.9 | 2.882 | 4.223 | 4.894 | 0.5556 | 0.6667 | 1.0000 | 1292019 | 12109.5 |

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
