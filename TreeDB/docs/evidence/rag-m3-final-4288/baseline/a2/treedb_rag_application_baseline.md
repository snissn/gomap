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
- command: `/tmp/treedb_rag_benchmark_30239761b -out-dir /tmp/gomap-rag-m3-abba-a2 -dir /tmp/gomap-rag-m3-abba-a2-db -product-base-sha d8ad352965493f6a31a1d50ac70f5d783103c454 -harness-revision 30239761b1e7b90cf66fadf894921757f347c9b1 -host-note Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1; M3 ABBA A2 control`

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
| 0 | 18 | 54 | 0.069800 | 257.88 | 773.64 | 2329280 | 4804 | 3748790 | true |
| 1 | 18 | 54 | 0.048301 | 372.66 | 1117.99 | 1847457 | 4683 | 3748827 | true |
| 2 | 18 | 54 | 0.040637 | 442.95 | 1328.85 | 1843188 | 4675 | 3748827 | true |
| 3 | 18 | 54 | 0.041615 | 432.53 | 1297.60 | 1849595 | 4674 | 3748827 | true |
| 4 | 18 | 54 | 0.043312 | 415.59 | 1246.76 | 1838778 | 4678 | 3748827 | true |

Median/p95 docs/s: **415.59 / 440.87**. Median/p95 B/source: **1847457 / 2233343**. Historical 37.59 docs/s / 132 GiB regime reproduced: **false**.

Frozen #4284 gate: source docs/s >= **325.45**, B/source <= **1947235**. retained pre-candidate #4284 gate frozen by the final repaired M1 baseline; final, repeated, and comparison-control artifacts evaluate the same thresholds

## Supported retained rows

Every supported row has >=1000 timed queries and three forward/reverse/forward repetitions.

Quality is measured by separate untimed queries. Direct score-only rows use compact responses with identical work, route, and filter to retain source attribution while timed score-only rows still fetch zero documents.

| embedding | surface | clients | route | vector route | projection | filter | collapse | QPS | p50 ms | p95 ms | p99 ms | chunk R@10 | parent R@10 | nDCG@10 | B/op | allocs/op |
|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4562.7 | 0.209 | 0.229 | 0.260 | 1.0000 | 1.0000 | 1.0000 | 253296 | 2274.1 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 4509.1 | 0.216 | 0.244 | 0.341 | 0.6667 | 1.0000 | 0.7767 | 259044 | 2323.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4622.0 | 0.208 | 0.230 | 0.389 | 0.5556 | 0.8333 | 1.0000 | 277691 | 2166.3 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4417.1 | 0.213 | 0.277 | 0.383 | 0.5556 | 1.0000 | 1.0000 | 286559 | 2236.5 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18067.3 | 0.053 | 0.061 | 0.089 | 1.0000 | 1.0000 | 1.0000 | 107766 | 510.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17193.1 | 0.055 | 0.064 | 0.083 | 0.6667 | 1.0000 | 0.7767 | 113565 | 561.4 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17898.1 | 0.053 | 0.061 | 0.122 | 0.5556 | 0.8333 | 1.0000 | 132858 | 403.8 |
| hashing_regression | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16221.5 | 0.056 | 0.071 | 0.130 | 0.5556 | 1.0000 | 1.0000 | 141908 | 474.5 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 4824.4 | 0.201 | 0.219 | 0.317 | 1.0000 | 1.0000 | 1.0000 | 221668 | 2216.6 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 4719.1 | 0.205 | 0.236 | 0.342 | 0.6667 | 1.0000 | 0.7767 | 221853 | 2252.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 5059.7 | 0.193 | 0.207 | 0.298 | 0.5556 | 0.6667 | 1.0000 | 215699 | 2064.0 |
| hashing_regression | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 4816.3 | 0.202 | 0.218 | 0.317 | 0.5556 | 0.8333 | 1.0000 | 241377 | 2168.0 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20010.9 | 0.048 | 0.054 | 0.074 | 1.0000 | 1.0000 | 1.0000 | 75546 | 453.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19413.3 | 0.050 | 0.055 | 0.072 | 0.6667 | 1.0000 | 0.7767 | 78938 | 492.4 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 24662.1 | 0.038 | 0.045 | 0.066 | 0.5556 | 0.6667 | 1.0000 | 69937 | 301.5 |
| hashing_regression | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 19636.0 | 0.049 | 0.054 | 0.061 | 0.5556 | 0.8333 | 1.0000 | 95969 | 405.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4957.7 | 0.175 | 0.338 | 0.558 | 1.0000 | 1.0000 | 1.0000 | 183359 | 1972.2 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 5334.9 | 0.180 | 0.204 | 0.368 | 0.6667 | 1.0000 | 0.7767 | 187774 | 2021.8 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 5775.9 | 0.169 | 0.182 | 0.204 | 0.5556 | 0.8889 | 1.0000 | 168592 | 1861.9 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 5552.6 | 0.176 | 0.187 | 0.242 | 0.5556 | 0.9444 | 1.0000 | 193922 | 1965.5 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 52006.6 | 0.017 | 0.022 | 0.036 | 1.0000 | 1.0000 | 1.0000 | 37538 | 208.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 46132.2 | 0.020 | 0.025 | 0.031 | 0.6667 | 1.0000 | 0.7767 | 42227 | 258.4 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 68354.8 | 0.013 | 0.016 | 0.020 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 47461.3 | 0.020 | 0.024 | 0.034 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 14190.9 | 0.242 | 0.492 | 0.815 | 1.0000 | 1.0000 | 1.0000 | 253470 | 2274.3 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 13458.0 | 0.247 | 0.496 | 0.902 | 0.6667 | 1.0000 | 0.7767 | 259152 | 2323.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 13445.1 | 0.247 | 0.559 | 0.886 | 0.5556 | 0.8333 | 1.0000 | 277849 | 2166.4 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 13171.2 | 0.252 | 0.522 | 0.887 | 0.5556 | 1.0000 | 1.0000 | 286712 | 2236.6 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 46729.6 | 0.068 | 0.152 | 0.295 | 1.0000 | 1.0000 | 1.0000 | 107915 | 510.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 43356.9 | 0.073 | 0.167 | 0.280 | 0.6667 | 1.0000 | 0.7767 | 113702 | 561.5 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 44872.2 | 0.072 | 0.168 | 0.274 | 0.5556 | 0.8333 | 1.0000 | 132998 | 403.8 |
| hashing_regression | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38549.6 | 0.077 | 0.212 | 0.365 | 0.5556 | 1.0000 | 1.0000 | 142049 | 474.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 13878.7 | 0.219 | 0.552 | 0.797 | 1.0000 | 1.0000 | 1.0000 | 221879 | 2216.9 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 15477.6 | 0.214 | 0.515 | 0.707 | 0.6667 | 1.0000 | 0.7767 | 222026 | 2252.3 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 16011.4 | 0.207 | 0.474 | 0.641 | 0.5556 | 0.6667 | 1.0000 | 216213 | 2064.5 |
| hashing_regression | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 15358.0 | 0.214 | 0.497 | 0.658 | 0.5556 | 0.8333 | 1.0000 | 241665 | 2168.4 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 63339.4 | 0.047 | 0.114 | 0.202 | 1.0000 | 1.0000 | 1.0000 | 75717 | 453.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 58260.5 | 0.049 | 0.128 | 0.267 | 0.6667 | 1.0000 | 0.7767 | 79097 | 492.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 67510.8 | 0.039 | 0.111 | 0.183 | 0.5556 | 0.6667 | 1.0000 | 70093 | 301.5 |
| hashing_regression | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 58780.2 | 0.048 | 0.138 | 0.242 | 0.5556 | 0.8333 | 1.0000 | 96163 | 405.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 18154.4 | 0.184 | 0.378 | 0.646 | 1.0000 | 1.0000 | 1.0000 | 183467 | 1972.5 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 17620.5 | 0.188 | 0.408 | 0.563 | 0.6667 | 1.0000 | 0.7767 | 187877 | 2022.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 18607.2 | 0.179 | 0.415 | 0.649 | 0.5556 | 0.8889 | 1.0000 | 168606 | 1862.0 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 17766.4 | 0.186 | 0.445 | 0.611 | 0.5556 | 0.9444 | 1.0000 | 193930 | 1965.6 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 128233.7 | 0.019 | 0.053 | 0.141 | 1.0000 | 1.0000 | 1.0000 | 37566 | 208.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 108430.9 | 0.023 | 0.069 | 0.122 | 0.6667 | 1.0000 | 0.7767 | 42295 | 258.4 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 207304.6 | 0.012 | 0.022 | 0.033 | 0.5556 | 0.8889 | 1.0000 | 22518 | 98.1 |
| hashing_regression | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 108957.8 | 0.019 | 0.059 | 0.109 | 0.5556 | 0.9444 | 1.0000 | 47817 | 201.4 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1389.0 | 0.689 | 0.757 | 1.507 | 1.0000 | 1.0000 | 1.0000 | 557930 | 5370.5 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1345.0 | 0.701 | 0.835 | 2.017 | 0.6667 | 1.0000 | 0.7767 | 563150 | 5425.9 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1412.6 | 0.689 | 0.756 | 1.213 | 0.5556 | 0.8333 | 1.0000 | 581891 | 5256.1 |
| hashing_regression | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1394.8 | 0.694 | 0.758 | 1.325 | 0.5556 | 1.0000 | 1.0000 | 590969 | 5329.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 1859.0 | 0.454 | 0.950 | 1.955 | 1.0000 | 1.0000 | 1.0000 | 343084 | 4164.3 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2202.3 | 0.444 | 0.509 | 0.678 | 0.6667 | 1.0000 | 0.7767 | 343424 | 4202.8 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 2279.1 | 0.434 | 0.461 | 0.768 | 0.5556 | 0.6667 | 1.0000 | 336380 | 4003.2 |
| hashing_regression | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 2220.9 | 0.444 | 0.485 | 0.663 | 0.5556 | 0.8333 | 1.0000 | 361995 | 4109.8 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1561.3 | 0.623 | 0.709 | 1.147 | 1.0000 | 1.0000 | 1.0000 | 469672 | 4666.2 |
| hashing_regression | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 902.7 | 1.063 | 1.340 | 1.749 | 0.5556 | 0.8889 | 1.0000 | 681462 | 5148.5 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 4335.7 | 0.749 | 1.689 | 3.985 | 1.0000 | 1.0000 | 1.0000 | 557810 | 5373.6 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 3942.4 | 0.871 | 1.909 | 2.951 | 0.6667 | 1.0000 | 0.7767 | 563183 | 5429.4 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 4558.5 | 0.750 | 1.514 | 2.140 | 0.5556 | 0.8333 | 1.0000 | 582509 | 5258.7 |
| hashing_regression | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 4288.9 | 0.751 | 1.821 | 3.110 | 0.5556 | 1.0000 | 1.0000 | 590345 | 5331.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 7552.1 | 0.457 | 0.905 | 1.309 | 1.0000 | 1.0000 | 1.0000 | 344197 | 4168.5 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 7549.1 | 0.455 | 0.984 | 1.256 | 0.6667 | 1.0000 | 0.7767 | 344330 | 4206.7 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 7974.0 | 0.433 | 0.867 | 1.311 | 0.5556 | 0.6667 | 1.0000 | 337619 | 4006.6 |
| hashing_regression | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 7560.6 | 0.457 | 0.863 | 1.215 | 0.5556 | 0.8333 | 1.0000 | 362588 | 4114.2 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 5327.8 | 0.641 | 1.300 | 1.785 | 1.0000 | 1.0000 | 1.0000 | 468947 | 4668.3 |
| hashing_regression | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 2721.5 | 1.256 | 2.307 | 4.710 | 0.5556 | 0.8889 | 1.0000 | 682112 | 5150.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 570.0 | 1.697 | 2.063 | 2.434 | 1.0000 | 1.0000 | 1.0000 | 832932 | 9222.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 568.3 | 1.695 | 2.084 | 2.479 | 0.6667 | 1.0000 | 0.7767 | 829672 | 9270.6 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 571.8 | 1.699 | 2.052 | 2.469 | 0.5556 | 0.7222 | 1.0000 | 854570 | 9119.2 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 571.6 | 1.694 | 2.046 | 2.511 | 0.5556 | 0.8333 | 1.0000 | 858632 | 9197.7 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 18144.8 | 0.053 | 0.061 | 0.113 | 1.0000 | 1.0000 | 1.0000 | 109055 | 510.4 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 17117.8 | 0.056 | 0.065 | 0.114 | 0.6667 | 1.0000 | 0.7767 | 114830 | 560.8 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 17371.4 | 0.054 | 0.065 | 0.094 | 0.5556 | 0.7222 | 1.0000 | 134266 | 408.1 |
| semantic_minilm | direct_collection | 1 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 16564.7 | 0.057 | 0.066 | 0.142 | 0.5556 | 0.8333 | 1.0000 | 143743 | 487.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 579.9 | 1.675 | 2.038 | 2.338 | 1.0000 | 1.0000 | 1.0000 | 797733 | 9164.8 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 581.5 | 1.678 | 1.992 | 2.353 | 0.6667 | 1.0000 | 0.7767 | 792125 | 9202.2 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | disabled | 584.6 | 1.666 | 1.993 | 2.293 | 0.5556 | 0.6667 | 1.0000 | 790333 | 9012.6 |
| semantic_minilm | direct_collection | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 583.0 | 1.672 | 1.982 | 2.269 | 0.5556 | 0.8333 | 1.0000 | 809201 | 9115.3 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | disabled | 20609.4 | 0.044 | 0.056 | 0.081 | 1.0000 | 1.0000 | 1.0000 | 75546 | 453.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 19702.8 | 0.047 | 0.057 | 0.081 | 0.6667 | 1.0000 | 0.7767 | 78918 | 492.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | disabled | 25126.6 | 0.038 | 0.047 | 0.061 | 0.5556 | 0.6667 | 1.0000 | 69926 | 301.4 |
| semantic_minilm | direct_collection | 1 | text_only | none | score_only | unfiltered | enabled_cap_2 | 20608.3 | 0.045 | 0.054 | 0.094 | 0.5556 | 0.8333 | 1.0000 | 95968 | 405.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 576.3 | 1.665 | 2.167 | 2.644 | 1.0000 | 1.0000 | 1.0000 | 760957 | 8919.4 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 583.2 | 1.664 | 2.039 | 2.453 | 0.6667 | 1.0000 | 0.7767 | 762056 | 8969.3 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 591.6 | 1.645 | 1.991 | 2.241 | 0.5556 | 0.6667 | 1.0000 | 744026 | 8809.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 583.3 | 1.664 | 2.055 | 2.299 | 0.5556 | 0.8333 | 1.0000 | 769332 | 8912.8 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 47956.8 | 0.019 | 0.025 | 0.033 | 1.0000 | 1.0000 | 1.0000 | 38811 | 208.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 42781.7 | 0.021 | 0.026 | 0.035 | 0.6667 | 1.0000 | 0.7767 | 43531 | 258.7 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 59092.5 | 0.016 | 0.019 | 0.025 | 0.5556 | 0.6667 | 1.0000 | 23798 | 98.1 |
| semantic_minilm | direct_collection | 1 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 40560.1 | 0.023 | 0.031 | 0.040 | 0.5556 | 0.8333 | 1.0000 | 49107 | 201.7 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1981.7 | 1.798 | 2.907 | 4.839 | 1.0000 | 1.0000 | 1.0000 | 833039 | 9222.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1898.6 | 1.817 | 3.274 | 5.146 | 0.6667 | 1.0000 | 0.7767 | 829755 | 9270.6 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1970.2 | 1.802 | 3.045 | 4.133 | 0.5556 | 0.7222 | 1.0000 | 854735 | 9119.3 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1942.5 | 1.827 | 3.154 | 4.713 | 0.5556 | 0.8333 | 1.0000 | 858775 | 9197.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | disabled | 42622.1 | 0.076 | 0.173 | 0.266 | 1.0000 | 1.0000 | 1.0000 | 109189 | 510.5 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 40159.5 | 0.079 | 0.184 | 0.290 | 0.6667 | 1.0000 | 0.7767 | 114967 | 560.8 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | disabled | 41334.9 | 0.079 | 0.169 | 0.350 | 0.5556 | 0.7222 | 1.0000 | 134409 | 408.1 |
| semantic_minilm | direct_collection | 4 | hybrid | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 38552.3 | 0.082 | 0.179 | 0.367 | 0.5556 | 0.8333 | 1.0000 | 143893 | 487.8 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 2104.9 | 1.735 | 2.659 | 3.620 | 1.0000 | 1.0000 | 1.0000 | 798220 | 9165.6 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 2060.0 | 1.746 | 2.803 | 3.488 | 0.6667 | 1.0000 | 0.7767 | 792606 | 9202.9 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | disabled | 2008.1 | 1.757 | 3.048 | 3.796 | 0.5556 | 0.6667 | 1.0000 | 791648 | 9013.4 |
| semantic_minilm | direct_collection | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1774.3 | 1.796 | 3.681 | 9.720 | 0.5556 | 0.8333 | 1.0000 | 809718 | 9116.0 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | disabled | 51815.1 | 0.053 | 0.156 | 0.278 | 1.0000 | 1.0000 | 1.0000 | 75676 | 453.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | tenant_alpha | enabled_cap_2 | 50078.4 | 0.055 | 0.155 | 0.256 | 0.6667 | 1.0000 | 0.7767 | 79096 | 492.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | disabled | 76997.2 | 0.039 | 0.088 | 0.169 | 0.5556 | 0.6667 | 1.0000 | 70097 | 301.5 |
| semantic_minilm | direct_collection | 4 | text_only | none | score_only | unfiltered | enabled_cap_2 | 59928.6 | 0.049 | 0.129 | 0.217 | 0.5556 | 0.8333 | 1.0000 | 96140 | 405.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 2092.7 | 1.721 | 2.894 | 3.673 | 1.0000 | 1.0000 | 1.0000 | 761235 | 8920.0 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 2053.2 | 1.719 | 2.993 | 3.950 | 0.6667 | 1.0000 | 0.7767 | 762153 | 8969.6 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 2163.1 | 1.691 | 2.580 | 3.326 | 0.5556 | 0.6667 | 1.0000 | 744060 | 8809.5 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 2101.2 | 1.711 | 2.761 | 3.526 | 0.5556 | 0.8333 | 1.0000 | 769376 | 8913.3 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | disabled | 105454.1 | 0.024 | 0.081 | 0.125 | 1.0000 | 1.0000 | 1.0000 | 38819 | 208.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | tenant_alpha | enabled_cap_2 | 95850.0 | 0.024 | 0.081 | 0.137 | 0.6667 | 1.0000 | 0.7767 | 43579 | 258.8 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | disabled | 178136.8 | 0.015 | 0.028 | 0.045 | 0.5556 | 0.6667 | 1.0000 | 23799 | 98.1 |
| semantic_minilm | direct_collection | 4 | vector_only | declared_column_graph_exact | score_only | unfiltered | enabled_cap_2 | 113355.2 | 0.022 | 0.065 | 0.100 | 0.5556 | 0.8333 | 1.0000 | 49108 | 201.7 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 403.4 | 2.402 | 2.846 | 3.243 | 1.0000 | 1.0000 | 1.0000 | 1162472 | 12329.6 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 401.7 | 2.400 | 2.902 | 3.310 | 0.6667 | 1.0000 | 0.7767 | 1158571 | 12383.9 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 406.6 | 2.390 | 2.851 | 3.720 | 0.5556 | 0.7222 | 1.0000 | 1183973 | 12221.3 |
| semantic_minilm | http_service | 1 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 404.5 | 2.395 | 2.828 | 3.573 | 0.5556 | 0.8333 | 1.0000 | 1187801 | 12302.1 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | disabled | 493.5 | 1.955 | 2.400 | 2.742 | 1.0000 | 1.0000 | 1.0000 | 921262 | 11113.5 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 487.8 | 1.969 | 2.461 | 2.857 | 0.6667 | 1.0000 | 0.7767 | 915138 | 11153.6 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | disabled | 489.2 | 1.959 | 2.513 | 2.920 | 0.5556 | 0.6667 | 1.0000 | 912848 | 10952.0 |
| semantic_minilm | http_service | 1 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 487.2 | 1.989 | 2.405 | 2.782 | 0.5556 | 0.8333 | 1.0000 | 931399 | 11057.6 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 421.8 | 2.277 | 2.850 | 3.368 | 1.0000 | 1.0000 | 1.0000 | 1072309 | 11625.0 |
| semantic_minilm | http_service | 1 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 351.4 | 2.753 | 3.351 | 3.702 | 0.5556 | 0.6667 | 1.0000 | 1290325 | 12107.4 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1404.3 | 2.542 | 4.283 | 6.786 | 1.0000 | 1.0000 | 1.0000 | 1162246 | 12332.5 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | tenant_alpha | enabled_cap_2 | 1398.5 | 2.530 | 4.409 | 6.951 | 0.6667 | 1.0000 | 0.7767 | 1159006 | 12387.0 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | disabled | 1421.6 | 2.531 | 4.178 | 5.299 | 0.5556 | 0.7222 | 1.0000 | 1183450 | 12223.7 |
| semantic_minilm | http_service | 4 | hybrid | declared_column_graph_exact | fetch_topk | unfiltered | enabled_cap_2 | 1356.7 | 2.612 | 4.765 | 6.847 | 0.5556 | 0.8333 | 1.0000 | 1187508 | 12303.7 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | disabled | 1784.3 | 2.051 | 3.152 | 3.930 | 1.0000 | 1.0000 | 1.0000 | 921557 | 11115.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | tenant_alpha | enabled_cap_2 | 1815.0 | 2.027 | 2.994 | 3.507 | 0.6667 | 1.0000 | 0.7767 | 915190 | 11155.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | disabled | 1819.3 | 2.037 | 2.933 | 3.318 | 0.5556 | 0.6667 | 1.0000 | 912559 | 10953.2 |
| semantic_minilm | http_service | 4 | text_only | none | fetch_topk | unfiltered | enabled_cap_2 | 1760.3 | 2.076 | 3.134 | 3.771 | 0.5556 | 0.8333 | 1.0000 | 931768 | 11059.7 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_exact | fetch_topk | tenant_alpha | disabled | 1514.8 | 2.392 | 3.896 | 4.681 | 1.0000 | 1.0000 | 1.0000 | 1071033 | 11626.6 |
| semantic_minilm | http_service | 4 | vector_only | declared_column_graph_ann | fetch_topk | unfiltered | disabled | 1256.7 | 2.971 | 4.334 | 5.056 | 0.5556 | 0.6667 | 1.0000 | 1292091 | 12109.1 |

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
