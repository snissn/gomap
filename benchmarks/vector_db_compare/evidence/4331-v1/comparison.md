# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `e3bffb65c6f1a8308fe16c9f69ac7d958fa921e9275471b1a4eafdc8c32ab805`  
Harness revision: `2dbd7810b59f5fc1954e9dd2657b5b863e62737c`  
TreeDB binary SHA-256: `5a15cef07b5e5b00a03df2b3ba1dedab39b2400d873e17bf8fef7c5f781289e9`  
TreeDB process CPU / peak RSS: `6.146247s` / `80183296 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU delta`; `getrusage(RUSAGE_SELF) process high-water RSS; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.620000s` / `279773184 bytes` / `683820267 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 904.99 | 1.068 | 1.284 | 1.466 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 906.97 | 1.069 | 1.309 | 1.432 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 888.26 | 1.096 | 1.315 | 1.482 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 920.27 | 1.062 | 1.261 | 1.411 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 598.79 | 1.647 | 1.889 | 1.973 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 618.15 | 1.589 | 1.860 | 1.982 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 632.68 | 1.556 | 1.757 | 1.959 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 608.43 | 1.617 | 1.844 | 1.939 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1099.55 | 0.884 | 1.084 | 1.185 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1091.54 | 0.893 | 1.062 | 1.230 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1044.44 | 0.880 | 1.078 | 1.283 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1102.15 | 0.881 | 1.090 | 1.251 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 665.31 | 1.470 | 1.747 | 2.047 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 670.95 | 1.465 | 1.772 | 2.041 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 740.02 | 1.333 | 1.520 | 1.792 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 670.51 | 1.468 | 1.626 | 1.987 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 650.64 | 1.494 | 1.816 | 2.175 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 651.58 | 1.499 | 1.798 | 2.113 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 712.16 | 1.371 | 1.611 | 2.160 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 652.48 | 1.510 | 1.795 | 2.011 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 655.48 | 1.499 | 1.740 | 2.049 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 658.43 | 1.497 | 1.711 | 2.018 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 923.42 | 0.940 | 1.385 | 1.656 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 677.10 | 1.466 | 1.695 | 2.031 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
