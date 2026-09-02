# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `e3bffb65c6f1a8308fe16c9f69ac7d958fa921e9275471b1a4eafdc8c32ab805`  
Harness revision: `0811f2d037aa3873cdfab6413c52c018e7e7f062`  
TreeDB binary SHA-256: `76801ebffc404333cc8bf9270296bf4a12e7098241b7ba64f254a4863f17c4d8`  
TreeDB process CPU / peak RSS: `6.308067s` / `96714752 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU delta`; `getrusage(RUSAGE_SELF) process high-water RSS; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.900000s` / `248365056 bytes` / `589248714 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 832.56 | 1.169 | 1.430 | 1.569 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 837.40 | 1.168 | 1.398 | 1.542 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 769.97 | 1.270 | 1.601 | 1.810 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 827.35 | 1.164 | 1.454 | 1.942 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 593.88 | 1.654 | 1.892 | 2.058 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 581.38 | 1.675 | 1.950 | 2.089 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 570.88 | 1.676 | 2.000 | 4.000 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 582.23 | 1.635 | 1.987 | 3.921 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1033.74 | 0.940 | 1.126 | 1.270 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1012.72 | 0.964 | 1.159 | 1.321 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 971.17 | 0.949 | 1.161 | 1.371 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1004.74 | 0.955 | 1.260 | 1.367 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 659.95 | 1.474 | 1.698 | 1.901 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 658.14 | 1.479 | 1.827 | 2.132 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 704.27 | 1.370 | 1.728 | 2.096 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 661.80 | 1.476 | 1.728 | 1.971 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 643.36 | 1.518 | 1.852 | 2.055 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 632.48 | 1.536 | 1.922 | 2.255 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 683.22 | 1.404 | 1.866 | 2.327 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 645.17 | 1.513 | 1.816 | 1.991 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 649.07 | 1.511 | 1.807 | 2.084 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 634.20 | 1.527 | 1.878 | 2.220 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 899.54 | 0.955 | 1.405 | 1.711 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 649.42 | 1.505 | 1.776 | 1.991 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
