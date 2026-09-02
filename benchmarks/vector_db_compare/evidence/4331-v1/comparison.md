# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `e3bffb65c6f1a8308fe16c9f69ac7d958fa921e9275471b1a4eafdc8c32ab805`  
Harness revision: `8d947031fcfd9c8c4d942949f0348c117f591688`  
TreeDB binary SHA-256: `1c059dec1f5019014ca731d9a90f81d86024ce5590e55b714f2acb72ddb4127f`  
TreeDB process CPU / peak RSS: `6.333353s` / `95879168 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU delta`; `getrusage(RUSAGE_SELF) process high-water RSS; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `2.010000s` / `249200640 bytes` / `589248749 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 830.33 | 1.181 | 1.431 | 1.622 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 822.36 | 1.174 | 1.442 | 1.698 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 746.59 | 1.307 | 1.535 | 1.866 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 838.91 | 1.148 | 1.434 | 1.676 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 577.78 | 1.706 | 1.961 | 2.117 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 535.59 | 1.761 | 2.092 | 2.630 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 533.12 | 1.746 | 2.742 | 3.615 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 580.29 | 1.679 | 1.947 | 2.216 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1005.64 | 0.967 | 1.187 | 1.363 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 993.42 | 0.973 | 1.283 | 1.454 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 958.79 | 0.956 | 1.162 | 1.438 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1003.78 | 0.964 | 1.197 | 1.375 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 654.22 | 1.490 | 1.816 | 2.034 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 596.14 | 1.540 | 2.299 | 5.569 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 715.96 | 1.359 | 1.691 | 1.938 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 602.07 | 1.518 | 2.195 | 4.599 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 646.02 | 1.514 | 1.812 | 2.110 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 620.67 | 1.558 | 1.908 | 2.203 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 702.39 | 1.385 | 1.740 | 1.964 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 627.08 | 1.541 | 1.898 | 2.115 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 648.49 | 1.509 | 1.829 | 2.003 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 636.93 | 1.525 | 1.867 | 2.151 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 884.89 | 0.972 | 1.460 | 1.754 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 649.23 | 1.500 | 1.810 | 2.132 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
