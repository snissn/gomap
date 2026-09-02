# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `2081d294eec7becd098c0355ce87b259c4b877a5474074a1ce681472693d42b9`  
Harness revision: `e44cfc0ecdde6f0e5a2d67d3d3337ccc95a433eb`  
TreeDB binary SHA-256: `d4e4031e2e3cb24826be60002e5b74d9104747beef584f39bd8912b0b64ea687`  
TreeDB process CPU / peak RSS: `6.039014s` / `96583680 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before`; `getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.680000s` / `280526848 bytes` / `366551574 bytes`

TreeDB durable bytes: `6349956 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 940.05 | 1.042 | 1.238 | 1.359 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 927.59 | 1.041 | 1.274 | 1.416 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 917.07 | 1.061 | 1.276 | 1.392 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 935.09 | 1.030 | 1.279 | 1.460 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 610.81 | 1.602 | 1.864 | 2.065 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 608.12 | 1.604 | 1.849 | 2.003 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 580.83 | 1.638 | 1.955 | 3.942 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 626.89 | 1.581 | 1.768 | 1.977 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1123.03 | 0.861 | 1.035 | 1.229 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1058.14 | 0.874 | 1.078 | 1.229 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1127.85 | 0.861 | 0.988 | 1.207 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1040.63 | 0.910 | 1.196 | 1.669 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 686.08 | 1.444 | 1.524 | 1.785 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 685.42 | 1.454 | 1.592 | 1.937 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 745.08 | 1.327 | 1.565 | 1.771 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 689.19 | 1.440 | 1.536 | 1.807 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 669.48 | 1.474 | 1.670 | 1.979 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 662.51 | 1.491 | 1.650 | 2.132 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 721.56 | 1.358 | 1.629 | 2.020 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 668.24 | 1.478 | 1.625 | 2.052 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 686.89 | 1.454 | 1.564 | 1.796 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 668.27 | 1.481 | 1.776 | 1.873 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 931.56 | 0.934 | 1.380 | 1.700 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 687.83 | 1.443 | 1.640 | 1.920 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
- CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.
