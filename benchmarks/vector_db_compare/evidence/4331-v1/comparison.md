# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `2081d294eec7becd098c0355ce87b259c4b877a5474074a1ce681472693d42b9`  
Harness revision: `03fe08f6d8a812c21ead64230e3fe7b73c1e33fd`  
TreeDB binary SHA-256: `0ef08cd9a71fdd652168a3683d84fd359b2a33abf7e4b5ae496aa1b02408c16a`  
TreeDB process CPU / peak RSS: `6.072812s` / `79675392 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before`; `getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.730000s` / `279183360 bytes` / `366551579 bytes`

TreeDB durable bytes: `6349918 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 926.66 | 1.053 | 1.259 | 1.422 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 876.57 | 1.098 | 1.371 | 1.608 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 859.86 | 1.139 | 1.358 | 1.510 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 928.83 | 1.046 | 1.297 | 1.471 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 600.75 | 1.642 | 1.875 | 1.975 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 572.04 | 1.703 | 1.959 | 2.187 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 565.53 | 1.676 | 1.994 | 4.315 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 599.12 | 1.626 | 1.887 | 2.192 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1137.67 | 0.853 | 1.037 | 1.188 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1045.11 | 0.875 | 1.106 | 1.324 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1136.75 | 0.854 | 1.013 | 1.261 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1120.09 | 0.855 | 1.110 | 1.296 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 681.07 | 1.450 | 1.578 | 1.885 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 674.57 | 1.463 | 1.582 | 2.073 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 739.72 | 1.334 | 1.485 | 1.802 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 683.21 | 1.448 | 1.529 | 1.926 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 664.49 | 1.483 | 1.692 | 1.973 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 642.16 | 1.515 | 1.911 | 2.218 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 713.44 | 1.365 | 1.655 | 2.172 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 660.26 | 1.486 | 1.690 | 1.966 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 671.88 | 1.476 | 1.558 | 1.850 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 657.75 | 1.491 | 1.777 | 2.081 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 909.04 | 0.947 | 1.419 | 1.899 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 673.79 | 1.466 | 1.662 | 2.022 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
- CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.
