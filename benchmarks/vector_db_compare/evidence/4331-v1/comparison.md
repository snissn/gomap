# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `27df8eedd78a34685ab31f211e5a6b6a61aa7a25877f153687e4647d20a21f5c`  
Harness revision: `3116223487bfb24e667b84b26f58c1442337dccc`  
TreeDB binary SHA-256: `100a8eb3a1030ff65113b2efc8bf2715bc7fa33fc9224f54c6fb449ef4f1dffa`  
TreeDB process CPU / peak RSS: `6.603939s` / `79953920 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before`; `getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant client lock SHA-256: `4c66f563c863801ab692132c5089075dc398959771784756ee9d14f7a353e595`  
Qdrant Python: `3.13.5` / `CPython` / `macOS-26.2-arm64-arm-64bit-Mach-O`  
Qdrant Python executable SHA-256: `a1f6d9dc20d4787a84dc2fe782094a7bac5946f49a962aa0af48a02f0e8d5bc5`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.730000s` / `274726912 bytes` / `366551572 bytes`  
Qdrant durable-byte semantics: `quiesced_after_server_shutdown`

TreeDB durable bytes: `6350013 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 935.63 | 1.035 | 1.255 | 1.403 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 914.54 | 1.062 | 1.313 | 1.435 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 794.69 | 1.206 | 1.474 | 1.961 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 945.46 | 1.034 | 1.236 | 1.398 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 603.69 | 1.617 | 1.865 | 2.052 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 604.62 | 1.628 | 1.823 | 1.987 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 573.55 | 1.653 | 1.983 | 4.198 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 542.37 | 1.618 | 1.924 | 2.117 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1109.33 | 0.863 | 1.129 | 1.363 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1051.98 | 0.869 | 1.108 | 1.370 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1107.41 | 0.874 | 1.062 | 1.265 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1087.00 | 0.868 | 1.179 | 1.384 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 616.22 | 1.604 | 1.733 | 1.993 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 614.18 | 1.616 | 1.810 | 2.154 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 651.82 | 1.477 | 1.951 | 2.139 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 619.82 | 1.600 | 1.752 | 1.946 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 606.65 | 1.630 | 1.745 | 2.156 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 599.50 | 1.647 | 1.983 | 2.173 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 658.59 | 1.502 | 1.789 | 1.970 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 604.27 | 1.635 | 1.776 | 2.238 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 616.71 | 1.614 | 1.803 | 2.012 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 601.11 | 1.642 | 1.905 | 2.127 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 838.51 | 1.032 | 1.586 | 1.804 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 621.99 | 1.600 | 1.834 | 2.020 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
- CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.
