# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `27df8eedd78a34685ab31f211e5a6b6a61aa7a25877f153687e4647d20a21f5c`  
Harness revision: `877e61520de512d14cca2d17fdc5f7d9991e5d72`  
TreeDB binary SHA-256: `f7e6ad2b0fce2a5b3d0d298d8abbbb9b4fc1c1ceb5ba7499d3fb96ecfa7ed374`  
TreeDB process CPU / peak RSS: `6.094454s` / `79806464 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before`; `getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant client lock SHA-256: `4c66f563c863801ab692132c5089075dc398959771784756ee9d14f7a353e595`  
Qdrant Python: `3.13.5` / `CPython` / `macOS-26.2-arm64-arm-64bit-Mach-O`  
Qdrant Python executable SHA-256: `a1f6d9dc20d4787a84dc2fe782094a7bac5946f49a962aa0af48a02f0e8d5bc5`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.660000s` / `279199744 bytes` / `366551562 bytes`  
Qdrant durable-byte semantics: `quiesced_after_server_shutdown`

TreeDB durable bytes: `6349968 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 933.74 | 1.042 | 1.273 | 1.392 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 915.09 | 1.052 | 1.296 | 1.510 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 915.19 | 1.070 | 1.253 | 1.377 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 945.95 | 1.034 | 1.246 | 1.380 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 615.86 | 1.596 | 1.842 | 2.051 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 601.88 | 1.620 | 1.859 | 2.010 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 584.96 | 1.629 | 1.893 | 4.209 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 615.43 | 1.588 | 1.821 | 1.982 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1136.87 | 0.846 | 1.075 | 1.265 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1061.72 | 0.869 | 1.046 | 1.260 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1123.19 | 0.864 | 1.002 | 1.279 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1095.08 | 0.864 | 1.153 | 1.294 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 679.89 | 1.450 | 1.551 | 1.910 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 673.05 | 1.464 | 1.727 | 2.069 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 741.71 | 1.331 | 1.538 | 1.724 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 674.08 | 1.461 | 1.725 | 1.892 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 668.27 | 1.478 | 1.592 | 1.963 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 661.69 | 1.487 | 1.836 | 2.163 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 724.47 | 1.357 | 1.656 | 1.992 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 663.89 | 1.481 | 1.702 | 2.016 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 659.57 | 1.488 | 1.764 | 1.989 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 654.16 | 1.502 | 1.773 | 1.947 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 916.89 | 0.943 | 1.403 | 1.828 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 650.10 | 1.473 | 1.802 | 3.275 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- All TreeDB-versus-Qdrant latency rows are directional: lexical scoring differs; TreeDB dense/hybrid uses declared_column_graph_exact, while Qdrant HNSW is indexed and exact=false is requested but server planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
- CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.
