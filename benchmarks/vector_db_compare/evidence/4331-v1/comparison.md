# TreeDB / Qdrant bounded RAG comparison

State: **validated**  
Manifest SHA-256: `27df8eedd78a34685ab31f211e5a6b6a61aa7a25877f153687e4647d20a21f5c`  
Harness revision: `d41fef853bb3ad5c5790e8b49ff74c69ae131ed0`  
TreeDB binary SHA-256: `7e5042d330cd0a37ce6347dcf57551f90a2b46cd117be283ed4f7a80a82230aa`  
TreeDB process CPU / peak RSS: `7.409620s` / `79118336 bytes`  
TreeDB resource semantics: `getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before`; `getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes`; `fresh comparison process; build, lifecycle reopen, and all 12 query cells`  
Qdrant client/server: `1.19.0` / `1.19.0`  
Qdrant client lock SHA-256: `4c66f563c863801ab692132c5089075dc398959771784756ee9d14f7a353e595`  
Qdrant Python: `3.13.5` / `CPython` / `macOS-26.2-arm64-arm-64bit-Mach-O`  
Qdrant Python executable SHA-256: `a1f6d9dc20d4787a84dc2fe782094a7bac5946f49a962aa0af48a02f0e8d5bc5`  
Qdrant server binary SHA-256: `036b94e5a39f1ea8f2329c8e528fcea54f83eb9205221a7dc1623c9862acc74d`  
Qdrant release asset SHA-256: `4e279a80cc1ebe73e859318ff86375af54c123887dd7ae46605c0eb6cb7c44e8`  
Qdrant process CPU / observed peak RSS / durable bytes: `1.810000s` / `278855680 bytes` / `366551575 bytes`  
Qdrant durable-byte semantics: `quiesced_after_server_shutdown`

TreeDB durable bytes: `6349968 bytes`

| Backend | Route | Filter | Semantics | Samples | Reps | QPS | p50 ms | p95 ms | p99 ms | P@10 | nDCG@10 | MRR@10 | Parent R@10 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| qdrant | dense | tenant_alpha | directional | 300 | 3 | 894.88 | 1.079 | 1.341 | 1.514 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 905.47 | 1.077 | 1.304 | 1.471 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 863.81 | 1.147 | 1.360 | 1.516 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | dense | unfiltered | directional | 300 | 3 | 917.63 | 1.062 | 1.277 | 1.456 | 1.000 | 1.000 | 1.000 | 0.667 |
| qdrant | hybrid | tenant_alpha | directional | 300 | 3 | 580.60 | 1.685 | 1.937 | 2.113 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 582.34 | 1.688 | 1.938 | 2.099 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 507.10 | 1.775 | 2.292 | 5.930 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | hybrid | unfiltered | directional | 300 | 3 | 600.03 | 1.638 | 1.878 | 2.130 | 1.000 | 1.000 | 1.000 | 0.722 |
| qdrant | lexical | tenant_alpha | directional | 300 | 3 | 1120.81 | 0.867 | 1.040 | 1.194 | 0.900 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 1033.32 | 0.893 | 1.095 | 1.278 | 0.600 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 1017.59 | 0.888 | 1.247 | 3.517 | 0.300 | 1.000 | 1.000 | 1.000 |
| qdrant | lexical | unfiltered | directional | 300 | 3 | 1091.39 | 0.877 | 1.119 | 1.243 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | dense | tenant_alpha | directional | 300 | 3 | 555.79 | 1.778 | 2.102 | 2.374 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red | directional | 300 | 3 | 556.10 | 1.770 | 2.093 | 2.464 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | dense | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 602.34 | 1.623 | 2.024 | 2.380 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | dense | unfiltered | directional | 300 | 3 | 564.84 | 1.726 | 2.014 | 2.318 | 1.000 | 1.000 | 1.000 | 0.667 |
| treedb | hybrid | tenant_alpha | directional | 300 | 3 | 516.60 | 1.844 | 2.513 | 4.195 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red | directional | 300 | 3 | 527.36 | 1.836 | 2.339 | 2.850 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 583.67 | 1.665 | 2.146 | 2.527 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | hybrid | unfiltered | directional | 300 | 3 | 493.03 | 1.796 | 3.943 | 6.331 | 1.000 | 1.000 | 1.000 | 0.722 |
| treedb | lexical | tenant_alpha | directional | 300 | 3 | 544.57 | 1.821 | 2.054 | 2.406 | 0.900 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red | directional | 300 | 3 | 549.31 | 1.784 | 2.094 | 2.447 | 0.600 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | tenant_alpha_workspace_red_updated_ge_2024 | directional | 300 | 3 | 756.61 | 1.160 | 1.770 | 2.165 | 0.300 | 1.000 | 1.000 | 1.000 |
| treedb | lexical | unfiltered | directional | 300 | 3 | 510.25 | 1.866 | 2.355 | 3.091 | 1.000 | 1.000 | 1.000 | 0.667 |

## Dispositions

- Latency and QPS are transport-non-comparable directional diagnostics: TreeDB uses the in-process direct_collection surface, while Qdrant uses URL-backed HTTP query_points plus retrieve; lexical scoring also differs, TreeDB dense/hybrid uses declared_column_graph_exact, and Qdrant HNSW planner selection is opaque.
- Parent collapse is disabled for both systems; chunk rankings are retained and parent recall is derived from frozen parent IDs.
- The 18-source/54-chunk synthetic fixture is bounded comparison evidence, not a public winner claim.
- CPU and RSS figures are scoped diagnostics, not cross-backend comparisons: TreeDB includes the in-process Go driver while Qdrant covers only the standalone server PID.
- Durable-byte totals are storage-non-comparable diagnostics: the TreeDB root includes the source collection and separately projected query collection, while the Qdrant root contains only the 54-chunk query collection.
