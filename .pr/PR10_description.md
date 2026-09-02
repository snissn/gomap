PR10: ValueLog dict compression benchmarks + CI baselines + iavl-bench comparison

Purpose
- Implement a stable ValueLog dictionary compressibility benchmark (throughput + observed ratio across compressible/incompressible workloads).
- Add a CI baseline checker (warning-only initially) to detect regressions in throughput/ratio.
- Run iavl-bench `2_run_fast.sh` comparing `main` vs PR10 for both TreeDB and MemDB.

Policy (mandatory)
- Commit often, push often.
- Log all perf results in `slab-optimization/FOLLOW_UP_AGENTS.md` and comment them on the PR.

Planned outputs
A) ValueLog dict compressibility benchmark results
B) CI perf baseline checker output
C) iavl-bench results: main vs PR10 (treedb + memdb)
