PR9: Perf follow-ups (profiling + regressions)

Purpose
- Run the follow-up perf/profiling runbook in `slab-optimization/FOLLOW_UP_AGENTS.md`.
- Establish stable PR8/PR9 vs `main` baselines (RUNS=5 KEEP=3 SLEEP_S=5) and eliminate any consistent regressions without changing defaults.
- Profile hot paths (trace/syscall/sync/mutex/block) and land targeted fixes.

Policy
- Commit often, push often.
- Every benchmark/profile run must be logged in:
  1) `slab-optimization/FOLLOW_UP_AGENTS.md`
  2) PR conversation comments (include `artifacts/bench/...` log/profile paths)

Initial gate
- Will be posted as a PR comment after running M0.
