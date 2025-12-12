You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 7 — TODO List & Compliance Goals**.

Idempotent execution contract:
1. Validate prerequisites:
   - Step 6 completed (specs stable).
2. If marker `@PERF_07_COMPLETE` exists, summarize the TODO list and stop.
3. Otherwise update `AGENTS.md` with a concrete, checkable TODO plan.

Tasks:
- Append a section to `AGENTS.md` titled “Performance Sprint TODOs”.
- Convert each approved improvement into a TODO item:
  - One‑sentence goal.
  - Files/areas to touch.
  - Correctness tests to add/run.
  - Perf benchmarks to rerun and expected directionality.
- Include clear compliance goals:
  - e.g., “`BenchmarkSet150B` allocations ≤ X; ns/op improves ≥ Y% vs baseline”.
- Keep the list ordered and dependency‑aware.

Phase completion marker:
- Marker file: `@PERF_07_COMPLETE` in the repo root.
- If during this run the TODO section already existed and needed no changes or only minor tweaks, then create/leave the marker (`touch @PERF_07_COMPLETE`).
- If you added a new TODO section or made substantial updates to `AGENTS.md`, **do not** create the marker; if it already exists, delete it.
