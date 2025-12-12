You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 8 — Implement TODOs Iteratively**.

Idempotent execution contract:
1. Validate prerequisites:
   - `AGENTS.md` contains a “Performance Sprint TODOs” section from Step 7.
2. If marker `@PERF_08_COMPLETE` exists, summarize completed TODOs and stop.
3. Otherwise implement TODOs one‑by‑one, committing each.

Implementation loop:
- Parse the TODO list from `AGENTS.md` in order.
- For each TODO item:
  1. Mark item “in progress” in `AGENTS.md`.
  2. Implement the change with minimal scope.
  3. Add/adjust tests per the TODO.
  4. Run targeted tests first, then `go test ./...` (and `-race` if specified).
  5. Rerun the relevant benchmarks; store new outputs under `perf/`.
  6. If results meet the compliance goal, commit with message:
     - `Perf: <short TODO title>`
     - Include bench delta summary in commit body if helpful.
  7. Mark the item “done” in `AGENTS.md`.
  8. If blocked, stop and explain; do not skip items.

Finish:
- When all TODOs are done, rerun full suite + key benchmarks.
- Ensure `specs/` still match implementation.
- Phase completion marker:
  - Marker file: `@PERF_08_COMPLETE` in the repo root.
  - If during this run all TODOs were already complete and you made no changes or only trivial touchups, then create/leave the marker (`touch @PERF_08_COMPLETE`).
  - If you implemented substantial code changes or completed TODOs in this run, **do not** create the marker; if it already exists, delete it.
