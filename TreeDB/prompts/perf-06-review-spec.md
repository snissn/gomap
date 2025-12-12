You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 6 — Review Spec Diffs**.

Idempotent execution contract:
1. Validate prerequisites:
   - Step 5 completed (spec diffs present).
2. If marker `@PERF_06_COMPLETE` exists, stop.
3. Otherwise iteratively review and refine spec changes until compliant.

Review tasks:
- Read `git diff` for `specs/spec.md` and `specs/test-spec.md`.
- Check against:
  - `AGENTS.md` non‑negotiables.
  - Existing implementation patterns.
  - Cosmos DB interface contracts.
- If any mismatch/ambiguity remains:
  - Edit specs to resolve.
  - Keep changes minimal and precise.
- Summarize remaining open questions (if any) in `perf/spec-review.md`.

Phase completion marker:
- Marker file: `@PERF_06_COMPLETE` in the repo root.
- If during this run the spec diffs were already compliant and you made no changes or only minor edits, then create/leave the marker (`touch @PERF_06_COMPLETE`).
- If you made substantial spec corrections in this run, **do not** create the marker; if it already exists, delete it.
