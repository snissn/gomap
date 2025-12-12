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
- Create `@PERF_06_COMPLETE` when specs are unambiguous and aligned.
