You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 4 — Security & Correctness Review**. No production code changes.

Idempotent execution contract:
1. Validate prerequisites:
   - `perf/improvements.md` exists.
2. If marker `@PERF_04_COMPLETE` exists, summarize `perf/review.md` and stop.
3. Otherwise review and refine improvements until safe.

Review tasks:
- For each proposed improvement:
  - Check correctness vs spec:
    - Key encoding/namespace isolation.
    - Iterator semantics.
    - COW + graveyard + pruning safety.
    - Slab durability ordering.
  - Check security/safety:
    - mmap bounds and SIGBUS avoidance.
    - CRC validation placement.
    - Memory‑bounded buffering (avoid OOM).
    - Concurrency races under SWMR/compaction.
  - Identify required guards, fallbacks, or tests.
- If any improvement is unsafe or ambiguous:
  - Revise it in place (edit `perf/improvements.md`) or document alternatives.

Deliverable:
- Write `perf/review.md` containing:
  - Final approved improvement list.
  - Required safety notes/guards.
  - Any dropped/reworked items with rationale.

Phase completion marker:
- Marker file: `@PERF_04_COMPLETE` in the repo root.
- If during this run `perf/review.md` already existed and required no changes or only minor edits, then create/leave the marker (`touch @PERF_04_COMPLETE`).
- If you created `perf/review.md` or made substantial safety/correctness revisions, **do not** create the marker; if it already exists, delete it.
