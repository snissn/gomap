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
- Create `@PERF_04_COMPLETE` when the approved list is stable.
