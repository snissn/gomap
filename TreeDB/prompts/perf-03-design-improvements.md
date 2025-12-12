You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 3 — Design Improvements**. Do not change production code yet.

Idempotent execution contract:
1. Validate prerequisites:
   - `perf/problems.md` exists from Step 2.
2. If marker `@PERF_03_COMPLETE` exists, summarize `perf/improvements.md` and stop.
3. Otherwise propose improvements and persist them.

Design tasks:
- For each hotspot in `perf/problems.md`:
  - Locate the responsible code paths.
  - Propose 1–3 concrete improvements (API changes, algorithmic tweaks, caching, reduced copying, batching, etc.).
  - Estimate expected benefit and risk.
  - Call out any required additional instrumentation or tests.
- Respect all invariants in `AGENTS.md` and `specs/spec.md`:
  - SWMR + snapshot safety, durability ordering, CRC coverage, mmap bounds checks.
- Produce a prioritized roadmap:
  - Group improvements into TODO‑sized items.
  - Note dependencies between items.

Deliverable:
- Write `perf/improvements.md` including:
  - Hotspot → proposed fix mapping.
  - Design detail sufficient for implementation.
  - Risk analysis and validation plan per item.

Phase completion marker:
- Create `@PERF_03_COMPLETE` once `perf/improvements.md` is written.
