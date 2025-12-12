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
- Marker file: `@PERF_03_COMPLETE` in the repo root.
- If during this run `perf/improvements.md` already existed and you made no changes or only minor clarifications, then create/leave the marker (`touch @PERF_03_COMPLETE`).
- If you created `perf/improvements.md` or made substantial new design changes, **do not** create the marker; if it already exists, delete it.
