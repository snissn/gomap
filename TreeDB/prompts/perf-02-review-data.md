You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 2 — Review Data & Find Hotspots**. Do not change production code.

Idempotent execution contract:
1. Validate prerequisites:
   - `perf/bench-*.txt` and relevant `perf/*.pprof` artifacts exist.
   If missing, explain and stop without changes.
2. If marker `@PERF_02_COMPLETE` exists, summarize `perf/problems.md` and stop.
3. Otherwise analyze artifacts and persist findings.

Analysis tasks:
- Parse benchmark results:
  - Extract `ns/op`, `ops/s`, `B/op`, `allocs/op` for each benchmark/config.
  - Use `benchstat` if multiple runs exist to compute deltas and confidence.
  - Identify any regressions vs baseline targets in `perf/plan.md`.
- Inspect CPU profiles:
  - Run `go tool pprof -top` and `-top -cum` for each `perf/cpu_*.pprof`.
  - Focus on user code stacks (tree COW, pager, slab, iterator) and runtime/GC share.
  - Note syscall hotspots (`pwrite`, `msync`, `kevent`) separately.
- Inspect memory profiles:
  - Run `go tool pprof -top -alloc_space perf/mem_*.pprof`.
  - Identify allocation sources and whether they are avoidable.
- Summarize derived metrics:
  - Index pages written/op (if instrumentation present).
  - Slab bytes/op and slab syscall rate.
  - Tree depth/fill if captured.

Deliverable:
- Write `perf/problems.md` with:
  - A ranked list of performance problems/hot spots.
  - Evidence for each (bench numbers, profile lines).
  - Expected impact class (high/medium/low) and area (tree/slab/pager/iterator/GC).

Phase completion marker:
- Create `@PERF_02_COMPLETE` when `perf/problems.md` is complete and no code changes were made.
