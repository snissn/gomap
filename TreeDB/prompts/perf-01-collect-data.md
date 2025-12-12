You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 1 — Collect Data**. Do not change production code except for temporary, clearly‑scoped instrumentation if required by the plan.

Idempotent execution contract:
1. Validate prerequisites:
   - `perf/plan.md` exists and lists target benches and profiles.
   - Current tests pass (`go test ./...`).
   If missing, explain and stop without changes.
2. If marker `@PERF_01_COMPLETE` exists, summarize existing artifacts in `perf/` and stop.
3. Otherwise collect data as specified in `perf/plan.md` and store artifacts under `perf/`.

Data collection tasks:
- Create `perf/` if missing.
- Run baseline benchmark suite (per plan), e.g.:
  - `go test -run=^$ -bench=Benchmark(Set150B|BatchSet150B|Get150B|IterScan) -benchmem -count=5 -benchtime=3s`
- Save raw output to `perf/bench-<date>.txt`.
- For the primary write benchmark(s), record profiles:
  - `go test -run=^$ -bench=BenchmarkSet150B/InlineThreshold=64  -benchtime=5s -count=1 -cpuprofile=perf/cpu_set64.pprof  -memprofile=perf/mem_set64.pprof`
  - `go test -run=^$ -bench=BenchmarkSet150B/InlineThreshold=256 -benchtime=5s -count=1 -cpuprofile=perf/cpu_set256.pprof -memprofile=perf/mem_set256.pprof`
- If callgraph PNGs are part of the plan:
  - Convert each CPU profile to callgrind and render:
    - `go tool pprof -callgrind -output=perf/cpu_set64.callgrind perf/cpu_set64.pprof`
    - `gprof2dot -f callgrind perf/cpu_set64.callgrind | dot -Tpng -o perf/cpu_set64.png`
  - Repeat for other profiles.
- If temporary instrumentation was needed:
  - Gate it behind build tags or test‑only hooks.
  - Record outputs to `perf/instrumentation-<date>.txt`.
  - Remove/disable instrumentation before finishing this step unless the plan explicitly keeps it for later steps.
- Ensure no stray binaries or temp files remain tracked (delete `*.test` if produced).

Deliverable:
- `perf/bench-<date>.txt`
- `perf/*.pprof` (and optional `perf/*.png`, `perf/*.callgrind`)
- A short `perf/README.md` noting how the artifacts were produced.

Phase completion marker:
- Create `@PERF_01_COMPLETE` once artifacts are present and labeled.
