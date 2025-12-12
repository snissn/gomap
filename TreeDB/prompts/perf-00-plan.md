You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and perform only **Performance Sprint Step 0 — Plan & Metrics**. Do not implement optimizations yet.

Idempotent execution contract:
1. Validate prerequisites:
   - Benchmarks exist (at least `bench_test.go` in root `treedb` package).
   - Specs are present under `specs/` and current code passes `go test ./...`.
   If missing, explain gaps and stop without changes.
2. If marker `@PERF_00_COMPLETE` exists, summarize prior plan (if any) and stop.
3. Otherwise create/refresh a sprint plan and persist it to `perf/plan.md` (create `perf/` if absent).

Planning tasks:
- Re-read `AGENTS.md` for performance‑relevant requirements and metrics:
  - Bench command guidance (`go test -bench`, `-benchmem`, `-race`) and any perf/telemetry expectations.
  - InlineThreshold/Fanout benchmark expectations in `specs/test-spec.md` (1.6 Fanout vs Fragmentation).
  - Mandatory durability, CRC, mmap safety invariants that optimizations must not violate.
- Define a **performance sprint scope**:
  - Target workloads (e.g., 150B IAVL‑like values, range scans, compaction copy/verify‑set).
  - Non‑goals (no behavior changes, no durability weakening, no mmap risk increase).
- Specify **metrics to track**, how to compute them, and why they matter:
  - **Throughput/latency:** `ns/op`, `ops/s` from `go test -bench`, for:
    - `BenchmarkSet150B`, `BenchmarkBatchSet150B`, `BenchmarkGet150B`, `BenchmarkIterScan`.
    - Any long/tagged bench for 1M‑key fanout validation if needed.
  - **Alloc pressure:** `B/op`, `allocs/op` from `-benchmem`.
  - **Index write amplification:** pages dirtied/retired per commit × `PageSize`.
    - If not already exposed, note required temporary instrumentation.
  - **Slab IO:** bytes appended per commit and syscall count.
    - Use `slab_write_bytes` (from batch prepare) and CPU profiles (`pwrite`/`WriteAt` stacks) as proxies.
  - **GC share:** time in `runtime.gc*` and alloc rate from CPU/mem profiles.
  - **Tree health:** depth, leaf fill, splits/commit.
    - Use `adaptive.ComputeLeafStats` or a debug walk to derive depth and fill.
- Define the **data collection protocol**:
  - Environment controls: fixed `GOMAXPROCS`, `-count=5`, `-benchtime=3s`, avoid background load.
  - Commands to run (exact strings) and how to store results:
    - Bench outputs to `perf/bench-<date>.txt`.
    - CPU profiles to `perf/cpu_<bench>_<config>.pprof`.
    - Mem profiles to `perf/mem_<bench>_<config>.pprof`.
    - Optional callgraph PNGs via `go tool pprof -callgrind` + `gprof2dot`.
  - File naming and a note to avoid committing large artifacts unless asked.
- Define **success criteria**:
  - Primary: reduce write `ns/op` and allocations without increasing read latency beyond acceptable bounds.
  - Secondary: reduce slab syscalls/commit and index pages written/op.

Deliverable:
- Write `perf/plan.md` containing:
  - Sprint scope & timeline.
  - Metric definitions with formulas.
  - Data collection steps/commands.
  - Baseline expectations and success criteria.

Phase completion marker:
- Marker file: `@PERF_00_COMPLETE` in the repo root.
- If during this run the plan already existed and you made no changes or only minor edits, then create/leave the marker (`touch @PERF_00_COMPLETE`).
- If you created a new plan or made substantial updates to `perf/plan.md`, **do not** create the marker; if it already exists, delete it.
