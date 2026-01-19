# Follow‑Up Performance / Profiling Runbook (Post PR0–PR8)

This document captures the **follow‑up work** after PR0–PR8 to (a) cement wins, (b) eliminate any remaining regressions, and (c) validate that all new features are performant under realistic and adversarial workloads.

Primary audiences: maintainers running the slab‑optimization waterfall and perf owners reviewing PR8 (#67).

## Guiding Principles

- **Apples‑to‑apples:** do not claim wins by changing defaults in one branch but not the other. When changing a knob, run the same knob on both `main` and PR8 and report both.
- **Stable methodology:** use **RUNS=5 KEEP=3 SLEEP_S=5** (trimmed mean) and record the full log path under `artifacts/bench/`.
- **Separate concerns:** measure baseline first (no new flags), then sweep a single feature at a time.
- **Profile to explain:** every regression investigation should end in:
  1) a reproducible bench case, 2) a profile identifying hotspots, 3) a targeted fix, 4) a re‑run showing improvement.
- **Prefer realistic data:** the iavl-bench synthetic uses incompressible values; for compression features use compressible dataset patterns.

## Baseline Gate (Regression Sentinel)

### Purpose
Single source of truth for “PR8 vs main” with consistent settings.

### Command
Run from repo root:

```bash
RUNS=5 KEEP=3 SLEEP_S=5 \
KEYS=1000000 VALSIZE=1024 BATCHSIZE=1000 \
bash scripts/bench_compare_pr8_vs_main_trimmed.sh
```

### Record
- Log path printed at end (e.g. `artifacts/bench/compare_pr8_vs_main_trimmed_<ts>.log`).
- Paste the summary table + link to the log in PR8 (#67) as a comment.

## Profiling Toolkit (Unified Bench)

Unified bench supports:
- `-trace <file>` (runtime trace)
- `-blockprofile <file>` and `-blockprofilerate 1`
- `-mutexprofile <file>` and `-mutexprofilefraction 1`
- `-cpuprofile <file>` (when available)

**Default recommendation (fast, actionable):**
- Use `-trace` + `go tool trace -pprof=sync|syscall|sched` to identify blocking + syscall hotspots.
- Add `-blockprofile`/`-mutexprofile` when contention is suspected.

### Profile capture template
Pick a single test and a moderate keycount so profiles are readable:

```bash
go run ./cmd/unified_bench \
  -dbs treedb -test random_write -keys 200000 -valsize 1024 -batchsize 1000 \
  -progress=false \
  -trace artifacts/bench/pr8_random_write_200k.trace \
  -blockprofile artifacts/bench/pr8_random_write_200k.block.pprof \
  -mutexprofile artifacts/bench/pr8_random_write_200k.mutex.pprof

go tool trace -pprof=sync artifacts/bench/pr8_random_write_200k.trace > artifacts/bench/pr8_random_write_200k.trace.sync.pb.gz
go tool trace -pprof=syscall artifacts/bench/pr8_random_write_200k.trace > artifacts/bench/pr8_random_write_200k.trace.syscall.pb.gz
go tool pprof -top artifacts/bench/pr8_random_write_200k.trace.syscall.pb.gz | head -n 40
go tool pprof -top artifacts/bench/pr8_random_write_200k.trace.sync.pb.gz | head -n 40
```

Run the same commands for `main` (via the built binary/worktree, or by checking out `main`) and compare.

## Immediate Follow‑Ups (Known Sensitive Areas)

These are the first “profile and optimize” targets because they’re most likely to hide medium‑hanging fruit.

### 1) Write throughput (journal + value log)
Focus: `batch_write` and `random_write` at `valsize=1024` with default `ValueLogPointerThreshold` (256).

- Baseline profiles (dict training disabled):
  - `random_write` (200k keys): capture trace + syscall + sync.
  - `batch_write` (200k–500k keys): capture trace + syscall + sync.
- Then repeat with multi‑lane enabled:
  - Sweep `-treedb-journal-lanes 1,2,4`.
  - Confirm the best lane count and whether extra lanes help at all under the benchmark’s concurrency model.

**Codepaths to examine when slow:**
- commitlog write path: record encoding/CRC, `bufio` flush cadence, rotate behavior.
- value‑log path: frame layout, per‑write allocations, dictionary lookup, dict codec pooling.
- lane locking: contention on `laneMu`, `walMu`, `vlogMu` (block/mutex profiles).
- syscall write amplification: `syscall.write`, `Open`, `Fstat`, `fsync`/`fdatasync` if enabled by profile.

### 2) Dict compression on write path (enabled)
Purpose: validate that enabling dict training/compression doesn’t regress incompressible workloads and provides real wins on compressible workloads.

#### 2a) Compressible datasets
Use dataset value patterns to create compressible payloads:
- `-dataset-val-pattern zero`
- `-dataset-val-pattern repeat`

Suggested matrix:
- Tests: `batch_write`, `random_write`, `random_read`, `prefix_scan`
- Data patterns: `random` vs `repeat`
- Dict training: OFF vs ON

Example (dict training ON; tune as needed):
```bash
go run ./cmd/unified_bench \
  -dbs treedb -test batch_write -keys 1000000 -valsize 1024 -batchsize 1000 \
  -dataset-val-pattern repeat \
  -treedb-split-value-log \
  -treedb-vlog-dict-train-bytes $((16<<20)) \
  -treedb-vlog-dict-dict-bytes $((40<<10)) \
  -treedb-vlog-dict-min-records 2048 \
  -treedb-vlog-dict-sample-stride 8 \
  -treedb-vlog-dict-min-savings-ratio 0.01
```

#### 2b) Incompressible datasets (regression guard)
Repeat the same matrix with `-dataset-val-pattern random` to ensure:
- dict training doesn’t burn CPU unnecessarily (adaptive pause should kick in),
- no huge throughput collapse from training overhead.

**Profile targets when dict training is ON:**
- trainer CPU/alloc overhead (trace sched latency + syscall; CPU profile if available),
- dictionary publication path (dictdb writes, SetCurrent frequency),
- pause/adaptive logic overhead (avoid per‑op expensive time calls).

### 3) Split value‑log vs unified WAL/vlog behavior
We need explicit evidence about:
- when split value‑log helps (reduced contention / better IO pattern),
- when it hurts (extra writes / extra metadata / extra syscalls).

Matrix:
- `-treedb-split-value-log` OFF/ON
- `-treedb-value-log-threshold`: 256 (default) and a larger value for sensitivity exploration (but apply to both branches).
- `random_write` and `batch_write` at `valsize=1024`.

### 4) Read path hot spots (RID indirection + checksum)
Focus: `random_read` improvements/regressions.

Matrix:
- `-treedb-disable-read-checksum` OFF/ON (unsafe; used only to isolate CRC cost)
- With/without value‑log pointers (implicit via threshold and SplitValueLog)

Profile targets:
- CRC verification cost (syscall + CPU),
- value‑log read amplification patterns,
- cache hit rate (if measurable via existing stats/trace patterns).

## Feature Validation Matrix (PR0–PR8)

Goal: ensure new features are not only correct, but also performant and “worth it”.

### A) Journal lanes (PR5)
- Sweep `-treedb-journal-lanes 1,2,4,8` on:
  - `batch_write` (high throughput)
  - `random_write` (more metadata churn)
- Record best lane count per workload.
- If lanes don’t help: profile for lock contention, writer wakeups, per‑lane overhead, and commit ordering costs.

### B) DictDB + lagging rule (PR2/PR4/PR6)
- Ensure dictdb current pointer reads/writes are amortized and not in the hot loop unnecessarily.
- Test publication frequency vs throughput:
  - vary `-treedb-vlog-dict-train-bytes` (e.g. 8MiB, 16MiB, 32MiB)
  - vary sampling (`-treedb-vlog-dict-sample-stride`, `-treedb-vlog-dict-max-record-bytes`)
- Confirm “no‑op dict” avoidance works on random payloads (low CPU cost, no perf cliff).

### C) Dynamic‑K grouped frames (PR4)
- Sweep `K` behavior via knobs (when present) by driving:
  - small vs large values
  - repeat vs random values
- Profile `AppendFrame` overhead: frame header encoding, offsets build, zstd encode/decode cost.

### D) Recovery hardening (PR6)
Perf‑sensitive check:
- ensure recovery work (two‑pass join, dict missing checks) doesn’t slow normal steady‑state writes.
- add a “reopen cost” microbench if we suspect open/recovery time is creeping up.

### E) Index flags (PR7)
Validate performance and memory/disk tradeoffs when enabled:
- `-treedb-index-columnar-leaves`
- `-treedb-index-internal-base-delta`
- plus existing index toggles:
  - `-treedb-leaf-prefix-compression`
  - `-treedb-prefer-append-alloc`
  - fill targets: `-treedb-leaf-fill-ppm`, `-treedb-internal-fill-ppm`

Matrix:
- `random_read` (point lookups)
- `prefix_scan` (range performance)
- `batch_write` (builder/split/compact impact)

Record:
- ops/sec
- disk usage of `index.db` (and `wal/` + `vlog/` if applicable)
- RSS if available (Linux: use `time -v` or `/proc/<pid>/status` sampling in a wrapper script)

## “Pathological Streams” to Add / Keep

We should maintain a set of workloads that are intentionally adversarial:

- **Incompressible values:** `-dataset-val-pattern random`, `valsize=1024`
- **Highly compressible values:** `-dataset-val-pattern repeat`, `valsize=1024`
- **Small values near threshold:** `valsize` in `[128..512]` and sweep `-treedb-value-log-threshold` to expose boundary effects
- **Large values:** `valsize=4096` (ensures pointer path dominates)
- **Mixed sizes:** (follow‑up: add a unified_bench mixed‑valsize dataset if missing)
- **Skewed key distributions:** repeated hot prefixes to stress iterator/zipper locality

## “When you find a regression” Workflow

1) **Reproduce** with the baseline gate and capture the log under `artifacts/bench/`.
2) **Minimize** to a single test case and smallest keycount that still shows the delta.
3) **Profile** both `main` and PR8 for that test:
   - trace + syscall + sync
   - mutex/block profiles if contention suspected
4) **Fix** only the responsible hotspot (avoid broad refactors).
5) **Verify**:
   - rerun baseline gate
   - rerun the minimized test 2–3 times to confirm stability
6) **Report**:
   - add a PR8 comment summarizing: reproduction, profile evidence, fix, new numbers, and the log path(s).

## Future Enhancements (Bench Infrastructure)

If profiling indicates we’re missing signal, consider follow‑ups:
- add a dedicated unified_bench “mixed values” test (size distribution + compressible mix)
- add a “concurrent writers” mode for `random_write` / `batch_write` to better exercise journal lanes
- add explicit disk usage reporting per DB in unified_bench output for every test
- add optional RSS reporting (Linux) into unified_bench runner output

