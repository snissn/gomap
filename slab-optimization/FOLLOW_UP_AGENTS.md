> **Legacy note:** This runbook predates the WAL on/off simplification and may reference removed options.
> Use the current merge-gate runbook + docs for up-to-date guidance.

# Follow‑Up Performance / Profiling Runbook (Post PR0–PR8)

This document captures the **follow‑up work** after PR0–PR8 to (a) cement wins, (b) eliminate any remaining regressions, and (c) validate that all new features are performant under realistic and adversarial workloads.

Primary audiences: maintainers running the slab‑optimization waterfall and perf owners reviewing PR8 (#67).

## PR9 Policy (Mandatory)

All follow‑up perf/profiling work MUST happen in a new PR:
- **PR9**: release‑candidate follow‑ups (profiling, benchmarks, targeted perf fixes, and bench infrastructure improvements).

**This policy MUST be followed.** Do not land additional perf/profiling changes directly on PR8 or earlier PR branches.

### PR9 workflow requirements
- **Branching:** create `sprint/slabopt-pr9-perf-followups` based on `sprint/slabopt-pr8-regression-rc`.
- **PR creation:** MUST open PR9 via GitHub CLI (`gh pr create`) and keep it updated.
- **Commit cadence:** commit often (small, scoped changes) and push often (so CI + reviewers see progress).
- **Reporting cadence:** every meaningful benchmark/profiling result MUST be logged in two places:
  1) append/update this file: `slab-optimization/FOLLOW_UP_AGENTS.md` (include timestamp + command + log paths + key deltas)
  2) add a comment to the PR9 conversation with the same summary (include `artifacts/bench/...` paths)
- **No rewriting history:** avoid force pushes; keep a linear story for perf investigations.

### What qualifies as “must log”
- Any baseline gate run (`scripts/bench_compare_pr8_vs_main_trimmed.sh`)
- Any feature sweep (lanes, split vlog, dict training knobs, index flags)
- Any profile capture (`-trace`, `-blockprofile`, `-mutexprofile`, `-cpuprofile`)
- Any regression discovery, hypothesis, fix, and verification run

## Milestones (Owner View)

Each milestone ends with a **posted PR8 comment** that includes: the exact commands, the summary table, and the `artifacts/bench/...` log/profile paths.

### M0 — Establish a Trusted Baseline (1 session)
**Goal:** “PR8 vs main” is stable enough that deltas are actionable.

**TODOs**
- [ ] Run the baseline gate twice back‑to‑back and confirm the deltas are directionally consistent (noise bounded).
- [ ] Record host facts in the PR8 comment: OS, CPU, disk type, Go version, filesystem.
- [ ] Confirm the bench excludes DB open/close time for the benchmark loop (so we’re measuring steady‑state ops/sec).

**Exit criteria**
- A baseline log is pinned in PR8 as the reference.

### M1 — Eliminate Regressions in Core Workloads (iterative)
**Goal:** On the baseline gate tests, PR8 has **no meaningful regressions** vs `main` for the default configuration.

**TODOs**
- [ ] If any test is negative vs `main` beyond noise, create a minimized reproducer (single test, smaller keycount).
- [ ] Capture trace + syscall + sync profiles for both `main` and PR8 for the minimized case.
- [ ] Implement targeted fixes (allocation, locking, syscalls, redundant work) without changing defaults.
- [ ] Re-run baseline gate and re-post the updated table.

**Exit criteria**
- Gate table is “all wins or within noise” for baseline config.

### M2 — Validate New Features Don’t Hide Regressions (matrix)
**Goal:** Each major feature introduced in PR0–PR7 is benchmarked ON vs OFF and does not introduce unacceptable cliffs.

**TODOs**
- [ ] Run per‑feature sweeps (lanes, split vlog, dict training, index flags) using consistent methodology.
- [ ] Identify “bad combos” (feature interactions) and either fix or document as “not recommended”.

**Exit criteria**
- A PR8 comment enumerates best settings per workload and any caveats.

### M3 — Demonstrate Compression Wins on Compressible Data (matrix + profiles)
**Goal:** Dict compression shows clear wins on compressible data and degrades gracefully on incompressible data.

**TODOs**
- [ ] Run `repeat` and `zero` datasets with dict training OFF vs ON.
- [ ] Profile dict‑enabled runs to ensure training overhead is bounded and adaptive pause behaves.

**Exit criteria**
- A PR8 comment with side‑by‑side results (random vs repeat) and the chosen recommended knobs.

## Future Work (Not Part Of The Merge-Gate)

- Background value-log recompression on rotation (keep foreground writes uncompressed; recompress sealed segments and rewrite pointers safely): tracked in issue #180.

### M4 — Resource / Footprint Validation (ops focused)
**Goal:** We understand disk growth and memory usage for recommended configs.

**TODOs**
- [ ] Add a repeatable measurement procedure for `index.db`, `wal/`, and `vlog/` sizes after each test.
- [ ] Capture RSS (Linux) for the heaviest write workload and at least one scan workload.

**Exit criteria**
- A PR8 comment includes disk + memory numbers for “recommended configs”.

### M5 — Release Candidate Sign‑off
**Goal:** PR8 is ready as the RC comparison baseline and the feature recommendations are documented.

**TODOs**
- [ ] Re-run baseline gate + one compressible matrix run and ensure results still hold.
- [ ] Add a short “Recommended Settings” section (with commands) to PR8.

**Exit criteria**
- One final PR8 comment with “baseline + compression + flags” summary.

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

### Output Standard (what to paste into PR8)
- Commit SHAs: `main` SHA and PR8 SHA.
- One table: test → main trimmed avg, PR8 trimmed avg, delta.
- The exact command line (including env vars).
- The full log path under `artifacts/bench/`.
- If a profile was captured: list the trace/pprof file paths.

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

## Profiling TODO List (What We Want to Measure)

This is the prioritized “profile backlog”. Each item should be closed out with: reproduced delta → profile evidence → fix → re-run.

### P0 — Core write path
- [ ] `random_write` @ `valsize=1024` (default threshold=256) baseline config.
- [ ] `batch_write` @ `valsize=1024` baseline config.
- [ ] Sweep `-treedb-journal-lanes` for both tests and profile the best + worst lane counts.

### P0 — Dict compression path
- [ ] Repeat the two write tests with dict training enabled on `repeat` data.
- [ ] Repeat on `random` data to confirm adaptive pause avoids CPU burn.
- [ ] Profile trainer overhead vs commit/value-log overhead (trace `sched` + `syscall` + `sync`).

### P1 — Read and scan
- [ ] `random_read` baseline and with value-log pointers dominant (`valsize=4096`).
- [ ] `prefix_scan` baseline and with PR7 index flags.

### P1 — Startup/reopen and recovery overhead
- [ ] Add a “reopen cost” microbench run (open/close loop) to detect creeping recovery time.
- [ ] Profile reopen on: empty, medium, and large WAL/vlog directories.

### P2 — Background activity
- [ ] Ensure background flush/compaction/vacuum workers are not waking too frequently under write load (trace sync blocking).
- [ ] Confirm no periodic `time.Now()` or `os.Stat` calls in per‑write hot loops.

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

### 3) Read path hot spots (RID indirection + checksum)
Focus: `random_read` improvements/regressions.

Matrix:
- `-treedb-disable-read-checksum` OFF/ON (unsafe; used only to isolate CRC cost)
- With/without value‑log pointers (via `-treedb-value-log-threshold`)

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

## Benchmark Matrices (Concrete Runs)

These are the “standard” runs to answer the most common questions without inventing new ad‑hoc commands each time.

### Matrix A — Baseline (default config)
- Workloads: `batch_write,random_write,random_read,prefix_scan`
- Data: `-dataset-val-pattern random`
- Keys: `1,000,000` (gate) and `200,000` (profiling)

### Matrix B — Compression ROI (repeat vs random)
- Workloads: `batch_write,random_write`
- Data patterns: `random` and `repeat`
- Dict training: OFF and ON
- Keys: `1,000,000` (numbers) and `200,000` (profiles)

### Matrix C — Lanes sweep
- Workloads: `batch_write,random_write`
- Lanes: `1,2,4,8`
- Note: if unified_bench workload is single-threaded, also add a concurrent-writer benchmark (future enhancement) to properly stress lanes.

### Matrix D — Index flags
- Workloads: `random_read,prefix_scan,batch_write`
- Flags:
  - `-treedb-index-columnar-leaves`
  - `-treedb-index-internal-base-delta`
  - `-treedb-leaf-prefix-compression`
  - `-treedb-prefer-append-alloc`

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

## Activity Log (PR9)

Append-only log of actual runs, results, and next actions. Every entry should include: timestamp, commit/branch, commands, and `artifacts/bench/...` paths.

`2026-01-18 21:39 HST`
- Branch: `sprint/slabopt-pr9-perf-followups` (PR: https://github.com/snissn/gomap/pull/68)
- Host: macOS 15.6.1 (Darwin 24.6.0) arm64, CPU `Apple M3`, Go `go1.25.5`, disk `df -h .` shows ~94% used (note: may increase IO variance)
- Baseline gate run #1 (RUNS=5 KEEP=3 SLEEP_S=5; keys=1,000,000 valsize=1024 batchsize=1000):
  - cmd: `RUNS=5 KEEP=3 SLEEP_S=5 KEYS=1000000 VALSIZE=1024 BATCHSIZE=1000 bash scripts/bench_compare_pr8_vs_main_trimmed.sh`
  - log: `artifacts/bench/compare_pr8_vs_main_trimmed_20260118213913.log`
  - deltas: `batch_write +34.22%`, `random_write -9.64%`, `random_read -40.84%` (suspected outlier), `prefix_scan +7.08%`
- Baseline gate run #2 (same settings):
  - log: `artifacts/bench/compare_pr8_vs_main_trimmed_20260118214516.log`
  - deltas: `batch_write +39.10%`, `random_write -16.83%`, `random_read -1.71%`, `prefix_scan +14.80%`
- Next: treat `random_write` regression as real (2 consecutive runs) and profile `main` vs PR9 for `random_write` with `-trace` + `-pprof=syscall|sync|sched`, then implement targeted fixes without changing defaults.

`2026-01-18 22:04 HST`
- Hypothesis: `random_write` regression is dominated by per-write overhead in `allowValueLogPointers()` calling `valueLogRetainedStats()` (allocates slices + maps and scales with retained-path count).
- Fix: amortize retained-bytes computation and refresh periodically (`valueLogRetainedBytesApprox()`), so the hard-cap check no longer allocates on every write.
- Bench gate run (WARMUP=1 added; unified_bench forced to `-progress=false` to reduce output noise):
  - cmd: `RUNS=5 KEEP=3 SLEEP_S=5 WARMUP=1 KEYS=1000000 VALSIZE=1024 BATCHSIZE=1000 bash scripts/bench_compare_pr8_vs_main_trimmed.sh`
  - log: `artifacts/bench/compare_pr8_vs_main_trimmed_20260118220332.log`
  - deltas: `random_write -8.07%` (improved from ~-16%/~-19% prior); other cells still show high variance (investigate after stabilizing disk/IO conditions).

`2026-01-18 22:20 HST`
- Additional fix: avoid per-write `markValueLogRetain()` calls on the hot path for single-key `Set()` pointer writes.
  - Track per-lane `vlogRetainedPath` and only emit a retain mark when the active segment path changes (once per segment, not once per write).
- Bench gate run (WARMUP=1):
  - log: `artifacts/bench/compare_pr8_vs_main_trimmed_20260118221956.log`
  - deltas: `batch_write +9.09%`, `random_write +3.21%` (regression cleared), `random_read +7.23%`, `prefix_scan +25.87%`

`2026-01-18 22:30 HST`
- Follow-up: `TestCachingDB_ValueLogHardCapDisablesPointers` exposed that periodic refresh caching delayed hard-cap enforcement for small op counts.
- Fix: make hard-cap checks **exact and cheap** (no per-write allocations, no delayed refresh):
  - Track retained closed bytes in `db.valueLogRetainedClosedBytes` (updated on vlog rotate + prune).
  - Compute current retained bytes by adding per-lane `vlogLiveBytes` only when the current segment is actually retained (`vlogPath == vlogRetainedPath`).
- Tests: `go test ./... -count=1` → PASS
