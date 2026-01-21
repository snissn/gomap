# Dictionary Compression Sprint Runbook (ValueLog / mode3+mode4)

This file is an execution runbook for the next optimization sprint focused on **ValueLog dictionary compression** (TreeDB cached mode, mode3 + mode4). It is written to be followed by a coding agent (and reviewed by humans) and is intentionally explicit about benchmarking discipline and the optimization loop.

## Scope and Goals

Primary goals:
- Make **dictionary compression “always-on safe”**:
  - On **incompressible** data it should rapidly self-disable (or probe rarely) such that overhead is **negligible**.
  - On **compressible** data it should produce clear **disk reduction** and ideally **throughput wins** (or small acceptable costs), especially at larger value sizes.
- Ensure **read throughput** (decode path) remains competitive:
  - No large regressions in random reads or scans due to decode overhead / allocations.
- Build a reliable, repeatable benchmarking matrix and workflow that prevents “bench drift” and makes perf work reviewable.

Non-goals (for this sprint unless explicitly approved):
- Changing the on-disk value-log frame format in incompatible ways.
- Adding new background threads or complex scheduling logic without measurement.
- “Winning” by changing unrelated defaults (thresholds, modes) that make comparisons misleading.

## Current Starting Point / References

Starting PR for this sprint:
- **PR19**: ValueLog dict compressibility benchmark expansion
  - Branch: `sprint/slabopt-pr19-vlog-dict-bench`
  - PR: `https://github.com/snissn/gomap/pull/78`
  - Key benches live at: `TreeDB/internal/valuelog/dict_compressibility_bench_test.go`

Related work (write-path baseline):
- Mode1 vs mode4 parity work culminating in PR18 and PR17; mode4 is now competitive.

## Definitions (what we measure)

Per benchmark case, we care about:
- **Throughput**:
  - `MB/s` (raw input bytes per second; go bench uses `b.SetBytes(rawBytes)`)
  - or `ops/sec` for end-to-end DB workloads
- **Compression effectiveness**:
  - `observed_ratio = stored_payload_bytes / raw_payload_bytes` (lower is better)
- **Work avoidance** (critical for incompressible data):
  - `attempted_frac`: fraction of frames where we actually ran zstd encode (even if we discarded the result)
  - `kept_frac`: fraction of frames stored compressed (currently represented by `compressed_frac`)
  - Goal: for incompressible streams, `attempted_frac` should converge to ~0 in steady-state.
- **Allocator pressure**:
  - `B/op` and `allocs/op` from `-benchmem`

Why `attempted_frac` matters:
- A stream can have `kept_frac≈0` but still be slow if we repeatedly attempt compression and then discard it.

## Benchmark Discipline (mandatory)

General rules:
- Always run comparisons with **RUNS=5**, **sleep=5s** between runs, report **median-3**.
- Always pin **seed** and **value pattern**.
- Always record:
  - the exact command line
  - commit hash
  - host info (Mac vs Celestia server)
  - the median-3 summary (and optionally the 5 raw runs)

Where to run what:
- Use the **Celestia Linux server** (`mikers@192.168.0.185`) for end-to-end DB perf whenever possible (less desktop noise).
- Use local Mac for quick iteration and CPU-only microbenches.

## Milestones and PR Sequencing (mapped to work)

We work sequentially. Every PR branch is based on the previous PR branch and merged sequentially.

### PR20 — Bench/Stats instrumentation (“attempted vs kept”)
Branch: `sprint/slabopt-pr20-vlog-dict-metrics`
Title: `PR20: value-log dict compression metrics (attempt vs kept) + bench wiring`
Deliverables:
- Add explicit instrumentation to expose:
  - attempted frames vs kept compressed frames
  - bytes attempted/kept (optional but useful)
- Update `dict_compressibility_bench_test.go` (and any other relevant benches) to report:
  - `attempted_frac` and `kept_frac` (keep existing `observed_ratio`)
- Add/extend runtime stats visibility (at least in TreeDB `Stats()` or structured logs) so end-to-end benchmarks can report:
  - dict id, k, pause state, window ratios
  - attempted/kept counters

Acceptance:
- `go test ./TreeDB/internal/valuelog -count=1`
- `go test ./... -count=1`
- Bench output includes the new metrics.

### PR21 — Read-path benchmarks (micro + IO)
Branch: `sprint/slabopt-pr21-vlog-dict-read-benches`
Title: `PR21: value-log dict read/decode benchmarks`
Deliverables:
- Add “decode-only” microbenches (NoIO) mirroring the encode benches:
  - decode raw vs decode dict-compressed payload
  - report allocs and throughput
- Add an IO-oriented read benchmark:
  - write once, then benchmark sequential read/decode and random read/decode

Acceptance:
- Benches complete in reasonable time and are stable enough for median-of-5 runs.

### PR22 — End-to-end unified-bench suite for dict compression (mode3 + mode4)
Branch: `sprint/slabopt-pr22-unifiedbench-dict-suite`
Title: `PR22: unified-bench suite for value-log dict compression (mode3/mode4)`
Deliverables:
- Add a suite (or extend an existing suite) to test:
  - mode3 and mode4
  - compression enabled vs disabled
  - compressible vs incompressible patterns
  - at least 2 value sizes (e.g. 1KiB and 16KiB)
- Include explicit warmup/training phase and then steady-state measurement so we can distinguish:
  - training cost vs steady-state encode/decode cost
- Print: ops/sec, observed_ratio, attempted_frac, kept_frac, disk bytes (wal/vlog/index).

Note:
- unified-bench already supports `-dataset-val-pattern random|zero|repeat`. We likely need a “medium” pattern option (e.g. sparse noise) for realism; add it here if needed.

### PR23+ — Milestone 4 optimization loop (the “real work”)
Branches: `sprint/slabopt-pr23-...`, `sprint/slabopt-pr24-...`, etc.
Each PR should be narrow: 1–2 targeted optimizations, backed by profiles and before/after benchmark tables.

Targets (in priority order):
1) **Incompressible overhead at 1KiB**
   - Make “dict enabled” converge to `attempted_frac≈0` quickly.
   - Confirm mode3/mode4 ops/sec is ~unchanged vs dict disabled.
2) **Compressible throughput + ratio**
   - For highly/medium compressible patterns, ensure we get strong `observed_ratio` while keeping throughput acceptable.
3) **Read throughput**
   - Ensure decode path remains efficient and low-allocation.

### PR Final — Guardrails + docs
Branch: `sprint/slabopt-prXX-dict-guardrails`
Title: `PRXX: dict compression guardrails + docs`
Deliverables:
- Add a lightweight perf baseline check (warning-only at first) driven by the new benchmark(s):
  - Incompressible + dict enabled must not exceed overhead budget.
  - Compressible must achieve at least some ratio improvement.
- Document recommended configs and how to interpret metrics.

## Milestone 4 Optimization Loop (mandatory process)

This is the core of the sprint. Treat it as a tight “measure → hypothesize → change → verify → repeat” loop.

### Loop step 0 — Pick the exact target case
Pick one case from the matrix and focus until it is “done”.
Examples:
- `valsize=1024`, `incompressible`, `dict_on`, `k=4` (overhead minimization)
- `valsize=16384`, `medium_compressible`, `dict_on`, `k=8` (throughput+ratio win)

### Loop step 1 — Establish baseline (RUNS=5, median-3)
Run:
- Microbench baseline (`go test ... -bench ... -benchmem`)
- End-to-end baseline (unified-bench suite; mode3 and mode4)

Record:
- throughput
- observed_ratio
- attempted_frac + kept_frac
- allocs/op

### Loop step 2 — Capture profiles
For the exact case, collect:
- CPU profile (always)
- alloc/mem profile (often)
- mutex/block profile (if contention shows up)
- syscall summary (Linux only) if IO dominates (e.g. `strace -c` or write-size histogram)

Commands (examples):
- Go bench CPU profile:
  - `go test ./TreeDB/internal/valuelog -run ^$ -bench BenchmarkValueLogDictCompressibilityCPU_NoIO/... -cpuprofile /tmp/vlog_cpu.prof`
- unified-bench CPU profile:
  - `./bin/unified-bench ... -cpuprofile /tmp/ub_mode4_dict.cpu`
- Inspect:
  - `go tool pprof -http=:0 /tmp/vlog_cpu.prof`

### Loop step 3 — Hypothesize (write down the top 3 hotspots)
For each hotspot, answer:
- Is it unavoidable work (zstd cost) or avoidable overhead (copies, allocations, repeated attempts)?
- Does it affect:
  - incompressible streams (overhead bug)
  - compressible streams (tuning opportunity)
  - read path (decode regression)

### Loop step 4 — Implement one change at a time
Make the smallest possible change that should move the needle.
Commit discipline:
- One logical optimization per commit.
- Commit message must say what is optimized and why (and which benchmark case it targets).

### Loop step 5 — Re-run the same baseline matrix
Re-run:
- the target microbench case
- a minimal “sanity sweep” (one compressible and one incompressible)
- end-to-end suite (mode3 + mode4)

If you can’t see improvement:
- revert the change (or keep it behind a flag) and move on.

### Loop step 6 — Report and log
Every optimization PR must include:
- A PR comment with:
  - exact commands
  - the median-3 results table (before/after or baseline vs head)
  - a short interpretation (“what got better, what stayed the same, what got worse”)
- Optional: append a short entry to “Results Log” in this file (append-only).

## Benchmark Matrix (initial)

Value sizes (start):
- 1KiB (sensitive to overhead; this is where regressions hide)
- 16KiB (where compression wins should be obvious)

Compressibility patterns:
- Highly compressible (repeat + small noisy tail)
- Medium compressible (repeat base + sparse noise)
- Low compressible (half random)
- Incompressible (random)

Dict modes:
- dict disabled (control)
- dict enabled (with adaptive pause)

K values:
- fixed `k=1`, `k=4`, `k=8` (plus dynamic K selection in end-to-end)

Workloads:
- Write-heavy (mode4 and mode3)
- Mixed write+read (later: add read phase after write)

## Implementation Hotspots to Watch (expected)

Write path:
- Raw payload concatenation/copying (memmove)
- zstd encode cost
- CRC computation overhead (defer unless it becomes dominant)
- Probe cadence (attempting encode too often on incompressible data)
- Syscall count / write sizes

Read path:
- Decode allocations (per-frame slices, per-record allocations)
- Dictionary codec cache behavior (enc/dec pools, lock contention)
- Reader buffering/mmap behavior (already have mmap read optimizations; verify they still win)

## End-to-end Mode3/Mode4 knobs (reference)

These are the knobs we use for controlled comparisons in unified-bench:

Mode3 (journaled, split vlog):
- `-treedb-split-value-log`
- (optional perf toggles for experiments) `-treedb-relaxed-sync -treedb-disable-read-checksum` with `-treedb-allow-unsafe`

Mode4 (deferred vlog, no redo log):
- `-treedb-disable-journal -treedb-split-value-log`
- same optional perf toggles as above for controlled comparisons

Dict training controls:
- disable training: `-treedb-vlog-dict-train-bytes -1`
- enable training: set train bytes (TBD by tuning), and tune:
  - `-treedb-vlog-dict-adaptive-ratio`
  - metrics window/min records/pause bytes
  - `-treedb-vlog-dict-min-savings-ratio`

## Required PR Hygiene (must comply)

For every PR in this sprint:
- Create branch: `sprint/slabopt-pr<N>-<slug>` (N increments sequentially).
- Commit and push often.
- Open PR via GH CLI (`gh pr create`) (no web UI).
- Include benchmark outputs in PR body and/or PR comments.
- Prefer append-only logs to reduce conflicts.

## Results Log (append-only)

Append entries here as we go (optional but helpful). Keep entries short and factual.

Template:
```
### YYYY-MM-DD (host: <mac|celestia>, commit: <hash>)
- Case: valsize=..., pattern=..., dict=..., k=...
- Baseline: ...
- Head: ...
- Notes: ...
```

