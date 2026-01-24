> **Legacy note:** This runbook predates the WAL on/off simplification and may reference removed options.
> Use the current merge-gate runbook + docs for up-to-date guidance.

# Dictionary Compression Sprint Runbook (ValueLog / wal_on+wal_off)

This file is an execution runbook for the next optimization sprint focused on **ValueLog dictionary compression** (TreeDB cached mode, wal_on + wal_off). It is written to be followed by a coding agent (and reviewed by humans) and is intentionally explicit about benchmarking discipline and the optimization loop.

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

## Status (current branches / PRs)

As of **2026-01-20**, we are actively in the Milestone 4 optimization loop.

Open PRs / recent changes:
- PR23 (#82): baseline incompressible bench stabilization (avoid value-pool accidental compressibility).
- PR24 (#83): value-log writer hot-path for “dict enabled but skip compression” (write directly into `appendBuf`).
- PR25 (#84): make dict-enabled cheap on incompressible (high-entropy gate + pause; unify_bench avoids waiting for dict publish when paused).
- PR26 (#85): temporary K-selection tuning (prefer `k=8` for small/medium values).

Key observation (important for interpreting numbers):
- In `unified_bench -suite vlog_dict`, `observed_ratio` currently includes the **warmup/training write phase** which is often **uncompressed**.
  - This can make a healthy steady-state compression stream look like “ratio ~0.39” even if the **steady-state ratio is ~0.08**.
  - TODO: report **measure-only ratio** (delta bytes written during the measurement phase) so we can evaluate steady-state correctness.

## Current Starting Point / References

Starting PR for this sprint:
- **PR19**: ValueLog dict compressibility benchmark expansion
  - Branch: `sprint/slabopt-pr19-vlog-dict-bench`
  - PR: `https://github.com/snissn/gomap/pull/78`
  - Key benches live at: `TreeDB/internal/valuelog/dict_compressibility_bench_test.go`

Related work (write-path baseline):
- Mode1 vs wal_off parity work culminating in PR18 and PR17; wal_off is now competitive.

## Definitions (what we measure)

Per benchmark case, we care about:
- **Throughput**:
  - `MB/s` (raw input bytes per second; go bench uses `b.SetBytes(rawBytes)`)
  - or `ops/sec` for end-to-end DB workloads
- **Compression effectiveness**:
  - `observed_ratio = stored_payload_bytes / raw_payload_bytes` (lower is better)
  - **NOTE:** for suite-style runs with warmup+measure phases, we must distinguish:
    - `observed_ratio_total` (warmup+measure blended; can look “worse” if warmup is uncompressed)
    - `observed_ratio_measure` (delta bytes in measure window / raw bytes in measure window; the real steady-state signal)
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

### Immediate TODO (before deeper tuning)

We must first verify “compression correctness” on *extremely* compressible payloads:
- Add a **very/ultra compressible** pattern that should compress to near-zero (e.g. all-zero, or fully repeated pattern with no random tail).
- Ensure the suite reports **measure-only ratio** so we’re not misled by warmup bytes.

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

### PR22 — End-to-end unified-bench suite for dict compression (wal_on + wal_off)
Branch: `sprint/slabopt-pr22-unifiedbench-dict-suite`
Title: `PR22: unified-bench suite for value-log dict compression (wal_on/wal_off)`
Deliverables:
- Add a suite (or extend an existing suite) to test:
  - wal_on and wal_off
  - compression enabled vs disabled
  - compressible vs incompressible patterns
  - **ultra-compressible** pattern to validate the “best case” ratio
  - at least 2 value sizes (e.g. 1KiB and 16KiB)
- Include explicit warmup/training phase and then steady-state measurement so we can distinguish:
  - training cost vs steady-state encode/decode cost
- Print: ops/sec, observed_ratio, attempted_frac, kept_frac, disk bytes (wal/vlog/index).
  - MUST print both:
    - `observed_ratio_total` (warmup+measure)
    - `observed_ratio_measure` (delta during measurement window)

Note:
- unified-bench already supports `-dataset-val-pattern random|zero|repeat`. We likely need a “medium” pattern option (e.g. sparse noise) for realism; add it here if needed.

### PR23+ — Milestone 4 optimization loop (the “real work”)
Branches: `sprint/slabopt-pr23-...`, `sprint/slabopt-pr24-...`, etc.
Each PR should be narrow: 1–2 targeted optimizations, backed by profiles and before/after benchmark tables.

Targets (in priority order):
0) **Compression correctness on ultra-compressible data**
   - Add/bench a pattern that should reliably compress to ~0 (or very close) for both 1KiB and 16KiB.
   - If ratio is not “obviously great”, treat it as a correctness/benchmark bug first (before tuning).
1) **Incompressible overhead at 1KiB**
   - Make “dict enabled” converge to `attempted_frac≈0` quickly.
   - Confirm wal_on/wal_off ops/sec is ~unchanged vs dict disabled.
2) **Compressible throughput + ratio**
   - For highly/medium compressible patterns, ensure we get strong `observed_ratio` while keeping throughput acceptable.
   - Avoid hard-coding `k=8`; K should be **chosen dynamically** from data (and may exceed 8). `k=8` is a temporary stopgap.
3) **Read throughput**
   - Ensure decode path remains efficient and low-allocation.

### PR Final — Guardrails + docs
Default PR number: **PR25** (bump the number if we add extra PR23+ optimization PRs).

Branch: `sprint/slabopt-pr25-dict-guardrails` (or `sprint/slabopt-pr<N>-dict-guardrails`)
Title: `PR25: dict compression guardrails + docs`

Goal:
- Turn our benchmark learnings into **enforced guardrails** (CI or opt-in) + **clear docs** so we don’t regress.

Concrete deliverables (files + exact steps):

1) **Perf baseline file (JSON)**
   - File: `.github/perf_baselines/vlog_dict_defaults.json`
   - Action:
     - Update benchmark names to match the current bench suite (including `valsize=...` and workload names).
     - Add separate entries for at least:
       - `valsize=1024` (overhead-sensitive)
       - `valsize=16384` (compression-win-sensitive)
     - Add thresholds for the new metrics once PR20 lands:
       - `attempted_frac` (must be low for incompressible dict_on steady-state)
       - `kept_frac` (optional; mostly informational)
     - Keep thresholds conservative enough to be stable on GH runners.

2) **Perf baseline checker script**
   - File: `.github/scripts/check_vlog_dict_bench.go`
   - Action:
     - Extend parsing to ingest the new benchmark metrics:
       - `attempted_frac` and `kept_frac` (or `compressed_frac` if that is the kept metric name)
     - Extend the baseline schema to include min/max bounds for those metrics.
     - Keep the “median-of-5” / trimmed-mean behavior (already implemented).

3) **CI wiring**
   - File: `.github/workflows/treedb-tests.yml` (job: `perf-smoke (linux)`)
   - Action:
     - Update the `go test ... -bench ... -count=5` regex to match the updated benchmark names.
     - Run at least the “k=4” cases (fast enough for CI).
     - Once tuning is stable, flip the checker to **gating** on linux:
       - change `-strict=false` → `-strict=true`
     - If CI noise is still an issue, keep `strict=false` but require an explicit opt-in label/flag to gate (document it).

4) **Docs**
   - Add a single, canonical doc page:
     - File: `docs/valuelog_dict_compression.md` (new)
   - Must include:
     - “What dict compression does” (value-log grouped frames + dictID + K)
     - “Always-on safety contract” (what we guarantee on incompressible data)
     - Recommended baseline configs:
       - how to disable training: `-treedb-vlog-dict-train-bytes -1`
       - suggested defaults for training bytes, adaptive ratio, window bytes, pause bytes (once established)
     - How to interpret metrics:
       - `observed_ratio`, `attempted_frac`, `kept_frac`
     - How to reproduce CI perf check locally:
       - `go test ./TreeDB/internal/valuelog -run '^$' -bench 'BenchmarkValueLogDictCompressibilitySweep/valsize=(1024|16384)/(highly_compressible_tail64|medium_compressible|incompressible)/dict_(off|on)/k=4$' -benchmem -count=5 | tee vlog_dict_bench.txt`
       - `go run .github/scripts/check_vlog_dict_bench.go -bench-output vlog_dict_bench.txt -baseline .github/perf_baselines/vlog_dict_defaults.json -strict=true`

Acceptance for PR25:
- CI perf check runs the intended benchmarks and either:
  - gates (strict=true), or
  - is warning-only with an explicit documented plan/date to flip to strict.
- Docs explain how to reproduce and tune dict compression safely.

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
- End-to-end baseline (unified-bench suite; wal_on and wal_off)

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
  - `./bin/unified-bench ... -cpuprofile /tmp/ub_wal_off_dict.cpu`
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
- end-to-end suite (wal_on + wal_off)

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
- Ultra compressible (repeat w/ *no* tail, or all-zero)
- Low compressible (half random)
- Incompressible (random)

Dict modes:
- dict disabled (control)
- dict enabled (with adaptive pause)

K values:
- fixed `k=1`, `k=4`, `k=8` (plus dynamic K selection in end-to-end)
- TODO: extend dynamic K exploration to include `k=16` and `k=MaxFrameK` where it makes sense (especially for write-heavy wal_off),
  while keeping read-path costs in mind.

Workloads:
- Write-heavy (wal_off and wal_on)
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

## End-to-end WAL on/WAL off knobs (reference)

These are the knobs we use for controlled comparisons in unified-bench:

WAL on (journaled):
- default behavior (no special flags)
- (optional perf toggles for experiments) `-treedb-relaxed-sync -treedb-disable-read-checksum` with `-treedb-allow-unsafe`

WAL off (no journal / redo log):
- `-treedb-disable-wal` (requires `-treedb-allow-unsafe`)
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
