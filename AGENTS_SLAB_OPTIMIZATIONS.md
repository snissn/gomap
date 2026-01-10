# Agent Plan: Slab Write Optimizations (Pointer Values / Slab Heavy)

This file is the issue tracker + work log for optimizing TreeDB’s **pointer-values / slab-heavy** configuration (i.e. values stored out-of-line in slabs).

Primary observed bottleneck: **syscall-heavy slab I/O** (writes + reads) under Celestia workloads. Micro-optimizations in Go often won’t move wall time unless they reduce syscalls/bytes.

## Definitions

**Pointer values / slab-heavy config**
- `ForceValuePointers=1` (store values out-of-line in slabs; index stores pointers)
- `OmitSlabKeys=1` (do not store keys in slab records)
- `EnableValueIndex=0` (avoid extra pointer->location index)
- `DisableValueLog=1` (no value log)
- `SlabCompression=zstd` or `none` (tuned)

**Metrics to record for every experiment**
- **Server (true workload):** `duration_seconds`, `index.db bytes`, `data-*.slab bytes`, `shutdown_seconds`, and pprof/trace directory.
- **Local (replay bench):** `ns/op` and a CPU profile top table.
- Always include: branch name, commit SHA(s), and whether accepted/reverted.

## Progress snapshot (update when RC changes)

- RC branch: `slab-opt-rc` (head `5a4aa6c`)
- Accepted: #9 hugepage hint (`26a4a4b`, Linux-only; needs Linux validation)
- Rejected: #1 multistream (`73d27d5` + `747bc86`), #6 tiering (`41f25a2` + `4cf2036`)
- Pending: #2–#5, #7–#8

## Compatibility stance (Alpha)

This is **alpha software**. For this optimization sprint:
- **Backward compatibility is NOT required.**
- It is acceptable to bump slab/index formats and invalidate old on-disk data.
- “Needs a migration plan” is **not** a valid blocker reason.
- The only acceptable blockers are concrete engineering constraints (time/complexity/safety) with a clear next step to unblock.

## Branching & RC Workflow (MANDATORY)

Goal: preserve a linear history of attempts while ensuring the active code path only contains accepted optimizations.

**Base (“RC”) branch**
- Start from the current RC branch (default: `treedb-tracing-capture` unless you intentionally choose another).
- Maintain a moving RC head branch (recommended name: `slab-opt-rc`) that represents “accepted + reverted attempts”.

**One branch per high-level feature**
- For each feature item `NN`, create branch `slab-opt-NN-<shortname>` from the current RC head.
- Implement the feature with atomic commits + tests/benchmarks.

**Accept vs Reject**
- **Accept**: If the feature is faster or neutral and passes tests, fast-forward merge it into `slab-opt-rc`.
- **Reject**: If the feature regresses, keep the implementation commits, then immediately `git revert` them (one or more revert commits) so the branch ends in a “baseline-equivalent” state. Fast-forward merge that branch into `slab-opt-rc` so history records the attempt but code is not active.

**Next branch rule**
- Always create the next feature branch off the updated `slab-opt-rc` head.
- If a feature was rejected, the next branch must be based on the RC head that includes the “attempt + revert” commits (so the work is preserved but inactive).

**Process hygiene**
- Update this file (checklist + Work Log) in the same PR/branch as the code changes.
- Keep deeper notes in `invalid_value_debug.md` as needed.

## Baseline commands (copy/paste)

### Local: trusted pointer-values replay (timeline)
Use the repo’s captured trace files (adjust paths if needed).

```bash
TREEDB_TRACE_SUMMARY="$(pwd)/tmp_traces/treedb_trace_20260109120107.summary.json" \
TREEDB_TRACE_JSONL="$(pwd)/tmp_traces/treedb_trace_20260109120107.jsonl" \
TREEDB_TRACE_MODE=backend \
TREEDB_TRACE_SEQUENTIAL_KEYS=1 \
TREEDB_TRACE_TIMELINE_NO_SLEEP=1 \
TREEDB_TRACE_TIMELINE_INLINE_ITERS=1 \
TREEDB_TRACE_SKIP_ITERS=1 \
TREEDB_TRACE_TIMELINE_DURATION_MS=5000 \
TREEDB_TRACE_FORCE_VALUE_POINTERS=1 \
TREEDB_TRACE_SLAB_OMIT_KEYS=1 \
TREEDB_TRACE_SLAB_COMPRESSION=zstd \
TREEDB_TRACE_SLAB_COMPRESSION_MIN_BYTES=1024 \
TREEDB_TRACE_SLAB_COMPRESSION_MIN_SAVINGS=0 \
go test -run '^$' -bench '^BenchmarkTraceReplayTimeline$' -benchtime=20s -count=3 ./TreeDB
```

CPU profile:
```bash
# Writes /tmp/treedb_ptrvalues_cpu.prof
TREEDB_TRACE_SUMMARY=... TREEDB_TRACE_JSONL=... \
TREEDB_TRACE_FORCE_VALUE_POINTERS=1 TREEDB_TRACE_SLAB_OMIT_KEYS=1 \
TREEDB_TRACE_SLAB_COMPRESSION=zstd TREEDB_TRACE_SLAB_COMPRESSION_MIN_BYTES=1024 \
go test -run '^$' -bench '^BenchmarkTraceReplayTimeline$' -benchtime=15s \
  -cpuprofile /tmp/treedb_ptrvalues_cpu.prof ./TreeDB
go tool pprof -top /tmp/treedb_ptrvalues_cpu.prof | head -n 40
```

### Server: true workload (Celestia)
- Prefer using `run_celestia_trace.sh` to capture `/home/mikers/pprof_*/cpu.pprof` etc.
- Record results from `sync/sync-time.log` and `sync/disk-breakdown.log`.
- Linux server access details (SSH host, paths, run scripts) are documented in `celestia_testing_info.md` and may be used for profiling (some optimizations are Linux-only).

## No Deferrals Policy (MANDATORY)

Deferring almost everything is not useful. For this sprint, **do not use “deferred”** as an outcome.

If an item is not implementable within reasonable scope, it must be handled as **“rejected for now”** with:
- a branch that contains the attempt commits AND a revert commit that restores baseline behavior, merged into `slab-opt-rc`, and
- a brief note for a future agent about what was tried and what to try next.

## Iterative convergence (how to avoid churn)

Target is convergence, not a binary accept/reject mindset.

- Iterate in small steps: baseline → 1 small change → measure → keep/adjust → repeat.
- Prefer landing enabling work if it improves iteration speed or measurement quality (even if perf-neutral):
  - instrumentation, counters, trace/pprof hooks, benchmarks, small refactors, format scaffolding behind a flag.
- It is normal for a high-level item to take multiple iterations:
  - land enabling sub-steps into `slab-opt-rc`, keep the item status as planned/in_progress, and continue on the next loop.
- Treat “rejected for now” as a last resort:
  - first try to salvage by reducing scope, gating behind opt-in, tuning thresholds, or adjusting the benchmark to better match the real workload.
- Keep the repository in a good state after each iteration (tests passing, benchmarks reproducible).

## Acceptance bar (avoid “noise wins”)

Mark an item **accepted** only if:
- Local: repeatable improvement across `-count=3` (or better), and
- Server (preferred): true-workload improvement, or at least a defensible reduction in syscalls/bytes with no wall-time regression.

If the change is Linux-only, acceptance should be based on Linux server results (see `celestia_testing_info.md`).

## Optimization punch list (ordered; update statuses as you go)

### 1) Multi-Stream Slabs (Parallel Writing) — High impact / medium risk
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [x] rejected
- Branch: `slab-opt-01-multistream`
- Checklist:
  - **Minimum viable attempt (MVA):** implement a *single-process* multi-stream writer that shards `AppendMany` into N independent buffers and writes them sequentially to the same file (no format change) to measure syscall reduction potential; keep it behind an opt-in option/env and benchmark.
  - [ ] Design: choose N streams, stream->file layout, and pointer encoding.
  - [x] Implement: parallel append streams + “soft barrier” rotation.
  - [ ] Safety: correctness under crash/restart; no torn writes.
  - [x] Bench: local pointer-values replay; capture cpu profile.
  - [ ] Server: run `run_celestia_trace.sh` and compare wall time + sizes.
  - [x] Decide: accept (merge) or reject (revert) and document.

### 2) Local Dictionary Compression (Zonal Dictionaries / Slab V2) — High impact / medium risk (breaking)
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [x] rejected
- Branch: `slab-opt-02-zonal-dicts`
- Checklist:
  - **MVA:** implement “global dictionary only” (single dict stored in slab header) before full zonal dicts; measure bytes/speed.
  - [ ] Spec compliance: implement Slab V2 header/zone layout as described.
  - [ ] Read path: O(1) dict selection; zero-copy dict slices from mmap.
  - [ ] Write path: zone headers + dict write policy.
  - [ ] Format bump: implement v2 alongside v1 or replace v1 entirely (breaking is OK). Migration is optional.
  - [ ] Tests: roundtrip, corruption detection, cross-zone reads.
  - [ ] Bench: pointer-values replay + server trace; track syscall reduction and bytes written.

### 3) Pre-emptive Training & Entropy Monitoring — Medium impact / medium risk
- Status: [ ] planned  [x] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-03-entropy-training`
- Checklist:
  - **MVA:** implement entropy/ratio metrics and logging only (no behavior change) to locate drift points; then add one safe trigger.
  - [x] Implement rolling compression-ratio metrics per zone.
  - [x] Add opt-in adaptive compression pause on degraded ratio (safe trigger).
  - [x] Trigger background training when ratio degrades; publish next-zone dictionary.
  - [ ] Verify no writer stalls; no extra syscalls on the read fast path.
  - [ ] Bench + server trace comparison.

### 4) Dictionary Deduplication — Medium impact / medium risk
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-04-dict-dedup`
- Checklist:
  - **MVA:** implement exact-hash dedup only (no similarity); measure wins on real slabs.
  - [ ] Hash dictionaries (xxhash64) and reuse global or recent locals when similar.
  - [ ] Implement USE_REF / USE_GLOBAL flags.
  - [ ] Bench: measure slab bytes reduction and any latency impact.

### 5) Two-Pass Compaction (Gold Standard) — Lower impact on write hot path
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-05-two-pass-compaction`
- Checklist:
  - **MVA:** add a compaction benchmark + instrumentation first; do not start with full algorithmic overhaul.
  - [ ] Implement representative global dict selection for rewritten slabs.
  - [ ] Detect shift points; schedule local dict overrides.
  - [ ] Benchmark compaction throughput and resulting slab sizes.

### 6) Multi-Level Slab Tiering — Lower impact on write hot path
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [x] rejected
- Branch: `slab-opt-06-slab-tiering`
- Checklist:
  - **MVA:** implement “cold slab copy + reopen read-only” tooling first; keep write path unchanged.
  - [ ] Define “active fast-append” vs “cold compressed” slab formats.
  - [x] Implement transition policy; verify reads across tiers.
  - [x] Benchmark space savings vs read latency.

### 7) Value Delta Encoding — Huge potential, high risk (data model)
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-07-value-delta`
- Checklist:
  - **MVA:** prototype delta encoding only for a single known value type in a synthetic benchmark (opt-in) to measure feasibility.
  - [ ] Decide delta format + how to reconstruct values for reads.
  - [ ] Integrate safely with compaction/vacuum.
  - [ ] Add adversarial tests (replay, corruption, partial updates).

### 8) Zonal Bloom Filters — Mostly recovery/audit
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-08-zone-bloom`
- Checklist:
  - **MVA:** add bloom-writing + bloom-reading with a standalone verifier tool first; keep it optional and benchmark write overhead.
  - [ ] Add bloom filters to zone headers (keys only).
  - [ ] Provide tooling to rebuild index faster using blooms.
  - [ ] Ensure omit-keys mode is still supported (bloom is “identity proof”).

### 9) Huge-Page Awareness (OS optimization) — Read-side CPU
- Status: [ ] planned  [ ] in_progress  [x] accepted  [ ] rejected
- Branch: `slab-opt-09-hugepage`
- Checklist:
  - [ ] Align zones to 2MB boundaries.
  - [x] Apply `madvise(MADV_HUGEPAGE)` on slab mmaps (linux-only hint).
  - [ ] Benchmark random-read CPU and TLB effects (needs careful measurement).

## Work Log (append-only; keep it current)

### 2026-01-10
- Baseline pointer-values replay + CPU profile captured: `/tmp/treedb_ptrvalues_cpu.prof`.
- #1 multistream MVA (rejected): `slab: add opt-in multi-stream AppendMany` (`73d27d5`) then reverted (`747bc86`); results recorded in `log: record multistream attempt results` (`4de3fda`).
- #6 tiering MVA (rejected): `slab: mark inactive slabs read-only` (`41f25a2`) then reverted (`4cf2036`); results recorded in `log: record slab tiering attempt` (`75c4202`).
- #9 hugepage hint (accepted): `Hint huge pages for slab mmaps` (`26a4a4b`); Linux-only, needs validation on Linux server in `celestia_testing_info.md`.
- Policy update: do not use “deferred”. Any older “defer …” placeholder commits in history do not count as completed work; remaining items must be revisited with an MVA + benchmark and end as accepted or rejected-for-now (attempt+revert).
- #2 zonal dict MVA (rejected): `slab: add v2 header + dict compression mva` (`d89e813`) then reverted (`8083f3f`). Baseline ns/op: 1,134,951,571 / 1,156,101,860 / 1,151,579,158. Attempt failed: `BenchmarkTraceReplayTimeline` aborted with `record too large` in `phase restore` (no CPU profile). Revert confirm ns/op: 1,148,604,129 / 1,170,820,099 / 1,167,754,520.
- #3 entropy metrics MVA (in progress): renamed old defer branch to `slab-opt-03-entropy-training-defer`, created fresh `slab-opt-03-entropy-training` off `slab-opt-rc`.
- Baseline replay (ptr-values): 1,144,469,998 / 1,160,093,383 / 1,164,282,942 ns/op. CPU profile: `/tmp/treedb_ptrvalues_cpu.prof` (pprof top captured).
- #3 replay after metrics logging: 1,145,068,735 / 1,157,760,511 / 1,159,801,847 ns/op. CPU profile: `/tmp/treedb_ptrvalues_cpu_entropy.prof`.
- #3 adaptive pause trigger (opt-in): baseline before change 1,152,491,465 / 1,180,121,349 / 1,171,889,825 ns/op; final bench with `TREEDB_SLAB_COMPRESSION_ADAPTIVE_RATIO=0.98`, `...PAUSE_BYTES=4194304`, `...MIN_RECORDS=1000`: 1,154,793,614 / 1,166,791,415 / 1,162,242,938 ns/op. CPU profile: `/tmp/treedb_ptrvalues_cpu_entropy_adaptive.prof`. Logging is now gated by `TREEDB_SLAB_COMPRESSION_METRICS`.
- Merged `slab-opt-03-entropy-training` into `slab-opt-rc` (ff at `6f8d93a`); RC log updated (head recorded as `2813c5d`).
- #3 training v2 branch created: `slab-opt-03-entropy-training-2` off `slab-opt-rc`.
- Baseline replay (ptr-values): 1,157,461,398 / 1,192,799,862 / 1,167,970,789 ns/op.
- #3 background training (opt-in) replay with `TREEDB_TRACE_SLAB_COMPRESSION_ADAPTIVE_RATIO=0.98` + training envs: 1,123,723,610 / 1,120,500,252 / 1,111,680,888 ns/op; training log ratio ~1.289 (dict worse than raw) on samples.
- CPU profile (training enabled): `/tmp/treedb_ptrvalues_cpu_entropy_train.prof` (pprof top captured; BuildDict/EncodeAll shows up in top).
- Merged `slab-opt-03-entropy-training-2` into `slab-opt-rc` (ff at `4042586`).
- #3 non-blocking sample queue (in progress): created `slab-opt-03-entropy-training-3` to move training sample collection off the writer path (background queue + Close hook).
  - Baseline replay (ptr-values): 1,154,576,021 / 1,191,200,208 / 1,184,142,853 ns/op.
  - Replay after change: 1,211,810,491 / 1,236,813,726 / 1,225,207,840 ns/op (training disabled; likely noise).
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_queue.prof` (pprof top captured).
  - Merged `slab-opt-03-entropy-training-3` into `slab-opt-rc` (ff at `21c0310`).
- #3 training stats (in progress): created `slab-opt-03-entropy-training-4` off `slab-opt-rc` to expose compression trainer queue/ratio counters via `Stats()`.
  - Baseline replay (ptr-values): 1,195,054,523 / 1,227,023,678 / 1,230,509,250 ns/op.
  - Replay after change: 1,218,892,274 / 1,265,395,086 / 1,263,555,979 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_stats.prof` (pprof top captured).
- Merged `slab-opt-03-entropy-training-4` into `slab-opt-rc` (ff at `4ff5917`).
- #3 queue max stats (in progress): created `slab-opt-03-entropy-training-5` off `slab-opt-rc` to track max trainer queue depth via `Stats()`.
  - Baseline replay (ptr-values): 1,200,810,020 / 1,201,450,787 / 1,289,387,910 ns/op.
  - Replay after change (first run, noisy): 2,824,964,528 / 1,819,851,757 / 1,215,812,887 ns/op (re-ran due to outlier).
  - Replay after change (rerun): 1,188,869,042 / 1,203,748,044 / 1,206,534,526 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_queue_max.prof` (pprof top captured).
- Merged `slab-opt-03-entropy-training-5` into `slab-opt-rc` (ff at `6e96967`).
- #3 sample pool attempt (rejected): created `slab-opt-03-entropy-training-6` to pool compression training samples.
  - Baseline replay (ptr-values): 1,175,282,044 / 1,202,878,798 / 1,203,404,127 ns/op.
  - Replay after change: 1,230,044,706 / 1,210,352,441 / 1,231,597,482 ns/op (regressed).
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_pool.prof` (pprof top captured).
  - Revert commit `c1f7129` (attempt `5700ba0`), baseline confirm: 1,198,962,321 / 1,205,787,116 / 1,212,346,842 ns/op.
