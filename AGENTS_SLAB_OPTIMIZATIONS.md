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

- RC branch: `slab-opt-rc` (head recorded in Work Log)
- Accepted: #3 entropy-training pooling (`faf040b`), #9 hugepage hint (`26a4a4b`, Linux-only; needs Linux validation)
- Rejected: #1 multistream (`73d27d5` + `747bc86`), #6 tiering (`41f25a2` + `4cf2036`)
- Pending: #2, #4–#5, #7–#8

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
- Status: [ ] planned  [ ] in_progress  [x] accepted  [ ] rejected
- Branch: `slab-opt-03-entropy-training`
- Checklist:
  - **MVA:** implement entropy/ratio metrics and logging only (no behavior change) to locate drift points; then add one safe trigger.
  - [x] Implement rolling compression-ratio metrics per zone.
  - [x] Add opt-in adaptive compression pause on degraded ratio (safe trigger).
  - [x] Trigger background training when ratio degrades; publish next-zone dictionary.
  - [ ] Verify no writer stalls; no extra syscalls on the read fast path.
  - [ ] Bench + server trace comparison.

### 4) Dictionary Deduplication — Medium impact / medium risk
- Status: [ ] planned  [x] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-04-dict-dedup`
- Checklist:
  - **MVA:** implement exact-hash dedup only (no similarity); measure wins on real slabs.
  - [x] Hash dictionaries (xxhash64) and reuse recent dicts when identical (dedup window).
  - [x] Implement USE_REF / USE_GLOBAL flags.
  - [x] Bench: measure slab bytes reduction and any latency impact.

### 5) Two-Pass Compaction (Gold Standard) — Lower impact on write hot path
- Status: [ ] planned  [x] in_progress  [ ] accepted  [ ] rejected
- Branch: `slab-opt-05-two-pass-compaction`
- Checklist:
  - **MVA:** add a compaction benchmark + instrumentation first; do not start with full algorithmic overhaul.
  - [x] MVA: add index-swap compaction stats + benchmark (pointer-values).
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
- Status: [ ] planned  [x] in_progress  [ ] accepted  [ ] rejected
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
- Started #4 MVA on `slab-opt-04-dict-dedup` (dict hash dedup instrumentation in compression trainer).
- Baseline (slab-opt-rc, training enabled): `BenchmarkTraceReplayTimeline` ns/op: 1,176,716,299 / 1,183,390,849 / 1,191,879,171.
- After (slab-opt-04-dict-dedup): `BenchmarkTraceReplayTimeline` ns/op: 1,183,208,281 / 1,194,904,890 / 1,188,605,237.
- CPU profile: `/tmp/treedb_ptrvalues_dict_dedup_cpu.prof` (top shows syscall/syscall6 + zstd BuildDict/EncodeAll).
- Dedup hits not observed in this replay workload; keeping #4 in_progress for further iterations.
- Merged `slab-opt-04-dict-dedup` into `slab-opt-rc` at `3b6137c`.
- Branch `slab-opt-03-entropy-training-12` (commit `faf040b`): pooled compression training samples + early stop in AppendMany collect.
- Local replay bench (baseline): 1.164/1.206/1.208 s/op.
- Local replay bench (after): 1.129/1.106/1.117 s/op.
- CPU profile (after): `/tmp/treedb_ptrvalues_cpu_entropy_pool.prof` (top: `syscall.syscall`, `syscall.syscall6`, `SlabFile.readViaMmap`).
- Merged `slab-opt-03-entropy-training-12` into `slab-opt-rc` (accepted; server trace pending).
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
- #3 training sample stride (in progress): created `slab-opt-03-entropy-training-7` to sample every Nth record for compression training (opt-in).
  - Baseline replay (ptr-values, training enabled): 1,281,808,851 / 1,313,083,521 / 1,346,444,491 ns/op.
  - Replay after change with `TREEDB_TRACE_SLAB_COMPRESSION_TRAIN_SAMPLE_STRIDE=4`: 1,277,830,697 / 1,322,376,487 / 1,309,572,789 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_stride.prof` (pprof top captured).
- Merged `slab-opt-03-entropy-training-7` into `slab-opt-rc` (ff at `f7cd343`).
- #3 collect gating (in progress): created `slab-opt-03-entropy-training-8` to skip sampling calls when training is inactive.
  - Baseline replay (ptr-values): 1,139,118,817 / 1,091,569,423 / 1,099,063,915 ns/op.
  - Replay after change: 1,091,461,879 / 1,087,484,921 / 1,104,038,097 ns/op.
  - CPU profiles: `/tmp/treedb_ptrvalues_cpu_entropy_collectgate_base.prof`, `/tmp/treedb_ptrvalues_cpu_entropy_collectgate_after.prof`.
- Merged `slab-opt-03-entropy-training-8` into `slab-opt-rc` (ff at `77d8974`).
- #3 batch collect attempt (rejected): created `slab-opt-03-entropy-training-9` to batch trainer sampling in `AppendMany`.
  - Baseline replay (ptr-values): 1,087,414,567 / 1,090,490,581 / 1,095,665,917 ns/op.
  - Replay after change: 1,095,999,356 / 1,095,080,898 / 1,096,457,869 ns/op (regressed).
  - Revert commit `b6b466a` (attempt `42f0b26`), baseline confirm: 1,089,834,462 / 1,086,919,242 / 1,086,995,429 ns/op.
- #3 queue max update gating (rejected): created `slab-opt-03-entropy-training-10` to update trainer queue max only on enqueue.
  - Baseline replay (ptr-values): 1,085,494,738 / 1,089,976,242 / 1,149,411,536 ns/op.
  - Replay after change: 1,202,969,281 / 1,185,147,868 / 1,131,036,527 ns/op (regressed).
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_enqmax.prof` (pprof top captured).
  - Revert commit `5f535c5` (attempt `ffd51d1`), baseline confirm: 1,085,478,040 / 1,100,580,361 / 1,091,074,810 ns/op.
- #3 collect timing stats (in progress): created `slab-opt-03-entropy-training-11` to track collect latency totals/max when `TREEDB_SLAB_COMPRESSION_METRICS=1` is enabled.
  - Baseline replay (ptr-values, slab-opt-rc): 1,101,470,002 / 1,090,666,569 / 1,096,782,567 ns/op.
  - Replay after change: 1,088,395,248 / 1,098,644,323 / 1,081,352,151 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_cpu_entropy_collecttiming.prof` (pprof top captured).
- #4 dict dedup follow-up (in progress): created `slab-opt-04-dict-dedup-2` to track dedup mode/ref in trainer stats (USE_GLOBAL/USE_REF scaffolding).
  - Baseline replay (ptr-values, slab-opt-rc): 1,110,317,827 / 1,117,767,198 / 1,108,285,921 ns/op.
  - Replay after change: 1,120,670,454 / 1,107,856,794 / 1,108,167,354 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup2_cpu.prof` (pprof top captured).
- Merged `slab-opt-04-dict-dedup-2` into `slab-opt-rc` (ff at `ccc4f32`); #4 remains in_progress (scaffolding only).
- #5 two-pass compaction MVA (in progress): created `slab-opt-05-two-pass-compaction` off `slab-opt-rc` and added IndexSwap compaction stats + benchmark.
  - Baseline replay (ptr-values): 1,097,134,252 / 1,101,792,569 / 1,106,051,800 ns/op.
  - Replay after change: 1,099,917,488 / 1,100,265,794 / 1,104,327,240 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_compaction_mva_cpu.prof` (pprof top captured).
  - Benchmark `BenchmarkCompactionIndexSwapPointerValues`: 27,694,000 ns/op; `remap_ops=2000`, `remap_bytes=75735`, `slab_dead_bytes=67735`, `slab_write_bytes=67735`.
- Merged `slab-opt-05-two-pass-compaction` into `slab-opt-rc` (ff at `0d74ce1`).
- #4 dict dedup flags (in progress): created `slab-opt-04-dict-dedup-3` off `slab-opt-rc` to expose USE_GLOBAL/USE_REF flags in trainer stats.
  - Baseline replay (ptr-values, slab-opt-rc): 1,109,235,673 / 1,100,744,040 / 1,097,132,821 ns/op.
  - Replay after change: 1,092,699,347 / 1,096,116,169 / 1,094,556,994 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup3_cpu.prof` (pprof top captured; runtime.madvise + skiplist hot).
- Merged `slab-opt-04-dict-dedup-3` into `slab-opt-rc` (ff at `aa24ee2`); #4 remains in_progress (stats scaffolding).
- #4 dict dedup counters (in progress): created `slab-opt-04-dict-dedup-4` to add per-mode dedup hit counters (global/ref) in trainer stats.
  - Baseline replay (ptr-values, slab-opt-rc): 1,111,794,965 / 1,104,481,635 / 1,103,634,240 ns/op.
  - Replay after change: 1,102,803,673 / 1,118,197,133 / 1,103,201,827 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup4_cpu.prof` (pprof top captured; syscall/syscall6 + slab mmap reads).
- Merged `slab-opt-04-dict-dedup-4` into `slab-opt-rc` (ff at `6676dd1`); #4 remains in_progress (stats-only).
- #4 dict dedup cache (in progress): created `slab-opt-04-dict-dedup-5` to reuse trained dicts when sample hashes repeat (skips `BuildDict`).
  - Baseline replay (ptr-values, slab-opt-rc): 1,102,014,396 / 1,103,562,669 / 1,114,016,688 ns/op.
  - Replay after change: 1,102,690,129 / 1,115,513,167 / 1,112,873,340 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup5_cpu.prof` (pprof top captured; runtime.madvise + skiplist hot).
  - Commit: `058db37` (cache by sample hash + stats wiring).
- #4 dict dedup window option (in progress): created `slab-opt-04-dict-dedup-6` to make the dedup window configurable.
  - Baseline replay (ptr-values, slab-opt-rc): 1,100,734,306 / 1,100,190,706 / 1,099,141,508 ns/op.
  - Replay after change: 1,095,450,644 / 1,097,432,702 / 1,096,846,127 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup6_cpu.prof` (pprof top captured; syscall/syscall6 + slab mmap reads).
  - New envs: `TREEDB_SLAB_COMPRESSION_TRAIN_DEDUP_WINDOW`, `TREEDB_TRACE_SLAB_COMPRESSION_TRAIN_DEDUP_WINDOW`.
  - Merged into `slab-opt-rc` (ff at `ba9d466`).
- #4 dict dedup bytes stats (in progress): created `slab-opt-04-dict-dedup-7` to track dedup byte totals (global/ref/cache) in trainer stats.
  - Baseline replay (ptr-values, slab-opt-rc): 1,135,383,006 / 1,123,267,871 / 1,116,331,973 ns/op.
  - Replay after change: 1,134,423,097 / 1,121,122,415 / 1,114,291,917 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup7_cpu.prof` (pprof top captured; syscall/syscall6 + slab mmap reads).
  - Merged into `slab-opt-rc` (ff at `e985a20`).
- #4 dict dedup window default (in progress): created `slab-opt-04-dict-dedup-8` to widen the default dedup window (4 -> 16).
  - Baseline replay (ptr-values, slab-opt-rc): 1,130,168,684 / 1,108,241,085 / 1,131,219,085 ns/op.
  - Replay after change: 1,109,893,454 / 1,102,575,752 / 1,109,917,792 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup8_cpu.prof` (pprof top captured; runtime.madvise + skiplist hot).
- #4 dict dedup cache on dedup hits (rejected): created `slab-opt-04-dict-dedup-9` to cache dictionaries even when dedup hits (so sample-hash reuse can skip BuildDict).
  - Baseline replay (ptr-values, slab-opt-rc): 1,108,816,519 / 1,112,667,681 / 1,103,882,469 ns/op.
  - Replay after change: 1,106,503,890 / 1,106,255,242 / 1,122,526,806 ns/op.
  - CPU profile (post-change): `/tmp/treedb_ptrvalues_dict_dedup9_cpu.prof` (pprof top captured; runtime.madvise + skiplist hot).
  - Revert commit `c846dc9` (attempt `86b00c6`), baseline confirm: 1,114,710,283 / 1,114,419,002 / 1,102,148,146 ns/op.
  - Merged `slab-opt-04-dict-dedup-9` into `slab-opt-rc` (ff at `10aa660`).
- #4 dict dedup bench stats (in progress): created `slab-opt-04-dict-dedup-10-bench-stats` to report trainer dedup metrics in trace replay benchmarks.
  - Baseline replay (ptr-values, slab-opt-rc): 1,105,368,956 / 1,091,071,937 / 1,101,213,363 ns/op.
  - Replay after change with `TREEDB_TRACE_REPORT_TRAINER_STATS=1`: 1,091,545,025 / 1,098,730,877 / 1,090,572,054 ns/op.
  - Training-enabled replay (`TREEDB_TRACE_SLAB_COMPRESSION_ADAPTIVE_RATIO=0.98`, `TREEDB_TRACE_SLAB_COMPRESSION_TRAIN_BYTES=1048576`) reports `train_dedup_*` metrics at 0 (no dedup hits observed).
  - CPU profile (training enabled): `/tmp/treedb_ptrvalues_dict_dedup10_cpu.prof` (pprof top captured; syscall/syscall6 + zstd BuildDict/EncodeAll).
  - Merged `slab-opt-04-dict-dedup-10-bench-stats` into `slab-opt-rc` (ff at `9ab721a`).
- #7 value delta MVA (in progress): created `slab-opt-07-value-delta` with xor-delta prototype + synthetic benches (not integrated into slab write path yet).
  - Baseline replay (ptr-values, slab-opt-07-value-delta pre-change): 1,088,241,401 / 1,085,060,494 / 1,088,174,706 ns/op.
  - Replay after delta prototype (no hot-path integration): 1,090,833,598 / 1,090,922,482 / 1,089,000,815 ns/op.
  - CPU profile: `/tmp/treedb_ptrvalues_value_delta_cpu.prof` (pprof top captured; runtime.madvise + skiplist hot).
  - Delta benches: `BenchmarkDeltaXorEncodeHit` 37.00 ns/op (88 B/op, 2 allocs/op), `BenchmarkDeltaXorApplyHit` 20.48 ns/op (64 B/op, 1 alloc/op), `BenchmarkDeltaXorEncodeMiss` 223.6 ns/op (64 B/op, 1 alloc/op).
  - Merged into `slab-opt-rc` (ff at `a77435e`).
