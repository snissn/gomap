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

## Optimization punch list (ordered; update statuses as you go)

### 1) Multi-Stream Slabs (Parallel Writing) — High impact / medium risk
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [x] rejected  [ ] deferred
- Branch: `slab-opt-01-multistream`
- Checklist:
  - [ ] Design: choose N streams, stream->file layout, and pointer encoding.
  - [x] Implement: parallel append streams + “soft barrier” rotation.
  - [ ] Safety: correctness under crash/restart; no torn writes.
  - [x] Bench: local pointer-values replay; capture cpu profile.
  - [ ] Server: run `run_celestia_trace.sh` and compare wall time + sizes.
  - [x] Decide: accept (merge) or reject (revert) and document.

### 2) Local Dictionary Compression (Zonal Dictionaries / Slab V2) — High impact / medium risk (breaking)
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-02-zonal-dicts`
- Checklist:
  - [ ] Spec compliance: implement Slab V2 header/zone layout as described.
  - [ ] Read path: O(1) dict selection; zero-copy dict slices from mmap.
  - [ ] Write path: zone headers + dict write policy.
  - [ ] Migration strategy: v1->v2 handling (explicitly breaking is OK, but must be intentional).
  - [ ] Tests: roundtrip, corruption detection, cross-zone reads.
  - [ ] Bench: pointer-values replay + server trace; track syscall reduction and bytes written.

### 3) Pre-emptive Training & Entropy Monitoring — Medium impact / medium risk
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-03-entropy-training`
- Checklist:
  - [ ] Implement rolling compression-ratio metrics per zone.
  - [ ] Trigger background training when ratio degrades; publish next-zone dictionary.
  - [ ] Verify no writer stalls; no extra syscalls on the read fast path.
  - [ ] Bench + server trace comparison.

### 4) Dictionary Deduplication — Medium impact / medium risk
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-04-dict-dedup`
- Checklist:
  - [ ] Hash dictionaries (xxhash64) and reuse global or recent locals when similar.
  - [ ] Implement USE_REF / USE_GLOBAL flags.
  - [ ] Bench: measure slab bytes reduction and any latency impact.

### 5) Two-Pass Compaction (Gold Standard) — Lower impact on write hot path
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-05-two-pass-compaction`
- Checklist:
  - [ ] Implement representative global dict selection for rewritten slabs.
  - [ ] Detect shift points; schedule local dict overrides.
  - [ ] Benchmark compaction throughput and resulting slab sizes.

### 6) Multi-Level Slab Tiering — Lower impact on write hot path
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-06-slab-tiering`
- Checklist:
  - [ ] Define “active fast-append” vs “cold compressed” slab formats.
  - [ ] Implement transition policy; verify reads across tiers.
  - [ ] Benchmark space savings vs read latency.

### 7) Value Delta Encoding — Huge potential, high risk (data model)
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-07-value-delta`
- Checklist:
  - [ ] Decide delta format + how to reconstruct values for reads.
  - [ ] Integrate safely with compaction/vacuum.
  - [ ] Add adversarial tests (replay, corruption, partial updates).

### 8) Zonal Bloom Filters — Mostly recovery/audit
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-08-zone-bloom`
- Checklist:
  - [ ] Add bloom filters to zone headers (keys only).
  - [ ] Provide tooling to rebuild index faster using blooms.
  - [ ] Ensure omit-keys mode is still supported (bloom is “identity proof”).

### 9) Huge-Page Awareness (OS optimization) — Read-side CPU
- Status: [ ] planned  [ ] in_progress  [ ] accepted  [ ] rejected  [ ] deferred
- Branch: `slab-opt-09-hugepage`
- Checklist:
  - [ ] Align zones to 2MB boundaries; apply `madvise(MADV_HUGEPAGE)` where applicable.
  - [ ] Benchmark random-read CPU and TLB effects (needs careful measurement).

## Work Log (append-only; keep it current)

### 2026-01-10
- Created this tracker + automation prompt/script scaffolding.
- Next: run pointer-values profiling and execute items starting from #1.
- Baseline (slab-opt-01-multistream): `BenchmarkTraceReplayTimeline` ns/op: 1,146,542,800 / 1,209,162,630 / 1,153,101,110. CPU profile: `/tmp/treedb_ptrvalues_cpu.prof`.
- Decision: deferred #1 (multi-stream slabs) — requires expanding meta to track multiple active slab tails/IDs; current on-disk format only records a single `ActiveSlabID/Tail`, so parallel active slabs would risk unrepaired torn tails on crash. Revisit once a meta v2 or multi-active slab recovery strategy is defined.

### 2026-01-10 (multistream attempt)
- Branch `slab-opt-01-multistream`: implemented opt-in `AppendMany` multi-stream batching (streams=4), commits `dff0eec` + revert `9cb85c4`.
- Baseline (#1 pre-change): `BenchmarkTraceReplayTimeline` ns/op: 1,122,240,140 / 1,102,395,519 / 1,108,738,446.
- After (#1 streams=4): `BenchmarkTraceReplayTimeline` ns/op: 1,110,628,821 / 1,172,653,904 / 1,110,231,683. CPU profile: `/tmp/treedb_ptrvalues_cpu_multistream.prof`.
- Revert confirm (#1): `BenchmarkTraceReplayTimeline` ns/op: 1,083,553,706 / 1,101,377,888 / 1,103,611,417.
- Decision: reject #1 for now (regression/no measurable local win); revisit only with meta v2 or a format that can safely track multiple active slab tails.
