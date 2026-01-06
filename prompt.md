# Prompt: Execute Phase 17.3 (Value Index + Unified Seq + Refcounted GC)

You are an autonomous coding agent working in the `gomap` repo to execute the Phase **17.3** plan described in `TreeDB/AGENTS.md` (“Value Index + Unified Seq + Refcounted GC”). Your goal is to implement the architecture so it can be benchmarked on Celestia mainnet sync workloads and merged as a clean PR series.

## Working Context

- Repo: `~/dev/snissn/gomap`
- Current “RC-ish” integration branch: `rc/treedb-pointer-compress+maint`
- Key adjacent consumers: `github.com/snissn/cosmos-db` (TreeDB adapter + env knobs), `celestia-app` testnet/mainnet sync harnesses.

## Objectives (Outcomes)

### Primary Outcomes

1) **Value Index (indirection layer)**
   - User tree stores `key -> ValueID` (fixed-size stable identifier).
   - Value Index stores `ValueID -> ValuePtr` (file/offset/len/flags, plus any needed metadata).
   - Compaction/GC that relocates value bytes updates the Value Index only; it must not require rewriting many User-tree keys.

2) **Unified sequence space + checkpoint boundary**
   - A single monotonic `seq` orders all replayable mutations that influence pointer validity.
   - Clear checkpoint invariant: “all ops ≤ seq X are reflected in index/meta”.
   - Replay is deterministic and correctly merges the streams.

3) **Refcounted GC for `vlog-*` / slab segments**
   - Reclaim disk by deleting/truncating segments whose references are no longer reachable from latest roots and any pinned snapshots.
   - Observability: retained bytes/segments, reclaimable bytes, last GC duration, last error.
   - Operator path: offline GC first (CLI), then bounded background GC later if needed.

### Benchmark/Operator Outcomes (Celestia workload)

- Reduce “mid-run index balloon” and maintenance churn during Celestia mainnet sync.
- Keep `wal/` growth bounded and reclaimable.
- Make post-sync cleanup predictable (checkpoint + GC + optional vacuum), and reduce total disk footprint.

## Execution Requirements

- Work on a new branch off `rc/treedb-pointer-compress+maint`:
  - Example: `phase17-rc-vi-gc` (or `phase17-sprint-01-value-index` etc.)
- Prefer an incremental PR series (mergeable steps):
  1) ValueID plumbing + Value Index read path (no GC yet).
  2) Write path changes + correctness tests.
  3) Unified seq + checkpoint contract.
  4) Refcounted GC with offline CLI + stats.
  5) (Optional) background bounded GC.
- Keep diffs reviewable; avoid unrelated refactors.
- Add tests only where the repo already has patterns (TreeDB has lots of tests).
- Run `go test ./...` for TreeDB scope when feasible; avoid heavyweight external integration tests unless required.

## Design Constraints / Decisions

### Value Index storage

- Store Value Index in the **System tree inside `index.db`** under a reserved prefix.
- Use a dedicated prefix, e.g.:
  - `0x00 'v' 'i'` + big-endian `ValueID` → encoded `ValuePtr`
- Must be snapshot/MVCC-safe: older ValueIDs remain readable while snapshots are pinned.

### Scope

- Start by applying Value Index only to **pointer-backed values** (e.g. large values and/or forced pointer mode).
- Keep behavior identical for inline values initially to reduce blast radius.

### Crash safety invariant (must hold)

- Never publish a pointer (or ValueID→ValuePtr mapping that becomes reachable) unless the referenced bytes are durable enough for the durability mode.

## Concrete To‑Dos

### A) ValueID + Value Index (first deliverable)

- Define `ValueID` type and encoding:
  - size: 8 bytes (uint64), big-endian key encoding.
  - allocation: monotonic counter stored durably (likely in System tree meta or Meta page extension).
- Implement Value Index:
  - encode/decode `ValuePtr` values (include flags; consider future fields).
  - CRUD operations in backend DB layer for mapping reads/writes.
- Update read path:
  - if leaf entry indicates “ValueID”, resolve to `ValuePtr` using System tree.
  - then read value bytes via slab/vlog.
- Update write path for pointer-backed writes:
  - allocate ValueID
  - write `ValueID -> ValuePtr` in Value Index
  - write `key -> ValueID` in User tree
- Add unit tests:
  - ValueID mapping roundtrip
  - snapshot visibility (old mapping stays visible with pinned snapshot)
  - correctness of get/iterator on pointer-backed values using ValueID indirection

### B) Unified seq + checkpoint contract (second deliverable)

- Establish one sequence space for replayable operations:
  - define how `seq` is assigned and persisted.
  - ensure both `wal-*` and `vlog-*` (if both exist) share ordering.
- Define checkpoint boundary:
  - what exactly does Checkpoint mean (files, meta, seq)?
  - how does it bound replay and GC safety?
- Update replay semantics to be deterministic and ordered.
- Add crash/recovery tests demonstrating:
  - ordering correctness and no dangling pointers
  - consistent replay after partial segments

### C) Refcounted GC for vlog/slab segments (third deliverable)

- Pick refcount granularity (start with segment-level).
- Compute reachability:
  - scan Value Index to build per-segment live counts.
  - account for pinned snapshots (ReaderRegistry min pinned seq) and “keep recent” retention.
- Implement offline GC:
  - delete segments with zero live refs and safely past checkpoint/min pinned seq.
  - add `treemap gc` (offline) command to run GC and report reclaimed bytes.
- Add observability stats keys (in `Stats()`):
  - retained segments/bytes, reclaimable bytes, last run time/duration, last error.
- Add tests for:
  - GC does not delete live segments
  - GC deletes segments once unreachable and safe

## Success Criteria / Acceptance

- `go test ./...` passes for TreeDB packages (at least non-integration subset).
- Bench harness shows:
  - reduced `index.db` growth during pointer-moving maintenance operations
  - bounded `wal/` growth and reclaimable disk via GC
- Documentation updated as needed:
  - `docs/TREEDB_TUNING.md` should mention new options/commands.

## Notes / Non‑Goals (for now)

- Do not add “column families” or multi-DB namespacing here.
- Avoid rewriting unrelated caching layer code.
- Keep defaults conservative; enable features behind options/envs until proven.

