# TreeDB Bloat/Pruner Fix Plan (KeepRecent=1 Churn Workloads)

## Problem Statement

On workloads like:

`unified-bench -test batch_write,random_write,batch_delete -checkpoint-between-tests -treedb-keep-recent 1`

`index.db` grows far beyond the post-vacuum size. The dominant suspected cause is
allocator starvation (freelist not replenished fast enough), which forces
`pager.Alloc()` to extend the file (truncate/mmap grow), causing heavy IO + long
checkpoint times.

Key observation from existing tests:
- During the delete phase the live tree becomes tiny, but `pages.total` remains
  huge; vacuum rebuild collapses file size and page count dramatically.

## Goal

1) Keep TreeDB fast under churn + KeepRecent=1 by avoiding needless file growth.
2) Make reclaim/reuse efficient enough that the freelist stays stocked for the
   writer, without introducing unbounded background work.
3) Add/keep regression tests that fail on true bloat behavior and pass after fixes.

Non-goals:
- No background compaction features or range scans.
- No “empty-ish DB” special casing.

## Strategy (Middle Path)

Implement a first-pass improvement that is safe and high leverage:

### A) Speed up “retired -> freelist” by batching frees, but bound lock hold time

We want the pruner to free large numbers of retired pages efficiently, but we
must not stall writers for long periods. `Allocator` currently uses a single
mutex; switching to RWLock and trying to “plan under read lock then apply under
write lock” is unsafe (state changes concurrently; head page can change).

Instead:
- Keep a single mutex for correctness.
- Introduce `FreeMany` with internal chunking so `a.mu` is held only for a
  bounded batch at a time, while still amortizing pager writes/checksums.

This yields most of the benefit of batching without turning pruning into a
writer-stop-the-world event.

## Phase 0: Repo Hygiene (Small Commits)

Split any existing work into small commits so we can bisect:
1. Graveyard eligibility / pinned-reader boundary changes (if any).
2. Pruner cadence default tuning (if any).
3. Batch write-path prune hooks (if any).
4. New/updated churn/bloat regression tests.

## Phase 1: Allocator Bulk Free (Bounded Lock Hold)

### 1.1 Add chunked `FreeMany`

File: `TreeDB/freelist/allocator.go`

Add:
- `func (a *Allocator) FreeMany(ids []uint64) (freed int, err error)`

Implementation notes:
- Use a deterministic chunk size (no new knob), e.g.:
  - `const freeManyChunkIDs = 2048` (or `page.MaxFreeIDs * 8`)
- Loop:
  - lock `a.mu`
  - process up to `min(len(remaining), freeManyChunkIDs)` IDs via `freeManyLocked`
  - unlock
  - repeat until done or error
- In `freeManyLocked`, avoid re-reading/verifying the freelist head page per ID:
  - load head once, fill as many slots as possible, update checksum once
  - when head is full, initialize a new head using a freed page

Correctness constraints:
- Exact semantics match repeated `Free(id)` (same head chaining behavior).
- Ensure page checksum updates occur for each modified freelist page.
- Keep existing `TestHookFreeBeforeChecksum` behavior for the slot-write path.

### 1.2 Add allocator unit tests

New file: `TreeDB/freelist/allocator_free_many_test.go`

Add passing tests:
- `TestAllocator_FreeMany_EmptyHeadCreatesFreelist`
- `TestAllocator_FreeMany_FillsHeadAndChainsPages`
- `TestAllocator_FreeMany_ThenAllocManyReusesAll`
- `TestAllocator_FreeMany_MatchesFreeLoopBehavior`

## Phase 2: Pruner Uses `FreeMany` (and remains responsive)

File: `TreeDB/db/prune.go`

### 2.1 Replace per-ID frees with `FreeMany`
- In `pruneSomeAt`, for each extracted batch:
  - before freeing, check `stopCh`/deadline
  - call `idx.allocator.FreeMany(b.IDs)`

### 2.2 Preserve stop/deadline semantics
If stop/deadline triggers:
- Do not partially free a batch without accounting:
  - If we stop between chunks, reinsert the remaining IDs at that batch seq.
  - Reinsert remaining later batches as today.

### 2.3 Validation
Re-run existing pruning tests and add a small new test (if missing) that ensures:
- pruner can free large batches without blocking commit progress indefinitely
  (bounded by time budget).

## Phase 3: Writer Path Prevents Starvation (Minimal Behavioral Change)

Goal: ensure freelist is replenished *before* Apply is forced to append pages.

File: `TreeDB/db/batch.go`

Approach (bounded, first pass):
- Keep current “pre-apply prune when freelist is empty” logic.
- Keep current “post-apply catch-up prune if append happened” logic.
- After `FreeMany`, reduce multiplier aggressiveness if it causes too much
  contention; otherwise leave as-is.

If starvation still happens (append continues to rise in tests), consider:
- pre-apply prune when freelist head is low, not only zero
  (define a deterministic threshold like “head exists but reclaimable < N” is
  hard to compute cheaply; prefer using alloc counters/append deltas first).

## Phase 4: Tests + Diagnostics

### 4.1 Keep failing “bloat” tests as red until fixed
Targets (existing):
- `TreeDB/caching/freelist_bloat_bench_test.go:TestCachedBenchBloatVacuum`
- `TreeDB/caching/freelist_bloat_pagecount_test.go:TestCachedBenchBloat_PageCountRatioVsVacuum`

### 4.2 Ensure a “phase timeline” diagnostic exists and passes
Existing:
- `TreeDB/caching/freelist_bloat_phase_stats_test.go:TestCachedBenchBloat_PhaseFragmentationTimeline`

If bloat tests still fail post-fix, use this to pinpoint which phase causes:
- pages.total growth
- leaf_fill collapse during delete
- lack of merge/compaction during rewrite

## Phase 5: Benchmark Verification

Run:
- `go test ./...`
- `go test ./TreeDB/caching -run TestCachedBenchBloatVacuum -count=1`
- `go test ./TreeDB/caching -run TestCachedBenchBloat_PageCountRatioVsVacuum -count=1`

Then run:
- `make unified-bench && time ./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb -profile fast -keys 2000000 -format markdown -checkpoint-between-tests -treedb-keep-recent 1 -keep`

Collect:
- ops/sec per phase
- checkpoint durations
- kept `index.db` size
- `treemap frag` reclaimable ratio and pages.total vs user.pages

## Expected Outcome

After `FreeMany` + pruner integration:
- significantly lower `alloc.append` under churn
- smaller peak `pages.total` during delete/rewrite phases
- reduced `index.db` growth and faster checkpoints

If bloat persists even with low `alloc.append`, the remaining issue is likely
structural (maintenance/merge policy), not reclamation timing; tackle those as a
separate follow-up once reuse is demonstrably working.

## Follow-Up: Cached Flush Burst Size (High-Watermark Growth)

Even with aggressive pruning, cached mode can still grow `index.db` to a large
high-watermark when *individual backend commits* (flushes) are very large.

Key mechanism:
- A single backend commit must allocate all new pages needed for that apply
  before any of the pages retired by that same commit can become eligible for
  reuse (KeepRecent gating uses commit sequence).
- In cached mode, `commit_seq` advances slowly (per flush), so each commit may
  touch/retire a very large number of pages.
- If "pages needed for this apply" exceeds "pages eligible from the previous
  commit", the allocator will append and raise the file high-watermark.

This is fundamentally different from "pruner is too slow": even a perfect
pruner cannot recycle pages retired in the same commit.

### Phase 6: Bound Backend Apply Burst Size (Micro-Batch Flush Apply)

Goal: keep each backend commit small enough that:
- eligible pages from the previous commit can cover most allocations, and
- the file high-watermark stays closer to the steady-state live page count.

Approach options (in increasing complexity):

1) Lower cached `FlushThreshold` (simple, but can change throughput/latency).
2) Micro-batch backend apply inside a single cached flush:
   - Consume the merged op stream in fixed-size chunks.
   - For each chunk:
     - write chunk ops into a backend batch
     - commit (advances commit_seq)
     - continue with next chunk
   - Correctness constraint: the merged stream must produce at most one op per
     key ("newest wins" already collapsed) so chunking is deterministic.

Option (2) preserves the larger cached flush threshold (64MiB) while improving
page reuse by increasing commit_seq cadence during heavy flushes.

Concrete deliverables for Phase 6:
- Add a cached option `FlushBackendMaxEntries` (0=disabled).
- In `TreeDB/caching/db.go` flush apply path, if `FlushBackendMaxEntries > 0`,
  commit the backend batch every N entries while draining the merged op stream.
- Add a regression test under `TreeDB/caching/` that:
  - runs a churn workload that previously produced multi-GB `index.db`
  - asserts `alloc.append` stays below a reasonable bound when micro-batching is enabled
  - asserts final `pages.total / user.pages` ratio improves versus disabled.

Notes on locking/allocator concerns:
- Micro-batching reduces the "burst" pages needed in any single backend commit,
  which directly reduces the need for append allocations (and therefore reduces
  expensive `pager.Alloc()` growth syscalls on APFS).
