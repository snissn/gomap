# Agent Plan: Hyper-Optimized `WriteRandom` (TreeDB Memtable)

This document tracks the implementation plan for eliminating large sort spikes in the `hash_sorted` memtable while preserving fast random writes.

The end state is **near real-time background construction of the ordered key view** so that flush/close/iteration does not pay `sort.Strings(1e6)` in one shot.

## Context / Problem

- `hash_sorted` uses a hash map for O(1) point ops and produces ordered iteration by sorting the key set on demand.
- In `op-geth` benchmarks, `WriteRandom` spends a meaningful fraction of time in `(*HashSorted).ensureSortedLocked` sorting ~N keys at flush/close/iterator time.
- This creates:
  - throughput loss for write-heavy workloads that must flush/close, and
  - large tail spikes for the first ordered enumeration of a big memtable.

## Goal

Build and maintain the memtable’s ordered key view **incrementally, in the background**, while writes proceed, so:

- random writes stay hash-fast (no per-write O(log n) ordered-index maintenance),
- flush/iterator sees mostly-ready ordering (no 1M-key cliff), and
- the system remains generally good (not tuned to a single benchmark).

## Non-Goals

- Benchmark-only fast paths (e.g. special-casing empty-backend bulk build).
- Changing TreeDB’s on-disk tree format.
- Adding durable WAL semantics in op-geth unsafe mode.

## Proposed Design (Incremental Ordered View for `hash_sorted`)

### Core idea

Track **only first-seen keys** during the lifetime of a memtable, and sort/merge them in small chunks asynchronously.

This works because:

- The memtable’s key set is the set of first-seen keys (including tombstones introduced via `Delete` on a missing key).
- Updates/deletes after first-seen do not change the key set; ordered iteration can use the sorted keys to look up the *current* value/tombstone state in the hash map.

### Data structures (inside `memtable.HashSorted`)

- `items map[string]hashEntry` (unchanged): canonical store for point ops; values live in the arena.
- New incremental-index fields:
  - `pendingKeys []string` and `pendingBytes int`:
    - append **only on map-miss** (first-seen key).
  - `indexMu sync.Mutex` + `indexCond sync.Cond`:
    - protects index state (`runs`, `doneSeq`) and provides a wait primitive for iterators/flush.
  - `runs [][]string` (seq-indexed):
    - each sealed chunk becomes one sorted run stored at `runs[seq-1]` once background sorting finishes.
  - `indexSeq uint64` and `doneSeq uint64`:
    - monotonic sequence counters so iterators/flush can wait until indexing is “caught up”.

### Background workers (global, constant count)

Use a **global indexer** (package-level) to avoid per-memtable goroutine lifetime problems.

**Goroutine A (chunk sorter):**

- reads sealed key chunks from a buffered work queue (sends are chunk-granularity, not per-write)
- sorts chunk keys (`sort.Strings`)
- stores the sorted run into `runs[seq-1]`
- updates `doneSeq` (with hole-tracking) and signals `indexCond`

Run compaction/merging is intentionally deferred; iteration/flush uses a k-way merge iterator across runs to avoid repeated copying of keys.

Bound goroutine count:

- Constant: 1 global goroutine (optionally 2 if split sorting vs merging later).

### Sealing policy (general, non-overfit)

Seal `pendingKeys` into a chunk when any of the following triggers:

- `pendingBytes >= sealBytesThreshold` (primary trigger; bytes-based is workload-general),
- `len(pendingKeys) >= sealKeysThreshold` (safety cap for small keys),
- optional later: time-based seal (latency cap) if needed.

Sealed chunks are swapped under the memtable lock, then enqueued to the global indexer **after unlocking**. If the queue is full, fall back to synchronous sort+merge outside the hot lock.

### Iterator/flush behavior (barriered correctness)

When `NewIterator(start,end)` is called on a `hash_sorted` memtable:

- For a *frozen* memtable:
  - seal any remaining `pendingKeys` immediately,
  - wait until `doneSeq >= indexSeq` (all keys indexed),
  - iterate via a k-way merge iterator across runs (no 1M-key materialized slice).
- For a *mutable* memtable (rare; but exists in some paths like WAL-disabled `DeleteRange` enumeration):
  - prefer to avoid waiting on background work while writes could be ongoing.
  - acceptable options (choose one explicitly in implementation):
    1) force a “local seal + build minimal iterator view” (cheap if `pending` is small), or
    2) fall back to existing `ensureSortedLocked()` full sort (correct, slower).

### Reset / lifecycle

- `Reset()` must:
  - clear runs/pending state,
  - reset seq counters,
  - keep arena reuse behavior intact.

## Implementation Checklist

### Phase 1 — Minimal incremental indexing (no compactor)

- [ ] Add new-key detection hook:
  - append to `pendingKeys` only on map miss (Set/Delete).
- [ ] Implement sealing (swap + async send) with byte-based threshold.
- [ ] Add a single background sorter goroutine:
  - consumes sealed chunks, sorts, stores into `runs[seq-1]`.
- [ ] Iterator uses k-way merge across runs + optional pending tail; correctness first.
- [ ] Add instrumentation counters (debug stats):
  - `pending_keys`, `pending_bytes`, `runs`, `index_lag_keys`, `seal_queue_depth`.

### Phase 2 — Optional run compaction (if needed)

- [ ] Only if k-way merge overhead is too high: add a compactor to merge runs in the background, but avoid repeatedly copying all keys.

### Phase 3 — Barriers and fallback safety

- [ ] Add `indexSeq/doneSeq` barrier semantics.
- [ ] Define explicit behavior for mutable-iterator edge cases.
- [ ] Add “fall back to full sort” escape hatch if background falls behind badly.

### Phase 4 — Tests and benchmarks

- [ ] Unit tests in `TreeDB/internal/memtable`:
  - random inserts, ensure iterator yields sorted keys covering all items.
  - tombstones + overwrites: key set stable; values/tombstones correct via map lookup.
  - Reset stops workers and clears state.
- [ ] `-race` sanity for `TreeDB/internal/memtable` package.
- [ ] Microbench:
  - compare `hash_sorted` (full sort) vs incremental approach for `NewIterator(nil,nil)` and flush-like scans.

## Success Criteria

- `WriteRandom` no longer shows a dominant single `sort.Strings(N)` spike at flush/close.
- Background indexing overhead is bounded and does not regress pure write throughput significantly.
- No goroutine leaks across memtable rotation/reset.
- Correctness holds under snapshot isolation and WAL-disabled paths.
