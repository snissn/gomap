# TreeDB Implementation & Testing Plan (Spec v2.7)

## Scope, Constraints, and Non‑Negotiables

- Embedded, persistent key‑value store implementing the `github.com/cosmos/cosmos-db` `DB` and `Batch` interfaces.
- Hybrid storage:
  - **Index**: memory‑mapped B+Tree in `index.db` with **chunked mmap** growth (expand only; never shrink).
  - **Values**: append‑only **slabs** (`data-XXXX.slab`) storing out‑of‑line values as self‑describing records.
- Copy‑On‑Write commits with **redundant superblocks** (Meta pages 0/1).
- **SWMR** concurrency: single writer lock; lock‑free reads via **Snapshots** (MVCC epochs).
- **CRC32C (Castagnoli)** checksums on every B+Tree page body and every slab record.
- Durability contract:
  - `*Sync` writes must make slab durable **before** meta/index durability boundary.
  - `Write()`/`Set()` may return before durability.
- Iterator semantics must match Cosmos SDK:
  - domain `[start,end)` where `end` is exclusive.
  - reverse iterators descend via repeated `Next()`.
  - `Key()`/`Value()` return **copies**; `Next()` panics if invalid; tombstones skipped.
- Compaction is concurrent and uses **Move‑and‑Micro‑Batch** locking: under the writer lock it verifies old pointers before setting new ones.
- Adaptive inline threshold controller is an optional feature but specified and tested.

**Keyspace Canonical Decision (Spec v2.7):**  
TreeDB uses **dual roots** for namespace isolation: the **User** B+Tree stores user keys **raw** (no prefixing), and internal metadata lives in the **System** B+Tree. Public iterators iterate the User tree only.

---

## Repository Bootstrap

1. Initialize module and deps:
   - `go mod init <module>`
   - add `github.com/cosmos/cosmos-db` and any mmap helpers if needed.
2. Create **canonical packages** (do not rename; prompts assume these paths):
   - `internal/crc` — CRC32C Castagnoli helpers.
   - `internal/pager` — `index.db` file, chunked mmap, page alloc/free, msync/fsync.
   - `internal/page` — page headers/types, slotted page primitives, leaf/internal layouts.
   - `internal/tree` — B+Tree search/COW merge, dual roots, compaction verify‑set helpers.
   - `internal/slab` — slab manager, record IO, stats.
   - `internal/mvcc` — DBState, snapshots, reader registry, graveyard, pruning.
   - `internal/compaction` — candidate selection, optimistic copy, micro‑batch locking, throttling.
   - `internal/adaptive` — telemetry + inline threshold controller.
   - module root package (recommend name `treedb`) — public `DB`, `Batch`, `Options`, `Open`, etc.
3. Optional tooling:
   - `cmd/treedbtool` for debug printing, corruption helpers, kill‑test driver.
4. CI targets:
   - `go test ./...`
   - `go test -race ./...`

---

## Implementation Plan

### Phase 1 — On‑Disk Primitives & Checksums

- Define global constants: `PageSize=4096`, default `InlineThreshold=256`, `InlineHardMin=64`, `InlineHardMax=2048`, slab rotation size `4GB`.
- Implement CRC32C Castagnoli utility:
  - `Checksum(data []byte) uint32`
  - `Verify(data []byte, want uint32) error`
  - shared by pager and slab.
- Implement `ValuePtr`:
  - in‑memory struct with 16‑byte natural alignment.
  - encode/decode to on‑disk LE bytes.
  - helpers for `Offset`, `Length`, `FileID` (`FileID` is `uint32` occupying the final 4 bytes of the 16‑byte layout).
- Implement slab record encode/decode:
  - write: `[CRC32C][KeyLen][ValueLen][Key][Value]`
  - compute CRC over `KeyLen..ValueBytes`.
  - return `ValuePtr` where `Offset` points at `KeyLen` and `Length = 2 + 4 + len(Key) + len(Value)`.
  - read: bounds check, parse forward from `Offset`, verify CRC, return value bytes.
- Implement page header and unsafe parsing/encoding:
  - header layout with CRC32C on body.
  - page flags/types (Meta/Free/Int/Leaf).

### Phase 2 — Pager (`index.db`) with Chunked Mmap

- Options: data directory, chunk size (default 256MB). `ChunkSize` MUST be a multiple of `PageSize` so pages never straddle chunk boundaries.
- Open/create `index.db`:
  - allocate two Meta pages and initial Freelist page.
  - initialize empty user/system roots and total pages.
- Chunked mmap manager:
  - map file in logical pages; physical chunks tracked in slice/map.
  - growth by pre‑allocating disk space (e.g., fallocate) then extending mmap **only for the new chunk**.
  - forbid shrink; guard with panic/error.
  - per‑chunk refcount; `Unmap` only when count==0.
  - provide safe accessors (no exported `[]byte` slices pointing to mmap).
  - enforce bounds (`Offset + PageSize` within mapped region) before any pointer/unsafe cast; SIGBUS cannot be recovered.
- Page allocation:
  - on‑disk freelist pages linked via `NextPageID`.
  - `AllocPage()` prefers freelist, else extends file.
  - `FreePages(ids []uint64)` appends into freelist pages.
- Durability plumbing:
  - `WriteSync()` path triggers `msync` for dirty pages and `fdatasync`/`fsync` for `index.db`.

### Phase 3 — Slotted Pages

- Implement slotted‑page heap/directory operations:
  - insert/update/delete variable‑length entries.
  - binary search on directory keys.
  - in‑page defragmentation and free‑space reuse.
- Leaf entry encode/decode:
  - `[KeyLen][ValueLen][Flags][Key][InlineValue|ValuePtr]`.
  - flags for Inline, Pointer, Tombstone.
- Internal node encode/decode:
  - `[Child PageID][KeyBytes]` in heap; directory offsets.

### Phase 4 — B+Tree Core (Dual Roots)

- Implement search returning cursor path stack (root → leaf).
- Copy‑On‑Write insert/update/delete:
  - clone pages along search path.
  - merge/overwrite/delete with tombstones.
  - split leaf/internal on overflow; propagate separators upward.
  - handle root split.
- Write page CRC32C on commit.
- Provide internal `Tree` interface:
  - `GetRaw(key) (LeafEntry, error)`
  - `SetRaw(key, LeafEntry) error`
- Implement separate User and System trees with root IDs in Meta.
- Decide and implement key‑encoding layer if required by section 1.7 tests.

### Phase 5 — Slab Manager & SlabSet

- On `Open()`:
  - scan `data-*.slab`, open handles, build immutable `SlabSet`.
  - identify active slab from Meta.
  - truncate active slab to `ActiveSlabTail`.
  - delete ghost slabs with `ID > ActiveSlabID`.
- Append‑only writer:
  - append large values as records; rotate at `4GB`:
    1. `fdatasync` active slab
    2. close/seal
    3. create new slab
    4. `fsync` parent directory
- Track per‑slab stats: `DeadBytes`, `TotalBytes`.
  - update on overwrite/delete producing dead bytes.
  - persist in System tree keys `0x00|"slab"|FileID`.
- Implement ref‑counted zombie deletion:
  - `IsZombie` set when removed from active set.
  - physical delete only when `RefCount==0`.

### Phase 6 — MVCC Snapshots, Graveyard, Pruning

- Define immutable `DBState`:
  - `CommitSeq`, `UserRootPageID`, `SystemRootPageID`, `SlabSet`.
- Maintain `atomic.Pointer[DBState]` published after each commit.
- Reader registry:
  - track pinned sequences from active snapshots/iterators.
  - compute `MinPinnedSeq`.
- Snapshot acquisition:
  - atomic load DBState once (Acquire).
  - register `CommitSeq`.
  - increment slab `RefCount` for all slabs in state.
- Snapshot close:
  - deregister seq.
  - decrement slab refs; delete zombies when last ref drops.
- Graveyard:
  - in‑memory `map[CommitSeq][]PageID` of replaced pages.
  - pruner moves pages to on‑disk freelist only when:
    - `RetiredAtSeq < MinPinnedSeq` and
    - `RetiredAtSeq < (CurrentSeq - KeepRecent)`

### Phase 7 — Batch “Zipper” Merge & Redundant Superblock Commit

- Implement `Batch`:
  - ops map, byte size tracking, strict state machine.
  - `NewBatchWithSize` uses size hint.
- Write path:
  - Phase 1 pre‑write large values to active slab and store `ValuePtr`.
  - Phase 2 sort keys and recursively merge into COW tree.
  - collect old pages into graveyard.
  - update system slab stats keys.
- Commit:
  - bump `CommitSeq`.
  - if Sync:
    - `fdatasync` active slab **before** meta/index persistence.
    - `msync` dirty pages + `fdatasync`/`fsync` `index.db`.
  - write inactive Meta page with new roots, freelist head, total pages, `ActiveSlabID/Tail`, CRC.
  - publish new DBState.
- Expose `Set/SetSync/Delete/DeleteSync` as single‑op batches under writer lock.

### Phase 8 — Iterators (Cursor Stack)

- Implement forward `Iterator(start,end)`:
  - initialize stack via search to `start`.
  - `Next()` drill‑down/up per spec.
- Implement reverse `ReverseIterator(start,end)`:
  - seek to first key `>= end` then step back; handle nil end to seek last key.
  - `Next()` moves backward; optional `Prev()` for symmetry.
- Ensure:
  - tombstones skipped.
  - invalid domain (`start>=end`) yields immediately invalid iterator.
  - `Key()/Value()` allocate copies.
  - iterator holds snapshot until `Close()`.

### Phase 9 — Compaction (Move‑and‑Micro‑Batch)

- Candidate selection:
  - scan system slab stats; choose slabs with `DeadBytes/TotalBytes > 0.5`.
- Optimistic copy:
  - sequentially parse cold slab records.
  - optional dead‑hint shortcut (start without, add later).
  - liveness check: if tree pointer != old pointer, skip.
  - copy live records to target slab; build local batch `(Key,OldPtr,NewPtr)`.
- Micro‑batch locking commit:
  - split into micro‑batches (~100 entries).
  - for each micro‑batch under writer lock:
    - `Current = Tree.Get(Key)`
    - if `Current == OldPtr`, `Tree.Set(Key, NewPtr)` else skip.
    - release lock and yield.
- Zombie transition:
  - atomic swap SlabSet to include target and remove cold.
  - mark cold slab zombie; delete if unreferenced.
- `DB.Compact()` triggers a full blocking cycle.
- Throttling:
  - leaky‑bucket limiter around copy loop to cap IO and isolate write latency.

### Phase 10 — Adaptive Inline Threshold Controller

- Add adaptive config: enabled flag, interval `K`, weights `w1..`, `step`, `alpha`.
- Maintain EWMA telemetry at commit boundaries:
  - `leaf_fill_avg`, `split_rate`, `index_write_bytes`,
  - `slab_write_bytes`, `slab_dead_ratio`, `compaction_io_bps`.
- Every `K` commits compute pressures and bounded‑step update.
- Latch threshold at commit start; enforce hard bounds and “always out‑of‑line” for `len(value) > InlineHardMax`.
- Export Stats keys listed in spec when enabled.

### Phase 11 — Public DB API & Misc

- Implement `Open(opts Options)`:
  - validate opts, create dir, open pager/slabs.
  - read/verify both Meta pages; pick highest valid `CommitSeq`.
  - perform slab repair and ghost slab cleanup.
  - initialize User/System trees.
  - rebuild and publish DBState.
- Implement public methods:
  - `Get`, `Has`, `Set`, `SetSync`, `Delete`, `DeleteSync`.
  - `Iterator`, `ReverseIterator`.
  - `NewBatch`, `NewBatchWithSize`.
  - `Stats` (mandatory keys + telemetry).
  - `Print` (debug tree dump).
  - `Close` (flush, unmap, close files).
- Add non‑blocking benchmarks for write throughput and range scans.

### Phase 12: Write-Back Caching Layer (Memtable & WAL)

**Goal:** Implement an LSM-style Level 0 in-memory buffer to resolve write amplification issues for high-frequency random writes.

### 12.1 Foundations
- [x] Add `github.com/google/btree` dependency.
- [x] Implement `WAL` (Write-Ahead Log) writer/reader in `internal/wal`.
    - Format: `[CRC][OpType][KeyLen][ValLen][Key][Value]`.
    - Support `Sync()` for durability.
- [x] Implement `Memtable` wrapper in `internal/memtable`.
    - Use `google/btree`.
    - Support `Set`, `Delete` (Tombstones), `Get`.
    - Track approximate memory usage.
- [x] **Test:** Unit tests for WAL durability and recovery, and Memtable CRUD.

### 12.2 Merging Iterator
- [x] Implement `Min-Heap` for sorting iterators by Key + Layer Precedence.
- [x] Implement `MergingIterator` in `internal/merging`.
    - Layers: `Mutable Memtable` > `Immutable Queue` > `Disk Iterator`.
    - Logic: Pop smallest key, consume shadows (deduplication), filter tombstones.
- [x] **Test:** Complex scenario unit test: Key A set in Disk, Deleted in Queue, Set in Mutable. Verify correct version returned.

### 12.3 CachingDB Core Logic
- [x] Create `caching` package (or extend `treedb`).
- [x] Implement `CachingDB` struct wrapping `treedb.DB`.
- [x] **Write Path:**
    - `Set`: Append to WAL -> Insert to Memtable.
    - `SetSync`: Call `WAL.Sync()`.
- [x] **Flush Pipeline:**
    - Check `Memtable.Size > FlushThreshold`.
    - Rotate: Mutable -> Queue. Close WAL. Create new WAL.
    - Background Worker: Pick from Queue -> `treedb.Batch` -> `WriteSync` -> Delete old WAL.
- [x] **Read Path:** `Get` checks Memtable(s) then Disk. `Iterator` uses `MergingIterator`.

### 12.4 Integration & Config
- [x] Update `treedb.Options` to include `Mode` and `FlushThreshold`.
- [x] Update `Open` to select cached vs backend mode.
- [x] **Spec Update:** Update `specs/spec.md` with Section 7 describing this architecture.
- **Verification:** Run `dbbench` to confirm performance improvement (~3.4k -> ~40k+ ops/s expected for load).

### 12.5 Adaptive Backpressure (Flush Debt Budget)

**Goal:** Make write-back caching behavior tunable and safe under sustained overload by budgeting flush debt in time/bytes instead of a fixed queue depth.

- [ ] Add caching options:
  - `SlowdownBacklogSeconds`: begin applying backpressure when backlog exceeds this many seconds of estimated flush work.
  - `StopBacklogSeconds`: block writers when backlog exceeds this many seconds (hard cap).
  - `MaxBacklogBytes`: absolute cap regardless of throughput estimate (safety net).
- [ ] Track backlog bytes (`mutable.Size + sum(queue.Size)`) and maintain EWMA of observed flush throughput (bytes/sec).
- [ ] Compute dynamic allowed backlog bytes from EWMA * seconds (with min/max clamps) and apply:
  - slowdown: writers help flush (bounded work per call or time budget).
  - stop: writers wait until backlog drops below a hysteresis threshold.
- [ ] Expose benchmark knobs in `unified-bench` to compare buffered ingest vs sustained throughput.
- [ ] Add stats/telemetry keys for backlog bytes and estimated flush throughput.
- [ ] Add unit tests for slowdown/stop thresholds and `MaxBacklogBytes` behavior.

---

## Phase 13: Read Optimization Sprint

**Goal:** Achieve 500k+ ranges/s by eliminating allocations and setup overhead.

### 13.1 O(1) Snapshot Acquisition (Group RefCounting)
- [x] Create `internal/slab/group.go` (or `SlabGroup` struct).
- [x] Group holds map of files + single `atomic.Int64` for snapshot pinning.
- [x] Update `DBState` to hold `*SlabGroup` instead of `SlabSet`.
- [x] Refactor `AcquireSnapshot` to increment group ref instead of iterating all files.
- [x] **Test:** Verify zombie deletion waits for group refcount to drop.

### 13.2 Unsafe Iterator Interface
- [x] Define `UnsafeIterator` in `internal/iterator`:
    - `Valid() bool`: Checks if the iterator points to a valid entry.
    - `Next()`: Advances the iterator.
    - `Seek(key []byte)`: Positions the iterator.
    - `UnsafeKey() []byte`: Returns a **view** (slice pointing directly to internal buffer) of the current key. Callers MUST NOT modify it and it is only valid until the next `Next()` or `Seek()`.
    - `UnsafeValue() []byte`: Returns a **view** of the current value (lazy-loaded if from slab). Similar safety warnings apply.
    - `Key() []byte`: Returns a **copy** of the key (for safe public API use).
    - `Value() []byte`: Returns a **copy** of the value (for safe public API use).
    - `IsDeleted() bool`: Indicates if the current entry is a tombstone.
    - `Error() error`: Returns any error encountered.
    - `Close() error`: Releases resources.
    - `Domain() (start, end []byte)`: Returns the iteration bounds.
- [x] Update `memtable.Iterator` to implement this (it already returns views).

### 13.3 Specialized TwoWayMerger
- [x] Implement `TwoWayMerger` in `internal/merging` to replace `MergingIterator` for the 2-source case.
- [x] Optimize logic: direct comparison `if keyA < keyB`, no interface dispatch.
- [x] Handle shadowing and tombstones efficiently.

### 13.4 Lazy Disk Iterator
- [x] Modify `internal/tree/iterator.go` (Disk Iterator) to implement `UnsafeIterator`.
- [x] **Lazy Value Loading:**
    - `Next()` (or `Seek()`) only parses the leaf entry to identify the key and whether the value is inline or a `ValuePtr`. It does NOT read the value bytes from the slab.
    - `UnsafeValue()` (or `Value()`) triggers the slab read *only when* the value is actually requested and is a `ValuePtr`. The read value is then cached within the iterator struct to prevent repeated I/O.
- [x] **Zero-Copy Keys:** `UnsafeKey()` returns slices directly into the memory-mapped index pages, eliminating allocations for keys during internal iteration.

### 13.5 Integration
- [x] Update `caching.DB` to use `TwoWayMerger` and `UnsafeIterator`.
- [x] Update public `Iterator` wrapper to perform the final safety copy (`append([]byte(nil), view...)`).
- [x] **Benchmark:** Run `dbbench` Range Scan to verify >500k ranges/s.

---

## Work Procedure

1.  **Read Context**: Before starting a phase, re-read relevant spec sections.
2.  **Implementation**: Write code + Unit Tests.
3.  **Verification**: Run tests.
4.  **Commit**: Git commit with clear message.
5.  **User Check**: Briefly pause for user feedback if major design decisions arise.
6.  **Spec Update**: Keep `specs/spec.md` in sync with completed phases.

## Phase 14: Verified Page Cache (Optimization)

**Goal:** Reduce CPU overhead by skipping CRC verification for already-checked pages.

### 14.1 Pager Update
- [x] Add `VerifiedBitset` (slice of uint64) to `Pager`.
- [x] Implement `IsVerified(pageID)`, `MarkVerified(pageID)`, `MarkUnverified(pageID)`.
- [x] **Thread Safety:** Ensure atomic access or lock protection.

### 14.2 Integration
- [x] **Tree:** Update `GetEntry` to check `IsVerified`. If verified, skip `VerifyChecksum`. If not, verify and call `MarkVerified`.
- [x] **Alloc/Write:** Ensure `GetForWrite` and `Alloc` call `MarkUnverified`.

### 14.3 Verification
- [x] **Benchmark:** Compare Read throughput before/after.
- [x] **Safety Test:** Corrupt a page in RAM (via unsafe access) *after* verification and ensure the system (correctly) fails to detect it if cached, but detects it if cache is cleared. (Validates the cache is working).

## Phase 15: Compaction/Vacuum + Fragmentation Controls

**Goal:** Sustain high throughput and predictable scan latency under long-running random write/delete workloads by actively controlling index + value-log fragmentation.

### 15.0 Phase Gates (Always-On)
- [ ] **Correctness:** `go test ./...` passes (no skipped new tests).
- [ ] **Race safety (when supported):** `go test -race ./...` passes.
- [ ] **Bench smoke:** `make unified-bench && ./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false` completes without errors.
- [ ] **Baseline capture (local, before code changes):** record the above smoke output + commit hash for phase15 comparisons.

### 15.1 Telemetry + Triggers (Foundations)
- [x] Persist per-slab stats `[DeadBytes][TotalBytes]` in the System tree and update on overwrite/delete (not just per-commit metrics).
- [x] Add stats for index health: leaf fill-factor stats, page span/locality, and freelist reclaimable pages (see `DB.FragmentationReport()`).
- [x] Define compaction/vacuum triggers:
  - slab dead ratio threshold + minimum bytes,
  - index page growth / fill-factor threshold,
  - manual “run once” hooks for ops.
- [ ] **Gates (before 15.2):**
  - [x] Unit test: slab stats encode/decode + persistence across reopen.
  - [x] Unit test: stats reflect overwrite/delete (dead bytes increases; total bytes monotonic).
  - [ ] Bench smoke: `./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false` within ±5% of baseline for write tests (unless intentionally changed).

### 15.2 Slab Compaction v2 (Actually Reclaims Space)
- [x] Implement “rewrite live records” compaction:
  - copy live records from candidate slab → destination slab (currently the active slab),
  - build `[]CompactionOp{Key,OldPtr,NewPtr}` as you copy.
  - **Note:** recovery currently prunes slabs with `ID > ActiveSlabID` (ghost slabs), so compaction must not create new slab IDs unless `ActiveSlabID` advances accordingly.
- [x] Add a compaction planner that picks candidate slabs by dead ratio / age, and skips active slab by default.
- [x] Mark compacted slabs **zombie** and delete only when unpinned (snapshot-safe).
- [ ] **Gates (before 15.3):**
  - [x] Integration test: compaction preserves data for pointer values (Get).
  - [x] Integration test: snapshot safety (old slab not deleted while pinned).
  - [x] Integration test: compaction is restartable/idempotent (rerun does not corrupt; second run is no-op-ish).
  - [ ] Bench smoke: write/read/scan unchanged when compaction is idle (no >5% regression vs baseline).

### 15.3 Micro-Batched Apply (Bound Writer Pauses)
- [x] Change compaction apply to commit in micro-batches under `writeMu`:
  - verify current pointer matches `OldPtr`,
  - apply pointer update (batch of N keys),
  - yield between micro-batches to bound tail latency.
- [x] Ensure crash safety: compactor is restartable/idempotent (re-copy safe; apply verifies pointers).
- [ ] **Gates (before 15.4):**
  - [x] Concurrency test: foreground writes can proceed between micro-batches (no deadlocks; bounded wait).
  - [x] Integration test: partial apply (stop after 1 micro-batch) leaves DB consistent and resumable.
  - [ ] Bench smoke: `random_write` not regressed >10% vs baseline (expected small overhead from more commits/locking).

### 15.4 Throttling + Backpressure Integration
- [x] Add compaction IO throttling (bytes/sec limiter) and expose config knobs.
- [x] Integrate with caching-layer backpressure so compaction does not starve foreground flush and vice-versa (bounded “assist” only under overload).
- [ ] **Gates (before 15.5):**
  - [x] Unit test: throttler enforces upper bound (time-based; allow small tolerance).
  - [x] Integration test: cached-mode compaction triggers bounded flush assist under backlog (no deadlock; backlog drains).
  - [ ] Integration test: compaction under sustained writes does not exceed configured writer-latency budget (bounded blocking).
  - [x] Bench smoke with compaction enabled: completes without stalls/timeouts (see `./bin/unified-bench -settle-before-scans -treedb-compact-before-scans ...`).

### 15.5 Index “VACUUM” / Rebuild (Fast Locality Reset)
- [x] Add an in-place “vacuum” that bulk-rebuilds the user B+Tree into freshly allocated pages (append-only allocator to keep pages contiguous).
- [x] Atomically swap the root to the rebuilt tree and retire the old pages to the graveyard for pruning.
- [ ] Optionally support a full offline rewrite (`index.db.new` swap) for maximum physical locality (requires robust open/rename protocol).
- [ ] **Gates (before 15.6):**
  - [x] Integration test: vacuum preserves all key/value pairs and iteration order.
  - [x] Structural test: post-vacuum live page span/locality meets target (e.g. `span <= 1.2x livePages`).
  - [ ] Benchmark gate (fragmentation stress): after a churn-heavy run, vacuum + “settled scans” improves `full_scan/prefix_scan` vs pre-vacuum on the same dataset.

### 15.6 Allocation Locality (Reduce Re-Fragmentation)
- [ ] Improve allocator locality beyond LIFO freelist reuse:
  - extent/segment allocation, or
  - allocate-near-sibling hints during zipper splits and vacuum builds.
- [x] Add “append-only / sequential alloc mode” as an option for vacuum and large rebuild operations (see `Options.PreferAppendAlloc` and vacuum allocator).
- [ ] **Gates (before 15.7):**
  - [x] Unit test: sequential alloc mode produces mostly monotonic page IDs during bulk build.
  - [x] Structural test: scan locality (live span) stays within target after post-vacuum rewrite + moderate churn.

### 15.7 B+Tree Maintenance Policies (Prevent Index Bloat)
- [x] Add a configurable leaf/internal **fill-factor target** (avoid “pack to 100% then split” churn).
- [ ] Implement underfull-page merge/rebalance after deletes/overwrites (merge siblings or redistribute entries below a threshold).
- [ ] **Gates (before 15.8):**
  - [ ] Unit/integration test: repeated overwrite workload does not grow page count unbounded (with maintenance enabled).
  - [ ] Structural test: average leaf fill-factor remains above target band after churn (with maintenance enabled).

### 15.8 Slotted-Page Defragmentation (In-Page Holes)
- [x] Implement heap compaction for leaf/internal pages on overwrite/update (defrag on demand when a page reports full).
- [x] Add targeted tests for repeated overwrite workloads to ensure stable fill-factor and page count.
- [ ] **Gates (before 15.9):**
  - [x] Unit test: repeated overwrite in a single leaf reuses space / compacts (does not hit `ErrNodeFull` prematurely).
  - [ ] Benchmark gate: fewer splits and higher leaf fill under update-heavy workloads (vs baseline).

### 15.9 Benchmarking + Diagnostics
- [x] Add benchmark modes that separate “ingest” vs “settled” scans:
  - optional flush/drain or close/reopen before scan tests,
  - record backlog/queue stats at each phase boundary.
- [x] Add a “fragmentation report” command/tool: leaf fill percentiles, freelist reclaimable pages, live page span, and slab dead ratios.
- [ ] **Gates (before 15.10):**
  - [x] `unified-bench` can run a churn suite that optionally settles before scans (deterministic, comparable).
  - [x] “fragmentation report” validates invariants (no missing pages; sane histograms) and is used in tests.

### 15.10 Production Durability/Footguns (Related Hardening)
- [x] `fsync` the directory on slab rotation (persist new file entry).
- [x] Preallocate pager growth (`fallocate`) before mapping new chunks to fail fast on ENOSPC.
- [x] Recovery: reject meta pages whose `ActiveSlabTail` exceeds slab file size (roll back to older meta).
- [x] Slab tail repair on open: detect and truncate partial/corrupt tail records (common crash case).
- [ ] **Gates (completion):**
  - [x] Recovery tests cover: torn slab tail, mid-rotation crash, and meta consistency.
  - [ ] End-to-end bench: long mixed workload + settle + scans shows stable scan throughput (no “collapsed” prefix_scan).
