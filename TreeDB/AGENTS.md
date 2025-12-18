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

## Phase 15: Compaction/Vacuum + Fragmentation Controls (Completed)

**Goal:** Sustain high throughput and predictable scan latency under long-running random write/delete workloads by actively controlling index + value-log fragmentation.

**Status:** Landed on `main` via PR #8. Remaining validation + follow-ups were rolled into Phase 16 (below) so there is a single “active” checklist.

**Shipped deliverables (merged):**
- [x] Persist slab stats (dead/total bytes) in System tree and validate on reopen.
- [x] Slab compaction: rewrite live records → update pointers safely (snapshot-safe, idempotent, micro-batched apply).
- [x] Compaction planner + IO throttler; cached-mode compaction can flush-assist under overload.
- [x] Index vacuum/rebuild for locality; optional append-only allocation mode for rebuilds.
- [x] Maintenance policies to prevent bloat: fill targets + underfull merge/rebalance + root collapse.
- [x] Slotted-page defragmentation on demand (avoid overwrite “holes” causing splits).
- [x] Fragmentation diagnostics (`FragmentationReport` + validation tests).
- [x] Production hardening: slab tail repair on open, meta tail validation, pager growth preallocation, directory fsync on slab rotation.
- [x] Benchmarking improvements: “settled scans” mode + scan-backlog warning.
- [x] Cached write-back layer: adaptive backlog-bytes backpressure, stable default backlog sizing, combined background flush commits.

**Deferred / rolled into Phase 16:**
- Missing “always-on” benchmark/race gates as automated, repeatable suites.
- Explicit regression suite for “small `FlushThreshold` + large keycount” (avoid runaway RAM/stalls).
- Long mixed-workload suite that demonstrates stable scans over time (no scan collapse).
- Compaction “writer-latency budget” test under sustained writes.
- Optional offline vacuum rewrite (`index.db.new` swap) + allocator locality improvements beyond LIFO freelist reuse.
- Production tuning docs for cached-mode knobs.

## Phase 16: Concurrency Optimizations + Phase 15 Rollup

**Goal:** Improve multi-core throughput and tail latency with bounded background work (prune/compaction scheduling) and reduced lock contention, without breaking SWMR semantics or snapshot correctness.

### 16.0 Work Rules (Non‑Negotiables)
- **SWMR stays SWMR:** backend commits remain serialized under `db.writeMu` unless a subphase explicitly changes the architecture *and* adds new proofs/tests.
- **Reads stay cheap:** do not add writer-lock acquisition to `Get`, iterators, or snapshot reads. Reads may use `RLock`/atomics only.
- **No goroutine leaks:** any new background goroutine must stop on `DB.Close()` and be covered by a test that closes the DB.
- **Bounded background work:** every background loop must have (a) a max duration per tick, and/or (b) a max work quota per tick.
- **Observability required:** add counters/timestamps to `Stats()` for any new background worker (e.g. “runs”, “bytes”, “pages”, “last_error”, “last_run_unix”).
- **Profiling required:** concurrency changes require `-race` and at least one of: mutex profile, block profile, or runtime trace.

### 16.1 Sprint Workflow (How to Work)
- [ ] Create a sprint branch from `main`: `git checkout -b phase16-sprint-XX`.
- [ ] Before coding, run **16.2 Baseline Gates** and paste results into the PR description.
- [ ] Implement exactly one subphase per sprint branch (keep diffs reviewable).
- [ ] After coding, re-run **16.2 Baseline Gates** + any subphase-specific gates.
- [ ] Pause for human approval before merging a sprint branch (include benchmark + profile artifacts).

### 16.2 Baseline Gates (Run Before/After Every Sprint)
- [ ] Tests: `go test ./... -count=1`
- [ ] Race: `go test -race ./... -count=1` (skip only if toolchain/platform cannot run it; note why in PR)
- [ ] Build bench: `make unified-bench`
- [ ] Bench smoke (cached): `./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false`
- [ ] Bench smoke (backend): `./bin/unified-bench -dbs treedbbackend -keys 300000 -seed 1 -progress=false`
- [ ] Capture baseline/after numbers:
  - record commit: `git rev-parse --short HEAD`
  - paste the full output table(s) into the PR description

**When the sprint touches scans / flush / compaction / vacuum, also run:**
- [ ] Settled scans: `./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false -settle-before-scans`
- [ ] Churn suites (existing):  
  - `./bin/unified-bench -suite churn -dbs treedb -seed 1 -progress=false`  
  - `./bin/unified-bench -suite churnvacuum -dbs treedb -seed 1 -progress=false`  
  - `./bin/unified-bench -suite churnmaint -dbs treedb -seed 1 -progress=false`

**When the sprint touches concurrency, also capture contention profiles (same host, same run flags):**
- [ ] Before:  
  `./bin/unified-bench -dbs treedb -test random_write -keys 1000000 -seed 1 -progress=false -mutexprofile before.mutex.pprof -mutexprofilefraction 1 -blockprofile before.block.pprof -blockprofilerate 1`
- [ ] After (same command, different filenames): `after.mutex.pprof`, `after.block.pprof`
- [ ] Compare (examples):  
  - `go tool pprof -top ./bin/unified-bench before.mutex.pprof`  
  - `go tool pprof -top ./bin/unified-bench after.mutex.pprof`
- [ ] Run multi-goroutine stress (backend-mode): `go run ./TreeDB/cmd/stress -duration 20s -workers 8 -keys 100000 -keeprecent 1`
  - **Gate:** no panics, no data corruption, and no obvious latency blowups vs baseline on the same machine.

### 16.3 Phase 15 Rollup: Make “Long-Run Stability” a First-Class Gate
- [ ] Update `cmd/unified_bench/README.md` to document all supported suites (`readme`, `churn`, `churnvacuum`, `churnmaint`) and what they validate.
- [ ] Add a new `unified-bench` suite: `flushthrash` (or similar) to catch the prior “small flush threshold + large key count” failure mode.
  - **Implementation notes:**
    - Add a `case` in the suite switch in `cmd/unified_bench/main.go`.
    - Configure TreeDB cached with a small flush threshold (e.g. `6_108_864` bytes) and a large key count (e.g. `5_000_000` or `10_000_000`).
    - Run at least `random_write` and `batch_write`; optionally run scans both “ingest” and “settled”.
    - Print TreeDB cache stats at the end (queue backlog bytes, max queued memtables, backpressure mode).
  - **Gate:** suite completes without stalls and prints a full result table for TreeDB.
- [ ] Add a new `unified-bench` suite: `longmix` to validate scan stability over time.
  - **Implementation notes:**
    - Phase 1: ingest (`random_write`) for N keys.
    - Phase 2: churn (overwrite + delete + read mix) for M operations (or seconds).
    - Phase 3: settle (`-settle-before-scans` behavior) and run `full_scan` + `prefix_scan`.
    - Emit a fragmentation report before and after settle (so regressions have a signature).
  - **Gate:** scan QPS does not collapse vs baseline on the same dataset, and fragmentation report looks sane.
- [ ] Add a compaction “writer-latency budget” test (Phase 15 deferred gate).
  - **Suggested location:** `TreeDB/compaction/compaction_test.go` or `TreeDB/compaction_backpressure_test.go`.
  - **Suggested approach:**
    - start compaction in a goroutine with small `MicroBatchSize` (e.g. 16–64),
    - concurrently execute N foreground writes and record per-op latency,
    - assert the test completes within a fixed deadline and latency stays under a generous budget (avoid flakiness; prefer timeouts over strict percentiles).

### 16.4 Background Pruning (Move Page Free Work Off the Commit Critical Path)
- [ ] **Outcome:** reduce tail latency of commits by removing `Prune()` work from the `finalizeCommit` critical path.
- [ ] **Files to touch:** `TreeDB/db/db.go`, `TreeDB/lifecycle/graveyard.go` (if adding quotas), plus new/updated tests under `TreeDB/db/*_test.go`.
- [ ] **Implementation steps (backend DB):**
  1. Add a background-worker lifecycle to `db.DB` (`closeCh`, `bgWg`, and a best-effort `pruneCh` signal channel).
  2. Start `go db.pruneLoop()` in `db.Open()` (or behind a new option like `Options.BackgroundPrune`).
  3. In `finalizeCommit`, remove the synchronous `db.Prune()` call; instead, signal `pruneCh` after state/meta are updated.
  4. Implement `pruneLoop` as a bounded loop:
     - waits on `pruneCh` or a `time.Ticker` (e.g. 250ms),
     - calls `pruneSome(maxPages, maxDuration)` and returns.
  5. `pruneSome` must not take `db.mu` for long; it should:
     - read `currentSeq` from `db.state.Load().CommitSeq`,
     - read `minPinnedSeq` from `db.registry.MinPinnedSeq()`,
     - call `db.graveyard.Extract(...)` and free pages via `db.allocator.Free`.
  6. If a quota is needed (recommended), extend `Graveyard` with an “extract up to N IDs” method so the worker can free incrementally without leaking pages.
- [ ] **Observability:** add stats counters (e.g. `treedb.prune.runs`, `treedb.prune.pages_freed`, `treedb.prune.last_run_unix`, `treedb.prune.last_err`).
- [ ] **Tests:**
  - Add an integration test that:
    - writes enough to generate retired pages,
    - closes/reopens to ensure no corruption,
    - asserts the background pruner makes progress (via the stats counters).
  - Add a “no goroutine leaks” test: Open → Close should return quickly; the pruner goroutine must exit.
- [ ] **Bench gates:** `cmd/unified_bench` smoke unchanged; multi-worker stress should show reduced p95/p99 commit latency (capture before/after).

### 16.5 Background Compaction Scheduler (Optional, But Bounded)
- [ ] **Outcome:** make compaction “always eventually happens” in production without operator scripts, while staying within a strict CPU/IO budget.
- [ ] **Files to touch:** likely `TreeDB/db/db.go` (lifecycle), `TreeDB/compaction/planner.go` + `TreeDB/compaction/compactor.go`, and `TreeDB/public.go` (cached-mode wiring).
- [ ] **Implementation steps:**
  1. Add `Options.BackgroundCompaction` + knobs (interval, max slabs/run, dead ratio/min bytes, copy bps, microbatch size).
  2. Start a single compaction goroutine in `db.Open()` (or in cached wrapper) that:
     - periodically runs the planner,
     - runs at most one compaction at a time,
     - respects throttling and writer-assist (cached mode should call `cached.CompactionAssist`).
  3. Ensure the scheduler backs off if the cached flush backlog is high (do not starve flush).
- [ ] **Observability:** add stats counters (runs, slabs compacted, bytes copied, last error, last duration).
- [ ] **Tests:** cached-mode test that runs sustained writes + background compaction and asserts:
  - no deadlocks,
  - backlog drains,
  - at least one zombie slab is created and then unpinned safely.
- [ ] **Bench gate:** churn suite completes without stalls; “settled scans” remain stable.

### 16.6 Compaction Copy Pipeline Optimizations (Profile-Driven)
- [ ] **Outcome:** reduce CPU + allocations during compaction copy (most wins come from fewer allocations and more sequential IO).
- [ ] **Implementation steps:**
  1. Replace per-record `ReadAt` + `make([]byte, ...)` with a sequential reader + reusable scratch buffers.
  2. Keep limiter semantics identical (bytes/sec).
  3. Only after the sequential rewrite, consider a bounded worker pool for decode/liveness checks if CPU-bound.
- [ ] **Tests:** existing compaction idempotence + snapshot safety tests must continue to pass.
- [ ] **Bench gates:** add (or run) a compaction-focused benchmark; compare wall time and allocations (use `-cpuprofile`).

### 16.7 Cached Flush Concurrency (Only If Profiles Show It Matters)
- [ ] **Outcome:** speed up very large combined flushes without increasing memory or breaking “newest wins” semantics.
- [ ] **Implementation sketch:**
  - Parallelize *per-memtable* iteration only (one goroutine per memtable, bounded by a worker pool).
  - Each goroutine returns a `[]batch.Entry` slice; the caller concatenates in **oldest → newest** order.
  - Keep the existing size caps (target bytes / max memtables) and do not affect writer-assist bounds.
- [ ] **Tests:** add a correctness test that multiple memtables with overlapping keys flush correctly under parallel build.
- [ ] **Bench gates:** `random_write` at small flush thresholds should not regress; mutex/block profiles should not worsen.

### 16.8 Offline Vacuum Rewrite + Allocator Locality (Phase 15 Deferred Items)
- [ ] Implement an offline vacuum mode (`index.db.new` swap) with a crash-safe protocol:
  - write new file, fsync, rename atomically, fsync directory, reopen.
  - add tests that simulate “crash between steps” by stopping after each stage and verifying recovery.
- [ ] Investigate allocator locality beyond LIFO freelist reuse:
  - extent/segment freelist bins, or
  - allocate-near-sibling hints during splits/rebalances.
- [ ] Bench gate: after churn, locality and settled scans must improve vs baseline.

### 16.9 Production Tuning Docs (Phase 15 Deferred Item)
- [ ] Add a “Cached Mode Tuning” section to `TreeDB/README.md` describing:
  - `FlushThreshold` and how it interacts with throughput vs scan behavior,
  - adaptive backlog knobs (`SlowdownBacklogSeconds`, `StopBacklogSeconds`, `MaxBacklogBytes`),
  - writer-assist bounds and recommended defaults,
  - when to use `-settle-before-scans` in benchmarking and what it means for production.
