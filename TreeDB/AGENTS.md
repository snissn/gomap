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
- (Done in Phase 16) Always-on benchmark/race gates as automated, repeatable suites.
- (Done in Phase 16) Explicit regression suites for “small `FlushThreshold` + large keycount” (avoid runaway RAM/stalls).
- Long mixed-workload suite that demonstrates stable scans over time (no scan collapse).
- (Done in Phase 16) Compaction “writer-latency budget” test under sustained writes.
- Optional offline vacuum rewrite (`index.db.new` swap) + allocator locality improvements beyond LIFO freelist reuse.
- Production tuning docs for cached-mode knobs.

## Phase 16: Concurrency Optimizations + Phase 15 Rollup

**Goal:** Improve multi-core throughput and tail latency with bounded background work (prune/compaction scheduling) and reduced lock contention, without breaking SWMR semantics or snapshot correctness.

### Phase 16 Near-Term RC (Prioritized Remaining Work)

**Goal:** Before attempting higher-risk concurrency or allocator changes, add hard regression signals so future Phase 16 work can’t “silently” reintroduce stalls, RAM runaway, or scan collapse.

- [x] **Guardrail suite for “big keycount + small flush threshold”** (prevents the 5M–10M key stall/RAM-runaway class)
  - Suite: `cmd/unified_bench/main.go` (`-suite bigkeys_guard`)
  - Guardrail flags:
    - `-max-wall` (abort the run if wall time exceeds the cap)
    - `-max-rss-mb` (Linux-only; reads `/proc/self/status` `VmRSS`)
  - CI: `.github/workflows/unified-bench-suites.yml` runs `bigkeys_guard` with wall + RSS caps.
- [x] **Compaction “writer-latency budget” test**
  - Test: `TreeDB/compaction/compaction_test.go` (`TestCompaction_WriterLatencyBudget`)
  - Gate: compaction and concurrent writers both complete under deadlines; per-op write latency stays under a generous cap.
- [x] **Documentation for suite intent + CI usage**
  - Updated: `cmd/unified_bench/README.md` (documents suites + guardrail flags).

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
- [x] Update `cmd/unified_bench/README.md` to document all supported suites and what they validate.
- [x] Add a new `unified-bench` suite: `flushthrash` (or similar) to catch the prior “small flush threshold + large key count” failure mode.
  - **Implementation notes:**
    - Add a `case` in the suite switch in `cmd/unified_bench/main.go`.
    - Configure TreeDB cached with a small flush threshold (e.g. `6_108_864` bytes) and a large key count (e.g. `5_000_000` or `10_000_000`).
    - Run at least `random_write` and `batch_write`; optionally run scans both “ingest” and “settled”.
    - Print TreeDB cache stats at the end (queue backlog bytes, max queued memtables, backpressure mode).
  - **Gate:** suite completes without stalls and prints a full result table for TreeDB.
- [x] Add a new `unified-bench` suite: `longmix` to validate scan stability over time.
  - **Implementation notes:**
    - Phase 1: ingest (`random_write`) for N keys.
    - Phase 2: churn (overwrite + delete + read mix) for M operations (or seconds).
    - Phase 3: settle (`-settle-before-scans` behavior) and run `full_scan` + `prefix_scan`.
    - Emit a fragmentation report before and after settle (so regressions have a signature).
  - **Gate:** scan QPS does not collapse vs baseline on the same dataset, and fragmentation report looks sane.
- [x] Add a compaction “writer-latency budget” test (Phase 15 deferred gate).
  - **Suggested location:** `TreeDB/compaction/compaction_test.go` or `TreeDB/compaction_backpressure_test.go`.
  - **Suggested approach:**
    - start compaction in a goroutine with small `MicroBatchSize` (e.g. 16–64),
    - concurrently execute N foreground writes and record per-op latency,
    - assert the test completes within a fixed deadline and latency stays under a generous budget (avoid flakiness; prefer timeouts over strict percentiles).

### 16.4 Background Pruning (Move Page Free Work Off the Commit Critical Path)
- [x] **Outcome:** reduce tail latency of commits by moving pruning off the commit critical path (bounded background pruner).
- [x] **Implementation:** `TreeDB/db/prune.go` worker + wiring in `TreeDB/db/db.go` (started on Open, stopped on Close).
- [x] **Observability:** `treedb.prune.*` stats (enabled, interval, max pages/duration, runs, pages freed, last error).
- [x] **Tests:** `TreeDB/db/background_prune_test.go` (progress + stop-on-close).
- [x] **Gates:** `go test -race ./...` + `./bin/unified-bench -suite longmix` (post-settle fragmentation report remains sane).

### 16.5 Background Compaction Scheduler (Optional, But Bounded)
- [x] **Outcome:** optional background slab compaction scheduler with strict bounds + stop-on-close safety.
- [x] **Implementation:** `TreeDB/bg_compaction.go` + wiring in `TreeDB/public.go` via `Options.BackgroundCompactionInterval` and related knobs.
- [x] **Observability:** `treedb.bg_compaction.*` stats (enabled/interval/runs/last error).
- [x] **Tests:** `TreeDB/background_compaction_test.go` (runs + stop-on-close).
- [x] **Gates:** `./bin/unified-bench -suite longmix` (churn + settle + scans).

### 16.6 Compaction Copy Pipeline Optimizations (Profile-Driven)
- [x] **Outcome:** reduce CPU + allocations during compaction copy (sequential buffered reads, fewer allocs, context-cancellable throttling).
- [x] **Implementation:** `TreeDB/compaction/compactor.go` (sequential reads), `TreeDB/compaction/limiter.go` (ctx-cancellable Wait), `TreeDB/compaction/planner.go` (context entrypoints).
- [x] **Gates:** existing compaction + snapshot-safety tests + `go test -race ./...`.

### 16.7 Cached Flush Concurrency (Only If Profiles Show It Matters)
- [x] **Outcome:** opt-in parallel build of combined flush batches without breaking “newest wins” semantics.
- [x] **Implementation:** `TreeDB/caching/db.go` (`Options.FlushBuildConcurrency`).
- [x] **Correctness test:** `TreeDB/caching/db_test.go` (`TestCachingDB_FlushAllParallelBuildPreservesNewestWins`).
- [x] **Gates:** `./bin/unified-bench -suite flushthrash` (small flush threshold), plus `go test -race ./...`.

### 16.8 Offline Vacuum Rewrite + Allocator Locality (Phase 15 Deferred Items)
- [x] Implement an offline vacuum mode (`index.db.new` swap) with a crash-safe protocol:
  - **Implementation:** `TreeDB/db/vacuum_offline.go` (`VacuumIndexOffline`)
  - **Recovery:** `TreeDB/db/index_swap.go` (`recoverIndexSwap`), invoked on backend `Open` before paging.
  - **Protocol artifacts:**
    - `index.db.new` (new file being built)
    - `index.db.new.ready` (durability marker after syncing new file)
    - `index.db.bak` (backup of old index during the swap window)
  - **Crash points tested:** after new sync, after ready marker, after rename old, after rename new.
  - **Public API:** `treedb.VacuumIndexOffline(opts)` (offline; acquires exclusive lock on `opts.Dir`).
  - **Gates:** `go test ./...`, `go test -race ./...`, `./bin/unified-bench -suite flushthrash`, `./bin/unified-bench -suite longmix`.
- [ ] (Moved to Phase 17) Investigate allocator locality beyond LIFO freelist reuse:
  - extent/segment freelist bins, or
  - allocate-near-sibling hints during splits/rebalances.
  - **Gate:** churn→settle scan stability improves vs baseline and `FragmentationReport` span density improves (lower `span_ratio_ppm`).

### 16.9 Production Tuning Docs (Phase 15 Deferred Item)
- [x] Production tuning docs updated (`TreeDB/README.md`, `docs/TREEDB_TUNING.md`) to cover cached-mode knobs and operational guidance.

## Phase 17 (Future Milestone): Allocator Locality + Deeper Concurrency

**Goal:** Improve physical locality (scan stability under churn) and multi-core performance without sacrificing tail latency or memory bounds.

### 17.0 Phase 17 Work Rules (Non‑Negotiables)

- **No silent format changes:** any on-disk format change must be explicitly called out in the PR title/body and include a migration/recovery story. Prefer metadata-only or policy-only changes.
- **Keep SWMR:** do not introduce concurrent writers to the backend engine unless a subphase explicitly does so *and* adds new correctness proofs + stress tests.
- **No writer lock in reads:** `Get`, iterators, and snapshots must remain lock-free-ish (atomics + `RLock` only). Any new read-path lock contention needs profiling evidence and a rollback plan.
- **Bounded background work:** any new goroutine must stop on `Close()` and have explicit per-tick quotas (time and/or bytes and/or items).
- **No “helpful” regressions:** allocator/locality work must not increase worst-case memory usage or cause long stalls under overload (validate with `bigkeys_guard` and RSS/wall caps).
- **One change at a time:** Phase 17 is high risk; implement one policy change per sprint branch, behind an option/flag when feasible.

### 17.0.1 Sprint Workflow (Phase 17)

- Create a sprint branch from the active RC (or `main` if Phase 16 is fully merged): `git checkout -b phase17-sprint-XX`.
- Before coding, run **17.0.2 Baseline Gates** and paste results into the PR description.
- Implement exactly one subphase per sprint branch (keep diffs reviewable).
- After coding, re-run **17.0.2 Baseline Gates** + the subphase-specific gates.
- Include artifacts (profiles + fragmentation report output) and stop for human approval before merge.

### 17.0.2 Baseline Gates (Run Before/After Every Phase 17 Sprint)

**If you see “no space left on device” from Go tests on macOS, run tests with a local TMPDIR:**
- `mkdir -p .tmp && TMPDIR=\"$PWD/.tmp\" go test ./... -count=1`

**Always run:**
- Tests: `go test ./... -count=1`
- Race: `go test -race ./... -count=1` (skip only if toolchain/platform cannot run it; note why)
- Build bench: `make unified-bench`

**Bench smoke (cached + backend):**
- `./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false`
- `./bin/unified-bench -dbs treedbbackend -keys 300000 -seed 1 -progress=false`
- Settled scans (catch scan collapse): `./bin/unified-bench -dbs treedb -keys 300000 -seed 1 -progress=false -settle-before-scans`

**Guardrail suites (must not fail / time out):**
- `./bin/unified-bench -suite flushthrash -progress=false`
- `./bin/unified-bench -suite bigkeys_guard -progress=false` (Linux-only enforces `-max-rss-mb`; still useful elsewhere for wall-cap behavior)
- `./bin/unified-bench -suite longmix -progress=false` (includes fragmentation reports pre/post settle)

**If the sprint touches scans / allocator locality / compaction / vacuum, also run:**
- `./bin/unified-bench -suite churnmaint -progress=false`

### 17.0.3 Required Evidence in PRs

- Paste the full benchmark tables for smoke + suites (before/after) into the PR.
- Include the `FragmentationReport` output emitted by `longmix` (pre/post settle) and call out `treedb.user.pages.span_ratio_ppm`.
- If touching locks/concurrency: include at least one of (before + after):
  - `-mutexprofile before.mutex.pprof` / `after.mutex.pprof`
  - `-blockprofile before.block.pprof` / `after.block.pprof`
  - `-trace before.trace` / `after.trace`
  - Use `go tool pprof -top ./bin/unified-bench before.mutex.pprof` (and after) for quick comparisons.

### 17.1 Allocator Locality Beyond LIFO

**Goal:** Improve scan stability under churn by reducing page-ID scattering without resorting to `PreferAppendAlloc` (which trades locality for file growth).

- [ ] **Design choice (pick one per sprint):**
  - **Extent/segment bins:** maintain freelist buckets by region (e.g. `region = pageID / regionPages`) and allocate from the “hot” region(s) first, or
  - **Allocate-near hints:** thread an “allocate near X” hint through split/merge/rebalance so newly-created pages cluster near siblings/parents.
- [ ] **Implementation notes:**
  - Keep the existing allocator invariants (no double-free, no invalid IDs, crash-safe freelist head persistence).
  - Prefer implementing behind a new option (e.g. `AllocatorPolicy` / `FreelistPolicy`) so we can A/B compare and roll back quickly.
  - Add stats for allocator behavior (e.g. allocations by region, cache hit rate if using bins).
- [ ] **Correctness gates:**
  - `go test -race ./...` (required).
  - Add a unit test for the new policy’s allocator bookkeeping (no leaks, no duplicates) if adding new data structures.
- [ ] **Locality gates (required before merge):**
  - Existing deterministic tests must pass (they already assert locality bounds):
    - `TreeDB/db/vacuum_locality_test.go`
    - `TreeDB/db/vacuum_churn_locality_test.go`
  - `./bin/unified-bench -suite longmix -progress=false` must show **no worse** `treedb.user.pages.span_ratio_ppm` after churn/settle than the baseline for the same workload (call out numbers in PR).

**Stop condition:** if scans regress (full/prefix scan throughput drops materially) or span density worsens, revert or keep the policy behind an explicit opt-in.

### 17.2 Concurrency Experiments (Profile-Driven)

**Goal:** Increase multi-core throughput and reduce tail latency without introducing contention regressions.

- [ ] **Audit first (required):**
  - Collect profiles on a representative workload **before changing code**:
    - `./bin/unified-bench -suite longmix -dbs treedb -progress=false -mutexprofile before.mutex.pprof -blockprofile before.block.pprof`
    - Optional: `-trace before.trace` (useful for scheduling/GC + lock interaction)
  - Identify top contended sites and classify:
    - backend writer serialization (`db.writeMu`) vs
    - cached layer locks (`flushMu`, `bpMu`, iterator-rotation effects) vs
    - GC/allocation hotspots.
- [ ] **Change policy:**
  - Implement exactly one concurrency change per sprint branch.
  - Prefer bounded parallelism (worker pools, limited goroutines) over “goroutine per op”.
  - Any new goroutine must be `Close()`-stoppable and have a “no leaks” test.
- [ ] **Example safe targets (only if profiles justify):**
  - bounded parallel decode/copy in slab compaction *outside* the writer lock (commit remains serialized),
  - scan prefetch/read-ahead with strict memory bounds and easy disable,
  - further moving maintenance work off the commit critical path (always with quotas + stats).
- [ ] **Gates (required):**
  - Run **17.0.2 Baseline Gates** (before/after).
  - Compare `before` vs `after` profiles and explicitly call out improvements/regressions.
  - No scan collapse: `-settle-before-scans` results must remain sane; `longmix` scans must not crater.

### 17.3 Operational Defaults & Guardrails (LevelDB-like) (Moved from Phase 16.10)

**Goal:** Reduce “operator footguns” in cached mode by enabling safe defaults, adding visibility, and adding guardrails so long-running workloads don’t silently accumulate unbounded debt (WAL, flush backlog, disk usage, fragmentation).

#### 17.3.1 Library Defaults for Cached-Mode Backpressure (not just unified-bench)
- [ ] **Outcome:** `treedb.Open` in cached mode has safe default backpressure knobs even when callers don’t set them.
- [ ] **Implementation:**
  - In `TreeDB/public.go`, when `opts.Mode != ModeBackend` and caller leaves them at zero, set:
    - `opts.SlowdownBacklogSeconds = 1`
    - `opts.StopBacklogSeconds = 2`
    - `opts.MaxBacklogBytes = 2<<30` (2GiB)
  - Preserve “explicit disable” semantics:
    - if caller sets all three to `0` intentionally, they get disabled only if we add an explicit `DisableBackpressure` knob (or use negative sentinel values). Decide and document.
  - Ensure this does not conflict with `opts.MaxQueuedMemtables` legacy mode; adaptive should take precedence when any adaptive knob is enabled.
- [ ] **Tests:** add a small unit/integration test that proves defaults are applied and that backlog is capped under a forced slow-flush scenario.
- [ ] **Bench gates:** `./bin/unified-bench -suite flushthrash` must not regress beyond agreed tolerance vs baseline; ensure no runaway RSS in a long random-write run.

#### 17.3.2 Cached-Mode Operational Stats (Explain “why am I slow?”)
- [ ] **Outcome:** Operators can see WAL growth and checkpoint/backpressure behavior via `Stats()`.
- [ ] **Implementation (cached mode `TreeDB/caching/db.go`):**
  - Add stats keys:
    - `treedb.cache.wal_bytes` (logical: include current WAL buffered bytes)
    - `treedb.cache.wal_segments`
    - `treedb.cache.last_checkpoint_unix`
    - `treedb.cache.last_checkpoint_ms`
    - `treedb.cache.last_checkpoint_err` (string; empty if none)
    - `treedb.cache.auto_checkpoint_interval_ms`
    - `treedb.cache.auto_checkpoint_max_wal_bytes`
  - Track last checkpoint outcome in `caching.DB` (atomic/locked fields).
- [ ] **Tests:** unit test that stats keys are present/non-empty when auto-checkpointing is enabled and after at least one checkpoint attempt.
- [ ] **Gates:** `go test -race ./...` must pass; add a small benchmark smoke that enables auto-checkpoint and confirms WAL stays bounded.

#### 17.3.3 Conservative Default Background Slab Compaction
- [ ] **Outcome:** Values/slabs don’t silently accumulate extreme dead space over long churn; compaction runs gently by default.
- [ ] **Implementation:**
  - Pick conservative defaults in `TreeDB/public.go` (cached mode):
    - `BackgroundCompactionInterval` default (e.g. 10m) with strict bounds: `MaxSlabs=1`, low `CopyBytesPerSec`, small microbatch.
  - Integrate with cache backpressure: do not run compaction if cache backlog is above slowdown/stop thresholds; or call `CompactionAssist()` to avoid starving flush.
  - Provide a clear disable knob (`BackgroundCompactionInterval <= 0`).
- [ ] **Tests:** existing background compaction tests + add a “does not run under extreme backlog” test if implementing gating.
- [ ] **Bench gates:** `./bin/unified-bench -suite longmix` must show stable/ improved fragmentation and no scan collapse.

#### 17.3.4 Disk-Usage Guardrails (Fail Fast vs Fill Disk)
- [ ] **Outcome:** Avoid catastrophic “disk full” incidents by adding a configurable cap/guard that triggers maintenance and then fails writes clearly.
- [ ] **Implementation:**
  - Add options (public `treedb.Options`) such as:
    - `MinFreeDiskBytes` (preferred; uses filesystem free-space query)
    - or `MaxTotalBytes` (sum of index + slabs + wal)
  - Enforce on write path (cached + backend): check periodically (e.g. every N commits) to avoid per-op syscall overhead.
  - When tripped:
    - attempt a best-effort `Checkpoint()` (cached),
    - attempt bounded compaction (if enabled),
    - if still over/under threshold, return a clear error (`ErrDiskBudgetExceeded`) and surface via stats.
- [ ] **Tests:** fake filesystem stat layer or dependency injection for disk budget checks; unit test for “maintenance attempted then error returned”.
- [ ] **Gates:** add a CI-only “small disk budget” test using temp dir that triggers the guard without requiring huge data.

#### 17.3.5 Auto-Checkpoint Hysteresis / Thrash Avoidance
- [ ] **Outcome:** Under sustained ingest near the WAL cap, we avoid checkpointing too frequently.
- [ ] **Implementation:**
  - Add a hysteresis policy: trigger at `MaxWALBytes`, then do not run again until `wal_bytes < MaxWALBytes * 0.5` (or a configurable fraction).
  - Ensure “force” triggers still work (explicit `Checkpoint()` and bench flags).
- [ ] **Tests:** unit test that repeated size-trigger checks do not run checkpoint when within hysteresis window.
- [ ] **Bench gates:** ensure throughput doesn’t degrade due to checkpoint thrash in a sustained random write test near the cap.
