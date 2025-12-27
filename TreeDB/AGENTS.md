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

## Audit Consolidation (2025-12-26 Reviews)

Note: This section consolidates all issues raised across the audits and perf review.
Each item includes a remediation plan and current status.

Priority legend: P0 (critical), P1 (high), P2 (medium), P3 (low/perf).

### P0/P1 Safety & Correctness

- [DONE] Unsafe durability/integrity toggles too easy to enable (DisableWAL, RelaxedSync, DisableReadChecksum, DisableSlabTailRepairOnOpen). Plan: require explicit AllowUnsafe and document profiles; implemented in Open plus docs.
- [DONE] RelaxedSync/SetSync semantics ambiguity. Plan: document that RelaxedSync downgrades sync durability and warn; docs updated.
- [DONE] WAL/vlog rotation atomicity (close-old-before-open-new risk). Plan: open new segment first, then swap/close old.
- [DONE] Missing directory fsync for WAL/vlog creation. Plan: sync parent dir on create/rotate (best-effort).
- [DONE] Slab O_APPEND offset risk. Plan: open slabs without O_APPEND; use WriteAt with explicit offsets.
- [DONE] Recovery should validate root/sysroot page readability. Plan: verify checksum + type before accepting meta candidate.
- [DONE] Skiplist 4GiB panic. Plan: enforce a hard per-shard memtable cap and return ErrMemtableFull before the arena limit.
- [NOT_APPLICABLE] Iterator "invalid" panics in public paths. Plan: keep the Cosmos SDK contract (Next panics if invalid) and ensure docs/tests reflect this behavior.
- [DONE] Empty key checks inconsistent (nil vs len==0). Plan: treat len==0 as empty across public APIs.
- [DONE] WAL record-size cap mismatch (writer vs reader). Plan: enforce max segment size in writer.
- [DONE] Fuzzing coverage for WAL/vlog/page decode and iterator merging. Plan: add targeted fuzz tests for log/page decode and merging bounds; fuzz tests added for WAL/vlog/page/node decode and iterator merging.
- [DONE] Tombstone persistence ambiguity. Plan: correct iterator comments; tombstones are persisted but skipped in iteration.
- [DONE] Docs for durability semantics, RelaxedSync caveats, unsafe views, checksum tradeoffs, confidentiality, and format stability.
- [DONE] Value-log retention / disk growth. Plan: live-pointer tracking + GC/compaction; guardrails and hard caps. Implemented checkpoint-time value-log pruning based on live pointer scans plus caps/warnings.
- [DONE] Verified cache paranoid mode. Plan: add option to force verify; implemented `VerifyOnRead` for always-verify.
- [DONE] Durability matrix + runtime mode reporting. Plan: add docs table and DB.DurabilityMode()/Stats key for effective policy.
- [DONE] Directory fsync on WAL/vlog deletion/rename. Plan: best-effort sync after deletions when durability is required.
- [DONE] Fault-injection tests (fsync/rename/create/short-write). Plan: add targeted crash/recovery tests; rotate/open/syncDir failure coverage added for WAL/vlog and index-swap paths.
- [DONE] Value-log retention cap enforcement. Plan: add hard cap that disables new value-log pointers once retained bytes exceed limit.

### P2/P3 Portability, Security, Ops

- [NOT_APPLICABLE] B-tree structural atomicity / no WAL for index pages. TreeDB uses COW + redundant meta pages; crash tests cover recovery.
- [FIXED] Mmap UAF hazard for GetUnsafe. Snapshots pin slabs; GetUnsafe returns view scoped to snapshot; docs emphasize lifetimes.
- [NOT_APPLICABLE] Page reuse race on freelist. MVCC + graveyard delays reuse until safe.
- [DONE] Windows support (pager uses unix mmap; slab/vlog mmap unsupported). Plan: add OS-specific mapping or safe ReadAt fallback. Status: pager mmap implemented for Windows; slab/vlog use ReadAt fallback when mmap is unavailable.
- [DONE] Endianness for UnsafeCastHeader. Plan: guard with runtime check + skip test on big-endian.
- [DONE] Directory permission hardening. Plan: validate existing perms and warn on open.
- [DONE] Threat model clarity (CRC vs adversarial tampering). Plan: documented that CRC is non-cryptographic; noted encryption/HMAC guidance.
- [DONE] CLI safety/exfil guardrails. Plan: require explicit flags for value dumps.
- [DONE] Error propagation on Close() defer paths. Plan: return close errors from syncDir paths; best-effort dir syncs report close errors.
- [DONE] Unsafe string/byte conversions require immutable inputs. Plan: add build tag (`treedb_safe`) that forces copies for debug/safety.
- [NOT_APPLICABLE] DB.GetUnsafe zero-copy expectation. DB.GetUnsafe returns a safe copy by design; Snapshot.GetUnsafe exposes views with documented lifetimes.
- [DONE] Checksum hotpath micro-optimizations. Plan: remove pooled hash allocations in ChecksumParts.

### Concurrency & Performance (P2/P3)

- [DONE] Global hash_sorted indexer singleton. Plan: scope per DB or make worker pool configurable.
- [DONE] Memtable iterator write blocking (mutable memtable). Plan: keep rotation-on-iterator policy; documented direct memtable usage constraints.
- [DONE] Slab manager opens all slabs at startup (FD exhaustion). Plan: lazy open + LRU. Status: lazy open implemented (no LRU yet).
- [DONE] Global slab lock + CRC inside lock. Plan: precompute CRC outside lock; evaluate per-file locks.
- [DONE] Range-delete WAL batching. Plan: batch copied keys for WAL appends in DeleteRange.
- [DONE] Faster shard hash. Plan: switch to xxhash for shard selection.
- [DONE] Auto-checkpoint hysteresis under retained segments. Plan: base size trigger/rearm on reclaimable WAL bytes.
- [DONE] Batch pooling for backend batches. Plan: pool batch.Batch instances and return on Close.
- [DONE] Compaction live-set memory pressure. Plan: fall back to a bloom filter when the live set grows too large.
- [DONE] CRC pool overhead. Plan: use crc32.Update directly in ChecksumParts.
- [DONE] Flush buffer pooling. Plan: pool batch.Entry slices in flush builder.
- [DONE] Adaptive memtable hysteresis. Plan: require consecutive rotations before switching modes.
- [DONE] mmap remap metrics. Plan: expose remap and dead-mapping counters via Stats.
- [DONE] Large-value pointer memtable mode. Plan: optional memtable value-log pointer storage, value-log reader lookup for Gets/iterators, and tests to validate large-value round-trips.
- [DONE] Iterator stack inline allocation. Plan: add small fixed buffer for typical tree depth.
- [DONE] Linear scan for small nodes. Plan: use linear search for small fanout to reduce branch overhead.
- [DONE] AllocMany + parallel zipper merges. Plan: add AllocMany and parallelize child rewrites with per-child metrics; implemented with concurrency gating.
- [DEFERRED] Freelist heuristics (better reuse). Plan: current region-aware allocator + AllocMany cover locality; defer bucketed allocator until profiling shows fragmentation pressure that warrants a format change.

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

- [x] Add caching options:
  - `SlowdownBacklogSeconds`: begin applying backpressure when backlog exceeds this many seconds of estimated flush work.
  - `StopBacklogSeconds`: block writers when backlog exceeds this many seconds (hard cap).
  - `MaxBacklogBytes`: absolute cap regardless of throughput estimate (safety net).
- [x] Track queued backlog bytes (`sum(queue.Size)`) and maintain EWMA of observed flush throughput (bytes/sec).
- [x] Compute dynamic allowed backlog bytes from EWMA * seconds (with min/max clamps) and apply:
  - slowdown: writers help flush (bounded work per call or time budget).
  - stop: writers wait until backlog drops below a hysteresis threshold.
- [x] Expose benchmark knobs in `unified-bench` to compare buffered ingest vs sustained throughput.
- [x] Add stats/telemetry keys for backlog bytes and estimated flush throughput.
- [x] Add unit tests for slowdown/stop thresholds and `MaxBacklogBytes` behavior.

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

### 17.0.0 Branching Model (Phase 17)

**Why:** Phase 17 changes are high risk and must be easy to bisect and roll back.

- RC branch (always green): `phase17-rc-01`
  - Create it from `main` when starting Phase 17:
    - `git checkout main && git pull`
    - `git checkout -b phase17-rc-01`
    - `git push -u origin phase17-rc-01`
- Sprint branches: `phase17-sprint-01`, `phase17-sprint-02`, ...
  - Always branch from the current RC tip:
    - `git checkout phase17-rc-01 && git pull`
    - `git checkout -b phase17-sprint-XX`
- Merge workflow:
  - Sprint → RC:
    - `git checkout phase17-rc-01`
    - `git merge --no-ff origin/phase17-sprint-XX -m "phase17-rc-01: merge sprint-XX"`
    - `git push origin phase17-rc-01`
  - RC → main happens only after explicit human approval with benchmark evidence.

### 17.0.1 Sprint Workflow (Phase 17)

- Start each sprint from `phase17-rc-01` (see 17.0.0).
- Before coding:
  - Run **17.0.2 Baseline Gates** (and save outputs to files for PR copy/paste).
  - Write a 1–2 sentence “risk hypothesis” in the PR description:
    - what might regress (scans? RAM? durability?),
    - and what specific gate(s) would catch it.
- During coding:
  - Implement exactly one subphase per sprint branch.
  - Keep diffs reviewable (prefer adding a new option and keeping default behavior unchanged until proven).
- After coding:
  - Re-run **17.0.2 Baseline Gates** + any subphase-specific gates.
  - Summarize deltas (before/after) and explicitly state “mergeable?”.

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

**Output capture (required for Phase 17 PRs):**
- Redirect each suite to a file so the PR can include exact text:
  - `./bin/unified-bench -suite longmix -progress=false > before.longmix.md`
  - `./bin/unified-bench -suite longmix -progress=false > after.longmix.md`
  - Repeat for `flushthrash`, `bigkeys_guard`, `churnmaint` as applicable.
- “Pass” means:
  - suites complete without `guard:` aborts,
  - TreeDB does not show `-` for core tests (write/read/scan) in the final table,
  - `longmix` prints valid fragmentation reports (no validation errors).

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

**Key metric:** `treedb.user.pages.span_ratio_ppm` from `FragmentationReport`.
- Interpreting it: `(live_span_pages / live_pages) * 1_000_000`. Lower is better.
- Existing bounds in deterministic tests:
  - after vacuum: expect `<= 1_200_000` (`TreeDB/db/vacuum_locality_test.go`)
  - after moderate churn with `PreferAppendAlloc`: expect `<= 3_000_000` (`TreeDB/db/vacuum_churn_locality_test.go`)

#### 17.1.0 Baseline (mandatory before changing allocator policy)
- [ ] Capture baseline locality + scans with defaults (no `PreferAppendAlloc`):
  - `./bin/unified-bench -suite longmix -progress=false > before.longmix.md`
  - Record `treedb.user.pages.span_ratio_ppm` pre/post settle in the PR.
- [ ] Capture an “upper bound” locality run with append-only allocation (optional but recommended):
  - `./bin/unified-bench -suite longmix -progress=false -treedb-prefer-append-alloc > before.append.longmix.md`

#### 17.1.1 Sprint candidate A (recommended first): Region-biased selection within freelist head (no on-disk format change)

**Goal:** Improve locality without changing freelist page format by *choosing which free page to pop* from the freelist head page.

- [ ] **Implementation plan (exact):**
  1. Add a new allocator policy knob (opt-in first):
     - `treedb.Options` (in `TreeDB/db/db.go`), e.g.:
       - `FreelistRegionPages uint64` (0 disables; start with 8192 pages ≈ 32MiB)
       - `FreelistRegionRadius int` (0 disables; start with 1 region)
     - Wire it in `TreeDB/db/db.go` when constructing the allocator (`alloc := freelist.New(...)`).
  2. Extend `TreeDB/freelist/allocator.go`:
     - Track `lastAlloc uint64` in `Allocator` (protected by `mu`).
     - In `Alloc()` (when using freelist head), if `lastAlloc != 0` and region knobs enabled:
       - decode head freelist `FreeIDs` (up to `Count`),
       - scan from the end for the first ID whose `region(id)` is within `±FreelistRegionRadius` of `region(lastAlloc)`,
       - remove it by swapping with the last element and decrementing `Count`,
       - write the modified body back (`FreelistPageBody.Encode`) and update checksum,
       - return the chosen ID.
     - Otherwise fall back to current behavior (LIFO pop).
     - Update `lastAlloc` on every successful allocation (freelist or append).
  3. Keep `PreferAppendAlloc` semantics unchanged (it should bypass this entirely).
- [ ] **Required new unit tests (exact):**
  - Add `TreeDB/freelist/allocator_locality_test.go`:
    - Construct a freelist head page with a mix of IDs from multiple regions.
    - Set allocator `lastAlloc` to a target region.
    - Assert `Alloc()` returns a same-region (or radius-near) page even when it’s not the LIFO element.
    - Assert fallback to LIFO when no candidate matches.
    - Assert `VerifyChecksumNonMutating` stays true for modified freelist head pages.
- [ ] **Acceptance (merge criteria):**
  - All Phase 17 baseline gates pass.
  - `TreeDB/db/vacuum_locality_test.go` and `TreeDB/db/vacuum_churn_locality_test.go` pass unchanged.
  - `longmix` does not show worse `span_ratio_ppm` than baseline by more than ~10% (call out numbers).
  - Scan throughput in `longmix` (full/prefix) does not crater (no “collapsed scans” symptom).

#### 17.1.2 Sprint candidate B (later): Allocate-near hints from tree operations

**Goal:** Allocate new pages near the page being split/merged (stronger locality), but this is higher risk and should only happen after 17.1.1 is stable.

- [ ] Thread an “allocate near pageID” hint through:
  - B+Tree split/merge/rebalance code paths (likely in `TreeDB/zipper` / `TreeDB/tree`),
  - into the allocator (new method like `AllocNear(hint uint64)`).
- [ ] Add an integration test that churns a small keyspace with deletes/overwrites and asserts locality metrics remain stable.

**Stop / rollback rule:** if scans regress materially or span density worsens, keep the policy opt-in and do not flip defaults.

### 17.2 Concurrency Experiments (Profile-Driven)

**Goal:** Increase multi-core throughput and reduce tail latency without introducing contention regressions.

#### 17.2.0 Profiling Baseline (Sprint candidate: audit-only)
- [ ] Collect CPU + contention profiles on a fixed workload before changing code:
  - `make unified-bench`
  - `./bin/unified-bench -suite longmix -dbs treedb -keys 1000000 -seed 1 -progress=false -cpuprofile before.cpu.pprof -mutexprofile before.mutex.pprof -blockprofile before.block.pprof > before.longmix.md`
  - Optional trace (bigger artifact): `-trace before.trace`
- [ ] Quick reads (copy/paste top 20 into PR):
  - CPU: `go tool pprof -top ./bin/unified-bench before.cpu.pprof`
  - Mutex: `go tool pprof -top ./bin/unified-bench before.mutex.pprof`
  - Block: `go tool pprof -top ./bin/unified-bench before.block.pprof`
- [ ] Categorize top issues (pick exactly one category for the next sprint):
  1. **CPU bound** (e.g. CRC, memmove, iterator merges): prefer algorithmic/alloc reductions.
  2. **Lock contention** (mutex/block profiles dominated by one lock): reduce critical sections or add bounded parallelism.
  3. **IO bound** (syscalls, msync/fsync): add batching, reduce sync frequency, or add background maintenance with strict quotas.

#### 17.2.1 Concurrency Change Template (one change per sprint)
- [ ] **Scope:** only one concurrency change in the sprint branch; everything else must be refactors required for that change.
- [ ] **Always add a knob first (opt-in):**
  - add `treedb.Options` field(s) for the concurrency behavior (0 = default, <0 = disable, >0 = enable/tune),
  - wire through `TreeDB/public.go` and/or `TreeDB/caching/db.go` as appropriate.
- [ ] **Always add a stop-on-close test:**
  - if you add a goroutine: add a unit test that opens, enables it, then closes and asserts it stops (pattern matches existing bg workers).
- [ ] **Always re-profile:**
  - run the same command as 17.2.0 but write `after.*.pprof` and `after.longmix.md`.
  - PR must include top-20 deltas (before vs after) for CPU + mutex + block.

#### 17.2.2 Allowed Targets (ordered; pick the first that profiles justify)
1. Reduce lock hold time in cached layer flush/checkpoint paths (prefer lock narrowing over new goroutines).
2. Add bounded worker pool to compaction copy/decode path (outside writer lock), with a hard cap on goroutines and memory.
3. Iterator/scan prefetching *only if* IO-bound and it can be disabled with one option (and must have a memory cap).

#### 17.2.3 Gates (required)
- [ ] Run **17.0.2 Baseline Gates** (before/after).
- [ ] No scan collapse: `-settle-before-scans` results must remain sane; `longmix` full/prefix scan ops/s must not crater.
- [ ] No runaway memory: `bigkeys_guard` must complete without `guard:` aborts (and without excessive RSS growth if running on Linux with `-max-rss-mb`).

### 17.2.4 (Optional / Tentative) Bulk/Streaming Writer for Append-Only Batches

**Goal:** allow cached TreeDB to match backend throughput for large, strictly ordered batch ingest (e.g. `batch_write`) without violating snapshot or durability invariants.
**Rationale:** limited user benefit because `treedbbackend` already exists for bulk loading; the bulk/streaming path mainly avoids switching engines to reach similar ingest throughput.

- [ ] Add an explicit Bulk/Streaming API (opt-in; not automatic fallback):
  - `BeginBulk(opts)` returns a `BulkWriter` with `Set/Delete/Flush/Close`.
  - Requires strictly increasing keys and append-only range beyond max in-memory key.
- [ ] Safety rules:
  - Acquire an exclusive bulk lock so no concurrent `Get/Iterator/Set/Delete/Batch` can observe partial backend writes.
  - Refuse to start unless append-only + sorted guarantees hold (or return an explicit error).
- [ ] Durability semantics:
  - Default: require WAL disabled.
  - If WAL enabled, require explicit opt-in (e.g. `AllowWALBypass`) and document weaker recovery semantics.
- [ ] Bench target:
  - `./bin/unified-bench -dbs treedb,treedbbackend -test batch_write -keys <large>` should show cached TreeDB close to backend throughput for append-only keys.
- [ ] Optional: add a unified-bench knob to exercise BulkWriter when available.

### 17.3 Operational Defaults & Guardrails (LevelDB-like) (Moved from Phase 16.10)

**Goal:** Reduce “operator footguns” in cached mode by enabling safe defaults, adding visibility, and adding guardrails so long-running workloads don’t silently accumulate unbounded debt (WAL, flush backlog, disk usage, fragmentation).

#### 17.3.1 Library Defaults for Cached-Mode Backpressure (not just unified-bench)
- [ ] **Outcome:** `treedb.Open` in cached mode has safe default backpressure knobs even when callers don’t set them.
- [ ] **Implementation (unambiguous semantics):**
  - Treat `0` as “use defaults”, and `<0` as “disable”.
    - If any of `{SlowdownBacklogSeconds, StopBacklogSeconds}` is `<0` OR `MaxBacklogBytes < 0`: disable adaptive backpressure (set all three to `0`).
    - Else if all three are `0`: set defaults:
      - `SlowdownBacklogSeconds = 1`
      - `StopBacklogSeconds = 2`
      - `MaxBacklogBytes = 2<<30` (2GiB)
    - Else: respect caller-provided values (including enabling partial knobs).
  - Implement the defaulting in `TreeDB/public.go` (cached mode only) so all callers benefit (not just unified-bench).
  - Document the new semantics in `docs/TREEDB_TUNING.md` (0=default, <0=disable).
  - Ensure this does not conflict with legacy `MaxQueuedMemtables` mode; adaptive takes precedence when enabled.
- [ ] **Tests:** add a small unit/integration test that proves defaults are applied and that backlog is capped under a forced slow-flush scenario.
- [ ] **Bench gates:** `./bin/unified-bench -suite flushthrash` must not regress beyond agreed tolerance vs baseline; ensure no runaway RSS in a long random-write run.

#### 17.3.2 Cached-Mode Operational Stats (Explain “why am I slow?”)
- [ ] **Outcome:** Operators can see WAL growth and checkpoint/backpressure behavior via `Stats()`.
- [ ] **Implementation plan (exact; cached mode `TreeDB/caching/db.go`):**
  1. Add fields to `caching.DB`:
     - config: `autoCheckpointInterval time.Duration`, `autoCheckpointMaxWALBytes int64`
     - last run telemetry:
       - `lastCheckpointUnix atomic.Int64` (unix seconds)
       - `lastCheckpointMs atomic.Int64` (duration ms of last checkpoint attempt)
       - `lastCheckpointErr atomic.Value` (stores `string`; empty means success/no error)
  2. Update `StartAutoCheckpoint` to store interval/max bytes into the DB fields (for stats).
  3. Update `Checkpoint()` to record:
     - start time,
     - duration,
     - error string (if any),
     - and set `lastCheckpointUnix` even on error (so operators see it is trying).
  4. Add stats keys in `Stats()`:
     - `treedb.cache.wal_bytes`:
       - compute as `(currentWalLogicalBytes + sum(on-disk segment sizes excluding current))`
       - reuse the existing `listNonEmptyWALSegments` helper
     - `treedb.cache.wal_segments`: `len(segments)` (include the current segment even if size=0)
     - `treedb.cache.last_checkpoint_unix`, `treedb.cache.last_checkpoint_ms`, `treedb.cache.last_checkpoint_err`
     - `treedb.cache.auto_checkpoint_interval_ms`, `treedb.cache.auto_checkpoint_max_wal_bytes`
  5. Ensure Stats computation is safe under concurrent writers (use existing locks/atomics; Stats can do best-effort os.ReadDir/stat).
- [ ] **Tests:** unit test that stats keys are present/non-empty when auto-checkpointing is enabled and after at least one checkpoint attempt.
- [ ] **Gates:** `go test -race ./...` must pass; add a small benchmark smoke that enables auto-checkpoint and confirms WAL stays bounded.

#### 17.3.3 Conservative Default Background Slab Compaction
- [ ] **Outcome:** Values/slabs don’t silently accumulate extreme dead space over long churn; compaction runs gently by default.
- [ ] **Implementation plan (exact):**
  1. Adopt the same “0=default, <0=disable” convention used by auto-checkpointing:
     - If `BackgroundCompactionInterval == 0`, set it to a conservative default: `10 * time.Minute`.
     - If `BackgroundCompactionInterval < 0`, disable background compaction.
  2. In `TreeDB/public.go` (cached mode):
     - keep `MaxSlabs=1` (already defaulted),
     - keep selection defaults conservative (recommend keeping current `DeadRatioThreshold=0.10`, `MinTotalBytes=1`, `MicroBatchSize=256`),
     - set a background copy throttle by default to avoid IO spikes:
       - if `CopyBytesPerSec == 0`, set `CopyBytesPerSec = 32<<20` (32MiB/s)
       - if `CopyBurstBytes == 0`, set `CopyBurstBytes = 64<<20` (64MiB)
  3. Backpressure interaction (pick one and document in code):
     - either: skip running background compaction when cache backlog is above stop threshold, OR
     - always run but call `cached.CompactionAssist()` periodically (existing hook) so foreground flush is not starved.
  4. Ensure the worker stops on `DB.Close()` (existing bg compaction worker already has stop-on-close tests).
- [ ] **Tests:** existing background compaction tests + add a “does not run under extreme backlog” test if implementing gating.
- [ ] **Bench gates:** `./bin/unified-bench -suite longmix` must show stable/ improved fragmentation and no scan collapse.

#### 17.3.4 Disk-Usage Guardrails (Fail Fast vs Fill Disk)
- [ ] **Outcome:** Avoid catastrophic “disk full” incidents by adding a configurable cap/guard that triggers maintenance and then fails writes clearly.
- [ ] **Implementation plan (exact):**
  1. Add options (public `treedb.Options`):
     - `MinFreeDiskBytes int64` (0 disables; default remains disabled until proven safe)
     - `DiskGuardCheckInterval time.Duration` (0 default to 1s; <0 disables checks entirely)
  2. Implement a platform helper in `TreeDB/internal` (or `TreeDB/db`) to read free bytes:
     - On unix: `syscall.Statfs(dir)` and compute `Bavail * Bsize`.
     - On unsupported platforms: return `(ok=false)`; guardrail is a no-op.
  3. Enforce in cached write path:
     - before accepting a write (or every N ops), if the interval elapsed:
       - check free bytes,
       - if under `MinFreeDiskBytes`, run best-effort maintenance (`Checkpoint()` + bounded compaction if enabled),
       - re-check; if still under, return `ErrDiskBudgetExceeded`.
  4. Enforce in backend write path similarly on commit boundaries (`finalizeCommit` or batch write path) to avoid per-op syscalls.
  5. Add stats keys so operators can see “disk guard tripped” and last error.
- [ ] **Tests:** fake filesystem stat layer or dependency injection for disk budget checks; unit test for “maintenance attempted then error returned”.
- [ ] **Gates:** add a CI-only “small disk budget” test using temp dir that triggers the guard without requiring huge data.

#### 17.3.5 Auto-Checkpoint Hysteresis / Thrash Avoidance
- [ ] **Outcome:** Under sustained ingest near the WAL cap, we avoid checkpointing too frequently.
- [ ] **Implementation plan (exact):**
  1. Implement hysteresis in `caching.DB.autoCheckpointLoop` (size-trigger path only):
     - Track an `armed` boolean (protected by the DB’s existing checkpoint/auto-checkpoint state).
     - When `effectiveWalBytes >= MaxWALBytes` and `armed == true`: run `Checkpoint()` and set `armed = false`.
     - Rearm only when `effectiveWalBytes < MaxWALBytes/2`.
  2. Do not apply hysteresis to explicit user triggers:
     - explicit `DB.Checkpoint()` calls should always run,
     - unified-bench `-checkpoint-*` should always run.
- [ ] **Tests:** unit test that repeated size-trigger checks do not run checkpoint when within hysteresis window.
- [ ] **Bench gates:** ensure throughput doesn’t degrade due to checkpoint thrash in a sustained random write test near the cap.
