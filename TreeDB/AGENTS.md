# TreeDB Implementation & Testing Plan (Spec v2.7)

This repository currently contains only the design and test specifications under `specs/`. The goal is to implement **TreeDB** end‑to‑end per `specs/spec.md` and validate it per `specs/test-spec.md`.

---

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

**Spec/Test mismatch to resolve early:**  
`spec.md` says dual roots store user keys raw. `test-spec.md` section 1.7 expects an internal key encoding/prefix (`0x01|userKey`) that is stripped on public iteration. Decide the canonical behavior up front and align implementation + tests accordingly (likely follow tests for compatibility unless clarified).

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
  - page flags/types (Meta/Freelist/Internal/Leaf).

### Phase 2 — Pager (`index.db`) with Chunked MMap

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
  - `[ChildPageID][KeyBytes]` in heap; directory offsets.

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
  - pruner moves pages to on‑disk freelist only if
    - `RetiredAtSeq < MinPinnedSeq` and
    - `RetiredAtSeq < CurrentSeq - KeepRecent`.

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

---

## Testing Plan (Aligned to `specs/test-spec.md`)

### Unit Tests

1. **Pager (Chunked MMap)**:
   - boundary‑crossing reads with tiny chunk sizes.
   - growth safety while readers pin old chunks.
   - negative shrink test (must fail/panic safely).
   - refcounted unmap safety.
   - durability ordering: `WriteSync` fdatasync slab before msync/fsync index.
2. **Slotted Pages & Cursor Stack**:
   - walk test: 10k inserts, forward and reverse counts match.
   - split logic & parent wiring invariants.
   - cursor stack depth correctness and drill‑down in `Next/Prev`.
   - defragmentation reuses freed heap space.
3. **Graveyard / Epoch Safety**:
   - hold/release and prune movement to freelist.
   - reachability barrier under open iterators + aggressive prune/commit.
   - `MinPinnedSeq` advancement.
   - `KeepRecent` history window behavior.
4. **Slab Record Format**:
   - round‑trip with CRC verification.
   - ValuePtr alignment/size (16B) and length precision.
   - corrupt record CRC detection.
   - sequential enumeration over variable‑length records.
5. **Superblocks / Recovery**:
   - dual meta selection with one/both CRC corrupted.
   - `ActiveSlabTail` truncation on open.
   - monotonic `CommitSeq` across restarts.
6. **Adaptive Inline Threshold**:
   - latch semantics per commit.
   - fanout preservation IAVL simulation.
   - bounded‑step + hard‑bound enforcement.
   - evaluation frequency/low‑overhead counters.
7. **Internal Metadata Keyspace**:
   - namespace isolation / internal encoding expectations.
   - slab stats key persistence and decoding.
   - meta page overflow prevention with thousands of slabs.

### Integration & Functional Tests

- CRUD cycles, overwrites, persistence across reopen.
- Input validation (nil keys/values, empty keys ok).
- Forward/reverse iterator semantics incl. end‑exclusive and nil bounds.
- Concurrent iteration snapshot stability under commits + pruning.
- Batch atomicity (panic mid‑merge yields no partial commit), state machine errors, large values memory safety.
- `NewBatchWithSize` correctness and hint effectiveness (if instrumented).
- Adaptive threshold mixed workload convergence and hard max enforcement.
- Manual compaction blocking behavior and dead‑byte reduction.

### Property‑Based / Fuzz Tests

- Model‑based randomized ops against Go map oracle.
- Open iterators across random commits + pruning to assert reachability invariant.
- Key/value fuzzing with arbitrary bytes and huge keys.
- File corruption fuzzing:
  - flip bits in `index.db`, slabs, and meta pages.
  - expect CRC errors / safe panics, never silent corruption.

### Failure‑Injection / Crash‑Recovery

- Kill‑test driver:
  - separate writer process; random `kill -9`.
  - on restart DB must be at state N or N‑1; no traversal corruption.
- IO failure simulations via mocks:
  - `ENOSPC`, read‑only mount, directory‑sync orphan slab cleanup.
- Torn‑write scenarios:
  - slab durable but meta not committed.
  - torn meta page selection.
  - torn index page CRC detection.

### Concurrency & Race

- Run full suite under `go test -race`.
- Reader/writer duel monotonicity.
- Zombie slab life‑support during iteration then deletion after close.
- Resurrection verify‑set race (user update wins).
- Torn compaction recovery.
- Compaction serialization under writer lock.
- Compaction throttling latency isolation.
- Dead‑hint optimization metrics (if implemented).

### Cosmos Compliance

- Run upstream `cosmos-db` backend suite (`db/db_test.go`):
  - iterator, reverse iterator, batch tests.
- Pruning validation scenario (10k commits, `KeepRecent` policy).

### Commands

- Standard tests: `go test ./...`
- Race detection: `go test -race ./...`
- Fuzzing (Go 1.18+): `go test -fuzz=Fuzz -run=^$ ./...`
- Benchmarks: `go test -bench=. ./...`

---

## Performance Sprint TODOs

### Prereqs

- Step 6 complete (specs stable): `@PERF_06_COMPLETE` and `perf/spec-review.md`.
- Baseline benches (median of 5 runs): `perf/bench-20251211_233513.txt`.

### Compliance Goals (vs baseline medians)

All goals must pass with the same controls used for the baseline (`GOMAXPROCS=1`, `-count=5`, `-benchmem`).

- `BenchmarkSet150B/InlineThreshold=64` (baseline: 18049 ns/op, 39654 B/op, 51 allocs/op): allocs/op ≤ 25; B/op ≤ 20000; ns/op improves ≥ 20% (≤ 14440).
- `BenchmarkSet150B/InlineThreshold=256` (baseline: 16562 ns/op, 36766 B/op, 77 allocs/op): allocs/op ≤ 35; B/op ≤ 22000; ns/op improves ≥ 15% (≤ 14078).
- `BenchmarkBatchSet150B/InlineThreshold=64` (baseline: 14058 ns/op, 37939 B/op, 36 allocs/op): allocs/op ≤ 15; B/op ≤ 18000; ns/op improves ≥ 20% (≤ 11246).
- `BenchmarkBatchSet150B/InlineThreshold=256` (baseline: 17309 ns/op, 38862 B/op, 71 allocs/op): allocs/op ≤ 30; B/op ≤ 22000; ns/op improves ≥ 15% (≤ 14712).
- `BenchmarkGet150B/InlineThreshold=64` (baseline: 7403 ns/op, 25011 B/op, 209 allocs/op): allocs/op ≤ 40; B/op ≤ 6000; ns/op improves ≥ 25% (≤ 5552).
- `BenchmarkGet150B/InlineThreshold=256` (baseline: 6075 ns/op, 22915 B/op, 188 allocs/op): allocs/op ≤ 35; B/op ≤ 6000; ns/op improves ≥ 25% (≤ 4556).
- `BenchmarkIterScan/InlineThreshold=64` (baseline: 2125759 ns/op, 4286288 B/op, 51697 allocs/op): allocs/op ≤ 1000; B/op ≤ 512000; ns/op improves ≥ 25% (≤ 1594319).
- `BenchmarkIterScan/InlineThreshold=256` (baseline: 7924554 ns/op, 26227888 B/op, 108456 allocs/op): allocs/op ≤ 1000; B/op ≤ 512000; ns/op improves ≥ 25% (≤ 5943415).

### Ordered TODOs (dependency-aware)

1. [x] Pager: cache preallocated size (avoid redundant `Stat/Truncate`).
   - Goal: Reduce pager grow syscalls while preserving expand-only mmap + SIGBUS safety.
   - Touch: `internal/pager/pager.go`.
   - Correctness: add/extend tests for cached-size staleness (file smaller than cache) and chunk-alignment invariants; run `go test ./...` and `go test -race ./...`.
   - Perf: rerun `BenchmarkSet150B` + `BenchmarkBatchSet150B`; expect `ns/op` ↓ (both thresholds), `allocs/op` ↔/↓.

2. [x] Commit-level index growth estimate (guesstimate once per commit).
   - Goal: Reduce incremental grow calls by estimating needed pages per commit (bounded to ≤2–4 chunks).
   - Touch: `commit.go`, `internal/pager/pager.go`.
   - Correctness: add tests for estimate under-shoot fallback and “cap” enforcement; run `go test ./...`.
   - Perf: rerun `BenchmarkBatchSet150B`; expect `ns/op` ↓ (both thresholds).

3. [x] Pager freelist pop/append in place (no slice materialization).
   - Goal: Eliminate freelist decode allocations during `AllocPage`/`FreePages`.
   - Touch: `internal/pager/freelist.go`.
   - Correctness: add chain/partial-page cases + CRC mutation checks; run `go test ./...`.
   - Perf: rerun `BenchmarkSet150B` + `BenchmarkBatchSet150B`; expect `allocs/op` ↓ and `B/op` ↓.

4. [x] Pager: add pinned read views (`ReadPageRef`/`PageRef`) with strict release discipline.
   - Goal: Remove PageSize copies on internal reads while keeping CRC verification and preventing escaping views.
   - Touch: `internal/pager/pager.go` (+ new `PageRef` type), callers in `internal/tree/*` and `iterator.go`.
   - Correctness: add tests for growth safety while refs are pinned, ref-leak detection, and `Close()` behavior with live refs; run `go test ./...` and `go test -race ./...`.
   - Perf: rerun all four benches; expect large `allocs/op` ↓ and `B/op` ↓ (especially `BenchmarkIterScan`).

5. [x] Pager: add `WithMutablePage` (new-pages-only) and opt-in skip-zeroing for file-extended pages.
   - Goal: Write freshly allocated pages directly into mmap (no `make([]byte, PageSize)` clones) while preserving snapshot/COW invariants.
   - Touch: `internal/pager/pager.go` (mutable pin API), `internal/page/*` init paths.
   - Correctness: add tests enforcing “new pages only” usage, CRC verify-before-clone, and CRC recompute on commit; run `go test ./...`.
   - Perf: rerun `BenchmarkSet150B` + `BenchmarkBatchSet150B`; expect `allocs/op` ↓, `B/op` ↓, `ns/op` ↓.

6. [x] Tree COW fast paths using pager views + mutable pages.
   - Goal: Make `cowSet*` clone/modify pages without extra PageSize allocations.
   - Depends on: TODO 4–5.
   - Touch: `internal/tree/ops.go`, `internal/tree/parse.go`.
   - Correctness: extend snapshot reachability invariants + ensure old pages are never mutated; run `go test ./...` and `go test -race ./...`.
   - Perf: rerun `BenchmarkSet150B` + `BenchmarkBatchSet150B`; expect `allocs/op` ↓ and `ns/op` ↓ (primary win).

7. [x] Tree: zero-alloc `searchView` for point reads + internal node search helper (no key copies).
   - Goal: Make `GetRaw` descend via pinned views with CRC verification and no per-level key slice allocations.
   - Depends on: TODO 4.
   - Touch: `internal/tree/tree.go`, `internal/tree/parse.go`, `internal/page/internal.go`.
   - Correctness: add tests for exact-boundary keys, deep trees, and “no escaping views”; run `go test ./...`.
   - Perf: rerun `BenchmarkGet150B`; expect `allocs/op` ↓↓↓ and `ns/op` ↓.

8. [x] Iterator: zero-copy scan via pinned views + view-based decoding; copy only in `Key()`/`Value()`.
   - Goal: Remove per-entry allocations from scans while keeping CRC checks, tombstone skipping, and public copy semantics.
   - Depends on: TODO 4.
   - Touch: `iterator.go`, `internal/page/leaf.go`, `internal/page/internal.go`.
   - Correctness: add tests for ref release on stack pop + `Close()`, and concurrent commits while iterating; run `go test ./...` and `go test -race ./...`.
   - Perf: rerun `BenchmarkIterScan`; expect `allocs/op` ↓↓↓ and `ns/op` ↓ (both thresholds).

9. [x] Slab: low-risk `AppendLarge` optimizations (`pwritev` + pooled header; byte-identical format).
   - Goal: Reduce syscall+alloc overhead for out-of-line writes without changing the `[CRC32C][KeyLen][ValueLen][Key][Value]` bytes or CRC coverage.
   - Touch: `internal/slab/manager.go`, `internal/slab/record.go`.
   - Correctness: add byte-exact encoding regression and fallback-path tests; run `go test ./...`.
   - Perf: rerun `BenchmarkSet150B/InlineThreshold=64` + `BenchmarkBatchSet150B/InlineThreshold=64`; expect `ns/op` ↓.

10. [x] Slab: bounded buffered sequential writer (`O_APPEND`) with flush-before-publish invariants.
   - Goal: Amortize slab write syscalls while ensuring commits never publish pointers to unwritten bytes.
   - Depends on: TODO 9.
   - Touch: `internal/slab/manager.go`, `commit.go`.
   - Correctness: add crash/reopen tail correctness tests and durability-ordering assertions (`*Sync`: slab durable before index/meta); run `go test ./...` and `go test -race ./...`.
   - Perf: rerun `BenchmarkBatchSet150B/InlineThreshold=64`; expect `ns/op` ↓ and `allocs/op` ↔/↓.

11. [x] Pager: optional stop-zeroing newly extended pages (keep default safe + deterministic in tests).
   - Goal: Make page zeroing policy configurable and preserve “no stale bytes under CRC” guarantees.
   - Touch: `internal/pager/pager.go`, `internal/page/*`.
   - Correctness: add tests that reused freelist pages are fully initialized/overwritten before publish; run `go test ./...`.
   - Perf: rerun `BenchmarkSet150B` + `BenchmarkBatchSet150B`; expect small `ns/op` ↓.

12. [ ] Instrumentation + re-profiling + adaptive stats exposure.
   - Goal: Keep perf counters low-overhead/race-safe, expose missing stats keys, and refresh CPU+mem profiles after landing TODOs 1–11.
   - Touch: `internal/adaptive/controller.go` (export `treedb.inline_threshold.index_write_bytes`), any commit-level counters in `commit.go`, perf notes under `perf/`.
   - Correctness: run `go test ./...`, `go test -race ./...`.
   - Perf: rerun all four benches with `-benchmem -count=5`, regenerate CPU+mem profiles for `BenchmarkGet150B` and `BenchmarkIterScan`; expect profiles shift away from `runtime.mallocgc`/page copies and toward actual compare/CRC work.
