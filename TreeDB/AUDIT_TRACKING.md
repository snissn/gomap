# TreeDB Audit Tracking (2026)

Purpose: Track each audit item with current applicability, code evidence, investigation plan, and resolution checklist. This file is designed so a future agent can proceed without extra context.

Status legend:
- OPEN: not yet validated or fixed
- IN_PROGRESS: investigation or fix underway
- MITIGATED: partially addressed, needs confirmation
- FIXED: implemented and verified by tests
- NOT_APPLICABLE: audit claim no longer applies (document why)

Owner and dates are optional; add them if you use a team workflow.

---

## P0 / P1 Data Integrity and Safety

### AUD-001: Freelist pop does not encode body (double allocation risk)
- Status: FIXED
- Severity: P0
- Evidence:
  - `TreeDB/freelist/allocator.go`: pop path encodes body and clears popped slot.
  - `TreeDB/freelist/allocator_body_test.go`: verifies slot clearing after alloc.
  - `TreeDB/freelist/allocator_invariant_test.go`: no duplicate allocations after frees.
  - `TreeDB/specs/spec.md`: freelist Count invariant documented.
- Risk:
  - If any decode path relies on stale tail bytes or ignores Count, pages can be reissued. Even if Count is authoritative, this is brittle and future changes could violate the invariant.
- Investigation:
  1) Confirm all freelist decodes use `Count` to bound the body. Search for `DecodeFreelistBody` usages.
  2) Add a property test: for random alloc/free cycles, a page is never reissued unless it was freed.
  3) Verify checksum coverage behavior: does checksum include stale body bytes?
- Fix options:
  - Option A: Always swap/pop and encode body bytes on pop.
  - Option B: Zero popped slot and update checksum; codify invariant in spec/tests.
- Acceptance criteria:
  - Test that proves no double allocation.
  - Explicit invariant documented in `specs/spec.md` or code comments.
  - Confirmed pop path writes body updates (implemented) and regression test added.

### AUD-002: API no-copy hazards (Set/Delete and memtables)
- Status: FIXED
- Severity: P0
- Evidence:
  - `TreeDB/db/api.go`: `DB.Set`/`Delete` use `batch.Set`/`Delete` (copying inputs).
  - `TreeDB/batch/batch.go`: `Set`/`Delete` copy key/value bytes; `SetView`/`DeleteView` are explicit unsafe views.
  - `TreeDB/caching/db.go`: cached `Batch.Set` copies inputs; `SetView` is only used with owned copies.
  - `TreeDB/internal/memtable/hash_sorted.go`: `SetSteal` stores slices; callers now pass owned copies.
- Risk:
  - Common user pattern (reused buffers) can corrupt pending writes and WAL/memtable state.
- Investigation:
  1) Add tests that mutate caller buffers after `Set`/`Delete` and confirm corruption reproduces.
  2) Inventory all view/unsafe methods and document lifetime guarantees.
- Fix tasks:
  - Change public API defaults to copy (`Set`/`Delete`).
  - Introduce explicit `SetView`/`DeleteView` (or `UnsafeSet`) with strong docs.
  - Update caching/db paths to only use view methods when lifetime is guaranteed (e.g., batch owns copies).
- Acceptance criteria:
  - New tests demonstrate safe default behavior.
  - View methods are clearly documented and only used in safe contexts.
  - Cached batch Set/Delete copy inputs and tests cover mutation-after-Set.

### AUD-003: Compaction pointer race (CAS)
- Status: NOT_APPLICABLE
- Severity: P0 (historical)
- Evidence:
  - `TreeDB/db/db.go` `ApplyCompactionMicroBatches`: re-reads entry and checks `ValuePtr == OldPtr` before update; protected by `writeMu`.
  - `TreeDB/db/compaction_apply_test.go`: `TestApplyCompactionMicroBatches_SkipsStalePointer`.
- Risk:
  - The original check-then-act race is mitigated by CAS-like logic.
- Validation tasks:
  - Add a concurrent writer test to prove the CAS skip behavior is correct.
- Acceptance criteria:
  - Test passes and doc mentions compare-before-update semantics.

---

## P1 Stability, Crash Safety, Concurrency

### AUD-004: Use-after-unmap from `GetUnsafe`
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/db/api.go`: `GetUnsafe` returns a safe copy (delegates to `Get`).
  - `TreeDB/api_alloc_test.go`: `TestGetUnsafe_BackendReturnsValue` and `TestGetUnsafe_CachedReturnsCopy`.
- Risk:
  - Returned slices can point to mmap pages unmapped after snapshot release; potential SIGSEGV.
- Investigation:
  1) Write a repro: call `GetUnsafe`, trigger compaction/zombie cleanup, then touch the slice.
  2) Confirm whether index ghosting applies to slab mappings (not just index pages).
- Fix options:
  - Return a handle that pins a snapshot or slab mapping and requires Close.
  - Remove or restrict `GetUnsafe` to iterators with scoped lifetime.
- Acceptance criteria:
  - Deterministic test or benchmark shows safety or required API changes.
  - DB-level GetUnsafe returns safe copies; Snapshot.GetUnsafe remains view-scoped.

### AUD-005: Memtable iterator race
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/internal/memtable/memtable.go`: `NewIterator` releases lock before returning iterator.
- Risk:
  - Concurrent writes can mutate skiplist while iterating, causing panic or data race.
- Investigation:
  1) Add a concurrent test: writer goroutine + iterator goroutine (run with `-race`).
  2) Confirm if cached DB iteration always uses frozen memtables; memtable API still unsafe elsewhere.
- Fix options:
  - Hold `RLock` for iterator lifetime, or snapshot copy for iteration.
  - Provide explicit `NewIteratorUnsafe` with lifetime constraints.
- Acceptance criteria:
  - `-race` clean and no panics in concurrent iteration test.
  - Iterator holds read lock until Close; regression test verifies writer blocking.

### AUD-006: WAL rotation missing fsync
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/internal/wal/wal.go`: `RotateTo` flushes buffers but does not call `Sync()` before close.
- Risk:
  - WAL tail can be lost on power failure after rotation.
- Investigation:
  1) Add crash-recovery test around rotate boundaries.
- Fix tasks:
  - Add `f.Sync()` before close or expose a durability contract (and test it).
- Acceptance criteria:
  - Crash-recovery test proves WAL durability across rotation.
  - RotateTo sync call verified by unit test.

### AUD-007: Index swap rename lacks directory fsync
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/db/index_swap.go`: rename operations without directory fsync.
- Risk:
  - Rename not durable after crash on POSIX filesystems.
- Investigation:
  1) Add crash-point tests for vacuum swap + recovery.
- Fix tasks:
  - `fsync` parent directory after rename and after removing ready marker.
- Acceptance criteria:
  - Crash tests confirm recovery works after simulated power loss.
  - Tests verify directory sync is invoked after rename paths.

### AUD-008: Background flush without sync when WAL disabled
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/caching/db.go`: background flush upgrades to synced writes when WAL is disabled (unless RelaxedSync).
  - `TreeDB/caching/db.go`: `syncBarrierAfterWrite` enforces a backend boundary for `SetSync`/`DeleteSync`/`Batch.WriteSync` when `DisableWAL=1`:
    - strict sync (`RelaxedSync=0`): forces `Checkpoint()` (durable boundary)
    - relaxed sync (`RelaxedSync=1`): forces flush-to-backend without fsync
  - `TreeDB/caching/db_test.go`: `TestCachingDB_FlushSyncsWhenWALDisabled`.
  - `TreeDB/recovery_spec_test.go`: `TestCrashRecovery_DurabilityTiers` (includes WAL-disabled strict sync and WAL-enabled relaxed/strict tiers, plus a value-log pointer replay case).
- Risk:
  - Operations appear successful but are not durable in WAL-disabled mode.
- Investigation:
  1) Add crash test with WAL disabled and background flush.
  2) Document required durability semantics.
- Fix options:
  - Force sync in some code paths or gate background flush in unsafe modes.
  - Label ProfileFast as unsafe in API docs and README.
- Acceptance criteria:
  - Background flush syncs when WAL is disabled (unit test).
  - `*Sync` APIs do not silently degrade when WAL is disabled (checkpoint/flush barrier enforced).
  - RelaxedSync retains opt-out behavior for power-loss durability (flush-to-kernel semantics).

### AUD-009: Corruption-induced panics in leaf search
- Status: FIXED
- Severity: P1
- Evidence:
  - `TreeDB/node/leaf.go`: `SearchLeaf` reads offsets without bounds checks.
- Risk:
  - Disk corruption can panic the process instead of returning a typed error.
- Investigation:
  1) Add a test that corrupts leaf offsets and expects `ErrCorruptedNode`.
- Fix tasks:
  - Add bounds checks for offset, keyLen, and payload bounds; return `ErrCorruptedNode`.
- Acceptance criteria:
  - Corruption tests pass and no panics under fuzz.
  - SearchLeaf returns ErrCorruptedNode on invalid offsets; tests added.

---

## Operational Security & Robustness

### AUD-010: File permissions too permissive
- Status: FIXED
- Severity: P2 (security)
- Evidence:
  - `TreeDB/pager/pager.go`: index file 0644
  - `TreeDB/slab/slab.go`: slab file 0644
  - `TreeDB/internal/lockfile/lockfile.go`: lock file 0644
  - `TreeDB/caching/db.go`: WAL dir 0755
- Risk:
  - Other users can read DB contents on multi-user systems.
- Investigation:
  1) Audit all file/dir creation calls and ensure consistent modes.
- Fix tasks:
  - Change defaults to 0600 (files) and 0700 (dirs).
  - Add option to override for explicit shared-read deployments.
- Acceptance criteria:
  - Tests or manual checks confirm modes.
  - New test asserts owner-only permissions on index/slab/lock and wal dir.

### AUD-011: Background error visibility
- Status: FIXED
- Severity: P2
- Evidence:
  - `TreeDB/caching/db.go`: errors logged to stderr and discarded.
- Risk:
  - Persistent background failures are invisible; user sees stale data or growing debt.
- Investigation:
  1) Inventory all stderr logging in background routines.
  2) Decide on error handling policy (callback, error queue, `Close()` aggregation).
- Fix tasks:
  - Add `NotifyError(func(error))` option.
  - Store error state and return on `Close()` or via health check.
- Acceptance criteria:
  - Errors surfaced to user code and tests validate callback is invoked.
  - Close returns stored background error when no other errors are present.

### AUD-012: VM map count exhaustion
- Status: FIXED
- Severity: P2
- Evidence:
  - `TreeDB/slab/slab.go`: `MaxDeadMappings` cap prevents unbounded remap retention.
  - `TreeDB/slab/slab_test.go`: `TestSlabRemap_CapsDeadMappings`.
- Risk:
  - Long-running processes can exhaust `vm.max_map_count`.
- Investigation:
  1) Add a stress test that grows slab file and tracks mapping count.
- Fix options:
  - Bound `deadMappings` by age or count.
  - Add scoped unsafe reads that explicitly pin and release mappings.
- Acceptance criteria:
  - Dead mappings are capped and remaps are suppressed once the cap is hit.

### AUD-013: OOM on corrupted slab headers
- Status: FIXED
- Severity: P1/P2 (robustness)
- Evidence:
  - `TreeDB/slab/slab.go`: `MaxRecordSize` cap enforced in mmap and pread read paths; write paths reject oversized records.
  - `TreeDB/slab/slab_test.go`: `TestSlabRead_RejectsOversizedHeader`.
- Risk:
  - Corrupt headers can trigger massive allocations and OOM.
- Investigation:
  1) Fuzz `Read` with corrupted headers and observe memory usage.
- Fix tasks:
  - Add a max record size cap (configurable) and return `ErrRecordTooLarge` if exceeded.
- Acceptance criteria:
  - Oversized headers are rejected without allocating (unit test).
  - Read and write paths honor the cap and return `ErrRecordTooLarge`.

### AUD-014: Endianness and unsafe casts
- Status: NOT_APPLICABLE (latent)
- Severity: P3
- Evidence:
  - `TreeDB/page/page.go`: `UnsafeCastHeader` assumes native endianness; appears unused in production.
- Risk:
  - Big-endian systems would misinterpret data if used.
- Action:
  - Keep it unused or guard with runtime check/build tag.

---

## Performance & Scalability

### AUD-015: Global write lock serialization
- Status: FIXED
- Severity: P2 (performance)
- Evidence:
  - `TreeDB/db/batch.go`: optimistic write path uses `writeMu.RLock` + `commitMu` with CAS on `UserRootPageID`.
  - `TreeDB/db/alloc_tracker.go`: tracks allocated pages and frees on conflicts.
  - `TreeDB/zipper/zipper.go`: `CloneWithAllocator` preserves zipper config with tracked allocations.
  - `TreeDB/db/concurrent_write_test.go`: concurrent writer correctness test.
  - `TreeDB/caching/db.go`: cached writes lock only `memShard.mu` for memtable mutation (plus a `writeMu.RLock` checkpoint barrier).
  - `TreeDB/caching/db_test.go`: `TestCachingDB_SetDoesNotBlockOnWriteMuRLock`.
  - `docs/TREEDB_TUNING.md`: write concurrency limits and mitigations documented.
  - `TreeDB/bench_test.go`: `BenchmarkWriteParallelCached` and `BenchmarkWriteParallelCachedRotationStress`.
- Risk:
  - Multi-writer workloads do not scale with cores.
- Resolution:
  - Backend writes use optimistic apply + CAS commit with bounded retries and a serialized fallback.
  - Allocation tracking frees pages on conflicts to avoid index growth from abandoned attempts.
  - Concurrent writer test validates no lost updates.
  - Cached writes no longer serialize on `db.mu` for memtable updates; remaining contention is tracked in the cached write concurrency milestone below (WAL and rotation/flush paths).
- Investigation:
  1) Benchmark multi-writer ingestion; capture CPU contention profile.
- Fix options:
  - Shard memtables or add concurrent index updates.
  - Consider a concurrent skiplist or partitioned writer queues.

### AUD-016: Compaction per-record lookup overhead
- Status: FIXED
- Severity: P2
- Evidence:
  - `TreeDB/compaction/compactor.go`: compaction builds a live pointer set and skips per-record tree lookups.
  - `TreeDB/compaction/compaction_test.go`: `TestCompaction_LiveSetSkipsTreeLookups`.
- Risk:
  - High CPU during compaction on large slabs.
- Investigation:
  1) Profile compaction CPU time; capture hot functions.
- Fix options:
  - Bloom filters, prefix hints, or cursor-based traversal.
  - Live pointer set for the target slab.
  - Stats counters to verify skipped lookups.

### AUD-017: Freelist churn / fragmentation
- Status: FIXED
- Severity: P2
- Evidence:
  - `TreeDB/db/db.go`: default freelist region bias enabled when reuse is active.
  - `TreeDB/db/freelist_region_defaults_test.go`: defaults and disable behavior tests.
  - Background index vacuum uses fragmentation span ratio to trigger rebuilds (default on).
  - Allocator locality knobs: `PreferAppendAlloc`, `FreelistRegionPages`, `FreelistRegionRadius`.
  - `FragmentationReport()` exposes span ratio + fill stats; tests cover churn/locality.
- Risk:
  - Long-term fragmentation degrades tree locality and performance.
- Investigation:
  1) Track fragmentation stats over churn; measure locality regression.
- Fix options:
  - Add allocation locality bias (already partially present), or automatic vacuum triggers.

---

## Cross-cutting tasks

### Testing matrix
- Add or update tests:
  - Concurrency: memtable iteration vs writes (`-race`).
  - Corruption: leaf offset bounds, slab header fuzz.
  - Recovery: WAL rotation, index swap rename, WAL-disabled flush.
  - API safety: mutate buffers after `Set`/`Delete` and verify data.

### Documentation updates
- Clarify safe vs unsafe APIs in README and public docs.
- Label ProfileFast / DisableWAL as unsafe and non-durable.
- Document freelist invariants (Count vs tail bytes).

### Telemetry and health
- Add metrics for background error count, zombie slabs, mapping count, and flush debt.

---

## Tracking table (fill as you go)

| ID | Status | Owner | Target | Notes |
|---|---|---|---|---|
| AUD-001 | FIXED |  |  | Pop path encodes body; tests + spec invariant added. |
| AUD-002 | FIXED |  |  | Safe default copies; mutation tests added. |
| AUD-003 | NOT_APPLICABLE |  |  | CAS skip test added. |
| AUD-004 | FIXED |  |  | GetUnsafe returns safe copy; tests updated. |
| AUD-005 | FIXED |  |  | Iterators hold read lock; concurrency test added. |
| AUD-006 | FIXED |  |  | RotateTo now syncs; test added. |
| AUD-007 | FIXED |  |  | recoverIndexSwap syncs parent dir; tests added. |
| AUD-008 | FIXED |  |  | WAL-disabled flushes sync unless RelaxedSync; `*Sync` barrier + crash-tier tests added. |
| AUD-009 | FIXED |  |  | SearchLeaf bounds checks + corruption test. |
| AUD-010 | FIXED |  |  | Default perms tightened; test added. |
| AUD-011 | FIXED |  |  | NotifyError hook + Close reports background errors. |
| AUD-012 | FIXED |  |  | Dead mapping cap enforced; remap suppression test added. |
| AUD-013 | FIXED |  |  | MaxRecordSize cap enforced + oversized header test. |
| AUD-014 | NOT_APPLICABLE |  |  |  |
| AUD-015 | MITIGATED |  |  | Backend uses optimistic apply + CAS commit; cached layer still contends on db.mu. |
| AUD-016 | FIXED |  |  | Live-set optimization + lookup skip test. |
| AUD-017 | FIXED |  |  | Default region bias enabled + tests. |

---

## Milestone: Cached Write Concurrency (Post-Audit)

Goal: remove `db.mu` write serialization in cached mode while preserving WAL ordering, snapshot isolation, and flush correctness.

Key findings:
- `BenchmarkReadUnderWriteCached/W=4` profiles show dominant contention in `caching.(*DB).set` on `db.mu.Lock`/`Unlock`.
- Reads (`getMemtable`) take `db.mu.RLock`; writers block them during memtable updates.

Proposed approach (phased, inspired by earlier RCU-style work where it is a clear win):
Phase 0: Reduce read-path contention without changing write semantics.
1) RCU snapshot for memtables:
   - Add `memtableView{mutable, queue, queueRanges}` stored in an `atomic.Pointer`.
   - Publish a new snapshot on any queue mutation or mutable rotation (flush, DeleteRange drops, rotate).
   - `getMemtable`/`Has` load the snapshot and never take `db.mu`; no per-read slice copies.
2) Iterator/writeBypass snapshots:
   - `Iterator` and `Batch.writeBypass` use the published snapshot for queue slices to avoid per-call copies.
3) Safety fix with clear concurrency benefit:
   - Copy keys in `noteWriteKeyLocked` to avoid retaining caller slices in adaptive stats.
4) Tests:
   - Snapshot update + immutability tests for queue/queueRanges.
   - Allocation tests for `Has`/`Iterator`/`writeBypass` with queued memtables.
   - Race test that `Has` does not block while `db.mu` is held (lock-free read path).

Phase 0 status:
- Implemented RCU memtable snapshots and lock-free `getMemtable`/`Has` reads (`TreeDB/caching/db.go`).
- Iterator and `writeBypass` now use the published snapshot to avoid per-call queue copies (`TreeDB/caching/db.go`).
- Added tests for snapshot updates, immutability, and allocation behavior (`TreeDB/caching/rcu_snapshot_test.go`).

Phase 1 status:
- Sharded mutables remove `db.mu` from the cached write hot path (`TreeDB/caching/db.go`).
- WAL append batching reduces contention under high writer counts (`TreeDB/caching/db.go`).
- Memtable rotations lock all shards until publish (avoids the global `writeMu` barrier while preserving snapshot correctness) (`TreeDB/caching/db.go`).
- Added `BenchmarkWriteParallelCachedRotationStress` to amplify rotation overhead in benchmarks (`TreeDB/bench_test.go`).

Phase 1: Memtable sharding (true write concurrency).
1) Data structures + config:
   - Add `memShard` struct: `mu`, `mt memtable.Interface`, `rng keyRange`, `bytes atomic.Int64`.
   - Add `mutableShards []memShard` to caching DB; choose shard count (power-of-two) with a default (e.g., `min(8, GOMAXPROCS*2)`), and keep a `shardMask`.
   - Add `mutableBytes atomic.Int64` to track total mutable size; remove reliance on `db.mutable.Size()` in hot paths.
2) Write path changes (`set`/`delete`):
   - Compute shard index from key hash (reuse existing hash helper or add a fast FNV32).
   - Keep `walMu` for WAL append as-is; then lock only the target shard to call `mt.Set/Delete`.
   - Update shard range + shard bytes; atomically add to `mutableBytes`.
   - Keep `db.mu` only for queue metadata or rotate state, not for the memtable mutation.
3) Read path changes (`getMemtable`):
   - Check the key’s shard first under shard RLock.
   - Use the RCU snapshot for queued memtables and iterate outside `db.mu`.
   - Keep semantics: newest-first over queue, then backend.
4) Rotation + flush:
   - Lock all shards and hold them until `publishMemtablesLocked()` so readers never miss writes to newly-swapped memtables.
   - Avoid rotating WAL segments in the hot path; checkpoints establish durable boundaries and trim segments.
5) Iterator + snapshots:
   - Snapshot should capture the current queue slice plus shard pointers (or force a rotation when creating iterators, as today).
   - Ensure iterator merges shard immutables + queue + backend in key order.
6) Tests + benchmarks:
   - Add a cached-layer concurrency test: multiple writers to distinct shards + reader loop, `-race` clean.
   - Update `BenchmarkWriteParallelCached` + `BenchmarkReadUnderWriteCached` as primary KPIs (already present).
   - Add a focused unit test to verify rotate + flush correctness with sharded mutables.

Guardrails (to avoid adopting unsafe older changes):
- Any operation that swaps shard memtables must hold shard locks until the new memtable view is published (to avoid reads missing concurrent writes).
- Keep `mutableBytes` updates under the same shard lock used for mutation, so iterator-triggered rotations never miss non-empty mutables due to transient accounting errors.
- Prefer the RCU snapshot for reads/queue access; it reduces contention without weakening rotation safety.

Acceptance criteria:
- `BenchmarkWriteParallelCached`: G=4 at least 1.5x throughput vs G=1.
- `BenchmarkReadUnderWriteCached`: W=4 p95 latency within +50% of W=0.
- Race tests pass; no lost updates; WAL ordering preserved; flush correctness intact.

---

## Milestone: WAL + Slab Dedup (Value Log)

Goal: eliminate cached-mode large-value “double write” (WAL write + slab write) by turning cached-mode WAL segments into a random-access **value log** whose records can be referenced directly by the backend index via `ValuePtr`.

Status: PLANNED (design review).

Design + phased TODO list:
- `TreeDB/specs/value-log.md`
