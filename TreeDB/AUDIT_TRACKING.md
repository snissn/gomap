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
- Status: IN_PROGRESS
- Severity: P0
- Evidence:
  - `TreeDB/freelist/allocator.go`: common pop path decrements count and updates checksum without rewriting body. The region-locality path does encode the body.
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
  - `TreeDB/db/api.go`: `DB.Set`/`Delete` call `SetView`/`DeleteView`.
  - `TreeDB/batch/batch.go`: `SetView` and `DeleteView` store caller slices directly.
  - `TreeDB/caching/db.go`: cached batch stores slices by reference and feeds `SetSteal` to memtables.
  - `TreeDB/internal/memtable/hash_sorted.go`: `SetSteal` stores slices directly in map entries.
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
  - `TreeDB/db/api.go`: `GetUnsafe` acquires snapshot then closes it before returning the view.
  - `TreeDB/slab/manager.go`: zombie slabs are closed when snapshot refcount reaches 0.
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
- Status: OPEN
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

### AUD-007: Index swap rename lacks directory fsync
- Status: OPEN
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

### AUD-008: Background flush without sync when WAL disabled
- Status: OPEN
- Severity: P1
- Evidence:
  - `TreeDB/caching/db.go`: background flush uses `sync=false`.
  - `TreeDB/profiles.go`: ProfileFast disables WAL and relaxes sync.
- Risk:
  - Operations appear successful but are not durable in WAL-disabled mode.
- Investigation:
  1) Add crash test with WAL disabled and background flush.
  2) Document required durability semantics.
- Fix options:
  - Force sync in some code paths or gate background flush in unsafe modes.
  - Label ProfileFast as unsafe in API docs and README.
- Acceptance criteria:
  - Tests clarify behavior; docs are explicit about non-durability.

### AUD-009: Corruption-induced panics in leaf search
- Status: OPEN
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

---

## Operational Security & Robustness

### AUD-010: File permissions too permissive
- Status: OPEN
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

### AUD-011: Background error visibility
- Status: OPEN
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

### AUD-012: VM map count exhaustion
- Status: OPEN
- Severity: P2
- Evidence:
  - `TreeDB/slab/slab.go`: `deadMappings` retained until Close.
- Risk:
  - Long-running processes can exhaust `vm.max_map_count`.
- Investigation:
  1) Add a stress test that grows slab file and tracks mapping count.
- Fix options:
  - Bound `deadMappings` by age or count.
  - Add scoped unsafe reads that explicitly pin and release mappings.
- Acceptance criteria:
  - Mapping count plateaus under sustained growth.

### AUD-013: OOM on corrupted slab headers
- Status: OPEN
- Severity: P1/P2 (robustness)
- Evidence:
  - `TreeDB/slab/slab.go`: fallback alloc uses `keyLen+valLen` without a hard cap.
- Risk:
  - Corrupt headers can trigger massive allocations and OOM.
- Investigation:
  1) Fuzz `Read` with corrupted headers and observe memory usage.
- Fix tasks:
  - Add a max record size cap (configurable) and return `ErrRecordTooLarge` if exceeded.
- Acceptance criteria:
  - Fuzz test avoids OOM and returns errors on oversized records.

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
- Status: OPEN
- Severity: P2 (performance)
- Evidence:
  - `TreeDB/db/batch.go`: `writeMu` serializes writers.
  - `TreeDB/caching/db.go`: write paths use `writeMu`.
- Risk:
  - Multi-writer workloads do not scale with cores.
- Investigation:
  1) Benchmark multi-writer ingestion; capture CPU contention profile.
- Fix options:
  - Shard memtables or add concurrent index updates.
  - Consider a concurrent skiplist or partitioned writer queues.

### AUD-016: Compaction per-record lookup overhead
- Status: OPEN
- Severity: P2
- Evidence:
  - `TreeDB/compaction/compactor.go`: liveness check per record uses tree lookup.
- Risk:
  - High CPU during compaction on large slabs.
- Investigation:
  1) Profile compaction CPU time; capture hot functions.
- Fix options:
  - Bloom filters, prefix hints, or cursor-based traversal.

### AUD-017: Freelist churn / fragmentation
- Status: OPEN
- Severity: P2
- Evidence:
  - Basic freelist allocator; relies on vacuum for compaction.
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
| AUD-001 | IN_PROGRESS |  |  | Pop path now rewrites body; tests pending. |
| AUD-002 | FIXED |  |  | Safe default copies; mutation tests added. |
| AUD-003 | NOT_APPLICABLE |  |  |  |
| AUD-004 | FIXED |  |  | GetUnsafe returns safe copy; tests updated. |
| AUD-005 | FIXED |  |  | Iterators hold read lock; concurrency test added. |
| AUD-006 | OPEN |  |  |  |
| AUD-007 | OPEN |  |  |  |
| AUD-008 | OPEN |  |  |  |
| AUD-009 | OPEN |  |  |  |
| AUD-010 | OPEN |  |  |  |
| AUD-011 | OPEN |  |  |  |
| AUD-012 | OPEN |  |  |  |
| AUD-013 | OPEN |  |  |  |
| AUD-014 | NOT_APPLICABLE |  |  |  |
| AUD-015 | OPEN |  |  |  |
| AUD-016 | OPEN |  |  |  |
| AUD-017 | OPEN |  |  |  |
