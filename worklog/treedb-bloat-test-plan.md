# TreeDB Bloat/Reuse: Test Plan + Tracker

Owner: @snissn (with Codex)
Branch: `test-bloat-regression`
PR: https://github.com/snissn/gomap/pull/231

Goal
- Add substantial, high-signal test coverage around prune/graveyard/freelist reuse.
- Keep a small number of intentional failing tests that represent real bugs (a "red bar" that matters).
- Make all other new tests deterministic and phase-aware (KeepRecent/pinning/head==0 gating).

Non-Goals
- Make everything green immediately. Some failures are desirable if they capture valid bugs.

Ground Rules
- Prefer allocator stats (`freelist.Stats`) over `FragmentationReport` for tests that need reclaimable counts.
  - `FragmentationReport` only includes reclaimable keys when `freelist.head != 0` (phase-dependent).
- CI intentional failures (target):
  - `TreeDB/caching/freelist_bloat_bench_test.go:TestCachedBenchBloatVacuum`
  - `TreeDB/db/freelist_integrity_test.go:TestFreelistCountsDecreaseAfterReuse`
- All other tests added in this plan should pass reliably.

---

## Current State (as of this file creation)

### Already implemented / pushed
- Allocator counters (`allocFromFreelist`, `allocFromAppend`) + `AllocCounters()`.
- `Stats()` exposure for alloc counters: `treedb.alloc.freelist`, `treedb.alloc.append`.
- `db.DB.FreelistStats()` API for tests/diagnostics (best-effort).
- Close-time meta persistence for freelist head/total pages (meta pages written on close).
- Cached `Checkpoint()` calls backend `Prune()` if supported.

### Known failing tests (expected)
- [ ] `TreeDB/caching/freelist_bloat_bench_test.go:TestCachedBenchBloatVacuum` (intentional red)
- [ ] `TreeDB/db/freelist_integrity_test.go:TestFreelistCountsDecreaseAfterReuse` (intentional red)

### Newly added probes
- `TreeDB/caching/flush_phase_trace_test.go` (timeline)
- `TreeDB/db/alloc_phase_trace_test.go` (timeline)
- `TreeDB/db/prune_phase_probe_test.go` (probe)
- `TreeDB/db/pinning_probe_test.go` (probe)

---

## Phase A: Make Probes Deterministic (PASS + Logs)

### A1) Cached timeline test uses allocator stats
- [x] `TreeDB/caching/flush_phase_trace_test.go` uses `backend.FreelistStats()` for reclaimables.
- [ ] Ensure this test has no strict assertions that depend on reuse working (should be logging-focused).

### A2) Backend timeline test uses allocator stats
- [x] `TreeDB/db/alloc_phase_trace_test.go` uses `idx.allocator.Stats()` for reclaimables/head.
- [ ] Ensure this test is logging-focused (no strict reuse assertions).

### A3) Convert prune probe into diagnostic (avoid asserting reuse until fixed)
- [ ] Update `TreeDB/db/prune_phase_probe_test.go`:
  - remove strict page reuse bounds (currently fails: pages write=698, rewrite=2086)
  - keep logs: pages, reclaimables, alloc counters, commit seq
  - only assert basic invariants that must always hold (e.g. allocator exists, stats calls succeed)

### A4) Pinning probe should pass
- [x] `TreeDB/db/pinning_probe_test.go` asserts `MinPinnedSeq()==MaxUint64` with no snapshots held.
- [ ] Add extra log lines: current commit seq, keepRecent, minPinned.

### A5) Focused probe test run
- [ ] Run:
  - `go test ./TreeDB/caching ./TreeDB/db -run 'TestCachedPhaseAllocTimeline|TestBackendPhaseAllocTimeline|TestPrunePhaseProbe|TestPruneNotBlockedByReadersWhenNoSnapshots' -count=1`
  - Expect all to PASS.

---

## Phase B: Add Passing Invariant Tests (Coverage)

### B1) FragmentationReport invariants when head != 0
- [ ] Add test: `TreeDB/db/fragmentation_validate_test.go:TestFragmentationReportFreelistKeysPresentWhenHeadNonZero`
  - Create churn + advance commit seq enough + prune until `allocator.Head()!=0`.
  - Assert `FragmentationReport()` includes freelist keys and passes `ValidateFragmentationReport`.

### B2) Allocator unit tests (freelist correctness independent of DB)
File: `TreeDB/freelist/allocator_test.go`
- [ ] `TestAllocator_FreeCreatesHead`
- [ ] `TestAllocator_AllocPrefersFreelistWhenAvailable`
- [ ] `TestAllocator_AllocManyUsesFreelistBeforeAppend`
- [ ] `TestAllocator_PreferAppendSkipsFreelist`

### B3) Graveyard extraction rules (KeepRecent correctness)
File: `TreeDB/lifecycle/graveyard_test.go` (create if missing)
- [ ] `TestGraveyard_KeepRecentDefersReclaim`
- [ ] `TestGraveyard_AdvancingSeqAllowsReclaim`

---

## Phase C: Keep Intentional Reds High-Signal

### C1) Bloat regression stays strict (intentional red)
File: `TreeDB/caching/freelist_bloat_bench_test.go`
- [ ] Enhance failure output (no behavior change):
  - log `backend.Stats()` and `backend.FreelistStats()` before close if possible
  - if not possible, reopen DB read-only for postmortem stats

### C2) Reclaimables decrease after reuse stays strict (intentional red)
File: `TreeDB/db/freelist_integrity_test.go`
- [ ] Ensure sequence is KeepRecent-aware:
  - write -> overwrite -> delete -> prune
  - rewrite -> rewrite -> prune
  - take reclaimable snapshot immediately before final rewrite and after
  - assert reclaimables strictly decrease

---

## Phase D: Start Fixes (After Probes + Coverage Are Green)

Order of attack:
1) Prune/graveyard eligibility (most likely root cause)
2) Cached Checkpoint/close actually triggers effective prune
3) Allocator reuse correctness

### D1) Align pruning “current seq” source
- [ ] Review `TreeDB/db/db.go:Prune()` vs `TreeDB/db/prune.go:pruneSome()`
  - `Prune()` uses `db.meta.CommitSeq` (racy / may be stale)
  - `pruneSome()` uses `db.state.Load().CommitSeq` (likely correct)
- [ ] Change `Prune()` to use `state := db.state.Load(); current := state.CommitSeq` (fall back safely).
- [ ] Add regression: a DB-only test that fails if `Prune()` is called after churn and frees nothing when it should.

### D2) Verify cached Checkpoint triggers prune when it matters
- [ ] Confirm `TreeDB/caching/db.go:Checkpoint()` calls backend prune at correct point (after flush).
- [ ] Add a caching test that asserts `backend.FreelistStats().ReclaimablePages()` becomes non-zero after enough churn+checkpoints.

### D3) Allocator consumes freelist
- [ ] If prune is freeing but allocations still append:
  - validate `Allocator.AllocMany` and `allocLocked` paths (head==0, preferAppend, region logic)
  - add a unit test that reproduces misbehavior in isolation.

---

## Activity Log

Use this section as a running journal of what changed and what was observed.

### 2026-01-30
- Added trace/probe tests and confirmed current failures:
  - bloat vacuum ratio ~7x at keys=20000 (intentional red)
  - reclaimables increased after rewrite (intentional red)
- Observed `FragmentationReport` gating on `freelist.head != 0`, so probes must use allocator stats.

