You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 6 — MVCC Snapshots, Graveyard, Pruning** in `internal/mvcc`.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 4 tree returns retired pages on COW writes (even if commit wiring is stubbed).
   - Phase 2 pager freelist exists.
   - Phase 5 slabs expose `SlabSet` with refcounts.
   If missing, explain and stop without changes.
2. Detect existing MVCC/graveyard work in `internal/mvcc` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run MVCC tests and stop.
4. Otherwise implement missing MVCC features and Phase‑6 unit tests only.

Implementation tasks (per `specs/spec.md` 5.2):
- Define immutable `DBState`:
  - `CommitSeq`, `UserRootPageID`, `SystemRootPageID`, `SlabSet`.
- Provide `StateHolder`/`Registry` that:
  - publishes `*DBState` via `atomic.Pointer`,
  - tracks active reader sequences and exposes `MinPinnedSeq()`.
- Implement `Snapshot`:
  - acquisition loads DBState once (Acquire), registers seq, increments all slab `RefCount`s.
  - close deregisters seq, decrements slab refs, deletes zombie slabs when refcount hits 0.
- Implement in‑memory graveyard:
  - `map[CommitSeq][]PageID` plus helpers to record retired pages per commit.
- Implement `Pruner`:
  - moves retired pages to pager freelist only when
    - `RetiredAtSeq < MinPinnedSeq` and
    - `RetiredAtSeq < CurrentSeq - KeepRecent`.

Definition of done (Phase 6 checklist):
- Snapshots pin roots and slabs correctly and release them safely.
- Reader registry reports correct `MinPinnedSeq`.
- Graveyard records retired pages per commit.
- Pruner enforces both reachability and KeepRecent windows.
- All Phase‑6 unit tests pass.

Tests to add (per `specs/test-spec.md` 1.3):
- Hold/release with pruning movement to freelist.
- Reachability barrier under open snapshots while pruning is invoked.
- `MinPinnedSeq` advancement as readers close.
- `KeepRecent` history window behavior.

Verification:
- Run `go test ./internal/mvcc`.

Do not implement zipper batch merge or iterators yet beyond stubs.

Phase completion marker:
- Marker file: `@PHASE_6_COMPLETE` in the repo root.
- If during this run Phase 6 was already complete **or** you made only trivial MVCC tweaks (minor bugfixes, test nits), then create/leave the marker (`touch @PHASE_6_COMPLETE`).
- If you implemented substantial snapshot/graveyard/pruner logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
