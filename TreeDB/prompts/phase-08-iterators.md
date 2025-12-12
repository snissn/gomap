You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 8 — Iterators (Cursor Stack)** in the **root `treedb` package**.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 4 tree search returns cursor stacks.
   - Phase 6 MVCC snapshots exist.
   - Phase 7 commits publish `DBState`/roots.
   If missing, explain and stop without changes.
2. Detect existing iterator code in root package and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run iterator tests and stop.
4. Otherwise implement missing iterators and Phase‑8 tests only.

Implementation tasks (per `specs/spec.md` 5.3 and Cosmos contracts):
- Implement forward `Iterator(start,end)`:
  - acquire snapshot,
  - build a cursor stack via tree search to `start`,
  - `Next()` follows the drill‑down/up algorithm in spec.
- Implement reverse `ReverseIterator(start,end)`:
  - domain `[start,end)`, `end` exclusive,
  - seek to first key `>= end` then step back; nil `end` seeks last key,
  - `Next()` moves backward.
- Enforce Cosmos semantics:
  - tombstones skipped,
  - invalid domain (`start>=end`) yields invalid iterator,
  - `Key()`/`Value()` allocate fresh copies,
  - `Next()` panics if `Valid()==false`,
  - iterators hold snapshot until `Close()`.

Definition of done (Phase 8 checklist):
- Forward and reverse iterators return correct sequences for bounded/unbounded domains.
- Snapshot visibility is stable under concurrent commits.
- Iterators never observe reclaimed pages/slabs while open.
- All Phase‑8 iterator tests pass.

Tests to add (per `specs/test-spec.md` 2.2):
- Forward/reverse scan semantics, nil bounds, end‑exclusive behavior.
- Concurrent iteration snapshot stability under commits.
- Aggressive pruning while iterators are open (reachability invariant).

Verification:
- Run `go test ./...` (iterator + mvcc heavy).

Stop after iterators only; do not implement compaction or adaptive threshold yet.

Phase completion marker:
- Marker file: `@PHASE_8_COMPLETE` in the repo root.
- If during this run Phase 8 was already complete **or** you made only trivial iterator tweaks (minor bugfixes, test nits), then create/leave the marker (`touch @PHASE_8_COMPLETE`).
- If you implemented substantial iterator/cursor‑stack logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
