You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 4 — B+Tree Core (Dual Roots)** in `internal/tree`.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 3 slotted pages exist (`internal/page` leaf/internal layouts).
   - Phase 2 pager alloc/read/write works.
   If missing, explain and stop without changes.
2. Detect existing B+Tree/COW code in `internal/tree` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run tree unit tests and stop.
4. Otherwise implement missing B+Tree features and unit tests only for this phase.

Implementation tasks (per `specs/spec.md` 5.1/5.3 and 3.5):
- Implement a B+Tree over pager pages:
  - search that returns a cursor/stack of nodes from root → leaf.
  - leaf and internal node split logic with separator propagation.
  - root split handling.
- Copy‑On‑Write updates:
  - clone pages along the search path on modification,
  - merge/update/delete keys in leaf pages,
  - retire replaced pages (just return the list for Phase 7/MVCC to consume; don’t prune yet).
- Dual roots:
  - maintain separate User and System trees with distinct root PageIDs.
  - **Resolve the spec vs test mismatch now**: implement explicit key encode/decode helpers so test‑spec 1.7 passes, and document the choice in code (`encodeUserKey`, `decodeUserKey`, etc.).
- Internal `Tree` interface needed later:
  - `GetRaw(key) (LeafEntry, error)`
  - `SetRaw(key, LeafEntry) (retired []PageID, err error)`

Definition of done (Phase 4 checklist):
- B+Tree search/insert/update/delete works across multiple pages with COW.
- Splits maintain sorted order and correct parent wiring.
- Dual‑root key encoding behavior is consistent and tested.
- All Phase‑4 unit tests pass.

Tests to add:
- Deterministic insert/delete sequences that force leaf and internal splits; verify ordering and fanout invariants.
- Namespace/encoding isolation per test‑spec 1.7.

Verification:
- Run `go test ./internal/tree`.

Do not implement slabs, MVCC, batches, iterators, or compaction beyond the minimal stubs required for compilation.

Phase completion marker:
- Marker file: `@PHASE_4_COMPLETE` in the repo root.
- If during this run Phase 4 was already complete **or** you made only trivial tree tweaks (minor bugfixes, test adjustments), then create/leave the marker (`touch @PHASE_4_COMPLETE`).
- If you implemented substantial B+Tree/COW/dual‑root functionality or added major tests/files, **do not** create the marker; if it already exists, delete it.
