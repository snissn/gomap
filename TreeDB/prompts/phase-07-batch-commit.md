You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 7 — Batch “Zipper” Merge & Redundant Superblock Commit** in the **root `treedb` package** (using internal packages).

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 4 tree supports COW merge + splits.
   - Phase 5 slab manager can append large values and expose active slab.
   - Phase 2 pager meta pages + freelist exist.
   - Phase 6 MVCC graveyard/pruner exists.
   If missing, explain and stop without changes.
2. Detect existing Batch/commit pipeline in root package and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run batch/commit tests and stop.
4. Otherwise implement missing batch state machine + commit wiring and Phase‑7 tests only.

Implementation tasks (per `specs/spec.md` 5.1 and 2.2.1):
- Implement public `Batch` to satisfy cosmos‑db Batch interface:
  - ops map keyed by stringified keys,
  - byte size tracking,
  - strict state machine: after `Write*`, batch is closed; further `Set/Delete/Write` error.
- Zipper write path:
  - Pre‑write out‑of‑line values (`len(value) > InlineThreshold`) to active slab immediately; store returned `ValuePtr` in batch.
  - Sort batch keys; call tree COW merge to produce new User/System roots and retired PageIDs.
  - Record retired PageIDs into MVCC graveyard.
  - Update slab stats keys in System tree for any modified slabs.
- Commit + meta:
  - increment `CommitSeq`, alternate meta pages.
  - If Sync:
    1. `fdatasync` active slab **before** any index/meta persistence.
    2. `msync` dirty index pages and `fdatasync/fsync` `index.db`.
  - Write inactive meta page with new roots, freelist head, total pages, active slab ID/tail, CRC.
  - Publish new `DBState` via MVCC holder.
- Wire `Set/SetSync/Delete/DeleteSync` to use single‑op batches under the writer lock.

Definition of done (Phase 7 checklist):
- Batch state machine matches test‑spec.
- Zipper merge creates atomic commits with correct retired‑page tracking.
- Meta alternation + CRC works.
- Sync durability ordering is enforced.
- All Phase‑7 unit/integration tests pass.

Tests to add (per `specs/test-spec.md` 2.3 and durability ordering):
- Atomicity: injected panic mid‑merge yields no partial commit after reopen.
- Batch invalid‑reuse errors.
- Large value handling smoke (no large memory spikes).
- Ordering hook: verify slab sync happens before index/meta sync in `WriteSync`.

Verification:
- Run `go test ./...` focusing on root batch/commit behavior.

Do not implement iterators or compaction yet.

Phase completion marker:
- Marker file: `@PHASE_7_COMPLETE` in the repo root.
- If during this run Phase 7 was already complete **or** you made only trivial batch/commit tweaks (minor ordering fixes, test nits), then create/leave the marker (`touch @PHASE_7_COMPLETE`).
- If you implemented substantial zipper merge/commit/meta/durability logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
