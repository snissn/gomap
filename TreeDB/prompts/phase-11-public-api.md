You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 11 — Public DB API & Misc** in the root `treedb` package.

Idempotent execution contract:
1. Validate prerequisites:
   - Phases 0–10 completed (pager, slabs, tree, mvcc, batches, iterators, compaction, adaptive controller).
   If anything is missing, list gaps and stop without changes.
2. Detect existing public API surface and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, run full suite (incl. race) and stop.
4. Otherwise implement missing public API + remaining tests only for this phase.

Implementation tasks (per `specs/spec.md` Section 6):
- Finalize `Open(opts Options)`:
  - validate options, open/create directory,
  - open pager; read/verify both meta pages and select highest valid `CommitSeq`,
  - call slab manager `Load` for tail truncation + ghost slab cleanup,
  - initialize User/System trees from roots,
  - build and publish initial `DBState`.
- Implement/finish public cosmos‑db methods:
  - `Get`, `Has`, `Set`, `SetSync`, `Delete`, `DeleteSync`,
  - `Iterator`, `ReverseIterator`,
  - `NewBatch`, `NewBatchWithSize`,
  - `Close`, `Print`, `Stats`, `Compact`.
- Ensure `Stats()` includes mandatory keys:
  - `"cosmos.db.type": "treedb"`,
  - `"treedb.pages.total"`,
  - `"treedb.slabs.active_id"`,
  - `"treedb.slabs.zombies"`,
  - plus adaptive telemetry when enabled.

Definition of done (Phase 11 checklist):
- Public API matches cosmos‑db contracts and uses snapshots for reads.
- Open/reopen + recovery logic works (dual meta selection, tail truncation, ghost slab deletion).
- All integration tests and upstream cosmos‑db suite pass.
- Long crash/kill/fuzz tests exist but are skipped or build‑tagged by default.

Tests to add/finish:
- Integration CRUD, persistence, input validation (test‑spec 2.1).
- Upstream cosmos‑db backend suite (test‑spec 6.1): add dep and run its tests against TreeDB.
- Pruning validation scenario (test‑spec 6.2).
- Property‑based model tests + fuzz targets (test‑spec 3.*).
- Failure‑injection/kill tests (test‑spec 4.*) as skipped/build‑tagged long tests.

Verification:
- Run `go test ./...`
- Run `go test -race ./...`
- Run cosmos‑db backend suite.
- Ensure fuzz targets compile (`go test -run=^$ -fuzz=.` briefly).

Finish with a tight summary and any remaining TODOs.

Phase completion marker:
- Marker file: `@PHASE_11_COMPLETE` in the repo root.
- If during this run Phase 11 was already complete **or** you made only trivial public‑API tweaks (minor bugfixes, test nits), then create/leave the marker (`touch @PHASE_11_COMPLETE`).
- If you implemented substantial public API/recovery/compliance work or added major tests/files, **do not** create the marker; if it already exists, delete it.
