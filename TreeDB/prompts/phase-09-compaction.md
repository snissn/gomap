You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 9 — Compaction (Move‑and‑Micro‑Batch)** in `internal/compaction`, wiring `DB.Compact()` in the root package.

Idempotent execution contract:
1. Validate prerequisites:
   - Slab stats persisted in System tree (Phase 5/7).
   - Tree raw `Get`/`Set` exists under writer lock (Phase 4/7).
   - Slab manager supports zombie + SlabSet swap (Phase 5).
   - Commit pipeline is stable (Phase 7).
   If missing, explain and stop without changes.
2. Detect existing compaction code in `internal/compaction` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run compaction tests and stop.
4. Otherwise implement missing compaction pieces and Phase‑9 tests only.

Implementation tasks (per `specs/spec.md` 5.4):
- Candidate selection:
  - scan System slab stats and select slabs with `DeadBytes/TotalBytes > 0.5`.
- Optimistic copy:
  - sequentially parse cold slab records,
  - liveness check vs current tree pointer (`GetRaw`); skip dead/moved records,
  - append live records into a target slab; collect `(Key, OldPtr, NewPtr)` locally.
  - dead‑hint optimization can be a stub, but leave clear extension points.
- Micro‑batch locking commit:
  - split updates into micro‑batches (~100),
  - under writer lock for each micro‑batch:
    - `Current = Tree.Get(Key)`
    - if `Current == OldPtr`, `Tree.Set(Key, NewPtr)` else skip,
  - yield between micro‑batches to avoid starving user writes.
- Zombie transition:
  - atomically swap active `SlabSet` to include target and remove cold,
  - mark cold slab `IsZombie=true`,
  - delete cold slab when `RefCount==0`.
- Root `DB.Compact()` runs a full cycle and blocks until completion.
- Add IO throttling (leaky bucket) around copy loop to cap compaction bandwidth.

Definition of done (Phase 9 checklist):
- Compaction selects candidates, copies live data, and applies pointers via micro‑batch verify‑and‑set correctly.
- Zombie slabs remain readable under active snapshots and delete afterward.
- Compaction obeys single‑writer serialization and throttling.
- All Phase‑9 tests pass (long‑running crash/kill tests may be `t.Skip` or build‑tagged).

Tests to add (per `specs/test-spec.md` 2.6 and 5.*):
- Manual `Compact()` blocks and reduces dead bytes.
- Zombie life‑support during iteration then deletion after close.
- Resurrection verify‑set race (user write wins).
- Torn compaction recovery (simulate kill mid‑apply; if too heavy, add as skipped long test).
- Serialization under writer lock; throttling latency smoke.

Verification:
- Run `go test -race ./...` focusing on compaction‑heavy tests.

Do not implement adaptive threshold yet.

Phase completion marker:
- Marker file: `@PHASE_9_COMPLETE` in the repo root.
- If during this run Phase 9 was already complete **or** you made only trivial compaction tweaks (minor bugfixes, test nits), then create/leave the marker (`touch @PHASE_9_COMPLETE`).
- If you implemented substantial compaction/locking/throttling/zombie logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
