You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 5 — Slab Manager & SlabSet** in `internal/slab`.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 1 slab record + `ValuePtr` exist.
   - Phase 4 System tree exists (for stats keys).
   If missing, explain and stop without changes.
2. Detect existing slab manager work in `internal/slab` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run slab tests and stop.
4. Otherwise implement missing slab manager features and Phase‑5 tests only.

Implementation tasks (per `specs/spec.md` 2.1, 3.4, 3.5.2):
- Implement `SlabManager` responsible for `.slab` files:
  - `Load(dir, activeID, activeTail)` (or equivalent) that:
    - scans `data-*.slab`,
    - opens handles,
    - builds an immutable `SlabSet`,
    - truncates the active slab to `activeTail`,
    - deletes ghost slabs with `ID > activeID`.
  - `AppendLarge(key, value)` that writes a slab record and returns a `ValuePtr`.
  - rotation when active slab exceeds 4GB with fsync ordering (slab sync → close → new slab → directory fsync).
- Track per‑slab stats (`DeadBytes`, `TotalBytes`) and expose update helpers for overwrites/deletes.
- Persist stats in the System tree keys `0x00|"slab"|uint32(FileID)` (integration into commit happens in Phase 7).
- Support ref‑counted zombie deletion semantics (`RefCount`, `IsZombie`) for later compaction.

Definition of done (Phase 5 checklist):
- `SlabManager.Load` builds correct `SlabSet`, truncates tail, and removes ghost slabs.
- Large appends produce correct `ValuePtr` and CRC‑validated records.
- Rotation logic is correct and durable.
- Stats tracking and encoding helpers exist.
- All Phase‑5 unit tests pass.

Tests to add (per `specs/test-spec.md` 1.4, 1.5, 1.7):
- Manager open/reopen:
  - tail truncation behavior,
  - ghost slab deletion.
- Stats persistence/decoding for multiple slabs (using System tree).

Verification:
- Run `go test ./internal/slab`.

Do not implement compaction or MVCC logic yet beyond any minimal stubs required to compile.

Phase completion marker:
- Marker file: `@PHASE_5_COMPLETE` in the repo root.
- If during this run Phase 5 was already complete **or** you made only trivial slab‑manager tweaks (minor fixes, test nits), then create/leave the marker (`touch @PHASE_5_COMPLETE`).
- If you implemented substantial slab manager/rotation/stats logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
