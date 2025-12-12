You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 3 — Slotted Pages** in `internal/page`.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 2 pager exists (pages alloc/read/write).
   - Phase 1 page header/types and `ValuePtr` exist.
   If missing, explain and stop without changes.
2. Detect existing slotted‑page code in `internal/page` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run slotted‑page tests and stop.
4. Otherwise implement missing slotted‑page primitives and unit tests only for this phase.

Implementation tasks (per `specs/spec.md` Section 4):
- Implement slotted‑page mechanics for variable‑length entries:
  - directory of `uint16` offsets growing downward,
  - heap growing upward,
  - binary search by key via directory,
  - insert/update/delete in a single page,
  - defragmentation that compacts heap and preserves directory order,
  - reuse freed space on subsequent inserts.
- Leaf heap entry encode/decode:
  - `[KeyLen u16][ValueLen u32][Flags u8][Key][InlineValue|ValuePtr]`
  - flags: Inline, Pointer, Tombstone.
- Internal heap entry encode/decode:
  - `[ChildPageID u64][KeyBytes]` with directory offsets.

Definition of done (Phase 3 checklist):
- Leaf and internal page types support search/insert/update/delete within one page.
- Free‑space accounting and defragmentation are correct.
- Encoding/decoding round‑trips.
- All Phase‑3 unit tests pass.

Tests to add (page‑local focus; full‑tree walks happen in later phases):
- Insert enough entries to fill a leaf page; verify directory order and binary search correctness.
- Update existing keys in‑place when possible; verify space accounting.
- Delete every other key; verify tombstones/compaction behavior and that freed space is reused.
- Defragmentation preserves sorted key order and allows further inserts.

Verification:
- Run `go test ./internal/page`.

Do not implement any multi‑page B+Tree logic yet; only single‑page slotted behavior.

Phase completion marker:
- Marker file: `@PHASE_3_COMPLETE` in the repo root.
- If during this run Phase 3 was already complete **or** you made only trivial slotted‑page tweaks (small bugfixes, test nits), then create/leave the marker (`touch @PHASE_3_COMPLETE`).
- If you implemented substantial slotted‑page mechanics or added major tests/files, **do not** create the marker; if it already exists, delete it.
