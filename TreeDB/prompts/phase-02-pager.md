You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 2 — Pager (`index.db`) with Chunked MMap** in `internal/pager`.

Idempotent execution contract:
1. Validate prerequisites:
   - Phase 1 completed (`internal/crc`, `internal/page` header/types).
   - Root `Options` has pager fields (`Dir`, `ChunkSize` at minimum).
   If missing, explain and stop without changes.
2. Detect existing pager/mmap work in `internal/pager` and compare to the checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run pager tests and do small fixes only, then stop.
4. Otherwise implement missing pager features and Phase‑2 unit tests only.

Implementation tasks (per `specs/spec.md` 2.2, 2.2.1, 3.3):
- `internal/pager` provides a `Pager` (or equivalent) responsible for:
  - opening/creating `index.db` in the DB directory,
  - addressing pages as fixed 4KB units,
  - allocating/freeing pages via an on‑disk freelist.
- Chunked mmap:
  - map file in configurable chunks (default 256MB),
  - enforce `ChunkSize % PageSize == 0` so pages never straddle chunk boundaries,
  - growth only via pre‑allocating disk space then `Truncate(expand)` + mmap of **new chunk only**,
  - forbid shrink (return error or panic guarded in tests),
  - per‑chunk refcount + safe `Unmap()` when refs==0,
  - safe accessors that never return Go slices backed by mmap,
  - enforce bounds (`Offset + PageSize` within mapped region) before any unsafe cast; SIGBUS cannot be recovered.
- Meta pages (Page 0 and Page 1):
  - define on‑disk meta layout per spec,
  - read both, verify CRC32C, select highest valid `CommitSeq`,
  - expose helpers to write the inactive meta page (actual commit wiring in Phase 7).
- Freelist pages:
  - linked list of freelist pages storing free PageIDs,
  - `AllocPage()` prefers freelist, else extends file,
  - `FreePages([]PageID)` appends to freelist.
- Durability helpers:
  - `SyncIndex()` (or similar) that `msync`s dirty pages and `fdatasync/fsync`s `index.db` when requested.

Definition of done (Phase 2 checklist):
- `internal/pager` can open/create `index.db`, map in chunks, and expand safely.
- Shrink is prevented.
- Page alloc/free via freelist works.
- Meta read/choose highest valid CommitSeq works.
- All Phase‑2 unit tests pass.

Tests to add (per `specs/test-spec.md` 1.1):
- Boundary crossing (alignment) behavior with small aligned chunks.
- Growth safety while readers pin old chunks.
- Negative shrink attempt fails safely (catch panic/error without crashing suite).
- Refcounted unmap safety.
- Basic durability helper smoke (no ordering assertions yet).

Verification:
- Run `go test ./internal/pager` with a small `ChunkSize` option.

Stop after Phase 2 only; do not implement slotted pages or trees beyond minimal compilation stubs.

Phase completion marker:
- Marker file: `@PHASE_2_COMPLETE` in the repo root.
- If during this run Phase 2 was already complete **or** you made only trivial pager tweaks (small bounds/alignment fixes, test cleanups), then create/leave the marker (`touch @PHASE_2_COMPLETE`).
- If you implemented substantial pager/mmap/freelist/meta functionality or added major tests/files, **do not** create the marker; if it already exists, delete it.
