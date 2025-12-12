You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 1 — On‑Disk Primitives & Checksums**.

Idempotent execution contract:
1. Validate prerequisites:
   - `go.mod` exists and Phase 0 canonical dirs exist.
   If missing, explain and stop without changes.
2. Detect existing Phase 1 artifacts in canonical packages (`internal/crc`, `internal/page`, `internal/slab`). Summarize done vs remaining against the checklist below.
3. If the checklist is fully satisfied, re‑run Phase‑1 tests, fix minor issues only, then stop.
4. Otherwise implement the missing pieces and the specified unit tests, keeping scope strictly Phase 1.

Implementation tasks (per `specs/spec.md`):
- Add global constants in a shared place (recommend `internal/page/constants.go`):
  - `PageSize = 4096`
  - `InlineThresholdDefault = 256`
  - `InlineHardMin = 64`
  - `InlineHardMax = 2048`
  - `SlabRotateSize = 4 << 30` (4GB)
- `internal/crc`:
  - CRC32C Castagnoli table + helpers: `Checksum([]byte) uint32`, `Verify([]byte, uint32) error`.
- `internal/page`:
  - `ValuePtr` type exactly matching the 16‑byte on‑disk layout with `FileID uint32` in the final 4 bytes, plus LE encode/decode helpers.
  - Page header struct/constants:
    - 16‑byte header (PageID, CRC32C of body, Flags, Count).
    - Flags/types: Meta(0x01), Freelist(0x02), Internal(0x03), Leaf(0x04).
    - Unsafe/zero‑copy parse helpers for header/body (no mmap slices exported yet).
- `internal/slab`:
  - Slab record encode/decode per spec:
    - `[CRC32C][KeyLen u16][ValueLen u32][Key][Value]`.
    - CRC over `KeyLen..ValueBytes`.
    - `ValuePtr.Offset` points to `KeyLen` (immediately after CRC).
    - `ValuePtr.Length = 2 + 4 + len(Key) + len(Value)`.
    - read path bounds‑checks, verifies CRC, and returns copies.

Definition of done (Phase 1 checklist):
- `internal/crc` provides Castagnoli CRC32C helpers with tests.
- `internal/page` defines constants, `ValuePtr` with LE encode/decode, and page header/types with CRC support.
- `internal/slab` can write/read slab records and return/consume `ValuePtr`.
- All Phase‑1 unit tests pass.

Tests to add (per `specs/test-spec.md` 1.4 and header CRC invariants):
- `ValuePtr`:
  - serializes to exactly 16 bytes,
  - LE field ordering (Offset first 8B),
  - length precision formula.
- Slab record:
  - round‑trip (key+value),
  - CRC mismatch detection,
  - sequential enumeration over variable lengths.
- Page header:
  - CRC32C computed/verified over body.

Verification:
- Run `go test ./internal/crc ./internal/page ./internal/slab`.

Finish with a concise summary and any new APIs Phase 2 should use.

Phase completion marker:
- Marker file: `@PHASE_1_COMPLETE` in the repo root.
- If during this run you found Phase 1 already complete **or** you made only trivial tweaks (small fixes/renames, test expectation nits), then create/leave the marker (`touch @PHASE_1_COMPLETE`).
- If you implemented any substantial Phase‑1 functionality or added significant new tests/files, **do not** create the marker; if it already exists, delete it.
