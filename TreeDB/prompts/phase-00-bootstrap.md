You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 0 (Repository Bootstrap)**.

Idempotent execution contract:
1. **Validate prerequisites**: `specs/spec.md`, `specs/test-spec.md`, and `AGENTS.md` exist. If any are missing, explain and stop without changes.
2. **Detect prior work** using the “definition of done” checklist below; summarize what is present vs missing.
3. If all checklist items are satisfied, run a light re‑validation (`go mod tidy`, `go list ./...`, `go test ./...`) and stop.
4. Otherwise, perform the bootstrap tasks, then re‑check the checklist.

Bootstrap tasks (canonical layout; do not invent alternate paths):
- If `go.mod` is missing, run `go mod init treedb` (or a reasonable default derived from the folder). Do not re‑init if it already exists.
- Ensure `github.com/cosmos/cosmos-db` is listed in `go.mod` as a requirement.
- Create these directories exactly (if missing):  
  `internal/crc`, `internal/pager`, `internal/page`, `internal/tree`, `internal/slab`, `internal/mvcc`, `internal/compaction`, `internal/adaptive`.
  Each directory should contain at least a minimal `doc.go` so `go list` succeeds.
- In the **module root package** (recommend package name `treedb`), add minimal public stubs only if missing:
  - `type Options struct { ... }` with placeholder fields for later phases (e.g., `Dir`, `ChunkSize`, `InlineThreshold`, `KeepRecent`, `AdaptiveEnabled`).
  - `type DB struct {}` placeholder.
  - `func Open(opts Options) (*DB, error)` returning a clear `not implemented` error.
- Do **not** implement any real functionality beyond compilation stubs.

Definition of done (Phase 0):
- `go.mod` exists and resolves dependencies.
- All canonical package directories exist with compilable stubs.
- Root `treedb` package exports `Options`, `DB`, and `Open`.

Verification:
- `go list ./...` works.
- `go test ./...` passes (may be “no tests”).

Finish with a short summary of what you changed and what Phase 1 will build on.

Phase completion marker:
- Marker file: `@PHASE_0_COMPLETE` in the repo root.
- If during this run you found Phase 0 already complete **or** you made only trivial tweaks (e.g., small doc/stub edits, tidy), then create/leave the marker (`touch @PHASE_0_COMPLETE`).
- If you performed substantial bootstrap work (new module init, new dirs, new stubs), **do not** create the marker; if it already exists, delete it.
