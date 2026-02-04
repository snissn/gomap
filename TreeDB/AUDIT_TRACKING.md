# TreeDB Audit Tracking

This repo previously kept a detailed audit tracker here. It referenced removed
components and legacy design docs (e.g. “slabs”, old specs/options) and was
removed to avoid misleading future agents.

## Current safety anchors (start here)

- Contracts (normative behavior): `docs/contracts/*`
- Storage layout + `ValuePtr` + value-log lifecycle: `docs/TREEDB_STORAGE_FORMAT.md`
- WAL on/off semantics: `docs/TREEDB_WRITE_PATHS.md`
- Crash recovery overview: `docs/TREEDB_RECOVERY.md`
- Crash recovery tests: `TreeDB/recovery_spec_test.go`
- Value-log robustness tests/fuzzing: `TreeDB/internal/valuelog/*_test.go`
- Default permissions: `TreeDB/permissions_test.go`

## When starting a new audit

- Create a fresh tracker that links to *current* code paths and tests.
- Prefer “evidence = file path + test name” over prose so agents can validate quickly.
