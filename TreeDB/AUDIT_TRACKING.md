# TreeDB Audit Tracking

This repo previously kept a detailed audit tracker here. It referenced removed
components and legacy design docs (e.g. old value-store terms, old specs/options) and was
removed to avoid misleading future agents.

## Current safety anchors (start here)

- Canonical spec index: `TreeDB/docs/spec/README.md`
- Contracts (normative behavior): `TreeDB/docs/spec/contracts.md`
- Storage layout + `ValuePtr` + value-log lifecycle: `TreeDB/docs/spec/storage-format.md` and `TreeDB/docs/spec/value-log-lifecycle.md`
- WAL on/off semantics: `TreeDB/docs/spec/write-path-and-durability.md`
- Crash recovery behavior: `TreeDB/docs/spec/recovery.md`
- Crash recovery tests: `TreeDB/recovery_spec_test.go`
- Value-log robustness tests/fuzzing: `TreeDB/internal/valuelog/*_test.go`
- Default permissions: `TreeDB/permissions_test.go`

## When starting a new audit

- Create a fresh tracker that links to *current* code paths and tests.
- Prefer “evidence = file path + test name” over prose so agents can validate quickly.
