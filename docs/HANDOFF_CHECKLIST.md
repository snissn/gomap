# Handoff Checklist (For Downstream Projects)

## TL;DR

This checklist exists so a downstream project can depend on this repo without tribal knowledge.

## Stable Surface

- [x] The stable public packages are explicitly defined (`docs/API_STABILITY.md`).
- [x] Public APIs are small and purposeful; implementation details live behind `internal/`.
- [x] Typed errors exist for key invariants (e.g. `ErrLocked`, `ErrClosed`, `ErrSnapshotCorrupt` where applicable).

## Contracts (Pinned by Tests)

- [x] Durability semantics are documented and tested (`docs/contracts/DURABILITY.md`).
- [x] Iterator semantics are documented and tested (`docs/contracts/ITERATION.md`).
- [x] Exclusive open is enforced and tested (`docs/contracts/LOCKING.md`).
- [x] Concurrency model is documented (`docs/contracts/CONCURRENCY.md`).

## GoDoc + Examples

- [x] Package docs (`doc.go`) exist for stable packages and include caveats.
- [x] Exported identifiers that are part of the stable surface have doc comments.
- [x] Runnable examples (`Example*`) exist for the primary entrypoints.

## Developer Onboarding

- [x] `docs/README.md` exists as an index (“start here”).
- [x] A “repo map” exists (where to look for what).
- [x] Benchmark methodology is documented and reproducible.
- [x] Profiling workflow is documented (`-profile-dir` + `cmd/benchprof`) for CPU/alloc/lock analysis.
- [x] Benchmark-tooling maintenance expectations are documented in agent docs (`docs/agents/BENCHPROF_MAINTENANCE.md`).

## Release / Change Communication

- [x] A change policy is documented (what can change freely vs what is stable).
- [ ] When ready, tags/releases follow SemVer so downstream repos can pin.
