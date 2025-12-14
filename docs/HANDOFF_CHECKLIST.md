# Handoff Checklist (For Downstream Projects)

## TL;DR

This checklist exists so a future project (e.g. “RaftDB”) can depend on this repo without tribal knowledge.

## Stable Surface

- [ ] The stable public packages are explicitly defined (`docs/API_STABILITY.md`).
- [ ] Public APIs are small and purposeful; implementation details live behind `internal/`.
- [ ] Typed errors exist for key invariants (e.g. `ErrLocked`, `ErrClosed`, `ErrCorrupt` where applicable).

## Contracts (Pinned by Tests)

- [ ] Durability semantics are documented and tested (`docs/contracts/DURABILITY.md`).
- [ ] Iterator semantics are documented and tested (`docs/contracts/ITERATION.md`).
- [ ] Exclusive open is enforced and tested (`docs/contracts/LOCKING.md`).
- [ ] Concurrency model is documented (`docs/contracts/CONCURRENCY.md`).

## GoDoc + Examples

- [ ] Package docs (`doc.go`) exist for stable packages and include caveats.
- [ ] Exported identifiers that are part of the stable surface have doc comments.
- [ ] Runnable examples (`Example*`) exist for the primary entrypoints.

## Developer Onboarding

- [ ] `docs/README.md` exists as an index (“start here”).
- [ ] A “repo map” exists (where to look for what).
- [ ] Benchmark methodology is documented and reproducible.

## Release / Change Communication

- [ ] A change policy is documented (what can change freely vs what is stable).
- [ ] When ready, tags/releases follow SemVer so downstream repos can pin.

