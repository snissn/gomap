# RaftDB Milestone (Roadmap)

## TL;DR

The long-term goal is to build a consensus-based KV “database” on top of TreeDB/HashDB.
This directory collects the repo-local prerequisites and contracts required to make that safe.

## Who Is This For?

- Anyone building replication/consensus on top of this repo.
- Contributors who want to evolve TreeDB/HashDB without breaking future RaftDB assumptions.

## What Raft Needs From Storage

At minimum, a Raft-backed system needs:

- **Log store**: append entries, read by index, truncate/compact.
- **Stable store**: persist term/vote/config.
- **State machine store**: apply committed commands atomically and snapshot/restore.

## Storage Contracts (Must Be Explicit)

Before a Raft layer can safely depend on this repo, we need explicit contracts for:

- `docs/contracts/DURABILITY.md`
- `docs/contracts/LOCKING.md`
- `docs/contracts/CONCURRENCY.md`
- `docs/contracts/ITERATION.md`

## Current Recommendation

- Use **TreeDB** for anything “committed” that must survive crashes:
  - cached mode is fine, but use `*Sync` operations for durability.
- Treat **HashDB** as a high-performance engine with evolving durability semantics (great for benchmarking; not yet a Raft commit store).

## Next Concrete Steps (Repo-Local)

- Add runnable examples for the stable surface (`Example*` tests) and expand GoDoc coverage.
- Add a “snapshot story” suitable for Raft (consistent full scan + restore) and test it end-to-end.
- Decide and document an API stability/versioning policy for the stable surface.

