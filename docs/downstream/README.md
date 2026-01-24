# Downstream Readiness Milestone (Roadmap)

## TL;DR

The long-term goal is to make TreeDB/HashDB reliable storage primitives that downstream projects
(including replication/consensus systems) can safely build on.
This directory collects the repo-local prerequisites and contracts required to make that safe.

## Who Is This For?

- Anyone building higher-level systems on top of this repo.
- Contributors who want to evolve TreeDB/HashDB without breaking downstream assumptions.

## What Downstream Systems Need From Storage

At minimum, a replicated state machine needs:

- **Log store**: append entries, read by index, truncate/compact.
- **Stable store**: persist term/vote/config.
- **State machine store**: apply committed commands atomically and snapshot/restore.

## Storage Contracts (Must Be Explicit)

Before a downstream system can safely depend on this repo, we need explicit contracts for:

- `docs/contracts/DURABILITY.md`
- `docs/contracts/LOCKING.md`
- `docs/contracts/CONCURRENCY.md`
- `docs/contracts/ITERATION.md`

## Primitives (Interfaces + Engine Choice)

- `docs/downstream/PRIMITIVES.md`

## Current Recommendation

- Use **TreeDB** for anything “committed” that must survive crashes:
  - cached mode is fine, but use `*Sync` operations for durability.
- Treat **HashDB** as a high-performance engine with explicit `*Sync` durability calls backed by slab-log recovery; prefer TreeDB if you need stronger integrated journal-based durability and corruption diagnostics.

## Next Concrete Steps (Repo-Local)

- Add runnable examples for the stable surface (`Example*` tests) and expand GoDoc coverage.
- Add a “snapshot story” suitable for replication/consensus (consistent full scan + restore) and test it end-to-end.
- Decide and document an API stability/versioning policy for the stable surface.
