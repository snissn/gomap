# Contracts (Durability, Concurrency, Iteration)

## TL;DR

These docs describe the behavioral contracts for TreeDB/HashDB that downstream projects
can rely on (or must not assume).

Start here:

- `docs/contracts/LOCKING.md`
- `docs/contracts/CONCURRENCY.md`
- `docs/contracts/DURABILITY.md`
- `docs/contracts/ITERATION.md`

## Who Is This For?

- Anyone building a service on top of TreeDB/HashDB (especially replication/consensus).
- Contributors changing storage semantics or iterator behavior.

