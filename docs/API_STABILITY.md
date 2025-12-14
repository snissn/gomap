# API Stability

## TL;DR

- The intended “stable surface” for downstream projects is:
  - `github.com/snissn/gomap/TreeDB` (package `treedb`)
  - `github.com/snissn/gomap/HashDB` (package `hashdb`)
- Everything else is either internal implementation detail or benchmark tooling and may change freely.

## Who Is This For?

- Engineers building “real systems” (e.g. consensus/replication, app state storage) on top of TreeDB/HashDB.
- Contributors changing on-disk formats or public APIs.

## Stability Levels

### Stable (intended for downstream use)

- `TreeDB` / package `treedb`
  - Primary entrypoint: `treedb.Open(opts)` (cached write-back mode by default).
  - Backend-only mode: `opts.Mode = treedb.ModeBackend` or `treedb.OpenBackend(opts)`.
  - Errors: `treedb.ErrLocked` is part of the contract.
- `HashDB` / package `hashdb`
  - Primary entrypoint: `hashdb.Open(dir)` / `hashdb.OpenWithShards(dir, n)`.
  - Single-shard DB: `hashdb.OpenSingle(dir)` (not goroutine-safe).
  - Errors: `hashdb.ErrLocked` is part of the contract.

### Internal (not stable)

- Anything in an `internal/` directory (Go’s internal visibility rules apply).
- Most packages under `TreeDB/*` and `HashDB/*` other than the root package are implementation details.

### Tooling / Benchmarks (not stable)

- `cmd/unified_bench`
- `HashDB/benchmark`, `HashDB/redisserver`

## On-Disk Format Compatibility

This repo is a dev project; formats may evolve.

Rules of thumb:
- Do not expect on-disk format compatibility across large refactors unless explicitly stated.
- Prefer rebuilding DB directories for benchmarks and experiments.
- If/when we stabilize formats, we’ll add:
  - an explicit version marker in metadata, and
  - a migration story (or a “rebuild required” guarantee).

## Documentation + Examples (Go Best Practices)

For the stable surface, the expectation is:
- Package-level docs (`doc.go`) describing purpose + caveats.
- Doc comments for exported identifiers that are part of the stable surface.
- Runnable examples (`Example*` tests) for the primary entrypoints.

