# API Stability

## TL;DR

- **Project status: pre-alpha.** APIs and on-disk formats may change without backward-compatibility guarantees.
- The intended “stable surface” for downstream projects (once we stabilize) is:
  - `github.com/snissn/gomap/TreeDB` (package `treedb`)
  - `github.com/snissn/gomap/HashDB` (package `hashdb`)
- Everything else is either internal implementation detail or benchmark tooling and may change freely.

## Who Is This For?

- Engineers building “real systems” (e.g. consensus/replication, app state storage) on top of TreeDB/HashDB.
- Contributors changing on-disk formats or public APIs.

## Stability Levels

### Stable (intended for downstream use)

Note: while these packages are the *intended* stable surface, the repo is still pre-alpha.
Downstream users should pin a tagged release or commit and still expect breaking
changes until the project reaches a stable post-pre-alpha phase.

- `TreeDB` / package `treedb`
  - Primary entrypoint: `treedb.Open(opts)` (cached write-back mode by default).
  - Errors: `treedb.ErrLocked` is part of the contract.
- `HashDB` / package `hashdb`
  - Primary entrypoint: `hashdb.Open(dir)` / `hashdb.OpenWithShards(dir, n)`.
  - Single-shard DB: `hashdb.OpenSingle(dir)` (not goroutine-safe).
  - Errors: `hashdb.ErrLocked` is part of the contract.

### Internal (not stable)

- Anything in an `internal/` directory (Go’s internal visibility rules apply).
- Most packages under `TreeDB/*` and `HashDB/*` other than the root package are implementation details.
- `TreeDB/mvcc` is an explicitly opt-in, pre-alpha downstream integration
  surface. It is importable for pinned experiments, but its Go API and reserved
  physical-key/value format are not yet stable compatibility promises.

### Tooling / Benchmarks (not stable)

- `cmd/unified_bench`
- `HashDB/benchmark`, `HashDB/redisserver`

## On-Disk Format Compatibility

This repo is a dev project; formats may evolve rapidly.

Rules of thumb:
- Do not expect on-disk format compatibility across commits unless explicitly stated.
- It is acceptable for new binaries to fail to open old DB directories (and vice versa).
- Prefer rebuilding DB directories for benchmarks and experiments.
- The optional immutable vector-partition catalog lifecycle identity adds
  manifest and placement SHA-256 fields to persisted catalog/meta commands and
  snapshots. Existing mutable inline identities keep their canonical bytes;
  older binaries are not promised to read snapshots containing the new fields.
- Owner-local immutable vector-partition preparation adds VLS1 scope bytes in
  VLC1 operation 8, VCP2 checkpoints, and VCP3 skipped-generation ranges.
  Unscoped VCP1 encoding is unchanged; older binaries are not promised to open
  VCP2 or VCP3. Rebuild prepared local state rather than assuming cross-version
  compatibility.
- If/when we stabilize formats, we’ll add:
  - an explicit version marker in metadata, and
  - a migration story (or a “rebuild required” guarantee).

## Change Policy (Stable Surface)

The intent is to let downstream projects depend on the stable surface without surprises.

Policy:
- Breaking changes to the stable surface should be reflected in `CHANGELOG.md`.
- Releases are tagged with SemVer, but pre-`1.0.0` tags do not yet guarantee
  API or on-disk compatibility across minor versions.
- Non-stable packages/tooling may change freely without guarantees.

## Documentation + Examples (Go Best Practices)

For the stable surface, the expectation is:
- Package-level docs (`doc.go`) describing purpose + caveats.
- Doc comments for exported identifiers that are part of the stable surface.
- Runnable examples (`Example*` tests) for the primary entrypoints.
