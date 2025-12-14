# Documentation

## TL;DR

- Start with the root `README.md` for the high-level overview.
- Use `docs/GETTING_STARTED.md` if you want to run things locally quickly.
- Use `docs/contracts/` for the “do not surprise me” behavioral contracts (durability, iteration, concurrency, locking).
- Use `cmd/unified_bench/README.md` for benchmark usage and methodology.

## Quickstart (dev)

Prereqs:
- Go `1.25+` (see `go.mod`)
- Linux/macOS (TreeDB uses `mmap`; HashDB has some Windows support, but TreeDB is currently Unix-focused)

Commands:
- `make test`
- `make build`
- `./bin/unified-bench` (after `make unified-bench`)

## What’s In This Repo

- **HashDB** (`HashDB/`, package `hashdb`)
  - mmap-backed hash index + append-only slab value log.
  - Optimized for very fast random reads and high throughput.
  - Provides the Redis-style benchmarking harness (`HashDB/redisserver`, `HashDB/benchmark`).
- **TreeDB** (`TreeDB/`, package `treedb`)
  - Persistent B+Tree with a memory-mapped index and slab value log for large values.
  - Two open modes behind one API:
    - cached write-back layer (default): `treedb.Open(...)`
    - backend-only engine: `opts.Mode = treedb.ModeBackend` or `treedb.OpenBackend(...)`
- **BTreeOnHashDB** (`HashDB/BTreeOnHashDB/`)
  - A B-Tree implementation layered on top of HashDB (mostly for benchmarking/comparison).
- **Unified Bench** (`cmd/unified_bench/`)
  - A single binary that runs a consistent workload across HashDB, TreeDB, TreeDB backend-only, etc.

## Contract Docs (Raft-Readiness)

If you’re building a higher-level system (e.g. Raft replication) on top of these engines,
start here:

- `docs/API_STABILITY.md`
- `docs/GETTING_STARTED.md`
- `docs/REPO_MAP.md`
- `docs/TREEDB_CACHED_VS_BACKEND.md`
- `docs/contracts/README.md`
- `docs/raft/README.md`

## Benchmarking

- `cmd/unified_bench/README.md`
- `docs/BENCHMARK_SPEC.md`
- `docs/DEV_NOTES.md` (HashDB perf notes and next-step ideas)

## Legacy / Historical

As the repo is cleaned up, older “planning” docs and one-off scripts may be moved into `docs/legacy/`.
