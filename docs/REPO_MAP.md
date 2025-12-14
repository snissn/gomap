# Repo Map

## TL;DR

The stable public packages are `TreeDB` (package `treedb`) and `HashDB` (package `hashdb`).
Most other directories are internal implementation details or benchmark tooling.

## Top Level

- `HashDB/` — HashDB engine + redis-style benchmark harness + BTreeOnHashDB
- `TreeDB/` — TreeDB engine + internal packages
- `cmd/unified_bench/` — unified benchmark binary
- `docs/` — design notes, benchmark spec, and stable contracts
- `scripts/` — small one-off local scripts
- `artifacts/` — benchmark outputs / plots (non-source)

## HashDB

- `HashDB/` — public package `hashdb` (sharded engine + single-shard DB)
- `HashDB/redisserver/` — redis protocol server wrappers for HashDB/Badger (benchmarking/dev)
- `HashDB/benchmark/` — benchmark runner/reporting/plots
- `HashDB/BTreeOnHashDB/` — BTree built on top of HashDB (benchmark/comparison)

## TreeDB

- `TreeDB/` — public package `treedb` (cached + backend-only behind one API)
- `TreeDB/db/` — backend B+Tree engine implementation
- `TreeDB/caching/` — cached write-back layer (memtable + WAL + flush)
- `TreeDB/internal/` — internal iterators, WAL format, lockfile, etc

