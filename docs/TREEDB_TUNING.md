# TreeDB Tuning

TreeDB is a dev project, but a few knobs are already useful and stable.
This doc describes the knobs exposed via `treedb.Options` and the cached write-back layer.

## TL;DR Defaults

- `ChunkSize`: defaults to 256 MiB (mmap chunk size for `index.db`)
- `FlushThreshold`: defaults to 4 MiB in cached mode (memtable/WAL rotation threshold)
- `KeepRecent`: defaults to 10,000 (backend tuning knob)

## Options

### `Options.Dir` (required)

DB directory containing:

- `index.db` (backend index)
- slab/value files (backend)
- `wal/` directory (cached mode WAL segments)
- `LOCK` file (exclusive open)

### `Options.Mode`

- `treedb.ModeCached` (default): enable write-back layer.
- `treedb.ModeBackend`: backend-only engine (no cached write-back layer).

Decision guide: `docs/TREEDB_CACHED_VS_BACKEND.md`.

### `Options.ChunkSize`

Controls the mmap “chunk” size used by the pager for `index.db`.

Larger chunks:
- reduce remap churn for growing DBs,
- but can increase address-space usage.

### `Options.FlushThreshold` (cached mode)

Controls when cached mode rotates the active memtable/WAL and triggers background flush work.

Higher threshold:
- more batching and better throughput on random small writes,
- but higher peak memory/WAL footprint and potentially longer recovery (more WAL to replay).

Lower threshold:
- less memory/WAL footprint,
- but potentially lower write throughput due to more frequent flush work.

### `Options.KeepRecent` (backend engine)

Backend knob used to influence internal lifecycle/retention behavior.
If you’re changing this, you should validate it with `cmd/unified_bench` and TreeDB’s tests.

## Benchmark-Driven Tuning

TreeDB performance depends heavily on workload shape. Prefer tuning with:

- `./bin/unified-bench` (after `make unified-bench`)
- `make bench-readme` (reproducible suite; prints environment metadata)

See:
- `cmd/unified_bench/README.md`
- `docs/BENCHMARK_SPEC.md`

