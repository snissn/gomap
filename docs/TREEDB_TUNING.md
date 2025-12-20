# TreeDB Tuning

TreeDB is a dev project, but a few knobs are already useful and stable.
This doc describes the knobs exposed via `treedb.Options` and the cached write-back layer.

## TL;DR Defaults

- `ChunkSize`: defaults to 64 MiB in `treedb.Open` (mmap chunk size for `index.db`)
- `FlushThreshold`: defaults to 64 MiB in cached mode (memtable/WAL rotation threshold)
- `KeepRecent`:
  - cached mode (`treedb.Open`): defaults to `1` (aggressive page reuse)
  - backend mode (`treedb.OpenBackend`): defaults to `10,000`
- Inline values: values up to 256 bytes are stored inline; larger values go to slabs (`data-*.slab`)
- Cached-mode auto checkpointing:
  - `BackgroundCheckpointInterval`: defaults to 30s
  - `BackgroundCheckpointIdleDuration`: defaults to 2s
  - `MaxWALBytes`: defaults to 2 GiB

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

### `Options.MaxQueuedMemtables` (cached mode)

Controls how many immutable memtables may be queued for flush before applying backpressure.

- `0` uses a default derived from `FlushThreshold` (targets a roughly stable backlog in bytes).
- `<0` disables backpressure entirely (not recommended in production).

### Adaptive backpressure (cached mode)

Cached mode enables adaptive backpressure by default in `treedb.Open`:
- `SlowdownBacklogSeconds=1`
- `StopBacklogSeconds=2`
- `MaxBacklogBytes=2GiB`

If any of these are non-zero, cached mode switches from a queue-length limit to a bytes-based
backpressure policy driven by an estimated flush throughput. Set all three to `0` to keep the
queue-length policy (or pass negative values, which are treated as `0` in `treedb.Open`).

- `Options.SlowdownBacklogSeconds`
- `Options.StopBacklogSeconds`
- `Options.MaxBacklogBytes`

Related bounds for writer-assisted flush work:

- `Options.WriterFlushMaxMemtables`
- `Options.WriterFlushMaxDuration`

### `Options.FlushBuildConcurrency` (cached mode)

When flushing a combined batch (multiple immutable memtables in one backend commit), TreeDB can
optionally build the `SetOps` batch in parallel:

- `<=1` disables parallelism (default).
- `>1` uses up to that many goroutines to build per-memtable ops, then concatenates in queue order
  (oldest → newest) to preserve “newest wins” semantics.

### Cached-mode auto checkpointing (cached wrapper)

TreeDB cached mode uses a WAL for crash recovery, but (like many engines) the default
`Set`/`Batch.Write` path does not force an `fsync` per operation.

To keep `wal/` from growing without bound in long-running workloads, TreeDB enables a
periodic cached-mode checkpoint by default:

- `Options.BackgroundCheckpointInterval` (default 30s): periodic checkpoint cadence
- `Options.BackgroundCheckpointIdleDuration` (default 2s): opportunistic checkpoint after write-idle
- `Options.MaxWALBytes` (default 2 GiB): safety cap that can trigger checkpointing early

A checkpoint:
- blocks writers briefly,
- rotates to a fresh WAL segment,
- flushes queued memtables to the backend with `WriteSync`,
- trims old WAL segments.

Tuning/disable:
- Set `BackgroundCheckpointInterval < 0` to disable periodic checkpoints.
- Set `BackgroundCheckpointIdleDuration < 0` to disable the idle trigger.
- Set `MaxWALBytes < 0` to disable the size trigger.
- To disable auto-checkpointing entirely, set all three to `< 0`.

### `Options.KeepRecent` (backend engine)

Backend knob used to influence internal lifecycle/retention behavior.
If you’re changing this, you should validate it with `cmd/unified_bench` and TreeDB’s tests.

### Background pruning (backend engine)

TreeDB reclaims retired index pages asynchronously to keep commit latency stable under churn.

- `Options.DisableBackgroundPrune` keeps pruning on the commit path (legacy behavior).
- `Options.PruneInterval`, `Options.PruneMaxPages`, `Options.PruneMaxDuration` bound the worker.

Stats keys:
- `treedb.prune.*`

### Background slab compaction (cached wrapper; optional)

TreeDB can optionally run slab compaction in the background (off by default):

- Enable: `Options.BackgroundCompactionInterval > 0`
- Selection knobs:
  - `Options.BackgroundCompactionMaxSlabs`
  - `Options.BackgroundCompactionDeadRatio`
  - `Options.BackgroundCompactionMinBytes`
- Apply/copy knobs:
  - `Options.BackgroundCompactionMicroBatch`
  - `Options.BackgroundCompactionCopyBytesPerSec`
  - `Options.BackgroundCompactionCopyBurstBytes`
  - `Options.BackgroundCompactionRotateBeforeWrite`

Stats keys:
- `treedb.bg_compaction.*`

### Offline index vacuum (backend index)

TreeDB’s `index.db` is **append-only** at the file level: it grows in chunks and never shrinks.
After heavy churn, this can leave `index.db` much larger than the live tree needs, and page IDs
can become scattered (hurting scan locality).

TreeDB provides an **offline** rewrite operation that rebuilds `index.db` into a fresh file and
swaps it in using a crash-safe protocol:

- Call: `treedb.VacuumIndexOffline(treedb.Options{Dir: ..., ChunkSize: ...})`
- Requires the database to be **closed** (it acquires the exclusive `LOCK` for `Options.Dir`)
- Crash safety: `treedb.Open`/`treedb.OpenBackend` will automatically recover from a partial swap
  (e.g. if the process crashed mid-vacuum).

## Benchmark-Driven Tuning

TreeDB performance depends heavily on workload shape. Prefer tuning with:

- `./bin/unified-bench` (after `make unified-bench`)
- `make bench-readme` (reproducible suite; prints environment metadata)

Recommended “gate-style” suites:

- `./bin/unified-bench -suite flushthrash` (catches small-flush-threshold regressions)
- `./bin/unified-bench -suite longmix` (mixed churn + settle + scans, with fragmentation diagnostics)

See:
- `cmd/unified_bench/README.md`
- `docs/BENCHMARK_SPEC.md`
