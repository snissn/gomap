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
- Background index vacuum: defaults to 30s interval (auto-on) with span ratio threshold `1_200_000`
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

### `Options.IteratorMutableMaxBytes` (cached mode; opt-in)

Allows iterators to read from mutable memtables without forcing a rotation when
the mutable size is small. This can reduce iterator-driven rotation overhead,
but can also block writers while an iterator holds a read lock on the mutable
memtable. Default is disabled (`<= 0`).

When to try it:
- Short-lived iterators or small range scans where rotation overhead dominates.
- Workloads with modest concurrent write pressure.

Risks:
- Long-lived iterators can block writers and reduce write throughput.

How to evaluate safely:
- Use a trace replay (e.g. `BenchmarkTraceReplayMemtableModes`) and a
  production-like run to measure both throughput and tail latency.
- Compare runs with and without `IteratorMutableMaxBytes` at a small threshold
  (1–4MB). Watch for stalls or reduced write throughput under heavy write load.

### Write concurrency (current limits)

TreeDB currently serializes write commits per DB handle (a single writer at a
time). This keeps WAL ordering and B+Tree root updates simple and safe, but it
means multi-core write throughput is gated by one writer lock.

Mitigations:
- Batch writes (`db.NewBatch`, `batch.Set`, `batch.Write`) to amortize lock hold
  time and reduce per-write overhead.
- Use cached mode (memtables + WAL) to absorb bursts and keep read latency
  stable under write-heavy workloads.

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

### Background index vacuum (cached or backend; default on)

TreeDB rebuilds the user index in the background when fragmentation
gets high. This restores scan locality and uses a short writer pause for the
final swap while the bulk of the work runs online.

The vacuum rewrites the index into `index.db.new` and swaps it in, so `index.db`
typically shrinks after a successful run. While the vacuum is running it will
temporarily consume additional disk space (old `index.db` + `index.db.new`), and
disk blocks from the previous mmap are reclaimed once all old
snapshots/iterators drain.

- Enable: `Options.BackgroundIndexVacuumInterval > 0` (default: 30s)
- Trigger threshold: `Options.BackgroundIndexVacuumSpanRatioPPM`
  - `0` uses the default (`1_200_000` ppm)
  - larger values make the vacuum less frequent
  - set interval `< 0` to disable

Stats keys:
- `treedb.bg_vacuum.*`

### Value Index (backend index; opt-in)

The Value Index adds an indirection layer for large (pointer-backed) values.
Instead of storing `Key -> ValuePtr` in the User tree, it stores `Key -> ValueID`
in the User tree and `ValueID -> ValuePtr` in a separate System tree.

Benefits:
- **Reduced index churn**: Moving value bytes (compaction/GC) only requires updating
  one Value Index entry instead of potentially many User tree keys.
- **Efficient GC**: Enables refcounted garbage collection of value segments.

Knobs:
- `Options.EnableValueIndex` (default: false): Enable the indirection layer.
- `Options.ForceValuePointers` (default: false): Force all values to be stored
  out-of-line (useful for testing or specialized workloads).

### Garbage Collection (backend index)

TreeDB supports refcounted garbage collection of unreachable value segments
(vlogs and slabs). The GC pass scans both the User tree and the Value Index to
identify segments that are no longer reachable from the latest root and any
pinned snapshots.

- Call: `db.GC()` (online or offline)
- CLI: `treemap gc <db-dir>` (offline)

Stats keys:
- `treedb.gc.*`

### Close-time maintenance (env)

TreeDB can run optional maintenance work at `DB.Close()` to keep on-disk state compact
without a separate offline tool. These hooks execute inline during shutdown.

- `TREEDB_CLOSE_CHECKPOINT=1`: call `Checkpoint()` before closing
- `TREEDB_CLOSE_COMPACT_INDEX=1`: call `CompactIndex()` before closing
- `TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1`: call `VacuumIndexOnline()` before closing
- `TREEDB_CLOSE_VACUUM_TIMEOUT`: timeout for the online vacuum (duration string or seconds)
- `TREEDB_CLOSE_LOG=1`: log close-maintenance start/stop messages
- `TREEDB_CLOSE_SCOPE_CONTAINS`: substring match on `Options.Dir` that scopes which DBs run
  close maintenance (default: all). Example: `TREEDB_CLOSE_SCOPE_CONTAINS=application.db`.

### Allocator locality and fragmentation knobs

TreeDB exposes a few knobs that directly influence page-ID locality and index
fragmentation under churn:

- `Options.PreferAppendAlloc` (default: false in durable profile, true in fast)
  - Bypasses freelist reuse and appends new pages to preserve scan locality.
  - Trades disk growth for better sequential layout; vacuum reclaims space later.
- `Options.FreelistRegionPages` + `Options.FreelistRegionRadius`
  - Biases freelist reuse toward pages near the most recent allocations.
  - Useful when you want reuse without scattering page IDs.
  - Default when `PreferAppendAlloc=false`: 8192 pages, radius 1.
  - Set radius < 0 to force-disable.
- `Options.LeafFillTargetPPM` / `Options.InternalFillTargetPPM`
  - Lowering fill targets reduces split churn and can slow re-fragmentation,
    at the cost of more pages.

Use `db.FragmentationReport()` to observe index span ratio and fill percentiles,
and let background index vacuum handle high-fragmentation recovery.

### Offline index vacuum (backend index)

TreeDB’s `index.db` grows in chunks and does not shrink in-place. Reclaiming
index disk space requires rewriting the index into a fresh file and swapping it
in.

TreeDB provides an **offline** rewrite operation (DB closed) that rebuilds
`index.db` into a fresh file and swaps it in using a crash-safe protocol:

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
