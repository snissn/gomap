# TreeDB Tuning

TreeDB is a dev project, but a few knobs are already useful and stable.
This doc describes the knobs exposed via `treedb.Options` and the cached write-back layer.

## TL;DR Defaults

- `ChunkSize`: defaults to 256 KiB in `treedb.Open` (mmap chunk size for `index.db`)
- `FlushThreshold`: defaults to 64 MiB in cached mode (memtable/journal rotation threshold)
- `KeepRecent`: defaults to `1` in `treedb.Open` (aggressive page reuse)
- Inline values: values up to 512 bytes are stored inline; larger values go to the value log.
- Background index vacuum: defaults to 30s interval (auto-on) with span ratio threshold `1_200_000`
- Cached-mode auto checkpointing:
  - `BackgroundCheckpointInterval`: defaults to 30s
  - `BackgroundCheckpointIdleDuration`: defaults to 2s
  - `MaxWALBytes`: defaults to 2 GiB (legacy name; journal/value-log bytes)

## Options

### `Options.Dir` (required)

DB directory containing:

- `index.db` (B+Tree index)
- `wal/` directory (journal + value-log segments)
- `LOCK` file (exclusive open)

### `Options.ChunkSize`

Controls the mmap “chunk” size used by the pager for `index.db`.

Larger chunks:
- reduce remap churn for growing DBs,
- but can increase address-space usage.

Side stores:
- `Options.DictDBChunkSize` controls the chunk size for `dictdb/` (default 64KiB).
- `Options.TemplateDBChunkSize` controls the chunk size for `templatedb/` (default 64KiB).

These are intentionally independent of `Options.ChunkSize` so you can tune the
main index pager without inflating side-store disk usage.

### `Options.FlushThreshold` (cached mode)

Controls when cached mode rotates the active memtable/journal and triggers background flush work.

Higher threshold:
- more batching and better throughput on random small writes,
- but higher peak memory/journal footprint and potentially longer recovery (more journal to replay).

Lower threshold:
- less memory/journal footprint,
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

When flushing a combined batch (multiple immutable memtables in one commit), TreeDB can
optionally build the `SetOps` batch in parallel:

- `<=1` disables parallelism (default).
- `>1` uses up to that many goroutines to build per-memtable ops, then concatenates in queue order
  (oldest → newest) to preserve “newest wins” semantics.

### Read latency under flush debt (cached mode)

TreeDB cached mode absorbs bursts by buffering writes in memtables and flushing
them asynchronously. If a workload does a heavy write phase and then immediately
starts doing point reads, reads may run while there is still a backlog of queued
immutable memtables (“flush debt”).

How to diagnose:

- Enable cache stats in your harness and watch:
  - `treedb.cache.queue_len`
  - `treedb.cache.queue_backlog_bytes`
  - `treedb.cache.flush_bps_ewma`
  - `treedb.cache.memtable_mode`
- In `cmd/unified_bench`, use:
  - mixed workload (demonstrates debt): default ordering, optionally `-treedb-cache-stats-before-reads`
  - settled reads/scans: `-checkpoint-between-tests` or `-settle-before-scans`

What to do about it:

- If your workload has a clear phase boundary (ingest → query), call `Checkpoint()`
  once before switching to read-heavy work.
- If your workload is mixed and read-latency-sensitive, keep debt bounded by tuning:
  - backpressure knobs (`SlowdownBacklogSeconds`, `StopBacklogSeconds`, `MaxBacklogBytes`)
  - `MaxQueuedMemtables` (queue-length policy)
  - `FlushBuildConcurrency` (faster flush batch construction on multi-core)
  - `FlushThreshold` (trade batching/throughput vs memory/debt footprint)

Note on memtable sharding:

- With `MemtableShards > 1`, `queue_len` counts per-shard immutables, so a queue
  length of 40 may correspond to only ~5 rotations at 8 shards.

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
time). This keeps journal ordering and B+Tree root updates simple and safe, but it
means multi-core write throughput is gated by one writer lock.

Mitigations:
- Batch writes (`db.NewBatch`, `batch.Set`, `batch.Write`) to amortize lock hold
  time and reduce per-write overhead.
- Use cached mode (memtables + journal) to absorb bursts and keep read latency
  stable under write-heavy workloads.

### Cached-mode auto checkpointing (cached wrapper)

TreeDB cached mode uses a journal for crash recovery, but (like many engines) the default
`Set`/`Batch.Write` path does not force an `fsync` per operation.

To keep `wal/` (journal + value log) from growing without bound in long-running workloads, TreeDB enables a
periodic cached-mode checkpoint by default:

- `Options.BackgroundCheckpointInterval` (default 30s): periodic checkpoint cadence
- `Options.BackgroundCheckpointIdleDuration` (default 2s): opportunistic checkpoint after write-idle
- `Options.MaxWALBytes` (default 2 GiB): safety cap that can trigger checkpointing early
  based on cached journal/value-log segment bytes (legacy name: WAL).

A checkpoint:
- blocks writers briefly,
- rotates to a fresh journal segment,
- flushes queued memtables to the index with `WriteSync`,
- trims old journal segments.

Tuning/disable:
- Set `BackgroundCheckpointInterval < 0` to disable periodic checkpoints.
- Set `BackgroundCheckpointIdleDuration < 0` to disable the idle trigger.
- Set `MaxWALBytes < 0` to disable the size trigger.
- To disable auto-checkpointing entirely, set all three to `< 0`.

### Value-log dictionary compression (cached mode; opt-in)

TreeDB can optionally train and apply Zstandard dictionaries to value-log **frames**
to reduce disk bytes for repetitive / structured value streams.

Enable (recommended helper):

```go
opts := treedb.Options{Dir: "./my-db-data"}
treedb.EnableValueLogDictCompression(&opts)
db, err := treedb.Open(opts)
```

Advanced tuning is exposed via:
- `Options.ValueLog.DictTrain` (training budget + sampling knobs)
- `Options.ValueLog.CompressionAutotune` (when to switch dict/K candidates)
- `Options.ValueLog.DictMaxK` (clamp max grouped-frame K; default 32)
- `Options.ValueLog.DictFrameEncodeLevel` (zstd level for dict-compressed frames; default `SpeedFastest`)
- `Options.ValueLog.DictFrameEnableEntropy` (enable entropy coding; higher ratio, lower throughput)

Notes / gotchas:
- Dict compression only affects values stored in the value log. If most of your values are inline
  (small), you may see no change unless you lower `ValueLog.PointerThreshold` or set
  `ValueLog.ForcePointers=true`.
- Dict training is CPU-heavy and is disabled by default.
- Trained dictionaries are persisted in `dictdb/` and used to decode values after reopen.

### Leaf key compression (`Options.LeafPrefixCompression`)

TreeDB can compress keys stored in **leaf pages** using a front-coding scheme
with restart points. This reduces `index.db` size and can improve cache
locality, which often helps write throughput (fewer splits / less rewrite work).

Behavior:
- Restart points are written at a fixed interval (every Nth key), so seeking can
  binary-search restart entries and then scan within a small block.
- Pointer/tombstone entries avoid storing inline value-length metadata in the
  leaf payload.

Tradeoffs:
- Extra CPU to reconstruct keys during seeks/iteration (restart points bound the
  work).
- Can be combined with `Options.IndexColumnarLeaves`; when both are enabled,
  TreeDB uses a combined **columnar+prefix** leaf encoding (front-coded keys +
  explicit key/value offsets).

How to evaluate:
- Disk + throughput: `cmd/unified_bench` with `-treedb-leaf-prefix-compression`
  on/off and compare `maindb/index.db` size + ops/sec.
- Leaf density microbench: `go test ./TreeDB/node -run '^$' -bench BenchmarkLeafPageDensity -benchmem -count=1`

### Internal base-delta compression (`Options.IndexInternalBaseDelta`)

Internal pages can be encoded with base-child + delta child IDs and separator
prefix coding.

Current strict behavior:
- when enabled, internal pages are encoded as base-delta for eligible pages
  (no heuristic downgrade to plain internal pages),
- child-ID deltas are adaptive per page: `u16` when representable, `u32`
  otherwise,
- if a page's child-ID range exceeds `u32` representability, page build returns
  an explicit error (no silent fallback).

### `Options.KeepRecent`

Backend knob used to influence internal lifecycle/retention behavior.
If you’re changing this, you should validate it with `cmd/unified_bench` and TreeDB’s tests.

### Background pruning

TreeDB reclaims retired index pages asynchronously to keep commit latency stable under churn.

- `Options.DisableBackgroundPrune` keeps pruning on the commit path (legacy behavior).
- `Options.PruneInterval`, `Options.PruneMaxPages`, `Options.PruneMaxDuration` bound the worker.

Stats keys:
- `treedb.prune.*`

### Background index vacuum (default on)

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

### Close-time maintenance (env)

TreeDB can run optional maintenance work at `DB.Close()` to keep on-disk state compact
without a separate offline tool. These hooks execute inline during shutdown.

- `TREEDB_CLOSE_CHECKPOINT=1`: call `Checkpoint()` before closing
- `TREEDB_CLOSE_COMPACT_INDEX=1`: call `CompactIndex()` before closing
- `TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1`: call `VacuumIndexOnline()` (alias to `CompactIndex`) before closing
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

### Offline index vacuum (index.db)

TreeDB’s `index.db` grows in chunks and does not shrink in-place. Reclaiming
index disk space requires rewriting the index into a fresh file and swapping it
in.

TreeDB provides an **offline** rewrite operation (DB closed) that rebuilds
`index.db` into a fresh file and swaps it in using a crash-safe protocol:

- Call: `treedb.VacuumIndexOffline(treedb.Options{Dir: ..., ChunkSize: ...})`
- Requires the database to be **closed** (it acquires the exclusive `LOCK` under `Dir/maindb/LOCK`)
- Crash safety: `treedb.Open` will automatically recover from a partial swap
  (e.g. if the process crashed mid-vacuum).

### Value-log maintenance (GC / rewrite)

TreeDB’s value log is persistent storage. Disk space is reclaimed via:

- **GC**: `db.ValueLogGC(...)` deletes fully-unreferenced value-log segments (after a checkpoint in cached mode).
- **Rewrite**: `treedb.ValueLogRewriteOffline(treedb.Options{Dir: ...})` rewrites live pointers into fresh segments and swaps `Dir/maindb/index.db` to reference the new log (offline; requires a clean commitlog).

Operationally, the recommended post-sync entrypoint is:

- `treemap vlog-postsync-optimize <db-dir> -rw -strategy=auto`
  - prefers the offline rewrite path when the DB is closed/clean enough to run it,
  - falls back to the bounded explicit exit-maintenance loop only when offline rewrite cannot run (for example because the commitlog is not clean).

The explicit path is also available directly when you want bounded incremental reclaim instead of a full offline rewrite:

- `treemap vlog-maint-exit-loop <db-dir> -rw`
  - runs batched `stage-confirm-exit` maintenance passes,
  - is most useful when you need a post-sync reclaim step without immediately doing a full offline rewrite,
  - but typically reclaims less space than offline rewrite.

Details: `docs/TREEDB_STORAGE_FORMAT.md`.

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
