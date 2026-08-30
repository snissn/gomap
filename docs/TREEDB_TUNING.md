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
  - `MaxWALBytes`: defaults to 2 GiB (legacy name; journal and side-log bytes)

## Options

### `Options.Dir` (required)

DB directory containing:

- `index.db` (B+Tree index)
- `wal/` directory (redo journal segments)
- `value_vlog/` directory (persistent large-value segments)
- optional `leaf_vlog/` directory (outer leaf generations)
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

### Span-native flush admission (default auto)

`Options.FlushAdmissionPolicy` selects how TreeDB admits the span-native
flush/apply + backlog coalescing path:

- `FlushAdmissionPolicyAuto` is the default/unconfigured policy. It admits the
  measured hardware-aware span-native + backlog candidate with adaptive
  write-side outer-leaf cache admission when the low-concurrency and durability
  guardrails pass. The default worker count is the detected physical-core count,
  capped by `GOMAXPROCS` and a conservative upper bound of 8. If physical-core
  detection is unavailable, auto falls back to the existing `min(GOMAXPROCS, 8)`
  bound.
- `FlushAdmissionPolicyOff` is the immediate rollback policy. It force-disables
  span-native apply, backlog coalescing, and flush-apply concurrency.
- `FlushAdmissionPolicyExplicit` preserves caller-supplied knobs for experiments
  and explicit c4/c8/c16/cache-disabled comparisons.

`FlushApplyConcurrency`, `FlushApplySpanNative`, `FlushBacklogCoalescing`, and
`JournalLanes` remain available as explicit knobs. Under the default `auto`
policy, TreeDB normalizes the admitted candidate to a hardware-aware capped
worker count with `FlushApplyMinEntries=1`, `FlushApplyMinSpans=1`,
`FlushApplyMinBytes=1`, span-native enabled, backlog coalescing enabled, and
adaptive write-side cache admission. A configured apply-concurrency value is an
explicit override and is capped only by `GOMAXPROCS`. Default journal/value-log
lanes are coalescing-safe: hot/warm/cold generation uses three total lanes (one
hot, one warm, one cold), while generation-off cached mode defaults to one hot
lane; configured `JournalLanes` values are authoritative. The policy declines
low-concurrency/c1-shaped configurations and WAL-off unsafe durability. Leaf and
value-log output remain persistent storage; failed or abandoned apply attempts do
not publish roots, and unreachable prepared output is reclaimed only by
reachability-based maintenance.

Rollout guidance:

- Use the default auto policy for normal durable cached-mode workloads.
- Roll back immediately with `FlushAdmissionPolicyOff` or
  `-treedb-flush-admission-policy=off`; no data migration is required because
  the option changes only in-memory apply/cache policy. The expected support
  signature is `treedb.flush_admission.policy=off`,
  `admitted=false`, `reason=policy_off`,
  `flush_apply_concurrency=0`, `flush_apply_span_native=false`, and
  `flush_backlog_coalescing=false`.
- Use `FlushAdmissionPolicyExplicit` for non-default experiments such as c4 on
  large hosts, c16, immediate cache admission, or cache-disabled diagnostic rows.
- Watch `treedb.flush_admission.*`, `treedb.cache.journal_lanes.*`,
  `treedb.cache.memtable_shards`, `treedb.flush_apply.*`,
  `treedb.cache.flush_apply.*`, `prepared_output.*`, checkpoint wall times,
  allocation profiles, cache hit/store/eviction counters, and final
  `index.db`/`leaf_vlog` footprint. Reproducible regressions in read/scan
  throughput, checkpoint time, allocation volume, or footprint should trigger
  rollback or a narrower explicit policy.

Admission support triage:

| Stat shape | Interpretation | Expected action |
| --- | --- | --- |
| `policy=off`, `admitted=false`, `reason=policy_off`, selected concurrency `0`, span-native `false`, backlog `false` | Rollback is active. Caller-provided concurrency remains visible in `flush_apply_concurrency_configured` for reproduction, but the selected/effective path is serial. | Keep this when stabilizing production or bisecting; remove only after new evidence is accepted. |
| `policy=auto`, `admitted=false`, `reason=unsafe_durability`, selected concurrency `0`, span-native `false`, backlog `false` | Auto declined because WAL-off relaxed durability is enabled. Value-log storage remains persistent, but span-native/backlog admission currently requires the normal durability envelope. | Treat as a deliberate fail-closed state; use durable WAL or `explicit` only for an accepted unsafe experiment. |
| `policy=auto`, `admitted=false`, `reason=low_concurrency`, selected concurrency `0` | Auto saw c1 or another sub-c2 shape after `GOMAXPROCS` normalization. | Expected on low-concurrency hosts or c1 rows; no production admission unless the host/options can select at least c2. |
| `flush_apply_concurrency_configured=16`, `flush_apply_concurrency=8`, `flush_apply_concurrency_cap_reason=configured_gomaxprocs_cap`, `gomaxprocs=8` | Caller requested c16, but the effective worker pool is capped by `GOMAXPROCS`. | Benchmark/report rows must carry both values; raise `GOMAXPROCS` to test true c16. |

The current hardware-aware/coalescing-safe default formula and benchmark gate
commands are summarized in `docs/TREEDB_DEFAULT_POLICY_2996.md`.

M5/#2788 evidence is recorded in
`docs/TREEDB_SPAN_NATIVE_DEFAULT_GATE_M5_REPORT.md`. The reports for issues
#2819/#2832, #2899/#2907, and #2925 kept c8/c16 explicit while checkpoint and
leaf-log-lane blockers were still open. Post-#2960 evidence and the #2974 graph
supersede that c4 default decision for the admitted span-native apply path:
`docs/TREEDB_C8_DEFAULT_PROMOTION_2974.md` records the c8 default promotion,
maintenance gate, and rollback policy. Checkpoint pauses are still tracked by
the barrier/debt counters and remain a workload guardrail, not a claim that
checkpoint debt disappeared.
The #2949 LANE-M0 append-path inventory is recorded in
`docs/TREEDB_LEAF_LOG_APPEND_PATHS_LANE_M0_INVENTORY.md`; it keeps generic
batch/default widening as a future proof-gated policy decision and explicitly
excludes maintenance and recovery paths. The #2950 LANE-M1 default-policy design
is recorded in `docs/TREEDB_WIDE_LEAF_LOG_DEFAULT_POLICY_M1.md`; it proposes a
future normal cached non-maintenance batch/prepared-batch c4-capped default,
requires a direct leaf-log append-lane rollback knob before implementation, and
keeps single-page appends, maintenance/rewrite/leaf-pack/vacuum writers,
command WAL replay/recovery, standalone/direct-backend leaf logs,
cold-build/rebuild/bulk callers, backend-direct benchmark rows, and c8/c16 paths
out of the default policy.

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

TreeDB cached mode uses recovery logging, but (like many engines) the default
`Set`/`Batch.Write` path does not force an `fsync` per operation.

To bound reclaimable redo-journal and side-log pressure in long-running
workloads, TreeDB enables periodic cached-mode auto maintenance by default:

- `Options.BackgroundCheckpointInterval` (default 30s): periodic auto-maintenance cadence
- `Options.BackgroundCheckpointIdleDuration` (default 2s): opportunistic auto maintenance after write-idle
- `Options.MaxWALBytes` (default 2 GiB): safety cap that can trigger maintenance early.
  External command-WAL mode counts command-WAL segment bytes; other cached
  modes use reclaimable cached journal/value-log pressure (legacy name: WAL).

In external command-WAL mode, automatic passes rotate and clean only the
recovery-covered command-WAL prefix without publishing the current visible
frontier. Legacy or otherwise unwired backends fall back to a full checkpoint.
Uncovered segments can remain until a later durable frontier covers them.

An explicit `Checkpoint`, sync, or close:
- blocks writers briefly,
- records pending checkpoint debt and kicks the background drainer when that is
  safe for the durability mode,
- rotates to a fresh journal segment,
- flushes queued memtables to the index with `WriteSync`,
- trims old journal segments.

Useful checkpoint counters:

- `treedb.cache.checkpoint.debt_memtables_last/max`
- `treedb.cache.checkpoint.debt_bytes_last/max`
- `treedb.cache.checkpoint.barrier_wait_ns_total/max`
- `treedb.cache.checkpoint.flush_all.worker_passes_total`
- `treedb.cache.checkpoint.flush_all.workers_total/max`
- `treedb.cache.checkpoint.stage.*`

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

### Leaf value-log mmap growth

TreeDB maps current writable `leaf_vlog` files ahead to a bounded segment-scale
target so recently appended outer leaf pages stay on the mmap read path without
remapping on every append.

- `TREEDB_VLOG_CURRENT_WRITABLE_MMAP_TARGET_BYTES` sets the map-ahead target
  for current writable value-log files. The default is 32 MiB, matching the
  default leaf segment scale. Set to `0` to map only the current file size.
- `TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_BYTES` caps sealed `leaf_vlog` files kept
  on the mmap read path. The default is 2 GiB, after which older sealed leaf
  generations use `ReadAt` fallback to reduce process RSS high-water during
  long sync/restore workloads. Raise it to restore the older throughput-biased
  larger mmap window when memory headroom is not the limiting factor.
- `TREEDB_VLOG_MAX_DEAD_MAPPINGS` still caps retained stale mappings for safety;
  frequent current-writable remaps can hit this cap and force `ReadAt` fallback.

Useful stats:
- `treedb.vlog.mmap_current_writable_map_target_bytes`
- `treedb.vlog.mmap_max_mapped_leaf_sealed_bytes`
- `treedb.vlog.mmap_sealed_bytes`
- `treedb.vlog.mmap_remaps`
- `treedb.vlog.mmap_dead_mappings`
- `treedb.vlog.mmap_read.fallback_readat`
- `treedb.vlog.mmap_read.miss_dead_mapping_cap`

### Outer-leaf read cache

When outer leaf pages are stored in `leaf_vlog`, TreeDB keeps a bounded
process-local set-associative cache of decoded leaf pages. This avoids rereading
freshly written or repeatedly read leaves from mmap or `ReadAt` during follow-up
publish, update, read, and maintenance work.

- `Options.LeafPageReadCacheEntries` sets the cache entry count per DB. `0` uses
  the process default/env override, `<0` disables the cache, and `>0` sets an
  explicit entry count.
- `TREEDB_LEAF_PAGE_CACHE_ENTRIES` sets the process default entry count when the
  option is left at `0`. The default is 8192 entries, or about 32 MiB of
  leaf-page payloads. Set the env var to `32768` to restore the historical
  128 MiB cache when a workload has evidence that the larger cache improves
  throughput enough to justify the retained heap. Set the env var to `0` to
  disable the cache for DBs that do not set `Options.LeafPageReadCacheEntries`.
- Explicit and env-derived cache sizes are capped at 262144 entries to fail
  early with a clear configuration error instead of risking an accidental huge
  cache.
- `Options.LeafPageReadCacheWriteAdmission` controls write-side population of
  this read cache. The default/zero policy is `immediate`, preserving historical
  behavior. `LeafPageReadCacheWriteAdmissionAdaptive` is opt-in: it warms the
  bounded cache, samples cold write streams, re-admits when read hits prove the
  cache is hot, and skips rather than blocking on cache locks. Skipping affects
  only in-memory cache population; leaf-log/value-log records and pointers stay
  persistent, and read-miss admission can still populate the cache later.
- `unified-bench` exposes these as `-treedb-leaf-page-read-cache-entries` and
  `-treedb-leaf-page-read-cache-write-admission` so cache-capacity/admission
  experiments are captured in the reproduced command instead of relying on
  ambient environment.

Useful stats:
- `treedb.process.read_path.outer_leaf.cache.hits`
- `treedb.process.read_path.outer_leaf.cache.misses`
- `treedb.process.read_path.outer_leaf.cache.stores`
- `treedb.process.read_path.outer_leaf.cache.evictions`
- `treedb.process.read_path.outer_leaf.cache.entries`
- `treedb.process.read_path.outer_leaf.cache.capacity`
- `treedb.process.read_path.outer_leaf.cache.bytes`
- `treedb.process.read_path.outer_leaf.cache.write_admission_policy`
- `treedb.process.read_path.outer_leaf.cache.write_admission_attempts`
- `treedb.process.read_path.outer_leaf.cache.write_admission_stores`
- `treedb.process.read_path.outer_leaf.cache.write_admission_skips`
- `treedb.process.read_path.outer_leaf.cache.write_admission_lock_skips`

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
- `TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1`: call `VacuumIndexOnline()` before closing; unsupported platforms skip it without failing close
- `TREEDB_CLOSE_VACUUM_TIMEOUT`: timeout for the online vacuum (duration string or seconds)
- `TREEDB_CLOSE_LOG=1`: log close-maintenance start/stop messages
- `TREEDB_CLOSE_SCOPE_CONTAINS`: substring match on `Options.Dir` that scopes which DBs run
  close maintenance (default: all). Example: `TREEDB_CLOSE_SCOPE_CONTAINS=application.db`.

### Allocator locality and fragmentation knobs

TreeDB exposes a few knobs that directly influence page-ID locality and index
fragmentation under churn:

- `Options.PreferAppendAlloc` (default: false)
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

### Full storage compaction

For final disk footprint, use the high-level compaction path:

```go
stats, err := db.CompactStorage(ctx, treedb.CompactStorageOptions{
    Mode: treedb.CompactStorageFull,
})
```

or:

```sh
treemap compact <db-dir> -rw
```

This plans and executes the best-practice sequence across storage domains:

- `value_vlog`: rewrite live pointer-backed values, then GC fully unreachable segments.
- `leaf_vlog`: pack sparse live leaf generations, then GC fully unreachable generations.
- `index.db`: vacuum after leaf packing so rewritten roots/internal pages are reflected in the final file.
- cleanup: remove zero-byte `value_vlog` segment files before final storage accounting.

If online index vacuum is unsupported on the current platform, `CompactStorage`
records the `index-vacuum` phase as skipped and still completes the value-log,
leaf-log, and cleanup phases.

Use `treemap compact-plan <db-dir>` to preview remaining compaction debt without
mutating storage.

### Advanced low-level maintenance

The individual maintenance APIs remain available, but they are domain-specific
building blocks:

- `db.ValueLogGC(...)` deletes fully-unreferenced `value_vlog` segments only.
- `treedb.ValueLogRewriteOffline(...)` rewrites value-log pointers only.
- `db.LeafGenerationPack*` packs sparse `leaf_vlog` generations only.
- `db.LeafGenerationGC(...)` deletes fully unreachable `leaf_vlog` generations only.
- `treedb.VacuumIndexOffline(...)` or `treemap compact -scope=index -rw` only rebuilds `index.db`.

Do not manually chain these for benchmark storage numbers unless you are
debugging TreeDB internals; use full storage compaction instead.

Details: `docs/TREEDB_STORAGE_FORMAT.md`.

## Benchmark-Driven Tuning

TreeDB performance depends heavily on workload shape. Prefer tuning with:

- `./bin/unified-bench` (after `make unified-bench`)
- `./bin/unified-bench -suite readme -format markdown` for the reproducible
  cross-engine snapshot shape with environment metadata

Recommended “gate-style” suites:

- `./bin/unified-bench -suite flushthrash` (catches small-flush-threshold regressions)
- `./bin/unified-bench -suite longmix` (mixed churn + settle + scans, with fragmentation diagnostics)

See:
- `cmd/unified_bench/README.md`
- `docs/BENCHMARK_SPEC.md`
