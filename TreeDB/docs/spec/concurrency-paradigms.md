# TreeDB Concurrency Paradigms and Controls

This document is the canonical concurrency map for TreeDB.

It is intended to be detailed enough to:
- audit existing synchronization and worker design,
- identify redundant or overlapping mechanisms,
- support refactors and performance work,
- support reimplementation in another language.

TreeDB is pre-alpha; this reflects current implementation behavior.

## 1. Scope

This document covers concurrency in:
- public `treedb` wrapper (`TreeDB/public.go`, `TreeDB/bg_vacuum.go`),
- cached layer (`TreeDB/caching/*.go`),
- backend/index layer (`TreeDB/db/*.go`),
- process locking (`TreeDB/internal/lockfile/*`).

This includes:
- in-process synchronization (mutexes, atomics, condvars),
- worker/queue architectures (goroutines + channels),
- process-level lock semantics,
- options and flags that change concurrency behavior.

## 2. Concurrency Model by Mode

### 2.1 Backend-only mode (no cached layer)

Primary model:
- multi-reader via snapshots,
- effectively single committer at commit linearization points.

Details:
- read paths use `AcquireSnapshot()` and do not block each other,
- writes use optimistic apply plus commit serialization (`commitMu`) and fallback serialized path (`writeMu.Lock`),
- page reclamation is delayed by snapshot pinning.

### 2.2 Cached mode (default write path)

Primary model:
- concurrent foreground writers,
- sharded mutable memtables,
- lane-partitioned commit-log and value-log appends,
- asynchronous flush and maintenance workers.

Details:
- writes usually take `writeMu.RLock` (shared writer gate),
- per-shard and per-lane locks partition hot paths,
- checkpoints and close take exclusive write/flush barriers.

### 2.3 Read-only mode

Primary model:
- no in-process mutating workers,
- no replay or mutating recovery.

Details:
- opens read-only pager + value-log manager,
- acquires shared lock if available,
- all write APIs return `ErrReadOnly`.

## 3. Paradigm Inventory

### 3.1 Process-level locking

Mechanism:
- lock file with OS advisory locking (`flock`/`LockFileEx`) and in-process lock map.

Where:
- `TreeDB/internal/lockfile/lockfile.go`,
- `TreeDB/internal/lockfile/lockfile_unix.go`,
- `TreeDB/internal/lockfile/lockfile_windows.go`.

Behavior:
- RW open acquires exclusive lock (`Acquire`),
- RO open attempts shared lock (`AcquireShared`) and proceeds without mutation.

Primary control:
- `Options.ReadOnly`.

### 3.2 Lock-based serialization and coordination

Mechanisms:
- mutexes/RWMutex/condvars for critical sections and barriers.

Key lock families:
- cached and backend DB: `updateLocks[]` for single-key `Update`/`UpdateSync`
  read-modify-write serialization,
- cached DB global: `mu`, `writeMu`, `flushMu`, `checkpointMu`, `laneMu`, `bpMu`,
- cached per shard/lane: shard `mu`, lane `walMu`, lane `vlogMu`, `flushLaneMu[]`, `walFastMu`,
- backend DB: `mu`, `writeMu`, `commitMu`, `idxMu`, plus worker-local mutexes.

Representative barriers:
- checkpoint barrier uses `checkpointing` + `checkpointCond`,
- lane sync barrier uses lane `syncing` + `laneCond`,
- close path enforces lock order to avoid deadlock with auto-checkpoint.

### 3.3 Atomic/RCU state publication

Mechanisms:
- `atomic.Pointer`, atomic scalars, atomic booleans/counters.

Key uses:
- cached memtable view publication (`atomic.Pointer[memtableView]`) for lock-free read snapshots,
- backend index generation publication (`atomic.Pointer[indexGen]`),
- checkpoint in-progress bit, queue backlog counters, worker stats, lane live/closed bytes.

### 3.4 Work-queue + worker goroutines

Mechanism:
- producer/consumer queues (`chan`) with dedicated workers.

Cached workers:
- `flushLoop`,
- per-lane `walWriteLoop`,
- per-lane `walFastLoop`,
- per-lane `vlogWriteLoop` workers,
- per-lane optional `vlogDictPrepareLoop` workers,
- optional `autoCheckpointLoop`,
- optional `valueLogDictLoop` (dictionary trainer/profile apply loop).

Backend/public workers:
- backend `pruneWorker`,
- backend `indexGhostManager` scavenger loop,
- top-level `bgIndexVacuumWorker`.

### 3.5 Lane partitioning

Mechanism:
- writes are partitioned into `JournalLanes` lanes with per-lane WAL/vlog state and locks.

Effects:
- reduces contention vs single log writer,
- sync writes reserve lane with `syncing=true` until completion,
- flush can run lane-parallel with per-lane flush mutexes.

### 3.6 Sharding

Mechanism:
- mutable memtable is sharded; each shard has own mutex and table.

Effects:
- foreground writes on different keys can proceed concurrently,
- rotation/flush pipeline tracks per-shard metadata and queue ranges.

### 3.7 Optimistic concurrency + validation (backend batch writes)

Mechanism:
- optimistic write computes new root from captured root/seq under shared writer lock,
- commit phase validates root unchanged before finalize,
- fallback path uses serialized writer lock if optimism fails.

Where:
- `TreeDB/db/batch.go` (`writeOptimistic`, `writeSerialized`).

### 3.8 Snapshot pinning and generation lifecycle

Mechanism:
- snapshots register pinned commit sequence and hold index generation refs,
- retired pages are reclaimed only when unpinned and outside keep window,
- old index generations are ghosted then closed asynchronously.

Where:
- `TreeDB/db/db.go` (`AcquireSnapshot`),
- `TreeDB/db/index_gen.go`,
- `TreeDB/db/index_gen_db.go`,
- `TreeDB/db/pools.go` (`indexGhostManager`).

### 3.9 Backpressure control loops

Mechanisms:
- legacy queue-length thresholds,
- adaptive backlog-bytes thresholds based on flush throughput EWMA,
- writer-assisted bounded flush work,
- coordinator credits for bytes already claimed by an active flush/apply pass.

Where:
- `TreeDB/caching/db.go` (`waitForStop`, `maybeAssistFlush`, `flushSome`, `flushSomeBlocking`).

Active-flush coordination:
- flush passes publish active/in-flight/progress/error state for writer admission;
- foreground yielding is currently scoped to the opt-in parallel apply path
  (`FlushApplyConcurrency > 1`);
- stop-backpressure foreground assists may yield when an active flush has already
  claimed enough bounded in-flight bytes to bring non-claimed backlog below the
  stop threshold. Here non-claimed backlog means
  `max(queue_backlog_bytes - active_flush_in_flight_bytes, 0)`, and the yield is
  currently gated by `FlushApplyConcurrency > 1`;
- hard overload beyond active credits is counted as an explicit active-flush
  yield in the foreground path; blocking fallback is reserved for stalled
  `waitForStop` progress, while checkpoint/close continue to drain via
  `flushAllLocked`.

### 3.10 Background maintenance loops

Mechanisms:
- periodic and kicked workers for maintenance tasks.

Workers:
- auto-checkpoint (cached WAL bound),
- background prune (backend page reclaim),
- background index vacuum (public wrapper),
- dictionary profile application loop.

### 3.11 Single-key update serialization

Mechanism:
- cached and backend `DB.Update`/`DB.UpdateSync` hash the requested key into a
  fixed set of per-handle mutex stripes before running the `Get` + callback +
  point-write sequence.

Effects:
- concurrent logical updates to the same key on the same `DB` handle are
  serialized,
- point `Set`/`SetSync`/`Delete`/`DeleteSync` calls use the same single-key
  coordinator on the same handle,
- callers can implement conflict-safe single-key mutations, such as
  set-membership add/remove, without an external global lock,
- unrelated keys usually proceed independently, subject to stripe collisions and
  the lower cached/backend write-path locks.

Limits:
- this is not a multi-key transaction mechanism,
- point `Set`/`Delete` calls remain unconditional writes,
- batch writes do not acquire the single-key update coordinator,
- the update callback runs while the stripe lock is held and should not recurse
  into `Update` for the same key/stripe.

## 4. Lock and Barrier Topology

### 4.1 Cached layer lock roles

- `writeMu`:
  - shared for normal writes (`Set`, `Delete`, regular batch writes),
  - exclusive for checkpoint close/drain barriers.
- `flushMu`:
  - serializes flush and checkpoint flush phases.
- `mu`:
  - protects queue/mutable structure changes and range metadata.
- `checkpointMu` + `checkpointCond` + `checkpointing`:
  - blocks new writes during checkpoint critical window.
- `laneMu` + `laneCond`:
  - coordinates lane availability and sync-lane reservation.
- `bpMu`:
  - protects backpressure threshold state (`flushBpsEWMA` updates).

### 4.2 Lock-order constraints that must hold

Current order constraints:
- cached/backend `Update`/`UpdateSync` and point writes take an update stripe
  lock before entering lower write paths.
- `flushMu` before `checkpointMu` when both are needed.
- `Checkpoint` acquires `flushMu` then `writeMu`.
- `Close` mirrors `flushMu` then `writeMu` to avoid deadlock with auto-checkpoint.
- value-log flush/sync paths take `vlogMu` before writer ops to act as write barrier.

Any refactor that changes this order MUST preserve deadlock freedom.

Planned user-command WAL lock-order constraints add a command admission layer
above backend publish locks:

1. DB close/checkpoint admission gate,
2. collection manager domain registry when a command touches collections,
3. per-scope mutation serialization,
4. external-ref prepare/protection,
5. shared commit-log command append lock,
6. short per-domain state install/completion lock for current flush-boundary
   write-domain state,
7. backend publish/commit/checkpoint locks that select roots and
   `AppliedCommandLSN`.

Collection domain state locks must not be held while performing command WAL I/O,
waiting for async publish, registering long-running GC work, calling backend
publish, or calling backend checkpoint. `DB.Checkpoint` must invoke collection
drain/checkpoint hooks before taking backend locks that async publishers need, or
use a nonblocking protocol that cannot wait on those publishers while holding
the locks they need.

### 4.3 Backend lock roles

- `writeMu`:
  - serializes serialized write path and heavyweight maintenance paths.
- `commitMu`:
  - linearizes optimistic commit finalize after root validation.
- `mu`:
  - protects mutable DB metadata state (`meta`, state swap).
- `idxMu`:
  - protects index generation tracking map/lifecycle operations.

## 5. Worker and Queue Topology

### 5.1 Cached write and flush pipeline

High-level flow:
1. writers append journal/value-log entries (if enabled),
2. writers update mutable memtable shards,
3. rotations enqueue immutable memtables,
4. `flushLoop` or writer assist drains queue to backend,
5. checkpoint drains and trims old WAL segments.

Core queues/channels:
- `flushCh` (trigger flush passes),
- per-lane `walCh`,
- per-lane `vlogCh`,
- per-lane `vlogPrepCh` (optional).

### 5.2 WAL path

Per lane:
- `walWriteLoop` handles queued WAL requests with batching,
- `walFastLoop` handles fast-queue records guarded by condvar queue.

Durability modes at append boundary:
- none, flush, sync.

### 5.3 Value-log path

Per lane:
- one to four `vlogWriteLoop` workers (derived from `GOMAXPROCS` and lane count),
- optional dict-frame prep worker pool for dictionary frame preparation.

Direct vs queued append:
- sync durability always goes direct,
- non-sync may queue based on payload size and queueing state.

### 5.4 Background worker inventory

- cached: `flushLoop`, optional `autoCheckpointLoop`, optional `valueLogDictLoop`,
- backend: optional `pruneWorker`, always-on `indexGhostManager` scavenger,
- public: optional `bgIndexVacuumWorker`.

## 6. Option and Flag Matrix

This section maps concurrency-affecting controls to mechanisms.

### 6.1 Core open mode and process-lock controls

| Control | Effect on Concurrency |
| --- | --- |
| `Options.ReadOnly` | Disables write/maintenance mutation paths; opens RO pager and attempts shared lock. |
| `Options.Durability` | Switches WAL/sync semantics; affects sync barriers and whether cached auto-checkpoint loop is started when WAL is disabled. |
| `Options.PagerSyncConcurrency` | Sets parallel `msync` concurrency in pager sync path. |

### 6.2 Cached write admission, sharding, and backpressure controls

| Control | Effect on Concurrency |
| --- | --- |
| `Options.FlushThreshold` | Sets mutable-to-queue rotation threshold; drives flush pressure timing. |
| `Options.MemtableMode` | Chooses mutable structure implementation; indirectly changes hot-path lock hold behavior. |
| `Options.MemtableShards` | Sets number of mutable shards; increases/decreases write partitioning. |
| `Options.MaxQueuedMemtables` | Legacy queue-length backpressure limit (`<0` disables this mode). |
| `Options.SlowdownBacklogSeconds` | Adaptive slowdown threshold based on estimated flush work seconds. |
| `Options.StopBacklogSeconds` | Adaptive hard-stop threshold for writer blocking/assist. |
| `Options.MaxBacklogBytes` | Absolute adaptive backlog cap in bytes. |
| `Options.WriterFlushMaxMemtables` | Max memtables a writer assists per stall pass. |
| `Options.WriterFlushMaxDuration` | Time budget for writer-assisted flush per write. |

Note:
- In `treedb.Open`, if all adaptive backlog knobs are zero, defaults are applied:
  - slowdown `1s`,
  - stop `2s`,
  - max backlog `2GiB`.
- `MemtableShards <= 0` is derived from runtime `GOMAXPROCS` and normalized to a power of two.
- `MaxQueuedMemtables == 0` defaults to a threshold-scaled value times shard count.

### 6.3 Flush parallelism and backend micro-batching controls

| Control | Effect on Concurrency |
| --- | --- |
| `Options.FlushBuildConcurrency` | Worker count for parallel flush build path (`<=1` disables parallel build). |
| `Options.FlushBuildMinEntries` | Entry-count gate before enabling parallel build. |
| `Options.FlushBuildMinUnits` | Unit-count gate before enabling parallel build. |
| `Options.FlushBuildChunkCap` | Chunk size strategy for flush build workers. |
| `Options.FlushBuildChunkTargetBytes` | Adaptive chunk target bytes. |
| `Options.FlushBuildChunkMinBytes` | Adaptive chunk lower bound. |
| `Options.FlushBuildChunkMaxBytes` | Adaptive chunk upper bound. |
| `Options.FlushBuildPrefetchUnits` | Build prefetch depth for flush pipeline. |
| `Options.FlushApplyConcurrency` | COW apply worker-pool cap. Default `FlushAdmissionPolicyAuto` selects detected physical cores capped by `GOMAXPROCS` and 8 for the admitted span-native/backlog path (falling back to `min(GOMAXPROCS, 8)` when physical cores are unknown); `<=1` disables the worker-pool path under explicit policy. |
| `Options.FlushApplyMinEntries` | Planned span-op gate before enabling opt-in parallel apply. |
| `Options.FlushApplyMinSpans` | Planned leaf-span gate before enabling opt-in parallel apply. |
| `Options.FlushApplyMinBytes` | Planned span-byte gate before enabling opt-in parallel apply. |
| `Options.FlushBackendMaxEntries` | Backend apply chunk size during flush; TreeDB stages chunks in one private root build. |
| `Options.FlushBackendMaxBatches` | Max backend apply chunks per flush; only the final complete root is publication-eligible. |

Notes:
- `FlushBuildConcurrency <= 0` defaults to `GOMAXPROCS`.
- `FlushBuildMinEntries <= 0` defaults to `16k`.
- `FlushBuildMinUnits <= 0` defaults to `2`.
- `FlushApplyConcurrency` is enabled by the default auto admission policy for durable cached-mode opens that pass guardrails. Auto selects a physical-core-aware worker count capped by `GOMAXPROCS` and 8, and remains capped by read-only leaf-span planning.
- Roll back the default with `FlushAdmissionPolicyOff` / `-treedb-flush-admission-policy=off`; use `FlushAdmissionPolicyExplicit` or a configured apply concurrency for c4/c8/c16 experiments.
- With value-log-backed outer leaves, workers may prepare block/dict
  grouped-frame bodies outside the leaf-log append mutex, but the durable append
  stream remains serialized per leaf-log lane. Published roots install leaf-log
  pointers only after guarded commit; failed/retried applies leave
  durable-but-unreachable output that is accounted as prepared/abandoned output
  and remains subject to reachability-based leaf-generation GC/rewrite. Segments
  must not be deleted by age.

### 6.4 Lane/log controls

| Control | Effect on Concurrency |
| --- | --- |
| `Options.JournalLanes` | Number of active WAL/vlog lanes (partition factor for append paths). |
| `Options.WALMaxSegmentBytes` | Segment rotation pressure; changes append/rotation cadence. |
| `Options.JournalCompression` | Generic journal/commitlog CPU/IO tradeoff; strict V2 command-WAL frames remain raw and are not compressed. |
| `Options.ValueLog.PointerThreshold` | Changes fraction of writes routed through value-log workers. |
| `Options.ValueLog.ForcePointers` | Forces value-log path for all puts; increases vlog lane pressure. |

Notes:
- `JournalLanes <= 0` uses the coalescing-safe default: hot/warm/cold generation opens three total lanes (one hot, one warm, one cold), while generation-off cached mode opens one hot lane.
- Configured `JournalLanes` values are authoritative for explicit lane experiments.
- Open can raise active lane count to match existing on-disk lane IDs.
- Max supported lanes is 255.

### 6.5 Checkpoint and background maintenance controls

| Control | Effect on Concurrency |
| --- | --- |
| `Options.BackgroundCheckpointInterval` | Enables/controls periodic auto-checkpoint trigger. |
| `Options.BackgroundCheckpointIdleDuration` | Enables/controls idle-triggered auto-checkpoint. |
| `Options.MaxWALBytes` | Enables size-triggered auto-checkpoint threshold. |
| `Options.DisableBackgroundPrune` | Keeps prune on commit path instead of worker thread. |
| `Options.PruneInterval` | Background prune worker tick frequency. |
| `Options.PruneMaxPages` | Bounded work per prune tick. |
| `Options.PruneMaxDuration` | Time budget per prune tick. |
| `Options.BackgroundIndexVacuumInterval` | Enables periodic online vacuum worker in public wrapper. |
| `Options.BackgroundIndexVacuumSpanRatioPPM` | Vacuum trigger threshold for span ratio. |
| `Options.ValueLog.DictTrain` | `TrainBytes > 0` enables dict trainer + dict apply loop goroutine. |

Notes:
- Background prune defaults: interval `250ms`, max pages `4096`, max duration `25ms`.
- Cached auto-checkpoint defaults: interval `30s`, idle trigger `2s`, size trigger `2GiB`.
- Background index vacuum defaults: interval `30s`, span ratio threshold `1_200_000` ppm.
- Background index vacuum starts only for writable supported-platform opens; a negative interval disables it. Close cancels and joins an active pass.
- Expected cutover races are recorded as retry outcomes. Unsupported capability quiesces the worker, while permanent failures remain visible through stats and `NotifyError` without repeating on an unchanged state.

### 6.6 Environment flags that change maintenance sequencing

These are not `Options`, but they affect runtime maintenance concurrency on close:
- `TREEDB_CLOSE_CHECKPOINT`,
- `TREEDB_CLOSE_COMPACT_INDEX`,
- `TREEDB_CLOSE_VACUUM_INDEX_ONLINE`,
- `TREEDB_CLOSE_VACUUM_TIMEOUT`,
- `TREEDB_CLOSE_SCOPE_CONTAINS`.

## 7. Observability for Concurrency Audits

Key stats for concurrency/throughput diagnosis:
- cached queue/backpressure:
  - `treedb.cache.queue_len`,
  - `treedb.cache.queue_backlog_bytes`,
  - `treedb.cache.flush_bps_ewma`,
  - `treedb.cache.backpressure_mode`.
- cached checkpoint:
  - `treedb.cache.auto_checkpoint.*`.
- flush batching:
  - `treedb.cache.stats.backend_write_batches_total`.
- flush coordinator/backpressure:
  - `treedb.cache.flush_apply.coordinator.active`,
  - `treedb.cache.flush_apply.coordinator.in_flight_bytes`,
  - `treedb.cache.flush_apply.coordinator.progress_total`,
  - `treedb.cache.flush_apply.coordinator.active_assist_skips_total`,
  - `treedb.cache.flush_apply.coordinator.stall_waits_total`,
  - `treedb.cache.flush_apply.coordinator.blocking_fallbacks_total`,
  - `treedb.cache.flush_apply.coordinator.hard_overload_fallbacks_total`.
- backend prune:
  - `treedb.prune.*`.
- background vacuum:
  - `treedb.bg_vacuum.*`.
- write-path and durability mode:
  - `treedb.write_path.*`,
  - `treedb.durability_mode`.

## 8. Review Hotspots for Redundancy or Reorganization

These are intended as review prompts for performance/refactor work.

### 8.1 Multiple overlapping write paths

- Cached path has `Set/Delete`, regular batch write, bypass batch write, and WAL-off streaming bypass.
- WAL has both `walWriteLoop` and `walFastLoop`.

Questions:
- Can fast and regular WAL paths be unified?
- Can bypass and regular batch paths share more code without regressing hot paths?

### 8.2 Multiple flush implementations

- `flushLaneOnce` and `flushOneLocked` are both present.
- Parallel and non-parallel build branches are separate and complex.

Questions:
- Can flush code paths be collapsed while preserving performance specializations?
- Can lane-parallel orchestration be simplified without losing throughput?

### 8.3 Dual backpressure models

- Legacy queue-length and adaptive backlog-bytes models coexist.

Questions:
- Is one model sufficient?
- Should one mode be formally deprecated to reduce tuning complexity?

### 8.4 Background loop proliferation

Active loops can include:
- flush,
- auto-checkpoint,
- dict trainer/apply,
- prune,
- bg vacuum,
- ghost scavenger.

Questions:
- Should maintenance loops be unified under a shared scheduler?
- Are there avoidable wakeups/ticker overlaps under low activity?

## 9. Concurrency Invariants for Refactors/Reimplementations

A compatible implementation SHOULD preserve:
1. process-level single RW opener semantics,
2. snapshot pinning semantics for readers,
3. no pointer publication before value bytes are readable at required durability boundary,
4. checkpoint barrier semantics (writers blocked in checkpoint critical window),
5. prune/vacuum safety under pinned readers,
6. deadlock-free lock ordering for checkpoint/close/flush interactions.

## 10. Existing Concurrency-Relevant Tests

Representative coverage includes:
- `TreeDB/update_test.go`,
- `TreeDB/db/concurrent_write_test.go`,
- `TreeDB/db/race_test.go`,
- `TreeDB/db/vacuum_panic_test.go`,
- `TreeDB/db/vacuum_online_swap_test.go`,
- `TreeDB/caching/backpressure_invariants_test.go`,
- `TreeDB/caching/backpressure_wait_test.go`.

When changing concurrency behavior, these tests SHOULD be reviewed/expanded with the change.
