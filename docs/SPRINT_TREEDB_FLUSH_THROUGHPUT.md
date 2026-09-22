# TreeDB Sprint: Flush Throughput (Cached Memtables)

Date: 2026-01-27

This file is the sprint source of truth:
- the full plan (checklist)
- a running work log (timestamped)
- baseline + final results and follow-ups

## Goals / Success Criteria

- Primary: reduce “flush debt drain time” (time to reach a durable boundary after write bursts).
- Primary metric (preferred): `cmd/unified_bench -checkpoint-between-tests` checkpoint durations after write-heavy phases.
  - If this is too noisy, fall back to a dedicated `-suite flushdrain` metric and/or `treedb.cache.flush_bps_ewma`.
- No consistent regressions in the existing unified_bench ops/sec rows (treat >~1–2% as suspicious unless explained).
- No correctness regressions: `go test ./...` passes.

## Plan (checklist)

### Phase 1 — Measurement
- [ ] Baseline on perf server:
  - mixed: `go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 -test all -treedb-cache-stats-before-reads -progress=false`
  - settled: same + `-checkpoint-between-tests`
- [x] Local baseline (scaled down): capture `keys=200000` mixed + settled results for `fast` and `wal_on_fast` while perf server access is unavailable.
- [x] Add a dedicated flush-drain suite to `cmd/unified_bench` (`-suite flushdrain`) and document it.
- [ ] Enable targeted profiling:
  - `-cpuprofile` + `-cpuprofile-tests=random_write,random_read` (and any flush-drain step)
  - example: `go run ./cmd/unified_bench -suite flushdrain -dbs treedb -profile fast -keys 900000 -cpuprofile /tmp/flushdrain.pprof -cpuprofile-tests=random_write,random_read -progress=false`
  - optional (more detail): `TREEDB_DEBUG_FLUSH_TIMING=1 ...` to print a flush stage breakdown to stderr

### Phase 2 — Improvements (caching flush pipeline)
- [x] Add optional internal timing breakdown for flush stages (env-gated).
- [x] Reduce flush build overhead:
  - avoid large transient allocations by streaming ops directly into the backend batch (`SetView`/`DeleteView`/`SetPointerView`) instead of materializing `[]batch.Entry`
  - avoid per-flush heap allocations for combined flush snapshots (`mems`/`ranges`/`walPaths`/`units` on stack for up to 32 memtables)
  - keep combined-flush latency bounded under small flush thresholds (see flushthrash CI note below)
- [x] Evaluate multi-core flush building cost (existing `FlushBuildConcurrency`):
  - perf server `-suite flushdrain` shows backend write dominates checkpoint time; parallel build yields only minor improvements
- [ ] If warranted by profiling: pipeline ops building and backend commit (without parallel backend commits).

### Acceptance gates
- [x] `go test ./...`
- [ ] unified_bench: no regressions in the main table; checkpoint/drain time improves measurably.

## Work log

- 2026-01-27: Created sprint tracker (`docs/SPRINT_TREEDB_FLUSH_THROUGHPUT.md`).
- 2026-01-27: Added `cmd/unified_bench -suite flushdrain` (write burst → checkpoint → read) to make drain-time comparisons easier.
- 2026-01-27: Added `TREEDB_DEBUG_FLUSH_TIMING=1` to print flush stage timing breakdowns from the caching layer.
- 2026-01-27: Prototype: reduced flush build allocations via bulk `SetOps` slices (later replaced by streaming view ops due to flushthrash stall).
- 2026-01-27: Fixed CI cancellation in `-suite flushthrash` by (a) lowering the combined-flush target when `FlushThreshold` is small and (b) streaming into backend batches via `SetView`/`DeleteView`/`SetPointerView` (no `[]batch.Entry` materialization).
- 2026-01-27: Reduced per-flush heap allocations in the combined-flush snapshot path (stack arrays for up to 32 memtables).
- 2026-01-27: Added regression test covering combined-flush bulk path (`totalLen > 2000`) to ensure queued memtables persist.
- 2026-01-27: Ran local `keys=200000` mixed + settled unified_bench comparisons (perf server SSH timed out); captured a candidate hang at `keys=900000` (see below) and recorded results for PR review.
- 2026-01-27: (WIP) Added `cmd/unified_bench` flag `-treedb-flush-build-concurrency` and implemented an order-preserving parallel combined-flush build path (chunked) behind `Options.FlushBuildConcurrency > 1`.
- 2026-01-27: Perf server: ran `-suite flushdrain` with `FlushBuildConcurrency={1,4,8}`; observed only small checkpoint-time improvements (backend write dominates). Captured `TREEDB_DEBUG_FLUSH_TIMING=1` stage breakdown.
- 2026-01-27: Added flush-build tuning knobs (`FlushBuildMinEntries`, `FlushBuildMinUnits`, `FlushBuildChunkCap`, `FlushBuildPrefetchUnits`) and pager tuning knobs (`PagerSyncConcurrency`, `ChunkSize`) with unified-bench flags.
- 2026-01-27: Added `TREEDB_DEBUG_COMMIT_TIMING=1` to break down commit sync costs.
- 2026-01-27: (WIP) Added adaptive flush-build chunk sizing (target 1–4MiB per chunk) and per-lane flush scaffolding; awaiting perf validation.
- 2026-01-27: Ran `./bin/unified-bench -dbs treedb -profile fast -keys 200000 -progress=false -format markdown -test sequential_write,random_read` as a perf gate for `sprint/flush-lane-adaptive`; sequential write reached 7,670,354 ops/sec and random read 1,274,499 ops/sec.
- 2026-01-27: Perf server (mikers@192.168.0.185) `-suite flushdrain` gating (5 seeds) comparing `origin/main` vs `origin/sprint/flush-lane-adaptive` on `keys=900000`. Trimmed mean results:
  - Random Write: main 1,227,890 → cand 1,218,250 (−0.8%).
  - Random Read: main 1,928,883 → cand 1,918,566 (−0.5%).

## Results / follow-ups

### M5 production gate and default decision (2026-06-14)

M5 consumes the merged M0-M4 parallel flush/apply stack and records the rollout decision. It does **not** ship the broad checkpoint allocation/memcopy optimization work; that work is split to M6 (#2757). The M5 decision is therefore conservative: `Options.FlushApplyConcurrency` remains opt-in/default-off and is not production-default-ready until the checkpoint gate is resolved.

Predecessor state consumed by M5:

| milestone | issue / PR | merge SHA | production-gate relevance |
| --- | --- | --- | --- |
| M0 observability | #2744 / #2750 | `1d80580679c50f7f4f60141804a39f7ce74ee2a1` | Flush/apply counters and benchprof metadata. |
| M1 span planning | #2745 / #2751 | `7e4e5eac8df60980bf64c42808ac85a3815d5a46` | Side-effect-free read-only leaf span planning. |
| M2 worker pool | #2746 / #2753 | `a699ada6b360358139f7a00d2a09eb52932cf07c` | Bounded opt-in COW apply worker pool; default remains off. |
| M3 leaf-log ownership | #2747 / #2754 | `41052a22ca50a2a3597f8903a0935df41e11968a` | Prepared leaf-log output outside append lock with persistent-pointer accounting. |
| M4 coordinator/backpressure | #2748 / #2755 | `f6a5292bacd6191a848374a38ad141fce06d9f24` | Foreground assist/yield coordination for the opt-in path. |

M4 closeout evidence showed the opt-in path can remove the active-flush wait bottleneck under the parent 10M command, but this is not enough for a default-on decision:

| Metric | M3 baseline | M4 opt-in | Notes |
| --- | ---: | ---: | --- |
| `sequential_write` | 2,724,512 ops/s | 2,594,436 ops/s | isolated M4 rerun was 2,683,525 ops/s; treat full-run cell as noisy guardrail. |
| `batch_random` | 299,948 ops/s | 2,350,647 ops/s | active parallel flush/backpressure improvement. |
| `random_write` | 177,997 ops/s | 1,685,212 ops/s | active parallel flush/backpressure improvement. |
| `batch_random` block samples | 667.49s | 89.43s | bottleneck moved substantially. |
| `batch_random` mutex samples | 49.93s | 0.68s | foreground lock contention removed on the opt-in path. |

Serial/default guardrail from the same M4 evidence stayed stable because the new path is disabled unless `FlushApplyConcurrency > 1`:

| Metric | M3 serial | M4 serial |
| --- | ---: | ---: |
| `sequential_write` | 2,620,851 ops/s | 2,728,865 ops/s |
| `batch_random` | 390,129 ops/s | 390,717 ops/s |
| `random_write` | 145,739 ops/s | 158,573 ops/s |

Checkpoint gate status: unresolved and assigned to M6 (#2757). User checkpoint-between-tests evidence on a parallel candidate (`FlushApplyConcurrency=16`, 10M keys, `sequential_write,batch_random`, min gates all `1`, `journal-lanes=1`) reported:

| Metric | Result |
| --- | ---: |
| `sequential_write` | 1,715,358 ops/s |
| `batch_random` | 196,609 ops/s |
| checkpoint before `sequential_write` | 201.53ms |
| checkpoint before `batch_random` | 707.53ms |
| post-run checkpoint | 48.91s |
| final `leaf_vlog` | 1.3GiB across 80 files |

M5 interpretation:

- Default decision: keep `FlushApplyConcurrency <= 1` as the default serial path.
- Production status: the parallel path is experimental/workload-specific opt-in, not a production default.
- Rollout: before enabling, run a workload-specific matrix such as `FlushApplyConcurrency=1,2,4,8` (or hardware-appropriate values) with `-checkpoint-between-tests`, CPU/alloc/block/mutex profiles, stage counters, and final `index.db`/`leaf_vlog` footprint.
- Rollback: set `FlushApplyConcurrency <= 1`; no data migration is required because the knob changes only in-memory apply strategy and durable leaf/value-log output remains persistent storage.
- Blocking follow-up: M6 (#2757) owns aggressive checkpoint allocation, memcopy, memclr, and drain/merge optimization. Parent production-default readiness should not be claimed until M6 reduces or explains the post-run checkpoint cost.

### Perf server results (B560 / i5-11400F, 2026-01-27)

Hardware:
- Ubuntu 22.04, Linux 6.8
- 11th Gen Intel i5-11400F (6c/12t)

Raw artifacts (copied from server run):
- `artifacts/perf_flushthr_20260127_014139/`

Commands (both `origin/main@cdb3efb` and candidate `origin/sprint/flush-throughput@9bd211f`):

```bash
./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false -format markdown

./bin/unified-bench -dbs treedb -profile wal_on_fast -keys 900000 -valsize 128 -batchsize 1000 \
  -test all -progress=false -format markdown -settle-before-scans -treedb-cache-stats-before-reads

./bin/unified-bench -dbs treedb -profile wal_on_fast -keys 900000 -valsize 128 -batchsize 1000 \
  -test all -progress=false -format markdown -settle-before-scans -treedb-cache-stats-before-reads \
  -checkpoint-between-tests
```

Summary deltas (cand vs main):
- `flushthrash`:
  - Random Write: 1,350,434 → 1,339,768 (-0.79%)
  - Batch Write: 1,824,422 → 1,918,448 (+5.15%)
- `wal_on_fast` + `-checkpoint-between-tests` (ops/sec):
  - Sequential Write: 2,250,412 → 2,128,881 (-5.40%)
  - Random Write: 1,000,194 → 979,633 (-2.06%)
  - Dataset Write (Random): 1,283,827 → 1,358,932 (+5.85%)
  - Random Read: 1,480,651 → 1,466,736 (-0.94%)
  - Prefix Scan: 8,154,654 → 7,939,674 (-2.64%)

Notes:
- In the *mixed* `wal_on_fast` run (no checkpoints), candidate showed a large `Prefix Scan` drop that did **not** reproduce in the checkpointed run; `pre-prefix_scan` cache stats also showed candidate had queued immutables at that point. Treat the mixed-prefix-scan delta as suspicious/noisy until it reproduces under `-checkpoint-between-tests` or a smaller “read-only” suite.

### Local exploratory results: `FlushBuildConcurrency` (2026-01-27)

Goal:
- See whether parallel building of combined flush batches can reduce the “checkpoint before reads” latency in `-suite flushdrain`, and whether it affects ops/sec.

Commands:

```bash
make unified-bench

./bin/unified-bench -suite flushdrain -dbs treedb -profile wal_on_fast -keys 200000 -progress=false \
  -treedb-flush-build-concurrency 1

./bin/unified-bench -suite flushdrain -dbs treedb -profile wal_on_fast -keys 200000 -progress=false \
  -treedb-flush-build-concurrency 4
```

Observed (Apple laptop; single-run sanity, noisy):
- `FlushBuildConcurrency=1`: checkpoint before `Random Read` ~353ms, Random Read ~2.62M ops/s
- `FlushBuildConcurrency=4`: checkpoint before `Random Read` ~177ms, Random Read ~3.98M ops/s

Follow-up:
- Rerun on the perf server with the same flags (and ideally multiple trials) to confirm whether the checkpoint/drain-time improvement is real and stable.

### Perf server: `FlushBuildConcurrency` (flushdrain) (2026-01-27)

Artifacts (perf server):
- `artifacts/perf_flushbuild_20260127_073354/` (suite outputs)
- `artifacts/perf_flushbuild_20260127_073425_timing/` (suite output + stderr timing)

Command:

```bash
./bin/unified-bench -suite flushdrain -profile wal_on_fast -keys 900000 -progress=false -format markdown \
  -treedb-flush-build-concurrency {1,4,8}
```

Observed (single run each; ops/sec):
- `c=1`: random_write 1,232,527; random_read 1,909,138; checkpoint before reads 1.51s
- `c=4`: random_write 1,211,869; random_read 1,932,999; checkpoint before reads 1.48s
- `c=8`: random_write 1,199,070; random_read 1,901,429; checkpoint before reads 1.30s

Flush stage breakdown (from `TREEDB_DEBUG_FLUSH_TIMING=1`, `c=1`):
- Combined flush at checkpoint (67MB): build ~139ms, backend_write ~364ms, total ~504ms
- Combined flush at checkpoint (37MB): build ~52ms, backend_write ~924ms, total ~977ms

Interpretation:
- Parallel build can reduce the CPU portion of the checkpoint, but end-to-end drain time is largely bound by `backend_write` on the perf server. If we need larger improvements in checkpoint time, focus should likely move to backend write/commit throughput or reducing the number of backend writes per checkpoint.

### Perf server: backend-write candidates (2026-01-27)

Baseline (main, 5 trials, trimmed mean):
- `checkpoint-before-reads`: **1.497s** (`artifacts/perf_flushdrain_20260127_074857/`)

Candidate (PR, build c=8, 5 trials, trimmed mean):
- `checkpoint-before-reads`: **1.460s** (`artifacts/perf_flushdrain_20260127_074857/`)

Candidates (PR, 5 trials unless noted):
- `PagerSyncConcurrency=4` (msync parallelism): **1.433s** (`artifacts/perf_flushdrain_syncconc_20260127_080334/`) — small win (~4.3%).
- Dirty-range msync (reverted): **1.460s** (`artifacts/perf_flushdrain_dirtyrange_20260127_080621/`) — no win.
- Data-sync without file.Sync (reverted): **1.427s** (`artifacts/perf_flushdrain_data_nofsync_20260127_080927/`) — tiny win (<1%), but reverted due to semantics risk.
- Chunk-size probe (1 trial each): 4MiB **1.74s**, 16MiB **1.46s**, 64MiB **1.43s** (`artifacts/perf_flushdrain_chunksize_probe_20260127_081032/`) — default 64MiB best.
- Leaf prefix compression (1 trial): **1.45s** checkpoint, **Random Read ~0.87M ops/s** (`artifacts/perf_flushdrain_prefix_probe_20260127_081112/`) — read regression; rejected.

Follow-up:
- If we need ≥20% checkpoint improvement, the biggest lever appears to be reducing `pager.Sync` time (msync + fsync) rather than flush build overhead.

### Local checkpoint/drain sanity (historical note)

Command (baseline main):
`go run ./cmd/unified_bench -dbs treedb -test random_write,random_read -keys 900000 -profile wal_on_fast -treedb-cache-stats-before-reads -checkpoint-between-tests -progress=false`

Note: unified_bench checkpoints **before** each test; the `Random Read` row corresponds to the checkpoint taken after the write phase, before reads begin.

Baseline (main @ cdb3efb):
- Random Write: ~1.92M ops/sec
- Random Read: ~2.78M ops/sec
- Checkpoint before reads (`Random Read` row): ~1.78s

Candidate (sprint/flush-throughput):
- Random Write: ~1.72M ops/sec
- Random Read: ~1.99M ops/sec
- Checkpoint before reads (`Random Read` row): ~77–100ms

Follow-ups:
- Run the same comparison on the perf server and include full `-test all` results + checkpoint durations.
- Investigate the local random_read regression (possible CPU contention/background work overlap vs true steady-state regression).

### Local full-table results (scaled): `keys=200000` (2026-01-26)

Perf server benchmarking was not available from this environment (SSH timeout). Running `keys=900000 -test all` locally on the candidate also stalled under backpressure (see hang note below), so we captured a smaller run.

Commands (both main + candidate; adjust `git checkout` / commit as needed):

```bash
NOBP='-treedb-slowdown-backlog-seconds=0 -treedb-stop-backlog-seconds=0 -treedb-max-backlog-bytes=0 -treedb-max-queued-memtables=-1'

# mixed
go run ./cmd/unified_bench -dbs treedb -profile fast       -keys 200000 -valsize 128 -batchsize 1000 -test all -progress=false -format markdown $NOBP
go run ./cmd/unified_bench -dbs treedb -profile wal_on_fast -keys 200000 -valsize 128 -batchsize 1000 -test all -progress=false -format markdown $NOBP

# settled (checkpoint between tests)
go run ./cmd/unified_bench -dbs treedb -profile fast       -keys 200000 -valsize 128 -batchsize 1000 -test all -checkpoint-between-tests -progress=false -format markdown $NOBP
go run ./cmd/unified_bench -dbs treedb -profile wal_on_fast -keys 200000 -valsize 128 -batchsize 1000 -test all -checkpoint-between-tests -progress=false -format markdown $NOBP
```

Raw logs captured locally under:
- `artifacts/bench/flush_throughput_keys200k_20260126222229/`

Key deltas (main @ `cdb3efb` → candidate @ `c5cc81a`):

- `fast` (mixed): `Random Read +43%`, `Sequential Write +14%`, **`Prefix Scan -26%`**
- `fast` (settled): **checkpoint before `Random Read`: `126.43ms → 1.38ms`**, but `Random Read -25%` and **scans regressed** (`Full Scan -73%`, `Prefix Scan -96%`)
- `wal_on_fast` (mixed): `Random Read ~flat`, **scans regressed** (`Full Scan -33%`, `Prefix Scan -68%`)
- `wal_on_fast` (settled): checkpoint before `Random Read`: `160.84ms → 39.43ms`, but `Random Read -28%` and **scans regressed** (`Full Scan -73%`, `Prefix Scan -95%`)

Interpretation / follow-ups:
- The checkpoint/drain-time improvements look promising, but the scan deltas are large enough that we should:
  - rerun on the perf server (once reachable) with default backpressure settings, and
  - confirm `-checkpoint-between-tests` is truly draining the relevant work under these settings (the very-low ms numbers may indicate “background kept up” *or* a premature return).

### CI / hang note (resolved): `-suite flushthrash` stall/cancel

CI symptom:
- GitHub Actions `unified_bench: suites (linux)` canceled while running `./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false` (job hit wall-time).

Local reproduction (pre-fix):
- `./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false` could stall under stop-backpressure.

Fix (current branch):
- Combined flush target now respects small `FlushThreshold` to avoid long single-pass flush latency.
- Combined flush + single-memtable flushes stream ops into backend batches (view methods) instead of building a giant `[]batch.Entry`.

Post-fix sanity run:
- `./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false` completes quickly (local: ~1–2s).

### Historical hang note (candidate, local): `keys=900000` + backpressure

Candidate run stalled locally with `keys=900000 -profile fast -test all` while committing a batch, blocked in:
- `github.com/snissn/gomap/TreeDB/caching.(*DB).waitForStop` (`TreeDB/caching/db.go:3522`)

Captured stack dump:
- `artifacts/bench/flush_throughput_20260126215335/cand_fast_mixed.log`

### Future design ideas (not implemented)

- Pipeline flush: build next `SetOps` while the backend commits the previous batch (still serialized commit ordering).
- Reduce scheduler overhead when `FlushBuildConcurrency > 1`: switch from per-flush goroutine spawns to a small reusable worker pool.
- Raise combine target dynamically under heavy debt (fewer backend commits) while keeping a small default for latency-sensitive workloads.
- Parallelize *safe* portions only:
  - per-shard flush building is safe (ordering preserved by concatenation),
  - parallel backend commits are **not** safe without sharding the durable keyspace.
- Tie “buffer lanes” together:
  - WAL/value-log already have lanes; consider mapping memtable shards → lanes to minimize cross-lane contention during flush/trim.
- Linux-only I/O experiments (behind build tags / flags):
  - `io_uring`/`readv`/`writev` for value-log-heavy flushes to reduce syscall overhead.
