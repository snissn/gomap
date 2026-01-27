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
- [x] Add a dedicated flush-drain suite to `cmd/unified_bench` (`-suite flushdrain`) and document it.
- [ ] Enable targeted profiling:
  - `-cpuprofile` + `-cpuprofile-tests=random_write,random_read` (and any flush-drain step)
  - example: `go run ./cmd/unified_bench -suite flushdrain -dbs treedb -profile fast -keys 900000 -cpuprofile /tmp/flushdrain.pprof -cpuprofile-tests=random_write,random_read -progress=false`
  - optional (more detail): `TREEDB_DEBUG_FLUSH_TIMING=1 ...` to print a flush stage breakdown to stderr

### Phase 2 — Improvements (caching flush pipeline)
- [x] Add optional internal timing breakdown for flush stages (env-gated).
- [x] Reduce flush build overhead:
  - fill `[]batch.Entry` directly (avoid per-memtable slices + extra copies)
  - avoid large transient allocations in `flushOneLocked`
  - avoid per-flush heap allocations for combined flush snapshots (`mems`/`ranges`/`walPaths`/`units`/`offsets`)
  - bulk-build ops directly into a single `SetOps` slice for combined flushes and large single-memtable flushes
- [ ] Evaluate multi-core flush building cost (existing `FlushBuildConcurrency`):
  - if goroutine/scheduler overhead shows up, consider a small reusable worker pool
- [ ] If warranted by profiling: pipeline ops building and backend commit (without parallel backend commits).

### Acceptance gates
- [x] `go test ./...`
- [ ] unified_bench: no regressions in the main table; checkpoint/drain time improves measurably.

## Work log

- 2026-01-27: Created sprint tracker (`docs/SPRINT_TREEDB_FLUSH_THROUGHPUT.md`).
- 2026-01-27: Added `cmd/unified_bench -suite flushdrain` (write burst → checkpoint → read) to make drain-time comparisons easier.
- 2026-01-27: Added `TREEDB_DEBUG_FLUSH_TIMING=1` to print flush stage timing breakdowns from the caching layer.
- 2026-01-27: Reduced flush build allocations/copies by filling bulk `SetOps` slices directly during flush.
- 2026-01-27: Reduced per-flush heap allocations in the combined-flush snapshot path (stack arrays for up to 32 memtables).
- 2026-01-27: Added regression test covering combined-flush bulk path (`totalLen > 2000`) to ensure queued memtables persist.

## Results / follow-ups

### Local checkpoint/drain sanity (2026-01-27)

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
