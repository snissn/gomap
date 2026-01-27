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
- 2026-01-27: Prototype: reduced flush build allocations via bulk `SetOps` slices (later replaced by streaming view ops due to flushthrash stall).
- 2026-01-27: Fixed CI cancellation in `-suite flushthrash` by (a) lowering the combined-flush target when `FlushThreshold` is small and (b) streaming into backend batches via `SetView`/`DeleteView`/`SetPointerView` (no `[]batch.Entry` materialization).
- 2026-01-27: Reduced per-flush heap allocations in the combined-flush snapshot path (stack arrays for up to 32 memtables).
- 2026-01-27: Added regression test covering combined-flush bulk path (`totalLen > 2000`) to ensure queued memtables persist.
- 2026-01-27: Ran local `keys=200000` mixed + settled unified_bench comparisons (perf server SSH timed out); captured a candidate hang at `keys=900000` (see below) and recorded results for PR review.

## Results / follow-ups

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
