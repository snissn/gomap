# TreeDB Optimization Checklist (Punch Sheet)

This is a one-page checklist to guide high-leverage TreeDB optimizations, with:

- the *exact benchmark(s)* that should move,
- the *metric(s) to watch* (and whether higher/lower is better),
- the *expected profile signature*,
- and the *likely code hotspots* to inspect/change.

## Reading `benchmark/out.bench` (avoid sign confusion)

`/Users/michaelseiler/dev/snissn/benchmark/out.bench` is produced by:

- `/Users/michaelseiler/dev/snissn/benchmark/mike.sh`
- `/Users/michaelseiler/dev/snissn/benchmark/run-bench-geth-kv-scenarios.sh`
- `/Users/michaelseiler/dev/snissn/benchmark/scripts/kv-db-summary.py`

In `kv-db-summary.py`, `diff` is always:

`diff = (treedb - leveldb) / leveldb`

So:

- `latency/* (ms)` tables: **lower is better** (positive diff is worse for treedb)
- `chain/*` and `engine/*` “DB-ish” tables: most entries are geth **ResettingTimers** in **nanoseconds** (lower is better even though printed as “raw units”)
- `gas/per_second`: **higher is better**

## Baselines to run (repeatable)

### Regression note (iavl-bench WAL-on full run)

- Full changeset (`./treedb-v1/treedb-v1-bench bench ...`) shows WAL-on regression:
  - `results/treedb-v1.jsonl` (commit `606c9c7`): **0.81 min** total
  - `results-compare-full/treedb-v1-head-wal-on.jsonl` (current HEAD): **1.98 min** total
- Treat this as a priority regression to explain before further optimization work.

#### Update (wal-value-log)

- Root cause: cached-mode auto-checkpoint size trigger could repeatedly run `Checkpoint()` when `effectiveWALBytes` stayed above `MaxWALBytes` (value-log segments retained for pointers cannot be trimmed), thrashing writer latency.
- Fix: disarm size-triggered auto-checkpoint after the first run; re-arm only once `effectiveWALBytes < MaxWALBytes/2` (commit `5dd76a5`).
- New result: `results-compare-full/treedb-v1-5dd76a5-wal-on.jsonl`: **1.32 min** total (down from 1.98 min).
- Follow-up: write-path overhead was still dominated by WAL writer goroutine coordination for non-sync writes; switch non-sync WAL appends to write inline (commit `73d1c11`).
- New result: `results-compare-full/treedb-v1-73d1c11-wal-on.jsonl`: **0.79 min** total (WAL-off: `results-compare-full/treedb-v1-73d1c11-wal-off.jsonl`: **0.75 min**).

### Microbenches (op-geth)

From `/Users/michaelseiler/dev/snissn/op-geth`:

- Iteration: `go test ./ethdb/treedb -run '^$' -bench '^BenchmarkTreeDB/Iteration/IterationRandom$' -count=5`
- Mixed batch ops: `go test ./ethdb/treedb -run '^$' -bench '^BenchmarkTreeDB/BatchMixedOps/BatchMixedOps10k$' -count=5`
- WriteRandom (primary): `go test ./ethdb/treedb -run '^$' -bench '^BenchmarkTreeDB/Write1M/WriteRandom$' -count=5 -benchtime=1x`

For profiles (microbench):

- CPU: add `-cpuprofile /tmp/treedb.prof`
- Alloc: add `-memprofile /tmp/treedb.mem -memprofilerate=1`
- Mutex: add `-mutexprofile /tmp/treedb.mutex -mutexprofilefraction=1`
- Block: add `-blockprofile /tmp/treedb.block -blockprofilerate=1`

### Scenario bench (base-bench)

From `/Users/michaelseiler/dev/snissn/benchmark`:

- `GETH_BIN=/Users/michaelseiler/dev/snissn/op-geth/build/bin/geth ./run-bench-geth-kv-scenarios.sh > out.bench`

Scenarios inside:

- `sload-readheavy` (read stress)
- `sstore-writeheavy` (write stress)
- `sstore-manytx` (write churn)

## Punch sheet (prioritized targets)

### 1) DisableWAL `DeleteRange` is allocation-dominant (BatchMixedOps10k)

- [x] **Benchmark(s) that should move**
  - `BenchmarkTreeDB/BatchMixedOps/BatchMixedOps10k` (primary)
  - `BenchmarkTreeDB/DeleteRange/*` (secondary)
- [x] **Metric(s) to watch**
  - `ns/op`: lower is better
  - `B/op` and `allocs/op`: lower is better (this is currently extremely high)
- [x] **Expected current profile signature**
  - `alloc_space`: `TreeDB/caching.(*DB).DeleteRange` dominates (disableWAL path collecting/copying keys)
  - CPU: `DeleteRange` dominates end-to-end time
- [x] **Likely hot code**
  - `TreeDB/caching/db.go` in `(*DB).DeleteRange` (disableWAL path)
- [x] **Change direction**
  - Make delete-range streaming (avoid building `[][]byte` of copied keys).
  - Avoid mutating a memtable while iterating it (rotate first, then apply deletes to the new mutable).
- [x] **Acceptance check**
  - `BenchmarkTreeDB/BatchMixedOps/BatchMixedOps10k`: `~121ms/op, ~74MB/op, ~530k allocs/op` → `~3.5ms/op, ~0.67MB/op, ~34k allocs/op`.
  - `alloc_space` no longer dominated by key-copy loops in delete-range.

### 2) IterationRandom dominated by hash_sorted run iteration overhead

- [x] **Benchmark(s) that should move**
  - `BenchmarkTreeDB/Iteration/IterationRandom` (primary)
  - Secondary correlation targets: `benchmark/out.bench` storage-read timers (below)
- [x] **Metric(s) to watch**
  - `ns/op`: lower is better
  - `B/op`, `allocs/op`: lower is better
- [x] **Expected current profile signature**
  - CPU: `(*hashRunsIterator).advance` + heap ops + `runtime.mapaccess2_faststr`
  - Indicates per-key overhead from: k-way merge + map lookup per key
- [x] **Likely hot code**
  - `TreeDB/internal/memtable/hash_sorted.go`
  - `TreeDB/internal/memtable/hash_sorted_indexer.go`
- [x] **Change direction**
  - Reduce run-count (background run compaction / leveled merging) to cut k-way heap work.
  - Avoid per-key map lookup during frozen iteration: store a stable reference per sorted entry (e.g. arena offset / entry pointer) at seal-time.
- [x] **Acceptance check**
  - `BenchmarkTreeDB/Iteration/IterationRandom`: `~194ms/op, ~707KB/op, ~689 allocs/op` → `~24ms/op, ~3.2KB/op, ~106 allocs/op`.
  - `BenchmarkTreeDB/Iteration/IterationSorted`: `~16ms/op, ~65KB/op, ~74 allocs/op` → `~18ms/op, ~2.4KB/op, ~79 allocs/op`.

### 3) Scenario read regressions: `chain/storage/reads` and forkchoice latency

Observed in `/Users/michaelseiler/dev/snissn/benchmark/out.bench`:

- `sload-readheavy`: sequencer + validator `latency/update_fork_choice` much worse for treedb; validator `chain/storage/reads.50-percentile` hugely worse.
- `sstore-writeheavy` and `sstore-manytx`: `chain/storage/reads.50-percentile` often worse for treedb (even when other timers improve).

- [ ] **Scenario metric(s) to watch (lower is better unless stated)**
  - `Sequencer latency/update_fork_choice (ms)`
  - `Validator latency/update_fork_choice (ms)`
  - `Sequencer chain/storage/reads.50-percentile` (ns)
  - `Validator chain/storage/reads.50-percentile` (ns)
  - Throughput sanity: `gas/per_second` (higher is better)
- [ ] **Working hypothesis**
  - Iterator creation/merge/index readiness is impacting read-heavy paths and/or triggering stalls during forkchoice.
- [ ] **Change direction**
  - Anything that improves `IterationRandom` and reduces iterator/merge overhead should reflect here.
  - Ensure iterator creation does not hold global locks during expensive work (already partially addressed; verify end-to-end).
- [ ] **Acceptance check**
  - `sload-readheavy` forkchoice latency gap shrinks materially without hurting `gas/per_second`.
  - If `chain/storage/reads.50-percentile` remains high, confirm it is a timer (ns) vs a counter, and identify the upstream metric source in geth.

### 4) WriteRandom throughput (still a top-line goal, but don’t regress it)

- [ ] **Benchmark(s) to keep stable**
  - `BenchmarkTreeDB/Write1M/WriteRandom`
  - `BenchmarkTreeDB/BatchWrite1M/WriteRandom`
- [ ] **Metric(s)**
  - `MB/s`: higher is better
  - `B/op`, `allocs/op`: lower is better
- [ ] **Expected risk areas**
  - Any background indexing that copies keys/allocates per key can hurt `WriteRandom`.
- [ ] **Guardrails**
  - Keep per-write overhead O(1) and allocation-free when possible.
  - Prefer chunk-level work and pooling over per-key allocations.

## “Profile-to-fix” loop (per target)

- [ ] Reproduce with `-count=5` (stabilize variance).
- [ ] Capture `cpu` + `alloc_space` first; add `mutex` only if contention suspected.
- [ ] Record: top-5 CPU and top-5 alloc frames (paths + symbols) in the PR notes.
- [ ] Implement the smallest change that deletes those frames from the top.
- [ ] Re-run the same benchmark(s) and verify the profile signature moved.
