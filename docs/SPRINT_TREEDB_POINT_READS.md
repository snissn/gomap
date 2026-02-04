# TreeDB Sprint: Point Reads Under Flush Debt

Date: 2026-01-27

This file is the sprint source of truth:
- the full plan (checklist)
- a running work log (timestamped)
- final results + follow-ups

## Goals / Success Criteria

- Mixed workload: point reads (`random_read`, `batch_random`) do **not** crater when there is significant flush debt (high `treedb.cache.queue_len` / `queue_backlog_bytes`).
- No correctness regressions: `go test ./...` passes.
- Docs clearly explain “mixed (under debt)” vs “settled (checkpointed)” read behavior and how to diagnose/tune.

## Plan (checklist)

### Phase 1 — Intent + scope
- [x] Confirm sprint order: **(B) point reads** → **(A) public API cleanup + docs** → **(C) flush throughput docs-only** → **(D) harness polish**
- [x] Record concrete acceptance checks + repro commands in this file

### Phase 2 — Point-read speedups under flush debt (B)
- [x] **B1: Shard-only immutable queue lookup**
  - Track immutable shard ownership alongside `db.queue`
  - In `getMemtable`, `getMemtableAppend`, `Has`, consult only the key’s shard in the immutable queue
  - Keep queue bookkeeping consistent on enqueue/drop/flush removal/clear
- [x] **B2: Regression test for the cliff**
  - Force multiple shards + deep immutable queue
  - Assert point reads don’t touch unrelated shard immutables
- [x] **B3: Benchmark validation (don’t “cheat”)**
  - Keep mixed-workload behavior as-is
  - Ensure diagnostics are printed/available (`treedb.cache.queue_len`, `queue_backlog_bytes`, `flush_bps_ewma`, `memtable_mode`)
  - Add an explicit reproducible “mixed read after write” command/suite

### Phase 3 — Public API cleanup + docs (A)
- [x] Audit remaining docs/examples for removed fields; update to `Options.Durability` + `Options.ValueLog`
- [x] Add a short migration mapping section (old → new)
- [x] Confirm external callers can configure autotune/dict via re-exported types

### Phase 4 — Flush throughput improvements (C) (docs-only)
- [x] Add a “Flush debt control / throughput knobs” section (how to tune + how to measure)

### Phase 5 — Harness polish (D) (minimal)
- [x] Ensure unified_bench docs describe mixed vs settled semantics and profiling/diagnostic flags

### Acceptance gates
- [x] `go test ./...`
- [x] unified_bench spot-check: mixed case improves materially under high `queue_len`; settled case still strong with `-checkpoint-between-tests`

Repro commands (TreeDB only):

- Mixed (reads under flush debt):
  - `go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read -treedb-cache-stats-before-reads -progress=false`
- Settled (reads after a durability boundary):
  - `go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read -checkpoint-between-tests -progress=false`

## Work log

- 2026-01-27: Created sprint plan + tracking file (`docs/SPRINT_TREEDB_POINT_READS.md`).
- 2026-01-27: Implemented shard-only immutable queue lookups for point reads (reduces O(queue_len) scans across unrelated shards).
- 2026-01-27: Added regression test asserting point reads only consult the key’s shard when immutables are queued (`TreeDB/caching/point_read_shard_queue_test.go`).
- 2026-01-27: Updated `cmd/unified_bench` to better diagnose mixed vs settled behavior:
  - `-treedb-cache-stats-before-reads`
  - fixed `-checkpoint-between-tests` reporting
  - improved `-cpuprofile` output naming and added `-cpuprofile-tests`
- 2026-01-27: Updated TreeDB public docs to reflect `Options.Durability` + `Options.ValueLog.*`; added a flush-debt tuning section in `docs/TREEDB_TUNING.md`.
- 2026-01-27: Added a short “old → new” migration mapping section for the public API (`docs/TREEDB_WRITE_PATHS.md`).
- 2026-01-27: Reran unified_bench spot checks (mixed vs settled) and recorded results below.

## Results / follow-ups

- unified_bench spot check (single run; machine-dependent):

  - Mixed (reads under debt):
    - Command: `go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read -treedb-cache-stats-before-reads -progress=false`
    - pre-random_read cache stats: `queue_len=40 backlog_bytes=335,744,232 flush_bps_ewma=220,819,963 memtable_mode=hash_sorted`
    - `random_read`: **799,959 ops/sec**

  - Settled (reads after checkpoint between tests):
    - Command: `go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read -checkpoint-between-tests -progress=false`
    - `random_read`: **1,776,496 ops/sec**

- Follow-ups:
  - If mixed reads are still too cliffy under extreme debt, consider per-immutable Bloom filters per shard (miss fast-path) as a next increment.
