# Collections Native Fast-Path Baseline Template

Status: template, non-normative.

Use this template before opening `R0`.

The goal is to freeze the comparison point so the rewrite is judged against a
stable main-based baseline and a stable oracle reference.

## Baseline Identity

- Issue:
  - `#768`
- Baseline date:
  - `YYYY-MM-DD`
- Operator:
  - `...`
- Repo path:
  - `/home/mikers/dev/snissn/gomap`
- Oracle worktree path:
  - `...`
- Native execution worktree path:
  - `/home/mikers/dev/snissn/gomap`
- Oracle branch:
  - `...`
- Main execution base branch:
  - `pr/native-fastpath-prep-main-sync-v2`
- Exact `origin/main` commit:
  - `...`
- Prep branch HEAD:
  - `...`

## Host / Runtime Freeze

- Hostname:
  - `...`
- CPU:
  - `...`
- Core/thread count:
  - `...`
- RAM:
  - `...`
- OS / kernel:
  - `...`
- Go version:
  - `...`
- `GOMAXPROCS`:
  - `...`
- Relevant env overrides:
  - `...`

## Benchmark Input Freeze

- `-keys`:
  - `500000`
- `-valsize`:
  - `100`
- `-batchsize`:
  - `8000`
- `-read-require-hit`:
  - `true`
- Mixed-under-debt runs:
  - `yes|no`
- Settled-after-checkpoint runs:
  - `yes|no`
- Durability profiles:
  - `fast`
  - `wal_on_fast`

## Raw TreeDB Anchor Commands

### `fast` write/read/scan bundle

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_small_seq,random_read,random_read_parallel_acquire_snapshot,full_scan,prefix_scan \
  -checkpoint-between-tests \
  -read-require-hit \
  -profile-dir "$OUT" \
  -progress=false
```

### `wal_on_fast` write/read/scan bundle

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile wal_on_fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_small_seq,random_read,random_read_parallel_acquire_snapshot,full_scan,prefix_scan \
  -checkpoint-between-tests \
  -read-require-hit \
  -profile-dir "$OUT" \
  -progress=false
```

### Delete bundles

Capture delete-focused anchors separately so read-hit guarantees remain valid.

`batch_delete`:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test random_write,batch_delete \
  -checkpoint-between-tests \
  -profile-dir "$OUT" \
  -progress=false
```

`delete_rand`:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test random_write,random_delete \
  -checkpoint-between-tests \
  -profile-dir "$OUT" \
  -progress=false
```

Repeat both delete bundles with `-profile wal_on_fast`.

### Deferred-work anchor

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile wal_on_fast \
  -suite flushdrain \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -progress=false
```

## Collection Benchmark Commands

### Oracle reference

```bash
ORACLE_WORKTREE=/path/to/oracle-worktree

TREEDB_COLLECTION_BENCH_ENGINE=cached \
BENCHTIME=1s \
COUNT=1 \
(cd "$ORACLE_WORKTREE" && git rev-parse HEAD && scripts/bench_collections_report.sh)
```

### Native rewrite path

```bash
NATIVE_WORKTREE=/home/mikers/dev/snissn/gomap

TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_fast \
TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 \
BENCHTIME=1s \
COUNT=1 \
(cd "$NATIVE_WORKTREE" && git rev-parse HEAD && scripts/bench_collections_report.sh)
```

If the native execution branch does not yet contain the collection benchmark
harness, record this section as:

- `N/A before R0 native harness bring-up`

Do not fabricate native collection comparability before the harness exists on
the main-based branch.

## Artifact Checklist

- Raw TreeDB `fast` artifact dir:
  - `/tmp/...`
- Raw TreeDB `wal_on_fast` artifact dir:
  - `/tmp/...`
- Deferred-work `flushdrain` artifact dir:
  - `/tmp/...`
- Oracle collection benchmark artifact dir:
  - `/tmp/...`
- Native collection benchmark artifact dir:
  - `/tmp/... | N/A before R0 native harness bring-up`
- `benchprof_results.md` paths:
  - `/tmp/.../benchprof_results.md`
- CPU profile paths:
  - `/tmp/...`
- Allocation profile paths:
  - `/tmp/...`
- Contention profile paths, if present:
  - `/tmp/...`

## Reference Metrics Table: Raw TreeDB Anchors

| Test | Profile | Mode | Ops/s | Notes |
|---|---|---|---:|---|
| `write_seq` | `fast` | `settled` |  |  |
| `write_rand` | `fast` | `settled` |  |  |
| `batch_write` | `fast` | `settled` |  |  |
| `batch_random` | `fast` | `settled` |  |  |
| `batch_delete` | `fast` | `settled` |  |  |
| `delete_rand` | `fast` | `settled` |  |  |
| `random_read` | `fast` | `settled` |  |  |
| `random_read_parallel_acquire_snapshot` | `fast` | `settled` |  |  |
| `full_scan` | `fast` | `settled` |  |  |
| `prefix_scan` | `fast` | `settled` |  |  |
| `write_seq` | `wal_on_fast` | `settled` |  |  |
| `write_rand` | `wal_on_fast` | `settled` |  |  |
| `batch_write` | `wal_on_fast` | `settled` |  |  |
| `batch_random` | `wal_on_fast` | `settled` |  |  |
| `batch_delete` | `wal_on_fast` | `settled` |  |  |
| `delete_rand` | `wal_on_fast` | `settled` |  |  |
| `random_read` | `wal_on_fast` | `settled` |  |  |
| `random_read_parallel_acquire_snapshot` | `wal_on_fast` | `settled` |  |  |
| `full_scan` | `wal_on_fast` | `settled` |  |  |
| `prefix_scan` | `wal_on_fast` | `settled` |  |  |

## Reference Metrics Table: Deferred Work

| Measurement | Profile | Baseline | Notes |
|---|---|---:|---|
| `flushdrain total time` | `wal_on_fast` |  |  |
| `checkpoint-focused batch ingest` | `wal_on_fast` |  |  |
| settled follow-up read/scan observations | `wal_on_fast` |  |  |

## Reference Architectural Counters

| Counter | Oracle baseline | Main-base baseline | Notes |
|---|---:|---:|---|
| `tiny_batch_fallback_count` |  |  |  |
| `per_item_key_probe_fallback_count` |  |  |  |
| `per_item_prefix_probe_fallback_count` |  |  |  |
| `detached_batch_replay_fallback_count` |  |  |  |
| `warm_apply_rebuild_fallback_count` |  |  |  |
| `warm_apply_per_key_retention_lookup_count` |  |  |  |

## Reference Metrics Table: Collections

| Benchmark | Path | Mode | Batch size | ns/op | docs/s | B/op | allocs/op | Notes |
|---|---|---|---:|---:|---:|---:|---:|---|
| `BenchmarkCollectionInsertBatchProvidedID` | `oracle` | `mixed` | `256` |  |  |  |  |  |
| `BenchmarkCollectionInsertBatchWithSecondaryIndexes` | `oracle` | `mixed` | `256` |  |  |  |  |  |
| `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes` | `oracle` | `settled/checkpoint` | `256` |  |  |  |  |  |
| `BenchmarkCollectionInsertBatchProvidedID` | `native` | `mixed` | `256` |  |  |  |  |  |
| `BenchmarkCollectionInsertBatchWithSecondaryIndexes` | `native` | `mixed` | `256` |  |  |  |  |  |
| `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes` | `native` | `settled/checkpoint` | `256` |  |  |  |  |  |

## Noise / Gate Policy

- Noise margin:
  - `...`
- Rerun policy for small deltas:
  - `...`
- Blocker threshold for regressions:
  - `...`
- Scaffolding-phase rule:
  - `...`
- Performance-phase rule:
  - `...`

## Path Selection Freeze

- Oracle comparison model:
  - `external branch oracle`
- Oracle branch name:
  - `...`
- Native execution branch:
  - `pr/native-fastpath-prep-main-sync-v2`
- Proof baseline was captured from the oracle branch:
  - `...`
- Proof main-based baseline was captured from current execution base:
  - `...`
- Proof native collection harness existed on the execution base when native
  collection numbers were recorded:
  - `... | N/A before R0 native harness bring-up`

## Go / No-Go

- [ ] Oracle branch frozen
- [ ] Main execution base frozen
- [ ] Benchmark inputs frozen
- [ ] Commands frozen
- [ ] Raw TreeDB artifacts captured
- [ ] Collection artifacts captured
- [ ] Metrics tables filled
- [ ] Noise margin recorded
- [ ] Ready to open `R0`
