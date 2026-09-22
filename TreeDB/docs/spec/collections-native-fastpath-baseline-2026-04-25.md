# Collections Native Fast-Path Baseline — 2026-04-25

Status: refreshed pre-`R0` baseline for `#768` after `#939` / `#1045`.

This note freezes the main-based rewrite baseline before any native collections
implementation lands on `origin/main`.

## Baseline Identity

- Issue:
  - `#768`
- Baseline date:
  - `2026-04-25`
- Operator:
  - `Codex`
- Repo path:
  - `/home/mikers/dev/snissn/gomap`
- Oracle worktree path:
  - `/home/mikers/dev/snissn/gomap-oracle-768`
- Native execution worktree path:
  - `/home/mikers/dev/snissn/gomap`
- Oracle branch:
  - `pr/688-phase52-root-domain-lifecycle`
- Oracle branch HEAD:
  - `75dfe0b038f56846d62db5f94dd174957bc266fb`
- Main execution base branch:
  - `pr/native-fastpath-prep-main-sync-v2`
- Exact `origin/main` commit:
  - `0aafde7b0967cc1124d9c39ef943f018a018ce17`
- Prep branch HEAD at capture time:
  - `0878f274` (`pr/native-fastpath-prep-main-sync-v2` rebased onto current `main`; docs-only refresh follows)

## Host / Runtime Freeze

- Hostname:
  - `mikers-B560-DS3H-AC-Y1`
- CPU:
  - `11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz`
- Core/thread count:
  - `6 cores / 12 threads`
- RAM:
  - `31 GiB`
- OS / kernel:
  - `Linux 6.8.0-110-generic`
- Go version:
  - `go1.25.7 linux/amd64`
- `GOMAXPROCS`:
  - unset
- Relevant env overrides:
  - `GOWORK=off` for tool builds and oracle collection benches

## Benchmark Input Freeze

- `-keys`:
  - `500000`
- `-valsize`:
  - `100`
- `-batchsize`:
  - `8000`
- `-read-require-hit`:
  - `true` on raw write/read/scan anchor bundles
- Mixed-under-debt runs:
  - `no`
- Settled-after-checkpoint runs:
  - `yes`
- Durability profiles:
  - `fast`
  - `wal_on_fast`

## Frozen Command Set

The original single-command mixed anchor bundle was rejected for baseline use:
delete phases preceding read-hit phases caused deterministic `random_read` miss
failures. The frozen raw TreeDB anchor capture therefore uses split bundles.

### Raw TreeDB write/read/scan bundle

```bash
OUT=$(mktemp -d /tmp/gomap_nf_r0_fast_reads_XXXXXX)

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

Repeat with `-profile wal_on_fast`.

### Raw TreeDB delete bundles

`batch_delete`:

```bash
OUT=$(mktemp -d /tmp/gomap_nf_r0_fast_batchdel_XXXXXX)

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

`random_delete`:

```bash
OUT=$(mktemp -d /tmp/gomap_nf_r0_fast_randdel_XXXXXX)

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
OUT=$(mktemp -d /tmp/gomap_nf_r0_flushdrain_XXXXXX)

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

### Oracle collection benchmark bundle

```bash
OUT=$(mktemp -d /tmp/gomap_nf_r0_oracle_collections_XXXXXX)

(cd /home/mikers/dev/snissn/gomap-oracle-768 && \
  GOWORK=off go test ./TreeDB/collections \
    -run '^$' \
    -bench '^(BenchmarkCollectionInsertBatchProvidedID|BenchmarkCollectionInsertBatchWithSecondaryIndexes|BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes)$' \
    -benchmem \
    -count=1 \
    -cpuprofile "$OUT/oracle_collections_cpu.pprof" \
    -memprofile "$OUT/oracle_collections_mem.pprof") \
  | tee "$OUT/oracle_collections_stdout.txt"
```

### Native collection benchmark bundle

- `N/A before R0 native harness bring-up`

## Artifact Checklist

- Raw TreeDB `fast` write/read/scan:
  - `/tmp/gomap_nf_r0_refresh_fast_reads_cNSdKi`
- Raw TreeDB `fast` batch delete:
  - `/tmp/gomap_nf_r0_refresh_fast_batchdel_NH5zvV`
- Raw TreeDB `fast` random delete:
  - `/tmp/gomap_nf_r0_refresh_fast_randdel_VuLFVx`
- Raw TreeDB `wal_on_fast` write/read/scan:
  - `/tmp/gomap_nf_r0_refresh_wal_reads_hQU99o`
- Raw TreeDB `wal_on_fast` batch delete:
  - `/tmp/gomap_nf_r0_refresh_wal_batchdel_pmtr3T`
- Raw TreeDB `wal_on_fast` random delete:
  - `/tmp/gomap_nf_r0_refresh_wal_randdel_1hqJV8`
- Deferred-work `flushdrain`:
  - `/tmp/gomap_nf_r0_refresh_flushdrain_Z5F5y8`
- Oracle collection bundle:
  - `/tmp/gomap_nf_r0_refresh_oracle_collections_H1D2CW`
- Native collection bundle:
  - `N/A before R0 native harness bring-up`

## Reference Metrics Table: Raw TreeDB Anchors

| Test | Profile | Mode | Ops/s | Notes |
|---|---|---|---:|---|
| `write_seq` | `fast` | `settled` | `3,003,976` | from split write/read/scan bundle |
| `write_rand` | `fast` | `settled` | `2,188,608` | from split write/read/scan bundle |
| `batch_write` | `fast` | `settled` | `3,692,736` | front-end ingest metric |
| `batch_random` | `fast` | `settled` | `3,483,067` | from split write/read/scan bundle |
| `batch_delete` | `fast` | `settled` | `4,033,929` | from dedicated delete bundle |
| `delete_rand` | `fast` | `settled` | `2,083,692` | from dedicated delete bundle |
| `random_read` | `fast` | `settled` | `194,849` | read-hit enforced |
| `random_read_parallel_acquire_snapshot` | `fast` | `settled` | `1,199,506` | read-hit enforced |
| `full_scan` | `fast` | `settled` | `4,368,804` | from split write/read/scan bundle |
| `prefix_scan` | `fast` | `settled` | `4,962,865` | from split write/read/scan bundle |
| `write_seq` | `wal_on_fast` | `settled` | `2,046,008` | from split write/read/scan bundle |
| `write_rand` | `wal_on_fast` | `settled` | `1,641,822` | from split write/read/scan bundle |
| `batch_write` | `wal_on_fast` | `settled` | `4,736,166` | front-end ingest metric |
| `batch_random` | `wal_on_fast` | `settled` | `2,400,011` | from split write/read/scan bundle |
| `batch_delete` | `wal_on_fast` | `settled` | `2,912,325` | from dedicated delete bundle |
| `delete_rand` | `wal_on_fast` | `settled` | `1,776,003` | from dedicated delete bundle |
| `random_read` | `wal_on_fast` | `settled` | `191,534` | read-hit enforced |
| `random_read_parallel_acquire_snapshot` | `wal_on_fast` | `settled` | `1,156,664` | read-hit enforced |
| `full_scan` | `wal_on_fast` | `settled` | `4,019,991` | from split write/read/scan bundle |
| `prefix_scan` | `wal_on_fast` | `settled` | `4,998,973` | from split write/read/scan bundle |

## Reference Metrics Table: Deferred Work

| Measurement | Profile | Baseline | Notes |
|---|---|---:|---|
| `flushdrain random_write` | `wal_on_fast` | `1,408,119` | suite output |
| `flushdrain random_read` | `wal_on_fast` | `300,840` | suite output |
| `flushdrain checkpoint before random_read` | `wal_on_fast` | `503.68ms` | checkpoint boundary cost |
| `checkpoint-focused batch ingest` | `wal_on_fast` | `N/A before R0` | native collections harness absent on prep branch |

## Reference Architectural Counters

| Counter | Oracle baseline | Main-base baseline | Notes |
|---|---:|---:|---|
| `tiny_batch_fallback_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |
| `per_item_key_probe_fallback_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |
| `per_item_prefix_probe_fallback_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |
| `detached_batch_replay_fallback_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |
| `warm_apply_rebuild_fallback_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |
| `warm_apply_per_key_retention_lookup_count` | `N/A` | `N/A before R0` | counter not present in oracle or prep branch |

## Reference Metrics Table: Collections

| Benchmark | Path | Mode | Batch size | ns/op | docs/s | B/op | allocs/op | Notes |
|---|---|---|---:|---:|---:|---:|---:|---|
| `BenchmarkCollectionInsertBatchProvidedID` | `oracle` | `mixed` | `256` | `334101` | `~766,235` | `160323` | `1060` | oracle branch focused bundle |
| `BenchmarkCollectionInsertBatchWithSecondaryIndexes` | `oracle` | `mixed` | `256` | `2074550` | `~123,400` | `1278306` | `12712` | oracle branch focused bundle |
| `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes` | `oracle` | `settled/checkpoint` | `256` | `98776827` | `~2,592` | `2603097` | `14263` | oracle branch focused bundle |
| `BenchmarkCollectionInsertBatchProvidedID` | `native` | `mixed` | `256` | `N/A before R0` | `N/A before R0` | `N/A before R0` | `N/A before R0` | native harness absent on prep branch |
| `BenchmarkCollectionInsertBatchWithSecondaryIndexes` | `native` | `mixed` | `256` | `N/A before R0` | `N/A before R0` | `N/A before R0` | `N/A before R0` | native harness absent on prep branch |
| `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes` | `native` | `settled/checkpoint` | `256` | `N/A before R0` | `N/A before R0` | `N/A before R0` | `N/A before R0` | native harness absent on prep branch |

## Noise / Gate Policy

- Raw TreeDB anchor noise margin:
  - `3%`
- Collection benchmark noise margin:
  - `5%`
- Rerun policy for small deltas:
  - rerun at least `count=3` and compare medians
- Blocker threshold for regressions:
  - any regression outside the relevant noise band unless there is explicit rationale
- Scaffolding-phase rule:
  - no material regression beyond noise band
- Performance-phase rule:
  - at least one material improvement in the targeted benchmark family and the relevant architectural counter moving toward zero or staying at zero

## Path Selection Freeze

- Oracle comparison model:
  - `external branch oracle`
- Oracle branch name:
  - `pr/688-phase52-root-domain-lifecycle`
- Native execution branch:
  - `pr/native-fastpath-prep-main-sync-v2`
- Proof baseline was captured from the oracle branch:
  - focused oracle collection bundle at `/tmp/gomap_nf_r0_refresh_oracle_collections_H1D2CW`
- Proof main-based baseline was captured from current execution base:
  - raw TreeDB anchor bundles under `/tmp/gomap_nf_r0_refresh_*`

## Go / No-Go

- [x] Oracle branch frozen
- [x] Main execution base frozen
- [x] Benchmark inputs frozen
- [x] Commands frozen
- [x] Raw TreeDB artifacts captured
- [x] Oracle collection artifacts captured
- [x] Metrics tables filled
- [x] Noise margin recorded
- [x] Ready to open `R0`
