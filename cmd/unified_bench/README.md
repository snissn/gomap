# unified_bench

Side-by-side benchmarks for `HashDB`, `BTreeOnHashDB`, `TreeDB` (cached), Badger, and LevelDB.

## Run

- Build: `make unified-bench` (writes `bin/unified-bench`)
- Run: `./bin/unified-bench`
- Or: `go run ./cmd/unified_bench`

## Guardrail Check (Read Snapshot + Append-Only)

Targeted regression guardrail for append-only writes plus read-heavy snapshot acquisition:

```bash
./scripts/check_read_snapshot_guardrail.sh
```

The script validates that `TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail`
actually ran (to avoid `go test` false-greens when `-run` matches nothing).
It retries once for diagnostics and still fails the job if only the retry
passes, so flaky regressions are surfaced instead of silently passing.

Direct invocation:

```bash
cd /path/to/gomap/cmd/unified_bench
GOWORK=off GOMEMLIMIT=4GiB GOMAXPROCS=2 go test -json -p 1 . \
  -run '^TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail$' -count=1
```

## Reproducibility

- Randomized tests use a per-test PRNG derived from `-seed` so every DB sees the same random key/query sequence.
- The chosen seed is printed to stderr at startup.

## Tests

- `write_seq` — Sequential Write
- `write_rand` — Random Write
- `batch_write` — Batch Write
- `batch_random` — Batch Random
- `batch_delete` — Batch Delete
- `delete_rand` — Random Delete
- `random_read` — Random Read
- `random_read_parallel` — Random Read (Parallel aggregate throughput)
- `random_read_parallel_acquire_snapshot` — Random Read (Parallel, Snapshot Per Key)
- `random_read_batch` — Random Read (Batch)
- `full_scan` — Full Scan (iterate the full keyspace)
- `prefix_scan` — Prefix Scan (range scans over `[start,end)`)
  - Aliases: `scan` → `full_scan`, `range_scan` → `prefix_scan`, `read_rand` → `random_read`, `read_rand_parallel` → `random_read_parallel`, `read_rand_batch`/`read_random_batch` → `random_read_batch`

`random_read_batch` always exercises value-read paths:
- Uses `GetMany` when available.
- Falls back to per-key `Get` otherwise.
- Any `GetMany`/`Get` error fails the test.
- Missing keys are not treated as benchmark-fatal by default (adapter/API contract). Use `-read-require-hit` to fail fast on misses and validate value lengths.

## Common flags

- `-profile` benchmark profile preset (see `cmd/unified_bench/profiles.go`):
  - `balanced` (default)
  - `durable` (strict durability)
  - `fast` (max throughput; TreeDB mirrors `treedb.ProfileFast`, including Celestia-aligned auto/snappy/balanced value-log compression; unsafe)
  - `wal_on_fast` (TreeDB mirrors `treedb.ProfileWALOnFast`, including the same compression defaults with WAL on; unsafe)
- `-dbs` (`all` or CSV): `hashdb,btree,treedb,badger,leveldb`
  - Hidden TreeDB variants can be selected explicitly, including
    `treedb_backend` for the direct backend path and
    `treedb_backend_command_wal` (alias `treedb_command_wal`) for the direct
    backend `command_wal_v1` path. The command-WAL variant reports
    `treedb.command_wal.*` and `treedb.applied_command_lsn` stats so benchmark
    artifacts prove the required feature was persisted and typed frames were
    appended.
- `-test` (`all` or CSV): see list above
- `-keys` number of keys (default 100000)
- `-keycounts` comma-separated key counts to sweep over (overrides `-keys`)
- `-keyscale` generate keycounts by scale: `log10` or `doubling` (uses `-keys-min` / `-keys-max`)
- `-valsize` value size in bytes (default 128)
- `-val-pattern` value pattern for write tests (including `dataset_write_*`) (`zero|repeat|repeat_tail64|ultra_compressible_repeat|highly_compressible_notail|half_repeat_half_random|medium_compressible_sparse|celestia_height_prefix_fill|random`)
  - Note: dataset-write generation now uses the same normalized behavior as other write tests (legacy pattern names are accepted as aliases, but generation is unified under `makeValuePool`).
- `-val-pool-size` number of distinct values to cycle through for `-val-pattern` (`0` = auto)
- `-batchsize` batch size (default 8000)
- `-read-workers` number of goroutines for `random_read_parallel` and `random_read_parallel_acquire_snapshot` (default `GOMAXPROCS`)
- `-read-require-hit` fail read benchmarks (`random_read*`, `random_read_batch`) on misses and validate value length matches `-valsize`
- `-range-queries` number of prefix/range queries (default 200)
- `-range-span` number of keys per range (default 100)
- `-leveldb-block-compression` LevelDB: block compression mode (`default|on|off|both`)
- `-leveldb-block-size` LevelDB: table block size in bytes (default 4096)

### TreeDB Main Knobs

- `-treedb-chunk-size` TreeDB: pager chunk size in bytes (default `256KiB`)
- `-treedb-flush-threshold` TreeDB (cached): flush threshold in bytes (default 64MB)
- `-treedb-maintenance-mode` TreeDB maintenance preset (`normal|bench`)
- `-treedb-memtable-mode` TreeDB memtable implementation override
- `-treedb-index-optimizations` TreeDB profile-style index optimization bundle
- `-treedb-index-outer-leaves-in-vlog` TreeDB: store outer leaves in `leaf_vlog`
- `-treedb-prefer-append-alloc` TreeDB: prefer append allocation over freelist reuse
- `-treedb-force-value-pointers` TreeDB: force all values into the value log
- `-treedb-value-log-threshold` TreeDB: inline-vs-pointer threshold
- `-treedb-vlog-compression` TreeDB: value-log compression mode (`default|off|block|dict|auto`)
- `-treedb-vlog-block-codec` TreeDB: block codec (`snappy|lz4`)
- `-treedb-vlog-auto-policy` TreeDB: value-log auto policy (`balanced|throughput|size`)
- `-treedb-vlog-generation-policy` TreeDB: generation policy (`default|off|hot_warm_cold`)

### TreeDB Advanced Tuning

These are mainly for experiments and should usually be left at engine defaults:

- `-treedb-vlog-compression-autotune`
- `-treedb-vlog-dict-*`
- `-treedb-vlog-rewrite-*`
- `-treedb-flush-build-*`
- `-treedb-max-queued-memtables`, `-treedb-slowdown-backlog-seconds`, `-treedb-stop-backlog-seconds`

Use `./bin/unified-bench -h` for the full grouped TreeDB advanced flag list.

### Additional Common Flags

- `-treedb-max-queued-memtables` TreeDB (cached) max queued immutable memtables before applying backpressure flush (`0`=default, `<0`=disable)
- `-treedb-slowdown-backlog-seconds` TreeDB (cached) start backpressure when queued backlog exceeds this many seconds of flush work
- `-treedb-stop-backlog-seconds` TreeDB (cached) block writers when queued backlog exceeds this many seconds of flush work
- `-treedb-max-backlog-bytes` TreeDB (cached) absolute cap on queued backlog bytes
- `-treedb-writer-flush-max-memtables` TreeDB (cached) max memtables a writer will help flush per op
- `-treedb-writer-flush-max-ms` TreeDB (cached) max time (ms) a writer will help flush per op
- `-treedb-iter-debug` print prefix scan iterator timing + debug stats
- `-treedb-iter-debug-limit` max per-query debug lines to print (default 20)
- `-treedb-maintenance-ops-per-coalesce` TreeDB: ops-per-coalesce maintenance budget (`0`=default, `<0`=disable budget)
- `-treedb-bg-vacuum-interval` TreeDB: background index vacuum interval (0=disabled)
- `-treedb-bg-vacuum-span-ppm` TreeDB: background index vacuum span ratio threshold (ppm), `0`=default
- `-treedb-allow-unsafe` TreeDB: allow unsafe durability/integrity options (required for unsafe toggles)
- `-treedb-vlog-dict` TreeDB: value-log dict compression mode (`default|on|off|both`)
- `-treedb-vlog-rewrite-min-segment-age-ms` TreeDB: minimum source segment age for online generational rewrite (`0`=default)
- `-treedb-vlog-dict-frame-encode-level` TreeDB: dict frame zstd encoder level (`engine|fastest|default|better|best|all|<int>`)
- `-treedb-vlog-dict-frame-entropy` TreeDB: dict frame entropy mode (`engine|on|off|both`)
- `-seed` PRNG seed for randomized tests (default 1; `0` = time-based)
- `-keep` keep temp DB directories after run
- `-settle-before-scans` close+reopen DBs before `full_scan`/`prefix_scan` to measure scan performance on a “settled” (fully flushed) state
- `-progress` live table updates to stderr (default true)
- `-format` output format: `table` or `markdown`
- `-cpuprofile` write per-test CPU profiles to `<prefix>_<test>_<db>.pprof`
- `-cpuprofile-tests` restrict CPU profiling to a CSV list of tests (e.g. `random_read,batch_random`)
- `-allocsprofile` write per-test allocation delta profiles to `<prefix>_<test>_<db>.pprof` (analyzable with `-sample_index=alloc_space|alloc_objects`)
- `-allocsprofile-tests` restrict allocation profiling to a CSV list of tests
- `-allocsprofilerate` allocation sampling rate in bytes for `runtime.MemProfileRate` (default `524288`)
- `-checkpoint-cpuprofile` write per-checkpoint CPU profiles to `<prefix>_checkpoint_<test>_<db>.pprof`
- `-checkpoint-cpuprofile-tests` restrict checkpoint CPU profiling to a CSV list of tests
- `-profile-dir` write all profile outputs into one directory (auto-sets defaults for `-cpuprofile`, `-allocsprofile`, `-checkpoint-cpuprofile`, `-blockprofile`, `-mutexprofile`, `-trace`; explicit flags still win). Also emits `benchprof_results.json` and `benchprof_results.md`, then automatically runs `benchprof` in-process. Requires `-path-label native-fastpath` or `-path-label oracle`.
- `-treedb-cache-stats-before-reads` print select `treedb.cache.*` stats before read/scan tests (treedb only)
- `-blockprofile`, `-mutexprofile` write global profiling artifacts to files and also emit per-test contention delta profiles in the same directory (`block_<test>_<db>.pprof`, `mutex_<test>_<db>.pprof`) when the computed delta is non-empty
- `-trace` write runtime execution trace to file
- `-max-wall` abort the run if wall time exceeds this duration (guardrail; `0` = disabled)
- `-max-rss-mb` abort the run if RSS exceeds this many MiB (guardrail; `0` = disabled; Linux-only)
- `-checkpoint-between-tests` force a best-effort durability checkpoint between tests (DBs that support `Checkpoint()`), and also once after the final test so end-of-run disk usage reflects a settled state
- `-vacuum-between-tests` vacuum supported DBs between tests (implies `-checkpoint-between-tests`; TreeDB uses `VacuumIndexOnline`)
- `-treedb-vlog-rewrite-after-run` run the full TreeDB `CompactStorage` path after the run and report before/after disk usage + the data directory path; the flag name is kept for compatibility
- `-treedb-vacuum-after-vlog-rewrite-run` run offline TreeDB index vacuum after `-treedb-vlog-rewrite-after-run` before final reporting (disable explicitly with `-treedb-vacuum-after-vlog-rewrite-run=false`)
- `-checkpoint-every-ops` force a best-effort durability checkpoint every N ops during write-heavy tests (DBs that support `Checkpoint()`)
- `-checkpoint-every-bytes` force a best-effort durability checkpoint every N approx bytes during write-heavy tests (DBs that support `Checkpoint()`)
- `-suite` named suite:
  - `readme` — generates the README graphs + sweep tables
  - `churn` — churn + settled scans (`treedb,leveldb`)
  - `churnvacuum` — churn + settled scans, then index compaction and scan again
  - `flushdrain` — write burst → checkpoint boundary → read; prints checkpoint timing (TreeDB-focused). Use `-flushdrain-checkpoint-max=<duration>` to fail the suite if the checkpoint before `random_read` exceeds your latency target.
  - `flushthrash` — forces a small TreeDB flush threshold; catches flush thrash / runaway backlog regressions
  - `bigkeys_guard` — small TreeDB flush threshold + large keycount, with wall/RSS caps for CI guardrails
  - `longmix` — long-ish mixed workload + settle boundary with fragmentation reports
  - `sload_readheavy` — settled point reads with value-log pointers + forkchoice-style batch commits
  - `maintenance_budget` — sweep TreeDB maintenance K values; reports checkpoint time vs index size, recommends K
- `-outdir` output directory for suite artifacts (plots/images; used by `-suite readme`)

## Standard Profile Workflow (`benchprof`)

Use `-profile-dir` so all profiles and ops outputs are captured in one place:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile fast \
  -checkpoint-between-tests \
  -test random_write,random_delete,random_read,full_scan,prefix_scan \
  -profile-dir "$OUT" \
  -path-label native-fastpath \
  -progress=false

./bin/benchprof -profiles-dir "$OUT"
```

This writes:
- `benchprof_results.json` / `benchprof_results.md`
- `cpu_<test>_<db>.pprof`
- `allocs_<test>_<db>.pprof`
- `block_<test>_<db>.pprof` (when non-empty delta)
- `mutex_<test>_<db>.pprof` (when non-empty delta)
- `checkpoint_cpu_checkpoint_<test>_<db>.pprof`
- `block.pprof`, `mutex.pprof`, `trace.out`
- `insights.md`, `insights.json`, `insights.html` (from `benchprof`)

For TreeDB runs, `benchprof_results.json` also preserves selected TreeDB stats
under `runs[].treedb_stats`, including ordered-root/root-apply and cache
counters used by raw-engine review gates.

## Notes

TreeDB is a cached engine (memtable + background flush). If you run long write-heavy phases and then measure `random_read`/scans immediately, the results can be dominated by background flush work (“flush debt”).

Recommended:

- For *settled read/scan performance*: use `-checkpoint-between-tests` or `-settle-before-scans`.
- For *mixed workload under flush debt*: keep defaults and optionally enable `-treedb-cache-stats-before-reads` to see queue/backlog stats.

### Repro: mixed vs settled reads (TreeDB)

Mixed (reads under flush debt; intentionally stressful):

```bash
go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 \\
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read \\
  -treedb-cache-stats-before-reads -progress=false
```

Settled (reads after a durability boundary):

```bash
go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 \\
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read \\
  -checkpoint-between-tests -progress=false
```

### Repro: compression matrix (TreeDB dict + LevelDB block compression)

Run TreeDB twice (dict on/off) and LevelDB twice (block compression on/off) in one invocation:

```bash
./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb,leveldb -profile fast -keys 4000000 -format markdown \\
  -treedb-force-value-pointers \\
  -treedb-vlog-dict both \\
  -leveldb-block-compression both
```

To sweep dict-frame encoder knobs (zstd level × entropy coding), use:

```bash
./bin/unified-bench -test batch_write -dbs treedb -profile fast -keys 1000000 -format markdown \\
  -treedb-force-value-pointers \\
  -treedb-vlog-dict on \\
  -treedb-vlog-dict-frame-encode-level all \\
  -treedb-vlog-dict-frame-entropy both
```

### Repro: random read parallel sweep

Run `random_read_parallel` with separate worker counts:

```bash
./bin/unified-bench -dbs treedb,leveldb -profile fast -keys 500000 -test random_read_parallel -read-workers 1 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile fast -keys 500000 -test random_read_parallel -read-workers 2 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile fast -keys 500000 -test random_read_parallel -read-workers 4 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile fast -keys 500000 -test random_read_parallel -read-workers 8 -progress=false
```

`-test all` now includes `random_read_parallel` and `random_read_parallel_acquire_snapshot` in the output table:

```bash
./bin/unified-bench -dbs treedb,leveldb -profile fast -keys 500000 -test all -read-workers 4 -format markdown -progress=false
```
