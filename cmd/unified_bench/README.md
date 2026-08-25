# unified_bench

Side-by-side benchmarks for `HashDB`, `BTreeOnHashDB`, `TreeDB` (cached), Pebble, Badger, and LevelDB.

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
- `batch_delete_range` — Batch DeleteRange (dense ordered range-delete batches; result table is DeleteRange calls/sec)
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

`batch_delete_range` loads dense sortable 8-byte keys before timing, excludes that load/checkpoint phase from delete timing, then commits configured DeleteRange calls in batches. The main result table reports DeleteRange calls/sec; the `Batch DeleteRange Metrics` section and `benchprof_results.json` also report affected keys/sec, loaded key count, range width, ranges per batch, value size, validation status, and whether each adapter path is native or fallback. TreeDB and Pebble are reported as `native`; LevelDB is `fallback_iterator_delete` because it expands each range into iterator-driven point deletes and should not be treated as native range-delete parity. Because this workload deletes the dense `[0, keys)` keyspace, it is opt-in and is not part of the default `-test all` order; run `-test batch_delete_range` or `-test all,batch_delete_range` when you want it.

## Common flags

- `-profile` benchmark profile preset (see `cmd/unified_bench/profiles.go`):
  - `balanced` (default)
  - `durable` (strict durability)
  - `fast` (benchmark-runner no-sync preset; TreeDB enters the explicit `bench_unsafe` boundary with the Celestia-aligned auto/snappy/balanced value-log compression defaults; unsafe)
  - `wal_on_fast` (benchmark-runner relaxed-WAL preset; TreeDB maps this to `command_wal_relaxed` with verified read integrity and the same compression defaults)
  - These names are unified-bench presets shared across database adapters, not the public TreeDB server profile vocabulary. Public TreeDB servers should use `command_wal_durable`, `command_wal_relaxed`, or `no_wal_fast`; benchmark-only `bench_unsafe` requires an explicit benchmark constructor boundary.
- `-dbs` (`all` or CSV): `hashdb,btree,treedb,pebble,badger,leveldb`
  - Hidden TreeDB variants can be selected explicitly, including
    `treedb_public_command_wal` (alias `treedb_cached_command_wal`) for the
    public cached `command_wal_v1` path,
    `treedb_backend` for the direct backend path and
    `treedb_backend_command_wal` (alias `treedb_command_wal`) for the direct
    backend `command_wal_v1` path. The command-WAL variants report
    `treedb.command_wal.live_accepted_*`,
    `treedb.command_wal.live_covered_*`, and `treedb.applied_command_lsn`
    stats without scanning WAL segments, so benchmark artifacts can prove that
    typed frames were accepted and covered. Use
    `-treedb-command-wal-stats-scan` only when segment inventory counters such
    as `treedb.command_wal.typed_segments` are required.
  - Downstream adapter evidence should record the resolved TreeDB profile and
    include command-WAL counters plus checkpoint counters. In particular,
    distinguish `*Sync` command-WAL sync cost from explicit
    `Checkpoint()`/`Close()` publication cost; see
    `docs/TREEDB_DOWNSTREAM_VALIDATION.md`.
- `-test` (`all` or CSV): see list above
- `-keys` number of keys (default 100000)
- `-keycounts` comma-separated key counts to sweep over (overrides `-keys`)
- `-keyscale` generate keycounts by scale: `log10` or `doubling` (uses `-keys-min` / `-keys-max`)
- `-valsize` value size in bytes (default 128)
- `-val-pattern` value pattern for write tests (including `dataset_write_*`) (`zero|repeat|repeat_tail64|ultra_compressible_repeat|highly_compressible_notail|half_repeat_half_random|medium_compressible_sparse|celestia_height_prefix_fill|random`)
  - Note: dataset-write generation now uses the same normalized behavior as other write tests (legacy pattern names are accepted as aliases, but generation is unified under `makeValuePool`). Reusable dataset key/value fixtures for `dataset_write_*` and `dataset_read_random` are materialized before per-test CPU/allocation/contention profiles and runtime traces start, so those artifacts represent the measured DB loops rather than fixture setup.
- `-val-pool-size` number of distinct values to cycle through for `-val-pattern` (`0` = auto)
- `-batchsize` batch size (default 8000)
- `-batch-delete-range-width` affected keys per `batch_delete_range` DeleteRange call (default 100)
- `-batch-delete-ranges-per-batch` DeleteRange calls per `batch_delete_range` batch commit (default 100)
- `-batch-delete-range-validate` validate after measured deletes that loaded dense keys were removed (excluded from delete timing)
- `-batch-delete-range-refill` reload deleted dense keys after measured deletes/validation so later tests see the dataset (excluded from delete timing)
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
- `-treedb-leaf-page-read-cache-entries` TreeDB: outer-leaf read cache entries for leaf pages stored in the value log (`0`=default/env, `<0`=disable)
- `-treedb-leaf-page-read-cache-write-admission` TreeDB: write-side outer-leaf read-cache admission policy (`immediate|adaptive`; default preserves immediate admission)
- `-treedb-prefer-append-alloc` TreeDB: prefer append allocation over freelist reuse
- `-treedb-force-value-pointers` TreeDB: force all values into the value log
- `-treedb-value-log-threshold` TreeDB: inline-vs-pointer threshold
- `-treedb-vlog-compression` TreeDB: value-log compression mode (`default|off|block|dict|auto`)
- `-treedb-vlog-block-codec` TreeDB: block codec (`snappy|lz4|zstd`). When `-treedb-vlog-compression=auto`, this is the forced block-mode/default block codec, not proof that auto selected that codec; use the value-log codec summary and `treedb.cache.vlog_auto.*` stats for actual per-frame selection.
- `-treedb-vlog-auto-policy` TreeDB: value-log auto policy (`balanced|throughput|size`)
- `-treedb-vlog-generation-policy` TreeDB: generation policy (`default|off|hot_warm_cold`)

### TreeDB Advanced Tuning

These are mainly for experiments and should usually be left at engine defaults:

- `-treedb-vlog-compression-autotune`
- `-treedb-vlog-dict-*`
- `-treedb-vlog-rewrite-*`
- `-treedb-flush-build-*`
- `-treedb-flush-admission-policy=auto|explicit|off` (auto defaults to the admitted hardware-aware span-native/backlog path; off is the rollback and reports `reason=policy_off`, selected concurrency `0`, span-native `false`, and backlog coalescing `false`)
- `-treedb-flush-apply-concurrency`, `-treedb-flush-apply-min-*`, and `-treedb-journal-lanes` (auto apply defaults to detected physical cores capped by `GOMAXPROCS` and 8, with `min(GOMAXPROCS,8)` fallback when physical cores are unknown; default journal/value-log lanes are coalescing-safe; configured apply/lane values preserve c4/c8/c16/lane experiments)
- `-treedb-flush-apply-span-native` (M10 span-native apply/reducer override for eligible exact point spans; auto enables when admitted)
- `-treedb-flush-span-run-target-planning` (diagnostic/default-off read-only target-leaf planning for canonical flush runs)
- `-treedb-flush-backlog-coalescing*` (M11 bounded adaptive backlog coalescing controller; auto enables when admitted, byte/op budgets are soft pre-next-memtable limits)
- `-treedb-max-queued-memtables`, `-treedb-slowdown-backlog-seconds`, `-treedb-stop-backlog-seconds`

Benchmark reports include resolved TreeDB options and selected stats for `flush_admission.policy`, `admitted`, `reason`, configured/requested concurrency, selected/effective concurrency, concurrency cap reason, defaulting, `GOMAXPROCS`, physical cores, span-native enablement, and backlog-coalescing enablement. `reason=unsafe_durability` is the expected auto decline when WAL-off durability is allowed; support triage should treat that the same as `policy_off` for the span-native/backlog path, while preserving the caller's configured concurrency in the report for reproduction.

Use `./bin/unified-bench -h` for the full grouped TreeDB advanced flag list.

### TreeDB value-log codec matrix

For #2194-style codec characterization, use the matrix harness to run `auto`,
`off`, `block/snappy`, and `block/lz4` with one shared dataset shape per value
pattern:

```bash
RUN_DIR=/tmp/treedb_vlog_codec_matrix_$(date +%Y%m%d_%H%M%S) \
  scripts/treedb_vlog_codec_matrix.sh
```

Defaults cover `zero`, `celestia_height_prefix_fill`,
`half_repeat_half_random`, and `random` patterns with
`dataset_write_random,batch_random,random_read_parallel,full_scan,prefix_scan`.
Set `KEYS`, `VALSIZE`, `BATCHSIZE`, `READ_WORKERS`, `TESTS`, `PATTERNS`, and
leaf-mmap environment variables to match the policy matrix under review. Final
codec/default decisions must rerun this after #2190 frame grouping is merged or
explicitly deferred.

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
- `-column-store-asset-read-integrity` column-store typed asset hot-read integrity for `-suite column_store` physical paths (`verify|cached_verify|skip_checksums`). `cached_verify` verifies each immutable typed asset ref once per process cache entry, then skips repeated hot-read CRC for that ref; post-verification file corruption may go undetected until cache eviction or process restart. `skip_checksums` skips hot-read CRC. Both relaxed modes require `-treedb-allow-unsafe`; when unset, `-treedb-disable-read-checksum` also disables typed column-asset hot-read CRC verification for the suite.
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
- `-profile-dir` write all profile outputs into one directory (auto-sets defaults for `-cpuprofile`, `-allocsprofile`, `-checkpoint-cpuprofile`, `-blockprofile`, `-mutexprofile`, `-trace`; explicit flags still win). Also emits `benchprof_results.json` and `benchprof_results.md`, then automatically runs `benchprof` in-process. Profile artifacts default to `-path-label native-fastpath`; pass `-path-label oracle` for explicit oracle/comparator captures, `-path-label m8-m14-10mm-gate` for the #2768+ mandatory span-run gate shape, `-path-label span-native-default-gate` for the default span-native production closeout matrix, or `-path-label span-native-read-scan-guardrail` for settled read/scan guardrails tied to that closeout. For TreeDB, the markdown includes selected flush/apply stage counters (`treedb.cache.flush_apply.*`, `treedb.flush_apply.*`, including publish prepare/final-install splits), raw span-native route and public-command-WAL fallback counters (`treedb.raw.span_native.*`), leaf-log lane summary counters (`treedb.cache.leaf_log_lanes.*` aggregates plus raw preserved lane keys), M8 span-run proof counters (`treedb.cache.flush_span_run.*`, `treedb.flush_apply.span_run.*`, `treedb.flush_apply.span_native.*`), ordered-root span-native proof counters and triage rows (`treedb.publish.ordered_root_delta_group.span_native.*`), checkpoint split counters, and a value-log codec summary with actual auto/write-mode/outer-leaf codec selection and block frame-K counters.
- `-treedb-cache-stats-before-reads` print select `treedb.cache.*` stats before read/scan tests (treedb only)
- `-blockprofile`, `-mutexprofile` write global profiling artifacts to files and also emit per-test contention delta profiles in the same directory (`block_<test>_<db>.pprof`, `mutex_<test>_<db>.pprof`) when the computed delta is non-empty
- `-trace` write runtime execution trace to file
- `-max-wall` abort the run if wall time exceeds this duration (guardrail; `0` = disabled)
- `-max-rss-mb` abort the run if RSS exceeds this many MiB (guardrail; `0` = disabled; Linux-only)
- `-checkpoint-between-tests` force a best-effort durability checkpoint between tests (DBs that support `Checkpoint()`), and also once after the final test so end-of-run disk usage reflects a settled state
- `-checkpoint-settle-before-tests` comma-separated checkpoint labels that should first wait for TreeDB queue/background debt to drain before checkpointing (for example `random_read`, `post-run`, or `all`; explicit labels must match the selected test order or the run fails); useful for immediate-vs-settled checkpoint comparisons without changing production behavior
- `-checkpoint-settle-timeout` maximum wait for `-checkpoint-settle-before-tests` (default `10m`)
- `-vacuum-between-tests` vacuum supported DBs between tests (implies `-checkpoint-between-tests`; TreeDB uses `VacuumIndexOnline`)
- `-treedb-vlog-rewrite-after-run` run the full TreeDB `CompactStorage` path after the run and report before/after disk usage + the data directory path; the flag name is kept for compatibility
- `-treedb-vacuum-after-vlog-rewrite-run` run offline TreeDB index vacuum after `-treedb-vlog-rewrite-after-run` before final reporting (disable explicitly with `-treedb-vacuum-after-vlog-rewrite-run=false`)
- `-checkpoint-every-ops` force a best-effort durability checkpoint every N ops during write-heavy tests (DBs that support `Checkpoint()`)
- `-checkpoint-every-bytes` force a best-effort durability checkpoint every N approx bytes during write-heavy tests (DBs that support `Checkpoint()`)
- `-suite` named suite:
  - `geth_hot_kv` — 30k-key raw-KV proxy for geth/Nitro hot database behavior: sequential point write, random read, full iteration, and dense `DeleteRange`; defaults to `treedb_public_command_wal,pebble,leveldb` with random 128-byte values and reports a compact #2392-style summary. Use `scripts/treedb_geth_hot_kv_bench.sh` for the standard unified-bench wrapper. For the integrated geth `node.OpenDatabase` / `ethdb` benchmark and key/value/batch matrix, use `benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go` via `scripts/treedb_geth_hot_kv_matrix.sh`.
  - `readme` — generates the README graphs + sweep tables
  - `churn` — churn + settled scans (`treedb,leveldb`)
  - `churnvacuum` — churn + settled scans, then index compaction and scan again
  - `flushdrain` — write burst → checkpoint boundary → read; prints checkpoint timing (TreeDB-focused). Use `-flushdrain-checkpoint-max=<duration>` to fail the suite if the checkpoint before `random_read` exceeds your latency target.
  - `flushthrash` — forces a small TreeDB flush threshold; catches flush thrash / runaway backlog regressions
  - `bigkeys_guard` — small TreeDB flush threshold + large keycount, with wall/RSS caps for CI guardrails
  - `longmix` — long-ish mixed workload + settle boundary with fragmentation reports
  - `sload_readheavy` — settled point reads with value-log pointers + forkchoice-style batch commits
  - `maintenance_budget` — sweep TreeDB maintenance K values; reports checkpoint time vs index size, recommends K
  - `column_store` — native TreeDB column-store benchmark/artifact suite; writes stage-separated throughput, parity, byte-accounting, manifest/recovery, `column_store_results.{json,md,html}`, `benchprof_results.{json,md}`, and `insights.{md,json,html}` when `-profile-dir` is set
  - `collection_storage` — TreeDB collection storage-mode comparison suite across retained-document, typed-row-asset, typed-column-part, hybrid, and vector-typed-column layouts; writes semantic-comparability metadata, per-workload correctness/telemetry, `collection_storage_results.{json,md,html}`, `benchprof_results.{json,md}`, and `insights.{md,json,html}` when `-profile-dir` is set
- `-outdir` output directory for suite artifacts (plots/images; used by `-suite readme`)

## Standard Profile Workflow (`benchprof`)

Use `-profile-dir` so all profiles and ops outputs are captured in one place.
This example uses unified-bench's legacy `fast` benchmark-runner preset for a
no-WAL profiling ceiling; it is not a TreeDB server profile recommendation:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile "$BENCH_PROFILE" \
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
under `runs[].treedb_stats`, including ordered-root/root-apply, value-log mmap
read counters, and generic plus leaf-specific mmap sealed-budget caps used by
raw-engine review gates. Checkpoint-enabled runs also export
`runs[].checkpoint_durations_seconds`; selected settled runs export
`runs[].checkpoint_settle_seconds`; and TreeDB checkpoint snapshots are preserved
under `runs[].checkpoint_treedb_stats` so immediate-vs-settled checkpoint rows can
use the checkpoint-local `*_last` counters even if a final no-op checkpoint runs
before end-of-run stats are captured. Selected mmap read display fields prefer the backend
`treedb.vlog.mmap_read.*` family over cache aliases so counts and ratios come
from one source family. The `collection_storage` suite also
adds `runs[].collection_workloads` entries with stable mode/workload names,
semantic-equivalence flags, correctness-validation status, asset-byte splits,
and materialization / typed-column / vector counters for `benchprof` rendering.

## Collection Storage Suite

Use `collection_storage` to compare logical TreeDB collection workloads across
first-class storage layouts without relying on ad-hoc collection benchmarks. The
suite builds the same deterministic JSON document fixture for every mode,
optionally checkpoints and reopens each DB before reads, then times only the
selected workload loops after a correctness pass. `-profile balanced` is accepted
as a durable alias; unsafe profiles are rejected because the suite is intended as
a comparable storage-mode gate.

Stable mode names:

- `document_only` — full retained JSON document, no typed-storage owners.
- `typed_row_asset` — declared scalar fields owned by typed row assets.
- `typed_column_part` — declared scalar fields owned by typed column parts.
- `hybrid_document_row`, `hybrid_document_column`, `hybrid_row_column`,
  `hybrid_document_row_column` — explicit hybrid retained-payload/typed-owner
  layouts.
- `vector_typed_column` — scalar row owners plus a typed-column float32 vector
  field and column-graph vector index.

Stable workload names:

- `insert_batch`, `point_get`, `predicate_scan`, `aggregate`,
  `vector_search_smoke`, `mixed`.

Unsupported combinations fail closed in the report: for example,
`vector_search_smoke` is marked unsupported for scalar-only modes instead of
silently comparing a different workload shape. If a selected workload has no
supported selected mode (for example `-collection-storage-modes document_only
-collection-storage-workloads vector_search_smoke`), the command exits with a
clear semantic error.

```bash
OUT=$(mktemp -d /tmp/gomap_collection_storage_profiles_XXXXXX)
BENCH_PROFILE=durable # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench \
  -suite collection_storage \
  -dbs treedb \
  -profile "$BENCH_PROFILE" \
  -keys 10000 \
  -batchsize 1000 \
  -collection-storage-modes all \
  -collection-storage-workloads all \
  -profile-dir "$OUT" \
  -path-label native-fastpath \
  -progress=false
```

Collection-storage flags:

- `-collection-storage-modes` CSV mode subset (`all` by default).
- `-collection-storage-workloads` CSV workload subset (`all` by default).
- `-collection-storage-query-count`, `-collection-storage-point-get-count`.
- `-collection-storage-selectivity`, `-collection-storage-cardinality`,
  `-collection-storage-payload-size`, `-collection-storage-field-count`.
- `-collection-storage-vector-dims`, `-collection-storage-vector-top-k`,
  `-collection-storage-include-final-fetch`. When final fetch is enabled, the
  default vector response shape is `projection_without_embedding` via
  `ProjectionOrientedVectorDocumentFetchPreset`; add
  `-collection-storage-vector-full-documents` only for explicit
  full-document/embedding-echo comparison rows.
- `-collection-storage-checkpoint-reopen` to include the durability/recovery
  boundary before read workloads (default true).
- `-collection-storage-asset-read-integrity` (`verify|cached_verify|skip_checksums`;
  relaxed modes require `-treedb-allow-unsafe`).

The suite writes:

- `collection_storage_results.json`, `collection_storage_results.md`,
  `collection_storage_results.html`
- `benchprof_results.json`, `benchprof_results.md`
- `insights.md`, `insights.json`, `insights.html`
- configured runtime profiles, including
  `cpu_collection_storage_treedb_collection_storage.pprof`,
  `allocs_collection_storage_treedb_collection_storage.pprof`,
  `checkpoint_cpu_checkpoint_collection_storage_treedb_collection_storage.pprof`,
  `block.pprof`, `mutex.pprof`, and `trace.out`

## Column Store Suite

Use `column_store` as the canonical native TreeDB column-store benchmark entry
point. It measures the production column-enabled collection manifest/control
path, durable command-WAL publication, isolated physical column assets, planner
diagnostics, parity, byte accounting, and executable row-store / B-tree /
physical-column labels with a deterministic JSONBench-shaped fixture.
`-profile balanced` is accepted as a standard benchmark preset for the column
store suite, so the unified-bench default still exercises the durable gate. The runnable
execution labels are `row_store_baseline`, `b_tree_index_baseline`,
`serial_column_scan`, `aggregate_metadata`, and `parallel_column_scan`.
The `aggregate_metadata` path uses typed aggregate metadata for q1, q4b, and
q5_metadata where those assets are available; the other synthetic query shapes
reroute to serial physical scans. `sum_time_second_of_day_square` is an
arbitrary-expression lane that must visit typed `time_us` cells unless a future
planner explicitly maintains a matching generated expression or aggregate.

```bash
OUT=$(mktemp -d /tmp/gomap_column_store_profiles_XXXXXX)
BENCH_PROFILE=durable # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench \
  -suite column_store \
  -dbs treedb \
  -profile "$BENCH_PROFILE" \
  -keys 100000 \
  -batchsize 1000 \
  -profile-dir "$OUT" \
  -path-label native-fastpath \
  -column-store-path row_store_baseline \
  -progress=false
```

Use `-column-store-query q3` or a comma-separated subset such as
`-column-store-query q2,q3,q4b,sum_time_second_of_day_square` when collecting
query-isolated 100K-1M row CPU/allocation profiles. Omit it, or pass `all`, for
the default full q1-q5/q5_metadata/expression suite. Duplicate query names are
rejected so benchprof tables and artifact labels stay unambiguous.

Use `-column-store-first-touch-after-open` when collecting the secondary
`first_touch_after_open` lane. It closes the warmed reopened DB after the main
query pass, then reopens the DB and collection once per selected query and
records separate `first_touch_queries` rows with reopen/open time included in
`prepare_setup_duration_ms`. The primary `queries` rows remain the
`one_shot_end_to_end` lane.

Column-store non-column retained payloads default to `semantic-stream-v1` so the
production storage-parity path uses retained semantic-stream side-root blocks.
Use `-column-store-retained-payload-encoding template-v1`, or set
`TREEDB_COLUMN_STORE_RETAINED_PAYLOAD_ENCODING=template-v1`, to run the legacy
template-v1 retained-payload path for comparison. `b_tree_index_baseline` still
keeps full retained JSON so that index-baseline comparisons remain unchanged.

The suite writes:

- `column_store_results.json`, `column_store_results.md`, `column_store_results.html`
- `benchprof_results.json`, `benchprof_results.md`
- `insights.md`, `insights.json`, `insights.html`
- configured runtime profiles, including `cpu_column_store_treedb_column_store.pprof`, `allocs_column_store_treedb_column_store.pprof`, `checkpoint_cpu_checkpoint_column_store_treedb_column_store.pprof`, `block.pprof`, `mutex.pprof`, `trace.out`, and the query-phase delta `block_column_store_treedb_column_store.pprof` / `mutex_column_store_treedb_column_store.pprof` when those profile classes produce non-empty deltas

Column-store JSON and Markdown insert statistics include physical-entry lookup
probes, comparisons, and admissions so scale runs can verify that indexed
root-publication lookup is active and bounded.

`column_store_results.json` includes `query_mode` and `metadata_mode` labels on
query rows and a `jsonbench_cells` matrix for the in-repo synthetic
JSONBench-shaped fixture. Each cell records the external-facing label
(`row-scan`, `column-direct`, `column-prepared`, `column-direct-metadata`, or
`column-prepared-metadata` when the mode exists), query name, sort layout,
storage source, direct/prepared mode, metadata-vs-data path, compression mode,
mutation mode, retained-payload policy, typed storage owner, row count, result
hash, and reconstruction/full-data caveats. Prepared cells are collected in a
separate `jsonbench_cell_report` stage after query-phase profiles finish, so the
query CPU/allocation profiles and `BenchmarkColumnStoreSuite*` query-loop
benchmarks remain scoped to the original measured query loop. These rows are
gomap-local smoke coverage. The external `snissn/JSONBench` harness owns
full-data retained-JSON/reconstruction parity (#2117) and apples-to-apples
storage accounting (#2118), so headline ClickHouse/full-data claims require a
fresh external JSONBench run against the selected gomap dependency.

PR descriptions for column-store milestones should paste the command, row count,
profile, forced path, q1-q5/q5_metadata/expression rows/sec, MiB/sec, ns/row,
planner time, scan time, reduce time, worker count, scheduled/skipped granules,
cache hit/miss counters, materialized-row count, parity status, storage source,
fallback reason, manifest root name/id, active manifest generation/checksum,
byte accounting, manifest/recovery identity, and the generated HTML artifact
paths. If a forced physical column path is not implemented yet, the PR must call that out
explicitly and include the fail-closed evidence. The suite reports measured
`column_asset_bytes` from the isolated `column_assets/` tree after physical
assets are published, plus explicit `column_asset_store_bytes`,
`primary_index_bytes`, `ordinary_value_vlog_bytes`, and `leaf_vlog_bytes` splits
so M12+ evidence does not confuse typed column assets with row value-log,
leaf-log, or primary-index storage. `durable_storage_bytes_wal_excluded` is the
steady-state comparison label: it subtracts only valid command WAL segment files
named `wal/commit-l<lane>-<seq>.log` (numeric lane, non-zero sequence) from
`db_total_bytes`, while durable `value_vlog`, `leaf_vlog`, `index.db`, column
assets, and manifest/control bytes remain included. `retained_payload_bytes`
reports the row/remainder payload for the selected path; physical paths that
strip declared columns into column assets should report less retained payload
than the source JSONBench document bytes. With `semantic-stream-v1`,
`retained_payload_bytes` includes primary locator bytes plus the retained
semantic-stream side-root block bytes.

For post-V1 production-vs-experiment attribution, use the slope harness:

```bash
RUN_DIR=/tmp/treedb_column_store_slope_$(date +%Y%m%d_%H%M%S) \
  KEYCOUNTS=10000,100000,500000 \
  ROUTED_PATHS=serial_column_scan,aggregate_metadata,parallel_column_scan \
  scripts/treedb_column_store_slope_profile.sh
```

The harness builds `unified-bench`, runs routed production `column_store` cases
into per-keycount/per-path profile directories, captures direct production
package benchmarks for the TCPA asset scanner / collection scanner / query
adapter, captures the older `experiments/colgranule` encoded-kernel baselines,
and writes `summary.tsv` plus `summary.md`. Use the generated HTML reports for
review and keep ClickHouse-equivalent JSONBench context tied to
`experiments/colgranule/JSONBENCH_COMPARISON_REPORT.md`.

## Notes

TreeDB is a cached engine (memtable + background flush). If you run long write-heavy phases and then measure `random_read`/scans immediately, the results can be dominated by background flush work (“flush debt”).

Recommended:

- For *settled read/scan performance*: use `-checkpoint-between-tests` or `-settle-before-scans`.
- For *mixed workload under flush debt*: keep defaults and optionally enable `-treedb-cache-stats-before-reads` to see queue/backlog stats.

### Repro: mixed vs settled reads (TreeDB)

Mixed (reads under flush debt; intentionally stressful):

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

go run ./cmd/unified_bench -dbs treedb -profile "$BENCH_PROFILE" -keys 900000 -valsize 128 -batchsize 1000 \\
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read \\
  -treedb-cache-stats-before-reads -progress=false
```

Settled (reads after a durability boundary):

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

go run ./cmd/unified_bench -dbs treedb -profile "$BENCH_PROFILE" -keys 900000 -valsize 128 -batchsize 1000 \\
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_delete,batch_small_seq,random_delete,random_read \\
  -checkpoint-between-tests -progress=false
```

### Repro: compression matrix (TreeDB dict + LevelDB block compression)

Run TreeDB twice (dict on/off) and LevelDB twice (block compression on/off) in one invocation:

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 4000000 -format markdown \\
  -treedb-force-value-pointers \\
  -treedb-vlog-dict both \\
  -leveldb-block-compression both
```

To sweep dict-frame encoder knobs (zstd level × entropy coding), use:

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench -test batch_write -dbs treedb -profile "$BENCH_PROFILE" -keys 1000000 -format markdown \\
  -treedb-force-value-pointers \\
  -treedb-vlog-dict on \\
  -treedb-vlog-dict-frame-encode-level all \\
  -treedb-vlog-dict-frame-entropy both
```

### Repro: random read parallel sweep

Run `random_read_parallel` with separate worker counts:

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test random_read_parallel -read-workers 1 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test random_read_parallel -read-workers 2 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test random_read_parallel -read-workers 4 -progress=false
./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test random_read_parallel -read-workers 8 -progress=false
```

`-test all` now includes `random_read_parallel` and `random_read_parallel_acquire_snapshot` in the output table:

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test all -read-workers 4 -format markdown -progress=false
```
