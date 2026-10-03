# unified_bench

Raw value-log frame read amplification has a standalone production-planner
microbenchmark:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkValueLogRawFrameRead$' -benchmem -benchtime=1s -count=3
```

It reports owned sealed-mmap read allocations and CRC bytes/checks per result.
See [the lifecycle runbook](../../TreeDB/docs/spec/value-log-lifecycle.md#13-durable-raw-frame-read-cost)
for its boundaries. Its Go benchmark text/profiles are not benchprof inputs;
use the public native Quicksilver harness for durability and workload guardrails.


Standalone checkpointed point-read allocation/throughput qualification:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB -run '^$' -bench '^BenchmarkDBCheckpointedValueLogGet$' -benchmem -benchtime=1s -count=5
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/db -run '^$' -bench '^BenchmarkGetVersioned$' -benchmem -benchtime=1s -count=5
```

The public benchmark uses 32,768 keys and 256-byte pointer values, then
checkpoints before timing so the cached API reaches backend capture. `Get` and
`GetUnsafe` return owned copies; `GetAppend` reuses caller storage. Backend
`BenchmarkGetVersioned` measures versioned appends into caller storage. Compare
identical fixtures and commands on the same host at exact source heads; warm
reads are the timed boundary. Output is Go benchmark text. Optional Go test
CPU/allocation profiles must be inspected directly with `go tool pprof`; they
are not `benchprof_results.json` inputs. No unified-bench artifact schema changes.


Side-by-side benchmarks for `HashDB`, `BTreeOnHashDB`, `TreeDB` (cached), Pebble, Badger, and LevelDB.

## Run

For the standalone public MVCC durable singleton point/batch comparison:

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkCommitAtCommandWALDurableSingleton$' -benchmem -benchtime=1000x -count=5
```

Inline, pointer and oversized cases report `B/op`, `allocs/op` and
frame/sync/checkpoint counters. Output is Go benchmark text; optional Go test
profiles are not benchprof inputs. Use matched fresh-process controls on the
same host for performance acceptance.

- Build: `make unified-bench` (writes `bin/unified-bench`)
- Run: `./bin/unified-bench`
- Or: `go run ./cmd/unified_bench`

`BenchmarkDocumentSnapshotGrowthV1` and `BenchmarkDocumentSnapshotForegroundV1`
in `TreeDB/internal/raftfsm` exercise real FSM export/install and foreground
commits, rather than a unified-bench database adapter. Capture their five leaves
in separate processes with
`scripts/treedb_document_snapshot_evidence.sh OUTPUT_DIRECTORY`. The script
uses `-benchtime=1x -count=1`, records Go test JSON and one CPU/heap profile per
leaf, and saves the source head and Go version from a required clean checkout.
The `DOCUMENT_SNAPSHOT_EVIDENCE` JSON log records the measured phase; process
profiles also include fixture population. These artifacts are not
`unified-bench -profile-dir` outputs.
Growth records use `source_directory_file_bytes_before_export` for the source
directory size before export and `archive_bytes` for the streamed archive size.
Foreground records use `archive_bytes` for the native snapshot size.

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

For public MVCC writes with early snapshots, use the standalone
`BenchmarkAdaptiveMVCCSnapshotCommandWAL` in `TreeDB/caching`:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkAdaptiveMVCCSnapshotCommandWAL$' -benchtime=2240x -count=5 -benchmem
```

It reports allocation bytes/counts, throughput, selection samples/reasons, and
command-WAL counters for relaxed/durable profiles and adaptive/fixed modes.
Use fixed counts >=1120; separate `-cpuprofile`/`-memprofile` runs are Go test
profiles for `go tool pprof`, not benchprof inputs. See `cmd/benchprof/README.md`.

`-test all` now includes `random_read_parallel` and `random_read_parallel_acquire_snapshot` in the output table:

```bash
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench -dbs treedb,leveldb -profile "$BENCH_PROFILE" -keys 500000 -test all -read-workers 4 -format markdown -progress=false
```

### Public exact-key MVCC iteration diagnostic

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^TestVersionIterationExactKeyFixture$' -count=1
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkVersionIterationExactKey$' -benchmem -benchtime=1x -count=1
```

The smoke exercises depths 1/8/64 over 128 logical keys and eight populated
shards, with non-exact timestamps and full open/seek/owned-consume/close cost.
The M1 fixture selects `VersionIteratorOptions.ExactKey`; a before/after packet
must declare the baseline's original logical `key` to `key+NUL` selection and
retain both fixture identities. Results and ownership work are equivalent;
canonical bounds and shard-local source opens are the intended difference.
FrozenQueue and PublishedRoot exclude setup; Interleaved includes eight
singleton replacements per completed read. `observe=false/true` isolates the
existing default-disabled iterator diagnostic switch. Cached cut/source metrics
exclude the public backend-only snapshot fast path. Heap end and sampled maxima
are process-wide (including setup); flush counters exclude cleanup/drain. See
`TreeDB/docs/spec/verification.md` section 10.4 for counter denominators.

For retained timing, select one leaf per fresh process, freeze the source/runtime
identities, host, toolchain, benchtime/count, matched AB/BA order and exclusion
policy before collection, and keep profiles in separate runs. Go benchmark text
and optional Go test profiles are standalone artifacts, not benchprof inputs or
rows of the matched TreeDB/Badger decision matrix. This harness does not alter
unified-bench producer filenames or the benchprof parser contract.

## Standalone accepted partition cost profiles

`TestMultiOwnerTCPAcceptedDisjointRetained100KV1` in `TreeDB/nativewire`
writes ten cumulative sampled allocation profiles when the comparative cost
opt-in is enabled: `accepted-cost-local-before.pprof`,
`accepted-cost-local-after.pprof`, and
`accepted-cost-node-{0,1,2,3}-{before,after}.pprof` (eight daemon files).
These standalone Go test profiles are **not benchprof inputs** and are not
`unified-bench -profile-dir` outputs. Retain all ten files with the 516 JSON
receipts, source/input pins and sampling-rate/boundary metadata.

After harness review/landing, exact-source freeze and runner admission, use
a reviewed retained-input descriptor and its independently pinned SHA256.
The descriptor must retain the accepted M3/source/DB provenance; the checksum
does not replace those guards. Use absolute input/receipt paths, create a fresh
receipt directory and run once
inside the admitted 8 GiB, zero-swap scope with the pinned offline toolchain:

```sh
mkdir -m 700 "$PARTITION_RECEIPTS"
unset GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1
GOWORK=off GOMAXPROCS=4 GOFLAGS='-p=2 -mod=readonly' \
  GOMAP_SELECTED_LIVE_FIXTURE="$PARTITION_INPUT" \
  GOMAP_SELECTED_LIVE_FIXTURE_SHA256="$PARTITION_INPUT_SHA256" \
  GOMAP_SELECTED_LIVE_RECEIPTS="$PARTITION_RECEIPTS" \
  GOMAP_ACCEPTED_COMPARATIVE_COST_V1=1 \
  go test ./TreeDB/nativewire \
    -run '^TestMultiOwnerTCPAcceptedDisjointRetained100KV1$' \
    -count=1 -timeout=750s -v
```

Inspect each matched pair directly, for example
`go tool pprof -sample_index=alloc_space -base BEFORE.pprof AFTER.pprof`;
repeat with `alloc_objects`. Run analysis under runner admission as well.
Two GCs flush samples outside search timers. Differences include background,
observer and profile/control work; they are statistical estimates, not exact
query/navigation/merge costs. Instrumented timings and global allocations are
not an apples-to-apples comparison with older uninstrumented samples.
See [the full observation contract](../../docs/benchmarks/treedb_partition_accepted_cost_diagnostic.md)
for framed-byte/RSS boundaries, validation and retained-collection limits.

### MVCC ownership allocation diagnostic

`BenchmarkMVCCOwnership` compares public GetAt, borrowed inspection and owned
version collection at widths 8/16/4096/32768 and depths 1/8/64, with ordinary
and forced-pointer routing. Ordinary large values can use automatic pointers.
Setup/checkpoint are excluded; completed reads, validation, final owned output
allocations and Close are charged. Iterator rows include matched clock samples
and report `first_entry_ns/op` alongside complete-consumption ns/op. Baseline
source without EntryView uses Entry for inspection, dispatching once per opened
iterator. Existing Filtered benchmarks remain traversal guards.

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkMVCCOwnership$' -benchmem -benchtime=1x -count=1
GOWORK=off go test -c -o /tmp/mvcc-ownership.test ./TreeDB/mvcc
GOMAP_MVCC_RETAINED_OUTPUTS=1 /tmp/mvcc-ownership.test -test.run '^TestMVCCRetainedOwnedOutputs/width=8/pointers=false$' -test.v
```

The first command is fixture smoke, not retained performance acceptance. Use the
same fixture source on base/head binaries and predeclare measured rows/repeats
before collection. Run each retained-output width/routing subtest in a fresh
process. It validates 4096 outputs after Store/DB closure and two GCs, retains
them with KeepAlive, and logs raw heap gauges and equal payload bytes. Transferred
payloads retain the whole envelope backing allocation and its allocator size
class; slice-capacity reduction cannot reduce that live storage. Lower B/op
alone does not establish lower retained heap or throughput. These Go test
artifacts/profiles are standalone diagnostics, not benchprof inputs.

### Value-log mixed-frame decode reuse

This standalone package benchmark alternates compressed 64 KiB/4 KiB frames
through the shared bounded decoder, warming codecs before the timer. It reports
Go benchmark text with ns/op, MB/s, B/op, allocs/op, `backing_allocs/op`, and
`scratch_cap_B` (one buffer's apparent capacity, not process RSS).

```sh
GOWORK=off go test ./TreeDB/internal/valuelog -run '^$' \
  -bench '^BenchmarkValueLogDecodeFrame(Alloc|MixedReuse)$' \
  -benchmem -count 5
GOWORK=off go test ./TreeDB -run '^$' \
  -bench '^BenchmarkDBValueLogGet$/^Get$' -benchmem -count 5
```

Keep the same benchmark source on baseline and candidate and serialize timed
captures. The public owned-Get benchmark is a separate guardrail; scratch
allocation reduction alone does not prove faster reads. These are Go test
outputs, not unified-bench profile-dir artifacts or benchprof inputs. No
profile parser or on-disk format changes are required. See the
[typed-storage performance guide](../../TreeDB/docs/guides/typed-storage-performance.md#persistent-value-log-decode-scratch).

### Owned persistent value-log reads

The standalone `BenchmarkFileReadAppendOwned` and
`BenchmarkDBOwnedValueLogRoute` compare cache copies, decode, OS-cache-warm
open/map/read/close, and ordinary public owned Get. The capture also runs landed
`BenchmarkFileReadAppendCompressedFallback`,
`BenchmarkValueLogRandomReadGroupedFrame_ReadUnsafeTo`, and
`BenchmarkDBValueLogGet/Get` and `/GetAppend` guardrails.

Prepare the source/binaries/fixture described in the
[retained packet](../../docs/benchmarks/treedb_owned_values_20261001/README.md), then run:

```sh
bash docs/benchmarks/treedb_owned_values_20261001/qualify.sh FROZEN_CAPTURE_DIR
```

It writes fresh-process Go benchmark text, separate `/usr/bin/time -v` RSS text,
and source/fixture hash inventories. These are standalone artifacts, not
unified-bench profile-dir output or benchprof inputs.

The final shared mmap publication repair has a separate
[95-cell qualification and 20-cell repair control](../../docs/benchmarks/treedb_owned_values_20261001/repair-9ab02b48/README.md).
It adds `BenchmarkOwnedMmapBudgetDenied512` and
`BenchmarkOwnedMmapConcurrentFirstAdmission` in an identical test-only overlay.
Retained stdout, stderr and time-v RSS are separate streams; source/binary/fixture
hashes and successful process statuses are checked. Prepare a new reviewed
freeze following that packet, then run:

```sh
bash docs/benchmarks/treedb_owned_values_20261001/repair-9ab02b48/inputs/qualify-mmap-repair.sh NEW_FROZEN_CAPTURE_DIR
```

Ordinary pointer Get improves in that bounded comparison; capped nil fallback
and OS-warm first-map lifecycle costs increase and remain explicitly disclosed.
The earlier packet retains its original source identities.

### Main-cache memory/placement workflow

`BenchmarkMemoryBudgetWorkflow` is a fixed public load/checkpoint/read/GC/update/
reopen workflow. Its dedicated capture freezes existing main leaf/frame cache
limits in five 64 MiB combined configured-budget splits, with two value sizes
and pointer thresholds. It records phase allocations, full Stats owners,
heap/RSS/mapping observations and filename-level logical storage bytes.

```sh
GOWORK=off go test ./TreeDB -run '^TestMemoryBudgetFixture$' -count=1
python3 scripts/treedb_memory_budget_capture.py self-check
```

Use the [memory-budget runbook](../../docs/benchmarks/treedb_memory_budget/README.md)
for the prepare/run/validate commands and external source/overlay/binary freeze.
An 8,192-key pilot is unretained; full cells use 250,000 keys in fresh processes
and wait for the reviewed harness to land. Configured cache bytes are not equal
physical RAM. These standalone package benchmark packets/logs are not
unified-bench profile-dir artifacts or benchprof inputs.

## TreeDB algorithm-work package harness

The standalone `BenchmarkAlgorithmSparseUpdates` and `BenchmarkAlgorithmGetMany`
use public TreeDB APIs without unified-bench adapters. Their fixed-work boundary,
counter-only overlay, artifact units, and reproduction commands are documented in
[`docs/benchmarks/treedb_algorithm_work_20261001`](../../docs/benchmarks/treedb_algorithm_work_20261001/README.md).
These package-test profiles are not benchprof inputs.
Its `capture.py prepare|capture|validate` flow retains an external source/dependency/
toolchain/binary freeze and separately hashed `stdout.log`/`stderr.log`; internal
visit diagnostics use a separate overlay binary and cannot enter timed captures.

`BenchmarkAlgorithmSparseUpdatesSnapshotRotations` adds one checked public
snapshot/read/close per acknowledged batch. It emits
`algorithm-work-snapshot-rotations-v1` packets with snapshot counts and
`snapshot_ns`, using the same freeze and raw artifact format. After review,
landing, preparation and coordinator runner grant, select it explicitly:

```sh
python3 docs/benchmarks/treedb_algorithm_work_20261001/capture.py capture \
  --source "$PWD" --prepared /tmp/algorithm-prepared \
  --output /tmp/algorithm-snapshot-rotations-eligibility --family snapshot-rotations \
  --grant COORDINATOR_EXCLUSIVE_GRANT --freeze-sha256 "$NORMAL_FREEZE_SHA"
```

### Leaf point lookup microprofiles

Capture `BenchmarkLeafPointMetadata`, `BenchmarkLeafCommonPrefixSuffixSearch`
(`TreeDB/node`) and `BenchmarkPointValueMetadata` (`TreeDB/tree`) sequentially in
fresh Go test processes:

```sh
RUN_DIR=/tmp/treedb_point_lookup_profiles scripts/treedb_point_lookup_profile.sh
# Bounded artifact smoke check; this does not measure throughput:
BENCHTIME=1x RUN_DIR=/tmp/treedb_point_lookup_smoke scripts/treedb_point_lookup_profile.sh
```

The default is `BENCHTIME=200ms`, count 1, with `GOWORK=off`. Each benchmark family
writes `<name>.txt` (Go benchmark stdout/stderr), `<name>.test` (test binary),
`<name>_cpu.pprof`, `<name>_allocs.pprof`, and matching `_cpu_top.txt` /
`_allocs_top.txt` reports. Names are `node_metadata`, `node_search`, and
`tree_metadata`. `source-head.txt`, `source-status.txt`, `source-diff.patch`,
`go-version.txt`, and `settings.txt` record source/toolchain provenance.
Profiles include fixture setup and the whole test process; compare unprofiled
benchmark timing separately. These standalone Go profiles are **not benchprof
inputs** or unified-bench profile-dir artifacts; inspect them with `go tool pprof`.

## Canonical Quicksilver workflow

The [canonical Quicksilver workflow](../../docs/benchmarks/treedb_quicksilver_workflow/README.md)
reuses the package fixture and main-cache freeze for explicit public read/update
phases. Its JSON/raw logs and standalone Go profiles have their own schema;
they are not unified-bench profile-dir artifacts or benchprof inputs.

The standalone Quicksilver workflow packet also retains40 (pilot8)
WriteSync-only update acknowledgement samples, a separate fixed-present owned
Get concurrent reader, and explicitly scoped process IO counters. See
[its measured boundaries](../../docs/benchmarks/treedb_quicksilver_workflow/README.md);
these package packets are not benchprof profile-dir artifacts.

## Quicksilver shaped KV workload

The native `-suite quicksilver` uses the ordinary database registry and adapters.
It measures a local KV workload, without reproducing Cloudflare replication.
The default fixture is `random4k`: 100,000 keys and 4096-byte deterministic random
values. `-quicksilver-case structured256` selects 250,000 keys and 256-byte
compressible values. Values contain big-endian key ID and generation headers;
32-byte keys share the reference's 24-byte prefix. Even keys exist; adjacent odd
keys are absent. `-keys` bounds either case for smoke runs.

```sh
GOWORK=off go build -o bin/unified-bench ./cmd/unified_bench
# durable is the cross-DB benchmark preset; TreeDB resolves command_wal_durable.
GOMAXPROCS=8 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable
# The cross-DB preset keeps aggregate counts fixed; reader width and GOMAXPROCS are independent.
GOMAXPROCS=8 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable \
  -read-workers 32 -quicksilver-case structured256
# Bounded correctness/profile rehearsal (not a throughput qualification).
OUT=$(mktemp -d /tmp/quicksilver_profiles_XXXXXX)
# durable is the cross-DB benchmark preset.
GOMAXPROCS=4 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable \
  -keys 8192 -read-workers 4 -quicksilver-reads 65536 \
  -quicksilver-duration 200ms -profile-dir "$OUT"
./bin/benchprof -profiles-dir "$OUT"
```

The workflow loads synchronous 1000-value batches, checkpoints, closes/reopens,
and performs 50,000 deterministic untimed warmup reads. Each fixed phase executes
**2,000,000 aggregate operations**, including reader-count remainders: hits,
misses, and 10 misses/1 hit. Default 4 readers each reuse a snapshot for 64 owned
`Get` reads. `-quicksilver-read-batch=1` uses ordinary owned `DB.Get`; larger
values require `ReadSnapshotter` and fail clearly when unavailable. The current
LevelDB adapter is unsupported, including `leveldb_block_comp_on`,
`leveldb_block_comp_off`, and ordinary Get mode: its checkpoint closes/reopens the live DB handle while
Quicksilver readers are active. Selections containing LevelDB (including `all`)
fail before any engine opens or loads data; select supported engines explicitly
or exclude LevelDB. `Batcher` and `Checkpoint` are also required. Unsupported engines and unknown registry
names fail instead of disappearing from the selected set. LMDB remains optional:
`GOWORK=off go build -tags lmdb -o bin/unified-bench ./cmd/unified_bench`. Its
normal adapter provides owned snapshot reads and a force-sync checkpoint; the
suite pins write-batch creation through commit/close to one OS thread. LMDB
selections accept at most 126 read workers, its default reader-slot limit, for
both snapshot and ordinary Get modes. Wider selections fail before any engine
opens or loads data; the suite allows up to 1024 workers for other engines.

RocksDB is optional: install the native RocksDB headers/shared library, then
`GOWORK=off CGO_ENABLED=1 go build -tags 'lmdb rocksdb' -o bin/unified-bench ./cmd/unified_bench`
and select `-dbs treedb,lmdb,rocksdb`. The `rocksdb` adapter uses the native C API,
64 MiB write buffer, 64 MiB LRU block cache, 10 bits/key whole-key Bloom filters,
Snappy compression, checksum verification and synchronous WAL writes (including
ordinary `Set`/batch `Commit`). Its checkpoint waits for a native flush while
preserving the live DB handle and read snapshots. Close snapshots before their
DB owner. `Name`/stats report the build-time header version; use a matching shared
library and record its package version in benchmark provenance. Go allocation
metrics exclude RocksDB's native allocations; reported cache/memtable properties
are current engine observations, not peak RSS or a total native-memory budget.

Concurrent mixed readers run for `-quicksilver-duration` (default 4s). A paced
writer updates `-quicksilver-updates` distinct keys (default min(40,000,keys)),
in 1000-value batches, checkpointing at four quarter intervals (up to four when
smoke runs have fewer batches). The reference's 7919 permutation stride is kept
when coprime to the key count, otherwise the next coprime stride is reported.
Every measured read validates absence or identity/length/generation. Final
checkpoint/close/reopen is followed by byte-for-byte verification of **every**
value and **every** adjacent absent key. Errors cancel and join all readers and
the writer before owner close. Failed suite DBs are retained with their path in
the error; successful DBs are removed unless `-keep` is supplied.

The suite prints its effective settings banner on stderr and a JSON array on
stdout. With `-profile-dir`, benchprof prints artifact notices on stderr and
also writes detailed `quicksilver_results.json` and canonical
`benchprof_results.json/md`, then requires benchprof `insights.json/md/html`, including a point-read throughput
table. The unsafe TreeDB adapter uses the distinct `TreeDB (bench_unsafe)`
label in engine columns, results, stats, and checkpoint maps, so selecting it
alongside `treedb` preserves both variants.
The JSON records actual fixture/workers/GOMAXPROCS/stride, phase counts, latency
quantiles/max, load/checkpoint/reopen/update timings, full proof counts, engine
stats before/after phases, actual relative-file logical sizes, and registered
CLI flag values. Phase after-stats include the joined writer, while process
MemStats metrics cover the reader interval. Registered flags include values
unused by this suite; its `config` is authoritative. File lengths are not allocated
filesystem blocks.
Explicit generic workload/sweep flags that cannot apply are rejected; engine
options, profiles, `-max-wall`, `-max-rss-mb` and profiling controls remain usable.

CPU and allocation artifact phase names are `quicksilver_hits`,
`quicksilver_misses`, `quicksilver_mixed`, and `quicksilver_concurrent`.
`-cpuprofile-tests` and `-allocsprofile-tests` select these exact names. Checkpoint
CPU artifacts use `quicksilver_initial` and `quicksilver_final`, selectable with
`-checkpoint-cpuprofile-tests`. The concurrent CPU capture includes writer and
checkpoint work only while it overlaps the readers: per-engine CPU captures
stop after reader join, before quantile sorting and writer drain. A slow final
writer batch or checkpoint can drain after that capture ends. Allocation delta
profiles finish after writer drain and quantile sorting, and also include phase
orchestration, stats and summary work. Update/checkpoint timings, composition
time and final reopen proof still cover the completed writer. Shared `block.pprof`,
`mutex.pprof`, and `trace.out` cover the **entire suite across all selected
engines**, including setup and verification; they are process observations.

`seconds` measures reader-group completion, and ops/sec uses that interval.
`composition_seconds` separately includes checked writer drain, stats capture,
and CPU profiler shutdown when enabled.
Latency includes owned Get and periodic snapshot close/acquire; max covers all
successful reads. Samples use the reference's xor-mixed 1/4 fixed-phase and 1/16
concurrent schedule, capped at 1,000,000 aggregate samples per phase. Quantiles
are empirical samples, not a production SLO. Once a reader fills its share of
the sample budget, its quantiles retain earlier samples; maximum still covers
the entire successful read window. Process MemStats deltas and
normalized B/op/allocs/op cover the reader interval, including engine background
activity and, in the concurrent phase, writer work overlapping the readers.
Writer tail drain after reader join is outside these process metrics. They
exclude fixture creation, quantile sorting and profiler stop; they are not
isolated engine allocations.
The reusable fixture holds 65,536 IDs and both present/absent keys (4,718,592 bytes)
plus one 8,000,000-byte sample backing buffer, partitioned across readers. No
whole payload corpus or per-read key allocation is retained. Small goroutine,
context and engine snapshot allocations are separate from those capacities.

Compared with the retained scratch `quicksilver_eval_test.go`, the default is
random4k and 64 reads/snapshot, fixed counts are explicit aggregate 2M instead of
per-worker `QS_OPS`, arbitrary key counts retain unique updates, all absent keys
are verified, and failures always stop/join. The PCG seeds, payload bytes,
key trace and 10:1 schedule match. Stock registered engine tuning and integrity
apply; private K=1, CRC-skipping, raw-engine, compaction, and filter hooks are
not copied. Scratch/private-experiment throughput is historical evidence.
Profiled and unprofiled runs must be labeled separately; retain three repeats
before summarizing numerical comparisons. The standalone TreeDB workflow in
[the canonical runbook](../../docs/benchmarks/treedb_quicksilver_workflow/README.md)
remains a separate workload and artifact schema.
