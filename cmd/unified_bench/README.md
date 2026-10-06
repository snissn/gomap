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
  - `durable` (strict durability benchmark preset)
  - `fast` (TreeDB selects production `no_wal_fast`: verified reads and volatile ordinary ACKs; explicit `*Sync`, `Checkpoint`, and clean `Close` seal a durable root. Persistent value-log/outer-leaf assets remain protected. A crash may lose recent volatile writes, never a torn batch or a root with missing references. Independently buffered collection domains do not promise a global ordinary-ACK-order prefix; explicit database boundaries drain registered managers.)
  - `bench_unsafe` (explicit benchmark-only ceiling; TreeDB skips read checksums and has no production durability promise; `unsafe` is its legacy alias)
  - `wal_on_fast` (benchmark-runner relaxed-WAL preset; TreeDB maps this to `command_wal_relaxed` with verified read integrity and the same compression defaults)
  - These names are unified-bench presets shared across database adapters. Native no-sync flags have engine-specific guarantees and are not durability-equivalent. The public TreeDB server profile vocabulary is separate. Public TreeDB servers should use `command_wal_durable`, `command_wal_relaxed`, or `no_wal_fast`; benchmark-only `bench_unsafe` requires an explicit benchmark constructor boundary.
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
This example uses unified-bench's `fast` preset, selecting production
`no_wal_fast` for TreeDB. Use `bench_unsafe` only for an explicit unsafe ceiling:

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

### Live partition canonical search cost

`BenchmarkVectorIndexPartitionLiveCanonicalSearchV1` measures one pinned live
partition domain with 16 base and 16 delta nodes, TopK 4 and EfSearch 16, at
2 and 128 dimensions. Fixture and pin creation are outside timing;
`SearchDomainV1` query preparation, candidate scoring and result allocations
are included. Compare identical benchmark source and options on both revisions:

```sh
GOWORK=off GOMAXPROCS=2 GOFLAGS='-p=1 -mod=readonly' \
  go test ./TreeDB/collections -run '^$' \
    -bench '^BenchmarkVectorIndexPartitionLiveCanonicalSearchV1$' \
    -count=5 -benchtime=500ms -benchmem -timeout=75s
```

Retain raw Go benchmark output (`ns/op`, `ops/s`, `B/op`, `allocs/op`, and
`scorecalls/op`) with source and runner identities. For a separate CPU capture,
use `-count=1 -benchtime=3s -cpuprofile=cpu.pprof -o collections.test` and
inspect `go tool pprof -top collections.test cpu.pprof`. These standalone Go
profiles are not benchprof inputs or `unified-bench -profile-dir` artifacts.

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
The default `realistic` fixture loads 3M varied namespace/hostname/opaque keys
and a seeded mixture of variable-size structured/opaque values. It supports
primary/holdout mixtures, uniform/1%/20% working sets, independently selected
miss percentages, exact distinct-access accounting and mutation bursts.
See [the generic workload contract](QUICKSILVER.md) for flags, generation,
compressibility sampling, correctness, allocation and durability boundaries.
`random4k` (100k random 4 KiB values) and `structured256` (250k compressible
256-byte values) retain historical fixed keys, seed and trace. `-keys` bounds
all cases for smoke runs.

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

The workflow loads groups of 1000 keys, checkpoints, closes/reopens, and
performs 50,000 deterministic untimed warmup reads. `-quicksilver-commit=auto`
resolves to ordinary batch Commit for realistic and CommitSync for historical
cases; `ordinary|sync` explicitly choose the API. Native adapter durability
remains engine-specific. The realistic case also performs a full initial
reopen oracle before warmup. Each fixed phase executes
**2,000,000 aggregate operations**, including reader-count remainders: hits,
misses, and mixed requests (realistic defaults to 90% absent; historical cases
keep 10 misses/1 hit). Default 4 readers each reuse a snapshot for 64 owned
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
writer processes `-quicksilver-updates` distinct mutation targets (default
min(40,000,keys)) in groups of up to 1000, checkpointing at four quarter intervals (up to four when
smoke runs have fewer batches). The reference's 7919 permutation stride is kept
when coprime to the key count, otherwise the next coprime stride is reported.
Historical cases update all targets once; realistic rotates updates, deletes,
inserts and four separately committed overwrite generations. Actual operation
and commit counts appear in the detailed report. Every measured read validates
absence or identity/length/permitted generation. Final
checkpoint/close/reopen is followed by byte-for-byte verification of **every**
surviving/inserted value and **every** miss domain. Errors cancel and join all readers and
the writer before owner close. Failed suite DBs are retained with their path in
the error; successful DBs are removed unless `-keep` is supplied.

Optional `-quicksilver-churn-rounds 2` adds bounded write/automatic-maintenance
characterization after the ordinary measured phases and final reopen proof.
It reuses one cached public TreeDB owner across rounds. The default
`-quicksilver-churn-shape full-refresh` restores the whole original population
and repeats deleted-key preparation, retaining the existing stress workload.
`-quicksilver-churn-shape sparse` restores only original update/delete/overwrite
targets before the same mutation sequence; it excludes insert targets and leaves
setup deletes absent. Inserts repeat existing disjoint identities in both shapes.
Each round checkpoints, pauses (default `-quicksilver-churn-pause 6s`), and runs
the full oracle. No automatic maintenance is forced.
Only realistic cached public TreeDB supports it; rounds are bounded to 1..32
and pauses to positive durations up to 1m, checked before loading. The additive
`maintenance_churn` result separates write/checkpoint/pause/proof wall time,
engine stats, Go heap, supported RSS and exact final files after clean close.
It labels the shape and repeated insert semantics, counts restored keys/commits,
and timestamps existing memory snapshots with `captured_at_unix_nano`.
Each round also records `pause_started_unix_nano` and `pause_finished_unix_nano`
at the actual pause boundaries; `pause_seconds` retains the monotonic elapsed
duration. The collector rejects unordered/out-of-round boundaries or a wall
versus monotonic duration difference above 1 millisecond. These stamps identify
the quiet interval for co-timed RSS observations; apply the frozen boundary
exclusion and require actual interior samples before claiming quiet residency.
They add no maintenance work and do not change phase/profile names.
Ordinary `final_files` remains the census before the ordinary final reopen;
timed reads, report fields and phase artifact names keep their existing meaning.
Whole-process HWM, block/mutex and trace include enabled rounds. This is a
short characterization, not a steady-state bound or a performance win.
See [capture configuration and method limits](../../docs/benchmarks/treedb_quicksilver_workflow/README.md#unified-generic-maintenance-characterization).

After closing the workload process and running offline maintenance, reuse the
same full value/miss oracle with `-quicksilver-verify-dir <retained-data-dir>`.
Select exactly one `-dbs` engine and supply the original case, mixture, seed,
key count and update count (plus the same TreeDB format/profile settings).
This mode opens an existing nonempty directory, verifies the final state and
closes it; it does not load, mutate or checkpoint a new workload. Normal engine
open/recovery and close behavior still apply, so this is not a read-only open.
Supported engines are TreeDB, LMDB and RocksDB; a nonempty engine database
marker is required before the factory opens. Profiling outputs are rejected.
Stdout is a single JSON object marked
`verification_only`, with the effective fixture and verified counts. A missing
or empty directory, oracle mismatch or close failure makes the command fail.
The ordinary suite and its profile artifact names remain unchanged.
The native capture script also accepts an optional boolean `keep` per cell
(default false), validating that the retained database is under its recorded
working database directory.

Use `-quicksilver-measure-dir <retained-data-dir>` to measure the operator-selected
final fixture after maintenance without loading or restoring it. Supply one
supported engine, the original realistic fixture flags, and the same format,
profile, compression and CRC settings. A nonempty marker is required before
open; full expected value bytes/misses and a live-key census must pass before
any workload writes. The same oracle runs after the final checkpoint/reopen.
The JSON labels this retained state and the **idempotent repeated writer**:
insert identities already exist and are SET again, so its insert count describes
scheduled mutations rather than newly inserted data. Deleted original hit targets
advance to surviving originals; the deleted miss class includes both setup and
mutation deletes. Actual hit/miss and distinct counts reflect those identities.
The existing owned `Get`, snapshot batching, paced writer and ACK behavior apply.
The supplied directory survives success, cancellation and failure even without
`-keep`. Churn and verification-only modes cannot be combined with measurement.
An initial checkpoint/load/reopen is absent and reports zero; canonical read and
final checkpoint profile filenames remain unchanged. Explicitly requesting an
initial checkpoint profile in this mode fails. Use separate profile output paths.

The native capture plan accepts `churn_shape` with enabled churn and an absolute
`measure_dir` for retained measurement. Each retained directory may appear once
per plan (one repeat); prepare independent copies for separate cells. Validation
binds its canonical path, registered flags, effective fixture, oracle counts and
zero setup timings, and requires hits to match present requests in all retained
phases. It expects no initial checkpoint profile for this mode.

Optional per-cell `rss_sample_interval_ms` (integer 1..60000) records co-timed Linux
`/proc` RSS, anonymous, file and shared-memory bytes in `rss_samples.jsonl`, with
absolute start/end timestamps and a sampling summary. Keep the same interval for
comparisons. The small `scripts/owned_process_rss.py` helper also accepts a
caller-validated native `treemap` command directly; callers retain responsibility
for source/binary attestation. It samples only its owned binary or the direct
`/usr/bin/time -v` child matching that binary's inode/device, and reaps the owned
launch on cancellation/failure. No unrelated process is sampled. Zero samples or
observation errors fail capture validation and retain artifacts. Sample maxima
are not process HWM: raw `/usr/bin/time -v` stderr remains separate. Observation
work belongs to capture overhead, not a product improvement claim.

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
Historical fixtures hold 65,536 IDs and both present/absent keys (4,718,592
bytes); realistic uses full-run PCG streams and 128-byte key scratch per reader.
All cases hold one 8,000,000-byte sample buffer and setup-allocated exact distinct
bitmaps, whose capacity is reported. Realistic also holds one byte/key mutation
state. No
whole payload corpus or per-read key allocation is retained. Small goroutine,
context and engine snapshot allocations are separate from those capacities.

Compared with the retained scratch `quicksilver_eval_test.go`, the historical
`-quicksilver-case random4k` uses 64 reads/snapshot, fixed counts are explicit
aggregate 2M instead of
per-worker `QS_OPS`, arbitrary key counts retain unique updates, all absent keys
are verified, and failures always stop/join. The PCG seeds, payload bytes,
key trace and 10:1 schedule match. Stock registered engine tuning and integrity
apply; private K=1, CRC-skipping, raw-engine, compaction, and filter hooks are
not copied. Scratch/private-experiment throughput is historical evidence.
Profiled and unprofiled runs must be labeled separately; retain three repeats
before summarizing numerical comparisons. The standalone TreeDB workflow in
[the canonical runbook](../../docs/benchmarks/treedb_quicksilver_workflow/README.md)
remains a separate workload and artifact schema.

### Owned cached snapshot microprofile

The standalone `BenchmarkSnapshotPublishedOwnedRead` in `TreeDB/caching`
compares caller-owned `Get` and reusable-destination `GetAppend` on 4096 published
256-byte values. Setup, checkpoint and warmup are outside the benchmark timers;
a queued unrelated write retains the cached snapshot view. Generic workload and
concurrent-writer qualification remain the Quicksilver suite's responsibility.

Capture it through the existing fresh-process point-lookup workflow:

```sh
RUN_DIR=/tmp/treedb_point_lookup_profiles scripts/treedb_point_lookup_profile.sh
# Bounded artifact smoke check, not throughput evidence:
BENCHTIME=1x RUN_DIR=/tmp/treedb_point_lookup_smoke scripts/treedb_point_lookup_profile.sh
```

The workflow uses `GOWORK=off`, count 1 and `BENCHTIME=200ms` by default. Alongside
the node/tree families, `cached_owned.txt` contains Go benchmark stdout/stderr,
`cached_owned.test` is the symbolization binary, `cached_owned_cpu.pprof` and
`cached_owned_allocs.pprof` are Go test profiles, and matching `_cpu_top.txt` /
`_allocs_top.txt` files contain pprof summaries. The shared `source-head.txt`,
`source-status.txt`, `source-diff.patch`, `go-version.txt` and `settings.txt`
retain source/toolchain provenance. Each benchmark family runs in a fresh Go
test process. Profiles include fixture setup and the whole test process; keep
profiled diagnostics separate from unprofiled benchmark timing. These profiles
are **not benchprof inputs** or unified-bench `-profile-dir` artifacts. Inspect
them with `go tool pprof`; see the [analyzer documentation](../benchprof/README.md#leaf-point-lookup-microprofiles).

To profile only this family in one fresh process, the corresponding Go command
from the repository root is:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkSnapshotPublishedOwnedRead$' \
  -benchmem -benchtime=200ms -count=1 -timeout=2m \
  -cpuprofile=/tmp/cached_owned_cpu.pprof -memprofile=/tmp/cached_owned_allocs.pprof \
  -o=/tmp/cached_owned.test
go tool pprof -top /tmp/cached_owned.test /tmp/cached_owned_cpu.pprof
go tool pprof -top -alloc_space /tmp/cached_owned.test /tmp/cached_owned_allocs.pprof
```

### Native fixed-quantum MVCC package harness

`BenchmarkNativePruneFixedQ` in `TreeDB/mvcc/native_prune_bench_test.go` is an
optional package benchmark (`mvcc_native_prune,treedb_test` tags), separate from
unified-bench. Untagged builds do not compile it. Explicit tagged builds on a
runtime without the bounded pruning API fail compilation; there is no fallback.

The stdlib-only `scripts/mvcc_native_prune.py` freezes expected source identities,
captures one fresh process per subcase, and validates `packet.json` schema 1.
Use separate, immutable runtime and harness checkouts; output must be outside
both. Full capture requires clean committed sources and Go 1.26.4 with CGO.
Review the expected identity file before capture. Expensive capture requires
landed reviewed tooling and a separately qualified exact runtime.

```sh
export GOWORK=off GOTOOLCHAIN=local CGO_ENABLED=1 GOFLAGS=-p=2
unset GOROOT
python3 scripts/mvcc_native_prune.py self-test
python3 scripts/mvcc_native_prune.py freeze --runtime "$RUNTIME" \
  --harness "$HARNESS" --go "$GO" --scope full --out "$EXPECTED"
python3 scripts/mvcc_native_prune.py capture --runtime "$RUNTIME" \
  --harness "$HARNESS" --go "$GO" --expected "$EXPECTED" --out "$OUT"
python3 scripts/mvcc_native_prune.py validate "$OUT/packet.json" --expected "$EXPECTED"
```

The capture uses a Go source overlay to compile the harness against the exact
runtime without editing it. `--scope smoke` freezes only an 8-version ascending
case. An extracted frozen runtime requires `--runtime-commit BASE_COMMIT
--archive SOURCE_TAR_GZ` on freeze and capture; these record the declared snapshot
base and archive SHA256, with a null actual runtime commit. Smoke validates
instrumentation only, never the original matrix or runtime qualification.

Schema 1 stores before/after runtime source SHA256 maps, exact actual runtime and
harness commits (null for uncommitted snapshot smoke), harness file hashes,
archive identity where applicable, Go version/environment, fixed options, build
status, and one result per case. The full serial matrix preserves original
shuffled (`(i*2053)%history+1`) and ascending 512/1024 inline histories under
`command_wal_durable`, plus pointer-backed queued pairwise producers with 1024
obsolete versions and 512/4096 future versions under `command_wal_durable` and
`no_wal_fast`. All prune calls use CommitDurable, Q32/1MiB and BatchSize1.
The ordinary call cap is history*16, record cap history*512 and doubling cap 3x;
queued call caps are 24,576/81,920. No existing fixture or raw-path gate changes.

Each result records actual calls, Records/Bytes, Visited, SetupRecords,
CleanupRecords, Deletes/Batches, maximum per-Q records/bytes/time, setup, cold
open, complete-pass time, first-ACK Close/Open time, final cursor close and DB
Close time, Go allocation bytes/counts, completion and exact logical/physical
remaining-value oracles. Queued passes immediately Close/Open at the first real
ACK; that recovery interval is included in PassNS and separately recorded.
SetupNS includes cold open; allocation deltas cover the whole combined process
only during the timed pass, including background work and recovery. Final Close
is separate. Concurrency is not selected by this serial harness.

Missing/duplicate cases, wrong fixtures/options/toolchain, source drift,
unclassified errors, missing oracles/completion, invalid counters and original
caps fail validation. Failed logs/packets are retained, and existing output
paths are never replaced. Each case has its raw Go benchmark log and JSON row;
commands, build log and test binary are retained for inspection.

`--profiles` adds `case-N.cpu.pprof`, `case-N.allocs.pprof`,
`case-N.mutex.pprof` and `case-N.block.pprof`. These ordinary Go package profiles
cover the process including setup/oracles/Close, unlike the timed pass allocation
delta. Analyze them with `go tool pprof`, using `sample_index=alloc_space` or
`alloc_objects` for allocation profiles. They are **not benchprof inputs** and
have no `benchprof_results.json` contract; unified-bench/benchprof parsers are
unchanged.

The explicit mandatory runtime gaps are `storage_sync_count`,
`fence_wait_max_ns`, `fence_hold_max_ns`, `owned_retained_bytes` and
`owned_peak_bytes`. Schema 1 emits only `tooling_only`; `validate --qualify`
always refuses while these actual owner-boundary measurements are unavailable.
It does not accept zeros, outer-call latency, profiles or process heap as
substitutes. A valid tooling packet makes no performance or qualification claim.

### Native prune memory lifecycle harness

`scripts/native_prune_memory.py` runs `TestNativePruneMemoryLifecycle` with
`treedb_test,mvcc_native_memory` tags in a fresh process per case. The full
matrix pairs N/2N histories, prune/no-prune/cancel and pinned/unpinned readers.
It uses real Q32/1MiB public RELAXED pruning, persistent-pointer survivors and
physical deletion, old-reader, custody, cancellation and reopen checks.

```sh
python3 scripts/native_prune_memory.py --go "$GO" --n 512 --out "$OUT"
python3 scripts/native_prune_memory.py --out "$OUT" --validate
python3 scripts/native_prune_memory.py --out "$OUT" --self-test
```

Output must be a new directory. `--smoke --n 64 --race` runs four diagnostic
cases: pinned prune at N/2N plus pinned cancellation and no-prune control.
Linux `/proc` supplies RSS; validation binds source, resolved Go executable/
version, tagged binary, build command/environment, each case invocation, raw
logs and results. Inherited memory controls are cleared; CGO=1, GOWORK=off,
GOTOOLCHAIN=local and GOFLAGS=-p=2 are recorded. Go memory controls
`GOGC`, `GOMEMLIMIT` and `GODEBUG` are cleared. Timed-out cases retain partial
raw output, process errors and rejected receipts. The validator rejects missing actual
partial-output/retirement witnesses, inconsistent native counters, source
changes, checksum drift and incomplete or duplicate smoke/full matrices. Self-tests mutate copies of real successful
packets and never count as lifecycle measurements.

Forced-GC heap cuts and sampled RSS describe the whole process. Logical
retirement payload and a held-buffer size are partial ownership witnesses;
exclusive cursor/tree retained and peak bytes remain unmeasured. Allocation
scopes separate fixture, maintenance and later Close/reopen/oracle work;
they include observer and verification traffic. These packets are standalone
artifacts, have no benchprof input contract and do not qualify schema 1's
native runtime gate. Expensive collection requires reviewed landed tooling
and an externally recorded exact product/harness source freeze.

Memory v3 retains four fixed-size scalar peak witnesses for overall/source
retirement cells, preparation window and native frames, including the observing
call and custody phase. Every witness is checked against all reported memory
and descriptor bounds. InputCount can witness a window before a private build
exists; private counts require real build custody. Zero maxima claim no call or
owner. Missing witnesses remain historical and fail current validation.
Receipt and derived-summary scope labels must equal the canonical capture
labels, so editing both cannot change aggregate measurements into exclusive
owner claims. Self-tests refresh result checksums and derived summaries together
to exercise case-level refusals. Work-record/byte maxima are budget checks;
they are not attained-maximum memory witnesses. These fixed scalar observations
still contribute to aggregate memory and do not measure exclusive native heap.
Partial-output cuts require real allocated output and nonzero source retirement.
Memory integers must fit the producer's uint64, uint8 Phase or nonnegative
64-bit Go int, as appropriate; zero witnesses retain their exact empty shape.
Custody bounds and record-count scaling are evaluated separately from this
presence witness.
The sampled maintenance RSS peak is checked against the eligible named cuts
and one constant-size periodic maximum witness (RSS and observing call).
Periodic samples run every 128 maintenance calls; the recorded sample count
must match that schedule, and missing periodic Linux RSS fails closed.
The maximum includes terminal maintenance and custody cleanup cuts, even after
native ownership is released. It remains aggregate process RSS. Memory schema v3
retains `RSSPeriodicPeak.ProcessHWM` from the same `/proc/self/status` observation
as its peak RSS and checks only that paired RSS does not exceed paired HWM.
An empty witness has exactly zero Call/RSS/ProcessHWM. Linux RSS and VmHWM are
approximate independent snapshots: ordered HWMs need not be monotone, and a
later/final HWM is not an upper bound for earlier RSS. Historical v2 packets
keep their original schema, validator and source identity; missing paired
witnesses fail current validation. Pair consistency and hashes cannot
authenticate a coordinated rewrite of an entire packet.
Validation also recomputes `summary.json` from the validated case results and
receipt labels, refusing missing, malformed, changed or contradictory summaries.

Earlier v2 results without these periodic witness fields remain historical and
fail the current validator; missing samples are never reconstructed.

## Native prune foreground causal pilot

Foreground overlap counters retain approximate caller-boundary samples of public-call envelopes.
Entry markers publish before peer sampling; return sampling precedes marker clearing.
These sampled envelopes do not establish continuous public-call overlap.
Options, payload construction, first-writer notification and result/latency bookkeeping
sit outside marked calls; each flag clears immediately after the return-boundary
observation. ACK/completion attribution keeps its first post-PruneVersions activity
load. Sampling can miss overlap and has unavoidable scheduling uncertainty between
adjacent caller instructions; it does not prove simultaneous internal critical-section
execution. The parser requires the complete typed 76-field producer schema, complete
histogram objects, representable counters, operation/work/phase/stop accounting (after-stop ACKs require drain calls) and
feasible histogram bounds. Coupled hostile fixtures refresh result checksums and
reach the schema/accounting boundary without changing retained measurements.


Foreground receipts retain `capture_out`, the original absolute capture directory.
Validation checks archived files in the current `--out` directory against their
hashes and checks recorded build/run paths against `capture_out`, so moving a
completed packet preserves validation. Missing, relative or inconsistent capture
paths fail closed. Original source bindings remain required.

`scripts/native_prune_foreground.py` runs eight fresh-process N64/128 cases:
Q32/1MiB RELAXED prune with finite burst, growing output or fixed-cardinality
churn, plus zero-work burst references. Real reads continue after writes stop; each case records a completed read
started after the final write, including early prune completion.
The bounded start cut requires actual allocated partial private output. The
pilot retains physical, writer, old-reader, pointer and reopen checks.

```sh
python3 scripts/native_prune_foreground.py --go "$GO" --race --out "$OUT"
python3 scripts/native_prune_foreground.py --out "$OUT" --validate
python3 scripts/native_prune_foreground.py --out "$OUT" --self-test
```

Use a new output directory. Schema `gomap-native-foreground-v2` binds source,
resolved Go executable/version, binary, build command/environment and each
case's command, raw log and result. The driver forces continuing readers and
rejects control/forced-error runs, malformed counters, duplicate/missing cases,
nonzero exits, failures and checksum/source/binary drift. The forced budget
error test verifies worker join and real cleanup under race; it is a diagnostic,
not a matrix measurement. Build controls are CGO=1, GOWORK=off,
GOTOOLCHAIN=local and GOFLAGS=-p=2; inherited GOMAXPROCS is recorded.

Fixed latency buckets cover actual public-call intervals including lock wait;
ACK active/stop classification is sampled at return. These are causal pilot
observations, not retained tail-latency qualification. Zero-work references
fence foreground and have a different start cut. Optional tags need the M7
native interfaces; ordinary main builds exclude these tests. Neither pilot
packet is a benchprof input. Reviewed tooling landing and exact product source
freeze precede expensive qualification collection.

Foreground ACK and completion attribution samples writer activity immediately
at the public prune return, before counter bookkeeping. The reader and prune loop
start after the writer announces it has started; overlap is established by the
sampled call-envelope witnesses. Writer duration begins inside its goroutine.

## Current source-population audit microbenchmarks

These standalone Go benchmarks time admitted source-vector proof work and
matched pre-existing iterator, JSON/vector and six-outcome audit-plan helpers:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' -bench '^Benchmark(VectorSourcePopulationProofV1|PopulationShared.*GuardV1|BufferedRootRunsIteratorBuildManyRuns)$' -benchmem
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkPopulationLegacyColocatedPlanGuardV1$' -benchmem
```

The proof uses 512x128 and 10000x128 current rows and reports inspected entries,
asset bytes and source-entry/projection bytes per operation. The legacy audit
plan guard uses the frozen six-write/four-ID ledger and times validation or
decoding, excluding authenticated transport, quorum and current-FSM fences.
Retain raw Go benchmark output and compare identical helper bytes in balanced
fresh processes. These package microbenchmarks are diagnostic artifacts, not
benchprof inputs or evidence of service throughput, recall or cluster capacity;
the unified-bench profile format and parser contract are unchanged.

### Standalone immutable memtable safe-build witness

`BenchmarkCOWLargeKeyLookup` is a package benchmark, outside the unified-bench
adapters and benchprof profile pipeline. Capture its Get/CursorSeek/Estimate/
RefusedPrepare sub-benchmarks as plain Go test logs:

```sh
GOWORK=off go test -tags treedb_safe ./TreeDB/internal/memtable -run '^$' \
  -bench '^BenchmarkCOWLargeKeyLookup$' -benchmem -benchtime=100x -count=3
```

Repeat without the tag for the default comparator. Fixture setup is excluded;
1 MiB keys prevent short-input conversion elision from hiding allocation costs.
See [ownership and qualification scope](../../TreeDB/docs/spec/cow-memtable-ownership.md).

### Standalone COW public integration diagnostics (#5046)

These Go package benchmarks compare `append_only`, `btree` and `cow_btree` on
identical resolved durability profiles. They emit plain Go benchmark rows
(ns/op, B/op, allocs/op), latency/counter metrics and ownership receipts; they
are not benchprof inputs and do not change the unified-bench profile format.

```sh
GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkCOWPublicDirtyCost$' \
  -benchmem -benchtime=16x -count=1
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkCOWIntegratedPublicMVCC$' \
  -benchmem -benchtime=128x -count=1
```

The raw KV fixture uses Inline64/Pointer4096 and N=1024/2048, with capture,
owned read, forward16, incremental write, actual dirty checkpoint and explicit
write-sync cases. Setup/reseed and final close are outside timing; fixture-total
COW budget/cut counters and a concrete final-drain receipt remain separate from
per-operation allocation results. The tiny MVCC fixture retains Store fences,
uses actual CommitAt/GetAt and full exact-key history, and limits fixed counts
to128/256. Balanced fresh-process repetitions and all failures/spread must be
retained before comparing costs. These are bounded integration diagnostics;
sustained C4 qualification requires its separately landed retained harness.
See the [current repair and cost packet](../../TreeDB/docs/benchmarks/cow-c2-ci-repair-5046/report.md)
for exact collection policies, commands and limitations. The
[earlier integration packet](../../TreeDB/docs/benchmarks/cow-c2-integration-5046/report.md)
retains its historical source identity.

### Native prune experiment environment and writer stop boundary

The native memory and foreground drivers construct a minimal environment and
record the exact non-null map passed to version, build and case processes. They
fix `GOENV=off`, `CGO_ENABLED=1`, `GOFLAGS=-p=2`, `GOWORK=off` and
`GOTOOLCHAIN=local`, record `HOME`, and use the resolved Go directory followed by
the platform's default system tool path. Default caches remain under recorded
`HOME`; inherited experiment, compiler, loader and GC overrides are excluded.
An explicitly supplied `GOMAXPROCS` must be a canonical positive decimal and is
recorded; when absent, the runtime uses its host-dependent default. Cases add
only the recorded test controls. Unknown, null or inconsistent environment keys
fail closed; older receipts with the former subset environment need their original
validator and cannot be treated as evidence captured by the revised driver.

Every foreground mode ends public writer activity at its final `CommitAt`
return, including cap, duration and error stops, before latency/counter cleanup.
Duration expiry is observed at public returns, preserving the existing approximate
duration policy. The same return time binds latency and current H2
`WriterDurationNS`, the elapsed writer duration at the observed terminal return.
The pending H3 retained extension (#5043) additionally records
`retained.WriterStopNS`, measured from `measurementStart`. `writerDone` remains
the later conservative worker completion and post-writer read fence. These
sampled phase witnesses do not qualify performance or prove continuous overlap.

The dedicated [R1 collection capture](../collection_workload_bench/README.md#r1-complete-local-row-comparison)
uses `scripts/r1_collection_capture.sh` and the `gomap-r1-row-v1` packet. It reports
public full-row and prepared-view boundaries with matched SQLite durability;
these dedicated artifacts are not `-profile-dir` or benchprof inputs.

Native capture build provenance uses two separate maps. `source-bindings.json`
is the offline Go/module/policy/driver preflight map. The drivers also require
`gomap-in-repo-build-inputs-v2`: exact-tool/environment/tag/race `go list -deps
-test -json` commands, raw metadata, and the resolved in-repo input hashes before
and after build and after collection. This includes ordinary/test/external-test
Go sources, cgo/native/assembly/SWIG/system-object fields, and main/test/external-test
embedded assets. Local module replacements outside the source root refuse.

Validation rehashes archived compiled files before any metadata subprocess, then
checks the current resolved graph for additions, removals or rerouting. H3's
invalid expected-source admission still makes no output or subprocess calls;
compiled-graph failures after admission retain the normal failed receipt. These
maps bind Go-enumerated repository inputs, not external module/toolchain/system
files or arbitrary compiler includes outside Go's package metadata. Original
receipts with only the offline map remain unchanged and require their archived
validator; they do not prove this newer compiled-input boundary. Runtime result
schemas and earlier measured results retain their original source identities.

Compiled-input contract v2 separately hashes each query’s JSON stdout in
`<query>.stdout` / `stdout_sha256`, while preserving the full combined
stdout/stderr diagnostic log and its `raw_sha256`. Offline validation parses
only the bound stdout; successful dependency-download diagnostics do not become
JSON metadata. Archive self-tests retain every required query command, stdout,
raw log, and input inventory. Historical compiled-input v1 packets require their
archived validator and are not migrated or relabeled as v2 evidence. Runtime
RESULT schemas and previously recorded measurements are unchanged.

The standalone `BenchmarkR1Lifecycle5060` uses `scripts/r1_lifecycle_capture.sh`
with the `gomap-r1-lifecycle-packet-v2` format, raw calibration/final process logs
and strict source/count validation. For a small nonqualifying rehearsal, use
`--qualification rehearsal --out /tmp/r1-lifecycle --repetitions 2 --epochs 3
--documents 32 --calls-per-epoch 8`. See the [lifecycle capture contract](../../TreeDB/docs/spec/r1-row-lifecycle.md#source-bound-standalone-capture).
These artifacts are separate from unified-bench profiles and benchprof inputs.

Lifecycle v2 measures logical fold, conditionally eligible typed rewrite/GC and
live direct-backend online vacuum with before/after census; cached-wrapper
overhead is omitted. It pins Go 1.26.4 Linux amd64 and runtime settings
GOMAXPROCS=16, GOGC=100, GOMEMLIMIT=off, GOFLAGS empty.


### Opt-in retained foreground duration/fence capture

Issue #5036 extends this same standalone driver with
`--retained`; the default schema-v2 causal pilot and its eight cases remain
unchanged. Retained schema `gomap-native-foreground-retained-v1` requires
`treedb_test,mvcc_native_foreground,mvcc_native_prune`. The native observer
package and interfaces are currently an explicit unmerged M7 runtime
dependency: ordinary builds exclude the fixtures, the disabled bridge preserves
the v2 tag combination, and requesting retained mode without the observer tag
fails clearly. A candidate overlay smoke demonstrates tooling construction;
it does not qualify clean main or a candidate with outstanding product failures.

```sh
# Small construction smoke; deliberately insufficient for qualification.
python3 scripts/native_prune_foreground.py --go "$GO" --retained --smoke \
  --n 64 --repetitions 1 --out "$OUT"
python3 scripts/native_prune_foreground.py --out "$OUT" --validate
python3 scripts/native_prune_foreground.py --out "$OUT" --self-test

# Only after reviewed tooling/runtime land and the exact runtime manifest is
# independently accepted: fixed Q32/1MiB, N/2N, growth/churn, fresh process/run.
python3 scripts/native_prune_foreground.py --go "$GO" --retained --n 512 \
  --repetitions 3 --runtime-status integrated-reviewed \
  --expected-source "$EXPECTED_SOURCE_BINDINGS" --qualify --out "$QUAL_OUT"
```

The runtime-status declaration and expected source digest bind the reviewed
runtime supplied by the caller; they are not an independent review attestation.
`--validate` checks capture integrity; `--qualify` additionally applies the
predeclared coverage/noise policy. Race captures establish correctness only.
The native schema-1 qualification refusals remain unchanged; this separate
capture does not populate exclusive `owned_retained_bytes`/`owned_peak_bytes`
or certify the five native schema-1 gaps.

Five raw-duration buffers have fixed capacity 65,536 uint64 values each:
2,621,440 bytes of payload allocated before collection, plus fixed headers.
Only scalar timing data is retained, with no source/DB pointer registry.
Each worker owns its recorder. Overflow rejects capture integrity; no sampling
or silent truncation occurs. Recorder writes happen after the public-operation
end timestamp and affect scheduling; observer atomics/clock calls inside
instrumented operations are included in observed latency. The fixed buffers
remain resident during measurement and are part of the instrumented workload,
not an exclusive native-owner heap measurement.

Durations use Go monotonic `time.Now` intervals in nanoseconds. Read phases
are defined by whether `writerDone` has closed at operation start; quantum
phases sample the writer-active flag at entry; all writes belong to the active
phase. Cross-boundary operations retain their start classification. Setup
quanta and their total duration are excluded explicitly. Workers join before
the first cumulative per-site observer snapshot. Cleanup duration runs from
that join through cursor/old-reader release, correctness scans, checkpoint,
reopen/pointer proof and final DB close; the second snapshot is then taken.
The collector begins once after quiescent private-output setup and never
resets during activity. Snapshots retain cumulative physical attempt/failure
counts and all nine fence-site counts/maxima. Maxima are checked against the
per-site maximum, never summed or subtracted into phase maxima. Nested holds
may include waiting/I/O; physical attempts are not universal kernel syscall
counts, and dedicated Finish-lock coverage remains limited by existing hooks.

Per-run p95/p99 use nearest-rank `ceil(p*count)` over actual durations.
Qualification requires at least 1,000 samples in each of ReadActive,
ReadDrain and WriteActive in every run, at least three fresh repetitions per
N/2N growth/churn cell, nonrace captures, and max/min <=1.25 for each phase's
p95 and p99 across repetitions. Quantum distributions are descriptive and
still require exact phase/count/duration accounting. Missing drain coverage,
overflow, source/binary drift, missing sites/tags, false counts/maxima, failed
children/oracles, or excessive spread fail closed. A tiny smoke cannot relax
this policy. All failure artifacts are retained. The original immediate
writer-activity sample at public prune return, partial-output start, overlap,
finite writer/continuing-reader, survivor/pointer/old-reader and reopen
oracles remain in force. The unbounded v2 reference has unmatched starting
custody and stays descriptive. The driver verifies testing-owned temporary
DB disposal only after the child releases all consumers and exits. These
JSON/log packets are not benchprof inputs.

### Standalone external MVCC COW read admission fixture

`BenchmarkC3PublicReadAdmission` in `TreeDB/mvcc` measures bounded public
CommitAt+GetAt, CommitGroupAt+actual exact-key all-version iteration, and ordinary
concurrent Store calls across three profiles and inline/forced-pointer values.
Use a fresh process per leaf with fixed `-benchtime=128x` for smoke or `1024x` for
matched collection; retain Go benchmark stdout, command, exit status, source,
module/toolchain and binary identities. Setup/Close are excluded and history/output
are fixed. Reproduction and allocation ownership are documented in
[the fixture contract](../../TreeDB/docs/benchmarks/cow-c3-read-5076/README.md).
These Go package benchmark logs/profiles are not benchprof inputs.

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc -run '^$' \
  -bench '^BenchmarkC3PublicReadAdmission/command_wal_relaxed/cow_btree/inline/point$' \
  -benchtime=1024x -count=1 -benchmem
```
