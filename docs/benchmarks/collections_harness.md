# Collections Benchmark Harness

`scripts/bench_collections_harness.sh` is the single entrypoint for collection
shape benchmarking and periodic sanity comparisons.

## Quick Run

```bash
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --count 3 \
  --benchtime 1s \
  --batch-size 8000
```

The default run captures:

- TreeDB collection benchmark cells using `production_wal_on_fast` unless
  `TREEDB_COLLECTION_BENCH_ENGINE` or `--engine` overrides it.
- JSON collection shape benchmarks.
- `template-v1` collection shape benchmarks.
- primary read shapes for both `Get` and reusable-buffer `GetInto`.
- JSON and `template-v1` two-index insert probes with index outer leaves stored
  in the value log.
- indexed insert phase metrics such as prepare, index-state extraction,
  preflight, run build, secondary-run shape, and publish time.
- per-cell `collections_report.{json,md,html}`.
- per-cell `collections_cpu.pprof`, `collections_mem.pprof`, and pprof top text.
- top-level `collections_matrix_summary.{tsv,md,html}`.
- top-level `collections_user_story_summary.tsv`.
- top-level `collections_disk_usage_summary.tsv`.
- top-level `collections_maintenance_summary.tsv` (always generated; may be
  header-only when maintenance metrics are not enabled or not present).

## Full Matrix Replication

Use this command when you need the full TreeDB-vs-SQLite collection matrix,
including the index-leaf-in-value-log probes, maintenance compaction rows, and
raw `unified_bench` TreeDB anchors:

Because this command enables `--include-sqlite`, ensure CGO is enabled and a
working C compiler/toolchain is available on the host.

```bash
set -o pipefail
OUT="/tmp/gomap_collections_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
TREEDB_COLLECTION_HARNESS_REPORT_VLOG_REWRITE=true \
TREEDB_COLLECTION_HARNESS_REPORT_SQLITE_VACUUM=true \
scripts/bench_collections_harness.sh \
  --out "$OUT" \
  --count 3 \
  --benchtime 1s \
  --batch-size 8000 \
  --include-sqlite \
  --include-unified \
  --unified-keys 100000 \
  2>&1 | tee "$OUT/harness_stdout.log"
```

The `set -o pipefail` line preserves the harness exit status through `tee`, so
missing SQLite prerequisites or other failures do not look like successful full
matrix runs.

The harness writes `$OUT/README.md` with the branch, commit, command settings,
and artifact paths. Keep that file with any benchmark notes so later reviewers
can separate code changes from host-to-host benchmark drift.

The main files to read after the run are:

- `$OUT/collections_user_story_summary.tsv` for ingest docs/sec, batch latency,
  and checkpoint split timing.
- `$OUT/collections_disk_usage_summary.tsv` for normalized `disk_bytes/doc` and
  derived collection/index byte splits.
- `$OUT/collections_maintenance_summary.tsv` for TreeDB value-log rewrite/GC
  and SQLite `VACUUM` before/after totals.
- `$OUT/collections_matrix_summary.md` for a reviewable Markdown summary with
  links to every per-cell report.
- `$OUT/harness_unified_index.tsv` plus `$OUT/unified_*/insights.md` for raw
  TreeDB engine anchors.

When writing conclusions, compare normalized rows, not raw total bytes. The Go
benchmark runner may store different document counts for different engines or
index counts. For two-index document insert comparisons, use:

- TreeDB default layout:
  `BenchmarkCollectionShapeInsertBatch/indexes_2` from
  `collections_json_shapes` and `collections_template_v1_shapes`.
- TreeDB index outer leaves in the value log:
  `collections_json_index_vlog_insert2` and
  `collections_template_v1_index_vlog_insert2`.
- SQLite parity rows:
  `BenchmarkSQLiteShapeInsertBatchJSON/indexes_2`,
  `BenchmarkSQLiteShapeInsertBatchNativeColumns/indexes_2`, and the
  `BenchmarkSQLite*WithSecondaryIndexes` rows.

For optimization planning, start with the phase columns in each
`collections_report.md`: `publish_ns/doc`, `secondary_runs_ns/doc`,
`index_state_extract_ns/doc`, `prepare_ns/doc`, `insert_ns/doc`, and
`sync_ns/doc`. The usual priority order is:

1. Reduce publish/root-group cost when `publish_ns/doc` dominates indexed
   inserts.
2. Reduce secondary-run and index-state construction cost when adding indexes
   scales poorly.
3. Reduce read-path allocation when primary reads or secondary lookups show high
   `B/op` and `allocs/op`.
4. Treat value-log rewrite/GC reclaim results as meaningful only on workloads
   with deletes, updates, or churn. Insert-only matrices should not be expected
   to reclaim space.
5. Judge index-leaf-in-value-log changes as a disk/throughput tradeoff: compare
   both `disk_bytes/doc` and docs/sec against the default layout.

## Optional Baselines

Add SQLite parity rows:

```bash
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --include-sqlite
```

SQLite runs require CGO and a C compiler. The SQLite cell includes JSON
generated-column and native-column variants for insert, checkpoint, read, and
secondary lookup shapes. Insert rows can report full `VACUUM` disk size
before/after metrics.

Add raw TreeDB `unified_bench` anchors:

```bash
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --include-unified \
  --unified-keys 100000
```

The unified anchors run TreeDB `fast` and `wal_on_fast` with `-profile-dir`, so
each anchor emits `benchprof_results.{json,md}` plus `insights.{json,md,html}`
and per-test pprof artifacts.

Add maintenance compaction metrics:

```bash
TREEDB_COLLECTION_HARNESS_REPORT_VLOG_REWRITE=true \
TREEDB_COLLECTION_HARNESS_REPORT_SQLITE_VACUUM=true \
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --include-sqlite
```

TreeDB maintenance rows in this harness are low-level diagnostics for specific
maintenance primitives (`value_vlog` rewrite/GC and `leaf_vlog` generation
pack/GC). Use the high-level `CompactStorage` path, for example
`treemap compact <db-dir> -rw`, for final storage-footprint measurements.
SQLite maintenance rows run `VACUUM` and a WAL checkpoint. These metrics are
opt-in because they add substantial untimed I/O and can perturb cache state
between cells.

## Focused Profiles

```bash
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --profile-benches \
  --timed-profiles
```

`--profile-benches` captures whole-process Go test CPU/allocation profiles for
focused benchmark rows. `--timed-profiles` captures timed-window collection CPU
profiles for the existing fixed-document indexed insert/checkpoint benchmarks.

## Useful Overrides

- `TREEDB_COLLECTION_HARNESS_JSON_REGEX`
- `TREEDB_COLLECTION_HARNESS_TEMPLATE_REGEX`
- `TREEDB_COLLECTION_HARNESS_INDEX_VLOG_REGEX`
- `TREEDB_COLLECTION_HARNESS_SQLITE_REGEX`
- `TREEDB_COLLECTION_HARNESS_REPORT_VLOG_REWRITE`
- `TREEDB_COLLECTION_HARNESS_REPORT_SQLITE_VACUUM`
- `TREEDB_COLLECTION_HARNESS_UNIFIED_TESTS`
- `TREEDB_COLLECTION_HARNESS_PROFILE_BENCH_LIST`
- `TREEDB_COLLECTION_HARNESS_TIMED_PROFILE_DOCS`

The default collection benchmark engine is `production_wal_on_fast` (WAL on with
the fast/relaxed sync profile). The default collection storage policy remains
production-oriented: `data_outer=true,index_outer=false`.

Collection insert benchmark rows, including the collection-shape inserts and the
fixed indexed insert/checkpoint variants used for timed profiling, report
end-of-run disk bytes after an untimed flush/checkpoint. The matrix summary
includes a disk-usage section that compares total bytes, collection bytes, and
index bytes; when an engine does not expose a direct object split, index bytes
are derived from the per-doc delta against the matching zero-index row.

The maintenance summary reports untimed TreeDB total disk bytes around the
selected low-level maintenance primitive. SQLite rows report total disk bytes
before and after `VACUUM`. Latency columns in both the per-cell reports and
matrix summaries include adjacent throughput columns such as `ns/op` plus
`ops/sec`.
