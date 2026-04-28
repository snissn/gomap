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

- JSON collection shape benchmarks.
- `template-v1` collection shape benchmarks.
- per-cell `collections_report.{json,md,html}`.
- per-cell `collections_cpu.pprof`, `collections_mem.pprof`, and pprof top text.
- top-level `collections_matrix_summary.{tsv,md,html}`.
- top-level `collections_user_story_summary.tsv`.
- top-level `collections_disk_usage_summary.tsv`.

## Optional Baselines

Add SQLite parity rows:

```bash
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_harness \
  --include-sqlite
```

SQLite runs require CGO and a C compiler. The SQLite cell includes JSON
generated-column and native-column variants for insert, checkpoint, read, and
secondary lookup shapes.

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
- `TREEDB_COLLECTION_HARNESS_SQLITE_REGEX`
- `TREEDB_COLLECTION_HARNESS_UNIFIED_TESTS`
- `TREEDB_COLLECTION_HARNESS_PROFILE_BENCH_LIST`
- `TREEDB_COLLECTION_HARNESS_TIMED_PROFILE_DOCS`

The default collection storage policy remains production-oriented:
`data_outer=true,index_outer=false`.

Insert-shape benchmark rows report end-of-run disk bytes after an untimed
flush/checkpoint. The matrix summary includes a disk-usage section that compares
total bytes, collection bytes, and index bytes; when an engine does not expose a
direct object split, index bytes are derived from the per-doc delta against the
matching zero-index row.
