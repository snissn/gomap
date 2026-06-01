# Collection Benchmark Matrix

`collection-bench-matrix` is the single entrypoint for the production collection
benchmark matrix. It runs the selected TreeDB collection cells, optionally runs
the SQLite comparison cell, generates per-cell markdown/html reports, then
generates a matrix-level markdown/html report with throughput, disk usage, and
maintenance-compaction tables.

Typical run:

```sh
make collection-bench-matrix
OUT=/tmp/collection-matrix-$(date +%s)
./bin/collection-bench-matrix -out-dir "$OUT" -batch-size 16000 -benchtime 100000x
```

The matrix uses the current public TreeDB profile vocabulary. The default
`command_wal_relaxed` engine exercises the command-WAL collection path; use
`bench` only when measuring the explicit benchmark-only no-WAL ceiling.

Useful focused variants:

```sh
# TreeDB only, mainline storage plus index outer leaves in the value log.
./bin/collection-bench-matrix -out-dir "$OUT" -skip-sqlite

# A faster smoke run that still emits the full report structure.
./bin/collection-bench-matrix -out-dir "$OUT" -benchtime 1000x -count 1

# Exercise leaf-generation pack/GC in a short matrix by forcing small leaf_vlog
# generations.
./bin/collection-bench-matrix -out-dir "$OUT" -benchtime 10000x -leaf-segment-target-bytes 65536

# Exercise a specific grouped leaf-frame size during leaf-generation pack.
./bin/collection-bench-matrix -out-dir "$OUT" -benchtime 10000x -leafgen-pack-frame-k 16

# Disable TreeDB post-maintenance index vacuum when you specifically need to
# inspect pre-vacuum index.db growth.
./bin/collection-bench-matrix -out-dir "$OUT" -report-post-maintenance-index-vacuum=false

# SQLite only is not a first-class mode; use a narrow TreeDB format/storage set
# and keep SQLite enabled when validating baseline drift.
./bin/collection-bench-matrix -out-dir "$OUT" -formats json -storage-cells mainline
```

For a narrower insert-throughput profile aimed at value-log compression
allocation costs, use the committed shell harness:

```sh
RUN_DIR=/tmp/treedb_insert_compression_profile_$(date +%Y%m%d_%H%M%S) \
  scripts/treedb_insert_compression_profile.sh
```

It runs the short-lived `BenchmarkCollectionShapeInsertBatch` template-v1 insert
shape, captures CPU/allocation profiles for the default compression path, and
optionally compares against `TREEDB_VLOG_COMPRESSION=off` with `benchstat`.
Use `COUNT=1 BENCHTIME=1000x RUN_COMPRESSION_OFF=false` for a fast harness
smoke test.

Primary outputs:

- `README.md`: run metadata and cell inventory.
- `collections_matrix_summary.md`: polished matrix report.
- `collections_matrix_summary.html`: HTML rendering of the same report.
- `collections_user_story_summary.tsv`: user-facing throughput rows.
- `collections_disk_usage_summary.tsv`: disk usage rows.
- `collections_maintenance_summary.tsv`: TreeDB `value_vlog` rewrite/GC plus
  post-GC index vacuum, TreeDB `leaf_vlog` generation pack/GC plus post-GC
  index vacuum, and SQLite VACUUM rows.
- `<cell>/collections_report.md`: per-cell detailed benchmark report.
- `<cell>/go_test.json`: raw `go test -json` benchmark output.
