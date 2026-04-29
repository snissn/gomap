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

Useful focused variants:

```sh
# TreeDB only, mainline storage plus index outer leaves in the value log.
./bin/collection-bench-matrix -out-dir "$OUT" -skip-sqlite

# A faster smoke run that still emits the full report structure.
./bin/collection-bench-matrix -out-dir "$OUT" -benchtime 1000x -count 1

# SQLite only is not a first-class mode; use a narrow TreeDB format/storage set
# and keep SQLite enabled when validating baseline drift.
./bin/collection-bench-matrix -out-dir "$OUT" -formats json -storage-cells mainline
```

Primary outputs:

- `README.md`: run metadata and cell inventory.
- `collections_matrix_summary.md`: polished matrix report.
- `collections_matrix_summary.html`: HTML rendering of the same report.
- `collections_user_story_summary.tsv`: user-facing throughput rows.
- `collections_disk_usage_summary.tsv`: disk usage rows.
- `collections_maintenance_summary.tsv`: TreeDB value-log rewrite/GC and SQLite
  VACUUM rows.
- `<cell>/collections_report.md`: per-cell detailed benchmark report.
- `<cell>/go_test.json`: raw `go test -json` benchmark output.
