# Mongo Gateway Comparison Artifact

Status: historical non-YCSB gateway microbenchmark. This bundle is useful for
Mongo-gateway attribution, but it is not a substitute for the external
MongoDB / TreeDB / TreeDB Mongo YCSB matrix. Use
`../ycsb_mongodb_treedb_current.md` for the current YCSB report index.

This directory is the first checked-in TreeDB Mongo gateway vs MongoDB
comparison bundle.

Run command:

```sh
OUT_DIR=/tmp/gomap_mongo_gateway_compare_20260429 \
DOCS_LIST="1000 10000" \
INDEXES_LIST="0 1 2" \
READS=1000 \
RANGE_READS=100 \
UPDATES=100 \
TIMEOUT=20m \
scripts/mongo_gateway_compare.sh
```

The run used Docker mode with `mongo:7`. The harness started a fresh MongoDB
container and isolated data directory for every matrix cell, stopped the
container after the run, and measured MongoDB physical disk usage with `du`
inside a temporary Docker container so host file permissions did not undercount
WiredTiger files.

Workload settings:

- document counts: `1000`, `10000`.
- secondary indexes: `0`, `1`, `2`.
- batch size: `500`.
- point reads per cell: `1000`.
- range reads per cell: `100`.
- updates per cell: `100`.
- deletes per cell: `0`.

Artifact files:

- `report.md`: reviewable Markdown comparison and highlights.
- `summary.tsv`: per-phase machine-readable comparison rows.
- `matrix.tsv`: report input index with target, document count, index count,
  raw JSON path, and physical bytes.
- `raw/*.json`: unmodified `cmd/mongo_gateway_bench -format json` outputs.

Regenerate the report from the committed raw inputs:

```sh
GOWORK=off go run ./cmd/mongo_gateway_compare_report \
  -matrix docs/benchmarks/mongo_gateway_compare_2026-04-29/matrix.tsv \
  -report docs/benchmarks/mongo_gateway_compare_2026-04-29/report.md \
  -summary docs/benchmarks/mongo_gateway_compare_2026-04-29/summary.tsv \
  -title "Mongo Gateway Benchmark Comparison - 2026-04-29"
```
