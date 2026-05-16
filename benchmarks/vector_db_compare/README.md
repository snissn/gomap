# Vector DB Comparison

This benchmark compares persistent database-tier ANN search:

- TreeDB native persisted HNSW via `cmd/treedb_vector_search_demo`
- SQLite with the Vectorlite loadable extension, backed by hnswlib/HNSW
- PostgreSQL with pgvector HNSW
- MongoDB Vector Search HNSW when pointed at Atlas or a local Atlas deployment

`sqlite-vec` is intentionally not used for the ANN comparison. Upstream
sqlite-vec `vec0` search is brute-force today, and ANN support is tracked as
future work. Vectorlite is the SQLite-native comparator used here because it
has a persisted HNSW index path.

MongoDB requires MongoDB Vector Search, which runs through Atlas Search
infrastructure (`mongot`). A plain `mongod` or `mongo:7` container is not an
ANN vector-search comparator for this harness.

Run:

```sh
scripts/bench_vector_db_compare.sh
```

Useful overrides:

```sh
RUN_DIR=/tmp/vector_db_compare \
BACKENDS=treedb,vectorlite,pgvector \
DOCS=10000 \
DIMS=64 \
QUERIES=10000 \
VALIDATE_QUERIES=64 \
SEARCH_CONCURRENCY=2,4,8,16,32,64,128 \
scripts/bench_vector_db_compare.sh
```

Backends:

- `treedb`: runs the TreeDB native persisted vector index benchmark.
- `vectorlite`: runs SQLite+Vectorlite with its persisted HNSW sidecar file.
- `pgvector`: runs PostgreSQL+pgvector. If `PGVECTOR_DSN` is not set, the
  runner starts a temporary `pgvector/pgvector:pg16` Docker container.
- `mongodb`: runs MongoDB Vector Search only when `MONGODB_VECTOR_URI` points
  at a deployment that supports `$vectorSearch` and `createSearchIndexes`.

The runner writes:

- `dataset/`: TreeDB-owned synthetic vectors and manifest
- `treedb.json`: TreeDB benchmark result
- `vectorlite.json`: SQLite+Vectorlite benchmark result
- `pgvector.json`: PostgreSQL+pgvector benchmark result when enabled
- `mongodb.json`: MongoDB Vector Search benchmark result when enabled
- `comparison.md`: normalized comparison table

The TreeDB dataset exporter writes row-major little-endian `float32` vector
files plus JSONL convenience files. TreeDB loads documents from the exported
JSONL and query vectors from the exported binary query file; the Python
benchmarks consume the exported binary vector files directly.

Example scale probe:

```sh
RUN_DIR=/tmp/vector_db_compare_100k \
BACKENDS=treedb,vectorlite,pgvector \
DOCS=100000 \
QUERIES=50000 \
SEARCH_CONCURRENCY=2,4,8,16,32,64,128 \
scripts/bench_vector_db_compare.sh
```
