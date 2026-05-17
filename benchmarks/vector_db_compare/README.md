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

The default backend set is `treedb,vectorlite`. Add `pgvector` or `mongodb` to
`BACKENDS` when those external services are available.

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
  runner starts a temporary `pgvector/pgvector:pg16` Docker container. The
  harness writes into a fresh benchmark schema (`PGVECTOR_SCHEMA`) instead of
  dropping tables in the caller's default schema. Set
  `PGVECTOR_DROP_SCHEMA_AFTER=true` when using an external DSN and you want the
  benchmark schema dropped after the run. Insert timing includes client-side
  vector text serialization for COPY; the JSON result reports that preparation
  time separately under the insert phase.
- `mongodb`: runs MongoDB Vector Search only when `MONGODB_VECTOR_URI` points
  at a deployment that supports `$vectorSearch` and `createSearchIndexes`. The
  runner installs PyMongo with SRV support for Atlas `mongodb+srv://` URIs and
  uses a fresh benchmark database name by default.

Configuration:

- `RUN_DIR`: output directory. Defaults to a timestamped directory under `/tmp`.
- `BACKENDS`: comma-separated backend list. Defaults to `treedb,vectorlite`.
- `DOCS`, `DIMS`, `QUERIES`, `VALIDATE_QUERIES`, `TOP_K`: dataset and validation sizes.
- `SEARCH_CONCURRENCY`: comma-separated search concurrency levels.
- `M`, `EF_CONSTRUCTION`, `EF_SEARCH`, `MIN_RECALL`: HNSW and recall parameters.
- `PGVECTOR_DSN`: external PostgreSQL DSN. If empty and `pgvector` is enabled,
  the runner starts Docker unless `PGVECTOR_DOCKER=false`.
- `PGVECTOR_DOCKER`, `PGVECTOR_IMAGE`, `PGVECTOR_MAX_CONNECTIONS`: automatic
  pgvector container controls.
- `PGVECTOR_SCHEMA`, `PGVECTOR_TABLE`: benchmark schema/table names.
- `PGVECTOR_ALLOW_DROP_SCHEMA`: pass `--allow-drop-schema` to permit replacing
  an existing disposable schema.
- `PGVECTOR_DROP_SCHEMA_AFTER`: drop the schema after the run, but only if this
  invocation created it.
- `MONGODB_VECTOR_URI`: Atlas or local Atlas connection URI.
- `MONGODB_VECTOR_DATABASE`, `MONGODB_VECTOR_COLLECTION`,
  `MONGODB_VECTOR_INDEX`: MongoDB benchmark namespace and index names.
- `MONGODB_VECTOR_NUM_CANDIDATES`: `$vectorSearch` candidate count.
- `MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS`: Vector Search index readiness timeout.

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
