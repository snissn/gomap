# Vector DB Comparison

This benchmark compares persistent database-tier ANN search:

- TreeDB native persisted HNSW via `cmd/treedb_vector_search_demo`
- TreeDB `column_graph` full-vector exact/default HNSW via the same demo
- TreeDB `column_graph` scalar_u8 `quantized_only` and `quantized_rerank`
  query modes via explicit demo flags
- SQLite with the Vectorlite loadable extension, backed by hnswlib/HNSW
- PostgreSQL with pgvector full-vector HNSW
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

The default backend set is `treedb,vectorlite`. Add `treedb_column_graph`, the
TreeDB quantized aliases, `pgvector`, or `mongodb` to `BACKENDS` when those
paths or external services are needed.

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

TreeDB exact vs quantized column-graph comparison:

```sh
RUN_DIR=/tmp/vector_db_compare_quantized \
BACKENDS=treedb_column_graph,treedb_column_graph_quantized_only,treedb_column_graph_quantized_rerank,pgvector \
DOCS=10000 \
DIMS=128 \
QUERIES=10000 \
VALIDATE_QUERIES=64 \
TOP_K=10 \
EF_SEARCH=128 \
TREEDB_QUANTIZED_INDEX_NAME=embedding.scalar_u8.fast \
TREEDB_QUANTIZED_RERANK_CANDIDATES=32 \
TREEDB_QUANTIZED_MIN_RECALL=0 \
scripts/bench_vector_db_compare.sh
```

The quantized comparison leaves TreeDB quantized recall as an observation by
setting `TREEDB_QUANTIZED_MIN_RECALL=0`; full-vector TreeDB/pgvector rows still
use `MIN_RECALL` (default `0.95`) as their recall gate.

Backends:

- `treedb`: runs the TreeDB native persisted vector index benchmark.
- `treedb_column_graph`: runs TreeDB `column_graph` exact/default full-vector
  graph search.
- `treedb_column_graph_quantized_only`: runs TreeDB `column_graph` with a named
  scalar_u8 score plane and `query_mode=quantized_only`.
- `treedb_column_graph_quantized_rerank`: runs TreeDB `column_graph` with a
  named scalar_u8 score plane and `query_mode=quantized_rerank`, exact-reranking
  `TREEDB_QUANTIZED_RERANK_CANDIDATES` candidates (or the demo's normalized
  `ef_search` set when set to `0`).
- `vectorlite`: runs SQLite+Vectorlite with its persisted HNSW sidecar file.
- `pgvector`: runs PostgreSQL+pgvector full-vector HNSW. If `PGVECTOR_DSN` is not set, the
  runner starts a temporary `pgvector/pgvector:pg16` Docker container. The
  harness does not use pgvector `halfvec`, `binary_quantize`, SQL rerank,
  custom byte-code scoring, or custom operator classes; pgvector remains the
  full-vector HNSW anchor. The
  harness writes into a fresh benchmark schema (`PGVECTOR_SCHEMA`) instead of
  dropping tables in the caller's default schema. Set
  `PGVECTOR_DROP_SCHEMA_AFTER=true` when using an external DSN and you want the
  benchmark schema dropped after the run. Insert timing includes client-side
  vector text serialization for COPY; the JSON result reports that preparation
  time separately under the insert phase.
- `mongodb`: runs MongoDB Vector Search and requires `MONGODB_VECTOR_URI` to
  point at a deployment that supports `$vectorSearch` and `createSearchIndexes`.
  The runner installs PyMongo with SRV support for Atlas `mongodb+srv://` URIs
  and uses a fresh benchmark database name by default.

Configuration:

- `RUN_DIR`: output directory. Defaults to a timestamped directory under `/tmp`.
- `BACKENDS`: comma-separated backend list. Defaults to `treedb,vectorlite`.
- `DOCS`, `DIMS`, `QUERIES`, `VALIDATE_QUERIES`, `TOP_K`: dataset and validation sizes.
- `SEARCH_CONCURRENCY`: comma-separated search concurrency levels.
- `M`, `EF_CONSTRUCTION`, `EF_SEARCH`: HNSW parameters.
- `MIN_RECALL`: recall gate for full-vector rows such as TreeDB exact/default,
  Vectorlite, pgvector, and MongoDB. Defaults to `0.95`.
- `TREEDB_COLUMN_GRAPH_EF_SEARCH`: optional efSearch override for TreeDB
  `column_graph` rows; defaults to `EF_SEARCH`.
- `TREEDB_QUANTIZED_INDEX_NAME`: scalar_u8 TreeDB quantized score-plane name.
  Defaults to `embedding.scalar_u8.fast`.
- `TREEDB_QUANTIZED_RERANK_CANDIDATES`: TreeDB quantized-rerank exact rerank
  candidate limit. Defaults to `32`; set `0` to use the normalized efSearch set.
- `TREEDB_QUANTIZED_MIN_RECALL`: recall gate for both TreeDB quantized rows.
  Defaults to `0`, so comparisons report quantized recall instead of failing
  before rendering; set a positive value to enforce a quantized recall floor.
- `TREEDB_QUANTIZED_ONLY_MIN_RECALL` and
  `TREEDB_QUANTIZED_RERANK_MIN_RECALL`: optional per-mode overrides for
  `TREEDB_QUANTIZED_MIN_RECALL`.
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
- `treedb.json`: TreeDB native benchmark result
- `treedb_column_graph.json`: TreeDB column_graph exact/default result when enabled
- `treedb_column_graph_quantized_only.json`: TreeDB scalar_u8 quantized-only result when enabled
- `treedb_column_graph_quantized_rerank.json`: TreeDB scalar_u8 quantized-rerank result when enabled
- `vectorlite.json`: SQLite+Vectorlite benchmark result
- `pgvector.json`: PostgreSQL+pgvector full-vector HNSW benchmark result when enabled
- `mongodb.json`: MongoDB Vector Search benchmark result when enabled
- `comparison.md`: normalized comparison table

The TreeDB dataset exporter writes row-major little-endian `float32` vector
files plus JSONL convenience files. TreeDB loads documents from the exported
JSONL and query vectors from the exported binary query file; the Python
benchmarks consume the exported binary vector files directly.

Record the generated `README.md`, backend JSON results, and `comparison.md` with
any published result. The script records the git commit and run shape; reruns
can also override `PYTHON`, `VENV`, `NUMPY_PACKAGE`, and `VECTORLITE_PACKAGE`
when exact dependency reproduction requires a different pinned package set.

Example scale probe:

```sh
RUN_DIR=/tmp/vector_db_compare_100k \
BACKENDS=treedb,vectorlite,pgvector \
DOCS=100000 \
QUERIES=50000 \
SEARCH_CONCURRENCY=2,4,8,16,32,64,128 \
scripts/bench_vector_db_compare.sh
```
