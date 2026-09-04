# Vector DB Comparison

For the separate Minima client/service lifecycle benchmark, use the
[Minima runbook](../../TreeDB/docs/benchmarks/treedb_rag_benchmark_runbook.md#minima-native-path-development-4614).
Its bounded 50K/250K modes are diagnostics, not full qualification or evidence
that mutable `column_graph` is already supported.

This benchmark compares persistent database-tier ANN search:

- TreeDB native persisted HNSW via `cmd/treedb_vector_search_demo`
- TreeDB `column_graph` full-vector exact/default HNSW via the same demo
- TreeDB `column_graph` scalar_u8 and rabitq_1bit `quantized_only` and
  `quantized_rerank` query modes via explicit demo flags
- SQLite with the Vectorlite loadable extension, backed by hnswlib/HNSW
- PostgreSQL with pgvector full-vector HNSW
- Milvus Standalone full-vector HNSW
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

For the bounded #4019 TreeDB topology-tax baseline, retain three checked
`system-bench` JSON files for both the single-daemon and native four-daemon
topologies, then validate and summarize them without discarding raw samples:

```sh
python3 benchmarks/vector_db_compare/topology_tax.py \
  --single single-1.json --single single-2.json --single single-3.json \
  --native native-1.json --native native-2.json --native native-3.json \
  --source-revision <40-character-lowercase-git-sha> \
  --executable-path <absolute-benchmark-binary-path> \
  --executable-sha256 <benchmark-binary-sha256> \
  --out topology-tax.json
```

The reducer requires the frozen 100k M2 p2/p16, c1/c8, EF128 shape and binds
each input SHA256, checked topology identity, fixture/truth identity,
generation, raw latency samples, wall elapsed time, counters, and coordinator
stage timings. It also validates every retained node readiness record against
the expected clean source revision, executable SHA256, node config, persistent
roots, and production route. It also validates the retained `/usr/bin/time`
client command, successful exit, and timed executable path. The output binds
all readiness and client-process attestation digests.
Every retained cell must meet recall@10 >= 0.90. The first bounded
baseline is retained under
`TreeDB/docs/evidence/vector-partition-local-system-qualification-4019/m2-95c60cbe`.
The full five-row comparative qualification remains a separate M3 execution of
the committed plan.

The default backend set is `treedb,vectorlite`. Add `treedb_column_graph`, the
TreeDB scalar_u8/RaBitQ quantized aliases, `pgvector`, `milvus`, or `mongodb` to
`BACKENDS` when those paths or external services are needed.

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

TreeDB exact vs scalar_u8/RaBitQ quantized column-graph comparison:

```sh
RUN_DIR=/tmp/vector_db_compare_quantized \
BACKENDS=treedb_column_graph,treedb_column_graph_scalar_u8_quantized_only,treedb_column_graph_scalar_u8_quantized_rerank,treedb_column_graph_rabitq_1bit_quantized_only,treedb_column_graph_rabitq_1bit_quantized_rerank,pgvector \
DOCS=10000 \
DIMS=128 \
QUERIES=10000 \
VALIDATE_QUERIES=64 \
TOP_K=10 \
EF_SEARCH=128 \
TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME=embedding.scalar_u8.fast \
TREEDB_RABITQ_QUANTIZED_INDEX_NAME=embedding.rabitq_1bit.fast \
TREEDB_QUANTIZED_RERANK_CANDIDATES=32 \
TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES=32 \
TREEDB_QUANTIZED_MIN_RECALL=0 \
scripts/bench_vector_db_compare.sh
```

The quantized comparison leaves TreeDB quantized recall as an observation by
setting `TREEDB_QUANTIZED_MIN_RECALL=0`; full-vector TreeDB/pgvector rows still
use `MIN_RECALL` (default `0.95`) as their recall gate. TreeDB `column_graph`
search timings are no-document buffered rows: they return IDs/scores, exclude
document fetch/materialization, and expose guardrail counters such as
`avg_response_owned_result_allocs`, `avg_documents_fetched`, and route/fallback
averages in the demo JSON.

Backends:

- `treedb`: runs the TreeDB native persisted vector index benchmark.
- `treedb_column_graph`: runs TreeDB `column_graph` exact/default full-vector
  graph search.
- `treedb_column_graph_quantized_only`: compatibility alias for a TreeDB
  `column_graph` quantized-only row using `TREEDB_QUANTIZED_CODEC` and
  `TREEDB_QUANTIZED_INDEX_NAME` (defaults to scalar_u8).
- `treedb_column_graph_quantized_rerank`: compatibility alias for a TreeDB
  `column_graph` quantized-rerank row using `TREEDB_QUANTIZED_CODEC` and
  `TREEDB_QUANTIZED_INDEX_NAME` (defaults to scalar_u8).
- `treedb_column_graph_scalar_u8_quantized_only`: runs TreeDB `column_graph`
  with a named scalar_u8 score plane and `query_mode=quantized_only`.
- `treedb_column_graph_scalar_u8_quantized_rerank`: runs TreeDB `column_graph`
  with a named scalar_u8 score plane and `query_mode=quantized_rerank`,
  exact-reranking `TREEDB_QUANTIZED_RERANK_CANDIDATES` candidates (or the
  demo's normalized `ef_search` set when set to `0`).
- `treedb_column_graph_rabitq_1bit_quantized_only`: runs TreeDB `column_graph`
  with a named RaBitQ `rabitq_1bit` score plane and
  `query_mode=quantized_only`.
- `treedb_column_graph_rabitq_1bit_quantized_rerank`: runs TreeDB
  `column_graph` with a named RaBitQ `rabitq_1bit` score plane and
  `query_mode=quantized_rerank`, exact-reranking
  `TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES` candidates (or the demo's
  normalized `ef_search` set when set to `0`).
- `vectorlite`: runs SQLite+Vectorlite with its persisted HNSW sidecar file.
- `pgvector`: runs PostgreSQL+pgvector full-vector HNSW. If `PGVECTOR_DSN` is not set, the
  runner starts the digest-pinned PostgreSQL 16 + pgvector 0.8.6 Docker image. The
  harness does not use pgvector `halfvec`, `binary_quantize`, SQL rerank,
  custom byte-code scoring, or custom operator classes; pgvector remains the
  full-vector HNSW anchor. The
  harness writes into a fresh benchmark schema (`PGVECTOR_SCHEMA`) instead of
  dropping tables in the caller's default schema. Set
  `PGVECTOR_DROP_SCHEMA_AFTER=true` when using an external DSN and you want the
  benchmark schema dropped after the run. Insert timing includes client-side
  vector text serialization for COPY; the JSON result reports that preparation
  time separately under the insert phase.
- `milvus`: runs Milvus Standalone full-vector HNSW. If `MILVUS_URI` is not
  set, the runner checksum-verifies the pinned upstream Milvus 2.6.20 compose
  file, replaces every server image with its frozen digest, and removes the
  containers/network on exit. Server data remains under the run directory for
  storage accounting.
- `mongodb`: runs MongoDB Vector Search and requires `MONGODB_VECTOR_URI` to
  point at a deployment that supports `$vectorSearch` and `createSearchIndexes`.
  The runner installs PyMongo with SRV support for Atlas `mongodb+srv://` URIs
  and uses a fresh benchmark database name by default.

Configuration:

- `RUN_DIR`: output directory. Defaults to a timestamped directory under `/tmp`.
- `BACKENDS`: comma-separated backend list. Defaults to `treedb,vectorlite`.
- `DOCS`, `DIMS`, `QUERIES`, `VALIDATE_QUERIES`, `VALIDATE_DOCS`,
  `TOP_K`: dataset and validation sizes. `VALIDATE_DOCS` applies only to TreeDB
  rows and defaults to `16`. The exporter writes query vectors for all
  `QUERIES`, but exhaustive `exact_truth.jsonl` covers only the leading
  `min(VALIDATE_QUERIES, QUERIES)` queries. The manifest records that effective
  prefix count as `exact_truth_queries`. Zero disables exported exact-truth rows
  while retaining all query vectors, matching disabled recall validation. When
  validation is disabled, the runner passes an effective minimum recall of `0`
  to every backend while retaining configured recall gates in the run metadata.
  Omitting the exporter's `-truth-queries` flag directly still preserves its
  standalone default of truth for all queries.
- `SEARCH_CONCURRENCY`: comma-separated search concurrency levels.
- `M`, `EF_CONSTRUCTION`, `EF_SEARCH`: HNSW parameters.
- `MIN_RECALL`: recall gate for full-vector rows such as TreeDB exact/default,
  Vectorlite, pgvector, and MongoDB. Defaults to `0.95`; its effective value is
  `0` when `VALIDATE_QUERIES=0`.
- `TREEDB_COLUMN_GRAPH_EF_SEARCH`: optional efSearch override for TreeDB
  `column_graph` rows; defaults to `EF_SEARCH`.
- `TREEDB_COMPACT`, `TREEDB_COMPACT_SYNC_EACH_PHASE`,
  `TREEDB_VALUE_POINTER_THRESHOLD`, `TREEDB_LEAF_GENERATION_SEGMENT_TARGET`,
  `TREEDB_REQUIRE_VALUE_LOG_BYTES`, and `TREEDB_REQUIRE_LEAF_VLOG_BYTES`:
  optional passthroughs to the matching `cmd/treedb_vector_search_demo` storage
  flags for every TreeDB row. Unset variables are not passed, so demo defaults
  are preserved.
- `TREEDB_VALIDATION_EXACT_SOURCE`: optional passthrough to
  `-validation-exact-source=treedb|dataset` for every TreeDB row. Unset keeps
  the demo default (`treedb`). Set `dataset` to compute validation exact top-K
  IDs from the exported dataset vectors instead of TreeDB exact search.
- `TREEDB_SEARCH_PROFILE_DIR`: optional top-level directory for TreeDB
  column_graph search profiles. When set, only TreeDB column_graph rows pass a
  backend-specific subdirectory to `-search-profile-dir`; the native `treedb`
  row does not receive the flag because native search profiling is not
  implemented. Column_graph rows emit per-concurrency
  `search_<mode>_c<N>_{cpu,heap,allocs,block,mutex}.pprof` files there. If the
  demo matrix mode is used, profiles are further nested under the matrix case
  name (`<profile-dir>/<backend>/<matrix_case>/...`) so case artifacts are not
  overwritten. CPU profiles are scoped to the search loop; the other files are
  runtime snapshots for supporting diagnosis. Heap and allocation profiles use
  the Go runtime's current/default sampling rate. Block profiles are emitted
  from the runtime's current block profiler and may be empty unless block
  profiling was already enabled.
- `TREEDB_QUANTIZED_CODEC`: codec for the compatibility quantized aliases.
  Defaults to `scalar_u8`; set to `rabitq_1bit` only when deliberately using the
  generic aliases for RaBitQ.
- `TREEDB_QUANTIZED_INDEX_NAME`: score-plane name for the compatibility
  quantized aliases. Defaults to `embedding.scalar_u8.fast`.
- `TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME`: scalar_u8 score-plane name for the
  explicit scalar_u8 aliases. Defaults to `TREEDB_QUANTIZED_INDEX_NAME`.
- `TREEDB_RABITQ_QUANTIZED_INDEX_NAME`: RaBitQ score-plane name for the
  explicit RaBitQ aliases. Defaults to `embedding.rabitq_1bit.fast`.
- `TREEDB_QUANTIZED_RERANK_CANDIDATES`: TreeDB scalar_u8/compat quantized-rerank
  exact rerank candidate limit. Defaults to `max(32, TOP_K)`; set `0` to use the
  normalized efSearch set.
- `TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES`: TreeDB RaBitQ quantized-rerank
  exact rerank candidate limit. Defaults to `TREEDB_QUANTIZED_RERANK_CANDIDATES`.
- `TREEDB_QUANTIZED_MIN_RECALL`: recall gate for both TreeDB quantized rows.
  Defaults to `0`, so comparisons report quantized recall instead of failing
  before rendering; set a positive value to enforce a quantized recall floor.
  All quantized recall gates are effectively `0` when `VALIDATE_QUERIES=0`.
- `TREEDB_QUANTIZED_ONLY_MIN_RECALL` and
  `TREEDB_QUANTIZED_RERANK_MIN_RECALL`: optional scalar_u8/compat per-mode
  overrides for `TREEDB_QUANTIZED_MIN_RECALL`.
- `TREEDB_RABITQ_QUANTIZED_MIN_RECALL`,
  `TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL`, and
  `TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL`: optional RaBitQ recall gates;
  default to the corresponding generic quantized gates.
- `PGVECTOR_DSN`: external PostgreSQL DSN. If empty and `pgvector` is enabled,
  the runner starts Docker unless `PGVECTOR_DOCKER=false`.
- `PGVECTOR_DOCKER`, `PGVECTOR_IMAGE`, `PGVECTOR_MAX_CONNECTIONS`: automatic
  pgvector container controls.
- `MILVUS_URI`, `MILVUS_TOKEN`: external Milvus endpoint and credential. If
  `MILVUS_URI` is empty and `milvus` is enabled, the pinned standalone compose
  topology is started automatically. The token defaults to `root:Milvus`.
- `MILVUS_DOCKER`, `MILVUS_COMPOSE_URL`, `MILVUS_COMPOSE_SHA256`,
  `MILVUS_IMAGE`, `MILVUS_ETCD_IMAGE`, `MILVUS_MINIO_IMAGE`: exact automatic
  Milvus topology controls.
- `MILVUS_COLLECTION`, `MILVUS_INDEX`, `MILVUS_ALLOW_DROP_COLLECTION`,
  `MILVUS_DROP_COLLECTION_AFTER`: benchmark namespace and cleanup controls.
- `MILVUS_STORAGE_DIR`: explicit server-data mount to include in storage
  accounting for an external Milvus endpoint. Without it, storage is reported
  as unavailable instead of attributing a local path.
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
- `treedb_column_graph_quantized_only.json`: compatibility quantized-only result when enabled
- `treedb_column_graph_quantized_rerank.json`: compatibility quantized-rerank result when enabled
- `treedb_column_graph_scalar_u8_quantized_only.json`: TreeDB scalar_u8 quantized-only result when enabled
- `treedb_column_graph_scalar_u8_quantized_rerank.json`: TreeDB scalar_u8 quantized-rerank result when enabled
- `treedb_column_graph_rabitq_1bit_quantized_only.json`: TreeDB RaBitQ quantized-only result when enabled
- `treedb_column_graph_rabitq_1bit_quantized_rerank.json`: TreeDB RaBitQ quantized-rerank result when enabled
- `vectorlite.json`: SQLite+Vectorlite benchmark result
- `pgvector.json`: PostgreSQL+pgvector full-vector HNSW benchmark result when enabled
- `milvus.json`: Milvus Standalone full-vector HNSW benchmark result when enabled
- `mongodb.json`: MongoDB Vector Search benchmark result when enabled
- `comparison.md`: normalized comparison table
- TreeDB column_graph search profiles under backend-specific subdirectories of
  `TREEDB_SEARCH_PROFILE_DIR`, when set

The TreeDB dataset exporter writes row-major little-endian `float32` vector
files plus JSONL convenience files. TreeDB loads documents from the exported
JSONL and query vectors from the exported binary query file; the Python
benchmarks consume the exported binary vector files directly. With
`TREEDB_VALIDATION_EXACT_SOURCE=dataset`, TreeDB recall validation computes the
exact baseline from `documents.f32`/`queries.f32` and compares result IDs only;
it does not materialize TreeDB documents for the exact baseline. Consumers do
not require an exact-truth row for every benchmark query: they validate the
declared leading prefix or recompute their selected exact baseline from the
vector files.

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
VALIDATE_QUERIES=64 \
SEARCH_CONCURRENCY=2,4,8,16,32,64,128 \
scripts/bench_vector_db_compare.sh
```
