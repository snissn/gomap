#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_vector_db_compare_$(date +%Y%m%d_%H%M%S)}"
PYTHON="${PYTHON:-python3}"
VENV="${VENV:-$RUN_DIR/venv}"
BACKENDS="${BACKENDS:-treedb,vectorlite}"
DOCS="${DOCS:-10000}"
DIMS="${DIMS:-64}"
QUERIES="${QUERIES:-10000}"
VALIDATE_QUERIES="${VALIDATE_QUERIES:-64}"
TOP_K="${TOP_K:-10}"
SEARCH_CONCURRENCY="${SEARCH_CONCURRENCY:-2,4,8,16,32,64,128}"
M="${M:-16}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-128}"
EF_SEARCH="${EF_SEARCH:-128}"
TREEDB_COLUMN_GRAPH_EF_SEARCH="${TREEDB_COLUMN_GRAPH_EF_SEARCH:-}"
TREEDB_QUANTIZED_INDEX_NAME="${TREEDB_QUANTIZED_INDEX_NAME:-embedding.scalar_u8.fast}"
DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES=32
if [[ "$TOP_K" =~ ^[0-9]+$ ]] && ((TOP_K > DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES)); then
	DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES="$TOP_K"
fi
TREEDB_QUANTIZED_RERANK_CANDIDATES="${TREEDB_QUANTIZED_RERANK_CANDIDATES:-$DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES}"
MIN_RECALL="${MIN_RECALL:-0.95}"
TREEDB_QUANTIZED_MIN_RECALL="${TREEDB_QUANTIZED_MIN_RECALL:-0}"
TREEDB_QUANTIZED_ONLY_MIN_RECALL="${TREEDB_QUANTIZED_ONLY_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_QUANTIZED_RERANK_MIN_RECALL="${TREEDB_QUANTIZED_RERANK_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
VALIDATE_DOCS="${VALIDATE_DOCS:-16}"
TREEDB_COMPACT="${TREEDB_COMPACT:-}"
TREEDB_COMPACT_SYNC_EACH_PHASE="${TREEDB_COMPACT_SYNC_EACH_PHASE:-}"
TREEDB_VALUE_POINTER_THRESHOLD="${TREEDB_VALUE_POINTER_THRESHOLD:-}"
TREEDB_LEAF_GENERATION_SEGMENT_TARGET="${TREEDB_LEAF_GENERATION_SEGMENT_TARGET:-}"
TREEDB_REQUIRE_VALUE_LOG_BYTES="${TREEDB_REQUIRE_VALUE_LOG_BYTES:-}"
TREEDB_REQUIRE_LEAF_VLOG_BYTES="${TREEDB_REQUIRE_LEAF_VLOG_BYTES:-}"
NUMPY_PACKAGE="${NUMPY_PACKAGE:-numpy==2.0.2}"
VECTORLITE_PACKAGE="${VECTORLITE_PACKAGE:-vectorlite-py==0.2.0}"

PGVECTOR_DSN="${PGVECTOR_DSN:-}"
PGVECTOR_DOCKER="${PGVECTOR_DOCKER:-auto}"
PGVECTOR_IMAGE="${PGVECTOR_IMAGE:-pgvector/pgvector:pg16}"
PGVECTOR_MAX_CONNECTIONS="${PGVECTOR_MAX_CONNECTIONS:-256}"
PGVECTOR_CONTAINER_NAME="${PGVECTOR_CONTAINER_NAME:-gomap-pgvector-$RANDOM-$$}"
PGVECTOR_SCHEMA="${PGVECTOR_SCHEMA:-gomap_vector_bench_${RANDOM}_$$}"
PGVECTOR_TABLE="${PGVECTOR_TABLE:-documents}"
PGVECTOR_ALLOW_DROP_SCHEMA="${PGVECTOR_ALLOW_DROP_SCHEMA:-false}"
PGVECTOR_DROP_SCHEMA_AFTER="${PGVECTOR_DROP_SCHEMA_AFTER:-false}"
PGVECTOR_CONTAINER=""

MONGODB_VECTOR_URI="${MONGODB_VECTOR_URI:-}"
MONGODB_VECTOR_DATABASE="${MONGODB_VECTOR_DATABASE:-gomap_vector_bench_${RANDOM}_$$}"
MONGODB_VECTOR_COLLECTION="${MONGODB_VECTOR_COLLECTION:-documents}"
MONGODB_VECTOR_INDEX="${MONGODB_VECTOR_INDEX:-embedding_vector_index}"
MONGODB_VECTOR_NUM_CANDIDATES="${MONGODB_VECTOR_NUM_CANDIDATES:-$EF_SEARCH}"
MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS="${MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS:-300}"

cleanup() {
	if [[ -n "$PGVECTOR_CONTAINER" ]]; then
		docker rm -f "$PGVECTOR_CONTAINER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

contains_backend() {
	local want="$1"
	local raw
	local backend
	IFS=',' read -r -a raw <<<"$BACKENDS"
	for backend in "${raw[@]}"; do
		backend="${backend//[[:space:]]/}"
		if [[ "$backend" == "$want" ]]; then
			return 0
		fi
	done
	return 1
}

validate_backends() {
	local raw
	local backend
	IFS=',' read -r -a raw <<<"$BACKENDS"
	for backend in "${raw[@]}"; do
		backend="${backend//[[:space:]]/}"
		case "$backend" in
			treedb|treedb_column_graph|treedb_column_graph_quantized_only|treedb_column_graph_quantized_rerank|vectorlite|pgvector|mongodb)
				;;
			"")
				echo "empty backend in BACKENDS=$BACKENDS" >&2
				exit 1
				;;
			*)
				echo "unknown backend: $backend (known: treedb,treedb_column_graph,treedb_column_graph_quantized_only,treedb_column_graph_quantized_rerank,vectorlite,pgvector,mongodb)" >&2
				exit 1
				;;
		esac
	done
}

validate_backend_configuration() {
	if contains_backend mongodb && [[ -z "$MONGODB_VECTOR_URI" ]]; then
		echo "mongodb backend requested, but MONGODB_VECTOR_URI is empty" >&2
		echo "Set MONGODB_VECTOR_URI to an Atlas or local Atlas Vector Search deployment, or remove mongodb from BACKENDS." >&2
		exit 1
	fi
}

start_pgvector_if_needed() {
	if [[ -n "$PGVECTOR_DSN" ]]; then
		return
	fi
	if [[ "$PGVECTOR_DOCKER" == "false" ]]; then
		echo "PGVECTOR_DSN is required when PGVECTOR_DOCKER=false" >&2
		exit 1
	fi
	if ! command -v docker >/dev/null 2>&1; then
		echo "docker is required for automatic pgvector startup; set PGVECTOR_DSN to use an external PostgreSQL service" >&2
		exit 1
	fi
	echo "starting PostgreSQL+pgvector container: $PGVECTOR_IMAGE"
	PGVECTOR_CONTAINER=$(docker run -d --rm \
		--name "$PGVECTOR_CONTAINER_NAME" \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=gomap_vector_bench \
		-p 127.0.0.1::5432 \
		"$PGVECTOR_IMAGE" \
		-c "max_connections=$PGVECTOR_MAX_CONNECTIONS")
	local mapped
	mapped=$(docker port "$PGVECTOR_CONTAINER" 5432/tcp | sed -nE 's/.*:([0-9]+)$/\1/p' | head -n 1)
	if [[ -z "$mapped" ]]; then
		echo "could not determine mapped pgvector container port" >&2
		exit 1
	fi
	PGVECTOR_DSN="postgresql://postgres:postgres@127.0.0.1:${mapped}/gomap_vector_bench?sslmode=disable"
	echo "pgvector dsn: [redacted]"
	"$VENV/bin/python" - <<'PY' "$PGVECTOR_DSN"
import sys
import time
import psycopg

dsn = sys.argv[1]
last = None
for _ in range(120):
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("select 1")
        raise SystemExit(0)
    except Exception as exc:  # noqa: BLE001
        last = exc
        time.sleep(0.5)
raise SystemExit(f"PostgreSQL+pgvector did not become ready: {last}")
PY
}

validate_backends
validate_backend_configuration
if [[ -z "$TREEDB_COLUMN_GRAPH_EF_SEARCH" ]]; then
	TREEDB_COLUMN_GRAPH_EF_SEARCH="$EF_SEARCH"
fi

# Preserve the existing TreeDB harness default for document validation, and pass
# the remaining storage knobs only when the caller explicitly sets their env var.
treedb_storage_args=(-validate-docs "$VALIDATE_DOCS")
if [[ -n "$TREEDB_COMPACT" ]]; then
	treedb_storage_args+=("-compact=$TREEDB_COMPACT")
fi
if [[ -n "$TREEDB_COMPACT_SYNC_EACH_PHASE" ]]; then
	treedb_storage_args+=("-compact-sync-each-phase=$TREEDB_COMPACT_SYNC_EACH_PHASE")
fi
if [[ -n "$TREEDB_VALUE_POINTER_THRESHOLD" ]]; then
	treedb_storage_args+=(-value-pointer-threshold "$TREEDB_VALUE_POINTER_THRESHOLD")
fi
if [[ -n "$TREEDB_LEAF_GENERATION_SEGMENT_TARGET" ]]; then
	treedb_storage_args+=(-leaf-generation-segment-target "$TREEDB_LEAF_GENERATION_SEGMENT_TARGET")
fi
if [[ -n "$TREEDB_REQUIRE_VALUE_LOG_BYTES" ]]; then
	treedb_storage_args+=("-require-value-log-bytes=$TREEDB_REQUIRE_VALUE_LOG_BYTES")
fi
if [[ -n "$TREEDB_REQUIRE_LEAF_VLOG_BYTES" ]]; then
	treedb_storage_args+=("-require-leaf-vlog-bytes=$TREEDB_REQUIRE_LEAF_VLOG_BYTES")
fi
mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/README.md" <<EOF
# Vector Database Comparison

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- backends: \`$BACKENDS\`
- docs: \`$DOCS\`
- dims: \`$DIMS\`
- queries: \`$QUERIES\`
- validate queries: \`$VALIDATE_QUERIES\`
- validate docs: \`$VALIDATE_DOCS\`
- top_k: \`$TOP_K\`
- concurrency: \`$SEARCH_CONCURRENCY\`
- M / efConstruction / efSearch: \`$M / $EF_CONSTRUCTION / $EF_SEARCH\`
- TreeDB column_graph efSearch: \`$TREEDB_COLUMN_GRAPH_EF_SEARCH\`
- minimum recall: \`$MIN_RECALL\`
- TreeDB quantized index/rerank candidates: \`$TREEDB_QUANTIZED_INDEX_NAME / $TREEDB_QUANTIZED_RERANK_CANDIDATES\`
- TreeDB quantized-only/rerank minimum recall: \`$TREEDB_QUANTIZED_ONLY_MIN_RECALL / $TREEDB_QUANTIZED_RERANK_MIN_RECALL\`
- TreeDB storage/validation knobs:
  - \`VALIDATE_DOCS=$VALIDATE_DOCS\`
  - \`TREEDB_COMPACT=${TREEDB_COMPACT:-<unset>}\`
  - \`TREEDB_COMPACT_SYNC_EACH_PHASE=${TREEDB_COMPACT_SYNC_EACH_PHASE:-<unset>}\`
  - \`TREEDB_VALUE_POINTER_THRESHOLD=${TREEDB_VALUE_POINTER_THRESHOLD:-<unset>}\`
  - \`TREEDB_LEAF_GENERATION_SEGMENT_TARGET=${TREEDB_LEAF_GENERATION_SEGMENT_TARGET:-<unset>}\`
  - \`TREEDB_REQUIRE_VALUE_LOG_BYTES=${TREEDB_REQUIRE_VALUE_LOG_BYTES:-<unset>}\`
  - \`TREEDB_REQUIRE_LEAF_VLOG_BYTES=${TREEDB_REQUIRE_LEAF_VLOG_BYTES:-<unset>}\`
- Python packages: \`$NUMPY_PACKAGE\`, \`$VECTORLITE_PACKAGE\`

This run compares persistent database-tier ANN search:

- TreeDB native persisted HNSW through \`cmd/treedb_vector_search_demo\`
- TreeDB column-store graph exact/default search through
  \`cmd/treedb_vector_search_demo -vector-index-strategy column_graph\`
- TreeDB column-store graph scalar_u8 quantized modes through explicit
  \`-vector-query-mode quantized_only|quantized_rerank\` demo flags
- SQLite+Vectorlite HNSW through Python's \`sqlite3\` and the \`vectorlite-py\`
  loadable extension
- PostgreSQL+pgvector HNSW through a PostgreSQL server
- MongoDB Vector Search HNSW only when \`MONGODB_VECTOR_URI\` points at Atlas or
  local Atlas Vector Search

\`sqlite-vec\` is not included because upstream sqlite-vec's \`vec0\` table is
brute-force today; ANN support is still future work.
EOF

echo "run dir: $RUN_DIR"
echo "creating Python environment: $VENV"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install -q --upgrade pip

binary_pip_packages=("$NUMPY_PACKAGE")
backend_pip_packages=()
if contains_backend vectorlite; then
	binary_pip_packages+=("$VECTORLITE_PACKAGE")
fi
if contains_backend pgvector; then
	backend_pip_packages+=("psycopg[binary]")
fi
if contains_backend mongodb; then
	backend_pip_packages+=("pymongo[srv]")
fi
"$VENV/bin/python" -m pip install -q --only-binary=:all: "${binary_pip_packages[@]}"
if ((${#backend_pip_packages[@]})); then
	"$VENV/bin/python" -m pip install -q --prefer-binary "${backend_pip_packages[@]}"
fi

echo "exporting TreeDB dataset"
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
	-out "$RUN_DIR/dataset" \
	-docs "$DOCS" \
	-dims "$DIMS" \
	-queries "$QUERIES" \
	-top-k "$TOP_K" \
	-json >"$RUN_DIR/dataset_export.json"

result_args=()

if contains_backend treedb; then
	echo "running TreeDB benchmark"
	GOWORK=off go run ./cmd/treedb_vector_search_demo \
		-matrix=false \
		-dataset-dir "$RUN_DIR/dataset" \
		-dir "$RUN_DIR/treedb" \
		-keep-dir \
		-docs "$DOCS" \
		-dims "$DIMS" \
		-queries "$QUERIES" \
		-search-concurrency "$SEARCH_CONCURRENCY" \
		-validate-queries "$VALIDATE_QUERIES" \
		"${treedb_storage_args[@]}" \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$EF_SEARCH" \
		-min-recall "$MIN_RECALL" \
		-json >"$RUN_DIR/treedb.json"
	result_args+=(--result "$RUN_DIR/treedb.json")
fi

if contains_backend treedb_column_graph; then
	echo "running TreeDB column-store graph benchmark"
	GOWORK=off go run ./cmd/treedb_vector_search_demo \
		-matrix=false \
		-vector-index-strategy column_graph \
		-dataset-dir "$RUN_DIR/dataset" \
		-dir "$RUN_DIR/treedb_column_graph" \
		-keep-dir \
		-docs "$DOCS" \
		-dims "$DIMS" \
		-queries "$QUERIES" \
		-search-concurrency "$SEARCH_CONCURRENCY" \
		-validate-queries "$VALIDATE_QUERIES" \
		"${treedb_storage_args[@]}" \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$TREEDB_COLUMN_GRAPH_EF_SEARCH" \
		-min-recall "$MIN_RECALL" \
		-json >"$RUN_DIR/treedb_column_graph.json"
	result_args+=(--result "$RUN_DIR/treedb_column_graph.json")
fi

if contains_backend treedb_column_graph_quantized_only; then
	echo "running TreeDB column-store graph scalar_u8 quantized_only benchmark"
	GOWORK=off go run ./cmd/treedb_vector_search_demo \
		-matrix=false \
		-vector-index-strategy column_graph \
		-vector-query-mode quantized_only \
		-quantized-index-name "$TREEDB_QUANTIZED_INDEX_NAME" \
		-dataset-dir "$RUN_DIR/dataset" \
		-dir "$RUN_DIR/treedb_column_graph_quantized_only" \
		-keep-dir \
		-docs "$DOCS" \
		-dims "$DIMS" \
		-queries "$QUERIES" \
		-search-concurrency "$SEARCH_CONCURRENCY" \
		-validate-queries "$VALIDATE_QUERIES" \
		"${treedb_storage_args[@]}" \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$TREEDB_COLUMN_GRAPH_EF_SEARCH" \
		-min-recall "$TREEDB_QUANTIZED_ONLY_MIN_RECALL" \
		-json >"$RUN_DIR/treedb_column_graph_quantized_only.json"
	result_args+=(--result "$RUN_DIR/treedb_column_graph_quantized_only.json")
fi

if contains_backend treedb_column_graph_quantized_rerank; then
	echo "running TreeDB column-store graph scalar_u8 quantized_rerank benchmark"
	GOWORK=off go run ./cmd/treedb_vector_search_demo \
		-matrix=false \
		-vector-index-strategy column_graph \
		-vector-query-mode quantized_rerank \
		-quantized-index-name "$TREEDB_QUANTIZED_INDEX_NAME" \
		-quantized-rerank-candidates "$TREEDB_QUANTIZED_RERANK_CANDIDATES" \
		-dataset-dir "$RUN_DIR/dataset" \
		-dir "$RUN_DIR/treedb_column_graph_quantized_rerank" \
		-keep-dir \
		-docs "$DOCS" \
		-dims "$DIMS" \
		-queries "$QUERIES" \
		-search-concurrency "$SEARCH_CONCURRENCY" \
		-validate-queries "$VALIDATE_QUERIES" \
		"${treedb_storage_args[@]}" \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$TREEDB_COLUMN_GRAPH_EF_SEARCH" \
		-min-recall "$TREEDB_QUANTIZED_RERANK_MIN_RECALL" \
		-json >"$RUN_DIR/treedb_column_graph_quantized_rerank.json"
	result_args+=(--result "$RUN_DIR/treedb_column_graph_quantized_rerank.json")
fi

if contains_backend vectorlite; then
	echo "running SQLite+Vectorlite benchmark"
	"$VENV/bin/python" benchmarks/vector_db_compare/vectorlite_bench.py \
		--dataset-dir "$RUN_DIR/dataset" \
		--db-dir "$RUN_DIR/vectorlite" \
		--output "$RUN_DIR/vectorlite.json" \
		--queries "$QUERIES" \
		--validate-queries "$VALIDATE_QUERIES" \
		--top-k "$TOP_K" \
		--search-concurrency "$SEARCH_CONCURRENCY" \
		--m "$M" \
		--ef-construction "$EF_CONSTRUCTION" \
		--ef-search "$EF_SEARCH" \
		--min-recall "$MIN_RECALL" >"$RUN_DIR/vectorlite.stdout.json"
	result_args+=(--result "$RUN_DIR/vectorlite.json")
fi

if contains_backend pgvector; then
	start_pgvector_if_needed
	echo "running PostgreSQL+pgvector benchmark"
	pgvector_args=(
		--dataset-dir "$RUN_DIR/dataset"
		--dsn "$PGVECTOR_DSN"
		--schema "$PGVECTOR_SCHEMA"
		--table "$PGVECTOR_TABLE"
		--output "$RUN_DIR/pgvector.json"
		--queries "$QUERIES"
		--validate-queries "$VALIDATE_QUERIES"
		--top-k "$TOP_K"
		--search-concurrency "$SEARCH_CONCURRENCY"
		--m "$M"
		--ef-construction "$EF_CONSTRUCTION"
		--ef-search "$EF_SEARCH"
		--min-recall "$MIN_RECALL"
	)
	if [[ "$PGVECTOR_DROP_SCHEMA_AFTER" == "true" ]]; then
		pgvector_args+=(--drop-schema-after)
	fi
	if [[ "$PGVECTOR_ALLOW_DROP_SCHEMA" == "true" ]]; then
		pgvector_args+=(--allow-drop-schema)
	fi
	"$VENV/bin/python" benchmarks/vector_db_compare/pgvector_bench.py "${pgvector_args[@]}" >"$RUN_DIR/pgvector.stdout.json"
	result_args+=(--result "$RUN_DIR/pgvector.json")
fi

if contains_backend mongodb; then
	echo "running MongoDB Vector Search benchmark"
	"$VENV/bin/python" benchmarks/vector_db_compare/mongodb_vector_bench.py \
		--dataset-dir "$RUN_DIR/dataset" \
		--uri "$MONGODB_VECTOR_URI" \
		--database "$MONGODB_VECTOR_DATABASE" \
		--collection "$MONGODB_VECTOR_COLLECTION" \
		--index-name "$MONGODB_VECTOR_INDEX" \
		--output "$RUN_DIR/mongodb.json" \
		--queries "$QUERIES" \
		--validate-queries "$VALIDATE_QUERIES" \
		--top-k "$TOP_K" \
		--search-concurrency "$SEARCH_CONCURRENCY" \
		--num-candidates "$MONGODB_VECTOR_NUM_CANDIDATES" \
		--index-timeout-seconds "$MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS" \
		--min-recall "$MIN_RECALL" >"$RUN_DIR/mongodb.stdout.json"
	result_args+=(--result "$RUN_DIR/mongodb.json")
fi

if ((${#result_args[@]} == 0)); then
	echo "no benchmark results were produced; check BACKENDS and required backend configuration" >&2
	exit 1
fi

echo "rendering comparison"
"$VENV/bin/python" benchmarks/vector_db_compare/summarize.py \
	"${result_args[@]}" \
	--output "$RUN_DIR/comparison.md"

echo "wrote $RUN_DIR/comparison.md"
