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
MIN_RECALL="${MIN_RECALL:-0.95}"

PGVECTOR_DSN="${PGVECTOR_DSN:-}"
PGVECTOR_DOCKER="${PGVECTOR_DOCKER:-auto}"
PGVECTOR_IMAGE="${PGVECTOR_IMAGE:-pgvector/pgvector:pg16}"
PGVECTOR_MAX_CONNECTIONS="${PGVECTOR_MAX_CONNECTIONS:-256}"
PGVECTOR_CONTAINER_NAME="${PGVECTOR_CONTAINER_NAME:-gomap-pgvector-$RANDOM-$$}"
PGVECTOR_SCHEMA="${PGVECTOR_SCHEMA:-gomap_vector_bench_${RANDOM}_$$}"
PGVECTOR_TABLE="${PGVECTOR_TABLE:-documents}"
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

start_pgvector_if_needed() {
	if [[ -n "$PGVECTOR_DSN" ]]; then
		return
	fi
	if [[ "$PGVECTOR_DOCKER" == "false" ]]; then
		echo "PGVECTOR_DSN is required when PGVECTOR_DOCKER=false" >&2
		exit 1
	fi
	if ! command -v docker >/dev/null 2>&1; then
		echo "docker is required for automatic pgvector startup; set PGVECTOR_DSN or PGVECTOR_DOCKER=false" >&2
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
- top_k: \`$TOP_K\`
- concurrency: \`$SEARCH_CONCURRENCY\`
- M / efConstruction / efSearch: \`$M / $EF_CONSTRUCTION / $EF_SEARCH\`

This run compares persistent database-tier ANN search:

- TreeDB native persisted HNSW through \`cmd/treedb_vector_search_demo\`
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

binary_pip_packages=(numpy)
backend_pip_packages=()
if contains_backend vectorlite; then
	binary_pip_packages+=(vectorlite-py)
fi
if contains_backend pgvector; then
	backend_pip_packages+=("psycopg[binary]")
fi
if contains_backend mongodb; then
	backend_pip_packages+=(pymongo)
fi
"$VENV/bin/python" -m pip install -q --only-binary=:all: "${binary_pip_packages[@]}"
if ((${#backend_pip_packages[@]})); then
	"$VENV/bin/python" -m pip install -q --only-binary=:all: "${backend_pip_packages[@]}"
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
		-validate-docs 16 \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$EF_SEARCH" \
		-min-recall "$MIN_RECALL" \
		-json >"$RUN_DIR/treedb.json"
	result_args+=(--result "$RUN_DIR/treedb.json")
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
	"$VENV/bin/python" benchmarks/vector_db_compare/pgvector_bench.py "${pgvector_args[@]}" >"$RUN_DIR/pgvector.stdout.json"
	result_args+=(--result "$RUN_DIR/pgvector.json")
fi

if contains_backend mongodb; then
	if [[ -z "$MONGODB_VECTOR_URI" ]]; then
		echo "mongodb backend requested, but MONGODB_VECTOR_URI is empty; skipping MongoDB Vector Search" | tee "$RUN_DIR/mongodb.skipped.txt"
	else
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
