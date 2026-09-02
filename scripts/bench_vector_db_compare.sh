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
EXPORT_TRUTH_QUERIES="$VALIDATE_QUERIES"
if ((EXPORT_TRUTH_QUERIES > QUERIES)); then
	EXPORT_TRUTH_QUERIES="$QUERIES"
fi
TOP_K="${TOP_K:-10}"
SEARCH_CONCURRENCY="${SEARCH_CONCURRENCY:-2,4,8,16,32,64,128}"
M="${M:-16}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-128}"
EF_SEARCH="${EF_SEARCH:-128}"
TREEDB_COLUMN_GRAPH_EF_SEARCH="${TREEDB_COLUMN_GRAPH_EF_SEARCH:-}"
TREEDB_QUANTIZED_CODEC="${TREEDB_QUANTIZED_CODEC:-scalar_u8}"
TREEDB_QUANTIZED_INDEX_NAME="${TREEDB_QUANTIZED_INDEX_NAME:-embedding.scalar_u8.fast}"
TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME="${TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME:-$TREEDB_QUANTIZED_INDEX_NAME}"
TREEDB_RABITQ_QUANTIZED_INDEX_NAME="${TREEDB_RABITQ_QUANTIZED_INDEX_NAME:-embedding.rabitq_1bit.fast}"
DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES=32
if [[ "$TOP_K" =~ ^[0-9]+$ ]] && ((TOP_K > DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES)); then
	DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES="$TOP_K"
fi
TREEDB_QUANTIZED_RERANK_CANDIDATES="${TREEDB_QUANTIZED_RERANK_CANDIDATES:-$DEFAULT_TREEDB_QUANTIZED_RERANK_CANDIDATES}"
TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES="${TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES:-$TREEDB_QUANTIZED_RERANK_CANDIDATES}"
MIN_RECALL="${MIN_RECALL:-0.95}"
TREEDB_QUANTIZED_MIN_RECALL="${TREEDB_QUANTIZED_MIN_RECALL:-0}"
TREEDB_QUANTIZED_ONLY_MIN_RECALL="${TREEDB_QUANTIZED_ONLY_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_QUANTIZED_RERANK_MIN_RECALL="${TREEDB_QUANTIZED_RERANK_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL:-$TREEDB_RABITQ_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL:-$TREEDB_RABITQ_QUANTIZED_MIN_RECALL}"
EFFECTIVE_MIN_RECALL="$MIN_RECALL"
EFFECTIVE_TREEDB_QUANTIZED_ONLY_MIN_RECALL="$TREEDB_QUANTIZED_ONLY_MIN_RECALL"
EFFECTIVE_TREEDB_QUANTIZED_RERANK_MIN_RECALL="$TREEDB_QUANTIZED_RERANK_MIN_RECALL"
EFFECTIVE_TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL="$TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL"
EFFECTIVE_TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL="$TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL"
if ((VALIDATE_QUERIES == 0)); then
	EFFECTIVE_MIN_RECALL=0
	EFFECTIVE_TREEDB_QUANTIZED_ONLY_MIN_RECALL=0
	EFFECTIVE_TREEDB_QUANTIZED_RERANK_MIN_RECALL=0
	EFFECTIVE_TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL=0
	EFFECTIVE_TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL=0
fi
VALIDATE_DOCS="${VALIDATE_DOCS:-16}"
TREEDB_COMPACT="${TREEDB_COMPACT:-}"
TREEDB_COMPACT_SYNC_EACH_PHASE="${TREEDB_COMPACT_SYNC_EACH_PHASE:-}"
TREEDB_VALUE_POINTER_THRESHOLD="${TREEDB_VALUE_POINTER_THRESHOLD:-}"
TREEDB_LEAF_GENERATION_SEGMENT_TARGET="${TREEDB_LEAF_GENERATION_SEGMENT_TARGET:-}"
TREEDB_REQUIRE_VALUE_LOG_BYTES="${TREEDB_REQUIRE_VALUE_LOG_BYTES:-}"
TREEDB_REQUIRE_LEAF_VLOG_BYTES="${TREEDB_REQUIRE_LEAF_VLOG_BYTES:-}"
TREEDB_VALIDATION_EXACT_SOURCE="${TREEDB_VALIDATION_EXACT_SOURCE:-}"
TREEDB_SEARCH_PROFILE_DIR="${TREEDB_SEARCH_PROFILE_DIR:-}"
NUMPY_PACKAGE="${NUMPY_PACKAGE:-numpy==2.0.2}"
VECTORLITE_PACKAGE="${VECTORLITE_PACKAGE:-vectorlite-py==0.2.0}"
PSYCOPG_PACKAGE="${PSYCOPG_PACKAGE:-psycopg[binary]==3.3.4}"
PYMILVUS_PACKAGE="${PYMILVUS_PACKAGE:-pymilvus==2.6.16}"

PGVECTOR_DSN="${PGVECTOR_DSN:-}"
PGVECTOR_DOCKER="${PGVECTOR_DOCKER:-auto}"
PGVECTOR_IMAGE="${PGVECTOR_IMAGE:-pgvector/pgvector:pg16@sha256:84a355869251af1a3379cfc9fa7b4dbf962c03f642a4bb7b339a203925071c43}"
PGVECTOR_MAX_CONNECTIONS="${PGVECTOR_MAX_CONNECTIONS:-256}"
PGVECTOR_CONTAINER_NAME="${PGVECTOR_CONTAINER_NAME:-gomap-pgvector-$RANDOM-$$}"
PGVECTOR_SCHEMA="${PGVECTOR_SCHEMA:-gomap_vector_bench_${RANDOM}_$$}"
PGVECTOR_TABLE="${PGVECTOR_TABLE:-documents}"
PGVECTOR_ALLOW_DROP_SCHEMA="${PGVECTOR_ALLOW_DROP_SCHEMA:-false}"
PGVECTOR_DROP_SCHEMA_AFTER="${PGVECTOR_DROP_SCHEMA_AFTER:-false}"
PGVECTOR_CONTAINER=""

MILVUS_URI="${MILVUS_URI:-}"
MILVUS_TOKEN="${MILVUS_TOKEN:-root:Milvus}"
MILVUS_DOCKER="${MILVUS_DOCKER:-auto}"
MILVUS_COMPOSE_URL="${MILVUS_COMPOSE_URL:-https://github.com/milvus-io/milvus/releases/download/v2.6.20/milvus-standalone-docker-compose.yml}"
MILVUS_COMPOSE_SHA256="${MILVUS_COMPOSE_SHA256:-9e0e8187e197ce23d3da3e63c19bc20189782f96bacb97287f8fcee80ba628c3}"
MILVUS_IMAGE="${MILVUS_IMAGE:-milvusdb/milvus:v2.6.20@sha256:e514fced2aa26cf3b94e7de20986fe9e535159fde08f9934d245d0e1a909c18c}"
MILVUS_ETCD_IMAGE="${MILVUS_ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.25@sha256:dc2bdc588d2adc5272204a1fff7f1d89f31e8caacea78fdf509fd409d7162a9d}"
MILVUS_MINIO_IMAGE="${MILVUS_MINIO_IMAGE:-minio/minio:RELEASE.2024-12-18T13-15-44Z@sha256:34c8e2f52a5984492555427fee07254c80036bdb7079bb91679232abd7a4fa20}"
MILVUS_PROJECT="${MILVUS_PROJECT:-gomap-milvus-$RANDOM-$$}"
MILVUS_COLLECTION="${MILVUS_COLLECTION:-gomap_vector_bench_${RANDOM}_$$}"
MILVUS_INDEX="${MILVUS_INDEX:-embedding_hnsw}"
MILVUS_ALLOW_DROP_COLLECTION="${MILVUS_ALLOW_DROP_COLLECTION:-false}"
MILVUS_DROP_COLLECTION_AFTER="${MILVUS_DROP_COLLECTION_AFTER:-false}"
MILVUS_STORAGE_DIR_EXPLICIT=false
if [[ -n "${MILVUS_STORAGE_DIR:-}" ]]; then
	MILVUS_STORAGE_DIR_EXPLICIT=true
fi
MILVUS_STORAGE_DIR="${MILVUS_STORAGE_DIR:-$RUN_DIR/milvus-server}"
MILVUS_COMPOSE_FILE=""
MILVUS_STARTED=false

MONGODB_VECTOR_URI="${MONGODB_VECTOR_URI:-}"
MONGODB_VECTOR_DATABASE="${MONGODB_VECTOR_DATABASE:-gomap_vector_bench_${RANDOM}_$$}"
MONGODB_VECTOR_COLLECTION="${MONGODB_VECTOR_COLLECTION:-documents}"
MONGODB_VECTOR_INDEX="${MONGODB_VECTOR_INDEX:-embedding_vector_index}"
MONGODB_VECTOR_NUM_CANDIDATES="${MONGODB_VECTOR_NUM_CANDIDATES:-$EF_SEARCH}"
MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS="${MONGODB_VECTOR_INDEX_TIMEOUT_SECONDS:-300}"

cleanup() {
	if [[ "$MILVUS_STARTED" == "true" ]]; then
		DOCKER_VOLUME_DIRECTORY="$MILVUS_STORAGE_DIR" docker compose -p "$MILVUS_PROJECT" -f "$MILVUS_COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
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
			treedb|treedb_column_graph|treedb_column_graph_quantized_only|treedb_column_graph_quantized_rerank|treedb_column_graph_scalar_u8_quantized_only|treedb_column_graph_scalar_u8_quantized_rerank|treedb_column_graph_rabitq_1bit_quantized_only|treedb_column_graph_rabitq_1bit_quantized_rerank|vectorlite|pgvector|milvus|mongodb)
				;;
			"")
				echo "empty backend in BACKENDS=$BACKENDS" >&2
				exit 1
				;;
			*)
				echo "unknown backend: $backend (known: treedb,treedb_column_graph,treedb_column_graph_quantized_only,treedb_column_graph_quantized_rerank,treedb_column_graph_scalar_u8_quantized_only,treedb_column_graph_scalar_u8_quantized_rerank,treedb_column_graph_rabitq_1bit_quantized_only,treedb_column_graph_rabitq_1bit_quantized_rerank,vectorlite,pgvector,milvus,mongodb)" >&2
				exit 1
				;;
		esac
	done
}

validate_treedb_quantized_codec() {
	local codec="$1"
	local source="$2"
	case "$codec" in
		scalar_u8|rabitq_1bit)
			;;
		*)
			echo "$source must be scalar_u8 or rabitq_1bit, got: $codec" >&2
			exit 1
			;;
	esac
}

validate_backend_configuration() {
	if contains_backend mongodb && [[ -z "$MONGODB_VECTOR_URI" ]]; then
		echo "mongodb backend requested, but MONGODB_VECTOR_URI is empty" >&2
		echo "Set MONGODB_VECTOR_URI to an Atlas or local Atlas Vector Search deployment, or remove mongodb from BACKENDS." >&2
		exit 1
	fi
	if contains_backend treedb_column_graph_quantized_only || contains_backend treedb_column_graph_quantized_rerank; then
		validate_treedb_quantized_codec "$TREEDB_QUANTIZED_CODEC" TREEDB_QUANTIZED_CODEC
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

start_milvus_if_needed() {
	if [[ -n "$MILVUS_URI" ]]; then
		return
	fi
	if [[ "$MILVUS_DOCKER" == "false" ]]; then
		echo "MILVUS_URI is required when MILVUS_DOCKER=false" >&2
		exit 1
	fi
	if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
		echo "docker compose is required for automatic Milvus startup; set MILVUS_URI to use an external service" >&2
		exit 1
	fi
	if ! command -v curl >/dev/null 2>&1; then
		echo "curl is required to fetch the pinned Milvus compose file" >&2
		exit 1
	fi
	mkdir -p "$MILVUS_STORAGE_DIR"
	local downloaded="$RUN_DIR/milvus-compose.downloaded.yml"
	MILVUS_COMPOSE_FILE="$RUN_DIR/milvus-compose.pinned.yml"
	curl -fsSL "$MILVUS_COMPOSE_URL" -o "$downloaded"
	"$VENV/bin/python" - <<'PY' "$downloaded" "$MILVUS_COMPOSE_SHA256"
import hashlib
import pathlib
import sys

actual = hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()
if actual != sys.argv[2]:
    raise SystemExit(f"Milvus compose SHA-256 {actual} does not match {sys.argv[2]}")
PY
	sed \
		-e '/^[[:space:]]*container_name:/d' \
		-e "s#quay.io/coreos/etcd:v3.5.25#$MILVUS_ETCD_IMAGE#" \
		-e "s#minio/minio:RELEASE.2024-12-18T13-15-44Z#$MILVUS_MINIO_IMAGE#" \
		-e "s#milvusdb/milvus:v2.6.20#$MILVUS_IMAGE#" \
		-e 's#- "9001:9001"#- "127.0.0.1::9001"#' \
		-e 's#- "9000:9000"#- "127.0.0.1::9000"#' \
		-e 's#- "19530:19530"#- "127.0.0.1::19530"#' \
		-e 's#- "9091:9091"#- "127.0.0.1::9091"#' \
		"$downloaded" | sed '/^networks:/,$d' >"$MILVUS_COMPOSE_FILE"
	for image in "$MILVUS_ETCD_IMAGE" "$MILVUS_MINIO_IMAGE" "$MILVUS_IMAGE"; do
		grep -Fq "image: $image" "$MILVUS_COMPOSE_FILE" || { echo "derived Milvus compose omits pinned image $image" >&2; exit 1; }
	done
	echo "starting pinned Milvus Standalone: $MILVUS_IMAGE"
	MILVUS_STARTED=true
	DOCKER_VOLUME_DIRECTORY="$MILVUS_STORAGE_DIR" docker compose -p "$MILVUS_PROJECT" -f "$MILVUS_COMPOSE_FILE" up -d
	local mapped
	mapped=$(DOCKER_VOLUME_DIRECTORY="$MILVUS_STORAGE_DIR" docker compose -p "$MILVUS_PROJECT" -f "$MILVUS_COMPOSE_FILE" port standalone 19530 | sed -nE 's/.*:([0-9]+)$/\1/p' | head -n 1)
	if [[ -z "$mapped" ]]; then
		echo "could not determine mapped Milvus port" >&2
		exit 1
	fi
	MILVUS_URI="http://127.0.0.1:${mapped}"
	"$VENV/bin/python" - <<'PY' "$MILVUS_URI" "$MILVUS_TOKEN"
import sys
import time
from pymilvus import MilvusClient

last = None
for _ in range(360):
    try:
        client = MilvusClient(uri=sys.argv[1], token=sys.argv[2])
        client.list_collections()
        client.close()
        raise SystemExit(0)
    except Exception as exc:  # noqa: BLE001
        last = exc
        time.sleep(0.5)
raise SystemExit(f"Milvus Standalone did not become ready: {last}")
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
if [[ -n "$TREEDB_VALIDATION_EXACT_SOURCE" ]]; then
	treedb_storage_args+=(-validation-exact-source "$TREEDB_VALIDATION_EXACT_SOURCE")
fi
treedb_profile_args=()
treedb_search_profile_backend_supported() {
	case "$1" in
		treedb_column_graph|treedb_column_graph_quantized_only|treedb_column_graph_quantized_rerank|treedb_column_graph_scalar_u8_quantized_only|treedb_column_graph_scalar_u8_quantized_rerank|treedb_column_graph_rabitq_1bit_quantized_only|treedb_column_graph_rabitq_1bit_quantized_rerank)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

treedb_profile_args_for_backend() {
	local backend="$1"
	treedb_profile_args=()
	if [[ -n "$TREEDB_SEARCH_PROFILE_DIR" ]] && treedb_search_profile_backend_supported "$backend"; then
		treedb_profile_args=(-search-profile-dir "$TREEDB_SEARCH_PROFILE_DIR/$backend")
	fi
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
- exported truth queries: \`$EXPORT_TRUTH_QUERIES\`
- validate docs: \`$VALIDATE_DOCS\`
- top_k: \`$TOP_K\`
- concurrency: \`$SEARCH_CONCURRENCY\`
- M / efConstruction / efSearch: \`$M / $EF_CONSTRUCTION / $EF_SEARCH\`
- TreeDB column_graph efSearch: \`$TREEDB_COLUMN_GRAPH_EF_SEARCH\`
- configured/effective minimum recall: \`$MIN_RECALL / $EFFECTIVE_MIN_RECALL\`
- TreeDB legacy quantized codec/index/rerank candidates: \`$TREEDB_QUANTIZED_CODEC / $TREEDB_QUANTIZED_INDEX_NAME / $TREEDB_QUANTIZED_RERANK_CANDIDATES\`
- TreeDB scalar_u8 quantized index: \`$TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME\`
- TreeDB RaBitQ quantized index/rerank candidates: \`$TREEDB_RABITQ_QUANTIZED_INDEX_NAME / $TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES\`
- TreeDB quantized-only/rerank configured/effective minimum recall: \`$TREEDB_QUANTIZED_ONLY_MIN_RECALL / $TREEDB_QUANTIZED_RERANK_MIN_RECALL\` / \`$EFFECTIVE_TREEDB_QUANTIZED_ONLY_MIN_RECALL / $EFFECTIVE_TREEDB_QUANTIZED_RERANK_MIN_RECALL\`
- TreeDB RaBitQ quantized-only/rerank configured/effective minimum recall: \`$TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL / $TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL\` / \`$EFFECTIVE_TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL / $EFFECTIVE_TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL\`
- TreeDB storage/validation knobs:
  - \`VALIDATE_DOCS=$VALIDATE_DOCS\`
  - \`TREEDB_COMPACT=${TREEDB_COMPACT:-<unset>}\`
  - \`TREEDB_COMPACT_SYNC_EACH_PHASE=${TREEDB_COMPACT_SYNC_EACH_PHASE:-<unset>}\`
  - \`TREEDB_VALUE_POINTER_THRESHOLD=${TREEDB_VALUE_POINTER_THRESHOLD:-<unset>}\`
  - \`TREEDB_LEAF_GENERATION_SEGMENT_TARGET=${TREEDB_LEAF_GENERATION_SEGMENT_TARGET:-<unset>}\`
  - \`TREEDB_REQUIRE_VALUE_LOG_BYTES=${TREEDB_REQUIRE_VALUE_LOG_BYTES:-<unset>}\`
  - \`TREEDB_REQUIRE_LEAF_VLOG_BYTES=${TREEDB_REQUIRE_LEAF_VLOG_BYTES:-<unset>}\`
  - \`TREEDB_VALIDATION_EXACT_SOURCE=${TREEDB_VALIDATION_EXACT_SOURCE:-<unset; demo default treedb>}\`
  - \`TREEDB_SEARCH_PROFILE_DIR=${TREEDB_SEARCH_PROFILE_DIR:-<unset>}\`
- Python packages: \`$NUMPY_PACKAGE\`, \`$VECTORLITE_PACKAGE\`, \`$PSYCOPG_PACKAGE\`, \`$PYMILVUS_PACKAGE\`
- PostgreSQL+pgvector image: \`$PGVECTOR_IMAGE\`
- Milvus compose SHA/images: \`$MILVUS_COMPOSE_SHA256\` / \`$MILVUS_IMAGE\` / \`$MILVUS_ETCD_IMAGE\` / \`$MILVUS_MINIO_IMAGE\`

This run compares persistent database-tier ANN search:

- TreeDB native persisted HNSW through \`cmd/treedb_vector_search_demo\`
- TreeDB column-store graph exact/default search through
  \`cmd/treedb_vector_search_demo -vector-index-strategy column_graph\`
- TreeDB column-store graph scalar_u8 and rabitq_1bit quantized modes through
  explicit \`-vector-query-mode quantized_only|quantized_rerank\`,
  \`-quantized-codec\`, and \`-quantized-index-name\` demo flags
- SQLite+Vectorlite HNSW through Python's \`sqlite3\` and the \`vectorlite-py\`
  loadable extension
- PostgreSQL+pgvector HNSW through a PostgreSQL server
- Milvus Standalone HNSW through the pinned upstream compose topology
- MongoDB Vector Search HNSW only when \`MONGODB_VECTOR_URI\` points at Atlas or
  local Atlas Vector Search

\`sqlite-vec\` is not included because upstream sqlite-vec's \`vec0\` table is
brute-force today; ANN support is still future work.

When \`TREEDB_SEARCH_PROFILE_DIR\` is set, the runner forwards it only to TreeDB
\`column_graph\` rows; the native \`treedb\` row does not emit search profile
artifacts.
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
	backend_pip_packages+=("$PSYCOPG_PACKAGE")
fi
if contains_backend milvus; then
	backend_pip_packages+=("$PYMILVUS_PACKAGE")
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
	-truth-queries "$EXPORT_TRUTH_QUERIES" \
	-top-k "$TOP_K" \
	-json >"$RUN_DIR/dataset_export.json"

result_args=()

if contains_backend treedb; then
	echo "running TreeDB benchmark"
	treedb_profile_args_for_backend treedb
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
		${treedb_storage_args[@]+"${treedb_storage_args[@]}"} \
		${treedb_profile_args[@]+"${treedb_profile_args[@]}"} \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$EF_SEARCH" \
		-min-recall "$EFFECTIVE_MIN_RECALL" \
		-json >"$RUN_DIR/treedb.json"
	result_args+=(--result "$RUN_DIR/treedb.json")
fi

if contains_backend treedb_column_graph; then
	echo "running TreeDB column-store graph benchmark"
	treedb_profile_args_for_backend treedb_column_graph
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
		${treedb_storage_args[@]+"${treedb_storage_args[@]}"} \
		${treedb_profile_args[@]+"${treedb_profile_args[@]}"} \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$TREEDB_COLUMN_GRAPH_EF_SEARCH" \
		-min-recall "$EFFECTIVE_MIN_RECALL" \
		-json >"$RUN_DIR/treedb_column_graph.json"
	result_args+=(--result "$RUN_DIR/treedb_column_graph.json")
fi

run_treedb_column_graph_quantized() {
	local backend="$1"
	local mode="$2"
	local codec="$3"
	local index_name="$4"
	local rerank_candidates="$5"
	local min_recall="$6"
	local output_name="$7"
	local quantized_args=(-vector-query-mode "$mode" -quantized-codec "$codec" -quantized-index-name "$index_name")
	if [[ "$mode" == "quantized_rerank" ]]; then
		quantized_args+=(-quantized-rerank-candidates "$rerank_candidates")
	fi
	echo "running TreeDB column-store graph $codec $mode benchmark"
	treedb_profile_args_for_backend "$backend"
	GOWORK=off go run ./cmd/treedb_vector_search_demo \
		-matrix=false \
		-vector-index-strategy column_graph \
		"${quantized_args[@]}" \
		-dataset-dir "$RUN_DIR/dataset" \
		-dir "$RUN_DIR/$output_name" \
		-keep-dir \
		-docs "$DOCS" \
		-dims "$DIMS" \
		-queries "$QUERIES" \
		-search-concurrency "$SEARCH_CONCURRENCY" \
		-validate-queries "$VALIDATE_QUERIES" \
		${treedb_storage_args[@]+"${treedb_storage_args[@]}"} \
		${treedb_profile_args[@]+"${treedb_profile_args[@]}"} \
		-top-k "$TOP_K" \
		-m "$M" \
		-ef-construction "$EF_CONSTRUCTION" \
		-ef-search "$TREEDB_COLUMN_GRAPH_EF_SEARCH" \
		-min-recall "$min_recall" \
		-json >"$RUN_DIR/$output_name.json"
	result_args+=(--result "$RUN_DIR/$output_name.json")
}

if contains_backend treedb_column_graph_quantized_only; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_quantized_only \
		quantized_only \
		"$TREEDB_QUANTIZED_CODEC" \
		"$TREEDB_QUANTIZED_INDEX_NAME" \
		"$TREEDB_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_QUANTIZED_ONLY_MIN_RECALL" \
		treedb_column_graph_quantized_only
fi

if contains_backend treedb_column_graph_quantized_rerank; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_quantized_rerank \
		quantized_rerank \
		"$TREEDB_QUANTIZED_CODEC" \
		"$TREEDB_QUANTIZED_INDEX_NAME" \
		"$TREEDB_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_QUANTIZED_RERANK_MIN_RECALL" \
		treedb_column_graph_quantized_rerank
fi

if contains_backend treedb_column_graph_scalar_u8_quantized_only; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_scalar_u8_quantized_only \
		quantized_only \
		scalar_u8 \
		"$TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME" \
		"$TREEDB_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_QUANTIZED_ONLY_MIN_RECALL" \
		treedb_column_graph_scalar_u8_quantized_only
fi

if contains_backend treedb_column_graph_scalar_u8_quantized_rerank; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_scalar_u8_quantized_rerank \
		quantized_rerank \
		scalar_u8 \
		"$TREEDB_SCALAR_U8_QUANTIZED_INDEX_NAME" \
		"$TREEDB_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_QUANTIZED_RERANK_MIN_RECALL" \
		treedb_column_graph_scalar_u8_quantized_rerank
fi

if contains_backend treedb_column_graph_rabitq_1bit_quantized_only; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_rabitq_1bit_quantized_only \
		quantized_only \
		rabitq_1bit \
		"$TREEDB_RABITQ_QUANTIZED_INDEX_NAME" \
		"$TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL" \
		treedb_column_graph_rabitq_1bit_quantized_only
fi

if contains_backend treedb_column_graph_rabitq_1bit_quantized_rerank; then
	run_treedb_column_graph_quantized \
		treedb_column_graph_rabitq_1bit_quantized_rerank \
		quantized_rerank \
		rabitq_1bit \
		"$TREEDB_RABITQ_QUANTIZED_INDEX_NAME" \
		"$TREEDB_RABITQ_QUANTIZED_RERANK_CANDIDATES" \
		"$EFFECTIVE_TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL" \
		treedb_column_graph_rabitq_1bit_quantized_rerank
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
		--min-recall "$EFFECTIVE_MIN_RECALL" >"$RUN_DIR/vectorlite.stdout.json"
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
		--min-recall "$EFFECTIVE_MIN_RECALL"
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

if contains_backend milvus; then
	start_milvus_if_needed
	echo "running Milvus Standalone benchmark"
	milvus_args=(
		--dataset-dir "$RUN_DIR/dataset"
		--uri "$MILVUS_URI"
		--token "$MILVUS_TOKEN"
		--collection "$MILVUS_COLLECTION"
		--index "$MILVUS_INDEX"
		--output "$RUN_DIR/milvus.json"
		--queries "$QUERIES"
		--validate-queries "$VALIDATE_QUERIES"
		--top-k "$TOP_K"
		--search-concurrency "$SEARCH_CONCURRENCY"
		--m "$M"
		--ef-construction "$EF_CONSTRUCTION"
		--ef-search "$EF_SEARCH"
		--min-recall "$EFFECTIVE_MIN_RECALL"
	)
	if [[ "$MILVUS_STARTED" == "true" || "$MILVUS_STORAGE_DIR_EXPLICIT" == "true" ]]; then
		milvus_args+=(--storage-dir "$MILVUS_STORAGE_DIR")
	fi
	if [[ "$MILVUS_DROP_COLLECTION_AFTER" == "true" ]]; then
		milvus_args+=(--drop-collection-after)
	fi
	if [[ "$MILVUS_ALLOW_DROP_COLLECTION" == "true" ]]; then
		milvus_args+=(--allow-drop-collection)
	fi
	"$VENV/bin/python" benchmarks/vector_db_compare/milvus_bench.py "${milvus_args[@]}" >"$RUN_DIR/milvus.stdout.json"
	result_args+=(--result "$RUN_DIR/milvus.json")
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
		--min-recall "$EFFECTIVE_MIN_RECALL" >"$RUN_DIR/mongodb.stdout.json"
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
