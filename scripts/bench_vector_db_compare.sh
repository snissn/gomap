#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_vector_db_compare_$(date +%Y%m%d_%H%M%S)}"
PYTHON="${PYTHON:-python3}"
VENV="${VENV:-$RUN_DIR/venv}"
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

mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB vs SQLite+Vectorlite Vector DB Comparison

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
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

\`sqlite-vec\` is not included because upstream sqlite-vec's \`vec0\` table is
brute-force today; ANN support is still future work.
EOF

echo "run dir: $RUN_DIR"
echo "creating Python environment: $VENV"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q --only-binary=:all: numpy vectorlite-py

echo "exporting TreeDB dataset"
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
	-out "$RUN_DIR/dataset" \
	-docs "$DOCS" \
	-dims "$DIMS" \
	-queries "$QUERIES" \
	-top-k "$TOP_K" \
	-json >"$RUN_DIR/dataset_export.json"

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

echo "rendering comparison"
"$VENV/bin/python" benchmarks/vector_db_compare/summarize.py \
	--treedb "$RUN_DIR/treedb.json" \
	--vectorlite "$RUN_DIR/vectorlite.json" \
	--output "$RUN_DIR/comparison.md"

echo "wrote $RUN_DIR/comparison.md"
