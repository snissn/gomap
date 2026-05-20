#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${RUN_DIR:="/tmp/gomap_column_graph_main_path_$(date +%Y%m%d_%H%M%S)"}"
: "${TREEDB_VECTOR_BENCH_DOCS:=10000}"
: "${TREEDB_VECTOR_BENCH_DIMS:=64}"
: "${BENCHTIME:=500ms}"
: "${COUNT:=3}"
: "${BENCH_REGEX:=BenchmarkCollectionVectorIndexColumnGraphMainPath}"

mkdir -p "${RUN_DIR}"

cat > "${RUN_DIR}/README.md" <<EOF
# TreeDB Column Graph Main-Path Benchmark

Command:

\`\`\`sh
TREEDB_VECTOR_BENCH_DOCS=${TREEDB_VECTOR_BENCH_DOCS} \\
TREEDB_VECTOR_BENCH_DIMS=${TREEDB_VECTOR_BENCH_DIMS} \\
GOWORK=off go test ./TreeDB/collections \\
  -run '^$' \\
  -bench '${BENCH_REGEX}' \\
  -benchmem \\
  -benchtime '${BENCHTIME}' \\
  -count '${COUNT}'
\`\`\`

This benchmark uses synthetic vectors but exercises the collection product path:
real column-store assets, manifest/root reopen, ColumnVectorGraph load/search,
and optional public document materialization variants. For the opt-in public
Deep1B dataset path, use scripts/bench_column_vector_deep1b.sh.
EOF

cd "${ROOT_DIR}"
TREEDB_VECTOR_BENCH_DOCS="${TREEDB_VECTOR_BENCH_DOCS}" \
TREEDB_VECTOR_BENCH_DIMS="${TREEDB_VECTOR_BENCH_DIMS}" \
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench "${BENCH_REGEX}" \
  -benchmem \
  -benchtime "${BENCHTIME}" \
  -count "${COUNT}" \
  | tee "${RUN_DIR}/column_graph_main_path.txt"

printf 'wrote %s\n' "${RUN_DIR}"
