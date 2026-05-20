#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${RUN_DIR:-}" ]]; then
  RUN_DIR="$(mktemp -d /tmp/gomap_column_graph_main_path_XXXXXX)"
elif [[ -e "${RUN_DIR}" ]] && [[ ! -d "${RUN_DIR}" ]]; then
  printf 'RUN_DIR exists and is not a directory: %s\n' "${RUN_DIR}" >&2
  exit 1
elif [[ -d "${RUN_DIR}" ]] && [[ -n "$(find "${RUN_DIR}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  printf 'RUN_DIR exists and is not empty: %s\n' "${RUN_DIR}" >&2
  exit 1
else
  mkdir -p "${RUN_DIR}"
fi
: "${TREEDB_VECTOR_BENCH_DOCS:=10000}"
: "${TREEDB_VECTOR_BENCH_DIMS:=64}"
: "${BENCHTIME:=500ms}"
: "${COUNT:=3}"
: "${BENCH_REGEX:=BenchmarkCollectionVectorIndexColumnGraphMainPath}"

cat > "${RUN_DIR}/README.md" <<EOF
# TreeDB Column Graph Main-Path Benchmark

Command:

~~~sh
TREEDB_VECTOR_BENCH_DOCS=${TREEDB_VECTOR_BENCH_DOCS} \\
TREEDB_VECTOR_BENCH_DIMS=${TREEDB_VECTOR_BENCH_DIMS} \\
GOWORK=off go test ./TreeDB/collections \\
  -run '^$' \\
  -bench "${BENCH_REGEX}" \\
  -benchmem \\
  -benchtime="${BENCHTIME}" \\
  -count="${COUNT}"
~~~

This benchmark uses synthetic vectors but exercises the collection product path:
real column-store assets, manifest/root reopen, decode into an in-memory
ColumnVectorGraph, in-memory graph search, and optional public document
materialization variants.

It does not measure column-store-native search over granules, marks, reader
caches, or decoded-block caches. For the opt-in public Deep1B dataset path, use
scripts/bench_column_vector_deep1b.sh.
EOF

cd "${ROOT_DIR}"
TREEDB_VECTOR_BENCH_DOCS="${TREEDB_VECTOR_BENCH_DOCS}" \
TREEDB_VECTOR_BENCH_DIMS="${TREEDB_VECTOR_BENCH_DIMS}" \
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench "${BENCH_REGEX}" \
  -benchmem \
  -benchtime="${BENCHTIME}" \
  -count="${COUNT}" \
  | tee "${RUN_DIR}/column_graph_main_path.txt"

printf 'wrote %s\n' "${RUN_DIR}"
