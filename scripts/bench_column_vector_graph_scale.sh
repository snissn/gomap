#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_column_vector_graph_scale_$(date +%Y%m%d_%H%M%S)}"
BENCHTIME="${BENCHTIME:-500ms}"
COUNT="${COUNT:-5}"
RUN_1M="${RUN_1M:-false}"
RUN_USEARCH="${RUN_USEARCH:-false}"
RUN_VECTOR_DB_COMPARE="${RUN_VECTOR_DB_COMPARE:-false}"

mkdir -p "$RUN_DIR"

if [[ "$RUN_1M" == "true" ]]; then
	column_regex='BenchmarkColumnVectorGraphSearchCosineScale'
else
	column_regex='BenchmarkColumnVectorGraphSearchCosineScale/rows_100k'
fi

echo "run dir: $RUN_DIR"
echo "column graph benchmark regex: $column_regex"
GOWORK=off go test ./TreeDB/collections \
	-run '^$' \
	-bench "$column_regex" \
	-benchmem \
	-benchtime "$BENCHTIME" \
	-count "$COUNT" | tee "$RUN_DIR/column_vector_graph_scale.txt"

if [[ "$RUN_USEARCH" == "true" ]]; then
	USEARCH_DOCS="${USEARCH_DOCS:-100000}"
	USEARCH_DIMS="${USEARCH_DIMS:-128}"
	echo "running usearch comparator docs=$USEARCH_DOCS dims=$USEARCH_DIMS"
	TREEDB_VECTOR_BENCH_DOCS="$USEARCH_DOCS" \
		TREEDB_VECTOR_BENCH_DIMS="$USEARCH_DIMS" \
		GOWORK=off go test -tags usearch_bench ./TreeDB/collections \
		-run '^$' \
		-bench 'BenchmarkCollectionVectorUSearchBaseline$' \
		-benchmem \
		-benchtime "$BENCHTIME" \
		-count "$COUNT" | tee "$RUN_DIR/usearch_baseline.txt"
fi

if [[ "$RUN_VECTOR_DB_COMPARE" == "true" ]]; then
	COMPARE_DOCS="${COMPARE_DOCS:-100000}"
	COMPARE_DIMS="${COMPARE_DIMS:-128}"
	COMPARE_QUERIES="${COMPARE_QUERIES:-50000}"
	COMPARE_VALIDATE_QUERIES="${COMPARE_VALIDATE_QUERIES:-64}"
	COMPARE_BACKENDS="${COMPARE_BACKENDS:-treedb,vectorlite}"
	echo "running vector-db compare docs=$COMPARE_DOCS dims=$COMPARE_DIMS backends=$COMPARE_BACKENDS"
	RUN_DIR="$RUN_DIR/vector_db_compare" \
		DOCS="$COMPARE_DOCS" \
		DIMS="$COMPARE_DIMS" \
		QUERIES="$COMPARE_QUERIES" \
		VALIDATE_QUERIES="$COMPARE_VALIDATE_QUERIES" \
		BACKENDS="$COMPARE_BACKENDS" \
		scripts/bench_vector_db_compare.sh
fi
