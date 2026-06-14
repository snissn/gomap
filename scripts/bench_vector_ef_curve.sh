#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_vector_ef_curve_$(date +%Y%m%d_%H%M%S)}"
EF_VALUES="${EF_VALUES:-8,12,16,24,32,48,64,96,128}"
PYTHON="${PYTHON:-python3}"

# Curve runs intentionally observe recall at low ef values instead of failing
# early because a point is below the normal full-vector recall gate.
MIN_RECALL="${MIN_RECALL:-0}"
TREEDB_QUANTIZED_MIN_RECALL="${TREEDB_QUANTIZED_MIN_RECALL:-0}"
TREEDB_QUANTIZED_ONLY_MIN_RECALL="${TREEDB_QUANTIZED_ONLY_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_QUANTIZED_RERANK_MIN_RECALL="${TREEDB_QUANTIZED_RERANK_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_MIN_RECALL:-$TREEDB_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL:-$TREEDB_RABITQ_QUANTIZED_MIN_RECALL}"
TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL="${TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL:-$TREEDB_RABITQ_QUANTIZED_MIN_RECALL}"

mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/README.md" <<EOF
# Vector EF Curve

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- ef values: \`$EF_VALUES\`
- backends: \`${BACKENDS:-treedb,vectorlite}\`
- min recall: \`$MIN_RECALL\`
- TreeDB quantized min recall: \`$TREEDB_QUANTIZED_MIN_RECALL\`

This is a normal \`scripts/bench_vector_db_compare.sh\` run with
\`EF_SEARCH_VALUES=$EF_VALUES\`, followed by curve collection.
EOF

echo "running ef_search curve -> $RUN_DIR"
RUN_DIR="$RUN_DIR" \
EF_SEARCH_VALUES="$EF_VALUES" \
MIN_RECALL="$MIN_RECALL" \
TREEDB_QUANTIZED_MIN_RECALL="$TREEDB_QUANTIZED_MIN_RECALL" \
TREEDB_QUANTIZED_ONLY_MIN_RECALL="$TREEDB_QUANTIZED_ONLY_MIN_RECALL" \
TREEDB_QUANTIZED_RERANK_MIN_RECALL="$TREEDB_QUANTIZED_RERANK_MIN_RECALL" \
TREEDB_RABITQ_QUANTIZED_MIN_RECALL="$TREEDB_RABITQ_QUANTIZED_MIN_RECALL" \
TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL="$TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL" \
TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL="$TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL" \
	scripts/bench_vector_db_compare.sh

echo "collecting curve"
"$PYTHON" benchmarks/vector_db_compare/collect_ef_curve.py \
	--run-dir "$RUN_DIR" \
	--csv "$RUN_DIR/curve.csv" \
	--markdown "$RUN_DIR/curve.md"

echo "wrote $RUN_DIR/curve.csv"
echo "wrote $RUN_DIR/curve.md"
