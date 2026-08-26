#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

MODE=${MODE:-representative}
RUN_DIR=${RUN_DIR:-/tmp/gomap_minima_qualification_$(date +%Y%m%d_%H%M%S)_${RANDOM}_$$}
MANIFEST_PATH=${MANIFEST_PATH:-$RUN_DIR/minima_manifest.json}
TREEDB_EVIDENCE=${TREEDB_EVIDENCE:-$RUN_DIR/treedb_backend.json}
QDRANT_EVIDENCE=${QDRANT_EVIDENCE:-$RUN_DIR/qdrant_backend.json}
OUTPUT_PATH=${OUTPUT_PATH:-$RUN_DIR/minima_qualification.json}
REPORT_PATH=${REPORT_PATH:-$RUN_DIR/minima_qualification.md}
TREEDB_DATA_DIR=${TREEDB_DATA_DIR:-$RUN_DIR/treedb-data}
TREEDB_URL=${TREEDB_URL:-http://127.0.0.1:17120}
TREEDB_COLLECTION=${TREEDB_COLLECTION:-gomap_minima_${RANDOM}_$$}
TREEDB_PROFILE=${TREEDB_PROFILE:-command_wal_durable}
TREEDB_EF_SEARCH=${TREEDB_EF_SEARCH:-128}
RECOMMENDATION=${RECOMMENDATION:-ready_with_alpha_limitations}
PYTHON=${PYTHON:-python3}

mkdir -p "$RUN_DIR/bin"

case "$MODE" in
representative)
	;;
small)
	printf '%s\n' 'MODE=small runs the real TreeDB small-scenario lifecycle and emits nonpassing partial evidence.' >&2
	go build -o "$RUN_DIR/bin/treedb-document-service" ./cmd/treedb-document-service
	go build -o "$RUN_DIR/bin/treedb-rag-benchmark" ./TreeDB/cmd/treedb_rag_benchmark
	"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -dump-minima-manifest "$MANIFEST_PATH"
	PYTHONPATH=clients/python/treedb_client/src "$PYTHON" \
		benchmarks/vector_db_compare/minima_treedb_runner.py \
		--small \
		--manifest "$MANIFEST_PATH" \
		--output "$TREEDB_EVIDENCE" \
		--service-bin "$RUN_DIR/bin/treedb-document-service" \
		--url "$TREEDB_URL" \
		--data-dir "$TREEDB_DATA_DIR" \
		--collection "$TREEDB_COLLECTION" \
		--profile "$TREEDB_PROFILE" \
		--ef-search "$TREEDB_EF_SEARCH"
	"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -validate-minima-artifact "$TREEDB_EVIDENCE"
	printf 'small manifest: %s\nvalidated partial TreeDB evidence: %s\n' "$MANIFEST_PATH" "$TREEDB_EVIDENCE"
	exit 0
	;;
*)
	printf 'unsupported MODE=%s (use small or representative)\n' "$MODE" >&2
	exit 2
	;;
esac

# The representative workload is frozen at 500,000 rows per representative
# scenario and 1,024 timed queries. Changing those values requires a new
# preflight manifest and hashes rather than an environment-only override.
go build -o "$RUN_DIR/bin/treedb-document-service" ./cmd/treedb-document-service
go build -o "$RUN_DIR/bin/treedb-rag-benchmark" ./TreeDB/cmd/treedb_rag_benchmark
"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -dump-minima-manifest "$MANIFEST_PATH"

PYTHONPATH=clients/python/treedb_client/src "$PYTHON" \
	benchmarks/vector_db_compare/minima_treedb_runner.py \
	--manifest "$MANIFEST_PATH" \
	--output "$TREEDB_EVIDENCE" \
	--service-bin "$RUN_DIR/bin/treedb-document-service" \
	--url "$TREEDB_URL" \
	--data-dir "$TREEDB_DATA_DIR" \
	--collection "$TREEDB_COLLECTION" \
	--profile "$TREEDB_PROFILE" \
	--ef-search "$TREEDB_EF_SEARCH"

if [[ "$(uname -s)" == "Darwin" && -z "${QDRANT_BIN:-}" && -z "${QDRANT_SERVER_PID:-}" ]]; then
	printf '%s\n' 'Representative qualification on Darwin requires QDRANT_BIN or an external QDRANT_SERVER_PID so RSS/CPU evidence is available.' >&2
	exit 2
fi

RUN_DIR="$RUN_DIR/qdrant" \
MANIFEST_PATH="$MANIFEST_PATH" \
OUTPUT_PATH="$QDRANT_EVIDENCE" \
	scripts/bench_minima_qdrant.sh

"$RUN_DIR/bin/treedb-rag-benchmark" \
	-workload=minima \
	-minima-treedb-evidence "$TREEDB_EVIDENCE" \
	-minima-qdrant-evidence "$QDRANT_EVIDENCE" \
	-minima-output "$OUTPUT_PATH" \
	-minima-report "$REPORT_PATH" \
	-minima-recommendation "$RECOMMENDATION"

printf 'manifest: %s\nTreeDB evidence: %s\nQdrant evidence: %s\nvalidated artifact: %s\nreport: %s\n' \
	"$MANIFEST_PATH" "$TREEDB_EVIDENCE" "$QDRANT_EVIDENCE" "$OUTPUT_PATH" "$REPORT_PATH"
