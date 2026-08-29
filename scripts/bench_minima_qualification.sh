#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

MODE=${MODE:-representative}
if [[ -z "${RUN_DIR:-}" ]]; then
	RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gomap_minima_qualification_XXXXXXXXXX")
fi
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
TREEDB_OPERATION_TIMEOUT=${TREEDB_OPERATION_TIMEOUT:-120}
TREEDB_STARTUP_TIMEOUT=${TREEDB_STARTUP_TIMEOUT:-3600}
TREEDB_DIAGNOSTICS_DIR=${TREEDB_DIAGNOSTICS_DIR:-}
TREEDB_DIAGNOSTICS_URL=${TREEDB_DIAGNOSTICS_URL:-http://127.0.0.1:17121}
TREEDB_DIAGNOSTIC_SLOW_SECONDS=${TREEDB_DIAGNOSTIC_SLOW_SECONDS:-30}
TREEDB_DIAGNOSTIC_PROFILE_SECONDS=${TREEDB_DIAGNOSTIC_PROFILE_SECONDS:-5}
TREEDB_DIAGNOSTIC_CAPTURE_TIMEOUT=${TREEDB_DIAGNOSTIC_CAPTURE_TIMEOUT:-10}
TREEDB_DIAGNOSTIC_RESUME_SCENARIO=${TREEDB_DIAGNOSTIC_RESUME_SCENARIO:-}
TREEDB_DIAGNOSTIC_RESUME_START=${TREEDB_DIAGNOSTIC_RESUME_START:-}
RECOMMENDATION=${RECOMMENDATION:-ready_with_alpha_limitations}
PYTHON=${PYTHON:-python3}

treedb_diagnostic_args=()
if [[ -n "$TREEDB_DIAGNOSTICS_DIR" ]]; then
	treedb_diagnostic_args+=(
		--diagnostics-dir "$TREEDB_DIAGNOSTICS_DIR"
		--diagnostics-url "$TREEDB_DIAGNOSTICS_URL"
		--diagnostic-slow-seconds "$TREEDB_DIAGNOSTIC_SLOW_SECONDS"
		--diagnostic-profile-seconds "$TREEDB_DIAGNOSTIC_PROFILE_SECONDS"
		--diagnostic-capture-timeout "$TREEDB_DIAGNOSTIC_CAPTURE_TIMEOUT"
	)
fi

mkdir -p "$RUN_DIR/bin"

case "$MODE" in
representative)
	;;
diagnostic-resume)
	if [[ -z "$TREEDB_DIAGNOSTICS_DIR" || -z "$TREEDB_DIAGNOSTIC_RESUME_SCENARIO" || -z "$TREEDB_DIAGNOSTIC_RESUME_START" ]]; then
		printf '%s\n' 'MODE=diagnostic-resume requires TREEDB_DIAGNOSTICS_DIR, TREEDB_DIAGNOSTIC_RESUME_SCENARIO, and TREEDB_DIAGNOSTIC_RESUME_START.' >&2
		exit 2
	fi
	treedb_diagnostic_args+=(
		--diagnostic-resume-scenario "$TREEDB_DIAGNOSTIC_RESUME_SCENARIO"
		--diagnostic-resume-start "$TREEDB_DIAGNOSTIC_RESUME_START"
	)
	;;
small)
	printf '%s\n' 'MODE=small runs the real TreeDB small-scenario lifecycle and emits nonpassing partial evidence.' >&2
	go build -o "$RUN_DIR/bin/treedb-document-service" ./cmd/treedb-document-service
	go build -o "$RUN_DIR/bin/treedb-rag-benchmark" ./TreeDB/cmd/treedb_rag_benchmark
	"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -dump-minima-manifest "$MANIFEST_PATH"
	treedb_status=0
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
		--operation-timeout "$TREEDB_OPERATION_TIMEOUT" \
		--startup-timeout "$TREEDB_STARTUP_TIMEOUT" \
		--ef-search "$TREEDB_EF_SEARCH" \
		${treedb_diagnostic_args[@]+"${treedb_diagnostic_args[@]}"} ||
		treedb_status=$?
	"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -validate-minima-artifact "$TREEDB_EVIDENCE"
	printf 'small manifest: %s\nvalidated partial TreeDB evidence: %s\n' "$MANIFEST_PATH" "$TREEDB_EVIDENCE"
	exit "$treedb_status"
	;;
*)
	printf 'unsupported MODE=%s (use small, representative, or diagnostic-resume)\n' "$MODE" >&2
	exit 2
	;;
esac

# The representative workload is frozen at 500,000 rows per representative
# scenario and 1,024 timed queries. Changing those values requires a new
# preflight manifest and hashes rather than an environment-only override.
go build -o "$RUN_DIR/bin/treedb-document-service" ./cmd/treedb-document-service
go build -o "$RUN_DIR/bin/treedb-rag-benchmark" ./TreeDB/cmd/treedb_rag_benchmark
"$RUN_DIR/bin/treedb-rag-benchmark" -workload=minima -dump-minima-manifest "$MANIFEST_PATH"

treedb_status=0
PYTHONPATH=clients/python/treedb_client/src "$PYTHON" \
	benchmarks/vector_db_compare/minima_treedb_runner.py \
	--manifest "$MANIFEST_PATH" \
	--output "$TREEDB_EVIDENCE" \
	--service-bin "$RUN_DIR/bin/treedb-document-service" \
	--url "$TREEDB_URL" \
	--data-dir "$TREEDB_DATA_DIR" \
	--collection "$TREEDB_COLLECTION" \
	--profile "$TREEDB_PROFILE" \
	--ef-search "$TREEDB_EF_SEARCH" \
	--operation-timeout "$TREEDB_OPERATION_TIMEOUT" \
	--startup-timeout "$TREEDB_STARTUP_TIMEOUT" \
	${treedb_diagnostic_args[@]+"${treedb_diagnostic_args[@]}"} ||
	treedb_status=$?

if [[ "$MODE" == "diagnostic-resume" ]]; then
	printf 'diagnostic manifest: %s\nnonqualifying TreeDB evidence: %s\ndiagnostic profiles: %s\n' \
		"$MANIFEST_PATH" "$TREEDB_EVIDENCE" "$TREEDB_DIAGNOSTICS_DIR"
	exit "$treedb_status"
fi

qdrant_status=0
if [[ "$(uname -s)" == "Darwin" && -z "${QDRANT_BIN:-}" && -z "${QDRANT_SERVER_PID:-}" ]]; then
	printf '%s\n' 'Representative qualification on Darwin requires QDRANT_BIN or an external QDRANT_SERVER_PID so RSS/CPU evidence is available.' >&2
	qdrant_status=2
else
	RUN_DIR="$RUN_DIR/qdrant" \
	MANIFEST_PATH="$MANIFEST_PATH" \
	OUTPUT_PATH="$QDRANT_EVIDENCE" \
		scripts/bench_minima_qdrant.sh ||
		qdrant_status=$?
fi

comparator_status=0
if [[ -f "$TREEDB_EVIDENCE" && -f "$QDRANT_EVIDENCE" ]]; then
	"$RUN_DIR/bin/treedb-rag-benchmark" \
		-workload=minima \
		-minima-treedb-evidence "$TREEDB_EVIDENCE" \
		-minima-qdrant-evidence "$QDRANT_EVIDENCE" \
		-minima-output "$OUTPUT_PATH" \
		-minima-report "$REPORT_PATH" \
		-minima-recommendation "$RECOMMENDATION" ||
		comparator_status=$?
else
	comparator_status=2
	printf 'comparator not evaluated: evidence exists TreeDB=%s Qdrant=%s\n' \
		"$([[ -f "$TREEDB_EVIDENCE" ]] && printf true || printf false)" \
		"$([[ -f "$QDRANT_EVIDENCE" ]] && printf true || printf false)" >&2
fi

printf 'manifest: %s\nTreeDB evidence: %s\nQdrant evidence: %s\nqualification artifact: %s\nreport: %s\n' \
	"$MANIFEST_PATH" "$TREEDB_EVIDENCE" "$QDRANT_EVIDENCE" "$OUTPUT_PATH" "$REPORT_PATH"

if ((treedb_status != 0 || qdrant_status != 0 || comparator_status != 0)); then
	printf 'qualification failed: TreeDB=%d Qdrant=%d comparator=%d\n' \
		"$treedb_status" "$qdrant_status" "$comparator_status" >&2
	exit 1
fi
