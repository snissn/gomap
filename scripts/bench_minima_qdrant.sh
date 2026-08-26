#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_minima_qdrant_$(date +%Y%m%d_%H%M%S)_${RANDOM}_$$}"
PYTHON="${PYTHON:-python3}"
VENV="${VENV:-$RUN_DIR/venv}"
MANIFEST_PATH="${MANIFEST_PATH:-$RUN_DIR/minima_manifest.json}"
OUTPUT_PATH="${OUTPUT_PATH:-$RUN_DIR/qdrant_backend.json}"
QDRANT_URL="${QDRANT_URL:-}"
QDRANT_BIN="${QDRANT_BIN:-}"
QDRANT_IMAGE="${QDRANT_IMAGE:-qdrant/qdrant:v1.19.0@sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc}"
QDRANT_COLLECTION="${QDRANT_COLLECTION:-gomap_minima_${RANDOM}_$$}"
QDRANT_STORAGE_PATH="${QDRANT_STORAGE_PATH:-$RUN_DIR/qdrant-storage}"
QDRANT_READY_TIMEOUT="${QDRANT_READY_TIMEOUT:-120}"
QDRANT_OPERATION_TIMEOUT="${QDRANT_OPERATION_TIMEOUT:-120}"
QDRANT_OPTIMIZER_TIMEOUT="${QDRANT_OPTIMIZER_TIMEOUT:-600}"
QDRANT_API_KEY="${QDRANT_API_KEY:-}"
ALLOW_DROP="${ALLOW_DROP:-false}"
QDRANT_PID=""
QDRANT_SERVER_PID="${QDRANT_SERVER_PID:-}"
QDRANT_CONTAINER=""
DEPLOYMENT=""

cleanup() {
	if [[ -n "$QDRANT_PID" ]]; then
		kill "$QDRANT_PID" >/dev/null 2>&1 || true
		wait "$QDRANT_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$QDRANT_CONTAINER" ]]; then
		docker rm -f "$QDRANT_CONTAINER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

case "$ALLOW_DROP" in
	true|false) ;;
	*) echo "ALLOW_DROP must be true or false" >&2; exit 2 ;;
esac
if [[ -n "$QDRANT_URL" && -n "$QDRANT_BIN" ]]; then
	echo "Set only one of QDRANT_URL or QDRANT_BIN" >&2
	exit 2
fi

mkdir -p "$RUN_DIR" "$QDRANT_STORAGE_PATH" "$(dirname "$MANIFEST_PATH")" "$(dirname "$OUTPUT_PATH")"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install --disable-pip-version-check "qdrant-client==1.19.0"

# The Go owner emits the compact fixture once. Both backend runners consume these bytes.
go run ./TreeDB/cmd/treedb_rag_benchmark \
	-workload=minima \
	-dump-minima-manifest "$MANIFEST_PATH"

if [[ -n "$QDRANT_URL" ]]; then
	DEPLOYMENT=external
elif [[ -n "$QDRANT_BIN" ]]; then
	if [[ ! -x "$QDRANT_BIN" ]]; then
		echo "QDRANT_BIN is not an executable standalone Qdrant binary: $QDRANT_BIN" >&2
		exit 2
	fi
	QDRANT_PORT=$("$VENV/bin/python" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
	QDRANT_URL="http://127.0.0.1:$QDRANT_PORT"
	DEPLOYMENT=standalone
	QDRANT__SERVICE__HOST=127.0.0.1 \
	QDRANT__SERVICE__HTTP_PORT="$QDRANT_PORT" \
	QDRANT__STORAGE__STORAGE_PATH="$QDRANT_STORAGE_PATH" \
		"$QDRANT_BIN" >"$RUN_DIR/qdrant.log" 2>&1 &
	QDRANT_PID=$!
	QDRANT_SERVER_PID="$QDRANT_PID"
else
	if [[ ! "$QDRANT_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]]; then
		echo "QDRANT_IMAGE must be digest-pinned, got: $QDRANT_IMAGE" >&2
		exit 2
	fi
	if ! command -v docker >/dev/null 2>&1; then
		echo "No QDRANT_URL or QDRANT_BIN was supplied, and Docker is unavailable; refusing backend substitution" >&2
		exit 2
	fi
	QDRANT_PORT=$("$VENV/bin/python" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
	QDRANT_URL="http://127.0.0.1:$QDRANT_PORT"
	DEPLOYMENT=docker
	QDRANT_CONTAINER="gomap-minima-qdrant-${RANDOM}-$$"
	docker run -d --rm \
		--name "$QDRANT_CONTAINER" \
		-p "127.0.0.1:${QDRANT_PORT}:6333" \
		-v "$QDRANT_STORAGE_PATH:/qdrant/storage" \
		"$QDRANT_IMAGE" >/dev/null
	if [[ "$(uname -s)" == "Linux" ]]; then
		QDRANT_SERVER_PID=$(docker inspect --format '{{.State.Pid}}' "$QDRANT_CONTAINER")
	fi
fi

QDRANT_API_KEY="$QDRANT_API_KEY" "$VENV/bin/python" - "$QDRANT_URL" "$QDRANT_READY_TIMEOUT" <<'PY'
import json
import os
import sys
import time
import urllib.request

url, timeout = sys.argv[1].rstrip("/") + "/", float(sys.argv[2])
deadline, last = time.monotonic() + timeout, None
while time.monotonic() < deadline:
    try:
        request = urllib.request.Request(url)
        if os.environ.get("QDRANT_API_KEY"):
            request.add_header("api-key", os.environ["QDRANT_API_KEY"])
        with urllib.request.urlopen(request, timeout=2) as response:
            value = json.load(response)
        if value.get("version") != "1.19.0":
            raise SystemExit(f"Qdrant server must be exactly 1.19.0, got {value!r}")
        raise SystemExit(0)
    except SystemExit:
        raise
    except Exception as exc:  # readiness evidence is bounded and surfaced on failure
        last = exc
        time.sleep(0.25)
raise SystemExit(f"Qdrant did not become ready within {timeout}s: {last}")
PY

RUNNER_ARGS=(
	--manifest "$MANIFEST_PATH"
	--output "$OUTPUT_PATH"
	--url "$QDRANT_URL"
	--collection "$QDRANT_COLLECTION"
	--operation-timeout "$QDRANT_OPERATION_TIMEOUT"
	--optimizer-timeout "$QDRANT_OPTIMIZER_TIMEOUT"
	--deployment "$DEPLOYMENT"
)
if [[ "$ALLOW_DROP" == "true" ]]; then
	RUNNER_ARGS+=(--allow-drop)
fi
if [[ "$DEPLOYMENT" == "docker" ]]; then
	RUNNER_ARGS+=(--image "$QDRANT_IMAGE")
fi
if [[ "$DEPLOYMENT" != "external" ]]; then
	RUNNER_ARGS+=(--storage-path "$QDRANT_STORAGE_PATH")
fi
if [[ -n "$QDRANT_SERVER_PID" ]]; then
	RUNNER_ARGS+=(--server-pid "$QDRANT_SERVER_PID")
fi

QDRANT_API_KEY="$QDRANT_API_KEY" "$VENV/bin/python" \
	benchmarks/vector_db_compare/minima_qdrant_runner.py \
	"${RUNNER_ARGS[@]}"

printf 'manifest: %s\nqdrant evidence: %s\n' "$MANIFEST_PATH" "$OUTPUT_PATH"
