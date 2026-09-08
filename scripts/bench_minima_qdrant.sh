#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

MINIMA_MEASURED=${MINIMA_MEASURED:-false}
if [[ "$MINIMA_MEASURED" == true && -z "${QDRANT_COLLECTION:-}" ]]; then
	echo "Measured Qdrant requires explicit QDRANT_COLLECTION" >&2
	exit 2
fi
MANIFEST_PATH_INPUT=${MANIFEST_PATH:-}
if [[ -z "${RUN_DIR:-}" ]]; then
	RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gomap_minima_qdrant_XXXXXXXXXX")
fi
PYTHON="${PYTHON:-python3}"
VENV="${VENV:-$RUN_DIR/venv}"
MANIFEST_PATH="${MANIFEST_PATH:-$RUN_DIR/minima_manifest.json}"
OUTPUT_PATH="${OUTPUT_PATH:-$RUN_DIR/qdrant_backend.json}"
QDRANT_URL="${QDRANT_URL:-}"
QDRANT_BIN="${QDRANT_BIN:-}"
QDRANT_IMAGE="${QDRANT_IMAGE:-qdrant/qdrant:v1.19.0@sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc}"
QDRANT_COLLECTION="${QDRANT_COLLECTION:-gomap_minima_${RANDOM}_$$}"
QDRANT_STORAGE_PATH_INPUT="${QDRANT_STORAGE_PATH:-}"
QDRANT_STORAGE_PATH="${QDRANT_STORAGE_PATH:-$RUN_DIR/qdrant-storage}"
QDRANT_READY_TIMEOUT="${QDRANT_READY_TIMEOUT:-120}"
QDRANT_OPERATION_TIMEOUT="${QDRANT_OPERATION_TIMEOUT:-120}"
QDRANT_OPTIMIZER_TIMEOUT="${QDRANT_OPTIMIZER_TIMEOUT:-600}"
QDRANT_API_KEY="${QDRANT_API_KEY:-}"
ALLOW_DROP="${ALLOW_DROP:-false}"
QDRANT_PID=""
QDRANT_SERVER_PID="${QDRANT_SERVER_PID:-}"
QDRANT_RESTART_HOOK="${QDRANT_RESTART_HOOK:-}"
QDRANT_PID_FILE="$RUN_DIR/qdrant.pid"
QDRANT_CONTAINER=""
DEPLOYMENT=""
OWNED_STANDALONE=false

cleanup() {
	if [[ "$OWNED_STANDALONE" == true && -s "$QDRANT_PID_FILE" ]]; then
		local expected_identity=""
		{
			IFS= read -r QDRANT_PID
			IFS= read -r expected_identity || true
		} <"$QDRANT_PID_FILE"
	fi
	if [[ -n "$QDRANT_PID" ]]; then
		local current_identity=""
		if [[ "$QDRANT_PID" =~ ^[1-9][0-9]*$ ]]; then
			current_identity=$(ps -o lstart= -o command= -p "$QDRANT_PID" 2>/dev/null || true)
		fi
		if [[ -z "${expected_identity:-}" || "$current_identity" != "$expected_identity" ]]; then
			echo "owned Qdrant PID file is stale for PID $QDRANT_PID; refusing to signal that process during cleanup" >&2
		else
			kill "$QDRANT_PID" >/dev/null 2>&1 || true
			wait "$QDRANT_PID" >/dev/null 2>&1 || true
		fi
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
case "$MINIMA_MEASURED" in
	true|false) ;;
	*) echo "MINIMA_MEASURED must be true or false" >&2; exit 2 ;;
esac
if [[ "$MINIMA_MEASURED" == true ]]; then
	if [[ -n "$QDRANT_URL" || -n "$QDRANT_BIN" || -n "$QDRANT_RESTART_HOOK" ]]; then
		echo "Measured Qdrant requires this launcher's owned Docker deployment" >&2
		exit 2
	fi
	if [[ -z "${QDRANT_CPUSET_CPUS:-}" ]]; then
		echo "Measured Qdrant requires explicit QDRANT_CPUSET_CPUS" >&2
		exit 2
	fi
	if [[ -z "$MANIFEST_PATH_INPUT" || ! -f "$MANIFEST_PATH" ||
		-z "${MINIMA_FREEZE:-}" || ! -f "$MINIMA_FREEZE" ||
		! "${MINIMA_EXPECTED_FREEZE_SHA256:-}" =~ ^[0-9a-f]{64}$ ||
		! -x "${MINIMA_COMPARATOR_BIN:-}" || ! -x "$VENV/bin/python" ]]; then
		echo "Measured Qdrant requires supplied manifest/freeze/hash, prebuilt comparator and pinned VENV" >&2
		exit 2
	fi
	if [[ -e "$RUN_DIR" && ( ! -d "$RUN_DIR" || -n "$(find "$RUN_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ) ]] || [[ -e "$OUTPUT_PATH" ]]; then
		echo "Measured Qdrant requires fresh empty RUN_DIR and unused OUTPUT_PATH" >&2
		exit 2
	fi
	if [[ -e "$QDRANT_STORAGE_PATH" || -L "$QDRANT_STORAGE_PATH" ]]; then
		echo "Measured Qdrant requires unused QDRANT_STORAGE_PATH" >&2
		exit 2
	fi
fi
if [[ -n "$QDRANT_URL" && -n "$QDRANT_BIN" ]]; then
	echo "Set only one of QDRANT_URL or QDRANT_BIN" >&2
	exit 2
fi

mkdir -p "$RUN_DIR" "$(dirname "$MANIFEST_PATH")" "$(dirname "$OUTPUT_PATH")"
if [[ -n "$QDRANT_URL" ]]; then
	if [[ -z "$QDRANT_RESTART_HOOK" || ! -x "$QDRANT_RESTART_HOOK" ]]; then
		echo "External Qdrant requires executable QDRANT_RESTART_HOOK that restarts the same durable service and prints its PID" >&2
		exit 2
	fi
	if [[ ! "$QDRANT_SERVER_PID" =~ ^[1-9][0-9]*$ ]]; then
		echo "External Qdrant requires authoritative positive QDRANT_SERVER_PID" >&2
		exit 2
	fi
	if [[ -z "$QDRANT_STORAGE_PATH_INPUT" || ! -d "$QDRANT_STORAGE_PATH_INPUT" ]]; then
		echo "External Qdrant requires existing authoritative QDRANT_STORAGE_PATH" >&2
		exit 2
	fi
else
	mkdir -p "$QDRANT_STORAGE_PATH"
fi
if [[ "$MINIMA_MEASURED" != true ]]; then
	"$PYTHON" -m venv "$VENV"
	"$VENV/bin/python" -m pip install --disable-pip-version-check "qdrant-client==1.19.0"
	# Historical convenience mode generates its own compact manifest.
	go run ./TreeDB/cmd/treedb_rag_benchmark \
		-workload=minima \
		-dump-minima-manifest "$MANIFEST_PATH"
fi

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
	QDRANT_RESTART_HOOK="$RUN_DIR/restart-qdrant.sh"
	printf '#!/usr/bin/env bash\nexec %q standalone %q %q %q %q %q\n' \
		"$ROOT/scripts/restart_minima_qdrant_backend.sh" "$QDRANT_BIN" "$QDRANT_PORT" \
		"$QDRANT_STORAGE_PATH" "$RUN_DIR/qdrant.log" "$QDRANT_PID_FILE" >"$QDRANT_RESTART_HOOK"
	chmod +x "$QDRANT_RESTART_HOOK"
	# The existing hook captures the stable post-exec identity on both starts.
	OWNED_STANDALONE=true
	QDRANT_PID=$("$QDRANT_RESTART_HOOK")
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
	QDRANT_RESTART_HOOK="$RUN_DIR/restart-qdrant.sh"
	printf '#!/usr/bin/env bash\nexec %q docker %q\n' \
		"$ROOT/scripts/restart_minima_qdrant_backend.sh" "$QDRANT_CONTAINER" >"$QDRANT_RESTART_HOOK"
	chmod +x "$QDRANT_RESTART_HOOK"
	docker_runtime_args=()
	if [[ -n "${QDRANT_CPUSET_CPUS:-}" ]]; then
		docker_runtime_args+=(--cpuset-cpus "$QDRANT_CPUSET_CPUS")
	fi
	if [[ "$MINIMA_MEASURED" == true ]]; then
		docker_runtime_args+=(--user "$(id -u):$(id -g)" --entrypoint /qdrant/qdrant
			-e QDRANT__STORAGE__SNAPSHOTS_PATH=/qdrant/storage/snapshots
			-e QDRANT_INIT_FILE_PATH=/qdrant/storage/.qdrant-initialized)
	fi
	docker run -d --rm \
		${docker_runtime_args[@]+"${docker_runtime_args[@]}"} \
		--name "$QDRANT_CONTAINER" \
		-p "127.0.0.1:${QDRANT_PORT}:6333" \
		-v "$QDRANT_STORAGE_PATH:/qdrant/storage" \
		"$QDRANT_IMAGE" >/dev/null
	if [[ "$(uname -s)" == "Linux" ]]; then
		QDRANT_SERVER_PID=$(docker inspect --format '{{.State.Pid}}' "$QDRANT_CONTAINER")
	fi
fi
if [[ ! "$QDRANT_SERVER_PID" =~ ^[1-9][0-9]*$ ]]; then
	echo "Qdrant qualification requires an authoritative host server PID; got: ${QDRANT_SERVER_PID:-unset}" >&2
	exit 2
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
	--restart-hook "$QDRANT_RESTART_HOOK"
)
if [[ "$MINIMA_MEASURED" == true ]]; then
	RUNNER_ARGS+=(--measured --freeze "$MINIMA_FREEZE"
		--expected-freeze-sha256 "$MINIMA_EXPECTED_FREEZE_SHA256" --comparator-bin "$MINIMA_COMPARATOR_BIN")
fi
if [[ "$ALLOW_DROP" == "true" ]]; then
	RUNNER_ARGS+=(--allow-drop)
fi
if [[ "$DEPLOYMENT" == "docker" ]]; then
	RUNNER_ARGS+=(--image "$QDRANT_IMAGE" --container "$QDRANT_CONTAINER")
fi
RUNNER_ARGS+=(--storage-path "$QDRANT_STORAGE_PATH")
RUNNER_ARGS+=(--server-pid "$QDRANT_SERVER_PID")

QDRANT_API_KEY="$QDRANT_API_KEY" "$VENV/bin/python" \
	benchmarks/vector_db_compare/minima_qdrant_runner.py \
	"${RUNNER_ARGS[@]}"

printf 'manifest: %s\nqdrant evidence: %s\n' "$MANIFEST_PATH" "$OUTPUT_PATH"
