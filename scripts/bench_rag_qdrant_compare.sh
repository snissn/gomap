#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR=${RUN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/gomap_rag_qdrant_XXXXXXXX")}
PYTHON=${PYTHON:-python3}
VENV=${VENV:-$RUN_DIR/venv}
TREEDB_ARTIFACT=${TREEDB_ARTIFACT:?set TREEDB_ARTIFACT to an existing retained TreeDB application JSON artifact}
QDRANT_BIN=${QDRANT_BIN:-}
QDRANT_IMAGE=${QDRANT_IMAGE:-qdrant/qdrant:v1.19.0@sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc}
QDRANT_STORAGE_PATH=${QDRANT_STORAGE_PATH:-$RUN_DIR/qdrant-storage}
QDRANT_COLLECTION=${QDRANT_COLLECTION:-gomap_rag_4331_${RANDOM}_$$}
MANIFEST=$RUN_DIR/application_comparison_manifest.json
QDRANT_ARTIFACT=$RUN_DIR/qdrant_backend.json
COMPARISON_JSON=$RUN_DIR/comparison.json
COMPARISON_MD=$RUN_DIR/comparison.md
PID_FILE=$RUN_DIR/qdrant.pid
CONTAINER=""
PID=""

cleanup() {
	if [[ -n "$CONTAINER" ]]; then docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; fi
	if [[ -s "$PID_FILE" ]]; then
		local saved_pid saved_identity current_identity
		{ IFS= read -r saved_pid; IFS= read -r saved_identity || true; } <"$PID_FILE"
		current_identity=$(ps -o lstart= -o command= -p "$saved_pid" 2>/dev/null || true)
		if [[ -n "$saved_identity" && "$current_identity" == "$saved_identity" ]]; then kill "$saved_pid" >/dev/null 2>&1 || true; wait "$saved_pid" >/dev/null 2>&1 || true; fi
	fi
}
trap cleanup EXIT
mkdir -p "$RUN_DIR" "$QDRANT_STORAGE_PATH"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install --disable-pip-version-check "qdrant-client==1.19.0"

# Portable hard cap for manifest and validation phases; the runner separately caps build/query/reopen.
run_90s() {
	"$PYTHON" -c 'import subprocess,sys; raise SystemExit(subprocess.run(sys.argv[1:], timeout=90).returncode)' "$@"
}
run_90s go run ./TreeDB/cmd/treedb_rag_benchmark -dump-application-comparison-manifest "$MANIFEST"
PORT=$("$PYTHON" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
URL="http://127.0.0.1:$PORT"
RESTART_HOOK=$RUN_DIR/restart-qdrant.sh
if [[ -n "$QDRANT_BIN" ]]; then
	[[ -x "$QDRANT_BIN" ]] || { echo "QDRANT_BIN is not executable" >&2; exit 2; }
	DEPLOYMENT=standalone
	printf '#!/usr/bin/env bash\nexec %q standalone %q %q %q %q %q\n' "$ROOT/scripts/restart_minima_qdrant_backend.sh" "$QDRANT_BIN" "$PORT" "$QDRANT_STORAGE_PATH" "$RUN_DIR/qdrant.log" "$PID_FILE" >"$RESTART_HOOK"
	chmod +x "$RESTART_HOOK"
	QDRANT__SERVICE__HOST=127.0.0.1 QDRANT__SERVICE__HTTP_PORT="$PORT" QDRANT__STORAGE__STORAGE_PATH="$QDRANT_STORAGE_PATH" "$QDRANT_BIN" >"$RUN_DIR/qdrant.log" 2>&1 &
	PID=$!
	IDENTITY=$(ps -o lstart= -o command= -p "$PID" 2>/dev/null || true)
	[[ -n "$IDENTITY" ]] || { echo "Qdrant exited before identity capture" >&2; exit 1; }
	printf '%s\n%s\n' "$PID" "$IDENTITY" >"$PID_FILE"
	SERVER_IDENTITY="pid:$PID:$IDENTITY"
else
	[[ "$QDRANT_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] || { echo "QDRANT_IMAGE must be digest pinned" >&2; exit 2; }
	command -v docker >/dev/null || { echo "Docker unavailable and QDRANT_BIN unset; refusing substitution" >&2; exit 2; }
	DEPLOYMENT=docker
	CONTAINER="gomap-rag-4331-${RANDOM}-$$"
	printf '#!/usr/bin/env bash\nexec %q docker %q\n' "$ROOT/scripts/restart_minima_qdrant_backend.sh" "$CONTAINER" >"$RESTART_HOOK"
	chmod +x "$RESTART_HOOK"
	docker run -d --rm --name "$CONTAINER" -p "127.0.0.1:${PORT}:6333" -v "$QDRANT_STORAGE_PATH:/qdrant/storage" "$QDRANT_IMAGE" >/dev/null
	SERVER_IDENTITY=$(docker inspect --format '{{.Id}}' "$CONTAINER")
fi
"$VENV/bin/python" - "$URL" <<'PY'
import json,sys,time,urllib.request
url=sys.argv[1].rstrip('/')+'/'
deadline=time.monotonic()+90
while time.monotonic()<deadline:
    try:
        with urllib.request.urlopen(url,timeout=2) as response: value=json.load(response)
        if value.get('version')!='1.19.0': raise SystemExit(f"wrong Qdrant version: {value}")
        raise SystemExit(0)
    except SystemExit: raise
    except Exception: time.sleep(.2)
raise SystemExit('Qdrant readiness exceeded 90 seconds')
PY

RUNNER_ARGS=(--manifest "$MANIFEST" --output "$QDRANT_ARTIFACT" --url "$URL" --collection "$QDRANT_COLLECTION" --deployment "$DEPLOYMENT" --image "$QDRANT_IMAGE" --server-identity "$SERVER_IDENTITY" --storage-path "$QDRANT_STORAGE_PATH" --restart-hook "$RESTART_HOOK" --allow-drop)
if [[ -n "$CONTAINER" ]]; then RUNNER_ARGS+=(--container-id "$CONTAINER"); fi
"$VENV/bin/python" benchmarks/vector_db_compare/rag_qdrant_runner.py "${RUNNER_ARGS[@]}"
run_90s go run ./TreeDB/cmd/treedb_rag_benchmark \
	-application-comparison-manifest "$MANIFEST" \
	-application-comparison-treedb "$TREEDB_ARTIFACT" \
	-application-comparison-qdrant "$QDRANT_ARTIFACT" \
	-application-comparison-output "$COMPARISON_JSON" \
	-application-comparison-report "$COMPARISON_MD"
printf 'run_dir=%s\nmanifest=%s\nqdrant=%s\ncomparison_json=%s\ncomparison_md=%s\n' "$RUN_DIR" "$MANIFEST" "$QDRANT_ARTIFACT" "$COMPARISON_JSON" "$COMPARISON_MD"
