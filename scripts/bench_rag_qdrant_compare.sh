#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR=${RUN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/gomap_rag_qdrant_XXXXXXXX")}
PYTHON=${PYTHON:-python3}
VENV=${VENV:-$RUN_DIR/venv}
QDRANT_BIN=${QDRANT_BIN:?set QDRANT_BIN to the pinned standalone Qdrant 1.19.0 release binary}
QDRANT_RELEASE_ASSET=${QDRANT_RELEASE_ASSET:?set QDRANT_RELEASE_ASSET to the pinned Qdrant 1.19.0 release archive}
QDRANT_STORAGE_PATH=${QDRANT_STORAGE_PATH:-$RUN_DIR/qdrant-storage}
QDRANT_COLLECTION=${QDRANT_COLLECTION:-gomap_rag_4331_${RANDOM}_$$}
COMPARATOR_BIN=$RUN_DIR/treedb_rag_benchmark
MANIFEST=$RUN_DIR/application_comparison_manifest.json
TREEDB_ARTIFACT=$RUN_DIR/treedb_backend.json
TREEDB_DIR=$RUN_DIR/treedb
QDRANT_ARTIFACT=$RUN_DIR/qdrant_backend.json
COMPARISON_JSON=$RUN_DIR/comparison.json
COMPARISON_MD=$RUN_DIR/comparison.md
PHASE_STATUS=$RUN_DIR/phase_status.jsonl
PID_FILE=$RUN_DIR/qdrant.pid
PID=""

HARNESS_REVISION=$(git rev-parse HEAD)
if [[ ! "$HARNESS_REVISION" =~ ^[0-9a-f]{40}$ ]] || [[ -n "$(git status --porcelain)" ]]; then
	echo "comparison requires a full clean harness revision" >&2
	exit 2
fi
COMPARATOR_LDFLAGS="-X main.applicationStampedVCSRevision=$HARNESS_REVISION -X main.applicationStampedVCSModified=false"
if [[ ! -x "$QDRANT_BIN" ]]; then
	echo "QDRANT_BIN is not an executable standalone Qdrant binary" >&2
	exit 2
fi
if [[ ! -f "$QDRANT_RELEASE_ASSET" ]]; then
	echo "QDRANT_RELEASE_ASSET is not a file" >&2
	exit 2
fi
QDRANT_BIN=$(cd -- "$(dirname -- "$QDRANT_BIN")" && pwd -P)/$(basename -- "$QDRANT_BIN")
QDRANT_RELEASE_ASSET=$(cd -- "$(dirname -- "$QDRANT_RELEASE_ASSET")" && pwd -P)/$(basename -- "$QDRANT_RELEASE_ASSET")

cleanup() {
	if [[ -s "$PID_FILE" ]]; then
		local saved_pid saved_identity current_identity
		{ IFS= read -r saved_pid; IFS= read -r saved_identity || true; } <"$PID_FILE"
		current_identity=$(ps -o lstart= -o command= -p "$saved_pid" 2>/dev/null || true)
		if [[ -n "$saved_identity" && "$current_identity" == "$saved_identity" ]]; then
			kill "$saved_pid" >/dev/null 2>&1 || true
			wait "$saved_pid" >/dev/null 2>&1 || true
		fi
	fi
}
trap cleanup EXIT
mkdir -p "$RUN_DIR"
if [[ -e "$QDRANT_STORAGE_PATH" ]]; then
	echo "Qdrant comparison storage path must be absent: $QDRANT_STORAGE_PATH" >&2
	exit 2
fi
mkdir -p "$QDRANT_STORAGE_PATH"
: >"$PHASE_STATUS"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install --disable-pip-version-check "qdrant-client==1.19.0"

# Every capped command owns a new process group. Timeout kills the complete group
# and appends a durable partial/failure record before returning nonzero.
run_capped() {
	local cap_seconds=$1
	local phase=$2
	shift 2
	"$PYTHON" - "$PHASE_STATUS" "$phase" "$cap_seconds" "$@" <<'PY'
import datetime, json, os, signal, subprocess, sys, time
status_path, phase, cap_seconds, command = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:]
started = time.monotonic()
process = subprocess.Popen(command, start_new_session=True)
state, code, error = "complete", None, ""
try:
    code = process.wait(timeout=cap_seconds)
    if code != 0:
        state, error = "failed", f"exit status {code}"
except subprocess.TimeoutExpired:
    state, error = "timeout", f"{cap_seconds} second hard phase cap"
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    code = 124
record = {"phase": phase, "state": state, "exit_code": code, "elapsed_seconds": time.monotonic() - started,
          "hard_cap_seconds": cap_seconds, "command": command, "error": error, "recorded_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
with open(status_path, "a", encoding="utf-8") as output:
    output.write(json.dumps(record, sort_keys=True) + "\n")
raise SystemExit(code)
PY
}

run_capped 90 build-comparator go build -buildvcs=true -ldflags "$COMPARATOR_LDFLAGS" -o "$COMPARATOR_BIN" ./TreeDB/cmd/treedb_rag_benchmark
run_capped 90 manifest "$COMPARATOR_BIN" -dump-application-comparison-manifest "$MANIFEST"
run_capped 200 treedb-build-query-reopen "$COMPARATOR_BIN" \
	-dir "$TREEDB_DIR" \
	-harness-revision "$HARNESS_REVISION" \
	-application-comparison-treedb-output "$TREEDB_ARTIFACT"

PORT=$("$PYTHON" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
URL="http://127.0.0.1:$PORT"
RESTART_HOOK=$RUN_DIR/restart-qdrant.sh
printf '#!/usr/bin/env bash\nset -euo pipefail\ncd %q\nexec %q standalone %q %q %q %q %q\n' "$RUN_DIR" "$ROOT/scripts/restart_minima_qdrant_backend.sh" "$QDRANT_BIN" "$PORT" "$QDRANT_STORAGE_PATH" "$RUN_DIR/qdrant.log" "$PID_FILE" >"$RESTART_HOOK"
chmod +x "$RESTART_HOOK"
PID=$(
	cd "$RUN_DIR"
	exec "$ROOT/scripts/restart_minima_qdrant_backend.sh" standalone \
		"$QDRANT_BIN" "$PORT" "$QDRANT_STORAGE_PATH" "$RUN_DIR/qdrant.log" "$PID_FILE"
)
{ IFS= read -r recorded_pid; IFS= read -r IDENTITY || true; } <"$PID_FILE"
[[ "$PID" == "$recorded_pid" && -n "$IDENTITY" ]] || {
	echo "Qdrant helper returned inconsistent process identity" >&2
	exit 1
}
SERVER_IDENTITY="pid:$PID:$IDENTITY"

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

run_capped 300 qdrant-build-query-reopen "$VENV/bin/python" benchmarks/vector_db_compare/rag_qdrant_runner.py \
	--manifest "$MANIFEST" --output "$QDRANT_ARTIFACT" --url "$URL" \
	--collection "$QDRANT_COLLECTION" --server-identity "$SERVER_IDENTITY" \
	--harness-revision "$HARNESS_REVISION" --storage-path "$QDRANT_STORAGE_PATH" \
	--restart-hook "$RESTART_HOOK" --server-pid "$PID" --server-binary "$QDRANT_BIN" \
	--server-release-asset "$QDRANT_RELEASE_ASSET"
run_capped 90 consolidation "$COMPARATOR_BIN" \
	-application-comparison-manifest "$MANIFEST" \
	-application-comparison-treedb "$TREEDB_ARTIFACT" \
	-application-comparison-qdrant "$QDRANT_ARTIFACT" \
	-application-comparison-treedb-storage "$TREEDB_DIR" \
	-application-comparison-qdrant-storage "$QDRANT_STORAGE_PATH" \
	-application-comparison-output "$COMPARISON_JSON" \
	-application-comparison-report "$COMPARISON_MD"
printf 'run_dir=%s\nphase_status=%s\nmanifest=%s\ntreedb=%s\nqdrant=%s\ncomparison_json=%s\ncomparison_md=%s\n' "$RUN_DIR" "$PHASE_STATUS" "$MANIFEST" "$TREEDB_ARTIFACT" "$QDRANT_ARTIFACT" "$COMPARISON_JSON" "$COMPARISON_MD"
