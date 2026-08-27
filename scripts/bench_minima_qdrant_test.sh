#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP=$(mktemp -d)
unrelated_pid=""
cleanup() {
	if [[ -n "$unrelated_pid" ]]; then
		kill "$unrelated_pid" >/dev/null 2>&1 || true
		wait "$unrelated_pid" 2>/dev/null || true
	fi
	rm -rf "$TMP"
}
trap cleanup EXIT

RUN_DIR="$TMP/run"
mkdir -p "$RUN_DIR"
printf '#!/usr/bin/env bash\nexit 0\n' >"$TMP/restart"
chmod +x "$TMP/restart"

sleep 30 &
unrelated_pid=$!
printf '%s\n%s\n' "$unrelated_pid" "deliberately-wrong-process-identity" >"$RUN_DIR/qdrant.pid"

set +e
output=$(RUN_DIR="$RUN_DIR" QDRANT_URL=http://127.0.0.1:1 QDRANT_BIN= \
	QDRANT_RESTART_HOOK="$TMP/restart" QDRANT_SERVER_PID="$unrelated_pid" \
	QDRANT_STORAGE_PATH= bash "$ROOT/scripts/bench_minima_qdrant.sh" 2>&1)
status=$?
set -e

[[ "$status" == 2 ]]
[[ "$output" == *"authoritative QDRANT_STORAGE_PATH"* ]]
[[ "$output" == *"refusing to signal that process during cleanup"* ]]
kill -0 "$unrelated_pid"
