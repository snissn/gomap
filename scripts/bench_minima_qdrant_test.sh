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
[[ "$output" != *"signal that process during cleanup"* ]]
kill -0 "$unrelated_pid"

# Even a matching stale file belongs to another invocation in external mode.
ps -o lstart= -o command= -p "$unrelated_pid" >"$TMP/identity"
printf '%s\n%s\n' "$unrelated_pid" "$(cat "$TMP/identity")" >"$RUN_DIR/qdrant.pid"
mkdir -p "$TMP/external-storage"
cat >"$TMP/setup-python" <<'PYTHON'
#!/usr/bin/env bash
echo 'intentional external setup failure' >&2
exit 73
PYTHON
chmod +x "$TMP/setup-python"
set +e
RUN_DIR="$RUN_DIR" MINIMA_MEASURED=false PYTHON="$TMP/setup-python" VENV="$TMP/external-venv" \
	QDRANT_URL=http://127.0.0.1:1 QDRANT_BIN= \
	QDRANT_RESTART_HOOK="$TMP/restart" QDRANT_SERVER_PID="$unrelated_pid" \
	QDRANT_STORAGE_PATH="$TMP/external-storage" \
	bash "$ROOT/scripts/bench_minima_qdrant.sh" >"$TMP/retry.log" 2>&1
status=$?
set -e
[[ "$status" == 73 ]]
[[ "$(cat "$TMP/retry.log")" == *"intentional external setup failure"* ]]
kill -0 "$unrelated_pid"

# Measured mode consumes existing inputs and delegates runtime inspection to
# the real runner; stubs below exercise only launcher ownership and arguments.
mkdir -p "$TMP/stubs" "$TMP/pinned/bin"
cat >"$TMP/stubs/docker" <<'DOCKER'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_CALLS"
case "$1" in
  run) echo owned-container ;;
  inspect) echo 12345 ;;
esac
DOCKER
cat >"$TMP/pinned/bin/python" <<'PYTHON'
#!/usr/bin/env bash
case "$1" in
  -c) echo 19333 ;;
  -) cat >/dev/null ;;
  benchmarks/vector_db_compare/minima_qdrant_runner.py)
    printf '%s\n' "$@" >"$RUNNER_CALLS"
    exit 7 ;;
  *) echo "unexpected Python setup: $*" >&2; exit 99 ;;
esac
PYTHON
cat >"$TMP/stubs/go" <<'GO'
#!/usr/bin/env bash
exit 99
GO
chmod +x "$TMP/stubs/"* "$TMP/pinned/bin/python"
printf '{}\n' >"$TMP/manifest.json"
printf '{}\n' >"$TMP/freeze.json"
cp "$TMP/restart" "$TMP/comparator"
export DOCKER_CALLS="$TMP/docker.calls" RUNNER_CALLS="$TMP/runner.calls"
if PATH="$TMP/stubs:$PATH" RUN_DIR="$TMP/no-cpuset" VENV="$TMP/pinned" MINIMA_MEASURED=true \
  MANIFEST_PATH="$TMP/manifest.json" MINIMA_FREEZE="$TMP/freeze.json" \
  MINIMA_EXPECTED_FREEZE_SHA256="$(printf 'f%.0s' {1..64})" MINIMA_COMPARATOR_BIN="$TMP/comparator" \
  QDRANT_URL= QDRANT_BIN= QDRANT_RESTART_HOOK= QDRANT_CPUSET_CPUS= \
  bash "$ROOT/scripts/bench_minima_qdrant.sh" >"$TMP/cpuset.log" 2>&1; then
  exit 1
fi
[[ "$(cat "$TMP/cpuset.log")" == *"explicit QDRANT_CPUSET_CPUS"* ]]
[[ ! -e "$DOCKER_CALLS" && ! -e "$RUNNER_CALLS" ]]
mkdir -p "$TMP/reused-storage"
printf 'retained data\n' >"$TMP/reused-storage/existing"
if PATH="$TMP/stubs:$PATH" RUN_DIR="$TMP/fresh-run" VENV="$TMP/pinned" MINIMA_MEASURED=true \
  MANIFEST_PATH="$TMP/manifest.json" MINIMA_FREEZE="$TMP/freeze.json" \
  MINIMA_EXPECTED_FREEZE_SHA256="$(printf 'f%.0s' {1..64})" MINIMA_COMPARATOR_BIN="$TMP/comparator" \
  QDRANT_URL= QDRANT_BIN= QDRANT_RESTART_HOOK= QDRANT_CPUSET_CPUS=2,4 \
  QDRANT_STORAGE_PATH="$TMP/reused-storage" \
  bash "$ROOT/scripts/bench_minima_qdrant.sh" >"$TMP/storage.log" 2>&1; then
  exit 1
fi
[[ "$(cat "$TMP/storage.log")" == *"unused QDRANT_STORAGE_PATH"* ]]
[[ ! -e "$DOCKER_CALLS" && ! -e "$RUNNER_CALLS" ]]
[[ "$(cat "$TMP/reused-storage/existing")" == 'retained data' ]]
set +e
PATH="$TMP/stubs:$PATH" RUN_DIR="$TMP/measured" VENV="$TMP/pinned" MINIMA_MEASURED=true \
  MANIFEST_PATH="$TMP/manifest.json" MINIMA_FREEZE="$TMP/freeze.json" \
  MINIMA_EXPECTED_FREEZE_SHA256="$(printf 'f%.0s' {1..64})" MINIMA_COMPARATOR_BIN="$TMP/comparator" \
  QDRANT_URL= QDRANT_BIN= QDRANT_RESTART_HOOK= QDRANT_CPUSET_CPUS=2,4 \
  bash "$ROOT/scripts/bench_minima_qdrant.sh" >"$TMP/measured.log" 2>&1
status=$?
set -e
[[ "$status" == 7 ]]
[[ "$(cat "$TMP/manifest.json")" == '{}' ]]
[[ ! -d "$TMP/measured/venv" ]]
[[ "$(cat "$DOCKER_CALLS")" == *"--cpuset-cpus 2,4"* ]]
[[ "$(cat "$DOCKER_CALLS")" == *"--user $(id -u):$(id -g) --entrypoint /qdrant/qdrant"* ]]
[[ "$(cat "$DOCKER_CALLS")" == *"QDRANT__STORAGE__SNAPSHOTS_PATH=/qdrant/storage/snapshots"* ]]
[[ "$(cat "$DOCKER_CALLS")" == *"QDRANT_INIT_FILE_PATH=/qdrant/storage/.qdrant-initialized"* ]]
[[ "$(tail -1 "$DOCKER_CALLS")" == 'rm -f gomap-minima-qdrant-'* ]]
[[ "$(cat "$RUNNER_CALLS")" == *$'--container\ngomap-minima-qdrant-'* ]]
[[ "$(cat "$RUNNER_CALLS")" == *$'--manifest\n'"$TMP/manifest.json"* ]]
[[ "$(cat "$RUNNER_CALLS")" == *$'--measured\n'* ]]
if MINIMA_MEASURED=true RUN_DIR="$TMP/refuse" QDRANT_URL=http://external \
  bash "$ROOT/scripts/bench_minima_qdrant.sh" >"$TMP/refuse.log" 2>&1; then
  exit 1
fi
[[ "$(cat "$TMP/refuse.log")" == *"owned Docker deployment"* ]]
