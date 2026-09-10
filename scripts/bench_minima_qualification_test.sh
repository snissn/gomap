#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP=$(mktemp -d)
unrelated_pid=""

replacement_pid=""
cleanup() {
	[[ -z "$replacement_pid" ]] || kill "$replacement_pid" >/dev/null 2>&1 || true
	[[ -z "$unrelated_pid" ]] || kill "$unrelated_pid" >/dev/null 2>&1 || true
	rm -rf "$TMP"
}
trap cleanup EXIT

REPO="$TMP/repo"
FAKE_BIN="$TMP/bin"
RUN_DIR="$TMP/run"
mkdir -p "$REPO/scripts" "$FAKE_BIN"
cp "$ROOT/scripts/bench_minima_qualification.sh" "$REPO/scripts/"

cat >"$FAKE_BIN/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${FAKE_GO_INVOKED:-}" ]]; then
	printf 'invoked\n' >"$FAKE_GO_INVOKED"
fi
[[ "$1" == build && "$2" == -o ]]
out=$3
mkdir -p "$(dirname "$out")"
if [[ "$out" == *treedb-rag-benchmark ]]; then
	cat >"$out" <<'PROGRAM'
#!/usr/bin/env bash
set -euo pipefail
manifest=""
output=""
report=""
validate=""
expected_commit=""
bounded_rows=""
while (($#)); do
	case "$1" in
	-dump-minima-manifest) manifest=$2; shift 2 ;;
	-minima-output) output=$2; shift 2 ;;
	-validate-minima-artifact) validate=$2; shift 2 ;;
	-minima-report) report=$2; shift 2 ;;
	-minima-expected-commit) expected_commit=$2; shift 2 ;;
	-minima-bounded-total-rows) bounded_rows=$2; shift 2 ;;
	*) shift ;;
	esac
done
if [[ -n "$manifest" ]]; then
	[[ -z "${FAKE_BOUNDED_ROWS_PATH:-}" ]] || printf '%s\n' "$bounded_rows" >"$FAKE_BOUNDED_ROWS_PATH"
	printf '{}\n' >"$manifest"
	exit 0
fi
if [[ -n "$validate" ]]; then
	exit 0
fi
if [[ -n "${FAKE_EXPECTED_COMMIT_PATH:-}" ]]; then
	printf '%s\n' "$expected_commit" >"$FAKE_EXPECTED_COMMIT_PATH"
fi
printf '{"state":"partial","passing":false,"readiness_recommendation":"not_evaluated"}\n' >"$output"
printf 'partial\n' >"$report"
exit 7
PROGRAM
else
	printf '#!/usr/bin/env bash\nexit 0\n' >"$out"
fi
chmod +x "$out"
EOF
chmod +x "$FAKE_BIN/go"

cat >"$FAKE_BIN/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == rev-parse ]]
printf '%s\n' "${FAKE_GIT_HEAD:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
EOF
chmod +x "$FAKE_BIN/git"

cat >"$FAKE_BIN/python" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  -c) echo 19333; exit 0 ;;
  -) cat >/dev/null; exit 0 ;;
esac
if [[ -n "${FAKE_PYTHON_CALLS:-}" ]]; then
	printf '%s\n' "$1" >>"$FAKE_PYTHON_CALLS"
fi
if [[ -n "${FAKE_PYTHON_ARGS:-}" ]]; then
	printf '%s\n' "$@" >"$FAKE_PYTHON_ARGS"
fi
if [[ -n "${FAKE_HANG_CHILD_PID:-}" &&
	( -z "${FAKE_HANG_STAGE:-}" || "$1" == *"minima_${FAKE_HANG_STAGE}_runner.py" ) ]]; then
	sleep 30 &
	printf '%s\n' "$!" >"$FAKE_HANG_CHILD_PID"
	wait "$!"
fi
output=""
while (($#)); do
	if [[ "$1" == --output ]]; then
		output=$2
		break
	fi
	shift
done
printf '{}\n' >"$output"
exit "${FAKE_PYTHON_STATUS:-0}"
EOF
chmod +x "$FAKE_BIN/python"

cat >"$REPO/scripts/bench_minima_qdrant.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${FAKE_QDRANT_INPUTS:-}" ]]; then
	printf '%s\n' "$MINIMA_MEASURED" "$MANIFEST_PATH" "$MINIMA_FREEZE" "$MINIMA_EXPECTED_FREEZE_SHA256" "$MINIMA_COMPARATOR_BIN" "$VENV" "${QDRANT_COLLECTION:-}" >"$FAKE_QDRANT_INPUTS"
fi
printf '{}\n' >"$OUTPUT_PATH"
EOF
chmod +x "$REPO/scripts/bench_minima_qdrant.sh"

for fixture in bounded-50k:50000 bounded-250k:250000 bounded-500k:500000 bounded-1000k:1000000; do
	mode=${fixture%:*}
	expected_rows=${fixture#*:}
	bounded_dir="$TMP/$mode"
	output=$(env -u MINIMA_WALL_SECONDS PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" RUN_DIR="$bounded_dir" \
		MODE="$mode" FAKE_PYTHON_ARGS="$TMP/$mode-args" FAKE_BOUNDED_ROWS_PATH="$TMP/$mode-rows" \
		"$REPO/scripts/bench_minima_qualification.sh" 2>&1)
	[[ "$output" == *"total rows across scenarios"* ]]
	[[ "$output" == *"full <1% sparse selectivity excluded; cannot qualify"* ]]
	[[ -f "$bounded_dir/treedb_backend.json" ]]
	[[ ! -f "$bounded_dir/qdrant_backend.json" ]]
	grep -qx -- '--strategy' "$TMP/$mode-args"
	grep -qx -- "$expected_rows" "$TMP/$mode-rows"
done

set +e
PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" RUN_DIR="$TMP/bounded-timeout" \
	MODE=bounded-50k MINIMA_WALL_SECONDS=1 FAKE_HANG_CHILD_PID="$TMP/timed-child.pid" \
	"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/timeout.log" 2>&1
timeout_status=$?
set -e
[[ "$timeout_status" == 124 ]]
IFS= read -r timed_child_pid <"$TMP/timed-child.pid"
# A killed child can briefly remain a zombie pending its init reaper. It must
# never remain running after the timeout killed the owned process group.
timed_child_state=$(ps -o stat= -p "$timed_child_pid" || true)
[[ -z "$timed_child_state" || "$timed_child_state" == Z* ]]

set +e
output=$(PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" QDRANT_BIN=/bin/true RUN_DIR="$RUN_DIR" \
	TREEDB_STARTUP_TIMEOUT=987 FAKE_PYTHON_ARGS="$TMP/treedb-args" \
	FAKE_EXPECTED_COMMIT_PATH="$TMP/expected-commit" \
	"$REPO/scripts/bench_minima_qualification.sh" 2>&1)
status=$?
set -e

if ((status == 0)); then
	printf '%s\n' "harness accepted a failing comparator" >&2
	exit 1
fi
[[ -f "$RUN_DIR/minima_qualification.json" ]]
[[ -f "$RUN_DIR/minima_qualification.md" ]]
[[ "$output" == *"qualification failed: TreeDB=0 Qdrant=0 comparator=7"* ]]
[[ "$output" == *"qualification artifact: $RUN_DIR/minima_qualification.json"* ]]
[[ "$output" == *"report: $RUN_DIR/minima_qualification.md"* ]]
grep -qx -- '--operation-timeout' "$TMP/treedb-args"
grep -qx -- '120' "$TMP/treedb-args"
grep -qx -- '--startup-timeout' "$TMP/treedb-args"
grep -qx -- '987' "$TMP/treedb-args"
[[ "$(<"$TMP/expected-commit")" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]]

# Measured matched mode consumes pinned inputs, including a bounded manifest;
# legacy bounded modes above still intentionally collect only TreeDB.
mkdir -p "$TMP/pinned-venv/bin"
ln -s "$FAKE_BIN/python" "$TMP/pinned-venv/bin/python"
cp "$RUN_DIR/bin/treedb-rag-benchmark" "$TMP/comparator"
cp "$RUN_DIR/bin/treedb-document-service" "$TMP/service"
printf '{"schema":"treedb_minima_manifest/v2","fixture":"bounded-50000"}\n' >"$TMP/supplied-manifest"
printf '{}\n' >"$TMP/freeze"
printf '{}\n' >"$TMP/serving"
manifest_before=$(cat "$TMP/supplied-manifest")
measured_env=(env PATH="$FAKE_BIN:$PATH" MODE=measured
	TREEDB_COLLECTION=frozen_treedb QDRANT_COLLECTION=frozen_qdrant
	MANIFEST_PATH="$TMP/supplied-manifest" MINIMA_FREEZE="$TMP/freeze"
	MINIMA_EXPECTED_FREEZE_SHA256="$(printf '%064d' 0)" VENV="$TMP/pinned-venv"
	TREEDB_SERVICE_BIN="$TMP/service" MINIMA_COMPARATOR_BIN="$TMP/comparator"
	TREEDB_STRATEGY=column_graph TREEDB_TRANSPORT=native TREEDB_NATIVE_ADDRESS=127.0.0.1:17122
	TREEDB_COLUMN_GRAPH_SERVING="$TMP/serving")
# Collection names are part of the exact frozen configuration.
for collection in TREEDB_COLLECTION QDRANT_COLLECTION; do
	set +e
	"${measured_env[@]}" env -u "$collection" RUN_DIR="$TMP/missing-$collection" \
		MINIMA_WALL_SECONDS=10 FAKE_GO_INVOKED="$TMP/missing-collection.build" \
		FAKE_PYTHON_CALLS="$TMP/missing-collection.calls" \
		FAKE_QDRANT_INPUTS="$TMP/missing-collection.qdrant" \
		"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/missing-collection.log" 2>&1
	collection_status=$?
	set -e
	[[ "$collection_status" == 2 ]]
	grep -q 'requires explicit TREEDB_COLLECTION and QDRANT_COLLECTION' "$TMP/missing-collection.log"
	[[ ! -e "$TMP/missing-$collection" && ! -e "$TMP/missing-collection.build" &&
		! -e "$TMP/missing-collection.calls" && ! -e "$TMP/missing-collection.qdrant" ]]
done

# A measured run must choose its whole-pipeline deadline explicitly. Reject
# both an absent and an empty value before any backend or build work starts.
for deadline in missing empty; do
	deadline_env=(env -u MINIMA_WALL_SECONDS)
	[[ "$deadline" != empty ]] || deadline_env+=(MINIMA_WALL_SECONDS=)
	set +e
	"${deadline_env[@]}" "${measured_env[@]}" RUN_DIR="$TMP/$deadline-wall" \
		FAKE_GO_INVOKED="$TMP/$deadline-wall.build" \
		FAKE_PYTHON_CALLS="$TMP/$deadline-wall.calls" \
		FAKE_QDRANT_INPUTS="$TMP/$deadline-wall.qdrant" \
		"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/$deadline-wall.log" 2>&1
	wall_status=$?
	set -e
	[[ "$wall_status" == 2 ]]
	grep -q 'MINIMA_WALL_SECONDS must be a positive integer' "$TMP/$deadline-wall.log"
	[[ ! -e "$TMP/$deadline-wall" && ! -e "$TMP/$deadline-wall.build" &&
		! -e "$TMP/$deadline-wall.calls" && ! -e "$TMP/$deadline-wall.qdrant" ]]
done

for wall in 0 -1 1.5 invalid; do
	set +e
	"${measured_env[@]}" RUN_DIR="$TMP/invalid-wall-$wall" MINIMA_WALL_SECONDS="$wall" \
		FAKE_PYTHON_CALLS="$TMP/invalid-wall.calls" \
		"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/invalid-wall.log" 2>&1
	wall_status=$?
	set -e
	[[ "$wall_status" == 2 ]]
	grep -q 'MINIMA_WALL_SECONDS must be a positive integer' "$TMP/invalid-wall.log"
	[[ ! -e "$TMP/invalid-wall.calls" ]]
done

# GNU timeout must terminate the owned process group at either backend stage,
# including the real Qdrant launcher's existing Docker cleanup trap.
mv "$REPO/scripts/bench_minima_qdrant.sh" "$TMP/qdrant-stub"
cp "$ROOT/scripts/bench_minima_qdrant.sh" "$REPO/scripts/bench_minima_qdrant.sh"
cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_DOCKER_CALLS"
case "$1" in
  run) echo owned-container ;;
  inspect) echo 12345 ;;
esac
EOF
chmod +x "$FAKE_BIN/docker"
sleep 30 &
unrelated_pid=$!
for stage in treedb qdrant; do
	set +e
	"${measured_env[@]}" RUN_DIR="$TMP/measured-timeout-$stage" MINIMA_WALL_SECONDS=1 \
		FAKE_HANG_STAGE="$stage" FAKE_HANG_CHILD_PID="$TMP/$stage-timeout.pid" \
		FAKE_PYTHON_CALLS="$TMP/$stage-timeout.calls" FAKE_DOCKER_CALLS="$TMP/$stage-docker.calls" \
		QDRANT_URL= QDRANT_BIN= QDRANT_RESTART_HOOK= QDRANT_CPUSET_CPUS=2,4 \
		"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/$stage-timeout.log" 2>&1
	timeout_status=$?
	set -e
	[[ "$timeout_status" == 124 ]]
	IFS= read -r timed_child_pid <"$TMP/$stage-timeout.pid"
	timed_child_state=$(ps -o stat= -p "$timed_child_pid" || true)
	[[ -z "$timed_child_state" || "$timed_child_state" == Z* ]]
	kill -0 "$unrelated_pid"
	[[ "$(grep -c 'minima_treedb_runner.py' "$TMP/$stage-timeout.calls")" == 1 ]]
	if [[ "$stage" == qdrant ]]; then
		[[ "$(grep -c 'minima_qdrant_runner.py' "$TMP/$stage-timeout.calls")" == 1 ]]
		[[ "$(tail -1 "$TMP/$stage-docker.calls")" == 'rm -f gomap-minima-qdrant-'* ]]
	else
		[[ ! -e "$TMP/$stage-docker.calls" ]]
	fi
done
kill "$unrelated_pid"
wait "$unrelated_pid" 2>/dev/null || true
unrelated_pid=""
mv "$TMP/qdrant-stub" "$REPO/scripts/bench_minima_qdrant.sh"

set +e
PATH="$FAKE_BIN:$PATH" MODE=measured RUN_DIR="$TMP/measured" MINIMA_WALL_SECONDS=10 \
	TREEDB_COLLECTION=frozen_treedb QDRANT_COLLECTION=frozen_qdrant \
	MANIFEST_PATH="$TMP/supplied-manifest" MINIMA_FREEZE="$TMP/freeze" \
	MINIMA_EXPECTED_FREEZE_SHA256=$(printf '%064d' 0) VENV="$TMP/pinned-venv" \
	TREEDB_SERVICE_BIN="$TMP/service" MINIMA_COMPARATOR_BIN="$TMP/comparator" \
	TREEDB_STRATEGY=column_graph TREEDB_TRANSPORT=native TREEDB_NATIVE_ADDRESS=127.0.0.1:17122 \
	TREEDB_COLUMN_GRAPH_SERVING="$TMP/serving" TREEDB_DIAGNOSTICS_URL=http://127.0.0.1:17123 \
	FAKE_GO_INVOKED="$TMP/measured-go" FAKE_PYTHON_ARGS="$TMP/measured-args" \
	FAKE_QDRANT_INPUTS="$TMP/measured-qdrant-inputs" \
	"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/measured.log" 2>&1
measured_status=$?
set -e
[[ "$measured_status" == 1 ]] # the deliberately failing comparator is retained
[[ ! -e "$TMP/measured-go" && ! -d "$TMP/measured/bin" ]]
[[ -f "$TMP/measured/treedb_backend.json" && -f "$TMP/measured/qdrant_backend.json" ]]
[[ "$(cat "$TMP/supplied-manifest")" == "$manifest_before" ]]
grep -qx -- '--measured' "$TMP/measured-args"
grep -qx -- '--native-address' "$TMP/measured-args"
grep -qx -- 'http://127.0.0.1:17123' "$TMP/measured-args"
grep -qx -- "$TMP/supplied-manifest" "$TMP/measured-qdrant-inputs"
grep -qx -- "$TMP/pinned-venv" "$TMP/measured-qdrant-inputs"
grep -qx -- 'true' "$TMP/measured-qdrant-inputs"
grep -qx -- 'frozen_treedb' "$TMP/measured-args"
grep -qx -- 'frozen_qdrant' "$TMP/measured-qdrant-inputs"

# The wrapper changes to the repository root before re-exec; a caller's
# relative script path must still resolve to the same launcher exactly once.
set +e
(
	cd "$REPO/scripts"
	"${measured_env[@]}" RUN_DIR="$TMP/measured-relative" MINIMA_WALL_SECONDS=10 \
		FAKE_PYTHON_CALLS="$TMP/measured-relative.calls" \
		./bench_minima_qualification.sh
) >"$TMP/measured-relative.log" 2>&1
relative_status=$?
set -e
[[ "$relative_status" == 1 ]] # the deliberately failing comparator is retained
[[ -f "$TMP/measured-relative/minima_qualification.json" ]]
[[ "$(grep -c 'minima_treedb_runner.py' "$TMP/measured-relative.calls")" == 1 ]]
grep -q 'qualification failed: TreeDB=0 Qdrant=0 comparator=7' "$TMP/measured-relative.log"

set +e
PATH="$FAKE_BIN:$PATH" MODE=measured RUN_DIR="$TMP/measured" MINIMA_WALL_SECONDS=10 \
	TREEDB_COLLECTION=frozen_treedb QDRANT_COLLECTION=frozen_qdrant \
	MANIFEST_PATH="$TMP/supplied-manifest" MINIMA_FREEZE="$TMP/freeze" \
	MINIMA_EXPECTED_FREEZE_SHA256=$(printf '%064d' 0) VENV="$TMP/pinned-venv" \
	TREEDB_SERVICE_BIN="$TMP/service" MINIMA_COMPARATOR_BIN="$TMP/comparator" \
	"$REPO/scripts/bench_minima_qualification.sh" >"$TMP/measured-reuse.log" 2>&1
reuse_status=$?
set -e
[[ "$reuse_status" == 2 ]]
grep -q 'fresh empty RUN_DIR' "$TMP/measured-reuse.log"

for mode in representative small diagnostic-resume; do
	build_marker="$TMP/override-$mode-build"
	python_marker="$TMP/override-$mode-python"
	set +e
	override_output=$(PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" MODE="$mode" \
		TREEDB_OPERATION_TIMEOUT=121 RUN_DIR="$TMP/override-$mode" \
		FAKE_GO_INVOKED="$build_marker" FAKE_PYTHON_ARGS="$python_marker" \
		"$REPO/scripts/bench_minima_qualification.sh" 2>&1)
	override_status=$?
	set -e
	[[ "$override_status" == 2 ]]
	[[ "$override_output" == *"TREEDB_OPERATION_TIMEOUT must be exactly 120"* ]]
	[[ ! -e "$build_marker" ]]
	[[ ! -e "$python_marker" ]]
done

set +e
mismatch_output=$(PATH="$FAKE_BIN:$PATH" MINIMA_EXPECTED_COMMIT=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
	RUN_DIR="$TMP/mismatch" "$REPO/scripts/bench_minima_qualification.sh" 2>&1)
mismatch_status=$?
set -e
[[ "$mismatch_status" == 2 ]]
[[ "$mismatch_output" == *"does not match frozen target"* ]]

set +e
small_output=$(PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" MODE=small \
	FAKE_PYTHON_STATUS=9 TMPDIR="$TMP" "$REPO/scripts/bench_minima_qualification.sh" 2>&1)
small_status=$?
set -e
[[ "$small_status" == 9 ]]
[[ "$small_output" == *"small manifest: $TMP/gomap_minima_qualification_"* ]]
[[ "$small_output" == *"validated partial TreeDB evidence: $TMP/gomap_minima_qualification_"* ]]
generated_runs=("$TMP"/gomap_minima_qualification_*)
[[ "${#generated_runs[@]}" == 1 && -d "${generated_runs[0]}" ]]

cat >"$TMP/fake-qdrant.go" <<'EOF'
package main

import "time"

func main() {
	for {
		time.Sleep(time.Hour)
	}
}
EOF
go build -o "$TMP/fake-qdrant" "$TMP/fake-qdrant.go"
mkdir -p "$TMP/qdrant-storage"
sleep 30 &
unrelated_pid=$!
printf '%s\n%s\n' "$unrelated_pid" "deliberately-wrong-process-identity" >"$TMP/qdrant.pid"
replacement_pid=$("$ROOT/scripts/restart_minima_qdrant_backend.sh" standalone \
	"$TMP/fake-qdrant" 12345 "$TMP/qdrant-storage" "$TMP/qdrant.log" "$TMP/qdrant.pid" \
	2>"$TMP/restart.err")
kill -0 "$unrelated_pid"
[[ "$(<"$TMP/restart.err")" == *"refusing to signal that process"* ]]
IFS= read -r recorded_pid <"$TMP/qdrant.pid"
[[ "$recorded_pid" == "$replacement_pid" ]]
recorded_identity=$(sed -n '2p' "$TMP/qdrant.pid")
[[ "$recorded_identity" == "$(ps -o lstart= -o command= -p "$replacement_pid")" ]]
recorded_command=$(ps -o command= -p "$replacement_pid")
recorded_command=${recorded_command#"${recorded_command%%[![:space:]]*}"}
recorded_command=${recorded_command%"${recorded_command##*[![:space:]]}"}
[[ "$recorded_command" == "$TMP/fake-qdrant" ]]
kill "$replacement_pid"
wait "$replacement_pid" 2>/dev/null || true
replacement_pid=""
kill "$unrelated_pid"
wait "$unrelated_pid" 2>/dev/null || true
unrelated_pid=""
