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
while (($#)); do
	case "$1" in
	-dump-minima-manifest) manifest=$2; shift 2 ;;
	-minima-output) output=$2; shift 2 ;;
	-validate-minima-artifact) validate=$2; shift 2 ;;
	-minima-report) report=$2; shift 2 ;;
	*) shift ;;
	esac
done
if [[ -n "$manifest" ]]; then
	printf '{}\n' >"$manifest"
	exit 0
fi
if [[ -n "$validate" ]]; then
	exit 0
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

cat >"$FAKE_BIN/python" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
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
printf '{}\n' >"$OUTPUT_PATH"
EOF
chmod +x "$REPO/scripts/bench_minima_qdrant.sh"

set +e
output=$(PATH="$FAKE_BIN:$PATH" PYTHON="$FAKE_BIN/python" QDRANT_BIN=/bin/true RUN_DIR="$RUN_DIR" \
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

cat >"$TMP/fake-qdrant" <<'EOF'
#!/usr/bin/env bash
trap 'exit 0' TERM INT
while :; do
	sleep 1
done
EOF
chmod +x "$TMP/fake-qdrant"
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
[[ "$(sed -n '2p' "$TMP/qdrant.pid")" == *"$TMP/fake-qdrant"* ]]
kill "$replacement_pid"
wait "$replacement_pid" 2>/dev/null || true
replacement_pid=""
kill "$unrelated_pid"
wait "$unrelated_pid" 2>/dev/null || true
unrelated_pid=""
