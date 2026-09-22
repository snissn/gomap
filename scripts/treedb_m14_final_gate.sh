#!/usr/bin/env bash
# Run the TreeDB M14 final-gate unified-bench matrix into an out-of-repo profile artifact root.
#
# Intended remote usage for #2774:
#   ROOT=/mnt/fast4tb/gomap-profiles COMMIT=$(git rev-parse HEAD) \
#     scripts/treedb_m14_final_gate.sh
#
# The script runs 10M-key TreeDB write profiles and then invokes
# scripts/treedb_m14_matrix_summary.py to produce m14_matrix_summary.{md,json}.
# Since #2788 changed unified-bench's default flush-admission policy to auto,
# all non-default explicit comparison rows below pass
# -treedb-flush-admission-policy=explicit so they are not normalized to the
# current default candidate shape.
# `unified-bench -profile-dir` auto-runs benchprof; set
# RUN_MANUAL_BENCHPROF=true to also run the explicit second pass used by older
# runbooks.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
SUMMARY_SCRIPT_SRC=$SCRIPT_DIR/treedb_m14_matrix_summary.py
if [[ ! -f "$SUMMARY_SCRIPT_SRC" ]]; then
  echo "missing summary parser: $SUMMARY_SCRIPT_SRC" >&2
  exit 2
fi
PRESERVED_SUMMARY_SCRIPT=$(mktemp)
trap 'rm -f "$PRESERVED_SUMMARY_SCRIPT"' EXIT
cp "$SUMMARY_SCRIPT_SRC" "$PRESERVED_SUMMARY_SCRIPT"

ROOT=${ROOT:-/mnt/fast4tb/gomap-profiles}
REPO=${REPO:-$(pwd)}
COMMIT=${COMMIT:-$(git -C "$REPO" rev-parse HEAD)}
STAMP=${STAMP:-$(date -u +%Y%m%d_%H%M%S)}
RUN_ROOT=${RUN_ROOT:-$ROOT/2774_m14_matrix_${STAMP}}
RUN_MANUAL_BENCHPROF=${RUN_MANUAL_BENCHPROF:-false}
LOG=$RUN_ROOT/gate.log

mkdir -p "$RUN_ROOT"
REPO_ABS=$(cd "$REPO" && pwd -P)
RUN_ROOT_ABS=$(cd "$RUN_ROOT" && pwd -P)
case "$RUN_ROOT_ABS/" in
  "$REPO_ABS"/*)
    echo "RUN_ROOT must be outside the repository because the script runs git clean -fdx: $RUN_ROOT_ABS" >&2
    exit 2
    ;;
esac
exec > >(tee -a "$LOG") 2>&1

printf "run_root=%s\n" "$RUN_ROOT"
printf "host=%s\n" "$(hostname)"
printf "date_utc=%s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf "go=%s\n" "$(go version)"
printf "git=%s\n" "$(git --version)"
printf "nproc=%s\n" "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
printf "kernel=%s\n" "$(uname -a)"
printf "repo=%s\n" "$REPO"
printf "run_manual_benchprof=%s\n" "$RUN_MANUAL_BENCHPROF"
cp "$PRESERVED_SUMMARY_SCRIPT" "$RUN_ROOT/treedb_m14_matrix_summary.py"

cd "$REPO"
git checkout --force "$COMMIT"
git clean -fdx
printf "checked_out=%s\n" "$(git rev-parse HEAD)"
git status --short --branch
make unified-bench benchprof

COMMON_ARGS=(
  -dbs treedb
  -test sequential_write,batch_random,random_write
  -keys 10000000
  -valsize 128
  -batchsize 8000
  -path-label m8-m14-10mm-gate
  -treedb-journal-lanes=1
  -checkpoint-between-tests
  -progress=false
)
APPLY_ARGS_BASE=(
  -treedb-flush-apply-min-entries=1
  -treedb-flush-apply-min-spans=1
  -treedb-flush-apply-min-bytes=1
)

write_meta() {
  local out=$1 label=$2 concurrency=$3 span_native=$4 backlog=$5 cache_mode=$6 admission_policy=$7 note=$8
  cat > "$out/variant.env" <<EOF_META
label=$label
commit=$COMMIT
concurrency=$concurrency
span_native=$span_native
backlog_coalescing=$backlog
cache_mode=$cache_mode
flush_admission_policy=$admission_policy
note=$note
EOF_META
}

run_one() {
  local label=$1; shift
  local concurrency=$1; shift
  local span_native=$1; shift
  local backlog=$1; shift
  local cache_mode=$1; shift
  local note=$1; shift
  local out="$RUN_ROOT/$label"
  mkdir -p "$out"
  printf "=== variant=%s concurrency=%s span_native=%s backlog=%s cache=%s out=%s ===\n" \
    "$label" "$concurrency" "$span_native" "$backlog" "$cache_mode" "$out"
  git rev-parse HEAD > "$out/COMMIT"
  git status --short --branch > "$out/git_status.txt"
  local admission_policy=auto_default
  if [[ "$label" != "default_unconfigured" ]]; then
    admission_policy=explicit
  fi
  write_meta "$out" "$label" "$concurrency" "$span_native" "$backlog" "$cache_mode" "$admission_policy" "$note"

  local args=("${COMMON_ARGS[@]}" -profile-dir "$out")
  if [[ "$admission_policy" == "explicit" ]]; then
    args+=(-treedb-flush-admission-policy=explicit)
  fi
  if [[ "$concurrency" != "default" ]]; then
    args+=("${APPLY_ARGS_BASE[@]}" -treedb-flush-apply-concurrency="$concurrency")
  fi
  if [[ "$span_native" == "true" ]]; then
    args+=(-treedb-flush-apply-span-native)
  fi
  if [[ "$backlog" == "true" ]]; then
    args+=(-treedb-flush-backlog-coalescing)
  fi
  case "$cache_mode" in
    default) ;;
    disabled) args+=(-treedb-leaf-page-read-cache-entries=-1) ;;
    large) args+=(-treedb-leaf-page-read-cache-entries=262144) ;;
    *) echo "unknown cache_mode=$cache_mode" >&2; return 2 ;;
  esac

  printf "./bin/unified-bench" > "$out/command.sh"
  printf " \\\n  %q" "${args[@]}" >> "$out/command.sh"
  printf "\n" >> "$out/command.sh"
  chmod +x "$out/command.sh"
  /usr/bin/time -v ./bin/unified-bench "${args[@]}" > "$out/unified_bench.stdout.md" 2> "$out/unified_bench.stderr.log"
  if [[ "$RUN_MANUAL_BENCHPROF" == "true" ]]; then
    /usr/bin/time -v ./bin/benchprof -profiles-dir "$out" > "$out/benchprof_manual.log" 2>&1
  fi
  printf "completed %s\n" "$label"
}

run_one default_unconfigured default false false default \
  "current default knobs; primary command without opt-in apply/span/backlog flags"
run_one legacy_parallel_c4 4 false false default \
  "M2/M9 recursive worker apply path at c4; span-native/backlog disabled"
run_one span_native_c1 1 true true default \
  "span-native plus M11 backlog coalescing; default leaf cache"
run_one span_native_c2 2 true true default \
  "span-native plus M11 backlog coalescing; default leaf cache"
run_one span_native_c4 4 true true default \
  "span-native plus M11 backlog coalescing; default leaf cache"
run_one span_native_c8 8 true true default \
  "span-native plus M11 backlog coalescing; default leaf cache"
run_one span_native_c16 16 true true default \
  "span-native plus M11 backlog coalescing; default leaf cache"
run_one span_native_c4_no_backlog 4 true false default \
  "span-native enabled; M11 backlog coalescing disabled"
run_one span_native_c4_cache_disabled 4 true true disabled \
  "span-native plus backlog coalescing; outer leaf read cache disabled"

python3 "$RUN_ROOT/treedb_m14_matrix_summary.py" "$RUN_ROOT" --baseline-label default_unconfigured
printf "M14 matrix done %s\nRUN_ROOT=%s\nLOG=%s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ROOT" "$LOG"
