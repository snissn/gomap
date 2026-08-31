#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
RUN_DIR="${RUN_DIR:-/tmp/gomap_text_hybrid_profile_$(date +%Y%m%d_%H%M%S)}"
ROWS="${ROWS:-10000}"
PHASES="${PHASES:-load,vector,phrase,broad,maintenance,reopen}"
TIMEOUT="${TIMEOUT:-20m}"
PROFILE_MODE="${PROFILE_MODE:-none}"
PROFILE_PHASE="${PROFILE_PHASE:-}"
RUN_100K="${RUN_100K:-false}"
TINY_SMOKE="${TINY_SMOKE:-false}"
DRY_RUN="${DRY_RUN:-false}"

case "$ROWS" in 10000|100000) ;; *) echo "ROWS must be 10000 or 100000" >&2; exit 2;; esac
case "$PROFILE_MODE" in none|runtime|alloc) ;; *) echo "PROFILE_MODE must be none, runtime, or alloc" >&2; exit 2;; esac
if [[ "$ROWS" == 100000 && "$RUN_100K" != true ]]; then echo "100k requires RUN_100K=true after this invocation's 10k matrix" >&2; exit 2; fi
if [[ -n "$(git status --porcelain)" ]]; then echo "worktree must be clean before profiling" >&2; exit 2; fi
if [[ -L "$RUN_DIR" || ( -e "$RUN_DIR" && ! -d "$RUN_DIR" ) ]]; then echo "RUN_DIR must be an empty directory: $RUN_DIR" >&2; exit 2; fi
mkdir -p "$RUN_DIR"
if [[ -n "$(find "$RUN_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then echo "RUN_DIR must be empty: $RUN_DIR; use a fresh RUN_DIR" >&2; exit 2; fi
{ echo "commit=$(git rev-parse HEAD)"; echo "command=$0 $*"; echo "host=$(uname -a)"; echo "go=$(go version)"; } > "$RUN_DIR/context.txt"
if [[ "$PROFILE_MODE" != none && ! ",$PHASES," =~ ,"$PROFILE_PHASE", ]]; then echo "PROFILE_PHASE must be selected by PHASES" >&2; exit 2; fi

run_matrix() {
  local rows="$1" phase phase_dir artifact_before artifact_after db_before db_after measured_seconds test_rows start elapsed mode godebug
  IFS=',' read -ra selected <<< "$PHASES"
  for phase in "${selected[@]}"; do
    case "$phase" in load|vector|phrase|broad|maintenance|reopen) ;; *) echo "unknown phase: $phase" >&2; return 2;; esac
    phase_dir="$RUN_DIR/${rows}/${phase}"
    mkdir -p "$RUN_DIR/${rows}"
    if [[ -e "$phase_dir" || -L "$phase_dir" ]]; then
      echo "phase artifact directory already exists: $phase_dir; use a fresh RUN_DIR" >&2
      return 2
    fi
    mkdir -p "$phase_dir"
    mode=none; if [[ "$PROFILE_PHASE" == "$phase" ]]; then mode="$PROFILE_MODE"; fi
    if [[ "$mode" == alloc ]]; then
      godebug="${GODEBUG:+${GODEBUG},}memprofilerate=1"
      cmd=(env GOWORK=off GODEBUG="$godebug" TREEDB_TEXT_PROFILE_PHASE="$phase" TREEDB_TEXT_PROFILE_ROWS="$rows" TREEDB_TEXT_PROFILE_MODE="$mode" TREEDB_TEXT_PROFILE_DIR="$phase_dir/profiles" TREEDB_TEXT_PROFILE_TINY="$([[ "$TINY_SMOKE" == true ]] && echo 1 || echo 0)" go test ./cmd/treedb_text_hybrid_scale -run '^TestManualTextHybridScaleProfile4546$' -count=1 -v -timeout "$TIMEOUT")
    else
      cmd=(env GOWORK=off TREEDB_TEXT_PROFILE_PHASE="$phase" TREEDB_TEXT_PROFILE_ROWS="$rows" TREEDB_TEXT_PROFILE_MODE="$mode" TREEDB_TEXT_PROFILE_DIR="$phase_dir/profiles" TREEDB_TEXT_PROFILE_TINY="$([[ "$TINY_SMOKE" == true ]] && echo 1 || echo 0)" go test ./cmd/treedb_text_hybrid_scale -run '^TestManualTextHybridScaleProfile4546$' -count=1 -v -timeout "$TIMEOUT")
    fi
    printf '%q ' "${cmd[@]}" > "$phase_dir/command.txt"; echo >> "$phase_dir/command.txt"
    if [[ "$DRY_RUN" == true ]]; then continue; fi
    artifact_before=$(du -sk "$phase_dir" | awk '{print $1}'); start=$(date +%s)
    "${cmd[@]}" 2>&1 | tee "$phase_dir/phase.log"
    elapsed=$(( $(date +%s) - start )); artifact_after=$(du -sk "$phase_dir" | awk '{print $1}')
    db_before=$(awk -F'db_bytes_before=' '/db_bytes_before=/{print $2}' "$phase_dir/phase.log" | tail -1)
    db_after=$(awk -F'db_bytes_after=' '/db_bytes_after=/{print $2}' "$phase_dir/phase.log" | tail -1)
    measured_seconds=$(awk -F'measured_seconds=' '/measured_seconds=/{print $2}' "$phase_dir/phase.log" | awk '{print $1}' | tail -1)
    test_rows=$(sed -n 's/.* rows=\([0-9][0-9]*\) setup_complete=.*/\1/p' "$phase_dir/phase.log" | tail -1)
    printf 'phase=%s\nmatrix_rows=%s\ntest_rows=%s\nsetup=logged before measured boundary\nmeasured_seconds=%s\nprocess_elapsed_seconds=%s\ndb_bytes_before=%s\ndb_bytes_after=%s\nartifact_kib_before=%s\nartifact_kib_after=%s\n' "$phase" "$rows" "$test_rows" "$measured_seconds" "$elapsed" "$db_before" "$db_after" "$artifact_before" "$artifact_after" > "$phase_dir/observations.txt"
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == runtime ]]; then for artifact in cpu.pprof trace.out block.pprof mutex.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == alloc ]]; then for artifact in alloc_before.pprof alloc_after.pprof alloc_delta.txt heap_after.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
  done
}

run_matrix 10000
if [[ "$ROWS" == 100000 ]]; then run_matrix 100000; fi
echo "artifacts: $RUN_DIR"
