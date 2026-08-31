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
mkdir -p "$RUN_DIR"
{ echo "commit=$(git rev-parse HEAD)"; echo "command=$0 $*"; echo "host=$(uname -a)"; echo "go=$(go version)"; } > "$RUN_DIR/context.txt"
if [[ "$PROFILE_MODE" != none && ! ",$PHASES," =~ ,"$PROFILE_PHASE", ]]; then echo "PROFILE_PHASE must be selected by PHASES" >&2; exit 2; fi

run_matrix() {
  local rows="$1" phase phase_dir before after start elapsed
  IFS=',' read -ra selected <<< "$PHASES"
  for phase in "${selected[@]}"; do
    case "$phase" in load|vector|phrase|broad|maintenance|reopen) ;; *) echo "unknown phase: $phase" >&2; return 2;; esac
    phase_dir="$RUN_DIR/${rows}/${phase}"; mkdir -p "$phase_dir"
    if [[ "$DRY_RUN" == true ]]; then continue; fi
    before=$(du -sk "$phase_dir" | awk '{print $1}'); start=$(date +%s)
    env GOWORK=off TREEDB_TEXT_PROFILE_PHASE="$phase" TREEDB_TEXT_PROFILE_MODE="$([[ "$PROFILE_PHASE" == "$phase" ]] && echo "$PROFILE_MODE" || echo none)" TREEDB_TEXT_PROFILE_DIR="$phase_dir/profiles" TREEDB_TEXT_PROFILE_TINY="$([[ "$TINY_SMOKE" == true ]] && echo 1 || echo 0)" go test ./cmd/treedb_text_hybrid_scale -run '^TestManualTextHybridScaleProfile4546$' -count=1 -timeout "$TIMEOUT" 2>&1 | tee "$phase_dir/phase.log"
    elapsed=$(( $(date +%s) - start )); after=$(du -sk "$phase_dir" | awk '{print $1}')
    printf 'phase=%s\nrows=%s\nsetup=logged before measured boundary\nelapsed_seconds=%s\nfilesystem_kib_before=%s\nfilesystem_kib_after=%s\n' "$phase" "$rows" "$elapsed" "$before" "$after" > "$phase_dir/observations.txt"
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == runtime ]]; then for artifact in cpu.pprof trace.out block.pprof mutex.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == alloc ]]; then for artifact in alloc_before.pprof alloc_after.pprof heap_after.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
  done
}

run_matrix 10000
if [[ "$ROWS" == 100000 ]]; then run_matrix 100000; fi
echo "artifacts: $RUN_DIR"
