#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
RUN_DIR="${RUN_DIR:-/tmp/gomap_text_hybrid_profile_$(date +%Y%m%d_%H%M%S)}"
ROWS="${ROWS:-10000}"
PHASES="${PHASES-load,vector,phrase,broad,maintenance,reopen}"
TIMEOUT="${TIMEOUT:-20m}"
PROFILE_MODE="${PROFILE_MODE:-none}"
PROFILE_PHASE="${PROFILE_PHASE:-}"
RUN_100K="${RUN_100K:-false}"
TINY_SMOKE="${TINY_SMOKE:-false}"
DRY_RUN="${DRY_RUN:-false}"
treedb_performance_env=(TREEDB_LEAF_PAGE_CACHE_ENTRIES TREEDB_COLUMN_STORE_TYPED_COMPRESSION TREEDB_COLUMN_STORE_TYPED_SECTION_COMPRESSION TREEDB_COLUMN_STORE_TYPED_LOCATOR_COMPRESSION TREEDB_COLUMN_STORE_TYPED_DICTIONARY_COMPRESSION TREEDB_COLUMN_STORE_TYPED_PRUNING_COMPRESSION TREEDB_COLUMN_STORE_TYPED_INT64_ENCODING TREEDB_COLUMN_STORE_TYPED_ROWS_PER_GRANULE TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_ENABLED TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_TARGET_BYTES TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MIN_ROWS TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MAX_ROWS TREEDB_DEBUG_COMMIT_TIMING TREEDB_DEBUG_VLOG_TIMINGS TREEDB_DEBUG_VLOG_TIMINGS_MIN_MS TREEDB_DEBUG_VLOG_TIMINGS_BUDGET TREEDB_DEBUG_MEMTABLE_ROTATE TREEDB_DEBUG_MEMTABLE_ROTATE_BUDGET TREEDB_DEBUG_VLOG_SHAPE TREEDB_DEBUG_VLOG_SHAPE_DISK TREEDB_DEBUG_VLOG_SHAPE_INTERVAL_MS TREEDB_DEBUG_VLOG_SHAPE_BUDGET TREEDB_DEBUG_VLOG_MAINT TREEDB_DEBUG_VLOG_MAINT_BUDGET TREEDB_OUTER_LEAF_READ_SAMPLE_MOD TREEDB_HOT_PATH_STATS TREEDB_VLOG_MAX_DEAD_MAPPINGS TREEDB_VLOG_ADAPTIVE_DEAD_MAPPINGS TREEDB_VLOG_ENABLE_CURRENT_WRITABLE_MMAP TREEDB_VLOG_ENABLE_CURRENT_LEAF_WRITABLE_MMAP TREEDB_VLOG_CURRENT_WRITABLE_MMAP_TARGET_BYTES TREEDB_VLOG_MAX_MAPPED_SEALED_SEGMENTS TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_SEGMENTS TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_BYTES)
controlled_go_env=(env -u GOMAXPROCS -u GOGC -u GOMEMLIMIT -u GOOS -u GOARCH -u GOAMD64 -u GOARM64 -u GO386 -u GOARM -u GOMIPS -u GOMIPS64 -u GOPPC64 -u GORISCV64 -u GOWASM -u GOEXPERIMENT -u CGO_ENABLED)
for treedb_env in "${treedb_performance_env[@]}"; do controlled_go_env+=(-u "$treedb_env"); done
controlled_go_env+=(GOWORK=off GOFLAGS= GOENV=off)
controlled_git_env=(env -u GIT_DIR -u GIT_WORK_TREE -u GIT_INDEX_FILE -u GIT_OBJECT_DIRECTORY -u GIT_ALTERNATE_OBJECT_DIRECTORIES -u GIT_COMMON_DIR -u GIT_CONFIG_COUNT -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_GLOBAL -u GIT_CONFIG_SYSTEM -u GIT_CONFIG_NOSYSTEM)
while IFS= read -r local_git_env_var; do
  controlled_git_env+=(-u "$local_git_env_var")
done < <("${controlled_git_env[@]}" git rev-parse --local-env-vars)

check_profile_source() {
  local status ignored_build_input="" entry path ignored_status_file
  if ! status=$("${controlled_git_env[@]}" git status --porcelain --untracked-files=all); then
    echo "git status failed before profiling" >&2
    return 1
  fi
  if [[ -n "$status" ]]; then echo "worktree must be clean before profiling" >&2; return 1; fi
  if ! ignored_status_file=$(mktemp); then
    echo "could not create ignored-status file before profiling" >&2
    return 1
  fi
  if ! "${controlled_git_env[@]}" git status --porcelain=v1 -z --untracked-files=all --ignored > "$ignored_status_file"; then
    rm -f "$ignored_status_file"
    echo "git status failed before profiling" >&2
    return 1
  fi
  while IFS= read -r -d '' entry; do
    [[ "$entry" == "!! "* ]] || continue
    path=${entry#!! }
    if [[ "$path" =~ \.(go|s|S|c|cc|cpp|cxx|h|hh|hpp|syso)$ ]]; then
      ignored_build_input=$path
      break
    fi
  done < "$ignored_status_file"
  rm -f "$ignored_status_file"
  if [[ -n "$ignored_build_input" ]]; then printf 'ignored build input before profiling: %q\n' "$ignored_build_input" >&2; return 1; fi
}

case "$ROWS" in 10000|100000) ;; *) echo "ROWS must be 10000 or 100000" >&2; exit 2;; esac
case "$PROFILE_MODE" in none|runtime|alloc) ;; *) echo "PROFILE_MODE must be none, runtime, or alloc" >&2; exit 2;; esac
if [[ "$ROWS" == 100000 && "$RUN_100K" != true ]]; then echo "100k requires RUN_100K=true after this invocation's 10k matrix" >&2; exit 2; fi
if [[ -z "$PHASES" ]]; then echo "PHASES must not be empty" >&2; exit 2; fi
if [[ "$PROFILE_MODE" != none && -z "$PROFILE_PHASE" ]]; then echo "PROFILE_PHASE is required with PROFILE_MODE=$PROFILE_MODE" >&2; exit 2; fi
IFS=',' read -ra configured_phases <<< "$PHASES"
profile_phase_selected=false
seen_phases=""
for phase in "${configured_phases[@]}"; do
  case "$phase" in load|vector|phrase|broad|maintenance|reopen) ;; *) echo "unknown phase: $phase" >&2; exit 2;; esac
  if [[ ",$seen_phases" == *,"$phase",* ]]; then echo "duplicate phase: $phase" >&2; exit 2; fi
  seen_phases+="$phase,"
  if [[ "$phase" == "$PROFILE_PHASE" ]]; then profile_phase_selected=true; fi
done
if [[ "$PROFILE_MODE" == none && -n "$PROFILE_PHASE" ]]; then echo "PROFILE_PHASE requires PROFILE_MODE=runtime or alloc" >&2; exit 2; fi
if [[ -n "$PROFILE_PHASE" && "$profile_phase_selected" != true ]]; then echo "PROFILE_PHASE must be selected by PHASES" >&2; exit 2; fi
if ! check_profile_source; then exit 2; fi
profile_commit=$("${controlled_git_env[@]}" git rev-parse HEAD)
if [[ -L "$RUN_DIR" || ( -e "$RUN_DIR" && ! -d "$RUN_DIR" ) ]]; then echo "RUN_DIR must be an empty directory: $RUN_DIR" >&2; exit 2; fi
mkdir -p "$RUN_DIR"
if (shopt -s nullglob dotglob; entries=("$RUN_DIR"/*); ((${#entries[@]}))); then echo "RUN_DIR must be empty: $RUN_DIR; use a fresh RUN_DIR" >&2; exit 2; fi
{
  echo "commit=$profile_commit"
  printf 'command='
  printf '%q ' "$0" "$@"
  printf '\n'
  echo "host=$(uname -a)"
  echo "go=$("${controlled_go_env[@]}" go version)"
  echo "goflags=cleared"
  echo "goenv=off"
  echo "gomaxprocs=cleared"
  echo "gogc=cleared"
  echo "gomemlimit=cleared"
  echo "compiler_tuning=cleared GOOS,GOARCH,GOAMD64,GOARM64,GO386,GOARM,GOMIPS,GOMIPS64,GOPPC64,GORISCV64,GOWASM,GOEXPERIMENT,CGO_ENABLED"
  echo "treedb_performance_overrides=cleared ${treedb_performance_env[*]}"
  if [[ -n "${GODEBUG:-}" ]]; then printf 'godebug_shell_escaped=%q\n' "$GODEBUG"; fi
} > "$RUN_DIR/context.txt"

run_matrix() {
  local rows="$1" phase phase_dir artifact_before artifact_after db_before db_after db_filesystem db_mount measured_seconds test_rows elapsed elapsed_file mode godebug
  IFS=',' read -ra selected <<< "$PHASES"
  for phase in "${selected[@]}"; do
    case "$phase" in load|vector|phrase|broad|maintenance|reopen) ;; *) echo "unknown phase: $phase" >&2; return 2;; esac
    phase_dir="$RUN_DIR/${rows}/${phase}"
    if ! check_profile_source; then return 2; fi
    if [[ "$("${controlled_git_env[@]}" git rev-parse HEAD)" != "$profile_commit" ]]; then
      echo "source commit changed before phase: $phase" >&2
      return 2
    fi
    mkdir -p "$RUN_DIR/${rows}"
    if [[ -e "$phase_dir" || -L "$phase_dir" ]]; then
      echo "phase artifact directory already exists: $phase_dir; use a fresh RUN_DIR" >&2
      return 2
    fi
    mkdir -p "$phase_dir"
    mode=none; if [[ "$PROFILE_PHASE" == "$phase" ]]; then mode="$PROFILE_MODE"; fi
    godebug="${GODEBUG:-}"
    if [[ "$mode" == alloc ]]; then
      godebug="${GODEBUG:+${GODEBUG},}memprofilerate=1"
    fi
    cmd=("${controlled_go_env[@]}")
    if [[ -n "$godebug" ]]; then cmd+=(GODEBUG="$godebug"); fi
    cmd+=(TREEDB_TEXT_PROFILE_PHASE="$phase" TREEDB_TEXT_PROFILE_ROWS="$rows" TREEDB_TEXT_PROFILE_MODE="$mode" TREEDB_TEXT_PROFILE_DIR="$phase_dir/profiles" TREEDB_TEXT_PROFILE_TINY="$([[ "$TINY_SMOKE" == true ]] && echo 1 || echo 0)" go test ./cmd/treedb_text_hybrid_scale -run '^TestManualTextHybridScaleProfile4546$' -count=1 -v -timeout "$TIMEOUT")
    printf '%q ' "${cmd[@]}" > "$phase_dir/command.txt"; echo >> "$phase_dir/command.txt"
    if [[ "$DRY_RUN" == true ]]; then continue; fi
    artifact_before=$(du -sk "$phase_dir" | awk '{print $1}')
    elapsed_file=$(mktemp "$phase_dir/process_elapsed.XXXXXX")
    TIMEFORMAT='%R'
    if ! { time "${cmd[@]}" 2>&1 | tee "$phase_dir/phase.log"; } 2>"$elapsed_file"; then
      rm "$elapsed_file"
      return 1
    fi
    if ! check_profile_source; then
      rm "$elapsed_file"
      return 2
    fi
    if [[ "$("${controlled_git_env[@]}" git rev-parse HEAD)" != "$profile_commit" ]]; then
      rm "$elapsed_file"
      echo "source commit changed after phase: $phase" >&2
      return 2
    fi
    elapsed=$(<"$elapsed_file")
    rm "$elapsed_file"
    artifact_after=$(du -sk "$phase_dir" | awk '{print $1}')
    db_before=$(awk -F'db_bytes_before=' '/db_bytes_before=/{print $2}' "$phase_dir/phase.log" | awk '{print $1}' | tail -1)
    db_after=$(awk -F'db_bytes_after=' '/db_bytes_after=/{print $2}' "$phase_dir/phase.log" | tail -1)
    db_filesystem=$(sed -n 's/.* db_filesystem=\([^ ]*\) db_mount=.*/\1/p' "$phase_dir/phase.log" | tail -1)
    db_mount=$(sed -n 's/.* db_filesystem=[^ ]* db_mount=\(.*\)$/\1/p' "$phase_dir/phase.log" | tail -1)
    measured_seconds=$(awk -F'measured_seconds=' '/measured_seconds=/{print $2}' "$phase_dir/phase.log" | awk '{print $1}' | tail -1)
    test_rows=$(sed -n 's/.* rows=\([0-9][0-9]*\) setup_complete=.*/\1/p' "$phase_dir/phase.log" | tail -1)
    printf 'phase=%s\nmatrix_rows=%s\ntest_rows=%s\nsetup=logged before measured boundary\nmeasured_seconds=%s\nprocess_elapsed_seconds=%s\ndb_bytes_before=%s\ndb_bytes_after=%s\ndb_filesystem=%s\ndb_mount=%s\nartifact_kib_before=%s\nartifact_kib_after=%s\n' "$phase" "$rows" "$test_rows" "$measured_seconds" "$elapsed" "$db_before" "$db_after" "$db_filesystem" "$db_mount" "$artifact_before" "$artifact_after" > "$phase_dir/observations.txt"
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == runtime ]]; then for artifact in cpu.pprof trace.out block.pprof mutex.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
    if [[ "$PROFILE_PHASE" == "$phase" && "$PROFILE_MODE" == alloc ]]; then for artifact in alloc_before.pprof alloc_after.pprof alloc_delta.txt heap_after.pprof; do test -s "$phase_dir/profiles/$artifact"; done; fi
  done
}

run_matrix 10000
if [[ "$ROWS" == 100000 ]]; then run_matrix 100000; fi
echo "artifacts: $RUN_DIR"
