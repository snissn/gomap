#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/treedb_native_ycsb_diagnostic_XXXXXX")}"
GOYCSB_DIR="${GOYCSB_DIR:-/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5}"
RECORDCOUNTS="${RECORDCOUNTS:-100000 1000000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-10000}"
THREADCOUNT="${THREADCOUNT:-16}"
TREEDB_PROFILE="${TREEDB_PROFILE:-command_wal_relaxed}"
ADDR_BASE_HOST="${ADDR_BASE_HOST:-127.0.0.1}"
ADDR_BASE_PORT="${ADDR_BASE_PORT:-17370}"
BUILD="${BUILD:-true}"
BUILD_GOYCSB="${BUILD_GOYCSB:-false}"
ERROR_PATTERN='INSERT_ERROR|EOF|ambiguous|panic|fatal|ERROR|Failed'

usage() {
  cat <<'EOF'
Usage: scripts/nativewire_ycsb_diagnostic.sh

Runs external go-ycsb treedb-native load diagnostics against fresh TreeDB
nativewire servers. The harness enables client-side operation error logging
with TREEDB_YCSB_LOG_ERRORS=1 and uses -p silence=false.

Outputs:
  $OUT_DIR/host.txt
  $OUT_DIR/commands.txt
  $OUT_DIR/summary.tsv
  $OUT_DIR/raw_scan.txt
  $OUT_DIR/scan_counts.txt
  $OUT_DIR/<recordcount>/server.log
  $OUT_DIR/<recordcount>/load.out
  $OUT_DIR/<recordcount>/load.err
  $OUT_DIR/<recordcount>/load_exit_code.txt

Environment overrides:
  OUT_DIR, GOYCSB_DIR, RECORDCOUNTS, OPERATIONCOUNT, THREADCOUNT,
  TREEDB_PROFILE, ADDR_BASE_HOST, ADDR_BASE_PORT, BUILD, BUILD_GOYCSB.

Examples:
  scripts/nativewire_ycsb_diagnostic.sh
  RECORDCOUNTS="100000" OUT_DIR=/tmp/nativewire_diag scripts/nativewire_ycsb_diagnostic.sh
EOF
}

if [[ "${1-}" == "-h" || "${1-}" == "--help" ]]; then
  usage
  exit 0
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
: >"$OUT_DIR/commands.txt"

GOYCSB="$GOYCSB_DIR/bin/go-ycsb"
SERVER="$ROOT/bin/treedb-native-server"

if [[ "$BUILD" == "true" ]]; then
  GOWORK=off go build -o "$SERVER" ./cmd/treedb-native-server
fi
if [[ "$BUILD_GOYCSB" == "true" || ! -x "$GOYCSB" ]]; then
  (cd "$GOYCSB_DIR" && make build)
fi
if [[ ! -x "$GOYCSB" ]]; then
  echo "go-ycsb binary not found or not executable: $GOYCSB" >&2
  exit 2
fi

SERVER_PIDS=()

cleanup() {
  local pid
  for pid in "${SERVER_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${SERVER_PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

wait_tcp() {
  local addr=$1
  local host=${addr%:*}
  local port=${addr##*:}
  local i
  for i in $(seq 1 60); do
    if timeout 1 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $addr" >&2
  return 1
}

log_command() {
  printf '%q ' "$@" >>"$OUT_DIR/commands.txt"
  printf '\n' >>"$OUT_DIR/commands.txt"
}

write_host_info() {
  {
    printf 'date_utc='
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    printf 'date_local='
    date '+%Y-%m-%dT%H:%M:%S%z'
    printf 'host='
    hostname
    printf 'uname='
    uname -a
    printf 'go_version='
    go version
    printf 'gomap_commit='
    git rev-parse HEAD
    printf 'gomap_branch='
    git rev-parse --abbrev-ref HEAD
    printf 'goycsb_commit='
    (cd "$GOYCSB_DIR" && git rev-parse HEAD)
    printf 'goycsb_branch='
    (cd "$GOYCSB_DIR" && git rev-parse --abbrev-ref HEAD)
    printf 'recordcounts=%s\n' "$RECORDCOUNTS"
    printf 'operationcount=%s\n' "$OPERATIONCOUNT"
    printf 'threadcount=%s\n' "$THREADCOUNT"
    printf 'treedb_profile=%s\n' "$TREEDB_PROFILE"
    printf 'goycsb_dir=%s\n' "$GOYCSB_DIR"
    printf 'server_binary=%s\n' "$SERVER"
    printf 'goycsb_binary=%s\n' "$GOYCSB"
    lscpu
    free -h
  } >"$OUT_DIR/host.txt"
}

scan_artifacts() {
  local output=$1
  shift
  if grep -HniE "$ERROR_PATTERN" "$@" >"$output" 2>/dev/null; then
    return 0
  fi
  : >"$output"
}

count_term() {
  local term=$1
  shift
  (grep -HniE "$term" "$@" 2>/dev/null || true) | wc -l | tr -d ' '
}

write_scan_counts() {
  {
    printf 'scope\tterm\tcount\n'
    local scope term count load_out load_err server_log
    for scope in "$@"; do
      load_out="$scope/load.out"
      load_err="$scope/load.err"
      server_log="$scope/server.log"
      for term in INSERT_ERROR EOF ambiguous panic fatal ERROR Failed; do
        count=$(count_term "$term" "$load_out" "$load_err" "$server_log")
        printf '%s\t%s\t%s\n' "$(basename "$scope")" "$term" "$count"
      done
    done
  } >"$OUT_DIR/scan_counts.txt"
}

write_host_info
printf 'recordcount\taddr\texit_code\tinsert_error_lines\terror_scan_lines\tload_out\tload_err\tserver_log\n' >"$OUT_DIR/summary.tsv"

diagnostic_failures=0
run_dirs=()
index=0
for recordcount in $RECORDCOUNTS; do
  addr="$ADDR_BASE_HOST:$((ADDR_BASE_PORT + index))"
  run_dir="$OUT_DIR/$recordcount"
  db_dir="$run_dir/db"
  mkdir -p "$run_dir"
  run_dirs+=("$run_dir")
  rm -rf "$db_dir"

  server_cmd=(
    "$SERVER"
    -dir "$db_dir"
    -profile "$TREEDB_PROFILE"
    -addr "$addr"
  )
  log_command "${server_cmd[@]}"
  "${server_cmd[@]}" >"$run_dir/server.log" 2>&1 &
  server_pid=$!
  SERVER_PIDS+=("$server_pid")
  wait_tcp "$addr"

  load_cmd=(
    "$GOYCSB" load treedb-native
    -p "treedb.addr=$addr"
    -p "recordcount=$recordcount"
    -p "operationcount=$OPERATIONCOUNT"
    -p "threadcount=$THREADCOUNT"
    -p "silence=false"
  )
  log_command env TREEDB_YCSB_LOG_ERRORS=1 "${load_cmd[@]}"
  set +e
  TREEDB_YCSB_LOG_ERRORS=1 "${load_cmd[@]}" >"$run_dir/load.out" 2>"$run_dir/load.err"
  exit_code=$?
  set -e
  printf '%s\n' "$exit_code" >"$run_dir/load_exit_code.txt"

  scan_artifacts "$run_dir/raw_scan.txt" "$run_dir/load.out" "$run_dir/load.err" "$run_dir/server.log"
  insert_error_lines=$(count_term 'INSERT_ERROR' "$run_dir/load.out" "$run_dir/load.err" "$run_dir/server.log")
  error_scan_lines=$(wc -l <"$run_dir/raw_scan.txt" | tr -d ' ')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$recordcount" "$addr" "$exit_code" "$insert_error_lines" "$error_scan_lines" \
    "$run_dir/load.out" "$run_dir/load.err" "$run_dir/server.log" >>"$OUT_DIR/summary.tsv"
  if (( exit_code != 0 || insert_error_lines != 0 || error_scan_lines != 0 )); then
    diagnostic_failures=$((diagnostic_failures + 1))
  fi

  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true

  index=$((index + 1))
done

: >"$OUT_DIR/raw_scan.txt"
for scope in "${run_dirs[@]}"; do
  if [[ -s "$scope/raw_scan.txt" ]]; then
    sed "s#^#$(basename "$scope")/#" "$scope/raw_scan.txt" >>"$OUT_DIR/raw_scan.txt"
  fi
done
write_scan_counts "${run_dirs[@]}"

printf 'nativewire YCSB diagnostic artifacts: %s\n' "$OUT_DIR"
if (( diagnostic_failures != 0 )); then
  printf 'nativewire YCSB diagnostic detected %d failing run(s); inspect %s/summary.tsv and %s/raw_scan.txt\n' \
    "$diagnostic_failures" "$OUT_DIR" "$OUT_DIR" >&2
  exit 1
fi
