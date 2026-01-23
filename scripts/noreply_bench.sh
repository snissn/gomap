#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT/bin"
SERVER_BIN="$BIN_DIR/redisserver"
BENCH_BIN="$BIN_DIR/noreply_bench"
ART_DIR="$ROOT/artifacts"
RESULTS_CSV="$ART_DIR/noreply_bench_results.csv"
RESULTS_MD="$ART_DIR/noreply_bench_results.md"
BENCH_DOCS_DIR="${BENCH_DOCS_DIR:-}"

NR_ENGINES="${NR_ENGINES:-hashdb,treedb}"
NR_ADDR="${NR_ADDR:-127.0.0.1}"
NR_BASE_PORT="${NR_BASE_PORT:-6390}"
NR_CLIENTS="${NR_CLIENTS:-32}"
NR_REQUESTS="${NR_REQUESTS:-100000}"
NR_PIPELINE="${NR_PIPELINE:-32}"
NR_KEYSPACE="${NR_KEYSPACE:-100000}"
NR_VALSIZE="${NR_VALSIZE:-128}"
NR_RESP3="${NR_RESP3:-1}"
NR_REPLY_OFF="${NR_REPLY_OFF:-1}"

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

build_bins() {
  mkdir -p "$BIN_DIR" "$ART_DIR"
  log "building redisserver"
  (cd "$ROOT" && env -u GOROOT go build -o "$SERVER_BIN" ./cmd/redisserver)
  log "building noreply_bench"
  (cd "$ROOT" && env -u GOROOT go build -o "$BENCH_BIN" ./cmd/noreply_bench)
}

wait_for_port() {
  local host="$1"
  local port="$2"
  for _ in $(seq 1 50); do
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_redisserver() {
  local engine="$1"
  local port="$2"
  local dir="$3"
  "$SERVER_BIN" -engine "$engine" -dir "$dir" -addr ":$port" >/dev/null 2>&1 &
  echo $!
}

stop_pid() {
  local pid="$1"
  if kill "$pid" >/dev/null 2>&1; then
    wait "$pid" 2>/dev/null || true
  fi
}

write_markdown() {
  {
    echo "| Engine | Scenario | RPS |"
    echo "|---|---|---:|---:|"
    tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r engine scenario rps; do
      printf '| %s | %s | %s |\n' "$engine" "$scenario" "$rps"
    done
  } > "$RESULTS_MD"
  if [[ -n "$BENCH_DOCS_DIR" ]]; then
    mkdir -p "$BENCH_DOCS_DIR"
    cp "$RESULTS_CSV" "$BENCH_DOCS_DIR/noreply_bench_results.csv"
    cp "$RESULTS_MD" "$BENCH_DOCS_DIR/noreply_bench_results.md"
  fi
}

main() {
  build_bins
  echo "engine,scenario,rps" > "$RESULTS_CSV"

  IFS=',' read -r -a engines <<< "$NR_ENGINES"
  local port="$NR_BASE_PORT"

  for engine in "${engines[@]}"; do
    engine="$(echo "$engine" | xargs)"
    if [[ -z "$engine" ]]; then
      continue
    fi

    log "starting $engine"
    tmpdir="$(mktemp -d)"
    pid=$(start_redisserver "$engine" "$port" "$tmpdir")
    if ! wait_for_port "$NR_ADDR" "$port"; then
      stop_pid "$pid"
      echo "failed to start $engine" >&2
      exit 1
    fi

    scenario="clients=${NR_CLIENTS};pipeline=${NR_PIPELINE};req=${NR_REQUESTS};val=${NR_VALSIZE};resp3=${NR_RESP3};replyoff=${NR_REPLY_OFF}"
    log "noreply $engine $scenario"

    output="$("$BENCH_BIN" \
      -addr "$NR_ADDR:$port" \
      -label "$engine" \
      -clients "$NR_CLIENTS" \
      -pipeline "$NR_PIPELINE" \
      -requests "$NR_REQUESTS" \
      -keyspace "$NR_KEYSPACE" \
      -value-size "$NR_VALSIZE" \
      -resp3=$([[ "$NR_RESP3" -eq 1 ]] && echo true || echo false) \
      -reply-off=$([[ "$NR_REPLY_OFF" -eq 1 ]] && echo true || echo false))"

    rps=$(echo "$output" | awk -F'rps=' '{print $2}' | awk '{print $1}')
    if [[ -z "$rps" ]]; then
      echo "failed to parse rps from output: $output" >&2
      stop_pid "$pid"
      rm -rf "$tmpdir"
      exit 1
    fi
    echo "${engine},${scenario},${rps}" >> "$RESULTS_CSV"

    stop_pid "$pid"
    rm -rf "$tmpdir"
    port=$((port + 1))
  done

  write_markdown
  log "results: $RESULTS_MD"
}

main "$@"
