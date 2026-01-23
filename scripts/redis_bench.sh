#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT/bin"
BIN="$BIN_DIR/redisserver"
ART_DIR="$ROOT/artifacts"
RESULTS_CSV="$ART_DIR/redis_bench_results.csv"
RESULTS_MD="$ART_DIR/redis_bench_results.md"
BENCH_DOCS_DIR="${BENCH_DOCS_DIR:-}"

BENCH_KEYS="${BENCH_KEYS:-100000}"
BENCH_VALSIZE="${BENCH_VALSIZE:-128}"
BENCH_PIPELINE="${BENCH_PIPELINE:-16}"
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-50}"
BENCH_SERVERS="${BENCH_SERVERS:-hashdb,treedb,redis,valkey,dragonfly}"

REDIS_IMAGE="${REDIS_IMAGE:-redis:8.4.0}"
VALKEY_IMAGE="${VALKEY_IMAGE:-valkey/valkey:9.0.1}"
DRAGONFLY_IMAGE="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.34.2}"

BASE_PORT="${BASE_PORT:-6380}"
USE_DOCKER="${USE_DOCKER:-auto}"

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
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

build_server() {
  mkdir -p "$BIN_DIR"
  log "building redisserver"
  (cd "$ROOT" && env -u GOROOT go build -o "$BIN" ./cmd/redisserver)
}

start_redisserver() {
  local engine="$1"
  local port="$2"
  local dir="$3"
  "$BIN" -engine "$engine" -dir "$dir" -addr ":$port" >/dev/null 2>&1 &
  echo $!
}

start_docker() {
  local image="$1"
  local port="$2"
  docker run -d --rm -p "$port:6379" "$image"
}

stop_pid() {
  local pid="$1"
  if kill "$pid" >/dev/null 2>&1; then
    wait "$pid" 2>/dev/null || true
  fi
}

run_bench() {
  local name="$1"
  local port="$2"
  shift 2
  local args=("$@")
  local out
  out=$(redis-benchmark -h 127.0.0.1 -p "$port" --csv "${args[@]}")
  while IFS= read -r line; do
    if [[ "$line" != \"*\" ]]; then
      continue
    fi
    local cmd rps
    cmd=$(echo "$line" | awk -F',' '{print $1}' | tr -d '"')
    rps=$(echo "$line" | awk -F',' '{print $2}' | tr -d '"')
    if [[ "$cmd" == "test" ]]; then
      continue
    fi
    echo "${name},${SCENARIO},${cmd},${rps}" >> "$RESULTS_CSV"
  done <<< "$out"
}

write_markdown() {
  {
    echo "| Engine | Scenario | Command | RPS |"
    echo "|---|---|---:|---:|"
    tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r engine scenario cmd rps; do
      printf '| %s | %s | %s | %s |\n' "$engine" "$scenario" "$cmd" "$rps"
    done
  } > "$RESULTS_MD"
  if [[ -n "$BENCH_DOCS_DIR" ]]; then
    mkdir -p "$BENCH_DOCS_DIR"
    cp "$RESULTS_CSV" "$BENCH_DOCS_DIR/redis_bench_results.csv"
    cp "$RESULTS_MD" "$BENCH_DOCS_DIR/redis_bench_results.md"
  fi
}

main() {
  need_cmd redis-benchmark
  mkdir -p "$ART_DIR"
  echo "engine,scenario,command,rps" > "$RESULTS_CSV"

  if [[ "$USE_DOCKER" == "auto" ]]; then
    if docker_available; then
      USE_DOCKER=1
    else
      USE_DOCKER=0
    fi
  fi

  build_server

  IFS=',' read -r -a servers <<< "$BENCH_SERVERS"

  local port="$BASE_PORT"

  for server in "${servers[@]}"; do
    server="$(echo "$server" | xargs)"
    if [[ -z "$server" ]]; then
      continue
    fi

    case "$server" in
      hashdb|treedb)
        log "starting $server"
        tmpdir="$(mktemp -d)"
        pid=$(start_redisserver "$server" "$port" "$tmpdir")
        if ! wait_for_port 127.0.0.1 "$port"; then
          stop_pid "$pid"
          echo "failed to start $server" >&2
          exit 1
        fi
        ;;
      redis)
        if [[ "$USE_DOCKER" -eq 1 ]]; then
          log "starting redis docker ($REDIS_IMAGE)"
          cid=$(start_docker "$REDIS_IMAGE" "$port")
        else
          log "starting local redis-server"
          redis-server --port "$port" --save "" --appendonly no >/dev/null 2>&1 &
          pid=$!
        fi
        if ! wait_for_port 127.0.0.1 "$port"; then
          echo "failed to start redis" >&2
          exit 1
        fi
        ;;
      valkey)
        if [[ "$USE_DOCKER" -eq 1 ]]; then
          log "starting valkey docker ($VALKEY_IMAGE)"
          cid=$(start_docker "$VALKEY_IMAGE" "$port")
        else
          if command -v valkey-server >/dev/null 2>&1; then
            log "starting local valkey-server"
            valkey-server --port "$port" >/dev/null 2>&1 &
            pid=$!
          else
            log "skipping valkey (no docker or valkey-server)"
            port=$((port + 1))
            continue
          fi
        fi
        if ! wait_for_port 127.0.0.1 "$port"; then
          echo "failed to start valkey" >&2
          exit 1
        fi
        ;;
      dragonfly)
        if [[ "$USE_DOCKER" -eq 1 ]]; then
          log "starting dragonfly docker ($DRAGONFLY_IMAGE)"
          cid=$(start_docker "$DRAGONFLY_IMAGE" "$port")
        else
          log "skipping dragonfly (docker required)"
          port=$((port + 1))
          continue
        fi
        if ! wait_for_port 127.0.0.1 "$port"; then
          echo "failed to start dragonfly" >&2
          exit 1
        fi
        ;;
      *)
        echo "unknown server: $server" >&2
        exit 1
        ;;
    esac

    for SCENARIO in Standard Pipeline16 RandomKeys LargeVal1KB; do
      case "$SCENARIO" in
        Standard)
          run_bench "$server" "$port" -t set,get -c "$BENCH_CONCURRENCY" -n "$BENCH_KEYS" -d "$BENCH_VALSIZE"
          ;;
        Pipeline16)
          run_bench "$server" "$port" -t set,get -c "$BENCH_CONCURRENCY" -P "$BENCH_PIPELINE" -n "$BENCH_KEYS" -d "$BENCH_VALSIZE"
          ;;
        RandomKeys)
          run_bench "$server" "$port" -t set,get -c "$BENCH_CONCURRENCY" -P "$BENCH_PIPELINE" -r "$BENCH_KEYS" -n "$BENCH_KEYS" -d "$BENCH_VALSIZE"
          ;;
        LargeVal1KB)
          run_bench "$server" "$port" -t set,get -c "$BENCH_CONCURRENCY" -P "$BENCH_PIPELINE" -n "$BENCH_KEYS" -d 1024
          ;;
      esac
    done

    case "$server" in
      hashdb|treedb)
        stop_pid "$pid"
        rm -rf "$tmpdir"
        ;;
      redis)
        if [[ "${cid:-}" != "" ]]; then
          docker rm -f "$cid" >/dev/null
        else
          stop_pid "$pid"
        fi
        ;;
      valkey|dragonfly)
        if [[ "${cid:-}" != "" ]]; then
          docker rm -f "$cid" >/dev/null
        else
          stop_pid "$pid"
        fi
        ;;
    esac
    unset cid pid tmpdir
    port=$((port + 1))
  done

  write_markdown
  log "results: $RESULTS_MD"
}

main "$@"
