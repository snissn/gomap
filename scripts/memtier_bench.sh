#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT/bin"
BIN="$BIN_DIR/redisserver"
ART_DIR="$ROOT/artifacts"
RESULTS_CSV="$ART_DIR/memtier_bench_results.csv"
RESULTS_MD="$ART_DIR/memtier_bench_results.md"
BENCH_DOCS_DIR="${BENCH_DOCS_DIR:-}"

MEMTIER_IMAGE="${MEMTIER_IMAGE:-redislabs/memtier_benchmark:latest}"
MEMTIER_USE_DOCKER="${MEMTIER_USE_DOCKER:-auto}"

BENCH_SERVERS="${BENCH_SERVERS:-hashdb,treedb,redis,valkey,dragonfly}"
BASE_PORT="${BASE_PORT:-6380}"

REDISSERVER_ARGS_ALL="${REDISSERVER_ARGS_ALL:-}"
REDISSERVER_ARGS_HASHDB="${REDISSERVER_ARGS_HASHDB:-}"
REDISSERVER_ARGS_TREEDB="${REDISSERVER_ARGS_TREEDB:-}"

# Matrix inputs (comma-separated).
KEYS_LIST="${KEYS_LIST:-100000}"
CLIENTS_LIST="${CLIENTS_LIST:-}"
THREADS_LIST="${THREADS_LIST:-}"
PIPELINE_LIST="${PIPELINE_LIST:-1,16,64}"
DATA_LIST="${DATA_LIST:-128}"
RATIO_LIST="${RATIO_LIST:-1:1}"
KEYPATTERN_LIST="${KEYPATTERN_LIST:-S:S,R:R}"
TEST_TIME="${TEST_TIME:-10}"
PROTOCOL_LIST="${PROTOCOL_LIST:-resp2,resp3}"

REDIS_IMAGE="${REDIS_IMAGE:-redis:8.4.0}"
VALKEY_IMAGE="${VALKEY_IMAGE:-valkey/valkey:9.0.1}"
DRAGONFLY_IMAGE="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.34.2}"
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

cpu_cores() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi
  if command -v getconf >/dev/null 2>&1; then
    getconf _NPROCESSORS_ONLN
    return
  fi
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu
    return
  fi
  echo 4
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
  local extra="$REDISSERVER_ARGS_ALL"
  case "$engine" in
    hashdb) extra="$extra $REDISSERVER_ARGS_HASHDB" ;;
    treedb) extra="$extra $REDISSERVER_ARGS_TREEDB" ;;
  esac
  # shellcheck disable=SC2086
  "$BIN" -engine "$engine" -dir "$dir" -addr ":$port" $extra >/dev/null 2>&1 &
  echo $!
}

start_docker() {
  local image="$1"
  local port="$2"
  shift 2
  docker run -d --rm -p "$port:6379" "$image" "$@"
}

stop_pid() {
  local pid="$1"
  if kill "$pid" >/dev/null 2>&1; then
    wait "$pid" 2>/dev/null || true
  fi
}

memtier_cmd() {
  local host="$1"
  local port="$2"
  local threads="$3"
  local clients="$4"
  local pipeline="$5"
  local ratio="$6"
  local data="$7"
  local keymin="$8"
  local keymax="$9"
  local keypattern="${10}"
  local protocol="${11}"
  local outfile="${12}"

  if command -v memtier_benchmark >/dev/null 2>&1 && [[ "$MEMTIER_USE_DOCKER" != "1" ]]; then
    memtier_benchmark \
      --server "$host" \
      --port "$port" \
      --protocol "$protocol" \
      --test-time "$TEST_TIME" \
      --threads "$threads" \
      --clients "$clients" \
      --ratio "$ratio" \
      --pipeline "$pipeline" \
      --data-size "$data" \
      --key-minimum "$keymin" \
      --key-maximum "$keymax" \
      --key-pattern "$keypattern" \
      --hide-histogram \
      --print-percentiles "50,99,99.9" \
      --out-file "$outfile"
    return
  fi

  if [[ "$MEMTIER_USE_DOCKER" == "auto" ]]; then
    if docker_available; then
      MEMTIER_USE_DOCKER=1
    else
      MEMTIER_USE_DOCKER=0
    fi
  fi
  if [[ "$MEMTIER_USE_DOCKER" -ne 1 ]]; then
    echo "memtier_benchmark not available and docker disabled" >&2
    exit 1
  fi

  docker run --rm --network host "$MEMTIER_IMAGE" \
    --server "$host" \
    --port "$port" \
    --protocol "$protocol" \
    --test-time "$TEST_TIME" \
    --threads "$threads" \
    --clients "$clients" \
    --ratio "$ratio" \
    --pipeline "$pipeline" \
    --data-size "$data" \
    --key-minimum "$keymin" \
    --key-maximum "$keymax" \
    --key-pattern "$keypattern" \
    --hide-histogram \
    --print-percentiles "50,99,99.9" \
    --out-file "$outfile"
}

parse_memtier() {
  local name="$1"
  local scenario="$2"
  local file="$3"
  awk -v name="$name" -v scenario="$scenario" '
    $1=="ALL" && $2=="STATS" {inblock=1; next}
    inblock && $1 ~ /^-+/ {next}
    inblock && NF==0 {exit}
    inblock && ($1=="Sets" || $1=="Gets" || $1=="Totals") {
      cmd = ($1=="Sets" ? "SET" : ($1=="Gets" ? "GET" : "TOTAL"))
      printf "%s,%s,%s,%s\n", name, scenario, cmd, $2
    }
  ' "$file" >> "$RESULTS_CSV"
}

write_markdown() {
  {
    echo "| Engine | Scenario | Command | Ops/sec |"
    echo "|---|---|---:|---:|"
    tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r engine scenario cmd rps; do
      printf '| %s | %s | %s | %s |\n' "$engine" "$scenario" "$cmd" "$rps"
    done
  } > "$RESULTS_MD"
  if [[ -n "$BENCH_DOCS_DIR" ]]; then
    mkdir -p "$BENCH_DOCS_DIR"
    cp "$RESULTS_CSV" "$BENCH_DOCS_DIR/memtier_bench_results.csv"
    cp "$RESULTS_MD" "$BENCH_DOCS_DIR/memtier_bench_results.md"
  fi
}

split_list() {
  local list="$1"
  IFS=',' read -r -a items <<< "$list"
  echo "${items[@]}"
}

main() {
  mkdir -p "$ART_DIR"
  echo "engine,scenario,command,ops_sec" > "$RESULTS_CSV"

  if [[ "$USE_DOCKER" == "auto" ]]; then
    if docker_available; then
      USE_DOCKER=1
    else
      USE_DOCKER=0
    fi
  fi

  if [[ -z "${CI:-}" ]]; then
    local cores
    cores="$(cpu_cores)"
    if [[ -z "$THREADS_LIST" ]]; then
      THREADS_LIST="$cores"
    fi
    if [[ -z "$CLIENTS_LIST" ]]; then
      CLIENTS_LIST="2"
    fi
  fi
  if [[ -z "$THREADS_LIST" ]]; then
    THREADS_LIST="4"
  fi
  if [[ -z "$CLIENTS_LIST" ]]; then
    CLIENTS_LIST="50"
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
          cid=$(start_docker "$REDIS_IMAGE" "$port" --save "" --appendonly no --protected-mode no)
        else
          log "starting local redis-server"
          redis-server --port "$port" --save "" --appendonly no --protected-mode no >/dev/null 2>&1 &
          pid=$!
        fi
        if ! wait_for_port 127.0.0.1 "$port"; then
          echo "failed to start redis" >&2
          exit 1
        fi
        sleep 1
        ;;
      valkey)
        if [[ "$USE_DOCKER" -eq 1 ]]; then
          log "starting valkey docker ($VALKEY_IMAGE)"
          cid=$(start_docker "$VALKEY_IMAGE" "$port" --save "" --appendonly no --protected-mode no)
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
        sleep 1
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
        sleep 1
        ;;
      *)
        echo "unknown server: $server" >&2
        exit 1
        ;;
    esac

    for keys in $(split_list "$KEYS_LIST"); do
      local keymin=1
      local keymax=$((keys))
      for clients in $(split_list "$CLIENTS_LIST"); do
        for threads in $(split_list "$THREADS_LIST"); do
          for pipeline in $(split_list "$PIPELINE_LIST"); do
            for data in $(split_list "$DATA_LIST"); do
              for ratio in $(split_list "$RATIO_LIST"); do
                for keypattern in $(split_list "$KEYPATTERN_LIST"); do
                  for proto in $(split_list "$PROTOCOL_LIST"); do
                    scenario="keys=${keys};clients=${clients};threads=${threads};pipe=${pipeline};data=${data};ratio=${ratio};pattern=${keypattern};proto=${proto}"
                    outfile="$(mktemp)"
                    log "memtier $server $scenario"
                    memtier_cmd "127.0.0.1" "$port" "$threads" "$clients" "$pipeline" "$ratio" "$data" "$keymin" "$keymax" "$keypattern" "$proto" "$outfile"
                    parse_memtier "$server" "$scenario" "$outfile"
                    rm -f "$outfile"
                  done
                done
              done
            done
          done
        done
      done
    done

    case "$server" in
      hashdb|treedb)
        stop_pid "$pid"
        rm -rf "$tmpdir"
        ;;
      redis|valkey|dragonfly)
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
