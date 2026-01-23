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

NR_ENGINES="${NR_ENGINES:-}"
NR_SERVERS="${NR_SERVERS:-${NR_ENGINES:-hashdb,treedb,redis,valkey,dragonfly}}"
NR_ADDR="${NR_ADDR:-127.0.0.1}"
NR_BASE_PORT="${NR_BASE_PORT:-6390}"
NR_CLIENTS="${NR_CLIENTS:-}"
NR_REQUESTS="${NR_REQUESTS:-0}"
NR_TEST_TIME="${NR_TEST_TIME:-10s}"
NR_PIPELINE="${NR_PIPELINE:-}"
NR_KEYSPACE="${NR_KEYSPACE:-100000}"
NR_VALSIZE="${NR_VALSIZE:-128}"
NR_VALSIZE_LIST="${NR_VALSIZE_LIST:-}"
NR_CLIENTS_LIST="${NR_CLIENTS_LIST:-}"
NR_PIPELINE_LIST="${NR_PIPELINE_LIST:-}"
NR_RESP3="${NR_RESP3:-1}"
NR_REPLY_OFF="${NR_REPLY_OFF:-1}"

REDISSERVER_ARGS_ALL="${REDISSERVER_ARGS_ALL:-}"
REDISSERVER_ARGS_HASHDB="${REDISSERVER_ARGS_HASHDB:-}"
REDISSERVER_ARGS_TREEDB="${REDISSERVER_ARGS_TREEDB:-}"

REDIS_IMAGE="${REDIS_IMAGE:-redis:8.4.0}"
VALKEY_IMAGE="${VALKEY_IMAGE:-valkey/valkey:9.0.1}"
DRAGONFLY_IMAGE="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.34.2}"
USE_DOCKER="${USE_DOCKER:-auto}"

log() {
  echo "[$(date +%H:%M:%S)] $*"
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
  local extra="$REDISSERVER_ARGS_ALL"
  case "$engine" in
    hashdb) extra="$extra $REDISSERVER_ARGS_HASHDB" ;;
    treedb) extra="$extra $REDISSERVER_ARGS_TREEDB" ;;
  esac
  # shellcheck disable=SC2086
  "$SERVER_BIN" -engine "$engine" -dir "$dir" -addr ":$port" $extra >/dev/null 2>&1 &
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

write_markdown() {
  {
    echo "| Engine | Scenario | RPS |"
    echo "|---|---|---:|"
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

split_csv() {
  local list="$1"
  IFS=',' read -r -a items <<< "$list"
  echo "${items[@]}"
}

main() {
  build_bins
  echo "engine,scenario,rps" > "$RESULTS_CSV"

  if [[ "$USE_DOCKER" == "auto" ]]; then
    if docker_available; then
      USE_DOCKER=1
    else
      USE_DOCKER=0
    fi
  fi

  if [[ -z "${CI:-}" ]]; then
    cores="$(cpu_cores)"
    if [[ -z "$NR_CLIENTS" ]]; then
      NR_CLIENTS=$((cores * 2))
    fi
    if [[ -z "$NR_PIPELINE" ]]; then
      NR_PIPELINE=64
    fi
  fi
  if [[ -z "$NR_CLIENTS" ]]; then
    NR_CLIENTS=32
  fi
  if [[ -z "$NR_PIPELINE" ]]; then
    NR_PIPELINE=32
  fi

  local clients_list pipeline_list valsize_list
  if [[ -n "$NR_CLIENTS_LIST" ]]; then
    clients_list="$(split_csv "$NR_CLIENTS_LIST")"
  else
    clients_list="$NR_CLIENTS"
  fi
  if [[ -n "$NR_PIPELINE_LIST" ]]; then
    pipeline_list="$(split_csv "$NR_PIPELINE_LIST")"
  else
    pipeline_list="$NR_PIPELINE"
  fi
  if [[ -n "$NR_VALSIZE_LIST" ]]; then
    valsize_list="$(split_csv "$NR_VALSIZE_LIST")"
  else
    valsize_list="$NR_VALSIZE"
  fi

  IFS=',' read -r -a servers <<< "$NR_SERVERS"
  local port="$NR_BASE_PORT"

  for clients in $clients_list; do
    for pipeline in $pipeline_list; do
      for valsize in $valsize_list; do
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
              ;;
            redis)
              if [[ "$USE_DOCKER" -ne 1 ]]; then
                log "skipping redis (docker required)"
                port=$((port + 1))
                continue
              fi
              log "starting redis docker ($REDIS_IMAGE)"
              cid=$(start_docker "$REDIS_IMAGE" "$port" --save "" --appendonly no --protected-mode no)
              ;;
            valkey)
              if [[ "$USE_DOCKER" -ne 1 ]]; then
                log "skipping valkey (docker required)"
                port=$((port + 1))
                continue
              fi
              log "starting valkey docker ($VALKEY_IMAGE)"
              cid=$(start_docker "$VALKEY_IMAGE" "$port" --save "" --appendonly no --protected-mode no)
              ;;
            dragonfly)
              if [[ "$USE_DOCKER" -ne 1 ]]; then
                log "skipping dragonfly (docker required)"
                port=$((port + 1))
                continue
              fi
              log "starting dragonfly docker ($DRAGONFLY_IMAGE)"
              cid=$(start_docker "$DRAGONFLY_IMAGE" "$port")
              ;;
            *)
              echo "unknown server: $server" >&2
              exit 1
              ;;
          esac

          if ! wait_for_port "$NR_ADDR" "$port"; then
            echo "failed to start $server" >&2
            if [[ "${cid:-}" != "" ]]; then
              docker rm -f "$cid" >/dev/null || true
            else
              stop_pid "${pid:-}"
            fi
            rm -rf "${tmpdir:-}"
            exit 1
          fi

          scenario="clients=${clients};pipeline=${pipeline};req=${NR_REQUESTS};val=${valsize};resp3=${NR_RESP3};replyoff=${NR_REPLY_OFF}"
          if [[ "$NR_TEST_TIME" != "0" && "$NR_TEST_TIME" != "0s" ]]; then
            scenario="clients=${clients};pipeline=${pipeline};time=${NR_TEST_TIME};val=${valsize};resp3=${NR_RESP3};replyoff=${NR_REPLY_OFF}"
          fi
          log "noreply $server $scenario"

          set +e
          output="$("$BENCH_BIN" \
            -addr "$NR_ADDR:$port" \
            -label "$server" \
            -clients "$clients" \
            -pipeline "$pipeline" \
            -requests "$NR_REQUESTS" \
            -test-time "$NR_TEST_TIME" \
            -keyspace "$NR_KEYSPACE" \
            -value-size "$valsize" \
            -resp3=$([[ "$NR_RESP3" -eq 1 ]] && echo true || echo false) \
            -reply-off=$([[ "$NR_REPLY_OFF" -eq 1 ]] && echo true || echo false) 2>&1)"
          status=$?
          set -e
          if [[ "$status" -ne 0 ]]; then
            log "noreply bench failed for $server: $output"
            # Likely unsupported CLIENT REPLY OFF; skip.
            case "$server" in
              hashdb|treedb)
                stop_pid "$pid"
                rm -rf "$tmpdir"
                ;;
              redis|valkey|dragonfly)
                docker rm -f "$cid" >/dev/null || true
                ;;
            esac
            unset cid pid tmpdir
            port=$((port + 1))
            continue
          fi

          rps=$(echo "$output" | awk -F'rps=' '{print $2}' | awk '{print $1}')
          if [[ -z "$rps" ]]; then
            echo "failed to parse rps from output: $output" >&2
            if [[ "${cid:-}" != "" ]]; then
              docker rm -f "$cid" >/dev/null || true
            else
              stop_pid "$pid"
              rm -rf "$tmpdir"
            fi
            exit 1
          fi
          echo "${server},${scenario},${rps}" >> "$RESULTS_CSV"

          case "$server" in
            hashdb|treedb)
              stop_pid "$pid"
              rm -rf "$tmpdir"
              ;;
            redis|valkey|dragonfly)
              docker rm -f "$cid" >/dev/null || true
              ;;
          esac
          unset cid pid tmpdir
          port=$((port + 1))
        done
      done
    done
  done

  write_markdown
  log "results: $RESULTS_MD"
}

main "$@"
