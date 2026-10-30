#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
VALIDATOR="$ROOT/scripts/ycsb_compare_validate.py"
PYTHON="${PYTHON:-python3}"
TMP_BASE="${TMPDIR:-/mnt/fast4tb}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/treedb_ycsb_compare_XXXXXX")}"
GOYCSB_DIR="${GOYCSB_DIR:-/home/mikers/dev/pingcap/go-ycsb}"
GOYCSB_BIN="${GOYCSB_BIN:-$GOYCSB_DIR/bin/go-ycsb}"
CLIENT_BUILD_MANIFEST="${CLIENT_BUILD_MANIFEST:-}"
RECORDCOUNT="${RECORDCOUNT:-100000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-10000}"
THREADCOUNT="${THREADCOUNT:-16}"
RUN_REPEATS="${RUN_REPEATS:-1}"
TREEDB_PROFILE="${TREEDB_PROFILE:-command_wal_relaxed}"
TREEDB_NATIVE_DOCUMENT_FORMAT="${TREEDB_NATIVE_DOCUMENT_FORMAT:-bson}"
TREEDB_MONGO_DOCUMENT_FORMAT="${TREEDB_MONGO_DOCUMENT_FORMAT:-bson}"
TREEDB_NATIVE_SERVER_BIN="${TREEDB_NATIVE_SERVER_BIN:-$ROOT/bin/treedb-native-server}"
TREEDB_MONGO_GATEWAY_BIN="${TREEDB_MONGO_GATEWAY_BIN:-$ROOT/bin/treedb-mongo-gateway}"
NATIVE_ADDR="${NATIVE_ADDR:-127.0.0.1:17130}"
TREEDB_MONGO_ADDR="${TREEDB_MONGO_ADDR:-127.0.0.1:27130}"
MONGODB_MODE="${MONGODB_MODE:-docker}"
MONGODB_ADDR="${MONGODB_ADDR:-127.0.0.1:27018}"
MONGODB_IMAGE="${MONGODB_IMAGE:-mongo:8}"
TARGETS="${TARGETS:-treedb_native treedb_mongo}"
BUILD="${BUILD:-true}"
BUILD_GOYCSB="${BUILD_GOYCSB:-false}"
PARSE_ONLY="${PARSE_ONLY:-false}"
ALLOW_YCSB_ERRORS="${ALLOW_YCSB_ERRORS:-false}"

usage() {
  cat <<'EOF'
Usage: scripts/ycsb_compare_mongodb_treedb.sh

TARGETS selects exactly the services and clients to run (space or comma separated:
  treedb_native, treedb_mongo, mongodb). The default runs the two TreeDB targets.

Supports supplied absolute TREEDB_NATIVE_SERVER_BIN/TREEDB_MONGO_GATEWAY_BIN,
GOYCSB_BIN, TREEDB_NATIVE_DOCUMENT_FORMAT and TREEDB_MONGO_DOCUMENT_FORMAT
(json|bson), TREEDB_PROFILE, RECORDCOUNT, OPERATIONCOUNT, THREADCOUNT,
RUN_REPEATS, BUILD, BUILD_GOYCSB, OUT_DIR and TMPDIR.

CLIENT_BUILD_MANIFEST is required for execution and binds producer source,
reviewed compatibility overlay, toolchain and binary. See ycsb_compare_receipts.md.
Every actual child has GOMAXPROCS removed. Original TOTAL timing is unchanged;
client close receipts and launcher identities are separate artifacts.

PARSE_ONLY=true consumes the existing manifest.json and selected target artifacts
and rewrites summary.tsv, summary.md and validation.json. It does not start any
service or mutate original input evidence; copy retained artifacts first.
Missing/failed close, runtime/COW, checkpoint, close, reopen, observed-state or
owned cleanup evidence causes a nonzero result. Server witness production is a
separate integration responsibility. ALLOW_YCSB_ERRORS never makes errors valid.

MONGODB_MODE=docker starts an owned service; external services currently require
an explicit process/ownership integration and are refused. skip excludes MongoDB.
EOF
}
if [[ "${1-}" == -h || "${1-}" == --help ]]; then usage; exit 0; fi
fail() { echo "$*" >&2; exit 2; }
for flag in BUILD BUILD_GOYCSB PARSE_ONLY ALLOW_YCSB_ERRORS; do
  [[ "${!flag}" == true || "${!flag}" == false ]] || fail "$flag must be true or false"
done
for count in RECORDCOUNT OPERATIONCOUNT THREADCOUNT RUN_REPEATS; do
  [[ "${!count}" =~ ^[1-9][0-9]*$ ]] || fail "$count must be a positive integer"
done
case "$TREEDB_PROFILE" in command_wal_durable|command_wal_relaxed|no_wal_fast) ;; *) fail "unsupported TREEDB_PROFILE";; esac
for format in "$TREEDB_NATIVE_DOCUMENT_FORMAT" "$TREEDB_MONGO_DOCUMENT_FORMAT"; do
  [[ "$format" == bson || "$format" == json ]] || fail "document format must be bson or json"
done
case "$MONGODB_MODE" in docker|external|skip) ;; *) fail "unknown MONGODB_MODE";; esac
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
if [[ "$PARSE_ONLY" == true ]]; then
  exec "$PYTHON" "$VALIDATOR" validate "$OUT_DIR"
fi
read -r -a selected <<<"${TARGETS//,/ }"
[[ ${#selected[@]} -gt 0 ]] || fail "TARGETS cannot be empty"
declare -A chosen=()
for target in "${selected[@]}"; do
  case "$target" in treedb_native|treedb_mongo|mongodb) ;; *) fail "unknown target $target";; esac
  [[ -z "${chosen[$target]:-}" ]] || fail "duplicate target $target"
  chosen[$target]=1
done
if [[ -n "${chosen[mongodb]:-}" ]]; then
  [[ "$MONGODB_MODE" == docker ]] || fail "MongoDB requires owned docker mode; external/skip cannot establish owned cleanup"
fi
# An invocation owns a new output directory. Never overwrite an earlier attempt.
[[ -z "$(ls -A "$OUT_DIR")" ]] || fail "execution requires a fresh empty OUT_DIR"
[[ -n "$CLIENT_BUILD_MANIFEST" && -f "$CLIENT_BUILD_MANIFEST" ]] || fail "CLIENT_BUILD_MANIFEST is required"
for binary in "$GOYCSB_BIN" "$TREEDB_NATIVE_SERVER_BIN" "$TREEDB_MONGO_GATEWAY_BIN"; do
  [[ "$binary" == /* ]] || fail "binary paths must be absolute"
done
if [[ "$BUILD" == true ]]; then
  if [[ -n "${chosen[treedb_native]:-}" ]]; then go build -o "$TREEDB_NATIVE_SERVER_BIN" ./cmd/treedb-native-server; fi
  if [[ -n "${chosen[treedb_mongo]:-}" ]]; then go build -o "$TREEDB_MONGO_GATEWAY_BIN" ./TreeDB/mongo_gateway/server.go; fi
fi
if [[ "$BUILD_GOYCSB" == true ]]; then (cd "$GOYCSB_DIR" && make build); fi
[[ -x "$GOYCSB_BIN" ]] || fail "missing supplied client binary"
for target in "${selected[@]}"; do mkdir "$OUT_DIR/$target"; done

"$PYTHON" - "$OUT_DIR" "$CLIENT_BUILD_MANIFEST" "$GOYCSB_DIR" "$GOYCSB_BIN" "$TREEDB_PROFILE" "$RECORDCOUNT" "$OPERATIONCOUNT" "$THREADCOUNT" "$RUN_REPEATS" "$TREEDB_NATIVE_DOCUMENT_FORMAT" "$TREEDB_MONGO_DOCUMENT_FORMAT" "${selected[@]}" <<'PY'
import hashlib, json, pathlib, subprocess, sys, uuid
out, build_path, repo, binary, profile, records, ops, threads, repeats, native_format, mongo_format, *targets = sys.argv[1:]
out = pathlib.Path(out)
build = json.loads(pathlib.Path(build_path).read_text())
def git(*args):
    return subprocess.check_output(['git', '-C', repo, *args])
def sha(path):
    with open(path, 'rb') as stream:
        value = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
        return value.hexdigest()
producer = '3c8148495b8d4490cf0ec30e9eb92d14a7a71bac'
subprocess.run(['git', '-C', repo, 'merge-base', '--is-ancestor', producer, 'HEAD'], check=True)
patch = git('diff', '--binary', 'HEAD')
# Untracked source is not represented by the diff or a build manifest.
if git('ls-files', '--others', '--exclude-standard').strip():
    raise SystemExit('client has untracked inputs; bind a clean/overlaid source tree first')
source = git('rev-parse', 'HEAD').decode().strip()
if not (build.get('schema_version') == 1 and build.get('producer_commit') == producer and build.get('source_commit') == source and build.get('source_patch_sha256') == hashlib.sha256(patch).hexdigest() and build.get('exe_sha256') == sha(binary) and build.get('build_id') and build.get('toolchain') and build.get('compatibility_configuration')):
    raise SystemExit('client source/overlay/ELF build manifest mismatch or incomplete binding')
(out / 'client.patch').write_bytes(patch)
(out / 'client-build.json').write_text(json.dumps(build, indent=2)+'\n')
(out / 'client-buildinfo.txt').write_bytes(subprocess.check_output(['go', 'version', '-m', binary]))
manifest = dict(schema_version=1, campaign_id=uuid.uuid4().hex, targets=targets, profile=profile, formats={'treedb_native':native_format, 'treedb_mongo':mongo_format, 'mongodb':'bson'}, recordcount=int(records), operationcount=int(ops), threadcount=int(threads), run_repeats=int(repeats), client=build)
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
PY

# Retain host and launcher source context outside timed client invocations.
"$PYTHON" - "$OUT_DIR" <<'PYHOST'
import datetime, hashlib, json, os, pathlib, platform, subprocess, sys
out = pathlib.Path(sys.argv[1])
context = dict(date_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), uname=platform.uname()._asdict(), logical_cpus=os.cpu_count(), cpu_affinity=sorted(os.sched_getaffinity(0)), gomap_commit=subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(), launcher_source_patch_sha256=hashlib.sha256(subprocess.check_output(['git','diff','--binary','HEAD'])).hexdigest(), go_version=subprocess.check_output(['go','version']).decode().strip())
for name, path in [('cpuinfo','/proc/cpuinfo'),('meminfo','/proc/meminfo'),('clocksource','/sys/devices/system/clocksource/clocksource0/current_clocksource')]:
    context[name] = pathlib.Path(path).read_text()
(out/'host.json').write_text(json.dumps(context,indent=2)+'\n')
PYHOST

declare -a SERVER_PIDS=() SERVER_TARGETS=()
MONGO_CONTAINER=""
CLEANUP_DONE=false
cleanup() {
  local failed=0 pid target index code i
  [[ "$CLEANUP_DONE" == false ]] || return 0
  CLEANUP_DONE=true
  for index in "${!SERVER_PIDS[@]}"; do
    pid=${SERVER_PIDS[$index]}; target=${SERVER_TARGETS[$index]}
    kill -TERM "$pid" 2>/dev/null || true
    for ((i=0; i<100; i++)); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    code=0; wait "$pid" || code=$?
    "$PYTHON" - "$OUT_DIR/$target" "$code" <<'PY'
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
launch = json.loads((out / 'server.launch.json').read_text())
with (out / 'server.exit.json').open('x') as stream:
    json.dump(dict(process=launch['process'], exit_code=int(sys.argv[2])), stream)
PY
    [[ "$code" == 0 ]] || failed=1
  done
  if [[ -n "$MONGO_CONTAINER" ]]; then
    docker logs "$MONGO_CONTAINER" >"$OUT_DIR/mongodb/server.log" 2>&1 || failed=1
    docker stop --time 10 "$MONGO_CONTAINER" >/dev/null || failed=1
    code=$(docker inspect --format '{{.State.ExitCode}}' "$MONGO_CONTAINER") || failed=1
    "$PYTHON" - "$OUT_DIR/mongodb" "${code:-1}" <<'PY'
import json, pathlib, sys
out=pathlib.Path(sys.argv[1]); launch=json.loads((out/'server.launch.json').read_text())
with (out/'server.exit.json').open('x') as stream:
    json.dump(dict(process=launch['process'], exit_code=int(sys.argv[2])),stream)
PY
    [[ "${code:-1}" == 0 ]] || failed=1
    docker rm "$MONGO_CONTAINER" >/dev/null || failed=1
  fi
  return "$failed"
}
finish() {
  local status=$? cleanup_status=0
  cleanup || cleanup_status=$?
  if [[ "$status" != 0 || "$cleanup_status" != 0 ]]; then
    echo '{"status":"invalid","errors":["launcher or owned cleanup failed"]}' >"$OUT_DIR/validation.json"
    [[ "$status" != 0 ]] || status=3
  fi
  exit "$status"
}
trap finish EXIT
wait_tcp() {
  local addr=$1 host=${1%:*} port=${1##*:} i
  for ((i=0; i<60; i++)); do
    # Positional parameters are intentionally expanded by the child shell.
    # shellcheck disable=SC2016
    if timeout 1 bash -c 'exec 3<>/dev/tcp/"$1"/"$2"' _ "$host" "$port" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $addr" >&2; return 1
}
log_command() { printf '%q ' "$@" >>"$OUT_DIR/commands.txt"; printf '\n' >>"$OUT_DIR/commands.txt"; }
start_server() {
  local target=$1 addr=$2 binary=$3 dir="$OUT_DIR/$1/db"
  shift 3
  mkdir "$dir"
  local command=(env -u GOMAXPROCS "$binary" -dir "$dir" -profile "$TREEDB_PROFILE" -addr "$addr" "$@")
  log_command "${command[@]}"
  "${command[@]}" >"$OUT_DIR/$target/server.log" 2>&1 &
  SERVER_PIDS+=("$!"); SERVER_TARGETS+=("$target")
  wait_tcp "$addr"
  "$PYTHON" "$VALIDATOR" observe "${SERVER_PIDS[-1]}" "$dir" "$OUT_DIR/$target/server.launch.json"
}
for target in "${selected[@]}"; do
  case "$target" in
    treedb_native) start_server "$target" "$NATIVE_ADDR" "$TREEDB_NATIVE_SERVER_BIN";;
    treedb_mongo) start_server "$target" "$TREEDB_MONGO_ADDR" "$TREEDB_MONGO_GATEWAY_BIN" -document-format "$TREEDB_MONGO_DOCUMENT_FORMAT";;
    mongodb)
      command -v docker >/dev/null || fail "docker is required"
      MONGO_CONTAINER="treedb-ycsb-mongo-$$-$(date +%s)"
      mkdir "$OUT_DIR/mongodb/db"
      docker run -d --name "$MONGO_CONTAINER" -p "$MONGODB_ADDR:27017" -v "$OUT_DIR/mongodb/db:/data/db" "$MONGODB_IMAGE" --bind_ip_all --quiet >"$OUT_DIR/mongodb/container.id"
      pid=$(docker inspect --format '{{.State.Pid}}' "$MONGO_CONTAINER")
      "$PYTHON" "$VALIDATOR" observe "$pid" "$OUT_DIR/mongodb/db" "$OUT_DIR/mongodb/server.launch.json"
      docker inspect "$MONGO_CONTAINER" >"$OUT_DIR/mongodb/container.inspect.json"
      wait_tcp "$MONGODB_ADDR";;
  esac
done
for target in "${selected[@]}"; do
  extra=()
  case "$target" in
    treedb_native) binding=treedb-native; extra=(-p "treedb.addr=$NATIVE_ADDR" -p "treedb.document_format=$TREEDB_NATIVE_DOCUMENT_FORMAT" -p treedb.ack=visible -p treedb.flush_on_close=true);;
    treedb_mongo) binding=mongodb; extra=(-p "mongodb.url=mongodb://$TREEDB_MONGO_ADDR/ycsb?w=1");;
    mongodb) binding=mongodb; extra=(-p "mongodb.url=mongodb://$MONGODB_ADDR/ycsb?w=1");;
  esac
  for ((i=0; i<=RUN_REPEATS; i++)); do
    phase=run; stem="run_$i"; [[ "$i" != 0 ]] || { phase=load; stem=load; }
    command=("$GOYCSB_BIN" "$phase" "$binding" "${extra[@]}" -p "recordcount=$RECORDCOUNT" -p "operationcount=$OPERATIONCOUNT" -p "threadcount=$THREADCOUNT" -p fieldcount=10 -p fieldlength=100 -p readallfields=true -p writeallfields=false)
    log_command env -u GOMAXPROCS "${command[@]}"
    "$PYTHON" "$VALIDATOR" launch "$OUT_DIR/$target" "$stem" -- "${command[@]}"
  done
done
cleanup
"$PYTHON" "$VALIDATOR" validate "$OUT_DIR"
