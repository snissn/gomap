#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/treedb_ycsb_compare_XXXXXX")}"
GOYCSB_DIR="${GOYCSB_DIR:-/home/mikers/dev/pingcap/go-ycsb}"
RECORDCOUNT="${RECORDCOUNT:-100000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-10000}"
THREADCOUNT="${THREADCOUNT:-16}"
TREEDB_PROFILE="${TREEDB_PROFILE:-command_wal_relaxed}"
TREEDB_MONGO_DOCUMENT_FORMAT="${TREEDB_MONGO_DOCUMENT_FORMAT:-bson}"
NATIVE_ADDR="${NATIVE_ADDR:-127.0.0.1:17130}"
TREEDB_MONGO_ADDR="${TREEDB_MONGO_ADDR:-127.0.0.1:27130}"
MONGODB_MODE="${MONGODB_MODE:-docker}"
MONGODB_ADDR="${MONGODB_ADDR:-127.0.0.1:27018}"
MONGODB_IMAGE="${MONGODB_IMAGE:-mongo:8}"
RUN_REPEATS="${RUN_REPEATS:-1}"
BUILD="${BUILD:-true}"
BUILD_GOYCSB="${BUILD_GOYCSB:-false}"
ALLOW_YCSB_ERRORS="${ALLOW_YCSB_ERRORS:-false}"
PARSE_ONLY="${PARSE_ONLY:-false}"

usage() {
  cat <<'EOF'
Usage: scripts/ycsb_compare_mongodb_treedb.sh

Runs the same external go-ycsb load/run shape against:
  - MongoDB through the go-ycsb mongodb binding
  - TreeDB nativewire through the go-ycsb treedb-native binding
  - TreeDB Mongo gateway through the go-ycsb mongodb binding

Outputs:
  $OUT_DIR/host.txt
  $OUT_DIR/commands.txt
  $OUT_DIR/summary.tsv
  $OUT_DIR/summary.md
  $OUT_DIR/{mongodb,treedb_native,treedb_mongo}/*.out
  server logs and DB directories for each target

Environment overrides:
  OUT_DIR, GOYCSB_DIR, RECORDCOUNT, OPERATIONCOUNT, THREADCOUNT,
  TREEDB_PROFILE, TREEDB_MONGO_DOCUMENT_FORMAT,
  NATIVE_ADDR, TREEDB_MONGO_ADDR,
  MONGODB_MODE, MONGODB_ADDR, MONGODB_IMAGE,
  RUN_REPEATS, BUILD, BUILD_GOYCSB,
  ALLOW_YCSB_ERRORS, PARSE_ONLY.

MONGODB_MODE values:
  docker    Start a fresh Docker container from MONGODB_IMAGE (default).
  external  Use an already-running MongoDB at MONGODB_ADDR.
  skip      Do not run the MongoDB baseline.

PARSE_ONLY=true expects an existing OUT_DIR with raw *.out files and only
regenerates summary.tsv/summary.md. By default, any nonzero YCSB *_ERROR
operation count marks the run invalid and exits nonzero; set
ALLOW_YCSB_ERRORS=true for exploratory invalid-result summaries.

Examples:
  scripts/ycsb_compare_mongodb_treedb.sh
  RUN_REPEATS=3 OUT_DIR=/tmp/ycsb_compare scripts/ycsb_compare_mongodb_treedb.sh
  MONGODB_MODE=external MONGODB_ADDR=127.0.0.1:27017 scripts/ycsb_compare_mongodb_treedb.sh
EOF
}

if [[ "${1-}" == "-h" || "${1-}" == "--help" ]]; then
  usage
  exit 0
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

case "$MONGODB_MODE" in
  docker|external|skip) ;;
  *)
    echo "unknown MONGODB_MODE=$MONGODB_MODE; expected docker, external, or skip" >&2
    exit 2
    ;;
esac

if [[ "$RUN_REPEATS" -lt 1 ]]; then
  echo "RUN_REPEATS must be >= 1" >&2
  exit 2
fi

case "$ALLOW_YCSB_ERRORS" in
  true|false) ;;
  *)
    echo "ALLOW_YCSB_ERRORS must be true or false" >&2
    exit 2
    ;;
esac

case "$PARSE_ONLY" in
  true|false) ;;
  *)
    echo "PARSE_ONLY must be true or false" >&2
    exit 2
    ;;
esac

if [[ "$PARSE_ONLY" != "true" ]]; then
  if [[ ! -x "$GOYCSB_DIR/bin/go-ycsb" ]]; then
    BUILD_GOYCSB=true
  fi

  if [[ "$BUILD" == "true" ]]; then
    go build -o bin/treedb-native-server ./cmd/treedb-native-server
    go build -o bin/treedb-mongo-gateway ./TreeDB/mongo_gateway/server.go
  fi

  if [[ "$BUILD_GOYCSB" == "true" ]]; then
    (cd "$GOYCSB_DIR" && make build)
  fi
fi

GOYCSB="$GOYCSB_DIR/bin/go-ycsb"

SERVER_PIDS=()
MONGO_CONTAINER=""

cleanup() {
  local pid
  for pid in "${SERVER_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${SERVER_PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  if [[ -n "$MONGO_CONTAINER" ]]; then
    docker rm -f "$MONGO_CONTAINER" >/dev/null 2>&1 || true
  fi
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

write_host_info() {
  {
    printf 'date_utc='
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    printf 'gomap_commit='
    git rev-parse HEAD
    printf 'gomap_branch='
    git rev-parse --abbrev-ref HEAD
    printf 'go_ycsb_commit='
    (cd "$GOYCSB_DIR" && git rev-parse HEAD)
    printf 'go_ycsb_branch='
    (cd "$GOYCSB_DIR" && git rev-parse --abbrev-ref HEAD)
    printf 'go_version='
    go version
    printf 'uname='
    uname -a
    printf 'cpu_model='
    lscpu | awk -F: '/Model name/{gsub(/^ +/, "", $2); print $2; exit}'
    printf 'logical_cpus='
    nproc
    printf 'memory='
    free -h | awk '/Mem:/{print $2}'
    printf 'clocksource='
    cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null || true
    printf 'recordcount=%s\n' "$RECORDCOUNT"
    printf 'operationcount=%s\n' "$OPERATIONCOUNT"
    printf 'threadcount=%s\n' "$THREADCOUNT"
    printf 'treedb_profile=%s\n' "$TREEDB_PROFILE"
    printf 'treedb_mongo_document_format=%s\n' "$TREEDB_MONGO_DOCUMENT_FORMAT"
    printf 'mongodb_mode=%s\n' "$MONGODB_MODE"
    printf 'mongodb_image=%s\n' "$MONGODB_IMAGE"
    if command -v docker >/dev/null 2>&1 && [[ "$MONGODB_MODE" == "docker" ]]; then
      docker image inspect "$MONGODB_IMAGE" \
        --format 'mongodb_image_id={{.Id}} mongodb_image_created={{.Created}} mongodb_image_os={{.Os}} mongodb_image_arch={{.Architecture}} mongodb_image_digests={{json .RepoDigests}}' \
        2>/dev/null || true
    fi
  } >"$OUT_DIR/host.txt"
}

log_command() {
  printf '%q ' "$@" >>"$OUT_DIR/commands.txt"
  printf '\n' >>"$OUT_DIR/commands.txt"
}

run_ycsb_load_run() {
  local target=$1
  local binding=$2
  local prop_name=$3
  local prop_value=$4
  local dir="$OUT_DIR/$target"
  mkdir -p "$dir"

  local load_cmd=(
    "$GOYCSB" load "$binding"
    -p "$prop_name=$prop_value"
    -p "recordcount=$RECORDCOUNT"
    -p "operationcount=$OPERATIONCOUNT"
    -p "threadcount=$THREADCOUNT"
  )
  log_command "${load_cmd[@]}"
  "${load_cmd[@]}" >"$dir/load.out"

  local i
  for i in $(seq 1 "$RUN_REPEATS"); do
    local run_cmd=(
      "$GOYCSB" run "$binding"
      -p "$prop_name=$prop_value"
      -p "recordcount=$RECORDCOUNT"
      -p "operationcount=$OPERATIONCOUNT"
      -p "threadcount=$THREADCOUNT"
    )
    log_command "${run_cmd[@]}"
    "${run_cmd[@]}" >"$dir/run_${i}.out"
  done
}

write_summary() {
  python - "$OUT_DIR" "$ALLOW_YCSB_ERRORS" <<'PY'
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])
allow_errors = sys.argv[2] == "true"

line_re = re.compile(
    r"^(?P<op>[A-Z_]+)\s+- Takes\(s\): (?P<takes>[0-9.]+), Count: (?P<count>[0-9]+), "
    r"OPS: (?P<ops>[0-9.]+), Avg\(us\): (?P<avg>[0-9.]+), Min\(us\): (?P<min>[0-9.]+), "
    r"Max\(us\): (?P<max>[0-9.]+), 50th\(us\): (?P<p50>[0-9.]+), 90th\(us\): (?P<p90>[0-9.]+), "
    r"95th\(us\): (?P<p95>[0-9.]+), 99th\(us\): (?P<p99>[0-9.]+), "
    r"99.9th\(us\): (?P<p999>[0-9.]+), 99.99th\(us\): (?P<p9999>[0-9.]+)"
)

labels = {
    "mongodb": "MongoDB",
    "treedb_native": "TreeDB nativewire",
    "treedb_mongo": "TreeDB Mongo gateway",
}

rows_by_key = {}
row_sequence = 0
for target_dir in sorted(out.iterdir()):
    if not target_dir.is_dir() or target_dir.name not in labels:
        continue
    for path in sorted(target_dir.glob("*.out")):
        if path.name == "load.out":
            phase = "load"
            repeat = 0
        elif path.name.startswith("run_"):
            phase = "run"
            repeat = int(path.stem.split("_", 1)[1])
        else:
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            m = line_re.match(line)
            if not m:
                continue
            data = m.groupdict()
            row_sequence += 1
            row = {
                "target": target_dir.name,
                "target_label": labels[target_dir.name],
                "phase": phase,
                "repeat": repeat,
                "operation": data["op"],
                "is_error": data["op"].endswith("_ERROR"),
                "sequence": row_sequence,
                "output": str(path.relative_to(out)),
            }
            for key in ("takes", "ops", "avg", "min", "max", "p50", "p90", "p95", "p99", "p999", "p9999"):
                row[key] = float(data[key])
            row["count"] = int(data["count"])
            rows_by_key[(target_dir.name, phase, repeat, data["op"])] = row

rows = sorted(rows_by_key.values(), key=lambda row: row["sequence"])

phase_error_counts = {}
for row in rows:
    key = (row["target"], row["phase"], row["repeat"])
    phase_error_counts.setdefault(key, 0)
    if row["is_error"]:
        phase_error_counts[key] += row["count"]

for row in rows:
    row["phase_error_count"] = phase_error_counts[(row["target"], row["phase"], row["repeat"])]
    row["valid"] = row["phase_error_count"] == 0

headers = [
    "target",
    "phase",
    "repeat",
    "operation",
    "count",
    "ops_per_sec",
    "avg_us",
    "p50_us",
    "p95_us",
    "p99_us",
    "p999_us",
    "max_us",
    "is_error",
    "phase_error_count",
    "valid",
    "output",
]

with (out / "summary.tsv").open("w", encoding="utf-8") as f:
    f.write("\t".join(headers) + "\n")
    for row in rows:
        f.write("\t".join([
            row["target"],
            row["phase"],
            str(row["repeat"]),
            row["operation"],
            str(row["count"]),
            f"{row['ops']:.1f}",
            f"{row['avg']:.1f}",
            f"{row['p50']:.1f}",
            f"{row['p95']:.1f}",
            f"{row['p99']:.1f}",
            f"{row['p999']:.1f}",
            f"{row['max']:.1f}",
            "true" if row["is_error"] else "false",
            str(row["phase_error_count"]),
            "true" if row["valid"] else "false",
            row["output"],
        ]) + "\n")

def pick(target, phase, operation, repeat=1):
    for row in rows:
        if row["target"] == target and row["phase"] == phase and row["operation"] == operation and row["repeat"] == repeat:
            return row
    return None

def md_num(value):
    return f"{value:,.1f}"

with (out / "summary.md").open("w", encoding="utf-8") as f:
    f.write("# YCSB MongoDB / TreeDB Comparison\n\n")
    f.write("## Host\n\n")
    f.write("```text\n")
    host_path = out / "host.txt"
    if host_path.exists():
        f.write(host_path.read_text(encoding="utf-8", errors="replace"))
    else:
        f.write("host metadata unavailable\n")
    f.write("```\n\n")
    f.write("## Validity\n\n")
    invalid = [
        (target, phase, repeat, count)
        for (target, phase, repeat), count in sorted(phase_error_counts.items())
        if count
    ]
    if invalid:
        f.write("At least one YCSB phase emitted nonzero `*_ERROR` operation counts. Those rows are invalid performance evidence.\n\n")
        f.write("| target | phase | repeat | operation errors |\n")
        f.write("| --- | --- | ---: | ---: |\n")
        for target, phase, repeat, count in invalid:
            f.write(f"| {labels[target]} | {phase} | {repeat} | {count:,} |\n")
        f.write("\n")
    else:
        f.write("All parsed YCSB phases have zero `*_ERROR` operation counts.\n\n")
    f.write("## Headline Totals\n\n")
    f.write("| target | load status | load errors | load ops/sec | run repeat | run status | run errors | run ops/sec | run avg us | run p99 us | run max us |\n")
    f.write("| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |\n")
    for target in ("mongodb", "treedb_native", "treedb_mongo"):
        load = pick(target, "load", "TOTAL", 0)
        repeats = sorted({row["repeat"] for row in rows if row["target"] == target and row["phase"] == "run"})
        if not load and not repeats:
            continue
        for repeat in repeats or [1]:
            run = pick(target, "run", "TOTAL", repeat)
            if not run:
                continue
            load_errors = load["phase_error_count"] if load else 0
            run_errors = run["phase_error_count"]
            f.write(
                f"| {labels[target]} | {'valid' if not load_errors else 'invalid'} | {load_errors:,} | "
                f"{md_num(load['ops']) if load else ''} | {repeat} | {'valid' if not run_errors else 'invalid'} | "
                f"{run_errors:,} | {md_num(run['ops'])} | {md_num(run['avg'])} | {md_num(run['p99'])} | {md_num(run['max'])} |\n"
            )
    f.write("\n## Detailed Rows\n\n")
    f.write("| target | phase | repeat | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | p99.9 us | max us | op error | phase errors | valid | output |\n")
    f.write("| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- |\n")
    for row in rows:
        f.write(
            f"| {row['target_label']} | {row['phase']} | {row['repeat']} | {row['operation']} | "
            f"{row['count']:,} | {md_num(row['ops'])} | {md_num(row['avg'])} | {md_num(row['p50'])} | "
            f"{md_num(row['p95'])} | {md_num(row['p99'])} | {md_num(row['p999'])} | {md_num(row['max'])} | "
            f"{'yes' if row['is_error'] else 'no'} | {row['phase_error_count']:,} | {'yes' if row['valid'] else 'no'} | "
            f"`{row['output']}` |\n"
        )
    f.write("\n## Commands\n\n")
    f.write("```sh\n")
    commands_path = out / "commands.txt"
    if commands_path.exists():
        f.write(commands_path.read_text(encoding="utf-8", errors="replace"))
    f.write("```\n")

if invalid and not allow_errors:
    print(
        f"YCSB operation errors detected in {len(invalid)} phase(s); "
        "set ALLOW_YCSB_ERRORS=true to keep exploratory invalid summaries",
        file=sys.stderr,
    )
    sys.exit(3)
PY
}

if [[ "$PARSE_ONLY" == "true" ]]; then
  write_summary
  echo "wrote $OUT_DIR/summary.tsv"
  echo "wrote $OUT_DIR/summary.md"
  exit 0
fi

write_host_info
: >"$OUT_DIR/commands.txt"

native_dir="$OUT_DIR/treedb_native/db"
mkdir -p "$native_dir"
./bin/treedb-native-server \
  -dir "$native_dir" \
  -profile "$TREEDB_PROFILE" \
  -addr "$NATIVE_ADDR" \
  >"$OUT_DIR/treedb_native/server.log" 2>&1 &
SERVER_PIDS+=("$!")
wait_tcp "$NATIVE_ADDR"

mongo_gateway_dir="$OUT_DIR/treedb_mongo/db"
mkdir -p "$mongo_gateway_dir"
./bin/treedb-mongo-gateway \
  -dir "$mongo_gateway_dir" \
  -profile "$TREEDB_PROFILE" \
  -document-format "$TREEDB_MONGO_DOCUMENT_FORMAT" \
  -addr "$TREEDB_MONGO_ADDR" \
  >"$OUT_DIR/treedb_mongo/server.log" 2>&1 &
SERVER_PIDS+=("$!")
wait_tcp "$TREEDB_MONGO_ADDR"

if [[ "$MONGODB_MODE" == "docker" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required for MONGODB_MODE=docker" >&2
    exit 2
  fi
  MONGO_CONTAINER="treedb-ycsb-mongo-$(date +%s)-$$"
  mongodb_dir="$OUT_DIR/mongodb/db"
  mkdir -p "$mongodb_dir"
  docker run --rm -d --name "$MONGO_CONTAINER" \
    -p "$MONGODB_ADDR:27017" \
    -v "$mongodb_dir:/data/db" \
    "$MONGODB_IMAGE" --bind_ip_all --quiet \
    >"$OUT_DIR/mongodb/container.id"
  wait_tcp "$MONGODB_ADDR"
  sleep 2
fi

if [[ "$MONGODB_MODE" != "skip" ]]; then
  run_ycsb_load_run \
    mongodb \
    mongodb \
    mongodb.url \
    "mongodb://$MONGODB_ADDR/ycsb?w=1"
fi

run_ycsb_load_run \
  treedb_native \
  treedb-native \
  treedb.addr \
  "$NATIVE_ADDR"

run_ycsb_load_run \
  treedb_mongo \
  mongodb \
  mongodb.url \
  "mongodb://$TREEDB_MONGO_ADDR/ycsb?w=1"

if [[ -n "$MONGO_CONTAINER" ]]; then
  docker logs "$MONGO_CONTAINER" >"$OUT_DIR/mongodb/server.log" 2>&1 || true
fi

write_summary

echo "wrote $OUT_DIR/summary.tsv"
echo "wrote $OUT_DIR/summary.md"
