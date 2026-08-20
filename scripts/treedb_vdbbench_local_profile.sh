#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Profile warmed Cohere 1M searches against an existing TreeDB index.

Required:
  DB_DIR=/path/to/prebuilt-db
  VDBBENCH_DIR=/path/to/snissn/vectordbbench-at-pr7

Example:
  DB_DIR=/mnt/fast4tb/treedb-high-recall-local-20260820/db/cohere1m-scalar-u8-8c6ef660 \
  VDBBENCH_DIR=/path/to/vectordbbench \
  DATASET_DIR=/path/to/vdb-dataset \
    scripts/treedb_vdbbench_local_profile.sh

Useful overrides:
  RUN_DIR, SERVICE_BIN, PYTHON_BIN, DATASET_DIR, EFS (default 600,1000),
  CONCURRENCY (default GOMAXPROCS), PROFILE_SECONDS, PROFILE_DELAY_SECONDS,
  TRACE_SECONDS, WARMUP_SECONDS, CELL_SECONDS, SERVICE_ADDR, PPROF_ADDR,
  DRY_RUN=true.

The script never loads data, creates an index, or rebuilds a graph. Each cell
runs a full-diagnostic warmup/route proof before the production IDs-only row.
EOF
}

is_true() {
	case "${1,,}" in
		1|true|yes|on) return 0 ;;
		*) return 1 ;;
	esac
}

quote_cmd() {
	local arg
	for arg in "$@"; do
		printf '%q ' "$arg"
	done
	printf '\n'
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 2
}

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DRY_RUN=${DRY_RUN:-false}
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
	usage
	exit 0
elif [[ ${1:-} == --dry-run ]]; then
	DRY_RUN=true
	shift
fi
[[ $# -eq 0 ]] || die "unexpected argument: $1"

DB_DIR=${DB_DIR:-}
VDBBENCH_DIR=${VDBBENCH_DIR:-}
[[ -n $DB_DIR ]] || die "DB_DIR is required"
[[ -n $VDBBENCH_DIR ]] || die "VDBBENCH_DIR is required"
[[ -d $DB_DIR ]] || die "DB_DIR is not a directory: $DB_DIR"
adapter=$VDBBENCH_DIR/vectordb_bench/backend/clients/treedb/cli.py
[[ -f $adapter ]] || die "VectorDBBench TreeDB adapter not found: $VDBBENCH_DIR"
if ! grep -q 'stats_mode' "$adapter" || ! grep -q 'response_format' "$adapter"; then
	die "VectorDBBench TreeDB adapter lacks production/IDs support (use PR #7 or later)"
fi

if [[ -z ${RUN_DIR:-} ]]; then
	run_base=/mnt/fast4tb
	[[ -d $run_base && -w $run_base ]] || run_base=${TMPDIR:-/tmp}
	RUN_DIR=$run_base/treedb_vdbbench_profile_$(date -u +%Y%m%dT%H%M%SZ)
fi
DATASET_DIR=${DATASET_DIR:-/tmp/vectordb_bench/dataset}
SERVICE_ADDR=${SERVICE_ADDR:-127.0.0.1:7120}
PPROF_ADDR=${PPROF_ADDR:-127.0.0.1:6060}
SERVICE_URL=http://$SERVICE_ADDR
PPROF_URL=http://$PPROF_ADDR/debug/pprof
EFS=${EFS:-600,1000}
RERANK_CANDIDATES=${RERANK_CANDIDATES:-200}
TOP_K=${TOP_K:-100}
GOMAXPROCS_VALUE=${GOMAXPROCS:-$(getconf _NPROCESSORS_ONLN)}
CONCURRENCY=${CONCURRENCY:-$GOMAXPROCS_VALUE}
PROFILE_SECONDS=${PROFILE_SECONDS:-60}
TRACE_SECONDS=${TRACE_SECONDS:-5}
PROFILE_DELAY_SECONDS=${PROFILE_DELAY_SECONDS:-12}
WARMUP_SECONDS=${WARMUP_SECONDS:-5}
CELL_SECONDS=${CELL_SECONDS:-$((PROFILE_DELAY_SECONDS + PROFILE_SECONDS + TRACE_SECONDS + 10))}
HEALTH_TIMEOUT_SECONDS=${HEALTH_TIMEOUT_SECONDS:-300}
INDEX_NAME=${INDEX_NAME:-cohere1m_scalar_u8_8c6ef660}
QUANTIZED_INDEX_NAME=${QUANTIZED_INDEX_NAME:-embedding.scalar_u8.fast}
SERVICE_BIN=${SERVICE_BIN:-$RUN_DIR/bin/treedb-document-service}
PYTHON_BIN=${PYTHON_BIN:-$VDBBENCH_DIR/.venv/bin/python}
[[ -x $PYTHON_BIN ]] || PYTHON_BIN=$(command -v python3 || true)
[[ -n $PYTHON_BIN && -x $PYTHON_BIN ]] || die "set PYTHON_BIN to a Python with VectorDBBench dependencies"
command -v curl >/dev/null || die "curl is required"
command -v setsid >/dev/null || die "setsid is required"
command -v go >/dev/null || die "go is required"

for value in "$RERANK_CANDIDATES" "$TOP_K" "$GOMAXPROCS_VALUE" "$CONCURRENCY" "$PROFILE_SECONDS" "$TRACE_SECONDS" "$PROFILE_DELAY_SECONDS" "$WARMUP_SECONDS" "$CELL_SECONDS"; do
	[[ $value =~ ^[0-9]+$ && $value -gt 0 ]] || die "positive integer required, got: $value"
done
(( RERANK_CANDIDATES >= TOP_K )) || die "RERANK_CANDIDATES must be at least TOP_K"
(( CELL_SECONDS > PROFILE_DELAY_SECONDS + PROFILE_SECONDS + TRACE_SECONDS )) || die "CELL_SECONDS must cover profile delay, CPU profile, and trace"

IFS=',' read -r -a ef_values <<<"$EFS"
[[ ${#ef_values[@]} -gt 0 ]] || die "EFS is empty"
for ef in "${ef_values[@]}"; do
	[[ $ef =~ ^[0-9]+$ && $ef -gt 0 ]] || die "EFS must contain positive integers: $EFS"
done

mkdir -p "$RUN_DIR" "$RUN_DIR/bin" "$RUN_DIR/cells"
SEARCH_SCRIPT=$RUN_DIR/search_existing_index.py
printf '%s\n' \
	'"""Run VDBBench search stages against a verified existing TreeDB index."""' \
	'import runpy' \
	'from treedb_client import TreeDBClient' \
	'TreeDBClient.create_index = lambda *args, **kwargs: None' \
	'if __name__ == "__main__":' \
	'    runpy.run_module("vectordb_bench.cli.vectordbbench", run_name="__main__")' \
	>"$SEARCH_SCRIPT"

service_pid=
client_pid=
telemetry_pid=
cleanup() {
	local pid
	for pid in "$telemetry_pid" "$client_pid" "$service_pid"; do
		if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
			kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
		fi
	done
	for pid in "$client_pid" "$service_pid"; do
		if [[ -n $pid ]]; then
			wait "$pid" 2>/dev/null || true
		fi
	done
}
trap cleanup EXIT INT TERM

write_context() {
	{
		printf 'started_utc=%s\n' "$(date --iso-8601=ns --utc)"
		printf 'gomap_root=%s\n' "$ROOT"
		printf 'gomap_head=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
		printf 'gomap_status=%s\n' "$(git -C "$ROOT" status --short | tr '\n' ';')"
		printf 'vectordbbench_dir=%s\n' "$(realpath "$VDBBENCH_DIR")"
		printf 'vectordbbench_head=%s\n' "$(git -C "$VDBBENCH_DIR" rev-parse HEAD 2>/dev/null || printf unknown)"
		printf 'db_dir=%s\n' "$(realpath "$DB_DIR")"
		stat "$DB_DIR" || true
		find "$DB_DIR" -maxdepth 1 -name '.durable-root-rebound-*' -printf 'db_marker=%f\n' 2>/dev/null || true
		printf 'dataset_dir=%s\n' "$DATASET_DIR"
		printf 'service_addr=%s\npprof_addr=%s\n' "$SERVICE_ADDR" "$PPROF_ADDR"
		printf 'efs=%s\nrerank_candidates=%s\ntop_k=%s\nconcurrency=%s\n' "$EFS" "$RERANK_CANDIDATES" "$TOP_K" "$CONCURRENCY"
		printf 'profile_seconds=%s\ntrace_seconds=%s\nprofile_delay_seconds=%s\nwarmup_seconds=%s\ncell_seconds=%s\n' "$PROFILE_SECONDS" "$TRACE_SECONDS" "$PROFILE_DELAY_SECONDS" "$WARMUP_SECONDS" "$CELL_SECONDS"
		printf 'gomaxprocs=%s\n' "$GOMAXPROCS_VALUE"
		go version
		uname -a
		printf '\n# lscpu\n'
		lscpu 2>/dev/null || true
		printf '\n# memory\n'
		free -h 2>/dev/null || true
		printf '\n# filesystem\n'
		df -h "$DB_DIR" "$RUN_DIR" 2>/dev/null || true
		printf '\n# uptime/load\n'
		uptime || true
		printf '\n# competing processes\n'
		ps -axo pid,ppid,psr,nlwp,pcpu,pmem,rss,etimes,comm,args --sort=-pcpu 2>/dev/null | head -n 50 || true
	} >"$RUN_DIR/context.txt"
}

common_command() {
	local ef=$1
	local duration=$2
	local concurrency=$3
	VDB_CMD=(
		"$PYTHON_BIN" "$SEARCH_SCRIPT" treedbhnsw
		--skip-drop-old --skip-load --skip-search-serial --search-concurrent
		--case-type Performance768D1M --k "$TOP_K"
		--num-concurrency "$concurrency" --concurrency-duration "$duration"
		--base-url "$SERVICE_URL" --index-name "$INDEX_NAME" --timeout 300
		--query-embedding-encoding f32_le --use-vector-index
		--query-mode quantized_rerank --m 16 --ef-construction 300 --ef-search "$ef"
		--quantized-codec scalar_u8 --quantized-index-name "$QUANTIZED_INDEX_NAME"
		--quantized-rerank-candidates "$RERANK_CANDIDATES"
	)
}

record_command() {
	local dest=$1
	local phase_dir
	shift
	phase_dir=$(dirname "$dest")
	{
		printf 'PYTHONPATH=%q DATASET_LOCAL_DIR=%q RESULTS_LOCAL_DIR=%q LOG_FILE=%q ' \
			"$VDBBENCH_DIR:$ROOT/clients/python/treedb_client/src" "$DATASET_DIR" "$phase_dir/results" "$phase_dir/vdbbench.log"
		quote_cmd "$@"
	} >"$dest"
}

run_vdbbench() {
	local cell_dir=$1
	local phase=$2
	shift 2
	local phase_dir=$cell_dir/$phase
	mkdir -p "$phase_dir/results"
	record_command "$phase_dir/command.txt" "$@"
	env \
		PYTHONPATH="$VDBBENCH_DIR:$ROOT/clients/python/treedb_client/src" \
		DATASET_LOCAL_DIR="$DATASET_DIR" RESULTS_LOCAL_DIR="$phase_dir/results" \
		LOG_FILE="$phase_dir/vdbbench.log" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
		MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
		"$@" >"$phase_dir/stdout.log" 2>"$phase_dir/stderr.log"
}

capture() {
	local dest=$1
	local endpoint=$2
	curl --fail --silent --show-error --output "$dest" "$PPROF_URL/$endpoint"
	[[ -s $dest ]] || die "empty profile: $dest"
}

process_snapshot() {
	local dest=$1
	{
		date --iso-8601=ns --utc
		ps -o pid,ppid,psr,nlwp,pcpu,pmem,rss,vsz,etimes,cmd -p "$service_pid"
		printf '\n# /proc/status\n'
		cat "/proc/$service_pid/status"
		printf '\n# /proc/smaps_rollup\n'
		cat "/proc/$service_pid/smaps_rollup"
		printf '\n# vmstat\n'
		vmstat 1 3 2>/dev/null || true
	} >"$dest"
}

pprof_top() {
	local dest=$1
	shift
	go tool pprof -top "$@" >"$dest"
}

write_context
BUILD_CMD=(env GOWORK=off go build -trimpath -o "$SERVICE_BIN" ./cmd/treedb-document-service)
SERVICE_CMD=(env "GOMAXPROCS=$GOMAXPROCS_VALUE" "$SERVICE_BIN" -dir "$DB_DIR" -addr "$SERVICE_ADDR" -profile command_wal_durable -pprof "$PPROF_ADDR" -block-profile-rate 1 -mutex-profile-fraction 1)
quote_cmd "${BUILD_CMD[@]}" >"$RUN_DIR/build-command.txt"
quote_cmd "${SERVICE_CMD[@]}" >"$RUN_DIR/service-command.txt"

for ef in "${ef_values[@]}"; do
	cell_dir=$RUN_DIR/cells/ef${ef}-r${RERANK_CANDIDATES}
	mkdir -p "$cell_dir/warmup/results" "$cell_dir/profile/results"
	common_command "$ef" "$WARMUP_SECONDS" "$CONCURRENCY"
	WARMUP_CMD=("${VDB_CMD[@]}" --stats-mode full_diagnostics --response-format full --require-vector-index-guards --db-label "TreeDB-warmup-ef${ef}-r${RERANK_CANDIDATES}" --task-label "warmup-ef${ef}-r${RERANK_CANDIDATES}")
	record_command "$cell_dir/warmup/command.txt" "${WARMUP_CMD[@]}"
	common_command "$ef" "$CELL_SECONDS" "$CONCURRENCY"
	PROFILE_CMD=("${VDB_CMD[@]}" --stats-mode production --response-format ids --skip-vector-index-guards --db-label "TreeDB-profile-ef${ef}-r${RERANK_CANDIDATES}" --task-label "profile-ef${ef}-r${RERANK_CANDIDATES}")
	record_command "$cell_dir/profile/command.txt" "${PROFILE_CMD[@]}"
done

if is_true "$DRY_RUN"; then
	printf 'dry run: no service or benchmark was started\n'
	printf 'run_dir=%s\n' "$RUN_DIR"
	cat "$RUN_DIR/build-command.txt" "$RUN_DIR/service-command.txt"
	find "$RUN_DIR/cells" -name command.txt -print -exec cat {} \;
	exit 0
fi

if [[ ! -x $SERVICE_BIN ]]; then
	(cd "$ROOT" && "${BUILD_CMD[@]}")
fi
sha256sum "$SERVICE_BIN" >"$RUN_DIR/service-binary.sha256"
setsid "${SERVICE_CMD[@]}" >"$RUN_DIR/service.log" 2>&1 &
service_pid=$!
printf '%s\n' "$service_pid" >"$RUN_DIR/service.pid"
for ((attempt = 0; attempt < HEALTH_TIMEOUT_SECONDS; attempt++)); do
	if curl --fail --silent "$SERVICE_URL/v1/health" >"$RUN_DIR/health.json" 2>/dev/null; then
		break
	fi
	if ! kill -0 "$service_pid" 2>/dev/null; then
		cat "$RUN_DIR/service.log" >&2
		die "service exited before readiness"
	fi
	sleep 1
done
curl --fail --silent "$SERVICE_URL/v1/health" >"$RUN_DIR/health.json" || die "service did not become healthy"
curl --fail --silent "$PPROF_URL/" >/dev/null || die "pprof did not become ready"

for ef in "${ef_values[@]}"; do
	cell_dir=$RUN_DIR/cells/ef${ef}-r${RERANK_CANDIDATES}
	printf 'warming and validating efSearch=%s rerank=%s\n' "$ef" "$RERANK_CANDIDATES"
	common_command "$ef" "$WARMUP_SECONDS" "$CONCURRENCY"
	WARMUP_CMD=("${VDB_CMD[@]}" --stats-mode full_diagnostics --response-format full --require-vector-index-guards --db-label "TreeDB-warmup-ef${ef}-r${RERANK_CANDIDATES}" --task-label "warmup-ef${ef}-r${RERANK_CANDIDATES}")
	run_vdbbench "$cell_dir" warmup "${WARMUP_CMD[@]}"

	process_snapshot "$cell_dir/process-before.txt"
	capture "$cell_dir/heap-before.pb" 'heap?gc=1'
	capture "$cell_dir/allocs-before.pb" allocs
	capture "$cell_dir/block-before.pb" block
	capture "$cell_dir/mutex-before.pb" mutex
	capture "$cell_dir/goroutine-before.txt" 'goroutine?debug=2'

	common_command "$ef" "$CELL_SECONDS" "$CONCURRENCY"
	PROFILE_CMD=("${VDB_CMD[@]}" --stats-mode production --response-format ids --skip-vector-index-guards --db-label "TreeDB-profile-ef${ef}-r${RERANK_CANDIDATES}" --task-label "profile-ef${ef}-r${RERANK_CANDIDATES}")
	profile_dir=$cell_dir/profile
	setsid env \
		PYTHONPATH="$VDBBENCH_DIR:$ROOT/clients/python/treedb_client/src" \
		DATASET_LOCAL_DIR="$DATASET_DIR" RESULTS_LOCAL_DIR="$profile_dir/results" \
		LOG_FILE="$profile_dir/vdbbench.log" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
		MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
		"${PROFILE_CMD[@]}" >"$profile_dir/stdout.log" 2>"$profile_dir/stderr.log" &
	client_pid=$!
	if command -v pidstat >/dev/null; then
		setsid pidstat -rud -p "$service_pid" 1 "$((CELL_SECONDS + 5))" >"$cell_dir/pidstat.txt" 2>&1 &
		telemetry_pid=$!
	fi
	sleep "$PROFILE_DELAY_SECONDS"
	capture "$cell_dir/cpu.pb" "profile?seconds=$PROFILE_SECONDS"
	capture "$cell_dir/trace.out" "trace?seconds=$TRACE_SECONDS"
	if ! wait "$client_pid"; then
		client_pid=
		die "VDBBench profile cell failed: $profile_dir/stderr.log"
	fi
	client_pid=
	if [[ -n $telemetry_pid ]]; then
		kill -- "-$telemetry_pid" 2>/dev/null || true
		wait "$telemetry_pid" 2>/dev/null || true
		telemetry_pid=
	fi

	capture "$cell_dir/heap-after.pb" 'heap?gc=1'
	capture "$cell_dir/allocs-after.pb" allocs
	capture "$cell_dir/block.pb" block
	capture "$cell_dir/mutex.pb" mutex
	capture "$cell_dir/goroutine-after.txt" 'goroutine?debug=2'
	process_snapshot "$cell_dir/process-after.txt"

	pprof_top "$cell_dir/cpu-top.txt" "$SERVICE_BIN" "$cell_dir/cpu.pb"
	pprof_top "$cell_dir/heap-top.txt" -sample_index=inuse_space "$SERVICE_BIN" "$cell_dir/heap-after.pb"
	pprof_top "$cell_dir/allocs-top.txt" -sample_index=alloc_space "$SERVICE_BIN" "$cell_dir/allocs-after.pb"
	pprof_top "$cell_dir/allocs-delta-top.txt" -sample_index=alloc_space -base "$cell_dir/allocs-before.pb" "$SERVICE_BIN" "$cell_dir/allocs-after.pb"
	pprof_top "$cell_dir/block-delta-top.txt" -base "$cell_dir/block-before.pb" "$SERVICE_BIN" "$cell_dir/block.pb"
	pprof_top "$cell_dir/mutex-delta-top.txt" -base "$cell_dir/mutex-before.pb" "$SERVICE_BIN" "$cell_dir/mutex.pb"
done

printf 'completed_utc=%s\n' "$(date --iso-8601=ns --utc)" >>"$RUN_DIR/context.txt"
checksum_tmp=$RUN_DIR/.SHA256SUMS.tmp
(cd "$RUN_DIR" && find . -type f ! -name SHA256SUMS ! -name .SHA256SUMS.tmp -print0 | sort -z | xargs -0 sha256sum) >"$checksum_tmp"
mv "$checksum_tmp" "$RUN_DIR/SHA256SUMS"
printf 'profiles retained in %s\n' "$RUN_DIR"
