#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_rabitq_1bit_profile_gate_$(date +%Y%m%d_%H%M%S)}"
SHAPE="${SHAPE:-1024x128}"
BENCH_ROWS="${BENCH_ROWS:-}"
BENCH_DIMS="${BENCH_DIMS:-}"
BENCH_M="${BENCH_M:-16}"
BENCH_TOP_K="${BENCH_TOP_K:-10}"
BENCH_EF_SEARCH="${BENCH_EF_SEARCH:-128}"
BENCH_QUERY_ORDINAL="${BENCH_QUERY_ORDINAL:-37}"
ROWS="${ROWS:-claim_core}"
PROFILE_ROWS="${PROFILE_ROWS:-rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8}"
BENCHTIME="${BENCHTIME:-}"
TIMING_BENCHTIME="${TIMING_BENCHTIME:-}"
PROFILE_BENCHTIME="${PROFILE_BENCHTIME:-}"
TIMING_COUNT="${TIMING_COUNT:-5}"
PROFILE_COUNT="${PROFILE_COUNT:-1}"
RUN_TIMING="${RUN_TIMING:-true}"
RUN_PROFILES="${RUN_PROFILES:-true}"
DRY_RUN="${DRY_RUN:-false}"
PROFILE_SCOPE="${PROFILE_SCOPE:-search_loop}"
GOMAXPROCS_VALUE="${GOMAXPROCS:-8}"
GOWORK_VALUE="${GOWORK:-off}"
PPROF_FRAMES="${PPROF_FRAMES:-columnVectorGraphRabitQQuantizedScorer.scoreOrdinalUnchecked,columnVectorGraphRabitQQuantizedScorer.scoreOrdinals,ScoreCosine,scoreAndPushFrontierVisitedTile,frontierSiftDown,insertTop,fetchTopPreparedSearchResults,acquireCollectionVectorIndexPreparedSearch}"
BASELINE_DIR="${BASELINE_DIR:-}"
RECALL_TOLERANCE_PCT="${RECALL_TOLERANCE_PCT:-0}"
BENCHMARK_LOCK="${BENCHMARK_LOCK:-}"
HOT_CPU_PROFILE_ENV=TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH
HOT_ALLOCS_PROFILE_ENV=TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_PROFILE_PATH
HOT_ALLOCS_BASE_PROFILE_ENV=TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_BASE_PROFILE_PATH
HOT_MEM_PROFILE_RATE_ENV=TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_MEM_PROFILE_RATE
HOT_MEM_PROFILE_RATE="${HOT_MEM_PROFILE_RATE:-${TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_MEM_PROFILE_RATE:-1}}"
ALLOC_PROFILE_IGNORE="${ALLOC_PROFILE_IGNORE:-runtime/pprof|compress/gzip|github.com/snissn/gomap/TreeDB/collections\\.\\(\\*columnGraphQuantizedSearchLoopProfileHook2541\\)|github.com/snissn/gomap/TreeDB/collections\\.writeColumnGraphQuantizedHotProfile2541|github.com/snissn/gomap/TreeDB/collections\\.createColumnGraphQuantizedHotProfileFile2541|time\\.Sleep|sync\\.\\(\\*WaitGroup\\)\\.Wait}"

mkdir -p "$RUN_DIR"

is_true() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		1|true|yes|y|on) return 0 ;;
		*) return 1 ;;
	esac
}

sanitize() {
	printf '%s' "$1" | tr -c 'A-Za-z0-9_.=-' '_'
}

quote_cmd() {
	local first=1 arg
	for arg in "$@"; do
		if [[ $first -eq 0 ]]; then
			printf ' '
		fi
		printf '%q' "$arg"
		first=0
	done
	printf '\n'
}

parse_shape_token() {
	local raw=$1
	local normalized rows rows_suffix dims dims_suffix
	normalized=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
	normalized=${normalized//_x_/x}
	normalized=${normalized//_/}
	case "$normalized" in
		""|default|claimcore)
			printf '1024 128\n'
			return 0
			;;
	esac
	if [[ "$normalized" =~ ^([0-9]+)(k?)x([0-9]+)(k?)$ ]]; then
		rows=${BASH_REMATCH[1]}
		rows_suffix=${BASH_REMATCH[2]}
		dims=${BASH_REMATCH[3]}
		dims_suffix=${BASH_REMATCH[4]}
		if [[ "$rows_suffix" == "k" ]]; then
			rows=$((rows * 1000))
		fi
		if [[ "$dims_suffix" == "k" ]]; then
			dims=$((dims * 1000))
		fi
		printf '%s %s\n' "$rows" "$dims"
		return 0
	fi
	printf 'unsupported SHAPE=%q (want default, 1024x128, 10k_x_1536, or <rows>x<dims>)\n' "$raw" >&2
	return 2
}

require_positive_int() {
	local name=$1 value=$2
	if ! [[ "$value" =~ ^[0-9]+$ ]] || ((value <= 0)); then
		printf '%s=%q must be a positive integer\n' "$name" "$value" >&2
		return 2
	fi
}

require_nonnegative_int() {
	local name=$1 value=$2
	if ! [[ "$value" =~ ^[0-9]+$ ]]; then
		printf '%s=%q must be a non-negative integer\n' "$name" "$value" >&2
		return 2
	fi
}

validate_profile_scope() {
	case "$PROFILE_SCOPE" in
		search_loop|go_test) ;;
		*)
			printf 'unsupported PROFILE_SCOPE=%q (want search_loop or go_test)\n' "$PROFILE_SCOPE" >&2
			return 2
			;;
	esac
	require_positive_int HOT_MEM_PROFILE_RATE "$HOT_MEM_PROFILE_RATE"
}

resolve_bench_shape() {
	local parsed shape_rows shape_dims
	parsed=$(parse_shape_token "$SHAPE")
	read -r shape_rows shape_dims <<<"$parsed"
	BENCH_ROWS="${BENCH_ROWS:-$shape_rows}"
	BENCH_DIMS="${BENCH_DIMS:-$shape_dims}"
	require_positive_int BENCH_ROWS "$BENCH_ROWS"
	require_positive_int BENCH_DIMS "$BENCH_DIMS"
	require_positive_int BENCH_M "$BENCH_M"
	require_positive_int BENCH_TOP_K "$BENCH_TOP_K"
	require_positive_int BENCH_EF_SEARCH "$BENCH_EF_SEARCH"
	require_nonnegative_int BENCH_QUERY_ORDINAL "$BENCH_QUERY_ORDINAL"
	if ((BENCH_TOP_K > BENCH_ROWS)); then
		printf 'BENCH_TOP_K=%s exceeds BENCH_ROWS=%s\n' "$BENCH_TOP_K" "$BENCH_ROWS" >&2
		return 2
	fi
	if ((BENCH_QUERY_ORDINAL >= BENCH_ROWS)); then
		printf 'BENCH_QUERY_ORDINAL=%s out of range for BENCH_ROWS=%s\n' "$BENCH_QUERY_ORDINAL" "$BENCH_ROWS" >&2
		return 2
	fi
	if [[ "$BENCH_ROWS" == "10000" && "$BENCH_DIMS" == "1536" ]]; then
		SHAPE_LABEL=10k_x_1536
	else
		SHAPE_LABEL="${BENCH_ROWS}x${BENCH_DIMS}"
	fi
	if [[ -z "$BENCHTIME" ]]; then
		if [[ "$SHAPE_LABEL" == "10k_x_1536" ]]; then
			BENCHTIME=1000x
		else
			BENCHTIME=100000x
		fi
	fi
	TIMING_BENCHTIME="${TIMING_BENCHTIME:-$BENCHTIME}"
	PROFILE_BENCHTIME="${PROFILE_BENCHTIME:-$BENCHTIME}"
}

write_go_test_env_prefix() {
	printf 'GOMAXPROCS=%q GOWORK=%q ' "$GOMAXPROCS_VALUE" "$GOWORK_VALUE"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=%q ' "$SHAPE"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_ROWS=%q ' "$BENCH_ROWS"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_DIMS=%q ' "$BENCH_DIMS"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_M=%q ' "$BENCH_M"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_TOP_K=%q ' "$BENCH_TOP_K"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_EF_SEARCH=%q ' "$BENCH_EF_SEARCH"
	printf 'TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERY_ORDINAL=%q ' "$BENCH_QUERY_ORDINAL"
}

write_pprof_top() {
	local profile=$1
	local dest=$2
	shift 2
	if [[ ! -s "$profile" ]]; then
		printf 'profile %s is missing or empty\n' "$profile" >"$dest"
		return 0
	fi
	if ! go tool pprof "$@" "$profile" >"$dest.tmp" 2>"$dest.err"; then
		{
			printf 'go tool pprof failed for %s\n' "$profile"
			cat "$dest.err"
		} >"$dest"
	else
		mv "$dest.tmp" "$dest"
	fi
	rm -f "$dest.tmp" "$dest.err"
}

write_pprof_list() {
	local profile=$1
	local frame=$2
	local dest=$3
	if [[ ! -s "$profile" ]]; then
		printf 'profile %s is missing or empty\n' "$profile" >"$dest"
		return 0
	fi
	if ! go tool pprof -list="$frame" "$profile" >"$dest.tmp" 2>"$dest.err"; then
		{
			printf 'go tool pprof -list=%s failed for %s\n' "$frame" "$profile"
			cat "$dest.err"
		} >"$dest"
	else
		mv "$dest.tmp" "$dest"
	fi
	rm -f "$dest.tmp" "$dest.err"
}

write_pprof_diff() {
	local base=$1
	local after=$2
	local dest=$3
	if [[ ! -s "$base" || ! -s "$after" ]]; then
		printf 'diff base %s or profile %s is missing or empty\n' "$base" "$after" >&2
		return 1
	fi
	if ! go tool pprof -proto -diff_base "$base" "$after" >"$dest.tmp" 2>"$dest.err"; then
		cat "$dest.err" >&2
		rm -f "$dest.tmp" "$dest.err"
		return 1
	fi
	mv "$dest.tmp" "$dest"
	rm -f "$dest.err"
}

write_pprof_filtered() {
	local src=$1
	local dest=$2
	local ignore=$3
	if [[ ! -s "$src" ]]; then
		printf 'profile %s is missing or empty\n' "$src" >&2
		return 1
	fi
	if [[ -z "$ignore" ]]; then
		cp "$src" "$dest"
		return 0
	fi
	if ! go tool pprof -proto -ignore="$ignore" "$src" >"$dest.tmp" 2>"$dest.err"; then
		cat "$dest.err" >&2
		rm -f "$dest.tmp" "$dest.err"
		return 1
	fi
	mv "$dest.tmp" "$dest"
	rm -f "$dest.err"
}

matches_selector() {
	local selector=$1
	local id=$2
	local codec=$3
	local layer=$4
	local mode=$5
	local concurrency=$6
	case "$selector" in
		all) return 0 ;;
		claim_core) [[ "$mode" == "quantized_only" && ( "$codec" == "rabitq_1bit" || "$codec" == "scalar_u8" ) ]] ;;
		claim_rerank|claim_with_rerank) [[ ( "$codec" == "rabitq_1bit" || "$codec" == "scalar_u8" ) && ( "$mode" == "quantized_only" || "$mode" == "quantized_rerank" ) ]] ;;
		target|rabitq|rabitq_1bit) [[ "$codec" == "rabitq_1bit" ]] ;;
		rabitq_only|rabitq_quantized_only) [[ "$codec" == "rabitq_1bit" && "$mode" == "quantized_only" ]] ;;
		rabitq_rerank|rabitq_quantized_rerank|rabitq_quantized_rerank32) [[ "$codec" == "rabitq_1bit" && "$mode" == "quantized_rerank" ]] ;;
		scalar|scalar_u8|scalar_guardrail|scalar_guardrails) [[ "$codec" == "scalar_u8" ]] ;;
		scalar_only|scalar_quantized_only) [[ "$codec" == "scalar_u8" && "$mode" == "quantized_only" ]] ;;
		scalar_rerank|scalar_quantized_rerank|scalar_quantized_rerank32) [[ "$codec" == "scalar_u8" && "$mode" == "quantized_rerank" ]] ;;
		lower) [[ "$layer" == "lower" ]] ;;
		collection) [[ "$layer" == "collection" ]] ;;
		quantized_only) [[ "$mode" == "quantized_only" ]] ;;
		quantized_rerank|rerank|rerank32) [[ "$mode" == "quantized_rerank" ]] ;;
		c1|c=1) [[ "$concurrency" == "1" ]] ;;
		c8|c=8) [[ "$concurrency" == "8" ]] ;;
		*) [[ "$selector" == "$id" ]] ;;
	esac
}

selected_by_list() {
	local selector_list=$1
	local id=$2
	local codec=$3
	local layer=$4
	local mode=$5
	local concurrency=$6
	local old_ifs=$IFS part
	IFS=','
	for part in $selector_list; do
		part=${part//[[:space:]]/}
		if [[ -z "$part" ]]; then
			continue
		fi
		if matches_selector "$part" "$id" "$codec" "$layer" "$mode" "$concurrency"; then
			IFS=$old_ifs
			return 0
		fi
	done
	IFS=$old_ifs
	return 1
}

validate_profile_scope
resolve_bench_shape

write_context() {
	local dest=$1
	{
		printf 'TreeDB rabitq_1bit profile gate context\n'
		printf '=======================================\n\n'
		printf 'run_dir: %s\n' "$RUN_DIR"
		printf 'worktree: %s\n' "$ROOT"
		printf 'branch: %s\n' "$(git rev-parse --abbrev-ref HEAD)"
		printf 'commit: %s\n' "$(git rev-parse HEAD)"
		printf 'short_commit: %s\n' "$(git rev-parse --short HEAD)"
		printf 'origin_main: %s\n' "$(git rev-parse origin/main 2>/dev/null || true)"
		printf 'go_version: %s\n' "$(go version)"
		printf 'go_env: '
		GOWORK="$GOWORK_VALUE" go env GOOS GOARCH GOVERSION GOMOD GOWORK CGO_ENABLED | tr '\n' ' '
		printf '\n'
		printf 'GOMAXPROCS: %s\n' "$GOMAXPROCS_VALUE"
		printf 'GOWORK: %s\n' "$GOWORK_VALUE"
		printf 'shape: %s\n' "$SHAPE_LABEL"
		printf 'shape_selector: %s\n' "$SHAPE"
		printf 'fixture: rows=%s dims=%s m=%s topK=%s efSearch=%s queryOrdinal=%s\n' "$BENCH_ROWS" "$BENCH_DIMS" "$BENCH_M" "$BENCH_TOP_K" "$BENCH_EF_SEARCH" "$BENCH_QUERY_ORDINAL"
		printf 'rows: %s\n' "$ROWS"
		printf 'profile_rows: %s\n' "$PROFILE_ROWS"
		printf 'timing: enabled=%s benchtime=%s count=%s\n' "$RUN_TIMING" "$TIMING_BENCHTIME" "$TIMING_COUNT"
		printf 'profiles: enabled=%s benchtime=%s count=%s\n' "$RUN_PROFILES" "$PROFILE_BENCHTIME" "$PROFILE_COUNT"
		printf 'profile_scope: %s\n' "$PROFILE_SCOPE"
		printf 'hot_profile_env: %s %s %s %s=%s\n' "$HOT_CPU_PROFILE_ENV" "$HOT_ALLOCS_PROFILE_ENV" "$HOT_ALLOCS_BASE_PROFILE_ENV" "$HOT_MEM_PROFILE_RATE_ENV" "$HOT_MEM_PROFILE_RATE"
		printf 'alloc_profile_ignore: %s\n' "$ALLOC_PROFILE_IGNORE"
		printf 'dry_run: %s\n' "$DRY_RUN"
		printf 'benchmark_lock: %s\n' "$BENCHMARK_LOCK"
		printf 'baseline_dir: %s\n' "$BASELINE_DIR"
		printf 'recall_tolerance_pct: %s\n' "$RECALL_TOLERANCE_PCT"
		printf 'pprof_frames: %s\n' "$PPROF_FRAMES"
		printf '\n# git status --short\n'
		git status --short || true
		printf '\n# uptime\n'
		uptime || true
		printf '\n# top competing processes by %%cpu\n'
		ps -axo pid,pcpu,pmem,comm,args 2>/dev/null | sort -nrk2 | head -n 40 || true
		printf '\n# visible benchmark/test/go processes\n'
		ps -axo pid,pcpu,pmem,comm,args 2>/dev/null | grep -E '(^|/)(go|compile|link|gomap|bench|test)( |$)|go test|pprof|benchstat|TreeDB' | grep -v grep || true
	} >"$dest"
}

run_go_test() {
	local outfile=$1
	shift
	GOMAXPROCS="$GOMAXPROCS_VALUE" \
		GOWORK="$GOWORK_VALUE" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE="$SHAPE" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_ROWS="$BENCH_ROWS" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_DIMS="$BENCH_DIMS" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_M="$BENCH_M" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_TOP_K="$BENCH_TOP_K" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_EF_SEARCH="$BENCH_EF_SEARCH" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERY_ORDINAL="$BENCH_QUERY_ORDINAL" \
		"$@" 2>&1 | tee "$outfile"
}

run_search_loop_profile_go_test() {
	local outfile=$1
	local cpu_profile=$2
	local alloc_profile=$3
	local alloc_base_profile=$4
	shift 4
	env \
		GOMAXPROCS="$GOMAXPROCS_VALUE" \
		GOWORK="$GOWORK_VALUE" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE="$SHAPE" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_ROWS="$BENCH_ROWS" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_DIMS="$BENCH_DIMS" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_M="$BENCH_M" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_TOP_K="$BENCH_TOP_K" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_EF_SEARCH="$BENCH_EF_SEARCH" \
		TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERY_ORDINAL="$BENCH_QUERY_ORDINAL" \
		"$HOT_CPU_PROFILE_ENV=$cpu_profile" \
		"$HOT_ALLOCS_PROFILE_ENV=$alloc_profile" \
		"$HOT_ALLOCS_BASE_PROFILE_ENV=$alloc_base_profile" \
		"$HOT_MEM_PROFILE_RATE_ENV=$HOT_MEM_PROFILE_RATE" \
		"$@" 2>&1 | tee "$outfile"
}

run_row() {
	local id=$1
	local codec=$2
	local layer=$3
	local mode=$4
	local concurrency=$5
	local rerank_candidates=$6
	local bench_regex=$7
	local profile_selected=$8
	local profile_captured=false
	if ! is_true "$DRY_RUN" && is_true "$RUN_PROFILES" && is_true "$profile_selected"; then
		profile_captured=true
	fi
	local row_dir="$RUN_DIR/$id"
	local timing_txt="$row_dir/bench_timing.txt"
	local profile_txt="$row_dir/bench_profile.txt"
	local cpu_profile="$row_dir/cpu.pprof"
	local alloc_profile="$row_dir/allocs.pprof"
	local alloc_base_profile="$row_dir/allocs_base.pprof"
	local alloc_raw_profile="$row_dir/allocs_raw.pprof"
	local alloc_diff_raw_profile="$row_dir/allocs_diff_raw.pprof"
	local block_profile="$row_dir/block.pprof"
	local mutex_profile="$row_dir/mutex.pprof"
	local lists_dir="$row_dir/pprof_lists"
	local test_binary="$row_dir/collections.test"

	mkdir -p "$row_dir" "$lists_dir"
	{
		printf 'row_id=%s\n' "$id"
		printf 'codec=%s\n' "$codec"
		printf 'layer=%s\n' "$layer"
		printf 'mode=%s\n' "$mode"
		printf 'concurrency=%s\n' "$concurrency"
		printf 'rerank_candidates=%s\n' "$rerank_candidates"
		printf 'profile_selected=%s\n' "$profile_selected"
		printf 'profile_captured=%s\n' "$profile_captured"
		printf 'profile_scope=%s\n' "$PROFILE_SCOPE"
		printf 'bench_regex=%s\n' "$bench_regex"
		printf 'shape=%s\n' "$SHAPE_LABEL"
		printf 'fixture_rows=%s\n' "$BENCH_ROWS"
		printf 'fixture_dims=%s\n' "$BENCH_DIMS"
		printf 'fixture_m=%s\n' "$BENCH_M"
		printf 'fixture_top_k=%s\n' "$BENCH_TOP_K"
		printf 'fixture_ef_search=%s\n' "$BENCH_EF_SEARCH"
		printf 'fixture_query_ordinal=%s\n' "$BENCH_QUERY_ORDINAL"
		printf 'fixture=%s rows / %s dims / topK=%s / efSearch=%s / queryOrdinal=%s\n' "$BENCH_ROWS" "$BENCH_DIMS" "$BENCH_TOP_K" "$BENCH_EF_SEARCH" "$BENCH_QUERY_ORDINAL"
	} >"$row_dir/row.env"

	local timing_cmd=(go test ./TreeDB/collections -run '^$' -bench "$bench_regex" -benchmem -benchtime "$TIMING_BENCHTIME" -count "$TIMING_COUNT")
	local profile_cmd=(go test ./TreeDB/collections -run '^$' -bench "$bench_regex" -benchmem -benchtime "$PROFILE_BENCHTIME" -count "$PROFILE_COUNT" -o "$test_binary")
	if [[ "$PROFILE_SCOPE" == "go_test" ]]; then
		profile_cmd+=(-cpuprofile "$cpu_profile" -memprofile "$alloc_profile" -blockprofile "$block_profile" -mutexprofile "$mutex_profile")
	fi

	{
		write_go_test_env_prefix
		quote_cmd "${timing_cmd[@]}"
	} >"$row_dir/command_timing.txt"
	{
		write_go_test_env_prefix
		if [[ "$PROFILE_SCOPE" == "search_loop" ]]; then
			printf '%s=%q %s=%q %s=%q %s=%q ' "$HOT_CPU_PROFILE_ENV" "$cpu_profile" "$HOT_ALLOCS_PROFILE_ENV" "$alloc_raw_profile" "$HOT_ALLOCS_BASE_PROFILE_ENV" "$alloc_base_profile" "$HOT_MEM_PROFILE_RATE_ENV" "$HOT_MEM_PROFILE_RATE"
		fi
		quote_cmd "${profile_cmd[@]}"
	} >"$row_dir/command_profile.txt"

	if is_true "$DRY_RUN"; then
		printf 'dry run: timing command not executed\n' >"$timing_txt"
		printf 'dry run: profile command not executed\n' >"$profile_txt"
		for dest in "$row_dir/cpu_top.txt" "$row_dir/allocs_top.txt" "$row_dir/block_top.txt" "$row_dir/mutex_top.txt"; do
			printf 'dry run: no profile captured\n' >"$dest"
		done
	else
		if is_true "$RUN_TIMING"; then
			printf '==> timing %s\n' "$id"
			run_go_test "$timing_txt" "${timing_cmd[@]}"
		else
			printf 'timing skipped for %s\n' "$id" >"$timing_txt"
		fi

		if is_true "$RUN_PROFILES" && is_true "$profile_selected"; then
			printf '==> profiles %s\n' "$id"
			if [[ "$PROFILE_SCOPE" == "search_loop" ]]; then
				run_search_loop_profile_go_test "$profile_txt" "$cpu_profile" "$alloc_raw_profile" "$alloc_base_profile" "${profile_cmd[@]}"
				if [[ ! -s "$cpu_profile" ]]; then
					printf 'missing search-loop CPU profile %s\n' "$cpu_profile" >&2
					exit 1
				fi
				if [[ ! -s "$alloc_raw_profile" ]]; then
					printf 'missing search-loop raw allocs profile %s\n' "$alloc_raw_profile" >&2
					exit 1
				fi
				if [[ ! -s "$alloc_base_profile" ]]; then
					printf 'missing search-loop allocs baseline profile %s\n' "$alloc_base_profile" >&2
					exit 1
				fi
				write_pprof_diff "$alloc_base_profile" "$alloc_raw_profile" "$alloc_diff_raw_profile"
				write_pprof_filtered "$alloc_diff_raw_profile" "$alloc_profile" "$ALLOC_PROFILE_IGNORE"
			else
				run_go_test "$profile_txt" "${profile_cmd[@]}"
			fi
			write_pprof_top "$cpu_profile" "$row_dir/cpu_top.txt" -top
			write_pprof_top "$alloc_profile" "$row_dir/allocs_top.txt" -top -sample_index=alloc_space
			if [[ "$PROFILE_SCOPE" == "go_test" ]]; then
				write_pprof_top "$block_profile" "$row_dir/block_top.txt" -top
				write_pprof_top "$mutex_profile" "$row_dir/mutex_top.txt" -top
			else
				printf 'block profile not captured for PROFILE_SCOPE=search_loop\n' >"$row_dir/block_top.txt"
				printf 'mutex profile not captured for PROFILE_SCOPE=search_loop\n' >"$row_dir/mutex_top.txt"
			fi

			local old_ifs=$IFS frame safe
			IFS=','
			for frame in $PPROF_FRAMES; do
				frame=${frame//[[:space:]]/}
				[[ -z "$frame" ]] && continue
				safe=$(sanitize "$frame")
				write_pprof_list "$cpu_profile" "$frame" "$lists_dir/${safe}.txt"
			done
			IFS=$old_ifs
		else
			printf 'profiles skipped for %s (RUN_PROFILES=%s profile_selected=%s)\n' "$id" "$RUN_PROFILES" "$profile_selected" >"$profile_txt"
			for dest in "$row_dir/cpu_top.txt" "$row_dir/allocs_top.txt" "$row_dir/block_top.txt" "$row_dir/mutex_top.txt"; do
				printf 'profile not selected for %s\n' "$id" >"$dest"
			done
		fi
	fi

	cat >"$row_dir/README.md" <<EOF
# RaBitQ profile gate row: $id

- codec: \`$codec\`
- layer: \`$layer\`
- mode: \`$mode\`
- concurrency: \`$concurrency\`
- rerank candidates: \`$rerank_candidates\`
- benchmark regex: \`$bench_regex\`
- fixture: \`$SHAPE_LABEL\` ($BENCH_ROWS rows / $BENCH_DIMS dims / \`topK=$BENCH_TOP_K\` / \`efSearch=$BENCH_EF_SEARCH\` / query ordinal $BENCH_QUERY_ORDINAL)
- timing command: \`$(tr '\n' ' ' <"$row_dir/command_timing.txt")\`
- profile command: \`$(tr '\n' ' ' <"$row_dir/command_profile.txt")\`
- profile selected: \`$profile_selected\`
- profile captured: \`$profile_captured\`
- profile scope: \`$PROFILE_SCOPE\`

Artifacts:

- \`bench_timing.txt\`: unprofiled per-row timing/guardrail output.
- \`bench_profile.txt\`: profiled benchmark output when this row is in \`PROFILE_ROWS\` and \`RUN_PROFILES=true\`.
- \`cpu.pprof\`, \`allocs.pprof\`: CPU/allocation profiles when \`profile_captured=true\`; for \`PROFILE_SCOPE=search_loop\`, \`allocs.pprof\` is \`allocs_diff_raw.pprof\` filtered by \`ALLOC_PROFILE_IGNORE\` so setup allocations and pprof-writer noise are removed.
- \`allocs_base.pprof\`, \`allocs_raw.pprof\`, \`allocs_diff_raw.pprof\`: supporting allocation profiles emitted only for \`PROFILE_SCOPE=search_loop\`.
- \`block.pprof\`, \`mutex.pprof\`: emitted only for \`PROFILE_SCOPE=go_test\`.
- \`collections.test\`: test binary emitted by \`go test -o\` when \`profile_captured=true\`.
- \`cpu_top.txt\`, \`allocs_top.txt\`, \`block_top.txt\`, \`mutex_top.txt\`.
- \`pprof_lists/*.txt\`: line-level CPU excerpts for configured target frames.

Use unprofiled \`bench_timing.txt\` rows for \`ns/op\`, \`B/op\`, and
\`allocs/op\`. With \`PROFILE_SCOPE=search_loop\`, \`cpu.pprof\` and
\`allocs.pprof\` come from benchmark-controlled hooks that start after fixture
setup, vector-index rebuild, collection prepared-cache warmup, and worker
warmup. \`HOT_MEM_PROFILE_RATE\` is preserved while \`allocs_raw.pprof\` is written
so Go pprof scales sampled allocations correctly before the harness filters the
published \`allocs.pprof\`. With \`PROFILE_SCOPE=go_test\`, Go's test-level
profiling flags include that setup/rebuild work and are kept only as a
compatibility fallback.
EOF
}

add_row() {
	local id=$1
	local codec=$2
	local layer=$3
	local mode=$4
	local concurrency=$5
	local rerank_candidates=$6
	local bench_regex=$7
	if selected_by_list "$ROWS" "$id" "$codec" "$layer" "$mode" "$concurrency"; then
		local profile_selected=false
		if selected_by_list "$PROFILE_ROWS" "$id" "$codec" "$layer" "$mode" "$concurrency"; then
			profile_selected=true
		fi
		if [[ "$PROFILE_SCOPE" == "search_loop" && "$profile_selected" == "true" && "$codec" != "rabitq_1bit" ]]; then
			printf 'PROFILE_SCOPE=search_loop only supports rabitq_1bit profile rows; row %s uses codec=%s. Set PROFILE_SCOPE=go_test or remove this row from PROFILE_ROWS.\n' "$id" "$codec" >&2
			exit 2
		fi
		row_ids+=("$id")
		row_codecs+=("$codec")
		row_layers+=("$layer")
		row_modes+=("$mode")
		row_concurrency+=("$concurrency")
		row_rerank_candidates+=("$rerank_candidates")
		row_regexes+=("$bench_regex")
		row_profile_selected+=("$profile_selected")
	fi
}

row_ids=()
row_codecs=()
row_layers=()
row_modes=()
row_concurrency=()
row_rerank_candidates=()
row_regexes=()
row_profile_selected=()

rabitq_lower_bench='^BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451$'
rabitq_collection_bench='^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452$'
scalar_lower_bench='^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$'
scalar_collection_bench='^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$'

add_quantized_rows() {
	local prefix=$1
	local codec=$2
	local lower_bench=$3
	local collection_bench=$4
	add_row "${prefix}_lower_quantized_only_c1" "$codec" lower quantized_only 1 0 "$lower_bench/^route=quantized_only$/^c=1$"
	add_row "${prefix}_lower_quantized_only_c8" "$codec" lower quantized_only 8 0 "$lower_bench/^route=quantized_only$/^c=8$"
	add_row "${prefix}_lower_quantized_rerank32_c1" "$codec" lower quantized_rerank 1 32 "$lower_bench/^route=quantized_rerank$/^candidates=32$/^c=1$"
	add_row "${prefix}_lower_quantized_rerank32_c8" "$codec" lower quantized_rerank 8 32 "$lower_bench/^route=quantized_rerank$/^candidates=32$/^c=8$"
	add_row "${prefix}_collection_quantized_only_c1" "$codec" collection quantized_only 1 0 "$collection_bench/^route=quantized_only$/^c=1$"
	add_row "${prefix}_collection_quantized_only_c8" "$codec" collection quantized_only 8 0 "$collection_bench/^route=quantized_only$/^c=8$"
	add_row "${prefix}_collection_quantized_rerank32_c1" "$codec" collection quantized_rerank 1 32 "$collection_bench/^route=quantized_rerank$/^candidates=32$/^c=1$"
	add_row "${prefix}_collection_quantized_rerank32_c8" "$codec" collection quantized_rerank 8 32 "$collection_bench/^route=quantized_rerank$/^candidates=32$/^c=8$"
}

add_quantized_rows rabitq rabitq_1bit "$rabitq_lower_bench" "$rabitq_collection_bench"
add_quantized_rows scalar scalar_u8 "$scalar_lower_bench" "$scalar_collection_bench"

if [[ ${#row_ids[@]} -eq 0 ]]; then
	printf 'no rows selected by ROWS=%s\n' "$ROWS" >&2
	exit 2
fi

write_context "$RUN_DIR/context.txt"

{
	printf 'row_id\tcodec\tlayer\tmode\tconcurrency\trerank_candidates\tprofile_selected\tshape\tfixture_rows\tfixture_dims\tfixture_top_k\tfixture_ef_search\tbench_regex\n'
	for i in "${!row_ids[@]}"; do
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${row_ids[$i]}" "${row_codecs[$i]}" "${row_layers[$i]}" "${row_modes[$i]}" "${row_concurrency[$i]}" "${row_rerank_candidates[$i]}" "${row_profile_selected[$i]}" "$SHAPE_LABEL" "$BENCH_ROWS" "$BENCH_DIMS" "$BENCH_TOP_K" "$BENCH_EF_SEARCH" "${row_regexes[$i]}"
	done
} >"$RUN_DIR/matrix.tsv"

for i in "${!row_ids[@]}"; do
	run_row "${row_ids[$i]}" "${row_codecs[$i]}" "${row_layers[$i]}" "${row_modes[$i]}" "${row_concurrency[$i]}" "${row_rerank_candidates[$i]}" "${row_regexes[$i]}" "${row_profile_selected[$i]}"
done

python3 - "$RUN_DIR" "$BASELINE_DIR" "$RECALL_TOLERANCE_PCT" <<'PY'
import glob
import math
import os
import re
import statistics
import sys

root = sys.argv[1]
baseline_root = sys.argv[2] if len(sys.argv) > 2 else ""
try:
    recall_tolerance_pct = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else 0.0
except ValueError:
    recall_tolerance_pct = 0.0
metric_units = {
    "rows",
    "dims",
    "top_k",
    "ef_search",
    "ns/op",
    "ops/sec",
    "B/op",
    "allocs/op",
    "recall_at_k_pct",
    "docs_fetched/search",
    "vector_B/search",
    "norm_B/search",
    "exact_vector_B/vector",
    "exact_norm_B/vector",
    "exact_vector_norm_B/vector",
    "quantized_code_B/search",
    "quantized_code_B/vector",
    "quantized_asset_B/vector",
    "quantized_asset_B_total",
    "quantized_rerank_candidates/search",
    "quantized_rerank_exact_score_calls/search",
    "prepared_score_calls/search",
    "quantized_score_calls/search",
    "quantized_scorer_active/search",
    "quantized_asset_missing/search",
    "quantized_asset_invalid/search",
    "quantized_asset_stale/search",
    "quantized_asset_closed/search",
    "quantized_asset_unavailable/search",
    "prepared_graph_search_views/search",
    "score_float64_fallbacks/search",
    "graph_row_fallbacks/search",
    "typed_column_vector_fallbacks/search",
    "vector_scratch_decodes/search",
    "search_route_quantized_only/search",
    "search_route_quantized_rerank/search",
    "search_route_column_graph_prepared/search",
    "search_route_column_graph_fallback/search",
    "open_setup_in_timed_loop",
    "open_searcher_calls/op",
    "collection_prepared_cache_builds/op",
    "collection_prepared_cache_hits/op",
    "collection_prepared_cache_misses/op",
    "collection_prepared_cache_waits/op",
    "collection_prepared_cache_errors/op",
}

number_re = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
bench_re = re.compile(r"^Benchmark\S+")


def parse_file(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not bench_re.match(line):
                continue
            fields = line.split()
            if len(fields) < 4 or not fields[1].isdigit():
                continue
            metrics = {"benchmark": fields[0], "iterations": float(fields[1])}
            i = 2
            while i + 1 < len(fields):
                value, unit = fields[i], fields[i + 1]
                if number_re.match(value):
                    try:
                        metrics[unit] = float(value)
                    except ValueError:
                        pass
                    i += 2
                else:
                    i += 1
            rows.append(metrics)
    return rows


def median(values):
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not values:
        return None
    return statistics.median(values)


def fmt(value):
    if value is None:
        return ""
    if not isinstance(value, (int, float)):
        return str(value)
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    if abs(value) >= 1000:
        return f"{value:.0f}"
    return f"{value:.3f}".rstrip("0").rstrip(".")


def present_all(rows, name):
    return all(name in row for row in rows)


def present_all_zero(rows, name):
    return present_all(rows, name) and all(row.get(name) == 0 for row in rows)


def present_all_eq(rows, name, want):
    return present_all(rows, name) and all(row.get(name) == want for row in rows)


def present_all_le(rows, name, limit):
    return present_all(rows, name) and all(row.get(name) <= limit for row in rows)


def present_all_ge(rows, name, minimum):
    return present_all(rows, name) and all(row.get(name) >= minimum for row in rows)


def recall_guardrail_ok(bench_rows, baseline_rows):
    if not present_all(bench_rows, "recall_at_k_pct"):
        return False
    if baseline_rows and present_all(baseline_rows, "recall_at_k_pct"):
        candidate = median([row.get("recall_at_k_pct") for row in bench_rows])
        baseline = median([row.get("recall_at_k_pct") for row in baseline_rows])
        return candidate is not None and baseline is not None and candidate + recall_tolerance_pct >= baseline
    return present_all_ge(bench_rows, "recall_at_k_pct", 99.999)


def guardrail_status(meta, bench_rows, baseline_rows):
    if not bench_rows:
        return "n/a"
    checks = []
    checks.append(present_all_zero(bench_rows, "B/op"))
    checks.append(present_all_zero(bench_rows, "allocs/op"))
    checks.append(present_all_zero(bench_rows, "docs_fetched/search"))
    for name in (
        "graph_row_fallbacks/search",
        "typed_column_vector_fallbacks/search",
        "vector_scratch_decodes/search",
        "score_float64_fallbacks/search",
        "quantized_asset_missing/search",
        "quantized_asset_invalid/search",
        "quantized_asset_stale/search",
        "quantized_asset_closed/search",
        "quantized_asset_unavailable/search",
    ):
        checks.append(present_all_zero(bench_rows, name))
    checks.append(present_all_eq(bench_rows, "search_route_column_graph_prepared/search", 1))
    checks.append(present_all_eq(bench_rows, "quantized_scorer_active/search", 1))
    checks.append(recall_guardrail_ok(bench_rows, baseline_rows))

    try:
        fixture_dims = int(meta.get("fixture_dims") or 0)
    except ValueError:
        fixture_dims = 0
    if fixture_dims <= 0:
        dims_median = median([row.get("dims") for row in bench_rows])
        fixture_dims = int(dims_median or 0)
    if meta.get("codec") == "rabitq_1bit":
        code_dims = 1
        while code_dims < fixture_dims:
            code_dims *= 2
        expected_code_bytes = (code_dims + 7) // 8
    else:
        expected_code_bytes = fixture_dims
    checks.append(expected_code_bytes > 0 and present_all_eq(bench_rows, "quantized_code_B/vector", expected_code_bytes))
    checks.append(present_all(bench_rows, "quantized_asset_B/vector"))

    mode = meta.get("mode", "")
    if mode == "quantized_only":
        checks.append(present_all_eq(bench_rows, "search_route_quantized_only/search", 1))
        checks.append(present_all_zero(bench_rows, "search_route_quantized_rerank/search"))
        checks.append(present_all_zero(bench_rows, "vector_B/search"))
        checks.append(present_all_zero(bench_rows, "norm_B/search"))
        checks.append(present_all_zero(bench_rows, "quantized_rerank_candidates/search"))
        checks.append(present_all_zero(bench_rows, "quantized_rerank_exact_score_calls/search"))
        checks.append(present_all_zero(bench_rows, "prepared_score_calls/search"))
    elif mode == "quantized_rerank":
        candidates = int(meta.get("rerank_candidates") or 0)
        checks.append(present_all_zero(bench_rows, "search_route_quantized_only/search"))
        checks.append(present_all_eq(bench_rows, "search_route_quantized_rerank/search", 1))
        checks.append(present_all_eq(bench_rows, "quantized_rerank_candidates/search", candidates))
        checks.append(present_all_le(bench_rows, "quantized_rerank_exact_score_calls/search", candidates))
        checks.append(present_all_le(bench_rows, "prepared_score_calls/search", candidates))
        exact_read_ok = True
        for row in bench_rows:
            required = ("vector_B/search", "norm_B/search", "exact_vector_B/vector", "exact_norm_B/vector")
            if any(name not in row for name in required):
                exact_read_ok = False
                break
            if row["vector_B/search"] > candidates * row["exact_vector_B/vector"]:
                exact_read_ok = False
                break
            if row["norm_B/search"] > candidates * row["exact_norm_B/vector"]:
                exact_read_ok = False
                break
        checks.append(exact_read_ok)
    return "pass" if all(checks) else "check"

selected_row_ids = []
matrix_path = os.path.join(root, "matrix.tsv")
if os.path.exists(matrix_path):
    with open(matrix_path, "r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle):
            if line_number == 0:
                continue
            fields = line.rstrip("\n").split("\t")
            if fields and fields[0]:
                selected_row_ids.append(fields[0])

summary_rows = []
for row_id in selected_row_ids:
    row_dir = os.path.join(root, row_id)
    env_path = os.path.join(row_dir, "row.env")
    if not os.path.exists(env_path):
        summary_rows.append({"row_id": row_id, "source": "row.env", "status": "missing row.env", "guardrail_status": "n/a"})
        continue
    meta = {}
    with open(env_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "=" in line:
                key, value = line.rstrip("\n").split("=", 1)
                meta[key] = value
    source = "bench_timing.txt"
    bench_rows = parse_file(os.path.join(row_dir, source))
    if not bench_rows:
        source = "bench_profile.txt"
        bench_rows = parse_file(os.path.join(row_dir, source))
    if not bench_rows:
        summary_rows.append({"row_id": row_id, "source": source, "status": "no benchmark rows", "guardrail_status": "n/a", **meta})
        continue
    out = {"row_id": row_id, "source": source, "status": "ok", **meta}
    all_metrics = sorted({key for row in bench_rows for key in row.keys()} | metric_units)
    for unit in all_metrics:
        if unit == "benchmark" or unit in meta:
            continue
        values = [row.get(unit) for row in bench_rows]
        out[unit] = median(values)
        out[unit + "__max"] = max([v for v in values if v is not None], default=None)
    baseline_rows = []
    if baseline_root:
        baseline_rows = parse_file(os.path.join(baseline_root, row_id, "bench_timing.txt"))
        if not baseline_rows:
            baseline_rows = parse_file(os.path.join(baseline_root, row_id, "bench_profile.txt"))
    out["guardrail_status"] = guardrail_status(meta, bench_rows, baseline_rows)
    summary_rows.append(out)

fields = [
    "row_id", "codec", "layer", "mode", "concurrency", "rerank_candidates", "profile_selected", "profile_captured", "profile_scope", "shape", "fixture_rows", "fixture_dims", "source", "status", "guardrail_status",
    "rows", "dims", "top_k", "ef_search", "ns/op", "ops/sec", "B/op", "allocs/op", "recall_at_k_pct",
    "docs_fetched/search", "vector_B/search", "norm_B/search", "quantized_code_B/search", "quantized_code_B/vector", "quantized_asset_B/vector",
    "quantized_rerank_candidates/search", "quantized_rerank_exact_score_calls/search", "prepared_score_calls/search",
    "graph_row_fallbacks/search", "typed_column_vector_fallbacks/search", "vector_scratch_decodes/search", "score_float64_fallbacks/search",
    "quantized_asset_missing/search", "quantized_asset_invalid/search", "quantized_asset_stale/search", "quantized_asset_closed/search", "quantized_asset_unavailable/search",
    "search_route_quantized_only/search", "search_route_quantized_rerank/search", "search_route_column_graph_prepared/search",
    "open_setup_in_timed_loop", "open_searcher_calls/op", "collection_prepared_cache_hits/op", "collection_prepared_cache_misses/op", "collection_prepared_cache_waits/op",
]

with open(os.path.join(root, "summary.tsv"), "w", encoding="utf-8") as out:
    out.write("\t".join(fields) + "\n")
    for row in summary_rows:
        out.write("\t".join(fmt(row.get(field)) for field in fields) + "\n")

with open(os.path.join(root, "summary.md"), "w", encoding="utf-8") as out:
    out.write("# TreeDB rabitq_1bit profile gate summary\n\n")
    out.write("This report is generated from isolated benchmark subrows. ")
    out.write("Guardrail status checks allocation, document/materialization, fallback, asset-unavailable, route, recall, code-byte, and exact-read constraints from benchmark counters.\n\n")
    out.write("| row | shape | codec | layer | mode | c | ns/op | ops/sec | B/op | allocs/op | recall@K % | code B/search | code B/vector | asset B/vector | vector B/search | norm B/search | rerank exact calls/search | guardrails | profile captured |\n")
    out.write("| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
    for row in summary_rows:
        out.write(
            "| {row_id} | {shape} | {codec} | {layer} | {mode} | {c} | {ns} | {ops} | {bop} | {allocs} | {recall} | {code_search} | {code_vector} | {asset_vector} | {vec} | {norm} | {rerank} | {guard} | {profiled} |\n".format(
                row_id=row.get("row_id", ""),
                shape=row.get("shape", ""),
                codec=row.get("codec", ""),
                layer=row.get("layer", ""),
                mode=row.get("mode", ""),
                c=row.get("concurrency", ""),
                ns=fmt(row.get("ns/op")),
                ops=fmt(row.get("ops/sec")),
                bop=fmt(row.get("B/op")),
                allocs=fmt(row.get("allocs/op")),
                recall=fmt(row.get("recall_at_k_pct")),
                code_search=fmt(row.get("quantized_code_B/search")),
                code_vector=fmt(row.get("quantized_code_B/vector")),
                asset_vector=fmt(row.get("quantized_asset_B/vector")),
                vec=fmt(row.get("vector_B/search")),
                norm=fmt(row.get("norm_B/search")),
                rerank=fmt(row.get("quantized_rerank_exact_score_calls/search")),
                guard=row.get("guardrail_status", ""),
                profiled=row.get("profile_captured", ""),
            )
        )
    out.write("\n## Artifact layout\n\n")
    out.write("Each selected row directory contains `bench_timing.txt`, `bench_profile.txt`, command files, a row README, top summaries, and CPU/alloc pprof files when `profile_captured=true` (`allocs_base.pprof`/`allocs_raw.pprof`/`allocs_diff_raw.pprof` support `PROFILE_SCOPE=search_loop`; `block.pprof`/`mutex.pprof` only for `PROFILE_SCOPE=go_test`).\n")
PY

if [[ -n "$BASELINE_DIR" ]]; then
	if command -v benchstat >/dev/null 2>&1; then
		: >"$RUN_DIR/benchstat_vs_baseline.txt"
		for id in "${row_ids[@]}"; do
			base_file="$BASELINE_DIR/$id/bench_timing.txt"
			cand_file="$RUN_DIR/$id/bench_timing.txt"
			if [[ -s "$base_file" && -s "$cand_file" ]]; then
				{
					printf '## %s\n' "$id"
					benchstat "$base_file" "$cand_file"
					printf '\n'
				} >>"$RUN_DIR/benchstat_vs_baseline.txt"
			fi
		done
	else
		printf 'benchstat not found; skipping BASELINE_DIR comparison\n' >"$RUN_DIR/benchstat_vs_baseline.txt"
	fi
fi

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB rabitq_1bit Profile Gate Bundle

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse HEAD)\`
- GOMAXPROCS: \`$GOMAXPROCS_VALUE\`
- GOWORK: \`$GOWORK_VALUE\`
- shape: \`$SHAPE_LABEL\` ($BENCH_ROWS rows / $BENCH_DIMS dims / topK=$BENCH_TOP_K / efSearch=$BENCH_EF_SEARCH / queryOrdinal=$BENCH_QUERY_ORDINAL)
- selected rows: \`$ROWS\`
- profile rows: \`$PROFILE_ROWS\`
- timing: enabled=\`$RUN_TIMING\`, benchtime=\`$TIMING_BENCHTIME\`, count=\`$TIMING_COUNT\`
- profiles: enabled=\`$RUN_PROFILES\`, benchtime=\`$PROFILE_BENCHTIME\`, count=\`$PROFILE_COUNT\`
- profile scope: \`$PROFILE_SCOPE\`
- dry run: \`$DRY_RUN\`
- benchmark lock: \`$BENCHMARK_LOCK\`
- baseline dir: \`$BASELINE_DIR\`
- recall tolerance pct: \`$RECALL_TOLERANCE_PCT\`

Primary files:

- \`context.txt\`: commit, Go version, OS/arch, uptime, and process inventory.
- \`matrix.tsv\`: exact row IDs, benchmark regexes, and profile selection.
- \`summary.md\`, \`summary.tsv\`: per-row timing medians and guardrail counter summary.
- \`<row>/bench_timing.txt\`: unprofiled benchmark output for the selected row.
- \`<row>/bench_profile.txt\`: profiled benchmark output for rows selected by \`PROFILE_ROWS\`.
- \`<row>/cpu.pprof\`, \`<row>/allocs.pprof\`: CPU/allocation profiles when \`profile_captured=true\`; for \`PROFILE_SCOPE=search_loop\`, \`allocs.pprof\` is \`allocs_diff_raw.pprof\` filtered by \`ALLOC_PROFILE_IGNORE\` so setup allocations and pprof-writer noise are removed.
- \`<row>/allocs_base.pprof\`, \`<row>/allocs_raw.pprof\`, \`<row>/allocs_diff_raw.pprof\`: supporting allocation profiles emitted only for \`PROFILE_SCOPE=search_loop\`.
  \`HOT_MEM_PROFILE_RATE\` is preserved while \`allocs_raw.pprof\` is written so Go pprof scales sampled allocations correctly before the published \`allocs.pprof\` filter runs.
- \`<row>/block.pprof\`, \`<row>/mutex.pprof\`: emitted only for \`PROFILE_SCOPE=go_test\`.
- \`<row>/collections.test\`: test binary emitted by \`go test -o\` when \`profile_captured=true\`.
- \`<row>/cpu_top.txt\`, \`<row>/allocs_top.txt\`, \`<row>/block_top.txt\`, \`<row>/mutex_top.txt\`.
- \`<row>/pprof_lists/*.txt\`: line-level CPU excerpts for target frames.

Host-load caveat: if \`context.txt\` shows competing benchmark/test processes or
high load, treat the run as contaminated and rerun before claiming a candidate
win. Smoke runs and dry runs validate shape only; they are not speedup evidence.
EOF

printf 'rabitq_1bit profile gate bundle: %s\n' "$RUN_DIR"
printf 'summary: %s\n' "$RUN_DIR/summary.md"
printf 'matrix: %s\n' "$RUN_DIR/matrix.tsv"
printf 'context: %s\n' "$RUN_DIR/context.txt"
