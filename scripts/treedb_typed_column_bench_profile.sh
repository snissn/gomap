#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/treedb_typed_column_bench_profile_$(date +%Y%m%d_%H%M%S)}"
RUN_SMOKE="${RUN_SMOKE:-true}"
RUN_1M="${RUN_1M:-false}"
RUN_HOT_PROFILE="${RUN_HOT_PROFILE:-true}"
RUN_ALLOC_PROFILE="${RUN_ALLOC_PROFILE:-true}"

ROWS="${ROWS:-4096}"
SMOKE_SHAPES="${SMOKE_SHAPES:-selective_range_1pct}"
SMOKE_DISTS="${SMOKE_DISTS:-clustered_monotonic}"
SMOKE_READ_INTEGRITY="${SMOKE_READ_INTEGRITY:-cached_verify}"
SMOKE_LAYOUTS="${SMOKE_LAYOUTS:-delta_varint}"
SMOKE_INCLUDE_FALLBACK="${SMOKE_INCLUDE_FALLBACK:-true}"
BENCHTIME="${BENCHTIME:-1x}"
COUNT="${COUNT:-1}"

ROWS_1M="${ROWS_1M:-1048576}"
SHAPES_1M="${SHAPES_1M:-no_filter,exact,tiny,range_1pct,range_10pct,all_pruned,all_match,tail}"
DISTS_1M="${DISTS_1M:-clustered,reverse,partial_clustered,random,hotspot}"
READ_INTEGRITY_1M="${READ_INTEGRITY_1M:-cached_verify}"
LAYOUTS_1M="${LAYOUTS_1M:-delta_varint}"
INCLUDE_FALLBACK_1M="${INCLUDE_FALLBACK_1M:-false}"
BENCHTIME_1M="${BENCHTIME_1M:-3x}"
COUNT_1M="${COUNT_1M:-1}"

PROFILE_ROWS="${PROFILE_ROWS:-$ROWS}"
PROFILE_SHAPE="${PROFILE_SHAPE:-selective_range_1pct}"
PROFILE_DIST="${PROFILE_DIST:-clustered_monotonic}"
PROFILE_READ_INTEGRITY="${PROFILE_READ_INTEGRITY:-cached_verify}"
PROFILE_LAYOUT="${PROFILE_LAYOUT:-delta_varint}"
PROFILE_BENCHTIME="${PROFILE_BENCHTIME:-$BENCHTIME}"
PROFILE_COUNT="${PROFILE_COUNT:-1}"

mkdir -p "$RUN_DIR"

is_true() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		1|true|yes|y|on) return 0 ;;
		*) return 1 ;;
	esac
}

canonical_shape() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		no_filter) printf '%s' "no_filter_full_aggregate" ;;
		exact|equality|equal) printf '%s' "exact_value" ;;
		tiny) printf '%s' "tiny_range" ;;
		range_1pct|selective_1pct) printf '%s' "selective_range_1pct" ;;
		range_10pct|wide_10pct) printf '%s' "wide_range_10pct" ;;
		all_pruned|no_match) printf '%s' "all_pruned_no_match" ;;
		tail|latest_window) printf '%s' "tail_range" ;;
		*) printf '%s' "$1" ;;
	esac
}

canonical_dist() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		clustered) printf '%s' "clustered_monotonic" ;;
		reverse) printf '%s' "reverse_monotonic" ;;
		partial_random|partially_random|partial_clustered) printf '%s' "partially_clustered" ;;
		random) printf '%s' "random_uniform" ;;
		hotspot|skew|skewed) printf '%s' "hotspot_skewed" ;;
		*) printf '%s' "$1" ;;
	esac
}

canonical_read_integrity() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		cached|cached_verify) printf '%s' "cached_verify" ;;
		skip|skip_checksums|unsafe_skip_checksums|unsafe_skip_checksums_ceiling) printf '%s' "unsafe_skip_checksums_ceiling" ;;
		*) printf '%s' "$1" ;;
	esac
}

canonical_layout_path() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		delta|delta_varint|"") printf '%s' "typed_column_part" ;;
		raw|raw_int64|fixed|fixed_width) printf '%s' "typed_column_part_raw_int64" ;;
		*) return 1 ;;
	esac
}

run_bench() {
	local name=$1
	local rows=$2
	local shapes=$3
	local dists=$4
	local read_integrity=$5
	local layouts=$6
	local include_fallback=$7
	local benchtime=$8
	local count=$9
	local bench_regex=${10}
	local dir="$RUN_DIR/$name"
	mkdir -p "$dir"
	cat >"$dir/env.txt" <<EOF
TREEDB_TYPED_COLUMN_BENCH_ROWS=$rows
TREEDB_TYPED_COLUMN_BENCH_SHAPES=$shapes
TREEDB_TYPED_COLUMN_BENCH_DISTS=$dists
TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY=$read_integrity
TREEDB_TYPED_COLUMN_BENCH_LAYOUTS=$layouts
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=$include_fallback
BENCHTIME=$benchtime
COUNT=$count
BENCH_REGEX=$bench_regex
EOF
	echo "running typed-column int64 aggregate $name into: $dir"
	(
		export GOWORK="${GOWORK:-off}"
		export TREEDB_TYPED_COLUMN_BENCH_ROWS="$rows"
		export TREEDB_TYPED_COLUMN_BENCH_SHAPES="$shapes"
		export TREEDB_TYPED_COLUMN_BENCH_DISTS="$dists"
		export TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY="$read_integrity"
		export TREEDB_TYPED_COLUMN_BENCH_LAYOUTS="$layouts"
		export TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK="$include_fallback"
		go test -run '^$' \
			-bench "$bench_regex" \
			-benchmem \
			-benchtime="$benchtime" \
			-count="$count" \
			./TreeDB/collections
	) 2>&1 | tee "$dir/typed_column_int64_aggregate_bench.txt"
}

run_hot_profile() {
	local dir="$RUN_DIR/hot_query_profile"
	mkdir -p "$dir"
	local shape dist read_integrity layout_path bench_regex cpu_profile alloc_profile
	shape=$(canonical_shape "$PROFILE_SHAPE")
	dist=$(canonical_dist "$PROFILE_DIST")
	read_integrity=$(canonical_read_integrity "$PROFILE_READ_INTEGRITY")
	layout_path=$(canonical_layout_path "$PROFILE_LAYOUT") || {
		echo "unsupported PROFILE_LAYOUT: $PROFILE_LAYOUT" >&2
		exit 1
	}
	bench_regex="^BenchmarkTypedColumnInt64PredicateAggregate/rows_${PROFILE_ROWS}/dist_${dist}/path_${layout_path}/shape_${shape}/timed_prepared_session_hot_scan/read_integrity_${read_integrity}/execution_serial/predicate_count_sum_avg$"
	cpu_profile="$dir/hot_query_cpu.pprof"
	alloc_profile="$dir/process_allocs.pprof"
	cat >"$dir/env.txt" <<EOF
TREEDB_TYPED_COLUMN_BENCH_ROWS=$PROFILE_ROWS
TREEDB_TYPED_COLUMN_BENCH_SHAPES=$shape
TREEDB_TYPED_COLUMN_BENCH_DISTS=$dist
TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY=$PROFILE_READ_INTEGRITY
TREEDB_TYPED_COLUMN_BENCH_LAYOUTS=$PROFILE_LAYOUT
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false
TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE=$cpu_profile
BENCHTIME=$PROFILE_BENCHTIME
COUNT=$PROFILE_COUNT
BENCH_REGEX=$bench_regex
EOF
	echo "running prepared-session hot-query CPU profile into: $dir"
	local args=(go test -run '^$' -bench "$bench_regex" -benchmem -benchtime="$PROFILE_BENCHTIME" -count="$PROFILE_COUNT")
	if is_true "$RUN_ALLOC_PROFILE"; then
		args+=(-memprofile "$alloc_profile")
	fi
	args+=(./TreeDB/collections)
	(
		export GOWORK="${GOWORK:-off}"
		export TREEDB_TYPED_COLUMN_BENCH_ROWS="$PROFILE_ROWS"
		export TREEDB_TYPED_COLUMN_BENCH_SHAPES="$shape"
		export TREEDB_TYPED_COLUMN_BENCH_DISTS="$dist"
		export TREEDB_TYPED_COLUMN_BENCH_READ_INTEGRITY="$PROFILE_READ_INTEGRITY"
		export TREEDB_TYPED_COLUMN_BENCH_LAYOUTS="$PROFILE_LAYOUT"
		export TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false
		export TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE="$cpu_profile"
		"${args[@]}"
	) 2>&1 | tee "$dir/hot_query_bench.txt"
	if [[ -s "$cpu_profile" ]]; then
		go tool pprof -top "$cpu_profile" >"$dir/hot_query_cpu_top.txt"
	fi
	if is_true "$RUN_ALLOC_PROFILE" && [[ -s "$alloc_profile" ]]; then
		go tool pprof -top -sample_index=alloc_space "$alloc_profile" >"$dir/process_allocs_top.txt"
	fi
}

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB typed-column int64 benchmark/profile bundle

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- smoke enabled: \`$RUN_SMOKE\`
- 1M matrix enabled: \`$RUN_1M\`
- hot prepared-session CPU profile enabled: \`$RUN_HOT_PROFILE\`

Primary artifacts:

- \`smoke/typed_column_int64_aggregate_bench.txt\` when smoke is enabled
- \`matrix_1m/typed_column_int64_aggregate_bench.txt\` when 1M matrix is enabled
- \`hot_query_profile/hot_query_cpu.pprof\`
- \`hot_query_profile/hot_query_cpu_top.txt\`
- \`hot_query_profile/process_allocs.pprof\` when allocation profiling is enabled
- \`hot_query_profile/process_allocs_top.txt\` when allocation profiling is enabled

The hot CPU profile uses the benchmark-owned
\`TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE\` boundary around the prepared-session
\`Run\` loop. Set \`PROFILE_LAYOUT=raw_int64\` to profile the #1838 raw
fixed-width path; set \`SMOKE_LAYOUTS\`/\`LAYOUTS_1M\` to \`delta,raw\` or
\`all\` for layout comparisons. The allocation profile is Go test process-wide;
use benchmark \`B/op\` and \`allocs/op\` as the hot-loop allocation signal.
EOF

if is_true "$RUN_SMOKE"; then
	run_bench "smoke" "$ROWS" "$SMOKE_SHAPES" "$SMOKE_DISTS" "$SMOKE_READ_INTEGRITY" "$SMOKE_LAYOUTS" "$SMOKE_INCLUDE_FALLBACK" "$BENCHTIME" "$COUNT" '^BenchmarkTypedColumnInt64PredicateAggregate$'
fi

if is_true "$RUN_1M"; then
	run_bench "matrix_1m" "$ROWS_1M" "$SHAPES_1M" "$DISTS_1M" "$READ_INTEGRITY_1M" "$LAYOUTS_1M" "$INCLUDE_FALLBACK_1M" "$BENCHTIME_1M" "$COUNT_1M" '^BenchmarkTypedColumnInt64PredicateAggregate$'
fi

if is_true "$RUN_HOT_PROFILE"; then
	run_hot_profile
fi

echo "typed-column int64 benchmark/profile bundle: $RUN_DIR"
echo "bundle index: $RUN_DIR/README.md"
