#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_quantized_buffered_rows_$(date +%Y%m%d_%H%M%S)}"
ROWS="${ROWS:-all}"
BENCHTIME="${BENCHTIME:-100000x}"
TIMING_BENCHTIME="${TIMING_BENCHTIME:-$BENCHTIME}"
PROFILE_BENCHTIME="${PROFILE_BENCHTIME:-$BENCHTIME}"
TIMING_COUNT="${TIMING_COUNT:-5}"
PROFILE_COUNT="${PROFILE_COUNT:-1}"
RUN_TIMING="${RUN_TIMING:-true}"
RUN_PROFILES="${RUN_PROFILES:-true}"
GOMAXPROCS_VALUE="${GOMAXPROCS:-8}"
GOWORK_VALUE="${GOWORK:-off}"
PPROF_FRAMES="${PPROF_FRAMES:-SearchCosine,scoreAndPushFrontierVisitedTile,frontierSiftDown,insertTop,fetchTopPreparedSearchResults,flushBufferedWrites,acquireCollectionVectorIndexPreparedSearch,dotScalarU8CenteredIndexedARM64Int32}"
BASELINE_DIR="${BASELINE_DIR:-}"

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

selected_row() {
	local id=$1
	if [[ "$ROWS" == "all" ]]; then
		return 0
	fi
	local old_ifs=$IFS part
	IFS=','
	for part in $ROWS; do
		part=${part//[[:space:]]/}
		if [[ "$part" == "$id" || "$part" == "lower" && "$id" == lower_* || "$part" == "collection" && "$id" == collection_* ]]; then
			IFS=$old_ifs
			return 0
		fi
	done
	IFS=$old_ifs
	return 1
}

write_context() {
	local dest=$1
	{
		printf 'TreeDB quantized buffered per-row profile context\n'
		printf '=================================================\n\n'
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
		printf 'rows: %s\n' "$ROWS"
		printf 'timing: enabled=%s benchtime=%s count=%s\n' "$RUN_TIMING" "$TIMING_BENCHTIME" "$TIMING_COUNT"
		printf 'profiles: enabled=%s benchtime=%s count=%s\n' "$RUN_PROFILES" "$PROFILE_BENCHTIME" "$PROFILE_COUNT"
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
	GOMAXPROCS="$GOMAXPROCS_VALUE" GOWORK="$GOWORK_VALUE" "$@" 2>&1 | tee "$outfile"
}

run_row() {
	local id=$1
	local layer=$2
	local mode=$3
	local concurrency=$4
	local bench_regex=$5
	local row_dir="$RUN_DIR/$id"
	local timing_txt="$row_dir/bench_timing.txt"
	local profile_txt="$row_dir/bench_profile.txt"
	local cpu_profile="$row_dir/cpu.pprof"
	local alloc_profile="$row_dir/allocs.pprof"
	local block_profile="$row_dir/block.pprof"
	local mutex_profile="$row_dir/mutex.pprof"
	local lists_dir="$row_dir/pprof_lists"
	local test_binary="$row_dir/collections.test"

	mkdir -p "$row_dir" "$lists_dir"
	{
		printf 'row_id=%s\n' "$id"
		printf 'layer=%s\n' "$layer"
		printf 'mode=%s\n' "$mode"
		printf 'concurrency=%s\n' "$concurrency"
		printf 'bench_regex=%s\n' "$bench_regex"
		printf 'fixture=1024 rows / 128 dims / topK=10 / efSearch=128 / queryOrdinal=37\n'
	} >"$row_dir/row.env"

	local timing_cmd=(go test ./TreeDB/collections -run '^$' -bench "$bench_regex" -benchmem -benchtime "$TIMING_BENCHTIME" -count "$TIMING_COUNT")
	local profile_cmd=(go test ./TreeDB/collections -run '^$' -bench "$bench_regex" -benchmem -benchtime "$PROFILE_BENCHTIME" -count "$PROFILE_COUNT" -o "$test_binary" -cpuprofile "$cpu_profile" -memprofile "$alloc_profile" -blockprofile "$block_profile" -mutexprofile "$mutex_profile")

	{
		printf 'GOMAXPROCS=%q GOWORK=%q ' "$GOMAXPROCS_VALUE" "$GOWORK_VALUE"
		quote_cmd "${timing_cmd[@]}"
	} >"$row_dir/command_timing.txt"
	{
		printf 'GOMAXPROCS=%q GOWORK=%q ' "$GOMAXPROCS_VALUE" "$GOWORK_VALUE"
		quote_cmd "${profile_cmd[@]}"
	} >"$row_dir/command_profile.txt"

	if is_true "$RUN_TIMING"; then
		printf '==> timing %s\n' "$id"
		run_go_test "$timing_txt" "${timing_cmd[@]}"
	else
		printf 'timing skipped for %s\n' "$id" >"$timing_txt"
	fi

	if is_true "$RUN_PROFILES"; then
		printf '==> profiles %s\n' "$id"
		run_go_test "$profile_txt" "${profile_cmd[@]}"
		write_pprof_top "$cpu_profile" "$row_dir/cpu_top.txt" -top
		write_pprof_top "$alloc_profile" "$row_dir/allocs_top.txt" -top -sample_index=alloc_space
		write_pprof_top "$block_profile" "$row_dir/block_top.txt" -top
		write_pprof_top "$mutex_profile" "$row_dir/mutex_top.txt" -top

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
		printf 'profiles skipped for %s\n' "$id" >"$profile_txt"
	fi

	cat >"$row_dir/README.md" <<EOF
# Quantized buffered profile row: $id

- layer: \`$layer\`
- mode: \`$mode\`
- concurrency: \`$concurrency\`
- benchmark regex: \`$bench_regex\`
- fixture: 1024 rows / 128 dims / \`topK=10\` / \`efSearch=128\`
- timing command: \`$(tr '\n' ' ' <"$row_dir/command_timing.txt")\`
- profile command: \`$(tr '\n' ' ' <"$row_dir/command_profile.txt")\`

Artifacts:

- \`bench_timing.txt\`: unprofiled per-row timing/guardrail output.
- \`bench_profile.txt\`: profiled per-row benchmark output.
- \`cpu.pprof\`, \`allocs.pprof\`, \`block.pprof\`, \`mutex.pprof\`.
- \`collections.test\`: test binary emitted by Go profiling flags for later raw-profile analysis.
- \`cpu_top.txt\`, \`allocs_top.txt\`, \`block_top.txt\`, \`mutex_top.txt\`.
- \`pprof_lists/*.txt\`: line-level CPU excerpts for configured target frames.

The pprof files intentionally contain exactly one selected benchmark subrow plus
that row's Go test fixture setup/teardown. Use fixed-iteration benchtimes large
enough for setup noise to be small before making optimization claims.
EOF
}

add_row() {
	local id=$1
	local layer=$2
	local mode=$3
	local concurrency=$4
	local bench_regex=$5
	if selected_row "$id"; then
		row_ids+=("$id")
		row_layers+=("$layer")
		row_modes+=("$mode")
		row_concurrency+=("$concurrency")
		row_regexes+=("$bench_regex")
	fi
}

row_ids=()
row_layers=()
row_modes=()
row_concurrency=()
row_regexes=()

lower_bench='^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$'
collection_bench='^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$'
add_row lower_quantized_only_c1 lower quantized_only 1 "$lower_bench/^route=quantized_only$/^c=1$"
add_row lower_quantized_only_c8 lower quantized_only 8 "$lower_bench/^route=quantized_only$/^c=8$"
add_row lower_quantized_rerank_c1 lower quantized_rerank 1 "$lower_bench/^route=quantized_rerank$/^candidates=32$/^c=1$"
add_row lower_quantized_rerank_c8 lower quantized_rerank 8 "$lower_bench/^route=quantized_rerank$/^candidates=32$/^c=8$"
add_row collection_quantized_only_c1 collection quantized_only 1 "$collection_bench/^route=quantized_only$/^c=1$"
add_row collection_quantized_only_c8 collection quantized_only 8 "$collection_bench/^route=quantized_only$/^c=8$"
add_row collection_quantized_rerank_c1 collection quantized_rerank 1 "$collection_bench/^route=quantized_rerank$/^candidates=32$/^c=1$"
add_row collection_quantized_rerank_c8 collection quantized_rerank 8 "$collection_bench/^route=quantized_rerank$/^candidates=32$/^c=8$"

if [[ ${#row_ids[@]} -eq 0 ]]; then
	printf 'no rows selected by ROWS=%s\n' "$ROWS" >&2
	exit 2
fi

write_context "$RUN_DIR/context.txt"

{
	printf 'row_id\tlayer\tmode\tconcurrency\tbench_regex\n'
	for i in "${!row_ids[@]}"; do
		printf '%s\t%s\t%s\t%s\t%s\n' "${row_ids[$i]}" "${row_layers[$i]}" "${row_modes[$i]}" "${row_concurrency[$i]}" "${row_regexes[$i]}"
	done
} >"$RUN_DIR/matrix.tsv"

for i in "${!row_ids[@]}"; do
	run_row "${row_ids[$i]}" "${row_layers[$i]}" "${row_modes[$i]}" "${row_concurrency[$i]}" "${row_regexes[$i]}"
done

python3 - "$RUN_DIR" <<'PY'
import glob
import math
import os
import re
import statistics
import sys

root = sys.argv[1]
metric_units = {
    "ns/op",
    "ops/sec",
    "B/op",
    "allocs/op",
    "docs_fetched/search",
    "vector_B/search",
    "norm_B/search",
    "quantized_code_B/search",
    "quantized_rerank_candidates/search",
    "quantized_rerank_exact_score_calls/search",
    "prepared_score_calls/search",
    "score_float64_fallbacks/search",
    "graph_row_fallbacks/search",
    "typed_column_vector_fallbacks/search",
    "vector_scratch_decodes/search",
    "search_route_quantized_only/search",
    "search_route_quantized_rerank/search",
    "search_route_column_graph_prepared/search",
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

summary_rows = []
for row_dir in sorted(glob.glob(os.path.join(root, "*"))):
    if not os.path.isdir(row_dir):
        continue
    row_id = os.path.basename(row_dir)
    env_path = os.path.join(row_dir, "row.env")
    if not os.path.exists(env_path):
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
        summary_rows.append({"row_id": row_id, "source": source, "status": "no benchmark rows", **meta})
        continue
    out = {"row_id": row_id, "source": source, "status": "ok", **meta}
    all_metrics = sorted({key for row in bench_rows for key in row.keys()} | metric_units)
    for unit in all_metrics:
        if unit in {"benchmark"}:
            continue
        out[unit] = median([row.get(unit) for row in bench_rows])
        out[unit + "__max"] = max([row.get(unit) for row in bench_rows if row.get(unit) is not None], default=None)
    def present_all_zero(name):
        return all(name in row and row.get(name) == 0 for row in bench_rows)

    b_op_all_zero = present_all_zero("B/op")
    allocs_all_zero = present_all_zero("allocs/op")
    docs_all_zero = present_all_zero("docs_fetched/search")
    fallbacks_all_zero = all(
        present_all_zero(name)
        for name in (
            "graph_row_fallbacks/search",
            "typed_column_vector_fallbacks/search",
            "vector_scratch_decodes/search",
            "score_float64_fallbacks/search",
        )
    )
    mode = meta.get("mode", "")
    if mode == "quantized_only":
        exact_ok = all(
            row.get("vector_B/search") == 0
            and row.get("norm_B/search") == 0
            and row.get("quantized_rerank_exact_score_calls/search") == 0
            and row.get("prepared_score_calls/search") == 0
            for row in bench_rows
        )
    elif mode == "quantized_rerank":
        exact_ok = True
        for row in bench_rows:
            required = (
                "quantized_rerank_candidates/search",
                "quantized_rerank_exact_score_calls/search",
                "prepared_score_calls/search",
                "vector_B/search",
                "norm_B/search",
                "exact_vector_B/vector",
                "exact_norm_B/vector",
            )
            if any(name not in row for name in required):
                exact_ok = False
                break
            candidates = row.get("quantized_rerank_candidates/search")
            max_vector_bytes = candidates * row.get("exact_vector_B/vector")
            max_norm_bytes = candidates * row.get("exact_norm_B/vector")
            if not (
                row.get("quantized_rerank_exact_score_calls/search") <= candidates
                and row.get("prepared_score_calls/search") <= candidates
                and row.get("vector_B/search") <= max_vector_bytes
                and row.get("norm_B/search") <= max_norm_bytes
            ):
                exact_ok = False
                break
    else:
        exact_ok = True
    out["guardrail_status"] = "pass" if (b_op_all_zero and allocs_all_zero and docs_all_zero and fallbacks_all_zero and exact_ok) else "check"
    summary_rows.append(out)

fields = [
    "row_id", "layer", "mode", "concurrency", "source", "status", "guardrail_status",
    "ns/op", "ops/sec", "B/op", "allocs/op",
    "docs_fetched/search", "vector_B/search", "norm_B/search", "quantized_code_B/search",
    "quantized_rerank_candidates/search", "quantized_rerank_exact_score_calls/search", "prepared_score_calls/search",
    "graph_row_fallbacks/search", "typed_column_vector_fallbacks/search", "vector_scratch_decodes/search", "score_float64_fallbacks/search",
    "collection_prepared_cache_hits/op", "collection_prepared_cache_misses/op", "collection_prepared_cache_waits/op",
]

with open(os.path.join(root, "summary.tsv"), "w", encoding="utf-8") as out:
    out.write("\t".join(fields) + "\n")
    for row in summary_rows:
        out.write("\t".join(fmt(row.get(field)) for field in fields) + "\n")

with open(os.path.join(root, "summary.md"), "w", encoding="utf-8") as out:
    out.write("# TreeDB quantized buffered per-row profile summary\n\n")
    out.write("This report is generated from one benchmark subrow per artifact directory. ")
    out.write("Guardrail status checks allocation, document/materialization, fallback, and exact-read constraints from the benchmark counters.\n\n")
    out.write("| row | ns/op | ops/sec | B/op | allocs/op | docs/search | vector_B/search | norm_B/search | rerank exact score calls/search | prepared exact score calls/search | guardrails |\n")
    out.write("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
    for row in summary_rows:
        out.write(
            "| {row_id} | {ns} | {ops} | {bop} | {allocs} | {docs} | {vec} | {norm} | {rerank} | {prepared} | {guard} |\n".format(
                row_id=row.get("row_id", ""),
                ns=fmt(row.get("ns/op")),
                ops=fmt(row.get("ops/sec")),
                bop=fmt(row.get("B/op")),
                allocs=fmt(row.get("allocs/op")),
                docs=fmt(row.get("docs_fetched/search")),
                vec=fmt(row.get("vector_B/search")),
                norm=fmt(row.get("norm_B/search")),
                rerank=fmt(row.get("quantized_rerank_exact_score_calls/search")),
                prepared=fmt(row.get("prepared_score_calls/search")),
                guard=row.get("guardrail_status", ""),
            )
        )
    out.write("\n## Artifact layout\n\n")
    out.write("Each row directory contains `bench_timing.txt`, `bench_profile.txt`, `cpu.pprof`, `allocs.pprof`, `block.pprof`, `mutex.pprof`, `collections.test`, top files, and `pprof_lists/*.txt`.\n")
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
# TreeDB Quantized Buffered Per-Row Profile Bundle

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse HEAD)\`
- GOMAXPROCS: \`$GOMAXPROCS_VALUE\`
- GOWORK: \`$GOWORK_VALUE\`
- selected rows: \`$ROWS\`
- timing: enabled=\`$RUN_TIMING\`, benchtime=\`$TIMING_BENCHTIME\`, count=\`$TIMING_COUNT\`
- profiles: enabled=\`$RUN_PROFILES\`, benchtime=\`$PROFILE_BENCHTIME\`, count=\`$PROFILE_COUNT\`

Primary files:

- \`context.txt\`: commit, Go version, OS/arch, uptime, and process inventory.
- \`matrix.tsv\`: exact row IDs and benchmark regexes.
- \`summary.md\`, \`summary.tsv\`: per-row timing medians and guardrail counter summary.
- \`<row>/bench_timing.txt\`: unprofiled benchmark output for the selected row.
- \`<row>/bench_profile.txt\`: profiled benchmark output for the selected row.
- \`<row>/cpu.pprof\`, \`<row>/allocs.pprof\`, \`<row>/block.pprof\`, \`<row>/mutex.pprof\`.
- \`<row>/collections.test\`: test binary emitted by Go profiling flags for later raw-profile analysis.
- \`<row>/cpu_top.txt\`, \`<row>/allocs_top.txt\`, \`<row>/block_top.txt\`, \`<row>/mutex_top.txt\`.
- \`<row>/pprof_lists/*.txt\`: line-level CPU excerpts for target frames.

Host-load caveat: if \`context.txt\` shows competing benchmark/test processes or
high load, treat the run as contaminated and rerun before claiming a candidate
win. This harness reduces mixed-row attribution noise; it does not make noisy
hosts reliable.
EOF

printf 'quantized buffered per-row profile bundle: %s\n' "$RUN_DIR"
printf 'summary: %s\n' "$RUN_DIR/summary.md"
printf 'matrix: %s\n' "$RUN_DIR/matrix.tsv"
printf 'context: %s\n' "$RUN_DIR/context.txt"
