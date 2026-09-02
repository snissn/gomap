#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/treedb_column_store_slope_$(date +%Y%m%d_%H%M%S)}"
KEYCOUNTS="${KEYCOUNTS:-10000,100000,500000}"
ROUTED_PATHS="${ROUTED_PATHS:-serial_column_scan,aggregate_metadata,parallel_column_scan}"
PROFILE="${PROFILE:-durable}"
BATCHSIZE="${BATCHSIZE:-5000}"
PATH_LABEL="${PATH_LABEL:-native-fastpath}"
RUN_ROUTED="${RUN_ROUTED:-true}"
RUN_DIRECT="${RUN_DIRECT:-true}"
RUN_EXPERIMENT="${RUN_EXPERIMENT:-true}"
DIRECT_BENCHTIME="${DIRECT_BENCHTIME:-20x}"
DIRECT_COUNT="${DIRECT_COUNT:-1}"
EXPERIMENT_BENCHTIME="${EXPERIMENT_BENCHTIME:-20x}"
EXPERIMENT_COUNT="${EXPERIMENT_COUNT:-1}"

mkdir -p "$RUN_DIR"

is_true() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		1|true|yes|y|on) return 0 ;;
		*) return 1 ;;
	esac
}

split_words() {
	printf '%s' "$1" | tr ',' ' '
}

slug() {
	printf '%s' "$1" | tr -c '[:alnum:]_=-' '_'
}

write_index() {
	cat >"$RUN_DIR/README.md" <<EOF
# TreeDB Column Store Slope Profile

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse HEAD)\`
- generated_at: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`
- keycounts: \`$KEYCOUNTS\`
- routed paths: \`$ROUTED_PATHS\`
- profile: \`$PROFILE\`
- batchsize: \`$BATCHSIZE\`
- execution path label: \`$PATH_LABEL\`
- direct package benchmarks: \`$RUN_DIRECT\`
- experiment kernel benchmarks: \`$RUN_EXPERIMENT\`

This bundle separates fixed setup/reporting cost from steady-state scan/reduce
throughput for the production column-store path. Routed production runs use
\`unified-bench -suite column_store\` and write the normal JSON/Markdown/HTML,
benchprof, pprof, and trace artifacts below \`routed/keys_<n>/<path>/\`.

Companion artifacts:

- \`production_package_bench.txt\` captures direct TCPA asset scan, collection
  scanner, and physical query adapter package benchmarks.
- \`experiment_colgranule_baseline.txt\` captures the older
  \`experiments/colgranule\` encoded/block reducer and metadata baselines.
- \`summary.tsv\` contains one row per routed production query.
- \`summary.md\` is the review-facing rollup.
- \`experiments/colgranule/JSONBENCH_COMPARISON_REPORT.md\` remains the local
  ClickHouse-equivalent context for JSONBench-shaped query and storage
  comparisons; copy exact ClickHouse numbers into PR descriptions only when the
  relevant query family is discussed.

The slope bundle is attribution evidence, not an automatic optimization gate.
Use it to classify planner/setup overhead, typed asset-manager/read overhead,
TCPA row-view decode, reducer shape, reporting/parity overhead, and missing
metadata/mark/dictionary/locator fast paths.
EOF
}

run_routed_case() {
	local keys=$1
	local path=$2
	local safe_path
	safe_path=$(slug "$path")
	local dir="$RUN_DIR/routed/keys_${keys}/${safe_path}"

	mkdir -p "$dir"
	echo "running routed column_store keys=$keys path=$path into: $dir"
	"$RUN_DIR/unified-bench" \
		-suite column_store \
		-dbs treedb \
		-profile "$PROFILE" \
		-keys "$keys" \
		-batchsize "$BATCHSIZE" \
		-profile-dir "$dir" \
		-path-label "$PATH_LABEL" \
		-column-store-path "$path" \
		-progress=false \
		2>&1 | tee "$dir/run.log"
}

write_routed_summary() {
	python3 - "$RUN_DIR" <<'PY'
import csv
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
rows = []
for report_path in sorted(root.glob("routed/keys_*/*/column_store_results.json")):
    with report_path.open("r", encoding="utf-8") as f:
        report = json.load(f)
    for q in report.get("queries", []):
        rows.append({
            "keys": report.get("rows", 0),
            "forced_path": report.get("forced_path", ""),
            "query": q.get("name", ""),
            "plan_label": q.get("plan_label", ""),
            "rows_per_second": q.get("rows_per_second", 0),
            "mib_per_second": q.get("mib_per_second", 0),
            "ns_per_row": q.get("ns_per_row", 0),
            "bytes_read": q.get("bytes_read", 0),
            "row_materializations": q.get("row_materializations", 0),
            "metadata_hits": q.get("metadata_hits", 0),
            "scheduled_granules": q.get("scheduled_granules", 0),
            "skipped_granules": q.get("skipped_granules", 0),
            "worker_count": q.get("worker_count", 0),
            "segment_file_cache_hits": q.get("segment_file_cache_hits", 0),
            "segment_file_cache_misses": q.get("segment_file_cache_misses", 0),
            "parity": report.get("parity", {}).get(q.get("name", ""), {}).get("pass", False),
            "html": str(report_path.with_name("column_store_results.html")),
        })

summary_tsv = root / "summary.tsv"
with summary_tsv.open("w", encoding="utf-8", newline="") as f:
    fieldnames = [
        "keys", "forced_path", "query", "plan_label", "rows_per_second",
        "mib_per_second", "ns_per_row", "bytes_read", "row_materializations",
        "metadata_hits", "scheduled_granules", "skipped_granules",
        "worker_count", "segment_file_cache_hits", "segment_file_cache_misses",
        "parity", "html",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)

summary_md = root / "summary.md"
with summary_md.open("w", encoding="utf-8") as f:
    f.write("# TreeDB Column Store Slope Summary\n\n")
    f.write(f"- routed query rows: `{len(rows)}`\n")
    f.write(f"- summary TSV: `{summary_tsv}`\n")
    f.write(f"- direct package benchmark: `{root / 'production_package_bench.txt'}`\n")
    f.write(f"- experiment baseline: `{root / 'experiment_colgranule_baseline.txt'}`\n\n")
    f.write("| keys | path | query | rows/s | ns/row | MiB/s | bytes | mat | meta | sched/skip | workers | cache hit/miss | parity |\n")
    f.write("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
    for r in rows:
        f.write(
            f"| {r['keys']} | `{r['forced_path']}` | `{r['query']}` | "
            f"{float(r['rows_per_second']):.2f} | {float(r['ns_per_row']):.2f} | "
            f"{float(r['mib_per_second']):.2f} | {r['bytes_read']} | "
            f"{r['row_materializations']} | {r['metadata_hits']} | "
            f"{r['scheduled_granules']}/{r['skipped_granules']} | {r['worker_count']} | "
            f"{r['segment_file_cache_hits']}/{r['segment_file_cache_misses']} | {r['parity']} |\n"
        )
PY
}

write_index

if is_true "$RUN_ROUTED"; then
	echo "building unified-bench"
	go build -o "$RUN_DIR/unified-bench" ./cmd/unified_bench
	for keys in $(split_words "$KEYCOUNTS"); do
		for path in $(split_words "$ROUTED_PATHS"); do
			run_routed_case "$keys" "$path"
		done
	done
	write_routed_summary
fi

if is_true "$RUN_DIRECT"; then
	echo "running direct package scanner/query benchmarks"
	go test ./TreeDB/collections \
		-run '^$' \
		-bench 'BenchmarkColumnPhysical(AssetSerialScan|CollectionSerialScan)M13A|BenchmarkColumnPhysicalQuery(AdapterM13B|VisibilityM13C)' \
		-benchmem \
		-benchtime="$DIRECT_BENCHTIME" \
		-count="$DIRECT_COUNT" \
		2>&1 | tee "$RUN_DIR/production_package_bench.txt"
fi

if is_true "$RUN_EXPERIMENT"; then
	echo "running experiment colgranule kernel baselines"
	GOWORK=off go test ./experiments/colgranule \
		-run '^$' \
		-bench 'BenchmarkJSONBenchEncodedPartQueries|BenchmarkJSONBenchQ4SortOrderFairness|BenchmarkJSONBenchAggregateMetadataQueries' \
		-benchmem \
		-benchtime="$EXPERIMENT_BENCHTIME" \
		-count="$EXPERIMENT_COUNT" \
		2>&1 | tee "$RUN_DIR/experiment_colgranule_baseline.txt"
fi

echo "column store slope bundle: $RUN_DIR"
echo "bundle index: $RUN_DIR/README.md"
if [[ -s "$RUN_DIR/summary.md" ]]; then
	echo "summary: $RUN_DIR/summary.md"
fi
