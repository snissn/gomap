#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/treedb_insert_compression_profile_$(date +%Y%m%d_%H%M%S)}"
COUNT="${COUNT:-10}"
BENCHTIME="${BENCHTIME:-50000x}"
INDEXES_REGEX="${INDEXES_REGEX:-0|1|2}"
BENCH_REGEX="${BENCH_REGEX:-^BenchmarkCollectionShapeInsertBatch$/^indexes_(${INDEXES_REGEX})$}"
BATCH_SIZE="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-50000}"
DOCUMENT_FORMAT="${TREEDB_COLLECTION_DOCUMENT_FORMAT:-template-v1}"
BENCH_ENGINE="${TREEDB_COLLECTION_BENCH_ENGINE:-bench_unsafe}"
PATH_LABEL="${TREEDB_COLLECTION_PATH_LABEL:-native-fastpath}"
RUN_COMPRESSION_OFF="${RUN_COMPRESSION_OFF:-true}"
RUN_TIMED_CPU="${RUN_TIMED_CPU:-false}"
TIMED_BENCHTIME="${TIMED_BENCHTIME:-1000000x}"
TIMED_BENCH_REGEX="${TIMED_BENCH_REGEX:-^BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes$}"

mkdir -p "$RUN_DIR"

is_true() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		1|true|yes|y|on) return 0 ;;
		*) return 1 ;;
	esac
}

extract_bench_text() {
	local raw_json=$1
	local dest=$2
	python3 - "$raw_json" "$dest" <<'PY'
import json
import sys

src, dest = sys.argv[1], sys.argv[2]
with open(src, "r", encoding="utf-8") as inp, open(dest, "w", encoding="utf-8") as out:
    for line in inp:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("Action") == "output":
            out.write(item.get("Output", ""))
PY
}

run_collection_report() {
	local name=$1
	local compression=$2
	local bench_regex=$3
	local benchtime=$4
	local count=$5
	local timed_cpu=$6
	local dir="$RUN_DIR/$name"

	mkdir -p "$dir"
	echo "running $name insert compression profile into: $dir"
	(
		export OUT_DIR="$dir"
		export TREEDB_VLOG_COMPRESSION="$compression"
		export TREEDB_COLLECTION_PATH_LABEL="$PATH_LABEL"
		export TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE"
		export TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE"
		export TREEDB_COLLECTION_DOCUMENT_FORMAT="$DOCUMENT_FORMAT"
		export BENCH_REGEX="$bench_regex"
		export BENCHTIME="$benchtime"
		export COUNT="$count"
		if is_true "$timed_cpu"; then
			export TREEDB_COLLECTION_TIMED_CPU_PROFILE=true
		else
			unset TREEDB_COLLECTION_TIMED_CPU_PROFILE
		fi
		scripts/bench_collections_report.sh
	) 2>&1 | tee "$dir/run.log"

	if [[ -s "$dir/collections_bench.json" ]]; then
		extract_bench_text "$dir/collections_bench.json" "$dir/bench.txt"
	fi
}

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB Insert Compression Profile

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- benchmark regex: \`$BENCH_REGEX\`
- benchmark count: \`$COUNT\`
- benchtime: \`$BENCHTIME\`
- benchmark engine: \`$BENCH_ENGINE\`
- document format: \`$DOCUMENT_FORMAT\`
- collection batch size: \`$BATCH_SIZE\`
- execution path: \`$PATH_LABEL\`
- compression-off comparison: \`$RUN_COMPRESSION_OFF\`
- timed CPU run: \`$RUN_TIMED_CPU\`

This harness captures the short-lived collection insert shape that exposes value-log
compression setup and encoder-history allocation costs. The \`auto\` run is the
default TreeDB value-log compression path. The optional \`off\` run is a ceiling
comparison only: it removes value-log compression work but usually grows
\`disk_bytes/doc\`.

Primary artifacts:

- \`auto/collections_report.md\`
- \`auto/collections_cpu_top.txt\`
- \`auto/collections_mem_top.txt\`
- \`auto/collections_cpu.pprof\`
- \`auto/collections_mem.pprof\`
- \`benchstat_auto_vs_off.txt\` when compression-off is enabled and \`benchstat\` is available

The allocation profile covers the whole Go test process, which is intentional
for this short-lived benchmark because dictionary training and zstd encoder
setup happen around the benchmarked collection lifetime.
EOF

run_collection_report "auto" "auto" "$BENCH_REGEX" "$BENCHTIME" "$COUNT" "false"

if is_true "$RUN_COMPRESSION_OFF"; then
	run_collection_report "off" "off" "$BENCH_REGEX" "$BENCHTIME" "$COUNT" "false"
	if command -v benchstat >/dev/null 2>&1; then
		benchstat "$RUN_DIR/auto/bench.txt" "$RUN_DIR/off/bench.txt" | tee "$RUN_DIR/benchstat_auto_vs_off.txt"
	else
		echo "benchstat not found; skipping auto/off benchstat comparison" | tee "$RUN_DIR/benchstat_auto_vs_off.txt"
	fi
fi

if is_true "$RUN_TIMED_CPU"; then
	run_collection_report "timed_cpu_auto" "auto" "$TIMED_BENCH_REGEX" "$TIMED_BENCHTIME" "1" "true"
fi

echo "insert compression profile bundle: $RUN_DIR"
echo "bundle index: $RUN_DIR/README.md"
echo "auto report: $RUN_DIR/auto/collections_report.md"
if is_true "$RUN_COMPRESSION_OFF"; then
	echo "auto/off benchstat: $RUN_DIR/benchstat_auto_vs_off.txt"
fi
