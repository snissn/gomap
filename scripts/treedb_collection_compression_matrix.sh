#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DOCS="${DOCS:-100000}"
BATCH="${BATCH:-16000}"
RUN_DIR="${RUN_DIR:-/tmp/treedb_collection_compression_$(date +%Y%m%d_%H%M%S)}"
PROFILE="${PROFILE:-bench_unsafe}"
COLLECTION_INDEXES="${COLLECTION_INDEXES:-0 1 2}"
COMPACT_MODE="${COMPACT_MODE:-full}"

mkdir -p "$RUN_DIR"/{dbs,logs}

collection_fixture="$repo_root/bin/collection-load-fixture"
treemap="$repo_root/bin/treemap"
raw_loader="$RUN_DIR/raw_template_v1_loader.go"
tsv="$RUN_DIR/compression_matrix.tsv"
md="$RUN_DIR/compression_matrix.md"

cat >"$raw_loader" <<'GO'
package main

import (
	"flag"
	"fmt"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	fixtureCities = 64
	fixturePad    = "01234567890123456789"
)

func main() {
	dir := flag.String("dir", "", "TreeDB directory")
	docs := flag.Int("docs", 100000, "documents to write")
	batchSize := flag.Int("batch-size", 16000, "documents per batch")
	reset := flag.Bool("reset", false, "remove directory before loading")
	profile := flag.String("profile", "bench_unsafe", "TreeDB profile: "+treedb.BenchmarkProfileFlagHelp)
	flag.Parse()

	if *dir == "" {
		fatalf("-dir is required")
	}
	if *docs <= 0 {
		fatalf("-docs must be > 0")
	}
	if *batchSize <= 0 {
		fatalf("-batch-size must be > 0")
	}
	if *reset {
		if err := os.RemoveAll(*dir); err != nil {
			fatalf("reset %s: %v", *dir, err)
		}
	}

	opts := treedb.OptionsForBenchmark(parseProfile(*profile), *dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	opts.KeepRecent = 1
	opts.PreferAppendAlloc = false

	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("open TreeDB: %v", err)
	}
	var closeErr error
	defer func() {
		if err := db.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		if closeErr != nil {
			fatalf("%v", closeErr)
		}
	}()

	var encoder collections.TemplateV1Encoder
	for start := 0; start < *docs; start += *batchSize {
		end := start + *batchSize
		if end > *docs {
			end = *docs
		}
		b := db.NewBatchWithSize(end - start)
		if b == nil {
			closeErr = fmt.Errorf("new batch returned nil")
			return
		}
		for n := start; n < end; n++ {
			doc, err := encoder.EncodeDocument(
				[]string{"name", "email", "city", "pad"},
				[]any{
					fmt.Sprintf("user-%09d", n),
					fmt.Sprintf("user-%09d@example.com", n),
					fmt.Sprintf("city-%02d", n%fixtureCities),
					fixturePad,
				},
			)
			if err != nil {
				_ = b.Close()
				closeErr = fmt.Errorf("encode document %d: %w", n, err)
				return
			}
			if err := b.Set([]byte(fmt.Sprintf("u-%09d", n)), doc); err != nil {
				_ = b.Close()
				closeErr = fmt.Errorf("set document %d: %w", n, err)
				return
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			closeErr = fmt.Errorf("write batch starting at %d: %w", start, err)
			return
		}
		if err := b.Close(); err != nil {
			closeErr = fmt.Errorf("close batch starting at %d: %w", start, err)
			return
		}
	}
	if err := db.Checkpoint(); err != nil {
		closeErr = fmt.Errorf("checkpoint: %w", err)
	}
}

func parseProfile(raw string) treedb.Profile {
	profile, ok := treedb.ParseBenchmarkProfile(raw, treedb.ProfileBenchUnsafe)
	if !ok {
		fatalf("unsupported -profile %q; allowed: %s", raw, treedb.BenchmarkProfileFlagHelp)
	}
	return profile
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "raw-template-v1-loader: "+format+"\n", args...)
	os.Exit(1)
}
GO

echo "Building benchmark helpers..."
make -C "$repo_root" collection-load-fixture treemap-bin >/dev/null

if [[ ! -x "$collection_fixture" ]]; then
	echo "missing $collection_fixture" >&2
	exit 1
fi
if [[ ! -x "$treemap" ]]; then
	echo "missing $treemap" >&2
	exit 1
fi

portable_stat_size() {
	local path="$1"
	if stat -f%z "$path" >/dev/null 2>&1; then
		stat -f%z "$path"
	else
		stat -c%s "$path"
	fi
}

dir_bytes() {
	local dir="$1"
	local total=0
	local file
	while IFS= read -r file; do
		total=$((total + $(portable_stat_size "$file")))
	done < <(find "$dir" -type f | LC_ALL=C sort)
	printf '%s\n' "$total"
}

gzip_dir_bytes() {
	local dir="$1"
	find "$dir" -type f | LC_ALL=C sort | while IFS= read -r file; do
		cat "$file"
	done | gzip -c | wc -c | tr -d '[:space:]'
}

path_bytes() {
	local path="$1"
	if [[ ! -e "$path" ]]; then
		printf '0\n'
	elif [[ -d "$path" ]]; then
		dir_bytes "$path"
	else
		portable_stat_size "$path"
	fi
}

path_gzip_bytes() {
	local path="$1"
	if [[ ! -e "$path" ]]; then
		printf '0\n'
	elif [[ -d "$path" ]]; then
		gzip_dir_bytes "$path"
	else
		gzip -c "$path" | wc -c | tr -d '[:space:]'
	fi
}

ratio() {
	awk -v bytes="$1" -v gzip_bytes="$2" 'BEGIN {
		if (gzip_bytes > 0) {
			printf "%.2f", bytes / gzip_bytes
		} else {
			printf "0.00"
		}
	}'
}

pct_delta() {
	awk -v before="$1" -v after="$2" 'BEGIN {
		if (before > 0) {
			printf "%.1f%%", ((after - before) / before) * 100
		} else {
			printf "0.0%%"
		}
	}'
}

measure_tsv_row() {
	local mode="$1"
	local indexes="$2"
	local db_dir="$3"
	local phase="$4"
	local total
	local total_gzip
	local leaf
	local leaf_gzip
	local index_db
	local value_vlog
	total="$(dir_bytes "$db_dir")"
	total_gzip="$(gzip_dir_bytes "$db_dir")"
	leaf="$(path_bytes "$db_dir/maindb/leaf_vlog")"
	leaf_gzip="$(path_gzip_bytes "$db_dir/maindb/leaf_vlog")"
	index_db="$(path_bytes "$db_dir/maindb/index.db")"
	value_vlog="$(path_bytes "$db_dir/maindb/value_vlog")"
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$mode" "$indexes" "$phase" "$total" "$total_gzip" "$(ratio "$total" "$total_gzip")" \
		"$leaf" "$(ratio "$leaf" "$leaf_gzip")" "$index_db" "$value_vlog"
}

	run_compact() {
		local case_name="$1"
		local db_dir="$2"
		"$treemap" compact "$db_dir" -rw -mode "$COMPACT_MODE" >"$RUN_DIR/logs/${case_name}.compact.log" 2>&1
	}

run_raw_case() {
	local case_name="raw_treedb"
	local db_dir="$RUN_DIR/dbs/$case_name"
	echo "Loading $case_name..."
	(
		cd "$repo_root"
		go run "$raw_loader" -dir "$db_dir" -reset -docs "$DOCS" -batch-size "$BATCH" -profile "$PROFILE"
	) >"$RUN_DIR/logs/${case_name}.load.log" 2>&1
		measure_tsv_row "raw_treedb" "-" "$db_dir" "before_compact" >>"$tsv"
		echo "Compacting $case_name..."
		run_compact "$case_name" "$db_dir"
		measure_tsv_row "raw_treedb" "-" "$db_dir" "after_compact" >>"$tsv"
	}

run_collection_case() {
	local indexes="$1"
	local case_name="collection_indexes_${indexes}"
	local db_dir="$RUN_DIR/dbs/$case_name"
	echo "Loading $case_name..."
	"$collection_fixture" \
		-json \
		-dir "$db_dir" \
		-reset \
		-docs "$DOCS" \
		-batch-size "$BATCH" \
		-format template-v1 \
		-indexes "$indexes" \
		-profile "$PROFILE" \
		-progress=false \
		>"$RUN_DIR/logs/${case_name}.load.json" 2>"$RUN_DIR/logs/${case_name}.load.stderr"
		measure_tsv_row "collection" "$indexes" "$db_dir" "before_compact" >>"$tsv"
		echo "Compacting $case_name..."
		run_compact "$case_name" "$db_dir"
		measure_tsv_row "collection" "$indexes" "$db_dir" "after_compact" >>"$tsv"
	}

printf 'mode\tindexes\tphase\ttotal_bytes\ttotal_gzip_bytes\ttotal_bytes_per_gzip_byte\tleaf_vlog_bytes\tleaf_vlog_bytes_per_gzip_byte\tindex_db_bytes\tvalue_vlog_bytes\n' >"$tsv"

run_raw_case
read -r -a collection_index_values <<<"$COLLECTION_INDEXES"
if [[ "${#collection_index_values[@]}" -eq 0 ]]; then
	echo "COLLECTION_INDEXES must include at least one index count" >&2
	exit 1
fi

for indexes in "${collection_index_values[@]}"; do
	if [[ ! "$indexes" =~ ^[0-9]+$ ]]; then
		echo "invalid COLLECTION_INDEXES entry: $indexes" >&2
		exit 1
	fi
	run_collection_case "$indexes"
done

{
	printf '# TreeDB Collection Compression Matrix\n\n'
	printf -- '- Run dir: `%s`\n' "$RUN_DIR"
	printf -- '- Docs: `%s`\n' "$DOCS"
	printf -- '- Batch size: `%s`\n' "$BATCH"
	printf -- '- Document shape: template-v1 fixture fields `name`, `email`, `city`, `pad`\n'
	printf -- '- Raw TreeDB case: generated template-v1 payloads stored directly by key\n'
	printf -- '- Collection cases: generated template-v1 payloads inserted through collection mode with indexes `%s`\n' "$COLLECTION_INDEXES"
		printf -- '- Compaction: `treemap compact <dir> -rw -mode %s`\n' "$COMPACT_MODE"
		printf -- '- Gzip ratio is `bytes/gzip_bytes`; lower is closer to gzip-compressed density already being present on disk.\n\n'
		printf '| Mode | Indexes | Before bytes | Before gzip | Before bytes/gzip | After compact bytes | After compact gzip | After bytes/gzip | Disk delta | Gzip delta | After leaf_vlog bytes | After leaf bytes/gzip | After index.db bytes | After value_vlog bytes |\n'
		printf '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'
	for mode in raw_treedb collection; do
		if [[ "$mode" == "raw_treedb" ]]; then
			index_values=("-")
		else
			index_values=("${collection_index_values[@]}")
		fi
		for indexes in "${index_values[@]}"; do
				before_line="$(awk -F '\t' -v mode="$mode" -v idx="$indexes" '$1 == mode && $2 == idx && $3 == "before_compact" { print; exit }' "$tsv")"
				after_line="$(awk -F '\t' -v mode="$mode" -v idx="$indexes" '$1 == mode && $2 == idx && $3 == "after_compact" { print; exit }' "$tsv")"
			IFS=$'\t' read -r _ _ _ before_total before_gzip before_ratio _ _ _ _ <<<"$before_line"
			IFS=$'\t' read -r _ _ _ after_total after_gzip after_ratio after_leaf after_leaf_ratio after_index_db after_value_vlog <<<"$after_line"
			printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
				"$mode" "$indexes" "$before_total" "$before_gzip" "$before_ratio" \
				"$after_total" "$after_gzip" "$after_ratio" \
				"$(pct_delta "$before_total" "$after_total")" "$(pct_delta "$before_gzip" "$after_gzip")" \
				"$after_leaf" "$after_leaf_ratio" "$after_index_db" "$after_value_vlog"
		done
	done
	printf '\nRaw TSV: `%s`\n' "$tsv"
} >"$md"

cat "$md"
