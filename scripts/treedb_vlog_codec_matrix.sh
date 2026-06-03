#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/treedb_vlog_codec_matrix_$(date +%Y%m%d_%H%M%S)}"
KEYS="${KEYS:-200000}"
VALSIZE="${VALSIZE:-128}"
BATCHSIZE="${BATCHSIZE:-8000}"
PROFILE="${PROFILE:-fast}"
PATH_LABEL="${PATH_LABEL:-native-fastpath}"
READ_WORKERS="${READ_WORKERS:-12}"
TESTS="${TESTS:-dataset_write_random,batch_random,random_read_parallel,full_scan,prefix_scan}"
PATTERNS="${PATTERNS:-zero celestia_height_prefix_fill half_repeat_half_random random}"
KEEP="${KEEP:-false}"
PROGRESS="${PROGRESS:-false}"
BUILD="${BUILD:-true}"

mkdir -p "$RUN_DIR"

is_true() {
	case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
		1|true|yes|y|on) return 0 ;;
		*) return 1 ;;
	esac
}

if is_true "$BUILD"; then
	echo "building unified-bench and benchprof..."
	make unified-bench benchprof >/dev/null
fi

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB Value-Log Codec Matrix

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse HEAD)\`
- keys: \`$KEYS\`
- valsize: \`$VALSIZE\`
- batchsize: \`$BATCHSIZE\`
- profile: \`$PROFILE\`
- path label: \`$PATH_LABEL\`
- read workers: \`$READ_WORKERS\`
- tests: \`$TESTS\`
- patterns: \`$PATTERNS\`
- keep DB dirs: \`$KEEP\`
- leaf mmap bytes env: \`${TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_BYTES:-<unset>}\`
- leaf mmap segment env: \`${TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_SEGMENTS:-<unset>}\`

This harness runs the issue #2194 codec policy matrix without changing the
TreeDB global/profile codec default. It expands \`treedb\` into the four required
variants via \`-treedb-vlog-compression-variant=all\` and excludes the dict-only
variant, leaving:

- \`TreeDB (vlog=auto)\`
- \`TreeDB (vlog=off)\`
- \`TreeDB (vlog=block/snappy)\`
- \`TreeDB (vlog=block/lz4)\`

Each pattern run writes a full \`unified-bench -profile-dir\` bundle. The
\`benchprof_results.md\` file contains disk usage, TreeDB value-log codec summary,
selected mmap stats, and kept directory paths when \`KEEP=true\`. The summary TSV
and markdown in this directory are convenience indexes only; use the per-pattern
artifacts for final evidence.

Final policy evidence must be rerun after #2190 is merged or after #2191 records
an explicit #2190 deferral. Do not use this pre-#2190 matrix alone to enable a
global/profile codec policy.
EOF

run_one_pattern() {
	local pattern=$1
	local out="$RUN_DIR/$pattern"
	mkdir -p "$out"
	echo "running pattern=$pattern into $out"
	args=(
		-dbs=treedb
		-exclude-dbs=treedb_vlog_dict
		-treedb-vlog-compression-variant=all
		-keys="$KEYS"
		-valsize="$VALSIZE"
		-batchsize="$BATCHSIZE"
		-profile="$PROFILE"
		-path-label="$PATH_LABEL"
		-progress="$PROGRESS"
		-read-workers="$READ_WORKERS"
		-test="$TESTS"
		-val-pattern="$pattern"
		-profile-dir="$out"
	)
	if is_true "$KEEP"; then
		args+=(-keep)
	fi
	./bin/unified-bench "${args[@]}" 2>&1 | tee "$out/run.log"
	./bin/benchprof -profiles-dir "$out" 2>&1 | tee "$out/benchprof.log"
}

for pattern in $PATTERNS; do
	run_one_pattern "$pattern"
done

python3 - "$RUN_DIR" <<'PY'
import json
import os
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
patterns = [p for p in sorted(run_dir.iterdir()) if p.is_dir() and (p / "benchprof_results.json").exists()]

tests = []
for p in patterns:
    with open(p / "benchprof_results.json", "r", encoding="utf-8") as f:
        payload = json.load(f)
    for run in payload.get("runs", []):
        for test_name in (run.get("results") or {}).keys():
            if test_name not in tests:
                tests.append(test_name)

def stat(stats, key, default=""):
    return (stats or {}).get(key, default)

def stat_first(stats, *keys, default=""):
    zero_value = ""
    for key in keys:
        v = stat(stats, key)
        if v and v != "0":
            return v
        if v and not zero_value:
            zero_value = v
    return zero_value or default

def result(results, test, db):
    try:
        value = results[test][db]
    except KeyError:
        return ""
    if value is None:
        return ""
    return f"{float(value):.3f}"

def auto_frames(stats):
    parts = []
    for name in ("off", "dict", "block_snappy", "block_lz4"):
        v = stat_first(stats,
            f"treedb.cache.vlog_leaf_scan.auto.frames.{name}",
            f"treedb.cache.vlog_auto.frames.{name}")
        if v and v != "0":
            frac = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.auto.frames_frac.{name}",
                f"treedb.cache.vlog_auto.frames_frac.{name}")
            parts.append(f"{name}={v}" + (f"/{frac}" if frac else ""))
    return ";".join(parts)

def outer_leaf_codecs(stats):
    parts = []
    for name in ("none", "snappy", "lz4", "legacy_page", "unknown", "mixed"):
        v = stat_first(stats,
            f"treedb.cache.vlog_leaf_scan.outer_leaf_codec.frames.{name}",
            f"treedb.cache.vlog_outer_leaf_codec.frames.{name}")
        if v and v != "0":
            ratio = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.outer_leaf_codec.stored_ratio.{name}",
                f"treedb.cache.vlog_outer_leaf_codec.stored_ratio.{name}")
            parts.append(f"{name}={v}" + (f"/{ratio}" if ratio else ""))
    return ";".join(parts)

def block_k(stats):
    parts = []
    for codec in ("snappy", "lz4"):
        count = stat_first(stats,
            f"treedb.cache.vlog_leaf_scan.block.k.count.{codec}",
            f"treedb.cache.vlog_block.k.count.{codec}")
        if count and count != "0":
            avg = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.block.k.avg.{codec}",
                f"treedb.cache.vlog_block.k.avg.{codec}")
            maxv = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.block.k.max.{codec}",
                f"treedb.cache.vlog_block.k.max.{codec}")
            le1 = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.block.k.bucket.{codec}.le_1",
                f"treedb.cache.vlog_block.k.bucket.{codec}.le_1")
            le128 = stat_first(stats,
                f"treedb.cache.vlog_leaf_scan.block.k.bucket.{codec}.le_128",
                f"treedb.cache.vlog_block.k.bucket.{codec}.le_128")
            parts.append(f"{codec}:count={count},avg={avg},max={maxv},le1={le1},le128={le128}")
    return ";".join(parts)

header = [
    "pattern", "db", *[f"ops_{t}" for t in tests],
    "write_mode_off_frames", "write_mode_block_frames", "write_mode_dict_frames",
    "write_mode_block_ratio", "payload_outer_leaf_ratio", "payload_outer_leaf_frames",
    "outer_leaf_codecs_frames_ratio", "auto_frames_frac", "block_k", "mmap_hit_ratio", "mmap_fallback_readat",
    "artifact_dir",
]
rows = []
for p in patterns:
    with open(p / "benchprof_results.json", "r", encoding="utf-8") as f:
        payload = json.load(f)
    for run in payload.get("runs", []):
        results = run.get("results") or {}
        stats_by_db = run.get("treedb_stats") or {}
        dbs = set(stats_by_db.keys())
        for per_test in results.values():
            dbs.update(per_test.keys())
        for db in sorted(dbs):
            stats = stats_by_db.get(db, {})
            rows.append([
                p.name,
                db,
                *[result(results, t, db) for t in tests],
                stat_first(stats, "treedb.cache.vlog_leaf_scan.write_mode.frames.off", "treedb.cache.vlog_write_mode.frames.off"),
                stat_first(stats, "treedb.cache.vlog_leaf_scan.write_mode.frames.block", "treedb.cache.vlog_write_mode.frames.block"),
                stat_first(stats, "treedb.cache.vlog_leaf_scan.write_mode.frames.dict", "treedb.cache.vlog_write_mode.frames.dict"),
                stat_first(stats, "treedb.cache.vlog_leaf_scan.write_mode.stored_ratio.block", "treedb.cache.vlog_write_mode.stored_ratio.block"),
                stat_first(stats, "treedb.cache.vlog_leaf_scan.payload_kind.stored_ratio.outer_leaf", "treedb.cache.vlog_payload_kind.stored_ratio.outer_leaf"),
                stat_first(stats, "treedb.cache.vlog_leaf_scan.payload_kind.frames.outer_leaf", "treedb.cache.vlog_payload_kind.frames.outer_leaf"),
                outer_leaf_codecs(stats),
                auto_frames(stats),
                block_k(stats),
                stat(stats, "treedb.cache.vlog_mmap.read.hit_ratio") or stat(stats, "treedb.vlog.mmap_read.hit_ratio"),
                stat(stats, "treedb.cache.vlog_mmap.read.fallback_readat") or stat(stats, "treedb.vlog.mmap_read.fallback_readat"),
                str(p),
            ])

tsv = run_dir / "codec_matrix_summary.tsv"
with open(tsv, "w", encoding="utf-8") as f:
    f.write("\t".join(header) + "\n")
    for row in rows:
        f.write("\t".join(str(x) for x in row) + "\n")

md = run_dir / "codec_matrix_summary.md"
with open(md, "w", encoding="utf-8") as f:
    f.write("# TreeDB Value-Log Codec Matrix Summary\n\n")
    f.write(f"- Run dir: `{run_dir}`\n")
    f.write(f"- Raw TSV: `{tsv}`\n")
    f.write("- Per-pattern artifacts contain `benchprof_results.md`, `insights.md`, CPU profiles, allocation profiles, mmap stats, and disk usage.\n\n")
    f.write("| " + " | ".join(header) + " |\n")
    f.write("|" + "|".join(["---"] * len(header)) + "|\n")
    for row in rows:
        f.write("| " + " | ".join(str(x).replace("|", "\\|") for x in row) + " |\n")
print(f"summary: {md}")
print(f"raw TSV: {tsv}")
PY

echo "codec matrix bundle: $RUN_DIR"
echo "bundle index: $RUN_DIR/README.md"
echo "summary: $RUN_DIR/codec_matrix_summary.md"
