#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

PR_NUMBER="${PR_NUMBER:-382}"
POST_COMMENTS="${POST_COMMENTS:-1}"
VALSIZES="${VALSIZES:-128 256 512 1024 2048}"
PATTERNS="${PATTERNS:-medium_compressible_sparse celestia_height_prefix_fill}"
TRAIN_RECORDS="${TRAIN_RECORDS:-20000}"
EVAL_RECORDS="${EVAL_RECORDS:-5000}"
BATCH="${BATCH:-1024}"
WORKERS="${WORKERS:-1}"
BENCH_MODE="${BENCH_MODE:-wal_off}"
DICT_TRAIN_MIB="${DICT_TRAIN_MIB:-4}"
DICT_BYTES="${DICT_BYTES:-40960}"
DICT_SAMPLE_STRIDE="${DICT_SAMPLE_STRIDE:-1}"
DICT_WAIT_SECONDS="${DICT_WAIT_SECONDS:-10}"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_ROOT}/issue379_vlog_warmup_sweep_${TS}"
mkdir -p "$OUT/runs" "$OUT/comments"

cat >"$OUT/meta.txt" <<EOF
ts=$TS
root=$ROOT
git_rev=$(git rev-parse HEAD)
git_branch=$(git rev-parse --abbrev-ref HEAD)
pr_number=$PR_NUMBER
valsizes=$VALSIZES
patterns=$PATTERNS
train_records=$TRAIN_RECORDS
eval_records=$EVAL_RECORDS
batch=$BATCH
workers=$WORKERS
bench_mode=$BENCH_MODE
dict_train_mib=$DICT_TRAIN_MIB
dict_bytes=$DICT_BYTES
dict_sample_stride=$DICT_SAMPLE_STRIDE
dict_wait_seconds=$DICT_WAIT_SECONDS
EOF

echo "issue379 vlog warmup sweep"
echo "out=$OUT"
echo "valsizes=$VALSIZES patterns=$PATTERNS"

run_case() {
  local valsize="$1"
  local raw_mib="$2"
  local pattern="$3"
  local mode="$4"
  local codec="$5"
  local id="$6"

  local log_file="$OUT/runs/${id}.log"
  local time_file="$OUT/runs/${id}.time"
  local json_file="$OUT/runs/${id}.json"

  echo "--- valsize=${valsize} raw_mib=${raw_mib} pattern=${pattern} mode=${mode} codec=${codec} ---"

  /usr/bin/time -p -o "$time_file" \
    go run ./TreeDB/cmd/vlog_dict_realdata \
      -bench-kv \
      -bench-synth \
      -bench-synth-pattern "$pattern" \
      -bench-synth-valsize "$valsize" \
      -bench-synth-train-records "$TRAIN_RECORDS" \
      -bench-synth-eval-records "$EVAL_RECORDS" \
      -bench-mode "$BENCH_MODE" \
      -bench-compression-mode "$mode" \
      -bench-block-codec "$codec" \
      -bench-template off \
      -bench-raw-mib "$raw_mib" \
      -bench-batch "$BATCH" \
      -bench-workers "$WORKERS" \
      -bench-key-mode random \
      -bench-pointer-threshold 1 \
      -bench-dict-train-mib "$DICT_TRAIN_MIB" \
      -bench-dict-bytes "$DICT_BYTES" \
      -bench-dict-sample-stride "$DICT_SAMPLE_STRIDE" \
      -bench-dict-wait-seconds "$DICT_WAIT_SECONDS" \
      -bench-out-json "$json_file" \
      >"$log_file" 2>&1
}

for valsize in $VALSIZES; do
  raw_mib=$((valsize * 262144 / 1048576))
  if [[ "$raw_mib" -le 0 ]]; then
    raw_mib=1
  fi

  for pattern in $PATTERNS; do
    run_case "$valsize" "$raw_mib" "$pattern" "off" "snappy" "val${valsize}_${pattern}_off"
    run_case "$valsize" "$raw_mib" "$pattern" "dict" "snappy" "val${valsize}_${pattern}_dict"
    run_case "$valsize" "$raw_mib" "$pattern" "block" "snappy" "val${valsize}_${pattern}_block_snappy"
    run_case "$valsize" "$raw_mib" "$pattern" "block" "lz4" "val${valsize}_${pattern}_block_lz4"
  done

  comment_file="$OUT/comments/val${valsize}.md"
  python3 - "$OUT" "$valsize" "$raw_mib" >"$comment_file" <<'PY'
import json
import re
import sys
from pathlib import Path

out = Path(sys.argv[1])
valsize = int(sys.argv[2])
raw_mib = int(sys.argv[3])
patterns = ["medium_compressible_sparse", "celestia_height_prefix_fill"]
mode_order = ["off", "dict", "block_snappy", "block_lz4"]

def wall_sec(path: Path) -> float:
    txt = path.read_text()
    m = re.search(r"^real\s+([0-9]+(?:\.[0-9]+)?)\s*$", txt, flags=re.M)
    if not m:
        raise SystemExit(f"missing real time in {path}")
    return float(m.group(1))

def mode_label(mode_key: str):
    if mode_key == "off":
        return ("off", "-")
    if mode_key == "dict":
        return ("dict", "zstd-dict")
    if mode_key == "block_snappy":
        return ("block", "snappy")
    if mode_key == "block_lz4":
        return ("block", "lz4")
    raise ValueError(mode_key)

lines = []
lines.append(f"## Warmup-first TreeDB vlog sweep (valsize={valsize} bytes)")
lines.append("")
lines.append(f"- artifacts: `{out}`")
lines.append(f"- steady raw target: `{raw_mib} MiB`")
lines.append("- scope: TreeDB value-log only (`off`, `dict`, `block/snappy|lz4`)")
lines.append("- patterns: `medium_compressible_sparse`, `celestia_height_prefix_fill`")
lines.append("")

for pattern in patterns:
    lines.append(f"### Pattern: `{pattern}`")
    lines.append("")
    lines.append("| mode | codec | steady ops/sec | steady wall (s) | total wall (s) | final vlog size (MB) | final vlog size (MiB) | steady vlog ratio | total vlog ratio | dict k |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for mode_key in mode_order:
        run_id = f"val{valsize}_{pattern}_{mode_key}"
        j = json.loads((out / "runs" / f"{run_id}.json").read_text())
        total_wall = wall_sec(out / "runs" / f"{run_id}.time")
        mode, codec = mode_label(mode_key)
        ops = 0.0
        steady_seconds = float(j.get("steady_seconds", 0.0))
        steady_records = float(j.get("steady_records", 0.0))
        if steady_seconds > 0:
            ops = steady_records / steady_seconds
        vlog_bytes = int(j.get("value_log_bytes", 0))
        steady_ratio = j.get("steady_vlog_ratio")
        total_ratio = j.get("total_vlog_ratio")
        k = j.get("current_k")
        k_str = "-" if k is None else str(k)
        sr = "-" if steady_ratio is None else f"{float(steady_ratio):.4f}x"
        tr = "-" if total_ratio is None else f"{float(total_ratio):.4f}x"
        lines.append(
            f"| {mode} | {codec} | {ops:,.0f} | {steady_seconds:.2f} | {total_wall:.2f} | {vlog_bytes/1_000_000:.2f} | {vlog_bytes/(1024**2):.2f} | {sr} | {tr} | {k_str} |"
        )
    lines.append("")

lines.append("### Reproduce (this valsize)")
lines.append("")
lines.append("```bash")
lines.append("cd /Users/michaelseiler/dev/snissn/gomap")
lines.append(f"VALSIZES=\"{valsize}\" POST_COMMENTS=0 scripts/issue379_vlog_warmup_sweep.sh")
lines.append("```")
print("\n".join(lines))
PY

  echo "posted summary for valsize=$valsize:"
  cat "$comment_file"

  if [[ "$POST_COMMENTS" != "0" ]]; then
    gh pr comment "$PR_NUMBER" --repo snissn/gomap --body-file "$comment_file"
  fi
done

echo "done: $OUT"
