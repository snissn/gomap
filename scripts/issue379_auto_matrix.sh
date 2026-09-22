#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

PR_NUMBER="${PR_NUMBER:-382}"
POST_COMMENTS="${POST_COMMENTS:-0}"
VALSIZES="${VALSIZES:-128 256 512 1024 2048}"
PATTERNS="${PATTERNS:-medium_compressible_sparse celestia_height_prefix_fill}"
RAW_MIB="${RAW_MIB:-64}"
BATCH="${BATCH:-1024}"
SEED="${SEED:-1}"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="$OUT_ROOT/issue379_auto_matrix_${TS}"
mkdir -p "$OUT/runs" "$OUT/comments"

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
git_rev=$(git rev-parse HEAD)
git_branch=$(git rev-parse --abbrev-ref HEAD)
pr_number=$PR_NUMBER
valsizes=$VALSIZES
patterns=$PATTERNS
raw_mib=$RAW_MIB
batch=$BATCH
seed=$SEED
META

variants=(
  "treedb_off|treedb_vlog_off|TreeDB (vlog=off)|off|-|treedb"
  "treedb_dict|treedb_vlog_dict|TreeDB (vlog=dict)|dict|zstd-dict|treedb"
  "treedb_block_snappy|treedb_vlog_block_snappy|TreeDB (vlog=block/snappy)|block|snappy|treedb"
  "treedb_block_lz4|treedb_vlog_block_lz4|TreeDB (vlog=block/lz4)|block|lz4|treedb"
  "treedb_auto|treedb_vlog_auto|TreeDB (vlog=auto)|auto|selector|treedb"
  "leveldb_off|leveldb_block_comp_off|LevelDB (block=off)|leveldb|off|leveldb"
  "leveldb_on|leveldb_block_comp_on|LevelDB (block=on)|leveldb|on|leveldb"
)

echo "issue379 auto matrix"
echo "out=$OUT"

to_key_count() {
  local valsize="$1"
  local keys=$((RAW_MIB * 1048576 / valsize))
  if [[ "$keys" -lt 5000 ]]; then
    keys=5000
  fi
  echo "$keys"
}

run_case() {
  local valsize="$1"
  local keys="$2"
  local pattern="$3"
  local vid="$4"
  local dbname="$5"

  local id="val${valsize}_${pattern}_${vid}"
  local log_file="$OUT/runs/${id}.log"
  local time_file="$OUT/runs/${id}.time"

  echo "--- valsize=${valsize} keys=${keys} pattern=${pattern} db=${dbname} ---"

  /usr/bin/time -p -o "$time_file" \
    go run ./cmd/unified_bench \
      -keys "$keys" \
      -valsize "$valsize" \
      -batchsize "$BATCH" \
      -dbs "$dbname" \
      -test batch_write,random_read \
      -val-pattern "$pattern" \
      -format markdown \
      -progress=false \
      -keep=false \
      -seed "$SEED" \
      -treedb-force-value-pointers=true \
      -treedb-value-log-threshold=1 \
      -treedb-cache-stats-before-reads=true \
      >"$log_file" 2>&1
}

for valsize in $VALSIZES; do
  keys=$(to_key_count "$valsize")
  for pattern in $PATTERNS; do
    for spec in "${variants[@]}"; do
      IFS='|' read -r vid dbname _ _ _ _ <<<"$spec"
      run_case "$valsize" "$keys" "$pattern" "$vid" "$dbname"
    done
  done

  comment_file="$OUT/comments/val${valsize}.md"
  python3 - "$OUT" "$valsize" "$RAW_MIB" "$BATCH" "$SEED" "$PATTERNS" >"$comment_file" <<'PY'
import json
import math
import re
import sys
from pathlib import Path

out = Path(sys.argv[1])
valsize = int(sys.argv[2])
raw_mib = int(sys.argv[3])
batch = int(sys.argv[4])
seed = int(sys.argv[5])
patterns = [p for p in sys.argv[6].split() if p]
variants = [
    ("treedb_off", "TreeDB (vlog=off)", "off", "-", "treedb"),
    ("treedb_dict", "TreeDB (vlog=dict)", "dict", "zstd-dict", "treedb"),
    ("treedb_block_snappy", "TreeDB (vlog=block/snappy)", "block", "snappy", "treedb"),
    ("treedb_block_lz4", "TreeDB (vlog=block/lz4)", "block", "lz4", "treedb"),
    ("treedb_auto", "TreeDB (vlog=auto)", "auto", "selector", "treedb"),
    ("leveldb_off", "LevelDB (block=off)", "leveldb", "off", "leveldb"),
    ("leveldb_on", "LevelDB (block=on)", "leveldb", "on", "leveldb"),
]

UNIT = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}


def parse_bytes(token: str) -> int:
    token = token.strip()
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)", token)
    if not m:
        raise ValueError(f"cannot parse bytes token: {token!r}")
    val = float(m.group(1))
    unit = m.group(2)
    mul = UNIT.get(unit)
    if mul is None:
        raise ValueError(f"unknown unit: {unit}")
    return int(val * mul)


def parse_real_seconds(path: Path) -> float:
    txt = path.read_text()
    m = re.search(r"^real\s+([0-9]+(?:\.[0-9]+)?)\s*$", txt, flags=re.M)
    if not m:
        return math.nan
    return float(m.group(1))


def parse_stats_map(line: str) -> dict:
    out = {}
    for tok in line.split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def parse_run(log_path: Path, time_path: Path, wrapper: str, engine: str):
    txt = log_path.read_text()
    ops = math.nan
    m_ops = re.search(r"Batch Write / .* = ([0-9][0-9,]*(?:\.[0-9]+)?)", txt)
    if m_ops:
        ops = float(m_ops.group(1).replace(",", ""))

    size_bytes = 0
    vlog_bytes = None
    if engine == "treedb":
        m_value_vlog = re.search(r"maindb/value_vlog:[^\n]*\bvalue=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)", txt)
        m_vlog = re.search(r"maindb/wal:[^\n]*\bvlog=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)", txt)
        m_value = re.search(r"maindb/wal:[^\n]*\bvalue=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)", txt)
        if m_value_vlog:
            vlog_bytes = parse_bytes(m_value_vlog.group(1))
        elif m_vlog:
            vlog_bytes = parse_bytes(m_vlog.group(1))
        elif m_value:
            vlog_bytes = parse_bytes(m_value.group(1))
        else:
            vlog_bytes = 0
        size_bytes = vlog_bytes
    else:
        m_total = re.search(re.escape(wrapper) + r":\s+total=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)", txt)
        if m_total:
            size_bytes = parse_bytes(m_total.group(1))

    stats_line = None
    m_stats = re.search(r"pre-random_read treedb\.cache \(" + re.escape(wrapper) + r"\):\s*(.*)", txt)
    if m_stats:
        stats_line = m_stats.group(1)
    stats = parse_stats_map(stats_line) if stats_line else {}

    return {
        "ops": ops,
        "total_wall": parse_real_seconds(time_path),
        "size_bytes": size_bytes,
        "vlog_bytes": vlog_bytes,
        "stats": stats,
    }


lines = []
lines.append(f"## Auto codec matrix (valsize={valsize} bytes)")
lines.append("")
lines.append(f"- raw target per run: `{raw_mib} MiB`")
lines.append(f"- batchsize: `{batch}`")
lines.append(f"- seed: `{seed}`")
lines.append(f"- artifacts: `{out}`")
lines.append("")

for pattern in patterns:
    keys = max(5000, (raw_mib * 1048576) // valsize)
    raw_bytes = keys * valsize
    lines.append(f"### Pattern: `{pattern}`")
    lines.append("")
    lines.append(f"- keys: `{keys}`")
    lines.append("| mode | codec | steady ops/sec | steady wall (s) | total wall (s) | final size (MB) | final size (MiB) | steady vlog ratio | total ratio | k |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    auto_stats = None

    for vid, wrapper, mode, codec, engine in variants:
        run_id = f"val{valsize}_{pattern}_{vid}"
        run = parse_run(out / "runs" / f"{run_id}.log", out / "runs" / f"{run_id}.time", wrapper, engine)
        ops = run["ops"]
        steady_wall = (keys / ops) if (ops and not math.isnan(ops) and ops > 0) else math.nan
        total_wall = run["total_wall"]
        size_bytes = int(run["size_bytes"])

        steady_ratio = "-"
        total_ratio = "-"
        if engine == "treedb":
            vlog_bytes = int(run["vlog_bytes"] or 0)
            ratio = (vlog_bytes / raw_bytes) if raw_bytes > 0 else math.nan
            if not math.isnan(ratio):
                steady_ratio = f"{ratio:.4f}x"
                total_ratio = f"{ratio:.4f}x"
        else:
            ratio = (size_bytes / raw_bytes) if raw_bytes > 0 else math.nan
            if not math.isnan(ratio):
                total_ratio = f"{ratio:.4f}x"

        stats = run["stats"]
        k = stats.get("treedb.cache.vlog_dict.current_k", "-")
        if engine != "treedb":
            k = "-"
        else:
            try:
                if int(k) <= 0:
                    k = "-"
            except Exception:
                pass

        if vid == "treedb_auto":
            auto_stats = stats

        lines.append(
            "| {mode} | {codec} | {ops:,} | {steady:.4f} | {total:.4f} | {mb:.2f} | {mib:.2f} | {steady_ratio} | {total_ratio} | {k} |".format(
                mode=mode,
                codec=codec,
                ops=0 if math.isnan(ops) else int(ops),
                steady=0.0 if math.isnan(steady_wall) else steady_wall,
                total=0.0 if math.isnan(total_wall) else total_wall,
                mb=size_bytes / 1_000_000,
                mib=size_bytes / (1024**2),
                steady_ratio=steady_ratio,
                total_ratio=total_ratio,
                k=k,
            )
        )

    if auto_stats:
        lines.append("")
        lines.append("auto stats (pre-random_read):")
        lines.append(
            "- picks frames: off={off} dict={dictv} block/snappy={snappy} block/lz4={lz4}".format(
                off=auto_stats.get("treedb.cache.vlog_auto.frames.off", "0"),
                dictv=auto_stats.get("treedb.cache.vlog_auto.frames.dict", "0"),
                snappy=auto_stats.get("treedb.cache.vlog_auto.frames.block_snappy", "0"),
                lz4=auto_stats.get("treedb.cache.vlog_auto.frames.block_lz4", "0"),
            )
        )
        lines.append(
            "- probe/hold: probes={p}/{s} holds={h_in}/{h_out} bypass_bytes={b}".format(
                p=auto_stats.get("treedb.cache.vlog_auto.probe_attempts", "0"),
                s=auto_stats.get("treedb.cache.vlog_auto.probe_successes", "0"),
                h_in=auto_stats.get("treedb.cache.vlog_auto.hold_enters", "0"),
                h_out=auto_stats.get("treedb.cache.vlog_auto.hold_exits", "0"),
                b=auto_stats.get("treedb.cache.vlog_auto.bypass_bytes", "0"),
            )
        )
        lines.append(
            "- dict status: dict_id={dict_id} frames_attempted={attempted} frames_kept={kept} k={k}".format(
                dict_id=auto_stats.get("treedb.cache.vlog_dict.last_applied_dict_id", "0"),
                attempted=auto_stats.get("treedb.cache.vlog_dict.frames_attempted", "0"),
                kept=auto_stats.get("treedb.cache.vlog_dict.frames_kept", "0"),
                k=auto_stats.get("treedb.cache.vlog_dict.current_k", "0"),
            )
        )
    lines.append("")

lines.append("### Reproduce (this valsize)")
lines.append("")
lines.append("```bash")
lines.append("cd /Users/michaelseiler/dev/snissn/gomap")
lines.append(f"VALSIZES=\"{valsize}\" POST_COMMENTS=0 scripts/issue379_auto_matrix.sh")
lines.append("```")

print("\n".join(lines))
PY

  echo "generated summary for valsize=$valsize"
  cat "$comment_file"

  if [[ "$POST_COMMENTS" != "0" ]]; then
    gh pr comment "$PR_NUMBER" --repo snissn/gomap --body-file "$comment_file"
  fi
done

echo "done: $OUT"
