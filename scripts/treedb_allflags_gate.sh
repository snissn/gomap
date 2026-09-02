#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_ROOT}/treedb_allflags_gate_${TS}"
RUNS="${RUNS:-7}"
KEYS="${KEYS:-4000000}"
PROFILE="${PROFILE:-fast}"
TESTS="${TESTS:-batch_write,batch_random,random_read,prefix_scan}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
MIN_THROUGHPUT_RATIO="${MIN_THROUGHPUT_RATIO:-1.01}"

BASE_FLAGS="-treedb-force-value-pointers -treedb-index-optimizations=false"
ALL_FLAGS="-treedb-index-optimizations=true"

mkdir -p "$OUT/runs"

cat >"$OUT/meta.txt" <<EOF
ts=$TS
git_rev=$(git rev-parse HEAD)
git_branch=$(git rev-parse --abbrev-ref HEAD)
runs=$RUNS
keys=$KEYS
profile=$PROFILE
tests=$TESTS
extra_args=$EXTRA_ARGS
min_throughput_ratio=$MIN_THROUGHPUT_RATIO
EOF

echo "treedb all-flags gate"
echo "out=$OUT"
echo "runs=$RUNS keys=$KEYS profile=$PROFILE tests=$TESTS min_throughput_ratio=$MIN_THROUGHPUT_RATIO"

make unified-bench >/dev/null

run_variant() {
  local variant="$1"
  local flags="$2"
  local run="$3"
  local out_file="$OUT/runs/${variant}_run${run}.md"
  echo "--- variant=$variant run=$run ---"
  # shellcheck disable=SC2086
  ./bin/unified-bench \
    -dbs treedb \
    -profile "$PROFILE" \
    -keys "$KEYS" \
    -test "$TESTS" \
    -format markdown \
    -progress=false \
    -checkpoint-between-tests \
    $flags \
    $EXTRA_ARGS \
    2>&1 | tee "$out_file" >/dev/null
}

for run in $(seq 1 "$RUNS"); do
  run_variant "base" "$BASE_FLAGS" "$run"
  run_variant "allflags" "$ALL_FLAGS" "$run"
done

python3 - "$OUT" "$MIN_THROUGHPUT_RATIO" <<'PY'
import json
import re
import sys
from pathlib import Path

out = Path(sys.argv[1])
min_throughput_ratio = float(sys.argv[2])
runs_dir = out / "runs"

metric_labels = {
    "batch_write": "Batch Write",
    "batch_random": "Batch Random",
    "random_read": "Random Read",
    "prefix_scan": "Prefix Scan",
}
metrics = list(metric_labels.keys())

def parse_size_to_bytes(s: str):
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]iB)\s*$", s)
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2)
    mul = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[unit]
    return int(n * mul)

def fmt_bytes(n):
    if n is None:
        return "-"
    for u, m in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if n >= m:
            return f"{n / m:.1f} {u}"
    return f"{n} B"

def median(values):
    values = sorted(values)
    return values[len(values) // 2]

def parse_run(path: Path):
    text = path.read_text()
    row = {}
    for metric, label in metric_labels.items():
        m = re.search(rf"^{re.escape(label)} / TreeDB = ([0-9,]+)\s*$", text, flags=re.M)
        if not m:
            raise SystemExit(f"missing metric {label} in {path}")
        row[metric] = int(m.group(1).replace(",", ""))
    m = re.search(r"^\s*maindb/index\.db:\s*(.+?)\s*$", text, flags=re.M)
    if not m:
        raise SystemExit(f"missing index.db size in {path}")
    idx = parse_size_to_bytes(m.group(1).strip())
    if idx is None:
        raise SystemExit(f"cannot parse index.db size '{m.group(1).strip()}' in {path}")
    row["index_db_bytes"] = idx
    return row

data = {"base": [], "allflags": []}
for variant in data.keys():
    files = sorted(runs_dir.glob(f"{variant}_run*.md"))
    if not files:
        raise SystemExit(f"no run files for {variant}")
    for f in files:
        data[variant].append(parse_run(f))

summary = {"base": {}, "allflags": {}, "delta_pct": {}, "ratio": {}}
for metric in metrics + ["index_db_bytes"]:
    summary["base"][metric] = median([r[metric] for r in data["base"]])
    summary["allflags"][metric] = median([r[metric] for r in data["allflags"]])
    b = summary["base"][metric]
    a = summary["allflags"][metric]
    if b == 0:
        summary["delta_pct"][metric] = None
        summary["ratio"][metric] = None
    else:
        summary["delta_pct"][metric] = (a - b) * 100.0 / b
        summary["ratio"][metric] = a / b

pass_checks = []
for metric in metrics:
    ratio = summary["ratio"][metric]
    pass_checks.append(ratio is not None and ratio > min_throughput_ratio)
pass_checks.append(summary["allflags"]["index_db_bytes"] < summary["base"]["index_db_bytes"])
gate_pass = all(pass_checks)

summary_path = out / "summary.json"
summary_payload = {
    "gate_pass": gate_pass,
    "min_throughput_ratio": min_throughput_ratio,
    "summary": summary,
    "runs": data,
}
summary_path.write_text(json.dumps(summary_payload, indent=2))

md = []
md.append("# TreeDB all-flags gate")
md.append("")
md.append(f"- runs per variant: {len(data['base'])}")
md.append(f"- artifacts: `{out}`")
md.append(f"- throughput gate: candidate/baseline > {min_throughput_ratio:.2f}x")
md.append("")
md.append("| metric | base median | all-flags median | ratio | delta | gate |")
md.append("|---|---:|---:|---:|---:|---|")
for metric in metrics:
    b = summary["base"][metric]
    a = summary["allflags"][metric]
    d = summary["delta_pct"][metric]
    ratio = summary["ratio"][metric]
    passed = ratio is not None and ratio > min_throughput_ratio
    ratio_s = "-" if ratio is None else f"{ratio:.4f}x"
    delta_s = "-" if d is None else f"{d:+.2f}%"
    md.append(f"| {metric} | {b:,} | {a:,} | {ratio_s} | {delta_s} | {'PASS' if passed else 'FAIL'} |")
b = summary["base"]["index_db_bytes"]
a = summary["allflags"]["index_db_bytes"]
d = summary["delta_pct"]["index_db_bytes"]
ratio = summary["ratio"]["index_db_bytes"]
ratio_s = "-" if ratio is None else f"{ratio:.4f}x"
delta_s = "-" if d is None else f"{d:+.2f}%"
md.append(f"| index_db_bytes | {fmt_bytes(b)} | {fmt_bytes(a)} | {ratio_s} | {delta_s} | {'PASS' if a < b else 'FAIL'} |")
md.append("")
md.append(f"- gate: {'PASS' if gate_pass else 'FAIL'}")

(out / "summary.md").write_text("\n".join(md) + "\n")
print((out / "summary.md").read_text())

if not gate_pass:
    raise SystemExit(3)
PY

echo "gate artifacts: $OUT"
