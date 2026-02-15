#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUNS="${RUNS:-3}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
KEYS="${KEYS:-1000000}"
BATCHSIZE="${BATCHSIZE:-8000}"
SEED="${SEED:-1}"
PROFILES="${PROFILES:-fast,wal_on_fast,durable}"
TESTS="${TESTS:-batch_write,batch_write_steady,prefix_scan}"
VAL_PATTERN="${VAL_PATTERN:-repeat_tail64}"
RANGE_QUERIES="${RANGE_QUERIES:-200}"
RANGE_SPAN="${RANGE_SPAN:-100}"
CHUNK_SIZE="${CHUNK_SIZE:-262144}"
BATCH_WRITE_STEADY_CHECKPOINT_BYTES="${BATCH_WRITE_STEADY_CHECKPOINT_BYTES:-67108864}"
CHECKPOINT_BETWEEN_TESTS="${CHECKPOINT_BETWEEN_TESTS:-1}"

STRICT_GATE="${STRICT_GATE:-1}"
GATE_PROFILES="${GATE_PROFILES:-fast}"
GATE_MIN_RATIO_BATCH_WRITE="${GATE_MIN_RATIO_BATCH_WRITE:-0.70}"
GATE_MIN_RATIO_BATCH_WRITE_STEADY="${GATE_MIN_RATIO_BATCH_WRITE_STEADY:-0.70}"
GATE_MIN_RATIO_PREFIX_SCAN="${GATE_MIN_RATIO_PREFIX_SCAN:-0.35}"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_ROOT}/outerleaf_v2_gate_${TS}"
mkdir -p "$OUT/runs"

if (( RUNS < 1 )); then
  echo "RUNS must be >= 1" >&2
  exit 2
fi
if (( WARMUP_RUNS < 0 )); then
  echo "WARMUP_RUNS must be >= 0" >&2
  exit 2
fi

make unified-bench >/dev/null

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
git_rev=$(git rev-parse HEAD)
git_branch=$(git rev-parse --abbrev-ref HEAD)
runs=$RUNS
warmup_runs=$WARMUP_RUNS
keys=$KEYS
batchsize=$BATCHSIZE
seed=$SEED
profiles=$PROFILES
tests=$TESTS
val_pattern=$VAL_PATTERN
range_queries=$RANGE_QUERIES
range_span=$RANGE_SPAN
chunk_size=$CHUNK_SIZE
batch_write_steady_checkpoint_bytes=$BATCH_WRITE_STEADY_CHECKPOINT_BYTES
checkpoint_between_tests=$CHECKPOINT_BETWEEN_TESTS
strict_gate=$STRICT_GATE
gate_profiles=$GATE_PROFILES
gate_min_ratio_batch_write=$GATE_MIN_RATIO_BATCH_WRITE
gate_min_ratio_batch_write_steady=$GATE_MIN_RATIO_BATCH_WRITE_STEADY
gate_min_ratio_prefix_scan=$GATE_MIN_RATIO_PREFIX_SCAN
META

cases=(
  "v1_inline|-treedb-index-outer-leaf-mode=v1 -treedb-force-value-pointers=false"
  "v1_forceptr|-treedb-index-outer-leaf-mode=v1 -treedb-force-value-pointers=true"
  "v2_fenceptr|-treedb-index-outer-leaf-mode=v2_fenceptr -treedb-force-value-pointers=true"
)

CASES_FILE="$OUT/cases.tsv"
printf '%s\n' "${cases[@]}" >"$CASES_FILE"

run_one() {
  local profile="$1"
  local case_id="$2"
  local variant_flags="$3"
  local phase="$4"
  local idx="$5"

  local out_dir="$OUT/runs/$profile/$case_id"
  mkdir -p "$out_dir"
  local log="$out_dir/${phase}_${idx}.md"

  local cmd=(
    ./bin/unified-bench
    -dbs treedb
    -profile "$profile"
    -keys "$KEYS"
    -batchsize "$BATCHSIZE"
    -test "$TESTS"
    -val-pattern "$VAL_PATTERN"
    -range-queries "$RANGE_QUERIES"
    -range-span "$RANGE_SPAN"
    -treedb-chunk-size "$CHUNK_SIZE"
    -batch-write-steady-checkpoint-bytes "$BATCH_WRITE_STEADY_CHECKPOINT_BYTES"
    -progress=false
    -format markdown
    -seed "$SEED"
  )
  if [[ "$CHECKPOINT_BETWEEN_TESTS" != "0" ]]; then
    cmd+=(-checkpoint-between-tests)
  fi

  # shellcheck disable=SC2206
  local flags=( $variant_flags )
  cmd+=("${flags[@]}")
  "${cmd[@]}" >"$log" 2>&1
}

IFS=',' read -r -a profile_list <<<"$PROFILES"
for profile in "${profile_list[@]}"; do
  profile="${profile//[[:space:]]/}"
  if [[ -z "$profile" ]]; then
    continue
  fi
  for spec in "${cases[@]}"; do
    IFS='|' read -r case_id variant_flags <<<"$spec"
    echo "--- profile=$profile case=$case_id ---" >&2
    for ((i = 1; i <= WARMUP_RUNS; i++)); do
      run_one "$profile" "$case_id" "$variant_flags" warmup "$i"
    done
    for ((i = 1; i <= RUNS; i++)); do
      run_one "$profile" "$case_id" "$variant_flags" measured "$i"
    done
  done
done

python3 - "$OUT" "$CASES_FILE" <<'PY'
import json
import re
import statistics
import sys
from pathlib import Path

out = Path(sys.argv[1])
cases_file = Path(sys.argv[2])

meta = {}
for line in (out / "meta.txt").read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        meta[k] = v

profiles = [p.strip() for p in meta.get("profiles", "").split(",") if p.strip()]
tests = [t.strip() for t in meta.get("tests", "").split(",") if t.strip()]
gate_profiles = {p.strip() for p in meta.get("gate_profiles", "").split(",") if p.strip()}
strict_gate = meta.get("strict_gate", "1") not in {"0", "false", "False"}

thresholds = {
    "batch_write": float(meta.get("gate_min_ratio_batch_write", "0.70")),
    "batch_write_steady": float(meta.get("gate_min_ratio_batch_write_steady", "0.70")),
    "prefix_scan": float(meta.get("gate_min_ratio_prefix_scan", "0.35")),
}

cases = []
for raw in cases_file.read_text().splitlines():
    raw = raw.strip()
    if not raw:
        continue
    case_id, flags = raw.split("|", 1)
    cases.append({"id": case_id, "flags": flags})

display = {
    "batch_write": "Batch Write",
    "batch_write_steady": "Batch Write (Steady)",
    "prefix_scan": "Prefix Scan",
}

pattern_cache = {}
def metric_pattern(test_name: str):
    key = test_name
    if key in pattern_cache:
        return pattern_cache[key]
    disp = re.escape(display.get(test_name, test_name))
    pat = re.compile(rf"^{disp} / TreeDB = ([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", re.M)
    pattern_cache[key] = pat
    return pat

def parse_metric(path: Path, test_name: str):
    text = path.read_text()
    m = metric_pattern(test_name).search(text)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))

def median(vals):
    return float(statistics.median(vals))

rows = []
failures = []

for profile in profiles:
    for case in cases:
        case_id = case["id"]
        logs = sorted((out / "runs" / profile / case_id).glob("measured_*.md"))
        if not logs:
            continue
        for test_name in tests:
            vals = []
            for log in logs:
                v = parse_metric(log, test_name)
                if v is not None:
                    vals.append(v)
            if not vals:
                continue
            rows.append({
                "profile": profile,
                "case": case_id,
                "test": test_name,
                "runs": vals,
                "median": median(vals),
            })

lookup = {(r["profile"], r["case"], r["test"]): r for r in rows}
compare_rows = []

for profile in profiles:
    for test_name in tests:
        v1_inline = lookup.get((profile, "v1_inline", test_name))
        v1_force = lookup.get((profile, "v1_forceptr", test_name))
        v2 = lookup.get((profile, "v2_fenceptr", test_name))
        if not v2:
            continue
        med_v2 = v2["median"]
        med_inline = v1_inline["median"] if v1_inline else None
        med_force = v1_force["median"] if v1_force else None
        ratio_vs_inline = (med_v2 / med_inline) if med_inline and med_inline > 0 else None
        ratio_vs_force = (med_v2 / med_force) if med_force and med_force > 0 else None
        delta_vs_inline = ((med_v2 - med_inline) / med_inline * 100.0) if med_inline and med_inline > 0 else None
        delta_vs_force = ((med_v2 - med_force) / med_force * 100.0) if med_force and med_force > 0 else None

        passed = True
        reason = ""
        if profile in gate_profiles and ratio_vs_force is not None:
            need = thresholds.get(test_name, 0.0)
            passed = ratio_vs_force >= need
            if not passed:
                reason = f"{test_name} ratio vs v1_forceptr {ratio_vs_force:.3f} < {need:.3f}"
                failures.append({"profile": profile, "test": test_name, "reason": reason})

        compare_rows.append({
            "profile": profile,
            "test": test_name,
            "v1_inline_median": med_inline,
            "v1_force_median": med_force,
            "v2_median": med_v2,
            "ratio_vs_inline": ratio_vs_inline,
            "ratio_vs_force": ratio_vs_force,
            "delta_vs_inline_pct": delta_vs_inline,
            "delta_vs_force_pct": delta_vs_force,
            "gate_pass": passed,
            "gate_reason": reason,
        })

gate_pass = len(failures) == 0
summary = {
    "meta": meta,
    "rows": rows,
    "comparisons": compare_rows,
    "gate_pass": gate_pass,
    "failures": failures,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2))

def fmt_num(v):
    if v is None:
        return "-"
    return f"{v:,.0f}"

def fmt_pct(v):
    if v is None:
        return "-"
    return f"{v:+.2f}%"

def fmt_ratio(v):
    if v is None:
        return "-"
    return f"{v:.3f}x"

md = []
md.append("# outerleaf v2 perf gate")
md.append("")
md.append(f"- git: `{meta.get('git_rev', '-')}` (`{meta.get('git_branch', '-')}`)")
md.append(f"- runs: warmup={meta.get('warmup_runs', '-')} measured={meta.get('runs', '-')}")
md.append(f"- keys={meta.get('keys', '-')} batchsize={meta.get('batchsize', '-')} seed={meta.get('seed', '-')}")
md.append(f"- profiles: `{meta.get('profiles', '-')}`")
md.append(f"- tests: `{meta.get('tests', '-')}`")
md.append(f"- val-pattern: `{meta.get('val_pattern', '-')}`")
md.append("")

for profile in profiles:
    md.append(f"## Profile: {profile}")
    md.append("")
    md.append("| test | v1 inline | v1 forceptr | v2 fenceptr | v2 vs v1 inline | v2 vs v1 forceptr | gate |")
    md.append("|---|---:|---:|---:|---:|---:|---|")
    for c in compare_rows:
        if c["profile"] != profile:
            continue
        gate = "PASS" if c["gate_pass"] else "FAIL"
        md.append(
            f"| {display.get(c['test'], c['test'])} | {fmt_num(c['v1_inline_median'])} | {fmt_num(c['v1_force_median'])} | "
            f"{fmt_num(c['v2_median'])} | {fmt_ratio(c['ratio_vs_inline'])} ({fmt_pct(c['delta_vs_inline_pct'])}) | "
            f"{fmt_ratio(c['ratio_vs_force'])} ({fmt_pct(c['delta_vs_force_pct'])}) | {gate} |"
        )
    md.append("")

md.append(f"- overall gate: {'PASS' if gate_pass else 'FAIL'}")
if failures:
    md.append("- failures:")
    for f in failures:
        md.append(f"  - {f['profile']}: {f['reason']}")

summary_md = "\n".join(md) + "\n"
(out / "summary.md").write_text(summary_md)
print(summary_md)

if strict_gate and not gate_pass:
    raise SystemExit(3)
PY

echo "artifacts: $OUT" >&2
