#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:-a2d8cbb802e0c611a82011a9ea18424817fcead8}"
RUNS="${RUNS:-5}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
KEYS="${KEYS:-4000000}"
BATCH="${BATCH:-1024}"
SEED="${SEED:-1}"
OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="$OUT_ROOT/issue384_gate_${TS}"

# Determinism knobs: pin CPU set externally when available.
RUN_PREFIX="${RUN_PREFIX:-}"
if [[ -n "${CPUSET:-}" ]]; then
  RUN_PREFIX="taskset -c ${CPUSET} ${RUN_PREFIX}"
fi

# The scored matrix is intentionally compression-off to isolate #384 changes.
SCORED_PATTERN="${SCORED_PATTERN:-medium_compressible_sparse}"
NIGHTLY_EXTRA_PATTERN="${NIGHTLY_EXTRA_PATTERN:-}"
AUTO_SANITY_MIN_FRAC="${AUTO_SANITY_MIN_FRAC:-1.01}"
STRICT_GATE="${STRICT_GATE:-1}"

if (( RUNS < 1 )); then
  echo "RUNS must be >= 1" >&2
  exit 2
fi
if (( WARMUP_RUNS < 0 )); then
  echo "WARMUP_RUNS must be >= 0" >&2
  exit 2
fi

export GOMAXPROCS="${GOMAXPROCS:-4}"

mkdir -p "$OUT/runs" "$OUT/bin" "$OUT/worktrees"

BASELINE_WT="$OUT/worktrees/baseline"
if ! git cat-file -e "${BASELINE_HASH}^{commit}" >/dev/null 2>&1; then
  git fetch --no-tags --depth=1 origin "$BASELINE_HASH" || git fetch --no-tags origin "$BASELINE_HASH"
fi
git worktree add --detach "$BASELINE_WT" "$BASELINE_HASH" >/dev/null
cleanup() {
  git worktree remove --force "$BASELINE_WT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

CAND_HASH=$(git rev-parse HEAD)
CAND_BRANCH=$(git rev-parse --abbrev-ref HEAD)

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
candidate_hash=$CAND_HASH
candidate_branch=$CAND_BRANCH
baseline_hash=$BASELINE_HASH
runs=$RUNS
warmup_runs=$WARMUP_RUNS
keys=$KEYS
batch=$BATCH
seed=$SEED
gomaxprocs=$GOMAXPROCS
run_prefix=$RUN_PREFIX
scored_pattern=$SCORED_PATTERN
nightly_extra_pattern=$NIGHTLY_EXTRA_PATTERN
auto_sanity_min_frac=$AUTO_SANITY_MIN_FRAC
strict_gate=$STRICT_GATE
META

GOFLAGS="${GOFLAGS:-}"
if [[ -n "$GOFLAGS" ]]; then
  export GOFLAGS
fi

go build -o "$OUT/bin/unified-bench-candidate" ./cmd/unified_bench
(
  cd "$BASELINE_WT"
  go build -o "$OUT/bin/unified-bench-baseline" ./cmd/unified_bench
)

cases=(
  "bw_v256|batch_write|256|$SCORED_PATTERN|off|15|scored"
  "bw_v2048|batch_write|2048|$SCORED_PATTERN|off|0|scored"
  "mix_v256|batch_write,random_read|256|$SCORED_PATTERN|off|10|scored"
  "mix_v2048|batch_write,random_read|2048|$SCORED_PATTERN|off|0|scored"
  "auto_sanity_v256|batch_write,random_read|256|$SCORED_PATTERN|auto|0|auto_sanity"
)

if [[ -n "$NIGHTLY_EXTRA_PATTERN" ]]; then
  cases+=(
    "bw_v256_rand|batch_write|256|$NIGHTLY_EXTRA_PATTERN|off|15|scored"
    "bw_v2048_rand|batch_write|2048|$NIGHTLY_EXTRA_PATTERN|off|0|scored"
    "mix_v256_rand|batch_write,random_read|256|$NIGHTLY_EXTRA_PATTERN|off|10|scored"
    "mix_v2048_rand|batch_write,random_read|2048|$NIGHTLY_EXTRA_PATTERN|off|0|scored"
  )
fi

CASES_FILE="$OUT/cases.tsv"
printf '%s\n' "${cases[@]}" >"$CASES_FILE"

run_one() {
  local bin="$1"
  local variant="$2"
  local case_id="$3"
  local tests="$4"
  local valsize="$5"
  local pattern="$6"
  local comp="$7"
  local phase="$8"
  local idx="$9"

  local out_dir="$OUT/runs/$case_id/$variant"
  mkdir -p "$out_dir"
  local log="$out_dir/${phase}_${idx}.md"

  local cmd=(
    "$bin"
    -dbs treedb
    -keys "$KEYS"
    -valsize "$valsize"
    -batchsize "$BATCH"
    -test "$tests"
    -val-pattern "$pattern"
    -format markdown
    -progress=false
    -keep=false
    -seed "$SEED"
    -treedb-force-value-pointers=true
    -treedb-value-log-threshold=1
    -treedb-cache-stats-before-reads=true
    -treedb-vlog-compression "$comp"
  )

  if [[ -n "$RUN_PREFIX" ]]; then
    # shellcheck disable=SC2206
    local prefix=( $RUN_PREFIX )
    "${prefix[@]}" "${cmd[@]}" >"$log" 2>&1
  else
    "${cmd[@]}" >"$log" 2>&1
  fi
}

run_pair() {
  local case_id="$1"
  local tests="$2"
  local valsize="$3"
  local pattern="$4"
  local comp="$5"
  local phase="$6"
  local idx="$7"

  # Alternate order to reduce systematic warm-cache/turbo bias.
  if (( idx % 2 == 1 )); then
    run_one "$OUT/bin/unified-bench-baseline" baseline "$case_id" "$tests" "$valsize" "$pattern" "$comp" "$phase" "$idx"
    run_one "$OUT/bin/unified-bench-candidate" candidate "$case_id" "$tests" "$valsize" "$pattern" "$comp" "$phase" "$idx"
  else
    run_one "$OUT/bin/unified-bench-candidate" candidate "$case_id" "$tests" "$valsize" "$pattern" "$comp" "$phase" "$idx"
    run_one "$OUT/bin/unified-bench-baseline" baseline "$case_id" "$tests" "$valsize" "$pattern" "$comp" "$phase" "$idx"
  fi
}

for spec in "${cases[@]}"; do
  IFS='|' read -r case_id tests valsize pattern comp threshold mode <<<"$spec"

  echo "--- case=$case_id mode=$mode tests=$tests valsize=$valsize pattern=$pattern comp=$comp ---"

  for ((i = 1; i <= WARMUP_RUNS; i++)); do
    run_pair "$case_id" "$tests" "$valsize" "$pattern" "$comp" warmup "$i"
  done

  for ((i = 1; i <= RUNS; i++)); do
    run_pair "$case_id" "$tests" "$valsize" "$pattern" "$comp" measured "$i"
  done
done

python3 - "$OUT" "$AUTO_SANITY_MIN_FRAC" "$STRICT_GATE" "$CASES_FILE" <<'PY'
import json
import math
import re
import statistics
import sys
from pathlib import Path

out = Path(sys.argv[1])
auto_min_frac = float(sys.argv[2])
strict_gate = sys.argv[3] not in {"0", "false", "False"}
cases_file = Path(sys.argv[4])
meta = {}
for line in (out / "meta.txt").read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        meta[k] = v

case_specs = []
for raw in cases_file.read_text().splitlines():
    raw = raw.strip()
    if not raw:
        continue
    parts = raw.split("|")
    if len(parts) != 7:
        raise SystemExit(f"invalid case spec (expected 7 fields): {raw}")
    case_id, tests, valsize, pattern, comp, threshold, mode = parts
    case_specs.append((case_id, tests, int(valsize), pattern, comp, float(threshold), mode))

pat_batch_diag = re.compile(r"^Batch Write / TreeDB = ([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", re.M)
pat_batch_md = re.compile(r"^\|\s*Batch Write\s*\|\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*\|", re.M)
pat_batch_text = re.compile(r"^\s*Batch Write\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", re.M)


def parse_batch_write(path: Path) -> float:
    txt = path.read_text()
    m = pat_batch_diag.search(txt)
    if not m:
        m = pat_batch_md.search(txt)
    if not m:
        m = pat_batch_text.search(txt)
    if not m:
        raise SystemExit(f"missing Batch Write metric in {path}")
    return float(m.group(1).replace(",", ""))


def median(vals):
    return float(statistics.median(vals))

rows = []
all_pass = True

for case_id, tests, valsize, pattern, comp, threshold, mode in case_specs:
    base_logs = sorted((out / "runs" / case_id / "baseline").glob("measured_*.md"))
    cand_logs = sorted((out / "runs" / case_id / "candidate").glob("measured_*.md"))
    if not base_logs or not cand_logs:
        raise SystemExit(f"missing measured logs for case {case_id}")

    base_vals = [parse_batch_write(p) for p in base_logs]
    cand_vals = [parse_batch_write(p) for p in cand_logs]

    base_med = median(base_vals)
    cand_med = median(cand_vals)
    if base_med <= 0:
        ratio = math.nan
        delta = math.nan
    else:
        ratio = cand_med / base_med
        delta = (cand_med - base_med) * 100.0 / base_med

    passed = True
    reason = ""
    min_ratio = math.nan
    if mode == "scored":
        min_ratio = max(1.01, 1.0 + threshold / 100.0)
        passed = (not math.isnan(ratio)) and ratio > min_ratio
        if not passed:
            reason = f"candidate/baseline {ratio:.4f} <= min {min_ratio:.4f}"
    elif mode == "auto_sanity":
        min_ratio = auto_min_frac
        frac = ratio
        passed = (not math.isnan(frac)) and frac > auto_min_frac
        if not passed:
            reason = f"candidate/baseline {frac:.4f} <= min {auto_min_frac:.4f}"
    else:
        raise SystemExit(f"unknown gate mode {mode!r} for case {case_id}")

    all_pass = all_pass and passed
    rows.append({
        "case": case_id,
        "tests": tests,
        "valsize": valsize,
        "pattern": pattern,
        "compression": comp,
        "mode": mode,
        "threshold_pct": threshold,
        "baseline_runs": base_vals,
        "candidate_runs": cand_vals,
        "baseline_median": base_med,
        "candidate_median": cand_med,
        "candidate_baseline_ratio": ratio,
        "min_ratio": min_ratio,
        "delta_pct": delta,
        "passed": passed,
        "reason": reason,
    })

summary = {
    "gate_pass": all_pass,
    "meta": meta,
    "rows": rows,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2))

md = []
md.append("# Issue 384 perf gate")
md.append("")
md.append(f"- candidate: `{meta.get('candidate_hash', '')}` ({meta.get('candidate_branch', '')})")
md.append(f"- baseline: `{meta.get('baseline_hash', '')}`")
md.append(f"- runs: warmup={meta.get('warmup_runs', '')} measured={meta.get('runs', '')}")
md.append(f"- keys={meta.get('keys', '')} batch={meta.get('batch', '')} seed={meta.get('seed', '')} GOMAXPROCS={meta.get('gomaxprocs', '')}")
if meta.get("run_prefix", ""):
    md.append(f"- run_prefix: `{meta.get('run_prefix', '')}`")
md.append("")
md.append("| case | mode | tests | valsize | pattern | comp | baseline median | candidate median | candidate/baseline | min ratio | delta | pass |")
md.append("|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|")
for r in rows:
    min_ratio = "-" if math.isnan(r["min_ratio"]) else f"{r['min_ratio']:.4f}x"
    md.append(
        f"| {r['case']} | {r['mode']} | {r['tests']} | {r['valsize']} | {r['pattern']} | {r['compression']} | "
        f"{r['baseline_median']:,.0f} | {r['candidate_median']:,.0f} | {r['candidate_baseline_ratio']:.4f}x | {min_ratio} | {r['delta_pct']:+.2f}% | {'PASS' if r['passed'] else 'FAIL'} |"
    )

failed = [r for r in rows if not r["passed"]]
md.append("")
md.append(f"- overall gate: {'PASS' if all_pass else 'FAIL'}")
if failed:
    md.append("- failures:")
    for r in failed:
        reason = r.get("reason") or "failed"
        md.append(f"  - {r['case']}: {reason}")

(out / "summary.md").write_text("\n".join(md) + "\n")
print((out / "summary.md").read_text())

if strict_gate and not all_pass:
    raise SystemExit(3)
PY

echo "gate artifacts: $OUT"
