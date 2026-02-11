#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_ROOT}/issue414_forceptr_matrix_${TS}"

KEYS="${KEYS:-1000000}"
BATCHSIZE="${BATCHSIZE:-8000}"
PROFILE="${PROFILE:-fast}"
TEST="${TEST:-random_write_parallel}"
RUNS="${RUNS:-3}"

VALSIZES="${VALSIZES:-128,256,1025}"
WORKERS="${WORKERS:-4,8,16}"
FORCE_FLAGS="${FORCE_FLAGS:-false,true}"

MEMTABLE_MODE="${MEMTABLE_MODE:-append_only}"
CHECKPOINT_BETWEEN_TESTS="${CHECKPOINT_BETWEEN_TESTS:-1}"
PROFILE_LIGHT="${PROFILE_LIGHT:-1}"
PROFILE_SAMPLE_ONLY="${PROFILE_SAMPLE_ONLY:-1}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    PYTHON_BIN="python"
  fi
fi

mkdir -p "$OUT/runs"

{
  echo "ts=$TS"
  echo "git_rev=$(git rev-parse HEAD)"
  echo "git_branch=$(git rev-parse --abbrev-ref HEAD)"
  echo "root=$ROOT"
  echo "keys=$KEYS"
  echo "batchsize=$BATCHSIZE"
  echo "profile=$PROFILE"
  echo "test=$TEST"
  echo "runs=$RUNS"
  echo "valsizes=$VALSIZES"
  echo "workers=$WORKERS"
  echo "force_flags=$FORCE_FLAGS"
  echo "memtable_mode=$MEMTABLE_MODE"
  echo "checkpoint_between_tests=$CHECKPOINT_BETWEEN_TESTS"
  echo "profile_light=$PROFILE_LIGHT"
  echo "profile_sample_only=$PROFILE_SAMPLE_ONLY"
  echo "extra_args=$EXTRA_ARGS"
} >"$OUT/meta.txt"

echo "issue414 force-pointer matrix" >&2
echo "out=$OUT" >&2
echo "profile=$PROFILE test=$TEST keys=$KEYS batchsize=$BATCHSIZE runs=$RUNS" >&2
echo "valsizes=$VALSIZES workers=$WORKERS force_flags=$FORCE_FLAGS" >&2

make unified-bench benchprof >/dev/null

RAW_CSV="$OUT/raw.csv"
echo "force,valsize,workers,run_idx,ops_per_sec,wall_seconds,top_target,top_flat_pct,run_dir" >"$RAW_CSV"

IFS=',' read -r -a valsize_list <<<"$VALSIZES"
IFS=',' read -r -a worker_list <<<"$WORKERS"
IFS=',' read -r -a force_list <<<"$FORCE_FLAGS"

for valsize in "${valsize_list[@]}"; do
  valsize=$(echo "$valsize" | xargs)
  [[ -z "$valsize" ]] && continue

  for workers in "${worker_list[@]}"; do
    workers=$(echo "$workers" | xargs)
    [[ -z "$workers" ]] && continue

    for run_idx in $(seq 1 "$RUNS"); do
      for force in "${force_list[@]}"; do
        force=$(echo "$force" | xargs)
        [[ -z "$force" ]] && continue
        case "$force" in
          true|false) ;;
          *)
            echo "invalid force flag value: '$force' (expected true|false)" >&2
            exit 2
            ;;
        esac

        run_dir="$OUT/runs/f${force}_v${valsize}_w${workers}_r${run_idx}"
        mkdir -p "$run_dir"

        ub_args=(
          -dbs treedb
          -profile "$PROFILE"
          -test "$TEST"
          -keys "$KEYS"
          -valsize "$valsize"
          -batchsize "$BATCHSIZE"
          -write-workers "$workers"
          -treedb-force-value-pointers="$force"
          -treedb-memtable-mode="$MEMTABLE_MODE"
          -progress=false
          -format markdown
        )
        if [[ "$CHECKPOINT_BETWEEN_TESTS" != "0" ]]; then
          ub_args+=(-checkpoint-between-tests)
        fi
        # Keep benchprof-compatible CPU/alloc artifacts while avoiding heavy
        # trace/block/mutex/checkpoint profiling overhead in throughput matrix runs.
        if [[ "$PROFILE_LIGHT" != "0" ]]; then
          ub_args+=(
            -blockprofile=
            -mutexprofile=
            -trace=
            -checkpoint-cpuprofile=
            -blockprofilerate=0
            -mutexprofilefraction=0
          )
        fi
        if [[ -n "$EXTRA_ARGS" ]]; then
          # shellcheck disable=SC2206
          extra_arr=($EXTRA_ARGS)
          ub_args+=("${extra_arr[@]}")
        fi

        profiled_run=1
        if [[ "$PROFILE_SAMPLE_ONLY" != "0" && "$run_idx" != "1" ]]; then
          profiled_run=0
        fi
        if [[ "$profiled_run" = "1" ]]; then
          ub_args+=(-profile-dir "$run_dir")
        fi

        echo "--- force=$force valsize=$valsize workers=$workers run=$run_idx ---" >&2
        env GOMAXPROCS="$workers" /usr/bin/time -p -o "$run_dir/time.txt" \
          ./bin/unified-bench "${ub_args[@]}" 2>&1 | tee "$run_dir/unified_bench.log" >/dev/null

        if [[ "$profiled_run" = "1" ]]; then
          ./bin/benchprof -profiles-dir "$run_dir" >"$run_dir/benchprof.log" 2>&1
          ops=$("$PYTHON_BIN" - "$run_dir/benchprof_results.json" "$TEST" <<'PY'
import json
import sys
path = sys.argv[1]
requested = [t.strip() for t in (sys.argv[2] if len(sys.argv) > 2 else "").split(",") if t.strip()]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
runs = data.get("runs") or []
if not runs:
    print("nan")
    raise SystemExit(0)
results = runs[0].get("results") or {}
if not results:
    print("nan")
    raise SystemExit(0)
if not requested:
    requested = list(results.keys())
test_name = None
for candidate in requested:
    if candidate in results:
        test_name = candidate
        break
if not test_name:
    available = ",".join(sorted(results.keys()))
    raise SystemExit(f"missing requested test in benchprof results; requested={requested} available={available}")
db_vals = results.get(test_name) or {}
v = db_vals.get("TreeDB")
if v is None:
    available = ",".join(sorted(db_vals.keys()))
    raise SystemExit(f"missing TreeDB metric for test={test_name}; labels={available}")
print(v)
PY
)
          top_target=$("$PYTHON_BIN" - "$run_dir/insights.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
targets = data.get("investigation_targets") or []
if not targets:
    print("")
    raise SystemExit(0)
top = targets[0]
print(top.get("function") or top.get("reference") or "")
PY
)
          top_flat_pct=$("$PYTHON_BIN" - "$run_dir/insights.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
targets = data.get("investigation_targets") or []
if not targets:
    print("nan")
    raise SystemExit(0)
v = targets[0].get("flat_pct")
print("nan" if v is None else v)
PY
)
        else
          ops=$("$PYTHON_BIN" - "$run_dir/unified_bench.log" "$TEST" <<'PY'
import re
import sys

path = sys.argv[1]
requested_tests = [t.strip() for t in (sys.argv[2] if len(sys.argv) > 2 else "").split(",") if t.strip()]
lines = open(path, "r", encoding="utf-8").read().splitlines()

def title_test(t: str) -> str:
    return " ".join(w.capitalize() for w in t.split("_"))

# Fast path: progress-style output.
text = "\n".join(lines)
if requested_tests:
    for t in requested_tests:
        m = re.findall(rf"^{re.escape(title_test(t))} / TreeDB = ([0-9,]+)\s*$", text, flags=re.M)
        if m:
            print(m[-1].replace(",", ""))
            raise SystemExit(0)
else:
    m = re.findall(r"/ TreeDB = ([0-9,]+)", text)
    if m:
        print(m[-1].replace(",", ""))
        raise SystemExit(0)

# Fallback: markdown table output.
header = None
for line in lines:
    s = line.strip()
    if s.startswith("|") and "TreeDB" in s:
        cols = [c.strip() for c in s.strip("|").split("|")]
        if "TreeDB" in cols:
            header = cols
            break

if not header:
    print("nan")
    raise SystemExit(0)

try:
    treedb_idx = header.index("TreeDB")
except ValueError:
    print("nan")
    raise SystemExit(0)

requested_names = {title_test(t) for t in requested_tests}
value = None
for line in lines:
    s = line.strip()
    if not s.startswith("|"):
        continue
    cols = [c.strip() for c in s.strip("|").split("|")]
    if not cols or len(cols) <= treedb_idx:
        continue
    if requested_names and cols[0] not in requested_names:
        continue
    if set(cols[0]) == {"-"}:
        continue
    value = cols[treedb_idx]
    break

if value is None:
    print("nan")
else:
    cleaned = re.sub(r"[^\d.]", "", value)
    print(cleaned if cleaned else "nan")
PY
)
          top_target=""
          top_flat_pct="nan"
        fi
        wall=$(awk '/^real / {print $2}' "$run_dir/time.txt")
        if [[ -z "$wall" ]]; then
          wall="nan"
        fi

        esc_target=${top_target//,/;}
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
          "$force" "$valsize" "$workers" "$run_idx" "$ops" "$wall" "$esc_target" "$top_flat_pct" "$run_dir" \
          >>"$RAW_CSV"
      done
    done
  done
done

SUMMARY_MD="$OUT/summary.md"
SUMMARY_CSV="$OUT/summary.csv"

"$PYTHON_BIN" - "$RAW_CSV" "$SUMMARY_CSV" "$SUMMARY_MD" "$OUT/meta.txt" <<'PY'
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

raw_csv = Path(sys.argv[1])
summary_csv = Path(sys.argv[2])
summary_md = Path(sys.argv[3])
meta_path = Path(sys.argv[4])

meta = {}
for line in meta_path.read_text(encoding="utf-8").splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        meta[k.strip()] = v.strip()

rows = list(csv.DictReader(raw_csv.open("r", encoding="utf-8")))
groups = defaultdict(list)
for r in rows:
    groups[(r["force"], r["valsize"], r["workers"])].append(r)

def to_num(v):
    try:
        return float(v)
    except Exception:
        return float("nan")

def pct_cv(values):
    vals = [x for x in values if not math.isnan(x)]
    if len(vals) < 2:
        return float("nan")
    m = statistics.mean(vals)
    if m == 0:
        return float("nan")
    return (statistics.pstdev(vals) / m) * 100.0

summary = []
for key, rs in sorted(groups.items(), key=lambda x: (x[0][1], x[0][2], x[0][0])):
    force, valsize, workers = key
    ops = [to_num(r["ops_per_sec"]) for r in rs]
    wall = [to_num(r["wall_seconds"]) for r in rs]
    ops_valid = [x for x in ops if not math.isnan(x)]
    wall_valid = [x for x in wall if not math.isnan(x)]
    top_targets = [r["top_target"] for r in rs if r["top_target"]]
    top_target = top_targets[0] if top_targets else ""
    top_flat = to_num(rs[0]["top_flat_pct"]) if rs else float("nan")
    summary.append({
        "force": force,
        "valsize": int(valsize),
        "workers": int(workers),
        "runs": len(rs),
        "ops_median": statistics.median(ops_valid) if ops_valid else float("nan"),
        "ops_cv_pct": pct_cv(ops_valid),
        "wall_median": statistics.median(wall_valid) if wall_valid else float("nan"),
        "wall_cv_pct": pct_cv(wall_valid),
        "top_target": top_target,
        "top_flat_pct": top_flat,
    })

out_fields = [
    "force", "valsize", "workers", "runs",
    "ops_median", "ops_cv_pct",
    "wall_median", "wall_cv_pct",
    "top_target", "top_flat_pct",
    "delta_ops_vs_force_false_pct",
    "delta_wall_vs_force_false_pct",
]

by_vw = defaultdict(dict)
for s in summary:
    by_vw[(s["valsize"], s["workers"])][s["force"]] = s

summary_out = []
for s in sorted(summary, key=lambda x: (x["valsize"], x["workers"], x["force"])):
    base = by_vw[(s["valsize"], s["workers"])].get("false")
    dop = float("nan")
    dwall = float("nan")
    if base is not None and not math.isnan(base["ops_median"]) and base["ops_median"] != 0:
        dop = ((s["ops_median"] - base["ops_median"]) / base["ops_median"]) * 100.0
    if base is not None and not math.isnan(base["wall_median"]) and base["wall_median"] != 0:
        dwall = ((s["wall_median"] - base["wall_median"]) / base["wall_median"]) * 100.0
    row = dict(s)
    row["delta_ops_vs_force_false_pct"] = dop
    row["delta_wall_vs_force_false_pct"] = dwall
    summary_out.append(row)

with summary_csv.open("w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=out_fields)
    w.writeheader()
    for row in summary_out:
        w.writerow(row)

def fmt_num(v, d=2):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    return f"{v:.{d}f}"

def fmt_ops(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    return f"{int(round(v)):,}"

lines = []
lines.append("# issue414 forced-pointer matrix")
lines.append("")
lines.append(f"- git: `{meta.get('git_rev', '-')}` (`{meta.get('git_branch', '-')}`)")
lines.append(f"- test/profile: `{meta.get('test', '-')}` / `{meta.get('profile', '-')}`")
lines.append(f"- keys/batchsize: `{meta.get('keys', '-')}` / `{meta.get('batchsize', '-')}`")
lines.append(f"- runs per cell: `{meta.get('runs', '-')}`")
lines.append(f"- memtable mode: `{meta.get('memtable_mode', '-')}`")
lines.append(f"- profile light mode: `{meta.get('profile_light', '-')}`")
lines.append(f"- profile sample only: `{meta.get('profile_sample_only', '-')}`")
lines.append("")

header = [
    "valsize", "workers", "force",
    "ops/s median", "ops cv%",
    "wall(s) median", "wall cv%",
    "ops Δ vs false%", "wall Δ vs false%",
    "top target (run1)",
]
lines.append("| " + " | ".join(header) + " |")
lines.append("|" + "|".join(["---"] * len(header)) + "|")
for row in summary_out:
    lines.append("| " + " | ".join([
        str(row["valsize"]),
        str(row["workers"]),
        row["force"],
        fmt_ops(row["ops_median"]),
        fmt_num(row["ops_cv_pct"]),
        fmt_num(row["wall_median"], 3),
        fmt_num(row["wall_cv_pct"]),
        fmt_num(row["delta_ops_vs_force_false_pct"]),
        fmt_num(row["delta_wall_vs_force_false_pct"]),
        (row["top_target"] or "-").replace("|", "/"),
    ]) + " |")

lines.append("")
lines.append("## Artifacts")
lines.append("")
lines.append(f"- raw csv: `{raw_csv}`")
lines.append(f"- summary csv: `{summary_csv}`")
lines.append("- per-run dirs: `runs/f<force>_v<valsize>_w<workers>_r<run>/`")
lines.append("  each contains unified-bench output and timing; profiled runs include benchprof outputs and pprof files.")

summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

echo "" >&2
echo "done:" >&2
echo "  out:      $OUT" >&2
echo "  summary:  $SUMMARY_MD" >&2
echo "  summary+: $SUMMARY_CSV" >&2
