#!/usr/bin/env bash
set -euo pipefail

# PR8 vs main regression gate.
#
# Method:
# - Build fixed binaries for main and current branch (default: PR8).
# - For each test, run N times with a sleep between runs.
# - Compute a trimmed mean by keeping the middle K values (default: N=5, K=3).
# - Print a compact table and write a full log under artifacts/bench/.
#
# Notes:
# - Uses only flags supported by both branches.
# - Keeps timing focused on the benchmark loops (unified_bench already excludes DB open/close).

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/artifacts/bench}"
mkdir -p "$OUT_DIR"
TS=$(date +%Y%m%d%H%M%S)
LOG="$OUT_DIR/compare_pr8_vs_main_trimmed_$TS.log"

RUNS="${RUNS:-5}"
KEEP="${KEEP:-3}" # keep middle KEEP values after sorting
SLEEP_S="${SLEEP_S:-5}"
WARMUP="${WARMUP:-1}" # warm-up runs per test (discarded; helps reduce first-run noise)

KEYS="${KEYS:-1000000}"
VALSIZE="${VALSIZE:-1024}"
BATCHSIZE="${BATCHSIZE:-1000}"
MIN_THROUGHPUT_RATIO="${MIN_THROUGHPUT_RATIO:-1.01}"

MAIN_WT="${MAIN_WT:-$ROOT/.worktrees/main_plain}"
BIN_MAIN="${BIN_MAIN:-/tmp/ubench-main-plain}"
BIN_CUR="${BIN_CUR:-/tmp/ubench-cur}"

echo "compare pr8 vs main (trimmed mean)" | tee "$LOG"
echo "ts=$TS runs=$RUNS keep=$KEEP warmup=$WARMUP sleep=${SLEEP_S}s keys=$KEYS valsize=$VALSIZE batchsize=$BATCHSIZE min_throughput_ratio=$MIN_THROUGHPUT_RATIO" | tee -a "$LOG"
echo "root=$ROOT" | tee -a "$LOG"
echo "main_wt=$MAIN_WT" | tee -a "$LOG"
echo "bin_main=$BIN_MAIN bin_cur=$BIN_CUR" | tee -a "$LOG"

if (( RUNS < 3 )); then
  echo "RUNS must be >= 3" | tee -a "$LOG"
  exit 2
fi
if (( KEEP <= 0 || KEEP > RUNS )); then
  echo "KEEP must be in [1..RUNS]" | tee -a "$LOG"
  exit 2
fi
if (( (RUNS - KEEP) % 2 != 0 )); then
  echo "KEEP must have same parity as RUNS (so we can drop equal min/max)" | tee -a "$LOG"
  exit 2
fi

ensure_main_worktree() {
  if [[ -d "$MAIN_WT/.git" ]]; then
    return 0
  fi
  mkdir -p "$(dirname "$MAIN_WT")"
  if [[ -e "$MAIN_WT" ]]; then
    git worktree remove --force "$MAIN_WT" >>"$LOG" 2>&1 || true
  fi
  git worktree add -f "$MAIN_WT" main >>"$LOG" 2>&1
}

build_bins() {
  echo "--- building binaries ---" | tee -a "$LOG"
  ensure_main_worktree
  (cd "$MAIN_WT" && go build -o "$BIN_MAIN" ./cmd/unified_bench) >>"$LOG" 2>&1
  (cd "$ROOT" && go build -o "$BIN_CUR" ./cmd/unified_bench) >>"$LOG" 2>&1
}

run_one() {
  local bin="$1"
  local test="$2"
  shift 2
  "$bin" -dbs treedb -test "$test" -keys "$KEYS" -valsize "$VALSIZE" -batchsize "$BATCHSIZE" "$@"
}

collect_runs() {
  local bin="$1"
  local test="$2"
  python - "$bin" "$test" "$RUNS" "$KEEP" "$SLEEP_S" "$WARMUP" "$LOG" <<'PY'
import os, re, statistics, subprocess, sys, time

binpath, test, runs_s, keep_s, sleep_s, warmup_s, log_path = sys.argv[1:8]
runs = int(runs_s)
keep = int(keep_s)
sleep_sec = float(sleep_s)
warmup = int(warmup_s)

val_re = re.compile(rf"{re.escape(test.replace('_',' ').title())} / TreeDB = ([0-9,]+)")
fallback_re = re.compile(r"TreeDB = ([0-9,]+)")

vals = []

def run_once():
    return subprocess.check_output(
        [
            binpath,
            "-dbs",
            "treedb",
            "-test",
            test,
            "-keys",
            str(int(os.environ.get("KEYS", "1000000"))),
            "-valsize",
            str(int(os.environ.get("VALSIZE", "1024"))),
            "-batchsize",
            str(int(os.environ.get("BATCHSIZE", "1000"))),
            "-progress=false",
        ],
        text=True,
        stderr=subprocess.STDOUT,
    )

for _ in range(max(0, warmup)):
    time.sleep(sleep_sec)
    _ = run_once()

for i in range(runs):
    time.sleep(sleep_sec)
    out = run_once()
    m = re.search(rf"{test.replace('_',' ').title()} / TreeDB = ([0-9,]+)", out)
    if not m:
        m = fallback_re.search(out)
    if not m:
        raise SystemExit(f"missing ops/sec in output for {test}")
    vals.append(int(m.group(1).replace(",", "")))

sorted_vals = sorted(vals)
drop_each = (runs - keep) // 2
kept = sorted_vals[drop_each: runs - drop_each]
avg = statistics.mean(kept)

print(",".join(str(v) for v in vals))
print(f"{avg:.3f}")
PY
}

build_bins

tests=("batch_write" "random_write" "random_read" "prefix_scan")

echo "" | tee -a "$LOG"
echo "--- running trimmed comparisons ---" | tee -a "$LOG"

printf "| test | main avg (trimmed) | cur avg (trimmed) | cur/main | delta | gate |\n" | tee -a "$LOG"
printf "|---|---:|---:|---:|---:|---|\n" | tee -a "$LOG"

failures=0

for test in "${tests[@]}"; do
  echo "" | tee -a "$LOG"
  echo "== $test ==" | tee -a "$LOG"

  export KEYS VALSIZE BATCHSIZE

  main_out=$(collect_runs "$BIN_MAIN" "$test")
  cur_out=$(collect_runs "$BIN_CUR" "$test")

  main_vals=$(echo "$main_out" | sed -n '1p')
  main_avg=$(echo "$main_out" | sed -n '2p')
  cur_vals=$(echo "$cur_out" | sed -n '1p')
  cur_avg=$(echo "$cur_out" | sed -n '2p')

  decision=$(python - "$main_avg" "$cur_avg" "$MIN_THROUGHPUT_RATIO" <<'PY'
import sys
base = float(sys.argv[1])
cur = float(sys.argv[2])
minimum = float(sys.argv[3])
if base <= 0:
    ratio = float("nan")
    delta = float("nan")
    passed = False
else:
    ratio = cur / base
    delta = (ratio - 1.0) * 100.0
    passed = ratio > minimum
print("nan" if ratio != ratio else f"{ratio:.4f}x")
print("nan" if delta != delta else f"{delta:+.2f}%")
print("PASS" if passed else "FAIL")
PY
)
  ratio=$(echo "$decision" | sed -n '1p')
  delta=$(echo "$decision" | sed -n '2p')
  gate=$(echo "$decision" | sed -n '3p')
  if [[ "$gate" != "PASS" ]]; then
    failures=$((failures + 1))
  fi

  echo "main runs: $main_vals" | tee -a "$LOG"
  echo "cur  runs: $cur_vals" | tee -a "$LOG"
  echo "main trimmed avg: $main_avg" | tee -a "$LOG"
  echo "cur  trimmed avg: $cur_avg" | tee -a "$LOG"

  printf "| %s | %'.0f | %'.0f | %s | %s | %s |\n" "$test" "$(printf "%.0f" "$main_avg")" "$(printf "%.0f" "$cur_avg")" "$ratio" "$delta" "$gate" | tee -a "$LOG"
done

echo "" | tee -a "$LOG"
echo "log: $LOG" | tee -a "$LOG"
if (( failures != 0 )); then
  echo "gate failed: $failures throughput metric(s) were <= ${MIN_THROUGHPUT_RATIO}x" | tee -a "$LOG"
  exit 3
fi
echo "gate: PASS" | tee -a "$LOG"
