#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:-origin/main}"
CANDIDATE_UNIFIED_BIN="${CANDIDATE_UNIFIED_BIN:-}"
CANDIDATE_TREEMAP_BIN="${CANDIDATE_TREEMAP_BIN:-}"
BASELINE_UNIFIED_BIN="${BASELINE_UNIFIED_BIN:-}"
BASELINE_TREEMAP_BIN="${BASELINE_TREEMAP_BIN:-}"
SCRIPT_GOWORK="${SCRIPT_GOWORK:-off}"

MAX_PAIRS="${MAX_PAIRS:-6}"
MIN_PAIRS="${MIN_PAIRS:-3}"
CLEAR_WIN_PAIRS="${CLEAR_WIN_PAIRS:-2}"
CLEAR_LOSS_PAIRS="${CLEAR_LOSS_PAIRS:-2}"
STOP_ON_CLEAR="${STOP_ON_CLEAR:-1}"
LOW_SIGNAL_MIN_PAIRS="${LOW_SIGNAL_MIN_PAIRS:-3}"
LOW_SIGNAL_NEUTRAL_STREAK="${LOW_SIGNAL_NEUTRAL_STREAK:-3}"
SLEEP_BETWEEN_RUNS_SECONDS="${SLEEP_BETWEEN_RUNS_SECONDS:-2}"

SIZE_FIELD="${SIZE_FIELD:-s_post_app_bytes}"
SIZE_TOLERANCE_BYTES="${SIZE_TOLERANCE_BYTES:-67108864}"
TIME_TOLERANCE_SECONDS="${TIME_TOLERANCE_SECONDS:-30}"

PROFILE="${PROFILE:-fast}"
DBS="${DBS:-treedb}"
TESTS="${TESTS:-batch_write}"
KEYS="${KEYS:-500000}"
VALSIZE="${VALSIZE:-128}"
BATCHSIZE="${BATCHSIZE:-8000}"
VAL_PATTERN="${VAL_PATTERN:-celestia_height_prefix_fill}"
SEED="${SEED:-1}"

FORCE_VALUE_POINTERS="${FORCE_VALUE_POINTERS:-true}"
OUTER_LEAVES_IN_VLOG="${OUTER_LEAVES_IN_VLOG:-true}"
VLOG_COMPRESSION="${VLOG_COMPRESSION:-dict}"
VLOG_COMPRESSION_AUTOTUNE="${VLOG_COMPRESSION_AUTOTUNE:-aggressive}"
VLOG_COMPRESSION_VARIANT="${VLOG_COMPRESSION_VARIANT:-dict}"
DICT_TRAIN_BYTES="${DICT_TRAIN_BYTES:-1048576}"
DICT_BYTES="${DICT_BYTES:-32768}"
VLOG_REWRITE_MIN_SEGMENT_AGE_MS="${VLOG_REWRITE_MIN_SEGMENT_AGE_MS:-}"

REWRITE_ENABLED="${REWRITE_ENABLED:-1}"
REWRITE_ARGS="${REWRITE_ARGS:--rw}"
MEASURE_GZIP="${MEASURE_GZIP:-1}"
KEEP_DB_DIRS="${KEEP_DB_DIRS:-1}"

COMMON_EXTRA_FLAGS="${COMMON_EXTRA_FLAGS:-}"
CONTROL_EXTRA_FLAGS="${CONTROL_EXTRA_FLAGS:-}"
CANDIDATE_EXTRA_FLAGS="${CANDIDATE_EXTRA_FLAGS:-}"

TS="$(date +%Y%m%d%H%M%S)"
OUT="${OUT_DIR:-$ROOT/artifacts/celestia_fast_gate/$TS}"

WORKTREE_PATH=""

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

du_bytes() {
  local target="$1"
  if [[ ! -e "$target" ]]; then
    echo 0
    return 0
  fi
  if du -sb "$target" >/dev/null 2>&1; then
    du -sb "$target" 2>/dev/null | awk '{print $1}'
    return 0
  fi
  du -sk "$target" 2>/dev/null | awk '{print $1 * 1024}'
}

gzip_dir_bytes() {
  local target="$1"
  if [[ "$MEASURE_GZIP" != "1" ]]; then
    echo 0
    return 0
  fi
  if [[ ! -d "$target" ]]; then
    echo 0
    return 0
  fi
  tar -C "$target" -cf - . 2>/dev/null | gzip -1 -c | wc -c | tr -d '[:space:]'
}

cleanup() {
  if [[ -n "$WORKTREE_PATH" && -d "$WORKTREE_PATH" ]]; then
    git worktree remove --force "$WORKTREE_PATH" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

parse_bench_log() {
  local log_path="$1"
  python3 - "$log_path" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace")
lines = text.splitlines()

throughput = None
for line in lines:
    m = re.search(r"Batch Write\s*/\s*TreeDB[^=]*=\s*([0-9][0-9,]*(?:\.[0-9]+)?)", line)
    if m:
        throughput = float(m.group(1).replace(",", ""))
        break
if throughput is None:
    for line in lines:
        m = re.match(r"\s*Batch Write\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", line)
        if m:
            throughput = float(m.group(1).replace(",", ""))
            break

keep_dir = ""
in_keep_block = False
for line in lines:
    stripped = line.strip()
    if stripped == "Kept Data Directories":
        in_keep_block = True
        continue
    if in_keep_block:
        if not stripped:
            continue
        if stripped.startswith("TreeDB (") and ":" in stripped:
            maybe = stripped.split(":", 1)[1].strip()
            if maybe.startswith("/"):
                keep_dir = maybe
                break

if not keep_dir:
    m = re.search(r"TreeDB \([^\)]*\):\s+(/tmp/bench[^\s]+)", text)
    if m:
        keep_dir = m.group(1)

if not keep_dir:
    raise SystemExit("unable to locate kept data directory in unified-bench output")

if throughput is None:
    throughput = 0.0

print(f"{keep_dir}\t{throughput}")
PY
}

setup_bins() {
  mkdir -p "$OUT/bin" "$OUT/worktrees" "$OUT/runs"

  if [[ -z "$CANDIDATE_UNIFIED_BIN" ]]; then
    CANDIDATE_UNIFIED_BIN="$OUT/bin/unified-bench-candidate"
    GOWORK="$SCRIPT_GOWORK" go build -o "$CANDIDATE_UNIFIED_BIN" ./cmd/unified_bench
  fi
  if [[ ! -x "$CANDIDATE_UNIFIED_BIN" ]]; then
    echo "candidate unified-bench binary not executable: $CANDIDATE_UNIFIED_BIN" >&2
    exit 2
  fi

  if [[ -z "$CANDIDATE_TREEMAP_BIN" ]]; then
    CANDIDATE_TREEMAP_BIN="$OUT/bin/treemap-candidate"
    GOWORK="$SCRIPT_GOWORK" go build -o "$CANDIDATE_TREEMAP_BIN" ./TreeDB/cmd/treemap
  fi
  if [[ ! -x "$CANDIDATE_TREEMAP_BIN" ]]; then
    echo "candidate treemap binary not executable: $CANDIDATE_TREEMAP_BIN" >&2
    exit 2
  fi

  if [[ -n "$BASELINE_UNIFIED_BIN" && -n "$BASELINE_TREEMAP_BIN" ]]; then
    if [[ ! -x "$BASELINE_UNIFIED_BIN" ]]; then
      echo "baseline unified-bench binary not executable: $BASELINE_UNIFIED_BIN" >&2
      exit 2
    fi
    if [[ ! -x "$BASELINE_TREEMAP_BIN" ]]; then
      echo "baseline treemap binary not executable: $BASELINE_TREEMAP_BIN" >&2
      exit 2
    fi
    return 0
  fi

  if ! git cat-file -e "${BASELINE_HASH}^{commit}" >/dev/null 2>&1; then
    fetch_refspec="$BASELINE_HASH"
    if [[ "$BASELINE_HASH" == origin/* ]]; then
      remote_branch="${BASELINE_HASH#origin/}"
      fetch_refspec="+refs/heads/${remote_branch}:refs/remotes/origin/${remote_branch}"
    fi
    git fetch --no-tags --depth=1 origin "$fetch_refspec" >/dev/null 2>&1 || git fetch --no-tags origin "$fetch_refspec" >/dev/null 2>&1
  fi

  WORKTREE_PATH="$OUT/worktrees/baseline"
  git worktree add --detach "$WORKTREE_PATH" "$BASELINE_HASH" >/dev/null

  if [[ -z "$BASELINE_UNIFIED_BIN" ]]; then
    BASELINE_UNIFIED_BIN="$OUT/bin/unified-bench-baseline"
    (
      cd "$WORKTREE_PATH"
      GOWORK="$SCRIPT_GOWORK" go build -o "$BASELINE_UNIFIED_BIN" ./cmd/unified_bench
    )
  fi
  if [[ -z "$BASELINE_TREEMAP_BIN" ]]; then
    BASELINE_TREEMAP_BIN="$OUT/bin/treemap-baseline"
    (
      cd "$WORKTREE_PATH"
      GOWORK="$SCRIPT_GOWORK" go build -o "$BASELINE_TREEMAP_BIN" ./TreeDB/cmd/treemap
    )
  fi

  if [[ ! -x "$BASELINE_UNIFIED_BIN" ]]; then
    echo "baseline unified-bench binary not executable: $BASELINE_UNIFIED_BIN" >&2
    exit 2
  fi
  if [[ ! -x "$BASELINE_TREEMAP_BIN" ]]; then
    echo "baseline treemap binary not executable: $BASELINE_TREEMAP_BIN" >&2
    exit 2
  fi
}

run_variant() {
  local pair_index="$1"
  local variant="$2"

  local bench_bin treemap_bin extra_flags
  if [[ "$variant" == "candidate" ]]; then
    bench_bin="$CANDIDATE_UNIFIED_BIN"
    treemap_bin="$CANDIDATE_TREEMAP_BIN"
    extra_flags="$CANDIDATE_EXTRA_FLAGS"
  else
    bench_bin="$BASELINE_UNIFIED_BIN"
    treemap_bin="$BASELINE_TREEMAP_BIN"
    extra_flags="$CONTROL_EXTRA_FLAGS"
  fi

  local run_id
  run_id=$(printf "%02d_%s" "$pair_index" "$variant")
  local run_dir="$OUT/runs/$run_id"
  mkdir -p "$run_dir"

  local cmd=(
    "$bench_bin"
    -profile "$PROFILE"
    -dbs "$DBS"
    -keys "$KEYS"
    -valsize "$VALSIZE"
    -batchsize "$BATCHSIZE"
    -test "$TESTS"
    -val-pattern "$VAL_PATTERN"
    -seed "$SEED"
    -progress=false
    -keep
    -treedb-force-value-pointers="$FORCE_VALUE_POINTERS"
    -treedb-index-outer-leaves-in-vlog="$OUTER_LEAVES_IN_VLOG"
    -treedb-vlog-compression "$VLOG_COMPRESSION"
    -treedb-vlog-compression-autotune "$VLOG_COMPRESSION_AUTOTUNE"
    -treedb-vlog-compression-variant "$VLOG_COMPRESSION_VARIANT"
    -treedb-vlog-dict-train-bytes "$DICT_TRAIN_BYTES"
    -treedb-vlog-dict-dict-bytes "$DICT_BYTES"
  )
  if [[ -n "$VLOG_REWRITE_MIN_SEGMENT_AGE_MS" ]]; then
    cmd+=(-treedb-vlog-rewrite-min-segment-age-ms "$VLOG_REWRITE_MIN_SEGMENT_AGE_MS")
  fi

  if [[ -n "$COMMON_EXTRA_FLAGS" ]]; then
    # shellcheck disable=SC2206
    local common_extra=( $COMMON_EXTRA_FLAGS )
    cmd+=("${common_extra[@]}")
  fi
  if [[ -n "$extra_flags" ]]; then
    # shellcheck disable=SC2206
    local variant_extra=( $extra_flags )
    cmd+=("${variant_extra[@]}")
  fi

  printf '%q ' "${cmd[@]}" >"$run_dir/cmd.txt"
  echo >>"$run_dir/cmd.txt"

  local bench_log="$run_dir/unified.log"
  local run_start run_end
  run_start=$(date +%s)
  "${cmd[@]}" >"$bench_log" 2>&1
  run_end=$(date +%s)

  local parse_out keep_dir batch_write_ops
  parse_out="$(parse_bench_log "$bench_log")"
  keep_dir="${parse_out%%$'\t'*}"
  batch_write_ops="${parse_out#*$'\t'}"

  if [[ -z "$keep_dir" || ! -d "$keep_dir" ]]; then
    echo "missing kept dir for $run_id (parsed=$keep_dir)" >&2
    exit 1
  fi

  local sync_app_bytes sync_wal_bytes sync_gzip_bytes
  sync_app_bytes="$(du_bytes "$keep_dir")"
  sync_wal_bytes="$(du_bytes "$keep_dir/maindb/wal")"
  sync_gzip_bytes="$(gzip_dir_bytes "$keep_dir")"

  local rewrite_attempted=0
  local rewrite_seconds=0
  local rewrite_rc=0
  local rewrite_log="$run_dir/rewrite.log"
  if [[ "$REWRITE_ENABLED" == "1" ]]; then
    rewrite_attempted=1
    local rw_start rw_end
    rw_start=$(date +%s)
    # shellcheck disable=SC2206
    local rw_args=( $REWRITE_ARGS )
    set +e
    "$treemap_bin" vlog-rewrite "$keep_dir" "${rw_args[@]}" >"$rewrite_log" 2>&1
    rewrite_rc=$?
    set -e
    rw_end=$(date +%s)
    rewrite_seconds=$((rw_end - rw_start))
  fi

  local post_app_bytes post_wal_bytes post_gzip_bytes
  post_app_bytes="$(du_bytes "$keep_dir")"
  post_wal_bytes="$(du_bytes "$keep_dir/maindb/wal")"
  post_gzip_bytes="$(gzip_dir_bytes "$keep_dir")"

  local run_json="$run_dir/run.json"
  python3 - "$run_json" "$pair_index" "$variant" "$run_start" "$run_end" "$rewrite_attempted" "$rewrite_seconds" "$rewrite_rc" "$batch_write_ops" "$keep_dir" "$sync_app_bytes" "$sync_wal_bytes" "$sync_gzip_bytes" "$post_app_bytes" "$post_wal_bytes" "$post_gzip_bytes" <<'PY'
import json
import sys
from pathlib import Path

out_path = Path(sys.argv[1])
pair_index = int(sys.argv[2])
variant = sys.argv[3]
run_start = int(sys.argv[4])
run_end = int(sys.argv[5])
rewrite_attempted = int(sys.argv[6])
rewrite_seconds = int(sys.argv[7])
rewrite_rc = int(sys.argv[8])
batch_write_ops = float(sys.argv[9])
keep_dir = sys.argv[10]
s_sync_app = int(sys.argv[11])
s_sync_wal = int(sys.argv[12])
s_sync_gzip = int(sys.argv[13])
s_post_app = int(sys.argv[14])
s_post_wal = int(sys.argv[15])
s_post_gzip = int(sys.argv[16])

t_sync = max(0, run_end - run_start)
t_rewrite = rewrite_seconds if rewrite_attempted == 1 else 0
if rewrite_attempted == 1 and rewrite_rc != 0:
    t_total = None
else:
    t_total = t_sync + t_rewrite

payload = {
    "pair_index": pair_index,
    "variant": variant,
    "keep_dir": keep_dir,
    "bench": {
        "duration_seconds": t_sync,
        "batch_write_ops_per_sec": batch_write_ops,
    },
    "rewrite": {
        "attempted": rewrite_attempted == 1,
        "seconds": t_rewrite,
        "exit_code": rewrite_rc,
    },
    "sizes": {
        "sync_app_bytes": s_sync_app,
        "sync_wal_bytes": s_sync_wal,
        "sync_gzip_bytes": s_sync_gzip,
        "post_app_bytes": s_post_app,
        "post_wal_bytes": s_post_wal,
        "post_gzip_bytes": s_post_gzip,
    },
    "metrics": {
        "t_sync_seconds": t_sync,
        "t_rewrite_seconds": t_rewrite,
        "t_total_seconds": t_total,
        "batch_write_ops_per_sec": batch_write_ops,
        "s_sync_app_bytes": s_sync_app,
        "s_sync_wal_bytes": s_sync_wal,
        "s_sync_gzip_bytes": s_sync_gzip,
        "s_post_app_bytes": s_post_app,
        "s_post_wal_bytes": s_post_wal,
        "s_post_gzip_bytes": s_post_gzip,
    },
}
out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
print(out_path)
PY

  if [[ "$KEEP_DB_DIRS" != "1" ]]; then
    rm -rf "$keep_dir"
  fi

  echo "run_id=$run_id keep_dir=$keep_dir json=$run_json"
}

aggregate_and_decide() {
  local decision_json="$OUT/decision.json"
  python3 - "$OUT" "$SIZE_FIELD" "$SIZE_TOLERANCE_BYTES" "$TIME_TOLERANCE_SECONDS" "$MIN_PAIRS" "$CLEAR_WIN_PAIRS" "$CLEAR_LOSS_PAIRS" "$MAX_PAIRS" "$STOP_ON_CLEAR" "$LOW_SIGNAL_MIN_PAIRS" "$LOW_SIGNAL_NEUTRAL_STREAK" "$decision_json" <<'PY'
import csv
import json
import statistics
import sys
from pathlib import Path

out = Path(sys.argv[1])
size_field = sys.argv[2]
size_tol = int(sys.argv[3])
time_tol = int(sys.argv[4])
min_pairs = int(sys.argv[5])
clear_win_pairs = int(sys.argv[6])
clear_loss_pairs = int(sys.argv[7])
max_pairs = int(sys.argv[8])
stop_on_clear = sys.argv[9] == "1"
low_signal_min_pairs = int(sys.argv[10])
low_signal_neutral_streak = int(sys.argv[11])
decision_path = Path(sys.argv[12])

run_files = sorted(out.glob("runs/*/run.json"))
runs = []
for p in run_files:
    try:
        runs.append(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        continue
runs.sort(key=lambda r: (int(r.get("pair_index", 0)), str(r.get("variant", ""))))

runs_csv = out / "runs.csv"
with runs_csv.open("w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow([
        "pair_index",
        "variant",
        "keep_dir",
        "t_sync_seconds",
        "t_rewrite_seconds",
        "t_total_seconds",
        "batch_write_ops_per_sec",
        "s_sync_app_bytes",
        "s_sync_wal_bytes",
        "s_sync_gzip_bytes",
        "s_post_app_bytes",
        "s_post_wal_bytes",
        "s_post_gzip_bytes",
        "rewrite_exit_code",
    ])
    for r in runs:
        m = r.get("metrics", {}) or {}
        rw = r.get("rewrite", {}) or {}
        w.writerow([
            int(r.get("pair_index", 0)),
            str(r.get("variant", "")),
            str(r.get("keep_dir", "")),
            m.get("t_sync_seconds"),
            m.get("t_rewrite_seconds"),
            m.get("t_total_seconds"),
            m.get("batch_write_ops_per_sec"),
            m.get("s_sync_app_bytes"),
            m.get("s_sync_wal_bytes"),
            m.get("s_sync_gzip_bytes"),
            m.get("s_post_app_bytes"),
            m.get("s_post_wal_bytes"),
            m.get("s_post_gzip_bytes"),
            rw.get("exit_code"),
        ])

by_pair = {}
for r in runs:
    pair = int(r.get("pair_index", 0))
    by_pair.setdefault(pair, {})[str(r.get("variant", ""))] = r

def delta(a, b):
    if a is None or b is None:
        return None
    try:
        return a - b
    except Exception:
        return None

pair_rows = []
wins = 0
losses = 0
raw_pairs = 0
invalid_pairs = 0
for pair in sorted(by_pair):
    row = by_pair[pair]
    ctrl = row.get("control")
    cand = row.get("candidate")
    if not ctrl or not cand:
        continue
    raw_pairs += 1

    cm = cand.get("metrics", {}) or {}
    bm = ctrl.get("metrics", {}) or {}

    d_sync = delta(cm.get("t_sync_seconds"), bm.get("t_sync_seconds"))
    d_total = delta(cm.get("t_total_seconds"), bm.get("t_total_seconds"))
    d_bw = delta(cm.get("batch_write_ops_per_sec"), bm.get("batch_write_ops_per_sec"))

    d_sync_app = delta(cm.get("s_sync_app_bytes"), bm.get("s_sync_app_bytes"))
    d_sync_wal = delta(cm.get("s_sync_wal_bytes"), bm.get("s_sync_wal_bytes"))
    d_sync_gzip = delta(cm.get("s_sync_gzip_bytes"), bm.get("s_sync_gzip_bytes"))
    d_post_app = delta(cm.get("s_post_app_bytes"), bm.get("s_post_app_bytes"))
    d_post_wal = delta(cm.get("s_post_wal_bytes"), bm.get("s_post_wal_bytes"))
    d_post_gzip = delta(cm.get("s_post_gzip_bytes"), bm.get("s_post_gzip_bytes"))

    d_size_primary = delta(cm.get(size_field), bm.get(size_field))
    cand_rewrite = cand.get("rewrite", {}) or {}
    ctrl_rewrite = ctrl.get("rewrite", {}) or {}
    cand_rewrite_failed = bool(cand_rewrite.get("attempted")) and int(cand_rewrite.get("exit_code", 0) or 0) != 0
    ctrl_rewrite_failed = bool(ctrl_rewrite.get("attempted")) and int(ctrl_rewrite.get("exit_code", 0) or 0) != 0

    outcome = "neutral"
    if cand_rewrite_failed and ctrl_rewrite_failed:
        outcome = "invalid"
        invalid_pairs += 1
    elif cand_rewrite_failed and not ctrl_rewrite_failed:
        outcome = "loss"
        losses += 1
    elif ctrl_rewrite_failed and not cand_rewrite_failed:
        # Baseline rewrite failures make the pair non-comparable; classify as a
        # conservative loss so fast-gate logic does not treat it as neutral.
        outcome = "loss"
        losses += 1
    elif d_size_primary is not None and d_total is not None:
        win = (d_size_primary <= -size_tol) and (d_total <= time_tol)
        loss = (d_size_primary >= size_tol) and (d_total >= -time_tol)
        if win and not loss:
            outcome = "win"
            wins += 1
        elif loss and not win:
            outcome = "loss"
            losses += 1

    pair_rows.append(
        {
            "pair_index": pair,
            "delta_t_sync_seconds": d_sync,
            "delta_t_total_seconds": d_total,
            "delta_batch_write_ops_per_sec": d_bw,
            "delta_s_sync_app_bytes": d_sync_app,
            "delta_s_sync_wal_bytes": d_sync_wal,
            "delta_s_sync_gzip_bytes": d_sync_gzip,
            "delta_s_post_app_bytes": d_post_app,
            "delta_s_post_wal_bytes": d_post_wal,
            "delta_s_post_gzip_bytes": d_post_gzip,
            "delta_size_primary_bytes": d_size_primary,
            "outcome": outcome,
        }
    )

pairs_csv = out / "pairs.csv"
with pairs_csv.open("w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(
        [
            "pair_index",
            "delta_t_sync_seconds",
            "delta_t_total_seconds",
            "delta_batch_write_ops_per_sec",
            "delta_s_sync_app_bytes",
            "delta_s_sync_wal_bytes",
            "delta_s_sync_gzip_bytes",
            "delta_s_post_app_bytes",
            "delta_s_post_wal_bytes",
            "delta_s_post_gzip_bytes",
            "delta_size_primary_bytes",
            "outcome",
        ]
    )
    for r in pair_rows:
        w.writerow(
            [
                r["pair_index"],
                r["delta_t_sync_seconds"],
                r["delta_t_total_seconds"],
                r["delta_batch_write_ops_per_sec"],
                r["delta_s_sync_app_bytes"],
                r["delta_s_sync_wal_bytes"],
                r["delta_s_sync_gzip_bytes"],
                r["delta_s_post_app_bytes"],
                r["delta_s_post_wal_bytes"],
                r["delta_s_post_gzip_bytes"],
                r["delta_size_primary_bytes"],
                r["outcome"],
            ]
        )

scored_rows = [row for row in pair_rows if row.get("outcome") != "invalid"]
completed_pairs = len(scored_rows)
neutral = max(0, completed_pairs - wins - losses)
neutral_streak = 0
for row in reversed(scored_rows):
    if row.get("outcome") == "neutral":
        neutral_streak += 1
        continue
    break

reason = "continue"
stop = False
if stop_on_clear and completed_pairs >= min_pairs:
    if wins >= clear_win_pairs and wins > losses:
        stop = True
        reason = "clear_improvement"
    elif losses >= clear_loss_pairs and losses > wins:
        stop = True
        reason = "clear_regression"
    else:
        remaining = max(0, max_pairs - raw_pairs)
        can_reach_clear_win = (wins + remaining) >= clear_win_pairs
        can_reach_clear_loss = (losses + remaining) >= clear_loss_pairs
        if not can_reach_clear_win and not can_reach_clear_loss:
            stop = True
            reason = "futile_remaining_pairs"

if (not stop) and completed_pairs >= low_signal_min_pairs and neutral_streak >= low_signal_neutral_streak:
    stop = True
    reason = "low_signal_neutral_streak"

if (not stop) and raw_pairs >= max_pairs:
    stop = True
    reason = "max_pairs"

med_delta_size = None
med_delta_total = None
size_values = [r["delta_size_primary_bytes"] for r in pair_rows if r.get("delta_size_primary_bytes") is not None]
time_values = [r["delta_t_total_seconds"] for r in pair_rows if r.get("delta_t_total_seconds") is not None]
if size_values:
    med_delta_size = statistics.median(size_values)
if time_values:
    med_delta_total = statistics.median(time_values)

summary_md = out / "summary.md"
lines = []
lines.append("# celestia_fast_gate summary")
lines.append("")
lines.append(f"- observed pairs: `{raw_pairs}`")
lines.append(f"- scored pairs: `{completed_pairs}`")
lines.append(f"- invalid pairs skipped: `{invalid_pairs}`")
lines.append(f"- wins/losses/neutral: `{wins}` / `{losses}` / `{neutral}`")
lines.append(f"- neutral streak (tail): `{neutral_streak}`")
lines.append(f"- size field: `{size_field}`")
lines.append(f"- size tolerance bytes: `{size_tol}`")
lines.append(f"- time tolerance seconds: `{time_tol}`")
lines.append(f"- low-signal min pairs: `{low_signal_min_pairs}`")
lines.append(f"- low-signal neutral streak: `{low_signal_neutral_streak}`")
lines.append(f"- median delta(size): `{med_delta_size}`")
lines.append(f"- median delta(time_total): `{med_delta_total}`")
lines.append(f"- decision: `{reason}`")
lines.append("")
lines.append("## Artifacts")
lines.append("")
lines.append(f"- runs csv: `{runs_csv}`")
lines.append(f"- pairs csv: `{pairs_csv}`")
lines.append(f"- per-run json: `{out / 'runs'}`")
summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

review_md = out / "process_review.md"
review = []
review.append("# Fast Loop Review")
review.append("")
review.append("## Signal Check")
review.append("")
review.append(f"- completed_pairs={completed_pairs}")
review.append(f"- neutral_streak={neutral_streak}")
review.append(f"- reason={reason}")
if med_delta_size is not None:
    review.append(f"- median_delta_size_bytes={int(med_delta_size)}")
if med_delta_total is not None:
    review.append(f"- median_delta_time_seconds={int(med_delta_total)}")
review.append("")
review.append("## Suggested Next Action")
review.append("")
if reason in {"low_signal_neutral_streak", "futile_remaining_pairs"}:
    review.append("- Stop long validation; this loop is currently low-signal for the configured tolerance.")
    review.append("- Increase expected effect size (bundle larger code changes) or increase micro workload stress before re-running.")
elif reason == "clear_regression":
    review.append("- Reject candidate as-is; run pprof on this fast gate to isolate removable overhead before retrying.")
elif reason == "clear_improvement":
    review.append("- Promote candidate to run_celestia A/B confirmation.")
else:
    review.append("- Continue collecting interleaved pairs until a clear outcome or low-signal stop triggers.")
review_md.write_text("\n".join(review) + "\n", encoding="utf-8")

payload = {
    "observed_pairs": raw_pairs,
    "completed_pairs": completed_pairs,
    "invalid_pairs": invalid_pairs,
    "wins": wins,
    "losses": losses,
    "neutral": neutral,
    "neutral_streak": neutral_streak,
    "size_field": size_field,
    "size_tolerance_bytes": size_tol,
    "time_tolerance_seconds": time_tol,
    "median_delta_size_bytes": med_delta_size,
    "median_delta_time_seconds": med_delta_total,
    "stop": stop,
    "reason": reason,
}
decision_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(payload, sort_keys=True))
PY
}

run_pair() {
  local pair_index="$1"
  if (( pair_index % 2 == 1 )); then
    run_variant "$pair_index" "control"
    sleep "$SLEEP_BETWEEN_RUNS_SECONDS"
    run_variant "$pair_index" "candidate"
  else
    run_variant "$pair_index" "candidate"
    sleep "$SLEEP_BETWEEN_RUNS_SECONDS"
    run_variant "$pair_index" "control"
  fi
}

require_cmd git
require_cmd go
require_cmd python3
require_cmd tar
require_cmd gzip
require_cmd wc

if (( MAX_PAIRS < 1 )); then
  echo "MAX_PAIRS must be >= 1" >&2
  exit 2
fi

mkdir -p "$OUT"
setup_bins

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
baseline_hash=$BASELINE_HASH
candidate_unified_bin=$CANDIDATE_UNIFIED_BIN
candidate_treemap_bin=$CANDIDATE_TREEMAP_BIN
baseline_unified_bin=$BASELINE_UNIFIED_BIN
baseline_treemap_bin=$BASELINE_TREEMAP_BIN
max_pairs=$MAX_PAIRS
min_pairs=$MIN_PAIRS
clear_win_pairs=$CLEAR_WIN_PAIRS
clear_loss_pairs=$CLEAR_LOSS_PAIRS
stop_on_clear=$STOP_ON_CLEAR
low_signal_min_pairs=$LOW_SIGNAL_MIN_PAIRS
low_signal_neutral_streak=$LOW_SIGNAL_NEUTRAL_STREAK
size_field=$SIZE_FIELD
size_tolerance_bytes=$SIZE_TOLERANCE_BYTES
time_tolerance_seconds=$TIME_TOLERANCE_SECONDS
sleep_between_runs_seconds=$SLEEP_BETWEEN_RUNS_SECONDS
profile=$PROFILE
dbs=$DBS
tests=$TESTS
keys=$KEYS
valsize=$VALSIZE
batchsize=$BATCHSIZE
val_pattern=$VAL_PATTERN
seed=$SEED
force_value_pointers=$FORCE_VALUE_POINTERS
outer_leaves_in_vlog=$OUTER_LEAVES_IN_VLOG
vlog_compression=$VLOG_COMPRESSION
vlog_compression_autotune=$VLOG_COMPRESSION_AUTOTUNE
vlog_compression_variant=$VLOG_COMPRESSION_VARIANT
dict_train_bytes=$DICT_TRAIN_BYTES
dict_bytes=$DICT_BYTES
vlog_rewrite_min_segment_age_ms=$VLOG_REWRITE_MIN_SEGMENT_AGE_MS
rewrite_enabled=$REWRITE_ENABLED
rewrite_args=$REWRITE_ARGS
measure_gzip=$MEASURE_GZIP
keep_db_dirs=$KEEP_DB_DIRS
script_gowork=$SCRIPT_GOWORK
common_extra_flags=$COMMON_EXTRA_FLAGS
control_extra_flags=$CONTROL_EXTRA_FLAGS
candidate_extra_flags=$CANDIDATE_EXTRA_FLAGS
META

echo "output=$OUT"
echo "baseline_hash=$BASELINE_HASH"
echo "size_field=$SIZE_FIELD"

decision_reason="continue"
for ((pair = 1; pair <= MAX_PAIRS; pair++)); do
  echo "pair=$pair start"
  run_pair "$pair"
  aggregate_and_decide
  decision_reason="$(python3 - "$OUT/decision.json" <<'PY'
import json
import sys
payload = json.loads(open(sys.argv[1], 'r', encoding='utf-8').read())
print(payload.get('reason', 'continue'))
print('1' if payload.get('stop') else '0')
PY
)"
  stop_flag="$(echo "$decision_reason" | tail -n 1)"
  decision_reason="$(echo "$decision_reason" | head -n 1)"
  echo "pair=$pair decision=$decision_reason"
  if [[ "$stop_flag" == "1" ]]; then
    break
  fi
  sleep "$SLEEP_BETWEEN_RUNS_SECONDS"
done

echo "completed decision=$decision_reason"
echo "summary=$OUT/summary.md"
echo "process_review=$OUT/process_review.md"
echo "runs_csv=$OUT/runs.csv"
echo "pairs_csv=$OUT/pairs.csv"
