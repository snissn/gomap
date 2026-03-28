#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ANALYZER="${ANALYZER:-$ROOT/scripts/analyze_vlog_maintenance_capacity.py}"
RUN_HOME_GLOB="${RUN_HOME_GLOB:-$HOME/.celestia-app-mainnet-treedb-*}"
RUN_CMD="${RUN_CMD:-$HOME/run_celestia.sh}"
CONTROL_ENV_FILE="${CONTROL_ENV_FILE:-}"
CANDIDATE_ENV_FILE="${CANDIDATE_ENV_FILE:-}"
TREEMAP_BIN="${TREEMAP_BIN:-/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local}"
REWRITE_ENABLED="${REWRITE_ENABLED:-1}"
MAX_PAIRS="${MAX_PAIRS:-10}"
MIN_PAIRS="${MIN_PAIRS:-4}"
CLEAR_WIN_PAIRS="${CLEAR_WIN_PAIRS:-3}"
CLEAR_LOSS_PAIRS="${CLEAR_LOSS_PAIRS:-3}"
SIZE_TOLERANCE_BYTES="${SIZE_TOLERANCE_BYTES:-67108864}"
TIME_TOLERANCE_SECONDS="${TIME_TOLERANCE_SECONDS:-120}"
STOP_ON_CLEAR="${STOP_ON_CLEAR:-1}"
SLEEP_BETWEEN_RUNS_SECONDS="${SLEEP_BETWEEN_RUNS_SECONDS:-5}"
TS="$(date +%Y%m%d%H%M%S)"
OUT="${OUT_DIR:-$ROOT/artifacts/celestia_ab/$TS}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi
if [[ ! -x "$ANALYZER" ]]; then
  echo "analyzer not found/executable: $ANALYZER" >&2
  exit 1
fi
if [[ "$MAX_PAIRS" -lt 1 ]]; then
  echo "MAX_PAIRS must be >= 1" >&2
  exit 1
fi

mkdir -p "$OUT/runs"

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
run_cmd=$RUN_CMD
control_env_file=$CONTROL_ENV_FILE
candidate_env_file=$CANDIDATE_ENV_FILE
treemap_bin=$TREEMAP_BIN
rewrite_enabled=$REWRITE_ENABLED
max_pairs=$MAX_PAIRS
min_pairs=$MIN_PAIRS
clear_win_pairs=$CLEAR_WIN_PAIRS
clear_loss_pairs=$CLEAR_LOSS_PAIRS
size_tolerance_bytes=$SIZE_TOLERANCE_BYTES
time_tolerance_seconds=$TIME_TOLERANCE_SECONDS
stop_on_clear=$STOP_ON_CLEAR
sleep_between_runs_seconds=$SLEEP_BETWEEN_RUNS_SECONDS
META

list_run_homes() {
  ls -1dt $RUN_HOME_GLOB 2>/dev/null || true
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

detect_new_run_home() {
  local before_file="$1"
  local -A seen=()
  while IFS= read -r path; do
    [[ -n "$path" ]] && seen["$path"]=1
  done <"$before_file"

  while IFS= read -r path; do
    if [[ -z "$path" ]]; then
      continue
    fi
    if [[ -z "${seen[$path]+x}" ]]; then
      echo "$path"
      return 0
    fi
  done < <(list_run_homes)

  list_run_homes | head -n 1
}

run_variant() {
  local pair_index="$1"
  local variant="$2"
  local env_file="$3"

  local run_id
  run_id=$(printf "%02d_%s" "$pair_index" "$variant")
  local run_dir="$OUT/runs/$run_id"
  mkdir -p "$run_dir"

  local before_file="$run_dir/before_homes.txt"
  list_run_homes >"$before_file"

  local run_start
  run_start=$(date +%s)
  (
    set -euo pipefail
    if [[ -n "$env_file" ]]; then
      # shellcheck source=/dev/null
      set -a
      source "$env_file"
      set +a
    fi
    bash -lc "$RUN_CMD"
  ) >"$run_dir/launcher.log" 2>&1
  local run_end
  run_end=$(date +%s)

  local run_home
  run_home="$(detect_new_run_home "$before_file")"
  if [[ -z "$run_home" || ! -d "$run_home" ]]; then
    echo "failed to detect run home for $run_id" >&2
    exit 1
  fi

  local app_db="$run_home/data/application.db"
  local pre_app_bytes pre_wal_bytes
  pre_app_bytes="$(du_bytes "$app_db")"
  pre_wal_bytes="$(du_bytes "$app_db/maindb/wal")"

  local analyze_json="$run_dir/maintenance.json"
  if ! "$ANALYZER" --json "$run_home" >"$analyze_json" 2>"$run_dir/analyze.stderr.log"; then
    rm -f "$analyze_json"
  fi

  local rewrite_attempted=0
  local rewrite_seconds=0
  local rewrite_rc=0
  if [[ "$REWRITE_ENABLED" == "1" && -x "$TREEMAP_BIN" && -d "$app_db" ]]; then
    rewrite_attempted=1
    local rewrite_start
    rewrite_start=$(date +%s)
    set +e
    "$TREEMAP_BIN" vlog-rewrite "$app_db" -rw >"$run_dir/rewrite.log" 2>&1
    rewrite_rc=$?
    set -e
    local rewrite_end
    rewrite_end=$(date +%s)
    rewrite_seconds=$((rewrite_end - rewrite_start))
  else
    rewrite_rc=0
  fi

  local post_app_bytes post_wal_bytes
  post_app_bytes="$(du_bytes "$app_db")"
  post_wal_bytes="$(du_bytes "$app_db/maindb/wal")"

  local run_json="$run_dir/run.json"
  python3 - "$run_home" "$run_json" "$variant" "$pair_index" "$run_start" "$run_end" "$rewrite_attempted" "$rewrite_seconds" "$rewrite_rc" "$pre_app_bytes" "$pre_wal_bytes" "$post_app_bytes" "$post_wal_bytes" "$analyze_json" <<'PY'
import json
import sys
from pathlib import Path

run_home = Path(sys.argv[1])
out_path = Path(sys.argv[2])
variant = sys.argv[3]
pair_index = int(sys.argv[4])
run_start = int(sys.argv[5])
run_end = int(sys.argv[6])
rewrite_attempted = int(sys.argv[7])
rewrite_seconds = int(sys.argv[8])
rewrite_rc = int(sys.argv[9])
pre_app_bytes = int(sys.argv[10])
pre_wal_bytes = int(sys.argv[11])
post_app_bytes = int(sys.argv[12])
post_wal_bytes = int(sys.argv[13])
analyze_json_path = Path(sys.argv[14])

def parse_sync_time(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line == "---" or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def safe_int(raw: str | None, default: int = 0) -> int:
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default
    try:
        return int(s)
    except Exception:
        try:
            return int(float(s))
        except Exception:
            return default

sync = parse_sync_time(run_home / "sync" / "sync-time.log")
maintenance = {}
if analyze_json_path.is_file():
    try:
        payload = json.loads(analyze_json_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            summary = payload.get("summary")
            if isinstance(summary, dict):
                maintenance = summary
    except Exception:
        maintenance = {}

t_sync = safe_int(sync.get("duration_seconds"), max(0, run_end - run_start))
t_rw = rewrite_seconds if rewrite_attempted == 1 else 0
if rewrite_attempted == 1 and rewrite_rc != 0:
    t_total = None
else:
    t_total = t_sync + t_rw

result = {
    "pair_index": pair_index,
    "variant": variant,
    "run_home": str(run_home),
    "sync": {
        "duration_seconds": t_sync,
        "max_rss_kb": safe_int(sync.get("max_rss_kb"), 0),
        "max_hwm_kb": safe_int(sync.get("max_hwm_kb"), 0),
        "end_app_bytes": safe_int(sync.get("end_app_bytes"), pre_app_bytes),
        "end_data_bytes": safe_int(sync.get("end_data_bytes"), 0),
        "end_home_bytes": safe_int(sync.get("end_home_bytes"), 0),
    },
    "rewrite": {
        "attempted": rewrite_attempted == 1,
        "seconds": t_rw,
        "exit_code": rewrite_rc,
    },
    "sizes": {
        "sync_app_bytes": pre_app_bytes,
        "sync_wal_bytes": pre_wal_bytes,
        "post_app_bytes": post_app_bytes,
        "post_wal_bytes": post_wal_bytes,
    },
    "metrics": {
        "t_sync_seconds": t_sync,
        "t_rewrite_seconds": t_rw,
        "t_total_seconds": t_total,
        "s_sync_app_bytes": pre_app_bytes,
        "s_sync_wal_bytes": pre_wal_bytes,
        "s_post_app_bytes": post_app_bytes,
        "s_post_wal_bytes": post_wal_bytes,
        "max_rss_kb": safe_int(sync.get("max_rss_kb"), 0),
    },
    "maintenance_summary": maintenance,
}
out_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
print(out_path)
PY

  echo "run_id=$run_id run_home=$run_home json=$run_json"
}

aggregate_and_decide() {
  local decision_json="$OUT/decision.json"
  python3 - "$OUT" "$SIZE_TOLERANCE_BYTES" "$TIME_TOLERANCE_SECONDS" "$MIN_PAIRS" "$CLEAR_WIN_PAIRS" "$CLEAR_LOSS_PAIRS" "$MAX_PAIRS" "$STOP_ON_CLEAR" "$decision_json" <<'PY'
import csv
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
size_tol = int(sys.argv[2])
time_tol = int(sys.argv[3])
min_pairs = int(sys.argv[4])
clear_win_pairs = int(sys.argv[5])
clear_loss_pairs = int(sys.argv[6])
max_pairs = int(sys.argv[7])
stop_on_clear = sys.argv[8] == "1"
decision_path = Path(sys.argv[9])

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
        "run_home",
        "t_sync_seconds",
        "t_rewrite_seconds",
        "t_total_seconds",
        "s_sync_app_bytes",
        "s_sync_wal_bytes",
        "s_post_app_bytes",
        "s_post_wal_bytes",
        "max_rss_kb",
        "rewrite_exit_code",
        "rewrite_runs",
        "gc_runs",
        "observed_gc_retry_queued",
        "observed_gc_retry_dropped",
    ])
    for r in runs:
        m = r.get("metrics", {}) or {}
        s = r.get("sizes", {}) or {}
        rw = r.get("rewrite", {}) or {}
        summary = r.get("maintenance_summary", {}) or {}
        w.writerow([
            int(r.get("pair_index", 0)),
            str(r.get("variant", "")),
            str(r.get("run_home", "")),
            m.get("t_sync_seconds"),
            m.get("t_rewrite_seconds"),
            m.get("t_total_seconds"),
            s.get("sync_app_bytes"),
            s.get("sync_wal_bytes"),
            s.get("post_app_bytes"),
            s.get("post_wal_bytes"),
            m.get("max_rss_kb"),
            rw.get("exit_code"),
            summary.get("rewrite_runs", 0),
            summary.get("gc_runs", 0),
            summary.get("observed_gc_retry_queued", 0),
            summary.get("observed_gc_retry_dropped", 0),
        ])

by_pair: dict[int, dict[str, dict]] = {}
for r in runs:
    pair = int(r.get("pair_index", 0))
    by_pair.setdefault(pair, {})[str(r.get("variant", ""))] = r

pair_rows = []
wins = 0
losses = 0
for pair in sorted(by_pair):
    row = by_pair[pair]
    ctrl = row.get("control")
    cand = row.get("candidate")
    if not ctrl or not cand:
        continue
    cm = cand.get("metrics", {}) or {}
    bm = ctrl.get("metrics", {}) or {}
    cand_total = cm.get("t_total_seconds")
    base_total = bm.get("t_total_seconds")
    cand_post_wal = cm.get("s_post_wal_bytes")
    base_post_wal = bm.get("s_post_wal_bytes")
    cand_sync = cm.get("t_sync_seconds")
    base_sync = bm.get("t_sync_seconds")
    cand_sync_app = cm.get("s_sync_app_bytes")
    base_sync_app = bm.get("s_sync_app_bytes")

    def delta(a, b):
        if a is None or b is None:
            return None
        return a - b

    d_total = delta(cand_total, base_total)
    d_sync = delta(cand_sync, base_sync)
    d_post_wal = delta(cand_post_wal, base_post_wal)
    d_sync_app = delta(cand_sync_app, base_sync_app)

    outcome = "neutral"
    if d_post_wal is not None and d_total is not None:
        win = (d_post_wal <= -size_tol) and (d_total <= time_tol)
        loss = (d_post_wal >= size_tol) and (d_total >= -time_tol)
        if win and not loss:
            outcome = "win"
            wins += 1
        elif loss and not win:
            outcome = "loss"
            losses += 1

    pair_rows.append({
        "pair_index": pair,
        "delta_t_sync_seconds": d_sync,
        "delta_t_total_seconds": d_total,
        "delta_s_sync_app_bytes": d_sync_app,
        "delta_s_post_wal_bytes": d_post_wal,
        "outcome": outcome,
    })

pairs_csv = out / "pairs.csv"
with pairs_csv.open("w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow([
        "pair_index",
        "delta_t_sync_seconds",
        "delta_t_total_seconds",
        "delta_s_sync_app_bytes",
        "delta_s_post_wal_bytes",
        "outcome",
    ])
    for r in pair_rows:
        w.writerow([
            r["pair_index"],
            r["delta_t_sync_seconds"],
            r["delta_t_total_seconds"],
            r["delta_s_sync_app_bytes"],
            r["delta_s_post_wal_bytes"],
            r["outcome"],
        ])

completed_pairs = len(pair_rows)
reason = "continue"
stop = False
if completed_pairs >= max_pairs:
    stop = True
    reason = "max_pairs"
elif stop_on_clear and completed_pairs >= min_pairs:
    if wins >= clear_win_pairs and wins > losses:
        stop = True
        reason = "clear_improvement"
    elif losses >= clear_loss_pairs and losses > wins:
        stop = True
        reason = "clear_regression"

summary_md = out / "summary.md"
lines = []
lines.append("# run_celestia A/B summary")
lines.append("")
lines.append(f"- completed pairs: `{completed_pairs}`")
lines.append(f"- wins/losses/neutral: `{wins}` / `{losses}` / `{max(0, completed_pairs - wins - losses)}`")
lines.append(f"- size tolerance bytes: `{size_tol}`")
lines.append(f"- time tolerance seconds: `{time_tol}`")
lines.append(f"- decision: `{reason}`")
lines.append("")
lines.append("## Artifacts")
lines.append("")
lines.append(f"- runs csv: `{runs_csv}`")
lines.append(f"- pairs csv: `{pairs_csv}`")
lines.append(f"- per-run json: `{out / 'runs'}`")
if pair_rows:
    last = pair_rows[-1]
    lines.append("")
    lines.append("## Last Pair")
    lines.append("")
    lines.append(f"- pair: `{last['pair_index']}` outcome=`{last['outcome']}`")
    lines.append(f"- delta_t_sync_seconds: `{last['delta_t_sync_seconds']}`")
    lines.append(f"- delta_t_total_seconds: `{last['delta_t_total_seconds']}`")
    lines.append(f"- delta_s_sync_app_bytes: `{last['delta_s_sync_app_bytes']}`")
    lines.append(f"- delta_s_post_wal_bytes: `{last['delta_s_post_wal_bytes']}`")
summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

payload = {
    "completed_pairs": completed_pairs,
    "wins": wins,
    "losses": losses,
    "neutral": max(0, completed_pairs - wins - losses),
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
    run_variant "$pair_index" "control" "$CONTROL_ENV_FILE"
    sleep "$SLEEP_BETWEEN_RUNS_SECONDS"
    run_variant "$pair_index" "candidate" "$CANDIDATE_ENV_FILE"
  else
    run_variant "$pair_index" "candidate" "$CANDIDATE_ENV_FILE"
    sleep "$SLEEP_BETWEEN_RUNS_SECONDS"
    run_variant "$pair_index" "control" "$CONTROL_ENV_FILE"
  fi
}

echo "output=$OUT"
echo "run_cmd=$RUN_CMD"

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
echo "runs_csv=$OUT/runs.csv"
echo "pairs_csv=$OUT/pairs.csv"
