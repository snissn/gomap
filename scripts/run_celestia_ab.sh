#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ANALYZER="${ANALYZER:-$ROOT/scripts/analyze_vlog_maintenance_capacity.py}"
RUN_HOME_GLOB="${RUN_HOME_GLOB:-$HOME/.celestia-app-mainnet-treedb-*}"
RUN_CMD="${RUN_CMD:-$HOME/run_celestia.sh}"
CONTROL_ENV_FILE="${CONTROL_ENV_FILE:-}"
CANDIDATE_ENV_FILE="${CANDIDATE_ENV_FILE:-}"
TREEMAP_BIN="${TREEMAP_BIN:-}"
if [[ -z "$TREEMAP_BIN" ]]; then
  if [[ -x "$ROOT/build/treemap-local" ]]; then
    TREEMAP_BIN="$ROOT/build/treemap-local"
  elif command -v treemap-local >/dev/null 2>&1; then
    TREEMAP_BIN="$(command -v treemap-local)"
  elif command -v treemap >/dev/null 2>&1; then
    TREEMAP_BIN="$(command -v treemap)"
  fi
fi
REWRITE_ENABLED="${REWRITE_ENABLED:-1}"
MAX_PAIRS="${MAX_PAIRS:-10}"
MIN_PAIRS="${MIN_PAIRS:-4}"
CLEAR_WIN_PAIRS="${CLEAR_WIN_PAIRS:-3}"
CLEAR_LOSS_PAIRS="${CLEAR_LOSS_PAIRS:-3}"
SIZE_TOLERANCE_BYTES="${SIZE_TOLERANCE_BYTES:-67108864}"
TIME_TOLERANCE_SECONDS="${TIME_TOLERANCE_SECONDS:-120}"
STOP_ON_CLEAR="${STOP_ON_CLEAR:-1}"
SLEEP_BETWEEN_RUNS_SECONDS="${SLEEP_BETWEEN_RUNS_SECONDS:-5}"
LOW_SIGNAL_MIN_PAIRS="${LOW_SIGNAL_MIN_PAIRS:-3}"
LOW_SIGNAL_NEUTRAL_STREAK="${LOW_SIGNAL_NEUTRAL_STREAK:-3}"
RUN_TIMEOUT_SECONDS="${RUN_TIMEOUT_SECONDS:-1800}"
RUN_MAX_ATTEMPTS_PER_VARIANT="${RUN_MAX_ATTEMPTS_PER_VARIANT:-2}"
RUN_RETRY_SLEEP_SECONDS="${RUN_RETRY_SLEEP_SECONDS:-20}"
INVALID_PAIR_STREAK_STOP="${INVALID_PAIR_STREAK_STOP:-2}"
BLOCK_DRIFT_TOLERANCE="${BLOCK_DRIFT_TOLERANCE:--1}"
SCORING_MODE="${SCORING_MODE:-absolute}"
ALLOW_DRIFT_SCORING="${ALLOW_DRIFT_SCORING:-0}"
AB_DISABLE_HEAVY_DIAGNOSTICS="${AB_DISABLE_HEAVY_DIAGNOSTICS:-1}"
AB_CAPTURE_HEAP_ON_MAX_RSS="${AB_CAPTURE_HEAP_ON_MAX_RSS:-0}"
AB_CAPTURE_PPROF_ON_STUCK="${AB_CAPTURE_PPROF_ON_STUCK:-0}"
AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS="${AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS:-0}"
AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS="${AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS:-0}"
TS="$(date +%Y%m%d%H%M%S)"

if [[ "$#" -gt 4 ]]; then
  echo "usage: $0 [out_dir] [run_cmd] [control_env_file] [candidate_env_file]" >&2
  exit 1
fi
if [[ "$#" -ge 1 ]]; then
  OUT="$1"
else
  OUT="${OUT_DIR:-$ROOT/artifacts/celestia_ab/$TS}"
fi
if [[ "$#" -ge 2 ]]; then
  RUN_CMD="$2"
fi
if [[ "$#" -ge 3 ]]; then
  CONTROL_ENV_FILE="$3"
fi
if [[ "$#" -ge 4 ]]; then
  CANDIDATE_ENV_FILE="$4"
fi

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
if [[ "$RUN_TIMEOUT_SECONDS" -lt 0 ]]; then
  echo "RUN_TIMEOUT_SECONDS must be >= 0" >&2
  exit 1
fi
if [[ "$RUN_TIMEOUT_SECONDS" -gt 0 ]] && ! command -v timeout >/dev/null 2>&1; then
  echo "RUN_TIMEOUT_SECONDS > 0 requires the 'timeout' command" >&2
  exit 1
fi
if [[ "$RUN_MAX_ATTEMPTS_PER_VARIANT" -lt 1 ]]; then
  echo "RUN_MAX_ATTEMPTS_PER_VARIANT must be >= 1" >&2
  exit 1
fi
if [[ "$RUN_RETRY_SLEEP_SECONDS" -lt 0 ]]; then
  echo "RUN_RETRY_SLEEP_SECONDS must be >= 0" >&2
  exit 1
fi
if [[ "$INVALID_PAIR_STREAK_STOP" -lt 1 ]]; then
  echo "INVALID_PAIR_STREAK_STOP must be >= 1" >&2
  exit 1
fi
if [[ "$BLOCK_DRIFT_TOLERANCE" -lt -1 ]]; then
  echo "BLOCK_DRIFT_TOLERANCE must be >= -1 (set -1 to disable drift gating)" >&2
  exit 1
fi
if [[ "$SCORING_MODE" != "absolute" && "$SCORING_MODE" != "per_block" ]]; then
  echo "SCORING_MODE must be one of: absolute, per_block" >&2
  exit 1
fi
if [[ "$ALLOW_DRIFT_SCORING" != "0" && "$ALLOW_DRIFT_SCORING" != "1" ]]; then
  echo "ALLOW_DRIFT_SCORING must be 0 or 1" >&2
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
low_signal_min_pairs=$LOW_SIGNAL_MIN_PAIRS
low_signal_neutral_streak=$LOW_SIGNAL_NEUTRAL_STREAK
run_timeout_seconds=$RUN_TIMEOUT_SECONDS
run_max_attempts_per_variant=$RUN_MAX_ATTEMPTS_PER_VARIANT
run_retry_sleep_seconds=$RUN_RETRY_SLEEP_SECONDS
invalid_pair_streak_stop=$INVALID_PAIR_STREAK_STOP
block_drift_tolerance=$BLOCK_DRIFT_TOLERANCE
scoring_mode=$SCORING_MODE
allow_drift_scoring=$ALLOW_DRIFT_SCORING
ab_disable_heavy_diagnostics=$AB_DISABLE_HEAVY_DIAGNOSTICS
ab_capture_heap_on_max_rss=$AB_CAPTURE_HEAP_ON_MAX_RSS
ab_capture_pprof_on_stuck=$AB_CAPTURE_PPROF_ON_STUCK
ab_capture_full_smaps_on_max_rss=$AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS
ab_capture_debug_vars_on_max_rss=$AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS
META

list_run_homes() {
  local homes=()
  while IFS= read -r path; do
    [[ -d "$path" ]] || continue
    homes+=("$path")
  done < <(compgen -G "$RUN_HOME_GLOB" 2>/dev/null || true)
  if [[ "${#homes[@]}" -eq 0 ]]; then
    return 0
  fi
  ls -1dt -- "${homes[@]}" 2>/dev/null || true
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
  return 1
}

run_variant() {
  local pair_index="$1"
  local variant="$2"
  local env_file="$3"

  local run_id
  run_id=$(printf "%02d_%s" "$pair_index" "$variant")
  local run_dir="$OUT/runs/$run_id"
  mkdir -p "$run_dir"

  local run_home=""
  local app_db=""
  local run_start=0
  local run_end=0
  local run_rc=0
  local attempt_used=0
  local invalid_reason=""
  local pre_app_bytes=0
  local pre_wal_bytes=0
  local post_app_bytes=0
  local post_wal_bytes=0
  local rewrite_attempted=0
  local rewrite_seconds=0
  local rewrite_rc=0
  local analyze_json="$run_dir/maintenance.json"
  rm -f "$analyze_json"
  : >"$run_dir/attempts.log"

  local attempt
  for ((attempt = 1; attempt <= RUN_MAX_ATTEMPTS_PER_VARIANT; attempt++)); do
    attempt_used="$attempt"
    local attempt_dir="$run_dir/attempt_${attempt}"
    mkdir -p "$attempt_dir"

    local before_file="$attempt_dir/before_homes.txt"
    list_run_homes >"$before_file"

    run_start=$(date +%s)
    set +e
    (
      set -euo pipefail
      if [[ -n "$env_file" ]]; then
        # shellcheck source=/dev/null
        set -a
        source "$env_file"
        set +a
      fi
      if [[ "$AB_DISABLE_HEAVY_DIAGNOSTICS" == "1" ]]; then
        # A/B runs prioritize stable wall-time+size measurements. Heavy
        # diagnostics can dominate runtime and produce invalid comparisons.
        export CAPTURE_HEAP_ON_MAX_RSS="${CAPTURE_HEAP_ON_MAX_RSS:-$AB_CAPTURE_HEAP_ON_MAX_RSS}"
        export CAPTURE_PPROF_ON_STUCK="${CAPTURE_PPROF_ON_STUCK:-$AB_CAPTURE_PPROF_ON_STUCK}"
        export CAPTURE_FULL_SMAPS_ON_MAX_RSS="${CAPTURE_FULL_SMAPS_ON_MAX_RSS:-$AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS}"
        export CAPTURE_DEBUG_VARS_ON_MAX_RSS="${CAPTURE_DEBUG_VARS_ON_MAX_RSS:-$AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS}"
      fi
      # Non-login shell avoids user profile side effects (e.g. tty-dependent exports)
      # that can fail under nohup/background runs.
      if [[ "$RUN_TIMEOUT_SECONDS" -gt 0 ]]; then
        timeout --signal=TERM --kill-after=60 "${RUN_TIMEOUT_SECONDS}s" bash -c "$RUN_CMD"
      else
        bash -c "$RUN_CMD"
      fi
    ) >"$attempt_dir/launcher.log" 2>&1
    run_rc=$?
    set -e
    cp "$attempt_dir/launcher.log" "$run_dir/launcher.log"
    run_end=$(date +%s)

    run_home="$(detect_new_run_home "$before_file" || true)"
    invalid_reason=""
    if [[ "$run_rc" -eq 124 || "$run_rc" -eq 137 || "$run_rc" -eq 143 ]]; then
      invalid_reason="run_timeout"
    elif [[ "$run_rc" -ne 0 ]]; then
      invalid_reason="run_cmd_failed"
    elif [[ -z "$run_home" || ! -d "$run_home" ]]; then
      invalid_reason="run_home_missing"
    fi

    echo "attempt=$attempt run_exit_code=$run_rc invalid_reason=${invalid_reason:-none} run_home=${run_home:-<none>}" >>"$run_dir/attempts.log"
    if [[ -z "$invalid_reason" ]]; then
      break
    fi
    if (( attempt < RUN_MAX_ATTEMPTS_PER_VARIANT )); then
      sleep "$RUN_RETRY_SLEEP_SECONDS"
    fi
  done

  if [[ -n "$run_home" && -d "$run_home" ]]; then
    app_db="$run_home/data/application.db"
    pre_app_bytes="$(du_bytes "$app_db")"
    pre_wal_bytes="$(du_bytes "$app_db/maindb/wal")"

    if ! "$ANALYZER" --json "$run_home" >"$analyze_json" 2>"$run_dir/analyze.stderr.log"; then
      rm -f "$analyze_json"
    fi
  fi

  if [[ -z "$invalid_reason" && "$REWRITE_ENABLED" == "1" && -x "$TREEMAP_BIN" && -n "$app_db" && -d "$app_db" ]]; then
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
    if [[ "$rewrite_rc" -ne 0 ]]; then
      invalid_reason="rewrite_failed"
    fi
  fi

  if [[ -n "$app_db" ]]; then
    post_app_bytes="$(du_bytes "$app_db")"
    post_wal_bytes="$(du_bytes "$app_db/maindb/wal")"
  fi

  local run_json="$run_dir/run.json"
  python3 - "$run_home" "$run_json" "$variant" "$pair_index" "$run_start" "$run_end" "$rewrite_attempted" "$rewrite_seconds" "$rewrite_rc" "$pre_app_bytes" "$pre_wal_bytes" "$post_app_bytes" "$post_wal_bytes" "$analyze_json" "$invalid_reason" "$run_rc" "$attempt_used" "$RUN_MAX_ATTEMPTS_PER_VARIANT" "$RUN_TIMEOUT_SECONDS" <<'PY'
import json
import re
import sys
from pathlib import Path

run_home_raw = sys.argv[1]
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
invalid_reason = str(sys.argv[15]).strip()
run_exit_code = int(sys.argv[16])
attempt = int(sys.argv[17])
max_attempts = int(sys.argv[18])
run_timeout_seconds = int(sys.argv[19])
run_home = Path(run_home_raw) if run_home_raw else None

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

def safe_div(num, den):
    try:
        if den is None or float(den) == 0:
            return None
        if num is None:
            return None
        return float(num) / float(den)
    except Exception:
        return None

def probe_sync_progress(node_log_path: Path | None) -> dict[str, object]:
    progress = {
        "node_log_present": False,
        "last_snapshot_chunk": 0,
        "last_snapshot_total": 0,
        "last_nonzero_snapshot_total": 0,
        "max_snapshot_total": 0,
        "snapshot_fetch_events": 0,
        "state_sync_complete": False,
    }
    if node_log_path is None or not node_log_path.is_file():
        return progress

    progress["node_log_present"] = True
    try:
        text = node_log_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return progress

    last_chunk = 0
    last_total = 0
    last_nonzero_total = 0
    max_total = 0
    events = 0
    for m in re.finditer(r"Fetching snapshot chunk chunk=(\d+).*total=(\d+)", text):
        events += 1
        try:
            last_chunk = int(m.group(1))
            last_total = int(m.group(2))
            if last_total > 0:
                last_nonzero_total = last_total
            if last_total > max_total:
                max_total = last_total
        except Exception:
            continue

    progress["last_snapshot_chunk"] = last_chunk
    progress["last_snapshot_total"] = last_total
    progress["last_nonzero_snapshot_total"] = last_nonzero_total
    progress["max_snapshot_total"] = max_total
    progress["snapshot_fetch_events"] = events
    progress["state_sync_complete"] = ("State sync complete" in text) or ("statesync complete" in text.lower())
    return progress

sync_path = run_home / "sync" / "sync-time.log" if run_home is not None else None
sync = parse_sync_time(sync_path) if sync_path is not None else {}
node_log_path = run_home / "sync" / "node.log" if run_home is not None else None
sync_probe = probe_sync_progress(node_log_path)
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
resolved_invalid_reason = invalid_reason
if not resolved_invalid_reason and rewrite_attempted == 1 and rewrite_rc != 0:
    resolved_invalid_reason = "rewrite_failed"
valid = resolved_invalid_reason == ""
t_total = (t_sync + t_rw) if valid else None
trust_height = safe_int(sync.get("trust_height"), 0)
stop_at_local_height = safe_int(sync.get("stop_at_local_height"), 0)
final_local_height = safe_int(sync.get("final_local_height"), 0)
final_remote_height = safe_int(sync.get("final_remote_height"), 0)
final_remote_height_actual = safe_int(sync.get("final_remote_height_actual"), 0)
freeze_remote_height_at_start = safe_int(sync.get("freeze_remote_height_at_start"), 0)
blocks_synced = 0
if trust_height > 0 and final_local_height >= trust_height:
    blocks_synced = final_local_height - trust_height
remote_minus_stop_height = None
if stop_at_local_height > 0 and final_remote_height > 0:
    remote_minus_stop_height = final_remote_height - stop_at_local_height
sync_end_app_bytes = safe_int(sync.get("end_app_bytes"), pre_app_bytes)
s_sync_app_bytes_per_block = safe_div(sync_end_app_bytes, blocks_synced)
s_post_app_bytes_per_block = safe_div(post_app_bytes, blocks_synced)
t_sync_seconds_per_block = safe_div(t_sync, blocks_synced)
t_total_seconds_per_block = safe_div(t_total, blocks_synced) if t_total is not None else None

result = {
    "pair_index": pair_index,
    "variant": variant,
    "run_home": run_home_raw,
    "status": {
        "valid": valid,
        "invalid_reason": resolved_invalid_reason,
        "run_exit_code": run_exit_code,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "run_timeout_seconds": run_timeout_seconds,
        "sync_time_present": sync_path.is_file() if sync_path is not None else False,
        "sync_probe": sync_probe,
    },
    "sync": {
        "duration_seconds": t_sync,
        "max_rss_kb": safe_int(sync.get("max_rss_kb"), 0),
        "max_hwm_kb": safe_int(sync.get("max_hwm_kb"), 0),
        "freeze_remote_height_at_start": freeze_remote_height_at_start,
        "trust_height": trust_height,
        "stop_at_local_height": stop_at_local_height,
        "final_local_height": final_local_height,
        "final_remote_height": final_remote_height,
        "final_remote_height_actual": final_remote_height_actual,
        "blocks_synced": blocks_synced,
        "remote_minus_stop_height": remote_minus_stop_height,
        "end_app_bytes": sync_end_app_bytes,
        "end_data_bytes": safe_int(sync.get("end_data_bytes"), 0),
        "end_home_bytes": safe_int(sync.get("end_home_bytes"), 0),
    },
    "rewrite": {
        "attempted": rewrite_attempted == 1,
        "seconds": t_rw,
        "exit_code": rewrite_rc,
    },
    "sizes": {
        "sync_app_bytes": sync_end_app_bytes,
        "du_sync_app_bytes": pre_app_bytes,
        "sync_wal_bytes": pre_wal_bytes,
        "post_app_bytes": post_app_bytes,
        "post_wal_bytes": post_wal_bytes,
    },
    "metrics": {
        "t_sync_seconds": t_sync,
        "t_rewrite_seconds": t_rw,
        "t_total_seconds": t_total,
        "s_sync_app_bytes": sync_end_app_bytes,
        "s_du_sync_app_bytes": pre_app_bytes,
        "s_sync_wal_bytes": pre_wal_bytes,
        "s_post_app_bytes": post_app_bytes,
        "s_post_wal_bytes": post_wal_bytes,
        "s_sync_app_bytes_per_block": s_sync_app_bytes_per_block,
        "s_post_app_bytes_per_block": s_post_app_bytes_per_block,
        "max_rss_kb": safe_int(sync.get("max_rss_kb"), 0),
        "blocks_synced": blocks_synced,
        "t_sync_seconds_per_block": t_sync_seconds_per_block,
        "t_total_seconds_per_block": t_total_seconds_per_block,
    },
    "maintenance_summary": maintenance,
}
out_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
print(out_path)
PY

  local run_valid="false"
  if [[ -z "$invalid_reason" ]]; then
    run_valid="true"
  fi
  echo "run_id=$run_id run_home=${run_home:-<none>} valid=$run_valid invalid_reason=${invalid_reason:-none} attempts=$attempt_used/$RUN_MAX_ATTEMPTS_PER_VARIANT json=$run_json"
}

aggregate_and_decide() {
  local decision_json="$OUT/decision.json"
  python3 - "$OUT" "$SIZE_TOLERANCE_BYTES" "$TIME_TOLERANCE_SECONDS" "$MIN_PAIRS" "$CLEAR_WIN_PAIRS" "$CLEAR_LOSS_PAIRS" "$MAX_PAIRS" "$STOP_ON_CLEAR" "$LOW_SIGNAL_MIN_PAIRS" "$LOW_SIGNAL_NEUTRAL_STREAK" "$INVALID_PAIR_STREAK_STOP" "$BLOCK_DRIFT_TOLERANCE" "$decision_json" "$SCORING_MODE" "$ALLOW_DRIFT_SCORING" <<'PY'
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
low_signal_min_pairs = int(sys.argv[9])
low_signal_neutral_streak = int(sys.argv[10])
invalid_pair_streak_stop = int(sys.argv[11])
block_drift_tolerance = int(sys.argv[12])
decision_path = Path(sys.argv[13])
scoring_mode = str(sys.argv[14]).strip().lower()
allow_drift_scoring = str(sys.argv[15]).strip() == "1"

run_files = sorted(out.glob("runs/*/run.json"))
runs = []
for p in run_files:
    try:
        runs.append(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        continue

runs.sort(key=lambda r: (int(r.get("pair_index", 0)), str(r.get("variant", ""))))

def run_is_valid(run: dict) -> bool:
    status = run.get("status")
    if isinstance(status, dict) and "valid" in status:
        return bool(status.get("valid"))
    metrics = run.get("metrics", {}) or {}
    rewrite = run.get("rewrite", {}) or {}
    return metrics.get("t_total_seconds") is not None and int(rewrite.get("exit_code", 0)) == 0

def run_invalid_reason(run: dict) -> str:
    status = run.get("status")
    if isinstance(status, dict):
        return str(status.get("invalid_reason", "") or "")
    return ""

def run_attempt(run: dict):
    status = run.get("status")
    if isinstance(status, dict):
        return status.get("attempt")
    return None

def run_max_attempts(run: dict):
    status = run.get("status")
    if isinstance(status, dict):
        return status.get("max_attempts")
    return None

def run_exit_code(run: dict):
    status = run.get("status")
    if isinstance(status, dict) and status.get("run_exit_code") is not None:
        return status.get("run_exit_code")
    rewrite = run.get("rewrite", {}) or {}
    return rewrite.get("exit_code")

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
        "blocks_synced",
        "trust_height",
        "stop_at_local_height",
        "final_local_height",
        "final_remote_height",
        "final_remote_height_actual",
        "freeze_remote_height_at_start",
        "remote_minus_stop_height",
        "s_sync_app_bytes_per_block",
        "s_post_app_bytes_per_block",
        "t_sync_seconds_per_block",
        "t_total_seconds_per_block",
        "valid",
        "invalid_reason",
        "run_exit_code",
        "run_attempt",
        "run_max_attempts",
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
        sync = r.get("sync", {}) or {}
        valid = run_is_valid(r)
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
            m.get("blocks_synced"),
            sync.get("trust_height"),
            sync.get("stop_at_local_height"),
            sync.get("final_local_height"),
            sync.get("final_remote_height"),
            sync.get("final_remote_height_actual"),
            sync.get("freeze_remote_height_at_start"),
            sync.get("remote_minus_stop_height"),
            m.get("s_sync_app_bytes_per_block"),
            m.get("s_post_app_bytes_per_block"),
            m.get("t_sync_seconds_per_block"),
            m.get("t_total_seconds_per_block"),
            valid,
            run_invalid_reason(r),
            run_exit_code(r),
            run_attempt(r),
            run_max_attempts(r),
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
raw_pairs = 0
invalid_pairs = 0
block_drift_invalid_pairs = 0
for pair in sorted(by_pair):
    row = by_pair[pair]
    ctrl = row.get("control")
    cand = row.get("candidate")
    if not ctrl or not cand:
        continue
    raw_pairs += 1
    ctrl_valid = run_is_valid(ctrl)
    cand_valid = run_is_valid(cand)
    ctrl_reason = run_invalid_reason(ctrl)
    cand_reason = run_invalid_reason(cand)
    if not ctrl_valid or not cand_valid:
        outcome = "invalid"
        if ctrl_valid and (not cand_valid) and cand_reason == "rewrite_failed":
            outcome = "loss"
            losses += 1
        invalid_pairs += 1
        pair_rows.append({
            "pair_index": pair,
            "delta_t_sync_seconds": None,
            "delta_t_total_seconds": None,
            "delta_s_sync_app_bytes": None,
            "delta_s_post_wal_bytes": None,
            "delta_blocks_synced": None,
            "delta_s_sync_app_bytes_per_block": None,
            "delta_t_total_seconds_per_block": None,
            "control_valid": ctrl_valid,
            "candidate_valid": cand_valid,
            "control_invalid_reason": ctrl_reason,
            "candidate_invalid_reason": cand_reason,
            "pair_invalid_reason": "run_invalid",
            "outcome": outcome,
        })
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
    cand_blocks = cm.get("blocks_synced")
    base_blocks = bm.get("blocks_synced")
    cand_sync_app_per_block = cm.get("s_sync_app_bytes_per_block")
    base_sync_app_per_block = bm.get("s_sync_app_bytes_per_block")
    cand_total_per_block = cm.get("t_total_seconds_per_block")
    base_total_per_block = bm.get("t_total_seconds_per_block")

    def delta(a, b):
        if a is None or b is None:
            return None
        return a - b

    d_total = delta(cand_total, base_total)
    d_sync = delta(cand_sync, base_sync)
    d_post_wal = delta(cand_post_wal, base_post_wal)
    d_sync_app = delta(cand_sync_app, base_sync_app)
    d_blocks = delta(cand_blocks, base_blocks)
    d_sync_app_per_block = delta(cand_sync_app_per_block, base_sync_app_per_block)
    d_total_per_block = delta(cand_total_per_block, base_total_per_block)

    outcome = "neutral"
    pair_invalid_reason = ""
    if block_drift_tolerance >= 0 and d_blocks is not None and abs(int(d_blocks)) > block_drift_tolerance:
        if allow_drift_scoring:
            pair_invalid_reason = "block_drift_exceeds_tolerance_scored"
        else:
            outcome = "invalid"
            pair_invalid_reason = "block_drift_too_high"
            invalid_pairs += 1
            block_drift_invalid_pairs += 1
    if outcome != "invalid":
        win = False
        loss = False
        use_per_block = (
            scoring_mode == "per_block"
            and d_sync_app_per_block is not None
            and d_total_per_block is not None
            and base_blocks is not None
            and int(base_blocks) > 0
        )
        if use_per_block:
            size_tol_metric = float(size_tol) / float(base_blocks)
            time_tol_metric = float(time_tol) / float(base_blocks)
            size_delta_metric = d_sync_app_per_block
            time_delta_metric = d_total_per_block
        elif d_post_wal is not None and d_total is not None:
            size_tol_metric = float(size_tol)
            time_tol_metric = float(time_tol)
            size_delta_metric = d_post_wal
            time_delta_metric = d_total
        else:
            size_tol_metric = None
            time_tol_metric = None
            size_delta_metric = None
            time_delta_metric = None
        if size_delta_metric is not None and time_delta_metric is not None:
            win = (size_delta_metric <= -size_tol_metric) and (time_delta_metric <= time_tol_metric)
            loss = (size_delta_metric >= size_tol_metric) and (time_delta_metric >= -time_tol_metric)
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
        "delta_blocks_synced": d_blocks,
        "delta_s_sync_app_bytes_per_block": d_sync_app_per_block,
        "delta_t_total_seconds_per_block": d_total_per_block,
        "control_valid": ctrl_valid,
        "candidate_valid": cand_valid,
        "control_invalid_reason": ctrl_reason,
        "candidate_invalid_reason": cand_reason,
        "pair_invalid_reason": pair_invalid_reason,
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
        "delta_blocks_synced",
        "delta_s_sync_app_bytes_per_block",
        "delta_t_total_seconds_per_block",
        "control_valid",
        "candidate_valid",
        "control_invalid_reason",
        "candidate_invalid_reason",
        "pair_invalid_reason",
        "outcome",
    ])
    for r in pair_rows:
        w.writerow([
            r["pair_index"],
            r["delta_t_sync_seconds"],
            r["delta_t_total_seconds"],
            r["delta_s_sync_app_bytes"],
            r["delta_s_post_wal_bytes"],
            r["delta_blocks_synced"],
            r["delta_s_sync_app_bytes_per_block"],
            r["delta_t_total_seconds_per_block"],
            r["control_valid"],
            r["candidate_valid"],
            r["control_invalid_reason"],
            r["candidate_invalid_reason"],
            r["pair_invalid_reason"],
            r["outcome"],
        ])

scored_rows = [row for row in pair_rows if row.get("outcome") != "invalid"]
completed_pairs = len(scored_rows)
neutral = max(0, completed_pairs - wins - losses)
nonzero_block_drift_pairs = 0
for row in scored_rows:
    d = row.get("delta_blocks_synced")
    if d is not None and d != 0:
        nonzero_block_drift_pairs += 1
neutral_streak = 0
for row in reversed(scored_rows):
    if row.get("outcome") == "neutral":
        neutral_streak += 1
        continue
    break
invalid_streak = 0
for row in reversed(pair_rows):
    if row.get("outcome") == "invalid":
        invalid_streak += 1
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

if (not stop) and invalid_streak >= invalid_pair_streak_stop:
    stop = True
    reason = "invalid_pair_streak"

if (not stop) and raw_pairs >= max_pairs:
    stop = True
    reason = "max_pairs"

summary_md = out / "summary.md"
lines = []
lines.append("# run_celestia A/B summary")
lines.append("")
lines.append(f"- observed pairs: `{raw_pairs}`")
lines.append(f"- scored pairs: `{completed_pairs}`")
lines.append(f"- invalid pairs skipped: `{invalid_pairs}`")
lines.append(f"- invalid pairs (block drift gate): `{block_drift_invalid_pairs}`")
lines.append(f"- wins/losses/neutral: `{wins}` / `{losses}` / `{neutral}`")
lines.append(f"- pairs with block-count drift: `{nonzero_block_drift_pairs}`")
lines.append(f"- neutral streak (tail): `{neutral_streak}`")
lines.append(f"- invalid streak (tail): `{invalid_streak}`")
lines.append(f"- size tolerance bytes: `{size_tol}`")
lines.append(f"- time tolerance seconds: `{time_tol}`")
lines.append(f"- low-signal min pairs: `{low_signal_min_pairs}`")
lines.append(f"- low-signal neutral streak: `{low_signal_neutral_streak}`")
lines.append(f"- invalid pair streak stop: `{invalid_pair_streak_stop}`")
lines.append(f"- block drift tolerance (abs blocks, -1=disabled): `{block_drift_tolerance}`")
lines.append(f"- scoring mode: `{scoring_mode}`")
lines.append(f"- allow drift scoring: `{allow_drift_scoring}`")
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
    lines.append(f"- control_valid: `{last['control_valid']}` reason=`{last['control_invalid_reason']}`")
    lines.append(f"- candidate_valid: `{last['candidate_valid']}` reason=`{last['candidate_invalid_reason']}`")
    lines.append(f"- pair_invalid_reason: `{last['pair_invalid_reason']}`")
    lines.append(f"- delta_t_sync_seconds: `{last['delta_t_sync_seconds']}`")
    lines.append(f"- delta_t_total_seconds: `{last['delta_t_total_seconds']}`")
    lines.append(f"- delta_s_sync_app_bytes: `{last['delta_s_sync_app_bytes']}`")
    lines.append(f"- delta_s_post_wal_bytes: `{last['delta_s_post_wal_bytes']}`")
    lines.append(f"- delta_blocks_synced: `{last['delta_blocks_synced']}`")
    lines.append(f"- delta_s_sync_app_bytes_per_block: `{last['delta_s_sync_app_bytes_per_block']}`")
    lines.append(f"- delta_t_total_seconds_per_block: `{last['delta_t_total_seconds_per_block']}`")
summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

payload = {
    "observed_pairs": raw_pairs,
    "completed_pairs": completed_pairs,
    "invalid_pairs": invalid_pairs,
    "block_drift_invalid_pairs": block_drift_invalid_pairs,
    "wins": wins,
    "losses": losses,
    "neutral": neutral,
    "nonzero_block_drift_pairs": nonzero_block_drift_pairs,
    "neutral_streak": neutral_streak,
    "invalid_streak": invalid_streak,
    "scoring_mode": scoring_mode,
    "allow_drift_scoring": allow_drift_scoring,
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
