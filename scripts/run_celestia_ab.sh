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
AB_POLICY="${AB_POLICY:-low_noise}"
if [[ "$AB_POLICY" == "legacy" ]]; then
  DEFAULT_BLOCK_DRIFT_TOLERANCE=-1
  DEFAULT_SCORING_MODE=absolute
  DEFAULT_ALLOW_DRIFT_SCORING=0
elif [[ "$AB_POLICY" == "low_noise" ]]; then
  # Default policy for moving-target runs: per-block scoring plus a drift
  # guardrail to reject large block-count mismatches.
  DEFAULT_BLOCK_DRIFT_TOLERANCE=50
  DEFAULT_SCORING_MODE=per_block
  DEFAULT_ALLOW_DRIFT_SCORING=0
else
  echo "AB_POLICY must be one of: low_noise, legacy" >&2
  exit 1
fi
BLOCK_DRIFT_TOLERANCE="${BLOCK_DRIFT_TOLERANCE:-$DEFAULT_BLOCK_DRIFT_TOLERANCE}"
SCORING_MODE="${SCORING_MODE:-$DEFAULT_SCORING_MODE}"
ALLOW_DRIFT_SCORING="${ALLOW_DRIFT_SCORING:-$DEFAULT_ALLOW_DRIFT_SCORING}"
COMPOSITE_WEIGHT_TIME="${COMPOSITE_WEIGHT_TIME:-0.5}"
COMPOSITE_WEIGHT_SIZE="${COMPOSITE_WEIGHT_SIZE:-0.5}"
COMPOSITE_STOP_ON_CLEAR="${COMPOSITE_STOP_ON_CLEAR:-0}"
COMPOSITE_MIN_PAIRS="${COMPOSITE_MIN_PAIRS:-4}"
COMPOSITE_CLEAR_WIN_PCT="${COMPOSITE_CLEAR_WIN_PCT:-1.0}"
COMPOSITE_CLEAR_LOSS_PCT="${COMPOSITE_CLEAR_LOSS_PCT:-1.0}"
AB_DISABLE_HEAVY_DIAGNOSTICS="${AB_DISABLE_HEAVY_DIAGNOSTICS:-1}"
AB_CAPTURE_HEAP_ON_MAX_RSS="${AB_CAPTURE_HEAP_ON_MAX_RSS:-0}"
AB_CAPTURE_PPROF_ON_STUCK="${AB_CAPTURE_PPROF_ON_STUCK:-0}"
AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS="${AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS:-0}"
AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS="${AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS:-0}"
AB_CAPTURE_LIGHT_VLOG_STATS="${AB_CAPTURE_LIGHT_VLOG_STATS:-1}"
AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS="${AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS:-20}"
AB_CAPTURE_LIVE_DEBUG_VARS="${AB_CAPTURE_LIVE_DEBUG_VARS:-1}"
AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS="${AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS:-30}"
AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS="${AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS:-5}"
AB_LIVE_DEBUG_VARS_URL="${AB_LIVE_DEBUG_VARS_URL:-http://127.0.0.1:6062/debug/vars}"

# Optional: require that the run exercised the maintenance rewrite lane before we
# accept it as a valid attempt. This helps avoid low-signal runs that spend most
# of their time in bootstrap/restore/catch-up where queued rewrite debt is not
# executed.
AB_REQUIRE_MAINTENANCE_WITH_REWRITE="${AB_REQUIRE_MAINTENANCE_WITH_REWRITE:-0}"
AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC="${AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC:-0}"
PAIR_ALIGN_TRUST_FROM_FIRST="${PAIR_ALIGN_TRUST_FROM_FIRST:-0}"
PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST="${PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST:-0}"
PAIR_ALIGN_STOP_MARGIN="${PAIR_ALIGN_STOP_MARGIN:-0}"
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
if [[ "$COMPOSITE_STOP_ON_CLEAR" != "0" && "$COMPOSITE_STOP_ON_CLEAR" != "1" ]]; then
  echo "COMPOSITE_STOP_ON_CLEAR must be 0 or 1" >&2
  exit 1
fi
if [[ "$COMPOSITE_MIN_PAIRS" -lt 1 ]]; then
  echo "COMPOSITE_MIN_PAIRS must be >= 1" >&2
  exit 1
fi
if [[ "$AB_CAPTURE_LIGHT_VLOG_STATS" != "0" && "$AB_CAPTURE_LIGHT_VLOG_STATS" != "1" ]]; then
  echo "AB_CAPTURE_LIGHT_VLOG_STATS must be 0 or 1" >&2
  exit 1
fi
if [[ "$AB_CAPTURE_LIVE_DEBUG_VARS" != "0" && "$AB_CAPTURE_LIVE_DEBUG_VARS" != "1" ]]; then
  echo "AB_CAPTURE_LIVE_DEBUG_VARS must be 0 or 1" >&2
  exit 1
fi
if [[ "$AB_REQUIRE_MAINTENANCE_WITH_REWRITE" != "0" && "$AB_REQUIRE_MAINTENANCE_WITH_REWRITE" != "1" ]]; then
  echo "AB_REQUIRE_MAINTENANCE_WITH_REWRITE must be 0 or 1" >&2
  exit 1
fi
if [[ "$AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC" != "0" && "$AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC" != "1" ]]; then
  echo "AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC must be 0 or 1" >&2
  exit 1
fi
if [[ "$PAIR_ALIGN_TRUST_FROM_FIRST" != "0" && "$PAIR_ALIGN_TRUST_FROM_FIRST" != "1" ]]; then
  echo "PAIR_ALIGN_TRUST_FROM_FIRST must be 0 or 1" >&2
  exit 1
fi
if [[ "$PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST" != "0" && "$PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST" != "1" ]]; then
  echo "PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST must be 0 or 1" >&2
  exit 1
fi
if ! [[ "$PAIR_ALIGN_STOP_MARGIN" =~ ^[0-9]+$ ]]; then
  echo "PAIR_ALIGN_STOP_MARGIN must be a non-negative integer" >&2
  exit 1
fi
if [[ "$AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS" -lt 0 ]]; then
  echo "AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS must be >= 0" >&2
  exit 1
fi
if [[ "$AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS" -lt 1 ]]; then
  echo "AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS must be >= 1" >&2
  exit 1
fi
if [[ "$AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS" -lt 1 ]]; then
  echo "AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS must be >= 1" >&2
  exit 1
fi
if [[ "$REWRITE_ENABLED" != "0" && "$REWRITE_ENABLED" != "1" ]]; then
  echo "REWRITE_ENABLED must be 0 or 1" >&2
  exit 1
fi
if [[ "$REWRITE_ENABLED" == "1" && -z "$TREEMAP_BIN" ]]; then
  echo "REWRITE_ENABLED=1 requires TREEMAP_BIN (set TREEMAP_BIN or ensure treemap-local is discoverable)" >&2
  exit 1
fi
if [[ "$REWRITE_ENABLED" == "1" && ! -x "$TREEMAP_BIN" ]]; then
  echo "treemap binary not found/executable: $TREEMAP_BIN" >&2
  exit 1
fi

# Freeze the resolved run command early so per-variant env sourcing cannot
# accidentally mutate the executed launcher command.
RUN_CMD_FROZEN="$RUN_CMD"
if [[ "$RUN_CMD_FROZEN" == *"run_celestia_ab.sh"* ]]; then
  echo "RUN_CMD resolves to run_celestia_ab.sh (self-invocation): $RUN_CMD_FROZEN" >&2
  exit 1
fi

mkdir -p "$OUT/runs"

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
run_cmd=$RUN_CMD
run_cmd_frozen=$RUN_CMD_FROZEN
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
ab_policy=$AB_POLICY
block_drift_tolerance=$BLOCK_DRIFT_TOLERANCE
scoring_mode=$SCORING_MODE
allow_drift_scoring=$ALLOW_DRIFT_SCORING
composite_weight_time=$COMPOSITE_WEIGHT_TIME
composite_weight_size=$COMPOSITE_WEIGHT_SIZE
composite_stop_on_clear=$COMPOSITE_STOP_ON_CLEAR
composite_min_pairs=$COMPOSITE_MIN_PAIRS
composite_clear_win_pct=$COMPOSITE_CLEAR_WIN_PCT
composite_clear_loss_pct=$COMPOSITE_CLEAR_LOSS_PCT
ab_disable_heavy_diagnostics=$AB_DISABLE_HEAVY_DIAGNOSTICS
ab_capture_heap_on_max_rss=$AB_CAPTURE_HEAP_ON_MAX_RSS
ab_capture_pprof_on_stuck=$AB_CAPTURE_PPROF_ON_STUCK
ab_capture_full_smaps_on_max_rss=$AB_CAPTURE_FULL_SMAPS_ON_MAX_RSS
ab_capture_debug_vars_on_max_rss=$AB_CAPTURE_DEBUG_VARS_ON_MAX_RSS
ab_capture_light_vlog_stats=$AB_CAPTURE_LIGHT_VLOG_STATS
ab_light_vlog_stats_timeout_seconds=$AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS
ab_capture_live_debug_vars=$AB_CAPTURE_LIVE_DEBUG_VARS
ab_live_debug_vars_interval_seconds=$AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS
ab_live_debug_vars_timeout_seconds=$AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS
ab_live_debug_vars_url=$AB_LIVE_DEBUG_VARS_URL
ab_require_maintenance_with_rewrite=$AB_REQUIRE_MAINTENANCE_WITH_REWRITE
ab_require_rewrite_queued_debt_exec=$AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC
pair_align_trust_from_first=$PAIR_ALIGN_TRUST_FROM_FIRST
pair_align_stop_height_from_first=$PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST
pair_align_stop_margin=$PAIR_ALIGN_STOP_MARGIN
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

capture_light_vlog_stats() {
  local app_db="$1"
  local out_file="$2"
  local err_file="$3"
  local env_file="${4:-}"
  local overlay_env_file="${5:-}"

  rm -f "$out_file" "$err_file"

  if [[ "$AB_CAPTURE_LIGHT_VLOG_STATS" != "1" ]]; then
    return 2
  fi
  if [[ -z "$TREEMAP_BIN" || ! -x "$TREEMAP_BIN" ]]; then
    return 3
  fi
  if [[ ! -d "$app_db" ]]; then
    return 4
  fi

  local rc=0
  local -a cmd_stats=("$TREEMAP_BIN" stats "$app_db" "-rw")
  local -a cmd_vlog_gc_dry_run=("$TREEMAP_BIN" vlog-gc "$app_db" "-rw" "-dry-run")

  set +e
  (
    set -euo pipefail
    if [[ -n "$env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$env_file"
      set +a
    fi
    if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$overlay_env_file"
      set +a
    fi
    if [[ "$AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
      timeout --signal=TERM --kill-after=30 "${AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS}s" "${cmd_stats[@]}"
    else
      "${cmd_stats[@]}"
    fi
  ) >"$out_file" 2>"$err_file"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 && -s "$out_file" ]]; then
    return 0
  fi

  {
    echo "light-stats primary command failed rc=$rc command=stats_rw"
    echo "falling back to vlog-gc -dry-run"
  } >>"$err_file"

  set +e
  (
    set -euo pipefail
    if [[ -n "$env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$env_file"
      set +a
    fi
    if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$overlay_env_file"
      set +a
    fi
    if [[ "$AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
      timeout --signal=TERM --kill-after=30 "${AB_LIGHT_VLOG_STATS_TIMEOUT_SECONDS}s" "${cmd_vlog_gc_dry_run[@]}"
    else
      "${cmd_vlog_gc_dry_run[@]}"
    fi
  ) >"$out_file" 2>>"$err_file"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 && -s "$out_file" ]]; then
    return 0
  fi

  rm -f "$out_file"
  return 1
}

run_json_path() {
  local pair_index="$1"
  local variant="$2"
  local run_id
  run_id=$(printf "%02d_%s" "$pair_index" "$variant")
  printf '%s/runs/%s/run.json\n' "$OUT" "$run_id"
}

build_pair_overlay_env() {
  local first_json="$1"
  local out_env="$2"
  python3 - "$first_json" "$out_env" "$PAIR_ALIGN_TRUST_FROM_FIRST" "$PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST" "$PAIR_ALIGN_STOP_MARGIN" <<'PY'
import json
import sys
from pathlib import Path

first_json = Path(sys.argv[1])
out_env = Path(sys.argv[2])
align_trust = sys.argv[3] == "1"
align_stop = sys.argv[4] == "1"
stop_margin = int(sys.argv[5])

def scalar_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

out_lines = []
if first_json.exists():
    payload = json.loads(first_json.read_text(encoding="utf-8"))
    sync = payload.get("sync") or {}
    run_home_raw = payload.get("run_home") or ""
    run_home = Path(run_home_raw) if run_home_raw else None
    trust_height = scalar_int(sync.get("trust_height"))
    trust_hash = ""
    if run_home:
        sync_time = run_home / "sync" / "sync-time.log"
        if sync_time.exists():
            for line in sync_time.read_text(encoding="utf-8", errors="ignore").splitlines():
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key == "trust_height":
                    parsed = scalar_int(value)
                    if parsed is not None:
                        trust_height = parsed
                elif key == "trust_hash":
                    trust_hash = value
    if align_trust and trust_height is not None:
        out_lines.append(f"TRUST_HEIGHT={trust_height}")
        if trust_hash:
            out_lines.append(f"TRUST_HASH={trust_hash}")
    if align_stop:
        final_local = scalar_int(sync.get("final_local_height"))
        if final_local is not None:
            target = final_local + stop_margin
            if target < 0:
                target = 0
            out_lines.append(f"STOP_AT_LOCAL_HEIGHT={target}")

if out_lines:
    out_env.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
else:
    out_env.write_text("", encoding="utf-8")
PY
}

capture_live_debug_vars_periodic() {
  local out_file="$1"
  local run_pid="$2"
  if [[ "$AB_CAPTURE_LIVE_DEBUG_VARS" != "1" ]]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    return 0
  fi

  local tmp_file="${out_file}.tmp"
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    if curl -fsS --max-time "$AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS" "$AB_LIVE_DEBUG_VARS_URL" >"$tmp_file" 2>/dev/null && [[ -s "$tmp_file" ]]; then
      mv "$tmp_file" "$out_file"
    else
      rm -f "$tmp_file"
    fi
    sleep "$AB_LIVE_DEBUG_VARS_INTERVAL_SECONDS"
  done
  if curl -fsS --max-time "$AB_LIVE_DEBUG_VARS_TIMEOUT_SECONDS" "$AB_LIVE_DEBUG_VARS_URL" >"$tmp_file" 2>/dev/null && [[ -s "$tmp_file" ]]; then
    mv "$tmp_file" "$out_file"
  else
    rm -f "$tmp_file"
  fi
}


capture_build_meta() {
  local env_file="$1"
  local overlay_env_file="$2"
  local out_file="$3"
  local err_file="$4"

  rm -f "$out_file" "$err_file"
  (
    set -euo pipefail
    if [[ -n "$env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$env_file"
      set +a
    fi
    if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$overlay_env_file"
      set +a
    fi

    echo "treedb_open_profile=${TREEDB_OPEN_PROFILE:-}"
    echo "use_local_tree_stack=${USE_LOCAL_TREE_STACK:-}"
    echo "local_gomap_dir=${LOCAL_GOMAP_DIR:-}"

    local gomap_dir="${LOCAL_GOMAP_DIR:-}"
    if [[ -n "$gomap_dir" && -d "$gomap_dir" ]] && command -v git >/dev/null 2>&1; then
      local head=""
      head="$(git -C "$gomap_dir" rev-parse HEAD 2>/dev/null || true)"
      if [[ -n "$head" ]]; then
        echo "gomap_git_head=$head"
        local desc=""
        desc="$(git -C "$gomap_dir" describe --always --dirty 2>/dev/null || true)"
        echo "gomap_git_describe=$desc"
        local dirty=0
        if [[ -n "$(git -C "$gomap_dir" status --porcelain=v1 --untracked-files=no 2>/dev/null)" ]]; then
          dirty=1
        fi
        echo "gomap_git_dirty=$dirty"
      fi
    fi
  ) >"$out_file" 2>"$err_file" || {
    printf "%s\n" "build_meta_error=1" >"$out_file"
  }
}

run_variant() {
  local pair_index="$1"
  local variant="$2"
  local env_file="$3"
  local overlay_env_file="${4:-}"

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
  local maintenance_source_file="$run_dir/maintenance_source.txt"
  local light_stats_pre="$run_dir/light_stats_pre.txt"
  local light_stats_pre_err="$run_dir/light_stats_pre.stderr.log"
  local light_stats_post="$run_dir/light_stats_post.txt"
  local light_stats_post_err="$run_dir/light_stats_post.stderr.log"
  local live_debug_vars="$run_dir/live_debug_vars_latest.json"
  local light_stats_pre_rc=2
  local light_stats_post_rc=2
  local maintenance_captured=0
  local require_maintenance_metrics="0"
  if [[ "$AB_REQUIRE_MAINTENANCE_WITH_REWRITE" == "1" || "$AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC" == "1" ]]; then
    require_maintenance_metrics="1"
  fi
  rm -f "$analyze_json"
  rm -f "$maintenance_source_file"
  rm -f "$light_stats_pre" "$light_stats_pre_err" "$light_stats_post" "$light_stats_post_err"
  rm -f "$live_debug_vars"
  : >"$run_dir/attempts.log"

  local attempt
  for ((attempt = 1; attempt <= RUN_MAX_ATTEMPTS_PER_VARIANT; attempt++)); do
    attempt_used="$attempt"
    maintenance_captured=0
    local attempt_dir="$run_dir/attempt_${attempt}"
    mkdir -p "$attempt_dir"

    # Clear per-attempt artifacts in $run_dir so a failed attempt cannot accidentally
    # reuse maintenance.json/live debug vars from an earlier attempt.
    rm -f "$analyze_json" "$maintenance_source_file" "$run_dir/analyze.stderr.log" "$live_debug_vars"

    local before_file="$attempt_dir/before_homes.txt"
    local attempt_live_debug_vars="$attempt_dir/live_debug_vars_latest.json"
    list_run_homes >"$before_file"
    rm -f "$attempt_live_debug_vars"

    run_start=$(date +%s)
    set +e
    (
      set -euo pipefail
      if [[ -n "$env_file" ]]; then
        set -a
        # shellcheck source=/dev/null
        source "$env_file"
        set +a
      fi
      if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
        set -a
        # shellcheck source=/dev/null
        source "$overlay_env_file"
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
        timeout --signal=TERM --kill-after=60 "${RUN_TIMEOUT_SECONDS}s" bash -c "$RUN_CMD_FROZEN"
      else
        bash -c "$RUN_CMD_FROZEN"
      fi
    ) >"$attempt_dir/launcher.log" 2>&1 &
    local runner_pid=$!
    local live_sampler_pid=""
    if [[ "$AB_CAPTURE_LIVE_DEBUG_VARS" == "1" ]]; then
      capture_live_debug_vars_periodic "$attempt_live_debug_vars" "$runner_pid" &
      live_sampler_pid=$!
    fi
    wait "$runner_pid"
    run_rc=$?
    if [[ -n "$live_sampler_pid" ]]; then
      kill "$live_sampler_pid" >/dev/null 2>&1 || true
      wait "$live_sampler_pid" >/dev/null 2>&1 || true
    fi
    set -e
    cp "$attempt_dir/launcher.log" "$run_dir/launcher.log"
    if [[ -s "$attempt_live_debug_vars" ]]; then
      cp "$attempt_live_debug_vars" "$live_debug_vars"
    fi
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


    local maintenance_with_rewrite=-1
    local rewrite_queued_debt_exec_runs=-1
    local maintenance_source="none"

    # If enabled, require that this attempt actually exercised the rewrite lane
    # we are optimizing (avoid accepting bootstrap/catch-up attempts where rewrite
    # does not run or does not execute queued debt).
    if [[ -z "$invalid_reason" && "$require_maintenance_metrics" == "1" ]]; then
      rm -f "$analyze_json" "$maintenance_source_file"
      if ! (
        set -euo pipefail
        if [[ -n "$env_file" ]]; then
          set -a
          # shellcheck source=/dev/null
          source "$env_file"
          set +a
        fi
        if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
          set -a
          # shellcheck source=/dev/null
          source "$overlay_env_file"
          set +a
        fi
        "$ANALYZER" --json "$run_home"
      ) >"$analyze_json" 2>"$run_dir/analyze.stderr.log"; then
        rm -f "$analyze_json"
        if [[ -s "$live_debug_vars" ]]; then
          if ! (
            set -euo pipefail
            "$ANALYZER" --json "$live_debug_vars"
          ) >"$analyze_json" 2>>"$run_dir/analyze.stderr.log"; then
            rm -f "$analyze_json"
          else
            printf "%s\n" "live_debug_vars" >"$maintenance_source_file"
          fi
        fi
      else
        printf "%s\n" "diagnostics_json" >"$maintenance_source_file"
      fi

      if [[ -s "$analyze_json" ]]; then
        maintenance_captured=1
        if read -r maintenance_with_rewrite rewrite_queued_debt_exec_runs < <(
          python3 - "$analyze_json" <<'PY'
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
try:
    payload = json.loads(p.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(2)

summary = payload.get("summary") if isinstance(payload, dict) else None
summary = summary if isinstance(summary, dict) else {}

def safe_int(value) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return 0
        if s == "true":
            return 1
        if s == "false":
            return 0
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return 0
    return 0

maint_with_rewrite = safe_int(summary.get("maintenance_with_rewrite", 0))
queued_exec_runs = safe_int(summary.get("rewrite_queued_debt_exec_runs", 0))
print(f"{maint_with_rewrite} {queued_exec_runs}")
PY
        ); then
          :
        else
          maintenance_with_rewrite=-1
          rewrite_queued_debt_exec_runs=-1
        fi
      fi

      if [[ -s "$maintenance_source_file" ]]; then
        maintenance_source="$(head -n 1 "$maintenance_source_file" | tr -d "\r")"
      fi

      if [[ "$AB_REQUIRE_MAINTENANCE_WITH_REWRITE" == "1" ]]; then
        if [[ "$maintenance_captured" != "1" ]]; then
          invalid_reason="maintenance_summary_missing"
        elif [[ "$maintenance_with_rewrite" -le 0 ]]; then
          invalid_reason="maintenance_rewrite_not_engaged"
        fi
      fi
      if [[ -z "$invalid_reason" && "$AB_REQUIRE_REWRITE_QUEUED_DEBT_EXEC" == "1" ]]; then
        if [[ "$maintenance_captured" != "1" ]]; then
          invalid_reason="maintenance_summary_missing"
        elif [[ "$rewrite_queued_debt_exec_runs" -le 0 ]]; then
          invalid_reason="rewrite_queued_debt_exec_not_engaged"
        fi
      fi
    fi

    if [[ "$require_maintenance_metrics" == "1" ]]; then
      echo "attempt=$attempt run_exit_code=$run_rc invalid_reason=${invalid_reason:-none} run_home=${run_home:-<none>} maintenance_with_rewrite=$maintenance_with_rewrite rewrite_queued_debt_exec_runs=$rewrite_queued_debt_exec_runs maintenance_source=$maintenance_source" >>"$run_dir/attempts.log"
    else
      echo "attempt=$attempt run_exit_code=$run_rc invalid_reason=${invalid_reason:-none} run_home=${run_home:-<none>}" >>"$run_dir/attempts.log"
    fi
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

    if [[ "$maintenance_captured" != "1" || ! -s "$analyze_json" ]]; then
      rm -f "$analyze_json" "$maintenance_source_file"
      if ! (
        set -euo pipefail
        if [[ -n "$env_file" ]]; then
          set -a
          # shellcheck source=/dev/null
          source "$env_file"
          set +a
        fi
        if [[ -n "$overlay_env_file" && -f "$overlay_env_file" ]]; then
          set -a
          # shellcheck source=/dev/null
          source "$overlay_env_file"
          set +a
        fi
        "$ANALYZER" --json "$run_home"
      ) >"$analyze_json" 2>"$run_dir/analyze.stderr.log"; then
        rm -f "$analyze_json"
        if [[ -s "$live_debug_vars" ]]; then
          if ! (
            set -euo pipefail
            "$ANALYZER" --json "$live_debug_vars"
          ) >"$analyze_json" 2>>"$run_dir/analyze.stderr.log"; then
            rm -f "$analyze_json"
          else
            printf "%s\n" "live_debug_vars" >"$maintenance_source_file"
          fi
        fi
      else
        printf "%s\n" "diagnostics_json" >"$maintenance_source_file"
      fi
    fi

    if capture_light_vlog_stats "$app_db" "$light_stats_pre" "$light_stats_pre_err" "$env_file" "$overlay_env_file"; then
      light_stats_pre_rc=0
    else
      light_stats_pre_rc=$?
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
    if capture_light_vlog_stats "$app_db" "$light_stats_post" "$light_stats_post_err" "$env_file" "$overlay_env_file"; then
      light_stats_post_rc=0
    else
      light_stats_post_rc=$?
    fi
  fi

  local run_json="$run_dir/run.json"
  local build_meta="$run_dir/build_meta.env"
  local build_meta_err="$run_dir/build_meta.stderr.log"
  capture_build_meta "$env_file" "$overlay_env_file" "$build_meta" "$build_meta_err"
  python3 - "$run_home" "$run_json" "$variant" "$pair_index" "$run_start" "$run_end" "$rewrite_attempted" "$rewrite_seconds" "$rewrite_rc" "$pre_app_bytes" "$pre_wal_bytes" "$post_app_bytes" "$post_wal_bytes" "$analyze_json" "$maintenance_source_file" "$light_stats_pre" "$light_stats_pre_rc" "$light_stats_post" "$light_stats_post_rc" "$invalid_reason" "$run_rc" "$attempt_used" "$RUN_MAX_ATTEMPTS_PER_VARIANT" "$RUN_TIMEOUT_SECONDS" "$build_meta" <<'PY'
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
maintenance_source_path = Path(sys.argv[15])
light_stats_pre_path = Path(sys.argv[16])
light_stats_pre_rc = int(sys.argv[17])
light_stats_post_path = Path(sys.argv[18])
light_stats_post_rc = int(sys.argv[19])
invalid_reason = str(sys.argv[20]).strip()
run_exit_code = int(sys.argv[21])
attempt = int(sys.argv[22])
max_attempts = int(sys.argv[23])
run_timeout_seconds = int(sys.argv[24])
build_meta_path = Path(sys.argv[25])
run_home = Path(run_home_raw) if run_home_raw else None

def parse_kv_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

build_meta = parse_kv_file(build_meta_path)

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

def safe_float(raw: str | None, default: float = 0.0) -> float:
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default
    try:
        return float(s)
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

def parse_treemap_stats(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return out
    for raw in text.splitlines():
        line = raw.strip()
        m = re.search(
            r"segments total=(\d+)\s+referenced=(\d+)\s+active=(\d+)\s+eligible=(\d+)\s+deleted=(\d+)\s+"
            r"bytes_total=(\d+)\s+bytes_referenced=(\d+)\s+bytes_active=(\d+)\s+bytes_eligible=(\d+)\s+bytes_deleted=(\d+)",
            line,
        )
        if m:
            out["vlog_gc_segments_total"] = m.group(1)
            out["vlog_gc_segments_referenced"] = m.group(2)
            out["vlog_gc_segments_active"] = m.group(3)
            out["vlog_gc_segments_eligible"] = m.group(4)
            out["vlog_gc_segments_deleted"] = m.group(5)
            out["vlog_gc_bytes_total"] = m.group(6)
            out["vlog_gc_bytes_referenced"] = m.group(7)
            out["vlog_gc_bytes_active"] = m.group(8)
            out["vlog_gc_bytes_eligible"] = m.group(9)
            out["vlog_gc_bytes_deleted"] = m.group(10)
            continue
        if not line or line == "Stats:" or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        out[key] = value.strip()
    return out

def build_light_summary(stats: dict[str, str]) -> dict[str, object]:
    if not stats:
        return {}
    def stat_int(*keys: str) -> int:
        for key in keys:
            if key in stats:
                return safe_int(stats.get(key), 0)
        return 0

    def stat_str(*keys: str) -> str:
        for key in keys:
            if key in stats:
                return str(stats.get(key, "")).strip()
        return ""

    out: dict[str, object] = {
        "bytes_live_total": stat_int(
            "vlog_gc_bytes_referenced",
            "treedb.cache.vlog_generation.bytes.live.total",
            "treedb.vlog_generation.bytes.live.total",
        ),
        "bytes_stale_total": stat_int(
            "vlog_gc_bytes_eligible",
            "treedb.cache.vlog_generation.bytes.stale.total",
            "treedb.vlog_generation.bytes.stale.total",
        ),
        "bytes_total_total": stat_int(
            "vlog_gc_bytes_total",
            "treedb.cache.vlog_generation.bytes.total.total",
            "treedb.vlog_generation.bytes.total.total",
        ),
        "bytes_active_total": stat_int("vlog_gc_bytes_active"),
        "bytes_deleted_total": stat_int("vlog_gc_bytes_deleted"),
        "bytes_live_hot": stat_int(
            "treedb.cache.vlog_generation.bytes.live.hot",
            "treedb.vlog_generation.bytes.live.hot",
        ),
        "bytes_live_warm": stat_int(
            "treedb.cache.vlog_generation.bytes.live.warm",
            "treedb.vlog_generation.bytes.live.warm",
        ),
        "bytes_live_cold": stat_int(
            "treedb.cache.vlog_generation.bytes.live.cold",
            "treedb.vlog_generation.bytes.live.cold",
        ),
        "bytes_stale_hot": stat_int(
            "treedb.cache.vlog_generation.bytes.stale.hot",
            "treedb.vlog_generation.bytes.stale.hot",
        ),
        "bytes_stale_warm": stat_int(
            "treedb.cache.vlog_generation.bytes.stale.warm",
            "treedb.vlog_generation.bytes.stale.warm",
        ),
        "bytes_stale_cold": stat_int(
            "treedb.cache.vlog_generation.bytes.stale.cold",
            "treedb.vlog_generation.bytes.stale.cold",
        ),
        "segments_total": stat_int(
            "vlog_gc_segments_total",
            "treedb.cache.vlog_generation.segments.total",
            "treedb.vlog_generation.segments.total",
        ),
        "segments_referenced": stat_int("vlog_gc_segments_referenced"),
        "segments_active": stat_int("vlog_gc_segments_active"),
        "segments_eligible": stat_int("vlog_gc_segments_eligible"),
        "segments_deleted": stat_int("vlog_gc_segments_deleted"),
        "segments_hot": stat_int(
            "treedb.cache.vlog_generation.segments.hot",
            "treedb.vlog_generation.segments.hot",
        ),
        "segments_warm": stat_int(
            "treedb.cache.vlog_generation.segments.warm",
            "treedb.vlog_generation.segments.warm",
        ),
        "segments_cold": stat_int(
            "treedb.cache.vlog_generation.segments.cold",
            "treedb.vlog_generation.segments.cold",
        ),
        "rewrite_queue_len": stat_int("treedb.cache.vlog_generation.rewrite.queue_len"),
        "rewrite_queue_live_bytes_after_tokens": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_live_bytes_after_tokens",
        ),
        "rewrite_penalties_active": stat_int("treedb.cache.vlog_generation.rewrite.penalties_active"),
        "rewrite_age_blocked_remaining_ms": stat_int(
            "treedb.cache.vlog_generation.rewrite.age_blocked_remaining_ms",
        ),
        "gc_last_eligible_bytes": stat_int(
            "treedb.cache.vlog_generation.gc.last_eligible_bytes",
            "treedb.vlog_generation.gc.last_eligible_bytes",
        ),
        "gc_last_pending_bytes": stat_int(
            "treedb.cache.vlog_generation.gc.last_pending_bytes",
            "treedb.vlog_generation.gc.last_pending_bytes",
        ),
        "gc_last_protected_retained_bytes": stat_int(
            "treedb.cache.vlog_generation.gc.last_protected_retained_bytes",
            "treedb.vlog_generation.gc.last_protected_retained_bytes",
        ),
    }
    phase = stat_str(
        "treedb.cache.vlog_generation.maintenance_phase",
        "treedb.vlog_generation.scheduler_state",
    )
    if phase:
        out["maintenance_phase"] = phase
    total = safe_float(out.get("bytes_total_total"), 0.0)
    stale = safe_float(out.get("bytes_stale_total"), 0.0)
    if total > 0:
        out["bytes_stale_ratio_pct"] = 100.0 * stale / total
    else:
        out["bytes_stale_ratio_pct"] = 0.0
    return out

def diff_light(pre: dict[str, object], post: dict[str, object]) -> dict[str, float]:
    out: dict[str, float] = {}
    if not pre or not post:
        return out
    for key, post_value in post.items():
        if key not in pre:
            continue
        pre_value = pre[key]
        if isinstance(pre_value, (int, float)) and isinstance(post_value, (int, float)):
            out[key] = float(post_value) - float(pre_value)
    return out

def build_maintenance_from_light_stats(stats: dict[str, str]) -> dict[str, object]:
    if not stats:
        return {}
    if not any(
        k.startswith("treedb.cache.vlog_generation.") or k.startswith("treedb.vlog_generation.")
        for k in stats.keys()
    ):
        return {}

    def stat_raw(key: str) -> str | None:
        value = stats.get(key)
        if value is not None:
            return value
        if key.startswith("treedb.cache.vlog_generation."):
            legacy_key = "treedb.vlog_generation." + key[len("treedb.cache.vlog_generation.") :]
            return stats.get(legacy_key)
        return None

    def stat_int(key: str, default: int = 0) -> int:
        return safe_int(stat_raw(key), default)

    def stat_float(key: str, default: float = 0.0) -> float:
        return safe_float(stat_raw(key), default)

    def stat_str(key: str, default: str = "") -> str:
        raw = stat_raw(key)
        if raw is None:
            return default
        return str(raw)

    out: dict[str, object] = {
        "maintenance_attempts": stat_int("treedb.cache.vlog_generation.maintenance.attempts"),
        "maintenance_acquired": stat_int("treedb.cache.vlog_generation.maintenance.acquired"),
        "maintenance_collisions": stat_int("treedb.cache.vlog_generation.maintenance.collisions"),
        "maintenance_acquired_source_periodic": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.periodic",
        ),
        "maintenance_acquired_source_bypass": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.bypass",
        ),
        "maintenance_acquired_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.checkpoint_pending",
        ),
        "maintenance_acquired_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_age_blocked",
        ),
        "maintenance_acquired_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_stage_confirm",
        ),
        "maintenance_acquired_source_other": stat_int(
            "treedb.cache.vlog_generation.maintenance.acquired.source.other",
        ),
        "maintenance_with_rewrite_source_periodic": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.periodic",
        ),
        "maintenance_with_rewrite_source_bypass": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.bypass",
        ),
        "maintenance_with_rewrite_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.checkpoint_pending",
        ),
        "maintenance_with_rewrite_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.rewrite_age_blocked",
        ),
        "maintenance_with_rewrite_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.rewrite_stage_confirm",
        ),
        "maintenance_with_rewrite_source_other": stat_int(
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.other",
        ),
        "maintenance_skip_priority_pending": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.priority_pending",
        ),
        "maintenance_skip_quiet_window": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.quiet_window",
        ),
        "maintenance_skip_age_blocked_gate": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.age_blocked_gate",
        ),
        "maintenance_skip_stage_gate_not_due": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.stage_gate_not_due",
        ),
        "maintenance_skip_stage_gate_due_reserved": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.stage_gate_due_reserved",
        ),
        "maintenance_skip_before_first_checkpoint": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.before_first_checkpoint",
        ),
        "maintenance_skip_checkpoint_inflight": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.checkpoint_inflight",
        ),
        "maintenance_skip_maintenance_phase": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.maintenance_phase",
        ),
        "maintenance_skip_wal_on_periodic": stat_int(
            "treedb.cache.vlog_generation.maintenance.skip.wal_on_periodic",
        ),
        "rewrite_runs": stat_int("treedb.cache.vlog_generation.rewrite.runs"),
        "rewrite_runs_source_periodic": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.periodic",
        ),
        "rewrite_runs_source_bypass": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.bypass",
        ),
        "rewrite_runs_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.checkpoint_pending",
        ),
        "rewrite_runs_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.rewrite_age_blocked",
        ),
        "rewrite_runs_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.rewrite_stage_confirm",
        ),
        "rewrite_runs_source_other": stat_int(
            "treedb.cache.vlog_generation.rewrite.runs.source.other",
        ),
        "rewrite_plan_runs": stat_int("treedb.cache.vlog_generation.rewrite.plan_runs"),
        "rewrite_plan_selected": stat_int("treedb.cache.vlog_generation.rewrite.plan_selected"),
        "rewrite_plan_selected_segments_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_selected_segments_total",
        ),
        "rewrite_plan_selected_bytes_stale": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_selected_bytes_stale",
        ),
        "rewrite_processed_stale_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.processed_stale_bytes",
        ),
        "rewrite_reclaimed_bytes": stat_int("treedb.cache.vlog_generation.rewrite.reclaimed_bytes"),
        "rewrite_reclaim_ratio": stat_float("treedb.cache.vlog_generation.rewrite.reclaim_ratio"),
        "rewrite_output_ratio": stat_float("treedb.cache.vlog_generation.rewrite.output_ratio"),
        "rewrite_processed_stale_ratio": stat_float("treedb.cache.vlog_generation.rewrite.processed_stale_ratio"),
        "rewrite_exec_total_ms": stat_float("treedb.cache.vlog_generation.rewrite.exec.total_ms"),
        "rewrite_bytes_in": stat_int("treedb.cache.vlog_generation.rewrite.bytes_in"),
        "rewrite_exec_bytes_in_per_sec": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.bytes_in_per_sec",
        ),
        "rewrite_exec_bytes_out_per_sec": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.bytes_out_per_sec",
        ),
        "rewrite_exec_reclaimed_bytes_per_sec": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.reclaimed_bytes_per_sec",
        ),
        "rewrite_exec_reclaimed_vs_churn_ratio": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.reclaimed_vs_churn_ratio",
        ),
        "rewrite_budget_bytes_per_sec": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.bytes_per_sec",
        ),
        "rewrite_budget_consumed_bytes_per_sec": stat_float(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_per_sec",
        ),
        "rewrite_budget_consumed_share_of_budget_pct": stat_float(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_share_of_budget_pct",
        ),
        "rewrite_budget_consumed_bytes_total_source_periodic": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.periodic",
        ),
        "rewrite_budget_consumed_bytes_total_source_bypass": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.bypass",
        ),
        "rewrite_budget_consumed_bytes_total_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.checkpoint_pending",
        ),
        "rewrite_budget_consumed_bytes_total_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.rewrite_age_blocked",
        ),
        "rewrite_budget_consumed_bytes_total_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.rewrite_stage_confirm",
        ),
        "rewrite_budget_consumed_bytes_total_source_other": stat_int(
            "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.other",
        ),
        "rewrite_no_reclaim_runs": stat_int(
            "treedb.cache.vlog_generation.rewrite.no_reclaim_runs",
        ),
        "rewrite_no_reclaim_stale_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.no_reclaim_stale_bytes",
        ),
        "rewrite_plan_canceled": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_canceled",
        ),
        "rewrite_plan_errors": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_errors",
        ),
        "rewrite_plan_empty_age_blocked": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_empty.age_blocked",
        ),
        "rewrite_plan_empty_no_selection": stat_int(
            "treedb.cache.vlog_generation.rewrite.plan_empty.no_selection",
        ),
        "rewrite_ineffective_runs": stat_int(
            "treedb.cache.vlog_generation.rewrite.ineffective_runs",
        ),
        "rewrite_ineffective_bytes_in": stat_int(
            "treedb.cache.vlog_generation.rewrite.ineffective_bytes_in",
        ),
        "rewrite_ineffective_bytes_out": stat_int(
            "treedb.cache.vlog_generation.rewrite.ineffective_bytes_out",
        ),
        "rewrite_exec_source_segments_requested_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_segments_requested_total",
        ),
        "rewrite_exec_source_segments_still_referenced_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_segments_still_referenced_total",
        ),
        "rewrite_exec_source_segments_unreferenced_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_segments_unreferenced_total",
        ),
        "rewrite_exec_source_bytes_requested_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total",
        ),
        "rewrite_exec_source_bytes_still_referenced_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_total",
        ),
        "rewrite_exec_source_bytes_unreferenced_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total",
        ),
        "rewrite_exec_source_bytes_requested_total_source_periodic": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.periodic",
        ),
        "rewrite_exec_source_bytes_requested_total_source_bypass": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.bypass",
        ),
        "rewrite_exec_source_bytes_requested_total_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.checkpoint_pending",
        ),
        "rewrite_exec_source_bytes_requested_total_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.rewrite_age_blocked",
        ),
        "rewrite_exec_source_bytes_requested_total_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.rewrite_stage_confirm",
        ),
        "rewrite_exec_source_bytes_requested_total_source_other": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.other",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_periodic": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.periodic",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_bypass": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.bypass",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_checkpoint_pending": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.checkpoint_pending",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_rewrite_age_blocked": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.rewrite_age_blocked",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_rewrite_stage_confirm": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.rewrite_stage_confirm",
        ),
        "rewrite_exec_source_bytes_unreferenced_total_source_other": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.other",
        ),
        "rewrite_queue_config_resume_max_segments": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_config.resume_max_segments",
        ),
        "rewrite_queue_config_debt_drain_max_segments": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_config.debt_drain_max_segments",
        ),
        "rewrite_queue_config_fresh_plan_debt_drain_min_segments": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_min_segments",
        ),
        "rewrite_queue_config_fresh_plan_debt_drain_max_segments": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_max_segments",
        ),
        "rewrite_queued_debt_passes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.passes",
        ),
        "rewrite_queued_debt_rewrite_started": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started",
        ),
        "rewrite_queued_debt_skip_quiet_window": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.quiet_window",
        ),
        "rewrite_queued_debt_skip_cancel_backoff": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.cancel_backoff",
        ),
        "rewrite_queued_debt_skip_ineffective_backoff": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.ineffective_backoff",
        ),
        "rewrite_queued_debt_skip_min_interval": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.min_interval",
        ),
        "rewrite_queued_debt_skip_budget_empty": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.budget_empty",
        ),
        "rewrite_queued_debt_skip_no_chunk": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.skip.no_chunk",
        ),
        "rewrite_queued_debt_exec_runs": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.runs",
        ),
        "rewrite_queued_debt_exec_segments": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.segments",
        ),
        "rewrite_queued_debt_exec_plan_bytes_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_total",
        ),
        "rewrite_queued_debt_exec_plan_bytes_live": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_live",
        ),
        "rewrite_queued_debt_exec_plan_bytes_stale": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_stale",
        ),
        "rewrite_queued_debt_exec_effective_bytes_before": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_before",
        ),
        "rewrite_queued_debt_exec_effective_bytes_after": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_after",
        ),
        "rewrite_queued_debt_exec_gc_bytes_deleted": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.gc_bytes_deleted",
        ),
        "rewrite_queued_debt_exec_reclaimed_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.reclaimed_bytes",
        ),
        "rewrite_queued_debt_exec_no_reclaim_runs": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.no_reclaim_runs",
        ),
        "rewrite_queued_debt_exec_source_bytes_requested": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.source_bytes_requested",
        ),
        "rewrite_queued_debt_exec_source_bytes_unreferenced": stat_int(
            "treedb.cache.vlog_generation.rewrite.queued_debt.exec.source_bytes_unreferenced",
        ),
        "rewrite_queue_live_hint_known": stat_str(
            "treedb.cache.vlog_generation.rewrite.queue_live_hint.known",
            "false",
        ),
        "rewrite_queue_live_hint_ids_present": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_present",
        ),
        "rewrite_queue_live_hint_ids_known": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_known",
        ),
        "rewrite_queue_live_hint_coverage_pct": stat_float(
            "treedb.cache.vlog_generation.rewrite.queue_live_hint.coverage_pct",
        ),
        "rewrite_queue_live_hint_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_live_hint.bytes",
        ),
        "rewrite_queue_run_segment_cap": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap",
        ),
        "rewrite_queue_run_segment_cap_limiter": stat_str(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter",
            "none",
        ),
        "rewrite_queue_run_segment_cap_by_budget": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget",
        ),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes",
        ),
        "rewrite_queue_run_segment_cap_checkpoint_kick": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.checkpoint_kick",
        ),
        "rewrite_queue_run_segment_cap_limiter_checkpoint_kick": stat_str(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.checkpoint_kick",
            "none",
        ),
        "rewrite_queue_run_segment_cap_by_budget_checkpoint_kick": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.checkpoint_kick",
        ),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_checkpoint_kick": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.checkpoint_kick",
        ),
        "rewrite_queue_run_segment_cap_fresh_plan": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.fresh_plan",
        ),
        "rewrite_queue_run_segment_cap_limiter_fresh_plan": stat_str(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.fresh_plan",
            "none",
        ),
        "rewrite_queue_run_segment_cap_by_budget_fresh_plan": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.fresh_plan",
        ),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_fresh_plan": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.fresh_plan",
        ),
        "rewrite_queue_run_segment_cap_decisions": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions",
        ),
        "rewrite_queue_run_segment_cap_decisions_fresh_plan": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions.fresh_plan",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_budget_tokens": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.budget_tokens",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_debt_drain_cap": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.debt_drain_cap",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_safety": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_safety",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_burst",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_queue_threshold": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_queue_threshold.fresh_plan",
        ),
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_cap": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_cap.fresh_plan",
        ),
        "rewrite_queue_progress_passes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.passes",
        ),
        "rewrite_queue_progress_segments_drained_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.segments_drained_total",
        ),
        "rewrite_queue_progress_segments_grown_total": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.segments_grown_total",
        ),
        "rewrite_queue_progress_live_bytes_known_passes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_known_passes",
        ),
        "rewrite_queue_progress_live_bytes_unknown_passes": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_unknown_passes",
        ),
        "rewrite_queue_progress_segments_delta_last": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.segments_delta_last",
        ),
        "rewrite_queue_progress_live_bytes_delta_last": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_delta_last",
        ),
        "rewrite_queue_progress_snapshot_errors": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_progress.snapshot_errors",
        ),
        "rewrite_queue_len": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_len",
        ),
        "rewrite_queue_live_bytes_after_tokens": stat_int(
            "treedb.cache.vlog_generation.rewrite.queue_live_bytes_after_tokens",
        ),
        "rewrite_queue_eta_seconds_budget": stat_float(
            "treedb.cache.vlog_generation.rewrite.queue_eta_seconds.budget",
        ),
        "rewrite_queue_eta_seconds_recent_exec": stat_float(
            "treedb.cache.vlog_generation.rewrite.queue_eta_seconds.recent_exec",
        ),
        "rewrite_exec_last_live_bytes": stat_int(
            "treedb.cache.vlog_generation.rewrite.exec.last_live_bytes",
        ),
        "rewrite_exec_last_duration_ms": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.last_duration_ms",
        ),
        "rewrite_exec_last_live_bytes_per_sec": stat_float(
            "treedb.cache.vlog_generation.rewrite.exec.last_live_bytes_per_sec",
        ),
        "rewrite_ledger_bytes_total": stat_int("treedb.cache.vlog_generation.rewrite.ledger_bytes_total"),
        "rewrite_budget_tokens_utilization_pct": stat_float(
            "treedb.cache.vlog_generation.rewrite_budget.tokens_utilization_pct",
        ),
        "checkpoint_kick_runs": stat_int(
            "treedb.cache.vlog_generation.checkpoint_kick.runs",
        ),
        "checkpoint_kick_rewrite_runs": stat_int(
            "treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs",
        ),
        "checkpoint_kick_gc_runs": stat_int(
            "treedb.cache.vlog_generation.checkpoint_kick.gc_runs",
        ),
        "checkpoint_kick_skipped_hot_no_debt": stat_int(
            "treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt",
        ),
        "checkpoint_kick_hot_no_debt_wake_runs": stat_int(
            "treedb.cache.vlog_generation.checkpoint_kick.hot_no_debt_wake.runs",
        ),
        "gc_runs": stat_int("treedb.cache.vlog_generation.gc.runs"),
        "observed_gc_pending_ids": stat_int("treedb.cache.vlog_generation.observed_gc.pending_ids"),
        "observed_gc_pending_oldest_age_ms": stat_int(
            "treedb.cache.vlog_generation.observed_gc.pending_oldest_age_ms",
        ),
        "observed_gc_latency_avg_ms": stat_float(
            "treedb.cache.vlog_generation.observed_gc.latency.avg_ms",
        ),
        "observed_gc_retry_queued": stat_int("treedb.cache.vlog_generation.observed_gc.retry_queued"),
        "observed_gc_retry_dropped": stat_int("treedb.cache.vlog_generation.observed_gc.retry_dropped"),
    }

    out["maintenance_skip_total"] = (
        safe_int(out.get("maintenance_skip_wal_on_periodic"), 0)
        + safe_int(out.get("maintenance_skip_maintenance_phase"), 0)
        + safe_int(out.get("maintenance_skip_stage_gate_not_due"), 0)
        + safe_int(out.get("maintenance_skip_stage_gate_due_reserved"), 0)
        + safe_int(out.get("maintenance_skip_age_blocked_gate"), 0)
        + safe_int(out.get("maintenance_skip_priority_pending"), 0)
        + safe_int(out.get("maintenance_skip_quiet_window"), 0)
        + safe_int(out.get("maintenance_skip_before_first_checkpoint"), 0)
        + safe_int(out.get("maintenance_skip_checkpoint_inflight"), 0)
    )
    attempts = safe_float(out.get("maintenance_attempts"), 0.0)
    acquired = safe_float(out.get("maintenance_acquired"), 0.0)
    collisions = safe_float(out.get("maintenance_collisions"), 0.0)
    out["maintenance_acquire_rate_pct"] = (100.0 * acquired / attempts) if attempts > 0 else 0.0
    out["maintenance_collision_rate_pct"] = (100.0 * collisions / attempts) if attempts > 0 else 0.0
    out["maintenance_acquired_source_periodic_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_periodic"), 0.0) / acquired if acquired > 0 else 0.0
    )
    out["maintenance_acquired_source_bypass_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_bypass"), 0.0) / acquired if acquired > 0 else 0.0
    )
    out["maintenance_acquired_source_checkpoint_pending_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_checkpoint_pending"), 0.0) / acquired if acquired > 0 else 0.0
    )
    out["maintenance_acquired_source_rewrite_age_blocked_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_rewrite_age_blocked"), 0.0) / acquired if acquired > 0 else 0.0
    )
    out["maintenance_acquired_source_rewrite_stage_confirm_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_rewrite_stage_confirm"), 0.0) / acquired if acquired > 0 else 0.0
    )
    out["maintenance_acquired_source_other_pct"] = (
        100.0 * safe_float(out.get("maintenance_acquired_source_other"), 0.0) / acquired if acquired > 0 else 0.0
    )
    queued_debt_passes = safe_float(out.get("rewrite_queued_debt_passes"), 0.0)
    queued_debt_rewrite_started = safe_float(out.get("rewrite_queued_debt_rewrite_started"), 0.0)
    out["rewrite_queued_debt_rewrite_start_rate_pct"] = (
        100.0 * queued_debt_rewrite_started / queued_debt_passes if queued_debt_passes > 0 else 0.0
    )
    out["rewrite_queued_debt_skip_total"] = (
        safe_int(out.get("rewrite_queued_debt_skip_quiet_window"), 0)
        + safe_int(out.get("rewrite_queued_debt_skip_cancel_backoff"), 0)
        + safe_int(out.get("rewrite_queued_debt_skip_ineffective_backoff"), 0)
        + safe_int(out.get("rewrite_queued_debt_skip_min_interval"), 0)
        + safe_int(out.get("rewrite_queued_debt_skip_budget_empty"), 0)
        + safe_int(out.get("rewrite_queued_debt_skip_no_chunk"), 0)
    )
    queued_debt_exec_effective_before = safe_float(out.get("rewrite_queued_debt_exec_effective_bytes_before"), 0.0)
    queued_debt_exec_reclaimed = safe_float(out.get("rewrite_queued_debt_exec_reclaimed_bytes"), 0.0)
    out["rewrite_queued_debt_exec_reclaim_ratio_pct"] = (
        100.0 * queued_debt_exec_reclaimed / queued_debt_exec_effective_before
        if queued_debt_exec_effective_before > 0
        else 0.0
    )
    queued_debt_exec_runs = safe_float(out.get("rewrite_queued_debt_exec_runs"), 0.0)
    queued_debt_exec_no_reclaim = safe_float(out.get("rewrite_queued_debt_exec_no_reclaim_runs"), 0.0)
    out["rewrite_queued_debt_exec_no_reclaim_rate_pct"] = (
        100.0 * queued_debt_exec_no_reclaim / queued_debt_exec_runs if queued_debt_exec_runs > 0 else 0.0
    )
    queued_debt_exec_source_requested = safe_float(out.get("rewrite_queued_debt_exec_source_bytes_requested"), 0.0)
    queued_debt_exec_source_unreferenced = safe_float(out.get("rewrite_queued_debt_exec_source_bytes_unreferenced"), 0.0)
    out["rewrite_queued_debt_exec_source_unreferenced_bytes_pct"] = (
        100.0 * queued_debt_exec_source_unreferenced / queued_debt_exec_source_requested
        if queued_debt_exec_source_requested > 0
        else 0.0
    )

    out["rewrite_queue_progress_live_bytes_net_drain_total"] = (
        stat_int("treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_drained_total")
        - stat_int("treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_grown_total")
    )
    out["rewrite_queue_progress_segments_net_drain_total"] = (
        safe_int(out.get("rewrite_queue_progress_segments_drained_total"), 0)
        - safe_int(out.get("rewrite_queue_progress_segments_grown_total"), 0)
    )
    queue_progress_passes = safe_float(out.get("rewrite_queue_progress_passes"), 0.0)
    known_passes = safe_float(out.get("rewrite_queue_progress_live_bytes_known_passes"), 0.0)
    out["rewrite_queue_progress_live_bytes_known_pct"] = (
        100.0 * known_passes / queue_progress_passes if queue_progress_passes > 0 else 0.0
    )

    selected_segments = safe_float(out.get("rewrite_plan_selected_segments_total"), 0.0)
    exec_segments = safe_float(out.get("rewrite_exec_source_segments_requested_total"), 0.0)
    out["rewrite_segment_realization_pct"] = (
        100.0 * exec_segments / selected_segments if selected_segments > 0 else 0.0
    )
    selected_stale = safe_float(out.get("rewrite_plan_selected_bytes_stale"), 0.0)
    processed_stale = safe_float(out.get("rewrite_processed_stale_bytes"), 0.0)
    reclaimed = safe_float(out.get("rewrite_reclaimed_bytes"), 0.0)
    out["rewrite_stale_selection_coverage_pct"] = (
        100.0 * processed_stale / selected_stale if selected_stale > 0 else 0.0
    )
    out["rewrite_immediate_reclaim_pct"] = (
        100.0 * reclaimed / processed_stale if processed_stale > 0 else 0.0
    )
    out["rewrite_stale_not_reclaimed_bytes"] = max(0.0, processed_stale - reclaimed)
    rewrite_exec_secs = safe_float(out.get("rewrite_exec_total_ms"), 0.0) / 1000.0
    bytes_in = safe_float(out.get("rewrite_bytes_in"), 0.0)
    out["rewrite_exec_throughput_bytes_per_sec"] = (
        bytes_in / rewrite_exec_secs if rewrite_exec_secs > 0 else 0.0
    )
    out["rewrite_reclaimed_minus_bytes_in"] = reclaimed - bytes_in
    out["rewrite_reclaimed_per_bytes_in_pct"] = (
        100.0 * reclaimed / bytes_in if bytes_in > 0 else 0.0
    )
    budget_bytes_per_sec = safe_float(out.get("rewrite_budget_bytes_per_sec"), 0.0)
    reclaimed_per_sec = safe_float(out.get("rewrite_exec_reclaimed_bytes_per_sec"), 0.0)
    out["rewrite_reclaimed_share_of_budget_pct"] = (
        100.0 * reclaimed_per_sec / budget_bytes_per_sec if budget_bytes_per_sec > 0 else 0.0
    )
    source_bytes_requested = safe_float(out.get("rewrite_exec_source_bytes_requested_total"), 0.0)
    source_bytes_unref = safe_float(out.get("rewrite_exec_source_bytes_unreferenced_total"), 0.0)
    source_bytes_still_ref = safe_float(out.get("rewrite_exec_source_bytes_still_referenced_total"), 0.0)
    out["rewrite_source_unreferenced_bytes_pct"] = (
        100.0 * source_bytes_unref / source_bytes_requested if source_bytes_requested > 0 else 0.0
    )
    out["rewrite_source_still_referenced_bytes_pct"] = (
        100.0 * source_bytes_still_ref / source_bytes_requested if source_bytes_requested > 0 else 0.0
    )
    checkpoint_like_runs = (
        safe_float(out.get("rewrite_runs_source_bypass"), 0.0)
        + safe_float(out.get("rewrite_runs_source_checkpoint_pending"), 0.0)
    )
    rewrite_runs_total = safe_float(out.get("rewrite_runs"), 0.0)
    out["rewrite_checkpoint_like_runs"] = checkpoint_like_runs
    out["rewrite_non_checkpoint_runs"] = max(0.0, rewrite_runs_total - checkpoint_like_runs)
    out["rewrite_checkpoint_like_run_share_pct"] = (
        100.0 * checkpoint_like_runs / rewrite_runs_total if rewrite_runs_total > 0 else 0.0
    )

    budget_consumed_total = safe_float(
        stat_int("treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total"),
        0.0,
    )
    checkpoint_like_budget_consumed = (
        safe_float(out.get("rewrite_budget_consumed_bytes_total_source_bypass"), 0.0)
        + safe_float(out.get("rewrite_budget_consumed_bytes_total_source_checkpoint_pending"), 0.0)
    )
    out["rewrite_checkpoint_like_budget_consumed_bytes_total"] = checkpoint_like_budget_consumed
    out["rewrite_non_checkpoint_budget_consumed_bytes_total"] = max(
        0.0,
        budget_consumed_total - checkpoint_like_budget_consumed,
    )
    out["rewrite_checkpoint_like_budget_share_pct"] = (
        100.0 * checkpoint_like_budget_consumed / budget_consumed_total
        if budget_consumed_total > 0
        else 0.0
    )

    checkpoint_like_source_bytes_requested = (
        safe_float(out.get("rewrite_exec_source_bytes_requested_total_source_bypass"), 0.0)
        + safe_float(out.get("rewrite_exec_source_bytes_requested_total_source_checkpoint_pending"), 0.0)
    )
    checkpoint_like_source_bytes_unreferenced = (
        safe_float(out.get("rewrite_exec_source_bytes_unreferenced_total_source_bypass"), 0.0)
        + safe_float(out.get("rewrite_exec_source_bytes_unreferenced_total_source_checkpoint_pending"), 0.0)
    )
    out["rewrite_checkpoint_like_source_bytes_requested_total"] = checkpoint_like_source_bytes_requested
    out["rewrite_checkpoint_like_source_bytes_unreferenced_total"] = checkpoint_like_source_bytes_unreferenced
    out["rewrite_non_checkpoint_source_bytes_requested_total"] = max(
        0.0,
        source_bytes_requested - checkpoint_like_source_bytes_requested,
    )
    out["rewrite_non_checkpoint_source_bytes_unreferenced_total"] = max(
        0.0,
        source_bytes_unref - checkpoint_like_source_bytes_unreferenced,
    )
    out["rewrite_checkpoint_like_source_unreferenced_bytes_pct"] = (
        100.0 * checkpoint_like_source_bytes_unreferenced / checkpoint_like_source_bytes_requested
        if checkpoint_like_source_bytes_requested > 0
        else 0.0
    )
    non_checkpoint_source_bytes_requested = safe_float(
        out.get("rewrite_non_checkpoint_source_bytes_requested_total"),
        0.0,
    )
    non_checkpoint_source_bytes_unreferenced = safe_float(
        out.get("rewrite_non_checkpoint_source_bytes_unreferenced_total"),
        0.0,
    )
    out["rewrite_non_checkpoint_source_unreferenced_bytes_pct"] = (
        100.0 * non_checkpoint_source_bytes_unreferenced / non_checkpoint_source_bytes_requested
        if non_checkpoint_source_bytes_requested > 0
        else 0.0
    )

    observed_gc_queued = stat_int("treedb.cache.vlog_generation.observed_gc.queued_ids")
    observed_gc_taken = stat_int("treedb.cache.vlog_generation.observed_gc.taken_ids")
    out["observed_gc_drain_pct"] = (
        (100.0 * float(observed_gc_taken) / float(observed_gc_queued))
        if observed_gc_queued > 0
        else 0.0
    )
    checkpoint_kick_runs = safe_float(out.get("checkpoint_kick_runs"), 0.0)
    checkpoint_kick_rewrite_runs = safe_float(out.get("checkpoint_kick_rewrite_runs"), 0.0)
    out["checkpoint_kick_rewrite_rate_pct"] = (
        100.0 * checkpoint_kick_rewrite_runs / checkpoint_kick_runs if checkpoint_kick_runs > 0 else 0.0
    )
    return out

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
maintenance_source = "none"
if analyze_json_path.is_file():
    try:
        payload = json.loads(analyze_json_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            summary = payload.get("summary")
            if isinstance(summary, dict):
                maintenance = summary
                if maintenance_source_path.is_file():
                    raw_source = maintenance_source_path.read_text(encoding="utf-8", errors="replace").strip()
                    if raw_source:
                        maintenance_source = raw_source
                if maintenance_source == "none":
                    maintenance_source = "diagnostics_json"
    except Exception:
        maintenance = {}
        maintenance_source = "none"

light_pre_stats = parse_treemap_stats(light_stats_pre_path) if light_stats_pre_rc == 0 else {}
light_post_stats = parse_treemap_stats(light_stats_post_path) if light_stats_post_rc == 0 else {}
maintenance_light_pre = build_light_summary(light_pre_stats)
maintenance_light_post = build_light_summary(light_post_stats)
maintenance_light_delta = diff_light(maintenance_light_pre, maintenance_light_post)
if not maintenance:
    maintenance_light_fallback = build_maintenance_from_light_stats(light_post_stats)
    if maintenance_light_fallback:
        maintenance = maintenance_light_fallback
        maintenance_source = "light_stats_post"

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
    "build": build_meta,
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
    "maintenance_summary_source": maintenance_source,
    "maintenance_summary_is_live_runtime": maintenance_source in {"diagnostics_json", "live_debug_vars"},
    "maintenance_summary": maintenance,
    "maintenance_light": {
        "capture": {
            "pre_exit_code": light_stats_pre_rc,
            "pre_file": str(light_stats_pre_path) if light_stats_pre_path.is_file() else "",
            "post_exit_code": light_stats_post_rc,
            "post_file": str(light_stats_post_path) if light_stats_post_path.is_file() else "",
        },
        "pre": maintenance_light_pre,
        "post": maintenance_light_post,
        "delta": maintenance_light_delta,
    },
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
  python3 - "$OUT" "$SIZE_TOLERANCE_BYTES" "$TIME_TOLERANCE_SECONDS" "$MIN_PAIRS" "$CLEAR_WIN_PAIRS" "$CLEAR_LOSS_PAIRS" "$MAX_PAIRS" "$STOP_ON_CLEAR" "$LOW_SIGNAL_MIN_PAIRS" "$LOW_SIGNAL_NEUTRAL_STREAK" "$INVALID_PAIR_STREAK_STOP" "$BLOCK_DRIFT_TOLERANCE" "$decision_json" "$SCORING_MODE" "$ALLOW_DRIFT_SCORING" "$AB_POLICY" "$COMPOSITE_WEIGHT_TIME" "$COMPOSITE_WEIGHT_SIZE" "$COMPOSITE_STOP_ON_CLEAR" "$COMPOSITE_MIN_PAIRS" "$COMPOSITE_CLEAR_WIN_PCT" "$COMPOSITE_CLEAR_LOSS_PCT" <<'PY'
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
ab_policy = str(sys.argv[16]).strip().lower()
composite_weight_time = float(sys.argv[17])
composite_weight_size = float(sys.argv[18])
composite_stop_on_clear = str(sys.argv[19]).strip() == "1"
composite_min_pairs = int(sys.argv[20])
composite_clear_win_pct = float(sys.argv[21])
composite_clear_loss_pct = float(sys.argv[22])
if composite_weight_time < 0.0 or composite_weight_size < 0.0:
    raise SystemExit("COMPOSITE_WEIGHT_TIME and COMPOSITE_WEIGHT_SIZE must be >= 0")
if (composite_weight_time + composite_weight_size) <= 0.0:
    raise SystemExit("COMPOSITE weights must sum to > 0")

run_files = sorted(out.glob("runs/*/run.json"))
runs = []
for p in run_files:
    try:
        runs.append(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        continue

runs.sort(key=lambda r: (int(r.get("pair_index", 0)), str(r.get("variant", ""))))

def as_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def median(values):
    xs = sorted(v for v in values if v is not None)
    n = len(xs)
    if n == 0:
        return None
    mid = n // 2
    if (n % 2) == 1:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0

def mean(values):
    xs = [v for v in values if v is not None]
    if not xs:
        return None
    return sum(xs) / float(len(xs))

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
        "maintenance_attempts",
        "maintenance_acquired",
        "maintenance_collisions",
        "maintenance_acquire_rate_pct",
        "maintenance_collision_rate_pct",
        "maintenance_skip_total",
        "maintenance_skip_priority_pending",
        "maintenance_skip_quiet_window",
        "maintenance_skip_age_blocked_gate",
        "maintenance_skip_stage_gate_not_due",
        "maintenance_skip_stage_gate_due_reserved",
        "maintenance_skip_before_first_checkpoint",
        "maintenance_acquired_source_periodic",
        "maintenance_acquired_source_bypass",
        "maintenance_acquired_source_checkpoint_pending",
        "maintenance_acquired_source_rewrite_age_blocked",
        "maintenance_acquired_source_rewrite_stage_confirm",
        "maintenance_acquired_source_other",
        "maintenance_acquired_source_periodic_pct",
        "maintenance_acquired_source_bypass_pct",
        "maintenance_acquired_source_checkpoint_pending_pct",
        "maintenance_acquired_source_rewrite_age_blocked_pct",
        "maintenance_acquired_source_rewrite_stage_confirm_pct",
        "maintenance_acquired_source_other_pct",
        "maintenance_with_rewrite_source_periodic",
        "maintenance_with_rewrite_source_bypass",
        "maintenance_with_rewrite_source_checkpoint_pending",
        "maintenance_with_rewrite_source_rewrite_age_blocked",
        "maintenance_with_rewrite_source_rewrite_stage_confirm",
        "maintenance_with_rewrite_source_other",
        "rewrite_runs",
        "rewrite_plan_runs",
        "rewrite_plan_selected",
        "rewrite_exec_source_segments_requested_total",
        "rewrite_exec_source_segments_still_referenced_total",
        "rewrite_exec_source_segments_unreferenced_total",
        "rewrite_exec_source_bytes_requested_total",
        "rewrite_exec_source_bytes_still_referenced_total",
        "rewrite_exec_source_bytes_unreferenced_total",
        "rewrite_segment_realization_pct",
        "rewrite_stale_selection_coverage_pct",
        "rewrite_immediate_reclaim_pct",
        "rewrite_stale_not_reclaimed_bytes",
        "rewrite_reclaim_ratio",
        "rewrite_output_ratio",
        "rewrite_processed_stale_ratio",
        "rewrite_reclaimed_minus_bytes_in",
        "rewrite_reclaimed_per_bytes_in_pct",
        "rewrite_exec_throughput_bytes_per_sec",
        "rewrite_exec_bytes_in_per_sec",
        "rewrite_exec_bytes_out_per_sec",
        "rewrite_exec_reclaimed_bytes_per_sec",
        "rewrite_exec_reclaimed_vs_churn_ratio",
        "rewrite_source_unreferenced_bytes_pct",
        "rewrite_source_still_referenced_bytes_pct",
        "rewrite_budget_bytes_per_sec",
        "rewrite_budget_consumed_bytes_per_sec",
        "rewrite_budget_consumed_share_of_budget_pct",
        "rewrite_checkpoint_like_runs",
        "rewrite_non_checkpoint_runs",
        "rewrite_checkpoint_like_run_share_pct",
        "rewrite_checkpoint_like_budget_consumed_bytes_total",
        "rewrite_non_checkpoint_budget_consumed_bytes_total",
        "rewrite_checkpoint_like_budget_share_pct",
        "rewrite_checkpoint_like_source_bytes_requested_total",
        "rewrite_checkpoint_like_source_bytes_unreferenced_total",
        "rewrite_checkpoint_like_source_unreferenced_bytes_pct",
        "rewrite_non_checkpoint_source_bytes_requested_total",
        "rewrite_non_checkpoint_source_bytes_unreferenced_total",
        "rewrite_non_checkpoint_source_unreferenced_bytes_pct",
        "rewrite_reclaimed_share_of_budget_pct",
        "rewrite_no_reclaim_runs",
        "rewrite_no_reclaim_stale_bytes",
        "rewrite_plan_canceled",
        "rewrite_plan_errors",
        "rewrite_plan_empty_age_blocked",
        "rewrite_plan_empty_no_selection",
        "rewrite_ineffective_runs",
        "rewrite_ineffective_bytes_in",
        "rewrite_ineffective_bytes_out",
        "rewrite_queue_config_resume_max_segments",
        "rewrite_queue_config_debt_drain_max_segments",
        "rewrite_queue_config_fresh_plan_debt_drain_min_segments",
        "rewrite_queue_config_fresh_plan_debt_drain_max_segments",
        "rewrite_queued_debt_passes",
        "rewrite_queued_debt_rewrite_started",
        "rewrite_queued_debt_rewrite_start_rate_pct",
        "rewrite_queued_debt_skip_total",
        "rewrite_queued_debt_skip_quiet_window",
        "rewrite_queued_debt_skip_cancel_backoff",
        "rewrite_queued_debt_skip_ineffective_backoff",
        "rewrite_queued_debt_skip_min_interval",
        "rewrite_queued_debt_skip_budget_empty",
        "rewrite_queued_debt_skip_no_chunk",
        "rewrite_queued_debt_exec_runs",
        "rewrite_queued_debt_exec_segments",
        "rewrite_queued_debt_exec_plan_bytes_total",
        "rewrite_queued_debt_exec_plan_bytes_live",
        "rewrite_queued_debt_exec_plan_bytes_stale",
        "rewrite_queued_debt_exec_effective_bytes_before",
        "rewrite_queued_debt_exec_effective_bytes_after",
        "rewrite_queued_debt_exec_gc_bytes_deleted",
        "rewrite_queued_debt_exec_reclaimed_bytes",
        "rewrite_queued_debt_exec_no_reclaim_runs",
        "rewrite_queued_debt_exec_source_bytes_requested",
        "rewrite_queued_debt_exec_source_bytes_unreferenced",
        "rewrite_queued_debt_exec_reclaim_ratio_pct",
        "rewrite_queued_debt_exec_no_reclaim_rate_pct",
        "rewrite_queued_debt_exec_source_unreferenced_bytes_pct",
        "rewrite_queue_live_hint_known",
        "rewrite_queue_live_hint_ids_present",
        "rewrite_queue_live_hint_ids_known",
        "rewrite_queue_live_hint_coverage_pct",
        "rewrite_queue_live_hint_bytes",
        "rewrite_queue_run_segment_cap",
        "rewrite_queue_run_segment_cap_limiter",
        "rewrite_queue_run_segment_cap_by_budget",
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes",
        "rewrite_queue_run_segment_cap_checkpoint_kick",
        "rewrite_queue_run_segment_cap_limiter_checkpoint_kick",
        "rewrite_queue_run_segment_cap_by_budget_checkpoint_kick",
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_checkpoint_kick",
        "rewrite_queue_run_segment_cap_fresh_plan",
        "rewrite_queue_run_segment_cap_limiter_fresh_plan",
        "rewrite_queue_run_segment_cap_by_budget_fresh_plan",
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_fresh_plan",
        "rewrite_queue_run_segment_cap_decisions",
        "rewrite_queue_run_segment_cap_decisions_fresh_plan",
        "rewrite_queue_run_segment_cap_limiter_count_budget_tokens",
        "rewrite_queue_run_segment_cap_limiter_count_debt_drain_cap",
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_safety",
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_queue_threshold",
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_cap",
        "rewrite_queue_progress_passes",
        "rewrite_queue_progress_segments_drained_total",
        "rewrite_queue_progress_segments_grown_total",
        "rewrite_queue_progress_segments_net_drain_total",
        "rewrite_queue_progress_segments_delta_last",
        "rewrite_queue_progress_live_bytes_known_pct",
        "rewrite_queue_progress_live_bytes_net_drain_total",
        "rewrite_queue_progress_live_bytes_delta_last",
        "rewrite_queue_progress_snapshot_errors",
        "rewrite_queue_len",
        "rewrite_queue_live_bytes_after_tokens",
        "rewrite_queue_eta_seconds_budget",
        "rewrite_queue_eta_seconds_recent_exec",
        "rewrite_exec_last_live_bytes",
        "rewrite_exec_last_duration_ms",
        "rewrite_exec_last_live_bytes_per_sec",
        "rewrite_ledger_bytes_total",
        "rewrite_budget_tokens_utilization_pct",
        "checkpoint_kick_runs",
        "checkpoint_kick_rewrite_runs",
        "checkpoint_kick_rewrite_rate_pct",
        "checkpoint_kick_gc_runs",
        "checkpoint_kick_skipped_hot_no_debt",
        "checkpoint_kick_hot_no_debt_wake_runs",
        "gc_runs",
        "observed_gc_pending_ids",
        "observed_gc_pending_oldest_age_ms",
        "observed_gc_latency_avg_ms",
        "observed_gc_drain_pct",
        "observed_gc_retry_queued",
        "observed_gc_retry_dropped",
        "maintenance_summary_source",
        "light_pre_bytes_total",
        "light_pre_bytes_stale",
        "light_pre_segments_total",
        "light_pre_stale_ratio_pct",
        "light_post_bytes_total",
        "light_post_bytes_stale",
        "light_post_segments_total",
        "light_post_stale_ratio_pct",
        "light_delta_bytes_total",
        "light_delta_bytes_stale",
        "light_delta_segments_total",
        "treedb_open_profile",
        "use_local_tree_stack",
        "local_gomap_dir",
        "gomap_git_head",
        "gomap_git_describe",
        "gomap_git_dirty",
    ])
    for r in runs:
        m = r.get("metrics", {}) or {}
        s = r.get("sizes", {}) or {}
        rw = r.get("rewrite", {}) or {}
        build = r.get("build", {}) or {}
        summary = r.get("maintenance_summary", {}) or {}
        light = r.get("maintenance_light", {}) or {}
        light_pre = {}
        light_post = {}
        light_delta = {}
        if isinstance(light, dict):
            maybe_pre = light.get("pre", {})
            maybe_post = light.get("post", {})
            maybe_delta = light.get("delta", {})
            if isinstance(maybe_pre, dict):
                light_pre = maybe_pre
            if isinstance(maybe_post, dict):
                light_post = maybe_post
            if isinstance(maybe_delta, dict):
                light_delta = maybe_delta
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
            summary.get("maintenance_attempts", 0),
            summary.get("maintenance_acquired", 0),
            summary.get("maintenance_collisions", 0),
            summary.get("maintenance_acquire_rate_pct", 0),
            summary.get("maintenance_collision_rate_pct", 0),
            summary.get("maintenance_skip_total", 0),
            summary.get("maintenance_skip_priority_pending", 0),
            summary.get("maintenance_skip_quiet_window", 0),
            summary.get("maintenance_skip_age_blocked_gate", 0),
            summary.get("maintenance_skip_stage_gate_not_due", 0),
            summary.get("maintenance_skip_stage_gate_due_reserved", 0),
            summary.get("maintenance_skip_before_first_checkpoint", 0),
            summary.get("maintenance_acquired_source_periodic", 0),
            summary.get("maintenance_acquired_source_bypass", 0),
            summary.get("maintenance_acquired_source_checkpoint_pending", 0),
            summary.get("maintenance_acquired_source_rewrite_age_blocked", 0),
            summary.get("maintenance_acquired_source_rewrite_stage_confirm", 0),
            summary.get("maintenance_acquired_source_other", 0),
            summary.get("maintenance_acquired_source_periodic_pct", 0),
            summary.get("maintenance_acquired_source_bypass_pct", 0),
            summary.get("maintenance_acquired_source_checkpoint_pending_pct", 0),
            summary.get("maintenance_acquired_source_rewrite_age_blocked_pct", 0),
            summary.get("maintenance_acquired_source_rewrite_stage_confirm_pct", 0),
            summary.get("maintenance_acquired_source_other_pct", 0),
            summary.get("maintenance_with_rewrite_source_periodic", 0),
            summary.get("maintenance_with_rewrite_source_bypass", 0),
            summary.get("maintenance_with_rewrite_source_checkpoint_pending", 0),
            summary.get("maintenance_with_rewrite_source_rewrite_age_blocked", 0),
            summary.get("maintenance_with_rewrite_source_rewrite_stage_confirm", 0),
            summary.get("maintenance_with_rewrite_source_other", 0),
            summary.get("rewrite_runs", 0),
            summary.get("rewrite_plan_runs", 0),
            summary.get("rewrite_plan_selected", 0),
            summary.get("rewrite_exec_source_segments_requested_total", 0),
            summary.get("rewrite_exec_source_segments_still_referenced_total", 0),
            summary.get("rewrite_exec_source_segments_unreferenced_total", 0),
            summary.get("rewrite_exec_source_bytes_requested_total", 0),
            summary.get("rewrite_exec_source_bytes_still_referenced_total", 0),
            summary.get("rewrite_exec_source_bytes_unreferenced_total", 0),
            summary.get("rewrite_segment_realization_pct", 0),
            summary.get("rewrite_stale_selection_coverage_pct", 0),
            summary.get("rewrite_immediate_reclaim_pct", 0),
            summary.get("rewrite_stale_not_reclaimed_bytes", 0),
            summary.get("rewrite_reclaim_ratio", 0),
            summary.get("rewrite_output_ratio", 0),
            summary.get("rewrite_processed_stale_ratio", 0),
            summary.get("rewrite_reclaimed_minus_bytes_in", 0),
            summary.get("rewrite_reclaimed_per_bytes_in_pct", 0),
            summary.get("rewrite_exec_throughput_bytes_per_sec", 0),
            summary.get("rewrite_exec_bytes_in_per_sec", 0),
            summary.get("rewrite_exec_bytes_out_per_sec", 0),
            summary.get("rewrite_exec_reclaimed_bytes_per_sec", 0),
            summary.get("rewrite_exec_reclaimed_vs_churn_ratio", 0),
            summary.get("rewrite_source_unreferenced_bytes_pct", 0),
            summary.get("rewrite_source_still_referenced_bytes_pct", 0),
            summary.get("rewrite_budget_bytes_per_sec", 0),
            summary.get("rewrite_budget_consumed_bytes_per_sec", 0),
            summary.get("rewrite_budget_consumed_share_of_budget_pct", 0),
            summary.get("rewrite_checkpoint_like_runs", 0),
            summary.get("rewrite_non_checkpoint_runs", 0),
            summary.get("rewrite_checkpoint_like_run_share_pct", 0),
            summary.get("rewrite_checkpoint_like_budget_consumed_bytes_total", 0),
            summary.get("rewrite_non_checkpoint_budget_consumed_bytes_total", 0),
            summary.get("rewrite_checkpoint_like_budget_share_pct", 0),
            summary.get("rewrite_checkpoint_like_source_bytes_requested_total", 0),
            summary.get("rewrite_checkpoint_like_source_bytes_unreferenced_total", 0),
            summary.get("rewrite_checkpoint_like_source_unreferenced_bytes_pct", 0),
            summary.get("rewrite_non_checkpoint_source_bytes_requested_total", 0),
            summary.get("rewrite_non_checkpoint_source_bytes_unreferenced_total", 0),
            summary.get("rewrite_non_checkpoint_source_unreferenced_bytes_pct", 0),
            summary.get("rewrite_reclaimed_share_of_budget_pct", 0),
            summary.get("rewrite_no_reclaim_runs", 0),
            summary.get("rewrite_no_reclaim_stale_bytes", 0),
            summary.get("rewrite_plan_canceled", 0),
            summary.get("rewrite_plan_errors", 0),
            summary.get("rewrite_plan_empty_age_blocked", 0),
            summary.get("rewrite_plan_empty_no_selection", 0),
            summary.get("rewrite_ineffective_runs", 0),
            summary.get("rewrite_ineffective_bytes_in", 0),
            summary.get("rewrite_ineffective_bytes_out", 0),
            summary.get("rewrite_queue_config_resume_max_segments", 0),
            summary.get("rewrite_queue_config_debt_drain_max_segments", 0),
            summary.get("rewrite_queue_config_fresh_plan_debt_drain_min_segments", 0),
            summary.get("rewrite_queue_config_fresh_plan_debt_drain_max_segments", 0),
            summary.get("rewrite_queued_debt_passes", 0),
            summary.get("rewrite_queued_debt_rewrite_started", 0),
            summary.get("rewrite_queued_debt_rewrite_start_rate_pct", 0),
            summary.get("rewrite_queued_debt_skip_total", 0),
            summary.get("rewrite_queued_debt_skip_quiet_window", 0),
            summary.get("rewrite_queued_debt_skip_cancel_backoff", 0),
            summary.get("rewrite_queued_debt_skip_ineffective_backoff", 0),
            summary.get("rewrite_queued_debt_skip_min_interval", 0),
            summary.get("rewrite_queued_debt_skip_budget_empty", 0),
            summary.get("rewrite_queued_debt_skip_no_chunk", 0),
            summary.get("rewrite_queued_debt_exec_runs", 0),
            summary.get("rewrite_queued_debt_exec_segments", 0),
            summary.get("rewrite_queued_debt_exec_plan_bytes_total", 0),
            summary.get("rewrite_queued_debt_exec_plan_bytes_live", 0),
            summary.get("rewrite_queued_debt_exec_plan_bytes_stale", 0),
            summary.get("rewrite_queued_debt_exec_effective_bytes_before", 0),
            summary.get("rewrite_queued_debt_exec_effective_bytes_after", 0),
            summary.get("rewrite_queued_debt_exec_gc_bytes_deleted", 0),
            summary.get("rewrite_queued_debt_exec_reclaimed_bytes", 0),
            summary.get("rewrite_queued_debt_exec_no_reclaim_runs", 0),
            summary.get("rewrite_queued_debt_exec_source_bytes_requested", 0),
            summary.get("rewrite_queued_debt_exec_source_bytes_unreferenced", 0),
            summary.get("rewrite_queued_debt_exec_reclaim_ratio_pct", 0),
            summary.get("rewrite_queued_debt_exec_no_reclaim_rate_pct", 0),
            summary.get("rewrite_queued_debt_exec_source_unreferenced_bytes_pct", 0),
            summary.get("rewrite_queue_live_hint_known", "false"),
            summary.get("rewrite_queue_live_hint_ids_present", 0),
            summary.get("rewrite_queue_live_hint_ids_known", 0),
            summary.get("rewrite_queue_live_hint_coverage_pct", 0),
            summary.get("rewrite_queue_live_hint_bytes", 0),
            summary.get("rewrite_queue_run_segment_cap", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter", "none"),
            summary.get("rewrite_queue_run_segment_cap_by_budget", 0),
            summary.get("rewrite_queue_run_segment_cap_per_segment_budget_bytes", 0),
            summary.get("rewrite_queue_run_segment_cap_checkpoint_kick", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_checkpoint_kick", "none"),
            summary.get("rewrite_queue_run_segment_cap_by_budget_checkpoint_kick", 0),
            summary.get("rewrite_queue_run_segment_cap_per_segment_budget_bytes_checkpoint_kick", 0),
            summary.get("rewrite_queue_run_segment_cap_fresh_plan", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_fresh_plan", "none"),
            summary.get("rewrite_queue_run_segment_cap_by_budget_fresh_plan", 0),
            summary.get("rewrite_queue_run_segment_cap_per_segment_budget_bytes_fresh_plan", 0),
            summary.get("rewrite_queue_run_segment_cap_decisions", 0),
            summary.get("rewrite_queue_run_segment_cap_decisions_fresh_plan", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_budget_tokens", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_debt_drain_cap", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_safety", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_fresh_plan_queue_threshold", 0),
            summary.get("rewrite_queue_run_segment_cap_limiter_count_fresh_plan_cap", 0),
            summary.get("rewrite_queue_progress_passes", 0),
            summary.get("rewrite_queue_progress_segments_drained_total", 0),
            summary.get("rewrite_queue_progress_segments_grown_total", 0),
            summary.get("rewrite_queue_progress_segments_net_drain_total", 0),
            summary.get("rewrite_queue_progress_segments_delta_last", 0),
            summary.get("rewrite_queue_progress_live_bytes_known_pct", 0),
            summary.get("rewrite_queue_progress_live_bytes_net_drain_total", 0),
            summary.get("rewrite_queue_progress_live_bytes_delta_last", 0),
            summary.get("rewrite_queue_progress_snapshot_errors", 0),
            summary.get("rewrite_queue_len", 0),
            summary.get("rewrite_queue_live_bytes_after_tokens", 0),
            summary.get("rewrite_queue_eta_seconds_budget", 0),
            summary.get("rewrite_queue_eta_seconds_recent_exec", 0),
            summary.get("rewrite_exec_last_live_bytes", 0),
            summary.get("rewrite_exec_last_duration_ms", 0),
            summary.get("rewrite_exec_last_live_bytes_per_sec", 0),
            summary.get("rewrite_ledger_bytes_total", 0),
            summary.get("rewrite_budget_tokens_utilization_pct", 0),
            summary.get("checkpoint_kick_runs", 0),
            summary.get("checkpoint_kick_rewrite_runs", 0),
            summary.get("checkpoint_kick_rewrite_rate_pct", 0),
            summary.get("checkpoint_kick_gc_runs", 0),
            summary.get("checkpoint_kick_skipped_hot_no_debt", 0),
            summary.get("checkpoint_kick_hot_no_debt_wake_runs", 0),
            summary.get("gc_runs", 0),
            summary.get("observed_gc_pending_ids", 0),
            summary.get("observed_gc_pending_oldest_age_ms", 0),
            summary.get("observed_gc_latency_avg_ms", 0),
            summary.get("observed_gc_drain_pct", 0),
            summary.get("observed_gc_retry_queued", 0),
            summary.get("observed_gc_retry_dropped", 0),
            r.get("maintenance_summary_source", ""),
            light_pre.get("bytes_total_total"),
            light_pre.get("bytes_stale_total"),
            light_pre.get("segments_total"),
            light_pre.get("bytes_stale_ratio_pct"),
            light_post.get("bytes_total_total"),
            light_post.get("bytes_stale_total"),
            light_post.get("segments_total"),
            light_post.get("bytes_stale_ratio_pct"),
            light_delta.get("bytes_total_total"),
            light_delta.get("bytes_stale_total"),
            light_delta.get("segments_total"),
            build.get("treedb_open_profile", ""),
            build.get("use_local_tree_stack", ""),
            build.get("local_gomap_dir", ""),
            build.get("gomap_git_head", ""),
            build.get("gomap_git_describe", ""),
            build.get("gomap_git_dirty", ""),
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
    ctrl_summary = ctrl.get("maintenance_summary", {}) or {}
    cand_summary = cand.get("maintenance_summary", {}) or {}
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
            "control_t_sync_seconds": None,
            "candidate_t_sync_seconds": None,
            "control_t_total_seconds": None,
            "candidate_t_total_seconds": None,
            "control_s_sync_app_bytes": None,
            "candidate_s_sync_app_bytes": None,
            "control_s_post_wal_bytes": None,
            "candidate_s_post_wal_bytes": None,
            "control_max_rss_kb": None,
            "candidate_max_rss_kb": None,
            "composite_score_pct": None,
            "control_rewrite_reclaimed_vs_churn_ratio": None,
            "candidate_rewrite_reclaimed_vs_churn_ratio": None,
            "delta_rewrite_reclaimed_vs_churn_ratio": None,
            "control_rewrite_reclaimed_share_of_budget_pct": None,
            "candidate_rewrite_reclaimed_share_of_budget_pct": None,
            "delta_rewrite_reclaimed_share_of_budget_pct": None,
            "control_rewrite_budget_consumed_share_of_budget_pct": None,
            "candidate_rewrite_budget_consumed_share_of_budget_pct": None,
            "delta_rewrite_budget_consumed_share_of_budget_pct": None,
            "control_rewrite_ineffective_runs": None,
            "candidate_rewrite_ineffective_runs": None,
            "delta_rewrite_ineffective_runs": None,
            "control_observed_gc_pending_ids": None,
            "candidate_observed_gc_pending_ids": None,
            "delta_observed_gc_pending_ids": None,
            "control_rewrite_queue_eta_seconds_budget": None,
            "candidate_rewrite_queue_eta_seconds_budget": None,
            "delta_rewrite_queue_eta_seconds_budget": None,
            "control_rewrite_queue_live_bytes_after_tokens": None,
            "candidate_rewrite_queue_live_bytes_after_tokens": None,
            "delta_rewrite_queue_live_bytes_after_tokens": None,
            "control_checkpoint_kick_skipped_hot_no_debt": None,
            "candidate_checkpoint_kick_skipped_hot_no_debt": None,
            "delta_checkpoint_kick_skipped_hot_no_debt": None,
            "control_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": None,
            "candidate_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": None,
            "delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": None,
            "control_rewrite_checkpoint_like_budget_share_pct": None,
            "candidate_rewrite_checkpoint_like_budget_share_pct": None,
            "delta_rewrite_checkpoint_like_budget_share_pct": None,
            "control_rewrite_checkpoint_like_source_unreferenced_bytes_pct": None,
            "candidate_rewrite_checkpoint_like_source_unreferenced_bytes_pct": None,
            "delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct": None,
            "control_rewrite_non_checkpoint_source_unreferenced_bytes_pct": None,
            "candidate_rewrite_non_checkpoint_source_unreferenced_bytes_pct": None,
            "delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct": None,
            "control_rewrite_queued_debt_passes": None,
            "candidate_rewrite_queued_debt_passes": None,
            "delta_rewrite_queued_debt_passes": None,
            "control_rewrite_queued_debt_rewrite_started": None,
            "candidate_rewrite_queued_debt_rewrite_started": None,
            "delta_rewrite_queued_debt_rewrite_started": None,
            "control_rewrite_queued_debt_rewrite_start_rate_pct": None,
            "candidate_rewrite_queued_debt_rewrite_start_rate_pct": None,
            "delta_rewrite_queued_debt_rewrite_start_rate_pct": None,
            "control_rewrite_queued_debt_skip_budget_empty": None,
            "candidate_rewrite_queued_debt_skip_budget_empty": None,
            "delta_rewrite_queued_debt_skip_budget_empty": None,
            "control_rewrite_queued_debt_skip_no_chunk": None,
            "candidate_rewrite_queued_debt_skip_no_chunk": None,
            "delta_rewrite_queued_debt_skip_no_chunk": None,
            "control_valid": ctrl_valid,
            "candidate_valid": cand_valid,
            "control_invalid_reason": ctrl_reason,
            "candidate_invalid_reason": cand_reason,
            "pair_invalid_reason": "run_invalid",
            "drift_scored_outcome": "n/a",
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
    cand_reclaimed_vs_churn = cand_summary.get("rewrite_exec_reclaimed_vs_churn_ratio")
    base_reclaimed_vs_churn = ctrl_summary.get("rewrite_exec_reclaimed_vs_churn_ratio")
    d_reclaimed_vs_churn = delta(cand_reclaimed_vs_churn, base_reclaimed_vs_churn)
    cand_reclaimed_share_budget = cand_summary.get("rewrite_reclaimed_share_of_budget_pct")
    base_reclaimed_share_budget = ctrl_summary.get("rewrite_reclaimed_share_of_budget_pct")
    d_reclaimed_share_budget = delta(cand_reclaimed_share_budget, base_reclaimed_share_budget)
    cand_budget_consumed_share = cand_summary.get("rewrite_budget_consumed_share_of_budget_pct")
    base_budget_consumed_share = ctrl_summary.get("rewrite_budget_consumed_share_of_budget_pct")
    d_budget_consumed_share = delta(cand_budget_consumed_share, base_budget_consumed_share)
    cand_ineffective_runs = cand_summary.get("rewrite_ineffective_runs")
    base_ineffective_runs = ctrl_summary.get("rewrite_ineffective_runs")
    d_ineffective_runs = delta(cand_ineffective_runs, base_ineffective_runs)
    cand_observed_gc_pending_ids = cand_summary.get("observed_gc_pending_ids")
    base_observed_gc_pending_ids = ctrl_summary.get("observed_gc_pending_ids")
    d_observed_gc_pending_ids = delta(cand_observed_gc_pending_ids, base_observed_gc_pending_ids)
    cand_rewrite_queue_eta_seconds_budget = cand_summary.get("rewrite_queue_eta_seconds_budget")
    base_rewrite_queue_eta_seconds_budget = ctrl_summary.get("rewrite_queue_eta_seconds_budget")
    d_rewrite_queue_eta_seconds_budget = delta(
        cand_rewrite_queue_eta_seconds_budget,
        base_rewrite_queue_eta_seconds_budget,
    )
    cand_rewrite_queue_live_bytes_after_tokens = cand_summary.get("rewrite_queue_live_bytes_after_tokens")
    base_rewrite_queue_live_bytes_after_tokens = ctrl_summary.get("rewrite_queue_live_bytes_after_tokens")
    d_rewrite_queue_live_bytes_after_tokens = delta(
        cand_rewrite_queue_live_bytes_after_tokens,
        base_rewrite_queue_live_bytes_after_tokens,
    )
    cand_checkpoint_kick_skipped_hot_no_debt = cand_summary.get("checkpoint_kick_skipped_hot_no_debt")
    base_checkpoint_kick_skipped_hot_no_debt = ctrl_summary.get("checkpoint_kick_skipped_hot_no_debt")
    d_checkpoint_kick_skipped_hot_no_debt = delta(
        cand_checkpoint_kick_skipped_hot_no_debt,
        base_checkpoint_kick_skipped_hot_no_debt,
    )
    cand_checkpoint_like_budget_share_pct = cand_summary.get("rewrite_checkpoint_like_budget_share_pct")
    base_checkpoint_like_budget_share_pct = ctrl_summary.get("rewrite_checkpoint_like_budget_share_pct")
    d_checkpoint_like_budget_share_pct = delta(
        cand_checkpoint_like_budget_share_pct,
        base_checkpoint_like_budget_share_pct,
    )
    cand_checkpoint_like_source_unreferenced_bytes_pct = cand_summary.get(
        "rewrite_checkpoint_like_source_unreferenced_bytes_pct",
    )
    base_checkpoint_like_source_unreferenced_bytes_pct = ctrl_summary.get(
        "rewrite_checkpoint_like_source_unreferenced_bytes_pct",
    )
    d_checkpoint_like_source_unreferenced_bytes_pct = delta(
        cand_checkpoint_like_source_unreferenced_bytes_pct,
        base_checkpoint_like_source_unreferenced_bytes_pct,
    )
    cand_non_checkpoint_source_unreferenced_bytes_pct = cand_summary.get(
        "rewrite_non_checkpoint_source_unreferenced_bytes_pct",
    )
    base_non_checkpoint_source_unreferenced_bytes_pct = ctrl_summary.get(
        "rewrite_non_checkpoint_source_unreferenced_bytes_pct",
    )
    d_non_checkpoint_source_unreferenced_bytes_pct = delta(
        cand_non_checkpoint_source_unreferenced_bytes_pct,
        base_non_checkpoint_source_unreferenced_bytes_pct,
    )
    cand_checkpoint_kick_burst_limiter_count = cand_summary.get(
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
    )
    base_checkpoint_kick_burst_limiter_count = ctrl_summary.get(
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
    )
    d_checkpoint_kick_burst_limiter_count = delta(
        cand_checkpoint_kick_burst_limiter_count,
        base_checkpoint_kick_burst_limiter_count,
    )
    cand_rewrite_queued_debt_passes = cand_summary.get("rewrite_queued_debt_passes")
    base_rewrite_queued_debt_passes = ctrl_summary.get("rewrite_queued_debt_passes")
    d_rewrite_queued_debt_passes = delta(
        cand_rewrite_queued_debt_passes,
        base_rewrite_queued_debt_passes,
    )
    cand_rewrite_queued_debt_rewrite_started = cand_summary.get("rewrite_queued_debt_rewrite_started")
    base_rewrite_queued_debt_rewrite_started = ctrl_summary.get("rewrite_queued_debt_rewrite_started")
    d_rewrite_queued_debt_rewrite_started = delta(
        cand_rewrite_queued_debt_rewrite_started,
        base_rewrite_queued_debt_rewrite_started,
    )
    cand_rewrite_queued_debt_rewrite_start_rate_pct = cand_summary.get("rewrite_queued_debt_rewrite_start_rate_pct")
    base_rewrite_queued_debt_rewrite_start_rate_pct = ctrl_summary.get("rewrite_queued_debt_rewrite_start_rate_pct")
    d_rewrite_queued_debt_rewrite_start_rate_pct = delta(
        cand_rewrite_queued_debt_rewrite_start_rate_pct,
        base_rewrite_queued_debt_rewrite_start_rate_pct,
    )
    cand_rewrite_queued_debt_skip_budget_empty = cand_summary.get("rewrite_queued_debt_skip_budget_empty")
    base_rewrite_queued_debt_skip_budget_empty = ctrl_summary.get("rewrite_queued_debt_skip_budget_empty")
    d_rewrite_queued_debt_skip_budget_empty = delta(
        cand_rewrite_queued_debt_skip_budget_empty,
        base_rewrite_queued_debt_skip_budget_empty,
    )
    cand_rewrite_queued_debt_skip_no_chunk = cand_summary.get("rewrite_queued_debt_skip_no_chunk")
    base_rewrite_queued_debt_skip_no_chunk = ctrl_summary.get("rewrite_queued_debt_skip_no_chunk")
    d_rewrite_queued_debt_skip_no_chunk = delta(
        cand_rewrite_queued_debt_skip_no_chunk,
        base_rewrite_queued_debt_skip_no_chunk,
    )

    def ratio(candidate, control):
        c = as_float(candidate)
        b = as_float(control)
        if c is None or b is None or b == 0.0:
            return None
        return c / b

    win = False
    loss = False
    composite_size_ratio = None
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
        composite_size_ratio = ratio(cand_sync_app_per_block, base_sync_app_per_block)
    elif d_post_wal is not None and d_total is not None:
        size_tol_metric = float(size_tol)
        time_tol_metric = float(time_tol)
        size_delta_metric = d_post_wal
        time_delta_metric = d_total
        composite_size_ratio = ratio(cand_post_wal, base_post_wal)
    else:
        size_tol_metric = None
        time_tol_metric = None
        size_delta_metric = None
        time_delta_metric = None
        composite_size_ratio = None
    if size_delta_metric is not None and time_delta_metric is not None:
        win = (size_delta_metric <= -size_tol_metric) and (time_delta_metric <= time_tol_metric)
        loss = (size_delta_metric >= size_tol_metric) and (time_delta_metric >= -time_tol_metric)

    drift_scored_outcome = "n/a"
    outcome = "neutral"
    if size_delta_metric is not None and time_delta_metric is not None:
        drift_scored_outcome = "neutral"
        if win and not loss:
            drift_scored_outcome = "win"
            outcome = "win"
        elif loss and not win:
            drift_scored_outcome = "loss"
            outcome = "loss"

    pair_invalid_reason = ""
    if block_drift_tolerance >= 0 and d_blocks is not None and abs(int(d_blocks)) > block_drift_tolerance:
        if allow_drift_scoring:
            pair_invalid_reason = "block_drift_exceeds_tolerance_scored"
        else:
            outcome = "invalid"
            pair_invalid_reason = "block_drift_too_high"
            invalid_pairs += 1
            block_drift_invalid_pairs += 1

    if outcome == "win":
        wins += 1
    elif outcome == "loss":
        losses += 1

    time_ratio = ratio(cand_total, base_total)
    composite_score_pct = None
    if time_ratio is not None and composite_size_ratio is not None:
        weighted = (
            (composite_weight_time * (time_ratio - 1.0))
            + (composite_weight_size * (composite_size_ratio - 1.0))
        )
        denom = (composite_weight_time + composite_weight_size)
        composite_score_pct = 100.0 * (weighted / denom)

    pair_rows.append({
        "pair_index": pair,
        "delta_t_sync_seconds": d_sync,
        "delta_t_total_seconds": d_total,
        "delta_s_sync_app_bytes": d_sync_app,
        "delta_s_post_wal_bytes": d_post_wal,
        "delta_blocks_synced": d_blocks,
        "delta_s_sync_app_bytes_per_block": d_sync_app_per_block,
        "delta_t_total_seconds_per_block": d_total_per_block,
        "control_t_sync_seconds": base_sync,
        "candidate_t_sync_seconds": cand_sync,
        "control_t_total_seconds": base_total,
        "candidate_t_total_seconds": cand_total,
        "control_s_sync_app_bytes": base_sync_app,
        "candidate_s_sync_app_bytes": cand_sync_app,
        "control_s_post_wal_bytes": base_post_wal,
        "candidate_s_post_wal_bytes": cand_post_wal,
        "control_max_rss_kb": bm.get("max_rss_kb"),
        "candidate_max_rss_kb": cm.get("max_rss_kb"),
        "composite_score_pct": composite_score_pct,
        "control_rewrite_reclaimed_vs_churn_ratio": base_reclaimed_vs_churn,
        "candidate_rewrite_reclaimed_vs_churn_ratio": cand_reclaimed_vs_churn,
        "delta_rewrite_reclaimed_vs_churn_ratio": d_reclaimed_vs_churn,
        "control_rewrite_reclaimed_share_of_budget_pct": base_reclaimed_share_budget,
        "candidate_rewrite_reclaimed_share_of_budget_pct": cand_reclaimed_share_budget,
        "delta_rewrite_reclaimed_share_of_budget_pct": d_reclaimed_share_budget,
        "control_rewrite_budget_consumed_share_of_budget_pct": base_budget_consumed_share,
        "candidate_rewrite_budget_consumed_share_of_budget_pct": cand_budget_consumed_share,
        "delta_rewrite_budget_consumed_share_of_budget_pct": d_budget_consumed_share,
        "control_rewrite_ineffective_runs": base_ineffective_runs,
        "candidate_rewrite_ineffective_runs": cand_ineffective_runs,
        "delta_rewrite_ineffective_runs": d_ineffective_runs,
        "control_observed_gc_pending_ids": base_observed_gc_pending_ids,
        "candidate_observed_gc_pending_ids": cand_observed_gc_pending_ids,
        "delta_observed_gc_pending_ids": d_observed_gc_pending_ids,
        "control_rewrite_queue_eta_seconds_budget": base_rewrite_queue_eta_seconds_budget,
        "candidate_rewrite_queue_eta_seconds_budget": cand_rewrite_queue_eta_seconds_budget,
        "delta_rewrite_queue_eta_seconds_budget": d_rewrite_queue_eta_seconds_budget,
        "control_rewrite_queue_live_bytes_after_tokens": base_rewrite_queue_live_bytes_after_tokens,
        "candidate_rewrite_queue_live_bytes_after_tokens": cand_rewrite_queue_live_bytes_after_tokens,
        "delta_rewrite_queue_live_bytes_after_tokens": d_rewrite_queue_live_bytes_after_tokens,
        "control_checkpoint_kick_skipped_hot_no_debt": base_checkpoint_kick_skipped_hot_no_debt,
        "candidate_checkpoint_kick_skipped_hot_no_debt": cand_checkpoint_kick_skipped_hot_no_debt,
        "delta_checkpoint_kick_skipped_hot_no_debt": d_checkpoint_kick_skipped_hot_no_debt,
        "control_rewrite_checkpoint_like_budget_share_pct": base_checkpoint_like_budget_share_pct,
        "candidate_rewrite_checkpoint_like_budget_share_pct": cand_checkpoint_like_budget_share_pct,
        "delta_rewrite_checkpoint_like_budget_share_pct": d_checkpoint_like_budget_share_pct,
        "control_rewrite_checkpoint_like_source_unreferenced_bytes_pct": base_checkpoint_like_source_unreferenced_bytes_pct,
        "candidate_rewrite_checkpoint_like_source_unreferenced_bytes_pct": cand_checkpoint_like_source_unreferenced_bytes_pct,
        "delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct": d_checkpoint_like_source_unreferenced_bytes_pct,
        "control_rewrite_non_checkpoint_source_unreferenced_bytes_pct": base_non_checkpoint_source_unreferenced_bytes_pct,
        "candidate_rewrite_non_checkpoint_source_unreferenced_bytes_pct": cand_non_checkpoint_source_unreferenced_bytes_pct,
        "delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct": d_non_checkpoint_source_unreferenced_bytes_pct,
        "control_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": base_checkpoint_kick_burst_limiter_count,
        "candidate_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": cand_checkpoint_kick_burst_limiter_count,
        "delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": d_checkpoint_kick_burst_limiter_count,
        "control_rewrite_queued_debt_passes": base_rewrite_queued_debt_passes,
        "candidate_rewrite_queued_debt_passes": cand_rewrite_queued_debt_passes,
        "delta_rewrite_queued_debt_passes": d_rewrite_queued_debt_passes,
        "control_rewrite_queued_debt_rewrite_started": base_rewrite_queued_debt_rewrite_started,
        "candidate_rewrite_queued_debt_rewrite_started": cand_rewrite_queued_debt_rewrite_started,
        "delta_rewrite_queued_debt_rewrite_started": d_rewrite_queued_debt_rewrite_started,
        "control_rewrite_queued_debt_rewrite_start_rate_pct": base_rewrite_queued_debt_rewrite_start_rate_pct,
        "candidate_rewrite_queued_debt_rewrite_start_rate_pct": cand_rewrite_queued_debt_rewrite_start_rate_pct,
        "delta_rewrite_queued_debt_rewrite_start_rate_pct": d_rewrite_queued_debt_rewrite_start_rate_pct,
        "control_rewrite_queued_debt_skip_budget_empty": base_rewrite_queued_debt_skip_budget_empty,
        "candidate_rewrite_queued_debt_skip_budget_empty": cand_rewrite_queued_debt_skip_budget_empty,
        "delta_rewrite_queued_debt_skip_budget_empty": d_rewrite_queued_debt_skip_budget_empty,
        "control_rewrite_queued_debt_skip_no_chunk": base_rewrite_queued_debt_skip_no_chunk,
        "candidate_rewrite_queued_debt_skip_no_chunk": cand_rewrite_queued_debt_skip_no_chunk,
        "delta_rewrite_queued_debt_skip_no_chunk": d_rewrite_queued_debt_skip_no_chunk,
        "control_valid": ctrl_valid,
        "candidate_valid": cand_valid,
        "control_invalid_reason": ctrl_reason,
        "candidate_invalid_reason": cand_reason,
        "pair_invalid_reason": pair_invalid_reason,
        "drift_scored_outcome": drift_scored_outcome,
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
        "control_t_sync_seconds",
        "candidate_t_sync_seconds",
        "control_t_total_seconds",
        "candidate_t_total_seconds",
        "control_s_sync_app_bytes",
        "candidate_s_sync_app_bytes",
        "control_s_post_wal_bytes",
        "candidate_s_post_wal_bytes",
        "control_max_rss_kb",
        "candidate_max_rss_kb",
        "composite_score_pct",
        "control_rewrite_reclaimed_vs_churn_ratio",
        "candidate_rewrite_reclaimed_vs_churn_ratio",
        "delta_rewrite_reclaimed_vs_churn_ratio",
        "control_rewrite_reclaimed_share_of_budget_pct",
        "candidate_rewrite_reclaimed_share_of_budget_pct",
        "delta_rewrite_reclaimed_share_of_budget_pct",
        "control_rewrite_budget_consumed_share_of_budget_pct",
        "candidate_rewrite_budget_consumed_share_of_budget_pct",
        "delta_rewrite_budget_consumed_share_of_budget_pct",
        "control_rewrite_ineffective_runs",
        "candidate_rewrite_ineffective_runs",
        "delta_rewrite_ineffective_runs",
        "control_observed_gc_pending_ids",
        "candidate_observed_gc_pending_ids",
        "delta_observed_gc_pending_ids",
        "control_rewrite_queue_eta_seconds_budget",
        "candidate_rewrite_queue_eta_seconds_budget",
        "delta_rewrite_queue_eta_seconds_budget",
        "control_rewrite_queue_live_bytes_after_tokens",
        "candidate_rewrite_queue_live_bytes_after_tokens",
        "delta_rewrite_queue_live_bytes_after_tokens",
        "control_checkpoint_kick_skipped_hot_no_debt",
        "candidate_checkpoint_kick_skipped_hot_no_debt",
        "delta_checkpoint_kick_skipped_hot_no_debt",
        "control_rewrite_checkpoint_like_budget_share_pct",
        "candidate_rewrite_checkpoint_like_budget_share_pct",
        "delta_rewrite_checkpoint_like_budget_share_pct",
        "control_rewrite_checkpoint_like_source_unreferenced_bytes_pct",
        "candidate_rewrite_checkpoint_like_source_unreferenced_bytes_pct",
        "delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct",
        "control_rewrite_non_checkpoint_source_unreferenced_bytes_pct",
        "candidate_rewrite_non_checkpoint_source_unreferenced_bytes_pct",
        "delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct",
        "control_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
        "candidate_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
        "delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
        "control_rewrite_queued_debt_passes",
        "candidate_rewrite_queued_debt_passes",
        "delta_rewrite_queued_debt_passes",
        "control_rewrite_queued_debt_rewrite_started",
        "candidate_rewrite_queued_debt_rewrite_started",
        "delta_rewrite_queued_debt_rewrite_started",
        "control_rewrite_queued_debt_rewrite_start_rate_pct",
        "candidate_rewrite_queued_debt_rewrite_start_rate_pct",
        "delta_rewrite_queued_debt_rewrite_start_rate_pct",
        "control_rewrite_queued_debt_skip_budget_empty",
        "candidate_rewrite_queued_debt_skip_budget_empty",
        "delta_rewrite_queued_debt_skip_budget_empty",
        "control_rewrite_queued_debt_skip_no_chunk",
        "candidate_rewrite_queued_debt_skip_no_chunk",
        "delta_rewrite_queued_debt_skip_no_chunk",
        "control_valid",
        "candidate_valid",
        "control_invalid_reason",
        "candidate_invalid_reason",
        "pair_invalid_reason",
        "drift_scored_outcome",
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
            r["control_t_sync_seconds"],
            r["candidate_t_sync_seconds"],
            r["control_t_total_seconds"],
            r["candidate_t_total_seconds"],
            r["control_s_sync_app_bytes"],
            r["candidate_s_sync_app_bytes"],
            r["control_s_post_wal_bytes"],
            r["candidate_s_post_wal_bytes"],
            r["control_max_rss_kb"],
            r["candidate_max_rss_kb"],
            r["composite_score_pct"],
            r["control_rewrite_reclaimed_vs_churn_ratio"],
            r["candidate_rewrite_reclaimed_vs_churn_ratio"],
            r["delta_rewrite_reclaimed_vs_churn_ratio"],
            r["control_rewrite_reclaimed_share_of_budget_pct"],
            r["candidate_rewrite_reclaimed_share_of_budget_pct"],
            r["delta_rewrite_reclaimed_share_of_budget_pct"],
            r["control_rewrite_budget_consumed_share_of_budget_pct"],
            r["candidate_rewrite_budget_consumed_share_of_budget_pct"],
            r["delta_rewrite_budget_consumed_share_of_budget_pct"],
            r["control_rewrite_ineffective_runs"],
            r["candidate_rewrite_ineffective_runs"],
            r["delta_rewrite_ineffective_runs"],
            r["control_observed_gc_pending_ids"],
            r["candidate_observed_gc_pending_ids"],
            r["delta_observed_gc_pending_ids"],
            r["control_rewrite_queue_eta_seconds_budget"],
            r["candidate_rewrite_queue_eta_seconds_budget"],
            r["delta_rewrite_queue_eta_seconds_budget"],
            r["control_rewrite_queue_live_bytes_after_tokens"],
            r["candidate_rewrite_queue_live_bytes_after_tokens"],
            r["delta_rewrite_queue_live_bytes_after_tokens"],
            r["control_checkpoint_kick_skipped_hot_no_debt"],
            r["candidate_checkpoint_kick_skipped_hot_no_debt"],
            r["delta_checkpoint_kick_skipped_hot_no_debt"],
            r["control_rewrite_checkpoint_like_budget_share_pct"],
            r["candidate_rewrite_checkpoint_like_budget_share_pct"],
            r["delta_rewrite_checkpoint_like_budget_share_pct"],
            r["control_rewrite_checkpoint_like_source_unreferenced_bytes_pct"],
            r["candidate_rewrite_checkpoint_like_source_unreferenced_bytes_pct"],
            r["delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct"],
            r["control_rewrite_non_checkpoint_source_unreferenced_bytes_pct"],
            r["candidate_rewrite_non_checkpoint_source_unreferenced_bytes_pct"],
            r["delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct"],
            r["control_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst"],
            r["candidate_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst"],
            r["delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst"],
            r["control_rewrite_queued_debt_passes"],
            r["candidate_rewrite_queued_debt_passes"],
            r["delta_rewrite_queued_debt_passes"],
            r["control_rewrite_queued_debt_rewrite_started"],
            r["candidate_rewrite_queued_debt_rewrite_started"],
            r["delta_rewrite_queued_debt_rewrite_started"],
            r["control_rewrite_queued_debt_rewrite_start_rate_pct"],
            r["candidate_rewrite_queued_debt_rewrite_start_rate_pct"],
            r["delta_rewrite_queued_debt_rewrite_start_rate_pct"],
            r["control_rewrite_queued_debt_skip_budget_empty"],
            r["candidate_rewrite_queued_debt_skip_budget_empty"],
            r["delta_rewrite_queued_debt_skip_budget_empty"],
            r["control_rewrite_queued_debt_skip_no_chunk"],
            r["candidate_rewrite_queued_debt_skip_no_chunk"],
            r["delta_rewrite_queued_debt_skip_no_chunk"],
            r["control_valid"],
            r["candidate_valid"],
            r["control_invalid_reason"],
            r["candidate_invalid_reason"],
            r["pair_invalid_reason"],
            r["drift_scored_outcome"],
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

def mean_pair_delta(rows, key):
    vals = []
    for row in rows:
        v = row.get(key)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    return sum(vals) / float(len(vals))

delta_reclaimed_vs_churn_ratio_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_reclaimed_vs_churn_ratio",
)
delta_reclaimed_share_budget_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_reclaimed_share_of_budget_pct",
)
delta_budget_consumed_share_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_budget_consumed_share_of_budget_pct",
)
delta_ineffective_runs_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_ineffective_runs",
)
delta_observed_gc_pending_ids_mean = mean_pair_delta(
    scored_rows,
    "delta_observed_gc_pending_ids",
)
delta_rewrite_queue_eta_seconds_budget_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queue_eta_seconds_budget",
)
delta_rewrite_queue_live_bytes_after_tokens_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queue_live_bytes_after_tokens",
)
delta_checkpoint_kick_skipped_hot_no_debt_mean = mean_pair_delta(
    scored_rows,
    "delta_checkpoint_kick_skipped_hot_no_debt",
)
delta_checkpoint_like_budget_share_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_checkpoint_like_budget_share_pct",
)
delta_checkpoint_like_source_unreferenced_bytes_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct",
)
delta_non_checkpoint_source_unreferenced_bytes_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct",
)
delta_checkpoint_kick_burst_limiter_count_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst",
)
delta_rewrite_queued_debt_passes_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queued_debt_passes",
)
delta_rewrite_queued_debt_rewrite_started_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queued_debt_rewrite_started",
)
delta_rewrite_queued_debt_rewrite_start_rate_pct_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queued_debt_rewrite_start_rate_pct",
)
delta_rewrite_queued_debt_skip_budget_empty_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queued_debt_skip_budget_empty",
)
delta_rewrite_queued_debt_skip_no_chunk_mean = mean_pair_delta(
    scored_rows,
    "delta_rewrite_queued_debt_skip_no_chunk",
)
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

def collect_float(rows, key):
    out = []
    for row in rows:
        out.append(as_float(row.get(key)))
    return out

comparable_rows = []
for row in scored_rows:
    if row.get("control_t_total_seconds") is None or row.get("candidate_t_total_seconds") is None:
        continue
    comparable_rows.append(row)

control_t_sync_vals = collect_float(comparable_rows, "control_t_sync_seconds")
candidate_t_sync_vals = collect_float(comparable_rows, "candidate_t_sync_seconds")
control_t_total_vals = collect_float(comparable_rows, "control_t_total_seconds")
candidate_t_total_vals = collect_float(comparable_rows, "candidate_t_total_seconds")
control_s_sync_app_vals = collect_float(comparable_rows, "control_s_sync_app_bytes")
candidate_s_sync_app_vals = collect_float(comparable_rows, "candidate_s_sync_app_bytes")
control_s_post_wal_vals = collect_float(comparable_rows, "control_s_post_wal_bytes")
candidate_s_post_wal_vals = collect_float(comparable_rows, "candidate_s_post_wal_bytes")
control_max_rss_vals = collect_float(comparable_rows, "control_max_rss_kb")
candidate_max_rss_vals = collect_float(comparable_rows, "candidate_max_rss_kb")
composite_scores = collect_float(comparable_rows, "composite_score_pct")

absolute_aggregates = {
    "comparable_pairs": len(comparable_rows),
    "median_control_t_sync_seconds": median(control_t_sync_vals),
    "median_candidate_t_sync_seconds": median(candidate_t_sync_vals),
    "mean_control_t_sync_seconds": mean(control_t_sync_vals),
    "mean_candidate_t_sync_seconds": mean(candidate_t_sync_vals),
    "median_control_t_total_seconds": median(control_t_total_vals),
    "median_candidate_t_total_seconds": median(candidate_t_total_vals),
    "mean_control_t_total_seconds": mean(control_t_total_vals),
    "mean_candidate_t_total_seconds": mean(candidate_t_total_vals),
    "median_control_s_sync_app_bytes": median(control_s_sync_app_vals),
    "median_candidate_s_sync_app_bytes": median(candidate_s_sync_app_vals),
    "mean_control_s_sync_app_bytes": mean(control_s_sync_app_vals),
    "mean_candidate_s_sync_app_bytes": mean(candidate_s_sync_app_vals),
    "median_control_s_post_wal_bytes": median(control_s_post_wal_vals),
    "median_candidate_s_post_wal_bytes": median(candidate_s_post_wal_vals),
    "mean_control_s_post_wal_bytes": mean(control_s_post_wal_vals),
    "mean_candidate_s_post_wal_bytes": mean(candidate_s_post_wal_vals),
    "median_control_max_rss_kb": median(control_max_rss_vals),
    "median_candidate_max_rss_kb": median(candidate_max_rss_vals),
    "mean_control_max_rss_kb": mean(control_max_rss_vals),
    "mean_candidate_max_rss_kb": mean(candidate_max_rss_vals),
    "median_composite_score_pct": median(composite_scores),
    "mean_composite_score_pct": mean(composite_scores),
}

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

comp_pairs = int(absolute_aggregates.get("comparable_pairs", 0) or 0)
comp_median = absolute_aggregates.get("median_composite_score_pct")
comp_win_thresh = abs(composite_clear_win_pct)
comp_loss_thresh = abs(composite_clear_loss_pct)
if (not stop) and composite_stop_on_clear and comp_pairs >= composite_min_pairs and comp_median is not None:
    if comp_median <= -comp_win_thresh:
        stop = True
        reason = "composite_clear_improvement"
    elif comp_median >= comp_loss_thresh:
        stop = True
        reason = "composite_clear_regression"

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
lines.append(f"- scoring policy: `{ab_policy}`")
lines.append(f"- block drift tolerance (abs blocks, -1=disabled): `{block_drift_tolerance}`")
lines.append(f"- scoring mode: `{scoring_mode}`")
lines.append(f"- allow drift scoring: `{allow_drift_scoring}`")
lines.append(f"- composite weights (time,size): `{composite_weight_time}` / `{composite_weight_size}`")
lines.append(f"- composite stop on clear: `{composite_stop_on_clear}`")
lines.append(f"- composite min pairs: `{composite_min_pairs}`")
lines.append(f"- composite clear thresholds (win/loss pct): `{composite_clear_win_pct}` / `{composite_clear_loss_pct}`")
lines.append(
    f"- mean delta rewrite_reclaimed_vs_churn_ratio: `{delta_reclaimed_vs_churn_ratio_mean}`"
)
lines.append(
    f"- mean delta rewrite_reclaimed_share_of_budget_pct: `{delta_reclaimed_share_budget_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_budget_consumed_share_of_budget_pct: `{delta_budget_consumed_share_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_ineffective_runs: `{delta_ineffective_runs_mean}`"
)
lines.append(
    f"- mean delta observed_gc_pending_ids: `{delta_observed_gc_pending_ids_mean}`"
)
lines.append(
    f"- mean delta rewrite_queue_eta_seconds_budget: `{delta_rewrite_queue_eta_seconds_budget_mean}`"
)
lines.append(
    f"- mean delta rewrite_queue_live_bytes_after_tokens: `{delta_rewrite_queue_live_bytes_after_tokens_mean}`"
)
lines.append(
    f"- mean delta checkpoint_kick_skipped_hot_no_debt: `{delta_checkpoint_kick_skipped_hot_no_debt_mean}`"
)
lines.append(
    f"- mean delta rewrite_checkpoint_like_budget_share_pct: `{delta_checkpoint_like_budget_share_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_checkpoint_like_source_unreferenced_bytes_pct: `{delta_checkpoint_like_source_unreferenced_bytes_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_non_checkpoint_source_unreferenced_bytes_pct: `{delta_non_checkpoint_source_unreferenced_bytes_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst: `{delta_checkpoint_kick_burst_limiter_count_mean}`"
)
lines.append(
    f"- mean delta rewrite_queued_debt_passes: `{delta_rewrite_queued_debt_passes_mean}`"
)
lines.append(
    f"- mean delta rewrite_queued_debt_rewrite_started: `{delta_rewrite_queued_debt_rewrite_started_mean}`"
)
lines.append(
    f"- mean delta rewrite_queued_debt_rewrite_start_rate_pct: `{delta_rewrite_queued_debt_rewrite_start_rate_pct_mean}`"
)
lines.append(
    f"- mean delta rewrite_queued_debt_skip_budget_empty: `{delta_rewrite_queued_debt_skip_budget_empty_mean}`"
)
lines.append(
    f"- mean delta rewrite_queued_debt_skip_no_chunk: `{delta_rewrite_queued_debt_skip_no_chunk_mean}`"
)
lines.append(f"- decision: `{reason}`")
lines.append("")
lines.append("## Absolute Medians")
lines.append("")
lines.append(f"- comparable pairs: `{absolute_aggregates.get('comparable_pairs')}`")
lines.append(
    f"- t_sync seconds (control/candidate): `{absolute_aggregates.get('median_control_t_sync_seconds')}` / `{absolute_aggregates.get('median_candidate_t_sync_seconds')}`"
)
lines.append(
    f"- t_total seconds (control/candidate): `{absolute_aggregates.get('median_control_t_total_seconds')}` / `{absolute_aggregates.get('median_candidate_t_total_seconds')}`"
)
lines.append(
    f"- s_sync_app bytes (control/candidate): `{absolute_aggregates.get('median_control_s_sync_app_bytes')}` / `{absolute_aggregates.get('median_candidate_s_sync_app_bytes')}`"
)
lines.append(
    f"- s_post_wal bytes (control/candidate): `{absolute_aggregates.get('median_control_s_post_wal_bytes')}` / `{absolute_aggregates.get('median_candidate_s_post_wal_bytes')}`"
)
lines.append(
    f"- max_rss_kb (control/candidate): `{absolute_aggregates.get('median_control_max_rss_kb')}` / `{absolute_aggregates.get('median_candidate_max_rss_kb')}`"
)
lines.append(
    f"- composite score pct (median/mean; lower is better): `{absolute_aggregates.get('median_composite_score_pct')}` / `{absolute_aggregates.get('mean_composite_score_pct')}`"
)
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
    lines.append(f"- drift_scored_outcome: `{last['drift_scored_outcome']}`")
    lines.append(f"- delta_t_sync_seconds: `{last['delta_t_sync_seconds']}`")
    lines.append(f"- delta_t_total_seconds: `{last['delta_t_total_seconds']}`")
    lines.append(f"- delta_s_sync_app_bytes: `{last['delta_s_sync_app_bytes']}`")
    lines.append(f"- delta_s_post_wal_bytes: `{last['delta_s_post_wal_bytes']}`")
    lines.append(f"- delta_blocks_synced: `{last['delta_blocks_synced']}`")
    lines.append(f"- delta_s_sync_app_bytes_per_block: `{last['delta_s_sync_app_bytes_per_block']}`")
    lines.append(f"- delta_t_total_seconds_per_block: `{last['delta_t_total_seconds_per_block']}`")
    lines.append(f"- control_t_sync_seconds: `{last['control_t_sync_seconds']}`")
    lines.append(f"- candidate_t_sync_seconds: `{last['candidate_t_sync_seconds']}`")
    lines.append(f"- control_t_total_seconds: `{last['control_t_total_seconds']}`")
    lines.append(f"- candidate_t_total_seconds: `{last['candidate_t_total_seconds']}`")
    lines.append(f"- control_s_post_wal_bytes: `{last['control_s_post_wal_bytes']}`")
    lines.append(f"- candidate_s_post_wal_bytes: `{last['candidate_s_post_wal_bytes']}`")
    lines.append(f"- control_max_rss_kb: `{last['control_max_rss_kb']}`")
    lines.append(f"- candidate_max_rss_kb: `{last['candidate_max_rss_kb']}`")
    lines.append(f"- composite_score_pct: `{last['composite_score_pct']}`")
    lines.append(
        f"- rewrite_reclaimed_vs_churn_ratio (control/candidate/delta): "
        f"`{last['control_rewrite_reclaimed_vs_churn_ratio']}` / "
        f"`{last['candidate_rewrite_reclaimed_vs_churn_ratio']}` / "
        f"`{last['delta_rewrite_reclaimed_vs_churn_ratio']}`"
    )
    lines.append(
        f"- rewrite_reclaimed_share_of_budget_pct (control/candidate/delta): "
        f"`{last['control_rewrite_reclaimed_share_of_budget_pct']}` / "
        f"`{last['candidate_rewrite_reclaimed_share_of_budget_pct']}` / "
        f"`{last['delta_rewrite_reclaimed_share_of_budget_pct']}`"
    )
    lines.append(
        f"- rewrite_budget_consumed_share_of_budget_pct (control/candidate/delta): "
        f"`{last['control_rewrite_budget_consumed_share_of_budget_pct']}` / "
        f"`{last['candidate_rewrite_budget_consumed_share_of_budget_pct']}` / "
        f"`{last['delta_rewrite_budget_consumed_share_of_budget_pct']}`"
    )
    lines.append(
        f"- rewrite_ineffective_runs (control/candidate/delta): "
        f"`{last['control_rewrite_ineffective_runs']}` / "
        f"`{last['candidate_rewrite_ineffective_runs']}` / "
        f"`{last['delta_rewrite_ineffective_runs']}`"
    )
    lines.append(
        f"- observed_gc_pending_ids (control/candidate/delta): "
        f"`{last['control_observed_gc_pending_ids']}` / "
        f"`{last['candidate_observed_gc_pending_ids']}` / "
        f"`{last['delta_observed_gc_pending_ids']}`"
    )
    lines.append(
        f"- rewrite_queue_eta_seconds_budget (control/candidate/delta): "
        f"`{last['control_rewrite_queue_eta_seconds_budget']}` / "
        f"`{last['candidate_rewrite_queue_eta_seconds_budget']}` / "
        f"`{last['delta_rewrite_queue_eta_seconds_budget']}`"
    )
    lines.append(
        f"- rewrite_queue_live_bytes_after_tokens (control/candidate/delta): "
        f"`{last['control_rewrite_queue_live_bytes_after_tokens']}` / "
        f"`{last['candidate_rewrite_queue_live_bytes_after_tokens']}` / "
        f"`{last['delta_rewrite_queue_live_bytes_after_tokens']}`"
    )
    lines.append(
        f"- checkpoint_kick_skipped_hot_no_debt (control/candidate/delta): "
        f"`{last['control_checkpoint_kick_skipped_hot_no_debt']}` / "
        f"`{last['candidate_checkpoint_kick_skipped_hot_no_debt']}` / "
        f"`{last['delta_checkpoint_kick_skipped_hot_no_debt']}`"
    )
    lines.append(
        f"- rewrite_checkpoint_like_budget_share_pct (control/candidate/delta): "
        f"`{last['control_rewrite_checkpoint_like_budget_share_pct']}` / "
        f"`{last['candidate_rewrite_checkpoint_like_budget_share_pct']}` / "
        f"`{last['delta_rewrite_checkpoint_like_budget_share_pct']}`"
    )
    lines.append(
        f"- rewrite_checkpoint_like_source_unreferenced_bytes_pct (control/candidate/delta): "
        f"`{last['control_rewrite_checkpoint_like_source_unreferenced_bytes_pct']}` / "
        f"`{last['candidate_rewrite_checkpoint_like_source_unreferenced_bytes_pct']}` / "
        f"`{last['delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct']}`"
    )
    lines.append(
        f"- rewrite_non_checkpoint_source_unreferenced_bytes_pct (control/candidate/delta): "
        f"`{last['control_rewrite_non_checkpoint_source_unreferenced_bytes_pct']}` / "
        f"`{last['candidate_rewrite_non_checkpoint_source_unreferenced_bytes_pct']}` / "
        f"`{last['delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct']}`"
    )
    lines.append(
        f"- rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst (control/candidate/delta): "
        f"`{last['control_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst']}` / "
        f"`{last['candidate_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst']}` / "
        f"`{last['delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst']}`"
    )
    lines.append(
        f"- rewrite_queued_debt_passes (control/candidate/delta): "
        f"`{last['control_rewrite_queued_debt_passes']}` / "
        f"`{last['candidate_rewrite_queued_debt_passes']}` / "
        f"`{last['delta_rewrite_queued_debt_passes']}`"
    )
    lines.append(
        f"- rewrite_queued_debt_rewrite_started (control/candidate/delta): "
        f"`{last['control_rewrite_queued_debt_rewrite_started']}` / "
        f"`{last['candidate_rewrite_queued_debt_rewrite_started']}` / "
        f"`{last['delta_rewrite_queued_debt_rewrite_started']}`"
    )
    lines.append(
        f"- rewrite_queued_debt_rewrite_start_rate_pct (control/candidate/delta): "
        f"`{last['control_rewrite_queued_debt_rewrite_start_rate_pct']}` / "
        f"`{last['candidate_rewrite_queued_debt_rewrite_start_rate_pct']}` / "
        f"`{last['delta_rewrite_queued_debt_rewrite_start_rate_pct']}`"
    )
    lines.append(
        f"- rewrite_queued_debt_skip_budget_empty (control/candidate/delta): "
        f"`{last['control_rewrite_queued_debt_skip_budget_empty']}` / "
        f"`{last['candidate_rewrite_queued_debt_skip_budget_empty']}` / "
        f"`{last['delta_rewrite_queued_debt_skip_budget_empty']}`"
    )
    lines.append(
        f"- rewrite_queued_debt_skip_no_chunk (control/candidate/delta): "
        f"`{last['control_rewrite_queued_debt_skip_no_chunk']}` / "
        f"`{last['candidate_rewrite_queued_debt_skip_no_chunk']}` / "
        f"`{last['delta_rewrite_queued_debt_skip_no_chunk']}`"
    )
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
    "ab_policy": ab_policy,
    "scoring_mode": scoring_mode,
    "allow_drift_scoring": allow_drift_scoring,
    "composite": {
        "weight_time": composite_weight_time,
        "weight_size": composite_weight_size,
        "stop_on_clear": composite_stop_on_clear,
        "min_pairs": composite_min_pairs,
        "clear_win_pct": composite_clear_win_pct,
        "clear_loss_pct": composite_clear_loss_pct,
    },
    "absolute_aggregates": absolute_aggregates,
    "mean_delta_rewrite_reclaimed_vs_churn_ratio": delta_reclaimed_vs_churn_ratio_mean,
    "mean_delta_rewrite_reclaimed_share_of_budget_pct": delta_reclaimed_share_budget_pct_mean,
    "mean_delta_rewrite_budget_consumed_share_of_budget_pct": delta_budget_consumed_share_pct_mean,
    "mean_delta_rewrite_ineffective_runs": delta_ineffective_runs_mean,
    "mean_delta_observed_gc_pending_ids": delta_observed_gc_pending_ids_mean,
    "mean_delta_rewrite_queue_eta_seconds_budget": delta_rewrite_queue_eta_seconds_budget_mean,
    "mean_delta_rewrite_queue_live_bytes_after_tokens": delta_rewrite_queue_live_bytes_after_tokens_mean,
    "mean_delta_checkpoint_kick_skipped_hot_no_debt": delta_checkpoint_kick_skipped_hot_no_debt_mean,
    "mean_delta_rewrite_checkpoint_like_budget_share_pct": delta_checkpoint_like_budget_share_pct_mean,
    "mean_delta_rewrite_checkpoint_like_source_unreferenced_bytes_pct": delta_checkpoint_like_source_unreferenced_bytes_pct_mean,
    "mean_delta_rewrite_non_checkpoint_source_unreferenced_bytes_pct": delta_non_checkpoint_source_unreferenced_bytes_pct_mean,
    "mean_delta_rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": delta_checkpoint_kick_burst_limiter_count_mean,
    "mean_delta_rewrite_queued_debt_passes": delta_rewrite_queued_debt_passes_mean,
    "mean_delta_rewrite_queued_debt_rewrite_started": delta_rewrite_queued_debt_rewrite_started_mean,
    "mean_delta_rewrite_queued_debt_rewrite_start_rate_pct": delta_rewrite_queued_debt_rewrite_start_rate_pct_mean,
    "mean_delta_rewrite_queued_debt_skip_budget_empty": delta_rewrite_queued_debt_skip_budget_empty_mean,
    "mean_delta_rewrite_queued_debt_skip_no_chunk": delta_rewrite_queued_debt_skip_no_chunk_mean,
    "stop": stop,
    "reason": reason,
}
decision_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(payload, sort_keys=True))
PY
}

run_pair() {
  local pair_index="$1"
  local first_variant=""
  local second_variant=""
  local first_env=""
  local second_env=""
  local first_json=""
  local pair_overlay_env=""

  if (( pair_index % 2 == 1 )); then
    first_variant="control"
    second_variant="candidate"
    first_env="$CONTROL_ENV_FILE"
    second_env="$CANDIDATE_ENV_FILE"
  else
    first_variant="candidate"
    second_variant="control"
    first_env="$CANDIDATE_ENV_FILE"
    second_env="$CONTROL_ENV_FILE"
  fi

  run_variant "$pair_index" "$first_variant" "$first_env"
  sleep "$SLEEP_BETWEEN_RUNS_SECONDS"

  if [[ "$PAIR_ALIGN_TRUST_FROM_FIRST" == "1" || "$PAIR_ALIGN_STOP_HEIGHT_FROM_FIRST" == "1" ]]; then
    first_json="$(run_json_path "$pair_index" "$first_variant")"
    pair_overlay_env="$OUT/runs/$(printf "pair_%02d_overlay.env" "$pair_index")"
    build_pair_overlay_env "$first_json" "$pair_overlay_env"
  fi

  run_variant "$pair_index" "$second_variant" "$second_env" "$pair_overlay_env"
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
