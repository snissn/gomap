#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_CMD="${RUN_CMD:-$HOME/run_celestia.sh}"
TREEMAP_BIN="${TREEMAP_BIN:-/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local}"
LOCAL_GOMAP_DIR="${LOCAL_GOMAP_DIR:-$ROOT}"
USE_LOCAL_TREE_STACK="${USE_LOCAL_TREE_STACK:-1}"
PROFILES=(${PROFILES:-wal_on_fast fast})
MODES=(${MODES:-sync_only dwell15m})
DWELL_SECONDS="${DWELL_SECONDS:-900}"
DWELL_SAMPLE_INTERVAL_SECONDS="${DWELL_SAMPLE_INTERVAL_SECONDS:-60}"
RSS_SAMPLE_INTERVAL_SECONDS="${RSS_SAMPLE_INTERVAL_SECONDS:-5}"
OFFLINE_MAINTENANCE_TIMEOUT_SECONDS="${OFFLINE_MAINTENANCE_TIMEOUT_SECONDS:-3600}"
FIXED_STOP_AT_LOCAL_HEIGHT="${FIXED_STOP_AT_LOCAL_HEIGHT:-}"
FIXED_BOOTSTRAP_FALLBACK_HOME="${FIXED_BOOTSTRAP_FALLBACK_HOME:-}"

TS="$(date +%Y%m%d%H%M%S)"
OUT_DIR="${1:-$ROOT/artifacts/celestia_profile_signals/$TS}"
mkdir -p "$OUT_DIR"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi
if [[ ! -x "$RUN_CMD" ]]; then
  echo "run command not executable: $RUN_CMD" >&2
  exit 1
fi
if [[ ! -x "$TREEMAP_BIN" ]]; then
  echo "treemap binary not executable: $TREEMAP_BIN" >&2
  exit 1
fi

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

safe_du_bytes() {
  local path="$1"
  if [[ -e "$path" ]]; then
    du -sb "$path" 2>/dev/null | awk '{print $1}'
  else
    printf '0\n'
  fi
}

sync_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key { sub($1 "=", ""); print; exit }' "$file" 2>/dev/null || true
}

latest_existing_home() {
  ls -1dt "$HOME"/.celestia-app-mainnet-treedb-* 2>/dev/null | head -n1 || true
}

fetch_remote_height() {
  local rpc
  local body
  local height
  for rpc in "https://celestia.rpc.kjnodes.com" "https://celestia-rpc.publicnode.com:443"; do
    body="$(curl -fsSL --max-time 10 --connect-timeout 5 "$rpc/status" 2>/dev/null || true)"
    if [[ -z "$body" ]]; then
      continue
    fi
    height="$(printf '%s' "$body" | jq -er '.result.sync_info.latest_block_height | tonumber' 2>/dev/null || true)"
    if [[ "$height" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$height"
      return 0
    fi
  done
  return 1
}

wait_for_new_home() {
  local before_file="$1"
  local run_pid="$2"
  local home=""
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    while IFS= read -r cand; do
      [[ -d "$cand" ]] || continue
      if ! grep -Fxq "$cand" "$before_file"; then
        home="$cand"
        break
      fi
    done < <(ls -1dt "$HOME"/.celestia-app-mainnet-treedb-* 2>/dev/null || true)
    if [[ -n "$home" ]]; then
      printf '%s\n' "$home"
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_node_pid() {
  local home_dir="$1"
  local run_pid="$2"
  local pid=""
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    pid="$(ps -eo pid=,args= | awk -v home="$home_dir" '
      index($0, "celestia-appd start --home " home) > 0 { print $1; exit }
    ' | head -n1)"
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$pid"
      return 0
    fi
    sleep 1
  done
  return 1
}

monitor_rss() {
  local node_pid="$1"
  local out_file="$2"
  printf 'ts_utc\tepoch\trss_kb\thwm_kb\tmax_rss_kb_so_far\tmax_hwm_kb_so_far\n' >"$out_file"
  local max_rss=0
  local max_hwm=0
  while kill -0 "$node_pid" >/dev/null 2>&1; do
    local ts epoch rss hwm
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    epoch="$(date +%s)"
    rss="$(ps -o rss= -p "$node_pid" 2>/dev/null | awk '{print $1}' || true)"
    hwm="$(awk '/VmHWM:/ {print $2}' "/proc/$node_pid/status" 2>/dev/null || true)"
    [[ "$rss" =~ ^[0-9]+$ ]] || rss=0
    [[ "$hwm" =~ ^[0-9]+$ ]] || hwm=0
    if (( rss > max_rss )); then
      max_rss="$rss"
    fi
    if (( hwm > max_hwm )); then
      max_hwm="$hwm"
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$ts" "$epoch" "$rss" "$hwm" "$max_rss" "$max_hwm" >>"$out_file"
    sleep "$RSS_SAMPLE_INTERVAL_SECONDS"
  done
}

wait_for_sync_complete() {
  local run_log="$1"
  local sync_log="$2"
  local run_pid="$3"
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    if [[ -f "$run_log" ]] && grep -q 'Sync complete:' "$run_log"; then
      return 0
    fi
    if [[ -f "$sync_log" ]] && grep -q '^sync_complete_utc=' "$sync_log"; then
      return 0
    fi
    sleep 5
  done
  [[ -f "$run_log" ]] && grep -q 'Sync complete:' "$run_log"
}

collect_live_sample_row() {
  local home_dir="$1"
  local app_db="$2"
  local node_pid="$3"
  local rss_samples_file="$4"
  local minute_index="$5"
  local sync_complete_epoch="$6"

  local ts epoch rss hwm max_rss max_hwm home_bytes data_bytes app_bytes wal_bytes
  local raw_json app_key
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  epoch="$(date +%s)"
  rss="$(ps -o rss= -p "$node_pid" 2>/dev/null | awk '{print $1}' || true)"
  hwm="$(awk '/VmHWM:/ {print $2}' "/proc/$node_pid/status" 2>/dev/null || true)"
  [[ "$rss" =~ ^[0-9]+$ ]] || rss=0
  [[ "$hwm" =~ ^[0-9]+$ ]] || hwm=0
  max_rss="$(awk 'NR>1 && $5 ~ /^[0-9]+$/ {m=$5} END {print m+0}' "$rss_samples_file" 2>/dev/null || true)"
  max_hwm="$(awk 'NR>1 && $6 ~ /^[0-9]+$/ {m=$6} END {print m+0}' "$rss_samples_file" 2>/dev/null || true)"
  [[ "$max_rss" =~ ^[0-9]+$ ]] || max_rss=0
  [[ "$max_hwm" =~ ^[0-9]+$ ]] || max_hwm=0
  home_bytes="$(safe_du_bytes "$home_dir")"
  data_bytes="$(safe_du_bytes "$home_dir/data")"
  app_bytes="$(safe_du_bytes "$app_db")"
  wal_bytes="$(safe_du_bytes "$app_db/maindb/wal")"
  raw_json="$(curl -fsS --max-time 5 http://127.0.0.1:6062/debug/vars 2>/dev/null || true)"

  if [[ -z "$raw_json" ]]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n' \
      "$minute_index" "$ts" "$epoch" "$(( epoch - sync_complete_epoch ))" "$rss" "$hwm" "$max_rss" "$max_hwm" \
      "$home_bytes" "$data_bytes" "$app_bytes" "$wal_bytes"
    return 0
  fi

  app_key="$(printf '%s' "$raw_json" | jq -r '.treedb.instances | keys[] | select(test("/data/application\\.db/"))' | head -n1)"
  if [[ -z "$app_key" ]]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n' \
      "$minute_index" "$ts" "$epoch" "$(( epoch - sync_complete_epoch ))" "$rss" "$hwm" "$max_rss" "$max_hwm" \
      "$home_bytes" "$data_bytes" "$app_bytes" "$wal_bytes"
    return 0
  fi

  printf '%s' "$raw_json" | jq -r --arg app "$app_key" --arg minute "$minute_index" --arg ts "$ts" --arg epoch "$epoch" \
    --arg elapsed "$(( epoch - sync_complete_epoch ))" --arg rss "$rss" --arg hwm "$hwm" --arg maxrss "$max_rss" \
    --arg maxhwm "$max_hwm" --arg homeb "$home_bytes" --arg datab "$data_bytes" --arg appb "$app_bytes" --arg walb "$wal_bytes" '
      .treedb.instances[$app] as $inst |
      [
        $minute,
        $ts,
        $epoch,
        $elapsed,
        $rss,
        $hwm,
        $maxrss,
        $maxhwm,
        $homeb,
        $datab,
        $appb,
        $walb,
        (($inst."treedb.cache.vlog_generation.enabled" // "") | tostring),
        ($inst."treedb.cache.vlog_generation.scheduler_state" // ""),
        ($inst."treedb.cache.vlog_generation.scheduler_last_reason" // ""),
        ($inst."treedb.cache.vlog_generation.maintenance_phase" // ""),
        ($inst."treedb.cache.vlog_generation.maintenance.attempts" // ""),
        ($inst."treedb.cache.vlog_generation.maintenance.acquired" // ""),
        ($inst."treedb.cache.vlog_generation.maintenance.passes.with_rewrite" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.runs" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.queued_debt.exec.runs" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.plan_runs" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.plan_last_result" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.plan_last_selected_segments" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.plan_last_selected_bytes_stale" // ""),
        (($inst."treedb.cache.vlog_generation.steady_probe.pending" // "") | tostring),
        ($inst."treedb.cache.vlog_generation.rewrite.planner_refresh.attempts" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.planner_refresh.successes" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.planner_refresh.last_retained_bytes" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.planner_refresh.last_plan_bytes_total" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.current_set_segments" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.current_set_bytes_total" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.current_set_refresh_scans" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.queue_len" // ""),
        ($inst."treedb.cache.vlog_generation.bytes.stale.total" // ""),
        (($inst."treedb.cache.vlog_generation.rewrite.debt_visible" // "") | tostring),
        ($inst."treedb.cache.vlog_generation.rewrite.debt_visible_source" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.debt_visible_bytes_stale" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.debt_last_deferral_reason" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.debt_last_deferral_age_ms" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.ledger_bytes_total" // ""),
        ($inst."treedb.cache.vlog_generation.rewrite.ledger_bytes_stale" // ""),
        ($inst."treedb.cache.vlog_retained_prune.runs" // ""),
        ($inst."treedb.cache.vlog_retained_prune.candidate_bytes" // ""),
        ($inst."treedb.cache.vlog_generation.observed_gc.runs" // ""),
        ($inst."treedb.cache.vlog_generation.gc.last_eligible_bytes" // ""),
        ($inst."treedb.cache.vlog_generation.checkpoint_kick.runs" // ""),
        (($inst."treedb.cache.vlog_generation.maintenance.foreground_quiet" // "") | tostring),
        (($inst."treedb.cache.vlog_generation.maintenance.foreground_full_quiet" // "") | tostring),
        (($inst."treedb.cache.vlog_generation.maintenance.foreground_low_pressure" // "") | tostring)
      ] | @tsv'
}

run_offline_maintenance() {
  local app_db="$1"
  local case_dir="$2"
  local size_file="$case_dir/offline_sizes.env"
  local gc_log="$case_dir/vlog_gc.log"
  local rewrite_log="$case_dir/vlog_rewrite.log"

  local post_run_home post_run_data post_run_app post_run_wal
  post_run_home="$(safe_du_bytes "$(dirname "$(dirname "$app_db")")")"
  post_run_data="$(safe_du_bytes "$(dirname "$app_db")")"
  post_run_app="$(safe_du_bytes "$app_db")"
  post_run_wal="$(safe_du_bytes "$app_db/maindb/wal")"

  {
    printf 'post_run_home_bytes=%s\n' "$post_run_home"
    printf 'post_run_data_bytes=%s\n' "$post_run_data"
    printf 'post_run_app_bytes=%s\n' "$post_run_app"
    printf 'post_run_wal_bytes=%s\n' "$post_run_wal"
  } >"$size_file"

  if command -v timeout >/dev/null 2>&1; then
    timeout "$OFFLINE_MAINTENANCE_TIMEOUT_SECONDS" "$TREEMAP_BIN" vlog-gc "$app_db" -rw >"$gc_log" 2>&1
  else
    "$TREEMAP_BIN" vlog-gc "$app_db" -rw >"$gc_log" 2>&1
  fi

  {
    printf 'post_gc_home_bytes=%s\n' "$(safe_du_bytes "$(dirname "$(dirname "$app_db")")")"
    printf 'post_gc_data_bytes=%s\n' "$(safe_du_bytes "$(dirname "$app_db")")"
    printf 'post_gc_app_bytes=%s\n' "$(safe_du_bytes "$app_db")"
    printf 'post_gc_wal_bytes=%s\n' "$(safe_du_bytes "$app_db/maindb/wal")"
  } >>"$size_file"

  if command -v timeout >/dev/null 2>&1; then
    timeout "$OFFLINE_MAINTENANCE_TIMEOUT_SECONDS" "$TREEMAP_BIN" vlog-rewrite "$app_db" -rw >"$rewrite_log" 2>&1
  else
    "$TREEMAP_BIN" vlog-rewrite "$app_db" -rw >"$rewrite_log" 2>&1
  fi

  {
    printf 'post_rewrite_home_bytes=%s\n' "$(safe_du_bytes "$(dirname "$(dirname "$app_db")")")"
    printf 'post_rewrite_data_bytes=%s\n' "$(safe_du_bytes "$(dirname "$app_db")")"
    printf 'post_rewrite_app_bytes=%s\n' "$(safe_du_bytes "$app_db")"
    printf 'post_rewrite_wal_bytes=%s\n' "$(safe_du_bytes "$app_db/maindb/wal")"
  } >>"$size_file"
}

write_run_summary() {
  local case_dir="$1"
  local profile="$2"
  local mode="$3"
  local sync_log="$4"
  local home_dir="$5"
  local rss_samples_file="$6"
  local summary_file="$case_dir/summary.env"
  local app_db="$home_dir/data/application.db"

  local sync_duration total_duration dwell_elapsed final_local final_remote
  local sync_complete end_utc max_rss max_hwm
  sync_duration="$(sync_value "$sync_log" sync_duration_seconds)"
  total_duration="$(sync_value "$sync_log" total_duration_seconds)"
  dwell_elapsed="$(sync_value "$sync_log" post_sync_dwell_elapsed_seconds)"
  final_local="$(sync_value "$sync_log" final_local_height)"
  final_remote="$(sync_value "$sync_log" final_remote_height)"
  sync_complete="$(sync_value "$sync_log" sync_complete_utc)"
  end_utc="$(sync_value "$sync_log" end_utc)"
  max_rss="$(awk 'NR>1 && $5 ~ /^[0-9]+$/ {m=$5} END {print m+0}' "$rss_samples_file" 2>/dev/null || true)"
  max_hwm="$(awk 'NR>1 && $6 ~ /^[0-9]+$/ {m=$6} END {print m+0}' "$rss_samples_file" 2>/dev/null || true)"
  [[ "$max_rss" =~ ^[0-9]+$ ]] || max_rss="$(sync_value "$sync_log" max_rss_kb)"
  [[ "$max_hwm" =~ ^[0-9]+$ ]] || max_hwm="$(sync_value "$sync_log" max_hwm_kb)"

  {
    printf 'profile=%s\n' "$profile"
    printf 'mode=%s\n' "$mode"
    printf 'home=%s\n' "$home_dir"
    printf 'app_db=%s\n' "$app_db"
    printf 'sync_complete_utc=%s\n' "$sync_complete"
    printf 'end_utc=%s\n' "$end_utc"
    printf 'sync_duration_seconds=%s\n' "$sync_duration"
    printf 'total_duration_seconds=%s\n' "$total_duration"
    printf 'post_sync_dwell_elapsed_seconds=%s\n' "${dwell_elapsed:-0}"
    printf 'final_local_height=%s\n' "$final_local"
    printf 'final_remote_height=%s\n' "$final_remote"
    printf 'max_rss_kb=%s\n' "$max_rss"
    printf 'max_hwm_kb=%s\n' "$max_hwm"
    printf 'home_bytes=%s\n' "$(safe_du_bytes "$home_dir")"
    printf 'data_bytes=%s\n' "$(safe_du_bytes "$home_dir/data")"
    printf 'app_bytes=%s\n' "$(safe_du_bytes "$app_db")"
    printf 'wal_bytes=%s\n' "$(safe_du_bytes "$app_db/maindb/wal")"
  } >"$summary_file"
}

append_campaign_row() {
  local summary_file="$1"
  local offline_file="${2:-}"
  local out_row="$OUT_DIR/campaign_summary.tsv"
  if [[ ! -f "$out_row" ]]; then
    printf 'profile\tmode\tsync_duration_seconds\ttotal_duration_seconds\tmax_rss_kb\tmax_hwm_kb\tfinal_local_height\thome_bytes\tdata_bytes\tapp_bytes\twal_bytes\tpost_gc_app_bytes\tpost_gc_wal_bytes\tpost_rewrite_app_bytes\tpost_rewrite_wal_bytes\thome\n' >"$out_row"
  fi
  # shellcheck disable=SC1090
  source "$summary_file"
  local post_gc_app="" post_gc_wal="" post_rewrite_app="" post_rewrite_wal=""
  if [[ -n "$offline_file" && -f "$offline_file" ]]; then
    # shellcheck disable=SC1090
    source "$offline_file"
    post_gc_app="${post_gc_app_bytes:-}"
    post_gc_wal="${post_gc_wal_bytes:-}"
    post_rewrite_app="${post_rewrite_app_bytes:-}"
    post_rewrite_wal="${post_rewrite_wal_bytes:-}"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${profile:-}" "${mode:-}" "${sync_duration_seconds:-}" "${total_duration_seconds:-}" "${max_rss_kb:-}" "${max_hwm_kb:-}" \
    "${final_local_height:-}" "${home_bytes:-}" "${data_bytes:-}" "${app_bytes:-}" "${wal_bytes:-}" \
    "${post_gc_app:-}" "${post_gc_wal:-}" "${post_rewrite_app:-}" "${post_rewrite_wal:-}" "${home:-}" >>"$out_row"
}

run_case() {
  local profile="$1"
  local mode="$2"
  local dwell="$3"
  local case_dir="$OUT_DIR/${profile}_${mode}"
  mkdir -p "$case_dir"

  log "starting profile=$profile mode=$mode dwell=${dwell}s"

  local before_file="$case_dir/homes_before.txt"
  ls -1d "$HOME"/.celestia-app-mainnet-treedb-* 2>/dev/null >"$before_file" || true

  local run_log="$case_dir/run_celestia.stdout.log"
  local rss_samples="$case_dir/rss_samples.tsv"
  local dwell_samples="$case_dir/dwell_samples.tsv"
  local run_pid rss_pid home_dir sync_log sync_complete_utc sync_complete_epoch node_pid

  (
    export USE_LOCAL_TREE_STACK="$USE_LOCAL_TREE_STACK"
    export LOCAL_GOMAP_DIR="$LOCAL_GOMAP_DIR"
    export TREEDB_OPEN_PROFILE="$profile"
    export POST_SYNC_DWELL_SECONDS="$dwell"
    export POLL_INTERVAL_SECONDS=10
    export CAPTURE_HEAP_ON_MAX_RSS=0
    export CAPTURE_PPROF_ON_STUCK=0
    export CAPTURE_FULL_SMAPS_ON_MAX_RSS=0
    export CAPTURE_DEBUG_VARS_ON_MAX_RSS=0
    export BOOTSTRAP_FALLBACK_MODE=config_only
    if [[ -n "$FIXED_STOP_AT_LOCAL_HEIGHT" ]]; then
      export STOP_AT_LOCAL_HEIGHT="$FIXED_STOP_AT_LOCAL_HEIGHT"
    fi
    if [[ -n "$FIXED_BOOTSTRAP_FALLBACK_HOME" ]]; then
      export BOOTSTRAP_FALLBACK_HOME="$FIXED_BOOTSTRAP_FALLBACK_HOME"
    fi
    exec "$RUN_CMD"
  ) >"$run_log" 2>&1 &
  run_pid=$!

  home_dir="$(wait_for_new_home "$before_file" "$run_pid")"
  sync_log="$home_dir/sync/sync-time.log"
  printf 'home=%s\n' "$home_dir" >"$case_dir/meta.env"
  node_pid="$(wait_for_node_pid "$home_dir" "$run_pid")"

  monitor_rss "$node_pid" "$rss_samples" &
  rss_pid=$!

  if (( dwell > 0 )); then
    wait_for_sync_complete "$run_log" "$sync_log" "$run_pid"
    sync_complete_utc="$(sync_value "$sync_log" sync_complete_utc)"
    if [[ -z "$sync_complete_utc" ]]; then
      sync_complete_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    fi
    sync_complete_epoch="$(date -u -d "$sync_complete_utc" +%s 2>/dev/null || date +%s)"
    printf 'minute_index\tts_utc\tepoch\telapsed_since_sync_seconds\trss_kb\thwm_kb\tmax_rss_kb_so_far\tmax_hwm_kb_so_far\thome_bytes\tdata_bytes\tapp_bytes\twal_bytes\tsched_enabled\tsched_state\tsched_reason\tmaint_phase\tmaint_attempts\tmaint_acquired\tmaint_with_rewrite\trewrite_runs\tqueued_exec_runs\tplan_runs\tplan_last_result\tplan_last_selected_segments\tplan_last_selected_bytes_stale\tsteady_probe_pending\tplanner_refresh_attempts\tplanner_refresh_successes\tplanner_refresh_last_retained_bytes\tplanner_refresh_last_plan_bytes_total\tcurrent_set_segments\tcurrent_set_bytes_total\tcurrent_set_refresh_scans\trewrite_queue_len\tbytes_stale_total\tdebt_visible\tdebt_visible_source\tdebt_visible_bytes_stale\tdebt_last_deferral_reason\tdebt_last_deferral_age_ms\trewrite_ledger_bytes_total\trewrite_ledger_bytes_stale\tretained_prune_runs\tretained_prune_candidate_bytes\tobserved_gc_runs\tgc_last_eligible_bytes\tcheckpoint_kick_runs\tforeground_quiet\tforeground_full_quiet\tforeground_low_pressure\n' >"$dwell_samples"
    local i
    for (( i=1; i<= dwell / DWELL_SAMPLE_INTERVAL_SECONDS; i++ )); do
      if ! kill -0 "$run_pid" >/dev/null 2>&1 || ! kill -0 "$node_pid" >/dev/null 2>&1; then
        break
      fi
      collect_live_sample_row "$home_dir" "$home_dir/data/application.db" "$node_pid" "$rss_samples" "$i" "$sync_complete_epoch" >>"$dwell_samples"
      sleep "$DWELL_SAMPLE_INTERVAL_SECONDS"
    done
  fi

  wait "$run_pid"
  kill "$rss_pid" >/dev/null 2>&1 || true
  wait "$rss_pid" 2>/dev/null || true

  write_run_summary "$case_dir" "$profile" "$mode" "$sync_log" "$home_dir" "$rss_samples"

  if [[ "$mode" == "sync_only" ]]; then
    run_offline_maintenance "$home_dir/data/application.db" "$case_dir"
    append_campaign_row "$case_dir/summary.env" "$case_dir/offline_sizes.env"
  else
    append_campaign_row "$case_dir/summary.env"
  fi

  log "finished profile=$profile mode=$mode"
}

if [[ -z "$FIXED_BOOTSTRAP_FALLBACK_HOME" ]]; then
  FIXED_BOOTSTRAP_FALLBACK_HOME="$(latest_existing_home)"
fi
if [[ -z "$FIXED_STOP_AT_LOCAL_HEIGHT" ]]; then
  FIXED_STOP_AT_LOCAL_HEIGHT="$(fetch_remote_height)"
fi

{
  printf 'fixed_bootstrap_fallback_home=%s\n' "$FIXED_BOOTSTRAP_FALLBACK_HOME"
  printf 'fixed_stop_at_local_height=%s\n' "$FIXED_STOP_AT_LOCAL_HEIGHT"
  printf 'run_cmd=%s\n' "$RUN_CMD"
  printf 'treemap_bin=%s\n' "$TREEMAP_BIN"
  printf 'local_gomap_dir=%s\n' "$LOCAL_GOMAP_DIR"
  printf 'profiles=%s\n' "${PROFILES[*]}"
  printf 'dwell_seconds=%s\n' "$DWELL_SECONDS"
  printf 'dwell_sample_interval_seconds=%s\n' "$DWELL_SAMPLE_INTERVAL_SECONDS"
  printf 'rss_sample_interval_seconds=%s\n' "$RSS_SAMPLE_INTERVAL_SECONDS"
} >"$OUT_DIR/meta.env"

for profile in "${PROFILES[@]}"; do
  for mode in "${MODES[@]}"; do
    case "$mode" in
      sync_only)
        run_case "$profile" "sync_only" "0"
        ;;
      dwell15m)
        run_case "$profile" "dwell15m" "$DWELL_SECONDS"
        ;;
      *)
        echo "unknown mode: $mode" >&2
        exit 1
        ;;
    esac
  done
done

log "all runs complete: $OUT_DIR"
