#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS="$(date +%Y%m%d%H%M%S)"
OUT="${1:-${ROOT}/artifacts/celestia_live_profile/${TS}}"
RUN_CMD="${2:-${RUN_CMD:-/home/mikers/dev/snissn/celestia-app-p4/run_celestia.sh}}"
RUN_HOME_GLOB="${RUN_HOME_GLOB:-$HOME/.celestia-app-mainnet-treedb-*}"
PPROF_BASE_URL="${PPROF_BASE_URL:-http://127.0.0.1:6062/debug/pprof}"
VARS_URL="${VARS_URL:-http://127.0.0.1:6062/debug/vars}"
DEBUG_VARS_INTERVAL_SECONDS="${DEBUG_VARS_INTERVAL_SECONDS:-30}"
CPU_PROFILE_INTERVAL_SECONDS="${CPU_PROFILE_INTERVAL_SECONDS:-180}"
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-10}"
FINAL_HEAP_CAPTURE="${FINAL_HEAP_CAPTURE:-1}"
FINAL_GOROUTINE_CAPTURE="${FINAL_GOROUTINE_CAPTURE:-1}"

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required binary: $1" >&2
    exit 1
  fi
}

require_bin curl
require_bin python3

mkdir -p "$OUT"/{vars,pprof}

list_run_homes() {
  compgen -G "$RUN_HOME_GLOB" | sort || true
}

detect_new_run_home() {
  local before_file="$1"
  python3 - "$before_file" "$RUN_HOME_GLOB" <<'PY'
import glob
import sys

before = set()
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if line:
            before.add(line)

current = sorted(glob.glob(sys.argv[2]))
new = [p for p in current if p not in before]
if new:
    print(new[-1])
PY
}

capture_debug_vars_loop() {
  local run_pid="$1"
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    local stamp tmp out_file latest_tmp latest_file
    stamp="$(date +%Y%m%d%H%M%S)"
    tmp="$OUT/vars/${stamp}.json.tmp"
    out_file="$OUT/vars/${stamp}.json"
    latest_tmp="$OUT/vars/latest.json.tmp"
    latest_file="$OUT/vars/latest.json"
    if curl -fsS --max-time 10 "$VARS_URL" >"$tmp" 2>/dev/null && [[ -s "$tmp" ]]; then
      mv "$tmp" "$out_file"
      if cp "$out_file" "$latest_tmp"; then
        mv "$latest_tmp" "$latest_file"
      else
        rm -f "$latest_tmp"
      fi
    else
      rm -f "$tmp"
    fi
    sleep "$DEBUG_VARS_INTERVAL_SECONDS"
  done
}

capture_cpu_profiles_loop() {
  local run_pid="$1"
  while kill -0 "$run_pid" >/dev/null 2>&1; do
    local stamp out_file
    stamp="$(date +%Y%m%d%H%M%S)"
    out_file="$OUT/pprof/cpu_${stamp}.pb.gz"
    curl -fsS --max-time $((CPU_PROFILE_SECONDS + 20)) \
      "${PPROF_BASE_URL}/profile?seconds=${CPU_PROFILE_SECONDS}" >"$out_file" 2>/dev/null || rm -f "$out_file"
    sleep "$CPU_PROFILE_INTERVAL_SECONDS"
  done
}

capture_final_profile() {
  local url_suffix="$1"
  local name="$2"
  local suffix="$3"
  local out_file="$OUT/pprof/${name}_${suffix}"
  curl -fsS --max-time 20 "${PPROF_BASE_URL}/${url_suffix}" >"$out_file" 2>/dev/null || rm -f "$out_file"
}

before_file="$OUT/run_homes_before.txt"
list_run_homes >"$before_file"

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
run_cmd=$RUN_CMD
treedb_open_profile=${TREEDB_OPEN_PROFILE:-command_wal_relaxed}
post_sync_dwell_seconds=${POST_SYNC_DWELL_SECONDS:-900}
pprof_base_url=$PPROF_BASE_URL
vars_url=$VARS_URL
debug_vars_interval_seconds=$DEBUG_VARS_INTERVAL_SECONDS
cpu_profile_interval_seconds=$CPU_PROFILE_INTERVAL_SECONDS
cpu_profile_seconds=$CPU_PROFILE_SECONDS
META

set +e
(
  set -euo pipefail
  export USE_LOCAL_TREE_STACK="${USE_LOCAL_TREE_STACK:-1}"
  export LOCAL_GOMAP_DIR="${LOCAL_GOMAP_DIR:-$ROOT}"
  export TREEDB_OPEN_PROFILE="${TREEDB_OPEN_PROFILE:-command_wal_relaxed}"
  export POST_SYNC_DWELL_SECONDS="${POST_SYNC_DWELL_SECONDS:-900}"
  bash -lc "$RUN_CMD"
) >"$OUT/launcher.log" 2>&1 &
run_pid=$!
set -e

capture_debug_vars_loop "$run_pid" &
vars_pid=$!
capture_cpu_profiles_loop "$run_pid" &
cpu_pid=$!

set +e
wait "$run_pid"
run_rc=$?
set -e

kill "$vars_pid" >/dev/null 2>&1 || true
kill "$cpu_pid" >/dev/null 2>&1 || true
wait "$vars_pid" >/dev/null 2>&1 || true
wait "$cpu_pid" >/dev/null 2>&1 || true

run_home="$(detect_new_run_home "$before_file" || true)"
printf '%s\n' "$run_home" >"$OUT/run_home.txt"

if [[ "$FINAL_HEAP_CAPTURE" == "1" ]]; then
  capture_final_profile "heap" "heap" "final.pb.gz"
fi
if [[ "$FINAL_GOROUTINE_CAPTURE" == "1" ]]; then
  capture_final_profile "goroutine?debug=1" "goroutine" "final.txt"
fi

python3 - "$OUT" "$run_home" "$run_rc" <<'PY'
import json
import pathlib
import os
import sys

out_dir = pathlib.Path(sys.argv[1])
run_home = pathlib.Path(sys.argv[2]) if sys.argv[2] else None
run_rc = int(sys.argv[3])
latest_vars = out_dir / "vars" / "latest.json"
sync_time = run_home / "sync" / "sync-time.log" if run_home else None
vars_dir = out_dir / "vars"
pprof_dir = out_dir / "pprof"

summary = {
    "run_rc": run_rc,
    "run_home": str(run_home) if run_home else "",
    "vars_latest": str(latest_vars) if latest_vars.exists() else "",
    "sync_time_log": str(sync_time) if sync_time and sync_time.exists() else "",
    "outer_leaf": {},
    "outer_leaf_peaks": {},
    "maintenance": {},
    "storage": {},
    "vlog": {},
    "artifacts": {
        "vars_snapshots": 0,
        "cpu_profiles": 0,
    },
}

def parse_sync_time(path):
    data = {}
    if not path or not path.exists():
        return data
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data

def tree_bytes(path):
    if not path or not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            total += (pathlib.Path(root) / name).stat().st_size
    return total

def load_json_object(path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}

sync = parse_sync_time(sync_time)
if sync:
    for key in (
        "sync_duration_seconds",
        "total_duration_seconds",
        "post_sync_dwell_elapsed_seconds",
        "max_rss_kb",
        "max_hwm_kb",
        "end_home_bytes",
        "end_app_bytes",
    ):
        if key in sync:
            summary[key] = sync[key]

vars_data = {}
var_snapshots = sorted(p for p in vars_dir.glob("*.json") if p.name != "latest.json")
summary["artifacts"]["vars_snapshots"] = len(var_snapshots)
summary["artifacts"]["cpu_profiles"] = len(list(pprof_dir.glob("cpu_*.pb.gz")))
if latest_vars.exists():
    vars_data = load_json_object(latest_vars)
    treedb = vars_data.get("treedb", vars_data)
    if not isinstance(treedb, dict):
        treedb = {}
    for key in (
        "treedb.process.read_path.outer_leaf.loads_total",
        "treedb.process.read_path.outer_leaf.point_loads_total",
        "treedb.process.read_path.outer_leaf.iterator_loads_total",
        "treedb.process.read_path.outer_leaf.bytes_total",
        "treedb.process.read_path.outer_leaf.sample_mod",
        "treedb.process.read_path.outer_leaf.samples_total",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hits_total",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hits_total",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hits_total",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hit_ratio",
    ):
        if key in treedb:
            summary["outer_leaf"][key] = treedb[key]
    peak_keys = (
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hit_ratio",
        "treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hit_ratio",
    )
    peak_values = {key: 0 for key in peak_keys}
    for snapshot_path in var_snapshots:
        snap = load_json_object(snapshot_path)
        snap_treedb = snap.get("treedb", snap)
        if not isinstance(snap_treedb, dict):
            snap_treedb = {}
        for key in peak_keys:
            value = snap_treedb.get(key)
            if isinstance(value, (int, float)) and value > peak_values[key]:
                peak_values[key] = value
    summary["outer_leaf_peaks"] = peak_values
    for key in (
        "treedb.cache.vlog_generation.leaf_pack.attempts",
        "treedb.cache.vlog_generation.leaf_pack.admitted",
        "treedb.cache.vlog_generation.leaf_pack.runs",
        "treedb.cache.vlog_generation.leaf_pack.last_skip_reason",
        "treedb.cache.vlog_generation.maintenance.passes.with_leaf_pack",
    ):
        if key in treedb:
            summary["maintenance"][key] = treedb[key]
    for key in (
        "treedb.cache.vlog_mmap.read.hit_ratio",
        "treedb.cache.vlog_mmap.read.fallback_readat",
        "treedb.cache.vlog_mmap.read.hits",
        "treedb.cache.vlog_mmap.read.miss_no_mapping",
        "treedb.vlog.grouped_frame_cache.hit_ratio",
        "treedb.vlog.grouped_frame_cache.hits",
        "treedb.vlog.grouped_frame_cache.misses",
    ):
        if key in treedb:
            summary["vlog"][key] = treedb[key]

if run_home:
    app_db = run_home / "data" / "application.db"
    maindb = app_db / "maindb"
    summary["storage"] = {
        "application_db_bytes": tree_bytes(app_db),
        "maindb.index_db_bytes": tree_bytes(maindb / "index.db"),
        "maindb.leaf_vlog_bytes": tree_bytes(maindb / "leaf_vlog"),
        "maindb.value_vlog_bytes": tree_bytes(maindb / "value_vlog"),
        "maindb.wal_bytes": tree_bytes(maindb / "wal"),
    }

(out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = ["# Live Celestia Profile Summary", ""]
lines.append(f"- run rc: `{run_rc}`")
if run_home:
    lines.append(f"- run home: `{run_home}`")
if "sync_duration_seconds" in summary:
    lines.append(f"- sync duration seconds: `{summary['sync_duration_seconds']}`")
if "total_duration_seconds" in summary:
    lines.append(f"- total duration seconds: `{summary['total_duration_seconds']}`")
if "max_rss_kb" in summary:
    lines.append(f"- max rss kb: `{summary['max_rss_kb']}`")
if "end_app_bytes" in summary:
    lines.append(f"- final application.db bytes: `{summary['end_app_bytes']}`")
lines.append("")
lines.append("## Outer Leaf Read Telemetry")
for key, value in summary["outer_leaf"].items():
    lines.append(f"- `{key}` = `{value}`")
if summary["outer_leaf_peaks"]:
    lines.append("")
    lines.append("## Outer Leaf Read Telemetry Peaks")
    for key, value in summary["outer_leaf_peaks"].items():
        lines.append(f"- `{key}` = `{value}`")
lines.append("")
lines.append("## Maintenance")
for key, value in summary["maintenance"].items():
    lines.append(f"- `{key}` = `{value}`")
if summary["storage"]:
    lines.append("")
    lines.append("## Storage Split")
    for key, value in summary["storage"].items():
        lines.append(f"- `{key}` = `{value}`")
lines.append("")
lines.append("## Value Log")
for key, value in summary["vlog"].items():
    lines.append(f"- `{key}` = `{value}`")
lines.append("")
lines.append("## Artifacts")
lines.append(f"- vars snapshots: `{summary['artifacts']['vars_snapshots']}`")
lines.append(f"- cpu profiles: `{summary['artifacts']['cpu_profiles']}`")
(out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

echo "out=$OUT"
echo "run_home=$run_home"
echo "run_rc=$run_rc"
exit "$run_rc"
