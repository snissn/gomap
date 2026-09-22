#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_CELESTIA_AB="${RUN_CELESTIA_AB:-$ROOT/scripts/run_celestia_ab.sh}"
CONTROL_ENV_FILE="${CONTROL_ENV_FILE:-}"
CANDIDATE_BASE_ENV_FILE="${CANDIDATE_BASE_ENV_FILE:-${CANDIDATE_ENV_FILE:-}}"
SEGMENT_BYTES_LIST="${SEGMENT_BYTES_LIST:-4194304 8388608 16777216}"
SEGMENT_SCOPE="${SEGMENT_SCOPE:-hot_warm_cold}"
OUT_ROOT="${OUT_ROOT:-$ROOT/artifacts/celestia_segment_sweep}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_DIR:-$OUT_ROOT/$TS}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

if [[ -z "$CONTROL_ENV_FILE" ]]; then
  echo "CONTROL_ENV_FILE is required" >&2
  exit 2
fi
if [[ -z "$CANDIDATE_BASE_ENV_FILE" ]]; then
  echo "CANDIDATE_BASE_ENV_FILE (or CANDIDATE_ENV_FILE) is required" >&2
  exit 2
fi
if [[ ! -f "$CONTROL_ENV_FILE" ]]; then
  echo "CONTROL_ENV_FILE not found: $CONTROL_ENV_FILE" >&2
  exit 2
fi
if [[ ! -f "$CANDIDATE_BASE_ENV_FILE" ]]; then
  echo "CANDIDATE_BASE_ENV_FILE not found: $CANDIDATE_BASE_ENV_FILE" >&2
  exit 2
fi
if [[ ! -x "$RUN_CELESTIA_AB" ]]; then
  echo "run_celestia_ab script not executable: $RUN_CELESTIA_AB" >&2
  exit 2
fi

require_cmd python3

mkdir -p "$OUT/runs" "$OUT/env"

cat >"$OUT/meta.txt" <<EOF
ts=$TS
root=$ROOT
git_rev=$(git rev-parse HEAD)
git_branch=$(git rev-parse --abbrev-ref HEAD)
control_env=$CONTROL_ENV_FILE
candidate_base_env=$CANDIDATE_BASE_ENV_FILE
segment_scope=$SEGMENT_SCOPE
segment_bytes_list=$SEGMENT_BYTES_LIST
run_celestia_ab=$RUN_CELESTIA_AB
EOF

echo "celestia segment sweep"
echo "out=$OUT"
echo "segment_scope=$SEGMENT_SCOPE"
echo "segment_bytes_list=$SEGMENT_BYTES_LIST"

run_one() {
  local segment_bytes="$1"
  local label="seg_${segment_bytes}"
  local run_out="$OUT/runs/$label"
  local candidate_env="$OUT/env/candidate_${label}.env"
  local run_log="$run_out/run.log"

  mkdir -p "$run_out"
  cp "$CANDIDATE_BASE_ENV_FILE" "$candidate_env"

  case "$SEGMENT_SCOPE" in
    hot_only)
      {
        echo "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES=$segment_bytes"
      } >>"$candidate_env"
      ;;
    hot_warm)
      {
        echo "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES=$segment_bytes"
        echo "TREEDB_VLOG_GENERATION_WARM_SEGMENT_TARGET_BYTES=$segment_bytes"
      } >>"$candidate_env"
      ;;
    hot_warm_cold)
      {
        echo "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES=$segment_bytes"
        echo "TREEDB_VLOG_GENERATION_WARM_SEGMENT_TARGET_BYTES=$segment_bytes"
        echo "TREEDB_VLOG_GENERATION_COLD_SEGMENT_TARGET_BYTES=$segment_bytes"
      } >>"$candidate_env"
      ;;
    *)
      echo "unsupported SEGMENT_SCOPE: $SEGMENT_SCOPE (use hot_only|hot_warm|hot_warm_cold)" >&2
      exit 2
      ;;
  esac

  echo "--- segment_bytes=$segment_bytes label=$label ---"

  if CONTROL_ENV_FILE="$CONTROL_ENV_FILE" \
     CANDIDATE_ENV_FILE="$candidate_env" \
     OUT_DIR="$run_out" \
     "$RUN_CELESTIA_AB" >"$run_log" 2>&1; then
    echo "run ok: $label"
    echo "0" >"$run_out/exit_code.txt"
  else
    code=$?
    echo "run failed: $label exit_code=$code" >&2
    echo "$code" >"$run_out/exit_code.txt"
  fi
}

for segment_bytes in $SEGMENT_BYTES_LIST; do
  run_one "$segment_bytes"
done

python3 - "$OUT" <<'PY'
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
rows = []
for run_dir in sorted((out / "runs").glob("seg_*")):
    name = run_dir.name
    try:
        segment_bytes = int(name.split("_", 1)[1])
    except Exception:
        continue
    exit_code = None
    try:
        exit_code = int((run_dir / "exit_code.txt").read_text(encoding="utf-8").strip())
    except Exception:
        exit_code = -1
    decision_path = run_dir / "decision.json"
    decision = {}
    if decision_path.is_file():
        try:
            decision = json.loads(decision_path.read_text(encoding="utf-8"))
        except Exception:
            decision = {}
    abs_aggr = decision.get("absolute_aggregates", {}) if isinstance(decision, dict) else {}
    c_t_total = abs_aggr.get("median_control_t_total_seconds")
    n_t_total = abs_aggr.get("median_candidate_t_total_seconds")
    c_s_sync = abs_aggr.get("median_control_s_sync_app_bytes")
    n_s_sync = abs_aggr.get("median_candidate_s_sync_app_bytes")
    rows.append(
        {
            "segment_bytes": segment_bytes,
            "segment_mib": segment_bytes / float(1024 * 1024),
            "exit_code": exit_code,
            "reason": decision.get("reason"),
            "wins": decision.get("wins"),
            "losses": decision.get("losses"),
            "neutral": decision.get("neutral"),
            "completed_pairs": decision.get("completed_pairs"),
            "median_control_t_total_seconds": c_t_total,
            "median_candidate_t_total_seconds": n_t_total,
            "delta_t_total_seconds": None if c_t_total is None or n_t_total is None else (n_t_total - c_t_total),
            "median_control_s_sync_app_bytes": c_s_sync,
            "median_candidate_s_sync_app_bytes": n_s_sync,
            "delta_s_sync_app_bytes": None if c_s_sync is None or n_s_sync is None else (n_s_sync - c_s_sync),
            "run_dir": str(run_dir),
        }
    )

summary_json = out / "summary.json"
summary_json.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")

summary_md = out / "summary.md"
lines = []
lines.append("# Celestia Segment Sweep")
lines.append("")
lines.append(f"- out: `{out}`")
lines.append("")
lines.append("| segment bytes | segment MiB | exit | decision reason | pairs | wins/losses/neutral | Δ t_total (s) | Δ s_sync_app (bytes) | run dir |")
lines.append("|---:|---:|---:|---|---:|---:|---:|---:|---|")
for row in rows:
    wln = f"{row['wins']}/{row['losses']}/{row['neutral']}"
    lines.append(
        f"| {row['segment_bytes']} | {row['segment_mib']:.2f} | {row['exit_code']} | {row['reason']} | "
        f"{row['completed_pairs']} | {wln} | {row['delta_t_total_seconds']} | {row['delta_s_sync_app_bytes']} | {row['run_dir']} |"
    )
summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"wrote: {summary_json}")
print(f"wrote: {summary_md}")
PY

echo "done: $OUT"
