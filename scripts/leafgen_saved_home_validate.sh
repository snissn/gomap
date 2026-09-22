#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SRC=${1:?usage: leafgen_saved_home_validate.sh <application.db> [output-dir]}
OUT=${2:-/tmp/leafgen_saved_home_validate_$(date +%Y%m%d%H%M%S)}
DST="$OUT/application.db"
TREEMAP=(go run ./TreeDB/cmd/treemap)

mkdir -p "$OUT"
cp -a "$SRC" "$DST"
cd "$ROOT"

du_bytes() {
  if [[ -e "$1" ]]; then
    if du -sb "$1" >/dev/null 2>&1; then
      du -sb "$1" | awk '{print $1}'
    else
      du -sk "$1" | awk '{print $1 * 1024}'
    fi
  else
    echo 0
  fi
}

capture_sizes() {
  local phase=$1
  jq -n \
    --arg phase "$phase" \
    --argjson application_db_bytes "$(du_bytes "$DST")" \
    --argjson maindb_bytes "$(du_bytes "$DST/maindb")" \
    --argjson index_db_bytes "$(du_bytes "$DST/maindb/index.db")" \
    --argjson leaf_vlog_bytes "$(du_bytes "$DST/maindb/leaf_vlog")" \
    --argjson value_vlog_bytes "$(du_bytes "$DST/maindb/value_vlog")" \
    --argjson wal_bytes "$(du_bytes "$DST/wal")" \
    '{
      phase:$phase,
      application_db_bytes:$application_db_bytes,
      maindb_bytes:$maindb_bytes,
      index_db_bytes:$index_db_bytes,
      leaf_vlog_bytes:$leaf_vlog_bytes,
      value_vlog_bytes:$value_vlog_bytes,
      wal_bytes:$wal_bytes
    }'
}

{
  echo "source=$SRC"
  echo "copy=$DST"
} > "$OUT/run.txt"

capture_sizes before > "$OUT/size-before.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json > "$OUT/plan-before.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-before-force.json"
IDS=$(jq -r '(.CandidateGenerationIDs // []) | map(tostring) | join(",")' "$OUT/plan-before-force.json")
printf '%s\n' "$IDS" > "$OUT/generation-ids.txt"

if [[ -n "$IDS" ]]; then
  GOWORK=off "${TREEMAP[@]}" leafgen-pack "$DST" -rw -from-plan -force -json > "$OUT/pack.json"
else
  jq -n '{skipped:true, reason:"no candidate generations"}' > "$OUT/pack.json"
fi
capture_sizes after_pack > "$OUT/size-after-pack.json"

GOWORK=off "${TREEMAP[@]}" leafgen-gc "$DST" -rw -json > "$OUT/gc1.json"
capture_sizes after_gc1 > "$OUT/size-after-gc1.json"

GOWORK=off "${TREEMAP[@]}" leafgen-gc "$DST" -rw -json > "$OUT/gc2.json"
capture_sizes after_gc2 > "$OUT/size-after-gc2.json"

GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json > "$OUT/plan-after.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-after-force.json"
ls -lh "$DST/maindb/leaf_vlog" > "$OUT/leaf-vlog-ls.txt"
ls -lh "$DST/maindb/value_vlog" > "$OUT/value-vlog-ls.txt"

jq -n \
  --arg source "$SRC" \
  --arg copy "$DST" \
  --arg generation_ids "$IDS" \
  --slurpfile before "$OUT/size-before.json" \
  --slurpfile after_pack "$OUT/size-after-pack.json" \
  --slurpfile after_gc1 "$OUT/size-after-gc1.json" \
  --slurpfile after_gc2 "$OUT/size-after-gc2.json" \
  --slurpfile plan_before "$OUT/plan-before.json" \
  --slurpfile plan_before_force "$OUT/plan-before-force.json" \
  --slurpfile pack "$OUT/pack.json" \
  --slurpfile gc1 "$OUT/gc1.json" \
  --slurpfile gc2 "$OUT/gc2.json" \
  --slurpfile plan_after "$OUT/plan-after.json" \
  --slurpfile plan_after_force "$OUT/plan-after-force.json" \
  '{
    source: $source,
    copy: $copy,
    generation_ids: $generation_ids,
    sizes: {
      before: $before[0],
      after_pack: $after_pack[0],
      after_gc1: $after_gc1[0],
      after_gc2: $after_gc2[0]
    },
    plan_before: {
      default: {
        admission: $plan_before[0].Admission,
        current_generation_id: $plan_before[0].CurrentGenerationID,
        candidate_generation_ids: $plan_before[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_before[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_before[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_before[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_before[0].ExpectedReclaimPerByteCopiedPPM
      },
      force: {
        admission: $plan_before_force[0].Admission,
        current_generation_id: $plan_before_force[0].CurrentGenerationID,
        candidate_generation_ids: $plan_before_force[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_before_force[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_before_force[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_before_force[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_before_force[0].ExpectedReclaimPerByteCopiedPPM
      }
    },
    pack: $pack[0],
    gc1: $gc1[0],
    gc2: $gc2[0],
    plan_after: {
      default: {
        admission: $plan_after[0].Admission,
        current_generation_id: $plan_after[0].CurrentGenerationID,
        candidate_generation_ids: $plan_after[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_after[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_after[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_after[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_after[0].ExpectedReclaimPerByteCopiedPPM
      },
      force: {
        admission: $plan_after_force[0].Admission,
        current_generation_id: $plan_after_force[0].CurrentGenerationID,
        candidate_generation_ids: $plan_after_force[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_after_force[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_after_force[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_after_force[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_after_force[0].ExpectedReclaimPerByteCopiedPPM
      }
    }
  }' > "$OUT/summary.json"

cat <<EOM
leafgen saved-home validation complete
  output: $OUT
  source: $SRC
  copy: $DST
  ids: ${IDS:--}
  summary: $OUT/summary.json
EOM
