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
  du -sb "$1" | awk '{print $1}'
}

capture_sizes() {
  local phase=$1
  jq -n \
    --arg phase "$phase" \
    --argjson application_db_bytes "$(du_bytes "$DST")" \
    --argjson leaf_vlog_bytes "$(du_bytes "$DST/maindb/leaf_vlog")" \
    '{phase:$phase, application_db_bytes:$application_db_bytes, leaf_vlog_bytes:$leaf_vlog_bytes}'
}

{
  echo "source=$SRC"
  echo "copy=$DST"
} > "$OUT/run.txt"

capture_sizes before > "$OUT/size-before.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-before.json"
IDS=$(jq -r '.CandidateGenerationIDs | map(tostring) | join(",")' "$OUT/plan-before.json")
printf '%s\n' "$IDS" > "$OUT/generation-ids.txt"

if [[ -n "$IDS" ]]; then
  GOWORK=off "${TREEMAP[@]}" leafgen-pack "$DST" -rw -generation-ids "$IDS" -json > "$OUT/pack.json"
else
  jq -n '{skipped:true, reason:"no candidate generations"}' > "$OUT/pack.json"
fi
capture_sizes after_pack > "$OUT/size-after-pack.json"

GOWORK=off "${TREEMAP[@]}" leafgen-gc "$DST" -rw -json > "$OUT/gc1.json"
capture_sizes after_gc1 > "$OUT/size-after-gc1.json"

GOWORK=off "${TREEMAP[@]}" leafgen-gc "$DST" -rw -json > "$OUT/gc2.json"
capture_sizes after_gc2 > "$OUT/size-after-gc2.json"

GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-after.json"
ls -lh "$DST/maindb/leaf_vlog" > "$OUT/leaf-vlog-ls.txt"

jq -n \
  --arg source "$SRC" \
  --arg copy "$DST" \
  --arg generation_ids "$IDS" \
  --slurpfile before "$OUT/size-before.json" \
  --slurpfile after_pack "$OUT/size-after-pack.json" \
  --slurpfile after_gc1 "$OUT/size-after-gc1.json" \
  --slurpfile after_gc2 "$OUT/size-after-gc2.json" \
  --slurpfile plan_before "$OUT/plan-before.json" \
  --slurpfile pack "$OUT/pack.json" \
  --slurpfile gc1 "$OUT/gc1.json" \
  --slurpfile gc2 "$OUT/gc2.json" \
  --slurpfile plan_after "$OUT/plan-after.json" \
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
      current_generation_id: $plan_before[0].CurrentGenerationID,
      candidate_generation_ids: $plan_before[0].CandidateGenerationIDs,
      expected_reclaim_bytes: $plan_before[0].ExpectedReclaimBytes,
      expected_reclaim_ratio_ppm: $plan_before[0].ExpectedReclaimRatioPPM
    },
    pack: $pack[0],
    gc1: $gc1[0],
    gc2: $gc2[0],
    plan_after: {
      current_generation_id: $plan_after[0].CurrentGenerationID,
      candidate_generation_ids: $plan_after[0].CandidateGenerationIDs,
      expected_reclaim_bytes: $plan_after[0].ExpectedReclaimBytes,
      expected_reclaim_ratio_ppm: $plan_after[0].ExpectedReclaimRatioPPM
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
