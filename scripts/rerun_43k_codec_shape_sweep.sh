#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT_SUFFIX="${OUT_SUFFIX:-rerun}"
TRAIN="${TRAIN:-15000}"
EVAL="${EVAL:-5000}"
K_LIST="${K_LIST:-1,2,4,8,16,32}"
LEVEL="${LEVEL:-fastest}"
DICT_VARIANTS="${DICT_VARIANTS:-}"

if [[ -z "$DICT_VARIANTS" ]]; then
  DICT_VARIANTS="64k:24k:noent,64k:32k:noent,64k:40k:noent,64k:64k:noent,64k:96k:noent,64k:128k:noent,96k:40k:noent,96k:64k:noent,96k:96k:noent,96k:128k:noent,128k:24k:noent,128k:32k:noent,128k:40k:noent,128k:40k:ent,128k:64k:noent,128k:96k:noent,128k:128k:noent,192k:40k:noent,192k:64k:noent,192k:96k:noent,192k:128k:noent,256k:40k:noent,256k:64k:noent,256k:96k:noent,256k:128k:noent,256k:192k:noent,256k:256k:noent,512k:96k:noent,512k:128k:noent,512k:192k:noent,512k:256k:noent"
fi

run_one() {
  local label="$1"
  local input="$2"
  local out="/tmp/${label}_43k_codec_shape_sweep_${OUT_SUFFIX}.json"
  if [[ ! -f "$input" ]]; then
    echo "skip ${label}: missing input ${input}" >&2
    return
  fi

  echo "== ${label}: ${input}" >&2
  go run ./TreeDB/cmd/vlog_codec_shape_sweep \
    -input "$input" \
    -train "$TRAIN" \
    -eval "$EVAL" \
    -cap 0 \
    -k "$K_LIST" \
    -level "$LEVEL" \
    -dict-variants "$DICT_VARIANTS" \
    -out "$out"

  echo "wrote: $out" >&2
  echo "top 12 rows by total_ratio:" >&2
  jq -r 'sort_by(.total_ratio)[:12][] | "\(.mode)\tk=\(.k)\tratio=\(.total_ratio)\ttotal_bytes=\(.total_bytes)"' "$out" >&2
  echo "$out"
}

FAST_OUT="$(run_one fast /tmp/fast_43k_band.jsonl)"
WAL_OUT="$(run_one wal /tmp/wal_43k_band.jsonl)"

echo "fast_out=${FAST_OUT}"
echo "wal_out=${WAL_OUT}"
