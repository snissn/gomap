#!/usr/bin/env bash
set -euo pipefail

# Template compression smoke bench.
#
# Generates a deterministic JSONL dataset and runs TreeDB's realdata KV harness in
# 4 modes: baseline/off, dict-only, template-only, and template+dict (prepass).
#
# Output: small markdown table with steady-state throughput and value-log size.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"

DATASET="${DATASET:-/tmp/treedb_template_smoke.jsonl}"
REGEN="${REGEN:-0}"
DATASET_KIND="${DATASET_KIND:-templatable}" # templatable|random

TRAIN_N="${TRAIN_N:-10000}"
EVAL_N="${EVAL_N:-5000}"
VALSIZE="${VALSIZE:-1024}"

MODE="${MODE:-wal_off}"           # wal_off|wal_on
RAW_MIB="${RAW_MIB:-64}"          # steady-state bytes written
BATCH="${BATCH:-1024}"            # ops per batch
WORKERS="${WORKERS:-1}"           # concurrent workers
KEY_MODE="${KEY_MODE:-dataset}"   # random|sequential|dataset
POINTER_THRESHOLD="${PTR_T:-1}"   # 1 forces value-log pointers

DICT_TRAIN_MIB="${DICT_TRAIN_MIB:-1}"
DICT_WAIT_S="${DICT_WAIT_S:-10}"

OUT_DIR="${OUT_DIR:-/tmp/treedb_template_smoke_out}"
mkdir -p "$OUT_DIR"

BIN="${BIN:-/tmp/vlog_dict_realdata}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }
}

need_cmd "$PYTHON_BIN"
need_cmd go

maybe_gen_dataset() {
  local want_lines=$((TRAIN_N + EVAL_N))
  local have_lines=0
  if [[ -f "$DATASET" ]]; then
    have_lines=$(wc -l <"$DATASET" | tr -d '[:space:]')
    if [[ -z "$have_lines" ]]; then
      have_lines=0
    fi
  fi

  if [[ "$REGEN" != "0" || ! -f "$DATASET" || "$have_lines" -lt "$want_lines" ]]; then
    echo "generating dataset: $DATASET (kind=$DATASET_KIND train=$TRAIN_N eval=$EVAL_N valsize=$VALSIZE)" >&2
    "$PYTHON_BIN" - "$DATASET" "$TRAIN_N" "$EVAL_N" "$VALSIZE" "$DATASET_KIND" <<'PY'
import json
import os
import random
import sys

path = sys.argv[1]
train_n = int(sys.argv[2])
eval_n = int(sys.argv[3])
valsize = int(sys.argv[4])
kind = sys.argv[5].strip().lower() if len(sys.argv) > 5 else "templatable"

if kind not in ("templatable", "random"):
    raise SystemExit(f"unsupported dataset kind: {kind!r} (expected templatable|random)")

random.seed(1)
total = train_n + eval_n

os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
with open(path, "w", encoding="utf-8") as f:
    for i in range(total):
        key = f"key-{i:08d}"
        if kind == "random":
            alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            v = "".join(random.choice(alphabet) for _ in range(valsize))
        else:
            # Constant prefix/suffix + small variable middle encourages templates.
            var = f"{random.getrandbits(64):016x}"
            v = f"prefix|bucket={i%97:02d}|seq={i:08d}|var={var}|suffix"
            if len(v) < valsize:
                v = v + ("A" * (valsize - len(v)))
            else:
                v = v[:valsize]
        f.write(json.dumps({"key": key, "val": v}, separators=(",", ":")) + "\n")
PY
  fi
}

build_bin() {
  echo "building $BIN" >&2
  go build -o "$BIN" ./TreeDB/cmd/vlog_dict_realdata
}

run_case() {
  local name="$1"
  local compression="$2" # on|off
  local tmpl="$3"        # off|on|prepass

  local out_json="$OUT_DIR/${name}.json"
  rm -f "$out_json"

  "$BIN" \
    -input "$DATASET" \
    -input-encoding string \
    -train "$TRAIN_N" \
    -eval "$EVAL_N" \
    -cap 0 \
    -bench-kv \
    -bench-mode "$MODE" \
    -bench-compression "$compression" \
    -bench-template "$tmpl" \
    -bench-raw-mib "$RAW_MIB" \
    -bench-batch "$BATCH" \
    -bench-workers "$WORKERS" \
    -bench-key-mode "$KEY_MODE" \
    -bench-pointer-threshold "$POINTER_THRESHOLD" \
    -bench-dict-train-mib "$DICT_TRAIN_MIB" \
    -bench-dict-wait-seconds "$DICT_WAIT_S" \
    -bench-out-json "$out_json" \
    >/dev/null

  if [[ ! -f "$out_json" ]]; then
    echo "bench case failed (missing json): case=$name out_json=$out_json" >&2
    return 1
  fi

  "$PYTHON_BIN" - "$name" "$out_json" <<'PY'
import json
import sys

name = sys.argv[1]
path = sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    r = json.load(f)

def get(k, default=""):
    v = r.get(k, default)
    return v if v is not None else default

steady = get("steady_raw_MBps", 0.0)
vlog = get("value_log_bytes", 0)
pub = get("templates_published", 0)
kept = get("template_kept", 0)

print(f"{name}\t{steady:.2f}\t{vlog}\t{pub}\t{kept}")
PY
}

maybe_gen_dataset
build_bin

echo "template smoke (mode=$MODE raw_mib=$RAW_MIB batch=$BATCH ptr_threshold=$POINTER_THRESHOLD)" >&2
echo "dataset=$DATASET kind=$DATASET_KIND train=$TRAIN_N eval=$EVAL_N valsize=$VALSIZE" >&2

printf "| case | steady_raw_MBps | value_log_bytes | templates_published | template_kept |\n"
printf "|---|---:|---:|---:|---:|\n"

for row in \
  $'off\toff\toff' \
  $'dict\ton\toff' \
  $'template\toff\ton' \
  $'prepass\ton\tprepass'; do
  IFS=$'\t' read -r name compression tmpl <<<"$row"
  if ! line=$(run_case "$name" "$compression" "$tmpl"); then
    echo "smoke failed in case=$name (compression=$compression template=$tmpl)" >&2
    exit 1
  fi
  IFS=$'\t' read -r c mbps vlog pub kept <<<"$line"
  printf "| %s | %.2f | %s | %s | %s |\n" "$c" "$mbps" "$vlog" "$pub" "$kept"
done

echo "wrote per-case JSON under: $OUT_DIR" >&2
