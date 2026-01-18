#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/artifacts/bench}"
RUNS="${RUNS:-1}"
TS=$(date +%Y%m%d%H%M%S)
LOG="$OUT_DIR/bench_slabopt_matrix_$TS.log"

mkdir -p "$OUT_DIR"
echo "slabopt bench matrix $(date)" | tee "$LOG"

run_case() {
  local name="$1"
  shift
  echo "== $name ==" | tee -a "$LOG"
  local i
  for ((i = 1; i <= RUNS; i++)); do
    echo "-- run $i" | tee -a "$LOG"
    /usr/bin/time -p "$@" 2>&1 | tee -a "$LOG"
  done
}

base_batch=(go run ./cmd/unified_bench -dbs treedb -test batch_write -keys 200000 -valsize 1024 -batchsize 1000)
base_random=(go run ./cmd/unified_bench -dbs treedb -test random_write -keys 200000 -valsize 1024 -batchsize 1000)

run_case "batch_write/random/baseline" "${base_batch[@]}" -dataset-val-pattern random -treedb-slab-compression none
run_case "batch_write/repeat/baseline" "${base_batch[@]}" -dataset-val-pattern repeat -treedb-slab-compression none
run_case "batch_write/zero/zstd" "${base_batch[@]}" -dataset-val-pattern zero -treedb-slab-compression zstd -treedb-slab-compression-min-bytes 64
run_case "batch_write/random/lanes2" "${base_batch[@]}" -dataset-val-pattern random -treedb-journal-lanes 2 -treedb-slab-compression none
run_case "batch_write/random/split_vlog" "${base_batch[@]}" -dataset-val-pattern random -treedb-split-value-log -treedb-memtable-value-log-pointers -treedb-slab-compression none
run_case "batch_write/random/columnar" "${base_batch[@]}" -dataset-val-pattern random -treedb-index-columnar-leaves -treedb-slab-compression none
run_case "batch_write/random/base_delta" "${base_batch[@]}" -dataset-val-pattern random -treedb-index-internal-base-delta -treedb-slab-compression none
run_case "batch_write/random/columnar_base_delta" "${base_batch[@]}" -dataset-val-pattern random -treedb-index-columnar-leaves -treedb-index-internal-base-delta -treedb-slab-compression none
run_case "random_write/random/baseline" "${base_random[@]}" -dataset-val-pattern random -treedb-slab-compression none
run_case "random_write/random/columnar_base_delta" "${base_random[@]}" -dataset-val-pattern random -treedb-index-columnar-leaves -treedb-index-internal-base-delta -treedb-slab-compression none

echo "log: $LOG"
