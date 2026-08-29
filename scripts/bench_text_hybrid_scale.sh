#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_text_hybrid_scale_$(date +%Y%m%d_%H%M%S)}"
GO_BIN="${GO_BIN:-go}"
BIN="$RUN_DIR/bin/treedb_text_hybrid_scale"
SMOKE_ROWS="${SMOKE_ROWS:-10000}"
SMOKE_QUERIES="${SMOKE_QUERIES:-3}"
SMOKE_BATCH_SIZE="${SMOKE_BATCH_SIZE:-4096}"
ONE_M_ROWS="${ONE_M_ROWS:-1000000}"
ONE_M_QUERIES="${ONE_M_QUERIES:-3}"
ONE_M_BATCH_SIZE="${ONE_M_BATCH_SIZE:-16384}"
TEN_M_ROWS="${TEN_M_ROWS:-10000000}"
TEN_M_QUERIES="${TEN_M_QUERIES:-3}"
TEN_M_BATCH_SIZE="${TEN_M_BATCH_SIZE:-32768}"
DIMS="${DIMS:-16}"
M="${M:-8}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-128}"
EF_SEARCH="${EF_SEARCH:-128}"
TOP_K="${TOP_K:-10}"
SMOKE_CANDIDATE_LIMIT="${SMOKE_CANDIDATE_LIMIT:-64}"
ONE_M_CANDIDATE_LIMIT="${ONE_M_CANDIDATE_LIMIT:-65536}"
TEN_M_CANDIDATE_LIMIT="${TEN_M_CANDIDATE_LIMIT:-655360}"
READERS="${READERS:-4}"
SMOKE_MAINTENANCE_UPDATES="${SMOKE_MAINTENANCE_UPDATES:-100}"
SMOKE_MAINTENANCE_DELETES="${SMOKE_MAINTENANCE_DELETES:-50}"
ONE_M_MAINTENANCE_UPDATES="${ONE_M_MAINTENANCE_UPDATES:-10000}"
ONE_M_MAINTENANCE_DELETES="${ONE_M_MAINTENANCE_DELETES:-5000}"
TEN_M_MAINTENANCE_UPDATES="${TEN_M_MAINTENANCE_UPDATES:-10000}"
TEN_M_MAINTENANCE_DELETES="${TEN_M_MAINTENANCE_DELETES:-5000}"
RUN_SMOKE="${RUN_SMOKE:-true}"
RUN_1M="${RUN_1M:-false}"
RUN_10M="${RUN_10M:-false}"
APPROVE_10M="${APPROVE_10M:-false}"
KEEP_DB="${KEEP_DB:-false}"
PHASES="${PHASES:-all}"
RETRIEVAL_REPETITIONS="${RETRIEVAL_REPETITIONS:-1}"

is_true() {
  [[ "$1" == "true" || "$1" == "1" || "$1" == "yes" ]]
}

if ! [[ "$RETRIEVAL_REPETITIONS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RETRIEVAL_REPETITIONS must be a positive integer" >&2
  exit 2
fi
if (( RETRIEVAL_REPETITIONS > 1 )) && [[ "$PHASES" != "retrieval" ]]; then
  echo "RETRIEVAL_REPETITIONS>1 requires PHASES=retrieval; refusing to repeat a non-retrieval campaign" >&2
  exit 2
fi
if is_true "$RUN_10M" && [[ "$TEN_M_ROWS" != "10000000" ]]; then
  echo "TEN_M_ROWS must be exactly 10000000" >&2
  exit 2
fi
if is_true "$RUN_10M" && [[ "$PHASES" != "all" ]]; then
  echo "the retained 10M campaign requires PHASES=all" >&2
  exit 2
fi
if is_true "$RUN_10M" && [[ "$KEEP_DB" != "false" && "$KEEP_DB" != "0" && "$KEEP_DB" != "no" ]]; then
  echo "the retained 10M campaign requires KEEP_DB=false so cleanup is measurable" >&2
  exit 2
fi
if is_true "$RUN_10M" && is_true "$APPROVE_10M" && [[ -n "$(git status --porcelain)" ]]; then
  echo "the retained 10M campaign requires a clean checkout before the binary is built" >&2
  exit 2
fi

mkdir -p "$RUN_DIR"
COMMIT=$(git rev-parse HEAD)
TREE_OID=$(git rev-parse HEAD^{tree})
TREEDB_SUBTREE_OID=$(git rev-parse "$TREE_OID:TreeDB")
HARNESS_SUBTREE_OID=$(git rev-parse "$TREE_OID:cmd/treedb_text_hybrid_scale")
BASE_SHA=$(git merge-base HEAD origin/main 2>/dev/null || true)
VCS_MODIFIED=true
if [[ -z "$(git status --porcelain)" ]]; then
  VCS_MODIFIED=false
fi

{
  echo "timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "repo=$ROOT"
  echo "branch=$(git branch --show-current)"
  echo "commit=$COMMIT"
  echo "tree_oid=$TREE_OID"
  echo "treedb_subtree_oid=$TREEDB_SUBTREE_OID"
  echo "harness_subtree_oid=$HARNESS_SUBTREE_OID"
  echo "base_ref=origin/main"
  echo "base_sha=$BASE_SHA"
  echo "go=$($GO_BIN version 2>/dev/null || true)"
  echo "uname=$(uname -a)"
  echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  echo "ncpu=$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  echo "uptime=$(uptime 2>/dev/null || true)"
  echo "filesystem=$(df -h "$RUN_DIR" 2>/dev/null | tail -1 || true)"
} > "$RUN_DIR/context.txt"

build_binary() {
  mkdir -p "$RUN_DIR/bin"
  local ldflags="-X main.buildCommit=$COMMIT -X main.buildTreeOID=$TREE_OID -X main.buildTreeDBSubtree=$TREEDB_SUBTREE_OID -X main.buildHarnessSubtree=$HARNESS_SUBTREE_OID -X main.buildVCSModified=$VCS_MODIFIED"
  env GOWORK=off "$GO_BIN" build -buildvcs=true -trimpath -ldflags "$ldflags" -o "$BIN" ./cmd/treedb_text_hybrid_scale 2>&1 | tee "$RUN_DIR/build.log"
  local binary_version binary_provenance
  binary_version=$("$GO_BIN" version -m "$BIN")
  printf '%s\n' "$binary_version" > "$RUN_DIR/binary.version.txt"
  binary_provenance=$("$BIN" -print-build-provenance)
  printf '%s\n' "$binary_provenance" > "$RUN_DIR/binary.provenance.json"
  for expected in "\"commit\":\"$COMMIT\"" "\"tree_oid\":\"$TREE_OID\"" "\"treedb_subtree_oid\":\"$TREEDB_SUBTREE_OID\"" "\"harness_subtree_oid\":\"$HARNESS_SUBTREE_OID\"" "\"vcs_modified\":\"$VCS_MODIFIED\""; do
    if [[ "$binary_provenance" != *"$expected"* ]]; then
      echo "built binary lacks required provenance setting: $expected" >&2
      exit 2
    fi
  done
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$BIN" | cut -d' ' -f1 > "$RUN_DIR/binary.sha256"
  else
    shasum -a 256 "$BIN" | cut -d' ' -f1 > "$RUN_DIR/binary.sha256"
  fi
}

build_scale_command() {
  local out="$1" rows="$2" queries="$3" batch_size="$4" candidate_limit="$5" updates="$6" deletes="$7"
  SCALE_CMD=("$BIN"
    -out-dir "$out"
    -rows "$rows"
    -batch-size "$batch_size"
    -dims "$DIMS"
    -m "$M"
    -ef-construction "$EF_CONSTRUCTION"
    -ef-search "$EF_SEARCH"
    -top-k "$TOP_K"
    -candidate-limit "$candidate_limit"
    -hybrid-max-postings-scanned "$((rows * 4))"
    -queries "$queries"
    -readers "$READERS"
    -backfill-rows "$rows"
    -text-only-rows "$rows"
    -run-text-only=true
    -source-chunk-rows "$rows"
    -run-source-chunk=true
    -maintenance-updates "$updates"
    -maintenance-deletes "$deletes"
    -keep-db="$KEEP_DB"
    -phases "$PHASES"
    -base-ref origin/main
    -base-sha "$BASE_SHA")
}

CURRENT_OUT=""
CURRENT_PHASE=""
record_interruption() {
  local signal="$1"
  if [[ -n "$CURRENT_OUT" ]]; then
    printf '{"status":"interrupted","exit_code":130,"failures":[{"phase":"%s","status":"interrupted","error":"signal %s"}]}\n' "$CURRENT_PHASE" "$signal" > "$CURRENT_OUT/run_status.json"
  fi
  exit 130
}
trap 'record_interruption INT' INT
trap 'record_interruption TERM' TERM

run_scale() {
  local label="$1" rows="$2" queries="$3" batch_size="$4" candidate_limit="$5" updates="$6" deletes="$7"
  local out="$RUN_DIR/$label"
  mkdir -p "$out"
  cp "$RUN_DIR/context.txt" "$out/context.txt"
  cp "$RUN_DIR/binary.sha256" "$out/binary.sha256"
  build_scale_command "$out" "$rows" "$queries" "$batch_size" "$candidate_limit" "$updates" "$deletes"
  printf '%q ' "${SCALE_CMD[@]}" > "$out/command.txt"
  echo >> "$out/command.txt"
  printf '{"status":"running","exit_code":-1,"failures":[]}\n' > "$out/run_status.json"
  CURRENT_OUT="$out"
  CURRENT_PHASE="$label"
  echo "==> $label rows=$rows queries=$queries candidate_limit=$candidate_limit"
  set +e
  if [[ "$(uname -s)" == "Darwin" ]]; then
    /usr/bin/time -l -o "$out/resources.txt" "${SCALE_CMD[@]}" 2>&1 | tee "$out/run.log"
  else
    /usr/bin/time -v -o "$out/resources.txt" "${SCALE_CMD[@]}" 2>&1 | tee "$out/run.log"
  fi
  local status=${PIPESTATUS[0]}
  set -e
  if (( status == 0 )); then
    printf '{"status":"complete","exit_code":0,"failures":[]}\n' > "$out/run_status.json"
  else
    printf '{"status":"failed","exit_code":%d,"failures":[{"phase":"%s","status":"failed","error":"command exit %d"}]}\n' "$status" "$label" "$status" > "$out/run_status.json"
  fi
  CURRENT_OUT=""
  CURRENT_PHASE=""
  return "$status"
}

write_campaign_commands() {
  local out="$RUN_DIR/scale_10m"
  local selected_phases="$PHASES"
  PHASES=all
  build_scale_command "$out" "$TEN_M_ROWS" "$TEN_M_QUERIES" "$TEN_M_BATCH_SIZE" "$TEN_M_CANDIDATE_LIMIT" "$TEN_M_MAINTENANCE_UPDATES" "$TEN_M_MAINTENANCE_DELETES"
  PHASES="$selected_phases"
  {
    echo "Build once from the clean candidate and run the identical final shape through this runner:"
    printf 'RUN_DIR=%q RUN_SMOKE=false RUN_1M=false RUN_10M=true APPROVE_10M=true PHASES=all KEEP_DB=false %q\n' "$RUN_DIR" "scripts/bench_text_hybrid_scale.sh"
    echo "Resolved direct command (the runner also captures command, binary/config hashes, context, logs, resources, status, cleanup, and validation):"
    printf '%q ' "${SCALE_CMD[@]}"
    echo
    printf '%q -seal-artifact %q\n' "$BIN" "$out"
    printf '%q -validate-artifact %q\n' "$BIN" "$out"
  } > "$RUN_DIR/campaign_commands.txt"
}

if is_true "$RUN_SMOKE" || is_true "$RUN_1M" || is_true "$RUN_10M"; then
  build_binary
fi
write_campaign_commands

if is_true "$RUN_SMOKE"; then
  run_scale "scale_smoke_${SMOKE_ROWS}" "$SMOKE_ROWS" "$SMOKE_QUERIES" "$SMOKE_BATCH_SIZE" "$SMOKE_CANDIDATE_LIMIT" "$SMOKE_MAINTENANCE_UPDATES" "$SMOKE_MAINTENANCE_DELETES"
fi

if is_true "$RUN_1M"; then
  for rep in $(seq 1 "$RETRIEVAL_REPETITIONS"); do
    run_scale "scale_1m_rep${rep}" "$ONE_M_ROWS" "$ONE_M_QUERIES" "$ONE_M_BATCH_SIZE" "$ONE_M_CANDIDATE_LIMIT" "$ONE_M_MAINTENANCE_UPDATES" "$ONE_M_MAINTENANCE_DELETES"
  done
fi

if is_true "$RUN_10M"; then
  if ! is_true "$APPROVE_10M"; then
    printf '{"status":"not_started","reason":"APPROVE_10M is not true"}\n' > "$RUN_DIR/10m_not_started.json"
    echo "10M run not started: set APPROVE_10M=true only after smoke/preflight qualification." >&2
    exit 2
  fi
  run_scale "scale_10m" "$TEN_M_ROWS" "$TEN_M_QUERIES" "$TEN_M_BATCH_SIZE" "$TEN_M_CANDIDATE_LIMIT" "$TEN_M_MAINTENANCE_UPDATES" "$TEN_M_MAINTENANCE_DELETES"
  "$BIN" -seal-artifact "$RUN_DIR/scale_10m" | tee "$RUN_DIR/scale_10m/seal.log"
  "$BIN" -validate-artifact "$RUN_DIR/scale_10m" | tee "$RUN_DIR/scale_10m/validation.log"
fi

echo "scale run artifacts: $RUN_DIR"
