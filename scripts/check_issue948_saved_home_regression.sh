#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TREEMAP_BIN="${TREEMAP_BIN:-${ROOT}/.tmp_treemap_issue943}"
WAL_ON_HOME="${TREEDB_ISSUE948_SAVED_HOME_WAL_ON_FAST:-/home/mikers/.celestia-app-mainnet-treedb-20260407150935}"
FAST_HOME="${TREEDB_ISSUE948_SAVED_HOME_FAST:-/home/mikers/.celestia-app-mainnet-treedb-20260407153655}"
OUT_DIR="${1:-${ROOT}/artifacts/issue948_saved_home_regression/$(date +%Y%m%d%H%M%S)}"

mkdir -p "${OUT_DIR}"

if [[ ! -x "${TREEMAP_BIN}" ]]; then
  echo "treemap binary not executable: ${TREEMAP_BIN}" >&2
  exit 1
fi

normalize_app_dir() {
  local raw="$1"
  if [[ "$(basename "${raw}")" == "application.db" ]]; then
    printf '%s\n' "${raw}"
    return 0
  fi
  printf '%s\n' "${raw}/data/application.db"
}

run_audit() {
  local label="$1"
  local app_dir="$2"
  local out_file="${OUT_DIR}/${label}.vlog_audit.txt"
  "${TREEMAP_BIN}" vlog-audit "${app_dir}" -rw -rewrite-min-stale-ratio 0.2 >"${out_file}" 2>&1
  grep -E '^(gc_dry_run|rewrite_plan):' "${out_file}" >"${OUT_DIR}/${label}.planner_summary.txt" || true
}

WAL_ON_APP_DIR="$(normalize_app_dir "${WAL_ON_HOME}")"
FAST_APP_DIR="$(normalize_app_dir "${FAST_HOME}")"

run_audit "wal_on_fast" "${WAL_ON_APP_DIR}"
run_audit "fast" "${FAST_APP_DIR}"

GOWORK=off \
TREEDB_ISSUE948_SAVED_HOME_WAL_ON_FAST="${WAL_ON_APP_DIR}" \
TREEDB_ISSUE948_SAVED_HOME_FAST="${FAST_APP_DIR}" \
go test ./TreeDB -run '^TestIssue948SavedHomeRuntimeDebtContract_' -count=1 -v | tee "${OUT_DIR}/go_test.log"

printf 'out_dir=%s\n' "${OUT_DIR}" | tee "${OUT_DIR}/summary.env"
