#!/usr/bin/env bash
# Bound the hosted semantic capture and retain an explicit failure receipt.
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
: "${RUN_DIR:?RUN_DIR must name a new capture directory}"
[[ ! -e "$RUN_DIR" ]] || { echo "refusing existing packet: $RUN_DIR" >&2; exit 2; }
case "${M0_QUALIFICATION_PROFILE:-}" in
  hosted-semantic) budget=720 ;;
  controlled-performance) budget=900 ;;
  *) echo 'an explicit M0 qualification profile is required' >&2; exit 2 ;;
esac
export M0_QUALIFICATION_PROFILE
set +e
timeout --signal=TERM --kill-after=30s "${budget}s" bash "$ROOT/scripts/treedb_vacuum_m0_capture.sh"
status=$?
set -e
mkdir -p "$RUN_DIR"
# No successful receipt is emitted when capture, parser, or budget enforcement fails.
printf '{"qualification_profile":"%s","capture_budget_seconds":%d,"exit_status":%d,"completed":%s}\n' \
  "$M0_QUALIFICATION_PROFILE" "$budget" "$status" "$([[ $status == 0 ]] && echo true || echo false)" \
  >"$RUN_DIR/ci-budget.json"
exit "$status"
