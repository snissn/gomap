#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 user@host [run_script] [trace_basename]" >&2
  exit 2
fi

host="$1"
run_script="${2:-~/run_celestia.sh}"
trace_base="${3:-treedb_trace_$(date +%Y%m%d%H%M%S)}"
remote_trace="/home/mikers/${trace_base}.jsonl"
remote_summary="/home/mikers/${trace_base}.summary.json"

ssh "$host" "TREEDB_TRACE_PATH=$remote_trace TREEDB_TRACE_SUMMARY_PATH=$remote_summary nohup $run_script > ~/celestia_trace_run.log 2>&1 & echo \$!"

echo "trace=$remote_trace"
echo "summary=$remote_summary"
