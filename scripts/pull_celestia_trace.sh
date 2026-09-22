#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 user@host /path/to/trace.jsonl [dest_dir]" >&2
  exit 2
fi

host="$1"
remote_trace="$2"
dest="${3:-.}"
remote_summary="${remote_trace%.jsonl}.summary.json"

mkdir -p "$dest"
scp "$host:$remote_trace" "$dest/"
scp "$host:$remote_summary" "$dest/"

printf "pulled %s and %s to %s\n" "$remote_trace" "$remote_summary" "$dest"
