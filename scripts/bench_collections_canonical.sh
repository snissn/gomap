#!/usr/bin/env bash
set -euo pipefail

# Optional durability env overrides (CLI flags -profile, -sqlite-journal-mode,
# and -sqlite-synchronous take precedence over these environment variables):
#   TREEDB_COLLECTION_PROFILE  (default: command_wal_relaxed; also: no_wal_fast, command_wal_durable, bench_unsafe)
#   TREEDB_SQLITE_JOURNAL_MODE (default: WAL; also: OFF, MEMORY, DELETE)
#   TREEDB_SQLITE_SYNCHRONOUS  (default: NORMAL; also: OFF, FULL, EXTRA)

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -n "${TREEDB_COLLECTION_PROFILE:-}" ]]; then
	export TREEDB_COLLECTION_PROFILE
fi
if [[ -n "${TREEDB_SQLITE_JOURNAL_MODE:-}" ]]; then
	export TREEDB_SQLITE_JOURNAL_MODE
fi
if [[ -n "${TREEDB_SQLITE_SYNCHRONOUS:-}" ]]; then
	export TREEDB_SQLITE_SYNCHRONOUS
fi

cmd_display="./scripts/bench_collections_canonical.sh"
for arg in "$@"; do
	printf -v quoted '%q' "$arg"
	cmd_display+=" $quoted"
done
export COLLECTION_CANONICAL_BENCH_COMMAND="$cmd_display"

if [[ "${USE_BUILT_BIN:-0}" == "1" ]]; then
	make collection-canonical-bench-bin >/dev/null
	exec "$repo_root/bin/collection-canonical-bench" "$@"
fi

exec go run ./cmd/collection_canonical_bench "$@"
