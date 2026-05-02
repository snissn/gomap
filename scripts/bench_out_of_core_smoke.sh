#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cmd_display="./scripts/bench_out_of_core_smoke.sh"
for arg in "$@"; do
	printf -v quoted '%q' "$arg"
	cmd_display+=" $quoted"
done
export TREEDB_OUT_OF_CORE_SMOKE_COMMAND="$cmd_display"

if [[ "${USE_BUILT_BIN:-0}" == "1" ]]; then
	make treedb-out-of-core-smoke-bin >/dev/null
	exec "$repo_root/bin/treedb-out-of-core-smoke" "$@"
fi

exec go run ./cmd/treedb_out_of_core_smoke "$@"
