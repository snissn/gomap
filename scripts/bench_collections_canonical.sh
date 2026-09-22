#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

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
