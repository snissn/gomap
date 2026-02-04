#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "docs-check requires ripgrep (rg)" >&2
  exit 2
fi

fail=0

check_no_matches() {
	local pattern="$1"
	shift
	if rg -n "$pattern" "$@"; then
		fail=1
	fi
}

treedb_docs=(docs/TREEDB_*.md TreeDB/README.md TreeDB/AGENTS.md AGENTS.md)

# TreeDB must not reference legacy slab files/paths.
check_no_matches 'TreeDB/slab|data-[0-9]+\\.slab|data-\\*\\.slab|\\.slab' "${treedb_docs[@]}"

# Legacy option names must not leak into TreeDB docs (except in the migration doc).
check_no_matches 'DisableWAL|RelaxedSync|AllowUnsafe|ValueLogPointerThreshold|MaxValueLogRetainedBytesHard|MaxValueLogRetainedBytes|ValueLogCompressionAutotune' \
	docs --glob 'TREEDB_*.md' --glob '!TREEDB_WRITE_PATHS.md'
check_no_matches 'DisableWAL|RelaxedSync|AllowUnsafe|ValueLogPointerThreshold|MaxValueLogRetainedBytesHard|MaxValueLogRetainedBytes|ValueLogCompressionAutotune' \
	TreeDB/README.md TreeDB/AGENTS.md

# No references to deleted doc roots.
check_no_matches 'docs/legacy/|slab-optimization/|TreeDB/specs/|@AGENTS\\.md|GEMINI_PLAN\\.md|celestia_testing_info\\.md|invalid_value_debug\\.md|treedb_bench_plan\\.md|prompt\\.md' \
	docs TreeDB AGENTS.md README.md

if [ "$fail" -ne 0 ]; then
  echo "docs-check: FAILED" >&2
  exit 1
fi
echo "docs-check: OK"
