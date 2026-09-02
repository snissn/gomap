#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

shopt -s nullglob

fail=0

have_rg=0
if command -v rg >/dev/null 2>&1; then
	have_rg=1
fi

search() {
	local pattern="$1"
	shift
	if [ "$have_rg" -eq 1 ]; then
		rg -n "$pattern" -- "$@"
	else
		grep -RInE "$pattern" -- "$@"
	fi
}

check_no_matches() {
	local pattern="$1"
	shift
	if search "$pattern" "$@"; then
		fail=1
	else
		local rc=$?
		if [ "$rc" -gt 1 ]; then
			echo "docs-check: search failed (rc=$rc) for pattern: $pattern" >&2
			fail=1
		fi
	fi
}

treedb_docs=(docs/TREEDB_*.md TreeDB/README.md TreeDB/AGENTS.md AGENTS.md)

# TreeDB must not reference legacy slab files/paths.
check_no_matches 'TreeDB/slab|data-[0-9]+[.]slab|data-\*[.]slab|[.]slab' "${treedb_docs[@]}"

# Legacy option names must not leak into TreeDB docs (except in the migration doc).
treedb_md_except_migration=()
for f in docs/TREEDB_*.md; do
	if [ "$(basename "$f")" != "TREEDB_WRITE_PATHS.md" ]; then
		treedb_md_except_migration+=("$f")
	fi
done
if [ "${#treedb_md_except_migration[@]}" -eq 0 ]; then
	echo "docs-check: missing docs/TREEDB_*.md files (unexpected)" >&2
	exit 1
fi
check_no_matches 'DisableWAL|RelaxedSync|AllowUnsafe|ValueLogPointerThreshold|MaxValueLogRetainedBytesHard|MaxValueLogRetainedBytes|ValueLogCompressionAutotune' \
	"${treedb_md_except_migration[@]}"
check_no_matches 'DisableWAL|RelaxedSync|AllowUnsafe|ValueLogPointerThreshold|MaxValueLogRetainedBytesHard|MaxValueLogRetainedBytes|ValueLogCompressionAutotune' \
	TreeDB/README.md TreeDB/AGENTS.md

# No references to deleted doc roots.
check_no_matches 'docs/legacy/|slab-optimization/|TreeDB/specs/|@AGENTS[.]md|GEMINI_PLAN[.]md|celestia_testing_info[.]md|invalid_value_debug[.]md|treedb_bench_plan[.]md|prompt[.]md' \
	docs TreeDB AGENTS.md README.md

if [ "$fail" -ne 0 ]; then
  echo "docs-check: FAILED" >&2
  exit 1
fi
echo "docs-check: OK"
