#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

echo "Deprecated: slab-opt matrix used removed flags and has been deleted. Use cmd/unified_bench and docs/TREEDB_VALUELOG_AUTOTUNE.md instead." >&2
exit 1
