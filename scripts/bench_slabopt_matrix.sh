#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

echo "Deprecated: slab-opt matrix uses removed flags. Use slab-optimization/AGENTS_LIVE_BENCH.md instead." >&2
exit 1
