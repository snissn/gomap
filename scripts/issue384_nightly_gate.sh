#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

KEYS="${KEYS:-12000000}"
RUNS="${RUNS:-5}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
SCORED_PATTERN="${SCORED_PATTERN:-medium_compressible_sparse}"
NIGHTLY_EXTRA_PATTERN="${NIGHTLY_EXTRA_PATTERN:-random}"

KEYS="$KEYS" \
RUNS="$RUNS" \
WARMUP_RUNS="$WARMUP_RUNS" \
SCORED_PATTERN="$SCORED_PATTERN" \
NIGHTLY_EXTRA_PATTERN="$NIGHTLY_EXTRA_PATTERN" \
"$ROOT/scripts/issue384_perf_gate.sh"
