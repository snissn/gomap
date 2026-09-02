#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

# #1242 PR1B canonical zero-secondary-index writer sweep:
# fresh load, non-indexed _id point updates, and writers 1/2/4/8/16/32.
export DOCS="${DOCS:-1000000}"
export INDEXES="${INDEXES:-0}"
export CONCURRENT_WRITES="${CONCURRENT_WRITES:-80000}"
export WRITERS_LIST="${WRITERS_LIST:-1 2 4 8 16 32}"
export UPDATE_INDEXED_FIELD="${UPDATE_INDEXED_FIELD:-false}"
export TITLE="${TITLE:-Mongo Gateway 0-Index Writer Sweep}"

exec "$ROOT/scripts/mongo_gateway_scaling_bench.sh" --no-reader-sweep "$@"
