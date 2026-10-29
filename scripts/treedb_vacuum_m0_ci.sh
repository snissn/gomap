#!/usr/bin/env bash
# exec preserves signal delivery to the supervisor that owns the capture group.
set -euo pipefail
: "${RUN_DIR:?RUN_DIR must name a new capture directory}"
exec python3 "$(dirname "${BASH_SOURCE[0]}")/treedb_vacuum_m0_ci.py"
