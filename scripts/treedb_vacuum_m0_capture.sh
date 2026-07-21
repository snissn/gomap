#!/usr/bin/env bash
set -euo pipefail

# Capture M0's fail-closed public status, offline shrink ceiling, and ten
# legacy-only online samples. This script never enables production vacuum.
ROOT=$(git rev-parse --show-toplevel)
RUN_DIR=${RUN_DIR:-"$ROOT/artifacts/treedb-vacuum-m0/$(date +%Y%m%d_%H%M%S)"}
SHA=$(git rev-parse HEAD)
mkdir -p "$RUN_DIR/raw"

capture_fixture() {
  TREEDB_VACUUM_M0_ARTIFACT="$RUN_DIR/fixture.json" \
  TREEDB_VACUUM_M0_COMMAND="$*" \
  TREEDB_VACUUM_M0_GIT_SHA="$SHA" \
  GOWORK=off go test ./TreeDB/db -run '^TestVacuumM0WriteArtifact$' -count=1 -v
}

cd "$ROOT"
capture_fixture "scripts/treedb_vacuum_m0_capture.sh"

for sample in $(seq 1 10); do
  GOWORK=off go test ./TreeDB/db -run '^$' \
    -bench '^BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x$' \
    -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/legacy-${sample}.txt"
  GOWORK=off go test ./TreeDB/db -run '^$' \
    -bench '^BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x$' \
    -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/public-${sample}.txt"
done

{
  echo "# TreeDB Vacuum M0 Capture"
  echo
  echo "- SHA: \`$SHA\`"
  echo "- Status: \`production-index-vacuum-unavailable\`"
  echo "- Fixture artifact: [fixture.json](fixture.json)"
  echo "- Raw samples: \`raw/legacy-*.txt\` (test-only legacy) and \`raw/public-*.txt\` (fail-closed public)"
  echo "- Command: \`$0\`"
  echo
  echo "The public samples are classification evidence, not successful vacuum measurements."
} >"$RUN_DIR/summary.md"

echo "M0 artifacts written to $RUN_DIR"
