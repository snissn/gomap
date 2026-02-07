#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

echo "issue384 invariant gate"

# Pointer durability across reopen/checkpoint/writesync.
go test ./TreeDB -run 'TestReopenVerify_WALOn_Checkpoint|TestReopenVerify_WALOn_WriteSync|TestReopenVerify_WALOn_Checkpoint_CompressionModes' -count=1

# Persistent vlog reachability and rewrite correctness.
go test ./TreeDB/db -run 'TestValueLogGC_RemovesUnreferencedSegment|TestValueLogRewriteOffline_RewritesAndShrinks' -count=1

# Core cached write/flush behavior remains healthy.
go test ./TreeDB/caching -run '^(TestFlush|TestBackpressure|TestCachingDB_CheckpointParallelBuildCompletes)' -count=1

echo "issue384 invariant gate: PASS"
