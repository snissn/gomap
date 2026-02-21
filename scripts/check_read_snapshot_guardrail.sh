#!/usr/bin/env bash
set -euo pipefail

cd cmd/unified_bench
GOWORK=off GOMEMLIMIT=4GiB GOMAXPROCS=2 go test -p 1 . -run '^TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail$' -count=1
