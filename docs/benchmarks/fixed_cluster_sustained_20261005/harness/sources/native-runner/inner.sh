#!/bin/bash
set -euo pipefail
base=/home/mikers/gomap-1242-v4-assigned-owner-semantic-red-root-v1
cd /home/mikers/gomap-1242-v4-assigned-owner-semantic-red-root-v1/source
export GOENV=off GOWORK=off GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 GOMEMLIMIT=4GiB
export GOFLAGS='-p=1 -mod=readonly'
export GOCACHE=/home/mikers/.cache/go-build GOMODCACHE=/home/mikers/go/pkg/mod
export GOTMPDIR="$base/go-tmp" GOROOT=/home/mikers/gomap-q5-evidence/toolchains/go1.26.0-linux-amd64
export PATH="$GOROOT/bin:/usr/bin:/bin"
cg="/sys/fs/cgroup$(awk -F: '$1=="0" {print $3}' /proc/self/cgroup)"
test "$(cat "$cg/memory.max")" = 8589934592
test "$(cat "$cg/memory.swap.max")" = 0
for n in memory.max memory.swap.max memory.peak memory.events; do cat "$cg/$n" > "$base/receipts/start-$n.txt"; done
set +e
/usr/bin/time -v timeout --kill-after=10s 250s go test -json ./TreeDB/collections -timeout=30s -run '^TestCollectionCommandWALAssignedApplyWaitsForAdmissionWithRawHeld$' -count=1 > "$base/receipts/test.jsonl" 2> "$base/receipts/test.stderr"
result=$?
set -e
printf '%s\n' "$result" > "$base/receipts/test.exit"
for n in memory.max memory.swap.max memory.peak memory.events; do cat "$cg/$n" > "$base/receipts/end-$n.txt"; done
exit "$result"
