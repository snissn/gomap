#!/bin/bash
set -u
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
A=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/minimization-ce808
S=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-minimization-ce808
cd "$S"
python3 "$A/verify.py" "$A" "$S" > "$A/initial-source-check.json" || exit $?
"$GOROOT/bin/go" env -json > "$A/goenv.json"
"$GOROOT/bin/go" test -json ./TreeDB/caching ./TreeDB/internal/valuelog ./TreeDB/db -run 'Test(COWEmptyBasisIteratorNeedsNoReadWorkspace|COWCodecSlotsGrowWithinActualAdmission|COWNativeBlockOwnerAvoidsZSTDState|RawKVPreparedFinalizer)' -count=1 > "$A/initial.json" 2>&1
E=$?
printf 'initial\t%s\n' "$E" > "$A/initial-exit.tsv"
exit "$E"
