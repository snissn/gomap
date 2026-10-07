#!/usr/bin/env bash
set -euo pipefail
exec 9>/home/mikers/gomap-r1-correctness-185.lock
flock 9
export GOROOT=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64
export GOCACHE=/home/mikers/.cache/gomap-r1-5060-profile-go1264 GOMODCACHE=/home/mikers/go/pkg/mod
export PATH="$GOROOT/bin:$PATH"
export GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=16 GOGC=100 GOMEMLIMIT=off GODEBUG= GOFLAGS=
export R1_GO="$GOROOT/bin/go"
cd /home/mikers/gomap-r1-5067-final-4b78
test "$(git rev-parse HEAD)" = 4b78a03d7ffea4e34a8d78f78c53c89bb9b8e064
test -z "$(git status --porcelain)"
OUT=/home/mikers/gomap-r1-5067-evidence-20261005/clean-4b78
test -d "$OUT"
python3 scripts/r1_collection_source.py > "$OUT/source-before.json"
"$GOROOT/bin/go" version > "$OUT/toolchain.txt"
"$GOROOT/bin/go" test ./TreeDB/collections -run 'TestR1|TestTypedStorage|TestCollectionReadView' -count=1 -timeout=5m > "$OUT/r1-normal.log" 2>&1
printf 'Affected R1 collections normal PASS\n'
"$GOROOT/bin/go" test ./TreeDB/collections -run '^TestVectorPartitionSourceImportDependencyEncodingContinuousV2$' -count=1 -timeout=2m > "$OUT/interrupted-vector-test.log" 2>&1
printf 'Isolated interrupted vector-import test PASS\n'
"$GOROOT/bin/go" test -race ./TreeDB/collections -run 'TestR1|TestTypedStorage' -count=1 -timeout=5m > "$OUT/r1-race.log" 2>&1
printf 'R1 plus naming race PASS\n'
"$GOROOT/bin/go" vet ./TreeDB/collections ./TreeDB/db ./TreeDB/internal/rootpublication > "$OUT/vet.log" 2>&1
CAPTURE="$OUT/rehearsal"
python3 scripts/r1_lifecycle_capture.py --out "$CAPTURE" --qualification rehearsal --repetitions 2 --epochs 3 --documents 32 --calls-per-epoch 8 --source-commit 4b78a03d7ffea4e34a8d78f78c53c89bb9b8e064
R1_LIFECYCLE_TEST_PACKET="$CAPTURE/packet.json" PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'r1_lifecycle_*test.py' > "$OUT/real-packet-tests.log" 2>&1
printf 'Current v3 actualpacket and childenvironment tests PASS\n'
python3 scripts/r1_collection_source.py > "$OUT/source-after.json"
cmp "$OUT/source-before.json" "$OUT/source-after.json"
test -z "$(git status --porcelain)"
printf 'Clean4b78 integrated verification complete\n'
