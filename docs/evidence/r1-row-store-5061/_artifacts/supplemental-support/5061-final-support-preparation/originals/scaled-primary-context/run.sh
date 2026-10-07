#!/bin/bash
set -euo pipefail
cd /home/mikers/gomap-r1-vlog-rotation-boundary
out=/home/mikers/gomap-r1-vlog-rotation-boundary-evidence-unlink-corrected
mkdir -p "$out"
exec >"$out/validation.log" 2>&1
trap 'status=$?; printf "%s\n" "$status" >"$out/exit-status"; git rev-parse HEAD >"$out/source-after.txt"; git status --porcelain >"$out/status-after.txt"' EXIT
git rev-parse HEAD >"$out/source.txt"
git rev-parse HEAD^{tree} >"$out/tree.txt"
git status --porcelain >"$out/status-before.txt"
test ! -s "$out/status-before.txt"
python3 -c 'import os,json; keys=("GOROOT","GOCACHE","GOMODCACHE","GOWORK","GOTOOLCHAIN","GOMAXPROCS","GOGC","GOMEMLIMIT","GOFLAGS","GODEBUG"); print(json.dumps({k:os.environ.get(k) for k in keys},sort_keys=True))' >"$out/environment.txt"
"$GOROOT/bin/go" version
"$GOROOT/bin/go" test -c ./TreeDB/db -o "$out/db.test"
sha256sum "$out/db.test" >"$out/binary.sha256"
"$GOROOT/bin/go" version -m "$out/db.test" >"$out/buildinfo.txt"
"$out/db.test" -test.run '^TestR1PrimaryValueLogScaledRotationEvacuatesSealedSource$' -test.v -test.count=1 -test.timeout=3m
"$GOROOT/bin/go" test -race ./TreeDB/db -run '^TestR1PrimaryValueLogScaledRotationEvacuatesSealedSource$' -count=1 -timeout=3m
"$GOROOT/bin/go" vet ./TreeDB/db
