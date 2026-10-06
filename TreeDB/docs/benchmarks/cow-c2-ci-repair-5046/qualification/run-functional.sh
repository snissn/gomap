#!/bin/bash
set -uo pipefail
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export PATH="$GOROOT/bin:$PATH" GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod GOWORK=off PYTHONDONTWRITEBYTECODE=1
source=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-pr5069-integrated-974d/source
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/pr5069-integrated-974d
cd "$source"
python3 "$out/source_verify.py" "$source" "$out/source-identity.json" "$out/pre-source-proof.json" || exit $?
go env -json > "$out/go-env.json"
run(){ name=$1; shift; "$@" > "$out/$name.stdout" 2> "$out/$name.stderr"; result=$?; echo "$result" > "$out/$name.exit"; return "$result"; }
pattern='COW|SnapshotAllocationAdmission|BoundedSnapshot|SelectedValueLogRewriteRevisionLifetime|RewriteSwapPreservesWinningCurrentRevision'
run normal go test -json -count=1 -run "$pattern" -timeout=15m ./TreeDB/caching ./TreeDB ./TreeDB/db ./TreeDB/internal/valuelog || exit $?
run race go test -race -json -count=1 -run "$pattern" -timeout=15m ./TreeDB/caching ./TreeDB ./TreeDB/db ./TreeDB/internal/valuelog || exit $?
run safe go test -tags=treedb_safe -json -count=1 -run "$pattern" -timeout=15m ./TreeDB/caching ./TreeDB ./TreeDB/db ./TreeDB/internal/valuelog || exit $?
run vet go vet ./TreeDB/caching ./TreeDB ./TreeDB/db ./TreeDB/internal/valuelog || exit $?
python3 "$out/source_verify.py" "$source" "$out/source-identity.json" "$out/post-source-proof.json" || exit $?
