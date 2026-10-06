#!/bin/bash
set -uo pipefail
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export PATH="$GOROOT/bin:$PATH" GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod GOWORK=off
owned=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-default-path-repair/repaired
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair
cd "$owned"
python3 - "$out/repair-source.json" <<'PY'
import pathlib,hashlib,json,sys
expected=json.loads(pathlib.Path(sys.argv[1]).read_text());actual={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('.').rglob('*') if p.is_file()}
assert expected==actual, {'extra':set(actual)-set(expected),'missing':set(expected)-set(actual),'changed':[k for k in expected.keys()&actual.keys() if expected[k]!=actual[k]]}
PY
run(){ name=$1; shift; "$@" > "$out/$name.stdout" 2> "$out/$name.stderr"; result=$?; echo "$result" > "$out/$name.exit"; return "$result"; }
run repair-db-build go test -c -trimpath -buildvcs=false -o "$out/bin/repaired-db.test" ./TreeDB/db || exit $?
run repair-public-build go test -c -trimpath -buildvcs=false -o "$out/bin/repaired-public.test" ./TreeDB || exit $?
run repaired-sizes go run "$out/sizes.go" || exit $?
pattern='COW|Snapshot|OneShot|GetVersioned|RawKVParity|PublicCommandWAL.*Batch|DurabilityProfilePublicEntrypointInventory|CompactIndex'
run repaired-normal go test -json -count=1 -run "$pattern" -timeout=10m ./TreeDB/db ./TreeDB/caching ./TreeDB || exit $?
run repaired-race go test -race -json -count=1 -run "$pattern" -timeout=10m ./TreeDB/db ./TreeDB/caching ./TreeDB || exit $?
run repaired-safe go test -tags=treedb_safe -json -count=1 -run "$pattern" -timeout=10m ./TreeDB/db ./TreeDB/caching ./TreeDB || exit $?
run repaired-vet go vet ./TreeDB/db ./TreeDB/caching ./TreeDB || exit $?
python3 - "$out/repair-source.json" <<'PY'
import pathlib,hashlib,json,sys
expected=json.loads(pathlib.Path(sys.argv[1]).read_text());actual={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('.').rglob('*') if p.is_file()}
assert expected==actual, {'extra':set(actual)-set(expected),'missing':set(expected)-set(actual),'changed':[k for k in expected.keys()&actual.keys() if expected[k]!=actual[k]]}
PY
