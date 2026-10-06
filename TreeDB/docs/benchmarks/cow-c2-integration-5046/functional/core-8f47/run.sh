#!/bin/bash
set -euo pipefail
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export GOWORK=off
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
GO=/home/mikers/.gvm/gos/go1.26.3/bin/go
OUT=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/candidate-review-fixed
cd /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-candidate-review-fixed
"$GO" version > "$OUT/go-version.txt"
"$GO" env -json > "$OUT/go-env.json"
python3 - <<'VERIFY'
import hashlib,json,pathlib
x=json.load(open("/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/candidate-review-fixed/source-identity.json"))
for p,h in x["files"].items():
 assert hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()==h,p
print("verified",len(x["files"]),"files")
VERIFY
ps -eo pid,ppid,comm,args > "$OUT/processes-start.txt"
packages=(./TreeDB ./TreeDB/caching ./TreeDB/db ./TreeDB/tree ./TreeDB/internal/dictdb ./TreeDB/internal/valuelog ./TreeDB/internal/memtable)
pattern="Test(COW|RawKVPreparedFinalizer|SnapshotAllocationAdmission|DictionaryReadDefinition|RootPublicationBuildGroup|Snapshot|CachedSnapshot|AcquireSnapshot|OwnedPointerProjection|ReaderRegistryFastOnly|RewriteSwapPreserves|SelectedValueLogRewriteRevisionLifetime|ValueLogRewriteOnline|RegisterLeafPageLog|OuterLeafCommit|LeafPageLogLanes|CompactStorageLeafPageLog)"
"$GO" test -json -count=1 -timeout=10m -run "$pattern" "${packages[@]}" > "$OUT/normal.json"
"$GO" test -race -json -count=1 -timeout=15m -run "$pattern" "${packages[@]}" > "$OUT/race.json"
"$GO" test -tags treedb_safe -json -count=1 -timeout=10m -run "$pattern" "${packages[@]}" > "$OUT/safe.json"
"$GO" test -json -count=1 -timeout=10m ./TreeDB/tree ./TreeDB/lifecycle > "$OUT/tree-lifecycle-normal.json"
"$GO" test -race -json -count=1 -timeout=15m ./TreeDB/tree ./TreeDB/lifecycle > "$OUT/tree-lifecycle-race.json"
"$GO" vet "${packages[@]}" > "$OUT/vet.log" 2>&1
"$GO" test -json -count=1 -timeout=10m -run "Test(AppendOnly|BackpressureModeStatsLegacyVsAdaptive|AdaptiveBackpressure|ReopenVerify|CommandWALPublicBatch)" ./TreeDB ./TreeDB/caching > "$OUT/compatibility.json"
