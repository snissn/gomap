#!/bin/bash
set -u
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
COW_GO="$GOROOT/bin/go"
COW_ARTIFACTS=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/minimization-ce808
cd /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-minimization-ce808
"$COW_GO" env -json > "$COW_ARTIFACTS/goenv.json"
COW_PATTERN='Test(COW|RawKVPreparedFinalizer|SnapshotAllocationAdmission|DictionaryReadDefinition|RootPublicationBuildGroup|Snapshot|CachedSnapshot|AcquireSnapshot|OwnedPointerProjection|ReaderRegistryFastOnly|RewriteSwapPreserves|SelectedValueLogRewriteRevisionLifetime|ValueLogRewriteOnline|RegisterLeafPageLog|OuterLeafCommit|LeafPageLogLanes|CompactStorageLeafPageLog|DurabilityProfilePublicEntrypointInventory|ProfileEntrypoint|BackendReadBarrier|CachingDB_Close|CachingLeafPageLogLane|Close_)'
COW_PACKAGES=(./TreeDB ./TreeDB/caching ./TreeDB/db ./TreeDB/tree ./TreeDB/internal/dictdb ./TreeDB/internal/valuelog ./TreeDB/internal/memtable)
: > "$COW_ARTIFACTS/gate-exits.tsv"
for COW_GATE in normal race safe vet; do
  case "$COW_GATE" in
    normal) "$COW_GO" test -json "${COW_PACKAGES[@]}" -run "$COW_PATTERN" -count=1 > "$COW_ARTIFACTS/full-normal.json" 2>&1 ;;
    race) "$COW_GO" test -race -json "${COW_PACKAGES[@]}" -run "$COW_PATTERN" -count=1 > "$COW_ARTIFACTS/full-race.json" 2>&1 ;;
    safe) "$COW_GO" test -tags treedb_safe -json "${COW_PACKAGES[@]}" -run "$COW_PATTERN" -count=1 > "$COW_ARTIFACTS/full-safe.json" 2>&1 ;;
    vet) "$COW_GO" vet "${COW_PACKAGES[@]}" > "$COW_ARTIFACTS/vet.log" 2>&1 ;;
  esac
  COW_EXIT=$?
  printf '%s\t%s\n' "$COW_GATE" "$COW_EXIT" >> "$COW_ARTIFACTS/gate-exits.tsv"
  if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
done
python3 "$COW_ARTIFACTS/verify.py" "$COW_ARTIFACTS" "$PWD" > "$COW_ARTIFACTS/post-gate-source-check.json"
