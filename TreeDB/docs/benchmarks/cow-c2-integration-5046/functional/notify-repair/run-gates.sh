#!/bin/bash
set -u
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
COW_GO="$GOROOT/bin/go"
COW_ARTIFACTS=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/notify-repair
cd /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-notify-repair
"$COW_GO" env -json > "$COW_ARTIFACTS/goenv.json"
COW_PATTERN='^Test(COWClose|COWPublicReadOwner|COWPublicViewCallback|COWPublicContract|COWPublicEmptyBatch|DurabilityProfilePublicEntrypointInventory|Close|CommandWALPublic.*Close)'
: > "$COW_ARTIFACTS/gate-exits.tsv"
for COW_GATE in normal race safe vet; do
 case "$COW_GATE" in
 normal) "$COW_GO" test -json ./TreeDB -run "$COW_PATTERN" -count=3 > "$COW_ARTIFACTS/normal.json" 2>&1 ;;
 race) "$COW_GO" test -race -json ./TreeDB -run "$COW_PATTERN" -count=3 > "$COW_ARTIFACTS/race.json" 2>&1 ;;
 safe) "$COW_GO" test -tags treedb_safe -json ./TreeDB -run "$COW_PATTERN" -count=3 > "$COW_ARTIFACTS/safe.json" 2>&1 ;;
 vet) "$COW_GO" vet ./TreeDB > "$COW_ARTIFACTS/vet.log" 2>&1 ;;
 esac
 COW_EXIT=$?
 printf '%s\t%s\n' "$COW_GATE" "$COW_EXIT" >> "$COW_ARTIFACTS/gate-exits.tsv"
 if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
done
