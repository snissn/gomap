#!/bin/bash
set -u
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off PYTHONDONTWRITEBYTECODE=1
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
COW_GO="$GOROOT/bin/go"
COW_ARTIFACTS=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/successor-minimization-4add47
cd /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-successor-minimization-4add47
"$COW_GO" env -json > "$COW_ARTIFACTS/goenv.json"
"$COW_GO" version > "$COW_ARTIFACTS/go-version.txt"
python3 -B "$COW_ARTIFACTS/verify.py" "$COW_ARTIFACTS" "$PWD" > "$COW_ARTIFACTS/initial-source-check.json"
COW_EXIT=$?
if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
"$COW_GO" test -json ./TreeDB/caching ./TreeDB/mvcc -run '^TestCOWSuccessor' -count=1 > "$COW_ARTIFACTS/initial.json" 2>&1
COW_EXIT=$?
printf 'initial\t%s\n' "$COW_EXIT" > "$COW_ARTIFACTS/initial-exit.tsv"
if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
COW_PATTERN='Test(COW|Snapshot|CachedSnapshot|AcquireSnapshot|DurabilityProfilePublicEntrypointInventory|SeekGE|PointSuccessor|CommitAt|GetAt|CommitGroup|Ownership)'
COW_PACKAGES=(./TreeDB ./TreeDB/caching ./TreeDB/mvcc)
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
python3 -B "$COW_ARTIFACTS/verify.py" "$COW_ARTIFACTS" "$PWD" > "$COW_ARTIFACTS/post-gate-source-check.json"
COW_EXIT=$?
printf 'final-no-extra-proof\t%s\n' "$COW_EXIT" >> "$COW_ARTIFACTS/gate-exits.tsv"
exit "$COW_EXIT"
