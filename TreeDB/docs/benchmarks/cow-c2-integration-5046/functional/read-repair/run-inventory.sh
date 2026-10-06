#!/bin/bash
set -u
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
cd /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-read-repair
"$GOROOT/bin/go" test ./TreeDB -run '^TestDurabilityProfilePublicEntrypointInventory$' -count=1 -json > /mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/read-repair/inventory.json 2>&1
COW_EXIT=$?
printf 'inventory\t%s\n' "$COW_EXIT" > /mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/read-repair/inventory-exit.tsv
exit "$COW_EXIT"
