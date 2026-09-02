#!/usr/bin/env bash
set -euo pipefail

# Build the test binary once before invoking this collector. It emits an annotated,
# balanced AB/BA transcript which snapshot_iterator_bench treats as fail-closed evidence.
binary=${1:?benchmark binary required}
output=${2:?raw output required}
pairs=${SNAPSHOT_ITERATOR_PAIRS:-8}
benchtime=${SNAPSHOT_ITERATOR_BENCHTIME:-500ms}
cpu=${SNAPSHOT_ITERATOR_CPU:-}
if (( pairs < 2 || pairs % 2 != 0 )); then echo "SNAPSHOT_ITERATOR_PAIRS must be positive and even" >&2; exit 2; fi
if [[ -z "$cpu" ]]; then cpu=$(taskset -pc $$ | sed -E 's/.*: *//' | sed -E 's/[^0-9].*$//' ); fi
if [[ ! "$cpu" =~ ^[0-9]+$ ]]; then echo "could not determine one allowed CPU" >&2; exit 2; fi
printf 'affinity=%s\n' "$cpu" > "${output%.txt}.affinity"
: > "$output"
for keys in 1024 16384; do
  for op in seek next; do
    for ((pair=1; pair<=pairs; pair++)); do
      if (( pair % 2 )); then order=AB; modes=(snapshot public); else order=BA; modes=(public snapshot); fi
      for mode in "${modes[@]}"; do
        if [[ "$mode" == public ]]; then name="public_${op}_baseline"; else name="snapshot_${op}"; fi
        printf '# pair=%d order=%s\n' "$pair" "$order" >> "$output"
        GOMAXPROCS=1 taskset -c "$cpu" "$binary" -test.run '^$' -test.bench "^BenchmarkSnapshotIteratorSeekNext/keys=${keys}/${name}$" -test.benchmem -test.benchtime "$benchtime" >> "$output"
      done
    done
  done
done
