#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$ROOT/artifacts/rewrite_micro/$(date +%Y%m%d%H%M%S)}"
COUNT="${COUNT:-5}"

mkdir -p "$OUT_DIR"

RAW="$OUT_DIR/bench.txt"
SUMMARY="$OUT_DIR/summary.txt"

cd "$ROOT"

echo "[rewrite-micro-gate] running rewrite microbench (count=$COUNT)..."
GOWORK=off go test ./TreeDB/db -run '^$' -bench 'BenchmarkValueLogRewriteOnline_LeafRefs(_ReserveRIDs)?$' -benchmem -count "$COUNT" | tee "$RAW"

awk '
/BenchmarkValueLogRewriteOnline_LeafRefs-12/ {
  ns[++n1] = $3
  alloc[++a1] = $15
}
/BenchmarkValueLogRewriteOnline_LeafRefs_ReserveRIDs-12/ {
  ns2[++n2] = $3
  alloc2[++a2] = $15
}
END {
  if (n1 == 0 || n2 == 0) {
    print "missing benchmark rows; expected both LeafRefs variants" > "/dev/stderr"
    exit 1
  }

  asort(ns); asort(alloc); asort(ns2); asort(alloc2)

  m1 = ns[int((n1 + 1) / 2)]
  ma1 = alloc[int((a1 + 1) / 2)]
  m2 = ns2[int((n2 + 1) / 2)]
  ma2 = alloc2[int((a2 + 1) / 2)]

  printf("LeafRefs: n=%d median_ns_op=%s median_allocs_op=%s min_ns=%s max_ns=%s\n", n1, m1, ma1, ns[1], ns[n1])
  printf("LeafRefs_ReserveRIDs: n=%d median_ns_op=%s median_allocs_op=%s min_ns=%s max_ns=%s\n", n2, m2, ma2, ns2[1], ns2[n2])
}
' "$RAW" | tee "$SUMMARY"

echo "[rewrite-micro-gate] raw: $RAW"
echo "[rewrite-micro-gate] summary: $SUMMARY"
