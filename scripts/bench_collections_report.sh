#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_report_XXXXXX)}"
BENCH_REGEX="${BENCH_REGEX:-Benchmark(CollectionInsertProvidedID|CollectionGetByID|CollectionDeleteByID|CollectionInsertWithSecondaryIndexes|CollectionDeleteWithSecondaryIndexes|SecondaryLookupUnique|SecondaryLookupNonUnique|SecondaryUpsertFieldChange|CollectionCreateIndexBackfillExistingDocs)}"
COUNT="${COUNT:-3}"
BENCHTIME="${BENCHTIME:-}"

RAW_JSON="$OUT_DIR/collections_bench.json"
CPU_PROFILE="$OUT_DIR/collections_cpu.pprof"
MEM_PROFILE="$OUT_DIR/collections_mem.pprof"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT="$(git rev-parse --short HEAD)"

mkdir -p "$OUT_DIR"

cmd=(
  go test ./TreeDB/collections
  -run '^$'
  -bench "$BENCH_REGEX"
  -benchmem
  -count "$COUNT"
  -json
  -cpuprofile "$CPU_PROFILE"
  -memprofile "$MEM_PROFILE"
)

if [[ -n "$BENCHTIME" ]]; then
  cmd+=(-benchtime "$BENCHTIME")
fi

echo "running focused collections benchmarks into: $OUT_DIR"
GOWORK=off "${cmd[@]}" | tee "$RAW_JSON"

GOWORK=off go run ./cmd/collection_bench_report \
  -in "$RAW_JSON" \
  -out-dir "$OUT_DIR" \
  -branch "$BRANCH" \
  -commit "$COMMIT" \
  -bench-pattern "$BENCH_REGEX" \
  -count "$COUNT"

cat >"$OUT_DIR/README.md" <<EOF
# Focused Collections Benchmark Bundle

- output directory: \`$OUT_DIR\`
- branch: \`$BRANCH\`
- commit: \`$COMMIT\`
- benchmark regex: \`$BENCH_REGEX\`
- benchmark count: \`$COUNT\`
- raw benchmark json: \`$RAW_JSON\`
- cpu profile: \`$CPU_PROFILE\`
- memory profile: \`$MEM_PROFILE\`
- markdown report: \`$OUT_DIR/collections_report.md\`
- html report: \`$OUT_DIR/collections_report.html\`
- json report: \`$OUT_DIR/collections_report.json\`
EOF

echo "markdown report: $OUT_DIR/collections_report.md"
echo "html report:     $OUT_DIR/collections_report.html"
echo "bundle index:    $OUT_DIR/README.md"
