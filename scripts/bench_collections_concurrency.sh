#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_concurrency_XXXXXX)}"
COUNT="${COUNT:-1}"
BENCHTIME="${BENCHTIME:-3s}"
CPU_LIST="${CPU_LIST:-1,2,4,8,12}"
TREE_ENGINE="${TREEDB_COLLECTION_BENCH_ENGINE:-command_wal_relaxed}"
TREE_FORMAT="${TREEDB_COLLECTION_DOCUMENT_FORMAT:-template-v1}"
TREE_BATCH="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-16000}"
DATA_OUTER="${TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG:-true}"
INDEX_OUTER="${TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG:-true}"

TREE_REGEX="${TREE_REGEX:-^BenchmarkCollectionConcurrency(ReadPrimaryParallel|ReadPrimaryIntoParallel|SecondaryLookupUniqueParallel|SecondaryLookupNonUniqueParallel)$|^BenchmarkCollectionMixedReadWrite(Primary|SecondaryUnique)$}"
SQLITE_REGEX="${SQLITE_REGEX:-^BenchmarkSQLiteConcurrency(ReadPrimaryJSONParallel|ReadPrimaryNativeColumnsParallel|SecondaryLookupJSONParallel|SecondaryLookupNativeColumnsParallel|MixedReadWriteJSON|MixedReadWriteNativeColumns)$}"

mkdir -p "$OUT_DIR"

run_go_bench() {
  local name=$1
  local tags=$2
  local regex=$3
  local out="$OUT_DIR/$name"
  mkdir -p "$out"
  local cmd=(go test)
  if [[ -n "$tags" ]]; then
    cmd+=(-tags "$tags")
  fi
  cmd+=(./TreeDB/collections -run '^$' -bench "$regex" -benchmem -count "$COUNT" -benchtime "$BENCHTIME" -cpu "$CPU_LIST" -json)
  echo "running $name -> $out/go_test.json"
  if [[ "$name" == treedb* ]]; then
    TREEDB_COLLECTION_BENCH_ENGINE="$TREE_ENGINE" \
    TREEDB_COLLECTION_DOCUMENT_FORMAT="$TREE_FORMAT" \
    TREEDB_COLLECTION_BENCH_BATCH_SIZE="$TREE_BATCH" \
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$DATA_OUTER" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$INDEX_OUTER" \
    GOWORK=off "${cmd[@]}" | tee "$out/go_test.json"
  else
    GOWORK=off "${cmd[@]}" | tee "$out/go_test.json"
  fi
  python3 - "$out/go_test.json" "$out/bench.txt" <<'PY'
import json, sys
src, dst = sys.argv[1:3]
with open(src, encoding='utf-8') as inp, open(dst, 'w', encoding='utf-8') as out:
    for line in inp:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get('Action') == 'output':
            out.write(item.get('Output', ''))
PY
}

run_go_bench treedb "" "$TREE_REGEX"
run_go_bench sqlite sqlite_bench "$SQLITE_REGEX"

cat >"$OUT_DIR/README.md" <<EOF
# TreeDB-vs-SQLite Collection Concurrency Benchmark

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- count: \`$COUNT\`
- benchtime: \`$BENCHTIME\`
- cpu list: \`$CPU_LIST\`
- TreeDB profile: \`$TREE_ENGINE\`
- TreeDB format: \`$TREE_FORMAT\`
- TreeDB batch size: \`$TREE_BATCH\`
- TreeDB storage policy: \`data_outer=$DATA_OUTER,index_outer=$INDEX_OUTER\`
- SQLite setup: WAL, synchronous=NORMAL, wal_autocheckpoint=0; concurrency rows report \`sqlite_max_open_conns\`.

Artifacts:

- \`treedb/go_test.json\`, \`treedb/bench.txt\`
- \`sqlite/go_test.json\`, \`sqlite/bench.txt\`

The benchmark suffix from Go's \`-cpu\` sweep records \`GOMAXPROCS\`. Rows also report \`gomaxprocs\`; SQLite rows report \`sqlite_max_open_conns\`.
EOF

echo "concurrency benchmark bundle: $OUT_DIR"
echo "bundle index: $OUT_DIR/README.md"
