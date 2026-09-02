#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

BENCHTIME="${BENCHTIME:-500ms}"
COUNT="${COUNT:-1}"
CC="${CC:-cc}"
CFLAGS="${CFLAGS:--O3 -std=c11 -Wall -Wextra}"

GO_OUT="$(mktemp /tmp/treedb_leaf_go_XXXXXX.txt)"
MATCHED_GO_OUT="$(mktemp /tmp/treedb_leaf_matched_go_XXXXXX.txt)"
MATCHED_C_OUT="$(mktemp /tmp/treedb_leaf_matched_c_XXXXXX.txt)"
RUST_OUT="$(mktemp /tmp/treedb_leaf_rust_XXXXXX.txt)"
PARSED_OUT="$(mktemp /tmp/treedb_leaf_parsed_XXXXXX.tsv)"
MATCHED_C_BIN="$(mktemp /tmp/treedb_leaf_matched_c_bin_XXXXXX)"
trap 'rm -f "$MATCHED_C_BIN"' EXIT

echo "TreeDB leaf page smoke benchmark"
echo "repo: $REPO_ROOT"
echo "go benchtime: $BENCHTIME count: $COUNT"
echo "c compiler: $CC $CFLAGS"
echo

(
  cd "$REPO_ROOT"
  go test -run '^$' \
    -bench 'Benchmark(AddLeafEntryWithPrefix_(NoPrefix|PrefixHeavy|PrefixLight)|SearchLeaf_(Columnar_FixedBE8|Columnar_Variable16|PrefixV2|ColumnarPrefixV2))$' \
    -benchtime="$BENCHTIME" \
    -count="$COUNT" \
    ./TreeDB/node
) | tee "$GO_OUT"

echo
(
  cd "$REPO_ROOT"
  go run ./experiments/rust_leaf_bench/matched_go
) | tee "$MATCHED_GO_OUT"

echo
(
  cd "$REPO_ROOT"
  "$CC" $CFLAGS "$SCRIPT_DIR/matched_c/main.c" -o "$MATCHED_C_BIN"
  "$MATCHED_C_BIN"
) | tee "$MATCHED_C_OUT"

echo
(
  cd "$REPO_ROOT"
  cargo run --release --quiet --manifest-path "$SCRIPT_DIR/Cargo.toml"
) | tee "$RUST_OUT"

awk '
function case_name(raw, name) {
  name = raw
  sub(/-[0-9]+$/, "", name)
  if (name == "BenchmarkAddLeafEntryWithPrefix_NoPrefix") return "builder/no_prefix"
  if (name == "BenchmarkAddLeafEntryWithPrefix_PrefixHeavy") return "builder/prefix_heavy"
  if (name == "BenchmarkAddLeafEntryWithPrefix_PrefixLight") return "builder/prefix_light"
  if (name == "BenchmarkSearchLeaf_Columnar_FixedBE8") return "search/columnar_fixed_be8"
  if (name == "BenchmarkSearchLeaf_Columnar_Variable16") return "search/columnar_variable16"
  if (name == "BenchmarkSearchLeaf_PrefixV2") return "search/prefix_v2"
  if (name == "BenchmarkSearchLeaf_ColumnarPrefixV2") return "search/columnar_prefix_v2"
  return ""
}
/^Benchmark/ {
  c = case_name($1)
  if (c == "") next
  for (i = 1; i <= NF; i++) {
    if ($i == "ns/op") {
      print "GO\t" c "\t" $(i-1)
      next
    }
  }
}
' "$GO_OUT" > "$PARSED_OUT"

awk '$1 == "MATCHED_GO" { print "MATCHED_GO\t" $2 "\t" $3 }' "$MATCHED_GO_OUT" >> "$PARSED_OUT"
awk '$1 == "MATCHED_C" { print "MATCHED_C\t" $2 "\t" $3 }' "$MATCHED_C_OUT" >> "$PARSED_OUT"
awk '$1 == "RESULT" { print "RUST\t" $2 "\t" $3 }' "$RUST_OUT" >> "$PARSED_OUT"

echo
awk '
BEGIN {
  order[1] = "builder/no_prefix"
  order[2] = "builder/prefix_heavy"
  order[3] = "builder/prefix_light"
  order[4] = "search/columnar_fixed_be8"
  order[5] = "search/columnar_variable16"
  order[6] = "search/prefix_v2"
  order[7] = "search/columnar_prefix_v2"
}
$1 == "GO" { go[$2] = $3 + 0 }
$1 == "MATCHED_GO" { matched_go[$2] = $3 + 0 }
$1 == "MATCHED_C" { matched_c[$2] = $3 + 0 }
$1 == "RUST" { rust[$2] = $3 + 0 }
END {
  printf "%-30s %13s %13s %10s %10s %10s %10s %8s\n", "case", "prod go ns/op", "match go ns/op", "c ns/op", "rust ns/op", "c/mgo", "r/mgo", "r/c"
  printf "%-30s %13s %13s %10s %10s %10s %10s %8s\n", "----", "-------------", "-------------", "-------", "----------", "-----", "-----", "---"
  for (i = 1; i <= 7; i++) {
    c = order[i]
    g = go[c]
    mg = matched_go[c]
    mc = matched_c[c]
    r = rust[c]
    if (g == 0 || mg == 0 || mc == 0 || r == 0) {
      printf "%-30s %13s %13s %10s %10s %10s %10s %8s\n", c, g ? sprintf("%.2f", g) : "missing", mg ? sprintf("%.2f", mg) : "missing", mc ? sprintf("%.2f", mc) : "missing", r ? sprintf("%.2f", r) : "missing", "n/a", "n/a", "n/a"
      continue
    }
    c_ratio = mc / mg
    r_ratio = r / mg
    rc_ratio = r / mc
    printf "%-30s %13.2f %13.2f %10.2f %10.2f %10.2fx %10.2fx %8.2fx\n", c, g, mg, mc, r, c_ratio, r_ratio, rc_ratio
  }
}
' "$PARSED_OUT"

echo
echo "raw production go output: $GO_OUT"
echo "raw matched go output:    $MATCHED_GO_OUT"
echo "raw matched c output:     $MATCHED_C_OUT"
echo "raw rust output:          $RUST_OUT"
