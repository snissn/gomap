#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/treedb_leaf_profiles_XXXXXX)}"
CASES="${CASES:-builder/prefix_heavy search/columnar_fixed_be8 search/prefix_v2 search/columnar_prefix_v2}"
LEAF_BUILD_ITERS="${LEAF_BUILD_ITERS:-5000000}"
LEAF_SEARCH_ITERS="${LEAF_SEARCH_ITERS:-20000000}"
PERF_FREQ="${PERF_FREQ:-997}"
PERF_CALLGRAPH="${PERF_CALLGRAPH:-fp}"
PERF_PERCENT_LIMIT="${PERF_PERCENT_LIMIT:-0.5}"
RUST_PROFILE_FEATURES="${RUST_PROFILE_FEATURES:-profile-attribution}"
GO_PROFILE_GOGC="${GO_PROFILE_GOGC:-off}"

GO_BIN="$OUT_DIR/matched_go_leaf"
RUST_BIN="$REPO_ROOT/experiments/rust_leaf_bench/target/release/rust_leaf_bench"
SUMMARY="$OUT_DIR/profile_summary.md"

mkdir -p "$OUT_DIR"

safe_case() {
  printf '%s' "$1" | tr '/:' '__'
}

write_header() {
  {
    echo "# TreeDB Leaf Go/Rust Profile Summary"
    echo
    echo "- repo: \`$REPO_ROOT\`"
    echo "- output: \`$OUT_DIR\`"
    echo "- cases: \`$CASES\`"
    echo "- LEAF_BUILD_ITERS: \`$LEAF_BUILD_ITERS\`"
    echo "- LEAF_SEARCH_ITERS: \`$LEAF_SEARCH_ITERS\`"
    echo "- perf: \`perf record -F $PERF_FREQ -g --call-graph $PERF_CALLGRAPH\`"
    echo "- Rust profile features: \`$RUST_PROFILE_FEATURES\`"
    echo "- Go GOGC during profile: \`$GO_PROFILE_GOGC\`"
    echo
    echo "This profiles the standalone matched Go and Rust smoke binaries only. It does not include the production TreeDB Go benchmark path."
    echo "The Rust binary is built for attribution with selected inlining disabled; use run_leaf_smoke.sh for throughput comparisons."
    echo
  } > "$SUMMARY"
}

append_top_symbols() {
  local label="$1"
  local case_name="$2"
  local report="$3"

  {
    echo "## $label \`$case_name\`"
    echo
    echo '```text'
    awk '
      /^[[:space:]]*[0-9]+\.[0-9]+%/ {
        gsub(/^[[:space:]]+/, "")
        print
        n++
        if (n >= 12) exit
      }
    ' "$report"
    echo '```'
    echo
  } >> "$SUMMARY"
}

run_profile() {
  local label="$1"
  local bin="$2"
  local case_name="$3"
  local safe
  safe="$(safe_case "$case_name")"

  local stdout="$OUT_DIR/${label}_${safe}.stdout.txt"
  local perf_data="$OUT_DIR/${label}_${safe}.perf.data"
  local report="$OUT_DIR/${label}_${safe}.perf.report.txt"

  GOGC="$GO_PROFILE_GOGC" \
    LEAF_CASE="$case_name" \
    LEAF_BUILD_ITERS="$LEAF_BUILD_ITERS" \
    LEAF_SEARCH_ITERS="$LEAF_SEARCH_ITERS" \
    perf record -F "$PERF_FREQ" -g --call-graph "$PERF_CALLGRAPH" -o "$perf_data" -- "$bin" > "$stdout"

  perf report --stdio --no-children --sort=symbol --percent-limit "$PERF_PERCENT_LIMIT" -i "$perf_data" > "$report"
  append_top_symbols "$label" "$case_name" "$report"
}

echo "profile output: $OUT_DIR"
write_header

(
  cd "$REPO_ROOT"
  go build -trimpath -o "$GO_BIN" ./experiments/rust_leaf_bench/matched_go
)

(
  cd "$REPO_ROOT"
  RUSTFLAGS="${RUSTFLAGS:--C force-frame-pointers=yes}" cargo build --release --quiet --features "$RUST_PROFILE_FEATURES" --manifest-path "$SCRIPT_DIR/Cargo.toml"
)

for case_name in $CASES; do
  echo "profiling matched_go $case_name"
  run_profile "matched_go" "$GO_BIN" "$case_name"
  echo "profiling rust $case_name"
  run_profile "rust" "$RUST_BIN" "$case_name"
done

{
  echo "## Raw Files"
  echo
  echo "- summary: \`$SUMMARY\`"
  echo "- perf reports: \`$OUT_DIR/*.perf.report.txt\`"
  echo "- perf data: \`$OUT_DIR/*.perf.data\`"
  echo "- stdout: \`$OUT_DIR/*.stdout.txt\`"
} >> "$SUMMARY"

echo
cat "$SUMMARY"
