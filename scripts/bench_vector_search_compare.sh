#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

USEARCH_VERSION="${USEARCH_VERSION:-2.25.2}"
USEARCH_ARCH="${USEARCH_ARCH:-}"
USEARCH_ROOT="${USEARCH_ROOT:-}"
USEARCH_MANAGED_CACHE=false
RUN_DIR="${RUN_DIR:-/tmp/gomap_vector_search_compare_$(date +%Y%m%d_%H%M%S)}"
COUNT="${COUNT:-3}"
BENCHTIME="${BENCHTIME:-1x}"
RUN_UNSAFE_USEARCH_FILTERED="${RUN_UNSAFE_USEARCH_FILTERED:-false}"
BENCH_REGEX="${BENCH_REGEX:-BenchmarkCollectionVector(SearchExact|IndexSearch(Int8)?|IndexGraphOnlySearch(Int8)?|IndexFilteredSearch)$}"
USEARCH_BENCH_REGEX="${USEARCH_BENCH_REGEX:-BenchmarkCollectionVectorUSearchBaseline$}"
BUILD_BENCH_REGEX="${BUILD_BENCH_REGEX:-BenchmarkCollectionVectorIndexBuild(Int8)?$}"
USEARCH_BUILD_BENCH_REGEX="${USEARCH_BUILD_BENCH_REGEX:-BenchmarkCollectionVectorUSearchBuild$}"
RUN_BUILD_BENCH="${RUN_BUILD_BENCH:-false}"
TREEDB_VECTOR_BENCH_DOCS="${TREEDB_VECTOR_BENCH_DOCS:-1000}"
TREEDB_VECTOR_BENCH_DIMS="${TREEDB_VECTOR_BENCH_DIMS:-32}"

case "${USEARCH_ARCH:-$(uname -m)}" in
	x86_64|amd64) USEARCH_ARCH=amd64 ;;
	aarch64|arm64) USEARCH_ARCH=arm64 ;;
	*) echo "unsupported USearch arch: ${USEARCH_ARCH:-$(uname -m)}" >&2; exit 1 ;;
esac

if [[ -z "$USEARCH_ROOT" ]]; then
	USEARCH_CACHE="/tmp/usearch_${USEARCH_VERSION}_${USEARCH_ARCH}"
	USEARCH_ROOT="$USEARCH_CACHE/root/usr/local"
	USEARCH_MANAGED_CACHE=true
else
	USEARCH_CACHE=""
fi

ensure_usearch() {
	if [[ -f "$USEARCH_ROOT/include/usearch.h" && -f "$USEARCH_ROOT/lib/libusearch_c.so" ]]; then
		return
	fi
	if [[ "$USEARCH_MANAGED_CACHE" != "true" ]]; then
		echo "USEARCH_ROOT is missing include/usearch.h or lib/libusearch_c.so: $USEARCH_ROOT" >&2
		return 1
	fi
	local deb="usearch_linux_${USEARCH_ARCH}_${USEARCH_VERSION}.deb"
	local url="https://github.com/unum-cloud/usearch/releases/download/v${USEARCH_VERSION}/${deb}"
	mkdir -p "$USEARCH_CACHE"
	if [[ ! -f "$USEARCH_CACHE/$deb" ]]; then
		if command -v gh >/dev/null 2>&1; then
			gh release download "v${USEARCH_VERSION}" --repo unum-cloud/usearch --pattern "$deb" --dir "$USEARCH_CACHE"
		else
			curl -L --fail "$url" -o "$USEARCH_CACHE/$deb"
		fi
	fi
	rm -rf "$USEARCH_CACHE/root"
	dpkg-deb -x "$USEARCH_CACHE/$deb" "$USEARCH_CACHE/root"
}

ensure_usearch
mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB Vector Search Comparison

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- USearch version: \`$USEARCH_VERSION\`
- USearch root: \`$USEARCH_ROOT\`
- docs: \`$TREEDB_VECTOR_BENCH_DOCS\`
- dims: \`$TREEDB_VECTOR_BENCH_DIMS\`
- benchtime: \`$BENCHTIME\`
- count: \`$COUNT\`
- TreeDB benchmark regex: \`$BENCH_REGEX\`
- USearch benchmark regex: \`$USEARCH_BENCH_REGEX\`
- TreeDB build benchmark regex: \`$BUILD_BENCH_REGEX\`
- USearch build benchmark regex: \`$USEARCH_BUILD_BENCH_REGEX\`
- run build benchmarks: \`$RUN_BUILD_BENCH\`
- unsafe USearch filtered benchmark: \`$RUN_UNSAFE_USEARCH_FILTERED\`

The harness compares TreeDB exact scan, TreeDB in-memory ANN, TreeDB int8 ANN,
and USearch cosine/f32 HNSW using the same synthetic vector generator.
EOF

echo "USearch root: $USEARCH_ROOT"
echo "run dir: $RUN_DIR"
(
	export TREEDB_VECTOR_BENCH_DOCS
	export TREEDB_VECTOR_BENCH_DIMS
	go test ./TreeDB/collections -run '^$' -bench "$BENCH_REGEX" -benchtime="$BENCHTIME" -count="$COUNT"
	if [[ "$RUN_BUILD_BENCH" == "true" ]]; then
		go test ./TreeDB/collections -run '^$' -bench "$BUILD_BENCH_REGEX" -benchtime="$BENCHTIME" -count="$COUNT"
	fi

	usearch_bench_regex="$USEARCH_BENCH_REGEX"
	if [[ "$RUN_UNSAFE_USEARCH_FILTERED" == "true" ]]; then
		usearch_bench_regex="BenchmarkCollectionVectorUSearch(Filtered)?Baseline$"
		export GODEBUG="cgocheck=0${GODEBUG:+,$GODEBUG}"
	fi
	export CGO_CFLAGS="-I$USEARCH_ROOT/include ${CGO_CFLAGS:-}"
	export CGO_LDFLAGS="-L$USEARCH_ROOT/lib ${CGO_LDFLAGS:-}"
	export LD_LIBRARY_PATH="$USEARCH_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
	(
		cd TreeDB/collections/usearchbench
		go test -tags usearch_bench . -run '^$' -bench "$usearch_bench_regex" -benchtime="$BENCHTIME" -count="$COUNT"
	)
	if [[ "$RUN_BUILD_BENCH" == "true" ]]; then
		(
			cd TreeDB/collections/usearchbench
			go test -tags usearch_bench . -run '^$' -bench "$USEARCH_BUILD_BENCH_REGEX" -benchtime="$BENCHTIME" -count="$COUNT"
		)
	fi
) 2>&1 | tee "$RUN_DIR/bench.txt"
