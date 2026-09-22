#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

USEARCH_VERSION="${USEARCH_VERSION:-2.25.2}"
USEARCH_ARCH="${USEARCH_ARCH:-}"
USER_USEARCH_ROOT="${USEARCH_ROOT:-}"
USEARCH_ROOT="${USEARCH_ROOT:-}"
RUN_DIR="${RUN_DIR:-/tmp/gomap_vector_search_compare_$(date +%Y%m%d_%H%M%S)}"
COUNT="${COUNT:-1}"
BENCHTIME="${BENCHTIME:-1x}"
CPU_LIST="${CPU_LIST:-1,8}"
RUN_UNSAFE_USEARCH_FILTERED="${RUN_UNSAFE_USEARCH_FILTERED:-false}"
BENCH_REGEX="${BENCH_REGEX:-BenchmarkCollectionVector(SearchExact|IndexSearch(Int8)?|IndexGraphOnlySearch(Int8)?|IndexFilteredSearch|USearchBaseline|USearchProductionCompare)$}"
BUILD_BENCH_REGEX="${BUILD_BENCH_REGEX:-BenchmarkCollectionVector(IndexBuild(Int8)?|USearchBuild)$}"
WRITE_BENCH_REGEX="${WRITE_BENCH_REGEX:-BenchmarkCollectionVector(IndexIncrementalWrite|USearchIncrementalWrite)$}"
RUN_BUILD_BENCH="${RUN_BUILD_BENCH:-false}"
RUN_WRITE_BENCH="${RUN_WRITE_BENCH:-false}"
TREEDB_VECTOR_BENCH_DOCS="${TREEDB_VECTOR_BENCH_DOCS:-10000}"
TREEDB_VECTOR_BENCH_DIMS="${TREEDB_VECTOR_BENCH_DIMS:-64}"
TREEDB_VECTOR_BENCH_M="${TREEDB_VECTOR_BENCH_M:-16}"
TREEDB_VECTOR_BENCH_EF_CONSTRUCTION="${TREEDB_VECTOR_BENCH_EF_CONSTRUCTION:-128}"
TREEDB_VECTOR_BENCH_EF_SEARCH="${TREEDB_VECTOR_BENCH_EF_SEARCH:-128}"
TREEDB_VECTOR_BENCH_TOPK="${TREEDB_VECTOR_BENCH_TOPK:-10}"
TREEDB_VECTOR_BENCH_QUERIES="${TREEDB_VECTOR_BENCH_QUERIES:-16}"

case "$(uname -s)" in
	Linux) USEARCH_OS=linux; USEARCH_LIB=libusearch_c.so ;;
	Darwin) USEARCH_OS=macos; USEARCH_LIB=libusearch_c.dylib ;;
	*) echo "unsupported USearch OS: $(uname -s)" >&2; exit 1 ;;
esac

case "${USEARCH_ARCH:-$(uname -m)}" in
	x86_64|amd64) USEARCH_LINUX_ARCH=amd64; USEARCH_MACOS_ARCH=x86_64 ;;
	aarch64|arm64) USEARCH_LINUX_ARCH=arm64; USEARCH_MACOS_ARCH=arm64 ;;
	*) echo "unsupported USearch arch: ${USEARCH_ARCH:-$(uname -m)}" >&2; exit 1 ;;
esac
if [[ "$USEARCH_OS" == "macos" ]]; then
	USEARCH_PACKAGE_ARCH="$USEARCH_MACOS_ARCH"
else
	USEARCH_PACKAGE_ARCH="$USEARCH_LINUX_ARCH"
fi

if [[ -z "$USEARCH_ROOT" ]]; then
	case "$USEARCH_OS" in
		linux)
			USEARCH_CACHE="/tmp/usearch_${USEARCH_VERSION}_linux_${USEARCH_LINUX_ARCH}"
			USEARCH_ROOT="$USEARCH_CACHE/root/usr/local"
			;;
		macos)
			USEARCH_CACHE="/tmp/usearch_${USEARCH_VERSION}_macos_${USEARCH_MACOS_ARCH}"
			USEARCH_ROOT="$USEARCH_CACHE/root"
			;;
	esac
else
	USEARCH_CACHE=$(dirname "$USEARCH_ROOT")
fi

find_usearch_include_dir() {
	local dir
	for dir in "$USEARCH_ROOT/include" "$USEARCH_ROOT"; do
		if [[ -f "$dir/usearch.h" ]]; then
			printf '%s\n' "$dir"
			return 0
		fi
	done
	return 1
}

find_usearch_lib_dir() {
	local dir
	for dir in "$USEARCH_ROOT/lib" "$USEARCH_ROOT"; do
		if [[ -f "$dir/$USEARCH_LIB" ]]; then
			printf '%s\n' "$dir"
			return 0
		fi
	done
	return 1
}

ensure_usearch() {
	if find_usearch_include_dir >/dev/null 2>&1 && find_usearch_lib_dir >/dev/null 2>&1; then
		return
	fi
	if [[ -n "$USER_USEARCH_ROOT" ]]; then
		echo "USEARCH_ROOT=$USER_USEARCH_ROOT does not contain usearch.h and $USEARCH_LIB" >&2
		exit 1
	fi
	mkdir -p "$USEARCH_CACHE"
	case "$USEARCH_OS" in
		linux)
			local deb="usearch_linux_${USEARCH_LINUX_ARCH}_${USEARCH_VERSION}.deb"
			local url="https://github.com/unum-cloud/usearch/releases/download/v${USEARCH_VERSION}/${deb}"
			if ! command -v dpkg-deb >/dev/null 2>&1; then
				echo "USearch Linux bootstrap requires dpkg-deb to extract $deb; install dpkg/dpkg-deb or set USEARCH_ROOT to an existing USearch install containing usearch.h and $USEARCH_LIB." >&2
				exit 1
			fi
			if [[ ! -f "$USEARCH_CACHE/$deb" ]]; then
				if command -v gh >/dev/null 2>&1; then
					gh release download "v${USEARCH_VERSION}" --repo unum-cloud/usearch --pattern "$deb" --dir "$USEARCH_CACHE"
				else
					curl -L --fail "$url" -o "$USEARCH_CACHE/$deb"
				fi
			fi
			rm -rf "$USEARCH_CACHE/root"
			dpkg-deb -x "$USEARCH_CACHE/$deb" "$USEARCH_CACHE/root"
			;;
		macos)
			local zip="usearch_macos_${USEARCH_MACOS_ARCH}_${USEARCH_VERSION}.zip"
			local url="https://github.com/unum-cloud/usearch/releases/download/v${USEARCH_VERSION}/${zip}"
			if [[ ! -f "$USEARCH_CACHE/$zip" ]]; then
				if command -v gh >/dev/null 2>&1; then
					gh release download "v${USEARCH_VERSION}" --repo unum-cloud/usearch --pattern "$zip" --dir "$USEARCH_CACHE"
				else
					curl -L --fail "$url" -o "$USEARCH_CACHE/$zip"
				fi
			fi
			rm -rf "$USEARCH_CACHE/root"
			mkdir -p "$USEARCH_CACHE/root"
			unzip -q "$USEARCH_CACHE/$zip" -d "$USEARCH_CACHE/root"
			;;
	esac
	if ! find_usearch_include_dir >/dev/null 2>&1 || ! find_usearch_lib_dir >/dev/null 2>&1; then
		echo "USearch bootstrap did not produce usearch.h and $USEARCH_LIB under $USEARCH_ROOT" >&2
		exit 1
	fi
}

ensure_usearch
USEARCH_INCLUDE_DIR=$(find_usearch_include_dir)
USEARCH_LIB_DIR=$(find_usearch_lib_dir)
mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/README.md" <<EOF
# TreeDB Vector Search Comparison

- worktree: \`$ROOT\`
- branch: \`$(git rev-parse --abbrev-ref HEAD)\`
- commit: \`$(git rev-parse --short HEAD)\`
- USearch version: \`$USEARCH_VERSION\`
- USearch OS/arch: \`$USEARCH_OS\` / \`$USEARCH_PACKAGE_ARCH\`
- USearch root: \`$USEARCH_ROOT\`
- USearch include dir: \`$USEARCH_INCLUDE_DIR\`
- USearch lib dir: \`$USEARCH_LIB_DIR\`
- docs: \`$TREEDB_VECTOR_BENCH_DOCS\`
- dims: \`$TREEDB_VECTOR_BENCH_DIMS\`
- M: \`$TREEDB_VECTOR_BENCH_M\`
- efConstruction: \`$TREEDB_VECTOR_BENCH_EF_CONSTRUCTION\`
- efSearch: \`$TREEDB_VECTOR_BENCH_EF_SEARCH\`
- topK: \`$TREEDB_VECTOR_BENCH_TOPK\`
- query stream length: \`$TREEDB_VECTOR_BENCH_QUERIES\`
- cpu/concurrency list: \`$CPU_LIST\`
- benchtime: \`$BENCHTIME\`
- count: \`$COUNT\`
- benchmark regex: \`$BENCH_REGEX\`
- build benchmark regex: \`$BUILD_BENCH_REGEX\`
- write benchmark regex: \`$WRITE_BENCH_REGEX\`
- run build benchmarks: \`$RUN_BUILD_BENCH\`
- run write benchmarks: \`$RUN_WRITE_BENCH\`
- unsafe USearch filtered benchmark: \`$RUN_UNSAFE_USEARCH_FILTERED\`

## Benchmark boundaries

Canonical current production comparison:

- \`BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexNoDocsOneShot\`
  times the public response-owned collection-level no-document convenience API.
  Exact healthy calls use the collection-owned prepared \`hnsw_search_pack_v1\`
  cache, so this row should report \`docs_fetched/search=0\`, no graph-row
  fallback, no typed-column vector fallback, no vector scratch decode,
  \`search_route_hnsw_search_pack/search=1\`, \`open_searcher_calls/op=0\`,
  and \`open_setup_in_timed_loop=0\`. It still reports
  \`response_owned_result_alloc/op=1\` and is not the zero-allocation target.
- \`BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_SearchWithBuffer\`
  and \`.../TreeDB_SearchWithBufferParallel\` time the persisted TreeDB
  no-document \`hnsw_search_pack_v1\` route through
  \`Collection.OpenVectorIndexSearcher\` plus \`VectorIndexSearcher.SearchWithBuffer\`
  when a live validated pack is available. The existing prepared \`column_graph\`
  route remains the fallback for missing/invalid/stale packs or unsupported
  query modes. Setup, inserts, rebuild, open, and warmup are outside the timed
  loop. Parallel runs use one searcher and one
  caller-owned buffer per Go worker; use \`CPU_LIST=1,8\` for c=1/c=8 evidence.
  The script emits a separate \`## search benchmarks cpu=<n>\` block for each
  requested concurrency so worker counts stay unambiguous.
  The timed loop uses production stats mode; a full-diagnostics sample is taken
  outside the timed loop to report candidates/search and edge counters. Healthy
  pack rows should report \`search_route_hnsw_search_pack/search=1\`, no document
  fetches, no graph-row fallback, no vector scratch decode, and route/search-pack
  guardrails such as \`search_route_column_graph_prepared/search\`,
  \`hnsw_search_pack_active/search\`,
  \`hnsw_search_pack_missing/search\`, \`hnsw_search_pack_invalid/search\`,
  \`hnsw_search_pack_stale/search\`, \`hnsw_search_pack_closed/search\`,
  \`hnsw_search_pack_open_ns\`, \`hnsw_search_pack_mapped_B\`,
  \`hnsw_search_pack_heap_copy_B\`, \`docs_fetched/search\`,
  \`graph_row_fallbacks/search\`, score-batch fallback reason flags,
  vector/adjacency source-state counters, and candidate/visited-edge byte
  counters used by the HNSW search-pack stack. Quantized benchmark rows should
  use codec-generic route/asset counters such as
  \`search_route_quantized_only/search\`,
  \`search_route_quantized_rerank/search\`, \`quantized_scorer_active/search\`,
  \`quantized_asset_unavailable/search\`, \`quantized_asset_invalid/search\`,
  \`quantized_asset_stale/search\`, \`quantized_asset_open_ns\`,
  \`quantized_asset_mapped_B\`, and \`quantized_asset_heap_copy_B\`; scalar_u8
  names are reserved for scalar_u8-specific benchmark labels and internals.
- \`BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexWithBuffer\`
  times the collection-level caller-owned result-buffer seam on a warmed
  collection-owned prepared search-pack cache. It uses the same exact
  no-document route contract as \`SearchWithBuffer\`: caller-owned buffer, no
  documents/projection, no graph-row fallback, no vector scratch decode, and
  \`search_route_hnsw_search_pack/search=1\` on healthy packs. Cache warmup/build
  happens before the timed loop; the timed row should report
  \`open_searcher_calls/op=0\`, \`open_setup_in_timed_loop=0\`,
  \`response_owned_result_alloc/op=0\`, and collection prepared-cache metrics
  such as \`collection_prepared_cache_builds/op\`,
  \`collection_prepared_cache_hits/op\`, and
  \`collection_prepared_cache_hit_ratio\`.
- \`BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot\`
  is the explicitly labeled with-documents/materialization row. It uses
  \`IncludeDocuments=true\`, reports \`docs_fetched/search\`, document bytes,
  output bytes, and document materializer sub-counters, and must not be included
  in high-QPS no-document success claims.
- \`.../USearch_Search\` and \`.../USearch_SearchParallel\` time the pure
  in-memory USearch Go binding baseline with cosine/f32 HNSW and the same
  synthetic vector/query generator, M, efConstruction, efSearch, topK, docs,
  dims, and CPU/concurrency list.

Legacy/control rows:

- \`BenchmarkCollectionVectorIndex*\` rows are older TreeDB in-memory/native-root
  vector-index controls. They are useful historical comparators but are not the
  current production no-document fast path.
- \`BenchmarkCollectionVectorSearchExact\` is an exact scan control.
- With-document or unsupported one-shot \`Collection.SearchVectorIndex\`
  benchmarks pay setup/open and/or materialization costs per operation and should
  not be presented as the no-document high-QPS production fast path.

Data boundary: TreeDB stores generated float32 vectors through JSON collection
inserts and rebuilds the persisted \`column_graph\` plus derived
\`hnsw_search_pack_v1\`; raw collection vectors remain authoritative while the
pack is vector-index serving state. USearch is built directly from the generated
float32 vectors as a pure in-memory external ANN baseline.

With \`RUN_WRITE_BENCH=true\`, the script also compares TreeDB incremental
\`InsertBatch\` with a registered in-memory vector index against USearch
incremental \`Add\` on the same synthetic vector stream. The TreeDB benchmark
includes collection document writes and vector-index update notifications; the
USearch benchmark is in-memory index insertion only.
EOF

echo "USearch root: $USEARCH_ROOT"
echo "USearch include dir: $USEARCH_INCLUDE_DIR"
echo "USearch lib dir: $USEARCH_LIB_DIR"
echo "run dir: $RUN_DIR"
(
	if [[ "$RUN_UNSAFE_USEARCH_FILTERED" == "true" ]]; then
		export BENCH_REGEX="BenchmarkCollectionVector(SearchExact|IndexSearch(Int8)?|IndexGraphOnlySearch(Int8)?|IndexFilteredSearch|USearchBaseline|USearchFilteredBaseline|USearchProductionCompare)$"
		export GODEBUG="cgocheck=0${GODEBUG:+,$GODEBUG}"
	fi
	export CGO_CFLAGS="-I$USEARCH_INCLUDE_DIR ${CGO_CFLAGS:-}"
	if [[ "$USEARCH_OS" == "macos" ]]; then
		export CGO_LDFLAGS="-L$USEARCH_LIB_DIR -Wl,-rpath,$USEARCH_LIB_DIR ${CGO_LDFLAGS:-}"
		export DYLD_LIBRARY_PATH="$USEARCH_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
	else
		export CGO_LDFLAGS="-L$USEARCH_LIB_DIR ${CGO_LDFLAGS:-}"
		export LD_LIBRARY_PATH="$USEARCH_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
	fi
	export TREEDB_VECTOR_BENCH_DOCS
	export TREEDB_VECTOR_BENCH_DIMS
	export TREEDB_VECTOR_BENCH_M
	export TREEDB_VECTOR_BENCH_EF_CONSTRUCTION
	export TREEDB_VECTOR_BENCH_EF_SEARCH
	export TREEDB_VECTOR_BENCH_TOPK
	export TREEDB_VECTOR_BENCH_QUERIES
	for cpu in ${CPU_LIST//,/ }; do
		echo "## search benchmarks cpu=$cpu"
		go test -tags usearch_bench ./TreeDB/collections -run '^$' -bench "$BENCH_REGEX" -benchmem -benchtime="$BENCHTIME" -count="$COUNT" -cpu="$cpu"
	done
	if [[ "$RUN_BUILD_BENCH" == "true" ]]; then
		for cpu in ${CPU_LIST//,/ }; do
			echo "## build benchmarks cpu=$cpu"
			go test -tags usearch_bench ./TreeDB/collections -run '^$' -bench "$BUILD_BENCH_REGEX" -benchmem -benchtime="$BENCHTIME" -count="$COUNT" -cpu="$cpu"
		done
	fi
	if [[ "$RUN_WRITE_BENCH" == "true" ]]; then
		for cpu in ${CPU_LIST//,/ }; do
			echo "## write benchmarks cpu=$cpu"
			go test -tags usearch_bench ./TreeDB/collections -run '^$' -bench "$WRITE_BENCH_REGEX" -benchmem -benchtime="$BENCHTIME" -count="$COUNT" -cpu="$cpu"
		done
	fi
) 2>&1 | tee "$RUN_DIR/bench.txt"
