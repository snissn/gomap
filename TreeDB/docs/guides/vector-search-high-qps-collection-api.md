# TreeDB high-QPS collection vector search

This guide summarizes the collection-level vector-search API boundary after the
#2483 closeout. It owns the API-selection and caveat guidance from #2406 and now
keeps exact FP32, `scalar_u8`, `rabitq_1bit`, and prototype `brq_1bit` evidence
separate. Benchmark snapshots and full counter workflow are documented by the
[benchmark workflow](vector-search-benchmark-workflow.md) and the
[#2483 closeout index](../spec/vector-search-closeout-2483.md).

## Choosing a vector search API

| User goal | API | Result ownership | Document/materialization boundary | Notes |
| --- | --- | --- | --- | --- |
| Production high-QPS exact no-document serving through the collection API | `Collection.SearchVectorIndexWithBuffer` | caller-owned `VectorIndexSearchBuffer` | `IncludeDocuments=false`; document fetch/projection/filter/fallback controls rejected | Primary exact collection-level fast path. Warm the collection-owned prepared `hnsw_search_pack_v1` state before the timed/serving loop; steady state targets `0 B/op`, `0 allocs/op`. |
| Collection-level buffered quantized serving for supported score planes | `Collection.SearchVectorIndexWithBuffer` with `QueryMode=quantized_only` or `quantized_rerank` and `QuantizedIndexName` | caller-owned `VectorIndexSearchBuffer` | `IncludeDocuments=false`; document materialization remains separate | Separate route state from exact FP32. Current collection evidence covers `scalar_u8` and `rabitq_1bit`; `quantized_only` reads no exact vectors/norms, while `quantized_rerank` exact-reads only the shortlist. |
| Selected mutable typed Minima scalar-u8 rerank | `Collection.SearchVectorIndexWithBufferReadView` with `QueryMode=quantized_rerank` and a named legacy scalar-u8 v1 plane | caller-owned buffer plus a returned captured `CollectionReadView` | fetch only through the returned view; close it after use | Narrow Q2 serving route, not a generic high-QPS no-document claim. It shares one typed owner/resource across base traversal, rerank, suffix merge, IDs, and fetch; qonly/codecs/transport outside its contract remain rejected. |
| Simple no-document call when per-call result allocation is acceptable | `Collection.SearchVectorIndex` with `IncludeDocuments=false` | response-owned results/IDs | no documents materialized | Convenience route. Healthy exact calls use the cached `hnsw_search_pack_v1` route, but returned result/ID storage is response-owned and intentionally allocates. |
| Search and materialize documents in the same call | `Collection.SearchVectorIndex` with `IncludeDocuments=true` | response-owned results/documents | with-document materialization is part of the call | explicit materialization path. Do not mix these rows into no-document high-QPS claims; report document fetch counters separately. |
| Search first, fetch top-k documents later | `CollectionReadView.FetchDocumentsForVectorIndexSearchResults` after a no-document search | response-owned documents | separate fetch/materialization phase | Use when a service can keep ANN search no-document and fetch only selected top-k IDs later. Buffered-search results alias the caller buffer, so do not reuse/reset the buffer until this fetch returns. |
| Reusable low-level serving when the caller owns snapshot/open lifetime | `OpenVectorIndexSearcher` + `SearchWithBuffer` | caller-owned `VectorIndexSearchBuffer` | buffered search rejects document materialization | Open/warm one searcher and one buffer per worker. Use this when explicit searcher lifetime control matters more than the collection-level convenience seam. |

## Production high-QPS serving recipe

For the collection-level serving path:

1. Build or rebuild the declared `column_graph` vector index for the current
   generation, and keep rebuild/setup outside the timed request loop.
2. Use the exact/zero query mode with `IncludeDocuments=false`; leave document
   fetch options, filters, projections, legacy fallback controls, and
   diagnostic stats modes (`benchmark_debug` and `work_accounting`) out of the
   high-QPS request shape.
3. Allocate one `VectorIndexSearchBuffer` per goroutine/worker. A buffer is not
   concurrency-safe, and returned `Results`/ID byte slices alias that buffer
   until it is reused or reset.
4. Warm once before serving or before `ResetTimer` so the collection-owned
   prepared `hnsw_search_pack_v1` state is built outside the measured loop.
5. Fetch documents only after the no-document search, as a separately measured
   materialization phase, or choose an explicitly with-document API row instead.

```go
var buffer collections.VectorIndexSearchBuffer
opts := collections.VectorIndexSearchOptions{
    IndexName: "embedding_graph",
    Query:     warmupQuery,
    TopK:      10,
    EfSearch:  128,
    StatsMode: collections.VectorIndexSearchStatsModeProduction,
}

// Use VectorIndexSearchStatsModeWorkAccounting only for diagnostic attribution
// runs. It adds explicit visited-node/edge, score-call, heap, and timer counters
// and should not be mixed into production-QPS evidence rows.

// Warm the collection-owned prepared search state outside the timed loop.
if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
    return err
}

for query := range queries {
    opts.Query = query
    response, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
    if err != nil {
        return err
    }
    // response.Results aliases buffer; copy IDs/results before reusing buffer
    // if another goroutine or later stage must retain them.
    consumeTopK(response.Results)
}
```

For quantized collection serving, use the same buffered API with an explicit
`QueryMode` and `QuantizedIndexName`. Keep `scalar_u8`, `rabitq_1bit`, and
`brq_1bit` evidence separate: #2487 covers collection-level `scalar_u8` and
`rabitq_1bit`; #2507 adds lower-level prototype `brq_1bit` rows only.

The Q2 mutable Minima path is deliberately different from those generic
buffered evidence rows. It is only the selected typed read-view legacy
`scalar_u8` v1 rerank route; it has no `quantized_only`, calibrated-scalar,
RaBitQ/BRQ, HTTP/native, Hybrid, or standalone-searcher extension. Keep its
captured view open through any final fetch. The immutable base's shared holder
owns one mapped code resource and request readers borrow a non-owning status, so
the route avoids a full code-plane reopen/copy per request. An empty base still
validates its selected zero-row image before empty/suffix-only success and holds
no fabricated resource/scorer; asset failures fail closed.

Its bounded work is a correctness contract, not a high-QPS throughput claim:
with base domain `A`, conservative shadow allowance `S`, effective width `E0`,
rerank cap `Rcap`, raw width `C=min(A,E0+S)`, eligible suffix count `D`, and
positive `B=SearchCandidates`, a nonempty ANN request requires
`B-D-Rcap > C`. It removes shadows after raw collection, never refills, and
uses the typed exact route for complete eligible filters of at most 4,096 rows.
Final rerank uses stable FP32 cosine distance with FP64 inverse norms. Its
versioned score-plane proof is internal in Q2; Q3 owns any transport/wire form.
See the [typed-column guide](vector-search-typed-column.md#selected-mutable-scalar-u8-rerank)
and [quantized score-plane contract](../spec/quantized-vector-index.md#selected-typed-read-view-legacy-scalar-u8-rerank-4685).

For lower-level serving, open `OpenVectorIndexSearcher` once per worker, warm
`SearchWithBuffer` with that worker's own buffer, and close/reopen the searcher
when the worker must move to a newer collection/vector-index generation.

## Runnable exact buffered demo

For an instructional smoke that builds a collection, checkpoints and reopens it,
warms the prepared state, reuses a caller-owned buffer, times no-document
searches, and prints top-k IDs/scores plus route guardrails, run:

```sh
GOWORK=off go run ./cmd/treedb_vector_highqps_demo \
  -docs 1000 \
  -dims 64 \
  -queries 1000 \
  -warmup-queries 16 \
  -top-k 10
```

The demo is exact-only and intentionally excludes document materialization and
quantized modes. It is not a benchmark replacement; use the
[benchmark workflow](vector-search-benchmark-workflow.md) for performance
evidence.

## Do not overclaim

- Do not turn dated fixture rows into general parity claims. The #2487 exact
  snapshot reports TreeDB and USearch rows on an Apple M3 (`darwin/arm64`) at
  commit `32e143240dbffb24172e0ec91c5565ea7c84328a` under moderate host load;
  use it as current-main route evidence, not a universal ranking.
- With-document search is a different materialization path. It includes final
  document fetch/reconstruction work and must not be mixed into no-document
  high-QPS claims.
- Filters, projections, debug-only stats, and quantized modes are outside the
  exact FP32 `hnsw_search_pack_v1` no-document success claim. Quantized modes
  must use their own route rows, fail-closed counters, exact-read bounds, recall,
  code/asset bytes, and allocation evidence.
- Quantized collection buffered search is supported as a separate route state for
  accepted `scalar_u8` and `rabitq_1bit` rows. The `brq_1bit` #2507 work is a
  lower-level prototype with its own benchmark evidence and no promotion claim.
  Future scale-sensitive positioning should consume #2494 crossover synthesis or
  say explicitly that crossover evidence is pending.
- The Q2 typed read-view rerank route is not one of the `0 B/op`, `0 allocs/op`,
  no-document high-QPS evidence rows. It pins a view, scores a live suffix, and
  performs bounded exact rerank work by design; measure it under its own
  correctness and allocation evidence instead of borrowing exact/collection
  benchmark claims.

## Required no-document route counters

Issue #2410 owns the historical benchmark workflow, while
[`vector-search-closeout-2483.md`](../spec/vector-search-closeout-2483.md)
indexes the current accepted evidence. As a contract summary, healthy exact
no-document rows must prove all of the following before claiming the exact
high-QPS path:

- `search_route_hnsw_search_pack/search=1`
- `hnsw_search_pack_active/search=1`
- `docs_fetched/search=0`
- `graph_row_fallbacks/search=0`
- `typed_column_vector_fallbacks/search=0`
- `vector_scratch_decodes/search=0`

Collection-level one-shot/buffer rows must additionally prove there is no
per-query open/setup in the timed loop:

- `open_searcher_calls/op=0`
- `open_setup_in_timed_loop=0`

Reusable-searcher `SearchWithBuffer` rows open the searcher before `ResetTimer`
and may not emit those collection-level open/setup counters; their open/setup
boundary is proven by the benchmark shape plus the route/fallback counters above.

`Collection.SearchVectorIndexWithBuffer` must also report `0 B/op` and
`0 allocs/op`. `Collection.SearchVectorIndex` no-document convenience rows
should report `response_owned_result_alloc/op=1` and account for the small
response-owned allocation separately.

Quantized no-document rows must additionally prove their own route state:

- `search_route_quantized_only/search=1` or
  `search_route_quantized_rerank/search=1`
- `quantized_scorer_active/search=1`
- `quantized_asset_unavailable/search=0`
- `docs_fetched/search=0`
- qonly rows: `vector_B/search=0`, `norm_B/search=0`, and
  `quantized_rerank_exact_score_calls/search=0`
- rerank rows: `quantized_rerank_candidates/search` and
  `quantized_rerank_exact_score_calls/search` bounded to the configured
  shortlist
- codec/storage evidence such as `quantized_code_B/vector`,
  `quantized_asset_B/vector`, recall@K, `B/op`, and `allocs/op`

## Benchmark command

Use the production comparison benchmark for final c=1/c=8 evidence:

```sh
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$' \
  scripts/bench_vector_search_compare.sh
```

Canonical rows:

- `TreeDB_CollectionSearchVectorIndexWithBuffer`: collection-level
  caller-owned-buffer no-document target.
- `TreeDB_CollectionSearchVectorIndexNoDocsOneShot`: response-owned
  no-document convenience route.
- `TreeDB_SearchWithBuffer` and `TreeDB_SearchWithBufferParallel`: reusable
  searcher/buffer route.
- `TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot`: explicit
  with-documents/materialization row.
- `USearch_Search` and `USearch_SearchParallel`: pure in-memory external ANN
  baseline.

### Tier F no-document scaling command

For 100k/128-class Tier F evidence, keep the same exact FP32 fixture and route
contract but focus the benchmark regex on the no-document TreeDB rows and the
USearch baseline. The with-documents/materialization row is intentionally
excluded here because it is not part of the high-QPS no-document contract and can
be measured separately as a document-fetch row.

```sh
TREEDB_VECTOR_BENCH_DOCS=100000 TREEDB_VECTOR_BENCH_DIMS=128 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare/(TreeDB_SearchWithBuffer|TreeDB_SearchWithBufferParallel|TreeDB_CollectionSearchVectorIndexWithBuffer|TreeDB_CollectionSearchVectorIndexNoDocsOneShot|USearch_Search|USearch_SearchParallel)$' \
  scripts/bench_vector_search_compare.sh
```

Report the same guardrail counters as the Tier S run, especially
`search_route_hnsw_search_pack/search=1`, `hnsw_search_pack_active/search=1`,
zero document/fallback/scratch counters, collection `open_*` counters at 0, and
allocation accounting for the caller-owned-buffer rows.

## Profile capture notes

For focused CPU profiles of the response-owned no-document convenience row,
bootstrap USearch with `scripts/bench_vector_search_compare.sh` and reuse the
host-specific include/library directories recorded in that run's README:

```sh
RUN_DIR=/tmp/gomap_vector_search_compare_profile_bootstrap
export RUN_DIR
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1 BENCHTIME=1x COUNT=1 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$' \
  scripts/bench_vector_search_compare.sh

USEARCH_INCLUDE_DIR=$(awk -F'`' '/USearch include dir:/ { print $2 }' "$RUN_DIR/README.md")
USEARCH_LIB_DIR=$(awk -F'`' '/USearch lib dir:/ { print $2 }' "$RUN_DIR/README.md")
export CGO_CFLAGS="-I$USEARCH_INCLUDE_DIR"
export CGO_LDFLAGS="-L$USEARCH_LIB_DIR -Wl,-rpath,$USEARCH_LIB_DIR -lusearch_c"
case "$(uname -s)" in
  Linux) export LD_LIBRARY_PATH="$USEARCH_LIB_DIR:${LD_LIBRARY_PATH:-}" ;;
  Darwin) export DYLD_LIBRARY_PATH="$USEARCH_LIB_DIR:${DYLD_LIBRARY_PATH:-}" ;;
esac

TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 \
  go test -tags usearch_bench ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexNoDocsOneShot$' \
  -benchmem -benchtime=100000x -count=1 -cpu=1 \
  -cpuprofile /tmp/treedb_collection_searchvector_c1.pprof
```

Repeat with `-cpu=8` for the c=8 profile. Go benchmark CPU profiles include
fixture setup/rebuild work before the timed loop; use the benchmark route
counters above to distinguish setup from steady-state query costs. In steady
state, expected dominant query costs are HNSW pack traversal, dot/scoring,
frontier/top-k maintenance, and final result-ID copy for response-owned
convenience calls. Dominant document/JSON materialization, graph-row fallback,
typed-column vector fallback, per-query open/prepare, or allocation/GC costs are
not acceptable in no-document fast rows.
