# TreeDB Document Service API (pre-alpha)

Issues: [#2531](https://github.com/snissn/gomap/issues/2531),
[#2534](https://github.com/snissn/gomap/issues/2534). Parent tracker:
[#2530](https://github.com/snissn/gomap/issues/2530).

This is the smallest TreeDB document/search service contract intended for the
Python client and the first Haystack integration. TreeDB is **pre-alpha**:
request/response fields and on-disk collection layouts may change before a
stable release.

## Running the service

```sh
go run ./cmd/treedb-document-service \
  -dir /tmp/treedb-document-service \
  -addr 127.0.0.1:7120 \
  -profile command_wal_durable
```

The command opens TreeDB with the selected public profile and serves the routes
below. `GET /v1/health` returns the current `contract_version`.

A Haystack-free sync Python client for this contract lives in
[`clients/python/treedb_client`](../clients/python/treedb_client/README.md).
Run its stdlib unit tests with:

```sh
cd clients/python/treedb_client
PYTHONPATH=src python3 -m unittest discover -s tests
```

## Scope and honesty

Supported now:

- create/open a document index;
- upsert Haystack-style documents;
- delete by ID or metadata filter;
- count/filter/list documents;
- exact dense-vector search with metadata filters;
- ranked keyword search over the declared `content` text index;
- TreeDB collection-native hybrid search over text and/or vector sources;
- optional embedding echo in returned documents.

Not supported now:

- metadata filters on keyword/hybrid routes (they fail closed with
  `unsupported`/HTTP 501);
- client-side or service-side full-document scan fallbacks for keyword/hybrid;
- silent vector-only/text-only downgrade when a requested source/index is
  missing, stale, corrupt, or unavailable.

Dense-vector search in this service is an **exact scoring correctness/MVP path**.
It scans service documents that match the metadata filter and scores their stored
embeddings. It is not the high-QPS `column_graph` ANN route documented in the
TreeDB vector-search guides, and this contract makes no ANN throughput claim.

## Haystack document mapping

Service documents use Haystack-compatible fields:

```json
{
  "id": "doc-1",
  "content": "body text",
  "embedding": [0.1, 0.2, 0.3],
  "meta": {
    "repo": "snissn/gomap",
    "path": "TreeDB/documentservice/service.go",
    "language": "go",
    "symbol": "SearchDenseVector",
    "start_line": 120,
    "end_line": 180,
    "chunk_kind": "function"
  },
  "score": 0.99
}
```

`score` is response-only. `embedding` is required on upsert for dense/vector
retrieval. Responses omit embeddings unless `return_embedding=true`.

Keyword and hybrid result documents add response-only explanation metadata under
`meta._treedb_search`. For keyword results it includes the text index, rank,
score kind, matched fields/terms, and text matches. For hybrid results it
includes fused rank/score, fusion method/tie policy, and per-source text/vector
contributions.

TreeDB stores each service index as a JSON collection. The document ID is the
TreeDB collection key and the JSON `id` field is kept for client compatibility.

## Indexes

Create an index:

```http
POST /v1/indexes
```

Request:

```json
{
  "name": "docs",
  "dimension": 768,
  "metric": "cosine"
}
```

`metric` defaults to `cosine`. Supported metrics are `cosine`, `l2`, and
`inner_product`. Scores are always higher-is-better:

- cosine: cosine similarity;
- l2: negative squared L2 distance;
- inner_product: dot product.

Service-created indexes declare these stable names:

- vector field/index: `embedding`;
- text field/index: `content`;
- document type: `treedb_document_service_v1`.

Cosine indexes are created with the collection `column_graph` vector strategy so
hybrid vector sources can use `Collection.SearchHybrid`. Non-cosine indexes keep
exact dense scoring available, but hybrid vector capability is reported false
until the collection vector path can serve that metric safely.

Open/read index metadata:

```http
GET /v1/indexes/{index}
```

Responses include:

```json
{
  "index": {
    "name": "docs",
    "dimension": 768,
    "metric": "cosine",
    "generation": 1,
    "contract_version": "treedb-document-service/v1alpha1",
    "embedding_field": "embedding",
    "vector_index_name": "embedding",
    "vector_strategy": "column_graph",
    "vector_m": 16,
    "vector_ef_construction": 128,
    "vector_ef_search": 64,
    "quantized_indexes": [
      {"name": "embedding.scalar_u8.fast", "codec": "scalar_u8", "version": 1}
    ],
    "text_field": "content",
    "text_index_name": "content",
    "document_type": "treedb_document_service_v1",
    "capabilities": {
      "dense_vector_search": true,
      "exact_dense_scoring": true,
      "metadata_filters": true,
      "keyword_search": true,
      "hybrid_search": true,
      "keyword_metadata_filters": false,
      "hybrid_metadata_filters": false,
      "benchmark_lifecycle": true,
      "vector_index_maintenance": true,
      "no_document_vector_search": true,
      "column_graph_vector_search": true,
      "exact_column_graph_search": true,
      "quantized_vector_search": true,
      "quantized_rerank": true,
      "scalar_u8_quantized_rerank": true,
      "rabitq_1bit_experimental": false
    }
  }
}
```

Operations may include `expected_generation`. If it does not match the current
index generation, the service returns `index_stale` rather than running against a
caller-stale contract.

## Write documents

```http
POST /v1/indexes/{index}/documents/upsert
```

```json
{
  "expected_generation": 1,
  "documents": [
    {
      "id": "doc-1",
      "content": "body text",
      "embedding": [0.1, 0.2, 0.3],
      "meta": {"repo": "snissn/gomap", "language": "go"}
    }
  ]
}
```

Duplicate IDs in one request, missing embeddings, non-finite vector values, and
dimension mismatches fail with `invalid_request`.

## Delete documents

Delete by ID:

```http
POST /v1/indexes/{index}/documents/delete
```

```json
{"ids": ["doc-1", "doc-2"]}
```

Delete by metadata filter:

```json
{"filter": {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"}}
```

Supplying both `ids` and `filter` is rejected as ambiguous.

## Count and filter/list documents

Count:

```http
POST /v1/indexes/{index}/documents/count
```

```json
{"filter": {"field": "meta.language", "operator": "==", "value": "go"}}
```

Filter/list:

```http
POST /v1/indexes/{index}/documents/filter
```

```json
{
  "filter": {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
  "limit": 100,
  "offset": 0,
  "return_embedding": false
}
```

`limit=0` means no service-side result cap. Results are returned in stable
TreeDB document-ID order.

## Metadata filters

Filter nodes use this shape:

```json
{"field": "meta.language", "operator": "==", "value": "go"}
```

Boolean nodes use `conditions`:

```json
{
  "operator": "AND",
  "conditions": [
    {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
    {"field": "meta.start_line", "operator": ">=", "value": 100}
  ]
}
```

Supported operators:

- boolean: `AND`, `OR`, `NOT`;
- comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`;
- membership: `in`, `not in`.

Fields may be `id`, `content`, `meta.<path>`, or a metadata path without the
`meta.` prefix. Missing fields do not match any operator, including `!=` and
`not in`, so filters fail closed rather than broadening result sets. Comparison
operators require numeric or string operands. Membership values must be arrays.
Unsupported operators fail with `invalid_request`; the service does not rewrite
unsupported filters into broader scans. This filter AST is currently supported by
document count/filter/delete and exact dense-vector search. Keyword and hybrid
requests validate the shape and then fail closed with `unsupported` when any
filter is supplied.


## Benchmark lifecycle and no-document vector-index search

These routes exist for external benchmark adapters such as VectorDBBench. They
are deliberately separate from Haystack's exact dense-vector route: they time the
TreeDB service/vector-index boundary and never broaden unsupported ANN or
quantized modes into exact document scans. VDBBench rows that use these routes
include Python/client/service overhead and are not native Go `B/op` or
`allocs/op` evidence.

Create a benchmark-shaped index by passing `vector_index_options` to
`POST /v1/indexes` or to the reset route when the index is missing:

```json
{
  "name": "bench",
  "dimension": 1536,
  "metric": "cosine",
  "vector_index_options": {
    "strategy": "column_graph",
    "m": 16,
    "ef_construction": 128,
    "ef_search": 64,
    "quantized_indexes": [
      {"name": "embedding.scalar_u8.fast", "codec": "scalar_u8", "version": 1}
    ]
  }
}
```

`codec=scalar_u8` plus `query_mode=quantized_rerank` is the exact-like
quantized benchmark lane; rerank32 is the baseline evidence target when
`quantized_rerank_candidates` is set to `32`. `rabitq_1bit` may be declared
for experimental compact rows, but the v1 codec semantics and recall caveats
are unchanged.

Reset/create for benchmark harnesses:

```http
POST /v1/indexes/{index}/reset
```

```json
{
  "dimension": 1536,
  "metric": "cosine",
  "drop_old": true,
  "vector_index_options": {"strategy": "column_graph"}
}
```

If `{index}` is missing, the route creates it. If `{index}` already exists and
`drop_old=false`, it returns `conflict`. Existing `column_graph` benchmark
indexes fail closed with `unsupported` for in-place `drop_old` reset; managed
benchmark runs should use a fresh data directory, and shared external services
should use a unique index name per run. This avoids adding a broad durable
collection-truncate/WAL format just for benchmark reset and preserves TreeDB's
insert-only graph rebuild boundary. Compatible non-`column_graph` indexes may be
cleared with existing document deletes.

Bulk loaders may pass `"defer_vector_index_rebuild": true` on document upserts
to avoid rebuilding column-graph vector assets after every inserted batch.
When rebuild is deferred, `/search/vector-index` fails closed until optimize has
built the assets for the loaded documents. The exact document route
`/search/vector` remains readable from stored documents before optimize.

Optimize/rebuild after load:

```http
POST /v1/indexes/{index}/optimize
```

```json
{"vector_index_name": "embedding", "expected_generation": 1}
```

No-document vector-index benchmark search:

```http
POST /v1/indexes/{index}/search/vector-index
```

```json
{
  "query_embedding": [0.1, 0.2, 0.3],
  "top_k": 10,
  "ef_search": 64,
  "query_mode": "quantized_rerank",
  "quantized_index_name": "embedding.scalar_u8.fast",
  "quantized_rerank_candidates": 32
}
```

Supported `query_mode` values are `exact`, `quantized_only`, and
`quantized_rerank`. Quantized modes require a declared quantized index name and
fail closed when assets are missing, invalid, stale, or unavailable. Responses
include result IDs/scores plus TreeDB stats/diagnostics so benchmark adapters can
assert no-document guardrails: no documents fetched, no exact fallback for
quantized modes, quantized scorer active, and rerank exact reads bounded by the
requested shortlist. Reproducible VectorDBBench setup commands and smoke evidence
are documented in
[`docs/benchmarks/treedb_vectordbbench_runbook_2026-06-11.md`](benchmarks/treedb_vectordbbench_runbook_2026-06-11.md).

## Dense-vector search

```http
POST /v1/indexes/{index}/search/vector
```

```json
{
  "query_embedding": [0.1, 0.2, 0.3],
  "top_k": 10,
  "filter": {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
  "return_embedding": false
}
```

Response:

```json
{
  "metric": "cosine",
  "exact": true,
  "candidates": 42,
  "documents": [
    {
      "id": "doc-1",
      "content": "body text",
      "meta": {"repo": "snissn/gomap"},
      "score": 0.991
    }
  ]
}
```

Tie order is deterministic: higher score first, then document ID ascending. This route remains exact dense document scoring for TreeDB's Python and Haystack clients; it is not the `column_graph` ANN/no-document benchmark route.

## Keyword search

```http
POST /v1/indexes/{index}/search/keyword
```

Request:

```json
{
  "query": "refund policy",
  "top_k": 10,
  "operator": "or",
  "candidate_limit": 1000,
  "max_postings_scanned": 100000,
  "return_embedding": false
}
```

`operator` is `or` (default) or `and`; explicit `AND`/`OR` in the query string is
also understood by TreeDB text search. `candidate_limit` and
`max_postings_scanned` are optional guardrails. If a guardrail is exceeded, the
route fails closed with `index_unavailable` rather than returning incomplete
rankings. Metadata `filter` is not supported here and returns `unsupported`.

Response (abridged; the actual payload also includes the top-level `index`
object shown in the index metadata section):

```json
{
  "text_index": "content",
  "documents": [
    {
      "id": "doc-1",
      "content": "refund policy text",
      "meta": {
        "repo": "snissn/gomap",
        "_treedb_search": {
          "type": "keyword",
          "text_index": "content",
          "rank": 1,
          "score_kind": "bm25f",
          "matched_terms": ["refund", "policy"],
          "matched_fields": ["content"],
          "text_matches": [{"field": "content", "terms": ["refund", "policy"]}]
        }
      },
      "score": 3.12
    }
  ],
  "stats": {
    "query_terms": 2,
    "candidates_requested": 1000,
    "candidates_returned": 1,
    "postings_scanned": 8,
    "candidates_scored": 1,
    "documents_fetched": 1
  }
}
```

Tie order is deterministic: higher BM25F score first, then document ID
ascending.

## Hybrid search

```http
POST /v1/indexes/{index}/search/hybrid
```

Request:

```json
{
  "query": "refund policy",
  "query_embedding": [0.1, 0.2, 0.3],
  "top_k": 10,
  "candidate_limit": 100,
  "text_candidate_limit": 100,
  "vector_candidate_limit": 100,
  "ef_search": 64,
  "fusion": {
    "method": "rrf",
    "rrf_k": 60,
    "tie_policy": "fused_score_best_rank_source_order_id",
    "source_order": ["text", "vector"]
  },
  "return_embedding": false
}
```

Hybrid requests require `capabilities.hybrid_search=true` in the index metadata.
At least one of `query` or `query_embedding` is required. Supplying only `query`
runs TreeDB text-only hybrid execution; supplying only `query_embedding` runs the
collection vector source; supplying both uses deterministic reciprocal-rank
fusion. `candidate_limit` is a shared default for omitted source-specific limits.
Metadata `filter` is not supported here and returns `unsupported`.

Response (abridged; the actual payload also includes the top-level `index`
object shown in the index metadata section):

```json
{
  "text_index": "content",
  "vector_index": "embedding",
  "documents": [
    {
      "id": "doc-1",
      "content": "refund policy text",
      "meta": {
        "repo": "snissn/gomap",
        "_treedb_search": {
          "type": "hybrid",
          "rank": 1,
          "fusion_method": "rrf",
          "fusion_tie_policy": "fused_score_best_rank_source_order_id",
          "fused_score": 0.0325,
          "sources": [
            {"source": "text", "index_name": "content", "source_rank": 1, "score": 3.12, "score_kind": "bm25f", "fusion_score": 0.0164},
            {"source": "vector", "index_name": "embedding", "source_rank": 2, "score": 0.991, "score_kind": "vector_similarity", "fusion_score": 0.0161}
          ]
        }
      },
      "score": 0.0325
    }
  ],
  "plan": {
    "scalar_filter_strategy": "union_fusion",
    "fusion_method": "rrf",
    "fusion_tie_policy": "fused_score_best_rank_source_order_id",
    "text_candidate_limit": 100,
    "vector_candidate_limit": 100,
    "final_top_k": 10
  },
  "stats": {
    "text_candidates_requested": 100,
    "text_candidates_returned": 10,
    "vector_candidates_requested": 100,
    "vector_candidates_returned": 10,
    "candidates_fused": 20,
    "fusion_both": 1,
    "documents_fetched": 10
  }
}
```

Missing/stale/unavailable text or vector indexes, text postings/candidate budget
exhaustion, corrupt index state, and bounded document-fetch failures return a
service error (`index_unavailable`, `index_stale`, or `unsupported`) rather than
empty success or a scan fallback.

Pre-alpha caveat: cosine service indexes attempt to refresh the `column_graph`
vector index after insert-only upserts. Updates/deletes can currently leave the
vector graph rebuild-needed in collection core; hybrid requests that need the
vector source then fail closed until that core mutation/rebuild path is available.
Keyword search and exact dense scoring continue to use their safe paths.

## Error envelope

Errors use a stable code/message envelope:

```json
{"error": {"code": "invalid_request", "message": "top_k must be positive"}}
```

Codes used by this contract:

- `invalid_request`
- `malformed_json`
- `index_not_found`
- `index_unavailable`
- `index_stale`
- `conflict`
- `unsupported`
- `internal`
