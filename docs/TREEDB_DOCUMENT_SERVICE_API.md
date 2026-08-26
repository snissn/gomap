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

- create/open a document index, including declaration-time scalar fields for
  bounded metadata filtering;
- upsert Haystack-style documents;
- delete by ID or metadata filter;
- count/filter/list documents;
- exact dense-vector search with metadata filters;
- dense `route=ann` search through compatible `native_runtime` and
  `column_graph` vector indexes, including declared scalar filters on `native_runtime`;
- ranked keyword search over the declared `content` text index, including
  declared-field metadata filters;
- TreeDB collection-native hybrid search over text and/or vector sources,
  including declared-field metadata filters;
- optional embedding echo in returned documents.

Not supported now:

- client-side or service-side full-document scan fallbacks for keyword/hybrid;
- silent vector-only/text-only downgrade when a requested source/index is
  missing, stale, corrupt, or unavailable;
- unsupported scalar shapes on filtered `route=ann` dense requests.

Dense `route=ann` uses graph traversal and returns exact scores for its bounded
candidate set. Declared scalar filters on `native_runtime` expose plan,
membership, candidate-work, fallback, materialization, and visibility-retry
diagnostics. `route=exact` scans only documents matching a bounded metadata
filter. Neither route silently substitutes a primary-document scan.

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
  "metric": "cosine",
  "scalar_fields": [
    {"field": "repo", "value_type": "string"},
    {"field": "start_line", "value_type": "int64"}
  ]
}
```

`scalar_fields` is declaration-time schema. A field may be written as a bare
metadata path (`repo`) or with the `meta.` prefix (`meta.repo`); the service
normalizes both to `meta.<path>`. `id`, `content`, and `embedding` are reserved
and cannot be declared. Supported `value_type` values are `string`, `bool`,
`int64`, and `double` (omitted `value_type` defaults to `string`).

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
hybrid vector sources and dense `route=ann` can use the collection graph. Non-
cosine indexes keep exact dense scoring available, but hybrid vector capability
is reported false until the collection vector path can serve that metric safely.
Metadata filters on keyword/hybrid routes require fields declared in
`scalar_fields`; undeclared or unbounded filters fail closed.

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
    "contract_version": "treedb-document-service/v1alpha2",
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
      "keyword_metadata_filters": true,
      "hybrid_metadata_filters": true,
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

Large exports use bounded cursor pages rather than `limit=0`. Set
`"cursor_page": true`, a positive `limit`, and optionally `"after_id"` from the
previous response's `next_after_id`. Cursor pages require `offset=0`, return at
most `limit` documents, and include `exhausted`; when `exhausted` is false,
resume strictly after `next_after_id`. A selective filter can produce an empty,
non-exhausted page because physical scan work is bounded as well as response
size. Each page starts a new TreeDB snapshot, so callers requiring one stable
state must prevent concurrent writes.

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

Supported operators for the general document/filter routes are boolean `AND`,
`OR`, `NOT`; comparisons `==`, `!=`, `>`, `>=`, `<`, `<=`; and membership `in`,
`not in`.

Fields may be `id`, `content`, `meta.<path>`, or a metadata path without the
`meta.` prefix. Missing fields do not match any operator, including `!=` and
`not in`, so filters fail closed rather than broadening result sets. Comparison
operators require numeric or string operands. Membership values must be arrays.
For keyword and hybrid routes, only the bounded scalar-filter subset is
representable: an undeclared field returns typed `invalid_request`, while an
unsupported boolean/operator/field shape returns typed `unsupported`. The
service never rewrites these requests into broader scans. If a declared
scalar allow-set exceeds its lookup bound, the route fails closed with typed
`index_unavailable` and `scalar_filter_unbounded`; no partial ranking or result
is returned. The AST is also supported by document count/filter/delete and exact
dense-vector search.


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

For high-QPS clients that already hold float32 query vectors, the same route
also accepts a typed little-endian float32 payload encoded as base64. Supply
exactly one of `query_embedding` or `query_embedding_f32_le_b64`:

```json
{
  "query_embedding_f32_le_b64": "AACAPwAAAAAAAAAA",
  "top_k": 10,
  "ef_search": 64,
  "query_mode": "exact"
}
```

For best-case single-query HTTP request measurements, the binary endpoint
accepts raw little-endian float32 query bytes and returns the same response shape
as `/search/vector-index`:

```http
POST /v1/indexes/{index}/search/vector-index:binary?top_k=10&ef_search=128&query_mode=exact
Content-Type: application/vnd.treedb.vector-search.f32le
```

Only the raw query vector is carried in the binary body. Supported query
parameters are `top_k` (required), `ef_search`, `query_mode` (`exact` only for
this endpoint), `vector_index_name`, `expected_generation`, and `stats_mode`.
Unsupported or invalid query parameters fail closed. The endpoint rejects the
wrong content type, bodies larger than the service body cap, byte lengths that
are not a multiple of four, empty bodies, and dimension mismatches before search.
It is a single-query HTTP lane, not a batch API, and it does not change the
dense `/search/vector` route.

Supported JSON `query_mode` values are `exact`, `quantized_only`, and
`quantized_rerank`. Quantized modes require a declared quantized index name and
fail closed when assets are missing, invalid, stale, or unavailable. Supported
`stats_mode` values are `minimal`/`production`, `full_diagnostics`, and
`work_accounting`; `work_accounting` adds per-query work counters and timers such
as visited nodes/edges, FP32/quantized/exact-rerank score calls, heap pushes/pops,
`distance_kernel_nanos`, `graph_traversal_nanos`, and
`service_response_nanos`, and should be kept out of headline QPS rows. Responses
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
  "route": "exact",
  "top_k": 10,
  "filter": {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
  "return_embedding": false
}
```

Response:

```json
{
  "metric": "cosine",
  "route": "exact",
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

Omit `route` (or use `route=ann`) for default graph traversal when the index
supports it. ANN responses report `route=ann` and `exact=false`; declared scalar
filters use `native_runtime` filtered ANN, while unsupported shapes fail closed
instead of silently switching to a scan.
Tie order is deterministic: higher score first, then document ID ascending.

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
  "filter": {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
  "return_embedding": false
}
```
`operator` is `or` (default) or `and`; explicit `AND`/`OR` in the query string is
also understood by TreeDB text search. `candidate_limit` and
`max_postings_scanned` are optional guardrails for unfiltered keyword search.
When a metadata `filter` is supplied, `max_postings_scanned` is rejected with
typed `unsupported` rather than ignored, because the filtered route currently
cannot propagate that guardrail. Other guardrail or scalar allow-set truncation
fails closed with `index_unavailable`; no incomplete ranking is returned.
`filter` is supported only for fields declared in `scalar_fields` at index
creation. Keyword/hybrid accepts equality and one/two-sided range leaves, either
alone or joined by nested `AND`; same-field bounds are merged and different
fields are intersected through their declared indexes. Undeclared fields return
`invalid_request`. `OR`, `NOT`, `!=`, membership, and other unrepresentable
shapes return `unsupported`.

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
  "max_chunks_per_parent": 2,
  "filter": {
    "operator": "AND",
    "conditions": [
      {"field": "meta.tenant_id", "operator": "==", "value": "acme"},
      {"field": "meta.workspace_id", "operator": "==", "value": "support"},
      {"field": "meta.created_at", "operator": ">=", "value": 1767225600}
    ]
  },
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
`max_chunks_per_parent` is disabled when omitted or zero and must otherwise be
positive. When enabled, the executor walks the already-bounded fused order,
keeps at most that many canonical `<parentID>#<ordinal>` built-in chunk IDs per
parent, and only then fetches the final documents. It does not fetch before
collapse, increase source candidate budgets, or scan for replacement results.
An ID is a chunk only when it parses and round-trips exactly through the
built-in child-ID constructor. IDs that are malformed, have extra separators,
or use non-canonical ordinals such as `parent#01` are independent documents.
Their literal IDs are never used as parent keys, so they cannot alias a valid
chunk parent.

`filter` uses the same bounded keyword/hybrid grammar: equality and one/two-sided
range leaves over declared `scalar_fields`, joined only by `AND`. The service
groups same-field bounds in first-appearance order, resolves every field through
its scalar index, and intersects the complete finite ID sets before text/vector
work. Every lookup uses the same per-lookup bound; at most 16 field groups are
accepted and aggregate retained input is bounded by `lookup_limit * lookup_count`.
An empty intersection succeeds without source work. An undeclared field returns
`invalid_request`; `OR`, `NOT`, `!=`, membership, nested non-AND shapes, and
malformed leaves return `unsupported`. A missing/corrupt index, snapshot change,
or any truncated lookup fails closed with no partial candidates, ranking, final
fetch, or primary-document scan. Truncation returns `index_unavailable` with
`scalar_filter_unbounded`.

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
    "scalar_filter_lookup_count": 3,
    "scalar_filter_lookup_limit": 4096,
    "scalar_filter_aggregate_limit": 12288,
    "fusion_method": "rrf",
    "fusion_tie_policy": "fused_score_best_rank_source_order_id",
    "text_candidate_limit": 100,
    "vector_candidate_limit": 100,
    "max_chunks_per_parent": 2,
    "final_top_k": 10
  },
  "stats": {
    "scalar_filter_lookups": 3,
    "scalar_filter_input_ids": 640,
    "scalar_filter_intersection_steps": 2,
    "scalar_filter_final_ids": 12,
    "text_candidates_requested": 100,
    "text_candidates_returned": 10,
    "vector_candidates_requested": 100,
    "vector_candidates_returned": 10,
    "candidates_fused": 20,
    "fusion_both": 1,
    "collapse_rejections": 4,
    "collapse_exhaustions": 0,
    "documents_fetched": 10
  }
}
```

Collapse preserves fused order, fused scores, text/vector source attribution,
filter scope, and snapshot identity. `collapse_rejections` counts higher-ranked
candidates skipped because their valid parent reached the cap.
`collapse_exhaustions` is `1` when the bounded fused candidates are exhausted
before `top_k` eligible results can be produced, otherwise `0`; exhaustion may
therefore return fewer documents than `top_k`. `truncated` continues to count
fused candidates omitted by the final bound, including cap rejections. With
collapse disabled, both collapse counters are zero and IDs, scores, and source
contributions retain their prior behavior.

Missing/stale/unavailable text or vector indexes, text postings/candidate budget
exhaustion, corrupt index state, and bounded document-fetch failures return a
service error (`index_unavailable`, `index_stale`, or `unsupported`) rather than
empty success or a scan fallback.

Pre-alpha caveat: cosine service indexes attempt to refresh the `column_graph`
vector index after insert-only upserts. Updates/deletes can currently leave the
vector graph rebuild-needed in collection core; hybrid requests that need the
vector source then fail closed until that core mutation/rebuild path is available.
Keyword and hybrid retrieval now use their bounded indexed-filter paths; exact
dense scoring remains the explicit filtered correctness path. Any unavailable
or stale source still fails closed.

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
