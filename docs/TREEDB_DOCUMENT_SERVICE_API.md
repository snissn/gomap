# TreeDB Document Service API (pre-alpha)

Issue: [#2531](https://github.com/snissn/gomap/issues/2531). Parent tracker:
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
- optional embedding echo in returned documents.

Not supported now:

- keyword search;
- hybrid search;
- text ranking/BM25/BM25F;
- client-side or service-side full-document scan fallbacks for keyword/hybrid.

Keyword and hybrid routes return `unsupported`/HTTP 501. This matches current
TreeDB core behavior: `SearchText` and `SearchHybrid` fail closed until ranked
text and hybrid executors land.

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

`score` is response-only. `embedding` is required on upsert for this dense-search
MVP. Responses omit embeddings unless `return_embedding=true`.

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
    "document_type": "treedb_document_service_v1",
    "capabilities": {
      "dense_vector_search": true,
      "exact_dense_scoring": true,
      "metadata_filters": true,
      "keyword_search": false,
      "hybrid_search": false
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
unsupported filters into broader scans.

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

Tie order is deterministic: higher score first, then document ID ascending.

## Keyword and hybrid fail-closed routes

These endpoints are reserved for downstream clients but are not implemented:

```http
POST /v1/indexes/{index}/search/keyword
POST /v1/indexes/{index}/search/hybrid
```

They return HTTP 501 with `unsupported`. They do **not** scan all documents or
silently downgrade to vector-only/text-only behavior.

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
