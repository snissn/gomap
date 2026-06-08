# TreeDB Python client (pre-alpha)

This package is a small synchronous Python client for TreeDB's pre-alpha
HTTP/JSON document service. It is intended to be the shared base used by a later
`treedb-haystack` package, but **does not import or require Haystack**.

Service contract: [`docs/TREEDB_DOCUMENT_SERVICE_API.md`](../../../docs/TREEDB_DOCUMENT_SERVICE_API.md)

## Scope

Supported by the base client:

- `TreeDBClient(base_url, timeout=...)`
- create/open/ensure index metadata
- upsert Haystack-shaped documents (`id`, `content`, `embedding`, `meta`)
- delete by explicit IDs
- delete by server-side metadata filter
- count/filter/list documents
- exact dense-vector search with optional metadata filters and embedding echo
- ranked keyword search over the service `content` text index
- TreeDB-native hybrid text/vector search with deterministic RRF fusion options
- typed dataclasses for documents, index metadata, keyword/hybrid requests and responses, stats, plan/snapshot, fusion options, and errors
- Haystack-style filter conversion/validation without a Haystack dependency

Not supported:

- metadata filters on keyword/hybrid routes (the service fails them closed with `unsupported`/HTTP 501)
- async client APIs
- client-side scans or text/vector fallbacks to emulate unsupported TreeDB behavior

TreeDB and this client are pre-alpha; APIs may change with the service contract.

## Install for local development

From the repository root, use a virtual environment for editable installs
(system Python installations may reject direct writes under PEP 668):

```sh
cd clients/python/treedb_client
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

The client has no runtime dependencies outside the Python standard library.

## Run tests

Unit tests use `unittest` and the standard library:

```sh
cd clients/python/treedb_client
PYTHONPATH=src python3 -m unittest discover -s tests
```

Optional integration tests start `go run ./cmd/treedb-document-service`, write to
a temporary TreeDB directory, and restart the service for a persistence/reopen
smoke:

```sh
cd clients/python/treedb_client
TREEDB_CLIENT_RUN_INTEGRATION=1 PYTHONPATH=src python3 -m unittest discover -s tests
```

You can also sanity-check the Go service contract from the repository root:

```sh
go test ./TreeDB/documentservice ./cmd/treedb-document-service
```

## Start the TreeDB document service

From the repository root:

```sh
go run ./cmd/treedb-document-service \
  -dir /tmp/treedb-document-service \
  -addr 127.0.0.1:7120 \
  -profile command_wal_durable
```

## Example

```python
from treedb_client import Document, Filter, TreeDBClient

client = TreeDBClient("http://127.0.0.1:7120", timeout=5)

client.ensure_index("docs", dimension=3, metric="cosine")
client.upsert_documents(
    "docs",
    [
        Document(
            id="TreeDB/documentservice/service.go:SearchDenseVector",
            content="SearchDenseVector runs exact dense scoring with filters.",
            embedding=[0.1, 0.2, 0.3],
            meta={
                "repo": "snissn/gomap",
                "path": "TreeDB/documentservice/service.go",
                "language": "go",
                "symbol": "SearchDenseVector",
                "start_line": 250,
                "end_line": 310,
                "chunk_kind": "function",
            },
        )
    ],
)

count = client.count_documents(
    "docs",
    {"field": "meta.language", "operator": "==", "value": "go"},
)
print(count.count)

results = client.query_by_embedding(
    "docs",
    query_embedding=[0.1, 0.2, 0.3],
    top_k=5,
    filter=Filter(
        operator="AND",
        conditions=[
            Filter(field="meta.repo", operator="==", value="snissn/gomap"),
            Filter(field="meta.start_line", operator=">=", value=1),
        ],
    ),
    return_embedding=False,
)

for doc in results.documents:
    print(doc.id, doc.score, doc.meta.get("path"))

keyword = client.search_keyword(
    "docs",
    query="dense scoring filters",
    top_k=5,
    operator="or",
    candidate_limit=1000,
    max_postings_scanned=100000,
)

hybrid = client.search_hybrid(
    "docs",
    query="dense scoring filters",
    query_embedding=[0.1, 0.2, 0.3],
    top_k=5,
    text_candidate_limit=100,
    vector_candidate_limit=100,
    ef_search=64,
    fusion={
        "method": "rrf",
        "rrf_k": 60,
        "tie_policy": "fused_score_best_rank_source_order_id",
        "source_order": ["text", "vector"],
    },
)

for doc in hybrid.documents:
    print(doc.id, doc.score, doc.meta.get("_treedb_search"))
```

## Filters

The client accepts the service/Haystack-style filter AST:

```python
{"field": "meta.language", "operator": "==", "value": "go"}
```

Boolean filters use `conditions`:

```python
{
    "operator": "AND",
    "conditions": [
        {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
        {"field": "meta.start_line", "operator": ">=", "value": 100},
    ],
}
```

Supported operators are `AND`, `OR`, `NOT`, `==`, `!=`, `>`, `>=`, `<`, `<=`,
`in`, and `not in`. The client also accepts common Haystack-style aliases such
as `$eq`, `$gte`, `$in`, and `$nin`, then sends the documented service operator.
Unsupported operators and the top-level `embedding` field filter raise
`InvalidFilterError`; embedding-named metadata paths such as
`meta.embedding.provider` are allowed. The client does not broaden unsupported
filters into local document scans.

This filter AST is supported by document count/filter/delete and exact dense
vector search. Keyword and hybrid methods will serialize a valid filter if you
provide one, but the current service contract rejects keyword/hybrid filters with
`UnsupportedError` (`unsupported`/HTTP 501) after validating the filter shape.

## Error mapping

Service error envelopes are mapped to typed exceptions:

| Service code | Exception |
| --- | --- |
| `invalid_request` | `InvalidRequestError` |
| `malformed_json` | `MalformedJSONError` |
| `index_not_found` | `IndexNotFoundError` |
| `index_unavailable` | `IndexUnavailableError` |
| `index_stale` | `IndexStaleError` |
| `conflict` | `ConflictError` |
| `unsupported` | `UnsupportedError` |
| `internal` | `InternalServiceError` |

Network failures raise `TreeDBTransportError`, timeouts raise
`TreeDBTimeoutError`, and malformed service responses raise
`TreeDBProtocolError`.
