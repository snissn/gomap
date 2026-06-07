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
- typed dataclasses for documents, index metadata, responses, and errors
- Haystack-style filter conversion/validation without a Haystack dependency

Not supported:

- keyword search
- hybrid search
- async client APIs
- client-side scans to emulate unsupported TreeDB behavior

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
Unsupported operators and embedding-field filters raise `InvalidFilterError`.
The client does not broaden unsupported filters into local document scans.

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
