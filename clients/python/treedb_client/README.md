# TreeDB Python client (pre-alpha)

This package is a small synchronous Python client for TreeDB's pre-alpha
HTTP/JSON document service. It is intended to be the shared base used by a later
`treedb-haystack` package, but **does not import or require Haystack**.

Service contract: [`docs/TREEDB_DOCUMENT_SERVICE_API.md`](../../../docs/TREEDB_DOCUMENT_SERVICE_API.md)

## Scope

- `TreeDBClient(base_url, timeout=...)`
- create/open/ensure index metadata, including declaration-time `scalar_fields`
- upsert Haystack-shaped documents (`id`, `content`, `embedding`, `meta`)
- delete by explicit IDs
- delete by server-side metadata filter
- count/filter/list documents
- exact dense-vector search with optional metadata filters and embedding echo
- `ann` dense-vector search through compatible `native_runtime` and
  `column_graph` indexes; declared scalar filters use native filtered ANN
- benchmark lifecycle helpers for reset/create, vector-index optimize/rebuild, and fail-closed no-document vector-index search
- explicit scalar_u8 + rerank benchmark request fields (`query_mode="quantized_rerank"`, quantized index name, rerank candidate count)
- ranked keyword search over the service `content` text index, including declared-field metadata filters
- TreeDB-native hybrid text/vector search with declared-field metadata filters and deterministic RRF fusion options
- typed dataclasses for documents, index metadata, benchmark vector options/responses, keyword/hybrid requests and responses, stats, plan/snapshot, fusion options, and errors
- Haystack-style filter conversion/validation without a Haystack dependency

Not supported:

- using benchmark no-document vector-index routes as Haystack/exact dense-search evidence
- in-place `drop_old` reset for existing `column_graph` benchmark indexes; use a fresh data directory or unique index name
- unsupported filtered `route="ann"` shapes (the service fails closed rather than using exact or primary-scan fallback)
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
Benchmark `/search/vector-index` calls reuse one HTTP(S) connection per client;
call `client.close()` when the instance is no longer needed. Ordinary client
and Haystack operations retain independent urllib requests. Only
`/search/vector-index` retries once after a broken connection; writes are never
replayed automatically. If the normalized TreeDB host uses an environment
proxy (and is not bypassed by `NO_PROXY`), vector-index calls retain urllib's
proxy/TLS/auth behavior instead of using the direct reusable connection.

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
from treedb_client import BenchmarkVectorIndexOptions, Document, Filter, QuantizedIndexInfo, TreeDBClient

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

# VectorDBBench-style no-document vector-index route. This is separate from
# query_by_embedding above; it fails closed rather than falling back to exact
# document scans. Managed benchmark runs should use a fresh TreeDB data dir.
# Omit metric when preserving an existing index's metric; pass it when creating
# or enforcing a benchmark schema.
client.reset_index(
    "bench_run_001",
    dimension=3,
    metric="cosine",
    drop_old=True,
    vector_index_options=BenchmarkVectorIndexOptions(
        strategy="column_graph",
        quantized_indexes=[QuantizedIndexInfo(name="embedding.scalar_u8.fast")],
    ),
)
client.upsert_documents(
    "bench_run_001",
    [Document(id="a", embedding=[0.1, 0.2, 0.3])],
    defer_vector_index_rebuild=True,
)
client.optimize_index("bench_run_001")
bench = client.search_vector_index(
    "bench_run_001",
    query_embedding=[0.1, 0.2, 0.3],
    top_k=1,
    # Optional high-QPS JSON-compatible encoding: "f32_le_b64" sends the query
    # as base64 little-endian float32 bytes instead of a JSON float array.
    query_embedding_encoding="f32_le_b64",
    query_mode="quantized_rerank",
    quantized_index_name="embedding.scalar_u8.fast",
    quantized_rerank_candidates=32,
)
print(bench.results[0].id, bench.no_documents, bench.diagnostics.get("route"))

# Timed runs may request only ordered IDs after a separate full-response
# preflight has checked the route, mode, and diagnostics.
compact = client.search_vector_index(
    "bench_run_001", query_embedding=[0.1, 0.2, 0.3], top_k=1,
    query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.fast",
    quantized_rerank_candidates=32, response_format="ids",
)
print(compact.ids)

# Best-case single-query HTTP request lane: raw little-endian float32 bytes to
# /search/vector-index:binary. It keeps the same explicit exact, quantized-only,
# and quantized-rerank modes as the JSON route; it is separate from batch APIs.
bench_binary = client.search_vector_index(
    "bench_run_001",
    query_embedding=[0.1, 0.2, 0.3],
    top_k=1,
    query_embedding_encoding="f32_le",
    query_mode="quantized_rerank",
    quantized_index_name="embedding.scalar_u8.fast",
    quantized_rerank_candidates=32,
    response_format="ids",
)
print(bench_binary.ids)

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
    max_chunks_per_parent=2,
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

`max_chunks_per_parent` is optional and disabled when omitted or zero. A positive
value preserves the service's fused order and source attribution while limiting
canonical built-in `<parent>#<ordinal>` chunk IDs per parent before final
document fetch. Malformed, extra-separator, and non-canonical IDs such as
`parent#01` remain independent documents.
Collapse stays within the supplied candidate limits, so
`hybrid.stats.collapse_exhaustions == 1` can accompany fewer than `top_k`
documents; `collapse_rejections` reports candidates skipped by the cap.

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

The full filter AST is supported by document count/filter/delete and exact dense
vector search. Filtered keyword/hybrid methods intentionally accept only
equality and one/two-sided range leaves, alone or nested under `AND`, and only
when every field was declared in `scalar_fields` at index creation. Same-field
bounds merge; different fields resolve through bounded scalar indexes and
intersect before text/vector work. `OR`, `NOT`, `!=`, `in`, and `not in` remain
typed `UnsupportedError` results on keyword/hybrid routes even though the client
can serialize them for other document methods.

Any missing/corrupt index, truncated lookup, or snapshot change fails closed
without partial ranking or a local/primary document scan. Truncation raises
`IndexUnavailableError` with `scalar_filter_unbounded`. Hybrid plan/stats models
expose lookup count, per-lookup and aggregate bounds, input IDs, intersection
steps, and final IDs. The client never broadens a filter into a local scan.

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
