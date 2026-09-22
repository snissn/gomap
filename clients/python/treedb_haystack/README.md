# treedb-haystack (pre-alpha)

`treedb-haystack` is the first Haystack integration package for TreeDB's
pre-alpha HTTP/JSON document service. It provides:

- `haystack_integrations.document_stores.treedb.TreeDBDocumentStore`
- `haystack_integrations.components.retrievers.treedb.TreeDBEmbeddingRetriever`
- `haystack_integrations.components.retrievers.treedb.TreeDBKeywordRetriever`
- `haystack_integrations.components.retrievers.treedb.TreeDBHybridRetriever`

The integration supports embedded Haystack `Document` writes, exact dense-vector
retrieval, ranked keyword retrieval, and TreeDB-native hybrid text/vector
retrieval through the TreeDB document service. It never scans documents
client-side to emulate unsupported filters or search modes.

Service contract: [`docs/TREEDB_DOCUMENT_SERVICE_API.md`](../../../docs/TREEDB_DOCUMENT_SERVICE_API.md)

## Install for local development

From the repository root, install the sibling TreeDB client first, then this
package:

```sh
python3 -m venv .venv-treedb-haystack
. .venv-treedb-haystack/bin/activate
python -m pip install -U pip
python -m pip install -e clients/python/treedb_client
python -m pip install -e 'clients/python/treedb_haystack[dev]'
```

If your pip version does not support editable extras for local paths, install the
test tools explicitly:

```sh
python -m pip install pytest mypy ruff
```

## Start TreeDB's document service

```sh
go run ./cmd/treedb-document-service \
  -dir /tmp/treedb-document-service \
  -addr 127.0.0.1:7120 \
  -profile command_wal_durable
```

## Example

```python
from haystack import Document, Pipeline
from haystack.document_stores.types import DuplicatePolicy
from haystack_integrations.components.retrievers.treedb import (
    TreeDBEmbeddingRetriever,
    TreeDBHybridRetriever,
    TreeDBKeywordRetriever,
)
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore

store = TreeDBDocumentStore(
    base_url="http://127.0.0.1:7120",
    index="docs",
    embedding_dimension=3,
    similarity="cosine",
    return_embedding=False,
)

store.write_documents(
    [
        Document(
            id="doc-a",
            content="TreeDB stores Haystack documents.",
            embedding=[1.0, 0.0, 0.0],
            meta={"repo": "snissn/gomap", "language": "python"},
        ),
        Document(
            id="doc-b",
            content="Keyword and hybrid retrieval run through TreeDB service indexes.",
            embedding=[0.0, 1.0, 0.0],
            meta={"repo": "snissn/gomap", "language": "docs"},
        ),
    ],
    policy=DuplicatePolicy.OVERWRITE,
)

embedding_pipeline = Pipeline()
embedding_pipeline.add_component("retriever", TreeDBEmbeddingRetriever(document_store=store, top_k=1))
embedding_result = embedding_pipeline.run({"retriever": {"query_embedding": [1.0, 0.0, 0.0]}})
print(embedding_result["retriever"]["documents"][0].id)

keyword_pipeline = Pipeline()
keyword_pipeline.add_component("retriever", TreeDBKeywordRetriever(document_store=store, top_k=1))
keyword_result = keyword_pipeline.run({"retriever": {"query": "TreeDB service indexes"}})
print(keyword_result["retriever"]["documents"][0].id)

hybrid_pipeline = Pipeline()
hybrid_pipeline.add_component(
    "retriever",
    TreeDBHybridRetriever(
        document_store=store,
        top_k=1,
        fusion={"method": "rrf", "rrf_k": 60, "source_order": ["text", "vector"]},
    ),
)
hybrid_result = hybrid_pipeline.run(
    {"retriever": {"query": "TreeDB service indexes", "query_embedding": [1.0, 0.0, 0.0]}}
)
print(hybrid_result["retriever"]["documents"][0].id)
```

## Runnable examples

Example scripts live in [`examples/`](examples/):

- [`basic_ingest_retrieve.py`](examples/basic_ingest_retrieve.py) writes two embedded Haystack documents and retrieves one through an embedding `Pipeline`.
- [`keyword_hybrid_retrieve.py`](examples/keyword_hybrid_retrieve.py) demonstrates keyword and hybrid retrievers over the same TreeDB index.
- [`code_search_metadata.py`](examples/code_search_metadata.py) demonstrates code-search metadata fields such as `repo`, `path`, `language`, `symbol`, `start_line`, `end_line`, and `chunk_kind` with service-side embedding filters.

After starting `cmd/treedb-document-service`, run them from the repository root with the local packages installed:

```sh
python clients/python/treedb_haystack/examples/basic_ingest_retrieve.py
python clients/python/treedb_haystack/examples/keyword_hybrid_retrieve.py
python clients/python/treedb_haystack/examples/code_search_metadata.py
```

## Filters

Filters use the TreeDB service/Haystack v2 filter AST and are executed by the
TreeDB service:

```python
{"field": "meta.repo", "operator": "==", "value": "snissn/gomap"}
```

Boolean filters use `conditions`. Supported operators are `AND`, `OR`, `NOT`,
`==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, and `not in`. Unsupported operators fail
closed through the base `treedb-client`; this package does not broaden them into
local scans.

The full AST is supported by document count/filter/delete and exact dense-vector
retrieval. Keyword/hybrid retrieval accepts only equality and one/two-sided
range leaves, alone or joined by nested `AND`, over fields declared in
`TreeDBDocumentStore(..., scalar_fields=[...])` (or in the service create-index
request). Same-field bounds merge and different fields use bounded indexed
intersection before source work. `OR`, `NOT`, `!=`, `in`, and `not in` remain
typed unsupported shapes on these two routes.

Any missing/corrupt index, truncated lookup, or snapshot change fails closed
without partial ranking. Truncation reports `index_unavailable` with
`scalar_filter_unbounded`; undeclared fields return `invalid_request`. The
retrievers never fetch and filter documents client-side.

## Duplicate and filter policies

- `DuplicatePolicy.OVERWRITE` maps to TreeDB service upsert and is the default
  because upsert is the only atomic write primitive in the MVP service.
- `DuplicatePolicy.FAIL` and `DuplicatePolicy.SKIP` check existing IDs with a
  server-side `id in [...]` filter before writing. They avoid client-side full
  scans, but they are best-effort across separate concurrent clients because the
  current service has no create-if-absent endpoint. If TreeDB reports an
  unexpected update after the preflight, the store raises `DocumentStoreError`
  rather than silently reporting success.
- `TreeDBEmbeddingRetriever`, `TreeDBKeywordRetriever`, and
  `TreeDBHybridRetriever` support Haystack `FilterPolicy.REPLACE` and
  `FilterPolicy.MERGE` via Haystack's `apply_filter_policy` helper.

## Run tests

Unit tests require Haystack and pytest:

```sh
cd clients/python/treedb_haystack
PYTHONPATH=src:../treedb_client/src python -m pytest -q
```

Optional integration tests start `go run ./cmd/treedb-document-service`, write to
a temporary TreeDB directory, and restart the service for a persistence smoke:

```sh
cd clients/python/treedb_haystack
TREEDB_HAYSTACK_RUN_INTEGRATION=1 PYTHONPATH=src:../treedb_client/src python -m pytest -q
```

Type/import checks used during local development:

```sh
cd clients/python/treedb_haystack
PYTHONPATH=src:../treedb_client/src python -m compileall -q src tests
python -m mypy -p haystack_integrations.document_stores.treedb -p haystack_integrations.components.retrievers.treedb
```

## Haystack integrations listing

A listing draft for the Haystack integrations index is prepared in the
`snissn/haystack-integrations` fork on branch `treedb-listing` as
`integrations/treedb.md`. It uses the gomap repository URL instead of a PyPI URL
until package publication is finalized. Open the upstream listing PR after the
installation/release URLs are stable.

## Scope and non-goals

TreeDB's service-backed dense-vector search is an exact correctness/MVP path. It
is not advertised as ANN throughput work. Keyword and hybrid retrievers delegate
to TreeDB's service indexes; this package does not implement sparse retrieval,
client-side keyword scans, client-side fusion fallbacks, or code-symbol graph
retrieval.
