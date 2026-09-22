#!/usr/bin/env python3
"""TreeDB Haystack code-search metadata example.

This example stores code chunks with repository/path/symbol/line metadata and
retrieves them with dense embeddings plus a service-side metadata filter.
Metadata filters are declared at index creation and executed by TreeDB; this
example uses dense retrieval to keep the code-search path focused.
"""

from __future__ import annotations

import argparse

from haystack import Document, Pipeline
from haystack.document_stores.types import DuplicatePolicy, FilterPolicy
from haystack_integrations.components.retrievers.treedb import TreeDBEmbeddingRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:7120", help="TreeDB document service base URL")
    parser.add_argument("--index", default="haystack_code_search_example", help="TreeDB document-service index name")
    args = parser.parse_args()

    store = TreeDBDocumentStore(
        base_url=args.base_url,
        index=args.index,
        embedding_dimension=4,
        similarity="cosine",
        return_embedding=False,
    )
    store.write_documents(
        [
            Document(
                id="TreeDB/documentservice/service.go:SearchDenseVector",
                content="SearchDenseVector scores stored embeddings exactly with optional metadata filters.",
                embedding=[1.0, 0.0, 0.0, 0.0],
                meta={
                    "repo": "snissn/gomap",
                    "path": "TreeDB/documentservice/service.go",
                    "language": "go",
                    "symbol": "SearchDenseVector",
                    "start_line": 250,
                    "end_line": 320,
                    "chunk_kind": "function",
                },
            ),
            Document(
                id="clients/python/treedb_haystack/README.md:filters",
                content="TreeDB Haystack filters are executed by the TreeDB document service.",
                embedding=[0.0, 1.0, 0.0, 0.0],
                meta={
                    "repo": "snissn/gomap",
                    "path": "clients/python/treedb_haystack/README.md",
                    "language": "markdown",
                    "symbol": "Filters",
                    "start_line": 70,
                    "end_line": 90,
                    "chunk_kind": "section",
                },
            ),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )

    filters = {"field": "meta.language", "operator": "==", "value": "go"}
    pipeline = Pipeline()
    pipeline.add_component(
        "retriever",
        TreeDBEmbeddingRetriever(
            document_store=store,
            filters={"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
            filter_policy=FilterPolicy.MERGE,
            top_k=2,
        ),
    )
    result = pipeline.run({"retriever": {"query_embedding": [1.0, 0.0, 0.0, 0.0], "filters": filters}})
    documents = result["retriever"]["documents"]
    if not documents or documents[0].meta.get("symbol") != "SearchDenseVector":
        raise SystemExit(f"unexpected result: {documents!r}")

    doc = documents[0]
    print(
        "retrieved "
        f"symbol={doc.meta['symbol']} path={doc.meta['path']} "
        f"lines={doc.meta['start_line']}-{doc.meta['end_line']} score={doc.score:.6f}"
    )


if __name__ == "__main__":
    main()
