#!/usr/bin/env python3
"""TreeDB Haystack keyword + hybrid retrieval example.

Start the service first, for example:

    go run ./cmd/treedb-document-service \
      -dir /tmp/treedb-document-service \
      -addr 127.0.0.1:7120 \
      -profile command_wal_durable

This example keeps filters out of the query to focus on ranking. Metadata filters
are supported when fields are declared with `scalar_fields` at index creation;
undeclared or unrepresentable filters fail closed rather than scanning locally.
"""

from __future__ import annotations

import argparse

from haystack import Document, Pipeline
from haystack.document_stores.types import DuplicatePolicy
from haystack_integrations.components.retrievers.treedb import TreeDBHybridRetriever, TreeDBKeywordRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:7120", help="TreeDB document service base URL")
    parser.add_argument("--index", default="haystack_keyword_hybrid_example", help="TreeDB document-service index name")
    args = parser.parse_args()

    store = TreeDBDocumentStore(
        base_url=args.base_url,
        index=args.index,
        embedding_dimension=3,
        similarity="cosine",
        return_embedding=False,
    )
    store.write_documents(
        [
            Document(
                id="treedb-search",
                content="TreeDB service keyword search and hybrid retrieval use indexed document content.",
                embedding=[1.0, 0.0, 0.0],
                meta={"topic": "treedb", "kind": "search"},
            ),
            Document(
                id="haystack-pipelines",
                content="Haystack pipelines connect writers, embedders, retrievers, and readers.",
                embedding=[0.0, 1.0, 0.0],
                meta={"topic": "haystack", "kind": "pipeline"},
            ),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )

    keyword_pipeline = Pipeline()
    keyword_pipeline.add_component("retriever", TreeDBKeywordRetriever(document_store=store, top_k=1))
    keyword_result = keyword_pipeline.run({"retriever": {"query": "TreeDB keyword search"}})
    keyword_docs = keyword_result["retriever"]["documents"]
    if not keyword_docs or keyword_docs[0].id != "treedb-search":
        raise SystemExit(f"unexpected keyword result: {keyword_docs!r}")

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
        {"retriever": {"query": "TreeDB keyword search", "query_embedding": [1.0, 0.0, 0.0]}}
    )
    hybrid_docs = hybrid_result["retriever"]["documents"]
    if not hybrid_docs or hybrid_docs[0].id != "treedb-search":
        raise SystemExit(f"unexpected hybrid result: {hybrid_docs!r}")

    keyword_meta = keyword_docs[0].meta.get("_treedb_search", {})
    hybrid_meta = hybrid_docs[0].meta.get("_treedb_search", {})
    print(
        "keyword "
        f"id={keyword_docs[0].id} score={keyword_docs[0].score:.6f} "
        f"explanation_type={keyword_meta.get('type')}"
    )
    print(
        "hybrid "
        f"id={hybrid_docs[0].id} score={hybrid_docs[0].score:.6f} "
        f"explanation_type={hybrid_meta.get('type')}"
    )


if __name__ == "__main__":
    main()
