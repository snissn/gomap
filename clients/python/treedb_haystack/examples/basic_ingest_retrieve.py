#!/usr/bin/env python3
"""Basic TreeDB Haystack document ingest + embedding retrieval example.

Start the service first, for example:

    go run ./cmd/treedb-document-service \
      -dir /tmp/treedb-document-service \
      -addr 127.0.0.1:7120 \
      -profile command_wal_durable
"""

from __future__ import annotations

import argparse

from haystack import Document, Pipeline
from haystack.document_stores.types import DuplicatePolicy
from haystack_integrations.components.retrievers.treedb import TreeDBEmbeddingRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:7120", help="TreeDB document service base URL")
    parser.add_argument("--index", default="haystack_basic_example", help="TreeDB document-service index name")
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
                id="treedb-overview",
                content="TreeDB stores Haystack documents behind a service API.",
                embedding=[1.0, 0.0, 0.0],
                meta={"topic": "treedb", "kind": "overview"},
            ),
            Document(
                id="haystack-overview",
                content="Haystack pipelines connect components such as retrievers.",
                embedding=[0.0, 1.0, 0.0],
                meta={"topic": "haystack", "kind": "overview"},
            ),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )

    pipeline = Pipeline()
    pipeline.add_component("retriever", TreeDBEmbeddingRetriever(document_store=store, top_k=1))
    result = pipeline.run({"retriever": {"query_embedding": [1.0, 0.0, 0.0]}})
    documents = result["retriever"]["documents"]
    if not documents or documents[0].id != "treedb-overview":
        raise SystemExit(f"unexpected result: {documents!r}")

    print(f"retrieved id={documents[0].id} score={documents[0].score:.6f}")


if __name__ == "__main__":
    main()
