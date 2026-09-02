from __future__ import annotations

import asyncio
import copy

import pytest
from haystack import Document as HaystackDocument
from haystack import Pipeline
from haystack.document_stores.types import DuplicatePolicy, FilterPolicy

import _support  # noqa: F401
from fakes import FakeTreeDBClient
from haystack_integrations.components.retrievers.treedb import TreeDBEmbeddingRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def make_populated_store(*, return_embedding: bool = False) -> tuple[TreeDBDocumentStore, FakeTreeDBClient]:
    client = FakeTreeDBClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        similarity="cosine",
        return_embedding=return_embedding,
        client=client,  # type: ignore[arg-type]
    )
    store.write_documents(
        [
            HaystackDocument(id="alpha", content="alpha", embedding=[1.0, 0.0, 0.0], meta={"category": "A", "rank": 1}),
            HaystackDocument(id="beta", content="beta", embedding=[0.0, 1.0, 0.0], meta={"category": "B", "rank": 2}),
            HaystackDocument(id="gamma", content="gamma", embedding=[0.8, 0.2, 0.0], meta={"category": "A", "rank": 3}),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )
    return store, client


def test_invalid_document_store_type() -> None:
    with pytest.raises(ValueError, match="TreeDBDocumentStore"):
        TreeDBEmbeddingRetriever(document_store="not-a-store")  # type: ignore[arg-type]


def test_run_top_k_and_return_embedding_override() -> None:
    store, client = make_populated_store(return_embedding=False)
    retriever = TreeDBEmbeddingRetriever(document_store=store, top_k=2, return_embedding=True)

    result = retriever.run(query_embedding=[1.0, 0.0, 0.0])

    assert [doc.id for doc in result["documents"]] == ["alpha", "gamma"]
    assert all(isinstance(doc, HaystackDocument) for doc in result["documents"])
    assert result["documents"][0].embedding == [1.0, 0.0, 0.0]
    assert client.query_calls[-1]["top_k"] == 2
    assert client.query_calls[-1]["return_embedding"] is True
    assert client.query_calls[-1]["route"] == "exact"


def test_run_filters_and_top_k_override() -> None:
    store, client = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(document_store=store, top_k=3)

    result = retriever.run(
        query_embedding=[1.0, 0.0, 0.0],
        filters={"field": "meta.category", "operator": "==", "value": "B"},
        top_k=1,
    )

    assert [doc.id for doc in result["documents"]] == ["beta"]
    assert client.query_calls[-1]["top_k"] == 1
    assert client.query_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "B"}


def test_filter_policy_replace_uses_runtime_filter() -> None:
    store, client = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=3,
        filter_policy=FilterPolicy.REPLACE,
    )

    result = retriever.run(
        query_embedding=[0.0, 1.0, 0.0],
        filters={"field": "meta.category", "operator": "==", "value": "B"},
    )

    assert [doc.id for doc in result["documents"]] == ["beta"]
    assert client.query_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "B"}


def test_filter_policy_merge_combines_init_and_runtime_filters() -> None:
    store, client = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=3,
        filter_policy="merge",
    )

    result = retriever.run(
        query_embedding=[1.0, 0.0, 0.0],
        filters={"field": "meta.rank", "operator": ">=", "value": 3},
    )

    assert [doc.id for doc in result["documents"]] == ["gamma"]
    assert client.query_calls[-1]["filter"] == {
        "operator": "AND",
        "conditions": [
            {"field": "meta.category", "operator": "==", "value": "A"},
            {"field": "meta.rank", "operator": ">=", "value": 3},
        ],
    }


def test_filter_policy_merge_does_not_mutate_init_filters() -> None:
    store, client = make_populated_store()
    init_filters = {
        "operator": "AND",
        "conditions": [{"field": "meta.category", "operator": "==", "value": "A"}],
    }
    retriever = TreeDBEmbeddingRetriever(
        document_store=store,
        filters=init_filters,
        top_k=3,
        filter_policy=FilterPolicy.MERGE,
    )

    first = retriever.run(
        query_embedding=[1.0, 0.0, 0.0],
        filters={"field": "meta.rank", "operator": ">=", "value": 3},
    )
    second = retriever.run(query_embedding=[1.0, 0.0, 0.0])

    assert [doc.id for doc in first["documents"]] == ["gamma"]
    assert [doc.id for doc in second["documents"]] == ["alpha", "gamma"]
    assert retriever.filters == init_filters
    assert client.query_calls[-1]["filter"] == init_filters


def test_to_dict_from_dict_round_trip() -> None:
    client = FakeTreeDBClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="serialized",
        embedding_dimension=3,
        ensure_index=False,
        client=client,  # type: ignore[arg-type]
    )
    retriever = TreeDBEmbeddingRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=5,
        return_embedding=True,
        filter_policy=FilterPolicy.MERGE,
    )

    serialized = retriever.to_dict()
    original = copy.deepcopy(serialized)

    assert serialized["type"] == (
        "haystack_integrations.components.retrievers.treedb.embedding_retriever.TreeDBEmbeddingRetriever"
    )
    assert serialized["init_parameters"]["filter_policy"] == "merge"
    restored = TreeDBEmbeddingRetriever.from_dict(serialized)
    assert serialized == original
    assert restored.top_k == 5
    assert restored.filter_policy == FilterPolicy.MERGE
    assert restored.return_embedding is True
    assert isinstance(restored.document_store, TreeDBDocumentStore)
    assert restored.document_store.ensure_index is False


def test_pipeline_wiring_runs_retriever_component() -> None:
    store, _ = make_populated_store()
    pipeline = Pipeline()
    pipeline.add_component("retriever", TreeDBEmbeddingRetriever(document_store=store, top_k=1))

    result = pipeline.run({"retriever": {"query_embedding": [1.0, 0.0, 0.0]}})

    assert [doc.id for doc in result["retriever"]["documents"]] == ["alpha"]


def test_run_async_returns_documents_without_calling_sync_path_on_event_loop() -> None:
    store, _ = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(document_store=store, top_k=1)

    result = asyncio.run(retriever.run_async(query_embedding=[1.0, 0.0, 0.0]))

    assert [doc.id for doc in result["documents"]] == ["alpha"]


def test_fake_cosine_handles_zero_norm_vectors_for_tests() -> None:
    store, _ = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(document_store=store, top_k=3)

    result = retriever.run(query_embedding=[0.0, 0.0, 0.0])

    assert [doc.score for doc in result["documents"]] == [0.0, 0.0, 0.0]


def test_run_rejects_non_positive_top_k() -> None:
    store, _ = make_populated_store()
    retriever = TreeDBEmbeddingRetriever(document_store=store)

    with pytest.raises(ValueError, match="top_k"):
        retriever.run(query_embedding=[1.0, 0.0, 0.0], top_k=0)
