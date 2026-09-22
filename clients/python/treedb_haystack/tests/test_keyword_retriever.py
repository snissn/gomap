from __future__ import annotations

import copy

import pytest
from haystack import Document as HaystackDocument
from haystack import Pipeline
from haystack.document_stores.errors import DocumentStoreError
from haystack.document_stores.types import DuplicatePolicy, FilterPolicy
from treedb_client import IndexNotFoundError, IndexStaleError, IndexUnavailableError, UnsupportedError

import _support  # noqa: F401
from fakes import FakeTreeDBClient
from haystack_integrations.components.retrievers.treedb import TreeDBKeywordRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def make_populated_store() -> tuple[TreeDBDocumentStore, FakeTreeDBClient]:
    client = FakeTreeDBClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        similarity="cosine",
        return_embedding=False,
        client=client,  # type: ignore[arg-type]
    )
    store.write_documents(
        [
            HaystackDocument(
                id="alpha",
                content="refund refund policy details",
                embedding=[1.0, 0.0, 0.0],
                meta={"category": "A", "rank": 1},
            ),
            HaystackDocument(
                id="beta",
                content="policy handbook",
                embedding=[0.0, 1.0, 0.0],
                meta={"category": "B", "rank": 2},
            ),
            HaystackDocument(
                id="gamma",
                content="shipping notes",
                embedding=[0.0, 0.0, 1.0],
                meta={"category": "A", "rank": 3},
            ),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )
    return store, client


def test_invalid_document_store_type() -> None:
    with pytest.raises(ValueError, match="TreeDBDocumentStore"):
        TreeDBKeywordRetriever(document_store="not-a-store")  # type: ignore[arg-type]


def test_keyword_retriever_returns_ranked_lexical_results_and_search_metadata() -> None:
    store, client = make_populated_store()
    retriever = TreeDBKeywordRetriever(
        document_store=store,
        top_k=2,
        operator="or",
        candidate_limit=10,
        max_postings_scanned=1000,
        return_embedding=True,
    )

    result = retriever.run(query="refund policy")

    documents = result["documents"]
    assert [doc.id for doc in documents] == ["alpha", "beta"]
    assert documents[0].score == 3.0
    assert documents[0].embedding == [1.0, 0.0, 0.0]
    assert documents[0].meta["_treedb_search"]["type"] == "keyword"
    assert documents[0].meta["_treedb_search"]["matched_terms"] == ["policy", "refund"]
    assert client.keyword_calls[-1] == {
        "query": "refund policy",
        "top_k": 2,
        "operator": "or",
        "candidate_limit": 10,
        "max_postings_scanned": 1000,
        "filter": None,
        "return_embedding": True,
        "expected_generation": 1,
    }


def test_keyword_filter_policy_replace_uses_runtime_filters_without_mutating_init_filters() -> None:
    store, client = make_populated_store()
    init_filters = {"field": "meta.category", "operator": "==", "value": "A"}
    retriever = TreeDBKeywordRetriever(
        document_store=store,
        filters=init_filters,
        top_k=3,
        filter_policy=FilterPolicy.REPLACE,
    )

    result = retriever.run(query="policy", filters={"field": "meta.category", "operator": "==", "value": "B"})

    assert [doc.id for doc in result["documents"]] == ["beta"]
    assert client.keyword_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "B"}
    assert retriever.filters == init_filters


def test_keyword_filter_policy_merge_passes_combined_runtime_and_init_filters() -> None:
    store, client = make_populated_store()
    retriever = TreeDBKeywordRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=3,
        filter_policy="merge",
    )

    result = retriever.run(query="refund policy", filters={"field": "meta.rank", "operator": "<", "value": 3})

    assert [doc.id for doc in result["documents"]] == ["alpha"]
    assert client.keyword_calls[-1]["filter"] == {
        "operator": "AND",
        "conditions": [
            {"field": "meta.category", "operator": "==", "value": "A"},
            {"field": "meta.rank", "operator": "<", "value": 3},
        ],
    }


def test_keyword_filter_policy_replace_uses_init_filters_when_runtime_filters_are_absent() -> None:
    store, client = make_populated_store()
    retriever = TreeDBKeywordRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=3,
    )

    result = retriever.run(query="policy")

    assert [doc.id for doc in result["documents"]] == ["alpha"]
    assert client.keyword_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "A"}


def test_keyword_filters_fail_closed_through_service_without_scan_fallback() -> None:
    class UnsupportedFilterClient(FakeTreeDBClient):
        def search_keyword(self, *args: object, **kwargs: object) -> object:
            self.keyword_calls.append({"filter": copy.deepcopy(kwargs.get("filter"))})
            raise UnsupportedError("unsupported", "keyword filters unsupported")

    client = UnsupportedFilterClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        client=client,  # type: ignore[arg-type]
    )
    retriever = TreeDBKeywordRetriever(document_store=store)

    with pytest.raises(DocumentStoreError, match="keyword filters unsupported"):
        retriever.run(query="policy", filters={"field": "meta.category", "operator": "==", "value": "A"})

    assert client.keyword_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "A"}
    assert client.filter_calls == []


@pytest.mark.parametrize(
    "error",
    [
        IndexNotFoundError("index_not_found", "missing text index"),
        IndexStaleError("index_stale", "stale text index"),
        IndexUnavailableError("index_unavailable", "text index unavailable"),
    ],
)
def test_keyword_service_index_errors_fail_closed_without_empty_success(error: Exception) -> None:
    class ErrorClient(FakeTreeDBClient):
        def search_keyword(self, *args: object, **kwargs: object) -> object:
            raise error

    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        client=ErrorClient(),  # type: ignore[arg-type]
    )
    retriever = TreeDBKeywordRetriever(document_store=store)

    with pytest.raises(DocumentStoreError, match=str(error).split(": ", 1)[-1]):
        retriever.run(query="policy")


def test_keyword_to_dict_from_dict_round_trip() -> None:
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="serialized",
        embedding_dimension=3,
        ensure_index=False,
        client=FakeTreeDBClient(),  # type: ignore[arg-type]
    )
    retriever = TreeDBKeywordRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=5,
        operator="and",
        candidate_limit=50,
        max_postings_scanned=500,
        return_embedding=True,
        filter_policy=FilterPolicy.MERGE,
    )

    serialized = retriever.to_dict()
    original = copy.deepcopy(serialized)

    assert serialized["type"] == "haystack_integrations.components.retrievers.treedb.keyword_retriever.TreeDBKeywordRetriever"
    restored = TreeDBKeywordRetriever.from_dict(serialized)
    assert serialized == original
    assert restored.top_k == 5
    assert restored.operator == "and"
    assert restored.candidate_limit == 50
    assert restored.max_postings_scanned == 500
    assert restored.return_embedding is True
    assert restored.filter_policy == FilterPolicy.MERGE
    assert isinstance(restored.document_store, TreeDBDocumentStore)


def test_keyword_pipeline_wiring_runs_retriever_component() -> None:
    store, _ = make_populated_store()
    pipeline = Pipeline()
    pipeline.add_component("retriever", TreeDBKeywordRetriever(document_store=store, top_k=1))

    result = pipeline.run({"retriever": {"query": "refund policy"}})

    assert [doc.id for doc in result["retriever"]["documents"]] == ["alpha"]


def test_keyword_run_rejects_non_positive_top_k() -> None:
    store, _ = make_populated_store()
    retriever = TreeDBKeywordRetriever(document_store=store)

    with pytest.raises(ValueError, match="top_k"):
        retriever.run(query="policy", top_k=0)
