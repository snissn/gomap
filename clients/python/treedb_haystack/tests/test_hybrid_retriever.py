from __future__ import annotations

import copy

import pytest
from haystack import Document as HaystackDocument
from haystack import Pipeline
from haystack.document_stores.errors import DocumentStoreError
from haystack.document_stores.types import DuplicatePolicy, FilterPolicy
from treedb_client import HybridFusionOptions, IndexNotFoundError, IndexStaleError, IndexUnavailableError, UnsupportedError

import _support  # noqa: F401
from fakes import FakeTreeDBClient
from haystack_integrations.components.retrievers.treedb import TreeDBHybridRetriever
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
                embedding=[0.9, 0.1, 0.0],
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
                content="refund shipping notes",
                embedding=[1.0, 0.0, 0.0],
                meta={"category": "A", "rank": 3},
            ),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    )
    return store, client


def test_invalid_document_store_type() -> None:
    with pytest.raises(ValueError, match="TreeDBDocumentStore"):
        TreeDBHybridRetriever(document_store="not-a-store")  # type: ignore[arg-type]


def test_hybrid_text_only_uses_tree_text_source_and_preserves_metadata() -> None:
    store, client = make_populated_store()
    retriever = TreeDBHybridRetriever(document_store=store, top_k=2, candidate_limit=10)

    result = retriever.run(query="refund policy")

    documents = result["documents"]
    assert [doc.id for doc in documents] == ["alpha", "beta"]
    assert documents[0].meta["_treedb_search"]["type"] == "hybrid"
    assert documents[0].meta["_treedb_search"]["sources"][0]["source"] == "text"
    assert client.hybrid_calls[-1]["query"] == "refund policy"
    assert client.hybrid_calls[-1]["query_embedding"] is None
    assert client.hybrid_calls[-1]["candidate_limit"] == 10


def test_hybrid_vector_only_uses_tree_vector_source_and_return_embedding_override() -> None:
    store, client = make_populated_store()
    retriever = TreeDBHybridRetriever(document_store=store, top_k=2, return_embedding=True)

    result = retriever.run(query_embedding=[1.0, 0.0, 0.0])

    documents = result["documents"]
    assert [doc.id for doc in documents] == ["gamma", "alpha"]
    assert documents[0].embedding == [1.0, 0.0, 0.0]
    assert documents[0].meta["_treedb_search"]["sources"][0]["source"] == "vector"
    assert client.hybrid_calls[-1]["query"] is None
    assert client.hybrid_calls[-1]["query_embedding"] == [1.0, 0.0, 0.0]
    assert client.hybrid_calls[-1]["return_embedding"] is True


def test_hybrid_overlapping_candidates_are_fused_and_fusion_options_pass_through() -> None:
    store, client = make_populated_store()
    fusion = HybridFusionOptions(
        method="rrf",
        rrf_k=10,
        tie_policy="fused_score_best_rank_source_order_id",
        source_order=["text", "vector"],
    )
    retriever = TreeDBHybridRetriever(
        document_store=store,
        top_k=3,
        text_candidate_limit=3,
        vector_candidate_limit=3,
        ef_search=64,
        fusion=fusion,
    )

    result = retriever.run(query="refund", query_embedding=[1.0, 0.0, 0.0])

    documents = result["documents"]
    assert documents[0].id in {"alpha", "gamma"}
    assert documents[0].meta["_treedb_search"]["fusion_method"] == "rrf"
    assert len(documents[0].meta["_treedb_search"]["sources"]) == 2
    assert client.hybrid_calls[-1]["fusion"] == {
        "method": "rrf",
        "rrf_k": 10,
        "tie_policy": "fused_score_best_rank_source_order_id",
        "source_order": ["text", "vector"],
    }
    assert client.hybrid_calls[-1]["text_candidate_limit"] == 3
    assert client.hybrid_calls[-1]["vector_candidate_limit"] == 3
    assert client.hybrid_calls[-1]["ef_search"] == 64


def test_hybrid_runtime_fusion_options_override_init_options() -> None:
    store, client = make_populated_store()
    retriever = TreeDBHybridRetriever(
        document_store=store,
        fusion={"method": "rrf", "rrf_k": 60, "source_order": ["text", "vector"]},
    )

    retriever.run(
        query="refund",
        query_embedding=[1.0, 0.0, 0.0],
        fusion={"method": "rrf", "rrf_k": 5, "source_order": ["vector", "text"]},
    )

    assert client.hybrid_calls[-1]["fusion"] == {"method": "rrf", "rrf_k": 5, "source_order": ["vector", "text"]}


def test_hybrid_filter_policy_replace_uses_runtime_filters_without_mutating_init_filters() -> None:
    store, client = make_populated_store()
    init_filters = {"field": "meta.category", "operator": "==", "value": "A"}
    retriever = TreeDBHybridRetriever(
        document_store=store,
        filters=init_filters,
        top_k=3,
        filter_policy=FilterPolicy.REPLACE,
    )

    result = retriever.run(query="policy", filters={"field": "meta.category", "operator": "==", "value": "B"})

    assert [doc.id for doc in result["documents"]] == ["beta"]
    assert client.hybrid_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "B"}
    assert retriever.filters == init_filters


def test_hybrid_filter_policy_merge_passes_combined_runtime_and_init_filters() -> None:
    store, client = make_populated_store()
    retriever = TreeDBHybridRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=3,
        filter_policy="merge",
    )

    result = retriever.run(query="refund", filters={"field": "meta.rank", "operator": ">=", "value": 3})

    assert [doc.id for doc in result["documents"]] == ["gamma"]
    assert client.hybrid_calls[-1]["filter"] == {
        "operator": "AND",
        "conditions": [
            {"field": "meta.category", "operator": "==", "value": "A"},
            {"field": "meta.rank", "operator": ">=", "value": 3},
        ],
    }


def test_hybrid_filters_fail_closed_through_service_without_scan_fallback() -> None:
    class UnsupportedFilterClient(FakeTreeDBClient):
        def search_hybrid(self, *args: object, **kwargs: object) -> object:
            self.hybrid_calls.append({"filter": copy.deepcopy(kwargs.get("filter"))})
            raise UnsupportedError("unsupported", "hybrid filters unsupported")

    client = UnsupportedFilterClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        client=client,  # type: ignore[arg-type]
    )
    retriever = TreeDBHybridRetriever(document_store=store)

    with pytest.raises(DocumentStoreError, match="hybrid filters unsupported"):
        retriever.run(query="policy", filters={"field": "meta.category", "operator": "==", "value": "A"})

    assert client.hybrid_calls[-1]["filter"] == {"field": "meta.category", "operator": "==", "value": "A"}
    assert client.filter_calls == []


@pytest.mark.parametrize(
    ("error", "run_kwargs"),
    [
        (IndexNotFoundError("index_not_found", "missing text index"), {"query": "policy"}),
        (IndexNotFoundError("index_not_found", "missing vector index"), {"query_embedding": [1.0, 0.0, 0.0]}),
        (IndexStaleError("index_stale", "stale hybrid snapshot"), {"query": "policy"}),
        (IndexUnavailableError("index_unavailable", "vector index unavailable"), {"query_embedding": [1.0, 0.0, 0.0]}),
    ],
)
def test_hybrid_service_index_errors_fail_closed_without_empty_success(
    error: Exception, run_kwargs: dict[str, object]
) -> None:
    class ErrorClient(FakeTreeDBClient):
        def search_hybrid(self, *args: object, **kwargs: object) -> object:
            raise error

    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        client=ErrorClient(),  # type: ignore[arg-type]
    )
    retriever = TreeDBHybridRetriever(document_store=store)

    with pytest.raises(DocumentStoreError, match=str(error).split(": ", 1)[-1]):
        retriever.run(**run_kwargs)  # type: ignore[arg-type]


def test_hybrid_to_dict_from_dict_round_trip() -> None:
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="serialized",
        embedding_dimension=3,
        ensure_index=False,
        client=FakeTreeDBClient(),  # type: ignore[arg-type]
    )
    retriever = TreeDBHybridRetriever(
        document_store=store,
        filters={"field": "meta.category", "operator": "==", "value": "A"},
        top_k=5,
        candidate_limit=50,
        text_candidate_limit=25,
        vector_candidate_limit=30,
        ef_search=64,
        fusion={"method": "rrf", "rrf_k": 60, "tie_policy": "fused_score_best_rank_source_order_id"},
        return_embedding=True,
        filter_policy=FilterPolicy.MERGE,
    )

    serialized = retriever.to_dict()
    original = copy.deepcopy(serialized)

    assert serialized["type"] == "haystack_integrations.components.retrievers.treedb.hybrid_retriever.TreeDBHybridRetriever"
    restored = TreeDBHybridRetriever.from_dict(serialized)
    assert serialized == original
    assert restored.top_k == 5
    assert restored.candidate_limit == 50
    assert restored.text_candidate_limit == 25
    assert restored.vector_candidate_limit == 30
    assert restored.ef_search == 64
    assert restored.fusion == {"method": "rrf", "rrf_k": 60, "tie_policy": "fused_score_best_rank_source_order_id"}
    assert restored.return_embedding is True
    assert restored.filter_policy == FilterPolicy.MERGE
    assert isinstance(restored.document_store, TreeDBDocumentStore)


def test_hybrid_pipeline_wiring_runs_retriever_component() -> None:
    store, _ = make_populated_store()
    pipeline = Pipeline()
    pipeline.add_component("retriever", TreeDBHybridRetriever(document_store=store, top_k=1))

    result = pipeline.run({"retriever": {"query": "refund", "query_embedding": [1.0, 0.0, 0.0]}})

    assert result["retriever"]["documents"]


def test_hybrid_run_rejects_missing_query_sources_and_non_positive_top_k() -> None:
    store, client = make_populated_store()
    retriever = TreeDBHybridRetriever(document_store=store)

    with pytest.raises(ValueError, match="query or query_embedding"):
        retriever.run()
    with pytest.raises(ValueError, match="query or query_embedding"):
        retriever.run(query="")
    assert client.hybrid_calls == []
    with pytest.raises(ValueError, match="top_k"):
        retriever.run(query="policy", top_k=0)
