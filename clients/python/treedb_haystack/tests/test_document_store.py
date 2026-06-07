from __future__ import annotations

import pytest
from haystack import Document as HaystackDocument
from haystack.dataclasses import ByteStream
from haystack.dataclasses.sparse_embedding import SparseEmbedding
from haystack.document_stores.errors import DocumentStoreError, DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.errors import FilterError
from treedb_client import Document as TreeDBDocument
from treedb_client import InvalidFilterError as TreeDBInvalidFilterError

import _support  # noqa: F401
from fakes import FakeTreeDBClient
from haystack_integrations.document_stores.treedb import (
    TreeDBDocumentStore,
    haystack_document_to_treedb_document,
    treedb_document_to_haystack_document,
)


def make_store(*, return_embedding: bool = False, ensure_index: bool = True) -> tuple[TreeDBDocumentStore, FakeTreeDBClient]:
    client = FakeTreeDBClient()
    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        similarity="cosine",
        return_embedding=return_embedding,
        ensure_index=ensure_index,
        client=client,  # type: ignore[arg-type]
    )
    return store, client


def haystack_doc(doc_id: str, embedding: list[float], **meta: object) -> HaystackDocument:
    return HaystackDocument(id=doc_id, content=f"content {doc_id}", embedding=embedding, meta=dict(meta))


def test_constructor_ensures_index_and_count_write_filter_delete_round_trip() -> None:
    store, client = make_store(return_embedding=True)

    assert client.ensure_calls == [("docs", 3, "cosine")]
    assert store.write_documents(
        [
            haystack_doc("a", [1, 0, 0], repo="gomap", language="go"),
            haystack_doc("b", [0, 1, 0], repo="gomap", language="python"),
            haystack_doc("c", [0, 0, 1], repo="other", language="go"),
        ],
        policy=DuplicatePolicy.OVERWRITE,
    ) == 3

    assert store.count_documents() == 3
    assert store.count_documents(filters={"field": "meta.repo", "operator": "==", "value": "gomap"}) == 2

    filtered = store.filter_documents({"field": "meta.language", "operator": "==", "value": "go"})
    assert [doc.id for doc in filtered] == ["a", "c"]
    assert filtered[0].embedding == [1.0, 0.0, 0.0]

    store.delete_documents(["c"])
    assert store.count_documents() == 2
    deleted = store.delete_by_filter({"field": "meta.language", "operator": "==", "value": "python"})
    assert deleted == 1
    assert store.count_documents() == 1


def test_write_documents_fail_policy_uses_server_side_id_filter() -> None:
    store, client = make_store()
    store.write_documents([haystack_doc("dup", [1, 0, 0])], policy=DuplicatePolicy.OVERWRITE)

    with pytest.raises(DuplicateDocumentError, match="dup"):
        store.write_documents([haystack_doc("dup", [0, 1, 0])], policy=DuplicatePolicy.FAIL)

    assert client.filter_calls[-1]["filter"] == {"field": "id", "operator": "in", "value": ["dup"]}
    assert client.upsert_calls == [["dup"]]


def test_write_documents_skip_policy_writes_only_new_ids() -> None:
    store, client = make_store()
    store.write_documents([haystack_doc("existing", [1, 0, 0])], policy=DuplicatePolicy.OVERWRITE)

    written = store.write_documents(
        [haystack_doc("existing", [0, 1, 0]), haystack_doc("new", [0, 0, 1])],
        policy=DuplicatePolicy.SKIP,
    )

    assert written == 1
    assert client.filter_calls[-1]["filter"] == {"field": "id", "operator": "in", "value": ["existing", "new"]}
    assert client.upsert_calls[-1] == ["new"]
    assert store.filter_documents({"field": "id", "operator": "==", "value": "existing"})[0].embedding is None


def test_write_documents_overwrite_policy_does_not_preflight_duplicates() -> None:
    store, client = make_store()
    store.write_documents([haystack_doc("same", [1, 0, 0])], policy=DuplicatePolicy.OVERWRITE)
    client.filter_calls.clear()

    written = store.write_documents([haystack_doc("same", [0, 1, 0])], policy="overwrite")

    assert written == 1
    assert client.filter_calls == []
    assert client.upsert_calls[-1] == ["same"]


def test_write_documents_rejects_unembedded_sparse_or_blob_documents() -> None:
    store, _ = make_store()

    with pytest.raises(DocumentStoreError, match="no embedding"):
        store.write_documents([HaystackDocument(id="missing", content="missing")], policy=DuplicatePolicy.OVERWRITE)
    with pytest.raises(DocumentStoreError, match="sparse embeddings"):
        store.write_documents(
            [
                HaystackDocument(
                    id="sparse",
                    content="sparse",
                    embedding=[1.0, 0.0, 0.0],
                    sparse_embedding=SparseEmbedding(indices=[0], values=[1.0]),
                )
            ],
            policy=DuplicatePolicy.OVERWRITE,
        )
    with pytest.raises(DocumentStoreError, match="blob"):
        store.write_documents(
            [
                HaystackDocument(
                    id="blob",
                    content="blob",
                    embedding=[1.0, 0.0, 0.0],
                    blob=ByteStream(data=b"blob"),
                )
            ],
            policy=DuplicatePolicy.OVERWRITE,
        )


def test_conversion_functions_preserve_haystack_fields_without_aliasing_meta() -> None:
    haystack = HaystackDocument(
        id="doc-1",
        content="body",
        embedding=[0.1, 0.2, 0.3],
        meta={"repo": "gomap", "nested": {"line": 7}},
        score=0.75,
    )

    treedb = haystack_document_to_treedb_document(haystack)
    assert treedb == TreeDBDocument(
        id="doc-1",
        content="body",
        embedding=[0.1, 0.2, 0.3],
        meta={"repo": "gomap", "nested": {"line": 7}},
        score=0.75,
    )
    treedb.meta["nested"]["line"] = 8
    assert haystack.meta["nested"]["line"] == 7

    back = treedb_document_to_haystack_document(treedb)
    assert back.id == "doc-1"
    assert back.content == "body"
    assert back.embedding == [0.1, 0.2, 0.3]
    assert back.meta["nested"]["line"] == 8
    assert back.score == 0.75


def test_to_dict_from_dict_round_trip_without_network() -> None:
    store = TreeDBDocumentStore(
        base_url="http://127.0.0.1:7120",
        index="serialized",
        embedding_dimension=128,
        similarity="dot_product",
        return_embedding=True,
        ensure_index=False,
        timeout=7,
    )

    data = store.to_dict()

    assert data["type"] == "haystack_integrations.document_stores.treedb.document_store.TreeDBDocumentStore"
    assert data["init_parameters"]["similarity"] == "inner_product"
    restored = TreeDBDocumentStore.from_dict(data)
    assert restored.base_url == "http://127.0.0.1:7120"
    assert restored.index == "serialized"
    assert restored.embedding_dimension == 128
    assert restored.similarity == "inner_product"
    assert restored.return_embedding is True
    assert restored.ensure_index is False


def test_invalid_service_filter_error_maps_to_haystack_filter_error() -> None:
    class InvalidFilterClient(FakeTreeDBClient):
        def count_documents(self, *args: object, **kwargs: object) -> object:
            raise TreeDBInvalidFilterError("unsupported filter operator 'contains'")

    store = TreeDBDocumentStore(
        base_url="http://fake-treedb",
        index="docs",
        embedding_dimension=3,
        ensure_index=False,
        client=InvalidFilterClient(),  # type: ignore[arg-type]
    )

    with pytest.raises(FilterError, match="unsupported filter operator"):
        store.count_documents({"field": "meta.repo", "operator": "contains", "value": "gomap"})


def test_recreate_index_and_delete_empty_filter_fail_honestly() -> None:
    with pytest.raises(DocumentStoreError, match="recreate_index"):
        TreeDBDocumentStore(base_url="http://127.0.0.1:7120", recreate_index=True)

    store, _ = make_store()
    with pytest.raises(FilterError, match="requires a non-empty filter"):
        store.delete_by_filter({})
