from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Optional

import _support  # noqa: F401
from treedb_client import (
    CountDocumentsResponse,
    DeleteDocumentsResponse,
    DenseVectorSearchResponse,
    Document,
    FilterDocumentsResponse,
    IndexCapabilities,
    IndexInfo,
    UpsertDocumentsResponse,
)


CAPABILITIES = IndexCapabilities(
    dense_vector_search=True,
    exact_dense_scoring=True,
    metadata_filters=True,
    keyword_search=False,
    hybrid_search=False,
)


def sample_index(name: str = "docs", dimension: int = 3, metric: str = "cosine") -> IndexInfo:
    return IndexInfo(
        name=name,
        dimension=dimension,
        metric=metric,
        generation=1,
        contract_version="treedb-document-service/v1alpha1",
        embedding_field="embedding",
        document_type="treedb_document_service_v1",
        capabilities=CAPABILITIES,
    )


class FakeTreeDBClient:
    def __init__(self, *, index_name: str = "docs", dimension: int = 3, metric: str = "cosine") -> None:
        self.base_url = "http://fake-treedb"
        self.index_info = sample_index(index_name, dimension, metric)
        self.documents: dict[str, Document] = {}
        self.ensure_calls: list[tuple[str, int, Optional[str]]] = []
        self.upsert_calls: list[list[str]] = []
        self.filter_calls: list[dict[str, Any]] = []
        self.count_calls: list[dict[str, Any]] = []
        self.delete_id_calls: list[list[str]] = []
        self.delete_filter_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []

    def ensure_index(self, name: str, dimension: int, metric: Optional[str] = "cosine") -> IndexInfo:
        self.ensure_calls.append((name, dimension, metric))
        return self.index_info

    def count_documents(
        self,
        index: str,
        filter: Optional[dict[str, Any]] = None,
        *,
        expected_generation: Optional[int] = None,
    ) -> CountDocumentsResponse:
        self.count_calls.append({"filter": deepcopy(filter), "expected_generation": expected_generation})
        docs = self._matching_documents(filter)
        return CountDocumentsResponse(index=self.index_info, count=len(docs))

    def filter_documents(
        self,
        index: str,
        filter: Optional[dict[str, Any]] = None,
        *,
        limit: int = 0,
        offset: int = 0,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> FilterDocumentsResponse:
        self.filter_calls.append(
            {
                "filter": deepcopy(filter),
                "limit": limit,
                "offset": offset,
                "return_embedding": return_embedding,
                "expected_generation": expected_generation,
            }
        )
        docs = self._matching_documents(filter)
        matched_count = len(docs)
        docs = docs[offset:]
        if limit:
            docs = docs[:limit]
        out = [self._copy_doc(doc, return_embedding=return_embedding) for doc in docs]
        return FilterDocumentsResponse(index=self.index_info, documents=out, matched_count=matched_count)

    def upsert_documents(
        self,
        index: str,
        documents: list[Document],
        *,
        expected_generation: Optional[int] = None,
    ) -> UpsertDocumentsResponse:
        self.upsert_calls.append([doc.id for doc in documents])
        inserted = 0
        updated = 0
        for doc in documents:
            if doc.id in self.documents:
                updated += 1
            else:
                inserted += 1
            self.documents[doc.id] = self._copy_doc(doc, return_embedding=True)
        return UpsertDocumentsResponse(
            index=self.index_info,
            upserted=len(documents),
            inserted=inserted,
            updated=updated,
            ids=[doc.id for doc in documents],
        )

    def delete_documents(
        self,
        index: str,
        ids: list[str],
        *,
        expected_generation: Optional[int] = None,
    ) -> DeleteDocumentsResponse:
        self.delete_id_calls.append(list(ids))
        deleted = 0
        for doc_id in ids:
            if doc_id in self.documents:
                deleted += 1
                del self.documents[doc_id]
        return DeleteDocumentsResponse(index=self.index_info, deleted=deleted, ids=list(ids))

    def delete_by_filter(
        self,
        index: str,
        filter: dict[str, Any],
        *,
        expected_generation: Optional[int] = None,
    ) -> DeleteDocumentsResponse:
        self.delete_filter_calls.append(deepcopy(filter))
        ids = [doc.id for doc in self._matching_documents(filter)]
        for doc_id in ids:
            del self.documents[doc_id]
        return DeleteDocumentsResponse(index=self.index_info, deleted=len(ids), ids=ids)

    def query_by_embedding(
        self,
        index: str,
        query_embedding: list[float],
        top_k: int,
        filter: Optional[dict[str, Any]] = None,
        *,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> DenseVectorSearchResponse:
        self.query_calls.append(
            {
                "query_embedding": list(query_embedding),
                "top_k": top_k,
                "filter": deepcopy(filter),
                "return_embedding": return_embedding,
                "expected_generation": expected_generation,
            }
        )
        scored: list[tuple[float, Document]] = []
        for doc in self._matching_documents(filter):
            if doc.embedding is None:
                continue
            score = _cosine(query_embedding, doc.embedding)
            scored.append((score, doc))
        scored.sort(key=lambda item: (-item[0], item[1].id))
        docs = []
        for score, doc in scored[:top_k]:
            out = self._copy_doc(doc, return_embedding=return_embedding)
            out.score = score
            docs.append(out)
        return DenseVectorSearchResponse(
            index=self.index_info,
            documents=docs,
            metric=self.index_info.metric,
            exact=True,
            candidates=len(scored),
        )

    def _matching_documents(self, filter: Optional[dict[str, Any]]) -> list[Document]:
        return [doc for doc in sorted(self.documents.values(), key=lambda item: item.id) if _matches_filter(filter, doc)]

    @staticmethod
    def _copy_doc(doc: Document, *, return_embedding: bool) -> Document:
        return Document(
            id=doc.id,
            content=doc.content,
            embedding=None if not return_embedding else (None if doc.embedding is None else list(doc.embedding)),
            meta=deepcopy(doc.meta),
            score=doc.score,
        )


def _matches_filter(filter: Optional[dict[str, Any]], doc: Document) -> bool:
    if not filter:
        return True
    op = str(filter.get("operator", "")).upper()
    if op == "AND":
        return all(_matches_filter(condition, doc) for condition in filter.get("conditions", []))
    if op == "OR":
        return any(_matches_filter(condition, doc) for condition in filter.get("conditions", []))
    if op == "NOT":
        return not _matches_filter(filter.get("conditions", [None])[0], doc)

    field = filter["field"]
    left = _field_value(doc, field)
    right = filter.get("value")
    if left is None:
        return False
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    if op == "IN":
        return left in right
    if op == "NOT IN":
        return left not in right
    if op == ">=":
        return left >= right
    if op == ">":
        return left > right
    if op == "<=":
        return left <= right
    if op == "<":
        return left < right
    raise AssertionError(f"unsupported fake filter operator {op!r}")


def _field_value(doc: Document, field: str) -> Any:
    if field == "id":
        return doc.id
    if field == "content":
        return doc.content
    if field.startswith("meta."):
        field = field[5:]
    current: Any = doc.meta
    for part in field.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _cosine(query: list[float], embedding: list[float]) -> float:
    dot = sum(float(q) * float(e) for q, e in zip(query, embedding, strict=True))
    q_norm = math.sqrt(sum(float(q) * float(q) for q in query))
    e_norm = math.sqrt(sum(float(e) * float(e) for e in embedding))
    return dot / (q_norm * e_norm)
