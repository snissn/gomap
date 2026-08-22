from __future__ import annotations

import math
import re
from copy import deepcopy
from typing import Any, Optional, Sequence

import _support  # noqa: F401
from treedb_client import (
    CountDocumentsResponse,
    DeleteDocumentsResponse,
    DenseVectorSearchResponse,
    Document,
    FilterDocumentsResponse,
    HybridFusionOptions,
    HybridSearchPlan,
    HybridSearchResponse,
    HybridSearchSnapshot,
    HybridSearchStats,
    IndexCapabilities,
    IndexInfo,
    KeywordSearchResponse,
    KeywordSearchStats,
    UpsertDocumentsResponse,
)


CAPABILITIES = IndexCapabilities(
    dense_vector_search=True,
    exact_dense_scoring=True,
    metadata_filters=True,
    keyword_search=True,
    hybrid_search=True,
    keyword_metadata_filters=False,
    hybrid_metadata_filters=False,
)


def sample_index(name: str = "docs", dimension: int = 3, metric: str = "cosine") -> IndexInfo:
    return IndexInfo(
        name=name,
        dimension=dimension,
        metric=metric,
        generation=1,
        contract_version="treedb-document-service/v1alpha2",
        embedding_field="embedding",
        vector_index_name="embedding",
        text_field="content",
        text_index_name="content",
        document_type="treedb_document_service_v1",
        capabilities=CAPABILITIES,
    )


class FakeTreeDBClient:
    def __init__(self, *, index_name: str = "docs", dimension: int = 3, metric: str = "cosine") -> None:
        self.base_url = "http://fake-treedb"
        self.index_info = sample_index(index_name, dimension, metric)
        self.ensure_calls: list[tuple[str, int, Optional[str]]] = []
        self.ensure_scalar_fields: list[Any] = []
        self.upsert_calls: list[list[str]] = []
        self.filter_calls: list[dict[str, Any]] = []
        self.count_calls: list[dict[str, Any]] = []
        self.delete_id_calls: list[list[str]] = []
        self.delete_filter_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []
        self.keyword_calls: list[dict[str, Any]] = []
        self.hybrid_calls: list[dict[str, Any]] = []

    def ensure_index(
        self,
        name: str,
        dimension: int,
        metric: Optional[str] = "cosine",
        *,
        scalar_fields: Optional[Sequence[Any]] = None,
    ) -> IndexInfo:
        self.ensure_scalar_fields.append(
            None
            if scalar_fields is None
            else [item.to_dict() if hasattr(item, "to_dict") else deepcopy(item) for item in scalar_fields]
        )
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
        route: Optional[str] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> DenseVectorSearchResponse:
        self.query_calls.append(
            {
                "query_embedding": list(query_embedding),
                "top_k": top_k,
                "filter": deepcopy(filter),
                "route": route,
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

    def search_keyword(
        self,
        index: str,
        query: str,
        top_k: int,
        *,
        operator: Optional[str] = None,
        candidate_limit: Optional[int] = None,
        max_postings_scanned: Optional[int] = None,
        filter: Optional[dict[str, Any]] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> KeywordSearchResponse:
        self.keyword_calls.append(
            {
                "query": query,
                "top_k": top_k,
                "operator": operator,
                "candidate_limit": candidate_limit,
                "max_postings_scanned": max_postings_scanned,
                "filter": deepcopy(filter),
                "return_embedding": return_embedding,
                "expected_generation": expected_generation,
            }
        )
        ranked = self._keyword_ranked(query, filter=filter, operator=operator)
        if candidate_limit is not None:
            ranked = ranked[:candidate_limit]
        docs = []
        for rank, (score, matched_terms, doc) in enumerate(ranked[:top_k], start=1):
            out = self._copy_doc(doc, return_embedding=return_embedding)
            out.score = score
            out.meta.setdefault(
                "_treedb_search",
                {
                    "type": "keyword",
                    "text_index": self.index_info.text_index_name,
                    "rank": rank,
                    "score_kind": "fake_bm25f",
                    "matched_terms": matched_terms,
                    "matched_fields": [self.index_info.text_field],
                    "text_matches": [{"field": self.index_info.text_field, "terms": matched_terms}],
                },
            )
            docs.append(out)
        return KeywordSearchResponse(
            index=self.index_info,
            documents=docs,
            text_index=self.index_info.text_index_name,
            stats=KeywordSearchStats(
                query_terms=len(_terms(query)),
                candidates_requested=candidate_limit or top_k,
                candidates_returned=len(docs),
                postings_scanned=sum(len(_terms(doc.content)) for _, _, doc in ranked),
                candidates_scored=len(ranked),
                documents_fetched=len(docs),
            ),
        )

    def search_hybrid(
        self,
        index: str,
        *,
        query: Optional[str] = None,
        query_embedding: Optional[list[float]] = None,
        top_k: int,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        fusion: Optional[HybridFusionOptions | dict[str, Any]] = None,
        filter: Optional[dict[str, Any]] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> HybridSearchResponse:
        fusion_payload = _fusion_to_dict(fusion)
        self.hybrid_calls.append(
            {
                "query": query,
                "query_embedding": None if query_embedding is None else list(query_embedding),
                "top_k": top_k,
                "candidate_limit": candidate_limit,
                "text_candidate_limit": text_candidate_limit,
                "vector_candidate_limit": vector_candidate_limit,
                "ef_search": ef_search,
                "fusion": deepcopy(fusion_payload),
                "filter": deepcopy(filter),
                "return_embedding": return_embedding,
                "expected_generation": expected_generation,
            }
        )
        text_limit = text_candidate_limit or candidate_limit or top_k
        vector_limit = vector_candidate_limit or candidate_limit or top_k
        text_ranked = [] if query is None else self._keyword_ranked(query, filter=filter)[:text_limit]
        vector_ranked = [] if query_embedding is None else self._vector_ranked(query_embedding, filter=filter)[:vector_limit]

        rrf_k = int(fusion_payload.get("rrf_k", 60))
        fusion_method = str(fusion_payload.get("method", "rrf"))
        tie_policy = str(fusion_payload.get("tie_policy", "fused_score_best_rank_source_order_id"))
        source_order = [str(item) for item in fusion_payload.get("source_order", ["text", "vector"])]
        source_preference = {source: i for i, source in enumerate(source_order)}
        fused: dict[str, dict[str, Any]] = {}

        for rank, (score, _matched_terms, doc) in enumerate(text_ranked, start=1):
            _add_hybrid_source(
                fused,
                doc,
                source="text",
                index_name=self.index_info.text_index_name,
                rank=rank,
                score=score,
                score_kind="fake_bm25f",
                fusion_score=1.0 / (rrf_k + rank),
            )
        for rank, (score, doc) in enumerate(vector_ranked, start=1):
            _add_hybrid_source(
                fused,
                doc,
                source="vector",
                index_name=self.index_info.vector_index_name,
                rank=rank,
                score=score,
                score_kind="vector_similarity",
                fusion_score=1.0 / (rrf_k + rank),
            )

        ordered = sorted(
            fused.values(),
            key=lambda item: (
                -float(item["fused_score"]),
                int(item["best_rank"]),
                min(source_preference.get(source["source"], len(source_preference)) for source in item["sources"]),
                item["doc"].id,
            ),
        )
        docs = []
        for rank, item in enumerate(ordered[:top_k], start=1):
            out = self._copy_doc(item["doc"], return_embedding=return_embedding)
            out.score = float(item["fused_score"])
            out.meta.setdefault(
                "_treedb_search",
                {
                    "type": "hybrid",
                    "rank": rank,
                    "fusion_method": fusion_method,
                    "fusion_tie_policy": tie_policy,
                    "fused_score": float(item["fused_score"]),
                    "sources": deepcopy(item["sources"]),
                },
            )
            docs.append(out)

        text_ids = {doc.id for _, _, doc in text_ranked}
        vector_ids = {doc.id for _, doc in vector_ranked}
        return HybridSearchResponse(
            index=self.index_info,
            documents=docs,
            text_index=self.index_info.text_index_name if query is not None else "",
            vector_index=self.index_info.vector_index_name if query_embedding is not None else "",
            plan=HybridSearchPlan(
                scalar_filter_strategy="none" if not filter else "service_filter",
                fusion_method=fusion_method,
                fusion_tie_policy=tie_policy,
                text_candidate_limit=0 if query is None else text_limit,
                vector_candidate_limit=0 if query_embedding is None else vector_limit,
                final_top_k=top_k,
            ),
            snapshot=HybridSearchSnapshot(consistency="fake_current_snapshot", collection_generation=self.index_info.generation),
            stats=HybridSearchStats(
                text_candidates_requested=0 if query is None else text_limit,
                text_candidates_returned=len(text_ranked),
                text_postings_scanned=sum(len(_terms(doc.content)) for _, _, doc in text_ranked),
                text_candidates_scored=len(text_ranked),
                vector_candidates_requested=0 if query_embedding is None else vector_limit,
                vector_candidates_returned=len(vector_ranked),
                vector_candidates_examined=len(vector_ranked),
                candidates_fused=len(text_ranked) + len(vector_ranked),
                candidates_after_fusion=len(ordered),
                fusion_text_only=len(text_ids - vector_ids),
                fusion_vector_only=len(vector_ids - text_ids),
                fusion_both=len(text_ids & vector_ids),
                fusion_duplicate_candidates=len(text_ids & vector_ids),
                candidates_after_filter=len(ordered),
                documents_fetched=len(docs),
            ),
        )

    def _keyword_ranked(
        self,
        query: str,
        *,
        filter: Optional[dict[str, Any]],
        operator: Optional[str] = None,
    ) -> list[tuple[float, list[str], Document]]:
        query_terms = _terms(query)
        if not query_terms:
            return []
        require_all = str(operator or "or").lower() == "and"
        ranked: list[tuple[float, list[str], Document]] = []
        for doc in self._matching_documents(filter):
            content_terms = _terms(doc.content)
            matched_terms = [term for term in query_terms if term in content_terms]
            if not matched_terms or (require_all and len(set(matched_terms)) != len(set(query_terms))):
                continue
            score = float(sum(content_terms.count(term) for term in query_terms))
            ranked.append((score, sorted(set(matched_terms)), doc))
        ranked.sort(key=lambda item: (-item[0], item[2].id))
        return ranked

    def _vector_ranked(self, query_embedding: list[float], *, filter: Optional[dict[str, Any]]) -> list[tuple[float, Document]]:
        ranked: list[tuple[float, Document]] = []
        for doc in self._matching_documents(filter):
            if doc.embedding is None:
                continue
            ranked.append((_cosine(query_embedding, doc.embedding), doc))
        ranked.sort(key=lambda item: (-item[0], item[1].id))
        return ranked

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


def _add_hybrid_source(
    fused: dict[str, dict[str, Any]],
    doc: Document,
    *,
    source: str,
    index_name: str,
    rank: int,
    score: float,
    score_kind: str,
    fusion_score: float,
) -> None:
    item = fused.setdefault(doc.id, {"doc": doc, "fused_score": 0.0, "best_rank": rank, "sources": []})
    item["fused_score"] = float(item["fused_score"]) + fusion_score
    item["best_rank"] = min(int(item["best_rank"]), rank)
    item["sources"].append(
        {
            "source": source,
            "index_name": index_name,
            "source_rank": rank,
            "score": score,
            "score_kind": score_kind,
            "fusion_score": fusion_score,
        }
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
    if q_norm == 0.0 or e_norm == 0.0:
        return 0.0
    return dot / (q_norm * e_norm)


def _terms(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def _fusion_to_dict(fusion: Optional[HybridFusionOptions | dict[str, Any]]) -> dict[str, Any]:
    if fusion is None:
        return {}
    if isinstance(fusion, HybridFusionOptions):
        return fusion.to_dict()
    return HybridFusionOptions.from_dict(fusion).to_dict()
