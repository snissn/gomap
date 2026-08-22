"""Haystack DocumentStore backed by TreeDB's document service."""

from __future__ import annotations

import copy
import math
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Optional, TypeVar, Union

from haystack import default_from_dict, default_to_dict
from haystack.dataclasses import Document as HaystackDocument
from haystack.document_stores.errors import DocumentStoreError, DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.errors import FilterError
from treedb_client import (
    Document as TreeDBDocument,
    IndexInfo,
    ScalarFieldDeclaration,
    TreeDBClient,
    TreeDBClientError,
    normalize_filter,
)

FilterLike = Mapping[str, Any]
_T = TypeVar("_T")


class TreeDBDocumentStore:
    """A Haystack DocumentStore backed by the TreeDB document service.

    The store delegates filtering, deletion, dense-vector search, keyword search,
    and hybrid search to TreeDB's HTTP service through `treedb-client`.
    Unsupported filters and search modes are not emulated with client-side
    document scans.
    """

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        index: str = "docs",
        embedding_dimension: int = 768,
        similarity: str = "cosine",
        scalar_fields: Optional[Sequence[ScalarFieldDeclaration | Mapping[str, Any]]] = None,
        ensure_index: bool = True,
        recreate_index: bool = False,
        timeout: Optional[float] = 30.0,
        client: Optional[TreeDBClient] = None,
    ) -> None:
        """Create a TreeDB-backed Haystack document store.

        :param base_url: Base URL for the TreeDB document service. Required unless `client` is supplied.
        :param index: TreeDB document-service index name.
        :param embedding_dimension: Dense embedding dimension for index creation/validation.
        :param similarity: TreeDB vector metric: `cosine`, `l2`, or `inner_product` (`dot_product` alias).
        :param scalar_fields: Declaration-time metadata scalar indexes to create with the service index.
        :param return_embedding: Whether filter/search responses should include stored embeddings by default.
        :param ensure_index: Create/open a compatible service index during construction. If false, operations are lazy.
        :param recreate_index: Unsupported by the current TreeDB service because there is no safe drop/recreate route.
        :param timeout: HTTP timeout in seconds for the default `TreeDBClient`.
        :param client: Optional preconfigured TreeDB client, useful for tests and custom transports.
        :raises DocumentStoreError: If `recreate_index` is requested or index setup fails.
        :raises ValueError: If `base_url` is missing and no client is supplied.
        """

        if recreate_index:
            msg = "recreate_index is not supported by the TreeDB document service MVP"
            raise DocumentStoreError(msg)
        if isinstance(embedding_dimension, bool) or not isinstance(embedding_dimension, int) or embedding_dimension <= 0:
            msg = "embedding_dimension must be a positive integer"
            raise ValueError(msg)
        if client is None and not base_url:
            msg = "base_url is required when client is not supplied"
            raise ValueError(msg)

        self.index = index
        self.embedding_dimension = embedding_dimension
        self.scalar_fields = _normalize_scalar_fields(scalar_fields)
        self.similarity = _normalize_similarity(similarity)
        self.return_embedding = bool(return_embedding)
        self.ensure_index = bool(ensure_index)
        self.recreate_index = False
        self.timeout = timeout
        self.client = client if client is not None else TreeDBClient(str(base_url), timeout=timeout)
        self.base_url = str(base_url or getattr(self.client, "base_url", ""))
        self.index_info: Optional[IndexInfo] = None
        self._write_lock = threading.Lock()

        if self.ensure_index:
            self.index_info = self._client_call(
                "ensure index",
                lambda: self.client.ensure_index(
                    self.index,
                    self.embedding_dimension,
                    self.similarity,
                    scalar_fields=self.scalar_fields,
                ),
            )
            self._validate_index_info(self.index_info)

    def count_documents(self, filters: Optional[FilterLike] = None) -> int:
        """Return the number of documents matching `filters`, or all documents."""

        prepared_filter = _prepare_filter(filters)
        response = self._client_call(
            "count documents",
            lambda: self.client.count_documents(
                self.index,
                prepared_filter,
                expected_generation=self._expected_generation(),
            ),
        )
        return response.count

    def filter_documents(self, filters: Optional[FilterLike] = None) -> list[HaystackDocument]:
        """Return documents matching `filters` using TreeDB service-side filtering."""

        prepared_filter = _prepare_filter(filters)
        response = self._client_call(
            "filter documents",
            lambda: self.client.filter_documents(
                self.index,
                prepared_filter,
                return_embedding=self.return_embedding,
                expected_generation=self._expected_generation(),
            ),
        )
        return [treedb_document_to_haystack_document(doc) for doc in response.documents]

    def write_documents(
        self,
        documents: Iterable[HaystackDocument],
        policy: Union[DuplicatePolicy, str] = DuplicatePolicy.OVERWRITE,
    ) -> int:
        """Write embedded Haystack documents to TreeDB.

        `DuplicatePolicy.OVERWRITE` maps directly to TreeDB's atomic upsert endpoint.
        `DuplicatePolicy.FAIL` and `DuplicatePolicy.SKIP` first ask the service
        for existing IDs with an `id in [...]` filter; they do not scan all
        documents client-side. TreeDB's MVP service has no create-only write,
        so these two policies are best-effort under separate concurrent clients;
        if a race is detected via an unexpected update response, the store raises
        `DocumentStoreError` instead of silently reporting success.
        """

        docs = _validate_haystack_documents(documents)
        policy = _normalize_duplicate_policy(policy)
        if not docs:
            return 0

        with self._write_lock:
            input_count = len(docs)
            docs = _deduplicate_input_documents(docs, policy)
            if policy in {DuplicatePolicy.FAIL, DuplicatePolicy.SKIP}:
                existing_ids = self._existing_document_ids([doc.id for doc in docs])
                if existing_ids and policy == DuplicatePolicy.FAIL:
                    ids = ", ".join(sorted(existing_ids))
                    msg = f"Document(s) with id(s) already exist in TreeDB index {self.index!r}: {ids}"
                    raise DuplicateDocumentError(msg)
                if existing_ids and policy == DuplicatePolicy.SKIP:
                    docs = [doc for doc in docs if doc.id not in existing_ids]
                    if not docs:
                        return 0

            treedb_docs = [haystack_document_to_treedb_document(doc) for doc in docs]
            response = self._client_call(
                "upsert documents",
                lambda: self.client.upsert_documents(
                    self.index,
                    treedb_docs,
                    expected_generation=self._expected_generation(),
                ),
            )
            if policy in {DuplicatePolicy.FAIL, DuplicatePolicy.SKIP} and response.updated:
                msg = (
                    f"TreeDB duplicate policy {policy.value!r} detected a concurrent update after preflight; "
                    "the MVP service does not provide atomic create-if-absent writes"
                )
                raise DocumentStoreError(msg)
            if policy == DuplicatePolicy.OVERWRITE:
                return input_count
            return response.upserted

    def delete_documents(self, document_ids: Sequence[str]) -> None:
        """Delete explicit document IDs through the TreeDB service."""

        ids = _validate_document_ids(document_ids)
        if not ids:
            return
        self._client_call(
            "delete documents",
            lambda: self.client.delete_documents(
                self.index,
                ids,
                expected_generation=self._expected_generation(),
            ),
        )

    def delete_by_filter(self, filters: FilterLike) -> int:
        """Delete documents matching a TreeDB/Haystack filter and return the delete count."""

        if not filters:
            msg = "delete_by_filter requires a non-empty filter; delete-all is not supported by this MVP"
            raise FilterError(msg)
        prepared_filter = _prepare_filter(filters)
        if prepared_filter is None:
            msg = "delete_by_filter requires a non-empty filter; delete-all is not supported by this MVP"
            raise FilterError(msg)
        response = self._client_call(
            "delete documents by filter",
            lambda: self.client.delete_by_filter(
                self.index,
                prepared_filter,
                expected_generation=self._expected_generation(),
            ),
        )
        return response.deleted

    def count_documents_by_filter(self, filters: FilterLike) -> int:
        """Return the number of documents matching `filters`."""

        return self.count_documents(filters=filters)

    def _query_by_embedding(
        self,
        *,
        query_embedding: Sequence[float],
        filters: Optional[FilterLike] = None,
        top_k: int = 10,
        return_embedding: Optional[bool] = None,
    ) -> list[HaystackDocument]:
        """Run dense-vector search through the TreeDB service."""

        prepared_filter = _prepare_filter(filters)
        response = self._client_call(
            "query by embedding",
            lambda: self.client.query_by_embedding(
                self.index,
                query_embedding=query_embedding,
                top_k=top_k,
                filter=prepared_filter,
                return_embedding=self.return_embedding if return_embedding is None else bool(return_embedding),
                expected_generation=self._expected_generation(),
            ),
        )
        return [treedb_document_to_haystack_document(doc) for doc in response.documents]

    def _search_keyword(
        self,
        *,
        query: str,
        filters: Optional[FilterLike] = None,
        top_k: int = 10,
        operator: Optional[str] = None,
        candidate_limit: Optional[int] = None,
        max_postings_scanned: Optional[int] = None,
        return_embedding: Optional[bool] = None,
    ) -> list[HaystackDocument]:
        """Run TreeDB ranked keyword search through the service."""

        prepared_filter = _prepare_filter(filters)
        response = self._client_call(
            "keyword search",
            lambda: self.client.search_keyword(
                self.index,
                query,
                top_k,
                operator=operator,
                candidate_limit=candidate_limit,
                max_postings_scanned=max_postings_scanned,
                filter=prepared_filter,
                return_embedding=self.return_embedding if return_embedding is None else bool(return_embedding),
                expected_generation=self._expected_generation(),
            ),
        )
        return [treedb_document_to_haystack_document(doc) for doc in response.documents]

    def _search_hybrid(
        self,
        *,
        query: Optional[str] = None,
        query_embedding: Optional[Sequence[float]] = None,
        filters: Optional[FilterLike] = None,
        top_k: int = 10,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        fusion: Optional[Any] = None,
        return_embedding: Optional[bool] = None,
    ) -> list[HaystackDocument]:
        """Run TreeDB hybrid text/vector search through the service."""

        prepared_filter = _prepare_filter(filters)
        response = self._client_call(
            "hybrid search",
            lambda: self.client.search_hybrid(
                self.index,
                query=query,
                query_embedding=query_embedding,
                top_k=top_k,
                candidate_limit=candidate_limit,
                text_candidate_limit=text_candidate_limit,
                vector_candidate_limit=vector_candidate_limit,
                ef_search=ef_search,
                fusion=fusion,
                filter=prepared_filter,
                return_embedding=self.return_embedding if return_embedding is None else bool(return_embedding),
                expected_generation=self._expected_generation(),
            ),
        )
        return [treedb_document_to_haystack_document(doc) for doc in response.documents]

    def to_dict(self) -> dict[str, Any]:
        """Serialize this document store for Haystack pipelines."""

        return default_to_dict(
            self,
            base_url=self.base_url,
            index=self.index,
            embedding_dimension=self.embedding_dimension,
            scalar_fields=(
                None
                if self.scalar_fields is None
                else [declaration.to_dict() for declaration in self.scalar_fields]
            ),
            similarity=self.similarity,
            return_embedding=self.return_embedding,
            ensure_index=self.ensure_index,
            recreate_index=False,
            timeout=self.timeout,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TreeDBDocumentStore":
        """Deserialize a document store from `to_dict()` output."""

        return default_from_dict(cls, data)

    def _existing_document_ids(self, ids: Sequence[str]) -> set[str]:
        unique_ids = list(OrderedDict((doc_id, None) for doc_id in ids).keys())
        if not unique_ids:
            return set()
        response = self._client_call(
            "check duplicate IDs",
            lambda: self.client.filter_documents(
                self.index,
                {"field": "id", "operator": "in", "value": unique_ids},
                limit=len(unique_ids),
                return_embedding=False,
                expected_generation=self._expected_generation(),
            ),
        )
        return {doc.id for doc in response.documents}

    def _expected_generation(self) -> Optional[int]:
        if self.index_info is None:
            return None
        return self.index_info.generation

    def _validate_index_info(self, info: IndexInfo) -> None:
        if info.dimension != self.embedding_dimension:
            msg = (
                f"TreeDB index {self.index!r} has dimension {info.dimension}, "
                f"expected {self.embedding_dimension}"
            )
            raise DocumentStoreError(msg)
        if info.metric != self.similarity:
            msg = f"TreeDB index {self.index!r} uses metric {info.metric!r}, expected {self.similarity!r}"
            raise DocumentStoreError(msg)
        if not info.capabilities.dense_vector_search:
            msg = f"TreeDB index {self.index!r} does not advertise dense vector search"
            raise DocumentStoreError(msg)
        if not info.capabilities.metadata_filters:
            msg = f"TreeDB index {self.index!r} does not advertise metadata filters"
            raise DocumentStoreError(msg)

    @staticmethod
    def _client_call(label: str, fn: Callable[[], _T]) -> _T:
        try:
            return fn()
        except TreeDBInvalidFilterError as exc:
            raise FilterError(str(exc)) from exc
        except TreeDBInvalidRequestError as exc:
            if _looks_like_filter_error(exc):
                raise FilterError(str(exc)) from exc
            msg = f"TreeDB {label} failed: {exc}"
            raise DocumentStoreError(msg) from exc
        except TreeDBClientError as exc:
            msg = f"TreeDB {label} failed: {exc}"
            raise DocumentStoreError(msg) from exc


def haystack_document_to_treedb_document(document: HaystackDocument) -> TreeDBDocument:
    """Convert a Haystack `Document` into the base TreeDB client document model."""

    if document.embedding is None:
        msg = f"Document {document.id!r} has no embedding; TreeDBDocumentStore requires embedded documents"
        raise DocumentStoreError(msg)
    if getattr(document, "sparse_embedding", None) is not None:
        msg = "TreeDBDocumentStore MVP does not support sparse embeddings"
        raise DocumentStoreError(msg)
    if getattr(document, "blob", None) is not None:
        msg = "TreeDBDocumentStore MVP does not support blob-only Haystack documents"
        raise DocumentStoreError(msg)
    return TreeDBDocument(
        id=document.id,
        content=document.content or "",
        embedding=[float(value) for value in document.embedding],
        meta=copy.deepcopy(document.meta),
        score=document.score,
    )


def treedb_document_to_haystack_document(document: TreeDBDocument) -> HaystackDocument:
    """Convert a base TreeDB client document into a Haystack `Document`."""

    return HaystackDocument(
        id=document.id,
        content=document.content,
        embedding=None if document.embedding is None else [float(value) for value in document.embedding],
        meta=copy.deepcopy(document.meta),
        score=document.score,
    )


def _validate_haystack_documents(documents: Iterable[HaystackDocument]) -> list[HaystackDocument]:
    if isinstance(documents, (str, bytes, bytearray)) or not isinstance(documents, Iterable):
        msg = "param 'documents' must contain an iterable of Haystack Document objects"
        raise ValueError(msg)
    docs = list(documents)
    if any(not isinstance(doc, HaystackDocument) for doc in docs):
        msg = "param 'documents' must contain an iterable of Haystack Document objects"
        raise ValueError(msg)
    return docs


def _validate_document_ids(document_ids: Sequence[str]) -> list[str]:
    if isinstance(document_ids, (str, bytes, bytearray)):
        msg = "document_ids must be a sequence of strings, not a single string"
        raise ValueError(msg)
    ids = list(document_ids)
    if any(not isinstance(doc_id, str) for doc_id in ids):
        msg = "document_ids must be a sequence of strings"
        raise ValueError(msg)
    return ids


def _normalize_duplicate_policy(policy: Union[DuplicatePolicy, str]) -> DuplicatePolicy:
    if isinstance(policy, DuplicatePolicy):
        return DuplicatePolicy.OVERWRITE if policy == DuplicatePolicy.NONE else policy
    if isinstance(policy, str):
        lowered = policy.strip().lower()
        for candidate in DuplicatePolicy:
            if candidate.value == lowered:
                return DuplicatePolicy.OVERWRITE if candidate == DuplicatePolicy.NONE else candidate
    msg = f"unsupported duplicate policy {policy!r}"
    raise ValueError(msg)


def _deduplicate_input_documents(
    documents: list[HaystackDocument],
    policy: DuplicatePolicy,
) -> list[HaystackDocument]:
    seen: set[str] = set()
    if policy == DuplicatePolicy.FAIL:
        duplicates: set[str] = set()
        for doc in documents:
            if doc.id in seen:
                duplicates.add(doc.id)
            seen.add(doc.id)
        if duplicates:
            ids = ", ".join(sorted(duplicates))
            msg = f"Duplicate document id(s) in write batch: {ids}"
            raise DuplicateDocumentError(msg)
        return documents

    if policy == DuplicatePolicy.SKIP:
        out: list[HaystackDocument] = []
        for doc in documents:
            if doc.id in seen:
                continue
            seen.add(doc.id)
            out.append(doc)
        return out

    by_id: "OrderedDict[str, HaystackDocument]" = OrderedDict()
    for doc in documents:
        if doc.id in by_id:
            del by_id[doc.id]
        by_id[doc.id] = doc
    return list(by_id.values())


def _prepare_filter(filters: Optional[FilterLike]) -> Optional[dict[str, Any]]:
    if isinstance(filters, Mapping) and len(filters) == 0:
        return None
    try:
        normalized = normalize_filter(filters)
    except TreeDBInvalidFilterError as exc:
        raise FilterError(str(exc)) from exc
    if normalized is not None:
        _validate_filter_semantics(normalized, "filter")
    return normalized


def _validate_filter_semantics(filter_node: Mapping[str, Any], path: str) -> None:
    operator = str(filter_node.get("operator", "")).upper()
    if operator in {"AND", "OR", "NOT"}:
        conditions = filter_node.get("conditions", [])
        for i, condition in enumerate(conditions):
            if isinstance(condition, Mapping):
                _validate_filter_semantics(condition, f"{path}.conditions[{i}]")
        return
    if operator in {">", ">=", "<", "<="} and not _is_filter_comparison_value(filter_node.get("value")):
        msg = f"{path}: operator {operator!r} requires a numeric or string value"
        raise FilterError(msg)


def _is_filter_comparison_value(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    return isinstance(value, str)


def _looks_like_filter_error(exc: TreeDBInvalidRequestError) -> bool:
    message = str(exc).lower()
    return any(token in message for token in ("filter", "operator", "conditions", "field is required"))


def _normalize_scalar_fields(
    fields: Optional[Sequence[ScalarFieldDeclaration | Mapping[str, Any]]],
) -> Optional[list[ScalarFieldDeclaration]]:
    if fields is None:
        return None
    if isinstance(fields, (str, bytes, bytearray)):
        raise ValueError("scalar_fields must be a sequence of declarations")
    try:
        declarations = list(fields)
    except TypeError as exc:
        raise ValueError("scalar_fields must be a sequence of declarations") from exc
    out: list[ScalarFieldDeclaration] = []
    for i, declaration in enumerate(declarations):
        try:
            model = (
                declaration
                if isinstance(declaration, ScalarFieldDeclaration)
                else ScalarFieldDeclaration.from_dict(declaration)
            )
            model.to_dict()
        except (TypeError, ValueError) as exc:
            raise ValueError(f"scalar_fields[{i}] is invalid: {exc}") from exc
        out.append(model)
    return out


def _normalize_similarity(similarity: str) -> str:
    normalized = similarity.strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "cosine": "cosine",
        "l2": "l2",
        "euclidean": "l2",
        "inner_product": "inner_product",
        "innerproduct": "inner_product",
        "dot_product": "inner_product",
        "dotproduct": "inner_product",
    }
    if normalized in aliases:
        return aliases[normalized]
    msg = "similarity must be one of 'cosine', 'l2', or 'inner_product'"
    raise ValueError(msg)
