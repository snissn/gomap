"""Haystack hybrid retriever for TreeDB."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from typing import Any, Optional, Union

from haystack import component, default_from_dict, default_to_dict
from haystack.dataclasses import Document
from haystack.document_stores.types import FilterPolicy
from haystack.document_stores.types.filter_policy import apply_filter_policy
from treedb_client import HybridFusionOptions

from haystack_integrations.document_stores.treedb import TreeDBDocumentStore

FusionLike = Union[HybridFusionOptions, Mapping[str, Any]]


@component
class TreeDBHybridRetriever:
    """Retrieve documents from a `TreeDBDocumentStore` with TreeDB hybrid text/vector search."""

    def __init__(
        self,
        *,
        document_store: TreeDBDocumentStore,
        filters: Optional[dict[str, Any]] = None,
        top_k: int = 10,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        fusion: Optional[FusionLike] = None,
        return_embedding: Optional[bool] = None,
        filter_policy: Union[str, FilterPolicy] = FilterPolicy.REPLACE,
    ) -> None:
        """Create the TreeDB hybrid retriever.

        :param document_store: `TreeDBDocumentStore` instance to query.
        :param filters: Init-time filters. Runtime filters are combined according to `filter_policy`.
        :param top_k: Default maximum number of documents to return.
        :param candidate_limit: Shared default candidate limit for omitted source-specific limits.
        :param text_candidate_limit: Text-source candidate limit.
        :param vector_candidate_limit: Vector-source candidate limit.
        :param ef_search: Optional vector-source ef_search hint.
        :param fusion: TreeDB fusion options (`HybridFusionOptions` or mapping).
        :param return_embedding: Override document-store embedding echo for retrieved documents.
        :param filter_policy: Haystack filter policy (`REPLACE` or `MERGE`).
        :raises ValueError: If `document_store` is not a `TreeDBDocumentStore` or `top_k` is not positive.
        """

        if not isinstance(document_store, TreeDBDocumentStore):
            msg = "document_store must be an instance of TreeDBDocumentStore"
            raise ValueError(msg)
        if top_k <= 0:
            msg = "top_k must be positive"
            raise ValueError(msg)

        self.document_store = document_store
        self.filters = copy.deepcopy(filters) if filters else None
        self.top_k = int(top_k)
        self.candidate_limit = _optional_positive_int(candidate_limit, "candidate_limit")
        self.text_candidate_limit = _optional_positive_int(text_candidate_limit, "text_candidate_limit")
        self.vector_candidate_limit = _optional_positive_int(vector_candidate_limit, "vector_candidate_limit")
        self.ef_search = _optional_positive_int(ef_search, "ef_search")
        self.fusion = _fusion_to_dict(fusion)
        self.return_embedding = return_embedding
        self.filter_policy = (
            filter_policy if isinstance(filter_policy, FilterPolicy) else FilterPolicy.from_str(filter_policy)
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this retriever for Haystack pipelines."""

        return default_to_dict(
            self,
            document_store=self.document_store.to_dict(),
            filters=self.filters,
            top_k=self.top_k,
            candidate_limit=self.candidate_limit,
            text_candidate_limit=self.text_candidate_limit,
            vector_candidate_limit=self.vector_candidate_limit,
            ef_search=self.ef_search,
            fusion=copy.deepcopy(self.fusion),
            return_embedding=self.return_embedding,
            filter_policy=self.filter_policy.value,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TreeDBHybridRetriever":
        """Deserialize a retriever from `to_dict()` output."""

        payload = copy.deepcopy(data)
        init_parameters = payload["init_parameters"]
        init_parameters["document_store"] = TreeDBDocumentStore.from_dict(init_parameters["document_store"])
        if filter_policy := init_parameters.get("filter_policy"):
            init_parameters["filter_policy"] = (
                filter_policy if isinstance(filter_policy, FilterPolicy) else FilterPolicy.from_str(filter_policy)
            )
        return default_from_dict(cls, payload)

    @component.output_types(documents=list[Document])
    def run(
        self,
        query: Optional[str] = None,
        query_embedding: Optional[list[float]] = None,
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        fusion: Optional[FusionLike] = None,
        return_embedding: Optional[bool] = None,
    ) -> dict[str, list[Document]]:
        """Retrieve documents with TreeDB hybrid text/vector search.

        :param query: Optional text query for TreeDB text/keyword source.
        :param query_embedding: Optional dense query embedding for TreeDB vector source.
        :param filters: Runtime TreeDB/Haystack filter AST. Hybrid prefilters accept bounded equality/range leaves joined only by AND over declared scalar fields; unsupported shapes fail closed with a typed error.
        :param top_k: Runtime maximum number of documents to return.
        :param candidate_limit: Runtime shared candidate limit override.
        :param text_candidate_limit: Runtime text-source candidate limit override.
        :param vector_candidate_limit: Runtime vector-source candidate limit override.
        :param ef_search: Runtime vector-source ef_search override.
        :param fusion: Runtime TreeDB fusion options override.
        :param return_embedding: Runtime embedding echo override.
        :returns: `{"documents": [...]}` with Haystack `Document` results.
        """

        if not query and query_embedding is None:
            msg = "query or query_embedding must be provided"
            raise ValueError(msg)
        effective_filters = apply_filter_policy(
            filter_policy=self.filter_policy,
            init_filters=copy.deepcopy(self.filters),
            runtime_filters=copy.deepcopy(filters),
        )
        effective_top_k = self.top_k if top_k is None else int(top_k)
        if effective_top_k <= 0:
            msg = "top_k must be positive"
            raise ValueError(msg)
        docs = self.document_store._search_hybrid(
            query=query,
            query_embedding=query_embedding,
            filters=effective_filters or None,
            top_k=effective_top_k,
            candidate_limit=self.candidate_limit if candidate_limit is None else _optional_positive_int(candidate_limit, "candidate_limit"),
            text_candidate_limit=(
                self.text_candidate_limit
                if text_candidate_limit is None
                else _optional_positive_int(text_candidate_limit, "text_candidate_limit")
            ),
            vector_candidate_limit=(
                self.vector_candidate_limit
                if vector_candidate_limit is None
                else _optional_positive_int(vector_candidate_limit, "vector_candidate_limit")
            ),
            ef_search=self.ef_search if ef_search is None else _optional_positive_int(ef_search, "ef_search"),
            fusion=copy.deepcopy(self.fusion) if fusion is None else _fusion_to_dict(fusion),
            return_embedding=self.return_embedding if return_embedding is None else return_embedding,
        )
        return {"documents": docs}

    @component.output_types(documents=list[Document])
    async def run_async(
        self,
        query: Optional[str] = None,
        query_embedding: Optional[list[float]] = None,
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        fusion: Optional[FusionLike] = None,
        return_embedding: Optional[bool] = None,
    ) -> dict[str, list[Document]]:
        """Asynchronously retrieve documents by hybrid search."""

        return await asyncio.to_thread(
            self.run,
            query=query,
            query_embedding=query_embedding,
            filters=filters,
            top_k=top_k,
            candidate_limit=candidate_limit,
            text_candidate_limit=text_candidate_limit,
            vector_candidate_limit=vector_candidate_limit,
            ef_search=ef_search,
            fusion=fusion,
            return_embedding=return_embedding,
        )


def _optional_positive_int(value: Optional[int], label: str) -> Optional[int]:
    if value is None:
        return None
    parsed = int(value)
    if parsed <= 0:
        msg = f"{label} must be positive"
        raise ValueError(msg)
    return parsed


def _fusion_to_dict(fusion: Optional[FusionLike]) -> Optional[dict[str, Any]]:
    if fusion is None:
        return None
    if isinstance(fusion, HybridFusionOptions):
        return fusion.to_dict()
    if isinstance(fusion, Mapping):
        return HybridFusionOptions.from_dict(fusion).to_dict()
    msg = "fusion must be HybridFusionOptions or a mapping"
    raise TypeError(msg)
