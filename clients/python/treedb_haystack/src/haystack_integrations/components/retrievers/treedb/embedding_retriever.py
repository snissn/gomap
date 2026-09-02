"""Haystack embedding retriever for TreeDB."""

from __future__ import annotations

import asyncio
import copy
from typing import Any, Optional, Union

from haystack import component, default_from_dict, default_to_dict
from haystack.dataclasses import Document
from haystack.document_stores.types import FilterPolicy
from haystack.document_stores.types.filter_policy import apply_filter_policy

from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


@component
class TreeDBEmbeddingRetriever:
    """Retrieve documents from a `TreeDBDocumentStore` by dense query embedding."""

    def __init__(
        self,
        *,
        document_store: TreeDBDocumentStore,
        filters: Optional[dict[str, Any]] = None,
        top_k: int = 10,
        return_embedding: Optional[bool] = None,
        filter_policy: Union[str, FilterPolicy] = FilterPolicy.REPLACE,
    ) -> None:
        """Create the TreeDB embedding retriever.

        :param document_store: `TreeDBDocumentStore` instance to query.
        :param filters: Init-time filters. Runtime filters are combined according to `filter_policy`.
        :param top_k: Default maximum number of documents to return.
        :param return_embedding: Override document-store embedding echo for retrieved documents.
        :param filter_policy: Haystack filter policy (`REPLACE` or `MERGE`).
        :raises ValueError: If `document_store` is not a `TreeDBDocumentStore`.
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
            return_embedding=self.return_embedding,
            filter_policy=self.filter_policy.value,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TreeDBEmbeddingRetriever":
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
        query_embedding: list[float],
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
        return_embedding: Optional[bool] = None,
    ) -> dict[str, list[Document]]:
        """Retrieve documents similar to `query_embedding`.

        :param query_embedding: Dense query embedding.
        :param filters: Runtime TreeDB/Haystack filter AST.
        :param top_k: Runtime maximum number of documents to return.
        :param return_embedding: Runtime embedding echo override.
        :returns: `{"documents": [...]}` with Haystack `Document` results.
        """

        effective_filters = apply_filter_policy(
            filter_policy=self.filter_policy,
            init_filters=copy.deepcopy(self.filters),
            runtime_filters=copy.deepcopy(filters),
        )
        effective_top_k = self.top_k if top_k is None else int(top_k)
        if effective_top_k <= 0:
            msg = "top_k must be positive"
            raise ValueError(msg)
        effective_return_embedding = self.return_embedding if return_embedding is None else return_embedding
        docs = self.document_store._query_by_embedding(
            query_embedding=query_embedding,
            filters=effective_filters or None,
            top_k=effective_top_k,
            return_embedding=effective_return_embedding,
        )
        return {"documents": docs}

    @component.output_types(documents=list[Document])
    async def run_async(
        self,
        query_embedding: list[float],
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
        return_embedding: Optional[bool] = None,
    ) -> dict[str, list[Document]]:
        """Asynchronously retrieve documents.

        The TreeDB client MVP is synchronous, so this runs `run()` in a worker
        thread rather than blocking the asyncio event loop.
        """

        return await asyncio.to_thread(
            self.run,
            query_embedding=query_embedding,
            filters=filters,
            top_k=top_k,
            return_embedding=return_embedding,
        )
