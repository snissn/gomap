"""Haystack keyword retriever for TreeDB."""

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
class TreeDBKeywordRetriever:
    """Retrieve documents from a `TreeDBDocumentStore` by TreeDB ranked keyword search."""

    def __init__(
        self,
        *,
        document_store: TreeDBDocumentStore,
        filters: Optional[dict[str, Any]] = None,
        top_k: int = 10,
        operator: Optional[str] = None,
        candidate_limit: Optional[int] = None,
        max_postings_scanned: Optional[int] = None,
        return_embedding: Optional[bool] = None,
        filter_policy: Union[str, FilterPolicy] = FilterPolicy.REPLACE,
    ) -> None:
        """Create the TreeDB keyword retriever.

        :param document_store: `TreeDBDocumentStore` instance to query.
        :param filters: Init-time filters. Runtime filters are combined according to `filter_policy`.
        :param top_k: Default maximum number of documents to return.
        :param operator: Optional TreeDB keyword operator (`or` default or `and`).
        :param candidate_limit: Optional keyword candidate guardrail.
        :param max_postings_scanned: Optional postings-scan guardrail.
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
        self.operator = operator
        self.candidate_limit = _optional_positive_int(candidate_limit, "candidate_limit")
        self.max_postings_scanned = _optional_positive_int(max_postings_scanned, "max_postings_scanned")
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
            operator=self.operator,
            candidate_limit=self.candidate_limit,
            max_postings_scanned=self.max_postings_scanned,
            return_embedding=self.return_embedding,
            filter_policy=self.filter_policy.value,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TreeDBKeywordRetriever":
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
        query: str,
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> dict[str, list[Document]]:
        """Retrieve documents matching `query` with TreeDB ranked keyword search.

        :param query: Text query to send to TreeDB keyword search.
        :param filters: Runtime TreeDB/Haystack filter AST. Keyword filters are served when they resolve to one bounded scalar allow-set over declared index scalar fields; otherwise the service fails closed with a typed error.
        :param top_k: Runtime maximum number of documents to return.
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
        docs = self.document_store._search_keyword(
            query=query,
            filters=effective_filters or None,
            top_k=effective_top_k,
            operator=self.operator,
            candidate_limit=self.candidate_limit,
            max_postings_scanned=self.max_postings_scanned,
            return_embedding=self.return_embedding,
        )
        return {"documents": docs}

    @component.output_types(documents=list[Document])
    async def run_async(
        self,
        query: str,
        filters: Optional[dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> dict[str, list[Document]]:
        """Asynchronously retrieve documents by keyword search."""

        return await asyncio.to_thread(self.run, query=query, filters=filters, top_k=top_k)


def _optional_positive_int(value: Optional[int], label: str) -> Optional[int]:
    if value is None:
        return None
    parsed = int(value)
    if parsed <= 0:
        msg = f"{label} must be positive"
        raise ValueError(msg)
    return parsed
