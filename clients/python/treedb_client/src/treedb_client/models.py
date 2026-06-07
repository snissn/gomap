"""Typed dataclasses for the TreeDB document service contract."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TypeVar


_T = TypeVar("_T")


def _as_mapping(value: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return value


def _reject_unknown(data: Mapping[str, Any], allowed: Sequence[str], label: str) -> None:
    allowed_set = set(allowed)
    unknown = sorted(str(key) for key in data.keys() if key not in allowed_set)
    if unknown:
        raise ValueError(f"{label} has unsupported field(s): {', '.join(unknown)}")


def _as_str(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    return value


def _as_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    return value


def _as_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{label} must be a boolean")
    return value


def _optional_float(value: Any, label: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number")
    return float(value)


def _optional_float_list(value: Any, label: str) -> Optional[list[float]]:
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{label} must be a sequence of numbers")
    out: list[float] = []
    for i, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise TypeError(f"{label}[{i}] must be a number")
        out.append(float(item))
    return out


def _copy_meta(value: Any, label: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return copy.deepcopy(dict(value))


@dataclass
class Document:
    """Haystack-compatible document shape used by the TreeDB service.

    The base client intentionally does not import Haystack. It mirrors the
    fields (`id`, `content`, `embedding`, `meta`, `score`) that the service
    accepts and returns. `score` is response-only and is omitted from write
    payloads unless `include_score=True` is explicitly passed to `to_dict`.
    """

    id: str
    content: str = ""
    embedding: Optional[list[float]] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    score: Optional[float] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Document":
        data = _as_mapping(data, "document")
        _reject_unknown(data, ["id", "content", "embedding", "meta", "score"], "document")
        if "id" not in data:
            raise ValueError("document.id is required")
        return cls(
            id=_as_str(data["id"], "document.id"),
            content=_as_str(data.get("content", ""), "document.content"),
            embedding=_optional_float_list(data.get("embedding"), "document.embedding"),
            meta=_copy_meta(data.get("meta"), "document.meta"),
            score=_optional_float(data.get("score"), "document.score"),
        )

    def to_dict(self, *, include_score: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {"id": self.id}
        if self.content:
            out["content"] = self.content
        if self.embedding is not None:
            out["embedding"] = [float(value) for value in self.embedding]
        if self.meta:
            out["meta"] = copy.deepcopy(self.meta)
        if include_score and self.score is not None:
            out["score"] = float(self.score)
        return out


@dataclass(frozen=True)
class IndexCapabilities:
    dense_vector_search: bool
    exact_dense_scoring: bool
    metadata_filters: bool
    keyword_search: bool
    hybrid_search: bool

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IndexCapabilities":
        data = _as_mapping(data, "index.capabilities")
        _reject_unknown(
            data,
            [
                "dense_vector_search",
                "exact_dense_scoring",
                "metadata_filters",
                "keyword_search",
                "hybrid_search",
            ],
            "index.capabilities",
        )
        return cls(
            dense_vector_search=_as_bool(data.get("dense_vector_search", False), "index.capabilities.dense_vector_search"),
            exact_dense_scoring=_as_bool(data.get("exact_dense_scoring", False), "index.capabilities.exact_dense_scoring"),
            metadata_filters=_as_bool(data.get("metadata_filters", False), "index.capabilities.metadata_filters"),
            keyword_search=_as_bool(data.get("keyword_search", False), "index.capabilities.keyword_search"),
            hybrid_search=_as_bool(data.get("hybrid_search", False), "index.capabilities.hybrid_search"),
        )

    def to_dict(self) -> Dict[str, bool]:
        return {
            "dense_vector_search": self.dense_vector_search,
            "exact_dense_scoring": self.exact_dense_scoring,
            "metadata_filters": self.metadata_filters,
            "keyword_search": self.keyword_search,
            "hybrid_search": self.hybrid_search,
        }


@dataclass(frozen=True)
class IndexInfo:
    name: str
    dimension: int
    metric: str
    generation: int
    contract_version: str
    embedding_field: str
    document_type: str
    capabilities: IndexCapabilities

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IndexInfo":
        data = _as_mapping(data, "index")
        _reject_unknown(
            data,
            [
                "name",
                "dimension",
                "metric",
                "generation",
                "contract_version",
                "embedding_field",
                "document_type",
                "capabilities",
            ],
            "index",
        )
        return cls(
            name=_as_str(data["name"], "index.name"),
            dimension=_as_int(data["dimension"], "index.dimension"),
            metric=_as_str(data["metric"], "index.metric"),
            generation=_as_int(data["generation"], "index.generation"),
            contract_version=_as_str(data["contract_version"], "index.contract_version"),
            embedding_field=_as_str(data["embedding_field"], "index.embedding_field"),
            document_type=_as_str(data["document_type"], "index.document_type"),
            capabilities=IndexCapabilities.from_dict(data.get("capabilities", {})),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dimension": self.dimension,
            "metric": self.metric,
            "generation": self.generation,
            "contract_version": self.contract_version,
            "embedding_field": self.embedding_field,
            "document_type": self.document_type,
            "capabilities": self.capabilities.to_dict(),
        }


@dataclass(frozen=True)
class UpsertDocumentsResponse:
    index: IndexInfo
    upserted: int
    inserted: int
    updated: int
    ids: list[str]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "UpsertDocumentsResponse":
        data = _as_mapping(data, "upsert response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            upserted=_as_int(data["upserted"], "upserted"),
            inserted=_as_int(data["inserted"], "inserted"),
            updated=_as_int(data["updated"], "updated"),
            ids=[_as_str(item, "ids[]") for item in data.get("ids", [])],
        )


@dataclass(frozen=True)
class DeleteDocumentsResponse:
    index: IndexInfo
    deleted: int
    ids: list[str]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DeleteDocumentsResponse":
        data = _as_mapping(data, "delete response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            deleted=_as_int(data["deleted"], "deleted"),
            ids=[_as_str(item, "ids[]") for item in data.get("ids", [])],
        )


@dataclass(frozen=True)
class CountDocumentsResponse:
    index: IndexInfo
    count: int

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CountDocumentsResponse":
        data = _as_mapping(data, "count response")
        return cls(index=IndexInfo.from_dict(data["index"]), count=_as_int(data["count"], "count"))


@dataclass(frozen=True)
class FilterDocumentsResponse:
    index: IndexInfo
    documents: list[Document]
    matched_count: int
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FilterDocumentsResponse":
        data = _as_mapping(data, "filter response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            documents=[Document.from_dict(item) for item in data.get("documents", [])],
            matched_count=_as_int(data["matched_count"], "matched_count"),
            truncated=_as_bool(data.get("truncated", False), "truncated"),
        )


@dataclass(frozen=True)
class DenseVectorSearchResponse:
    index: IndexInfo
    documents: list[Document]
    metric: str
    exact: bool
    candidates: int

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DenseVectorSearchResponse":
        data = _as_mapping(data, "vector search response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            documents=[Document.from_dict(item) for item in data.get("documents", [])],
            metric=_as_str(data["metric"], "metric"),
            exact=_as_bool(data["exact"], "exact"),
            candidates=_as_int(data["candidates"], "candidates"),
        )
