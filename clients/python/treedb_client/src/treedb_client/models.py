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


def _copy_extra(data: Mapping[str, Any], allowed: Sequence[str]) -> Dict[str, Any]:
    allowed_set = set(allowed)
    return copy.deepcopy({str(key): value for key, value in data.items() if key not in allowed_set})


def _merge_extra(out: Dict[str, Any], extra: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in extra.items():
        if key not in out:
            out[key] = copy.deepcopy(value)
    return out


def _as_optional_str_default(value: Any, label: str) -> str:
    if value is None:
        return ""
    return _as_str(value, label)


def _as_optional_int_default(value: Any, label: str) -> int:
    if value is None:
        return 0
    return _as_int(value, label)


def _as_optional_bool_default(value: Any, label: str) -> bool:
    if value is None:
        return False
    return _as_bool(value, label)


def _float_list(value: Any, label: str) -> list[float]:
    parsed = _optional_float_list(value, label)
    if parsed is None:
        return []
    return parsed


def _filter_to_dict(filter_value: Any) -> Optional[Dict[str, Any]]:
    if filter_value is None:
        return None
    from .filters import normalize_filter

    return normalize_filter(filter_value)


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
    keyword_metadata_filters: bool = False
    hybrid_metadata_filters: bool = False
    benchmark_lifecycle: bool = False
    vector_index_maintenance: bool = False
    no_document_vector_search: bool = False
    column_graph_vector_search: bool = False
    exact_column_graph_search: bool = False
    quantized_vector_search: bool = False
    quantized_rerank: bool = False
    scalar_u8_quantized_rerank: bool = False
    rabitq_1bit_experimental: bool = False
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IndexCapabilities":
        data = _as_mapping(data, "index.capabilities")
        allowed = [
            "dense_vector_search",
            "exact_dense_scoring",
            "metadata_filters",
            "keyword_search",
            "hybrid_search",
            "keyword_metadata_filters",
            "hybrid_metadata_filters",
            "benchmark_lifecycle",
            "vector_index_maintenance",
            "no_document_vector_search",
            "column_graph_vector_search",
            "exact_column_graph_search",
            "quantized_vector_search",
            "quantized_rerank",
            "scalar_u8_quantized_rerank",
            "rabitq_1bit_experimental",
        ]
        return cls(
            dense_vector_search=_as_bool(data.get("dense_vector_search", False), "index.capabilities.dense_vector_search"),
            exact_dense_scoring=_as_bool(data.get("exact_dense_scoring", False), "index.capabilities.exact_dense_scoring"),
            metadata_filters=_as_bool(data.get("metadata_filters", False), "index.capabilities.metadata_filters"),
            keyword_search=_as_bool(data.get("keyword_search", False), "index.capabilities.keyword_search"),
            hybrid_search=_as_bool(data.get("hybrid_search", False), "index.capabilities.hybrid_search"),
            keyword_metadata_filters=_as_bool(
                data.get("keyword_metadata_filters", False), "index.capabilities.keyword_metadata_filters"
            ),
            hybrid_metadata_filters=_as_bool(
                data.get("hybrid_metadata_filters", False), "index.capabilities.hybrid_metadata_filters"
            ),
            benchmark_lifecycle=_as_bool(data.get("benchmark_lifecycle", False), "index.capabilities.benchmark_lifecycle"),
            vector_index_maintenance=_as_bool(
                data.get("vector_index_maintenance", False), "index.capabilities.vector_index_maintenance"
            ),
            no_document_vector_search=_as_bool(
                data.get("no_document_vector_search", False), "index.capabilities.no_document_vector_search"
            ),
            column_graph_vector_search=_as_bool(
                data.get("column_graph_vector_search", False), "index.capabilities.column_graph_vector_search"
            ),
            exact_column_graph_search=_as_bool(
                data.get("exact_column_graph_search", False), "index.capabilities.exact_column_graph_search"
            ),
            quantized_vector_search=_as_bool(
                data.get("quantized_vector_search", False), "index.capabilities.quantized_vector_search"
            ),
            quantized_rerank=_as_bool(data.get("quantized_rerank", False), "index.capabilities.quantized_rerank"),
            scalar_u8_quantized_rerank=_as_bool(
                data.get("scalar_u8_quantized_rerank", False), "index.capabilities.scalar_u8_quantized_rerank"
            ),
            rabitq_1bit_experimental=_as_bool(
                data.get("rabitq_1bit_experimental", False), "index.capabilities.rabitq_1bit_experimental"
            ),
            extra=_copy_extra(data, allowed),
        )

    def to_dict(self) -> Dict[str, Any]:
        return _merge_extra(
            {
                "dense_vector_search": self.dense_vector_search,
                "exact_dense_scoring": self.exact_dense_scoring,
                "metadata_filters": self.metadata_filters,
                "keyword_search": self.keyword_search,
                "hybrid_search": self.hybrid_search,
                "keyword_metadata_filters": self.keyword_metadata_filters,
                "hybrid_metadata_filters": self.hybrid_metadata_filters,
                "benchmark_lifecycle": self.benchmark_lifecycle,
                "vector_index_maintenance": self.vector_index_maintenance,
                "no_document_vector_search": self.no_document_vector_search,
                "column_graph_vector_search": self.column_graph_vector_search,
                "exact_column_graph_search": self.exact_column_graph_search,
                "quantized_vector_search": self.quantized_vector_search,
                "quantized_rerank": self.quantized_rerank,
                "scalar_u8_quantized_rerank": self.scalar_u8_quantized_rerank,
                "rabitq_1bit_experimental": self.rabitq_1bit_experimental,
            },
            self.extra,
        )


@dataclass(frozen=True)
class ScalarU8AlphaPolicy:
    name: str = ""
    quantile_ppm: int = 0

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ScalarU8AlphaPolicy":
        if data is None:
            return cls()
        data = _as_mapping(data, "scalar_u8 alpha policy")
        _reject_unknown(data, ["name", "quantile_ppm"], "scalar_u8 alpha policy")
        return cls(
            name=_as_optional_str_default(data.get("name"), "scalar_u8 alpha policy.name"),
            quantile_ppm=_as_optional_int_default(data.get("quantile_ppm"), "scalar_u8 alpha policy.quantile_ppm"),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.name:
            out["name"] = self.name
        if self.quantile_ppm:
            out["quantile_ppm"] = self.quantile_ppm
        return out


@dataclass(frozen=True)
class ScalarU8CalibrationConfig:
    mode: str = ""
    grouping: str = ""
    alpha_policy: ScalarU8AlphaPolicy | Mapping[str, Any] = field(default_factory=ScalarU8AlphaPolicy)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ScalarU8CalibrationConfig":
        if data is None:
            return cls()
        data = _as_mapping(data, "scalar_u8 calibration")
        _reject_unknown(data, ["mode", "grouping", "alpha_policy"], "scalar_u8 calibration")
        return cls(
            mode=_as_optional_str_default(data.get("mode"), "scalar_u8 calibration.mode"),
            grouping=_as_optional_str_default(data.get("grouping"), "scalar_u8 calibration.grouping"),
            alpha_policy=ScalarU8AlphaPolicy.from_dict(data.get("alpha_policy")),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.mode:
            out["mode"] = self.mode
        if self.grouping:
            out["grouping"] = self.grouping
        policy = (
            self.alpha_policy
            if isinstance(self.alpha_policy, ScalarU8AlphaPolicy)
            else ScalarU8AlphaPolicy.from_dict(self.alpha_policy)
        )
        policy_dict = policy.to_dict()
        if policy_dict:
            out["alpha_policy"] = policy_dict
        return out


@dataclass(frozen=True)
class QuantizedIndexInfo:
    name: str
    codec: str = "scalar_u8"
    version: int = 1
    scalar_u8_calibration: Optional[ScalarU8CalibrationConfig | Mapping[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QuantizedIndexInfo":
        data = _as_mapping(data, "quantized index")
        _reject_unknown(data, ["name", "codec", "version", "scalar_u8_calibration"], "quantized index")
        raw_calibration = data.get("scalar_u8_calibration")
        return cls(
            name=_as_str(data["name"], "quantized index.name"),
            codec=_as_optional_str_default(data.get("codec"), "quantized index.codec") or "scalar_u8",
            version=_as_optional_int_default(data.get("version"), "quantized index.version") or 1,
            scalar_u8_calibration=(
                None if raw_calibration is None else ScalarU8CalibrationConfig.from_dict(raw_calibration)
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"name": self.name, "codec": self.codec, "version": self.version}
        if self.scalar_u8_calibration is not None:
            calibration = (
                self.scalar_u8_calibration
                if isinstance(self.scalar_u8_calibration, ScalarU8CalibrationConfig)
                else ScalarU8CalibrationConfig.from_dict(self.scalar_u8_calibration)
            )
            out["scalar_u8_calibration"] = calibration.to_dict()
        return out


@dataclass(frozen=True)
class BenchmarkVectorIndexOptions:
    strategy: str = ""
    m: Optional[int] = None
    ef_construction: Optional[int] = None
    ef_search: Optional[int] = None
    quantized_indexes: Sequence[QuantizedIndexInfo | Mapping[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkVectorIndexOptions":
        data = _as_mapping(data, "vector index options")
        _reject_unknown(data, ["strategy", "m", "ef_construction", "ef_search", "quantized_indexes"], "vector index options")
        raw_quantized = data.get("quantized_indexes", [])
        if isinstance(raw_quantized, (str, bytes, bytearray)) or not isinstance(raw_quantized, Sequence):
            raise TypeError("vector index options.quantized_indexes must be a sequence")
        return cls(
            strategy=_as_optional_str_default(data.get("strategy"), "vector index options.strategy"),
            m=None if "m" not in data or data.get("m") is None else _as_int(data.get("m"), "vector index options.m"),
            ef_construction=None
            if "ef_construction" not in data or data.get("ef_construction") is None
            else _as_int(data.get("ef_construction"), "vector index options.ef_construction"),
            ef_search=None
            if "ef_search" not in data or data.get("ef_search") is None
            else _as_int(data.get("ef_search"), "vector index options.ef_search"),
            quantized_indexes=[QuantizedIndexInfo.from_dict(item) for item in raw_quantized],
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.strategy:
            out["strategy"] = self.strategy
        if self.m is not None:
            out["m"] = _as_int(self.m, "vector index options.m")
        if self.ef_construction is not None:
            out["ef_construction"] = _as_int(self.ef_construction, "vector index options.ef_construction")
        if self.ef_search is not None:
            out["ef_search"] = _as_int(self.ef_search, "vector index options.ef_search")
        if self.quantized_indexes:
            out["quantized_indexes"] = [
                item.to_dict() if isinstance(item, QuantizedIndexInfo) else QuantizedIndexInfo.from_dict(item).to_dict()
                for item in self.quantized_indexes
            ]
        return out


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
    vector_index_name: str = ""
    vector_strategy: str = ""
    vector_m: int = 0
    vector_ef_construction: int = 0
    vector_ef_search: int = 0
    quantized_indexes: list[QuantizedIndexInfo] = field(default_factory=list)
    text_field: str = ""
    text_index_name: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IndexInfo":
        data = _as_mapping(data, "index")
        allowed = [
            "name",
            "dimension",
            "metric",
            "generation",
            "contract_version",
            "embedding_field",
            "vector_index_name",
            "vector_strategy",
            "vector_m",
            "vector_ef_construction",
            "vector_ef_search",
            "quantized_indexes",
            "text_field",
            "text_index_name",
            "document_type",
            "capabilities",
        ]
        embedding_field = _as_str(data["embedding_field"], "index.embedding_field")
        return cls(
            name=_as_str(data["name"], "index.name"),
            dimension=_as_int(data["dimension"], "index.dimension"),
            metric=_as_str(data["metric"], "index.metric"),
            generation=_as_int(data["generation"], "index.generation"),
            contract_version=_as_str(data["contract_version"], "index.contract_version"),
            embedding_field=embedding_field,
            vector_index_name=_as_optional_str_default(data.get("vector_index_name", embedding_field), "index.vector_index_name"),
            vector_strategy=_as_optional_str_default(data.get("vector_strategy", ""), "index.vector_strategy"),
            vector_m=_as_optional_int_default(data.get("vector_m"), "index.vector_m"),
            vector_ef_construction=_as_optional_int_default(
                data.get("vector_ef_construction"), "index.vector_ef_construction"
            ),
            vector_ef_search=_as_optional_int_default(data.get("vector_ef_search"), "index.vector_ef_search"),
            quantized_indexes=[QuantizedIndexInfo.from_dict(item) for item in data.get("quantized_indexes", [])],
            text_field=_as_optional_str_default(data.get("text_field", ""), "index.text_field"),
            text_index_name=_as_optional_str_default(data.get("text_index_name", ""), "index.text_index_name"),
            document_type=_as_str(data["document_type"], "index.document_type"),
            capabilities=IndexCapabilities.from_dict(data.get("capabilities", {})),
            extra=_copy_extra(data, allowed),
        )

    def to_dict(self) -> Dict[str, Any]:
        return _merge_extra(
            {
                "name": self.name,
                "dimension": self.dimension,
                "metric": self.metric,
                "generation": self.generation,
                "contract_version": self.contract_version,
                "embedding_field": self.embedding_field,
                "vector_index_name": self.vector_index_name,
                "vector_strategy": self.vector_strategy,
                "vector_m": self.vector_m,
                "vector_ef_construction": self.vector_ef_construction,
                "vector_ef_search": self.vector_ef_search,
                "quantized_indexes": [item.to_dict() for item in self.quantized_indexes],
                "text_field": self.text_field,
                "text_index_name": self.text_index_name,
                "document_type": self.document_type,
                "capabilities": self.capabilities.to_dict(),
            },
            self.extra,
        )


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


@dataclass(frozen=True)
class ResetIndexResponse:
    index: IndexInfo
    created: bool
    reset: bool
    drop_old: bool
    dropped_documents: int

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ResetIndexResponse":
        data = _as_mapping(data, "reset index response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            created=_as_bool(data.get("created", False), "reset response.created"),
            reset=_as_bool(data.get("reset", False), "reset response.reset"),
            drop_old=_as_bool(data.get("drop_old", False), "reset response.drop_old"),
            dropped_documents=_as_int(data.get("dropped_documents", 0), "reset response.dropped_documents"),
        )


@dataclass(frozen=True)
class VectorIndexMaintenanceStatus:
    name: str = ""
    strategy: str = ""
    state: str = ""
    reason: str = ""
    loaded: bool = False
    rebuild_needed: bool = False
    root_id: int = 0
    native_root_loaded: bool = False
    native_root_bytes: int = 0
    duration_nanos: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VectorIndexMaintenanceStatus":
        data = _as_mapping(data, "vector index maintenance status")
        allowed = [
            "name",
            "strategy",
            "state",
            "reason",
            "loaded",
            "rebuild_needed",
            "root_id",
            "native_root_loaded",
            "native_root_bytes",
            "duration_nanos",
        ]
        return cls(
            name=_as_optional_str_default(data.get("name"), "maintenance status.name"),
            strategy=_as_optional_str_default(data.get("strategy"), "maintenance status.strategy"),
            state=_as_optional_str_default(data.get("state"), "maintenance status.state"),
            reason=_as_optional_str_default(data.get("reason"), "maintenance status.reason"),
            loaded=_as_optional_bool_default(data.get("loaded"), "maintenance status.loaded"),
            rebuild_needed=_as_optional_bool_default(data.get("rebuild_needed"), "maintenance status.rebuild_needed"),
            root_id=_as_optional_int_default(data.get("root_id"), "maintenance status.root_id"),
            native_root_loaded=_as_optional_bool_default(
                data.get("native_root_loaded"), "maintenance status.native_root_loaded"
            ),
            native_root_bytes=_as_optional_int_default(
                data.get("native_root_bytes"), "maintenance status.native_root_bytes"
            ),
            duration_nanos=_as_optional_int_default(data.get("duration_nanos"), "maintenance status.duration_nanos"),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class OptimizeIndexResponse:
    index: IndexInfo
    vector_index_name: str
    status: VectorIndexMaintenanceStatus

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OptimizeIndexResponse":
        data = _as_mapping(data, "optimize index response")
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            vector_index_name=_as_str(data["vector_index_name"], "optimize response.vector_index_name"),
            status=VectorIndexMaintenanceStatus.from_dict(data.get("status", {})),
        )


@dataclass(frozen=True)
class BenchmarkVectorSearchResult:
    id: str
    ordinal: int
    score: float

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkVectorSearchResult":
        data = _as_mapping(data, "benchmark vector search result")
        return cls(
            id=_as_str(data["id"], "benchmark result.id"),
            ordinal=_as_int(data.get("ordinal", 0), "benchmark result.ordinal"),
            score=_optional_float(data.get("score", 0.0), "benchmark result.score") or 0.0,
        )


@dataclass(frozen=True)
class BenchmarkVectorSearchIDsResponse:
    response_format: str
    ids: list[str]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkVectorSearchIDsResponse":
        data = _as_mapping(data, "benchmark vector IDs response")
        if _as_str(data["response_format"], "benchmark IDs response.response_format") != "ids":
            raise ValueError("benchmark IDs response.response_format must be 'ids'")
        ids = data.get("ids")
        if not isinstance(ids, list):
            raise ValueError("benchmark IDs response.ids must be a list")
        return cls(response_format="ids", ids=[_as_str(value, "benchmark IDs response.ids") for value in ids])


@dataclass(frozen=True)
class BenchmarkVectorSearchResponse:
    index: IndexInfo
    results: list[BenchmarkVectorSearchResult]
    metric: str
    vector_index_name: str
    query_mode: str
    quantized_index_name: str = ""
    quantized_rerank_candidates: int = 0
    no_documents: bool = False
    stats: Dict[str, Any] = field(default_factory=dict)
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkVectorSearchResponse":
        data = _as_mapping(data, "benchmark vector search response")
        allowed = [
            "index",
            "results",
            "metric",
            "vector_index_name",
            "query_mode",
            "quantized_index_name",
            "quantized_rerank_candidates",
            "no_documents",
            "stats",
            "diagnostics",
        ]
        stats = data.get("stats", {})
        diagnostics = data.get("diagnostics", {})
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            results=[BenchmarkVectorSearchResult.from_dict(item) for item in data.get("results", [])],
            metric=_as_str(data["metric"], "benchmark response.metric"),
            vector_index_name=_as_str(data["vector_index_name"], "benchmark response.vector_index_name"),
            query_mode=_as_str(data["query_mode"], "benchmark response.query_mode"),
            quantized_index_name=_as_optional_str_default(
                data.get("quantized_index_name"), "benchmark response.quantized_index_name"
            ),
            quantized_rerank_candidates=_as_optional_int_default(
                data.get("quantized_rerank_candidates"), "benchmark response.quantized_rerank_candidates"
            ),
            no_documents=_as_optional_bool_default(data.get("no_documents"), "benchmark response.no_documents"),
            stats=dict(_as_mapping(stats, "benchmark response.stats")),
            diagnostics=dict(_as_mapping(diagnostics, "benchmark response.diagnostics")),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class KeywordSearchRequest:
    """Request payload for `POST /v1/indexes/{index}/search/keyword`."""

    query: str
    top_k: int
    expected_generation: Optional[int] = None
    operator: Optional[str] = None
    candidate_limit: Optional[int] = None
    max_postings_scanned: Optional[int] = None
    filter: Any = None
    return_embedding: bool = False

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "query": _as_str(self.query, "keyword request.query"),
            "top_k": _as_int(self.top_k, "keyword request.top_k"),
            "return_embedding": _as_bool(self.return_embedding, "keyword request.return_embedding"),
        }
        if self.expected_generation is not None:
            out["expected_generation"] = _as_int(self.expected_generation, "keyword request.expected_generation")
        if self.operator is not None:
            out["operator"] = _as_str(self.operator, "keyword request.operator")
        if self.candidate_limit is not None:
            out["candidate_limit"] = _as_int(self.candidate_limit, "keyword request.candidate_limit")
        if self.max_postings_scanned is not None:
            out["max_postings_scanned"] = _as_int(self.max_postings_scanned, "keyword request.max_postings_scanned")
        normalized_filter = _filter_to_dict(self.filter)
        if normalized_filter is not None:
            out["filter"] = normalized_filter
        return out


@dataclass(frozen=True)
class KeywordSearchStats:
    query_terms: int = 0
    candidates_requested: int = 0
    candidates_returned: int = 0
    postings_scanned: int = 0
    candidates_scored: int = 0
    documents_fetched: int = 0
    documents_missing: int = 0
    full_document_scan_fallbacks: int = 0
    postings_scan_nanos: int = 0
    candidate_score_nanos: int = 0
    document_fetch_nanos: int = 0
    truncated: bool = False
    fail_closed: int = 0
    fail_closed_reason: str = ""
    unavailable: bool = False
    unavailable_reason: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "KeywordSearchStats":
        data = _as_mapping(data, "keyword stats")
        allowed = [
            "query_terms",
            "candidates_requested",
            "candidates_returned",
            "postings_scanned",
            "candidates_scored",
            "documents_fetched",
            "documents_missing",
            "full_document_scan_fallbacks",
            "postings_scan_nanos",
            "candidate_score_nanos",
            "document_fetch_nanos",
            "truncated",
            "fail_closed",
            "fail_closed_reason",
            "unavailable",
            "unavailable_reason",
        ]
        return cls(
            query_terms=_as_optional_int_default(data.get("query_terms"), "keyword stats.query_terms"),
            candidates_requested=_as_optional_int_default(data.get("candidates_requested"), "keyword stats.candidates_requested"),
            candidates_returned=_as_optional_int_default(data.get("candidates_returned"), "keyword stats.candidates_returned"),
            postings_scanned=_as_optional_int_default(data.get("postings_scanned"), "keyword stats.postings_scanned"),
            candidates_scored=_as_optional_int_default(data.get("candidates_scored"), "keyword stats.candidates_scored"),
            documents_fetched=_as_optional_int_default(data.get("documents_fetched"), "keyword stats.documents_fetched"),
            documents_missing=_as_optional_int_default(data.get("documents_missing"), "keyword stats.documents_missing"),
            full_document_scan_fallbacks=_as_optional_int_default(
                data.get("full_document_scan_fallbacks"), "keyword stats.full_document_scan_fallbacks"
            ),
            postings_scan_nanos=_as_optional_int_default(data.get("postings_scan_nanos"), "keyword stats.postings_scan_nanos"),
            candidate_score_nanos=_as_optional_int_default(data.get("candidate_score_nanos"), "keyword stats.candidate_score_nanos"),
            document_fetch_nanos=_as_optional_int_default(data.get("document_fetch_nanos"), "keyword stats.document_fetch_nanos"),
            truncated=_as_optional_bool_default(data.get("truncated"), "keyword stats.truncated"),
            fail_closed=_as_optional_int_default(data.get("fail_closed"), "keyword stats.fail_closed"),
            fail_closed_reason=_as_optional_str_default(data.get("fail_closed_reason"), "keyword stats.fail_closed_reason"),
            unavailable=_as_optional_bool_default(data.get("unavailable"), "keyword stats.unavailable"),
            unavailable_reason=_as_optional_str_default(data.get("unavailable_reason"), "keyword stats.unavailable_reason"),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class KeywordSearchResponse:
    index: IndexInfo
    documents: list[Document]
    text_index: str
    stats: KeywordSearchStats
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "KeywordSearchResponse":
        data = _as_mapping(data, "keyword search response")
        allowed = ["index", "documents", "text_index", "stats"]
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            documents=[Document.from_dict(item) for item in data.get("documents", [])],
            text_index=_as_str(data["text_index"], "keyword response.text_index"),
            stats=KeywordSearchStats.from_dict(data.get("stats", {})),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class HybridFusionOptions:
    """Deterministic hybrid fusion request options."""

    method: Optional[str] = None
    rrf_k: Optional[int] = None
    tie_policy: Optional[str] = None
    source_order: Optional[Sequence[str]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HybridFusionOptions":
        data = _as_mapping(data, "hybrid fusion")
        allowed = ["method", "rrf_k", "tie_policy", "source_order"]
        source_order = None
        if "source_order" in data and data.get("source_order") is not None:
            raw_source_order = data.get("source_order")
            if isinstance(raw_source_order, (str, bytes, bytearray)) or not isinstance(raw_source_order, Sequence):
                raise TypeError("hybrid fusion.source_order must be a sequence of strings")
            source_order = [_as_str(item, "hybrid fusion.source_order[]") for item in raw_source_order]
        return cls(
            method=_as_optional_str_default(data.get("method"), "hybrid fusion.method") or None,
            rrf_k=None if "rrf_k" not in data or data.get("rrf_k") is None else _as_int(data.get("rrf_k"), "hybrid fusion.rrf_k"),
            tie_policy=_as_optional_str_default(data.get("tie_policy"), "hybrid fusion.tie_policy") or None,
            source_order=source_order,
            extra=_copy_extra(data, allowed),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.method is not None:
            out["method"] = _as_str(self.method, "hybrid fusion.method")
        if self.rrf_k is not None:
            out["rrf_k"] = _as_int(self.rrf_k, "hybrid fusion.rrf_k")
        if self.tie_policy is not None:
            out["tie_policy"] = _as_str(self.tie_policy, "hybrid fusion.tie_policy")
        if self.source_order is not None:
            if isinstance(self.source_order, (str, bytes, bytearray)) or not isinstance(self.source_order, Sequence):
                raise TypeError("hybrid fusion.source_order must be a sequence of strings")
            out["source_order"] = [_as_str(item, "hybrid fusion.source_order[]") for item in self.source_order]
        return _merge_extra(out, self.extra)


@dataclass(frozen=True)
class HybridSearchRequest:
    """Request payload for `POST /v1/indexes/{index}/search/hybrid`."""

    top_k: int
    expected_generation: Optional[int] = None
    query: Optional[str] = None
    query_embedding: Optional[Sequence[float]] = None
    candidate_limit: Optional[int] = None
    text_candidate_limit: Optional[int] = None
    vector_candidate_limit: Optional[int] = None
    ef_search: Optional[int] = None
    fusion: Any = None
    filter: Any = None
    return_embedding: bool = False

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "top_k": _as_int(self.top_k, "hybrid request.top_k"),
            "return_embedding": _as_bool(self.return_embedding, "hybrid request.return_embedding"),
        }
        if self.expected_generation is not None:
            out["expected_generation"] = _as_int(self.expected_generation, "hybrid request.expected_generation")
        if self.query is not None:
            out["query"] = _as_str(self.query, "hybrid request.query")
        if self.query_embedding is not None:
            out["query_embedding"] = _float_list(self.query_embedding, "hybrid request.query_embedding")
        if self.candidate_limit is not None:
            out["candidate_limit"] = _as_int(self.candidate_limit, "hybrid request.candidate_limit")
        if self.text_candidate_limit is not None:
            out["text_candidate_limit"] = _as_int(self.text_candidate_limit, "hybrid request.text_candidate_limit")
        if self.vector_candidate_limit is not None:
            out["vector_candidate_limit"] = _as_int(self.vector_candidate_limit, "hybrid request.vector_candidate_limit")
        if self.ef_search is not None:
            out["ef_search"] = _as_int(self.ef_search, "hybrid request.ef_search")
        if self.fusion is not None:
            if isinstance(self.fusion, HybridFusionOptions):
                out["fusion"] = self.fusion.to_dict()
            elif isinstance(self.fusion, Mapping):
                out["fusion"] = HybridFusionOptions.from_dict(self.fusion).to_dict()
            else:
                raise TypeError("hybrid request.fusion must be HybridFusionOptions or a mapping")
        normalized_filter = _filter_to_dict(self.filter)
        if normalized_filter is not None:
            out["filter"] = normalized_filter
        return out


@dataclass(frozen=True)
class HybridSearchPlan:
    scalar_filter_strategy: str = ""
    fusion_method: str = ""
    fusion_tie_policy: str = ""
    text_candidate_limit: int = 0
    vector_candidate_limit: int = 0
    final_top_k: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HybridSearchPlan":
        data = _as_mapping(data, "hybrid plan")
        allowed = [
            "scalar_filter_strategy",
            "fusion_method",
            "fusion_tie_policy",
            "text_candidate_limit",
            "vector_candidate_limit",
            "final_top_k",
        ]
        return cls(
            scalar_filter_strategy=_as_optional_str_default(data.get("scalar_filter_strategy"), "hybrid plan.scalar_filter_strategy"),
            fusion_method=_as_optional_str_default(data.get("fusion_method"), "hybrid plan.fusion_method"),
            fusion_tie_policy=_as_optional_str_default(data.get("fusion_tie_policy"), "hybrid plan.fusion_tie_policy"),
            text_candidate_limit=_as_optional_int_default(data.get("text_candidate_limit"), "hybrid plan.text_candidate_limit"),
            vector_candidate_limit=_as_optional_int_default(data.get("vector_candidate_limit"), "hybrid plan.vector_candidate_limit"),
            final_top_k=_as_optional_int_default(data.get("final_top_k"), "hybrid plan.final_top_k"),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class HybridSearchSnapshot:
    consistency: str = ""
    commit_seq: int = 0
    system_root_page_id: int = 0
    collection_generation: int = 0
    text_index_epoch: int = 0
    vector_index_epoch: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HybridSearchSnapshot":
        data = _as_mapping(data, "hybrid snapshot")
        allowed = [
            "consistency",
            "commit_seq",
            "system_root_page_id",
            "collection_generation",
            "text_index_epoch",
            "vector_index_epoch",
        ]
        return cls(
            consistency=_as_optional_str_default(data.get("consistency"), "hybrid snapshot.consistency"),
            commit_seq=_as_optional_int_default(data.get("commit_seq"), "hybrid snapshot.commit_seq"),
            system_root_page_id=_as_optional_int_default(data.get("system_root_page_id"), "hybrid snapshot.system_root_page_id"),
            collection_generation=_as_optional_int_default(data.get("collection_generation"), "hybrid snapshot.collection_generation"),
            text_index_epoch=_as_optional_int_default(data.get("text_index_epoch"), "hybrid snapshot.text_index_epoch"),
            vector_index_epoch=_as_optional_int_default(data.get("vector_index_epoch"), "hybrid snapshot.vector_index_epoch"),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class HybridSearchStats:
    text_candidates_requested: int = 0
    text_candidates_returned: int = 0
    text_postings_scanned: int = 0
    text_candidates_scored: int = 0
    vector_candidates_requested: int = 0
    vector_candidates_returned: int = 0
    vector_candidates_examined: int = 0
    vector_edges_visited: int = 0
    scalar_prefilter_ids: int = 0
    scalar_postfilter_checks: int = 0
    scalar_filter_matched: int = 0
    scalar_filter_rejected: int = 0
    candidates_fused: int = 0
    candidates_after_fusion: int = 0
    fusion_text_only: int = 0
    fusion_vector_only: int = 0
    fusion_both: int = 0
    fusion_duplicate_candidates: int = 0
    candidates_after_filter: int = 0
    documents_fetched: int = 0
    documents_missing: int = 0
    full_document_scan_fallbacks: int = 0
    truncated: int = 0
    fail_closed: int = 0
    fail_closed_reason: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HybridSearchStats":
        data = _as_mapping(data, "hybrid stats")
        allowed = [
            "text_candidates_requested",
            "text_candidates_returned",
            "text_postings_scanned",
            "text_candidates_scored",
            "vector_candidates_requested",
            "vector_candidates_returned",
            "vector_candidates_examined",
            "vector_edges_visited",
            "scalar_prefilter_ids",
            "scalar_postfilter_checks",
            "scalar_filter_matched",
            "scalar_filter_rejected",
            "candidates_fused",
            "candidates_after_fusion",
            "fusion_text_only",
            "fusion_vector_only",
            "fusion_both",
            "fusion_duplicate_candidates",
            "candidates_after_filter",
            "documents_fetched",
            "documents_missing",
            "full_document_scan_fallbacks",
            "truncated",
            "fail_closed",
            "fail_closed_reason",
        ]
        return cls(
            text_candidates_requested=_as_optional_int_default(data.get("text_candidates_requested"), "hybrid stats.text_candidates_requested"),
            text_candidates_returned=_as_optional_int_default(data.get("text_candidates_returned"), "hybrid stats.text_candidates_returned"),
            text_postings_scanned=_as_optional_int_default(data.get("text_postings_scanned"), "hybrid stats.text_postings_scanned"),
            text_candidates_scored=_as_optional_int_default(data.get("text_candidates_scored"), "hybrid stats.text_candidates_scored"),
            vector_candidates_requested=_as_optional_int_default(
                data.get("vector_candidates_requested"), "hybrid stats.vector_candidates_requested"
            ),
            vector_candidates_returned=_as_optional_int_default(
                data.get("vector_candidates_returned"), "hybrid stats.vector_candidates_returned"
            ),
            vector_candidates_examined=_as_optional_int_default(
                data.get("vector_candidates_examined"), "hybrid stats.vector_candidates_examined"
            ),
            vector_edges_visited=_as_optional_int_default(data.get("vector_edges_visited"), "hybrid stats.vector_edges_visited"),
            scalar_prefilter_ids=_as_optional_int_default(data.get("scalar_prefilter_ids"), "hybrid stats.scalar_prefilter_ids"),
            scalar_postfilter_checks=_as_optional_int_default(
                data.get("scalar_postfilter_checks"), "hybrid stats.scalar_postfilter_checks"
            ),
            scalar_filter_matched=_as_optional_int_default(data.get("scalar_filter_matched"), "hybrid stats.scalar_filter_matched"),
            scalar_filter_rejected=_as_optional_int_default(data.get("scalar_filter_rejected"), "hybrid stats.scalar_filter_rejected"),
            candidates_fused=_as_optional_int_default(data.get("candidates_fused"), "hybrid stats.candidates_fused"),
            candidates_after_fusion=_as_optional_int_default(data.get("candidates_after_fusion"), "hybrid stats.candidates_after_fusion"),
            fusion_text_only=_as_optional_int_default(data.get("fusion_text_only"), "hybrid stats.fusion_text_only"),
            fusion_vector_only=_as_optional_int_default(data.get("fusion_vector_only"), "hybrid stats.fusion_vector_only"),
            fusion_both=_as_optional_int_default(data.get("fusion_both"), "hybrid stats.fusion_both"),
            fusion_duplicate_candidates=_as_optional_int_default(
                data.get("fusion_duplicate_candidates"), "hybrid stats.fusion_duplicate_candidates"
            ),
            candidates_after_filter=_as_optional_int_default(data.get("candidates_after_filter"), "hybrid stats.candidates_after_filter"),
            documents_fetched=_as_optional_int_default(data.get("documents_fetched"), "hybrid stats.documents_fetched"),
            documents_missing=_as_optional_int_default(data.get("documents_missing"), "hybrid stats.documents_missing"),
            full_document_scan_fallbacks=_as_optional_int_default(
                data.get("full_document_scan_fallbacks"), "hybrid stats.full_document_scan_fallbacks"
            ),
            truncated=_as_optional_int_default(data.get("truncated"), "hybrid stats.truncated"),
            fail_closed=_as_optional_int_default(data.get("fail_closed"), "hybrid stats.fail_closed"),
            fail_closed_reason=_as_optional_str_default(data.get("fail_closed_reason"), "hybrid stats.fail_closed_reason"),
            extra=_copy_extra(data, allowed),
        )


@dataclass(frozen=True)
class HybridSearchResponse:
    index: IndexInfo
    documents: list[Document]
    text_index: str = ""
    vector_index: str = ""
    plan: HybridSearchPlan = field(default_factory=HybridSearchPlan)
    snapshot: HybridSearchSnapshot = field(default_factory=HybridSearchSnapshot)
    stats: HybridSearchStats = field(default_factory=HybridSearchStats)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HybridSearchResponse":
        data = _as_mapping(data, "hybrid search response")
        allowed = ["index", "documents", "text_index", "vector_index", "plan", "snapshot", "stats"]
        return cls(
            index=IndexInfo.from_dict(data["index"]),
            documents=[Document.from_dict(item) for item in data.get("documents", [])],
            text_index=_as_optional_str_default(data.get("text_index"), "hybrid response.text_index"),
            vector_index=_as_optional_str_default(data.get("vector_index"), "hybrid response.vector_index"),
            plan=HybridSearchPlan.from_dict(data.get("plan", {})),
            snapshot=HybridSearchSnapshot.from_dict(data.get("snapshot", {})),
            stats=HybridSearchStats.from_dict(data.get("stats", {})),
            extra=_copy_extra(data, allowed),
        )
