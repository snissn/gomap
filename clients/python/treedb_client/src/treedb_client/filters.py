"""Haystack-style filter normalization for the TreeDB document service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from .errors import TreeDBClientError


class InvalidFilterError(TreeDBClientError, ValueError):
    """Raised when a filter cannot be represented safely by the service."""


_BOOL_OPERATORS = {"and": "AND", "or": "OR", "not": "NOT"}
_LEAF_OPERATORS = {
    "==": "==",
    "=": "==",
    "eq": "==",
    "$eq": "==",
    "!=": "!=",
    "ne": "!=",
    "$ne": "!=",
    ">": ">",
    "gt": ">",
    "$gt": ">",
    ">=": ">=",
    "gte": ">=",
    "$gte": ">=",
    "<": "<",
    "lt": "<",
    "$lt": "<",
    "<=": "<=",
    "lte": "<=",
    "$lte": "<=",
    "in": "in",
    "$in": "in",
    "not in": "not in",
    "not_in": "not in",
    "not-in": "not in",
    "nin": "not in",
    "$nin": "not in",
}
_ALLOWED_KEYS = {"operator", "field", "value", "conditions"}


@dataclass(frozen=True)
class Filter:
    """Filter AST node accepted by the TreeDB document service.

    Boolean nodes set `operator` to AND/OR/NOT and provide `conditions`. Leaf
    nodes set `field`, `operator`, and `value`. The client validates only the
    shapes the service can execute; it never rewrites unsupported filters into
    broader client-side scans.
    """

    operator: str
    field: Optional[str] = None
    value: Any = None
    conditions: Optional[Sequence["FilterLike"]] = None

    def to_dict(self) -> Dict[str, Any]:
        return normalize_filter(self)


FilterLike = Union[Filter, Mapping[str, Any]]


def normalize_filter(filter_value: Optional[FilterLike]) -> Optional[Dict[str, Any]]:
    """Return a service-compatible filter dictionary.

    The accepted input shape intentionally matches Haystack v2 style filters and
    the TreeDB service contract. Unsupported operators and malformed nodes raise
    `InvalidFilterError` instead of falling back to a client-side document scan.
    """

    if filter_value is None:
        return None
    if isinstance(filter_value, Filter):
        raw: Dict[str, Any] = {"operator": filter_value.operator}
        if filter_value.field is not None:
            raw["field"] = filter_value.field
        if filter_value.conditions is not None:
            raw["conditions"] = list(filter_value.conditions)
        if filter_value.conditions is None or filter_value.field is not None:
            raw["value"] = filter_value.value
        return _normalize_mapping(raw, "filter")
    if isinstance(filter_value, Mapping):
        return _normalize_mapping(dict(filter_value), "filter")
    raise InvalidFilterError("filter must be a mapping, Filter, or None")


def _normalize_mapping(raw: Dict[str, Any], path: str) -> Dict[str, Any]:
    unknown = sorted(str(key) for key in raw if key not in _ALLOWED_KEYS)
    if unknown:
        raise InvalidFilterError(f"{path}: unsupported field(s): {', '.join(unknown)}")
    operator = raw.get("operator")
    if not isinstance(operator, str) or not operator.strip():
        raise InvalidFilterError(f"{path}: operator is required")
    normalized = _normalize_operator(operator)
    if normalized in _BOOL_OPERATORS.values():
        return _normalize_boolean(raw, normalized, path)
    return _normalize_leaf(raw, normalized, path)


def _normalize_operator(operator: str) -> str:
    key = " ".join(operator.strip().lower().replace("_", " ").split())
    if key in _BOOL_OPERATORS:
        return _BOOL_OPERATORS[key]
    if key in _LEAF_OPERATORS:
        return _LEAF_OPERATORS[key]
    raise InvalidFilterError(f"unsupported filter operator {operator!r}")


def _normalize_boolean(raw: Dict[str, Any], operator: str, path: str) -> Dict[str, Any]:
    if raw.get("field") not in (None, ""):
        raise InvalidFilterError(f"{path}: boolean operator {operator!r} cannot set field")
    if "value" in raw and raw.get("value") is not None:
        raise InvalidFilterError(f"{path}: boolean operator {operator!r} cannot set value")
    conditions = raw.get("conditions")
    if isinstance(conditions, (str, bytes, bytearray)) or not isinstance(conditions, Sequence):
        raise InvalidFilterError(f"{path}: boolean operator {operator!r} requires conditions")
    if operator in {"AND", "OR"} and len(conditions) == 0:
        raise InvalidFilterError(f"{path}: operator {operator!r} requires at least one condition")
    if operator == "NOT" and len(conditions) != 1:
        raise InvalidFilterError(f"{path}: NOT requires exactly one condition")
    return {
        "operator": operator,
        "conditions": [
            _normalize_mapping(_coerce_mapping(condition, f"{path}.conditions[{i}]"), f"{path}.conditions[{i}]")
            for i, condition in enumerate(conditions)
        ],
    }


def _normalize_leaf(raw: Dict[str, Any], operator: str, path: str) -> Dict[str, Any]:
    if "conditions" in raw and raw.get("conditions") not in (None, []):
        raise InvalidFilterError(f"{path}: leaf operator {operator!r} cannot set conditions")
    field = raw.get("field")
    if not isinstance(field, str) or not field.strip():
        raise InvalidFilterError(f"{path}: field is required for operator {operator!r}")
    field = field.strip()
    if field == "embedding":
        raise InvalidFilterError(f"{path}: embedding filters are unsupported; filter metadata fields instead")
    if "value" not in raw:
        raise InvalidFilterError(f"{path}: value is required for operator {operator!r}")
    value = raw.get("value")
    if operator in {"in", "not in"}:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
            raise InvalidFilterError(f"{path}: operator {operator!r} requires an array value")
        value = list(value)
    return {"field": field, "operator": operator, "value": value}


def _coerce_mapping(value: Any, path: str) -> Dict[str, Any]:
    if isinstance(value, Filter):
        return value.to_dict()
    if isinstance(value, Mapping):
        return dict(value)
    raise InvalidFilterError(f"{path}: condition must be a filter mapping")
