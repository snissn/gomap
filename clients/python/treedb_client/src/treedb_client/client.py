"""Synchronous HTTP client for the TreeDB document service."""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any, Optional, Union

from .errors import (
    InvalidRequestError,
    TreeDBConfigError,
    TreeDBProtocolError,
    TreeDBTimeoutError,
    TreeDBTransportError,
    service_error_from_code,
)
from .filters import FilterLike, InvalidFilterError, normalize_filter
from .models import (
    CountDocumentsResponse,
    DeleteDocumentsResponse,
    DenseVectorSearchResponse,
    Document,
    FilterDocumentsResponse,
    IndexInfo,
    UpsertDocumentsResponse,
)

DocumentLike = Union[Document, Mapping[str, Any]]


class TreeDBClient:
    """Small sync client for the pre-alpha TreeDB document service.

    The client uses only Python's standard library and has no Haystack runtime
    dependency. All filtering/deletion/search behavior is delegated to the
    service; unsupported filters raise locally or fail closed on the service.
    """

    def __init__(self, base_url: str, timeout: Optional[float] = 30.0) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.timeout = _normalize_timeout(timeout)
        self._opener = urllib.request.build_opener()

    def health(self) -> Mapping[str, Any]:
        """Return the service health payload from `GET /v1/health`."""

        payload = self._request("GET", "/v1/health")
        if not isinstance(payload, Mapping):
            raise TreeDBProtocolError("health response must be a JSON object")
        return payload

    def create_index(self, name: str, dimension: int, metric: Optional[str] = "cosine") -> IndexInfo:
        """Create or idempotently open a compatible document index."""

        request: dict[str, Any] = {"name": name, "dimension": dimension}
        if metric:
            request["metric"] = metric
        payload = self._request("POST", "/v1/indexes", request)
        return _index_from_envelope(payload)

    def ensure_index(self, name: str, dimension: int, metric: Optional[str] = "cosine") -> IndexInfo:
        """Ensure a compatible index exists.

        The service's create route is idempotent for compatible existing indexes
        and returns `conflict` for incompatible schemas.
        """

        return self.create_index(name, dimension, metric)

    def open_index(self, name: str) -> IndexInfo:
        """Open/read metadata for an existing index."""

        payload = self._request("GET", self._index_path(name))
        return _index_from_envelope(payload)

    def upsert_documents(
        self,
        index: str,
        documents: Sequence[DocumentLike],
        *,
        expected_generation: Optional[int] = None,
    ) -> UpsertDocumentsResponse:
        """Write or replace documents in an index."""

        request: dict[str, Any] = {"documents": [_document_for_write(doc) for doc in documents]}
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "upsert"), request)
        return UpsertDocumentsResponse.from_dict(_expect_mapping(payload, "upsert response"))

    def delete_documents(
        self,
        index: str,
        ids: Sequence[str],
        *,
        expected_generation: Optional[int] = None,
    ) -> DeleteDocumentsResponse:
        """Delete explicit document IDs.

        This method only sends ID deletes. Use `delete_by_filter` for the
        service-supported metadata-filter delete path.
        """

        request: dict[str, Any] = {"ids": _list_of_strings(ids, "ids")}
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "delete"), request)
        return DeleteDocumentsResponse.from_dict(_expect_mapping(payload, "delete response"))

    def delete_by_filter(
        self,
        index: str,
        filter: FilterLike,
        *,
        expected_generation: Optional[int] = None,
    ) -> DeleteDocumentsResponse:
        """Delete documents matching a server-side metadata filter.

        The client never scans locally to emulate unsupported delete behavior.
        """

        normalized = normalize_filter(filter)
        if normalized is None:
            raise InvalidFilterError("delete_by_filter requires a filter")
        request: dict[str, Any] = {"filter": normalized}
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "delete"), request)
        return DeleteDocumentsResponse.from_dict(_expect_mapping(payload, "delete response"))

    def count_documents(
        self,
        index: str,
        filter: Optional[FilterLike] = None,
        *,
        expected_generation: Optional[int] = None,
    ) -> CountDocumentsResponse:
        """Count documents matching a server-side filter, or all documents."""

        request: dict[str, Any] = {}
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "count"), request)
        return CountDocumentsResponse.from_dict(_expect_mapping(payload, "count response"))

    def filter_documents(
        self,
        index: str,
        filter: Optional[FilterLike] = None,
        *,
        limit: int = 0,
        offset: int = 0,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> FilterDocumentsResponse:
        """List documents matching a server-side filter in document-ID order."""

        request: dict[str, Any] = {"limit": limit, "offset": offset, "return_embedding": return_embedding}
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "filter"), request)
        return FilterDocumentsResponse.from_dict(_expect_mapping(payload, "filter response"))

    def query_by_embedding(
        self,
        index: str,
        query_embedding: Sequence[float],
        top_k: int,
        filter: Optional[FilterLike] = None,
        *,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> DenseVectorSearchResponse:
        """Run exact dense-vector search through the TreeDB service."""

        request: dict[str, Any] = {
            "query_embedding": [float(value) for value in query_embedding],
            "top_k": top_k,
            "return_embedding": return_embedding,
        }
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "search", "vector"), request)
        return DenseVectorSearchResponse.from_dict(_expect_mapping(payload, "vector search response"))

    def _index_path(self, index: str, *parts: str) -> str:
        encoded = urllib.parse.quote(index, safe="")
        suffix = "/".join(urllib.parse.quote(part, safe="") for part in parts)
        if suffix:
            return f"/v1/indexes/{encoded}/{suffix}"
        return f"/v1/indexes/{encoded}"

    def _request(self, method: str, path: str, body: Optional[Mapping[str, Any]] = None) -> Any:
        url = self.base_url + path
        data: Optional[bytes] = None
        headers = {"Accept": "application/json"}
        if body is not None:
            try:
                data = json.dumps(body, allow_nan=False, separators=(",", ":")).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise InvalidRequestError("invalid_request", f"request payload is not JSON-serializable: {exc}") from exc
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                response_body = response.read()
                return self._decode_success(response.getcode(), response_body)
        except urllib.error.HTTPError as exc:
            try:
                response_body = exc.read()
            finally:
                exc.close()
            raise self._decode_error(exc.code, response_body) from None
        except urllib.error.URLError as exc:
            if _is_timeout(exc.reason):
                raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from exc
            raise TreeDBTransportError(f"TreeDB request to {url} failed: {exc.reason}") from exc
        except (socket.timeout, TimeoutError) as exc:
            raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from exc

    def _decode_success(self, status_code: int, body: bytes) -> Any:
        decoded = _decode_json_body(body, status_code=status_code)
        if isinstance(decoded, Mapping) and "error" in decoded:
            error = decoded.get("error")
            if isinstance(error, Mapping):
                code = str(error.get("code", "internal"))
                message = str(error.get("message", ""))
                raise service_error_from_code(code, message, status_code=status_code, response_body=_body_to_text(body))
            raise TreeDBProtocolError("error envelope must contain an object", status_code=status_code, response_body=_body_to_text(body))
        return decoded

    def _decode_error(self, status_code: int, body: bytes) -> Exception:
        decoded = _decode_json_body(body, status_code=status_code)
        if not isinstance(decoded, Mapping):
            return TreeDBProtocolError(
                f"TreeDB service returned HTTP {status_code} with a non-object JSON body",
                status_code=status_code,
                response_body=_body_to_text(body),
            )
        error = decoded.get("error")
        if not isinstance(error, Mapping):
            return TreeDBProtocolError(
                f"TreeDB service returned HTTP {status_code} without an error envelope",
                status_code=status_code,
                response_body=_body_to_text(body),
            )
        code = str(error.get("code", "internal"))
        message = str(error.get("message", ""))
        return service_error_from_code(code, message, status_code=status_code, response_body=_body_to_text(body))


def _normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise TreeDBConfigError("base_url must be a non-empty HTTP(S) URL")
    trimmed = base_url.strip().rstrip("/")
    parsed = urllib.parse.urlparse(trimmed)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise TreeDBConfigError("base_url must be an absolute HTTP(S) URL")
    if parsed.query or parsed.fragment:
        raise TreeDBConfigError("base_url must not include query parameters or fragments")
    return trimmed


def _normalize_timeout(timeout: Optional[float]) -> Optional[float]:
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TreeDBConfigError("timeout must be a positive number of seconds or None")
    value = float(timeout)
    if value <= 0:
        raise TreeDBConfigError("timeout must be positive")
    return value


def _add_expected_generation(request: dict[str, Any], expected_generation: Optional[int]) -> None:
    if expected_generation is None:
        return
    if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation < 0:
        raise InvalidRequestError("invalid_request", "expected_generation must be a non-negative integer")
    request["expected_generation"] = expected_generation


def _add_filter(request: dict[str, Any], filter_value: Optional[FilterLike]) -> None:
    normalized = normalize_filter(filter_value)
    if normalized is not None:
        request["filter"] = normalized


def _document_for_write(document: DocumentLike) -> Mapping[str, Any]:
    if isinstance(document, Document):
        return document.to_dict(include_score=False)
    if isinstance(document, Mapping):
        return Document.from_dict(document).to_dict(include_score=False)
    raise InvalidRequestError("invalid_request", "documents must be Document instances or mappings")


def _list_of_strings(values: Sequence[str], label: str) -> list[str]:
    if isinstance(values, (str, bytes, bytearray)):
        raise InvalidRequestError("invalid_request", f"{label} must be a sequence of strings, not a single string")
    try:
        out = list(values)
    except TypeError as exc:
        raise InvalidRequestError("invalid_request", f"{label} must be a sequence of strings") from exc
    for i, value in enumerate(out):
        if not isinstance(value, str):
            raise InvalidRequestError("invalid_request", f"{label}[{i}] must be a string")
    return out


def _index_from_envelope(payload: Any) -> IndexInfo:
    envelope = _expect_mapping(payload, "index response")
    index = envelope.get("index")
    if not isinstance(index, Mapping):
        raise TreeDBProtocolError("index response is missing index object")
    return IndexInfo.from_dict(index)


def _expect_mapping(payload: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise TreeDBProtocolError(f"{label} must be a JSON object")
    return payload


def _decode_json_body(body: bytes, *, status_code: int) -> Any:
    text = _body_to_text(body)
    try:
        return json.loads(text) if text else {}
    except json.JSONDecodeError as exc:
        raise TreeDBProtocolError(
            f"TreeDB service returned malformed JSON for HTTP {status_code}: {exc}",
            status_code=status_code,
            response_body=text,
        ) from exc


def _body_to_text(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def _is_timeout(reason: Any) -> bool:
    if isinstance(reason, (socket.timeout, TimeoutError)):
        return True
    return "timed out" in str(reason).lower()
