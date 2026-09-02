"""Synchronous HTTP client for the TreeDB document service."""

from __future__ import annotations

import base64
import http.client
import json
import socket
import ssl
import struct
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Optional, TypeVar, Union

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
    BenchmarkVectorIndexOptions,
    BenchmarkVectorSearchIDsResponse,
    BenchmarkVectorSearchResponse,
    CountDocumentsResponse,
    DeleteDocumentsResponse,
    DenseVectorSearchResponse,
    Document,
    FilterDocumentsResponse,
    HybridFusionOptions,
    HybridSearchRequest,
    HybridSearchResponse,
    IndexInfo,
    KeywordSearchRequest,
    KeywordSearchResponse,
    OptimizeIndexResponse,
    ResetIndexResponse,
    ScalarFieldDeclaration,
    ScalarFieldDeclarationLike,
    UpsertDocumentsResponse,
)

ScalarFieldDeclarationsLike = Sequence[ScalarFieldDeclarationLike]
DocumentLike = Union[Document, Mapping[str, Any]]
VectorIndexOptionsLike = Union[BenchmarkVectorIndexOptions, Mapping[str, Any]]
BINARY_VECTOR_SEARCH_F32LE_CONTENT_TYPE = "application/vnd.treedb.vector-search.f32le"
_ResponseT = TypeVar("_ResponseT")


class TreeDBClient:
    """Small sync client for the pre-alpha TreeDB document service.

    The client uses only Python's standard library and has no Haystack runtime
    dependency. All filtering/deletion/search behavior is delegated to the
    service; unsupported filters raise locally or fail closed on the service.
    """

    def __init__(self, base_url: str, timeout: Optional[float] = 30.0) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.timeout = _normalize_timeout(timeout)
        parsed = urllib.parse.urlparse(self.base_url)
        self._request_prefix = parsed.path + (";" + parsed.params if parsed.params else "")
        connection_type = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
        self._connection = connection_type(parsed.hostname, port, timeout=self.timeout)
        self._opener = urllib.request.build_opener()
        proxies = urllib.request.getproxies()
        self._benchmark_uses_proxy = bool(proxies.get(parsed.scheme)) and not urllib.request.proxy_bypass(parsed.netloc)

    def close(self) -> None:
        """Close this client's reusable HTTP connection."""

        self._connection.close()

    def __del__(self) -> None:
        connection = getattr(self, "_connection", None)
        if connection is not None:
            connection.close()

    def health(self) -> Mapping[str, Any]:
        """Return the service health payload from `GET /v1/health`."""

        payload = self._request("GET", "/v1/health")
        if not isinstance(payload, Mapping):
            raise TreeDBProtocolError("health response must be a JSON object")
        return payload

    def create_index(
        self,
        name: str,
        dimension: int,
        metric: Optional[str] = "cosine",
        *,
        scalar_fields: Optional[ScalarFieldDeclarationsLike] = None,
        vector_index_options: Optional[VectorIndexOptionsLike] = None,
    ) -> IndexInfo:
        """Create or idempotently open a compatible document index."""

        request: dict[str, Any] = {"name": name, "dimension": dimension}
        _add_optional_non_empty_string(request, "metric", metric, "metric")
        _add_scalar_fields(request, scalar_fields)
        _add_vector_index_options(request, vector_index_options)
        payload = self._request("POST", "/v1/indexes", request)
        return _index_from_envelope(payload)
    def ensure_index(
        self,
        name: str,
        dimension: int,
        metric: Optional[str] = "cosine",
        *,
        scalar_fields: Optional[ScalarFieldDeclarationsLike] = None,
        vector_index_options: Optional[VectorIndexOptionsLike] = None,
    ) -> IndexInfo:
        """Ensure a compatible index exists.

        The service's create route is idempotent for compatible existing indexes
        and returns `conflict` for incompatible schemas.
        """

        return self.create_index(
            name,
            dimension,
            metric,
            scalar_fields=scalar_fields,
            vector_index_options=vector_index_options,
        )


    def reset_index(
        self,
        index: str,
        *,
        dimension: int,
        metric: Optional[str] = None,
        drop_old: bool = False,
        vector_index_options: Optional[VectorIndexOptionsLike] = None,
    ) -> ResetIndexResponse:
        """Create or reset a benchmark index through the service lifecycle route.

        Existing column_graph benchmark indexes fail closed on the service when
        `drop_old=True`; managed benchmark runs should use a fresh data directory
        or a unique index name to preserve TreeDB's insert-only graph rebuild
        boundary. Omit `metric` to preserve an existing index's metric; pass it
        when creating a missing index or when compatibility should be enforced.
        """

        request: dict[str, Any] = {"dimension": dimension, "drop_old": bool(drop_old)}
        _add_optional_non_empty_string(request, "metric", metric, "metric")
        _add_vector_index_options(request, vector_index_options)
        payload = self._request("POST", self._index_path(index, "reset"), request)
        return _parse_response("reset index response", ResetIndexResponse.from_dict, payload)

    def optimize_index(
        self,
        index: str,
        *,
        vector_index_name: Optional[str] = None,
        expected_generation: Optional[int] = None,
    ) -> OptimizeIndexResponse:
        """Rebuild service vector assets after a benchmark load phase."""

        request: dict[str, Any] = {}
        _add_expected_generation(request, expected_generation)
        if vector_index_name:
            request["vector_index_name"] = vector_index_name
        payload = self._request("POST", self._index_path(index, "optimize"), request)
        return _parse_response("optimize index response", OptimizeIndexResponse.from_dict, payload)

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
        defer_vector_index_rebuild: bool = False,
    ) -> UpsertDocumentsResponse:
        """Write or replace documents in an index."""

        request: dict[str, Any] = {"documents": [_document_for_write(doc) for doc in documents]}
        _add_expected_generation(request, expected_generation)
        if defer_vector_index_rebuild:
            request["defer_vector_index_rebuild"] = True
        payload = self._request("POST", self._index_path(index, "documents", "upsert"), request)
        return _parse_response("upsert response", UpsertDocumentsResponse.from_dict, payload)

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
        return _parse_response("delete response", DeleteDocumentsResponse.from_dict, payload)

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
        return _parse_response("delete response", DeleteDocumentsResponse.from_dict, payload)

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
        return _parse_response("count response", CountDocumentsResponse.from_dict, payload)

    def filter_documents(
        self,
        index: str,
        filter: Optional[FilterLike] = None,
        *,
        limit: int = 0,
        offset: int = 0,
        return_embedding: bool = False,
        after_id: Optional[str] = None,
        cursor_page: bool = False,
        expected_generation: Optional[int] = None,
    ) -> FilterDocumentsResponse:
        """List documents matching a server-side filter in document-ID order."""

        if after_id is not None and not cursor_page:
            raise InvalidRequestError("invalid_request", "after_id requires cursor_page=True")
        request: dict[str, Any] = {"limit": limit, "offset": offset, "return_embedding": return_embedding}
        if after_id is not None:
            request["after_id"] = str(after_id)
        if cursor_page:
            request["cursor_page"] = True
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "documents", "filter"), request)
        return _parse_response("filter response", FilterDocumentsResponse.from_dict, payload)

    def query_by_embedding(
        self,
        index: str,
        query_embedding: Sequence[float],
        top_k: int,
        filter: Optional[FilterLike] = None,
        *,
        route: Optional[str] = None,
        ef_search: Optional[int] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> DenseVectorSearchResponse:
        """Score a query embedding through the TreeDB dense search route.

        ``route`` selects the execution path (v1alpha2): ``"ann"`` uses a
        compatible native_runtime or column_graph vector index; ``"exact"``
        keeps the bounded filtered scan. Declared scalar filters are supported
        by native_runtime ANN and expose route/work diagnostics in the response.
        Unsupported filter shapes fail closed; neither the client nor service
        silently downgrades an ANN request to exact search.
        """

        if route is not None and route not in ("ann", "exact"):
            raise InvalidRequestError("invalid_request", f"unsupported dense search route {route!r}; use 'ann' or 'exact'")
        ef_search_value = None
        if ef_search is not None:
            ef_search_value = _validate_binary_int_query_param(ef_search, "ef_search", minimum=0)
        request: dict[str, Any] = {
            "query_embedding": [float(value) for value in query_embedding],
            "top_k": top_k,
            "return_embedding": return_embedding,
        }
        if route is not None:
            request["route"] = route
        if ef_search_value is not None:
            request["ef_search"] = ef_search_value
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request("POST", self._index_path(index, "search", "vector"), request)
        return _parse_response("vector search response", DenseVectorSearchResponse.from_dict, payload)

    def search_vector_index(
        self,
        index: str,
        query_embedding: Sequence[float],
        top_k: int,
        *,
        vector_index_name: Optional[str] = None,
        ef_search: Optional[int] = None,
        query_mode: Optional[str] = None,
        quantized_index_name: Optional[str] = None,
        quantized_rerank_candidates: Optional[int] = None,
        stats_mode: Optional[str] = None,
        expected_generation: Optional[int] = None,
        query_embedding_encoding: str = "json",
        response_format: Optional[str] = None,
    ) -> Union[BenchmarkVectorSearchResponse, BenchmarkVectorSearchIDsResponse]:
        """Run fail-closed no-document vector-index benchmark search.

        This calls `/search/vector-index`, not the exact dense `/search/vector`
        route used by Haystack. Quantized modes must be explicit and are not
        emulated by client-side or service-side exact fallback.
        """

        if query_embedding_encoding == "f32_le":
            query = _encode_f32_le_bytes(query_embedding)
            params = _binary_vector_index_query_params(
                top_k=top_k,
                vector_index_name=vector_index_name,
                ef_search=ef_search,
                query_mode=query_mode,
                quantized_index_name=quantized_index_name,
                quantized_rerank_candidates=quantized_rerank_candidates,
                stats_mode=stats_mode,
                expected_generation=expected_generation,
                response_format=response_format,
            )
            payload = self._request_bytes(
                "POST",
                self._index_path(index, "search") + "/vector-index:binary",
                query,
                BINARY_VECTOR_SEARCH_F32LE_CONTENT_TYPE,
                query_params=params,
                retry_broken_connection=True,
            )
            return _parse_benchmark_vector_search_response(payload, response_format)

        request: dict[str, Any] = {"top_k": top_k}
        _add_query_embedding(request, query_embedding, query_embedding_encoding)
        _add_expected_generation(request, expected_generation)
        if vector_index_name:
            request["vector_index_name"] = vector_index_name
        if ef_search is not None:
            request["ef_search"] = int(ef_search)
        _add_optional_non_empty_string(request, "query_mode", query_mode, "query_mode")
        _add_optional_non_empty_string(request, "quantized_index_name", quantized_index_name, "quantized_index_name")
        if quantized_rerank_candidates is not None:
            request["quantized_rerank_candidates"] = int(quantized_rerank_candidates)
        _add_optional_non_empty_string(request, "stats_mode", stats_mode, "stats_mode")
        _add_optional_non_empty_string(request, "response_format", response_format, "response_format")
        payload = self._request(
            "POST", self._index_path(index, "search", "vector-index"), request, retry_broken_connection=True
        )
        return _parse_benchmark_vector_search_response(payload, response_format)

    def search_keyword(
        self,
        index: str,
        query: str,
        top_k: int,
        *,
        operator: Optional[str] = None,
        candidate_limit: Optional[int] = None,
        max_postings_scanned: Optional[int] = None,
        filter: Optional[FilterLike] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> KeywordSearchResponse:
        """Run ranked keyword search through the TreeDB service.

        Metadata filters are serialized and sent only when provided. Since
        contract v1alpha2 the service serves filters that resolve to one
        bounded scalar allow-set over an index's declared scalar fields
        (create-index ``scalar_fields``); anything else fails closed with a
        typed error. The client never scans locally as a fallback.
        """

        _validate_expected_generation(expected_generation)
        request = KeywordSearchRequest(
            expected_generation=expected_generation,
            query=query,
            top_k=top_k,
            operator=operator,
            candidate_limit=candidate_limit,
            max_postings_scanned=max_postings_scanned,
            filter=filter,
            return_embedding=return_embedding,
        ).to_dict()
        payload = self._request("POST", self._index_path(index, "search", "keyword"), request)
        return _parse_response("keyword search response", KeywordSearchResponse.from_dict, payload)

    def search_hybrid(
        self,
        index: str,
        *,
        query: Optional[str] = None,
        query_embedding: Optional[Sequence[float]] = None,
        top_k: int,
        candidate_limit: Optional[int] = None,
        text_candidate_limit: Optional[int] = None,
        vector_candidate_limit: Optional[int] = None,
        ef_search: Optional[int] = None,
        max_chunks_per_parent: Optional[int] = None,
        fusion: Optional[Union[HybridFusionOptions, Mapping[str, Any]]] = None,
        filter: Optional[FilterLike] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
    ) -> HybridSearchResponse:
        """Run TreeDB collection-native hybrid text/vector search.

        At least one of `query` or `query_embedding` must be supplied by the
        caller/service. `max_chunks_per_parent` is disabled when omitted or
        zero; positive values cap built-in chunk children after fusion without
        expanding candidate budgets. Metadata equality/range leaves over
        declared scalar fields may be joined only by AND and are served through
        bounded indexed intersection; other shapes fail closed with typed
        errors. There is no client-side text/vector fallback.
        """

        if not query and query_embedding is None:
            raise InvalidRequestError("invalid_request", "search_hybrid requires query or query_embedding")
        _validate_expected_generation(expected_generation)
        request = HybridSearchRequest(
            expected_generation=expected_generation,
            query=query,
            query_embedding=query_embedding,
            top_k=top_k,
            candidate_limit=candidate_limit,
            text_candidate_limit=text_candidate_limit,
            vector_candidate_limit=vector_candidate_limit,
            ef_search=ef_search,
            max_chunks_per_parent=max_chunks_per_parent,
            fusion=fusion,
            filter=filter,
            return_embedding=return_embedding,
        ).to_dict()
        payload = self._request("POST", self._index_path(index, "search", "hybrid"), request)
        return _parse_response("hybrid search response", HybridSearchResponse.from_dict, payload)

    def _index_path(self, index: str, *parts: str) -> str:
        encoded = urllib.parse.quote(index, safe="")
        suffix = "/".join(urllib.parse.quote(part, safe="") for part in parts)
        if suffix:
            return f"/v1/indexes/{encoded}/{suffix}"
        return f"/v1/indexes/{encoded}"

    def _request(
        self, method: str, path: str, body: Optional[Mapping[str, Any]] = None, *, retry_broken_connection: bool = False
    ) -> Any:
        data: Optional[bytes] = None
        headers = {"Accept": "application/json"}
        if body is not None:
            try:
                data = json.dumps(body, allow_nan=False, separators=(",", ":")).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise InvalidRequestError("invalid_request", f"request payload is not JSON-serializable: {exc}") from exc
            headers["Content-Type"] = "application/json"
        return self._send_request(method, path, data, headers, retry_broken_connection=retry_broken_connection)

    def _request_bytes(
        self,
        method: str,
        path: str,
        body: bytes,
        content_type: str,
        *,
        query_params: Optional[Sequence[tuple[str, str]]] = None,
        retry_broken_connection: bool = False,
    ) -> Any:
        if query_params:
            path = path + "?" + urllib.parse.urlencode(query_params)
        headers = {"Accept": "application/json", "Content-Type": content_type}
        return self._send_request(method, path, body, headers, retry_broken_connection=retry_broken_connection)

    def _send_request(
        self, method: str, path: str, data: Optional[bytes], headers: Mapping[str, str], *, retry_broken_connection: bool = False
    ) -> Any:
        url = self.base_url + path
        if not retry_broken_connection or self._benchmark_uses_proxy:
            for attempt in range(2 if retry_broken_connection else 1):
                request = urllib.request.Request(url, data=data, headers=dict(headers), method=method)
                try:
                    with self._opener.open(request, timeout=self.timeout) as response:
                        response_body = response.read()
                        return self._decode_success(response.getcode(), response_body)
                except urllib.error.HTTPError as exc:
                    try:
                        try:
                            response_body = exc.read()
                        except (socket.timeout, TimeoutError) as read_exc:
                            raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from read_exc
                        except (http.client.RemoteDisconnected, http.client.IncompleteRead, ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as read_exc:
                            if retry_broken_connection and attempt == 0:
                                continue
                            raise TreeDBTransportError(f"TreeDB request to {url} failed: {read_exc}") from read_exc
                    finally:
                        exc.close()
                    raise self._decode_error(exc.code, response_body) from None
                except urllib.error.URLError as exc:
                    if retry_broken_connection and attempt == 0 and _is_broken_connection(exc.reason):
                        continue
                    if _is_timeout(exc.reason):
                        raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from exc
                    raise TreeDBTransportError(f"TreeDB request to {url} failed: {exc.reason}") from exc
                except (socket.timeout, TimeoutError) as exc:
                    raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from exc
                except (http.client.RemoteDisconnected, http.client.IncompleteRead, ssl.SSLEOFError, ssl.SSLZeroReturnError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
                    if retry_broken_connection and attempt == 0:
                        continue
                    raise TreeDBTransportError(f"TreeDB request to {url} failed: {exc}") from exc
        for attempt in range(2 if retry_broken_connection else 1):
            try:
                self._connection.request(method, self._request_prefix + path, body=data, headers=dict(headers))
                response = self._connection.getresponse()
                try:
                    response_body = response.read()
                    if 200 <= response.status < 300:
                        return self._decode_success(response.status, response_body)
                    raise self._decode_error(response.status, response_body)
                finally:
                    response.close()
            except (
                http.client.RemoteDisconnected,
                http.client.CannotSendRequest,
                http.client.ResponseNotReady,
                http.client.BadStatusLine,
                http.client.IncompleteRead,
                ssl.SSLEOFError,
                ssl.SSLZeroReturnError,
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
            ) as exc:
                self._connection.close()
                if retry_broken_connection and attempt == 0:
                    continue
                raise TreeDBTransportError(f"TreeDB request to {url} failed: {exc}") from exc
            except (socket.timeout, TimeoutError) as exc:
                self._connection.close()
                raise TreeDBTimeoutError(f"TreeDB request to {url} timed out after {self.timeout} seconds") from exc
            except OSError as exc:
                self._connection.close()
                raise TreeDBTransportError(f"TreeDB request to {url} failed: {exc}") from exc

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
    try:
        parsed = urllib.parse.urlparse(trimmed)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise TreeDBConfigError("base_url must have a valid host and port") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise TreeDBConfigError("base_url must be an absolute HTTP(S) URL")
    if hostname is None:
        raise TreeDBConfigError("base_url must have a valid host and port")
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


def _validate_expected_generation(expected_generation: Optional[int]) -> None:
    if expected_generation is None:
        return
    if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation <= 0:
        raise InvalidRequestError("invalid_request", "expected_generation must be a positive integer")


def _add_expected_generation(request: dict[str, Any], expected_generation: Optional[int]) -> None:
    _validate_expected_generation(expected_generation)
    if expected_generation is not None:
        request["expected_generation"] = expected_generation


def _add_optional_non_empty_string(request: dict[str, Any], key: str, value: Optional[str], label: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or value.strip() == "":
        raise InvalidRequestError("invalid_request", f"{label} must be a non-empty string when provided")
    request[key] = value


def _add_query_embedding(request: dict[str, Any], query_embedding: Sequence[float], encoding: str) -> None:
    if encoding == "json":
        request["query_embedding"] = _coerce_query_embedding_floats(query_embedding)
        return
    if encoding == "f32_le_b64":
        request["query_embedding_f32_le_b64"] = _encode_f32_le_base64(query_embedding)
        return
    raise InvalidRequestError("invalid_request", "query_embedding_encoding must be 'json', 'f32_le_b64', or 'f32_le'")


def _coerce_query_embedding_floats(values: Sequence[float]) -> list[float]:
    if isinstance(values, (str, bytes, bytearray)):
        raise InvalidRequestError("invalid_request", "query_embedding must be a sequence of floats")
    try:
        return [float(value) for value in values]
    except (TypeError, ValueError) as exc:
        raise InvalidRequestError("invalid_request", "query_embedding must be a sequence of floats") from exc


def _encode_f32_le_base64(values: Sequence[float]) -> str:
    return base64.b64encode(_encode_f32_le_bytes(values)).decode("ascii")


def _encode_f32_le_bytes(values: Sequence[float]) -> bytes:
    floats = _coerce_query_embedding_floats(values)
    return struct.pack(f"<{len(floats)}f", *floats) if floats else b""


def _binary_vector_index_query_params(
    *,
    top_k: int,
    vector_index_name: Optional[str],
    ef_search: Optional[int],
    query_mode: Optional[str],
    quantized_index_name: Optional[str],
    quantized_rerank_candidates: Optional[int],
    stats_mode: Optional[str],
    expected_generation: Optional[int],
    response_format: Optional[str],
) -> list[tuple[str, str]]:
    _validate_expected_generation(expected_generation)
    top_k_value = _validate_binary_int_query_param(top_k, "top_k", minimum=1)
    params = [("top_k", str(top_k_value))]
    _add_binary_optional_non_empty_string(params, "query_mode", "exact" if query_mode is None else query_mode, "query_mode")
    if ef_search is not None:
        ef_search_value = _validate_binary_int_query_param(ef_search, "ef_search", minimum=0)
        params.append(("ef_search", str(ef_search_value)))
    _add_binary_optional_non_empty_string(params, "vector_index_name", vector_index_name, "vector_index_name")
    _add_binary_optional_non_empty_string(params, "quantized_index_name", quantized_index_name, "quantized_index_name")
    if quantized_rerank_candidates is not None:
        rerank_value = _validate_binary_int_query_param(quantized_rerank_candidates, "quantized_rerank_candidates", minimum=0)
        params.append(("quantized_rerank_candidates", str(rerank_value)))
    _add_binary_optional_non_empty_string(params, "stats_mode", stats_mode, "stats_mode")
    if expected_generation is not None:
        params.append(("expected_generation", str(expected_generation)))
    _add_binary_optional_non_empty_string(params, "response_format", response_format, "response_format")
    return params


def _validate_binary_int_query_param(value: Any, label: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRequestError("invalid_request", f"{label} must be an integer")
    if value < minimum:
        if minimum == 1:
            raise InvalidRequestError("invalid_request", f"{label} must be a positive integer")
        raise InvalidRequestError("invalid_request", f"{label} must be a non-negative integer")
    return value


def _add_binary_optional_non_empty_string(params: list[tuple[str, str]], key: str, value: Optional[str], label: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or value.strip() == "":
        raise InvalidRequestError("invalid_request", f"{label} must be a non-empty string when provided")
    params.append((key, value))


def _add_filter(request: dict[str, Any], filter_value: Optional[FilterLike]) -> None:
    normalized = normalize_filter(filter_value)
    if normalized is not None:
        request["filter"] = normalized


def _add_scalar_fields(request: dict[str, Any], fields: Optional[ScalarFieldDeclarationsLike]) -> None:
    if fields is None:
        return
    if isinstance(fields, (str, bytes, bytearray)):
        raise InvalidRequestError("invalid_request", "scalar_fields must be a sequence of declarations")
    try:
        declarations = list(fields)
    except TypeError as exc:
        raise InvalidRequestError("invalid_request", "scalar_fields must be a sequence of declarations") from exc
    serialized: list[dict[str, Any]] = []
    for i, declaration in enumerate(declarations):
        try:
            model = (
                declaration
                if isinstance(declaration, ScalarFieldDeclaration)
                else ScalarFieldDeclaration.from_dict(declaration)
            )
            serialized.append(model.to_dict())
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError("invalid_request", f"scalar_fields[{i}] is invalid: {exc}") from exc
    request["scalar_fields"] = serialized


def _add_vector_index_options(request: dict[str, Any], options: Optional[VectorIndexOptionsLike]) -> None:
    if options is None:
        return
    if isinstance(options, BenchmarkVectorIndexOptions):
        request["vector_index_options"] = options.to_dict()
        return
    if isinstance(options, Mapping):
        request["vector_index_options"] = BenchmarkVectorIndexOptions.from_dict(options).to_dict()
        return
    raise InvalidRequestError("invalid_request", "vector_index_options must be BenchmarkVectorIndexOptions, mapping, or None")


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
    return _parse_mapping("index response", IndexInfo.from_dict, index)


def _parse_response(
    label: str,
    parser: Callable[[Mapping[str, Any]], _ResponseT],
    payload: Any,
) -> _ResponseT:
    return _parse_mapping(label, parser, _expect_mapping(payload, label))


def _parse_benchmark_vector_search_response(
    payload: Any, response_format: Optional[str]
) -> Union[BenchmarkVectorSearchResponse, BenchmarkVectorSearchIDsResponse]:
    if response_format == "ids":
        return _parse_response("benchmark vector IDs response", BenchmarkVectorSearchIDsResponse.from_dict, payload)
    return _parse_response("benchmark vector search response", BenchmarkVectorSearchResponse.from_dict, payload)


def _parse_mapping(
    label: str,
    parser: Callable[[Mapping[str, Any]], _ResponseT],
    payload: Mapping[str, Any],
) -> _ResponseT:
    try:
        return parser(payload)
    except TreeDBProtocolError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise TreeDBProtocolError(f"{label} is malformed: {exc}") from exc


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


def _is_broken_connection(reason: Any) -> bool:
    return isinstance(
        reason,
        (
            http.client.RemoteDisconnected,
            http.client.CannotSendRequest,
            http.client.ResponseNotReady,
            http.client.BadStatusLine,
            http.client.IncompleteRead,
            ssl.SSLEOFError,
            ssl.SSLZeroReturnError,
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
        ),
    )
