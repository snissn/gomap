"""Synchronous HTTP client for the TreeDB document service."""

from __future__ import annotations

import base64
import http.client
import json
import math
import socket
import ssl
import struct
import time
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
    UnsupportedError,
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

    def __init__(self, base_url: str, timeout: Optional[float] = 30.0, *, native_address: Optional[str] = None) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.timeout = _normalize_timeout(timeout)
        self._native = None
        if native_address is not None:
            from ._native import _NativeConnection
            self._native = _NativeConnection(native_address, self.timeout)
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
        if self._native is not None:
            self._native.close()

    def __del__(self) -> None:
        native = getattr(self, "_native", None)
        if native is not None:
            native.close()
        connection = getattr(self, "_connection", None)
        if connection is not None:
            connection.close()

    def get_many(self, index: str, ids: Sequence[str], *, index_info: Optional[IndexInfo] = None) -> list[Optional[Document]]:
        """Fetch owned full documents in request order using native GetMany.

        Missing IDs yield None. Typed IndexInfo selects generation-checked v2
        with one batch view, without requiring graph admission. Omission keeps
        generic v1 per-ID semantics. This is separate from search/full-fetch.
        """
        if self._native is None:
            raise UnsupportedError("unsupported", "get_many requires native_address")
        from ._native import _decode_vector, _section, _sections, _vector, _uint
        if not isinstance(index, str) or not index:
            raise TreeDBConfigError("native collection name is required")
        if isinstance(ids, (str, bytes)) or len(ids) > 1_000_000:
            raise TreeDBConfigError("native ids must be a bounded sequence")
        encoded = []
        encoded_bytes = len(index.encode("utf-8")) + 128
        for item in ids:
            if not isinstance(item, str) or not item:
                raise TreeDBConfigError("native IDs must be nonempty strings")
            encoded_id = item.encode("utf-8")
            encoded_bytes += len(encoded_id) + 10
            if encoded_bytes > self._native.limit:
                raise TreeDBConfigError("native GetMany request exceeds frame limit")
            encoded.append(encoded_id)
        version = 1
        request = _section(100, b"\x01" + index.encode("utf-8")) + _section(102, _vector(encoded))
        if index_info is not None:
            if (index_info.name != index or index_info.extra.get("typed_input") is not True
                    or index_info.vector_strategy != "column_graph" or index_info.generation <= 0):
                raise TreeDBConfigError("typed GetMany requires matching typed IndexInfo")
            version = 2
            request += _section(133, _uint(index_info.generation)) + _section(4, _uint(time.time_ns() + int(self.timeout * 1e9)))
        response = self._native.command(50, version, request, "get_many_versions")
        sections = _sections(response, {103, 116, 11})
        if 103 not in sections or 116 not in sections or len(sections[116]) != (len(ids) + 7) // 8:
            raise TreeDBProtocolError("native GetMany response sections mismatch")
        payloads = _decode_vector(sections[103], len(ids))
        documents = []
        for i, payload in enumerate(payloads):
            if not sections[116][i // 8] & (1 << (i % 8)):
                if payload:
                    raise TreeDBProtocolError("missing native document has payload")
                documents.append(None)
                continue
            try:
                document = Document.from_dict(json.loads(payload))
            except (ValueError, KeyError, TypeError) as exc:
                raise TreeDBProtocolError("invalid native document payload") from exc
            if document.id != ids[i]:
                raise TreeDBProtocolError("native document ID mismatch")
            documents.append(document)
        return documents

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
        typed_input: bool = False,
        column_graph_serving: Optional[Mapping[str, Any]] = None,
    ) -> IndexInfo:
        """Create or idempotently open a compatible document index."""

        request: dict[str, Any] = {"name": name, "dimension": dimension}
        if typed_input:
            request["typed_input"] = True
        if column_graph_serving is not None:
            request["column_graph_serving"] = dict(column_graph_serving)
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
        typed_input: bool = False,
        column_graph_serving: Optional[Mapping[str, Any]] = None,
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
            typed_input=typed_input,
            column_graph_serving=column_graph_serving,
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

        if self._native is not None:
            raise UnsupportedError("unsupported", "reset requires an explicit HTTP control client")
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
        column_graph_serving: Optional[Mapping[str, Any]] = None,
        column_graph_action: Optional[str] = None,
    ) -> OptimizeIndexResponse:
        """Rebuild service vector assets after a benchmark load phase."""

        request: dict[str, Any] = {}
        if column_graph_serving is not None:
            request["column_graph_serving"] = dict(column_graph_serving)
        if column_graph_action is not None:
            request["column_graph_action"] = column_graph_action
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
        index_info: Optional[IndexInfo] = None,
    ) -> UpsertDocumentsResponse:
        """Write or replace documents in an index."""

        if self._native is not None:
            from ._native import _section, _uint, _typed_upsert_request, _typed_upsert_response
            if (index_info is None or index_info.name != index or index_info.extra.get("typed_input") is not True
                    or index_info.vector_strategy != "column_graph" or index_info.generation <= 0
                    or (expected_generation is not None and expected_generation != index_info.generation)):
                raise TreeDBConfigError("native typed upsert requires matching typed IndexInfo and generation")
            sections, ids = _typed_upsert_request(index, documents, index_info)
            sections += _section(4, _uint(time.time_ns() + int(self.timeout * 1e9)))
            body = self._native.command(65, 1, sections, "typed_document_upsert_versions")
            inserted, updated = _typed_upsert_response(body, index_info.generation, len(ids))
            return UpsertDocumentsResponse(index=index_info, upserted=len(ids), inserted=inserted, updated=updated, ids=ids)

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

        if self._native is not None:
            raise UnsupportedError("unsupported", "native delete is not implemented")
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

        if self._native is not None:
            raise UnsupportedError("unsupported", "native filter delete is not implemented")
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
        query_mode: Optional[str] = None,
        quantized_index_name: Optional[str] = None,
        quantized_rerank_candidates: Optional[int] = None,
        return_embedding: bool = False,
        expected_generation: Optional[int] = None,
        index_info: Optional[IndexInfo] = None,
    ) -> DenseVectorSearchResponse:
        """Score a query embedding through the TreeDB dense search route.

        Selected typed column_graph indexes accept an omitted route or
        ``"ann"`` after explicit admission, including declared string equality/range
        leaves joined by AND. ``dense_work.graph.route`` identifies executed
        empty, typed exact (complete eligible sets up to 4096), or HNSW work
        independently of the public ``ann`` tag. Selected ``"exact"`` rejects rather than scanning.
        Legacy ``"exact"`` scans documents and applies an optional filter;
        compatible native_runtime ANN also supports declared scalar filters.
        Unsupported ANN shapes fail closed without a document-scan fallback.
        Embedding echo is opt-in via ``return_embedding=True`` on either
        transport. Native dense requires caller-held selected typed IndexInfo.
        """

        if route is not None and route not in ("ann", "exact"):
            raise InvalidRequestError("invalid_request", f"unsupported dense search route {route!r}; use 'ann' or 'exact'")
        top_k_value = _validate_binary_int_query_param(top_k, "top_k", minimum=1)
        mode = "exact" if query_mode is None else query_mode
        if not isinstance(mode, str) or mode.strip() == "":
            raise InvalidRequestError("invalid_request", "query_mode must be a non-empty string when provided")
        mode = mode.strip().lower()
        if mode not in ("exact", "quantized_rerank"):
            raise UnsupportedError("unsupported", f"unsupported dense query_mode {mode!r}")
        if quantized_index_name is not None and (not isinstance(quantized_index_name, str) or not quantized_index_name):
            raise InvalidRequestError("invalid_request", "quantized_index_name must be a non-empty string when provided")
        rerank_value = 0 if quantized_rerank_candidates is None else _validate_binary_int_query_param(
            quantized_rerank_candidates, "quantized_rerank_candidates", minimum=0
        )
        if mode == "exact" and (quantized_index_name is not None or quantized_rerank_candidates is not None):
            raise InvalidRequestError("invalid_request", "exact dense search does not accept quantized options")
        if mode == "quantized_rerank":
            if not quantized_index_name:
                raise InvalidRequestError("invalid_request", "quantized_rerank requires quantized_index_name")
            if rerank_value and rerank_value < top_k_value:
                raise InvalidRequestError("invalid_request", "quantized_rerank_candidates must be zero or at least top_k")
            if index_info is not None:
                if index_info.name != index or not index_info.capabilities.typed_dense_quantized_rerank:
                    raise TreeDBConfigError("typed dense quantized rerank capability is unavailable")
                selected = next((item for item in index_info.quantized_indexes if item.name == quantized_index_name), None)
                if not _is_legacy_scalar_u8_v1_index(selected):
                    raise TreeDBConfigError("typed dense quantized rerank requires the selected legacy scalar_u8/v1 index")
        ef_search_value = None
        if ef_search is not None:
            ef_search_value = _validate_binary_int_query_param(ef_search, "ef_search", minimum=0)
        if self._native is not None:
            from ._native import _dense_request, _dense_response, _section, _uint
            if (index_info is None or index_info.name != index or index_info.extra.get("typed_input") is not True
                    or index_info.vector_strategy != "column_graph" or index_info.metric != "cosine"
                    or index_info.generation <= 0 or len(query_embedding) != index_info.dimension):
                raise TreeDBConfigError("native dense search requires matching selected typed IndexInfo")
            if mode == "quantized_rerank":
                if not index_info.capabilities.typed_dense_quantized_rerank:
                    raise TreeDBConfigError("native dense quantized rerank capability is unavailable")
                selected = next((item for item in index_info.quantized_indexes if item.name == quantized_index_name), None)
                if not _is_legacy_scalar_u8_v1_index(selected):
                    raise TreeDBConfigError("native dense quantized rerank requires the selected legacy scalar_u8/v1 index")
            if route not in (None, "ann") or (expected_generation is not None and expected_generation != index_info.generation):
                raise TreeDBConfigError("native dense route or generation conflicts with IndexInfo")
            version = 3 if mode == "quantized_rerank" else 2
            payload = _dense_request(index, query_embedding, top_k_value, ef_search_value or 0, index_info.generation, return_embedding, normalize_filter(filter) if filter is not None else None)
            deadline = _uint(time.time_ns() + int(self.timeout * 1_000_000_000))
            sections = _section(129, payload) + _section(4, deadline)
            if version == 3:
                from ._native import _dense_quantized_options
                sections += _section(135, _dense_quantized_options(mode, quantized_index_name, rerank_value))
            raw = self._native.command(64, version, sections, "dense_vector_search_versions")
            decoded = _dense_response(
                raw,
                top_k_value,
                version=version,
                query_mode=mode,
                quantized_index_name=quantized_index_name,
                quantized_rerank_candidates=rerank_value,
                ef_search=ef_search_value or 0,
                query_dimension=len(query_embedding),
                expected_generation=index_info.generation,
                filter_requested=filter is not None,
            )
            if version == 3:
                ids, payloads, scores, candidates, work, score_plane = decoded
            else:
                ids, payloads, scores, candidates, work = decoded
                score_plane = None
            documents = []
            for item_id, document_raw, score in zip(ids, payloads, scores):
                try:
                    document = Document.from_dict(json.loads(document_raw))
                    if document.id.encode("utf-8") != item_id:
                        raise ValueError("document ID mismatch")
                    document.score = score
                except (ValueError, KeyError, TypeError, OverflowError) as exc:
                    raise TreeDBProtocolError("invalid native dense document", dense_work=work, score_plane=score_plane) from exc
                documents.append(document)
            return DenseVectorSearchResponse(index=index_info, documents=documents, metric=index_info.metric,
                                             exact=False, candidates=candidates, route="ann",
                                             native_base_plus_live_delta=False, native_command_version=version, dense_work=work,
                                             score_plane=score_plane)
        request: dict[str, Any] = {
            "query_embedding": [float(value) for value in query_embedding],
            "top_k": top_k_value,
            "return_embedding": return_embedding,
        }
        if route is not None:
            request["route"] = route
        if ef_search_value is not None:
            request["ef_search"] = ef_search_value
        if query_mode is not None:
            request["query_mode"] = mode
        if mode == "quantized_rerank":
            request["quantized_index_name"] = quantized_index_name
            if rerank_value:
                request["quantized_rerank_candidates"] = rerank_value
        _add_filter(request, filter)
        _add_expected_generation(request, expected_generation)
        payload = self._request(
            "POST",
            self._index_path(index, "search", "vector"),
            request,
            dense_proof=True,
            dense_score_plane=mode == "quantized_rerank",
        )
        response = _parse_response("vector search response", DenseVectorSearchResponse.from_dict, payload)
        if mode == "quantized_rerank":
            _validate_http_dense_quantized_response(
                response,
                index=index,
                top_k=top_k_value,
                ef_search=ef_search_value or 0,
                query_dimension=len(request["query_embedding"]),
                quantized_index_name=quantized_index_name,
                quantized_rerank_candidates=rerank_value,
                expected_generation=expected_generation,
                filter_requested=filter is not None,
            )
        elif response.score_plane is not None:
            raise TreeDBProtocolError(
                "dense HTTP exact response unexpectedly includes a score-plane proof",
                dense_work=response.dense_work,
                score_plane=response.score_plane,
            )
        return response

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
        self,
        method: str,
        path: str,
        body: Optional[Mapping[str, Any]] = None,
        *,
        retry_broken_connection: bool = False,
        dense_proof: bool = False,
        dense_score_plane: bool = False,
    ) -> Any:
        data: Optional[bytes] = None
        headers = {"Accept": "application/json"}
        if body is not None:
            try:
                data = json.dumps(body, allow_nan=False, separators=(",", ":")).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise InvalidRequestError("invalid_request", f"request payload is not JSON-serializable: {exc}") from exc
            headers["Content-Type"] = "application/json"
        return self._send_request(
            method,
            path,
            data,
            headers,
            retry_broken_connection=retry_broken_connection,
            dense_proof=dense_proof,
            dense_score_plane=dense_score_plane,
        )

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
        self,
        method: str,
        path: str,
        data: Optional[bytes],
        headers: Mapping[str, str],
        *,
        retry_broken_connection: bool = False,
        dense_proof: bool = False,
        dense_score_plane: bool = False,
    ) -> Any:
        url = self.base_url + path
        if not retry_broken_connection or self._benchmark_uses_proxy:
            for attempt in range(2 if retry_broken_connection else 1):
                request = urllib.request.Request(url, data=data, headers=dict(headers), method=method)
                try:
                    with self._opener.open(request, timeout=self.timeout) as response:
                        response_body = response.read()
                        return self._decode_success(
                            response.getcode(),
                            response_body,
                            dense_proof=dense_proof,
                            dense_score_plane=dense_score_plane,
                        )
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
                    raise self._decode_error(
                        exc.code,
                        response_body,
                        dense_proof=dense_proof,
                        dense_score_plane=dense_score_plane,
                    ) from None
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
                        return self._decode_success(
                            response.status,
                            response_body,
                            dense_proof=dense_proof,
                            dense_score_plane=dense_score_plane,
                        )
                    raise self._decode_error(
                        response.status,
                        response_body,
                        dense_proof=dense_proof,
                        dense_score_plane=dense_score_plane,
                    )
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

    def _decode_success(
        self,
        status_code: int,
        body: bytes,
        *,
        dense_proof: bool = False,
        dense_score_plane: bool = False,
    ) -> Any:
        decoded = _decode_json_body(body, status_code=status_code, dense_proof=dense_proof)
        if isinstance(decoded, Mapping) and "error" in decoded:
            error = decoded.get("error")
            if isinstance(error, Mapping):
                code = str(error.get("code", "internal"))
                message = str(error.get("message", ""))
                work, score_plane = (
                    _error_dense_proofs(error, score_plane_allowed=dense_score_plane)
                    if dense_proof
                    else (None, None)
                )
                raise service_error_from_code(code, message, status_code=status_code, response_body=_body_to_text(body), dense_work=work, score_plane=score_plane)
            raise TreeDBProtocolError("error envelope must contain an object", status_code=status_code, response_body=_body_to_text(body))
        return decoded

    def _decode_error(
        self,
        status_code: int,
        body: bytes,
        *,
        dense_proof: bool = False,
        dense_score_plane: bool = False,
    ) -> Exception:
        decoded = _decode_json_body(body, status_code=status_code, dense_proof=dense_proof)
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
        work, score_plane = (
            _error_dense_proofs(error, score_plane_allowed=dense_score_plane)
            if dense_proof
            else (None, None)
        )
        return service_error_from_code(code, message, status_code=status_code, response_body=_body_to_text(body), dense_work=work, score_plane=score_plane)


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
    if value >= 1 << 63:
        raise InvalidRequestError("invalid_request", f"{label} is outside the native integer range")
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


def _validate_http_dense_quantized_response(
    response: DenseVectorSearchResponse,
    *,
    index: str,
    top_k: int,
    ef_search: int,
    query_dimension: int,
    quantized_index_name: str,
    quantized_rerank_candidates: int,
    expected_generation: Optional[int],
    filter_requested: bool,
) -> None:
    """Require the HTTP response proof for an explicitly selected public route."""

    from ._dense_work import (
        dense_cosine_scores_valid,
        dense_document_ids_valid,
        dense_quantized_response_work_matches,
        dense_score_plane_byte_counters_match,
    )

    proof = response.score_plane
    work = response.dense_work
    selected = next((item for item in response.index.quantized_indexes if item.name == quantized_index_name), None)
    expected_graph_route = {
        "typed_empty": "typed_empty",
        "typed_exact": "typed_exact",
        "quantized_rerank": "typed_hnsw",
    }.get(proof.route if proof is not None else "")
    if (
        proof is None
        or work is None
        or not work.completed
        or not work.graph.available
        or not work.graph.completed
        or expected_graph_route is None
        or work.graph.route != expected_graph_route
        or work.graph.snapshot != proof.snapshot
        or work.graph.filter.attempted != filter_requested
        or (work.graph.filter.attempted and not work.graph.filter.completed)
        or (filter_requested and len(response.documents) != min(top_k, work.graph.filter.eligible_rows))
        or len(response.documents) > top_k
        or response.candidates != len(response.documents)
        or not dense_document_ids_valid(document.id for document in response.documents)
        or any(document.score is None for document in response.documents)
        or not dense_cosine_scores_valid(document.score for document in response.documents)
        or not _dense_http_results_ordered(response.documents)
        or response.index.dimension != query_dimension
        or type(response.index.generation) is not int
        or not 0 < response.index.generation < 1 << 64
        or proof.snapshot.schema_generation > response.index.generation
        or not dense_score_plane_byte_counters_match(proof, query_dimension)
        or not dense_quantized_response_work_matches(
            work, proof, top_k, len(response.documents), filter_requested
        )
        or response.index.name != index
        or (expected_generation is not None and response.index.generation != expected_generation)
        or response.metric != "cosine"
        or response.metric != response.index.metric
        or response.index.metric != "cosine"
        or response.index.vector_strategy != "column_graph"
        or response.index.extra.get("typed_input") is not True
        or not response.index.capabilities.typed_dense_quantized_rerank
        or response.route != "ann"
        or response.exact
        or response.native_base_plus_live_delta
        or response.exact_fallbacks != 0
        or response.full_document_scan_fallbacks != 0
        or response.primary_document_scans != 0
        or response.document_materialization_rows != len(response.documents)
        or response.scalar_filter_membership_source != ""
        or response.scalar_filter_plan != ""
        or response.scalar_filter_exact_scoring
        or response.scalar_filter_underfill
        or any((
            response.scalar_filter_probe_ids,
            response.scalar_filter_probe_truncated,
            response.scalar_filter_candidates,
            response.scalar_filter_candidate_ids,
            response.scalar_filter_retained_candidate_ids,
            response.scalar_filter_refined_candidate_ids,
            response.scalar_filter_visited,
            response.scalar_filter_scored,
            response.scalar_filter_admitted,
            response.scalar_filter_unbounded,
            response.allowed_id_materialization_rows,
            response.visibility_mismatch_count,
            response.visibility_retry_count,
        ))
        or (proof.route == "typed_empty" and (
            len(response.documents) != 0
            or not work.graph.filter.attempted
            or not work.graph.filter.completed
            or work.graph.filter.eligible_rows != 0
        ))
        or (proof.route in ("typed_exact", "quantized_rerank") and len(response.documents) != min(top_k, proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls + proof.exact_suffix_score_calls))
        or (proof.route == "quantized_rerank" and (
            proof.actual_rerank_candidates > proof.rerank_candidate_cap
            or proof.live_shortlist_candidates > proof.raw_retained_candidates
            or proof.raw_retained_candidates > proof.raw_candidate_width
            or proof.actual_rerank_candidates > proof.live_shortlist_candidates
            or (quantized_rerank_candidates and proof.rerank_candidate_cap > quantized_rerank_candidates)
        ))
        or not proof.available
        or not proof.completed
        or not proof.snapshot.available
        or proof.requested_mode != "quantized_rerank"
        or proof.effective_mode != "quantized_rerank"
        or proof.route not in ("typed_empty", "typed_exact", "quantized_rerank")
        or proof.quantized_index_name != quantized_index_name
        or proof.quantized_codec != "scalar_u8"
        or proof.quantized_version != 1
        or not _is_legacy_scalar_u8_v1_index(selected)
        or proof.requested_top_k != top_k
        or proof.requested_ef_search != ef_search
        or proof.requested_rerank_candidates != quantized_rerank_candidates
    ):
        raise TreeDBProtocolError(
            "dense HTTP score-plane proof does not match the request",
            dense_work=response.dense_work,
            score_plane=proof,
        )


def _dense_http_results_ordered(documents: Sequence[Document]) -> bool:
    for previous, current in zip(documents, documents[1:]):
        if previous.score is None or current.score is None:
            return False
        try:
            previous_id = previous.id.encode("utf-8", errors="strict")
            current_id = current.id.encode("utf-8", errors="strict")
        except UnicodeError:
            return False
        if previous.score < current.score or (previous.score == current.score and previous_id >= current_id):
            return False
    return True


def _is_legacy_scalar_u8_v1_index(selected: Any) -> bool:
    if selected is None or selected.codec != "scalar_u8" or selected.version != 1:
        return False
    calibration = selected.scalar_u8_calibration
    if calibration is None:
        return True

    def field(value: Any, name: str, default: Any) -> Any:
        return value.get(name, default) if isinstance(value, Mapping) else getattr(value, name, default)

    policy = field(calibration, "alpha_policy", None)
    return (
        field(calibration, "mode", "") in ("", "legacy")
        and field(calibration, "grouping", "") == ""
        and field(policy, "name", "") == ""
        and field(policy, "quantile_ppm", 0) == 0
    )


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


def _decode_json_body(body: bytes, *, status_code: int, dense_proof: bool = False) -> Any:
    text = _body_to_text(body)
    try:
        if not dense_proof:
            return json.loads(text) if text else {}
        duplicate = False
        proof_envelope = False

        def object_pairs(pairs):
            nonlocal duplicate, proof_envelope
            # The root object is decoded last. Inspect pairs before duplicate
            # keys collapse, including overwritten proof-bearing error keys.
            proof_envelope = any(key in ("dense_work", "score_plane") or (key == "error" and isinstance(value, dict) and ("dense_work" in value or "score_plane" in value))
                                 for key, value in pairs)
            out = {}
            for key, value in pairs:
                duplicate |= key in out
                out[key] = value
            return out

        decoded = json.loads(text, object_pairs_hook=object_pairs) if text else {}
        if isinstance(decoded, dict) and proof_envelope and duplicate:
            raise TreeDBProtocolError("duplicate field in dense proof envelope", status_code=status_code, response_body=text)
        return decoded
    except json.JSONDecodeError as exc:
        raise TreeDBProtocolError(
            f"TreeDB service returned malformed JSON for HTTP {status_code}: {exc}",
            status_code=status_code,
            response_body=text,
        ) from exc


def _error_dense_proofs(error, *, score_plane_allowed):
    from ._dense_work import optional_dense_score_plane, optional_dense_work
    work = score_plane = None
    work_error = score_plane_error = None
    try:
        work = optional_dense_work(error.get("dense_work"))
    except (ValueError, TypeError, KeyError) as exc:
        work_error = exc
    raw_score_plane = error.get("score_plane")
    unexpected_score_plane = raw_score_plane is not None and not score_plane_allowed
    if raw_score_plane is not None and score_plane_allowed:
        try:
            score_plane = optional_dense_score_plane(raw_score_plane)
        except (ValueError, TypeError, KeyError) as exc:
            score_plane_error = exc
    if work_error is not None:
        raise TreeDBProtocolError("invalid dense error work proof", score_plane=score_plane) from work_error
    if unexpected_score_plane:
        raise TreeDBProtocolError("dense HTTP exact error unexpectedly includes a score-plane proof", dense_work=work)
    if score_plane_error is not None:
        raise TreeDBProtocolError("invalid dense score-plane error proof", dense_work=work) from score_plane_error
    return work, score_plane


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
