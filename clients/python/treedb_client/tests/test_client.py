from __future__ import annotations

import base64
import copy
import http.client
import json
import os
import ssl
import struct
import threading
import time
import urllib.error
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple
from unittest import mock

import _support  # noqa: F401
from treedb_client import (
    BenchmarkVectorIndexOptions,
    Document,
    HybridFusionOptions,
    IndexNotFoundError,
    IndexStaleError,
    IndexUnavailableError,
    InvalidRequestError,
    QuantizedIndexInfo,
    SnapshotMismatchError,
    ScalarFieldDeclaration,
    TreeDBClient,
    TreeDBConfigError,
    TreeDBProtocolError,
    TreeDBTimeoutError,
    TreeDBTransportError,
    UnsupportedError,
)
from treedb_client.client import _is_legacy_scalar_u8_v1_index


SAMPLE_INDEX = {
    "name": "docs",
    "dimension": 2,
    "metric": "cosine",
    "generation": 1,
    "contract_version": "treedb-document-service/v1alpha2",
    "embedding_field": "embedding",
    "vector_index_name": "embedding",
    "vector_strategy": "column_graph",
    "vector_m": 16,
    "vector_ef_construction": 128,
    "vector_ef_search": 64,
    "quantized_indexes": [{"name": "embedding.scalar_u8.fast", "codec": "scalar_u8", "version": 1}],
    "text_field": "content",
    "text_index_name": "content",
    "document_type": "treedb_document_service_v1",
    "capabilities": {
        "dense_vector_search": True,
        "exact_dense_scoring": True,
        "metadata_filters": True,
        "keyword_search": True,
        "hybrid_search": True,
        "keyword_metadata_filters": False,
        "hybrid_metadata_filters": False,
        "benchmark_lifecycle": True,
        "vector_index_maintenance": True,
        "no_document_vector_search": True,
        "column_graph_vector_search": True,
        "exact_column_graph_search": True,
        "quantized_vector_search": True,
        "quantized_rerank": True,
        "scalar_u8_quantized_rerank": True,
        "rabitq_1bit_experimental": False,
    },
}


class FixtureHandler(BaseHTTPRequestHandler):
    server_version = "TreeDBClientFixture/1.0"
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self.server.accepted_connections += 1  # type: ignore[attr-defined]

    def do_GET(self) -> None:  # noqa: N802
        self._serve()

    def do_POST(self) -> None:  # noqa: N802
        self._serve()

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _serve(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        route = (self.command, self.path)
        self.server.records.append(  # type: ignore[attr-defined]
            {"method": self.command, "path": self.path, "body": body, "headers": dict(self.headers)}
        )
        if route in self.server.drop_once_routes:  # type: ignore[attr-defined]
            self.server.drop_once_routes.remove(route)  # type: ignore[attr-defined]
            self.close_connection = True
            return
        status, payload, delay = self.server.routes.get(  # type: ignore[attr-defined]
            route, (404, {"error": {"code": "index_not_found", "message": "missing route"}}, 0)
        )
        if delay:
            time.sleep(delay)
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except OSError:
            pass


class FixtureServer:
    def __init__(self, routes: Dict[Tuple[str, str], Tuple[int, Any, float]], prefix: str = "", drop_once_routes: set[Tuple[str, str]] | None = None) -> None:
        self.routes = routes
        self.prefix = prefix
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        self.httpd.routes = routes  # type: ignore[attr-defined]
        self.httpd.records = []  # type: ignore[attr-defined]
        self.httpd.accepted_connections = 0  # type: ignore[attr-defined]
        self.httpd.drop_once_routes = set(drop_once_routes or ())  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self) -> "FixtureServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address
        return f"http://{host}:{port}{self.prefix}"

    @property
    def records(self) -> list[dict[str, Any]]:
        return self.httpd.records  # type: ignore[attr-defined]

    @property
    def accepted_connections(self) -> int:
        return self.httpd.accepted_connections  # type: ignore[attr-defined]


def json_body(record: dict[str, Any]) -> Any:
    return json.loads(record["body"].decode("utf-8"))


class TreeDBClientTests(unittest.TestCase):
    def test_selected_quantized_index_requires_legacy_calibration(self) -> None:
        selected = QuantizedIndexInfo(name="embedding.scalar_u8.public")
        self.assertTrue(_is_legacy_scalar_u8_v1_index(selected))
        self.assertTrue(_is_legacy_scalar_u8_v1_index(QuantizedIndexInfo(
            name=selected.name,
            scalar_u8_calibration={"mode": "legacy"},
        )))
        for calibration in (
            {"mode": "legacy", "grouping": "per_granule"},
            {"mode": "legacy", "alpha_policy": {"name": "quantile", "quantile_ppm": 999000}},
        ):
            with self.subTest(calibration=calibration):
                self.assertFalse(_is_legacy_scalar_u8_v1_index(QuantizedIndexInfo(
                    name=selected.name,
                    scalar_u8_calibration=calibration,
                )))

    def test_vector_index_proxy_selection_preserves_urllib_and_bypass_uses_direct_connection(self) -> None:
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}

        class Response:
            status = 200

            def read(self) -> bytes:
                return json.dumps(response).encode("utf-8")

            def close(self) -> None:
                return

            def getcode(self) -> int:
                return self.status

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                self.close()

        class Opener:
            def __init__(self, fail_once: bool = False) -> None:
                self.calls = 0
                self.fail_once = fail_once

            def open(self, request: Any, timeout: float | None = None) -> Response:
                self.calls += 1
                if self.fail_once and self.calls == 1:
                    raise urllib.error.URLError(ConnectionResetError("connection reset by proxy"))
                return Response()

        class DirectConnection:
            calls = 0

            def request(self, method: str, path: str, body: Any = None, headers: Any = None) -> None:
                self.calls += 1

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return

        proxy_env = {"http_proxy": "http://proxy.example:8080", "HTTP_PROXY": "http://proxy.example:8080", "no_proxy": "", "NO_PROXY": ""}
        with mock.patch.dict(os.environ, proxy_env):
            client = TreeDBClient("http://treedb.example", timeout=1)
            opener = Opener(fail_once=True)
            direct = DirectConnection()
            client._opener = opener  # type: ignore[attr-defined]
            client._connection = direct  # type: ignore[assignment]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(opener.calls, 2)
            self.assertEqual(direct.calls, 0)
            client.close()

        with mock.patch.dict(os.environ, {**proxy_env, "no_proxy": "treedb.example", "NO_PROXY": "treedb.example"}):
            client = TreeDBClient("http://treedb.example", timeout=1)
            opener = Opener()
            direct = DirectConnection()
            client._opener = opener  # type: ignore[attr-defined]
            client._connection = direct  # type: ignore[assignment]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(opener.calls, 0)
            self.assertEqual(direct.calls, 1)
            client.close()

        with mock.patch.dict(os.environ, {**proxy_env, "no_proxy": "treedb.example:7120", "NO_PROXY": "treedb.example:7120"}):
            client = TreeDBClient("http://treedb.example:7120", timeout=1)
            opener = Opener()
            direct = DirectConnection()
            client._opener = opener  # type: ignore[attr-defined]
            client._connection = direct  # type: ignore[assignment]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(opener.calls, 0)
            self.assertEqual(direct.calls, 1)
            client.close()

        with mock.patch.dict(os.environ, {"all_proxy": "http://proxy.example:8080"}, clear=True):
            client = TreeDBClient("http://treedb.example", timeout=1)
            opener = Opener()
            direct = DirectConnection()
            client._opener = opener  # type: ignore[attr-defined]
            client._connection = direct  # type: ignore[assignment]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(opener.calls, 0)
            self.assertEqual(direct.calls, 1)
            client.close()

    def test_default_ports_handle_bracketed_ipv6(self) -> None:
        http_client = TreeDBClient("http://[::1]")
        https_client = TreeDBClient("https://[::1]")

        self.assertEqual((http_client._connection.host, http_client._connection.port), ("::1", 80))
        self.assertEqual((https_client._connection.host, https_client._connection.port), ("::1", 443))
        http_client.close()
        https_client.close()

    def test_reuses_connection_and_close_reconnects(self) -> None:
        route = ("POST", "/v1/indexes/docs/search/vector-index")
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}
        with FixtureServer({route: (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(server.accepted_connections, 1)
            client.close()
            self.assertIsNone(client._connection.sock)  # type: ignore[attr-defined]
            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(server.accepted_connections, 2)
            client.close()

    def test_ordinary_requests_remain_independent(self) -> None:
        with FixtureServer({("GET", "/v1/health"): (200, {"ok": True}, 0.05)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            start = threading.Barrier(3)
            errors: list[Exception] = []

            def health() -> None:
                start.wait()
                try:
                    client.health()
                except Exception as exc:  # pragma: no cover - assertion below reports it
                    errors.append(exc)

            first = threading.Thread(target=health)
            second = threading.Thread(target=health)
            first.start()
            second.start()
            start.wait()
            first.join(timeout=2)
            second.join(timeout=2)
            self.assertFalse(errors)
            self.assertEqual(server.accepted_connections, 2)
            client.close()

    def test_vector_index_search_retries_once_after_connection_break(self) -> None:
        route = ("POST", "/v1/indexes/docs/search/vector-index")
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}
        with FixtureServer({route: (200, response, 0)}, drop_once_routes={route}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(len(server.records), 2)
            client.close()

    def test_vector_index_search_retries_once_after_tls_eof(self) -> None:
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}

        class Response:
            status = 200

            def read(self) -> bytes:
                return json.dumps(response).encode("utf-8")

            def close(self) -> None:
                return

        class Connection:
            def __init__(self) -> None:
                self.calls = 0

            def request(self, method: str, path: str, body: Any = None, headers: Any = None) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return

        with mock.patch.dict(os.environ, {}, clear=True):
            client = TreeDBClient("https://treedb.example", timeout=1)
            connection = Connection()
            client._connection = connection  # type: ignore[assignment]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(connection.calls, 2)
            client.close()

    def test_proxy_vector_index_retries_incomplete_response_once(self) -> None:
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}

        class Response:
            status = 200

            def __init__(self, incomplete: bool = False) -> None:
                self.incomplete = incomplete

            def read(self) -> bytes:
                if self.incomplete:
                    raise http.client.IncompleteRead(b"", 1)
                return json.dumps(response).encode("utf-8")

            def getcode(self) -> int:
                return self.status

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return

        class Opener:
            def __init__(self, always_incomplete: bool = False) -> None:
                self.calls = 0
                self.always_incomplete = always_incomplete

            def open(self, request: Any, timeout: float | None = None) -> Response:
                self.calls += 1
                return Response(incomplete=self.always_incomplete or self.calls == 1)

        with mock.patch.dict(os.environ, {"http_proxy": "http://proxy.example:8080"}, clear=True):
            client = TreeDBClient("http://treedb.example", timeout=1)
            opener = Opener()
            client._opener = opener  # type: ignore[attr-defined]

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(opener.calls, 2)
            client.close()

            client = TreeDBClient("http://treedb.example", timeout=1)
            opener = Opener(always_incomplete=True)
            client._opener = opener  # type: ignore[attr-defined]

            with self.assertRaises(TreeDBTransportError):
                client.search_vector_index("docs", [1, 0], 1)
            self.assertEqual(opener.calls, 2)
            client.close()

    def test_http_error_body_read_failure_maps_to_transport_error(self) -> None:
        class BrokenHTTPError(urllib.error.HTTPError):
            def read(self) -> bytes:
                raise http.client.IncompleteRead(b"", 1)

        class Opener:
            def open(self, request: Any, timeout: float | None = None) -> Any:
                raise BrokenHTTPError(request.full_url, 503, "unavailable", {}, None)

        client = TreeDBClient("http://treedb.example", timeout=1)
        client._opener = Opener()  # type: ignore[attr-defined]

        with self.assertRaises(TreeDBTransportError):
            client.health()
        client.close()

    def test_write_is_not_replayed_after_connection_break(self) -> None:
        route = ("POST", "/v1/indexes")
        with FixtureServer({route: (200, {"index": SAMPLE_INDEX}, 0)}, drop_once_routes={route}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            with self.assertRaises(TreeDBTransportError):
                client.create_index("docs", 2)
            self.assertEqual(len(server.records), 1)
            client.close()
    def test_vector_index_compact_ids_response(self) -> None:
        with FixtureServer({("POST", "/v1/indexes/docs/search/vector-index"): (200, {"response_format": "ids", "ids": ["doc-1", "doc-2"]}, 0)}) as server:
            response = TreeDBClient(server.base_url, timeout=1).search_vector_index("docs", [1, 0], 2, response_format="ids")

            self.assertEqual(response.ids, ["doc-1", "doc-2"])
            self.assertEqual(json_body(server.records[0])["response_format"], "ids")

    def test_create_index_posts_contract_payload(self) -> None:
        with FixtureServer({("POST", "/v1/indexes"): (200, {"index": SAMPLE_INDEX}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            info = client.create_index("docs", 2)

            self.assertEqual(info.name, "docs")
            self.assertEqual(json_body(server.records[0]), {"name": "docs", "dimension": 2, "metric": "cosine"})
            self.assertEqual(server.records[0]["headers"]["Content-Type"], "application/json")

    def test_create_index_serializes_scalar_field_declarations(self) -> None:
        with FixtureServer({("POST", "/v1/indexes"): (200, {"index": SAMPLE_INDEX}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            client.create_index(
                "docs",
                2,
                scalar_fields=[
                    ScalarFieldDeclaration("meta.repo"),
                    {"field": "priority", "value_type": "int64"},
                ],
            )
            self.assertEqual(
                json_body(server.records[0])["scalar_fields"],
                [
                    {"field": "meta.repo", "value_type": "string"},
                    {"field": "priority", "value_type": "int64"},
                ],
            )


    def test_malformed_index_response_maps_to_protocol_error(self) -> None:
        malformed_index = dict(SAMPLE_INDEX)
        del malformed_index["dimension"]
        with FixtureServer({("POST", "/v1/indexes"): (200, {"index": malformed_index}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            with self.assertRaisesRegex(TreeDBProtocolError, "index response is malformed"):
                client.create_index("docs", 2)

    def test_base_url_path_prefix_is_preserved(self) -> None:
        with FixtureServer({("GET", "/api/v1/health"): (200, {"ok": True}, 0)}, prefix="/api") as server:
            client = TreeDBClient(server.base_url + "/", timeout=1)

            self.assertEqual(client.health()["ok"], True)
            self.assertEqual(server.records[0]["path"], "/api/v1/health")

    def test_base_url_path_params_are_preserved(self) -> None:
        route = ("POST", "/api;v=1/v1/indexes/docs/search/vector-index")
        response = {"index": SAMPLE_INDEX, "results": [], "metric": "cosine", "vector_index_name": "embedding", "query_mode": "exact", "no_documents": True, "stats": {}, "diagnostics": {}}
        with FixtureServer({route: (200, response, 0)}, prefix="/api;v=1") as server:
            client = TreeDBClient(server.base_url, timeout=1)

            self.assertEqual(client.search_vector_index("docs", [1, 0], 1).results, [])
            self.assertEqual(server.records[0]["path"], route[1])
            client.close()

    def test_upsert_documents_omits_response_score_from_write_payload(self) -> None:
        route = "/v1/indexes/docs/documents/upsert"
        response = {"index": SAMPLE_INDEX, "upserted": 1, "inserted": 1, "updated": 0, "ids": ["a"]}
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.upsert_documents(
                "docs",
                [Document(id="a", content="alpha", embedding=[1, 0], meta={"repo": "gomap"}, score=0.5)],
                expected_generation=1,
                defer_vector_index_rebuild=True,
            )

            self.assertEqual(result.upserted, 1)
            body = json_body(server.records[0])
            self.assertEqual(body["expected_generation"], 1)
            self.assertEqual(body["defer_vector_index_rebuild"], True)
            self.assertNotIn("score", body["documents"][0])
            self.assertEqual(body["documents"][0]["embedding"], [1.0, 0.0])

    def test_upsert_documents_emits_compact_embedding_from_models_and_mappings(self) -> None:
        route = "/v1/indexes/docs/documents/upsert"
        response = {
            "index": SAMPLE_INDEX,
            "upserted": 2,
            "inserted": 2,
            "updated": 0,
            "ids": ["model", "mapping"],
            "compact_embeddings": 2,
        }
        encoded = base64.b64encode(struct.pack("<2f", 1.0, -2.0)).decode("ascii")
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.upsert_documents(
                "docs",
                [
                    Document(id="model", embedding_f32_le_b64=encoded),
                    {"id": "mapping", "embedding_f32_le_b64": encoded},
                ],
            )

            self.assertEqual(result.compact_embeddings, 2)
            documents = json_body(server.records[0])["documents"]
            self.assertEqual(documents[0], {"id": "model", "embedding_f32_le_b64": encoded})
            self.assertEqual(documents[1], {"id": "mapping", "embedding_f32_le_b64": encoded})

    def test_count_filter_search_and_delete_by_filter_parse_responses(self) -> None:
        routes = {
            ("POST", "/v1/indexes/docs/documents/count"): (200, {"index": SAMPLE_INDEX, "count": 2}, 0),
            ("POST", "/v1/indexes/docs/documents/filter"): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "matched_count": 1,
                    "documents": [{"id": "a", "content": "alpha", "meta": {"repo": "gomap"}}],
                    "next_after_id": "a",
                    "exhausted": False,
                },
                0,
            ),
            ("POST", "/v1/indexes/docs/search/vector"): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "metric": "cosine",
                    "exact": True,
                    "candidates": 1,
                    "documents": [{"id": "a", "content": "alpha", "score": 1.0}],
                },
                0,
            ),
            ("POST", "/v1/indexes/docs/documents/delete"): (
                200,
                {"index": SAMPLE_INDEX, "deleted": 1, "ids": ["a"]},
                0,
            ),
        }
        with FixtureServer(routes) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            self.assertEqual(client.count_documents("docs", {"field": "meta.repo", "operator": "==", "value": "gomap"}).count, 2)
            filtered = client.filter_documents("docs", limit=10, after_id="before", cursor_page=True)
            self.assertEqual(filtered.documents[0].id, "a")
            self.assertEqual(filtered.next_after_id, "a")
            self.assertFalse(filtered.exhausted)
            self.assertEqual(client.query_by_embedding("docs", [1, 0], 1).documents[0].score, 1.0)
            self.assertEqual(client.delete_by_filter("docs", {"field": "meta.repo", "operator": "$eq", "value": "gomap"}).deleted, 1)

            delete_body = json_body(server.records[-1])
            self.assertEqual(delete_body["filter"], {"field": "meta.repo", "operator": "==", "value": "gomap"})
            filter_body = json_body(server.records[1])
            self.assertEqual(filter_body["after_id"], "before")
            self.assertTrue(filter_body["cursor_page"])

    def test_http_quantized_query_requires_completed_matching_score_plane(self) -> None:
        payload = {
            "index": SAMPLE_INDEX,
            "metric": "cosine",
            "exact": False,
            "candidates": 1,
            "route": "ann",
            "documents": [{"id": "a", "content": "alpha", "score": 1.0}],
        }
        with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, payload, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                client.query_by_embedding(
                    "docs",
                    [1, 0],
                    1,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.fast",
                )

    def test_http_quantized_query_binds_outer_response_and_dense_work(self) -> None:
        typed_index = copy.deepcopy(SAMPLE_INDEX)
        typed_index["typed_input"] = True
        typed_index["quantized_indexes"] = [{"name": "embedding.scalar_u8.public", "codec": "scalar_u8", "version": 1}]
        typed_index["capabilities"]["typed_dense_quantized_rerank"] = True
        manifest = {"generation": 1, "format": "", "version": 0, "checksum": 0}
        snapshot = {
            "available": True,
            "schema_hash": 1,
            "schema_generation": 1,
            "base_manifest": manifest,
            "current_manifest": manifest,
            "base_coverage_lsn": 1,
            "current_coverage_lsn": 1,
        }
        dense_work = {
            "version": 1,
            "completed": True,
            "graph": {
                "available": True,
                "completed": True,
                "route": "typed_hnsw",
                "base_ann_scored": 1,
                "base_candidates": 1,
                "base_edges": 0,
                "delta_scored": 0,
                "exact_base_scored": 1,
                "base_shadowed": 0,
                "base_result_ids": 1,
                "filter": {
                    "attempted": False,
                    "completed": False,
                    "eligible_rows": 0,
                    "source_ids": 0,
                    "source_bytes": 0,
                    "inspected_entries": 0,
                    "mapping_work_charged": 0,
                    "retained_bytes": 0,
                    "scratch_id_bytes": 0,
                    "scratch_rows": 0,
                    "ordinal_growth_peak_bytes": 0,
                },
                "snapshot": snapshot,
            },
            "output": {
                "attempted": True,
                "completed": True,
                "requested": 1,
                "fetched": 1,
                "missing": 0,
                "output_bytes": 1,
                "retained_payload_fetches": 1,
                "json_reconstruction_rows": 1,
                "typed_column_rows": 1,
            },
        }
        score_plane = {
            "version": 1,
            "available": True,
            "completed": True,
            "requested_mode": "quantized_rerank",
            "effective_mode": "quantized_rerank",
            "route": "quantized_rerank",
            "reason": "",
            "quantized_index_name": "embedding.scalar_u8.public",
            "quantized_codec": "scalar_u8",
            "quantized_version": 1,
            "quantized_config_hash": 0,
            "requested_top_k": 1,
            "requested_ef_search": 0,
            "requested_rerank_candidates": 0,
            "normalized_candidate_width": 1,
            "raw_candidate_width": 1,
            "rerank_candidate_cap": 1,
            "raw_retained_candidates": 1,
            "live_shortlist_candidates": 1,
            "actual_rerank_candidates": 1,
            "quantized_score_calls": 1,
            "quantized_code_bytes_read": 2,
            "exact_base_rerank_score_calls": 1,
            "exact_suffix_score_calls": 0,
            "exact_small_filter_score_calls": 0,
            "exact_base_vector_bytes_read": 8,
            "exact_suffix_vector_bytes_read": 0,
            "snapshot": snapshot,
        }
        payload = {
            "index": typed_index,
            "metric": "cosine",
            "exact": False,
            "candidates": 1,
            "route": "ann",
            "documents": [{"id": "a", "content": "alpha", "score": 1.0}],
            "dense_work": dense_work,
            "score_plane": score_plane,
        }
        with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, payload, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            result = client.query_by_embedding(
                "docs", [1, 0], 1, query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                expected_generation=1,
            )
            self.assertEqual(result.documents[0].id, "a")
            filtered_payload = copy.deepcopy(payload)
            filtered_payload["dense_work"]["graph"]["filter"].update(attempted=True, completed=True, eligible_rows=4097)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, filtered_payload, 0)}) as filtered_server:
                filtered_client = TreeDBClient(filtered_server.base_url, timeout=1)
                filtered_result = filtered_client.query_by_embedding(
                    "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                    query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertEqual(filtered_result.documents[0].id, "a")
                filtered_client.close()
            filtered_exact = copy.deepcopy(filtered_payload)
            filtered_exact["dense_work"]["graph"].update(
                route="typed_exact", base_ann_scored=0, base_candidates=0,
                exact_base_scored=1, base_result_ids=1,
            )
            filtered_exact["dense_work"]["graph"]["filter"]["eligible_rows"] = 1
            filtered_exact["score_plane"].update(
                route="typed_exact", raw_retained_candidates=0, live_shortlist_candidates=0,
                actual_rerank_candidates=0, quantized_score_calls=0, quantized_code_bytes_read=0,
                exact_base_rerank_score_calls=0, exact_small_filter_score_calls=1,
                exact_base_vector_bytes_read=8,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, filtered_exact, 0)}) as filtered_exact_server:
                filtered_exact_client = TreeDBClient(filtered_exact_server.base_url, timeout=1)
                self.assertEqual(
                    filtered_exact_client.query_by_embedding(
                        "docs", [1, 0], 1,
                        filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    ).documents[0].id,
                    "a",
                )
                filtered_exact_client.close()
            oversized_filtered_exact = copy.deepcopy(filtered_exact)
            oversized_filtered_exact["dense_work"]["graph"]["filter"]["eligible_rows"] = 4097
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, oversized_filtered_exact, 0)}) as oversized_server:
                oversized_client = TreeDBClient(oversized_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    oversized_client.query_by_embedding(
                        "docs", [1, 0], 1,
                        filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                oversized_client.close()
            underfilled_filter = copy.deepcopy(filtered_payload)
            underfilled_filter["dense_work"]["graph"]["filter"]["eligible_rows"] = 2
            underfilled_filter["score_plane"].update(
                requested_top_k=2, normalized_candidate_width=2, raw_candidate_width=2, rerank_candidate_cap=2,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, underfilled_filter, 0)}) as underfilled_server:
                underfilled_client = TreeDBClient(underfilled_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    underfilled_client.query_by_embedding(
                        "docs", [1, 0], 2, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                    )
                underfilled_client.close()
            over_scored_filter = copy.deepcopy(filtered_payload)
            over_scored_filter["dense_work"]["graph"].update(
                route="typed_exact", base_ann_scored=0, exact_base_scored=2, base_result_ids=2
            )
            over_scored_filter["dense_work"]["graph"]["filter"]["eligible_rows"] = 1
            over_scored_filter["score_plane"].update(
                route="typed_exact",
                raw_retained_candidates=0,
                live_shortlist_candidates=0,
                actual_rerank_candidates=0,
                quantized_score_calls=0,
                quantized_code_bytes_read=0,
                exact_base_rerank_score_calls=0,
                exact_small_filter_score_calls=2,
                exact_base_vector_bytes_read=16,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, over_scored_filter, 0)}) as over_scored_server:
                over_scored_client = TreeDBClient(over_scored_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    over_scored_client.query_by_embedding(
                        "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                    )
                over_scored_client.close()
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, payload, 0)}) as missing_filter_server:
                missing_filter_client = TreeDBClient(missing_filter_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    missing_filter_client.query_by_embedding(
                        "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                    )
                missing_filter_client.close()
            stale_generation = copy.deepcopy(payload)
            stale_generation["index"]["generation"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, stale_generation, 0)}) as stale_server:
                stale_client = TreeDBClient(stale_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    stale_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", expected_generation=1,
                    )
                stale_client.close()
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, payload, 0)}) as exact_proof_server:
                exact_proof_client = TreeDBClient(exact_proof_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    exact_proof_client.query_by_embedding("docs", [1, 0], 1, query_mode="exact")
                exact_proof_client.close()
            reversed_coverage = copy.deepcopy(payload)
            reversed_snapshot = {**snapshot, "base_coverage_lsn": 100, "current_coverage_lsn": 1}
            reversed_coverage["dense_work"]["graph"]["snapshot"] = reversed_snapshot
            reversed_coverage["score_plane"]["snapshot"] = copy.deepcopy(reversed_snapshot)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, reversed_coverage, 0)}) as reversed_server:
                reversed_client = TreeDBClient(reversed_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "snapshot coverage"):
                    reversed_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                reversed_client.close()
            for mutation in (
                lambda item: item.update(exact=True),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "route": "typed_exact"}}),
                lambda item: item.update(score_plane={**score_plane, "snapshot": {**snapshot, "schema_hash": 2}}),
                lambda item: item.update(
                    dense_work={**dense_work, "graph": {**dense_work["graph"], "route": "typed_empty"}},
                    score_plane={**score_plane, "route": "typed_empty"},
                ),
                lambda item: item.update(metric="l2"),
                lambda item: item.update(native_base_plus_live_delta=True),
                lambda item: item.update(exact_fallbacks=1),
                lambda item: item.update(full_document_scan_fallbacks=1),
                lambda item: item.update(primary_document_scans=1),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "filter": {**dense_work["graph"]["filter"], "attempted": True, "completed": True, "eligible_rows": 1}}}),
                lambda item: item.update(score_plane={**score_plane, "quantized_score_calls": 2}),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "base_candidates": 2}}),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "base_edges": 1}}),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "base_shadowed": 1}}),
                lambda item: item.update(score_plane={**score_plane, "exact_base_rerank_score_calls": 0, "exact_small_filter_score_calls": 1}),
                lambda item: item.update(score_plane={**score_plane, "reason": "stale error"}),
                lambda item: item.update(score_plane={
                    **score_plane,
                    "normalized_candidate_width": 0,
                    "raw_candidate_width": 0,
                    "rerank_candidate_cap": 0,
                    "raw_retained_candidates": 0,
                    "live_shortlist_candidates": 0,
                    "actual_rerank_candidates": 0,
                    "exact_base_rerank_score_calls": 0,
                    "exact_base_vector_bytes_read": 0,
                }),
                lambda item: item.update(score_plane={**score_plane, "quantized_code_bytes_read": 0}),
                lambda item: item.update(score_plane={**score_plane, "exact_base_vector_bytes_read": 0}),
                lambda item: item.update(score_plane={**score_plane, "exact_suffix_vector_bytes_read": 1}),
                lambda item: item.update(score_plane={**score_plane, "quantized_config_hash": 1}),
                lambda item: item.update(index={**typed_index, "quantized_indexes": [{"name": "embedding.scalar_u8.public", "codec": "scalar_u8", "version": 0}]}),
                lambda item: item.update(index={**typed_index, "quantized_indexes": [{"name": "embedding.scalar_u8.public", "codec": "", "version": 1}]}),
                lambda item: item.update(score_plane={**score_plane, "requested_ef_search": 1, "normalized_candidate_width": 2, "raw_candidate_width": 2, "rerank_candidate_cap": 2, "raw_retained_candidates": 2, "live_shortlist_candidates": 2, "actual_rerank_candidates": 2}),
                lambda item: item.update(candidates=2),
                lambda item: item.update(score_plane={**score_plane, "raw_retained_candidates": 2, "quantized_score_calls": 1}),
                lambda item: item.update(dense_work={**dense_work, "graph": {**dense_work["graph"], "base_result_ids": 0}}),
                lambda item: item.update(dense_work={**dense_work, "output": {**dense_work["output"], "retained_payload_fetches": 0}}),
                lambda item: item.update(dense_work={**dense_work, "output": {**dense_work["output"], "json_reconstruction_rows": 0}}),
                lambda item: item.update(dense_work={**dense_work, "output": {**dense_work["output"], "typed_column_rows": 2}}),
                lambda item: item.update(index={
                    **typed_index,
                    "quantized_indexes": [{
                        "name": "embedding.scalar_u8.public",
                        "codec": "scalar_u8",
                        "version": 1,
                        "scalar_u8_calibration": {"mode": "legacy", "grouping": "per_granule"},
                    }],
                }),
                lambda item: item.update(index={
                    **typed_index,
                    "quantized_indexes": [{
                        "name": "embedding.scalar_u8.public",
                        "codec": "scalar_u8",
                        "version": 1,
                        "scalar_u8_calibration": {
                            "mode": "legacy",
                            "alpha_policy": {"name": "quantile", "quantile_ppm": 999000},
                        },
                    }],
                }),
                lambda item: item.update(
                    dense_work={**dense_work, "graph": {**dense_work["graph"], "route": "typed_exact"}},
                    score_plane={**score_plane, "route": "typed_exact", "quantized_score_calls": 0, "raw_retained_candidates": 1,
                                 "live_shortlist_candidates": 0, "actual_rerank_candidates": 0, "exact_base_rerank_score_calls": 0},
                ),
                lambda item: item.update(
                    dense_work={**dense_work, "graph": {**dense_work["graph"], "route": "typed_empty"}},
                    score_plane={**score_plane, "route": "typed_empty", "quantized_score_calls": 0, "raw_retained_candidates": 0,
                                 "live_shortlist_candidates": 1, "actual_rerank_candidates": 0, "exact_base_rerank_score_calls": 0,
                                 "exact_suffix_score_calls": 0, "exact_small_filter_score_calls": 0},
                ),
            ):
                invalid = copy.deepcopy(payload)
                mutation(invalid)
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid, 0)}) as bad_server:
                    bad_client = TreeDBClient(bad_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        bad_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public"
                        )
                    bad_client.close()
            wrong_dimension = copy.deepcopy(payload)
            wrong_dimension["index"]["dimension"] = 1
            wrong_dimension["score_plane"].update(quantized_code_bytes_read=1, exact_base_vector_bytes_read=4)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, wrong_dimension, 0)}) as dimension_server:
                dimension_client = TreeDBClient(dimension_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    dimension_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                dimension_client.close()
            for bad_score in (None, float("nan"), -100, 100):
                invalid_score = copy.deepcopy(payload)
                invalid_score["documents"][0]["score"] = bad_score
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid_score, 0)}) as score_server:
                    score_client = TreeDBClient(score_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        score_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    score_client.close()
            duplicate = copy.deepcopy(payload)
            duplicate["documents"] = [copy.deepcopy(payload["documents"][0]), copy.deepcopy(payload["documents"][0])]
            duplicate["candidates"] = 2
            duplicate["dense_work"]["graph"].update(base_ann_scored=2, exact_base_scored=2, base_result_ids=2)
            duplicate["dense_work"]["output"].update(requested=2, fetched=2, output_bytes=2, retained_payload_fetches=2, json_reconstruction_rows=2, typed_column_rows=2)
            duplicate["score_plane"].update(
                requested_top_k=2,
                normalized_candidate_width=2,
                raw_candidate_width=2,
                rerank_candidate_cap=2,
                raw_retained_candidates=2,
                live_shortlist_candidates=2,
                actual_rerank_candidates=2,
                quantized_score_calls=2,
                quantized_code_bytes_read=4,
                exact_base_rerank_score_calls=2,
                exact_base_vector_bytes_read=16,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, duplicate, 0)}) as duplicate_server:
                duplicate_client = TreeDBClient(duplicate_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    duplicate_client.query_by_embedding(
                        "docs", [1, 0], 2, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                duplicate_client.close()
            for name, documents in (
                ("ascending score", [
                    {"id": "a", "content": "alpha", "score": 0.1},
                    {"id": "b", "content": "beta", "score": 0.9},
                ]),
                ("descending ID tie", [
                    {"id": "b", "content": "beta", "score": 0.5},
                    {"id": "a", "content": "alpha", "score": 0.5},
                ]),
            ):
                out_of_order = copy.deepcopy(duplicate)
                out_of_order["documents"] = documents
                with self.subTest(name=name), FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, out_of_order, 0)}) as order_server:
                    order_client = TreeDBClient(order_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        order_client.query_by_embedding(
                            "docs", [1, 0], 2, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    order_client.close()
            under_cap = copy.deepcopy(duplicate)
            under_cap["score_plane"]["rerank_candidate_cap"] = 1
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, under_cap, 0)}) as under_cap_server:
                under_cap_client = TreeDBClient(under_cap_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    under_cap_client.query_by_embedding(
                        "docs", [1, 0], 2, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                under_cap_client.close()
            underfill = copy.deepcopy(duplicate)
            underfill["documents"] = [copy.deepcopy(duplicate["documents"][0])]
            underfill["candidates"] = 1
            underfill["dense_work"]["output"].update(requested=1, fetched=1, output_bytes=1, retained_payload_fetches=1, json_reconstruction_rows=1, typed_column_rows=1)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, underfill, 0)}) as underfill_server:
                underfill_client = TreeDBClient(underfill_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    underfill_client.query_by_embedding(
                        "docs", [1, 0], 2, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                underfill_client.close()
            exact_route = copy.deepcopy(payload)
            exact_route["dense_work"]["graph"].update(
                route="typed_exact", base_ann_scored=0, base_candidates=0, delta_scored=1,
                exact_base_scored=0, base_shadowed=0, base_result_ids=0,
            )
            exact_route["score_plane"].update(
                route="typed_exact", normalized_candidate_width=0, raw_candidate_width=0,
                rerank_candidate_cap=0, raw_retained_candidates=0, live_shortlist_candidates=0,
                actual_rerank_candidates=0, quantized_score_calls=0, quantized_code_bytes_read=0,
                exact_base_rerank_score_calls=0, exact_base_vector_bytes_read=0,
                exact_suffix_score_calls=1,
                exact_suffix_vector_bytes_read=8,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, exact_route, 0)}) as exact_server:
                exact_client = TreeDBClient(exact_server.base_url, timeout=1)
                self.assertEqual(
                    exact_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    ).documents[0].id,
                    "a",
                )
                exact_client.close()
            exact_mutations = []
            exact_with_plan = copy.deepcopy(exact_route)
            exact_with_plan["score_plane"].update(
                normalized_candidate_width=1, raw_candidate_width=1, rerank_candidate_cap=1,
            )
            exact_mutations.append(exact_with_plan)
            exact_with_small_filter = copy.deepcopy(exact_route)
            exact_with_small_filter["dense_work"]["graph"].update(exact_base_scored=1, base_result_ids=1)
            exact_with_small_filter["score_plane"].update(
                exact_small_filter_score_calls=1, exact_base_vector_bytes_read=8,
            )
            exact_mutations.append(exact_with_small_filter)
            exact_with_shadow = copy.deepcopy(exact_route)
            exact_with_shadow["dense_work"]["graph"]["base_shadowed"] = 1
            exact_mutations.append(exact_with_shadow)
            for invalid_exact in exact_mutations:
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid_exact, 0)}) as exact_server:
                    exact_client = TreeDBClient(exact_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        exact_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    exact_client.close()
            valid_empty = copy.deepcopy(payload)
            valid_empty["documents"] = []
            valid_empty["candidates"] = 0
            valid_empty["dense_work"]["graph"].update(
                route="typed_empty", base_ann_scored=0, base_candidates=0, base_edges=0,
                delta_scored=0, exact_base_scored=0, base_shadowed=0, base_result_ids=0,
                filter={**dense_work["graph"]["filter"], "attempted": True, "completed": True, "eligible_rows": 0},
            )
            valid_empty["dense_work"]["output"].update(
                requested=0, fetched=0, missing=0, output_bytes=0, retained_payload_fetches=0,
                json_reconstruction_rows=0, typed_column_rows=0,
            )
            valid_empty["score_plane"].update(
                route="typed_empty", normalized_candidate_width=1, raw_candidate_width=1,
                rerank_candidate_cap=1, raw_retained_candidates=0, live_shortlist_candidates=0,
                actual_rerank_candidates=0, quantized_score_calls=0, quantized_code_bytes_read=0,
                exact_base_rerank_score_calls=0, exact_suffix_score_calls=0, exact_small_filter_score_calls=0,
                exact_base_vector_bytes_read=0, exact_suffix_vector_bytes_read=0,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, valid_empty, 0)}) as empty_server:
                empty_client = TreeDBClient(empty_server.base_url, timeout=1)
                self.assertEqual(
                    empty_client.query_by_embedding(
                        "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    ).documents,
                    [],
                )
                empty_client.close()
            for filter_patch in (
                {"attempted": False, "completed": False},
                {"attempted": True, "completed": False},
                {"attempted": True, "completed": True, "eligible_rows": 1},
            ):
                invalid_empty = copy.deepcopy(valid_empty)
                invalid_empty["dense_work"]["graph"]["filter"].update(filter_patch)
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid_empty, 0)}) as empty_server:
                    empty_client = TreeDBClient(empty_server.base_url, timeout=1)
                    with self.assertRaises(TreeDBProtocolError):
                        empty_client.query_by_embedding(
                            "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                            query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    empty_client.close()
            overflow = copy.deepcopy(payload)
            overflow["documents"] = [
                {"id": "a", "content": "alpha", "score": 1.0},
                {"id": "b", "content": "beta", "score": 0.5},
            ]
            overflow["dense_work"]["output"].update(requested=2, fetched=2, output_bytes=2, retained_payload_fetches=2, json_reconstruction_rows=2, typed_column_rows=2)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, overflow, 0)}) as overflow_server:
                overflow_client = TreeDBClient(overflow_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    overflow_client.query_by_embedding(
                        "docs", [1, 0], 2, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                overflow_client.close()
            capped = copy.deepcopy(payload)
            capped["score_plane"].update(
                requested_rerank_candidates=2,
                rerank_candidate_cap=3,
                raw_candidate_width=3,
                raw_retained_candidates=3,
                live_shortlist_candidates=3,
                actual_rerank_candidates=3,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, capped, 0)}) as capped_server:
                capped_client = TreeDBClient(capped_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    capped_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=2,
                    )
                capped_client.close()
            for mutation in (
                lambda item: item["score_plane"].update(rerank_candidate_cap=2),
                lambda item: item["score_plane"].update(live_shortlist_candidates=2),
                lambda item: item["score_plane"].update(normalized_candidate_width=2),
            ):
                invalid = copy.deepcopy(payload)
                mutation(invalid)
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid, 0)}) as bad_server:
                    bad_client = TreeDBClient(bad_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        bad_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public"
                        )
                    bad_client.close()
            for mutation in (
                lambda item: item["score_plane"].update(route="typed_empty", quantized_score_calls=0, exact_suffix_score_calls=1),
                lambda item: item["score_plane"].update(route="typed_empty", quantized_score_calls=0, exact_small_filter_score_calls=1),
                lambda item: item["score_plane"].update(route="typed_empty", quantized_score_calls=1),
            ):
                invalid = copy.deepcopy(payload)
                invalid["dense_work"]["graph"]["route"] = "typed_empty"
                invalid["dense_work"]["graph"]["base_ann_scored"] = invalid["dense_work"]["graph"]["exact_base_scored"] = invalid["dense_work"]["graph"]["delta_scored"] = 0
                invalid["documents"] = []
                invalid["dense_work"]["output"].update(requested=0, fetched=0, output_bytes=0, retained_payload_fetches=0, json_reconstruction_rows=0, typed_column_rows=0)
                mutation(invalid)
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid, 0)}) as bad_server:
                    bad_client = TreeDBClient(bad_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        bad_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public"
                        )
                    bad_client.close()


    def test_benchmark_lifecycle_and_vector_index_search_methods(self) -> None:
        reset_route = "/v1/indexes/bench/reset"
        optimize_route = "/v1/indexes/bench/optimize"
        search_route = "/v1/indexes/bench/search/vector-index"
        raw_search_route = "/v1/indexes/bench/search/vector-index:binary?top_k=1&query_mode=exact"
        raw_quantized_route = "/v1/indexes/bench/search/vector-index:binary?top_k=1&query_mode=quantized_rerank&quantized_index_name=embedding.scalar_u8.fast&quantized_rerank_candidates=32&stats_mode=production&response_format=ids"
        routes = {
            ("POST", reset_route): (
                200,
                {"index": SAMPLE_INDEX, "created": True, "reset": False, "drop_old": True, "dropped_documents": 0},
                0,
            ),
            ("POST", optimize_route): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "vector_index_name": "embedding",
                    "status": {"name": "embedding", "strategy": "column_graph", "state": "column_graph_loaded", "loaded": True, "column_graph_build": {"total_nanos": 11}},
                    "timing": {"total_nanos": 13, "cache_warm_nanos": 2},
                },
                0,
            ),
            ("POST", search_route): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "results": [{"id": "doc-1", "ordinal": 1, "score": 0.98}],
                    "metric": "cosine",
                    "vector_index_name": "embedding",
                    "query_mode": "quantized_rerank",
                    "quantized_index_name": "embedding.scalar_u8.fast",
                    "quantized_rerank_candidates": 32,
                    "no_documents": True,
                    "stats": {"documents_fetched": 0, "quantized_rerank_exact_score_calls": 32},
                    "diagnostics": {"route": "quantized_rerank"},
                },
                0,
            ),
            ("POST", raw_search_route): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "results": [{"id": "doc-1", "ordinal": 1, "score": 0.98}],
                    "metric": "cosine",
                    "vector_index_name": "embedding",
                    "query_mode": "exact",
                    "no_documents": True,
                    "stats": {"documents_fetched": 0},
                    "diagnostics": {"route": "exact_hnsw_search_pack_v1"},
                },
                0,
            ),
            ("POST", raw_quantized_route): (
                200,
                {"response_format": "ids", "ids": ["doc-1"]},
                0,
            ),
        }
        with FixtureServer(routes) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            reset = client.reset_index(
                "bench",
                dimension=2,
                drop_old=True,
                vector_index_options=BenchmarkVectorIndexOptions(
                    strategy="column_graph",
                    m=16,
                    ef_construction=128,
                    ef_search=64,
                    quantized_indexes=[QuantizedIndexInfo(name="embedding.scalar_u8.fast")],
                ),
            )
            optimize = client.optimize_index("bench", expected_generation=1)
            search = client.search_vector_index(
                "bench",
                [1, 0],
                1,
                query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.fast",
                quantized_rerank_candidates=32,
            )

            self.assertTrue(reset.created)
            self.assertTrue(optimize.status.loaded)
            self.assertEqual(optimize.status.column_graph_build.total_nanos, 11)
            self.assertEqual(optimize.timing.total_nanos, 13)
            self.assertEqual(optimize.timing.cache_warm_nanos, 2)
            b64_search = client.search_vector_index(
                "bench",
                [1, 0],
                1,
                query_embedding_encoding="f32_le_b64",
            )
            raw_search = client.search_vector_index(
                "bench",
                [1, 0],
                1,
                query_embedding_encoding="f32_le",
            )
            raw_quantized = client.search_vector_index(
                "bench",
                [1, 0],
                1,
                query_embedding_encoding="f32_le",
                query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.fast",
                quantized_rerank_candidates=32,
                stats_mode="production",
                response_format="ids",
            )

            self.assertEqual(search.query_mode, "quantized_rerank")
            self.assertEqual(search.results[0].id, "doc-1")
            self.assertEqual(b64_search.results[0].id, "doc-1")
            self.assertEqual(raw_search.query_mode, "exact")
            self.assertEqual(raw_search.results[0].id, "doc-1")
            self.assertEqual(raw_quantized.ids, ["doc-1"])
            reset_body = json_body(server.records[0])
            self.assertNotIn("metric", reset_body)
            self.assertEqual(reset_body["vector_index_options"]["strategy"], "column_graph")
            self.assertEqual(reset_body["vector_index_options"]["quantized_indexes"][0]["codec"], "scalar_u8")
            self.assertEqual(json_body(server.records[1]), {"expected_generation": 1})
            self.assertEqual(json_body(server.records[2])["quantized_rerank_candidates"], 32)
            b64_body = json_body(server.records[3])
            self.assertNotIn("query_embedding", b64_body)
            self.assertEqual(struct.unpack("<2f", base64.b64decode(b64_body["query_embedding_f32_le_b64"])), (1.0, 0.0))
            raw_record = server.records[4]
            self.assertEqual(raw_record["path"], raw_search_route)
            self.assertEqual(raw_record["headers"]["Content-Type"], "application/vnd.treedb.vector-search.f32le")
            self.assertEqual(struct.unpack("<2f", raw_record["body"]), (1.0, 0.0))
            raw_quantized_record = server.records[5]
            self.assertEqual(raw_quantized_record["path"], raw_quantized_route)
            self.assertEqual(struct.unpack("<2f", raw_quantized_record["body"]), (1.0, 0.0))

    def test_empty_optional_benchmark_fields_fail_closed_locally(self) -> None:
        client = TreeDBClient("http://127.0.0.1:1", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "metric"):
            client.create_index("docs", 2, metric="")
        with self.assertRaisesRegex(InvalidRequestError, "metric"):
            client.reset_index("docs", dimension=2, metric="")
        with self.assertRaisesRegex(InvalidRequestError, "query_mode"):
            client.search_vector_index("docs", [1, 0], 1, query_mode="")
        with self.assertRaisesRegex(InvalidRequestError, "stats_mode"):
            client.search_vector_index("docs", [1, 0], 1, stats_mode="")
        with self.assertRaisesRegex(InvalidRequestError, "query_embedding_encoding"):
            client.search_vector_index("docs", [1, 0], 1, query_embedding_encoding="binary")
        with self.assertRaisesRegex(InvalidRequestError, "query_mode"):
            client.search_vector_index("docs", [1, 0], 1, query_embedding_encoding="f32_le", query_mode="")
        with self.assertRaisesRegex(InvalidRequestError, "quantized_index_name"):
            client.search_vector_index("docs", [1, 0], 1, query_embedding_encoding="f32_le", quantized_index_name="")
        with self.assertRaisesRegex(InvalidRequestError, "response_format"):
            client.search_vector_index("docs", [1, 0], 1, query_embedding_encoding="f32_le", response_format="")
        for bad_top_k in (True, 1.9, 0):
            with self.subTest(bad_top_k=bad_top_k):
                with self.assertRaisesRegex(InvalidRequestError, "top_k"):
                    client.search_vector_index("docs", [1, 0], bad_top_k, query_embedding_encoding="f32_le")
        for bad_ef_search in (True, 1.9, -1):
            with self.subTest(bad_ef_search=bad_ef_search):
                with self.assertRaisesRegex(InvalidRequestError, "ef_search"):
                    client.search_vector_index("docs", [1, 0], 1, query_embedding_encoding="f32_le", ef_search=bad_ef_search)
        with self.assertRaisesRegex(InvalidRequestError, "query_embedding"):
            client.search_vector_index("docs", ["not-a-number"], 1, query_embedding_encoding="f32_le_b64")
        with self.assertRaisesRegex(InvalidRequestError, "query_embedding"):
            client.search_vector_index("docs", ["not-a-number"], 1, query_embedding_encoding="json")
        with self.assertRaisesRegex(InvalidRequestError, "after_id requires cursor_page=True"):
            client.filter_documents("docs", after_id="cursor")

    def test_keyword_search_serializes_request_and_parses_response(self) -> None:
        route = "/v1/indexes/docs/search/keyword"
        response = {
            "index": SAMPLE_INDEX,
            "text_index": "content",
            "documents": [
                {
                    "id": "doc-1",
                    "content": "refund policy text",
                    "meta": {"_treedb_search": {"type": "keyword", "rank": 1}},
                    "score": 3.12,
                }
            ],
            "stats": {"query_terms": 2, "candidates_returned": 1, "documents_fetched": 1, "new_counter": 7},
        }
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.search_keyword(
                "docs",
                "refund policy",
                5,
                operator="and",
                candidate_limit=100,
                max_postings_scanned=1000,
                return_embedding=True,
                expected_generation=2,
            )

            self.assertEqual(result.text_index, "content")
            self.assertEqual(result.documents[0].id, "doc-1")
            self.assertEqual(result.documents[0].score, 3.12)
            self.assertEqual(result.documents[0].meta["_treedb_search"]["type"], "keyword")
            self.assertEqual(result.stats.query_terms, 2)
            self.assertEqual(result.stats.extra["new_counter"], 7)
            self.assertTrue(result.index.capabilities.keyword_search)
            body = json_body(server.records[0])
            self.assertEqual(
                body,
                {
                    "query": "refund policy",
                    "top_k": 5,
                    "return_embedding": True,
                    "expected_generation": 2,
                    "operator": "and",
                    "candidate_limit": 100,
                    "max_postings_scanned": 1000,
                },
            )

    def test_hybrid_search_serializes_fusion_and_parses_response(self) -> None:
        route = "/v1/indexes/docs/search/hybrid"
        response = {
            "index": SAMPLE_INDEX,
            "text_index": "content",
            "vector_index": "embedding",
            "documents": [
                {
                    "id": "doc-1",
                    "content": "refund policy text",
                    "meta": {"_treedb_search": {"type": "hybrid", "sources": [{"source": "text"}]}},
                    "score": 0.0325,
                }
            ],
            "plan": {
                "fusion_method": "rrf",
                "fusion_tie_policy": "fused_score_best_rank_source_order_id",
                "text_candidate_limit": 25,
                "vector_candidate_limit": 30,
                "max_chunks_per_parent": 1,
                "final_top_k": 5,
            },
            "snapshot": {"consistency": "current_snapshot", "commit_seq": 9},
            "stats": {
                "text_candidates_returned": 3,
                "vector_candidates_returned": 4,
                "fusion_both": 1,
                "collapse_rejections": 2,
                "collapse_exhaustions": 1,
                "documents_fetched": 3,
            },
        }
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.search_hybrid(
                "docs",
                query="refund policy",
                query_embedding=[0.1, 0.2],
                top_k=5,
                candidate_limit=50,
                text_candidate_limit=25,
                vector_candidate_limit=30,
                ef_search=64,
                max_chunks_per_parent=1,
                fusion=HybridFusionOptions(
                    method="rrf",
                    rrf_k=60,
                    tie_policy="fused_score_best_rank_source_order_id",
                    source_order=["text", "vector"],
                ),
                return_embedding=False,
                expected_generation=2,
            )

            self.assertEqual(result.text_index, "content")
            self.assertEqual(result.vector_index, "embedding")
            self.assertEqual(result.plan.fusion_method, "rrf")
            self.assertEqual(result.plan.final_top_k, 5)
            self.assertEqual(result.plan.max_chunks_per_parent, 1)
            self.assertEqual(result.snapshot.consistency, "current_snapshot")
            self.assertEqual(result.stats.fusion_both, 1)
            self.assertEqual(result.stats.collapse_rejections, 2)
            self.assertEqual(result.stats.collapse_exhaustions, 1)
            self.assertEqual(result.documents[0].meta["_treedb_search"]["type"], "hybrid")
            body = json_body(server.records[0])
            self.assertEqual(body["query_embedding"], [0.1, 0.2])
            self.assertEqual(body["fusion"]["source_order"], ["text", "vector"])
            self.assertEqual(body["fusion"]["tie_policy"], "fused_score_best_rank_source_order_id")
            self.assertEqual(body["text_candidate_limit"], 25)
            self.assertEqual(body["vector_candidate_limit"], 30)
            self.assertEqual(body["max_chunks_per_parent"], 1)
            self.assertEqual(body["return_embedding"], False)

    def test_hybrid_search_requires_query_or_embedding_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaises(InvalidRequestError) as caught:
            client.search_hybrid("docs", top_k=1)

        self.assertEqual(caught.exception.code, "invalid_request")
        self.assertIn("query or query_embedding", caught.exception.message)

    def test_service_errors_propagate(self) -> None:
        routes = {
            ("POST", "/v1/indexes/docs/search/keyword"): (
                501,
                {"error": {"code": "unsupported", "message": "keyword filters unsupported"}},
                0,
            ),
            ("POST", "/v1/indexes/missing/search/keyword"): (
                404,
                {"error": {"code": "index_not_found", "message": "missing"}},
                0,
            ),
            ("POST", "/v1/indexes/stale/search/hybrid"): (
                409,
                {"error": {"code": "index_stale", "message": "stale"}},
                0,
            ),
            ("POST", "/v1/indexes/unavailable/search/hybrid"): (
                503,
                {"error": {"code": "index_unavailable", "message": "text/vector unavailable"}},
                0,
            ),
            ("POST", "/v1/indexes/mismatch/search/vector"): (
                409,
                {"error": {"code": "snapshot_mismatch", "message": "visibility changed"}},
                0,
            ),
        }
        with FixtureServer(routes) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            with self.assertRaises(UnsupportedError):
                client.search_keyword("docs", "refund", 1, filter={"field": "meta.repo", "operator": "$eq", "value": "gomap"})
            self.assertEqual(json_body(server.records[-1])["filter"], {"field": "meta.repo", "operator": "==", "value": "gomap"})
            with self.assertRaises(IndexNotFoundError):
                client.search_keyword("missing", "refund", 1)
            with self.assertRaises(IndexStaleError):
                client.search_hybrid("stale", query="refund", top_k=1)
            with self.assertRaises(IndexUnavailableError):
                client.search_hybrid("unavailable", query_embedding=[1.0, 0.0], top_k=1)
            with self.assertRaises(SnapshotMismatchError):
                client.query_by_embedding("mismatch", [1.0, 0.0], 1)

    def test_delete_documents_rejects_bare_string_ids_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "not a single string"):
            client.delete_documents("docs", "doc-1")  # type: ignore[arg-type]

    def test_expected_generation_zero_is_rejected_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "positive integer"):
            client.count_documents("docs", expected_generation=0)

    def test_dense_search_route_serializes_and_parses(self) -> None:
        route = "/v1/indexes/docs/search/vector"
        response = {
            "index": SAMPLE_INDEX,
            "documents": [{"id": "doc-1", "content": "text", "score": 0.5}],
            "metric": "cosine",
            "exact": False,
            "candidates": 10,
            "route": "ann",
            "native_base_plus_live_delta": True,
            "scalar_filter_membership_source": "bounded_candidate_refinement",
            "scalar_filter_plan": "mixed_refined",
            "scalar_filter_probe_ids": 4096,
            "scalar_filter_probe_truncated": 1,
            "scalar_filter_candidates": 41,
            "scalar_filter_candidate_ids": 5,
            "scalar_filter_retained_candidate_ids": 23,
            "scalar_filter_refined_candidate_ids": 5,
            "scalar_filter_visited": 41,
            "scalar_filter_scored": 41,
            "scalar_filter_admitted": 5,
            "scalar_filter_exact_scoring": True,
            "scalar_filter_underfill": False,
            "scalar_filter_unbounded": 0,
            "exact_fallbacks": 0,
            "full_document_scan_fallbacks": 0,
            "allowed_id_materialization_rows": 23,
            "primary_document_scans": 0,
            "document_materialization_rows": 5,
            "visibility_mismatch_count": 1,
            "visibility_retry_count": 1,
        }
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.query_by_embedding("docs", [0.1, 0.2], 10, route="ann", ef_search=64)

            self.assertEqual(result.route, "ann")
            self.assertFalse(result.exact)
            self.assertEqual(result.candidates, 10)
            self.assertTrue(result.native_base_plus_live_delta)
            self.assertEqual(result.scalar_filter_membership_source, "bounded_candidate_refinement")
            self.assertEqual(result.scalar_filter_plan, "mixed_refined")
            self.assertEqual(result.scalar_filter_probe_ids, 4096)
            self.assertEqual(result.scalar_filter_probe_truncated, 1)
            self.assertEqual(result.scalar_filter_candidates, 41)
            self.assertEqual(result.scalar_filter_candidate_ids, 5)
            self.assertNotEqual(result.scalar_filter_candidates, result.scalar_filter_candidate_ids)
            self.assertEqual(result.scalar_filter_retained_candidate_ids, 23)
            self.assertEqual(result.scalar_filter_refined_candidate_ids, 5)
            self.assertEqual(result.scalar_filter_visited, 41)
            self.assertEqual(result.scalar_filter_scored, 41)
            self.assertEqual(result.scalar_filter_admitted, 5)
            self.assertTrue(result.scalar_filter_exact_scoring)
            self.assertFalse(result.scalar_filter_underfill)
            self.assertEqual(result.scalar_filter_unbounded, 0)
            self.assertEqual(result.exact_fallbacks, 0)
            self.assertEqual(result.full_document_scan_fallbacks, 0)
            self.assertEqual(result.allowed_id_materialization_rows, 23)
            self.assertEqual(result.primary_document_scans, 0)
            self.assertEqual(result.document_materialization_rows, 5)
            self.assertEqual(result.visibility_mismatch_count, 1)
            self.assertEqual(result.visibility_retry_count, 1)
            body = json_body(server.records[0])
            self.assertEqual(body["route"], "ann")
            self.assertEqual(body["ef_search"], 64)

        with FixtureServer({("POST", route): (200, dict(response, exact=True, route=None), 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            result = client.query_by_embedding("docs", [0.1], 1, route="exact")
            self.assertEqual(result.route, "")
            self.assertTrue(result.exact)
            self.assertEqual(json_body(server.records[0])["route"], "exact")

    def test_dense_search_route_validation_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)
        with self.assertRaisesRegex(InvalidRequestError, "unsupported dense search route"):
            client.query_by_embedding("docs", [0.1], 1, route="fast")
        for value in (-1, True, 1.5, "64"):
            with self.subTest(ef_search=value), self.assertRaisesRegex(InvalidRequestError, "ef_search"):
                client.query_by_embedding("docs", [0.1], 1, route="ann", ef_search=value)

    def test_typed_quantized_integer_validation_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)
        for value in (True, 1.5, "32", 1 << 63):
            with self.subTest(rerank_candidates=value), self.assertRaisesRegex(InvalidRequestError, "quantized_rerank_candidates"):
                client.query_by_embedding(
                    "docs",
                    [0.1, 0.2],
                    1,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.fast",
                    quantized_rerank_candidates=value,
                )

    def test_keyword_and_hybrid_filter_bodies_serialize(self) -> None:
        routes = {
            ("POST", "/v1/indexes/docs/search/keyword"): (
                503,
                {"error": {"code": "index_unavailable", "message": "filtered keyword search failed closed: scalar_filter_unbounded"}},
                0,
            ),
            ("POST", "/v1/indexes/docs/search/hybrid"): (
                503,
                {"error": {"code": "index_unavailable", "message": "scalar_filter_unbounded"}},
                0,
            ),
        }
        with FixtureServer(routes) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            want_filter = {"field": "meta.tenant", "operator": "==", "value": "t1"}
            self.assertEqual(len(server.records), 0)
            with self.assertRaises(IndexUnavailableError):
                client.search_keyword("docs", "refund", 1, filter=dict(want_filter))
            self.assertEqual(json_body(server.records[-1])["filter"], want_filter)
            with self.assertRaises(IndexUnavailableError):
                client.search_hybrid("docs", query="refund", top_k=1, filter=dict(want_filter))
            self.assertEqual(json_body(server.records[-1])["filter"], want_filter)

    def test_service_error_mapping(self) -> None:
        route = "/v1/indexes/missing/documents/count"
        with FixtureServer({("POST", route): (404, {"error": {"code": "index_not_found", "message": "missing"}}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            with self.assertRaises(IndexNotFoundError) as caught:
                client.count_documents("missing")

            self.assertEqual(caught.exception.code, "index_not_found")
            self.assertEqual(caught.exception.status_code, 404)
            self.assertIn("missing", caught.exception.message)

    def test_unsupported_error_mapping(self) -> None:
        route = "/v1/indexes/docs/documents/count"
        with FixtureServer({("POST", route): (501, {"error": {"code": "unsupported", "message": "not implemented"}}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            with self.assertRaises(UnsupportedError):
                client.count_documents("docs")

    def test_config_validation(self) -> None:
        for base_url in ("", "localhost:7120", "ftp://127.0.0.1:7120", "http://127.0.0.1:7120?x=1", "http://127.0.0.1:invalid", "http://127.0.0.1:99999", "http://[::1"):
            with self.subTest(base_url=base_url):
                with self.assertRaises(TreeDBConfigError):
                    TreeDBClient(base_url)
        with self.assertRaises(TreeDBConfigError):
            TreeDBClient("http://127.0.0.1:7120", timeout=0)

    def test_timeout_mapping(self) -> None:
        with FixtureServer({("GET", "/v1/health"): (200, {"ok": True}, 0.25)}) as server:
            client = TreeDBClient(server.base_url, timeout=0.01)

            with self.assertRaises(TreeDBTimeoutError):
                client.health()

    def test_peer_reset_maps_to_transport_error(self) -> None:
        class ResettingOpener:
            def open(self, request: Any, timeout: float | None = None) -> Any:
                raise ConnectionResetError("connection reset by peer")

        client = TreeDBClient("http://127.0.0.1:9", timeout=1)
        client._opener = ResettingOpener()  # type: ignore[attr-defined]

        with self.assertRaisesRegex(TreeDBTransportError, "connection reset"):
            client.health()


if __name__ == "__main__":
    unittest.main()
