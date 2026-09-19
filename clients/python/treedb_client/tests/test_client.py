from __future__ import annotations

import base64
import copy
from dataclasses import asdict, replace
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
    IndexInfo,
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
from treedb_client.client import (
    _dense_document_embeddings_match,
    _dense_embedding_scores_match_query,
    _dense_http_results_ordered,
    _dense_work_requires_score_plane,
    _is_legacy_scalar_u8_v1_index,
)
from treedb_client._dense_work import DenseScorePlaneProof, dense_quantized_response_work_matches


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
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
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
    def test_dense_http_ordering_rejects_unencodable_tie_id(self) -> None:
        documents = [Document(id="a", score=1.0), Document(id="\ud800", score=1.0)]
        self.assertFalse(_dense_http_results_ordered(documents))

    def test_dense_embedding_echo_matches_request(self) -> None:
        omitted = [Document(id="a")]
        included = [Document(id="a", embedding=[1.0, 0.0])]
        self.assertTrue(_dense_document_embeddings_match(omitted, False, 2))
        self.assertTrue(_dense_document_embeddings_match(included, True, 2))
        self.assertFalse(_dense_document_embeddings_match(included, False, 2))
        self.assertFalse(_dense_document_embeddings_match(omitted, True, 2))
        self.assertFalse(_dense_document_embeddings_match(
            [Document(id="a", embedding=[1.0])], True, 2,
        ))
        self.assertFalse(_dense_document_embeddings_match(
            [Document(id="a", embedding=[float("inf"), 0.0])], True, 2,
        ))
        self.assertFalse(_dense_document_embeddings_match(
            [Document(id="a", embedding_f32_le_b64="AACAPwAAAAA=")], False, 2,
        ))
        self.assertFalse(_dense_document_embeddings_match(
            [Document(id="a", embedding=[1.0, 0.0], embedding_f32_le_b64="AACAPwAAAAA=")], True, 2,
        ))

    def test_dense_embedding_scores_match_query(self) -> None:
        self.assertTrue(_dense_embedding_scores_match_query([
            Document(id="same", embedding=[1, 0], score=1),
            Document(id="orthogonal", embedding=[0, 1], score=0),
            Document(id="opposite", embedding=[-1, 0], score=-1),
        ], [1, 0]))
        self.assertTrue(_dense_embedding_scores_match_query([
            Document(id="rounded", embedding=[1, 0], score=1 - 0.5e-6),
        ], [1, 0]))
        for name, document in (
            ("mismatched score", Document(id="a", embedding=[1, 0], score=0)),
            ("outside tolerance", Document(id="a", embedding=[1, 0], score=1 - 2e-6)),
            ("zero embedding", Document(id="a", embedding=[0, 0], score=0)),
            ("non-FP32 embedding", Document(id="a", embedding=[3.5e38, 0], score=1)),
            ("missing score", Document(id="a", embedding=[1, 0])),
        ):
            with self.subTest(name=name):
                self.assertFalse(_dense_embedding_scores_match_query([document], [1, 0]))
        self.assertFalse(_dense_embedding_scores_match_query([
            Document(id="a", embedding=[1, 0], score=0),
        ], [0, 0]))

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

    def test_replace_source_by_id_sends_one_explicit_atomic_request(self) -> None:
        route = "/v1/indexes/docs/documents/replace_source_by_id"
        response = {"index": SAMPLE_INDEX, "deleted_count": 2, "inserted_count": 1}
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            result = client.replace_source_by_id(
                "docs", ["source#0", "source#1", "missing"],
                [Document(id="source#0", content="fresh", embedding=[1, 0], meta={"repo": "gomap"})],
                expected_generation=1,
            )
            self.assertEqual((result.deleted_count, result.inserted_count), (2, 1))
            self.assertEqual(len(server.records), 1)
            body = json_body(server.records[0])
            self.assertEqual(body["expected_generation"], 1)
            self.assertEqual(body["delete_ids"], ["source#0", "source#1", "missing"])
            self.assertEqual([doc["id"] for doc in body["documents"]], ["source#0"])
        for generation in (None, 0, -1, True, 1 << 64):
            with self.subTest(generation=generation), self.assertRaises(InvalidRequestError):
                TreeDBClient("http://localhost:1").replace_source_by_id("docs", [], [], expected_generation=generation)

    def test_update_metadata_by_id_sends_no_vectors_and_validates_paths(self) -> None:
        route = "/v1/indexes/docs/documents/update_metadata_by_id"
        response = {"index": SAMPLE_INDEX, "matched_count": 1, "modified_count": 1}
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)
            result = client.update_metadata_by_id(
                "docs", ["a", "missing"], {"meta.acl": "new", "meta.rank": 2}, ["meta.old"], expected_generation=1
            )
            self.assertEqual((result.matched_count, result.modified_count), (1, 1))
            body = json_body(server.records[0])
            self.assertEqual(body["ids"], ["a", "missing"])
            self.assertNotIn("embedding", json.dumps(body))
        invalid = (
            (["a", "a"], {"meta.x": 1}, []),
            (["a"], {"content": "x"}, []),
            (["a"], {"meta.x": 1}, ["meta.x"]),
            (["a"], {"meta.x": 1}, ["meta.x.child"]),
            ([], {"meta.x": 1}, []),
        )
        for ids, set_values, unset in invalid:
            with self.subTest(ids=ids, set=set_values, unset=unset), self.assertRaises(InvalidRequestError):
                TreeDBClient("http://localhost:1").update_metadata_by_id(
                    "docs", ids, set_values, unset, expected_generation=1
                )
        for generation in (None, 0, -1, True, 1 << 64):
            with self.subTest(generation=generation), self.assertRaises(InvalidRequestError):
                TreeDBClient("http://localhost:1").update_metadata_by_id(
                    "docs", ["a"], {}, [], expected_generation=generation
                )

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
        manifest = {"generation": 1, "format": "", "version": 1, "checksum": 3}
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
                "base_candidates": 0,
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
            "document_materialization_rows": 1,
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
            default_width_boundary = copy.deepcopy(payload)
            default_width_boundary["score_plane"].update(
                normalized_candidate_width=64,
                raw_candidate_width=64,
                rerank_candidate_cap=64,
            )
            with mock.patch.object(client, "_request", return_value=default_width_boundary):
                boundary = client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertEqual(boundary.score_plane.normalized_candidate_width, 64)
            explicit_ef = copy.deepcopy(payload)
            explicit_ef["score_plane"]["requested_ef_search"] = 8
            with mock.patch.object(client, "_request", return_value=explicit_ef):
                explicit = client.query_by_embedding(
                    "docs", [1, 0], 1, ef_search=8, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertEqual(explicit.score_plane.requested_ef_search, 8)
            default_width_overflow = copy.deepcopy(payload)
            default_width_overflow["score_plane"].update(
                normalized_candidate_width=65,
                raw_candidate_width=65,
                rerank_candidate_cap=65,
            )
            zero_index_default = copy.deepcopy(payload)
            zero_index_default["index"]["vector_ef_search"] = 0
            for name, candidate in (
                ("width above index default", default_width_overflow),
                ("nonpositive index default", zero_index_default),
            ):
                with self.subTest(http_default_ef=name), \
                     mock.patch.object(client, "_request", return_value=candidate), \
                     self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
            embedded_payload = copy.deepcopy(payload)
            embedded_payload["documents"][0]["embedding"] = [1, 0]
            with mock.patch.object(client, "_request", return_value=embedded_payload):
                embedded_result = client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", return_embedding=True,
                )
            self.assertEqual(embedded_result.documents[0].embedding, [1.0, 0.0])
            wrong_dimension_embedding = copy.deepcopy(embedded_payload)
            wrong_dimension_embedding["documents"][0]["embedding"] = [1]
            nonfinite_embedding = copy.deepcopy(embedded_payload)
            nonfinite_embedding["documents"][0]["embedding"] = [float("nan"), 0]
            mismatched_embedding_score = copy.deepcopy(embedded_payload)
            mismatched_embedding_score["documents"][0]["embedding"] = [0, 1]
            zero_embedding = copy.deepcopy(embedded_payload)
            zero_embedding["documents"][0]["embedding"] = [0, 0]
            compact_embedding = copy.deepcopy(payload)
            compact_embedding["documents"][0]["embedding_f32_le_b64"] = "AACAPwAAAAA="
            for name, candidate, return_embedding in (
                ("unrequested", embedded_payload, False),
                ("missing", payload, True),
                ("wrong dimension", wrong_dimension_embedding, True),
                ("nonfinite", nonfinite_embedding, True),
                ("mismatched score", mismatched_embedding_score, True),
                ("zero vector", zero_embedding, True),
                ("write-only compact", compact_embedding, False),
            ):
                with self.subTest(embedding_echo=name), \
                     mock.patch.object(client, "_request", return_value=candidate), \
                     self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        return_embedding=return_embedding,
                    )
            pre_owner_filter = replace(
                result.dense_work.graph.filter,
                attempted=True, completed=True, eligible_rows=1, source_ids=1, source_bytes=1,
                inspected_entries=1, mapping_work_charged=1, retained_bytes=1, scratch_id_bytes=1,
                scratch_rows=1, ordinal_growth_peak_bytes=1,
            )
            pre_owner_graph = replace(
                result.dense_work.graph,
                completed=False, route="", base_ann_scored=0, base_candidates=0, base_edges=0,
                delta_scored=0, exact_base_scored=0, base_shadowed=0, base_result_ids=0,
                filter=pre_owner_filter,
            )
            pre_owner_output = replace(
                result.dense_work.output,
                attempted=False, completed=False, requested=0, fetched=0, missing=0, output_bytes=0,
                retained_payload_fetches=0, json_reconstruction_rows=0, typed_column_rows=0,
            )
            pre_owner = replace(result.dense_work, completed=False, graph=pre_owner_graph, output=pre_owner_output)
            self.assertFalse(_dense_work_requires_score_plane(pre_owner))
            execution_signals = [
                ("service completed", replace(pre_owner, completed=True)),
                ("graph completed", replace(pre_owner, graph=replace(pre_owner.graph, completed=True))),
                ("output attempted", replace(pre_owner, output=replace(pre_owner.output, attempted=True))),
                ("output completed", replace(pre_owner, output=replace(pre_owner.output, completed=True))),
            ]
            execution_signals.extend(
                (f"route {route}", replace(pre_owner, graph=replace(pre_owner.graph, route=route)))
                for route in ("typed_empty", "typed_exact", "typed_hnsw")
            )
            execution_signals.extend(
                (field, replace(pre_owner, graph=replace(pre_owner.graph, **{field: 1})))
                for field in (
                    "base_ann_scored", "base_candidates", "base_edges", "delta_scored",
                    "exact_base_scored", "base_shadowed", "base_result_ids",
                )
            )
            execution_signals.extend(
                (field, replace(pre_owner, output=replace(pre_owner.output, **{field: 1})))
                for field in (
                    "requested", "fetched", "missing", "output_bytes", "retained_payload_fetches",
                    "json_reconstruction_rows", "typed_column_rows",
                )
            )
            for name, candidate in execution_signals:
                with self.subTest(score_plane_execution_signal=name):
                    self.assertTrue(_dense_work_requires_score_plane(candidate))
            hostile_unfiltered = copy.deepcopy(payload)
            hostile_unfiltered["dense_work"]["graph"].update(
                base_ann_scored=2, base_candidates=0, exact_base_scored=2, base_result_ids=2,
            )
            hostile_unfiltered["score_plane"].update(
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
            with mock.patch.object(client, "_request", return_value=hostile_unfiltered):
                expanded = client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertTrue(dense_quantized_response_work_matches(
                expanded.dense_work, expanded.score_plane, 1, 1, False
            ))
            self.assertFalse(dense_quantized_response_work_matches(
                replace(expanded.dense_work, output=replace(expanded.dense_work.output, output_bytes=0)),
                expanded.score_plane, 1, 1, False,
            ))
            self.assertFalse(dense_quantized_response_work_matches(
                replace(expanded.dense_work, graph=replace(expanded.dense_work.graph, base_candidates=1)),
                expanded.score_plane, 1, 1, False,
            ))
            zero_nonempty_output = copy.deepcopy(payload)
            zero_nonempty_output["dense_work"]["output"]["output_bytes"] = 0
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, zero_nonempty_output, 0)}) as hostile_server:
                hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    hostile_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                hostile_client.close()
            hostile_unfiltered["dense_work"]["graph"]["base_candidates"] = 1
            with mock.patch.object(client, "_request", return_value=hostile_unfiltered), \
                 self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            for name, field, value in (
                ("missing materialization row", "document_materialization_rows", 0),
                ("excess materialization row", "document_materialization_rows", 2),
                ("legacy membership source", "scalar_filter_membership_source", "bounded_complete_set"),
                ("legacy filter plan", "scalar_filter_plan", "complete_exact"),
                ("legacy probe IDs", "scalar_filter_probe_ids", 1),
                ("legacy truncated probes", "scalar_filter_probe_truncated", 1),
                ("legacy candidates", "scalar_filter_candidates", 1),
                ("legacy candidate IDs", "scalar_filter_candidate_ids", 1),
                ("legacy retained IDs", "scalar_filter_retained_candidate_ids", 1),
                ("legacy refined IDs", "scalar_filter_refined_candidate_ids", 1),
                ("legacy visited", "scalar_filter_visited", 1),
                ("legacy scored", "scalar_filter_scored", 1),
                ("legacy admitted", "scalar_filter_admitted", 1),
                ("legacy exact scoring", "scalar_filter_exact_scoring", True),
                ("legacy underfill", "scalar_filter_underfill", True),
                ("legacy unbounded", "scalar_filter_unbounded", 1),
                ("legacy materialization", "allowed_id_materialization_rows", 1),
                ("typed visibility mismatch", "visibility_mismatch_count", 1),
                ("typed visibility retry", "visibility_retry_count", 1),
            ):
                invalid_outer = copy.deepcopy(payload)
                invalid_outer[field] = value
                with self.subTest(outer_diagnostic=name), mock.patch.object(client, "_request", return_value=invalid_outer):
                    with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
            malformed_score = copy.deepcopy(payload)
            malformed_score["score_plane"]["version"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, malformed_score, 0)}) as malformed_score_server:
                malformed_score_client = TreeDBClient(malformed_score_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof") as caught:
                    malformed_score_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertEqual(caught.exception.dense_work, result.dense_work)
                self.assertIsNone(caught.exception.score_plane)
                malformed_score_client.close()
            malformed_work = copy.deepcopy(payload)
            malformed_work["dense_work"]["version"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, malformed_work, 0)}) as malformed_work_server:
                malformed_work_client = TreeDBClient(malformed_work_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                    malformed_work_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertIsNone(caught.exception.dense_work)
                self.assertEqual(caught.exception.score_plane, result.score_plane)
                malformed_work_client.close()
            malformed_error_score = {
                "error": {"code": "index_unavailable", "message": "budget", "dense_work": dense_work,
                          "score_plane": malformed_score["score_plane"]}
            }
            with self.assertRaisesRegex(TreeDBProtocolError, "score-plane error proof") as caught:
                client._decode_error(
                    503,
                    json.dumps(malformed_error_score).encode(),
                    dense_proof=True,
                    dense_score_plane=True,
                )
            self.assertEqual(caught.exception.dense_work, result.dense_work)
            self.assertIsNone(caught.exception.score_plane)
            malformed_error_work = {
                "error": {"code": "index_unavailable", "message": "budget", "dense_work": malformed_work["dense_work"],
                          "score_plane": score_plane}
            }
            with self.assertRaisesRegex(TreeDBProtocolError, "work proof") as caught:
                client._decode_success(
                    200,
                    json.dumps(malformed_error_work).encode(),
                    dense_proof=True,
                    dense_score_plane=True,
                )
            self.assertIsNone(caught.exception.dense_work)
            self.assertEqual(caught.exception.score_plane, result.score_plane)
            error_payload = {
                "error": {
                    "code": "index_unavailable",
                    "message": "budget",
                    "dense_work": dense_work,
                    "score_plane": score_plane,
                }
            }
            for status in (200, 503):
                with self.subTest(error_status=status, query_mode="exact"):
                    with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, error_payload, 0)}) as error_server:
                        error_client = TreeDBClient(error_server.base_url, timeout=1)
                        with self.assertRaisesRegex(TreeDBProtocolError, "exact error unexpectedly includes") as caught:
                            error_client.query_by_embedding("docs", [1, 0], 1, query_mode="exact")
                        self.assertIsNotNone(caught.exception.dense_work)
                        self.assertIsNone(caught.exception.score_plane)
                        error_client.close()
                with self.subTest(error_status=status, query_mode="quantized_rerank"):
                    with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, error_payload, 0)}) as error_server:
                        error_client = TreeDBClient(error_server.base_url, timeout=1)
                        with self.assertRaises(IndexUnavailableError) as caught:
                            error_client.query_by_embedding(
                                "docs",
                                [1, 0],
                                1,
                                query_mode="quantized_rerank",
                                quantized_index_name="embedding.scalar_u8.public",
                            )
                        self.assertIsNotNone(caught.exception.dense_work)
                        self.assertIsNotNone(caught.exception.score_plane)
                        error_client.close()
            wide_default_error = copy.deepcopy(error_payload)
            wide_default_error["error"]["score_plane"].update(
                normalized_candidate_width=2,
                raw_candidate_width=2,
                rerank_candidate_cap=2,
            )
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (503, wide_default_error, 0)}) as error_server:
                error_client = TreeDBClient(error_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request"):
                    error_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                with self.assertRaises(IndexUnavailableError) as caught:
                    error_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        index_info=result.index,
                        expected_generation=result.index.generation,
                    )
                self.assertEqual(caught.exception.score_plane.normalized_candidate_width, 2)
                error_client.close()
            work_only_cases = [("pre-owner", pre_owner, False)]
            work_only_cases.extend(
                (route, replace(pre_owner, graph=replace(pre_owner.graph, route=route)), True)
                for route in ("typed_empty", "typed_exact", "typed_hnsw")
            )
            work_only_cases.extend((
                ("route-empty score", replace(pre_owner, graph=replace(pre_owner.graph, base_ann_scored=1)), True),
                ("output attempted", replace(pre_owner, output=replace(pre_owner.output, attempted=True)), True),
            ))
            for status in (200, 503):
                for name, candidate, rejected in work_only_cases:
                    work_only_payload = {
                        "error": {"code": "index_unavailable", "message": "budget", "dense_work": asdict(candidate)},
                    }
                    with self.subTest(work_only_error_status=status, work_only=name), \
                         FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, work_only_payload, 0)}) as error_server:
                        error_client = TreeDBClient(error_server.base_url, timeout=1)
                        expected = "proof does not match the request" if rejected else "index_unavailable"
                        error_type = TreeDBProtocolError if rejected else IndexUnavailableError
                        with self.assertRaisesRegex(error_type, expected) as caught:
                            error_client.query_by_embedding(
                                "docs", [1, 0], 1,
                                filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                                query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                            )
                        self.assertEqual(caught.exception.dense_work, candidate)
                        self.assertIsNone(caught.exception.score_plane)
                        error_client.close()
            proof_only_error_payload = copy.deepcopy(error_payload)
            del proof_only_error_payload["error"]["dense_work"]
            for status in (200, 503):
                with self.subTest(proof_only_error_status=status), \
                     FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, proof_only_error_payload, 0)}) as error_server:
                    error_client = TreeDBClient(error_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                        error_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    self.assertIsNone(caught.exception.dense_work)
                    self.assertEqual(caught.exception.score_plane, result.score_plane)
                    error_client.close()
            mismatched_error_payload = copy.deepcopy(error_payload)
            mismatched_error_payload["error"]["score_plane"]["quantized_index_name"] = "embedding.scalar_u8.other"
            for status in (200, 503):
                with self.subTest(mismatched_error_status=status), \
                     FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, mismatched_error_payload, 0)}) as error_server:
                    error_client = TreeDBClient(error_server.base_url, timeout=1)
                    with self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                        error_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    self.assertIsNotNone(caught.exception.dense_work)
                    self.assertIsNotNone(caught.exception.score_plane)
                    error_client.close()
            empty_manifest = replace(
                result.score_plane.snapshot.base_manifest,
                generation=0, format="", version=0, checksum=0,
            )
            unavailable_snapshot = replace(
                result.score_plane.snapshot,
                available=False, schema_hash=0, schema_generation=0,
                base_manifest=empty_manifest, current_manifest=empty_manifest,
                base_coverage_lsn=0, current_coverage_lsn=0,
            )
            unavailable_graph = replace(
                result.dense_work.graph,
                available=False, completed=False, route="",
                base_ann_scored=0, base_candidates=0, base_edges=0, delta_scored=0,
                exact_base_scored=0, base_shadowed=0, base_result_ids=0,
                snapshot=unavailable_snapshot,
            )
            for name, changed_proof, changed_work, request in (
                ("mode", replace(result.score_plane, requested_mode="exact"), result.dense_work, {}),
                ("proof unavailable", replace(result.score_plane, available=False), result.dense_work, {}),
                ("graph unavailable", result.score_plane,
                    replace(result.dense_work, graph=unavailable_graph), {}),
                ("proof snapshot unavailable", replace(result.score_plane, snapshot=unavailable_snapshot),
                    result.dense_work, {}),
                ("graph snapshot unavailable", result.score_plane,
                    replace(result.dense_work, graph=replace(result.dense_work.graph, snapshot=unavailable_snapshot)), {}),
                ("index name", replace(result.score_plane, quantized_index_name="embedding.scalar_u8.other"), result.dense_work, {}),
                ("top k", replace(result.score_plane, requested_top_k=2), result.dense_work, {}),
                ("EF", replace(result.score_plane, requested_ef_search=2), result.dense_work, {}),
                ("rerank limit", replace(result.score_plane, requested_rerank_candidates=2), result.dense_work, {}),
                ("future generation", replace(result.score_plane,
                    snapshot=replace(result.score_plane.snapshot, schema_generation=2)), result.dense_work,
                    {"expected_generation": 1}),
                ("sibling snapshot", replace(result.score_plane,
                    snapshot=replace(result.score_plane.snapshot, schema_hash=2)), result.dense_work, {}),
                ("unexpected filter", result.score_plane, replace(result.dense_work,
                    graph=replace(result.dense_work.graph, filter=replace(
                        result.dense_work.graph.filter, attempted=True, completed=True, eligible_rows=4097))), {}),
                ("missing requested filter work", result.score_plane, result.dense_work,
                    {"filter": {"field": "meta.repo", "operator": "==", "value": "gomap"}}),
            ):
                error = IndexUnavailableError(
                    "index_unavailable", "budget", dense_work=changed_work, score_plane=changed_proof,
                )
                with self.subTest(error_request_binding=name), \
                     mock.patch.object(client, "_request", side_effect=error), \
                     self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", **request,
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)

            # Normalized HTTP requests pin the caller-held generation even
            # when expected_generation was omitted from the public call.
            normalized_info = IndexInfo.from_dict({
                **typed_index, "vector_representation": "cosine_normalized_f32_v1",
            })
            for future in (False, True):
                snapshot = replace(result.score_plane.snapshot, schema_generation=2 if future else 1)
                proof = replace(result.score_plane, snapshot=snapshot,
                                requested_ef_search=normalized_info.vector_ef_search,
                                requested_rerank_candidates=normalized_info.vector_ef_search)
                work = replace(result.dense_work, graph=replace(result.dense_work.graph, snapshot=snapshot))
                error = IndexUnavailableError("index_unavailable", "budget", dense_work=work, score_plane=proof)
                with self.subTest(normalized_implicit_generation=future), \
                     mock.patch.object(client, "_request", side_effect=error) as request, \
                     self.assertRaises(TreeDBProtocolError if future else IndexUnavailableError) as caught:
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", diagnostics=True,
                        index_info=normalized_info,
                    )
                self.assertEqual(request.call_args.args[2]["expected_generation"], normalized_info.generation)
                self.assertEqual(caught.exception.dense_work, work)
                self.assertEqual(caught.exception.score_plane, proof)

            partial_proof = replace(
                result.score_plane, completed=False, reason="scoring interrupted",
                quantized_code_bytes_read=0, exact_base_vector_bytes_read=0,
            )
            partial_work = replace(
                result.dense_work, completed=False,
                graph=replace(result.dense_work.graph, completed=False),
                output=replace(
                    result.dense_work.output,
                    attempted=False, completed=False, requested=0, fetched=0, missing=0, output_bytes=0,
                    retained_payload_fetches=0, json_reconstruction_rows=0, typed_column_rows=0,
                ),
            )
            missing_reason_proof = asdict(partial_proof)
            missing_reason_proof["reason"] = ""
            with self.assertRaisesRegex(ValueError, "incomplete dense score-plane proof has no reason"):
                DenseScorePlaneProof.from_dict(missing_reason_proof)
            missing_reason_error = {
                "error": {
                    "code": "index_unavailable",
                    "message": "budget",
                    "dense_work": asdict(partial_work),
                    "score_plane": missing_reason_proof,
                }
            }
            for status in (200, 503):
                decoder = client._decode_success if status == 200 else client._decode_error
                with self.subTest(missing_reason_status=status), \
                     self.assertRaisesRegex(TreeDBProtocolError, "score-plane error proof") as caught:
                    decoder(
                        status,
                        json.dumps(missing_reason_error).encode(),
                        dense_proof=True,
                        dense_score_plane=True,
                    )
                self.assertEqual(caught.exception.dense_work, partial_work)
                self.assertIsNone(caught.exception.score_plane)
            partial_error = IndexUnavailableError(
                "index_unavailable", "budget", dense_work=partial_work, score_plane=partial_proof,
            )
            with mock.patch.object(client, "_request", side_effect=partial_error), \
                 self.assertRaises(IndexUnavailableError) as caught:
                client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertIs(caught.exception, partial_error)

            post_search = replace(
                result.dense_work,
                completed=False,
                output=replace(
                    result.dense_work.output,
                    attempted=False, completed=False, requested=0, fetched=0, missing=0, output_bytes=0,
                    retained_payload_fetches=0, json_reconstruction_rows=0, typed_column_rows=0,
                ),
            )
            partial_fetch = replace(
                post_search,
                output=replace(post_search.output, attempted=True, requested=1, retained_payload_fetches=1, typed_column_rows=1),
            )
            completed_fetch_missing = replace(
                post_search,
                output=replace(post_search.output, attempted=True, completed=True, requested=1, missing=1),
            )
            for name, candidate in (
                ("post-search before fetch", post_search),
                ("partial fetch", partial_fetch),
                ("completed fetch missing", completed_fetch_missing),
            ):
                error = IndexUnavailableError(
                    "index_unavailable", name, dense_work=candidate, score_plane=result.score_plane,
                )
                with self.subTest(valid_completion_prefix=name), mock.patch.object(client, "_request", side_effect=error), \
                     self.assertRaises(IndexUnavailableError) as caught:
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertIs(caught.exception, error)

            proof_incomplete = replace(result.score_plane, completed=False, reason="scoring interrupted")

            def with_advanced_frontier(candidate_work, candidate_proof):
                snapshot = candidate_proof.snapshot
                if snapshot.current_manifest.generation != snapshot.base_manifest.generation:
                    return candidate_work, candidate_proof
                advanced = replace(
                    snapshot,
                    current_manifest=replace(
                        snapshot.current_manifest,
                        generation=snapshot.current_manifest.generation + 1,
                        checksum=snapshot.current_manifest.checksum + 1,
                    ),
                    current_coverage_lsn=snapshot.current_coverage_lsn + 1,
                )
                return (
                    replace(candidate_work, graph=replace(candidate_work.graph, snapshot=advanced)),
                    replace(candidate_proof, snapshot=advanced),
                )

            def incomplete_prefix(graph_route):
                candidate_work, candidate_proof = partial_work, partial_proof
                if graph_route in ("", "typed_empty"):
                    candidate_work = replace(candidate_work, graph=replace(
                        candidate_work.graph, route=graph_route,
                        base_ann_scored=0, base_candidates=0, base_edges=0, delta_scored=0,
                        exact_base_scored=0, base_shadowed=0, base_result_ids=0,
                    ))
                    candidate_proof = replace(
                        candidate_proof, route="quantized_rerank" if not graph_route else "typed_empty",
                        raw_retained_candidates=0, live_shortlist_candidates=0, actual_rerank_candidates=0,
                        quantized_score_calls=0, quantized_code_bytes_read=0,
                        exact_base_rerank_score_calls=0, exact_small_filter_score_calls=0,
                        exact_suffix_score_calls=0, exact_base_vector_bytes_read=0,
                        exact_suffix_vector_bytes_read=0,
                    )
                elif graph_route == "typed_exact":
                    candidate_work = replace(candidate_work, graph=replace(
                        candidate_work.graph, route="typed_exact", base_ann_scored=0,
                        exact_base_scored=0, base_result_ids=0, delta_scored=1,
                    ))
                    candidate_proof = replace(
                        candidate_proof, route="typed_exact", quantized_score_calls=0,
                        raw_retained_candidates=0, live_shortlist_candidates=0, actual_rerank_candidates=0,
                        quantized_code_bytes_read=0, exact_base_rerank_score_calls=0,
                        exact_small_filter_score_calls=0, exact_suffix_score_calls=1,
                        exact_base_vector_bytes_read=0, exact_suffix_vector_bytes_read=8,
                    )
                    candidate_work, candidate_proof = with_advanced_frontier(
                        candidate_work, candidate_proof
                    )
                return candidate_work, candidate_proof

            prefixes = {route: incomplete_prefix(route) for route in ("", "typed_empty", "typed_exact", "typed_hnsw")}
            prefixes["typed_hnsw"] = with_advanced_frontier(*prefixes["typed_hnsw"])
            zero_width_work, zero_width_proof = prefixes["typed_exact"]
            zero_width_proof = replace(
                zero_width_proof,
                normalized_candidate_width=0, raw_candidate_width=0, rerank_candidate_cap=0,
            )
            prefixes["typed_exact_zero_width"] = (zero_width_work, zero_width_proof)
            for route, (candidate_work, candidate_proof) in prefixes.items():
                error = IndexUnavailableError(
                    "index_unavailable", "valid prefix", dense_work=candidate_work, score_plane=candidate_proof,
                )
                with self.subTest(valid_counter_prefix=route), mock.patch.object(client, "_request", side_effect=error), \
                     self.assertRaises(IndexUnavailableError) as caught:
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertIs(caught.exception, error)

            def counter_failure(candidate_work, candidate_proof, field):
                if field == "quantized":
                    return candidate_work, replace(
                        candidate_proof, quantized_score_calls=candidate_proof.quantized_score_calls + 1)
                if field == "exact base":
                    return replace(candidate_work, graph=replace(
                        candidate_work.graph, exact_base_scored=candidate_work.graph.exact_base_scored + 1)), candidate_proof
                if field == "base result IDs":
                    return replace(candidate_work, graph=replace(
                        candidate_work.graph, base_result_ids=candidate_work.graph.base_result_ids + 1)), candidate_proof
                if field == "suffix":
                    candidate_work, candidate_proof = with_advanced_frontier(
                        candidate_work, candidate_proof
                    )
                    return candidate_work, replace(
                        candidate_proof, exact_suffix_score_calls=candidate_proof.exact_suffix_score_calls + 1)
                return candidate_work, replace(
                    candidate_proof, exact_base_rerank_score_calls=(1 << 64) - 1,
                    exact_small_filter_score_calls=1)

            counter_prefix_failures = tuple(
                (f"prefix counters {route or 'empty'} {field}", *counter_failure(*prefixes[route], field))
                for route in ("", "typed_exact", "typed_hnsw")
                for field in ("quantized", "exact base", "base result IDs", "suffix", "overflow")
            )
            prefix_work, prefix_proof = prefixes["typed_hnsw"]
            zero_width_work, zero_width_proof = prefixes["typed_exact_zero_width"]
            prefix_shape_failures = (
                ("prefix shape base candidates exceed quantized calls", replace(
                    prefix_work, graph=replace(
                        prefix_work.graph, base_candidates=prefix_proof.quantized_score_calls + 1)), prefix_proof),
                ("prefix shape rerank cap does not match plan", prefix_work, replace(
                    prefix_proof, rerank_candidate_cap=prefix_proof.rerank_candidate_cap - 1)),
                ("prefix shape normalized width exceeds explicit EF", prefix_work, replace(
                    prefix_proof, normalized_candidate_width=9, raw_candidate_width=9, rerank_candidate_cap=8)),
                ("prefix shape normalized width exceeds raw width", prefix_work, replace(
                    prefix_proof, raw_candidate_width=prefix_proof.raw_candidate_width - 1)),
                ("prefix shape raw retained exceeds quantized calls", prefix_work, replace(
                    prefix_proof, raw_candidate_width=3, raw_retained_candidates=3)),
                ("prefix shape raw retained exceeds raw width", replace(
                    prefix_work, graph=replace(prefix_work.graph, base_ann_scored=3)), replace(
                        prefix_proof, quantized_score_calls=3, raw_retained_candidates=3)),
                ("prefix shape live shortlist exceeds raw retained", prefix_work, replace(
                    prefix_proof, normalized_candidate_width=3, raw_candidate_width=3,
                    rerank_candidate_cap=3, live_shortlist_candidates=3)),
                ("prefix shape live shortlist exceeds normalized width", replace(
                    prefix_work, graph=replace(prefix_work.graph, base_ann_scored=3)), replace(
                        prefix_proof, quantized_score_calls=3, raw_candidate_width=3,
                        raw_retained_candidates=3, live_shortlist_candidates=3)),
                ("prefix shape actual rerank exceeds shortlist", replace(
                    prefix_work, graph=replace(
                        prefix_work.graph, base_ann_scored=3, exact_base_scored=3, base_result_ids=3)), replace(
                            prefix_proof, normalized_candidate_width=3, raw_candidate_width=3,
                            rerank_candidate_cap=3, raw_retained_candidates=3, quantized_score_calls=3,
                            actual_rerank_candidates=3, exact_base_rerank_score_calls=3)),
                ("prefix shape actual rerank exceeds cap", replace(
                    prefix_work, graph=replace(prefix_work.graph, exact_base_scored=3, base_result_ids=3)), replace(
                        prefix_proof, actual_rerank_candidates=3, exact_base_rerank_score_calls=3)),
                ("prefix shape actual rerank differs from exact base calls", replace(
                    prefix_work, graph=replace(prefix_work.graph, exact_base_scored=0, base_result_ids=0)), replace(
                        prefix_proof, exact_base_rerank_score_calls=0)),
                ("zero-width prefix raw candidate width", zero_width_work, replace(
                    zero_width_proof, raw_candidate_width=1)),
                ("zero-width prefix base scoring", replace(
                    zero_width_work, graph=replace(
                        zero_width_work.graph, delta_scored=0, exact_base_scored=1, base_result_ids=1)), replace(
                            zero_width_proof, exact_suffix_score_calls=0, exact_small_filter_score_calls=1)),
                ("zero-width prefix base shadowing", replace(
                    zero_width_work, graph=replace(zero_width_work.graph, base_shadowed=1)), zero_width_proof),
            )
            route_empty_incomplete, route_empty_proof = prefixes[""]
            completion_prefix_failures = (
                ("proof complete graph incomplete", replace(post_search, graph=replace(post_search.graph, completed=False)), result.score_plane),
                ("graph complete proof incomplete", post_search, proof_incomplete),
                ("route", replace(post_search, graph=replace(post_search.graph, route="typed_exact")), result.score_plane),
                ("snapshot", post_search, replace(result.score_plane, snapshot=replace(result.score_plane.snapshot, schema_hash=2))),
                ("filter ownership", replace(post_search, graph=replace(post_search.graph, filter=pre_owner_filter)), result.score_plane),
                ("graph counters", replace(post_search, graph=replace(post_search.graph, base_ann_scored=2)), result.score_plane),
                ("planning counters", post_search, replace(result.score_plane, rerank_candidate_cap=0)),
                ("byte counters", post_search, replace(result.score_plane, quantized_code_bytes_read=1)),
                ("output requested", replace(post_search, output=replace(post_search.output, attempted=True)), result.score_plane),
                ("fetched beyond retained", replace(post_search, output=replace(post_search.output, attempted=True, requested=1, fetched=1, json_reconstruction_rows=1)), result.score_plane),
                ("retained plus missing", replace(post_search, output=replace(post_search.output, attempted=True, requested=1, missing=1, retained_payload_fetches=1)), result.score_plane),
                ("JSON rows", replace(post_search, output=replace(post_search.output, attempted=True, requested=1, fetched=1, retained_payload_fetches=1)), result.score_plane),
                ("typed rows", replace(post_search, output=replace(post_search.output, attempted=True, requested=1, typed_column_rows=1)), result.score_plane),
                ("bytes before fetch", replace(post_search, output=replace(post_search.output, attempted=True, requested=1, output_bytes=1)), result.score_plane),
                ("completed partial output", replace(post_search, output=replace(post_search.output, attempted=True, completed=True, requested=1)), result.score_plane),
                ("outer complete before output", replace(post_search, completed=True), result.score_plane),
                ("proof unavailable", partial_work, replace(partial_proof, available=False)),
                ("graph unavailable", replace(partial_work, graph=unavailable_graph), partial_proof),
                ("proof snapshot unavailable", partial_work, replace(partial_proof, snapshot=unavailable_snapshot)),
                ("graph snapshot unavailable", replace(
                    partial_work, graph=replace(partial_work.graph, snapshot=unavailable_snapshot)), partial_proof),
                ("incomplete route", replace(post_search, graph=replace(post_search.graph, completed=False, route="typed_exact")), proof_incomplete),
                ("empty graph typed-empty proof", route_empty_incomplete, replace(proof_incomplete, route="typed_empty")),
                ("empty graph typed-exact proof", route_empty_incomplete, replace(proof_incomplete, route="typed_exact")),
            ) + counter_prefix_failures + prefix_shape_failures
            for name, candidate_work, candidate_proof in completion_prefix_failures:
                error = IndexUnavailableError(
                    "index_unavailable", name, dense_work=candidate_work, score_plane=candidate_proof,
                )
                with self.subTest(invalid_completion_prefix=name), mock.patch.object(client, "_request", side_effect=error), \
                     self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                    client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertEqual(caught.exception.dense_work, candidate_work)
                self.assertEqual(caught.exception.score_plane, candidate_proof)

            route_empty_error = IndexUnavailableError(
                "index_unavailable", "route empty", dense_work=route_empty_incomplete, score_plane=route_empty_proof,
            )
            with mock.patch.object(client, "_request", side_effect=route_empty_error), \
                 self.assertRaises(IndexUnavailableError) as caught:
                client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertIs(caught.exception, route_empty_error)

            completion_prefix_by_name = {
                name: (candidate_work, candidate_proof)
                for name, candidate_work, candidate_proof in completion_prefix_failures
            }
            http_failure_names = (
                "proof complete graph incomplete", "graph complete proof incomplete", "route", "snapshot",
                "byte counters", "output requested", "proof unavailable", "graph unavailable",
                "proof snapshot unavailable", "graph snapshot unavailable", "incomplete route",
                "empty graph typed-empty proof", "empty graph typed-exact proof",
                "prefix counters empty quantized", "prefix counters typed_exact exact base",
                "prefix counters typed_hnsw suffix", "prefix counters typed_hnsw overflow",
            ) + tuple(name for name, _, _ in prefix_shape_failures)
            http_completion_cases = (
                ("post-search before fetch", post_search, result.score_plane, False),
                ("partial fetch", partial_fetch, result.score_plane, False),
                ("completed fetch missing", completed_fetch_missing, result.score_plane, False),
            ) + tuple(
                (name, *completion_prefix_by_name[name], True)
                for name in http_failure_names
            )
            for status in (200, 503):
                for name, candidate_work, candidate_proof, rejected in http_completion_cases:
                    error_payload = {"error": {
                        "code": "index_unavailable", "message": name,
                        "dense_work": asdict(candidate_work), "score_plane": asdict(candidate_proof),
                    }}
                    with self.subTest(http_completion_status=status, completion_prefix=name), \
                         FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (status, error_payload, 0)}) as error_server:
                        error_client = TreeDBClient(error_server.base_url, timeout=1)
                        error_type = TreeDBProtocolError if rejected else IndexUnavailableError
                        expected = "proof does not match the request" if rejected else "index_unavailable"
                        with self.assertRaisesRegex(error_type, expected) as caught:
                            error_client.query_by_embedding(
                                "docs", [1, 0], 1, query_mode="quantized_rerank",
                                quantized_index_name="embedding.scalar_u8.public",
                            )
                        self.assertEqual(caught.exception.dense_work, candidate_work)
                        self.assertEqual(caught.exception.score_plane, candidate_proof)
                        error_client.close()

            for proof_name, field in (
                ("dense_work", "manifest identity"),
                ("dense_work", "coverage"),
                ("dense_work", "delta score"),
                ("dense_work", "shadowed base"),
                ("score_plane", "manifest identity"),
                ("score_plane", "coverage"),
                ("score_plane", "suffix score"),
                ("score_plane", "suffix bytes"),
                ("score_plane", "shadow allowance"),
            ):
                hostile_error = {
                    "error": {
                        "code": "index_unavailable",
                        "message": "hostile unchanged frontier",
                        "dense_work": asdict(result.dense_work),
                        "score_plane": asdict(result.score_plane),
                    }
                }
                target = hostile_error["error"][proof_name]
                snapshot = target["graph"]["snapshot"] if proof_name == "dense_work" else target["snapshot"]
                if field == "manifest identity":
                    snapshot["current_manifest"]["checksum"] += 1
                elif field == "coverage":
                    snapshot["current_coverage_lsn"] += 1
                elif field == "delta score":
                    target["graph"]["delta_scored"] = 1
                elif field == "shadowed base":
                    target["graph"]["base_shadowed"] = 1
                elif field == "suffix score":
                    target["exact_suffix_score_calls"] = 1
                elif field == "suffix bytes":
                    target["exact_suffix_vector_bytes_read"] = 8
                else:
                    target["raw_candidate_width"] = target["normalized_candidate_width"] + 1
                with self.subTest(http_unchanged_error=(proof_name, field)), \
                     FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (503, hostile_error, 0)}) as hostile_server:
                    hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                    with self.assertRaisesRegex(
                            TreeDBProtocolError, "dense score-plane proof does not match the request on error") as caught:
                        hostile_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    if proof_name == "dense_work":
                        self.assertIsNone(caught.exception.dense_work)
                        self.assertEqual(caught.exception.score_plane, result.score_plane)
                    else:
                        self.assertEqual(caught.exception.dense_work, result.dense_work)
                        self.assertIsNone(caught.exception.score_plane)
                    hostile_client.close()

            completed_work_only_error = IndexUnavailableError(
                "index_unavailable", "budget", dense_work=result.dense_work,
            )
            with mock.patch.object(client, "_request", side_effect=completed_work_only_error), \
                 self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            self.assertEqual(caught.exception.dense_work, result.dense_work)
            self.assertIsNone(caught.exception.score_plane)

            malformed = TreeDBProtocolError(
                "malformed success", dense_work=result.dense_work,
                score_plane=replace(result.score_plane, requested_top_k=2),
            )
            with mock.patch.object(client, "_request", side_effect=malformed), \
                 self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request"):
                client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
            filtered_payload = copy.deepcopy(payload)
            filtered_payload["dense_work"]["graph"]["base_candidates"] = 1
            filtered_payload["dense_work"]["graph"]["filter"].update(attempted=True, completed=True, eligible_rows=4097)
            filtered_payload["documents"][0]["meta"] = {"repo": "gomap"}
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, filtered_payload, 0)}) as filtered_server:
                filtered_client = TreeDBClient(filtered_server.base_url, timeout=1)
                filtered_result = filtered_client.query_by_embedding(
                    "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                    query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertEqual(filtered_result.documents[0].id, "a")
                filtered_client.close()
            raw_filtered = json.dumps(filtered_payload, separators=(",", ":")).encode("utf-8")
            duplicate_filter_field = raw_filtered.replace(
                b'"meta":{"repo":"gomap"}',
                b'"meta":{"repo":"other","repo":"gomap"}',
                1,
            )
            nested_payload = copy.deepcopy(filtered_payload)
            nested_payload["documents"][0]["meta"]["items"] = [{"rank": 2}]
            duplicate_nested_field = json.dumps(nested_payload, separators=(",", ":")).encode("utf-8").replace(
                b'"rank":2', b'"rank":1,"rank":2', 1,
            )
            for name, raw_payload in (
                ("filter field", duplicate_filter_field),
                ("nested list object", duplicate_nested_field),
            ):
                with self.subTest(http_duplicate_metadata_field=name), \
                     FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, raw_payload, 0)}) as hostile_server:
                    hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                    with self.assertRaisesRegex(
                        TreeDBProtocolError, "duplicate field in dense proof envelope"
                    ) as caught:
                        hostile_client.query_by_embedding(
                            "docs", [1, 0], 1,
                            filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                            query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    self.assertIsNotNone(caught.exception.dense_work)
                    self.assertIsNotNone(caught.exception.score_plane)
                    hostile_client.close()
            for name, meta in (("mismatching", {"repo": "other"}), ("missing", {})):
                hostile_filtered_payload = copy.deepcopy(filtered_payload)
                hostile_filtered_payload["documents"][0]["meta"] = meta
                with self.subTest(http_filter_result=name), \
                     FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, hostile_filtered_payload, 0)}) as hostile_server:
                    hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                    with self.assertRaisesRegex(
                        TreeDBProtocolError, "does not satisfy the request filter"
                    ) as caught:
                        hostile_client.query_by_embedding(
                            "docs", [1, 0], 1,
                            filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                            query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    self.assertIsNotNone(caught.exception.dense_work)
                    self.assertIsNotNone(caught.exception.score_plane)
                    hostile_client.close()
            numeric_filtered_payload = copy.deepcopy(filtered_payload)
            numeric_filtered_payload["documents"][0]["meta"] = {"number": (1 << 53) + 1}
            with FixtureServer({
                ("POST", "/v1/indexes/docs/search/vector"): (200, numeric_filtered_payload, 0),
            }) as hostile_server:
                hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                with self.assertRaisesRegex(
                    TreeDBProtocolError, "does not satisfy the request filter"
                ) as caught:
                    hostile_client.query_by_embedding(
                        "docs", [1, 0], 1,
                        filter={"field": "meta.number", "operator": "==", "value": 1 << 53},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)
                hostile_client.close()
            filtered_underreported = copy.deepcopy(filtered_payload)
            filtered_underreported["dense_work"]["graph"]["base_candidates"] = 0
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, filtered_underreported, 0)}) as filtered_server:
                filtered_client = TreeDBClient(filtered_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    filtered_client.query_by_embedding(
                        "docs", [1, 0], 1,
                        filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public",
                    )
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
            packed_filtered_exact = copy.deepcopy(filtered_exact)
            packed_filtered_exact["dense_work"]["graph"].update(
                base_ann_scored=4097, exact_base_scored=4097, base_result_ids=4097,
            )
            packed_filtered_exact["dense_work"]["graph"]["filter"]["eligible_rows"] = 4097
            packed_filtered_exact["score_plane"].update(
                exact_small_filter_score_calls=4097,
                exact_base_vector_bytes_read=4097 * 8,
                packed_score_batch_calls=1,
                packed_score_candidates=4097,
                packed_vector_bytes_read=4097 * 8,
                forbidden_stable_score_calls=0,
            )
            with FixtureServer({
                ("POST", "/v1/indexes/docs/search/vector"): (200, packed_filtered_exact, 0),
            }) as packed_exact_server:
                packed_exact_client = TreeDBClient(packed_exact_server.base_url, timeout=1)
                packed_result = packed_exact_client.query_by_embedding(
                    "docs", [1, 0], 1,
                    filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertTrue(dense_quantized_response_work_matches(
                    packed_result.dense_work, packed_result.score_plane, 1, 1, True,
                ))
                packed_exact_client.close()
            zero_width_filtered_exact = copy.deepcopy(filtered_exact)
            zero_width_filtered_exact["dense_work"]["graph"].update(
                delta_scored=1, exact_base_scored=0, base_shadowed=0, base_result_ids=0,
            )
            zero_width_filtered_exact["score_plane"].update(
                normalized_candidate_width=0, raw_candidate_width=0, rerank_candidate_cap=0,
                exact_small_filter_score_calls=0, exact_base_vector_bytes_read=0,
                exact_suffix_score_calls=1, exact_suffix_vector_bytes_read=8,
            )
            for target in (
                zero_width_filtered_exact["dense_work"]["graph"]["snapshot"],
                zero_width_filtered_exact["score_plane"]["snapshot"],
            ):
                target["current_manifest"] = {
                    **target["base_manifest"],
                    "generation": target["base_manifest"]["generation"] + 1,
                    "checksum": target["base_manifest"]["checksum"] + 1,
                }
                target["current_coverage_lsn"] = target["base_coverage_lsn"] + 1
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, zero_width_filtered_exact, 0)}) as zero_width_server:
                zero_width_client = TreeDBClient(zero_width_server.base_url, timeout=1)
                self.assertEqual(
                    zero_width_client.query_by_embedding(
                        "docs", [1, 0], 1,
                        filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    ).documents[0].id,
                    "a",
                )
                zero_width_client.close()
            zero_width_hostiles = []
            zero_width_raw = copy.deepcopy(zero_width_filtered_exact)
            zero_width_raw["score_plane"]["raw_candidate_width"] = 1
            zero_width_hostiles.append(("raw candidate width", zero_width_raw))
            zero_width_base_score = copy.deepcopy(zero_width_filtered_exact)
            zero_width_base_score["dense_work"]["graph"].update(
                delta_scored=0, exact_base_scored=1, base_result_ids=1,
            )
            zero_width_base_score["score_plane"].update(
                exact_small_filter_score_calls=1, exact_base_vector_bytes_read=8,
                exact_suffix_score_calls=0, exact_suffix_vector_bytes_read=0,
            )
            zero_width_hostiles.append(("base scoring", zero_width_base_score))
            zero_width_shadow = copy.deepcopy(zero_width_filtered_exact)
            zero_width_shadow["dense_work"]["graph"]["base_shadowed"] = 1
            zero_width_hostiles.append(("base shadowing", zero_width_shadow))
            for name, hostile in zero_width_hostiles:
                if name != "base shadowing":
                    with self.subTest(zero_width_parser=name), self.assertRaises(ValueError):
                        DenseScorePlaneProof.from_dict(hostile["score_plane"])
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, hostile, 0)}) as hostile_server:
                    hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                    with self.subTest(zero_width=name), self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        hostile_client.query_by_embedding(
                            "docs", [1, 0], 1,
                            filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                            query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    hostile_client.close()
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
            reversed_manifest = copy.deepcopy(payload)
            reversed_manifest_snapshot = copy.deepcopy(snapshot)
            reversed_manifest_snapshot["base_manifest"] = {**manifest, "generation": 2}
            reversed_manifest_snapshot["current_manifest"] = {**manifest, "generation": 1}
            reversed_manifest["dense_work"]["graph"]["snapshot"] = reversed_manifest_snapshot
            reversed_manifest["score_plane"]["snapshot"] = copy.deepcopy(reversed_manifest_snapshot)
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, reversed_manifest, 0)}) as reversed_server:
                reversed_client = TreeDBClient(reversed_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "manifest generation"):
                    reversed_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                reversed_client.close()
            for proof_name in ("dense_work", "score_plane"):
                mismatched_identity = copy.deepcopy(payload)
                if proof_name == "dense_work":
                    target = copy.deepcopy(mismatched_identity[proof_name]["graph"]["snapshot"])
                    mismatched_identity[proof_name]["graph"]["snapshot"] = target
                else:
                    target = copy.deepcopy(mismatched_identity[proof_name]["snapshot"])
                    mismatched_identity[proof_name]["snapshot"] = target
                target["current_manifest"] = copy.deepcopy(target["base_manifest"])
                target["current_manifest"]["checksum"] += 1
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, mismatched_identity, 0)}) as identity_server:
                    identity_client = TreeDBClient(identity_server.base_url, timeout=1)
                    with self.subTest(equal_generation_manifest_identity=proof_name), self.assertRaisesRegex(
                            TreeDBProtocolError, "score-plane proof"):
                        identity_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    identity_client.close()
            for proof_name in ("dense_work", "score_plane"):
                mismatched_coverage = copy.deepcopy(payload)
                if proof_name == "dense_work":
                    target = copy.deepcopy(mismatched_coverage[proof_name]["graph"]["snapshot"])
                    mismatched_coverage[proof_name]["graph"]["snapshot"] = target
                else:
                    target = copy.deepcopy(mismatched_coverage[proof_name]["snapshot"])
                    mismatched_coverage[proof_name]["snapshot"] = target
                target["current_manifest"] = copy.deepcopy(target["base_manifest"])
                target["current_coverage_lsn"] = target["base_coverage_lsn"] + 1
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, mismatched_coverage, 0)}) as coverage_server:
                    coverage_client = TreeDBClient(coverage_server.base_url, timeout=1)
                    with self.subTest(equal_manifest_coverage=proof_name), self.assertRaisesRegex(
                            TreeDBProtocolError, "score-plane proof"):
                        coverage_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    coverage_client.close()
            for proof_name, field, value in (
                ("dense_work", "delta_scored", 1),
                ("dense_work", "base_shadowed", 1),
                ("score_plane", "exact_suffix_score_calls", 1),
                ("score_plane", "exact_suffix_vector_bytes_read", 8),
                ("score_plane", "raw_candidate_width", 2),
            ):
                unchanged_frontier_work = copy.deepcopy(payload)
                target = (
                    unchanged_frontier_work[proof_name]["graph"]
                    if proof_name == "dense_work"
                    else unchanged_frontier_work[proof_name]
                )
                target[field] = value
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, unchanged_frontier_work, 0)}) as frontier_server:
                    frontier_client = TreeDBClient(frontier_server.base_url, timeout=1)
                    with self.subTest(unchanged_manifest_work=(proof_name, field)), self.assertRaisesRegex(
                            TreeDBProtocolError, "score-plane proof"):
                        frontier_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    frontier_client.close()
            newer_aggregate = copy.deepcopy(payload)
            newer_aggregate["index"]["generation"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, newer_aggregate, 0)}) as aggregate_server:
                aggregate_client = TreeDBClient(aggregate_server.base_url, timeout=1)
                aggregate_result = aggregate_client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertEqual(aggregate_result.index.generation, 2)
                aggregate_client.close()
            newer_snapshot_generation = copy.deepcopy(payload)
            newer_snapshot_generation["dense_work"]["graph"]["snapshot"]["schema_generation"] = 2
            newer_snapshot_generation["score_plane"]["snapshot"]["schema_generation"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, newer_snapshot_generation, 0)}) as generation_server:
                generation_client = TreeDBClient(generation_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    generation_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                generation_client.close()
            for invalid_generation in (0, -1, 1 << 64):
                invalid_index_generation = copy.deepcopy(payload)
                invalid_index_generation["index"]["generation"] = invalid_generation
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid_index_generation, 0)}) as generation_server:
                    generation_client = TreeDBClient(generation_server.base_url, timeout=1)
                    with self.subTest(invalid_index_generation=invalid_generation), self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                        generation_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    generation_client.close()
            for field in ("schema_hash", "schema_generation", "base_coverage_lsn"):
                missing_snapshot_identity = copy.deepcopy(payload)
                missing_snapshot_identity["dense_work"]["graph"]["snapshot"][field] = 0
                missing_snapshot_identity["score_plane"]["snapshot"][field] = 0
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, missing_snapshot_identity, 0)}) as identity_server:
                    identity_client = TreeDBClient(identity_server.base_url, timeout=1)
                    with self.subTest(missing_snapshot_identity=field), self.assertRaises(TreeDBProtocolError):
                        identity_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    identity_client.close()
            for field in ("generation", "version", "checksum"):
                incomplete_manifest = copy.deepcopy(payload)
                incomplete_manifest["dense_work"]["graph"]["snapshot"]["base_manifest"][field] = 0
                incomplete_manifest["score_plane"]["snapshot"]["base_manifest"][field] = 0
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, incomplete_manifest, 0)}) as manifest_server:
                    manifest_client = TreeDBClient(manifest_server.base_url, timeout=1)
                    with self.subTest(incomplete_manifest=field), self.assertRaises(TreeDBProtocolError):
                        manifest_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    manifest_client.close()
            unsupported_manifest = copy.deepcopy(payload)
            unsupported_manifest["dense_work"]["graph"]["snapshot"]["base_manifest"]["version"] = 2
            unsupported_manifest["score_plane"]["snapshot"]["base_manifest"]["version"] = 2
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, unsupported_manifest, 0)}) as manifest_server:
                manifest_client = TreeDBClient(manifest_server.base_url, timeout=1)
                with self.assertRaises(TreeDBProtocolError):
                    manifest_client.query_by_embedding(
                        "docs", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                manifest_client.close()
            for name, item_id in (
                ("empty", ""),
                ("whitespace only", " \t"),
                ("leading whitespace", " a"),
                ("trailing whitespace", "a "),
                ("Unicode whitespace", "\u2000a"),
                ("lone surrogate", "\ud800"),
            ):
                invalid_id = copy.deepcopy(payload)
                invalid_id["documents"][0]["id"] = item_id
                with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, invalid_id, 0)}) as invalid_id_server:
                    invalid_id_client = TreeDBClient(invalid_id_server.base_url, timeout=1)
                    with self.subTest(invalid_id=name), self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof") as caught:
                        invalid_id_client.query_by_embedding(
                            "docs", [1, 0], 1, query_mode="quantized_rerank",
                            quantized_index_name="embedding.scalar_u8.public",
                        )
                    self.assertIsNotNone(caught.exception.dense_work)
                    self.assertIsNotNone(caught.exception.score_plane)
                    invalid_id_client.close()
            boundary_id = copy.deepcopy(payload)
            boundary_id["documents"][0]["id"] = "\x1ca"
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, boundary_id, 0)}) as boundary_server:
                boundary_client = TreeDBClient(boundary_server.base_url, timeout=1)
                boundary_result = boundary_client.query_by_embedding(
                    "docs", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertEqual(boundary_result.documents[0].id, "\x1ca")
                boundary_client.close()
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
            duplicate["document_materialization_rows"] = 2
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
            underfill["document_materialization_rows"] = 1
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
            for target in (
                exact_route["dense_work"]["graph"]["snapshot"],
                exact_route["score_plane"]["snapshot"],
            ):
                target["current_manifest"] = {
                    **target["base_manifest"],
                    "generation": target["base_manifest"]["generation"] + 1,
                    "checksum": target["base_manifest"]["checksum"] + 1,
                }
                target["current_coverage_lsn"] = target["base_coverage_lsn"] + 1
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
            valid_empty["document_materialization_rows"] = 0
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
                empty_result = empty_client.query_by_embedding(
                    "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                )
                self.assertEqual(empty_result.documents, [])
                self.assertTrue(dense_quantized_response_work_matches(
                    empty_result.dense_work, empty_result.score_plane, 1, 0, True,
                ))
                self.assertFalse(dense_quantized_response_work_matches(
                    replace(empty_result.dense_work, output=replace(empty_result.dense_work.output, output_bytes=1)),
                    empty_result.score_plane, 1, 0, True,
                ))
                empty_client.close()
            nonzero_empty_output = copy.deepcopy(valid_empty)
            nonzero_empty_output["dense_work"]["output"]["output_bytes"] = 1
            with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, nonzero_empty_output, 0)}) as hostile_server:
                hostile_client = TreeDBClient(hostile_server.base_url, timeout=1)
                with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                    hostile_client.query_by_embedding(
                        "docs", [1, 0], 1, filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                    )
                hostile_client.close()
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
            overflow["document_materialization_rows"] = 2
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
                invalid["document_materialization_rows"] = 0
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
                text_query_mode="literal",
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
                    "text_query_mode": "literal",
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
                text_query_mode="literal",
                text_operator="and",
                candidate_limit=50,
                text_candidate_limit=25,
                max_postings_scanned=1000,
                vector_candidate_limit=30,
                vector_query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.fast",
                quantized_rerank_candidates=32,
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
            self.assertEqual(body["text_query_mode"], "literal")
            self.assertEqual(body["text_operator"], "and")
            self.assertEqual(body["max_postings_scanned"], 1000)
            self.assertEqual(body["vector_candidate_limit"], 30)
            self.assertEqual(body["vector_query_mode"], "quantized_rerank")
            self.assertEqual(body["quantized_index_name"], "embedding.scalar_u8.fast")
            self.assertEqual(body["quantized_rerank_candidates"], 32)
            self.assertEqual(body["max_chunks_per_parent"], 1)
            self.assertEqual(body["return_embedding"], False)

    def test_hybrid_search_requires_query_or_embedding_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaises(InvalidRequestError) as caught:
            client.search_hybrid("docs", top_k=1)

        self.assertEqual(caught.exception.code, "invalid_request")
        self.assertIn("query or query_embedding", caught.exception.message)

        with self.assertRaises(InvalidRequestError) as lexical:
            client.search_hybrid(
                "docs", query_embedding=[1.0, 0.0], top_k=1, text_query_mode="literal"
            )
        self.assertIn("lexical options require query", lexical.exception.message)

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
