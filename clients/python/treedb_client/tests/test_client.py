from __future__ import annotations

import base64
import http.client
import json
import os
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
    TreeDBClient,
    TreeDBConfigError,
    TreeDBProtocolError,
    TreeDBTimeoutError,
    TreeDBTransportError,
    UnsupportedError,
)


SAMPLE_INDEX = {
    "name": "docs",
    "dimension": 2,
    "metric": "cosine",
    "generation": 1,
    "contract_version": "treedb-document-service/v1alpha1",
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

    def test_count_filter_search_and_delete_by_filter_parse_responses(self) -> None:
        routes = {
            ("POST", "/v1/indexes/docs/documents/count"): (200, {"index": SAMPLE_INDEX, "count": 2}, 0),
            ("POST", "/v1/indexes/docs/documents/filter"): (
                200,
                {
                    "index": SAMPLE_INDEX,
                    "matched_count": 1,
                    "documents": [{"id": "a", "content": "alpha", "meta": {"repo": "gomap"}}],
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
            self.assertEqual(client.filter_documents("docs", limit=10).documents[0].id, "a")
            self.assertEqual(client.query_by_embedding("docs", [1, 0], 1).documents[0].score, 1.0)
            self.assertEqual(client.delete_by_filter("docs", {"field": "meta.repo", "operator": "$eq", "value": "gomap"}).deleted, 1)

            delete_body = json_body(server.records[-1])
            self.assertEqual(delete_body["filter"], {"field": "meta.repo", "operator": "==", "value": "gomap"})


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
                    "status": {"name": "embedding", "strategy": "column_graph", "state": "column_graph_loaded", "loaded": True},
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
                "final_top_k": 5,
            },
            "snapshot": {"consistency": "current_snapshot", "commit_seq": 9},
            "stats": {"text_candidates_returned": 3, "vector_candidates_returned": 4, "fusion_both": 1, "documents_fetched": 5},
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
            self.assertEqual(result.snapshot.consistency, "current_snapshot")
            self.assertEqual(result.stats.fusion_both, 1)
            self.assertEqual(result.documents[0].meta["_treedb_search"]["type"], "hybrid")
            body = json_body(server.records[0])
            self.assertEqual(body["query_embedding"], [0.1, 0.2])
            self.assertEqual(body["fusion"]["source_order"], ["text", "vector"])
            self.assertEqual(body["fusion"]["tie_policy"], "fused_score_best_rank_source_order_id")
            self.assertEqual(body["text_candidate_limit"], 25)
            self.assertEqual(body["vector_candidate_limit"], 30)
            self.assertEqual(body["return_embedding"], False)

    def test_hybrid_search_requires_query_or_embedding_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaises(InvalidRequestError) as caught:
            client.search_hybrid("docs", top_k=1)

        self.assertEqual(caught.exception.code, "invalid_request")
        self.assertIn("query or query_embedding", caught.exception.message)

    def test_keyword_and_hybrid_service_errors_propagate(self) -> None:
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

    def test_delete_documents_rejects_bare_string_ids_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "not a single string"):
            client.delete_documents("docs", "doc-1")  # type: ignore[arg-type]

    def test_expected_generation_zero_is_rejected_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "positive integer"):
            client.count_documents("docs", expected_generation=0)

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
