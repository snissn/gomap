from __future__ import annotations

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple

import _support  # noqa: F401
from treedb_client import (
    Document,
    IndexNotFoundError,
    InvalidRequestError,
    TreeDBClient,
    TreeDBConfigError,
    TreeDBTimeoutError,
    UnsupportedError,
)


SAMPLE_INDEX = {
    "name": "docs",
    "dimension": 2,
    "metric": "cosine",
    "generation": 1,
    "contract_version": "treedb-document-service/v1alpha1",
    "embedding_field": "embedding",
    "document_type": "treedb_document_service_v1",
    "capabilities": {
        "dense_vector_search": True,
        "exact_dense_scoring": True,
        "metadata_filters": True,
        "keyword_search": False,
        "hybrid_search": False,
    },
}


class FixtureHandler(BaseHTTPRequestHandler):
    server_version = "TreeDBClientFixture/1.0"

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
    def __init__(self, routes: Dict[Tuple[str, str], Tuple[int, Any, float]], prefix: str = "") -> None:
        self.routes = routes
        self.prefix = prefix
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        self.httpd.routes = routes  # type: ignore[attr-defined]
        self.httpd.records = []  # type: ignore[attr-defined]
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


def json_body(record: dict[str, Any]) -> Any:
    return json.loads(record["body"].decode("utf-8"))


class TreeDBClientTests(unittest.TestCase):
    def test_create_index_posts_contract_payload(self) -> None:
        with FixtureServer({("POST", "/v1/indexes"): (200, {"index": SAMPLE_INDEX}, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            info = client.create_index("docs", 2)

            self.assertEqual(info.name, "docs")
            self.assertEqual(json_body(server.records[0]), {"name": "docs", "dimension": 2, "metric": "cosine"})
            self.assertEqual(server.records[0]["headers"]["Content-Type"], "application/json")

    def test_base_url_path_prefix_is_preserved(self) -> None:
        with FixtureServer({("GET", "/api/v1/health"): (200, {"ok": True}, 0)}, prefix="/api") as server:
            client = TreeDBClient(server.base_url + "/", timeout=1)

            self.assertEqual(client.health()["ok"], True)
            self.assertEqual(server.records[0]["path"], "/api/v1/health")

    def test_upsert_documents_omits_response_score_from_write_payload(self) -> None:
        route = "/v1/indexes/docs/documents/upsert"
        response = {"index": SAMPLE_INDEX, "upserted": 1, "inserted": 1, "updated": 0, "ids": ["a"]}
        with FixtureServer({("POST", route): (200, response, 0)}) as server:
            client = TreeDBClient(server.base_url, timeout=1)

            result = client.upsert_documents(
                "docs",
                [Document(id="a", content="alpha", embedding=[1, 0], meta={"repo": "gomap"}, score=0.5)],
                expected_generation=1,
            )

            self.assertEqual(result.upserted, 1)
            body = json_body(server.records[0])
            self.assertEqual(body["expected_generation"], 1)
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

    def test_delete_documents_rejects_bare_string_ids_before_http(self) -> None:
        client = TreeDBClient("http://127.0.0.1:9", timeout=1)

        with self.assertRaisesRegex(InvalidRequestError, "not a single string"):
            client.delete_documents("docs", "doc-1")  # type: ignore[arg-type]

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
        for base_url in ("", "localhost:7120", "ftp://127.0.0.1:7120", "http://127.0.0.1:7120?x=1"):
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


if __name__ == "__main__":
    unittest.main()
