from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from typing import Optional

import _support
from treedb_client import Document, TreeDBClient, TreeDBClientError


def _free_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


class TreeDBServiceProcess:
    def __init__(self, repo_root: Path, data_dir: str) -> None:
        self.repo_root = repo_root
        self.data_dir = data_dir
        self.addr = _free_addr()
        self.log = tempfile.NamedTemporaryFile("w+", prefix="treedb_document_service_", suffix=".log", delete=False)
        self.proc: Optional[subprocess.Popen[str]] = None

    @property
    def base_url(self) -> str:
        return f"http://{self.addr}"

    def start(self) -> None:
        cmd = [
            "go",
            "run",
            "./cmd/treedb-document-service",
            "-dir",
            self.data_dir,
            "-addr",
            self.addr,
            "-profile",
            "command_wal_durable",
        ]
        self.proc = subprocess.Popen(
            cmd,
            cwd=str(self.repo_root),
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        client = TreeDBClient(self.base_url, timeout=1)
        deadline = time.monotonic() + 90
        last_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"service exited early with code {self.proc.returncode}: {self.read_log()}")
            try:
                health = client.health()
                if health.get("ok"):
                    return
            except (TreeDBClientError, OSError) as exc:
                last_error = exc
            time.sleep(0.25)
        raise AssertionError(f"service did not become healthy: {last_error}; log={self.read_log()}")

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            try:
                if hasattr(os, "killpg"):
                    os.killpg(self.proc.pid, signal.SIGTERM)
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                if hasattr(os, "killpg") and hasattr(signal, "SIGKILL"):
                    os.killpg(self.proc.pid, signal.SIGKILL)
                else:
                    self.proc.kill()
                self.proc.wait(timeout=5)
        log_name = self.log.name
        self.log.close()
        try:
            os.unlink(log_name)
        except OSError:
            pass

    def read_log(self) -> str:
        self.log.flush()
        with open(self.log.name, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read()


@unittest.skipUnless(
    os.environ.get("TREEDB_CLIENT_RUN_INTEGRATION") == "1" and shutil.which("go"),
    "set TREEDB_CLIENT_RUN_INTEGRATION=1 and install Go to run TreeDB service integration tests",
)
class TreeDBClientIntegrationTests(unittest.TestCase):
    def test_service_round_trip_and_reopen_smoke(self) -> None:
        with tempfile.TemporaryDirectory(prefix="treedb_client_integration_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir)
            try:
                service.start()
                client = TreeDBClient(service.base_url, timeout=5)
                client.ensure_index("docs", dimension=2)
                client.upsert_documents(
                    "docs",
                    [
                        Document(id="a", content="alpha", embedding=[1, 0], meta={"repo": "gomap", "language": "go"}),
                        Document(id="b", content="beta", embedding=[0, 1], meta={"repo": "other", "language": "python"}),
                    ],
                )
                self.assertEqual(client.count_documents("docs", {"field": "meta.repo", "operator": "==", "value": "gomap"}).count, 1)
                listed = client.filter_documents("docs", {"field": "meta.language", "operator": "in", "value": ["go"]})
                self.assertEqual([doc.id for doc in listed.documents], ["a"])
                self.assertIsNone(listed.documents[0].embedding)
                searched = client.query_by_embedding("docs", [1, 0], 1, return_embedding=True)
                self.assertEqual(searched.documents[0].id, "a")
                self.assertEqual(searched.documents[0].embedding, [1.0, 0.0])
                self.assertEqual(client.delete_by_filter("docs", {"field": "meta.repo", "operator": "==", "value": "other"}).deleted, 1)
                self.assertEqual(client.count_documents("docs").count, 1)
            finally:
                service.stop()

            reopened = TreeDBServiceProcess(_support.REPO_ROOT, data_dir)
            try:
                reopened.start()
                client = TreeDBClient(reopened.base_url, timeout=5)
                info = client.open_index("docs")
                self.assertEqual(info.dimension, 2)
                self.assertEqual(client.count_documents("docs").count, 1)
                searched = client.query_by_embedding("docs", [1, 0], 1, return_embedding=True)
                self.assertEqual(searched.documents[0].id, "a")
                self.assertEqual(searched.documents[0].embedding, [1.0, 0.0])
            finally:
                reopened.stop()


if __name__ == "__main__":
    unittest.main()
