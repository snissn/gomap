from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

import pytest
from haystack import Document as HaystackDocument
from haystack import Pipeline
from haystack.document_stores.types import DuplicatePolicy
from treedb_client import TreeDBClient, TreeDBClientError

import _support
from haystack_integrations.components.retrievers.treedb import TreeDBEmbeddingRetriever
from haystack_integrations.document_stores.treedb import TreeDBDocumentStore


def _free_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


class TreeDBServiceProcess:
    def __init__(self, repo_root: Path, data_dir: str) -> None:
        self.repo_root = repo_root
        self.data_dir = data_dir
        self.addr = ""
        self.log = tempfile.NamedTemporaryFile("w+", prefix="treedb_haystack_service_", suffix=".log", delete=False)
        self.proc: Optional[subprocess.Popen[str]] = None

    @property
    def base_url(self) -> str:
        return f"http://{self.addr}"

    def start(self) -> None:
        last_error: Optional[AssertionError] = None
        for attempt in range(5):
            self.addr = _free_addr()
            log_before = self.read_log()
            try:
                self._start_once()
                return
            except AssertionError as exc:
                last_error = exc
                attempt_log = self.read_log()[len(log_before) :].lower()
                if "address already in use" not in attempt_log or attempt == 4:
                    raise
                self._terminate_process()
        raise AssertionError(f"service did not start after retrying address allocation: {last_error}")

    def _start_once(self) -> None:
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
                if client.health().get("ok"):
                    return
            except (TreeDBClientError, OSError) as exc:
                last_error = exc
            time.sleep(0.25)
        raise AssertionError(f"service did not become healthy: {last_error}; log={self.read_log()}")

    def stop(self) -> None:
        self._terminate_process()
        log_name = self.log.name
        self.log.close()
        try:
            os.unlink(log_name)
        except OSError:
            pass

    def _terminate_process(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
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

    def read_log(self) -> str:
        self.log.flush()
        with open(self.log.name, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read()


pytestmark = pytest.mark.skipif(
    os.environ.get("TREEDB_HAYSTACK_RUN_INTEGRATION") != "1" or not shutil.which("go"),
    reason="set TREEDB_HAYSTACK_RUN_INTEGRATION=1 and install Go to run TreeDB service integration tests",
)


@pytest.mark.integration
def test_haystack_store_retriever_pipeline_and_reopen_smoke() -> None:
    with tempfile.TemporaryDirectory(prefix="treedb_haystack_integration_") as data_dir:
        service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir)
        try:
            service.start()
            store = TreeDBDocumentStore(
                base_url=service.base_url,
                index="docs",
                embedding_dimension=3,
                similarity="cosine",
                return_embedding=False,
                timeout=5,
            )
            docs = [
                HaystackDocument(
                    id="a",
                    content="alpha TreeDB document",
                    embedding=[1.0, 0.0, 0.0],
                    meta={"repo": "snissn/gomap", "language": "go"},
                ),
                HaystackDocument(
                    id="b",
                    content="beta Haystack document",
                    embedding=[0.0, 1.0, 0.0],
                    meta={"repo": "snissn/gomap", "language": "python"},
                ),
            ]
            start = time.perf_counter()
            written = store.write_documents(docs, policy=DuplicatePolicy.OVERWRITE)
            ingest_seconds = time.perf_counter() - start
            assert written == 2
            assert store.count_documents({"field": "meta.repo", "operator": "==", "value": "snissn/gomap"}) == 2
            assert [doc.id for doc in store.filter_documents({"field": "meta.language", "operator": "==", "value": "go"})] == ["a"]

            pipeline = Pipeline()
            pipeline.add_component("retriever", TreeDBEmbeddingRetriever(document_store=store, top_k=1, return_embedding=True))
            start = time.perf_counter()
            result = pipeline.run({"retriever": {"query_embedding": [1.0, 0.0, 0.0]}})
            query_seconds = time.perf_counter() - start
            assert result["retriever"]["documents"][0].id == "a"
            assert result["retriever"]["documents"][0].embedding == [1.0, 0.0, 0.0]
            # Smoke metrics are printed for PR evidence; this is not an ANN throughput benchmark.
            print(f"treedb_haystack_smoke docs=2 ingest_seconds={ingest_seconds:.6f} query_seconds={query_seconds:.6f}")
        finally:
            service.stop()

        reopened = TreeDBServiceProcess(_support.REPO_ROOT, data_dir)
        try:
            reopened.start()
            store = TreeDBDocumentStore(
                base_url=reopened.base_url,
                index="docs",
                embedding_dimension=3,
                similarity="cosine",
                return_embedding=True,
                timeout=5,
            )
            assert store.count_documents({"field": "meta.repo", "operator": "==", "value": "snissn/gomap"}) == 2
            assert [doc.id for doc in store.filter_documents({"field": "meta.language", "operator": "==", "value": "go"})] == ["a"]
            result = TreeDBEmbeddingRetriever(document_store=store, top_k=1).run(query_embedding=[1.0, 0.0, 0.0])
            assert result["documents"][0].id == "a"
            assert result["documents"][0].embedding == [1.0, 0.0, 0.0]
        finally:
            reopened.stop()
