from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tracemalloc
import unittest
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from typing import Optional

import _support
from treedb_client import Document, TreeDBClient, TreeDBClientError
from treedb_client.errors import TreeDBProtocolError
from treedb_client._dense_work import (
    dense_quantized_response_work_matches,
    dense_score_plane_byte_counters_match,
)


def _free_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


def _dense_work_without_filter_materialization(work):
    """Compare route semantics while ignoring cache-cold physical filter work."""

    filter_work = replace(
        work.graph.filter,
        source_ids=0,
        source_bytes=0,
        inspected_entries=0,
        mapping_work_charged=0,
        retained_bytes=0,
        scratch_id_bytes=0,
        scratch_rows=0,
        ordinal_growth_peak_bytes=0,
    )
    return replace(work, graph=replace(work.graph, filter=filter_work))


def _process_group_has_live_members(pgid: int) -> bool:
    """True while the process group holds a member that can still write.

    Zombies ('Z' state) have already exited and released their files, so they
    must not count — signal 0 alone would succeed for a zombie-only group and
    burn the whole wait budget under a PID 1 that does not reap.
    """
    if os.path.isdir("/proc"):
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat", "rb") as handle:
                    stat = handle.read()
            except OSError:
                continue
            rparen = stat.rfind(b")")
            if rparen < 0:
                continue
            fields = stat[rparen + 1 :].split()
            # fields[0]=state, fields[1]=ppid, fields[2]=pgrp
            if len(fields) > 2 and int(fields[2]) == pgid and fields[0] != b"Z":
                return True
        return False
    try:
        os.killpg(pgid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def _typed_serving_limits():
    return {
        "Publication": {"Rows": 512, "Tombstones": 512, "ValueSlots": 4096, "OwnedBytes": 16 << 20, "EncodedOutputBytes": 16 << 20},
        "Owners": {"Owners": 8, "States": 8, "StateBytes": 128 << 20, "AssetBytes": 128 << 20,
                   "Cold": {"ManifestRecords": 4096, "ManifestBytes": 8 << 20, "AssetBytes": 64 << 20, "DecodedTermBytes": 64 << 20},
                   "Physical": {"segments": 4096, "descriptors": 4096, "mapped_bytes": 1 << 30,
                                "fallback_bytes": 1 << 30, "inventory_bytes": 64 << 20}},
        "CandidateOutput": {"Bytes": 1 << 30, "AppenderAttempts": 4096},
        "Maintenance": {"NativeEntries": 4096, "ColumnSegments": 4096, "ManifestRecords": 4096, "LifecycleEntries": 4096,
                        "NativeBytes": 128 << 20, "ColumnBytes": 64 << 20, "ManifestBytes": 8 << 20, "RetainedBytes": 256 << 20, "PagerPages": 32768},
        "Filter": {"SourceIDs": 4096, "SourceBytes": 4 << 20, "RetainedBytes": 4 << 20, "MappingWork": 100000, "InspectedEntries": 4096},
        "FoldRows": 4096, "SearchCandidates": 4096,
    }


class TreeDBServiceProcess:
    def __init__(self, repo_root: Path, data_dir: str, *, native: bool = False) -> None:
        self.repo_root = repo_root
        self.data_dir = data_dir
        self.addr = _free_addr()
        self.native_addr = _free_addr() if native else None
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
        if self.native_addr is not None:
            cmd.extend(["-native-addr", self.native_addr])
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
        # `go run` exits before its compiled child finishes shutdown writes
        # into data_dir; draining the live members of the process group
        # prevents ENOTEMPTY when the test's TemporaryDirectory cleans up
        # right after stop().
        if hasattr(os, "killpg"):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if not _process_group_has_live_members(self.proc.pid):
                    break
                time.sleep(0.1)
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
    def test_literal_and_bounded_filtered_lexical_search(self) -> None:
        with tempfile.TemporaryDirectory(prefix="treedb_lexical_client_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir)
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=10)) as client:
                    client.ensure_index(
                        "docs",
                        2,
                        scalar_fields=[{"field": "meta.tenant", "value_type": "string"}],
                    )
                    client.upsert_documents(
                        "docs",
                        [
                            Document(id="a", content="alpha and refund policy", embedding=[1, 0], meta={"tenant": "t1"}),
                            Document(id="b", content="refund policy", embedding=[0, 1], meta={"tenant": "t2"}),
                        ],
                    )
                    wanted = {"field": "meta.tenant", "operator": "==", "value": "t1"}
                    keyword = client.search_keyword(
                        "docs",
                        '("alpha") and refund',
                        5,
                        text_query_mode="literal",
                        operator="and",
                        max_postings_scanned=64,
                        filter=wanted,
                    )
                    hybrid = client.search_hybrid(
                        "docs",
                        query='("alpha") and refund',
                        top_k=5,
                        text_query_mode="literal",
                        text_operator="and",
                        max_postings_scanned=64,
                        filter=wanted,
                    )
                    self.assertEqual([doc.id for doc in keyword.documents], ["a"])
                    self.assertEqual([doc.id for doc in hybrid.documents], ["a"])
            finally:
                service.stop()

    def test_native_public_listener_and_client_capability(self) -> None:
        """The native listener must belong to the same running public service."""
        with tempfile.TemporaryDirectory(prefix="treedb_native_client_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=10)) as control:
                    control.ensure_index("native_docs", 2)
                    control.upsert_documents("native_docs", [Document(id="a", content="owned", embedding=[1, 0])])
                with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as client:
                    self.assertTrue(client.health()["ok"])
                    documents = client.get_many("native_docs", ["a", "missing", "a"])
                    self.assertEqual([doc.id if doc else None for doc in documents], ["a", None, "a"])
                    self.assertEqual(documents[0].content, "owned")
                    client.get_many("native_docs", ["missing"])
                    self.assertEqual(documents[0].content, "owned")
            finally:
                service.stop()

    @unittest.skipUnless(sys.platform.startswith("linux"), "selected serving fixture requires Linux namespace authority and mmap")
    def test_column_graph_declared_scalar_lifecycle(self) -> None:
        """Exercise the real public client; no mocked transport or exact fallback."""
        with tempfile.TemporaryDirectory(prefix="treedb_typed_client_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            declarations = [{"field": "meta.user_id", "value_type": "string"},
                            {"field": "meta.fpath", "value_type": "string"}]
            wanted = {"field": "meta.user_id", "operator": "==", "value": "owner"}
            limits = _typed_serving_limits()
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=10)) as client:
                    info = client.ensure_index("typed", 2, scalar_fields=declarations, typed_input=True,
                                               vector_index_options={"strategy": "column_graph"})
                    self.assertEqual(info.vector_strategy, "column_graph")
                    self.assertTrue(info.extra["typed_input"])
                    rows = [Document(id=str(i), content=f"text {i}", embedding=[1, i / 32],
                                     meta={"user_id": "owner", "fpath": f"path/{i}"})
                            for i in range(32)]
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as writer:
                        self.assertEqual(writer.upsert_documents("typed", rows, index_info=info).upserted, 32)
                        # Typed batch retrieval does not require graph admission.
                        before_build = writer.get_many("typed", ["0", "missing", "0"], index_info=info)
                        self.assertEqual([doc.id if doc else None for doc in before_build], ["0", None, "0"])
                        self.assertEqual(before_build[0].content, "text 0")
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as stale:
                        with self.assertRaises(TreeDBClientError):
                            stale.upsert_documents("typed", rows[:1], index_info=replace(info, generation=info.generation + 1))
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as stale:
                        with self.assertRaises(TreeDBClientError):
                            stale.get_many("typed", ["0"], index_info=replace(info, generation=info.generation + 1))
                    client.optimize_index("typed", column_graph_serving=limits)
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as native:
                        native_response = native.query_by_embedding("typed", [1, 0], 4, wanted, return_embedding=True, index_info=info)
                        self.assertEqual(len(native_response.documents), 4)
                        self.assertEqual(native_response.native_command_version, 2)
                        self.assertFalse(native_response.native_base_plus_live_delta)
                        self.assertEqual(native_response.documents[0].id, "0")
                        self.assertEqual(native_response.documents[0].content, "text 0")
                        proof = native_response.dense_work
                        self.assertTrue(proof.completed)
                        self.assertEqual(proof.graph.route, "typed_exact")
                        self.assertEqual(proof.graph.exact_base_scored, 32)
                        self.assertEqual(proof.graph.filter.eligible_rows, 32)
                        self.assertEqual(proof.graph.snapshot.schema_generation, info.generation)
                        self.assertEqual(proof.output.fetched, 4)
                        self.assertGreater(proof.output.output_bytes, 0)
                        retained_proof = asdict(proof)
                        empty = native.query_by_embedding("typed", [1, 0], 4, {
                            "field": "meta.user_id", "operator": "==", "value": "absent"}, index_info=info)
                        self.assertEqual(empty.documents, [])
                        self.assertEqual(empty.dense_work.graph.route, "typed_empty")
                        self.assertEqual(empty.dense_work.output.fetched, 0)
                        self.assertEqual(empty.dense_work.graph.base_ann_scored, 0)
                        self.assertEqual(empty.dense_work.graph.exact_base_scored, 0)
                        self.assertTrue(empty.dense_work.completed)
                        self.assertEqual(asdict(proof), retained_proof)
                        retrieved = native.get_many("typed", ["0", "missing", "0"], index_info=info)
                        self.assertEqual([d.id if d else None for d in retrieved], ["0", None, "0"])
                        self.assertEqual(retrieved[0].content, "text 0")
                        if os.environ.get("TREEDB_CLIENT_NATIVE_PROFILE") == "1":
                            tracemalloc.start()
                            start = time.perf_counter_ns()
                            for _ in range(32):
                                measured = native.query_by_embedding("typed", [1, 0], 4, wanted, return_embedding=True, index_info=info)
                                self.assertEqual(len(measured.documents), 4)
                            elapsed = time.perf_counter_ns() - start
                            current, peak = tracemalloc.get_traced_memory()
                            tracemalloc.stop()
                            print(f"native_public_warm32 ns_per_query={elapsed // 32} python_current_B={current} python_peak_B={peak}")
                    response = client.query_by_embedding("typed", [1, 0], 4, wanted,
                                                         route="ann", return_embedding=True)
                    self.assertEqual(response.route, "ann")
                    self.assertEqual(len(response.documents), 4)
                    self.assertEqual(response.documents[0].id, "0")
                    self.assertEqual(response.documents[0].content, "text 0")
                    self.assertEqual(response.documents[0].embedding, [1.0, 0.0])
                    self.assertEqual(
                        _dense_work_without_filter_materialization(response.dense_work),
                        _dense_work_without_filter_materialization(proof),
                    )
                    changed = Document(id="0", content="replacement", embedding=[0, 1],
                                       meta={"user_id": "changed", "fpath": "new/path"})
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as writer:
                        extra = Document(id="extra", content="inserted", embedding=[1, 0], meta={"user_id": "owner", "fpath": "extra/path"})
                        mixed = writer.upsert_documents("typed", [rows[3], changed, extra], index_info=info)
                        self.assertEqual((mixed.upserted, mixed.updated, mixed.inserted), (3, 2, 1))
                        self.assertEqual(writer.get_many("typed", ["extra"], index_info=info)[0].content, "inserted")
                    self.assertEqual(client.delete_documents("typed", ["extra"]).deleted, 1)
                    self.assertEqual(client.delete_documents("typed", ["1"]).deleted, 1)
                    self.assertEqual(client.delete_by_filter("typed", {
                        "field": "meta.fpath", "operator": "==", "value": "path/2"}).deleted, 1)
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as writer:
                        self.assertEqual(writer.upsert_documents("typed", [rows[1]], index_info=info).inserted, 1)
                    self.assertEqual(client.count_documents("typed").count, 31)
                    client.optimize_index("typed", column_graph_action="fold")
            finally:
                service.stop()
            reopened = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                reopened.start()
                with closing(TreeDBClient(reopened.base_url, timeout=10)) as client:
                    with self.assertRaises(TreeDBClientError):
                        client.query_by_embedding("typed", [0, 1], 1, route="ann")
                    with closing(TreeDBClient(reopened.base_url, timeout=10, native_address=reopened.native_addr)) as native:
                        self.assertEqual(native.get_many("typed", ["0"], index_info=info)[0].content, "replacement")
                    info = client.ensure_index("typed", 2, scalar_fields=declarations, typed_input=True, column_graph_serving=limits,
                                        vector_index_options={"strategy": "column_graph"})
                    response = client.query_by_embedding("typed", [0, 1], 1, {
                        "field": "meta.user_id", "operator": "==", "value": "changed"},
                        route="ann", return_embedding=True)
                    self.assertEqual([doc.id for doc in response.documents], ["0"])
                    self.assertEqual(response.documents[0].content, "replacement")
                    self.assertEqual(response.documents[0].meta["fpath"], "new/path")
                    self.assertEqual(response.documents[0].embedding, [0.0, 1.0])
                    with closing(TreeDBClient(reopened.base_url, timeout=10, native_address=reopened.native_addr)) as native:
                        latest = native.query_by_embedding("typed", [0, 1], 1, {
                            "field": "meta.user_id", "operator": "==", "value": "changed"},
                            index_info=info, return_embedding=True)
                        self.assertEqual([doc.id for doc in latest.documents], ["0"])
                        self.assertEqual(latest.documents[0].content, "replacement")
                        self.assertEqual(latest.documents[0].meta["fpath"], "new/path")
                        self.assertTrue(latest.dense_work.completed)
                        self.assertEqual(
                            _dense_work_without_filter_materialization(latest.dense_work),
                            _dense_work_without_filter_materialization(response.dense_work),
                        )
                        self.assertNotEqual(latest.dense_work.graph.snapshot.current_manifest,
                                            proof.graph.snapshot.current_manifest)
                        self.assertEqual(asdict(proof), retained_proof)
                        self.assertEqual(native.get_many("typed", ["2"], index_info=info), [None])
            finally:
                reopened.stop()

    @unittest.skipUnless(sys.platform.startswith("linux"), "selected serving fixture requires Linux namespace authority and mmap")
    def test_atomic_typed_source_replacement_lifecycle(self) -> None:
        """Prove native 5→2→0 replacement and public retrieval across reopen."""
        with tempfile.TemporaryDirectory(prefix="treedb_source_replace_") as data_dir:
            limits = _typed_serving_limits()
            declarations = [{"field": "meta.user_id", "value_type": "string"}]
            delete_ids = [f"source#{i}" for i in range(5)] + ["missing"]
            original = [Document(id=f"source#{i}", content="obsolete", embedding=[1, i / 8],
                                 meta={"user_id": "owner"}) for i in range(5)]
            unrelated = Document(id="other#0", content="unrelated", embedding=[0, 1], meta={"user_id": "other"})
            live = [Document(id=f"source#{i}", content="fresh", embedding=[1 - i, i],
                             meta={"user_id": "owner"}) for i in range(2)]

            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=10)) as control:
                    info = control.ensure_index(
                        "sources", 2, typed_input=True, scalar_fields=declarations,
                        vector_index_options={"strategy": "column_graph"},
                    )
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as native:
                        native.upsert_documents("sources", original + [unrelated], index_info=info)
                    control.optimize_index("sources", column_graph_serving=limits)
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as native:
                        result = native.replace_source_by_id(
                            "sources", delete_ids, live,
                            expected_generation=info.generation, index_info=info,
                        )
                        self.assertEqual((result.deleted_count, result.inserted_count), (5, 2))
                        got = native.get_many("sources", delete_ids + ["other#0"], index_info=info)
                        self.assertEqual([doc.id if doc else None for doc in got],
                                         ["source#0", "source#1", None, None, None, None, "other#0"])
                    control.optimize_index("sources", column_graph_action="ensure", column_graph_serving=limits)
                    self.assertEqual(control.count_documents("sources").count, 3)
                    self.assertEqual(control.search_keyword("sources", "obsolete", 8).documents, [])
                    self.assertEqual({d.id for d in control.filter_documents(
                        "sources", {"field": "meta.user_id", "operator": "==", "value": "owner"}
                    ).documents}, {"source#0", "source#1"})
                    self.assertEqual({d.id for d in control.search_hybrid(
                        "sources", query="fresh", query_embedding=[1, 0], top_k=8,
                        text_candidate_limit=8, vector_candidate_limit=8, ef_search=8,
                    ).documents}, {"source#0", "source#1", "other#0"})
            finally:
                service.stop()

            reopened = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                reopened.start()
                with closing(TreeDBClient(reopened.base_url, timeout=10)) as control:
                    info = control.open_index("sources")
                    control.optimize_index("sources", column_graph_action="ensure", column_graph_serving=limits)
                    self.assertEqual(control.search_keyword("sources", "obsolete", 8).documents, [])
                    with closing(TreeDBClient(reopened.base_url, timeout=10, native_address=reopened.native_addr)) as native:
                        result = native.replace_source_by_id(
                            "sources", ["source#0", "source#1"], [],
                            expected_generation=info.generation, index_info=info,
                        )
                        self.assertEqual((result.deleted_count, result.inserted_count), (2, 0))
                    control.optimize_index("sources", column_graph_action="ensure", column_graph_serving=limits)
                    self.assertEqual(control.search_keyword("sources", "fresh", 8).documents, [])
                    self.assertFalse(any(d.id.startswith("source#") for d in control.query_by_embedding(
                        "sources", [1, 0], 8, route="ann"
                    ).documents))
                    self.assertFalse(any(d.id.startswith("source#") for d in control.search_hybrid(
                        "sources", query="fresh", query_embedding=[1, 0], top_k=8,
                        text_candidate_limit=8, vector_candidate_limit=8, ef_search=8,
                    ).documents))
            finally:
                reopened.stop()

            final = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                final.start()
                with closing(TreeDBClient(final.base_url, timeout=10)) as control:
                    info = control.open_index("sources")
                    control.optimize_index("sources", column_graph_action="ensure", column_graph_serving=limits)
                    self.assertEqual(control.count_documents("sources").count, 1)
                    self.assertEqual(control.search_keyword("sources", "fresh", 8).documents, [])
                    self.assertFalse(any(d.id.startswith("source#") for d in control.query_by_embedding(
                        "sources", [1, 0], 8, route="ann"
                    ).documents))
            finally:
                final.stop()

    @unittest.skipUnless(sys.platform.startswith("linux"), "selected serving fixture requires Linux namespace authority and mmap")
    def test_typed_metadata_update_without_vector_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory(prefix="treedb_metadata_update_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=10)) as control:
                    info = control.ensure_index(
                        "metadata", 2, typed_input=True,
                        scalar_fields=[{"field": "meta.acl", "value_type": "string"}],
                        vector_index_options={"strategy": "column_graph"},
                    )
                    original = Document(
                        id="a", content="unchanged", embedding=[0.25, 0.75],
                        meta={"acl": "old", "residual": {"keep": True}},
                    )
                    with closing(TreeDBClient(service.base_url, timeout=10, native_address=service.native_addr)) as native:
                        native.upsert_documents("metadata", [original], index_info=info)
                        result = native.update_metadata_by_id(
                            "metadata", ["a", "missing"],
                            {"meta.acl": "new", "meta.residual.changed": 1}, [],
                            expected_generation=info.generation, index_info=info,
                        )
                        self.assertEqual((result.matched_count, result.modified_count), (1, 1))
                        document = native.get_many("metadata", ["a"], index_info=info)[0]
                        self.assertEqual(document.content, "unchanged")
                        self.assertEqual(document.embedding, [0.25, 0.75])
                        self.assertEqual(document.meta["acl"], "new")
                        self.assertEqual(document.meta["residual"], {"keep": True, "changed": 1})
                        noop = native.update_metadata_by_id(
                            "metadata", ["a"], {"meta.acl": "new"}, [],
                            expected_generation=info.generation, index_info=info,
                        )
                        self.assertEqual((noop.matched_count, noop.modified_count), (1, 0))
            finally:
                service.stop()

    @unittest.skipUnless(sys.platform.startswith("linux"), "selected serving fixture requires Linux namespace authority and mmap")
    def test_normalized_v4_column_graph_public_clients(self) -> None:
        """Cross the 4,096 cutoff through real native-v4 and HTTP clients."""
        with tempfile.TemporaryDirectory(prefix="treedb_quantized_client_") as data_dir:
            service = TreeDBServiceProcess(_support.REPO_ROOT, data_dir, native=True)
            limits = {
                "Publication": {"Rows": 8192, "Tombstones": 8192, "ValueSlots": 16384,
                                "OwnedBytes": 64 << 20, "EncodedOutputBytes": 64 << 20},
                "Owners": {"Owners": 8, "States": 8, "StateBytes": 256 << 20, "AssetBytes": 256 << 20,
                           "Cold": {"ManifestRecords": 8192, "ManifestBytes": 16 << 20,
                                    "AssetBytes": 128 << 20, "DecodedTermBytes": 128 << 20},
                           "Physical": {"segments": 8192, "descriptors": 8192, "mapped_bytes": 2 << 30,
                                        "fallback_bytes": 2 << 30, "inventory_bytes": 128 << 20}},
                "CandidateOutput": {"Bytes": 1 << 30, "AppenderAttempts": 16384},
                "Maintenance": {"NativeEntries": 16384, "ColumnSegments": 8192, "ManifestRecords": 8192,
                                "LifecycleEntries": 16384, "NativeBytes": 256 << 20, "ColumnBytes": 128 << 20,
                                "ManifestBytes": 16 << 20, "RetainedBytes": 512 << 20, "PagerPages": 65536},
                "Filter": {"SourceIDs": 8192, "SourceBytes": 8 << 20, "RetainedBytes": 8 << 20,
                           "MappingWork": 1_000_000, "InspectedEntries": 8192},
                "FoldRows": 8192, "SearchCandidates": 16384,
            }
            declarations = [{"field": "meta.user_id", "value_type": "string"},
                            {"field": "meta.fpath", "value_type": "string"}]
            try:
                service.start()
                with closing(TreeDBClient(service.base_url, timeout=30)) as control:
                    info = control.ensure_index(
                        "quantized", 2, "cosine", scalar_fields=declarations, typed_input=True,
                        vector_index_options={
                            "strategy": "column_graph", "m": 16, "ef_construction": 32,
                            "representation": "cosine_normalized_f32_v1",
                            "quantized_indexes": [{"name": "minima_sq8", "codec": "scalar_u8", "version": 1}],
                        },
                    )
                    self.assertTrue(info.capabilities.typed_dense_quantized_rerank)
                    self.assertEqual(info.vector_representation, "cosine_normalized_f32_v1")
                    self.assertEqual(
                        [(row.name, row.codec, row.version, row.scalar_u8_calibration)
                         for row in info.quantized_indexes],
                        [("minima_sq8", "scalar_u8", 1, None)],
                    )
                    self.assertEqual(
                        [(row.field, row.index_name, row.value_type) for row in info.scalar_fields],
                        [("meta.fpath", "meta_fpath", "string"),
                         ("meta.user_id", "meta_user_id", "string")],
                    )
                    with closing(TreeDBClient(service.base_url, timeout=30, native_address=service.native_addr)) as native:
                        for start in range(0, 4097, 256):
                            rows = [
                                Document(
                                    id=f"row-{ordinal:06d}", content=f"content-{ordinal}",
                                    embedding=[1.0, ordinal / 8192.0],
                                    meta={"user_id": "owner", "fpath": f"/rows/{ordinal // 256:03d}"},
                                )
                                for ordinal in range(start, min(start + 256, 4097))
                            ]
                            self.assertEqual(native.upsert_documents("quantized", rows, index_info=info).upserted,
                                             len(rows))
                    control.optimize_index("quantized", column_graph_serving=limits)
                    with closing(TreeDBClient(service.base_url, timeout=30, native_address=service.native_addr)) as rejected:
                        # Finite zero vectors reach the server, which remains
                        # the authority for zero-norm refusal. The existing
                        # protocol rejection closes this connection.
                        with self.assertRaisesRegex(TreeDBProtocolError, "native error 6: invalid command"):
                            rejected.query_by_embedding("quantized", [0.0, 0.0], 5,
                                ef_search=64, index_info=info)
                    with closing(TreeDBClient(service.base_url, timeout=30, native_address=service.native_addr)) as native:
                        response = native.query_by_embedding(
                            "quantized", [1.0, 0.0], 5,
                            {"field": "meta.user_id", "operator": "==", "value": "owner"},
                            route="ann", ef_search=64, query_mode="quantized_rerank",
                            quantized_index_name="minima_sq8", quantized_rerank_candidates=64,
                            index_info=info,
                        )
                        diagnostic = native.query_by_embedding(
                            "quantized", [1.0, 0.0], 5,
                            {"field": "meta.user_id", "operator": "==", "value": "owner"},
                            route="ann", ef_search=64, query_mode="quantized_rerank",
                            quantized_index_name="minima_sq8", quantized_rerank_candidates=64,
                            diagnostics=True, index_info=info,
                        )
                        unfiltered = native.query_by_embedding(
                            "quantized", [1.0, 0.0], 5, route="ann", ef_search=64,
                            query_mode="quantized_rerank", quantized_index_name="minima_sq8",
                            quantized_rerank_candidates=64, index_info=info,
                        )
                        exact = native.query_by_embedding(
                            "quantized", [1.0, 0.0], 5, route="ann", ef_search=64,
                            query_mode="exact", index_info=info,
                        )
                        with_embedding = native.query_by_embedding(
                            "quantized", [1.0, 0.0], 1, route="ann", ef_search=64,
                            query_mode="exact", return_embedding=True, index_info=info,
                        )
                    http_quantized = control.query_by_embedding(
                        "quantized", [1.0, 0.0], 5,
                        {"field": "meta.user_id", "operator": "==", "value": "owner"},
                        route="ann", ef_search=64, query_mode="quantized_rerank",
                        quantized_index_name="minima_sq8", quantized_rerank_candidates=64,
                        index_info=info,
                    )
                    http_exact = control.query_by_embedding(
                        "quantized", [1.0, 0.0], 5, route="ann", ef_search=64,
                        query_mode="exact", index_info=info,
                    )
                    hybrid_quantized = control.search_hybrid(
                        "quantized", query="content-0", query_embedding=[1.0, 0.0], top_k=5,
                        text_query_mode="literal", text_candidate_limit=64,
                        vector_candidate_limit=64, ef_search=64,
                        vector_query_mode="quantized_rerank", quantized_index_name="minima_sq8",
                        quantized_rerank_candidates=64,
                    )
                    self.assertEqual(response.native_command_version, 4)
                    self.assertEqual(len(response.documents), 5)
                    self.assertIsNone(response.dense_work)
                    self.assertIsNone(response.score_plane)
                    self.assertIsNotNone(response.route_identity)
                    self.assertFalse(response.route_identity.diagnostics)
                    self.assertEqual(response.route_identity.embedding_vector_reads, 0)
                    self.assertEqual(response.route_identity.embedding_vector_bytes, 0)
                    self.assertEqual(response.route_identity.embedding_output_bytes, 0)
                    self.assertGreater(response.route_identity.quantized_score_calls, 0)
                    self.assertEqual(response.route_identity.packed_score_calls, 1)
                    self.assertEqual(
                        [(row.id, row.score) for row in response.documents],
                        [(row.id, row.score) for row in diagnostic.documents],
                    )
                    self.assertEqual(
                        [(row.id, row.score) for row in response.documents],
                        [(row.id, row.score) for row in http_quantized.documents],
                    )
                    self.assertEqual(
                        [(row.id, row.score) for row in exact.documents],
                        [(row.id, row.score) for row in http_exact.documents],
                    )
                    self.assertEqual(hybrid_quantized.plan.vector_query_mode, "quantized_rerank")
                    self.assertEqual(hybrid_quantized.plan.quantized_index_name, "minima_sq8")
                    self.assertEqual(hybrid_quantized.stats.vector_route["route"], "typed_hnsw")
                    self.assertGreater(hybrid_quantized.stats.vector_quantized_score_calls, 0)
                    self.assertEqual(hybrid_quantized.stats.vector_packed_exact_score_calls, 1)
                    self.assertTrue(all(row.embedding is None for row in hybrid_quantized.documents))
                    self.assertTrue(all(row.embedding is None for row in response.documents + exact.documents))
                    self.assertEqual(len(with_embedding.documents[0].embedding), 2)
                    self.assertEqual(with_embedding.route_identity.embedding_vector_reads, 1)
                    self.assertEqual(with_embedding.route_identity.embedding_vector_bytes, 8)
                    self.assertGreater(with_embedding.route_identity.embedding_output_bytes, 0)
                    work, proof = diagnostic.dense_work, diagnostic.score_plane
                    self.assertIsNotNone(work)
                    self.assertIsNotNone(proof)
                    self.assertTrue(diagnostic.route_identity.diagnostics)
                    self.assertEqual((work.graph.route, proof.route), ("typed_hnsw", "quantized_rerank"))
                    self.assertGreater(proof.quantized_score_calls, 0)
                    self.assertGreater(proof.exact_base_rerank_score_calls, 0)
                    self.assertTrue(dense_score_plane_byte_counters_match(proof, 2))
                    self.assertTrue(dense_quantized_response_work_matches(work, proof, 5, 5, True))
                    self.assertLessEqual(proof.raw_retained_candidates, work.graph.base_candidates)
                    self.assertLessEqual(work.graph.base_candidates, proof.quantized_score_calls)
                    self.assertIsNone(unfiltered.dense_work)
                    self.assertIsNone(unfiltered.score_plane)
                    self.assertEqual(unfiltered.route_identity.execution_route, "typed_hnsw")
                    self.assertGreater(unfiltered.route_identity.quantized_score_calls, 0)
                    for document in response.documents:
                        ordinal = int(document.id.removeprefix("row-"))
                        self.assertEqual(document.content, f"content-{ordinal}")
                        self.assertEqual(document.meta["user_id"], "owner")
                        expected = 1.0 / (1.0 + (ordinal / 8192.0) ** 2) ** 0.5
                        self.assertAlmostEqual(document.score, expected, places=6)
            finally:
                service.stop()

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
