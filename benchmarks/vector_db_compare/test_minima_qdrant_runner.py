#!/usr/bin/env python3

from __future__ import annotations

from collections.abc import Callable
import copy
import os
import subprocess
import threading
import socket
import time
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import minima_qdrant_runner as runner


class Model:
    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)

    def model_dump(self, **_: object) -> dict[str, object]:
        def dump(value: object) -> object:
            if isinstance(value, Model):
                return {key: dump(item) for key, item in vars(value).items()}
            if isinstance(value, dict):
                return {key: dump(item) for key, item in value.items()}
            if isinstance(value, list):
                return [dump(item) for item in value]
            return value
        return {key: dump(value) for key, value in vars(self).items()}


class Models:
    FieldCondition = MatchValue = Filter = VectorParams = PointStruct = PointIdsList = Model
    HnswConfigDiff = OptimizersConfigDiff = Model
    Distance = SimpleNamespace(COSINE="cosine")
    PayloadSchemaType = SimpleNamespace(KEYWORD="keyword")


class SharedQdrant:
    def __init__(self) -> None:
        self.exists = False
        self.points: dict[str, Model] = {}
        self.payload_schema: dict[str, str] = {}
        self.vector_params: dict[str, Model] = {}
        self.hnsw_config = Model()
        self.optimizers_config = Model()
        self.events: list[str] = []
        self.clients: list[FakeClient] = []
        self.index_fields: list[str] = []
        self.query_filters: list[Model] = []
        self.lock = threading.Lock()
        self.writer_completed = threading.Event()

        self.restart_count = 0

    def restart(self) -> int:
        self.restart_count += 1
        return 2

    def factory(self) -> "FakeClient":
        client = FakeClient(self)
        self.clients.append(client)
        return client


class FakeClient:
    def __init__(self, shared: SharedQdrant) -> None:
        self.shared = shared
        self.closed = False

    def collection_exists(self, _: str) -> bool:
        return self.shared.exists

    def delete_collection(self, _: str, **__: object) -> None:
        self.shared.exists = False
        self.shared.points.clear()

    def create_collection(self, *, vectors_config: dict[str, Model],
                          hnsw_config: Model, optimizers_config: Model,
                          **_: object) -> None:
        self.shared.exists = True
        self.shared.vector_params = vectors_config
        self.shared.hnsw_config = hnsw_config
        self.shared.optimizers_config = optimizers_config
        self.shared.events.append("create_initial_upload_collection")

    def update_collection(self, *, hnsw_config: Model,
                          optimizers_config: Model, **_: object) -> None:
        self.shared.hnsw_config = hnsw_config
        self.shared.optimizers_config = optimizers_config
        self.shared.events.append("restore_production_configuration")

    def create_payload_index(self, *, field_name: str, field_schema: str, **_: object) -> None:
        self.shared.payload_schema[field_name] = field_schema
        self.shared.index_fields.append(field_name)

    def get_collection(self, _: str) -> Model:
        self.shared.events.append("get_collection")
        return Model(
            status="green",
            optimizer_status=Model(ok=True),
            points_count=len(self.shared.points),
            indexed_vectors_count=len(self.shared.points),
            segments_count=1,
            payload_schema=self.shared.payload_schema,
            config=Model(
                params=Model(vectors=self.shared.vector_params),
                hnsw_config=self.shared.hnsw_config,
                optimizer_config=self.shared.optimizers_config,
                wal_config=Model(wal_capacity_mb=32),
            ),
        )

    def get_optimizations(self, **_: object) -> Model:
        return Model(
            summary=Model(
                queued_optimizations=0, queued_points=0,
                queued_segments=0, idle_segments=0,
            ),
            running=[],
        )

    def upsert(self, *, points: list[Model], **_: object) -> None:
        time.sleep(0.002)
        with self.shared.lock:
            for point in points:
                self.shared.points[point.id] = point
        self.shared.writer_completed.set()
        self.shared.events.append("upsert")

    @staticmethod
    def _matches(point: Model, filter_value: Model) -> bool:
        return all(point.payload[condition.key] == condition.match.value for condition in filter_value.must)

    def query_points(self, *, query: list[float], query_filter: Model, using: str, limit: int, **_: object) -> Model:
        time.sleep(0.002)
        with self.shared.lock:
            self.shared.query_filters.append(query_filter)
            matches = [point for point in self.shared.points.values() if self._matches(point, query_filter)]
        scored = sorted(
            ((runner.document_score({"vector": point.vector[using]}), point) for point in matches),
            key=lambda item: (-item[0], item[1].payload["id"]),
        )[:limit]
        return Model(points=[Model(payload=point.payload, score=score) for score, point in scored])

    def retrieve(self, *, ids: list[str], **_: object) -> list[Model]:
        with self.shared.lock:
            return [self.shared.points[value] for value in ids if value in self.shared.points]

    def delete(self, *, points_selector: Model, **_: object) -> None:
        time.sleep(0.002)
        with self.shared.lock:
            if hasattr(points_selector, "must"):
                doomed = [key for key, point in self.shared.points.items() if self._matches(point, points_selector)]
            else:
                doomed = list(points_selector.points)
            for key in doomed:
                self.shared.points.pop(key, None)
        self.shared.writer_completed.set()
    def count(self, *, count_filter: Model, **_: object) -> Model:
        with self.shared.lock:
            return Model(count=sum(self._matches(point, count_filter) for point in self.shared.points.values()))

    def scroll(self, *, limit: int, offset: str | None, **_: object) -> tuple[list[Model], str | None]:
        keys = sorted(self.shared.points)
        start = 0 if offset is None else keys.index(offset)
        selected = keys[start:start + limit]
        rows = [Model(payload=self.shared.points[key].payload, vector=self.shared.points[key].vector) for key in selected]
        next_offset = keys[start + limit] if start + limit < len(keys) else None
        return rows, next_offset

    def close(self) -> None:
        self.closed = True


def reader_schedule(names: list[str]) -> list[dict[str, object]]:
    return [{"ordinal": ordinal, "actor": "reader", "scenario": name, "query_ordinal": ordinal} for ordinal, name in enumerate(names)]


def tiny_manifest(reader_concurrency: int = 1) -> dict[str, object]:
    definitions = [
        ("small", 8, 0, 4, "user_id", "small-user", "", 0, 0, 0, 0),
        ("all_match", 6, 0, 6, "user_id", "all-user", "", 0, 0, 0, 0),
        ("over_limit_4097", 6, 0, 4, "user_id", "over-user", "", 0, 0, 0, 0),
        ("broad_10pct", 6, 0, 3, "user_id", "broad-user", "", 0, 0, 0, 0),
        ("sparse_over_limit", 6, 0, 2, "user_id", "sparse-user", "", 0, 0, 0, 0),
        ("mixed_broad_narrow", 8, 2, 2, "user_id+fpath", "mixed-user", "/mixed/target.txt", 0, 6, 2, 2),
        ("empty_user", 4, 0, 0, "user_id", "missing-user", "", 0, 0, 0, 0),
        ("empty_file", 8, 0, 0, "user_id+fpath", "empty-file-user", "/missing.txt", 0, 4, 0, 0),
    ]
    corpora = []
    for name, rows, start, eligible, filter_name, user_id, fpath, broad_start, broad_rows, narrow_start, narrow_rows in definitions:
        value = {
            "name": name, "shape": "small", "corpus_rows": rows, "eligible_start": start,
            "eligible_rows": eligible, "filter": filter_name, "user_id": user_id,
            "selectivity": eligible / rows, "generator": runner.GENERATOR,
        }
        if filter_name == "user_id+fpath":
            value["fpath"] = fpath
            for key, field_value in (("broad_start", broad_start), ("broad_rows", broad_rows),
                                     ("narrow_start", narrow_start), ("narrow_rows", narrow_rows)):
                if field_value:
                    value[key] = field_value
        corpora.append(value)
    config = {
        "collection": "minima", "vector_field": "embedding", "content_field": "content", "dimension": 8,
        "metric": "cosine", "scalar_fields": ["user_id", "fpath"], "top_k": 5, "batch_size": 3,
        "reader_concurrency": reader_concurrency, "writer_concurrency": 1, "warmup_queries": len(corpora),
        "timed_queries": len(corpora) * reader_concurrency, "lookup_limit": 4096, "order_tolerance": 0,
        "score_tolerance": 0.000001, "ordering": "manifest_ordinal_serial; timed_search_round_robin",
        "completion_boundary": "successful_mutation_response_before_visibility_probe",
        "timing_boundary": "storage_calls_only; embeddings_and_llm_excluded; fetch_and_decode_separate",
    }
    initial_ranges = [{"scenario": row["name"], "start": 0, "rows": row["corpus_rows"] - 1} for row in corpora]
    concurrent_ranges = [{"scenario": row["name"], "start": row["corpus_rows"] - 1, "rows": 1} for row in corpora]
    mixed = next(row for row in corpora if row["name"] == "mixed_broad_narrow")
    mixed_ids = [f"minima/mixed_broad_narrow/{ordinal:06d}" for ordinal in range(2, 4)]
    replacements = []
    for ordinal in range(2, 4):
        document = runner.generated_document(mixed, ordinal)
        document["id"] = f"minima/mixed_broad_narrow/replacement/{ordinal - 2:06d}"
        document["content"] = f"minima:mixed_broad_narrow:replacement:{ordinal - 2}"
        replacements.append(document)
    small = corpora[0]
    updated = runner.generated_document(small, 0)
    updated["content"] = "minima:small:0:updated"
    deleted = runner.generated_document(small, 1)
    timed_plan = {
        "query_count": config["timed_queries"], "scenario_order": [row["name"] for row in corpora],
        "reader_concurrency": config["reader_concurrency"], "writer_concurrency": config["writer_concurrency"],
        "assignment": f"round=ordinal/{reader_concurrency};reader=ordinal%{reader_concurrency};scenario=scenario_order[ordinal%8]",
        "rounds": [
            {
                "ordinal": ordinal, "query_start": ordinal * reader_concurrency,
                "query_count": reader_concurrency, "insert_range": insertion,
                "start_barrier": "round_start_readers_and_writer",
                "end_barrier": "round_end_queries_and_insert_complete",
            }
            for ordinal, insertion in enumerate(concurrent_ranges)
        ],
    }
    def mutation_plan(mutation: str) -> dict[str, object]:
        return {
            "mutation": mutation, "reader_concurrency": config["reader_concurrency"],
            "reader_assignments": [
                {"reader": reader, "query_ordinal": reader, "scenario": "mixed_broad_narrow"}
                for reader in range(reader_concurrency)
            ],
            "start_barrier": "reindex_start_all_readers_and_writer",
            "end_barrier": "reindex_end_all_readers_and_mutation_complete",
        }
    operations = [
        {"ordinal": 0, "name": runner.OPERATION_NAMES[0], "target": "all", "effect": "none"},
        {"ordinal": 1, "name": runner.OPERATION_NAMES[1], "target": "all", "effect": "insert", "insert_ranges": initial_ranges},
        {"ordinal": 2, "name": runner.OPERATION_NAMES[2], "target": "all", "effect": "none", "schedule": reader_schedule([row["name"] for row in corpora])},
        {"ordinal": 3, "name": runner.OPERATION_NAMES[3], "target": "all", "timed": True, "effect": "insert",
         "insert_ranges": concurrent_ranges, "timed_reader_plan": timed_plan},
        {"ordinal": 4, "name": runner.OPERATION_NAMES[4], "target": "mixed_broad_narrow", "effect": "delete",
         "filter": {"user_id": "mixed-user", "fpath": "/mixed/target.txt"}, "ids": mixed_ids,
         "concurrent_mutation_plan": mutation_plan("delete_by_user_id_and_fpath")},
        {"ordinal": 5, "name": runner.OPERATION_NAMES[5], "target": "mixed_broad_narrow", "effect": "insert",
         "documents": replacements, "concurrent_mutation_plan": mutation_plan("replacement_insert")},
        {"ordinal": 6, "name": runner.OPERATION_NAMES[6], "target": "mixed_broad_narrow", "effect": "none", "schedule": reader_schedule(["mixed_broad_narrow"])},
        {"ordinal": 7, "name": runner.OPERATION_NAMES[7], "target": "small", "effect": "update", "documents": [updated]},
        {"ordinal": 8, "name": runner.OPERATION_NAMES[8], "target": "small", "effect": "none", "schedule": reader_schedule(["small"])},
        {"ordinal": 9, "name": runner.OPERATION_NAMES[9], "target": "small", "effect": "delete", "ids": [deleted["id"]]},
        {"ordinal": 10, "name": runner.OPERATION_NAMES[10], "target": "small", "effect": "none", "schedule": reader_schedule(["small"])},
        {"ordinal": 11, "name": runner.OPERATION_NAMES[11], "target": "empty_user,empty_file", "effect": "none", "schedule": reader_schedule(["empty_user", "empty_file"])},
        {"ordinal": 12, "name": runner.OPERATION_NAMES[12], "target": "all", "effect": "none"},
        {"ordinal": 13, "name": runner.OPERATION_NAMES[13], "target": "all", "effect": "none"},
        {"ordinal": 14, "name": runner.OPERATION_NAMES[14], "target": "all", "effect": "none"},
        {"ordinal": 15, "name": runner.OPERATION_NAMES[15], "target": "all", "effect": "none", "schedule": reader_schedule([row["name"] for row in corpora])},
    ]
    queries = []
    for spec in corpora:
        vector = [1.0] + [0.0] * 7
        stop = min(spec["eligible_start"] + spec["eligible_rows"], spec["eligible_start"] + config["top_k"])
        ordinals = range(spec["eligible_start"], stop)
        queries.append({"scenario": spec["name"], "vector": vector,
            "initial_oracle_ids": [f"minima/{spec['name']}/{ordinal:06d}" for ordinal in ordinals],
            "initial_oracle_scores": [runner.document_score(runner.generated_document(spec, ordinal)) for ordinal in range(spec["eligible_start"], stop)],
            "final_oracle_ids": [], "final_oracle_scores": []})
    manifest = {"schema": runner.MANIFEST_SCHEMA, "config": config, "corpora": corpora, "queries": queries,
                "operations": operations, "corpus_sha256": "", "query_sha256": "", "operation_sha256": "", "expected_state_sha256": ""}
    for spec, query in zip(corpora, queries, strict=True):
        query["final_oracle_ids"], query["final_oracle_scores"] = runner.final_oracle(manifest, spec)
    manifest.update(runner.manifest_hashes(manifest))
    manifest["expected_state_sha256"] = runner.expected_state_hash(manifest)
    return manifest


def new_runner(manifest: dict[str, object], shared: SharedQdrant, allow_drop: bool = False) -> runner.QdrantMinimaRunner:
    return runner.QdrantMinimaRunner(manifest, client_factory=shared.factory, models=Models, url="http://fake",
        collection="tiny", allow_drop=allow_drop, operation_timeout=1, optimizer_timeout=0.1,
        poll_interval=0, server_version="1.19.0", deployment="standalone", image="", storage_path=None,
        server_pid=1, restart_server=shared.restart, restart_identity="fake owned server",
        process_identity=lambda pid: f"fake-process-{pid}",
        process_running=lambda _pid: False, process_owns_endpoint=lambda _pid, _url, _port: True)


class MinimaQdrantRunnerTest(unittest.TestCase):
    def test_manifest_reconstruction_and_hash_rejection(self) -> None:
        manifest = tiny_manifest()
        self.assertIs(runner.validate_manifest(manifest, require_frozen=False), manifest)
        for key in ("corpus_sha256", "query_sha256", "operation_sha256", "expected_state_sha256"):
            changed = copy.deepcopy(manifest)
            changed[key] = "bad"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                runner.validate_manifest(changed, require_frozen=False)
        changed = copy.deepcopy(manifest)
        changed["operations"][7]["documents"][0]["content"] = "doctored"
        with self.assertRaisesRegex(ValueError, "operation_sha256 mismatch"):
            runner.validate_manifest(changed, require_frozen=False)

    def test_go_omitempty_scenario_fields_are_supported(self) -> None:
        manifest = tiny_manifest()
        empty_file = next(row for row in manifest["corpora"] if row["name"] == "empty_file")
        self.assertNotIn("narrow_start", empty_file)
        self.assertNotIn("narrow_rows", empty_file)
        document = runner.generated_document(empty_file, 0)
        self.assertEqual(document["user_id"], "empty-file-user")
        self.assertNotEqual(document["fpath"], empty_file["fpath"])
        small = next(row for row in manifest["corpora"] if row["name"] == "small")
        defaulted = runner.generated_document(small, 7)
        self.assertEqual(defaulted["user_id"], "small-other-user-07")
        self.assertEqual(defaulted["fpath"], "/small/other/07.txt")

    def test_go_python_oracle_score_rounding_tolerance(self) -> None:
        manifest = tiny_manifest()
        manifest["queries"][0]["initial_oracle_scores"][0] += 2e-16
        manifest["queries"][0]["final_oracle_scores"][0] += 2e-16
        manifest.update(runner.manifest_hashes(manifest))
        self.assertIs(runner.validate_manifest(manifest, require_frozen=False), manifest)

        changed = copy.deepcopy(manifest)
        changed["queries"][0]["initial_oracle_scores"][0] += 2e-12
        changed.update(runner.manifest_hashes(changed))
        with self.assertRaisesRegex(ValueError, "exact initial scores mismatch"):
            runner.validate_manifest(changed, require_frozen=False)

    def test_go_json_encoding_contract(self) -> None:
        self.assertEqual(
            runner.go_json({"html": "<>&", "fixed": 1e-5, "scientific": 1e-7, "zero": 0.0}),
            '{"html":"\\u003c\\u003e\\u0026","fixed":0.00001,"scientific":1e-7,"zero":0}',
        )

    def test_operation_ordering_rejected_even_when_rehashed(self) -> None:
        manifest = tiny_manifest()
        manifest["operations"][1]["name"] = "out_of_order"
        manifest.update(runner.manifest_hashes(manifest))
        manifest["expected_state_sha256"] = runner.expected_state_hash(manifest)
        with self.assertRaisesRegex(ValueError, "frozen order"):
            runner.validate_manifest(manifest, require_frozen=False)

    def test_bulk_upload_transition_is_ordered_once_and_preserves_manifest(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        before_hashes = runner.manifest_hashes(manifest)
        before_operations = copy.deepcopy(manifest["operations"])
        workload = new_runner(manifest, shared)
        workload.connect()
        workload.create_owned_collection()
        initial = manifest["operations"][1]
        workload.insert_ranges(initial["name"], initial["insert_ranges"], False)

        self.assertEqual(shared.hnsw_config.model_dump(), runner.PRODUCTION_HNSW_CONFIG)
        self.assertEqual(shared.optimizers_config.model_dump(), runner.PRODUCTION_OPTIMIZERS_CONFIG)
        self.assertEqual(shared.events.count("restore_production_configuration"), 1)
        restoration = shared.events.index("restore_production_configuration")
        self.assertGreater(restoration, max(
            index for index, event in enumerate(shared.events) if event == "upsert"
        ))
        with self.assertRaisesRegex(RuntimeError, "already attempted"):
            workload.restore_production_configuration()

        artifact = workload.artifact()
        raw = artifact["backend_raw_evidence"]["qdrant"]
        transition = raw["collection_configuration_transition"]
        self.assertTrue(transition["attempted"])
        self.assertTrue(transition["completed"])
        self.assertEqual(transition["boundary"], "initial_batch_insert_to_warmup_search")
        self.assertEqual(
            [row["phase"] for row in raw["readiness"]["sessions"]],
            ["initial_upload_collection_created", "initial_load_to_query"],
        )
        self.assertTrue(all(row["resource_samples"] for row in raw["readiness"]["sessions"]))
        self.assertTrue(all(
            snapshot["optimizations"]["available"]
            for session in raw["readiness"]["sessions"]
            for snapshot in session["snapshots"]
        ))
        self.assertFalse(artifact["passing"])
        self.assertEqual(artifact["state"], "partial")
        self.assertEqual(runner.manifest_hashes(manifest), before_hashes)
        self.assertEqual(manifest["operations"], before_operations)
        workload.close()

    def test_readiness_dispositions_are_deterministic(self) -> None:
        def snapshot(*, status: str = "yellow", optimizer: object = "ok",
                     running: list[object] | None = None,
                     summary: dict[str, int] | None = None,
                     available: bool = True, elapsed: float = 99,
                     indexed: int = 1) -> dict[str, object]:
            detail = {
                "running": running or [],
                "summary": summary or {
                    "queued_optimizations": 0, "queued_points": 0,
                    "queued_segments": 0, "idle_segments": 0,
                },
            }
            return {
                "elapsed_seconds": elapsed,
                "status": status, "optimizer_status": optimizer,
                "indexed_vectors_count": indexed, "segments_count": 1,
                "optimizations": (
                    {"available": True, "detail": detail}
                    if available else {"available": False, "reason": "not exposed"}
                ),
            }

        cases = {
            "active progress": ([
                snapshot(running=[{"progress": 0.25}], elapsed=95, indexed=1),
                snapshot(running=[{"progress": 0.50}], elapsed=99, indexed=2),
            ], []),
            "queued/idle": ([snapshot(
                status="grey", summary={"queued_optimizations": 1},
            )], []),
            "optimizer error": ([snapshot(
                status="red", optimizer={"error": "optimizer failed"},
            )], []),
            "resource starvation": ([snapshot()], [{
                "available": True, "tail": "No space left on device",
            }]),
            "unknown": ([snapshot(available=False)], []),
        }
        for expected, (snapshots, logs) in cases.items():
            with self.subTest(expected=expected):
                self.assertEqual(
                    runner.readiness_disposition(snapshots, logs), expected,
                )

    def test_readiness_progress_must_change_near_deadline(self) -> None:
        def snapshot(elapsed: float, indexed: int) -> dict[str, object]:
            return {
                "elapsed_seconds": elapsed,
                "status": "yellow", "optimizer_status": "ok",
                "points_count": 100, "indexed_vectors_count": indexed,
                "segments_count": 2,
                "optimizations": {
                    "available": True,
                    "detail": {
                        "summary": {
                            "queued_optimizations": 1, "queued_points": 100 - indexed,
                            "queued_segments": 1, "idle_segments": 0,
                        },
                        "running": [{"operation_id": 7}],
                    },
                },
            }

        early_progress_then_stall = [
            snapshot(0, 0), snapshot(20, 50),
            snapshot(91, 50), snapshot(99, 50),
        ]
        recent_progress = [
            snapshot(0, 0), snapshot(20, 50),
            snapshot(91, 50), snapshot(99, 75),
        ]
        self.assertEqual(
            runner.readiness_disposition(
                early_progress_then_stall, [], deadline_seconds=100,
            ),
            "queued/idle",
        )
        self.assertEqual(
            runner.readiness_disposition(
                recent_progress, [], deadline_seconds=100,
            ),
            "active progress",
        )

    def test_grey_is_rejected_and_missing_optimization_monitoring_is_explicit(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.connect()
        workload.optimizer_timeout = 0.001
        grey = Model(
            status="grey", optimizer_status=Model(ok=True),
            points_count=0, indexed_vectors_count=0, segments_count=1,
            payload_schema={"user_id": "keyword", "fpath": "keyword"},
            config=Model(params=Model(vectors={})),
        )
        unavailable_resource = {
            "captured": False, "rss_bytes": 0, "cpu_seconds": 0.0, "disk_bytes": 0,
            "availability": {
                "rss_bytes": "unavailable", "cpu_seconds": "unavailable",
                "disk_bytes": "unavailable", "bytes_per_op": "unavailable",
                "allocs_per_op": "unavailable", "measurement_error": "",
            },
        }
        with (
            mock.patch.object(workload.client, "get_collection", return_value=grey),
            mock.patch.object(
                workload.client, "get_optimizations",
                side_effect=NotImplementedError("endpoint unavailable"),
            ),
            mock.patch.object(runner, "server_resource_usage", return_value=unavailable_resource),
        ):
            with self.assertRaisesRegex(TimeoutError, "queued/idle"):
                workload.wait_ready(expected_count=0, phase="grey_timeout")
            artifact = workload.artifact()

        session = artifact["backend_raw_evidence"]["qdrant"]["readiness"]["sessions"][-1]
        self.assertEqual(session["outcome"], "timeout")
        self.assertEqual(session["disposition"], "queued/idle")
        self.assertEqual(session["snapshots"][-1]["status"], "grey")
        self.assertFalse(session["snapshots"][-1]["optimizations"]["available"])
        self.assertIn("NotImplementedError", session["snapshots"][-1]["optimizations"]["reason"])
        self.assertFalse(artifact["passing"])
        self.assertEqual(artifact["state"], "partial")
        workload.close()

    def test_optimizer_diagnostic_uses_per_request_timeout(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.connect()
        request = mock.Mock(return_value=Model(result=Model(summary=Model(queued_optimizations=0))))
        workload.client.http = Model(client=Model(request=request))
        workload.models = Model(InlineResponse20011=object)
        snapshot = workload.optimization_snapshot(0.25)
        self.assertTrue(snapshot["available"])
        self.assertEqual(request.call_args.kwargs["timeout"], 0.25)
        self.assertEqual(request.call_args.kwargs["params"], {"with": "completed", "completed_limit": 16})
        workload.close()

    def test_readiness_deduplicates_unchanged_server_log_tail(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.connect()
        workload.optimizer_timeout = 0.01
        workload.poll_interval = 0
        grey = Model(
            status="grey", optimizer_status=Model(ok=True),
            points_count=0, indexed_vectors_count=0, segments_count=1,
            payload_schema={"user_id": "keyword", "fpath": "keyword"},
            config=Model(params=Model(vectors={})),
        )
        with (
            mock.patch.object(workload.client, "get_collection", return_value=grey),
            mock.patch.object(workload, "server_log_snapshot", return_value={
                "available": True, "path": "/tmp/qdrant.log", "size_bytes": 7, "tail": "same",
            }),
            mock.patch.object(runner, "READINESS_RESOURCE_INTERVAL_SECONDS", 0),
        ):
            with self.assertRaises(TimeoutError):
                workload.wait_ready(expected_count=0, phase="dedupe_timeout")
        self.assertEqual(len(workload.readiness_evidence[-1]["server_log_samples"]), 1)
        workload.close()

    def test_frozen_hashes_and_operation_order_remain_literal(self) -> None:
        self.assertEqual(runner.FROZEN_HASHES, {
            "corpus_sha256": "0b1a213652fc97a4460f254f4d9e90f027e4b30ef6111a26807591ade10923e1",
            "query_sha256": "eb4f076023e361b9a2cf18a06a5e1d69e5023c304da25d38848fc7011575288a",
            "operation_sha256": "08f38acec8a5ad746dbffadef5ad9c198852c88d1920746229cb0733bfd9c434",
            "expected_state_sha256": "c2986f2b44e67b33e7bb3f92f5f92b1316e60117ed2505bef73327e0b1e5687f",
        })
        self.assertEqual(runner.OPERATION_NAMES, [
            "ensure_compatible_collection", "initial_batch_insert", "warmup_search",
            "timed_search_with_batch_insert", "reindex_delete_by_user_and_fpath_while_reading",
            "reindex_replacement_insert_while_reading", "reindex_visibility_probe", "explicit_update",
            "update_visibility_probe", "explicit_delete", "delete_visibility_probe",
            "empty_user_and_file_probes", "close", "reopen", "idempotent_ensure_after_reopen",
            "final_manifest_and_oracle_comparison",
        ])

    def test_payload_filter_shape(self) -> None:
        value = runner.payload_filter(Models, {"user_id": "u", "fpath": "/a"})
        self.assertEqual([(row.key, row.match.value) for row in value.must], [("user_id", "u"), ("fpath", "/a")])
        workload = new_runner(tiny_manifest(), SharedQdrant())
        point = workload.point(runner.generated_document(tiny_manifest()["corpora"][0], 0))
        self.assertEqual(set(point.payload), {"id", "content", "user_id", "fpath"})
        self.assertEqual(set(point.vector), {"embedding"})

    def test_final_state_hash_is_order_independent_and_sensitive(self) -> None:
        docs = list(runner.final_documents(tiny_manifest()))
        first, second = runner.StateAccumulator(), runner.StateAccumulator()
        for document in docs:
            first.add(document)
        for document in reversed(docs):
            second.add(document)
        self.assertEqual(first.hexdigest(), second.hexdigest())
        vector_changed = runner.StateAccumulator()
        for document in docs:
            changed_vector = {**document, "vector": [0.0] * len(document["vector"])}
            vector_changed.add(changed_vector)
        self.assertEqual(first.hexdigest(), vector_changed.hexdigest())
        docs[0]["content"] += ":changed"
        changed = runner.StateAccumulator()
        for document in docs:
            changed.add(document)
        self.assertNotEqual(first.hexdigest(), changed.hexdigest())

    def test_timeout_and_error_evidence(self) -> None:
        evidence = runner.Evidence(tiny_manifest())
        with self.assertRaises(TimeoutError):
            evidence.call("search", "search", "small", lambda: (_ for _ in ()).throw(TimeoutError("late")))
        with self.assertRaises(RuntimeError):
            evidence.call("write", "writer", "small", lambda: (_ for _ in ()).throw(RuntimeError("bad")))
        self.assertEqual((evidence.errors["small"], evidence.timeouts["small"]), (2, 1))
        self.assertEqual([row["kind"] for row in evidence.events], ["timeout", "error"])

    def test_unowned_server_resources_are_not_claimed(self) -> None:
        resource = runner.server_resource_usage(None, None, "Qdrant")
        self.assertFalse(resource["captured"])
        self.assertEqual(resource["rss_bytes"], 0)
        self.assertEqual(resource["cpu_seconds"], 0.0)
        self.assertEqual(resource["availability"]["rss_bytes"], "unavailable")
        with mock.patch.object(runner.subprocess, "run", return_value=SimpleNamespace(stdout="2048 01:02.5")):
            self.assertFalse(runner.server_resource_usage(123, None, "Qdrant")["captured"])
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(runner.subprocess, "run", return_value=SimpleNamespace(stdout="2048 01:02.5")):
                owned = runner.server_resource_usage(123, Path(directory), "Qdrant")
        self.assertTrue(owned["captured"])
        self.assertEqual(owned["rss_bytes"], 2048 * 1024)
        self.assertEqual(owned["cpu_seconds"], 62.5)
        self.assertEqual(owned["availability"]["rss_bytes"], "Qdrant server PID 123")
        self.assertEqual(owned["availability"]["cpu_seconds"], "Qdrant server PID 123")
        baseline = {**owned, "rss_bytes": 1024, "cpu_seconds": 1.0, "disk_bytes": 100}
        end = {**owned, "rss_bytes": 4096, "cpu_seconds": 2.5, "disk_bytes": 250}
        delta = runner.resource_delta(baseline, end)
        self.assertEqual((delta["rss_bytes"], delta["cpu_seconds"], delta["disk_bytes"]), (3072, 1.5, 150))

    def test_shared_disk_snapshot_tolerates_disappearing_files_only(self) -> None:
        class Entries:
            def __init__(self, values: list[object]) -> None:
                self.values = values

            def __iter__(self):
                return iter(self.values)

            def __enter__(self):
                return self

            def __exit__(self, *_args: object) -> None:
                return None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stable = root / "stable"
            vanished = root / "vanished"
            stable.write_bytes(b"stable!")
            vanished.write_bytes(b"gone")
            with os.scandir(root) as scan:
                entries = list(scan)
            vanished.unlink()
            with mock.patch.object(runner.os, "scandir", return_value=Entries(entries)):
                self.assertEqual(runner.disk_bytes(root), len(b"stable!"))

            denied = SimpleNamespace(
                path=str(root / "denied"),
                stat=lambda **_kwargs: (_ for _ in ()).throw(PermissionError("denied")),
            )
            with mock.patch.object(runner.os, "scandir", return_value=Entries([denied])):
                with self.assertRaises(PermissionError):
                    runner.disk_bytes(root)

    def test_fake_qdrant_lifecycle_and_exact_oracles(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.run()
        artifact = workload.artifact()
        corrupt_id = runner.point_id("minima/empty_file/000007")
        shared.points[corrupt_id].vector["embedding"] = [0.0, 1.0] + [0.0] * 6
        expected_payload, _ = workload.expected_scroll()
        with mock.patch.object(runner, "final_documents", side_effect=AssertionError("corpus-sized materialization")):
            actual_payload, _, vector_evidence = workload.actual_scroll()
        self.assertEqual(actual_payload, expected_payload)
        self.assertFalse(vector_evidence["match"])
        self.assertEqual(vector_evidence["mismatch_rows"], 1)
        for identifier in (
            "minima/empty_file/000007",
            "minima/small/000000",
            "minima/mixed_broad_narrow/replacement/000000",
        ):
            self.assertEqual(len(workload.expected_vector(identifier)), manifest["config"]["dimension"])
        workload.close()
        self.assertEqual(shared.index_fields, ["user_id", "fpath"])
        self.assertEqual(len(shared.clients), 2)
        self.assertTrue(all(client.closed for client in shared.clients))
        self.assertEqual(shared.restart_count, 1)
        raw = artifact["backend_raw_evidence"]["qdrant"]
        self.assertEqual(raw["restart_boundary"]["old_pid"], 1)
        self.assertEqual(raw["restart_boundary"]["new_pid"], 2)
        self.assertTrue(raw["restart_boundary"]["verified"])
        self.assertTrue(raw["final_scroll_state"]["match"])
        self.assertFalse(artifact["passing"])
        self.assertEqual(artifact["state"], "partial")
        operations = artifact["backends"][0]["operations"]
        trace = operations["timed_execution_trace"]
        self.assertEqual(operations["timed_queries_executed"], manifest["config"]["timed_queries"])
        self.assertEqual(operations["timed_rounds_completed"], len(manifest["operations"][3]["timed_reader_plan"]["rounds"]))
        self.assertEqual(len(trace["queries"]), manifest["config"]["timed_queries"])
        for round_value in trace["rounds"]:
            queries = [row for row in trace["queries"] if row["round"] == round_value["ordinal"]]
            for reader in range(manifest["config"]["reader_concurrency"]):
                self.assertTrue(any(
                    row["reader"] == reader and runner.intervals_overlap(
                        row["started_monotonic_ns"], row["ended_monotonic_ns"],
                        round_value["writer_started_monotonic_ns"], round_value["writer_ended_monotonic_ns"],
                    )
                    for row in queries
                ))
        query_specs = {row["scenario"]: row for row in manifest["queries"]}
        for row in trace["queries"]:
            query = query_specs[row["scenario"]]
            self.assertTrue(row["result_captured"])
            self.assertTrue(workload.results_match(
                (row["actual_ids"], row["actual_scores"]),
                (query["initial_oracle_ids"], query["initial_oracle_scores"]),
            ))
        reindex_trace = operations["reindex_execution_trace"]
        self.assertEqual(operations["reindex_operations_executed"], 2)
        self.assertEqual(operations["reindex_execution_sha256"], runner.reindex_trace_digest(reindex_trace))
        self.assertTrue(all(
            runner.intervals_overlap(
                row["started_monotonic_ns"], row["ended_monotonic_ns"],
                operation["mutation_started_monotonic_ns"], operation["mutation_ended_monotonic_ns"],
            )
            for operation in reindex_trace["operations"]
            for row in operation["reader_queries"]
        ))
        queries = query_specs
        for operation in reindex_trace["operations"]:
            for row in operation["reader_queries"]:
                query = queries[row["scenario"]]
                oracles = (
                    ((query["initial_oracle_ids"], query["initial_oracle_scores"]), ([], []))
                    if operation["mutation"] == "delete_by_user_id_and_fpath"
                    else (([], []), (query["final_oracle_ids"], query["final_oracle_scores"]))
                )
                self.assertTrue(row["result_captured"])
                self.assertTrue(any(
                    workload.results_match((row["actual_ids"], row["actual_scores"]), oracle)
                    for oracle in oracles
                ))
        self.assertTrue(operations["reindex_delete_replace"])
        self.assertNotIn("phase_latency_samples", raw)
        self.assertGreater(raw["phase_latency_distributions"]["search"]["count"], 0)
        self.assertEqual(len(trace["rounds"]), len(manifest["operations"][3]["timed_reader_plan"]["rounds"]))
        self.assertEqual(operations["timed_execution_sha256"], runner.timed_trace_digest(trace))
        self.assertTrue(operations["batch_insert_during_search"])
        self.assertTrue(raw["timed_overlap"]["all_rounds_writer_search_overlap_observed"])
        self.assertTrue(all(row["initial_actual_ids"] == row["initial_oracle_ids"] for row in artifact["scenarios"]))
        self.assertTrue(all(row["actual_ids"] == row["final_oracle_ids"] for row in artifact["scenarios"]))
        self.assertTrue(all(row["reopen_ids"] == row["actual_ids"] for row in artifact["scenarios"]))
        self.assertTrue(shared.query_filters)

    def test_fake_qdrant_multi_reader_plan_records_every_reader(self) -> None:
        manifest, shared = tiny_manifest(2), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.run()
        artifact = workload.artifact()
        workload.close()

        operations = artifact["backends"][0]["operations"]
        timed_trace = operations["timed_execution_trace"]
        for round_value in timed_trace["rounds"]:
            readers = {
                row["reader"]
                for row in timed_trace["queries"]
                if row["round"] == round_value["ordinal"]
            }
            self.assertEqual(readers, {0, 1})
        for operation in operations["reindex_execution_trace"]["operations"]:
            self.assertEqual({row["reader"] for row in operation["reader_queries"]}, {0, 1})

    def test_multi_reader_mutation_waits_for_actual_writer_start(self) -> None:
        manifest, shared = tiny_manifest(2), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.connect()
        workload.create_owned_collection()
        original_delete_filter = workload.delete_filter
        original_delete = FakeClient.delete

        def delayed_delete_filter(operation: dict[str, object],
                                  on_writer_start: Callable[[], None] | None = None) -> None:
            original_delete_filter(operation, on_writer_start)

        def slow_delete(client: FakeClient, **kwargs: object) -> None:
            time.sleep(0.02)
            original_delete(client, **kwargs)

        workload.delete_filter = delayed_delete_filter
        with mock.patch.object(FakeClient, "delete", slow_delete):
            workload.run_concurrent_mutation(manifest["operations"][4])
        operation = workload.operations["reindex_execution_trace"]["operations"][0]
        self.assertEqual({row["reader"] for row in operation["reader_queries"]}, {0, 1})
        self.assertTrue(all(
            runner.intervals_overlap(
                row["started_monotonic_ns"], row["ended_monotonic_ns"],
                operation["mutation_started_monotonic_ns"], operation["mutation_ended_monotonic_ns"],
            )
            for row in operation["reader_queries"]
        ))
        workload.close()


    def test_timed_multi_reader_waits_for_actual_writer_start(self) -> None:
        manifest, shared = tiny_manifest(2), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.connect()
        workload.create_owned_collection()
        workload.insert_ranges(
            manifest["operations"][1]["name"], manifest["operations"][1]["insert_ranges"], False,
        )
        original_upsert_method = workload.upsert
        original_client_upsert = FakeClient.upsert

        def delayed_upsert(operation: str, scenario: str, documents: list[dict[str, object]],
                           wait_ready: bool = True,
                           on_writer_start: Callable[[], None] | None = None) -> None:
            time.sleep(0.05)
            original_upsert_method(
                operation, scenario, documents, wait_ready=wait_ready, on_writer_start=on_writer_start,
            )

        def slow_client_upsert(client: FakeClient, **kwargs: object) -> None:
            time.sleep(0.02)
            original_client_upsert(client, **kwargs)

        workload.upsert = delayed_upsert
        with mock.patch.object(FakeClient, "upsert", slow_client_upsert):
            workload.run_timed_overlap(manifest["operations"][3])
        trace = workload.operations["timed_execution_trace"]
        for round_value in trace["rounds"]:
            queries = [row for row in trace["queries"] if row["round"] == round_value["ordinal"]]
            self.assertEqual({row["reader"] for row in queries}, {0, 1})
            self.assertTrue(all(
                runner.intervals_overlap(
                    row["started_monotonic_ns"], row["ended_monotonic_ns"],
                    round_value["writer_started_monotonic_ns"], round_value["writer_ended_monotonic_ns"],
                )
                for row in queries
            ))
        workload.close()
    def test_actual_scroll_rejects_nonadvancing_cursor(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.run()
        original_scroll = workload.client.scroll

        def stuck_scroll(**kwargs: object) -> tuple[list[Model], str | None]:
            rows, next_offset = original_scroll(**kwargs)
            offset = kwargs["offset"]
            return rows, offset if offset is not None else next_offset

        with mock.patch.object(workload.client, "scroll", side_effect=stuck_scroll):
            with self.assertRaisesRegex(RuntimeError, "cursor did not advance"):
                workload.actual_scroll()
        workload.close()

    def test_fake_scroll_rejects_unknown_cursor(self) -> None:
        client = FakeClient(SharedQdrant())
        with self.assertRaises(ValueError):
            client.scroll(limit=1, offset="missing")

    def test_close_rejects_missing_restart_hook(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.restart_server = None
        with self.assertRaisesRegex(RuntimeError, "restart hook"):
            workload.run()
        workload.close()
    def test_restart_rejects_noop_pid(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.restart_server = lambda: workload.server_pid
        with self.assertRaisesRegex(RuntimeError, "original PID"):
            workload.restart_backend()
    def test_restart_rejects_live_original_pid(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.process_running = lambda _pid: True
        with self.assertRaisesRegex(RuntimeError, "still running"):
            workload.restart_backend()

    def test_restart_rejects_pid_not_owning_endpoint(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.process_owns_endpoint = lambda _pid, _url, _port: False
        with self.assertRaisesRegex(RuntimeError, "does not own"):
            workload.restart_backend()

    def test_process_owner_proof_binds_pid_to_listener_and_docker_port_mapping(self) -> None:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            container_port = listener.getsockname()[1]
            host_port = container_port - 1 if container_port == 65535 else container_port + 1
            self.assertTrue(runner.server_process_running(os.getpid()))
            self.assertTrue(runner.server_process_owns_endpoint(
                os.getpid(), f"http://127.0.0.1:{host_port}", container_port,
            ))

        manifest, shared = tiny_manifest(), SharedQdrant()
        workload = new_runner(manifest, shared)
        workload.server_listener_port = 6333
        observed: list[tuple[int, str, int | None]] = []
        workload.process_owns_endpoint = lambda pid, url, port: not observed.append((pid, url, port))
        workload.restart_backend()
        self.assertEqual(observed, [(2, "http://fake", 6333)])

    def test_linux_socket_inode_scan_skips_disappearing_descriptors_only(self) -> None:
        fds = [Path("/fake/1"), Path("/fake/2")]
        vanished = OSError(runner.errno.ENOENT, "descriptor disappeared")
        with (
            mock.patch.object(runner.Path, "iterdir", return_value=iter(fds)),
            mock.patch.object(runner.os, "readlink", side_effect=[vanished, "socket:[42]"]),
        ):
            self.assertEqual(runner.linux_process_socket_inodes(123), {"42"})
        denied = PermissionError(runner.errno.EACCES, "denied")
        with (
            mock.patch.object(runner.Path, "iterdir", return_value=iter(fds[:1])),
            mock.patch.object(runner.os, "readlink", side_effect=denied),
            self.assertRaises(PermissionError),
        ):
            runner.linux_process_socket_inodes(123)

    def test_main_writes_artifact_when_run_and_close_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            hook = root / "restart"
            hook.write_text("#!/bin/sh\n", encoding="utf-8")
            hook.chmod(0o755)
            output = root / "artifact.json"
            args = SimpleNamespace(
                server_pid=1, storage_path=root, manifest=root / "manifest.json",
                url="http://127.0.0.1:6333", api_key="", restart_hook=hook,
                collection="test", allow_drop=False, operation_timeout=1,
                optimizer_timeout=1, poll_interval=0, deployment="external", image="",
                output=output,
            )

            class FailingRunner:
                def __init__(self, *_args: object, **_kwargs: object) -> None:
                    self.evidence = SimpleNamespace(failures=[])

                def run(self) -> None:
                    raise ValueError("workload failed")

                def close(self) -> None:
                    raise RuntimeError("close failed")

                def artifact(self) -> dict[str, object]:
                    return {"failures": self.evidence.failures}

            qdrant_module = SimpleNamespace(QdrantClient=object, models=object)
            with (
                mock.patch.object(runner, "parse_args", return_value=args),
                mock.patch.object(runner, "load_manifest", return_value={}),
                mock.patch.object(runner.importlib.metadata, "version", return_value=runner.CLIENT_VERSION),
                mock.patch.object(runner, "server_info", return_value={"version": runner.SERVER_VERSION}),
                mock.patch.object(runner, "QdrantMinimaRunner", FailingRunner),
                mock.patch.dict(sys.modules, {"qdrant_client": qdrant_module}),
            ):
                self.assertEqual(runner.main(), 1)
            artifact = runner.json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["failures"], [
                "ValueError: workload failed",
                "RuntimeError: close failed",
            ])

    def test_qdrant_evidence_inputs_require_pid_and_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner.validate_qdrant_evidence_inputs(123, Path(directory))
            for pid, storage in ((None, Path(directory)), (0, Path(directory)), (123, None),
                                 (123, Path(directory) / "missing")):
                with self.subTest(pid=pid, storage=storage), self.assertRaises(RuntimeError):
                    runner.validate_qdrant_evidence_inputs(pid, storage)

    def test_external_launcher_requires_explicit_storage_before_setup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            hook = Path(directory) / "restart"
            hook.write_text("#!/usr/bin/env bash\nprintf '2\\n'\n", encoding="utf-8")
            hook.chmod(0o755)
            environment = dict(os.environ)
            for key in list(environment):
                if key.startswith("QDRANT_"):
                    environment.pop(key)
            environment.pop("RUN_DIR", None)
            environment.update({
                "QDRANT_URL": "http://127.0.0.1:1",
                "QDRANT_RESTART_HOOK": str(hook),
                "QDRANT_SERVER_PID": "1",
                "TMPDIR": directory,
            })
            script = Path(__file__).resolve().parents[2] / "scripts" / "bench_minima_qdrant.sh"
            result = subprocess.run(
                ["bash", str(script)], cwd=script.parents[1], env=environment,
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("authoritative QDRANT_STORAGE_PATH", result.stderr)
            self.assertEqual(len(list(Path(directory).glob("gomap_minima_qdrant_*"))), 1)


    def test_reads_after_writer_completion_fail_overlap_contracts(self) -> None:
        manifest = tiny_manifest()
        for method_name, operation_ordinal in (("run_timed_overlap", 3), ("run_concurrent_mutation", 4)):
            with self.subTest(method=method_name):
                shared = SharedQdrant()
                workload = new_runner(manifest, shared)
                workload.connect()
                workload.create_owned_collection()
                shared.writer_completed.clear()
                def late_search(_operation: str, scenario: str, interval: dict[str, int] | None = None):
                    if not shared.writer_completed.wait(1):
                        raise RuntimeError("writer did not complete")
                    time.sleep(0.01)
                    if interval is not None:
                        interval["started_monotonic_ns"] = time.monotonic_ns()
                        interval["ended_monotonic_ns"] = interval["started_monotonic_ns"] + 1
                    query = next(row for row in manifest["queries"] if row["scenario"] == scenario)
                    if method_name == "run_timed_overlap":
                        return query["initial_oracle_ids"], query["initial_oracle_scores"]
                    return [], []

                workload.search = late_search
                with self.assertRaisesRegex(RuntimeError, "overlap contract failed"):
                    getattr(workload, method_name)(manifest["operations"][operation_ordinal])
                workload.close()

    def test_existing_namespace_requires_explicit_allow_drop(self) -> None:
        manifest, shared = tiny_manifest(), SharedQdrant()
        shared.exists = True
        workload = new_runner(manifest, shared)
        with self.assertRaisesRegex(RuntimeError, "ALLOW_DROP"):
            workload.run()
        workload.close()


if __name__ == "__main__":
    unittest.main()
