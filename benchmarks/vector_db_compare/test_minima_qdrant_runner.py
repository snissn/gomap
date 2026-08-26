#!/usr/bin/env python3

from __future__ import annotations

import copy
import os
import subprocess
import threading
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
    Distance = SimpleNamespace(COSINE="cosine")
    PayloadSchemaType = SimpleNamespace(KEYWORD="keyword")


class SharedQdrant:
    def __init__(self) -> None:
        self.exists = False
        self.points: dict[str, Model] = {}
        self.payload_schema: dict[str, str] = {}
        self.vector_params: dict[str, Model] = {}
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

    def create_collection(self, *, vectors_config: dict[str, Model], **_: object) -> None:
        self.shared.exists = True
        self.shared.vector_params = vectors_config

    def create_payload_index(self, *, field_name: str, field_schema: str, **_: object) -> None:
        self.shared.payload_schema[field_name] = field_schema
        self.shared.index_fields.append(field_name)

    def get_collection(self, _: str) -> Model:
        return Model(
            status="green",
            optimizer_status=Model(ok=True),
            points_count=len(self.shared.points),
            payload_schema=self.shared.payload_schema,
            config=Model(params=Model(vectors=self.shared.vector_params), wal_config=Model(wal_capacity_mb=32)),
        )

    def upsert(self, *, points: list[Model], **_: object) -> None:
        time.sleep(0.002)
        with self.shared.lock:
            for point in points:
                self.shared.points[point.id] = point
        self.shared.writer_completed.set()

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
        start = keys.index(offset) if offset in keys else 0
        selected = keys[start:start + limit]
        rows = [Model(payload=self.shared.points[key].payload, vector=self.shared.points[key].vector) for key in selected]
        next_offset = keys[start + limit] if start + limit < len(keys) else None
        return rows, next_offset

    def close(self) -> None:
        self.closed = True


def reader_schedule(names: list[str]) -> list[dict[str, object]]:
    return [{"ordinal": ordinal, "actor": "reader", "scenario": name, "query_ordinal": ordinal} for ordinal, name in enumerate(names)]


def tiny_manifest() -> dict[str, object]:
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
        "reader_concurrency": 1, "writer_concurrency": 1, "warmup_queries": len(corpora),
        "timed_queries": len(corpora), "lookup_limit": 4096, "order_tolerance": 0,
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
        "assignment": "round=ordinal/1;reader=ordinal%1;scenario=scenario_order[ordinal%8]",
        "rounds": [
            {
                "ordinal": ordinal, "query_start": ordinal, "query_count": 1, "insert_range": insertion,
                "start_barrier": "round_start_readers_and_writer",
                "end_barrier": "round_end_queries_and_insert_complete",
            }
            for ordinal, insertion in enumerate(concurrent_ranges)
        ],
    }
    def mutation_plan(mutation: str) -> dict[str, object]:
        return {
            "mutation": mutation, "reader_concurrency": config["reader_concurrency"],
            "reader_assignments": [{"reader": 0, "query_ordinal": 0, "scenario": "mixed_broad_narrow"}],
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
        process_identity=lambda pid: f"fake-process-{pid}")


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
        resource = runner.server_resource_usage(None, None)
        self.assertFalse(resource["captured"])
        self.assertEqual(resource["rss_bytes"], 0)
        self.assertEqual(resource["cpu_seconds"], 0.0)
        self.assertEqual(resource["availability"]["rss_bytes"], "unavailable")
        with mock.patch.object(runner.subprocess, "run", return_value=SimpleNamespace(stdout="2048 01:02.5")):
            self.assertFalse(runner.server_resource_usage(123, None)["captured"])
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(runner.subprocess, "run", return_value=SimpleNamespace(stdout="2048 01:02.5")):
                owned = runner.server_resource_usage(123, Path(directory))
        self.assertTrue(owned["captured"])
        self.assertEqual(owned["rss_bytes"], 2048 * 1024)
        self.assertEqual(owned["cpu_seconds"], 62.5)
        baseline = {**owned, "rss_bytes": 1024, "cpu_seconds": 1.0, "disk_bytes": 100}
        end = {**owned, "rss_bytes": 4096, "cpu_seconds": 2.5, "disk_bytes": 250}
        delta = runner.resource_delta(baseline, end)
        self.assertEqual((delta["rss_bytes"], delta["cpu_seconds"], delta["disk_bytes"]), (3072, 1.5, 150))

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
            environment.pop("QDRANT_STORAGE_PATH", None)
            environment.update({
                "QDRANT_URL": "http://127.0.0.1:1",
                "QDRANT_RESTART_HOOK": str(hook),
                "QDRANT_SERVER_PID": "1",
                "RUN_DIR": str(Path(directory) / "run"),
            })
            script = Path(__file__).resolve().parents[2] / "scripts" / "bench_minima_qdrant.sh"
            result = subprocess.run(
                ["bash", str(script)], cwd=script.parents[1], env=environment,
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("authoritative QDRANT_STORAGE_PATH", result.stderr)


    def test_reads_after_writer_completion_fail_overlap_contracts(self) -> None:
        manifest = tiny_manifest()
        for method_name, operation_ordinal in (("run_timed_overlap", 3), ("run_concurrent_mutation", 4)):
            with self.subTest(method=method_name):
                shared = SharedQdrant()
                workload = new_runner(manifest, shared)
                workload.connect()
                workload.create_owned_collection()
                shared.writer_completed.clear()
                original_search = workload.search

                def late_search(operation: str, scenario: str, interval: dict[str, int] | None = None):
                    if not shared.writer_completed.wait(1):
                        raise RuntimeError("writer did not complete")
                    time.sleep(0.01)
                    return original_search(operation, scenario, interval)

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
