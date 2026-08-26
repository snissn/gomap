#!/usr/bin/env python3
"""Execute the frozen Minima operation manifest through TreeDB's public HTTP client."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import platform
import subprocess
import threading
import time
from types import SimpleNamespace
from typing import Any

import minima_qdrant_runner as common
from treedb_client import TreeDBClient

CLIENT_VERSION = "0.1.0"
SERVICE_CONTRACT = "treedb-document-service/v1alpha2"


def scalar_filter(spec: dict[str, Any]) -> dict[str, Any]:
    conditions = [{"field": "meta.user_id", "operator": "==", "value": spec["user_id"]}]
    if spec["filter"] == "user_id+fpath":
        conditions.append({"field": "meta.fpath", "operator": "==", "value": spec["fpath"]})
    return conditions[0] if len(conditions) == 1 else {"operator": "AND", "conditions": conditions}


def service_document(document: dict[str, Any]) -> dict[str, Any]:
    return {"id": document["id"], "content": document["content"], "embedding": document["vector"],
            "meta": {"user_id": document["user_id"], "fpath": document["fpath"]}}


class ServiceController:
    def __init__(self, binary: Path, url: str, data_dir: Path, profile: str, timeout: float) -> None:
        self.binary, self.url, self.data_dir, self.profile, self.timeout = binary, url.rstrip("/"), data_dir, profile, timeout
        self.process: subprocess.Popen[str] | None = None
        self.rss_max = 0
        self.cpu_seconds = 0.0
        self.samples_complete = True

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process is not None and self.process.poll() is None else None

    def _sample(self) -> None:
        usage = common.server_resource_usage(self.pid, self.data_dir)
        self.samples_complete = self.samples_complete and usage["captured"]
        if usage["rss_bytes"] is not None:
            self.rss_max = max(self.rss_max, int(usage["rss_bytes"]))
        if usage["cpu_seconds"] is not None:
            self.cpu_seconds += float(usage["cpu_seconds"])

    def start(self) -> None:
        if self.pid is not None:
            return
        self.data_dir.mkdir(parents=True, exist_ok=True)
        address = self.url.removeprefix("http://")
        if "/" in address or self.url.startswith("https://"):
            raise ValueError("owned TreeDB service URL must be a plain http://host:port address")
        self.process = subprocess.Popen(
            [str(self.binary), "-addr", address, "-dir", str(self.data_dir), "-profile", self.profile],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        deadline, last = time.monotonic() + self.timeout, ""
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                last = self.process.stdout.read() if self.process.stdout is not None else ""
                raise RuntimeError(f"TreeDB service exited during startup: {last}")
            try:
                health = TreeDBClient(self.url, timeout=1).health()
                if health.get("ok") is True and health.get("contract_version") == SERVICE_CONTRACT:
                    return
                last = repr(health)
            except BaseException as exc:
                last = repr(exc)
            time.sleep(0.05)
        raise TimeoutError(f"TreeDB service readiness exceeded {self.timeout}s: {last}")

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self._sample()
            self.process.terminate()
            try:
                self.process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=self.timeout)
        self.process = None

    def resource(self) -> dict[str, Any]:
        self._sample()
        return {"captured": self.samples_complete, "rss_bytes": self.rss_max,
                "cpu_seconds": self.cpu_seconds, "disk_bytes": common.disk_bytes(self.data_dir)}


class ThreadLocalClients:
    def __init__(self, url: str, timeout: float, controller: ServiceController) -> None:
        self.url, self.timeout, self.controller = url, timeout, controller
        self.local = threading.local()
        self.lock = threading.Lock()
        self.clients: list[TreeDBClient] = []

    def current(self) -> TreeDBClient:
        client = getattr(self.local, "client", None)
        if client is None:
            client = TreeDBClient(self.url, timeout=self.timeout)
            self.local.client = client
            with self.lock:
                self.clients.append(client)
        return client

    def __getattr__(self, name: str) -> Any:
        return getattr(self.current(), name)

    def close(self) -> None:
        with self.lock:
            clients, self.clients = self.clients, []
        for client in clients:
            client.close()
        self.local = threading.local()
        self.controller.stop()


class TreeDBMinimaRunner(common.QdrantMinimaRunner):
    def __init__(self, manifest: dict[str, Any], *, controller: ServiceController, collection: str,
                 operation_timeout: float, ef_search: int) -> None:
        self.controller = controller
        controller.start()
        clients = ThreadLocalClients(controller.url, operation_timeout, controller)
        super().__init__(manifest, client_factory=lambda: clients, models=None, url=controller.url,
                         collection=collection, allow_drop=False, operation_timeout=int(operation_timeout),
                         optimizer_timeout=operation_timeout, poll_interval=0.05,
                         server_version=SERVICE_CONTRACT, deployment="owned_process", image="",
                         storage_path=controller.data_dir, server_pid=controller.pid,
                         restart_server=self.restart_controller, restart_identity="owned TreeDB service controller")
        self.clients, self.ef_search = clients, ef_search
        self.route_evidence: dict[str, Any] = {}
    def restart_controller(self) -> int:
        self.controller.start()
        if self.controller.pid is None:
            raise RuntimeError("TreeDB service controller restarted without a PID")
        return self.controller.pid


    def connect(self) -> None:
        self.controller.start()
        self.client = self.clients

    def create_owned_collection(self) -> None:
        assert self.client is not None
        self.evidence.call("ensure_compatible_collection", "writer", "all", lambda: self.client.ensure_index(
            self.collection, self.config["dimension"], self.config["metric"],
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"},
                           {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options={"strategy": "native_runtime"},
        ))
        self.ensure_compatible()

    def ensure_compatible(self) -> None:
        assert self.client is not None
        info = self.client.ensure_index(
            self.collection, self.config["dimension"], self.config["metric"],
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"},
                           {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options={"strategy": "native_runtime"},
        )
        fields = {(row.field, row.value_type) for row in info.scalar_fields}
        if (info.dimension, info.metric, info.vector_strategy, fields) != (
            self.config["dimension"], self.config["metric"], "native_runtime",
            {("meta.user_id", "string"), ("meta.fpath", "string")},
        ):
            raise RuntimeError("TreeDB index is not the compatible native_runtime Minima schema")
        self.effective_collection = info.to_dict()

    def wait_ready(self, expected_count: int | None = None) -> None:
        assert self.client is not None
        if expected_count is not None:
            count = self.client.count_documents(self.collection).count
            if count != expected_count:
                raise RuntimeError(f"TreeDB visible document count={count}, expected={expected_count}")

    def upsert(self, operation: str, scenario: str, documents: list[dict[str, Any]], wait_ready: bool = True) -> None:
        assert self.client is not None
        for start in range(0, len(documents), self.config["batch_size"]):
            batch = [service_document(row) for row in documents[start:start + self.config["batch_size"]]]
            response = self.evidence.call(operation, "writer", scenario, lambda batch=batch: self.client.upsert_documents(
                self.collection, batch, defer_vector_index_rebuild=True))
            if response.upserted != len(batch) or response.ids != [row["id"] for row in batch]:
                raise RuntimeError("TreeDB upsert completion did not cover the submitted batch")

    def search(self, operation: str, scenario: str, interval: dict[str, int] | None = None) -> tuple[list[str], list[float]]:
        assert self.client is not None
        spec, query = self.specs[scenario], self.queries[scenario]
        if interval is not None:
            interval["started_monotonic_ns"] = time.monotonic_ns()
        try:
            response = self.evidence.call(operation, "search", scenario, lambda: self.client.query_by_embedding(
                self.collection, query["vector"], self.config["top_k"], scalar_filter(spec),
                route="ann", ef_search=self.ef_search))
        finally:
            if interval is not None:
                interval["ended_monotonic_ns"] = time.monotonic_ns()
        if response.route != "ann" or not response.native_base_plus_live_delta or response.exact_fallbacks != 0 or response.full_document_scan_fallbacks != 0:
            raise RuntimeError(f"TreeDB query left required native route: {response!r}")
        self.route_evidence[scenario] = response
        started, ids, scores = time.monotonic_ns(), [], []
        for document in response.documents:
            if document.meta.get("user_id") != spec.get("user_id") or (spec["filter"] == "user_id+fpath" and document.meta.get("fpath") != spec.get("fpath")):
                self.evidence.cross_user[scenario] += 1
            ids.append(document.id)
            if document.score is None:
                raise RuntimeError("TreeDB ANN result omitted score")
            scores.append(float(document.score))
        ended = time.monotonic_ns()
        self.evidence.samples.append({"operation": operation, "scenario": scenario, "category": "decode",
                                      "start_nanos": started, "end_nanos": ended, "duration_nanos": ended - started})
        return ids, scores

    def retrieve(self, operation: str, scenario: str, ids: list[str]) -> list[Any]:
        assert self.client is not None
        rows = []
        for identifier in ids:
            result = self.evidence.call(operation, "fetch", scenario, lambda identifier=identifier: self.client.filter_documents(
                self.collection, {"field": "id", "operator": "==", "value": identifier}, limit=1))
            rows.extend(SimpleNamespace(payload={"id": row.id, "content": row.content, **row.meta}) for row in result.documents)
        return rows

    def delete_filter(self, operation: dict[str, Any]) -> None:
        assert self.client is not None
        name, scenario = operation["name"], operation["target"]
        filt = scalar_filter({**self.specs[scenario], **operation["filter"]})
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete_by_filter(self.collection, filt))
        remaining = self.evidence.call(name, "fetch", scenario, lambda: self.client.count_documents(self.collection, filt)).count
        self.evidence.stale_delete[scenario] += remaining
        if remaining:
            raise RuntimeError(f"filtered reindex delete left {remaining} matching rows")

    def delete_ids(self, operation: dict[str, Any]) -> None:
        assert self.client is not None
        name, scenario, ids = operation["name"], operation["target"], operation["ids"]
        response = self.evidence.call(name, "writer", scenario, lambda: self.client.delete_documents(self.collection, ids))
        if response.deleted != len(ids):
            raise RuntimeError("explicit delete completion count mismatch")
        stale = len(self.retrieve(name, scenario, ids))
        self.evidence.stale_delete[scenario] += stale
        if stale:
            raise RuntimeError(f"explicit delete left {stale} IDs visible")

    def actual_scroll(self) -> tuple[str, int, dict[str, Any]]:
        assert self.client is not None
        accumulator, mismatches, maximum_delta = common.StateAccumulator(), 0, 0.0
        after_id: str | None = None
        while True:
            result = self.client.filter_documents(
                self.collection, limit=1024, return_embedding=True,
                after_id=after_id, cursor_page=True,
            )
            if result.matched_count != len(result.documents) or len(result.documents) > 1024:
                raise RuntimeError("TreeDB cursor page count exceeds its bounded response")
            for row in result.documents:
                document = {"id": row.id, "content": row.content, "vector": row.embedding,
                            "user_id": row.meta.get("user_id"), "fpath": row.meta.get("fpath")}
                accumulator.add(document)
                try:
                    expected, actual = self.expected_vector(row.id), common.normalized_f32_vector(row.embedding or [])
                    deltas = [abs(left - right) for left, right in zip(actual, expected, strict=True)]
                    maximum_delta = max(maximum_delta, max(deltas, default=0.0))
                    mismatches += int(any(delta > self.config["score_tolerance"] for delta in deltas))
                except (KeyError, TypeError, ValueError):
                    mismatches += 1
            if result.exhausted:
                break
            if not result.next_after_id or result.next_after_id == after_id:
                raise RuntimeError("TreeDB cursor did not advance")
            after_id = result.next_after_id
        return accumulator.hexdigest(), accumulator.count, {
            "algorithm": "public filter stream plus normalized-float32 full-vector comparison",
            "checked_rows": accumulator.count, "mismatch_rows": mismatches,
            "maximum_component_delta": maximum_delta, "tolerance": self.config["score_tolerance"],
            "match": mismatches == 0,
        }

    def run_small(self) -> None:
        """Exercise the real small-scenario lifecycle without claiming qualification."""
        self.resource_baseline = common.server_resource_usage(self.controller.pid, self.storage_path)
        self.connect()
        self.create_owned_collection()
        spec = self.specs["small"]
        documents = [common.generated_document(spec, ordinal) for ordinal in range(spec["corpus_rows"])]
        self.upsert("small_initial_batch_insert", "small", documents)
        initial = self.search("small_initial_oracle", "small")
        self.evidence.initial["small"] = initial
        self.compare_oracle("initial", "small", initial)
        update = self.manifest["operations"][7]
        self.upsert(update["name"], "small", update["documents"])
        fetched = self.retrieve(update["name"], "small", [update["documents"][0]["id"]])
        self.operations["explicit_update_visible"] = (
            len(fetched) == 1 and fetched[0].payload.get("content") == update["documents"][0]["content"]
        )
        delete = self.manifest["operations"][9]
        self.delete_ids(delete)
        self.operations["explicit_delete_visible"] = True
        self.evidence.preclose["small"] = self.search("small_preclose", "small")
        assert self.client is not None
        self.client.close()
        self.client = None
        self.reopen_attempted = True
        self.connect()
        self.ensure_compatible()
        self.evidence.reopen["small"] = self.search("small_reopen", "small")
        final = self.search("small_final_oracle", "small")
        self.evidence.final["small"] = final
        self.compare_oracle("final", "small", final)
        self.reopen_parity = self.results_match(self.evidence.preclose["small"], self.evidence.reopen["small"])
        actual_hash, actual_rows, vector_evidence = self.actual_scroll()
        self.state_scroll = {"algorithm": "small diagnostic public filter stream", "actual_hash": actual_hash,
                             "actual_rows": actual_rows, "vectors": vector_evidence, "match": False}
        self.evidence.failures.append("small diagnostic intentionally omits representative scenarios and cannot qualify")

    def artifact(self) -> dict[str, Any]:
        artifact = super().artifact()
        resource = self.controller.resource()
        backend = artifact["backends"][0]
        backend.update({
            "name": "treedb", "server_version": SERVICE_CONTRACT, "client_version": CLIENT_VERSION,
            "durability": f"TreeDB {self.controller.profile}; owned service restart on the same data directory",
            "configuration": {"url": self.url, "collection": self.collection, "dimension": str(self.config["dimension"]),
                              "metric": self.config["metric"], "scalar_fields": "user_id,fpath",
                              "vector_strategy": "native_runtime", "ef_search": str(self.ef_search),
                              "profile": self.controller.profile, "service_binary": str(self.controller.binary)},
            "environment": {"os": platform.system() + " " + platform.release(), "arch": platform.machine() or "unavailable",
                            "cpu": platform.processor() or "unavailable", "memory": common.memory_bytes(),
                            "python": platform.python_version()},
        })
        for row in artifact["scenarios"]:
            row["backend"] = "treedb"
            route = self.route_evidence.get(row["scenario"])
            if route is None:
                continue
            row["route"] = {
                "identity": "native_base_plus_live_delta", "declared_scalar_filtering": True,
                "native_base_plus_live_delta": route.native_base_plus_live_delta,
                "full_document_scan_fallbacks": route.full_document_scan_fallbacks,
                "scalar_filter_unbounded": route.scalar_filter_unbounded,
                "probe_ids": route.scalar_filter_probe_ids, "candidate_ids": route.scalar_filter_candidates,
                "retained_candidate_ids": route.scalar_filter_retained_candidate_ids,
                "refined_candidate_ids": route.scalar_filter_refined_candidate_ids,
                "membership_source": route.scalar_filter_membership_source, "plan": route.scalar_filter_plan,
                "allowed_id_materialization_rows": route.allowed_id_materialization_rows,
                "primary_document_scans": route.primary_document_scans,
                "visited_candidates": route.scalar_filter_visited, "scored_candidates": route.scalar_filter_scored,
                "admitted_candidates": route.scalar_filter_admitted,
            }
            row["visibility"] = {"generation_consistent": True,
                                 "visibility_mismatch_count": route.visibility_mismatch_count,
                                 "visibility_retry_count": route.visibility_retry_count}
            row["resource"] = {"captured": resource["captured"], "bytes_per_op": None, "allocs_per_op": None,
                               "allocation_availability": "unavailable", "rss_bytes": resource["rss_bytes"],
                               "cpu_seconds": resource["cpu_seconds"], "disk_bytes": resource["disk_bytes"]}
        raw = artifact["backend_raw_evidence"].pop("qdrant")
        artifact["backend_raw_evidence"]["treedb"] = raw
        raw["native_route_responses"] = {
            scenario: {"membership_source": value.scalar_filter_membership_source,
                       "plan": value.scalar_filter_plan, "probe_ids": value.scalar_filter_probe_ids,
                       "candidates": value.scalar_filter_candidates,
                       "retained": value.scalar_filter_retained_candidate_ids,
                       "refined": value.scalar_filter_refined_candidate_ids,
                       "visited": value.scalar_filter_visited, "scored": value.scalar_filter_scored,
                       "admitted": value.scalar_filter_admitted,
                       "visibility_mismatches": value.visibility_mismatch_count,
                       "visibility_retries": value.visibility_retry_count}
            for scenario, value in self.route_evidence.items()
        }
        raw["resource_measurement"] = resource
        raw["resource_availability"] = {
            "baseline": {"rss_bytes": "owned TreeDB service process", "cpu_seconds": "owned TreeDB service process",
                         "disk_bytes": str(self.controller.data_dir), "bytes_per_op": "unavailable",
                         "allocs_per_op": "unavailable", "measurement_error": ""},
            "end": {"rss_bytes": "owned TreeDB service process", "cpu_seconds": "owned TreeDB service process",
                    "disk_bytes": str(self.controller.data_dir), "bytes_per_op": "unavailable",
                    "allocs_per_op": "unavailable", "measurement_error": ""},
        }
        return artifact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--service-bin", type=Path, required=True)
    parser.add_argument("--url", default="http://127.0.0.1:17120")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--profile", default="command_wal_durable")
    parser.add_argument("--operation-timeout", type=float, default=120)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--ef-search", type=int, default=128)
    parser.add_argument("--small", action="store_true", help="run the real small-scenario lifecycle and emit validated partial evidence")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = common.load_manifest(args.manifest)
    controller = ServiceController(args.service_bin.resolve(), args.url, args.data_dir.resolve(), args.profile, args.startup_timeout)
    runner = TreeDBMinimaRunner(manifest, controller=controller, collection=args.collection,
                                operation_timeout=args.operation_timeout, ef_search=args.ef_search)
    exit_code = 0
    try:
        runner.run_small() if args.small else runner.run()
    except BaseException as exc:
        runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(runner.artifact(), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        runner.close()
        controller.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
