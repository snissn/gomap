#!/usr/bin/env python3
"""Matched 500Kx768D Cohere query-ready RSS boundary for Qdrant and TreeDB."""
from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_qdrant_runner as existing

SCHEMA = native.RSS_ARTIFACT_SCHEMA
CONTROLS = [32, 64, 128, 256, 512, 1024, 2048]


def qdrant_point(document):
    return {
        "logical_id": document["id"], "vector": document["embedding"],
        "payload": {"id": document["id"], "content": document["content"], "meta": document["meta"]},
    }


def ready_snapshot(snapshot, rows):
    schema = snapshot.get("payload_schema") or {}
    config = snapshot.get("config") or {}
    hnsw, optimizer, params = (config.get("hnsw_config") or {}, config.get("optimizer_config") or {},
                               config.get("params") or {})
    vector = params.get("vectors") or {}
    config_matches = not (any((schema.get(field) or {}).get("data_type") != "keyword"
            for field in ("meta.user_id", "meta.fpath"))
            or hnsw.get("m") != 16 or hnsw.get("ef_construct") != 100
            or hnsw.get("full_scan_threshold") != 10000 or hnsw.get("on_disk") is not False
            or optimizer.get("indexing_threshold") != 10000 or optimizer.get("max_optimization_threads") != 1
            or params.get("on_disk_payload") is not True or vector.get("size") != 768
            or str(vector.get("distance", "")).lower() != "cosine" or vector.get("on_disk") is not False)
    return (config_matches and snapshot.get("status") == "green"
            and existing.optimizer_is_ok(snapshot.get("optimizer_status"))
            and snapshot.get("points_count") == rows
            and snapshot.get("exact_points_count") == rows
            and snapshot.get("indexed_vectors_count") == rows
            and all((schema.get(field) or {}).get("points") == rows
                    for field in ("meta.user_id", "meta.fpath")))


def validate_ready_snapshot(snapshot, rows):
    if not ready_snapshot(snapshot, rows):
        raise RuntimeError("Qdrant did not reach the matched query-ready boundary")


def compare_artifacts(treedb, qdrant):
    reasons = []
    for artifact, backend in ((treedb, "treedb"), (qdrant, "qdrant")):
        if artifact.get("schema") != SCHEMA or artifact.get("backend") != backend or artifact.get("state") != "calibrated":
            reasons.append(f"{backend} artifact is not calibrated")
        if artifact.get("quality", {}).get("evaluation", {}).get("passed") is not True:
            reasons.append(f"{backend} evaluation quality missed the target")
        rss = artifact.get("rss", {})
        if (rss.get("availability") != "measured" or type(rss.get("bytes")) is not int
                or rss["bytes"] <= 0 or not rss.get("process_identity")):
            reasons.append(f"{backend} server VmHWM is unavailable")
    if treedb.get("comparison_contract") != qdrant.get("comparison_contract"):
        reasons.append("TreeDB and Qdrant comparison contracts differ")
    tree_provenance, qdrant_provenance = treedb.get("provenance", {}), qdrant.get("provenance", {})
    if (not tree_provenance.get("harness_commit")
            or tree_provenance.get("harness_commit") != qdrant_provenance.get("harness_commit")
            or tree_provenance.get("harness_trees") != qdrant_provenance.get("harness_trees")):
        reasons.append("TreeDB and Qdrant harness provenance differs")
    if treedb.get("rss", {}).get("process_identity") == qdrant.get("rss", {}).get("process_identity"):
        reasons.append("backend process identities are not distinct")
    if reasons:
        return {"state": "uncalibrated", "reasons": reasons}
    tree_bytes, qdrant_bytes = treedb["rss"]["bytes"], qdrant["rss"]["bytes"]
    accepted = tree_bytes <= qdrant_bytes
    return {
        "state": "accept" if accepted else "investigate", "treedb_rss_bytes": tree_bytes,
        "qdrant_rss_bytes": qdrant_bytes, "delta_bytes": tree_bytes - qdrant_bytes,
        "treedb_to_qdrant_ratio": tree_bytes / qdrant_bytes,
        "recommendation": ("stop prioritizing TreeDB RSS for this 500K x 768D initial-ready workload"
                           if accepted else "quantify the higher TreeDB owner before redesign"),
    }


def comparison_contract(dataset_manifest_sha256, dataset_files_sha256, cpu_affinity):
    return native.rss_comparison_contract({
        "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "dataset_files_sha256": dataset_files_sha256, "cpu_affinity": cpu_affinity,
        "host_memory_bytes": existing.memory_bytes(), "platform": native.platform.platform(),
        "treedb_go_runtime": {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")},
        "host_resource_identity": native.host_resource_identity(),
        "rss_recall_target": native.RSS_RECALL_TARGET,
        "rss_calibration_queries": native.RSS_CALIBRATION_QUERIES,
        "rss_evaluation_queries": native.RSS_EVALUATION_QUERIES,
    })


def validate_imports(source):
    for imported in (native, existing):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / "benchmarks/vector_db_compare"):
            raise ValueError("imported RSS dependency is outside the frozen source tree")


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    harness = native.existing.repository_commit()
    dataset, _, files, _ = native.dataset_identity(args.dataset, 500000)
    tree = json.loads(args.treedb_artifact.read_text())
    if not isinstance(tree, dict):
        raise ValueError("TreeDB RSS artifact must be a JSON object")
    binary = args.qdrant_bin.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Qdrant binary must be an executable file")
    parsed = urllib.parse.urlparse(args.url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
        raise ValueError("Qdrant URL must name an explicit 127.0.0.1 HTTP port")
    if args.storage_path.exists() or args.run_dir.exists():
        raise ValueError("Qdrant storage and run directories must not exist before the owned launch")
    if importlib.metadata.version("qdrant-client") != existing.CLIENT_VERSION:
        raise RuntimeError(f"qdrant-client must be exactly {existing.CLIENT_VERSION}")
    affinity = sorted(os.sched_getaffinity(0))
    root_tree = lambda path: subprocess.check_output(
        ["git", "rev-parse", "HEAD:" + path], cwd=source, text=True,
    ).strip()
    harness_trees = {path: root_tree(path) for path in (
        "benchmarks/vector_db_compare", "clients/python/treedb_client",
    )}
    contract = comparison_contract(native.digest(dataset / "manifest.json"), files, affinity)
    if (tree.get("schema") != SCHEMA or tree.get("backend") != "treedb" or tree.get("state") != "calibrated"
            or tree.get("quality", {}).get("evaluation", {}).get("passed") is not True
            or tree.get("comparison_contract") != contract
            or tree.get("provenance", {}).get("harness_commit") != harness
            or tree.get("provenance", {}).get("harness_trees") != harness_trees):
        raise RuntimeError("TreeDB RSS artifact quality, contract, or harness provenance is not current")
    return {
        "schema": "treedb_cohere_qdrant_rss_plan/v1", "harness_commit": harness,
        "harness_source_sha256": native.digest(Path(__file__)),
        "harness_trees": harness_trees,
        "dataset": str(dataset), "dataset_manifest_sha256": native.digest(dataset / "manifest.json"),
        "dataset_files_sha256": files, "treedb_artifact": str(args.treedb_artifact.resolve()),
        "treedb_artifact_sha256": native.digest(args.treedb_artifact),
        "comparison_contract": contract, "qdrant_bin": str(binary),
        "qdrant_bin_sha256": native.digest(binary), "qdrant_server_version": existing.SERVER_VERSION,
        "qdrant_client_version": existing.CLIENT_VERSION,
        "storage_path": str(args.storage_path.resolve()), "url": args.url, "collection": args.collection,
        "run_dir": str(args.run_dir.resolve()), "cpu_affinity": affinity,
        "batch_size": 256, "rows": 500000, "dimensions": 768, "top_k": 10,
        "controls": CONTROLS, "operation_timeout_s": args.operation_timeout,
        "startup_timeout_s": args.startup_timeout,
        "optimizer_timeout_s": args.optimizer_timeout, "poll_interval_s": args.poll_interval,
        "production_hnsw": existing.PRODUCTION_HNSW_CONFIG,
        "production_optimizers": existing.PRODUCTION_OPTIMIZERS_CONFIG,
        "initial_upload_hnsw": existing.INITIAL_UPLOAD_HNSW_CONFIG,
        "initial_upload_optimizers": existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
    }


def validate_plan(plan, expected):
    if plan != expected:
        raise ValueError("frozen Qdrant RSS plan differs from current source/runtime/dataset")


class Run:
    optimization_snapshot = existing.QdrantMinimaRunner.optimization_snapshot
    server_log_snapshot = existing.QdrantMinimaRunner.server_log_snapshot

    def __init__(self, plan, client_factory, models, api_key=""):
        self.plan, self.client_factory, self.models = plan, client_factory, models
        self.api_key = api_key
        self.client = self.process = self.process_identity = self.process_command_identity = None
        self.server_pid = None
        self.server_log = None
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)
        self.collection = plan["collection"]
        self.operation_timeout = plan["operation_timeout_s"]
        self.optimizer_timeout = plan["optimizer_timeout_s"]
        self.poll_interval = plan["poll_interval_s"]
        self.storage_path = Path(plan["storage_path"])
        self.resource_server_name = "Qdrant"
        self.server_log_path = self.output / "qdrant.log"
        self.config = {"scalar_fields": ["meta.user_id", "meta.fpath"]}
        self.readiness_evidence = []
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
        self.queries = np.memmap(data / "queries.f32", mode="r", dtype="<f4", shape=(100, 768))
        self.truth = json.loads((data / "truth.json").read_text())["500000"]

    def wait_ready(self, rows, phase, production=True):
        deadline = time.monotonic() + self.optimizer_timeout
        while True:
            saved_timeout = self.optimizer_timeout
            self.optimizer_timeout = max(0, deadline - time.monotonic())
            try:
                existing.QdrantMinimaRunner.wait_ready(self, expected_count=rows, phase=phase)
            finally:
                self.optimizer_timeout = saved_timeout
            snapshot = self.readiness_evidence[-1]["snapshots"][-1]
            if not production or ready_snapshot(snapshot, rows):
                return snapshot
            if time.monotonic() >= deadline:
                raise TimeoutError("Qdrant full query-ready boundary exceeded optimizer timeout")
            time.sleep(self.poll_interval)

    def start_server(self):
        if self.storage_path.exists():
            raise RuntimeError("owned Qdrant storage path already exists")
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.server_log = (self.output / "qdrant.log").open("xb")
        parsed = urllib.parse.urlparse(self.plan["url"])
        env = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
        env.update(QDRANT__SERVICE__HOST="127.0.0.1", QDRANT__SERVICE__HTTP_PORT=str(parsed.port),
                   QDRANT__STORAGE__STORAGE_PATH=str(self.storage_path))
        if self.api_key:
            env["QDRANT__SERVICE__API_KEY"] = self.api_key
        self.process = subprocess.Popen([self.plan["qdrant_bin"]], stdin=subprocess.DEVNULL,
                                        stdout=self.server_log, stderr=subprocess.STDOUT,
                                        cwd=self.output, env=env, start_new_session=True)
        self.server_pid = self.process.pid
        deadline, last = time.monotonic() + self.plan["startup_timeout_s"], None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned Qdrant exited during startup with {self.process.returncode}")
            try:
                identity = existing.linux_process_identity(self.server_pid)
                info = existing.server_info(self.plan["url"], self.api_key)
                if (identity and info.get("version") == self.plan["qdrant_server_version"]
                        and Path(f"/proc/{self.server_pid}/exe").resolve() == Path(self.plan["qdrant_bin"])
                        and existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                    self.process_identity = identity
                    self.process_command_identity = existing.server_process_identity(self.server_pid)
                    self.client = self.client_factory()
                    return
            except Exception as exc:
                last = exc
            time.sleep(self.plan["poll_interval_s"])
        raise TimeoutError(f"owned Qdrant startup exceeded timeout: {last}")

    def stop_server(self):
        if self.process is None:
            return
        if self.process.poll() is not None:
            raise RuntimeError(f"owned Qdrant exited before shutdown with {self.process.returncode}")
        if (self.process_identity is not None
                and existing.linux_process_identity(self.server_pid) != self.process_identity):
            raise RuntimeError("owned Qdrant identity changed; refusing to signal")
        self.process.terminate()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
            raise RuntimeError("owned Qdrant required forced shutdown")
        if self.server_log:
            self.server_log.close()
        if self.process.returncode != 0:
            raise RuntimeError(f"owned Qdrant shutdown exited with {self.process.returncode}")

    def validate_fresh(self):
        identity = existing.linux_process_identity(self.server_pid)
        collections = getattr(self.client.get_collections(), "collections", None)
        collection_dir = self.storage_path / "collections"
        if (identity != self.process_identity or sorted(os.sched_getaffinity(self.server_pid)) != self.plan["cpu_affinity"]
                or collections is None or collections or (collection_dir.exists() and any(collection_dir.iterdir()))):
            raise RuntimeError("Qdrant process/backend is not fresh or changed before load")
        peak = native.process_peak_at_boundary(self.server_pid, identity, self.plan["cpu_affinity"])
        if peak.get("availability") != "measured":
            raise RuntimeError("fresh Qdrant process VmHWM is unavailable")
        return peak

    def create(self):
        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=self.models.VectorParams(size=768, distance=self.models.Distance.COSINE, on_disk=False),
            hnsw_config=self.models.HnswConfigDiff(**self.plan["initial_upload_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["initial_upload_optimizers"]),
            on_disk_payload=True,
            timeout=self.operation_timeout,
        )
        for field in self.config["scalar_fields"]:
            self.client.create_payload_index(collection_name=self.collection, field_name=field,
                field_schema=self.models.PayloadSchemaType.KEYWORD, wait=True, timeout=self.operation_timeout)
        self.wait_ready(0, "fresh_collection", production=False)

    def load(self):
        for start in range(0, self.plan["rows"], self.plan["batch_size"]):
            points = []
            for row in range(start, min(start + self.plan["batch_size"], self.plan["rows"])):
                document = native.make_document(self.vectors, row, self.plan["rows"])
                value = qdrant_point(document)
                points.append(self.models.PointStruct(
                    id=existing.point_id(value["logical_id"]), vector=value["vector"], payload=value["payload"],
                ))
            self.client.upsert(collection_name=self.collection, points=points, wait=True,
                               timeout=self.operation_timeout)
        self.client.update_collection(
            collection_name=self.collection,
            hnsw_config=self.models.HnswConfigDiff(**self.plan["production_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["production_optimizers"]),
            timeout=self.operation_timeout,
        )
        return self.wait_ready(self.plan["rows"], "initial_load_to_ann_ready")

    def search(self, control, query, exact=False):
        response = self.client.query_points(
            collection_name=self.collection, query=self.queries[query].tolist(), limit=10,
            with_payload=True, with_vectors=False,
            search_params=self.models.SearchParams(hnsw_ef=control, exact=exact),
            timeout=self.operation_timeout,
        )
        ids = []
        for point in getattr(response, "points", response):
            payload = getattr(point, "payload", None) or {}
            if (not isinstance(payload.get("id"), str) or not isinstance(payload.get("content"), str)
                    or set((payload.get("meta") or {})) != {"user_id", "fpath"}):
                raise RuntimeError("Qdrant query returned a mismatched logical document")
            try:
                row = int(payload["id"].removeprefix("row-"))
                expected = native.make_document(self.vectors, row, self.plan["rows"])
            except (IndexError, ValueError):
                raise RuntimeError("Qdrant query returned an invalid logical ID") from None
            if payload["id"] != expected["id"] or payload["content"] != expected["content"] or payload["meta"] != expected["meta"]:
                raise RuntimeError("Qdrant query returned mismatched logical payload values")
            ids.append(payload["id"])
        return ids

    def execute(self):
        artifact = None
        try:
            self.start_server()
            baseline = self.validate_fresh()
            tree = json.loads(Path(self.plan["treedb_artifact"]).read_text())
            if native.digest(self.plan["treedb_artifact"]) != self.plan["treedb_artifact_sha256"]:
                raise RuntimeError("TreeDB RSS artifact changed after freeze")
            if tree.get("comparison_contract") != self.plan["comparison_contract"]:
                raise RuntimeError("TreeDB RSS artifact does not match the Qdrant plan")
            self.create()
            readiness = self.load()
            quality = native.calibrate_ann_control(
                lambda control, query: self.search(control, query), self.truth, self.plan["controls"],
                native.RSS_CALIBRATION_QUERIES, native.RSS_EVALUATION_QUERIES, native.RSS_RECALL_TARGET,
            )
            rss = native.process_peak_at_boundary(
                self.server_pid, self.process_identity, self.plan["cpu_affinity"],
            )
            # Keep the exact correctness reference outside the sampled ANN RSS boundary.
            exact_ids = self.search(self.plan["controls"][-1], 0, exact=True)
            if set(exact_ids) != set(self.truth[0]):
                raise RuntimeError("Qdrant exact correctness reference differs from exhaustive truth")
            reasons = []
            if not quality["evaluation"]["passed"]:
                reasons.append("no independently selected Qdrant hnsw_ef passed evaluation recall")
            if rss.get("availability") != "measured":
                reasons.append("Qdrant server VmHWM unavailable or process drifted")
            if (existing.server_process_identity(self.server_pid) != self.process_command_identity
                    or Path(f"/proc/{self.server_pid}/exe").resolve() != Path(self.plan["qdrant_bin"])
                    or not existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                reasons.append("Qdrant binary, command, or listener changed before result finalization")
            artifact = {
                "schema": SCHEMA, "state": "calibrated" if not reasons else "uncalibrated",
                "backend": "qdrant", "comparison_contract": self.plan["comparison_contract"],
                "quality": {**quality, "control_name": "hnsw_ef", "exact_mode": False,
                            "exact_correctness_reference_recall_at_10": 1.0,
                            "exact_correctness_reference_timing": "after_rss_boundary"},
                "rss": rss, "reasons": reasons, "readiness": readiness,
                "fresh_start_peak_rss": baseline,
                "durability_visibility": "Qdrant 1.19.0 upsert wait=true",
                "physical_id_mapping": "uuid5(logical row-*); payload id preserves the matched logical ID",
                "configuration": {key: self.plan[key] for key in (
                    "production_hnsw", "production_optimizers", "initial_upload_hnsw", "initial_upload_optimizers",
                )},
                "provenance": {key: self.plan[key] for key in (
                    "harness_commit", "harness_source_sha256", "harness_trees", "qdrant_bin_sha256",
                    "qdrant_server_version", "qdrant_client_version", "dataset_manifest_sha256",
                    "dataset_files_sha256", "treedb_artifact_sha256",
                )},
            }
            artifact["provenance"].update(process_identity=self.process_identity,
                                          process_command_identity=self.process_command_identity)
            comparison = compare_artifacts(tree, artifact)
        except BaseException as exc:
            reason = f"{type(exc).__name__}: {exc}"
            artifact = artifact or {"schema": SCHEMA, "backend": "qdrant",
                                    "comparison_contract": self.plan["comparison_contract"],
                                    "quality": {"evaluation": {"passed": False}},
                                    "rss": {"availability": "unavailable", "bytes": None,
                                            "process_identity": self.process_identity or ""},
                                    "readiness": self.readiness_evidence[-1] if self.readiness_evidence else {}}
            artifact["state"] = "uncalibrated"
            artifact.setdefault("reasons", []).append(reason)
            comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
        finally:
            try:
                if self.client:
                    self.client.close()
            except BaseException as exc:
                artifact["state"] = "uncalibrated"
                artifact.setdefault("reasons", []).append(f"client close: {type(exc).__name__}: {exc}")
                comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
            try:
                self.stop_server()
            except BaseException as exc:
                artifact["state"] = "uncalibrated"
                artifact.setdefault("reasons", []).append(f"server stop: {type(exc).__name__}: {exc}")
                comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
        (self.output / "qdrant-rss.json").write_bytes(native.canonical(artifact))
        (self.output / "comparison.json").write_bytes(native.canonical(comparison))
        print(json.dumps(comparison, sort_keys=True, allow_nan=False))
        return int(comparison["state"] == "uncalibrated")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--treedb-artifact", required=True, type=Path)
    parser.add_argument("--qdrant-bin", required=True, type=Path)
    parser.add_argument("--storage-path", required=True, type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api-key", default=os.environ.get("QDRANT_API_KEY", ""))
    parser.add_argument("--collection", default="minima_cohere_rss")
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--operation-timeout", type=int, default=600)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--optimizer-timeout", type=float, default=2700)
    parser.add_argument("--poll-interval", type=float, default=.25)
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        args.freeze.parent.mkdir(parents=True, exist_ok=True)
        with args.freeze.open("xb") as stream:
            stream.write(native.canonical(plan))
        print(native.digest(args.freeze), args.freeze)
        return 0
    if not args.expected_plan_sha256 or native.digest(args.run) != args.expected_plan_sha256:
        raise ValueError("externally pinned Qdrant RSS plan hash required")
    validate_plan(json.loads(args.run.read_text()), plan)
    from qdrant_client import QdrantClient, models
    factory = lambda: QdrantClient(url=args.url, api_key=args.api_key or None,
                                   timeout=args.operation_timeout, prefer_grpc=False)
    return Run(plan, factory, models, args.api_key).execute()


if __name__ == "__main__":
    raise SystemExit(main())
