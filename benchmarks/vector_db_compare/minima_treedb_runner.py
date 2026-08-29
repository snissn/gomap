#!/usr/bin/env python3
"""Execute the frozen Minima operation manifest through TreeDB's public HTTP client."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import subprocess
import threading
import time
from types import SimpleNamespace
from typing import Any
import urllib.parse
import urllib.request

import minima_qdrant_runner as common
from treedb_client import TreeDBClient

CLIENT_VERSION = "0.1.0"
SERVICE_CONTRACT = "treedb-document-service/v1alpha2"
SERVICE_LOG_TAIL_BYTES = 64 << 10
DIAGNOSTICS_STATS_PATH = "/debug/treedb/stats"
DIAGNOSTIC_CAPTURE_BYTES = 32 << 20
DIAGNOSTIC_STATS_BYTES = 4 << 20
STATE_SCROLL_PAGE_SIZE = 8192
COMPACT_BATCH_CORRELATION_MAX_BYTES = 2048
BATCH_CORRELATION_SCHEMA = "treedb-minima-upsert-batch-correlations/v1"
PHASE_UNATTRIBUTED_RULE = (
    "total_duration_nanos = sum(phase.duration_nanos) + unattributed_nanos; "
    "unattributed_nanos <= max(60000000000, total_duration_nanos / 100); "
    "unattributed covers only runner bookkeeping between declared boundaries"
)
PHASE_CLASSIFICATIONS = {
    "initial_durable_load": "production_path",
    "warmup_search": "production_path",
    "timed_search_write_overlap": "production_path",
    "lifecycle_mutations": "production_path",
    "pre_close_queries": "production_path",
    "restart_open_readiness": "production_path",
    "post_reopen": "production_path",
    "final_state_scroll_artifact_work": "qualification_only",
}
DIAGNOSTIC_PROFILE_ENDPOINTS = {
    "cpu": ("/debug/pprof/profile", "cpu.pprof"),
    "goroutine": ("/debug/pprof/goroutine?debug=2", "goroutine.txt"),
    "mutex": ("/debug/pprof/mutex", "mutex.pprof"),
    "block": ("/debug/pprof/block", "block.pprof"),
    "trace": ("/debug/pprof/trace", "trace.out"),
    "stats": (DIAGNOSTICS_STATS_PATH, "stats.json"),
}


def scalar_filter(spec: dict[str, Any]) -> dict[str, Any]:
    conditions = [{"field": "meta.user_id", "operator": "==", "value": spec["user_id"]}]
    if spec["filter"] == "user_id+fpath":
        conditions.append({"field": "meta.fpath", "operator": "==", "value": spec["fpath"]})
    return conditions[0] if len(conditions) == 1 else {"operator": "AND", "conditions": conditions}


def service_document(document: dict[str, Any]) -> dict[str, Any]:
    return {"id": document["id"], "content": document["content"], "embedding": document["vector"],
            "meta": {"user_id": document["user_id"], "fpath": document["fpath"]}}

def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def repository_commit() -> str:
    root = Path(__file__).resolve().parents[2]
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root,
        check=True, capture_output=True, text=True,
    )
    if status.stdout:
        raise RuntimeError("TreeDB Minima runner requires a clean source checkout")
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root,
        check=True, capture_output=True, text=True,
    )
    commit = result.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("TreeDB Minima runner could not bind an exact source commit")
    return commit

def service_binary_build_provenance(binary: Path, expected_commit: str) -> tuple[str, str]:
    try:
        result = subprocess.run(
            ["go", "version", "-m", str(binary)],
            check=False, capture_output=True, text=True,
        )
    except OSError as exc:
        raise RuntimeError("TreeDB Minima runner could not inspect service binary Go build metadata") from exc
    if result.returncode != 0:
        raise RuntimeError("TreeDB Minima runner could not inspect service binary Go build metadata")
    settings: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split("\t")
        if len(fields) != 2 or fields[0] != "build" or "=" not in fields[1]:
            continue
        key, value = fields[1].split("=", 1)
        settings[key] = value
    revision, modified = settings.get("vcs.revision", ""), settings.get("vcs.modified", "")
    if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
        raise RuntimeError("TreeDB Minima service binary is missing an exact vcs.revision")
    if revision != expected_commit:
        raise RuntimeError("TreeDB Minima service binary vcs.revision does not match the source checkout")
    if modified != "false":
        raise RuntimeError("TreeDB Minima service binary must record vcs.modified=false")
    return revision, modified


class ServiceController:
    def __init__(self, binary: Path, url: str, data_dir: Path, profile: str,
                 startup_timeout: float, shutdown_timeout: float, *,
                 diagnostics_url: str | None = None, block_profile_rate: int = 1,
                 mutex_profile_fraction: int = 1, diagnostics_timeout: float = 2) -> None:
        self.binary, self.url, self.data_dir, self.profile = binary, url.rstrip("/"), data_dir, profile
        self.startup_timeout, self.shutdown_timeout = startup_timeout, shutdown_timeout
        self.diagnostics_url = diagnostics_url.rstrip("/") if diagnostics_url else None
        self.block_profile_rate = block_profile_rate
        self.mutex_profile_fraction = mutex_profile_fraction
        self.diagnostics_timeout = diagnostics_timeout
        self.process: subprocess.Popen[str] | None = None
        self.log_path = data_dir.parent / "treedb-document-service.log"
        self.log_file: Any | None = None
        self.last_shutdown_resource_end: dict[str, Any] | None = None

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process is not None and self.process.poll() is None else None

    def _listen_address(self, url: str, label: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "http" or parsed.path not in ("", "/") or parsed.query or parsed.fragment or not parsed.hostname or parsed.port is None:
            raise ValueError(f"owned TreeDB {label} URL must be a plain http://host:port address")
        if label == "diagnostics":
            try:
                loopback = parsed.hostname == "localhost" or ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError:
                loopback = False
            if not loopback:
                raise ValueError("owned TreeDB diagnostics URL must use a loopback host")
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        return f"{host}:{parsed.port}"
    def _read_bounded(self, url: str, timeout: float, maximum: int) -> bytes:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = response.read(maximum + 1)
        if len(payload) > maximum:
            raise RuntimeError(f"diagnostic response exceeded {maximum} bytes")
        return payload

    def _read_json(self, path: str, timeout: float | None = None) -> dict[str, Any]:
        if self.diagnostics_url is None:
            raise RuntimeError("TreeDB diagnostics are disabled")
        payload = self._read_bounded(
            self.diagnostics_url + path, timeout or self.diagnostics_timeout, DIAGNOSTIC_STATS_BYTES,
        )
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise RuntimeError("TreeDB diagnostics response is not an object")
        return decoded

    def start(self) -> None:
        if self.pid is not None:
            return
        self.last_shutdown_resource_end = None
        self.data_dir.mkdir(parents=True, exist_ok=True)
        address = self._listen_address(self.url, "service")
        argv = [str(self.binary), "-addr", address, "-dir", str(self.data_dir), "-profile", self.profile]
        if self.diagnostics_url is not None:
            argv.extend([
                "-pprof", self._listen_address(self.diagnostics_url, "diagnostics"),
                "-block-profile-rate", str(self.block_profile_rate),
                "-mutex-profile-fraction", str(self.mutex_profile_fraction),
            ])
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_path.open("a", encoding="utf-8", buffering=1)
        try:
            self.process = subprocess.Popen(
                argv, stdout=self.log_file, stderr=subprocess.STDOUT, text=True,
            )
            deadline, last = time.monotonic() + self.startup_timeout, ""
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"TreeDB service exited during startup; log tail: {self.log_evidence()['tail']}")
                try:
                    health = TreeDBClient(self.url, timeout=1).health()
                    ready = health.get("ok") is True and health.get("contract_version") == SERVICE_CONTRACT
                    if ready and self.diagnostics_url is not None:
                        stats = self._read_json(DIAGNOSTICS_STATS_PATH)
                        ready = stats.get("contract_version") == SERVICE_CONTRACT
                    if ready:
                        return
                    last = repr(health)
                except BaseException as exc:
                    last = repr(exc)
                time.sleep(0.05)
            raise TimeoutError(f"TreeDB service readiness exceeded {self.startup_timeout}s: {last}")
        except BaseException:
            try:
                self.stop()
            except BaseException:
                pass
            raise

    def stop(self) -> None:
        try:
            timed_out = False
            if self.process is not None:
                process = self.process
                if process.poll() is None:
                    pid = process.pid
                    latest_process = common.server_process_resource_usage(pid, "TreeDB")
                    process.terminate()
                    deadline = time.monotonic() + self.shutdown_timeout
                    exited = False
                    while not exited:
                        sample = common.server_process_resource_usage(pid, "TreeDB")
                        if sample["captured"] and (
                            not latest_process["captured"]
                            or sample["cpu_seconds"] >= latest_process["cpu_seconds"]
                        ):
                            latest_process = sample
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        try:
                            process.wait(timeout=min(0.05, remaining))
                            exited = True
                        except subprocess.TimeoutExpired:
                            exited = process.poll() is not None
                    if not exited:
                        process.kill()
                        process.wait(timeout=min(5, self.shutdown_timeout))
                        timed_out = True
                    disk_available = self.data_dir.exists()
                    self.last_shutdown_resource_end = {
                        **latest_process,
                        "captured": latest_process["captured"] and disk_available,
                        "disk_bytes": common.disk_bytes(self.data_dir),
                        "availability": {
                            **latest_process["availability"],
                            "disk_bytes": str(self.data_dir) if disk_available else "unavailable",
                        },
                    }
            if timed_out:
                raise TimeoutError(f"TreeDB graceful shutdown exceeded {self.shutdown_timeout}s")
        finally:
            self.process = None
            if self.log_file is not None:
                self.log_file.close()
                self.log_file = None

    def log_evidence(self) -> dict[str, Any]:
        tail = b""
        if self.log_path.is_file():
            with self.log_path.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                stream.seek(max(0, size - SERVICE_LOG_TAIL_BYTES))
                tail = stream.read(SERVICE_LOG_TAIL_BYTES)
        return {
            "path": str(self.log_path),
            "tail": tail.decode("utf-8", errors="replace"),
            "max_tail_bytes": SERVICE_LOG_TAIL_BYTES,
        }

    def stats_snapshot(self) -> dict[str, Any]:
        captured = time.monotonic_ns()
        if self.diagnostics_url is None:
            return {"status": "disabled", "captured_monotonic_ns": captured}
        try:
            return {
                "status": "captured", "captured_monotonic_ns": captured,
                "snapshot": self._read_json(DIAGNOSTICS_STATS_PATH),
            }
        except BaseException as exc:
            return {
                "status": "failed", "captured_monotonic_ns": captured,
                "error": f"{type(exc).__name__}: {exc}",
            }

    def capture_profiles(self, directory: Path, *, profile_seconds: int,
                         capture_timeout: float) -> dict[str, Any]:
        result: dict[str, Any] = {
            "directory": str(directory), "profile_seconds": profile_seconds,
            "capture_timeout_seconds": capture_timeout, "captures": {},
        }
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except BaseException as exc:
            error = f"{type(exc).__name__}: {exc}"
            result["captures"] = {
                name: {"status": "failed", "path": str(directory / filename), "error": error}
                for name, (_, filename) in DIAGNOSTIC_PROFILE_ENDPOINTS.items()
            }
            result["manifest"] = {"status": "failed", "path": str(directory / "capture.json"), "error": error}
            return result
        if self.diagnostics_url is None:
            error = "TreeDB diagnostics are disabled"
            result["captures"] = {
                name: {"status": "failed", "path": str(directory / filename), "error": error}
                for name, (_, filename) in DIAGNOSTIC_PROFILE_ENDPOINTS.items()
            }
        else:
            def capture(name: str, endpoint: str, filename: str) -> tuple[str, dict[str, Any]]:
                separator = "&" if "?" in endpoint else "?"
                if name in ("cpu", "trace"):
                    seconds = max(1, min(profile_seconds, max(1, int(capture_timeout) - 1)))
                    endpoint = f"{endpoint}{separator}seconds={seconds}"
                path = directory / filename
                url = self.diagnostics_url + endpoint
                try:
                    payload = self._read_bounded(url, capture_timeout, DIAGNOSTIC_CAPTURE_BYTES)
                    path.write_bytes(payload)
                    return name, {"status": "captured", "path": str(path), "bytes": len(payload), "url": url}
                except BaseException as exc:
                    return name, {
                        "status": "failed", "path": str(path), "url": url,
                        "error": f"{type(exc).__name__}: {exc}",
                    }

            with ThreadPoolExecutor(max_workers=len(DIAGNOSTIC_PROFILE_ENDPOINTS)) as pool:
                futures = [
                    pool.submit(capture, name, endpoint, filename)
                    for name, (endpoint, filename) in DIAGNOSTIC_PROFILE_ENDPOINTS.items()
                ]
                for future in as_completed(futures):
                    name, evidence = future.result()
                    result["captures"][name] = evidence
        manifest_path = directory / "capture.json"
        try:
            manifest_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            result["manifest"] = {"status": "captured", "path": str(manifest_path)}
        except BaseException as exc:
            result["manifest"] = {
                "status": "failed", "path": str(manifest_path),
                "error": f"{type(exc).__name__}: {exc}",
            }
        return result


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
        # Closing the aggregate client owns the current service stop; restart_controller
        # creates the replacement service used for reopen verification.
        self.controller.stop()


class TreeDBMinimaRunner(common.QdrantMinimaRunner):
    restart_requires_configuration_reassertion = False

    def __init__(self, manifest: dict[str, Any], *, controller: ServiceController, collection: str,
                 operation_timeout: float, ef_search: int, diagnostics_dir: Path | None = None,
                 diagnostic_slow_seconds: float = 30, diagnostic_profile_seconds: int = 5,
                 diagnostic_capture_timeout: float = 10) -> None:
        self._phase_total_start: int | None = None
        self._phase_start: int | None = None
        self._phase_name: str | None = None
        self._phase_resource_start: dict[str, Any] | None = None
        self._phase_boundaries: list[dict[str, Any]] = []
        self._phase_attribution: dict[str, Any] | None = None
        self._phase_restart_old_end: dict[str, Any] | None = None
        self._controller_restart_origin: tuple[int, str] | None = None
        self.controller = controller
        self.source_commit = repository_commit()
        self.service_binary_vcs_revision, self.service_binary_vcs_modified = \
            service_binary_build_provenance(controller.binary.resolve(), self.source_commit)
        self.runner_sha256 = file_sha256(Path(__file__).resolve())
        self.service_binary_sha256 = file_sha256(controller.binary.resolve())
        self.operation_timeout_seconds = operation_timeout
        controller.start()
        clients = ThreadLocalClients(controller.url, operation_timeout, controller)
        super().__init__(manifest, client_factory=lambda: clients, models=None, url=controller.url,
                         collection=collection, allow_drop=False, operation_timeout=int(operation_timeout),
                         optimizer_timeout=operation_timeout, poll_interval=0.05,
                         server_version=SERVICE_CONTRACT, deployment="owned_process", image="",
                         storage_path=controller.data_dir, server_pid=controller.pid,
                         restart_server=self.restart_controller, restart_identity="owned TreeDB service controller",
                         resource_server_name="TreeDB")
        self.clients, self.ef_search = clients, ef_search
        self.route_evidence: dict[str, Any] = {}
        self.diagnostics_dir = diagnostics_dir
        self.diagnostic_slow_seconds = diagnostic_slow_seconds
        self.diagnostic_profile_seconds = diagnostic_profile_seconds
        self.diagnostic_capture_timeout = diagnostic_capture_timeout
        self.batch_correlations: list[dict[str, Any]] = []
        self.diagnostic_resume: dict[str, Any] | None = None
        self._diagnostic_lock = threading.Lock()
        self._expected_rows = 0
        self._expected_insert_batches: dict[tuple[str, str, int], int] = {}
        expected_rows = 0
        self._batch_correlation_expected_identities: set[tuple[str, str, int, int]] = set()
        for operation in manifest["operations"]:
            for insertion in operation.get("insert_ranges", []):
                for start in range(insertion["start"], insertion["start"] + insertion["rows"], self.config["batch_size"]):
                    rows = min(self.config["batch_size"], insertion["start"] + insertion["rows"] - start)
                    self._batch_correlation_expected_identities.add(
                        (operation["name"], insertion["scenario"], start, rows)
                    )
                    if operation["name"] in ("initial_batch_insert", "timed_search_with_batch_insert"):
                        expected_rows += rows
                        self._expected_insert_batches[(operation["name"], insertion["scenario"], start)] = expected_rows
            if operation.get("effect") in ("insert", "update") and operation.get("documents"):
                documents = operation["documents"]
                for local_start in range(0, len(documents), self.config["batch_size"]):
                    batch = documents[local_start:local_start + self.config["batch_size"]]
                    batch_start = self._batch_start(operation["target"], batch, local_start)
                    self._batch_correlation_expected_identities.add(
                        (operation["name"], operation["target"], batch_start, len(batch))
                    )
        self._batch_correlation_max_records = len(self._batch_correlation_expected_identities)
        if diagnostics_dir is not None and controller.diagnostics_url is None:
            raise ValueError("diagnostics_dir requires an enabled controller diagnostics URL")

    def restart_controller(self) -> int:
        if self._controller_restart_origin is None:
            raise RuntimeError("TreeDB restart resource origin is unavailable")
        old_pid, old_identity = self._controller_restart_origin
        shutdown_end = self.controller.last_shutdown_resource_end
        if shutdown_end is None or not shutdown_end["captured"]:
            raise RuntimeError("TreeDB graceful shutdown resource endpoint is unavailable")
        self.restart_origin_resource_end = shutdown_end
        self._phase_restart_old_end = {
            **shutdown_end,
            "pid": old_pid,
            "process_identity": old_identity,
        }
        if self.resource_baseline is not None:
            self.completed_resource_segments.append(
                common.resource_delta(self.resource_baseline, shutdown_end)
            )
        self.controller.start()
        if self.controller.pid is None:
            raise RuntimeError("TreeDB service controller restarted without a PID")
        return self.controller.pid

    def restart_backend(self) -> None:
        super().restart_backend()
        if self.resource_baseline is None or self.restart_origin_resource_end is None:
            raise RuntimeError("TreeDB restarted resource baseline is unavailable")
        self.resource_baseline = {
            **self.resource_baseline,
            "rss_bytes": 0,
            "cpu_seconds": 0.0,
            "disk_bytes": self.restart_origin_resource_end["disk_bytes"],
        }

    def _phase_process_snapshot(self) -> dict[str, Any]:
        pid = self.controller.pid
        if type(pid) is not int or pid <= 0:
            raise RuntimeError("TreeDB phase resource snapshot requires a live service PID")
        identity = self.process_identity(pid)
        snapshot = common.server_process_resource_usage(pid, self.resource_server_name)
        snapshot["pid"] = pid
        snapshot["process_identity"] = identity
        return snapshot

    def _phase_disk_snapshot(self) -> dict[str, Any]:
        available = self.storage_path is not None and self.storage_path.exists()
        return {
            "captured": available,
            "disk_bytes": common.disk_bytes(self.storage_path),
            "availability": {
                "disk_bytes": str(self.storage_path) if available else "unavailable",
            },
        }

    @staticmethod
    def _phase_resource_endpoint(process: dict[str, Any], disk: dict[str, Any]) -> dict[str, Any]:
        return {
            **process,
            "captured": process["captured"] and disk["captured"],
            "disk_bytes": disk["disk_bytes"],
            "availability": {
                **process["availability"],
                **disk["availability"],
            },
        }
    def capture_restart_origin(self) -> None:
        old_pid = self.server_pid
        if type(old_pid) is not int or old_pid <= 0:
            raise RuntimeError("close/reopen requires the original TreeDB server PID")
        origin = (old_pid, self.process_identity(old_pid))
        self.restart_origin = origin
        self._controller_restart_origin = origin
        self.restart_origin_resource_end = None
        self._phase_restart_old_end = None

    def begin_phase_attribution(self) -> None:
        if self._phase_start is not None:
            raise RuntimeError("TreeDB phase attribution already started")
        disk = self._phase_disk_snapshot()
        process = self._phase_process_snapshot()
        self._phase_resource_start = self._phase_resource_endpoint(process, disk)
        phase_start = time.monotonic_ns()
        self._phase_total_start = phase_start
        self._phase_start = phase_start
        self._phase_name = "initial_durable_load"

    def phase_transition(self, name: str) -> None:
        if name not in PHASE_CLASSIFICATIONS:
            raise RuntimeError(f"unknown TreeDB attribution phase {name!r}")
        if self._phase_start is None or self._phase_name is None or self._phase_resource_start is None:
            raise RuntimeError("TreeDB phase attribution was not started")
        phase_end = time.monotonic_ns()
        end_process = self._phase_process_snapshot()
        disk = self._phase_disk_snapshot()
        resource_end = self._phase_resource_endpoint(end_process, disk)
        next_process = self._phase_process_snapshot()
        next_resource_start = self._phase_resource_endpoint(next_process, disk)
        next_start = time.monotonic_ns()
        resource_segments = [{"start": self._phase_resource_start, "end": resource_end}]
        if self._phase_name == "restart_open_readiness":
            old_end = self._phase_restart_old_end
            if old_end is None:
                raise RuntimeError("TreeDB restart phase is missing the old-process resource endpoint")
            new_start = {
                **resource_end,
                "rss_bytes": 0,
                "cpu_seconds": 0.0,
                "disk_bytes": old_end["disk_bytes"],
            }
            resource_segments = [
                {"start": self._phase_resource_start, "end": old_end},
                {"start": new_start, "end": resource_end},
            ]
        self._phase_boundaries.append({
            "name": self._phase_name,
            "classification": PHASE_CLASSIFICATIONS[self._phase_name],
            "start_nanos": self._phase_start,
            "end_nanos": phase_end,
            "duration_nanos": phase_end - self._phase_start,
            "resource_segments": resource_segments,
        })
        self._phase_name, self._phase_start = name, next_start
        self._phase_resource_start = next_resource_start

    def _finish_phase_attribution(self) -> dict[str, Any]:
        if self._phase_attribution is not None:
            return self._phase_attribution
        if self._phase_start is None:
            self.begin_phase_attribution()
        assert self._phase_name is not None and self._phase_resource_start is not None
        assert self._phase_total_start is not None
        phase_end = time.monotonic_ns()
        boundary = {
            "name": self._phase_name,
            "classification": PHASE_CLASSIFICATIONS[self._phase_name],
            "start_nanos": self._phase_start,
            "end_nanos": phase_end,
            "duration_nanos": phase_end - self._phase_start,
        }
        shutdown_end = getattr(self.controller, "last_shutdown_resource_end", None)
        incomplete_reason = ""
        if self.controller.pid is None:
            endpoint = self._phase_resource_start
            incomplete_reason = "service_unavailable_before_phase_endpoint"
            if self._phase_name == "restart_open_readiness":
                if self._phase_restart_old_end is not None:
                    endpoint = self._phase_restart_old_end
                    incomplete_reason = "replacement_service_unavailable_after_shutdown"
                elif shutdown_end is not None and self._controller_restart_origin is not None:
                    old_pid, old_identity = self._controller_restart_origin
                    endpoint = {**shutdown_end, "pid": old_pid, "process_identity": old_identity}
                    self._phase_restart_old_end = endpoint
                    incomplete_reason = "graceful_shutdown_failed_before_reopen"
        else:
            try:
                end_process = self._phase_process_snapshot()
                disk = self._phase_disk_snapshot()
                endpoint = self._phase_resource_endpoint(end_process, disk)
                if not endpoint["captured"]:
                    incomplete_reason = "resource_endpoint_unavailable"
            except BaseException:
                endpoint = self._phase_resource_start
                incomplete_reason = "resource_endpoint_unavailable"
        resource_segments = [{
            "start": self._phase_resource_start,
            "end": endpoint,
        }]
        if self._phase_name == "restart_open_readiness" and self._phase_restart_old_end is not None:
            old_end = self._phase_restart_old_end
            if (
                endpoint.get("pid"),
                endpoint.get("process_identity"),
            ) != (old_end["pid"], old_end["process_identity"]):
                new_start = {
                    **endpoint,
                    "rss_bytes": 0,
                    "cpu_seconds": 0.0,
                    "disk_bytes": old_end["disk_bytes"],
                }
                resource_segments = [
                    {"start": self._phase_resource_start, "end": old_end},
                    {"start": new_start, "end": endpoint},
                ]
            if not incomplete_reason:
                incomplete_reason = "restart_verification_failed_after_reopen"
        boundary["resource_segments"] = resource_segments
        if incomplete_reason:
            boundary["resource_evidence_complete"] = False
            boundary["incomplete_reason"] = incomplete_reason
        end = time.monotonic_ns()
        self._phase_boundaries.append(boundary)
        for phase in self._phase_boundaries:
            samples = [
                sample for sample in self.evidence.samples
                if sample["start_nanos"] >= phase["start_nanos"] and sample["end_nanos"] <= phase["end_nanos"]
            ]
            phase["sample_count"] = len(samples)
            phase["sample_duration_nanos"] = sum(sample["duration_nanos"] for sample in samples)
        total = end - self._phase_total_start
        attributed = sum(phase["duration_nanos"] for phase in self._phase_boundaries)
        self._phase_attribution = {
            "clock": "time.monotonic_ns",
            "total_start_nanos": self._phase_total_start,
            "total_end_nanos": end,
            "total_duration_nanos": total,
            "unattributed_nanos": total - attributed,
            "unattributed_rule": PHASE_UNATTRIBUTED_RULE,
            "phases": self._phase_boundaries,
        }
        return self._phase_attribution


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

    def initial_load_to_query_boundary(self) -> None:
        pass

    def wait_ready(self, expected_count: int | None = None,
                   phase: str = "mutation_visibility") -> None:
        assert self.client is not None
        if expected_count is not None:
            count = self.client.count_documents(self.collection).count
            if count != expected_count:
                raise RuntimeError(f"TreeDB visible document count={count}, expected={expected_count}")

    def _batch_start(self, scenario: str, documents: list[dict[str, Any]], local_start: int) -> int:
        prefix = f"minima/{scenario}/"
        try:
            ordinals = [int(row["id"].removeprefix(prefix)) for row in documents]
        except (KeyError, TypeError, ValueError):
            return local_start
        if all(row["id"].startswith(prefix) for row in documents) and ordinals == list(range(ordinals[0], ordinals[0] + len(ordinals))):
            return ordinals[0]
        return local_start

    def _capture_directory(self, correlation: dict[str, Any]) -> Path:
        assert self.diagnostics_dir is not None
        label = "_".join(
            "".join(character if character.isalnum() or character in "-_" else "_" for character in str(value))
            for value in (
                f"{correlation['sequence']:06d}", correlation["operation"], correlation["scenario"],
                f"batch_{correlation['batch_ordinal']:06d}", f"start_{correlation['batch_start']}",
            )
        )
        return self.diagnostics_dir / label

    @staticmethod
    def _timeout_failure(exc: BaseException) -> bool:
        return isinstance(exc, TimeoutError) or "timed out" in str(exc).lower() or "timeout" in type(exc).__name__.lower()

    def _public_count_snapshot(self) -> dict[str, Any]:
        assert self.client is not None
        try:
            return {"status": "captured", "rows": self.client.count_documents(self.collection).count}
        except BaseException as exc:
            return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}

    @staticmethod
    def _compact_completed_batch_correlation(correlation: dict[str, Any]) -> None:
        if correlation.get("capture_reason"):
            raise RuntimeError("captured TreeDB batch correlation cannot be compacted")
        correlation.pop("before_stats", None)
        correlation.pop("after_stats", None)
        correlation["stats_retention"] = "compact_completed"
        encoded = json.dumps(correlation, indent=2, sort_keys=True, allow_nan=False)
        encoded_bytes = len(encoded.replace("\n", "\n        ").encode())
        if encoded_bytes > COMPACT_BATCH_CORRELATION_MAX_BYTES:
            raise RuntimeError(
                f"compact TreeDB batch correlation exceeds {COMPACT_BATCH_CORRELATION_MAX_BYTES} bytes"
            )

    def _batch_correlation_contract(self) -> dict[str, Any]:
        if len(self.batch_correlations) > self._batch_correlation_max_records:
            raise RuntimeError("TreeDB batch correlation count exceeds the frozen manifest bound")
        completed = getattr(self, "operations", {}).get("manifest_ordered", False)
        if completed:
            expected_records = self._batch_correlation_max_records if self.diagnostics_dir is not None else 0
            if len(self.batch_correlations) != expected_records:
                raise RuntimeError(
                    "completed TreeDB batch correlation count does not match the diagnostics contract"
                )
        observed_sequences: set[int] = set()
        observed_identities: set[tuple[str, str, int, int]] = set()
        compact_records = 0
        full_records = 0
        for correlation in self.batch_correlations:
            sequence = correlation.get("sequence")
            identity = (
                correlation.get("operation"),
                correlation.get("scenario"),
                correlation.get("batch_start"),
                correlation.get("rows"),
            )
            if not isinstance(sequence, int) or sequence in observed_sequences or \
                    not isinstance(identity[0], str) or not identity[0] or \
                    not isinstance(identity[1], str) or not identity[1] or \
                    not isinstance(identity[2], int) or not isinstance(identity[3], int) or identity[3] <= 0 or \
                    identity in observed_identities:
                raise RuntimeError("TreeDB batch correlation identity/cardinality is invalid")
            observed_sequences.add(sequence)
            observed_identities.add(identity)
            outcome = correlation.get("outcome")
            if completed and outcome != "completed":
                raise RuntimeError("completed TreeDB batch correlations contain a failed outcome")
            if correlation.get("stats_retention") == "compact_completed":
                if correlation.get("outcome") != "completed" or correlation.get("capture_reason") or \
                        correlation.get("profile_capture", {}).get("status") != "not_triggered" or \
                        "before_stats" in correlation or "after_stats" in correlation:
                    raise RuntimeError("compact TreeDB batch correlation retained diagnostic evidence")
                self._compact_completed_batch_correlation(correlation)
                compact_records += 1
                continue
            profile_status = correlation.get("profile_capture", {}).get("status")
            diagnostic = correlation.get("capture_reason") in ("slow", "failed", "timeout") and \
                profile_status in ("captured", "failed", "in_progress")
            capture_reason = correlation.get("capture_reason")
            if capture_reason == "failed" and outcome != "failed" or \
                    capture_reason == "timeout" and outcome != "timeout":
                raise RuntimeError("TreeDB batch correlation outcome and capture reason disagree")
            if correlation.get("stats_retention") != "full_diagnostic" or not diagnostic or \
                    not isinstance(correlation.get("before_stats"), dict) or \
                    not isinstance(correlation.get("after_stats"), dict):
                raise RuntimeError("TreeDB diagnostic batch correlation stats retention is invalid")
            full_records += 1
        if completed and self.diagnostics_dir is not None:
            if observed_sequences != set(range(self._batch_correlation_max_records)) or \
                    observed_identities != self._batch_correlation_expected_identities:
                raise RuntimeError("completed TreeDB batch correlations do not cover the frozen manifest")
        return {
            "schema": BATCH_CORRELATION_SCHEMA,
            "record_count": len(self.batch_correlations),
            "maximum_record_count": self._batch_correlation_max_records,
            "compact_completed_records": compact_records,
            "full_diagnostic_records": full_records,
            "compact_record_max_bytes": COMPACT_BATCH_CORRELATION_MAX_BYTES,
            "full_stats_retention": ["failed", "timeout", "slow", "profile_captured"],
        }

    def upsert(self, operation: str, scenario: str, documents: list[dict[str, Any]], wait_ready: bool = True,
               on_writer_start: Callable[[], None] | None = None) -> None:
        assert self.client is not None
        batch_size = self.config["batch_size"]
        for local_start in range(0, len(documents), batch_size):
            source_batch = documents[local_start:local_start + batch_size]
            batch = [service_document(row) for row in source_batch]
            if self.diagnostics_dir is None:
                response = self.evidence.call(
                    operation, "writer", scenario,
                    lambda batch=batch: self.client.upsert_documents(
                        self.collection, batch, defer_vector_index_rebuild=True,
                    ),
                    on_start=on_writer_start,
                )
                if response.upserted != len(batch) or response.ids != [row["id"] for row in batch]:
                    raise RuntimeError("TreeDB upsert completion did not cover the submitted batch")
                continue
            batch_start = self._batch_start(scenario, source_batch, local_start)
            key = (operation, scenario, batch_start)
            mapped_expected = key in self._expected_insert_batches
            before_public_count = None if mapped_expected else self._public_count_snapshot()
            expected_before = (
                self._expected_rows if mapped_expected
                else before_public_count.get("rows") if before_public_count is not None else None
            )
            expected_after = self._expected_insert_batches.get(key)
            with self._diagnostic_lock:
                sequence = len(self.batch_correlations)
                correlation: dict[str, Any] = {
                    "sequence": sequence, "operation": operation, "scenario": scenario,
                    "batch_ordinal": batch_start // batch_size, "batch_start": batch_start,
                    "rows": len(batch), "accumulated_expected_rows_before": expected_before,
                    "accumulated_expected_rows": expected_after,
                    "accumulated_rows_source": "frozen_manifest" if mapped_expected else "public_count",
                    "before_stats": self.controller.stats_snapshot(),
                    "outcome": "in_progress", "profile_capture": {"status": "not_triggered"},
                }
                if before_public_count is not None:
                    correlation["before_public_count"] = before_public_count
                self.batch_correlations.append(correlation)
            done = threading.Event()
            capture_started = threading.Event()
            capture_lock = threading.Lock()
            started_monotonic_ns = time.monotonic_ns()
            correlation["started_monotonic_ns"] = started_monotonic_ns

            def capture(reason: str, *, correlation: dict[str, Any] = correlation,
                        capture_started: threading.Event = capture_started,
                        capture_lock: threading.Lock = capture_lock) -> None:
                with capture_lock:
                    if capture_started.is_set():
                        return
                    capture_started.set()
                correlation["capture_reason"] = reason
                directory = self._capture_directory(correlation)
                correlation["profile_capture"] = {"status": "in_progress", "directory": str(directory)}
                try:
                    profile_capture = self.controller.capture_profiles(
                        directory, profile_seconds=self.diagnostic_profile_seconds,
                        capture_timeout=self.diagnostic_capture_timeout,
                    )
                    if profile_capture.get("status") not in ("captured", "failed"):
                        capture_statuses = {
                            evidence.get("status")
                            for evidence in profile_capture.get("captures", {}).values()
                        }
                        profile_capture["status"] = "captured" if "captured" in capture_statuses else "failed"
                    correlation["profile_capture"] = profile_capture
                except BaseException as exc:
                    correlation["profile_capture"] = {
                        "status": "failed", "directory": str(directory),
                        "error": f"{type(exc).__name__}: {exc}",
                    }

            def slow_capture(*, done: threading.Event = done,
                             capture_batch: Callable[[str], None] = capture,
                             started_ns: int = started_monotonic_ns) -> None:
                deadline_ns = started_ns + int(self.diagnostic_slow_seconds * 1e9)
                remaining_seconds = max(0.0, (deadline_ns - time.monotonic_ns()) / 1e9)
                if not done.wait(remaining_seconds):
                    capture_batch("slow")

            watcher: threading.Thread | None = None
            if self.diagnostics_dir is not None:
                watcher = threading.Thread(target=slow_capture, name=f"treedb-batch-diagnostic-{sequence}", daemon=True)
                watcher.start()
            response: Any | None = None
            failure: BaseException | None = None
            try:
                response = self.evidence.call(
                    operation, "writer", scenario,
                    lambda batch=batch: self.client.upsert_documents(
                        self.collection, batch, defer_vector_index_rebuild=True,
                    ),
                    on_start=on_writer_start,
                )
                if response.upserted != len(batch) or response.ids != [row["id"] for row in batch]:
                    raise RuntimeError("TreeDB upsert completion did not cover the submitted batch")
                if mapped_expected:
                    with self._diagnostic_lock:
                        self._expected_rows = expected_after
                correlation["outcome"] = "completed"
            except BaseException as exc:
                failure = exc
                correlation["outcome"] = "timeout" if self._timeout_failure(exc) else "failed"
                correlation["error"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                correlation["ended_monotonic_ns"] = time.monotonic_ns()
                correlation["duration_nanos"] = correlation["ended_monotonic_ns"] - correlation["started_monotonic_ns"]
                done.set()
                correlation["after_stats"] = self.controller.stats_snapshot()
                if not mapped_expected:
                    after_public_count = self._public_count_snapshot()
                    correlation["after_public_count"] = after_public_count
                    correlation["accumulated_expected_rows"] = after_public_count.get("rows")
                    if after_public_count["status"] == "captured":
                        with self._diagnostic_lock:
                            self._expected_rows = after_public_count["rows"]
                if failure is not None:
                    capture(correlation["outcome"])
                elif correlation["duration_nanos"] >= int(self.diagnostic_slow_seconds * 1e9):
                    capture("slow")
                if watcher is not None:
                    watcher.join(self.diagnostic_capture_timeout + self.diagnostic_profile_seconds + 1)
                    if watcher.is_alive():
                        correlation["capture_wait"] = {
                            "status": "failed",
                            "error": "diagnostic capture exceeded its bounded join interval",
                        }
                retain_full_stats = (
                    correlation["outcome"] != "completed" or
                    capture_started.is_set() or
                    correlation["profile_capture"].get("status") != "not_triggered"
                )
                if retain_full_stats:
                    correlation["stats_retention"] = "full_diagnostic"
                else:
                    self._compact_completed_batch_correlation(correlation)

    def _preflight_batch_ids(self, ids: list[str]) -> list[str]:
        assert self.client is not None
        present = []
        for identifier in ids:
            result = self.client.filter_documents(
                self.collection, {"field": "id", "operator": "==", "value": identifier}, limit=1,
            )
            if result.matched_count not in (0, 1) or len(result.documents) != result.matched_count:
                raise RuntimeError(f"diagnostic resume ID preflight was ambiguous for {identifier!r}")
            if result.documents:
                if result.documents[0].id != identifier:
                    raise RuntimeError(f"diagnostic resume ID preflight returned the wrong document for {identifier!r}")
                present.append(identifier)
        return present
    def _initial_prefix_identity(self, operation: dict[str, Any], insertion: dict[str, Any],
                                 start: int, expected_before: int) -> dict[str, Any]:
        expected = common.StateAccumulator()
        found = False
        for row in operation.get("insert_ranges", []):
            end = row["start"] + row["rows"]
            if row is insertion:
                end = start
                found = True
            spec = self.specs[row["scenario"]]
            for ordinal in range(row["start"], end):
                expected.add(common.generated_document(spec, ordinal))
            if found:
                break
        if not found or expected.count != expected_before:
            raise RuntimeError(
                f"diagnostic resume prefix construction produced {expected.count} rows, expected {expected_before}",
            )

        assert self.client is not None
        actual = common.StateAccumulator()
        after_id: str | None = None
        while True:
            result = self.client.filter_documents(
                self.collection, limit=1024, after_id=after_id, cursor_page=True,
            )
            if result.matched_count != len(result.documents) or len(result.documents) > 1024:
                raise RuntimeError("diagnostic resume prefix stream returned an ambiguous page")
            for document in result.documents:
                actual.add({
                    "id": document.id, "content": document.content,
                    "user_id": document.meta.get("user_id"), "fpath": document.meta.get("fpath"),
                })
            if result.exhausted:
                break
            if not result.next_after_id or result.next_after_id == after_id:
                raise RuntimeError("diagnostic resume prefix stream cursor did not advance")
            after_id = result.next_after_id
        expected_digest, actual_digest = expected.hexdigest(), actual.hexdigest()
        return {
            "algorithm": "public cursor stream plus minima-committed-payload-v1",
            "expected_rows": expected.count, "actual_rows": actual.count,
            "expected_digest": expected_digest, "actual_digest": actual_digest,
            "match": expected.count == actual.count and expected_digest == actual_digest,
        }


    def run_diagnostic_resume(self, scenario: str, start: int) -> None:
        self.diagnostic_resume = {
            "enabled": True, "nonqualifying": True, "operation": "initial_batch_insert",
            "scenario": scenario, "batch_start": start, "state": "preflight",
        }
        try:
            self.evidence.failures.append("diagnostic exact-batch resume is nonqualifying evidence")
            self._run_diagnostic_resume(scenario, start)
        except BaseException as exc:
            state = self.diagnostic_resume["state"]
            if not state.startswith("rejected_"):
                self.diagnostic_resume["failure_phase"] = state
                self.diagnostic_resume["state"] = "failed"
            self.diagnostic_resume["error"] = f"{type(exc).__name__}: {exc}"
            raise

    def _run_diagnostic_resume(self, scenario: str, start: int) -> None:
        self.resource_baseline = common.server_resource_usage(
            self.controller.pid, self.storage_path, self.resource_server_name,
        )
        self.connect()
        self.ensure_compatible()
        batch_size = self.config["batch_size"]
        if batch_size != 256:
            raise RuntimeError(f"diagnostic exact resume requires the frozen 256-document batch size, got {batch_size}")
        operation = next(row for row in self.manifest["operations"] if row["name"] == "initial_batch_insert")
        ranges = [
            row for row in operation.get("insert_ranges", [])
            if row["scenario"] == scenario and row["start"] <= start < row["start"] + row["rows"]
        ]
        if len(ranges) != 1:
            raise RuntimeError("diagnostic resume selector does not identify exactly one initial insert range")
        insertion = ranges[0]
        if (start - insertion["start"]) % batch_size or start + batch_size > insertion["start"] + insertion["rows"]:
            raise RuntimeError("diagnostic resume selector is not an exact full frozen batch")
        expected_after = self._expected_insert_batches.get(("initial_batch_insert", scenario, start))
        if expected_after is None:
            raise RuntimeError("diagnostic resume selector is not in the frozen initial insertion stream")
        expected_before = expected_after - batch_size
        spec = self.specs[scenario]
        documents = [common.generated_document(spec, ordinal) for ordinal in range(start, start + batch_size)]
        ids = [row["id"] for row in documents]
        try:
            present = self._preflight_batch_ids(ids)
            self.diagnostic_resume["present_ids"] = len(present)
            if len(present) == batch_size:
                self.diagnostic_resume["state"] = "rejected_all_present"
                raise RuntimeError("diagnostic resume rejected: selected batch is all-present")
            if present:
                self.diagnostic_resume["state"] = "rejected_mixed"
                raise RuntimeError(f"diagnostic resume rejected: selected batch is mixed ({len(present)}/{batch_size} present)")
            visible_rows = self.client.count_documents(self.collection).count
            self.diagnostic_resume["visible_rows_before"] = visible_rows
            self.diagnostic_resume["expected_rows_before"] = expected_before
            if visible_rows != expected_before:
                self.diagnostic_resume["state"] = "rejected_ambiguous_count"
                raise RuntimeError(
                    f"diagnostic resume rejected: visible rows={visible_rows}, expected first missing boundary={expected_before}",
                )
            prefix_identity = self._initial_prefix_identity(operation, insertion, start, expected_before)
            self.diagnostic_resume["prefix_identity"] = prefix_identity
            if not prefix_identity["match"]:
                self.diagnostic_resume["state"] = "rejected_prefix_mismatch"
                raise RuntimeError(
                    "diagnostic resume rejected: existing public collection does not match the exact initial prefix",
                )
            self._expected_rows = expected_before
            self.diagnostic_resume["state"] = "submitting"
            self.upsert("initial_batch_insert", scenario, documents)
            present_after = self._preflight_batch_ids(ids)
            visible_after = self.client.count_documents(self.collection).count
            self.diagnostic_resume.update({
                "present_ids_after": len(present_after), "visible_rows_after": visible_after,
                "expected_rows_after": expected_after,
            })
            if len(present_after) != batch_size or visible_after != expected_after:
                self.diagnostic_resume["state"] = "failed_postflight"
                raise RuntimeError("diagnostic resume batch did not establish the exact expected public state")
            self.diagnostic_resume["state"] = "completed"
        except BaseException as exc:
            self.diagnostic_resume.setdefault("state", "failed")
            self.diagnostic_resume["error"] = f"{type(exc).__name__}: {exc}"
            raise
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

    def delete_filter(self, operation: dict[str, Any],
                      on_writer_start: Callable[[], None] | None = None) -> None:
        assert self.client is not None
        name, scenario = operation["name"], operation["target"]
        filt = scalar_filter({**self.specs[scenario], **operation["filter"]})
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete_by_filter(
            self.collection, filt), on_start=on_writer_start)
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
                self.collection, limit=STATE_SCROLL_PAGE_SIZE, return_embedding=True,
                after_id=after_id, cursor_page=True,
            )
            if result.matched_count != len(result.documents) or len(result.documents) > STATE_SCROLL_PAGE_SIZE:
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
        self.resource_baseline = common.server_resource_usage(
            self.controller.pid, self.storage_path, self.resource_server_name)
        self.connect()
        self.create_owned_collection()
        self.begin_phase_attribution()
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
        self.capture_restart_origin()
        self.client.close()
        self.client = None
        self.restart_backend()
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
        resource = self.resource_evidence()
        backend = artifact["backends"][0]
        environment = {
            "os": platform.system() + " " + platform.release(),
            "arch": platform.machine() or "unavailable",
            "cpu": platform.processor() or "unavailable",
            "memory": common.memory_bytes(),
            "python": platform.python_version(),
            "host": platform.node() or "unavailable",
        }
        backend.update({
            "name": "treedb", "server_version": SERVICE_CONTRACT, "client_version": CLIENT_VERSION,
            "durability": f"TreeDB {self.controller.profile}; owned service restart on the same data directory",
            "configuration": {"url": self.url, "collection": self.collection, "dimension": str(self.config["dimension"]),
                              "metric": self.config["metric"], "scalar_fields": "meta.user_id,meta.fpath",
                              "vector_strategy": "native_runtime", "ef_search": str(self.ef_search),
                              "profile": self.controller.profile, "service_binary": str(self.controller.binary),
                              "service_binary_sha256": self.service_binary_sha256,
                              "service_binary_vcs_revision": self.service_binary_vcs_revision,
                              "service_binary_vcs_modified": self.service_binary_vcs_modified,
                              "runner_sha256": self.runner_sha256,
                              "product_commit": self.source_commit, "harness_commit": self.source_commit,
                              "operation_timeout_seconds": str(self.operation_timeout_seconds),
                              "startup_reopen_timeout_seconds": str(self.controller.startup_timeout),
                              "shutdown_timeout_seconds": str(self.controller.shutdown_timeout),
                              "service_log_path": str(self.controller.log_path),
                              "diagnostics_url": self.controller.diagnostics_url or "disabled",
                              "block_profile_rate": str(self.controller.block_profile_rate) if self.controller.diagnostics_url else "0",
                              "mutex_profile_fraction": str(self.controller.mutex_profile_fraction) if self.controller.diagnostics_url else "0",
                              "effective_collection": json.dumps(
                                  self.effective_collection, sort_keys=True, separators=(",", ":"))},
            "environment": environment,
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
                "probe_ids": route.scalar_filter_probe_ids, "candidate_ids": route.scalar_filter_candidate_ids,
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
        raw.pop("collection_configuration_transition", None)
        raw.pop("readiness", None)
        raw["native_route_responses"] = {
            scenario: {"membership_source": value.scalar_filter_membership_source,
                       "plan": value.scalar_filter_plan, "probe_ids": value.scalar_filter_probe_ids,
                       "candidates": value.scalar_filter_candidates,
                       "candidate_ids": value.scalar_filter_candidate_ids,
                       "retained": value.scalar_filter_retained_candidate_ids,
                       "refined": value.scalar_filter_refined_candidate_ids,
                       "visited": value.scalar_filter_visited, "scored": value.scalar_filter_scored,
                       "admitted": value.scalar_filter_admitted,
                       "visibility_mismatches": value.visibility_mismatch_count,
                       "visibility_retries": value.visibility_retry_count}
            for scenario, value in self.route_evidence.items()
        }
        raw["resource_measurement"] = resource
        raw["service_log"] = self.controller.log_evidence()
        raw["upsert_batch_correlations"] = self.batch_correlations
        raw["upsert_batch_correlation_contract"] = self._batch_correlation_contract()
        raw["diagnostic_resume"] = self.diagnostic_resume
        raw["diagnostics"] = {
            "enabled": self.diagnostics_dir is not None,
            "directory": str(self.diagnostics_dir) if self.diagnostics_dir is not None else None,
            "slow_batch_seconds": self.diagnostic_slow_seconds,
            "profile_seconds": self.diagnostic_profile_seconds,
            "capture_timeout_seconds": self.diagnostic_capture_timeout,
            "nonqualifying": self.diagnostic_resume is not None,
        }
        raw["resource_availability"] = {
            "measurement": common.RESOURCE_SEMANTICS,
            "baseline": resource["baseline"]["availability"],
            "end": resource["end"]["availability"],
        }
        # Include artifact construction and a representative full encoding in the
        # qualification-only final phase; the final write and Go validator are outside the runner span.
        json.dumps(artifact, sort_keys=True, allow_nan=False)
        raw["phase_attribution"] = self._finish_phase_attribution()
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
    parser.add_argument("--diagnostics-dir", type=Path)
    parser.add_argument("--diagnostics-url", default="http://127.0.0.1:17121")
    parser.add_argument("--diagnostic-slow-seconds", type=float, default=30)
    parser.add_argument("--diagnostic-profile-seconds", type=int, default=5)
    parser.add_argument("--diagnostic-capture-timeout", type=float, default=10)
    parser.add_argument("--diagnostic-resume-scenario")
    parser.add_argument("--diagnostic-resume-start", type=int)
    args = parser.parse_args()
    resume_flags = (args.diagnostic_resume_scenario is not None, args.diagnostic_resume_start is not None)
    if any(resume_flags) and not all(resume_flags):
        parser.error("diagnostic resume requires both --diagnostic-resume-scenario and --diagnostic-resume-start")
    if any(resume_flags) and args.diagnostics_dir is None:
        parser.error("diagnostic resume requires --diagnostics-dir")
    if any(resume_flags) and args.small:
        parser.error("--small and diagnostic resume are mutually exclusive")
    if args.diagnostic_slow_seconds <= 0 or args.diagnostic_profile_seconds <= 0 or args.diagnostic_capture_timeout <= 0:
        parser.error("diagnostic durations must be positive")
    return args


def main() -> int:
    args = parse_args()
    manifest = common.load_manifest(args.manifest)
    diagnostics_dir = args.diagnostics_dir.resolve() if args.diagnostics_dir is not None else None
    controller = ServiceController(
        args.service_bin.resolve(), args.url, args.data_dir.resolve(), args.profile,
        args.startup_timeout, args.operation_timeout,
        diagnostics_url=args.diagnostics_url if diagnostics_dir is not None else None,
    )
    runner = TreeDBMinimaRunner(
        manifest, controller=controller, collection=args.collection,
        operation_timeout=args.operation_timeout, ef_search=args.ef_search,
        diagnostics_dir=diagnostics_dir, diagnostic_slow_seconds=args.diagnostic_slow_seconds,
        diagnostic_profile_seconds=args.diagnostic_profile_seconds,
        diagnostic_capture_timeout=args.diagnostic_capture_timeout,
    )
    exit_code = 0
    try:
        if args.diagnostic_resume_scenario is not None:
            runner.run_diagnostic_resume(args.diagnostic_resume_scenario, args.diagnostic_resume_start)
        elif args.small:
            runner.run_small()
        else:
            runner.run()
    except BaseException as exc:
        runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        artifact_error: BaseException | None = None
        try:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(runner.artifact(), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        except BaseException as exc:
            artifact_error = exc
        cleanup_error: BaseException | None = None
        try:
            runner.close()
        except BaseException as exc:
            cleanup_error = exc
        try:
            controller.stop()
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
        if artifact_error is not None:
            raise artifact_error
        if cleanup_error is not None:
            raise cleanup_error
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
