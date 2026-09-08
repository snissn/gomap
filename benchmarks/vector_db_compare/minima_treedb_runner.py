#!/usr/bin/env python3
"""Execute the Minima operation manifest through TreeDB's ordinary public client."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import subprocess
import signal
import sys
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
                 mutex_profile_fraction: int = 1, diagnostics_timeout: float = 2,
                 native_address: str | None = None, measured: bool = False) -> None:
        self.binary, self.url, self.data_dir, self.profile = binary, url.rstrip("/"), data_dir, profile
        self.startup_timeout, self.shutdown_timeout = startup_timeout, shutdown_timeout
        self.diagnostics_url = diagnostics_url.rstrip("/") if diagnostics_url else None
        self.block_profile_rate = block_profile_rate
        self.mutex_profile_fraction = mutex_profile_fraction
        self.diagnostics_timeout = diagnostics_timeout
        self.native_address = native_address
        self.measured = measured
        self.lifetimes: list[dict[str, Any]] = []
        self._log_region_start = 0
        self._owned_identity = ""
        self.process: subprocess.Popen[str] | None = None
        self.log_path = data_dir.parent / "treedb-document-service.log"
        self.log_file: Any | None = None
        self.last_shutdown_resource_end: dict[str, Any] | None = None

    @property
    def pid(self) -> int | None:
        if self.process is None:
            return None
        running = self._reap_owned() is None if self.measured else self.process.poll() is None
        return self.process.pid if running else None

    def _reap_owned(self) -> int | None:
        process = self.process
        if process is None or process.returncode is not None:
            return None if process is None else process.returncode
        lifetime = self.lifetimes[-1]
        try:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid == 0:
                return None
            if pid != process.pid:
                raise RuntimeError("wait4 returned an unowned child")
            code = os.waitstatus_to_exitcode(status)
            process.returncode = code  # Popen must never reap this child itself.
            if type(usage.ru_maxrss) is not int or not 0 < usage.ru_maxrss <= ((1 << 63) - 1) // 1024:
                raise RuntimeError("wait4 peak RSS is unavailable or out of range")
            lifetime["exit"] = {
                "availability": "measured", "pid": pid,
                "linux_process_identity": self._owned_identity,
                "observed_monotonic_ns": time.monotonic_ns(), "exit_code": code,
                "peak_rss_bytes": int(usage.ru_maxrss) * 1024,
                "source": "linux_wait4_ru_maxrss_kib_times_1024",
                "scope": "owned_process_start_through_exit"}
            return code
        except (ChildProcessError, OSError, RuntimeError, AttributeError, TypeError, ValueError) as exc:
            lifetime["exit"].update(availability="unavailable", reason=f"{type(exc).__name__}: {exc}")
            # Ownership is lost; never substitute aggregate child usage or allow
            # Popen to silently reap a different observation on destruction.
            process.returncode = 255
            return 255

    def work_snapshot(self) -> dict[str, Any]:
        start = time.monotonic_ns()
        result: dict[str, Any] = {"availability": "unavailable", "started_monotonic_ns": start}
        try:
            pid = self.pid
            before = common.linux_process_identity(pid)
            if not before or before != self._owned_identity:
                raise RuntimeError("owned Linux identity unavailable before work read")
            work = self._read_json(DIAGNOSTICS_STATS_PATH)["work"]
            after = common.linux_process_identity(pid)
            if (after != before or work.get("pid") != pid
                    or work.get("schema_version") != "treedb-work-v1"
                    or work.get("scope") != "process" or work.get("origin_kind") != "go_package_init"):
                raise RuntimeError("work snapshot identity/scope changed")
            result.update(availability="measured", work=work)
        except BaseException as exc:
            result["reason"] = f"{type(exc).__name__}: {exc}"
        result["ended_monotonic_ns"] = time.monotonic_ns()
        if self.lifetimes:
            self.lifetimes[-1]["last_live_work"] = result
        return result

    def _terminal_work(self) -> None:
        lifetime = self.lifetimes[-1]
        record = None
        try:
            with self.log_path.open("rb") as stream:
                stream.seek(self._log_region_start)
                # Bound every read, including ordinary lines, before allocation.
                while line := stream.readline(DIAGNOSTIC_STATS_BYTES + 1):
                    if len(line) > DIAGNOSTIC_STATS_BYTES:
                        raise RuntimeError("owned service log line exceeds bound")
                    if b"treedb_document_service_terminal_work" not in line:
                        continue
                    row = json.loads(line[line.index(b"{"):])
                    if row.get("event") == "treedb_document_service_terminal_work":
                        if record is not None:
                            raise RuntimeError("expected one owned terminal work record, got 2")
                        record = {key: row[key] for key in (
                            "event", "version", "contract_version", "cleanup_completed", "shutdown_failures", "work")}
                        lifetime["terminal_work"] = record
            if record is None:
                raise RuntimeError("expected one owned terminal work record, got 0")
        except BaseException as exc:
            lifetime["terminal_error"] = f"{type(exc).__name__}: {exc}"

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
        if self.measured and (sys.platform != "linux" or not hasattr(os, "wait4")):
            raise RuntimeError("measured ownership requires Linux wait4")
        self.last_shutdown_resource_end = None
        self.data_dir.mkdir(parents=True, exist_ok=True)
        address = self._listen_address(self.url, "service")
        argv = [str(self.binary), "-addr", address, "-dir", str(self.data_dir), "-profile", self.profile]
        if self.native_address is not None:
            argv.extend(["-native-addr", self.native_address])
        if self.diagnostics_url is not None:
            argv.extend([
                "-pprof", self._listen_address(self.diagnostics_url, "diagnostics"),
                "-block-profile-rate", str(self.block_profile_rate),
                "-mutex-profile-fraction", str(self.mutex_profile_fraction),
            ])
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_region_start = self.log_path.stat().st_size if self.log_path.exists() else 0
        self.log_file = self.log_path.open("a", encoding="utf-8", buffering=1)
        try:
            self.process = subprocess.Popen(
                argv, stdout=self.log_file, stderr=subprocess.STDOUT, text=True,
            )
            if self.measured:
                self._owned_identity = common.linux_process_identity(self.process.pid)
                unavailable = {"availability": "unavailable", "reason": "startup incomplete",
                               "started_monotonic_ns": time.monotonic_ns(),
                               "ended_monotonic_ns": time.monotonic_ns()}
                self.lifetimes.append({"ordinal": len(self.lifetimes), "pid": self.process.pid,
                    "linux_process_identity": self._owned_identity,
                    "first_work": dict(unavailable), "last_live_work": dict(unavailable),
                    "exit": {"availability": "unavailable", "reason": "process has not exited",
                             "pid": self.process.pid, "linux_process_identity": self._owned_identity,
                             "observed_monotonic_ns": time.monotonic_ns(), "source": "", "scope": ""}})
            deadline, last = time.monotonic() + self.startup_timeout, ""
            while time.monotonic() < deadline:
                if (self._reap_owned() if self.measured else self.process.poll()) is not None:
                    raise RuntimeError(f"TreeDB service exited during startup; log tail: {self.log_evidence()['tail']}")
                try:
                    health = TreeDBClient(self.url, timeout=1).health()
                    ready = health.get("ok") is True and health.get("contract_version") == SERVICE_CONTRACT
                    if ready and self.diagnostics_url is not None:
                        stats = self._read_json(DIAGNOSTICS_STATS_PATH)
                        ready = stats.get("contract_version") == SERVICE_CONTRACT
                    if ready:
                        if self.measured:
                            identity = common.linux_process_identity(self.process.pid)
                            endpoints = [self.url, self.diagnostics_url]
                            if self.native_address:
                                endpoints.append("http://" + self.native_address)
                            if identity != self._owned_identity or any(
                                endpoint is None or not common.server_process_owns_endpoint(self.process.pid, endpoint, None)
                                for endpoint in endpoints
                            ) or common.linux_process_identity(self.process.pid) != identity:
                                raise RuntimeError("ready service does not own every measured listener")
                            first = self.work_snapshot()
                            if first["availability"] != "measured":
                                raise RuntimeError(first["reason"])
                            self.lifetimes[-1]["first_work"] = first
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
        if self.measured:
            self._stop_measured()
            return
        try:
            timed_out = False
            if self.process is not None:
                process = self.process
                if process.poll() is None:
                    pid = process.pid
                    latest_process = common.server_process_resource_usage(pid, "TreeDB")
                    peak_rss = None

                    def retain_peak(sample: dict[str, Any]) -> bool:
                        nonlocal peak_rss
                        peak = sample.get("peak_rss", {})
                        if peak.get("availability") != "measured":
                            return True
                        if peak.get("pid") != pid or not peak.get("process_identity"):
                            return False
                        if peak_rss is not None and peak["process_identity"] != peak_rss["process_identity"]:
                            return False
                        if peak_rss is None or peak["bytes"] > peak_rss["bytes"]:
                            peak_rss = peak
                        return True

                    retain_peak(latest_process)
                    process.terminate()
                    deadline = time.monotonic() + self.shutdown_timeout
                    exited = False
                    while not exited:
                        sample = common.server_process_resource_usage(pid, "TreeDB")
                        same_process = retain_peak(sample)
                        if same_process and sample["captured"] and (
                            not latest_process["captured"]
                            or (
                                sample["cpu_seconds"] >= latest_process["cpu_seconds"]
                                and (sample["rss_bytes"] > 0 or latest_process["rss_bytes"] == 0)
                            )
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
                    # VmHWM can disappear before ps stops reporting CPU/RSS.
                    # Keep the measured highwater independently of that endpoint.
                    if peak_rss is not None:
                        latest_process = {**latest_process, "peak_rss": peak_rss}
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

    def _stop_measured(self) -> None:
        if self.process is None:
            return
        process = self.process
        failure = None
        try:
            if self._reap_owned() is None:
                # This is the last real drained service observation. The terminal
                # record will include additional close/flush work after it.
                try:
                    self.work_snapshot()
                    latest = common.server_process_resource_usage(process.pid, "TreeDB")
                    self.last_shutdown_resource_end = {
                        **latest, "disk_bytes": common.disk_bytes(self.data_dir),
                        "captured": latest["captured"] and self.data_dir.exists(),
                        "availability": {**latest["availability"], "disk_bytes": str(self.data_dir)}}
                except Exception as exc:
                    # Failed measurement must not prevent cleanup of the same
                    # still-owned child. Its unavailable evidence stays failed.
                    failure = exc
                if common.linux_process_identity(process.pid) != self._owned_identity:
                    raise RuntimeError("owned process identity changed before shutdown")
                os.kill(process.pid, signal.SIGTERM)
                deadline = time.monotonic() + self.shutdown_timeout
                while self._reap_owned() is None and time.monotonic() < deadline:
                    try:
                        sample = common.server_process_resource_usage(process.pid, "TreeDB")
                    except Exception as exc:
                        failure = failure or exc
                        sample = {"captured": False}
                    if (self.last_shutdown_resource_end is not None and sample["captured"]
                            and sample.get("linux_process_identity") == self._owned_identity
                            and sample["cpu_seconds"] >= self.last_shutdown_resource_end["cpu_seconds"]):
                        # Preserve legacy live endpoint semantics separately from
                        # the final qualification storage endpoint already frozen.
                        self.last_shutdown_resource_end.update(sample)
                    time.sleep(0.01)
                if self._reap_owned() is None:
                    os.kill(process.pid, signal.SIGKILL)
                    deadline = time.monotonic() + min(5, self.shutdown_timeout)
                    while self._reap_owned() is None and time.monotonic() < deadline:
                        time.sleep(0.01)
                    failure = TimeoutError("measured graceful shutdown timed out")
                if self.last_shutdown_resource_end is not None:
                    self.last_shutdown_resource_end["disk_bytes"] = common.disk_bytes(self.data_dir)
            self._terminal_work()
            lifetime = self.lifetimes[-1]
            if lifetime["exit"].get("exit_code") != 0 or lifetime["exit"].get("availability") != "measured":
                failure = failure or RuntimeError("owned service exit evidence failed")
            if "terminal_error" in lifetime:
                failure = failure or RuntimeError(lifetime["terminal_error"])
            if failure is not None:
                raise failure
        finally:
            # No Popen wait/poll/signal helper participates in measured reaping.
            if process.returncode is not None:
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

    @property
    def native(self) -> TreeDBClient:
        if self.controller.native_address is None:
            raise RuntimeError("native Minima transport requires an explicit listener")
        client = getattr(self.local, "native", None)
        if client is None:
            client = TreeDBClient(self.url, timeout=self.timeout, native_address=self.controller.native_address)
            self.local.native = client
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
                 diagnostic_capture_timeout: float = 10, strategy: str = "native_runtime",
                 transport: str | None = None, column_graph_serving: dict[str, Any] | None = None) -> None:
        self.strategy = strategy
        self.transport = transport or ("native" if strategy == "column_graph" else "http")
        if strategy not in ("native_runtime", "column_graph") or self.transport not in ("http", "native"):
            raise ValueError("unsupported Minima strategy or transport")
        if strategy == "native_runtime" and self.transport != "http":
            raise ValueError("legacy Minima baseline requires HTTP transport")
        if strategy == "column_graph" and not column_graph_serving:
            raise ValueError("column_graph requires explicit serving limits")
        if self.transport == "native" and not controller.native_address:
            raise ValueError("native Minima transport requires an explicit listener")
        self.column_graph_serving = column_graph_serving
        self.index_info = None
        self._graph_built = False
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
        self._batch_correlation_expected_identities: set[tuple[str, str, int, int]] = set()
        self._batch_correlation_max_records = 0
        self._batch_prepared = False
        if diagnostics_dir is not None and controller.diagnostics_url is None:
            raise ValueError("diagnostics_dir requires an enabled controller diagnostics URL")

    def _prepare_batch_correlations(self) -> None:
        if self._batch_prepared:
            return
        expected_rows = 0
        self._batch_correlation_expected_identities: set[tuple[str, str, int, int]] = set()
        for operation in self.manifest["operations"]:
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
        self._batch_prepared = True

    def _measured_settings(self) -> dict[str, str]:
        return {"scalar_fields": "meta.user_id,meta.fpath", "product_commit": self.source_commit,
                "service_binary_sha256": self.service_binary_sha256,
                "service_binary_vcs_revision": self.service_binary_vcs_revision,
                "service_binary_vcs_modified": self.service_binary_vcs_modified,
                "vector_strategy": self.strategy, "transport": self.transport, "control_transport": "http",
                "ef_search": str(self.ef_search), "column_graph_serving": json.dumps(self.column_graph_serving, sort_keys=True),
                "profile": self.controller.profile,
                "operation_timeout_seconds": str(self.operation_timeout_seconds),
                "startup_reopen_timeout_seconds": str(self.controller.startup_timeout),
                "shutdown_timeout_seconds": str(self.controller.shutdown_timeout),
                "block_profile_rate": str(self.controller.block_profile_rate),
                "mutex_profile_fraction": str(self.controller.mutex_profile_fraction)}

    def _request_context(self) -> dict[str, Any]:
        return {"phase": self._phase_name or "setup", "lifetime_ordinal": self.lifetime_ordinal,
                "transport": "http"}

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
        if getattr(self, "measured", False):
            lifetime = self.controller.lifetimes[-1]
            self._phase_restart_old_end.update(lifetime_ordinal=lifetime["ordinal"],
                linux_process_identity=lifetime["linux_process_identity"], work_snapshot=lifetime["last_live_work"])
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
        if getattr(self, "measured", False):
            self._verify_measured_server_affinity(self.measurement_configuration["cpu_affinity"])
            snapshot["lifetime_ordinal"] = self.lifetime_ordinal
            snapshot["linux_process_identity"] = common.linux_process_identity(pid)
            snapshot["work_snapshot"] = self.controller.work_snapshot()
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
        self.restart_origin_linux_identity = common.linux_process_identity(old_pid)
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
        if hasattr(self, "setup_interval"):
            self.setup_interval["ended_monotonic_ns"] = phase_start
        self._phase_start = phase_start
        self._phase_name = "initial_durable_load"
        self._prepare_batch_correlations()

    def phase_transition(self, name: str) -> None:
        if name not in PHASE_CLASSIFICATIONS:
            raise RuntimeError(f"unknown TreeDB attribution phase {name!r}")
        if self._phase_start is None or self._phase_name is None or self._phase_resource_start is None:
            raise RuntimeError("TreeDB phase attribution was not started")
        if name == "pre_close_queries" and self.strategy == "column_graph":
            self._fold_graph()
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
            if getattr(self, "measured", False):
                new_start["work_snapshot"] = self.controller.lifetimes[-1]["first_work"]
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
                final = getattr(self, "_artifact_resource_end", None)
                disk = {"captured": final["captured"], "disk_bytes": final["disk_bytes"],
                        "availability": {"disk_bytes": final["availability"]["disk_bytes"]}} if final is not None else self._phase_disk_snapshot()
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
        if self.strategy == "column_graph" and self._graph_built:
            if getattr(self, "measured", False):
                self.evidence.call("column_graph_reopen_schema", "control", "all", self.ensure_compatible)
            else:
                self.ensure_compatible()
            self.evidence.call("column_graph_reopen_ensure", "writer_wait", "all", lambda: self.client.optimize_index(
                self.collection, column_graph_action="ensure", column_graph_serving=self.column_graph_serving))

    def create_owned_collection(self) -> None:
        assert self.client is not None
        self.evidence.call("ensure_compatible_collection", "writer", "all", self.ensure_compatible)

    def ensure_compatible(self) -> None:
        assert self.client is not None
        info = self.client.ensure_index(
            self.collection, self.config["dimension"], self.config["metric"],
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"},
                           {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options={"strategy": self.strategy},
            **({"typed_input": True} if self.strategy == "column_graph" else {}),
        )
        fields = {(row.field, row.value_type) for row in info.scalar_fields}
        if (info.dimension, info.metric, info.vector_strategy, fields) != (
            self.config["dimension"], self.config["metric"], self.strategy,
            {("meta.user_id", "string"), ("meta.fpath", "string")},
        ):
            raise RuntimeError("TreeDB index is not the compatible selected Minima schema")
        if self.strategy == "column_graph" and info.extra.get("typed_input") is not True:
            raise RuntimeError("TreeDB column_graph index lacks authoritative typed input")
        self.index_info = info
        self.effective_collection = info.to_dict()

    def initial_load_to_query_boundary(self) -> None:
        if self.strategy == "column_graph":
            self.evidence.call("column_graph_initial_build", "writer_wait", "all", lambda: self.client.optimize_index(
                self.collection, column_graph_action="build", column_graph_serving=self.column_graph_serving))
            self._graph_built = True

    def _upsert_batch(self, batch: list[dict[str, Any]]) -> Any:
        if self.transport == "native":
            return self.clients.native.upsert_documents(self.collection, batch, index_info=self.index_info)
        return self.client.upsert_documents(self.collection, batch, defer_vector_index_rebuild=True)

    def _fold_graph(self) -> None:
        self.evidence.call("column_graph_fold", "writer_wait", "all", lambda: self.client.optimize_index(
            self.collection, column_graph_action="fold"))

    def wait_ready(self, expected_count: int | None = None,
                   phase: str = "mutation_visibility") -> None:
        assert self.client is not None
        if expected_count is not None:
            count = self.client.count_documents(self.collection).count
            if count != expected_count:
                raise RuntimeError(f"TreeDB visible document count={count}, expected={expected_count}")

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
            request = None
            if getattr(self, "measured", False):
                request = {"transport": self.transport, "requested_count": len(batch),
                           "batch_start": self._batch_start(scenario, source_batch, local_start)}
                if self.transport == "native":
                    request.update(command_version=1, expected_generation=self.index_info.generation)
            if self.diagnostics_dir is None:
                response = self.evidence.call(
                    operation, "writer", scenario,
                    lambda batch=batch: self._upsert_batch(batch),
                    on_start=on_writer_start, record=request,
                )
                if request is not None:
                    request["result_count"] = response.upserted
                if response.upserted != len(batch) or response.ids != [row["id"] for row in batch]:
                    if request is not None:
                        request.update(outcome="error", error="TreeDB upsert completion did not cover the submitted batch")
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
                    lambda batch=batch: self._upsert_batch(batch),
                    on_start=on_writer_start, record=request,
                )
                if request is not None:
                    request["result_count"] = response.upserted
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
        measured = getattr(self, "measured", False)
        owned: dict[str, Any] | None = {} if measured else None
        outer = interval if interval is not None else {} if measured else None
        if outer is not None:
            outer["started_monotonic_ns"] = time.monotonic_ns()
        failure = None
        response = None
        def query_request() -> Any:
            client = self.clients.native if self.transport == "native" else self.client
            return client.query_by_embedding(
                self.collection, query["vector"], self.config["top_k"], scalar_filter(spec),
                route="ann", ef_search=self.ef_search,
                **({"index_info": self.index_info} if self.transport == "native" else {}))
        try:
            response = self.evidence.call(operation, "search", scenario, query_request, record=owned)
        except BaseException as exc:
            failure = exc
        finally:
            if outer is not None:
                outer["ended_monotonic_ns"] = time.monotonic_ns()
        # Ordinary client selection, request and response decoding are inside the
        # outer timer. Owned proof normalization deliberately begins after it.
        try:
            if measured:
                owned.update(outer)
                if interval is not None:
                    interval["request_sequence"] = owned["request_sequence"]
                owned["transport"] = self.transport
                owned["expected_generation"] = self.index_info.generation
                if self.transport == "native":
                    owned["command_version"] = 2
                proof = getattr(failure if failure is not None else response, "dense_work", None)
                if proof is not None:
                    owned["dense_work"] = asdict(proof)
                if response is not None:
                    owned["result_count"] = len(response.documents)
            if failure is not None:
                raise failure
            selected = response.native_base_plus_live_delta
            if self.strategy == "column_graph":
                selected = (not response.native_base_plus_live_delta
                            and response.index.vector_strategy == "column_graph"
                            and response.index.generation == self.index_info.generation
                            and (self.transport != "native" or response.native_command_version == 2))
            if response.route != "ann" or not selected or response.exact_fallbacks != 0 or response.full_document_scan_fallbacks != 0:
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

        except BaseException as exc:
            if measured:
                owned.update(outcome="error", error=f"{type(exc).__name__}: {exc}")
            raise

    def retrieve(self, operation: str, scenario: str, ids: list[str]) -> list[Any]:
        assert self.client is not None
        record = None
        try:
            measured = getattr(self, "measured", False)
            if self.transport == "native":
                record = {"operation": "fetch", "transport": "native", "command_version": 2,
                          "expected_generation": self.index_info.generation,
                          "requested_count": len(ids), "requested_ids": ids,
                          "projection": "full_fp32_document"} if measured else None
                documents = self.evidence.call(operation, "fetch", scenario, lambda: self.clients.native.get_many(
                    self.collection, ids, index_info=self.index_info), record=record)
                if measured:
                    actual = [row.id for row in documents if row is not None]
                    record.update(operation="fetch", transport="native", command_version=2,
                                  requested_count=len(ids), result_count=len(actual), missing_count=len(ids)-len(actual),
                                  requested_ids=ids, result_ids=actual, projection="full_fp32_document")
                    for row in documents:
                        if row is not None:
                            expected = self.expected_vector(row.id)
                            actual_vector = common.normalized_f32_vector(row.embedding or [])
                            if len(expected) != len(actual_vector) or any(abs(a-b) > self.config["score_tolerance"]
                                    for a, b in zip(expected, actual_vector)):
                                record.update(outcome="error", error="requested native GetMany vector mismatch")
                                raise RuntimeError("requested native GetMany vector mismatch")
                return [SimpleNamespace(payload={"id": row.id, "content": row.content, **row.meta})
                        for row in documents if row is not None]
            rows = []
            for identifier in ids:
                record = {"operation": "fetch", "requested_count": 1, "requested_ids": [identifier],
                          "projection": "payload_only_per_id"} if measured else None
                result = self.evidence.call(operation, "fetch", scenario, lambda identifier=identifier: self.client.filter_documents(
                    self.collection, {"field": "id", "operator": "==", "value": identifier}, limit=1), record=record)
                if measured:
                    record.update(operation="fetch", requested_count=1, result_count=len(result.documents),
                                  missing_count=1-len(result.documents), requested_ids=[identifier],
                                  result_ids=[row.id for row in result.documents], projection="payload_only_per_id")
                rows.extend(SimpleNamespace(payload={"id": row.id, "content": row.content, **row.meta}) for row in result.documents)
            return rows
        except BaseException as exc:
            if record is not None:
                record.update(outcome="error", error=f"{type(exc).__name__}: {exc}")
            raise

    def delete_filter(self, operation: dict[str, Any],
                      on_writer_start: Callable[[], None] | None = None) -> None:
        assert self.client is not None
        name, scenario = operation["name"], operation["target"]
        filt = scalar_filter({**self.specs[scenario], **operation["filter"]})
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete_by_filter(
            self.collection, filt), on_start=on_writer_start)
        record = {} if getattr(self, "measured", False) else None
        remaining = self.evidence.call(name, "fetch", scenario, lambda: self.client.count_documents(
            self.collection, filt), record=record).count
        if record is not None:
            record["result_count"] = remaining
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
            result = self.evidence.call("final_scroll_page", "control", "all", lambda: self.client.filter_documents(
                self.collection, limit=STATE_SCROLL_PAGE_SIZE, return_embedding=True,
                after_id=after_id, cursor_page=True,
            ))
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
        self.setup_interval = {"started_monotonic_ns": time.monotonic_ns()}
        self.connect()
        self.create_owned_collection()
        self.begin_phase_attribution()
        spec = self.specs["small"]
        documents = [common.generated_document(spec, ordinal) for ordinal in range(spec["corpus_rows"])]
        self.upsert("small_initial_batch_insert", "small", documents)
        self.initial_load_to_query_boundary()
        self.phase_transition("warmup_search")
        initial = self.search("small_initial_oracle", "small")
        self.evidence.initial["small"] = initial
        self.compare_oracle("initial", "small", initial)
        self.phase_transition("lifecycle_mutations")
        update = self.manifest["operations"][7]
        self.upsert(update["name"], "small", update["documents"])
        fetched = self.retrieve(update["name"], "small", [update["documents"][0]["id"]])
        self.operations["explicit_update_visible"] = (
            len(fetched) == 1 and fetched[0].payload.get("content") == update["documents"][0]["content"]
        )
        delete = self.manifest["operations"][9]
        self.delete_ids(delete)
        self.operations["explicit_delete_visible"] = True
        self.phase_transition("pre_close_queries")
        self.evidence.preclose["small"] = self.search("small_preclose", "small")
        assert self.client is not None
        self.phase_transition("restart_open_readiness")
        self.capture_restart_origin()
        self.client.close()
        self.client = None
        self.restart_backend()
        self.reopen_attempted = True
        self.connect()
        self.ensure_compatible()
        self.phase_transition("post_reopen")
        self.evidence.reopen["small"] = self.search("small_reopen", "small")
        self.phase_transition("final_state_scroll_artifact_work")
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
        if self.manifest.get("schema") == common.BOUNDED_MANIFEST_SCHEMA:
            artifact["schema"] = common.BOUNDED_ARTIFACT_SCHEMA
            artifact["native_path_proof"] = {
                "schema": "treedb_minima_native_path_proof/v1", "strategy": "native_runtime",
                "availability": "unavailable", "counters": None,
                "reason": "native baseline diagnostic; typed column_graph lifecycle counters require M1-M4; bounded sparse scenario does not preserve full <1% selectivity",
            }
        if self.strategy == "column_graph":
            artifact["native_path_proof"] = {
                "schema": "treedb_minima_native_path_proof/v1", "strategy": "column_graph",
                "availability": "unavailable", "counters": None,
                "reason": "typed public dispatch diagnostic only; phase and replay producer evidence is not yet captured",
            }
        resource = artifact["backend_raw_evidence"]["qdrant"]["resource_measurement"]
        self._artifact_resource_end = resource["end"]
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
                              "vector_strategy": self.strategy, "transport": self.transport,
                              "control_transport": "http", "ef_search": str(self.ef_search),
                              "column_graph_serving": json.dumps(self.column_graph_serving, sort_keys=True),
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
            if self.strategy == "column_graph":
                row["route"] = {"identity": "typed_column_graph_dispatch",
                                "native_command_version": route.native_command_version,
                                "work_counters_availability": "unavailable"}
                row["visibility"] = {"generation_consistent": route.index.generation == self.index_info.generation}
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
        if self.strategy == "column_graph":
            raw["native_route_responses"] = {
                scenario: {"native_command_version": value.native_command_version,
                           "generation": value.index.generation, "work_counters_availability": "unavailable"}
                for scenario, value in self.route_evidence.items()
            }
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
        if getattr(self, "measured", False):
            artifact["schema"] = common.MEASURED_SCHEMA
            artifact["freeze_sha256"] = self.freeze["sha256"]
            artifact.pop("native_path_proof", None)
            backend["configuration"].update(self.measurement_configuration)
            raw.pop("native_route_responses", None)
            raw["request_evidence"] = self.evidence.requests
            for row in artifact["scenarios"]:
                route = self.route_evidence.get(row["scenario"])
                proof = getattr(route, "dense_work", None)
                row["route"] = {"identity": "typed_column_graph_dispatch",
                                "declared_scalar_filtering": True,
                                "membership_source": "captured_typed_scalar",
                                "plan": proof.graph.route if proof is not None else "unavailable"}
                row["visibility"] = {"generation_consistent": route is not None and
                                     route.index.generation == self.index_info.generation}
        return artifact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", choices=("native_runtime", "column_graph"), default="native_runtime")
    parser.add_argument("--transport", choices=("http", "native"), help="column_graph defaults to native; HTTP enables the bridge comparison")
    parser.add_argument("--native-address", default="127.0.0.1:17122")
    parser.add_argument("--column-graph-serving", type=Path, help="explicit JSON serving limits for column_graph")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--measured", action="store_true")
    parser.add_argument("--legacy-diagnostic-control", action="store_true",
                        help="keep ordinary proof decoding/resources; disable only added measured capture")
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--expected-freeze-sha256")
    parser.add_argument("--comparator-bin", type=Path)
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
    for name, default in (("measured", False), ("legacy_diagnostic_control", False),
                          ("freeze", None), ("expected_freeze_sha256", None), ("comparator_bin", None)):
        if not hasattr(args, name):
            setattr(args, name, default)
    serving = None
    if args.strategy == "column_graph":
        if args.column_graph_serving is None:
            raise SystemExit("column_graph requires --column-graph-serving with explicit limits")
        serving = json.loads(args.column_graph_serving.read_text(encoding="utf-8"))
        if not isinstance(serving, dict) or not serving:
            raise SystemExit("column_graph serving limits must be a nonempty JSON object")
    transport = getattr(args, "transport", None) or ("native" if args.strategy == "column_graph" else "http")
    manifest = common.load_manifest(args.manifest)
    freeze = common.load_measured_freeze(args.freeze, args.expected_freeze_sha256, args.manifest, manifest)
    measured = args.measured and not args.legacy_diagnostic_control
    if args.measured and (freeze is None or args.comparator_bin is None or args.strategy != "column_graph"):
        raise SystemExit("measured collection requires selected typed route, trusted freeze and comparator binary")
    source = common.measured_source_configuration("treedb", args.manifest, args.comparator_bin,
        freeze["reviewed_product_commit"]) if args.measured else None
    diagnostics_dir = args.diagnostics_dir.resolve() if args.diagnostics_dir is not None else None
    controller = ServiceController(
        args.service_bin.resolve(), args.url, args.data_dir.resolve(), args.profile,
        args.startup_timeout, args.operation_timeout,
        diagnostics_url=args.diagnostics_url if diagnostics_dir is not None or measured else None,
        block_profile_rate=0 if args.measured else 1,
        mutex_profile_fraction=0 if args.measured else 1,
        native_address=args.native_address if transport == "native" else None,
        measured=measured,
    )
    runner = None
    artifact = None
    exit_code = 0
    try:
        runner = TreeDBMinimaRunner(
            manifest, controller=controller, collection=args.collection,
            operation_timeout=args.operation_timeout, ef_search=args.ef_search,
            diagnostics_dir=diagnostics_dir, diagnostic_slow_seconds=args.diagnostic_slow_seconds,
            diagnostic_profile_seconds=args.diagnostic_profile_seconds,
            diagnostic_capture_timeout=args.diagnostic_capture_timeout,
            strategy=args.strategy, transport=transport, column_graph_serving=serving,
        )
        if args.measured:
            runner.configure_measurement(freeze, source, "treedb")
            if args.legacy_diagnostic_control:
                runner.measured = False
                runner.evidence.request_context = None
        if args.diagnostic_resume_scenario is not None:
            runner.run_diagnostic_resume(args.diagnostic_resume_scenario, args.diagnostic_resume_start)
        elif args.small:
            runner.run_small()
        else:
            runner.run()
    except BaseException as exc:
        if runner is not None:
            runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
        else:
            artifact = {"schema": common.MEASURED_SCHEMA if measured else common.BOUNDED_ARTIFACT_SCHEMA,
                        "state": "partial", "passing": False, "manifest": manifest,
                        "backends": [], "scenarios": [], "backend_raw_evidence": {},
                        "failures": [f"constructor failed: {type(exc).__name__}: {exc}"],
                        "readiness_recommendation": "not_evaluated"}
            if freeze is not None:
                artifact["freeze_sha256"] = freeze["sha256"]
        exit_code = 1
    finally:
        try:
            if artifact is None:
                artifact = runner.artifact()  # Freeze the final live disk endpoint once.
            if not isinstance(artifact, dict) or not isinstance(artifact.get("failures", []), list):
                raise RuntimeError("artifact construction returned an invalid envelope")
        except BaseException as exc:
            artifact = {"schema": common.MEASURED_SCHEMA if measured else common.BOUNDED_ARTIFACT_SCHEMA,
                        "state": "partial", "passing": False, "manifest": manifest,
                        "backends": [], "scenarios": [],
                        "backend_raw_evidence": {"treedb": common.retained_partial_evidence(runner)},
                        "failures": [*(runner.evidence.failures if runner is not None else []),
                                     f"artifact construction failed: {type(exc).__name__}: {exc}"],
                        "readiness_recommendation": "not_evaluated"}
            if freeze is not None:
                artifact["freeze_sha256"] = freeze["sha256"]
            exit_code = 1
        for cleanup in ((runner.close,) if runner is not None else ()) + (controller.stop,):
            try:
                cleanup()
            except BaseException as exc:
                artifact.setdefault("failures", []).append(f"cleanup failed: {type(exc).__name__}: {exc}")
                exit_code = 1
        if measured:
            raw = artifact["backend_raw_evidence"].setdefault("treedb", {})
            raw["process_lifetimes"] = controller.lifetimes
        if args.legacy_diagnostic_control:
            artifact["measurement_control"] = "legacy_diagnostics_without_added_measured_capture"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
