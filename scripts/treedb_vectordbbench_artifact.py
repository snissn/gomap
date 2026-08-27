#!/usr/bin/env python3
"""Capture a reproducible TreeDB VectorDBBench artifact.

The harness owns a fresh TreeDB data directory, starts
cmd/treedb-document-service, writes command/context manifests, and can run a
small route-proof smoke or selected VectorDBBench TreeDB rows from a checkout
named by VECTORDBBENCH_DIR. Lifecycle mode proves the optimized route after a
cold reopen instead of running the independent smoke.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import gzip
import hashlib
import ipaddress
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ARTIFACT_SCHEMA = "treedb-vectordbbench-artifact/v1"
ROUTE_PROOF_SCHEMA = "treedb-vectordbbench-route-proof/v2"
LIFECYCLE_SCHEMA = "treedb-vectordbbench-lifecycle/v1"
LIFECYCLE_EVENT_SCHEMA = "treedb-vectordbbench-lifecycle-event/v1"
LIFECYCLE_VALIDATION_SCHEMA = "treedb-vectordbbench-lifecycle-validation/v1"
PPROF_PROFILE_KINDS = frozenset({"cpu", "heap", "allocs", "block", "mutex"})
OPTIMIZED_ROUTE_NAMES = frozenset({"exact_hnsw_search_pack_v1", "quantized_rerank"})
LIFECYCLE_STAGES = (
    "startup",
    "reset",
    "load_start",
    "load_end",
    "drain_checkpoint",
    "optimize_start",
    "optimize_end",
    "cache_prime",
    "cache_warm",
    "graceful_close",
    "cold_open_ready",
    "exact_verify",
    "route_verify",
    "teardown",
)
DEFAULT_QUANTIZED_INDEX_NAME = "embedding.scalar_u8.fast"
VDBBENCH_UV_DEPS = [
    "pytest",
    "click",
    "pydantic",
    "pyyaml",
    "environs",
    "pandas",
    "polars",
    "pyarrow",
    "psutil",
    "pytz",
    "tqdm",
    "plotly",
    "ujson",
    "hdrhistogram",
    "scikit-learn",
    "s3fs",
    "oss2",
]
VDBBENCH_OWNED_OPTIONS = frozenset({
    "--base-url",
    "--case-type",
    "--concurrency-duration",
    "--db-label",
    "--dry-run",
    "--ef-construction",
    "--ef-search",
    "--index-name",
    "--k",
    "--m",
    "--num-concurrency",
    "--quantized-index-name",
    "--quantized-rerank-candidates",
    "--skip-load",
    "--skip-search-concurrent",
    "--skip-search-serial",
    "--timeout",
})
LIFECYCLE_DIAGNOSTIC_BOUNDARIES = (
    "load_end", "optimize_start", "optimize_end", "cache_prime", "cache_warm",
)


def iso_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def default_out_dir() -> Path:
    stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(f"/tmp/treedb_vdbbench_artifact_{stamp}_{os.getpid()}")


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_text(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw


def shjoin(cmd: list[str]) -> str:
    return shlex.join([str(part) for part in cmd])


@dataclass
class CommandRecord:
    name: str
    command: list[str]
    command_string: str
    cwd: str
    started_at: str
    finished_at: str
    duration_seconds: float
    exit_code: int
    stdout: str | None = None
    stderr: str | None = None
    skipped: bool = False
    skip_reason: str | None = None


@dataclass
class HarnessState:
    root: Path
    commands: list[CommandRecord] = field(default_factory=list)
    skips: list[dict[str, str]] = field(default_factory=list)
    service_pid: int | None = None
    service_binary: dict[str, Any] | None = None
    health: dict[str, Any] | None = None
    route_proof: dict[str, Any] | None = None
    vdbbench: list[dict[str, Any]] = field(default_factory=list)
    lifecycle: dict[str, Any] | None = None
    lifecycle_started_ns: int | None = None
    diagnostics: list[dict[str, Any]] = field(default_factory=list)


def add_skip(state: HarnessState, name: str, reason: str) -> None:
    state.skips.append({"name": name, "reason": reason})


def run_command(
    state: HarnessState,
    name: str,
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    required: bool = True,
) -> CommandRecord:
    logs_dir = state.root / "commands"
    logs_dir.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
    stdout_path = logs_dir / f"{safe_name}.stdout.txt"
    stderr_path = logs_dir / f"{safe_name}.stderr.txt"
    started = time.monotonic()
    started_at = iso_now()
    proc = subprocess.run(
        [str(part) for part in cmd],
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    duration = time.monotonic() - started
    stdout_path.write_text(proc.stdout, encoding="utf-8")
    stderr_path.write_text(proc.stderr, encoding="utf-8")
    record = CommandRecord(
        name=name,
        command=[str(part) for part in cmd],
        command_string=shjoin(cmd),
        cwd=str(cwd),
        started_at=started_at,
        finished_at=iso_now(),
        duration_seconds=duration,
        exit_code=proc.returncode,
        stdout=str(stdout_path.relative_to(state.root)),
        stderr=str(stderr_path.relative_to(state.root)),
    )
    state.commands.append(record)
    if required and proc.returncode != 0:
        raise RuntimeError(f"command {name!r} failed with exit code {proc.returncode}; see {stdout_path} and {stderr_path}")
    return record


def git_context(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "available": False, "reason": "path does not exist"}
    ctx: dict[str, Any] = {"path": str(path), "available": True}
    for key, cmd in {
        "commit": ["git", "rev-parse", "HEAD"],
        "branch": ["git", "branch", "--show-current"],
    }.items():
        try:
            res = subprocess.run(cmd, cwd=str(path), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            ctx[key] = res.stdout.strip()
        except Exception as exc:  # noqa: BLE001 - context capture must not hide the artifact
            ctx[key] = None
            ctx[f"{key}_error"] = str(exc)
    try:
        res = subprocess.run(["git", "status", "--short"], cwd=str(path), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        ctx["dirty"] = bool(res.stdout.strip())
        if res.stdout.strip():
            ctx["status_short"] = res.stdout.strip().splitlines()[:50]
    except Exception as exc:  # noqa: BLE001
        ctx["dirty"] = None
        ctx["dirty_error"] = str(exc)
    return ctx


def command_output(cmd: list[str], cwd: Path | None = None) -> str:
    try:
        res = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        return res.stdout.strip()
    except Exception as exc:  # noqa: BLE001
        return f"unavailable: {exc}"


def cpu_brand() -> str:
    if platform.system() == "Darwin":
        return command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
    if platform.system() == "Linux":
        with contextlib.suppress(OSError):
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.startswith("model name"):
                    return line.partition(":")[2].strip()
    return platform.processor() or "unknown"


def physical_cpu_count() -> int | None:
    pairs: set[tuple[str, str]] = set()
    physical = core = None
    with contextlib.suppress(OSError):
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines() + [""]:
            if line.startswith("physical id"):
                physical = line.partition(":")[2].strip()
            elif line.startswith("core id"):
                core = line.partition(":")[2].strip()
            elif not line and physical is not None and core is not None:
                pairs.add((physical, core))
                physical = core = None
    if pairs:
        return len(pairs)
    value = command_output(["sysctl", "-n", "hw.physicalcpu"])
    if value.isascii() and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def memory_bytes() -> int | None:
    with contextlib.suppress(OSError, ValueError):
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                total = int(line.split()[1]) * 1024
                if total > 0:
                    return total
    with contextlib.suppress(AttributeError, OSError, ValueError):
        total = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
        if total > 0:
            return total
    value = command_output(["sysctl", "-n", "hw.memsize"])
    if value.isascii() and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def storage_context(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=False)
    try:
        resolved = path.resolve(strict=True)
        result = subprocess.run(
            ["findmnt", "--json", "-b", "-o", "SOURCE,FSTYPE,TARGET,SIZE", "--target", str(resolved)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=10,
        )
        document = json.loads(result.stdout)
        if not isinstance(document, dict):
            raise ValueError("findmnt output is not an object")
        filesystems = document.get("filesystems")
        if not isinstance(filesystems, list) or len(filesystems) != 1 or not isinstance(filesystems[0], dict):
            raise ValueError("findmnt did not identify exactly one filesystem")
        filesystem = filesystems[0]
        capacity = filesystem.get("size")
        if isinstance(capacity, str) and capacity.isascii() and capacity.isdigit():
            capacity = int(capacity)
        storage = {
            "path": str(resolved),
            "method": "findmnt",
            "device": filesystem.get("source"),
            "filesystem": filesystem.get("fstype"),
            "mount": filesystem.get("target"),
            "capacity_bytes": capacity,
        }
        if valid_storage_context(storage):
            return storage
    except (FileNotFoundError, json.JSONDecodeError, OSError, subprocess.SubprocessError, ValueError):
        pass

    try:
        result = subprocess.run(
            ["df", "-P", "-k", str(resolved)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=10,
        )
        fields = result.stdout.strip().splitlines()[-1].split(maxsplit=5)
        if len(fields) != 6 or not fields[1].isascii() or not fields[1].isdigit():
            raise ValueError("df did not report POSIX storage fields")
        stat_command = ["stat", "-f", "%T", str(resolved)]
        if platform.system() == "Linux":
            stat_command = ["stat", "-f", "-c", "%T", str(resolved)]
        filesystem = subprocess.run(
            stat_command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=10,
        ).stdout.strip()
        storage = {
            "path": str(resolved),
            "method": "df-p+stat",
            "device": fields[0],
            "filesystem": filesystem,
            "mount": fields[5],
            "capacity_bytes": int(fields[1]) * 1024,
        }
        return storage if valid_storage_context(storage) else {}
    except (FileNotFoundError, OSError, subprocess.SubprocessError, ValueError):
        return {}


def valid_storage_context(storage: Any) -> bool:
    def meaningful_text(key: str) -> bool:
        value = storage.get(key)
        return (
            isinstance(value, str)
            and bool(value.strip())
            and value.strip().lower() != "unknown"
            and not value.strip().lower().startswith("unavailable")
        )

    return (
        isinstance(storage, dict)
        and isinstance(storage.get("method"), str)
        and storage["method"] in {"findmnt", "df-p+stat"}
        and all(meaningful_text(key) for key in ("path", "device", "filesystem", "mount"))
        and Path(storage["path"]).is_absolute()
        and Path(storage["mount"]).is_absolute()
        and isinstance(storage.get("capacity_bytes"), int)
        and not isinstance(storage.get("capacity_bytes"), bool)
        and storage["capacity_bytes"] > 0
    )


def collect_context(
    gomap_root: Path, vectordbbench_dir: Path | None, storage_path: Path | None = None
) -> dict[str, Any]:
    return {
        "generated_at": iso_now(),
        "host": {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": sys.version.replace("\n", " "),
            "go": command_output(["go", "version"]),
            "uname": command_output(["uname", "-a"]),
            "cpu_brand": cpu_brand(),
            "logical_cpu_count": os.cpu_count() or 1,
            "physical_cpu_count": physical_cpu_count(),
            "memory_bytes": memory_bytes(),
            "storage": storage_context(storage_path or gomap_root),
        },
        "gomap": git_context(gomap_root),
        "vectordbbench": git_context(vectordbbench_dir) if vectordbbench_dir else {"available": False, "reason": "VECTORDBBENCH_DIR not set"},
    }


def loopback_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def host_port(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def find_free_port(host: str) -> int:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: float = 10.0) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - local/user-provided benchmark URL
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {method} {url} failed status={exc.code}: {body}") from exc
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def index_url(base_url: str, index: str, suffix: str = "") -> str:
    encoded = urllib.parse.quote(index, safe="")
    return f"{base_url.rstrip('/')}/v1/indexes/{encoded}{suffix}"


def wait_health(base_url: str, timeout_seconds: float, interval: float = 0.25) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return http_json("GET", f"{base_url.rstrip('/')}/v1/health", timeout=2.0)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(interval)
    raise RuntimeError(f"TreeDB document service did not become healthy within {timeout_seconds}s: {last_error}")


def terminate_process_group(
    proc: subprocess.Popen[str], *, graceful_timeout: float = 10.0
) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=graceful_timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=5)


def close_process_group_cleanly(
    proc: subprocess.Popen[str], *, graceful_timeout: float
) -> None:
    """Stop a service and reject timeout, signal, or nonzero exit."""
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=graceful_timeout)
        except subprocess.TimeoutExpired as exc:
            terminate_process_group(proc, graceful_timeout=0)
            raise RuntimeError(
                f"document service did not close within {graceful_timeout}s"
            ) from exc
    if proc.returncode != 0:
        raise RuntimeError(f"document service did not close cleanly: exit={proc.returncode}")


def build_service(state: HarnessState, gomap_root: Path, service_bin: str | None) -> Path:
    if service_bin:
        path = Path(service_bin).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"TREEDB_VDBBENCH_SERVICE_BIN does not exist: {path}")
        return path
    bin_path = state.root / "bin" / "treedb-document-service"
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        state,
        "go_build_treedb_document_service",
        ["go", "build", "-o", str(bin_path), "./cmd/treedb-document-service"],
        cwd=gomap_root,
        timeout=180,
        required=True,
    )
    return bin_path


def start_service(
    state: HarnessState,
    *,
    gomap_root: Path,
    service_bin: Path,
    data_dir: Path,
    host: str,
    port: int,
    profile: str,
    health_timeout: float,
    pprof_addr: str = "",
    append_log: bool = False,
) -> tuple[subprocess.Popen[str], dict[str, Any], list[str]]:
    service_log = state.root / "service.log"
    address = host_port(host, port)
    cmd = [str(service_bin), "-dir", str(data_dir), "-addr", address, "-profile", profile]
    if pprof_addr:
        cmd.extend(["-pprof", pprof_addr])
    data_dir.mkdir(parents=True, exist_ok=True)
    log_fh = service_log.open("a" if append_log else "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=str(gomap_root),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    state.service_pid = proc.pid
    try:
        health = wait_health(f"http://{address}", health_timeout)
    except BaseException:
        try:
            terminate_process_group(proc)
        finally:
            log_fh.close()
        raise
    # Keep the file descriptor owned by the process open until shutdown; Python
    # can close its duplicate without affecting the child process.
    log_fh.close()
    return proc, health, cmd


def int_field(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key, 0)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def fallback_reason(response: dict[str, Any]) -> str:
    diagnostics = response.get("diagnostics") or {}
    reason = str(diagnostics.get("fallback_reason") or "none")
    return reason or "none"


def route_response_stats(response: dict[str, Any]) -> dict[str, Any] | None:
    stats = response.get("stats", {})
    return stats if isinstance(stats, dict) else None


def no_document_guardrails(response: dict[str, Any]) -> bool:
    stats = route_response_stats(response)
    diagnostics = response.get("diagnostics")
    return (
        isinstance(stats, dict)
        and isinstance(diagnostics, dict)
        and diagnostics.get("no_document_guardrails_ok") is True
        and all(
            isinstance(stats.get(key, 0), int)
            and not isinstance(stats.get(key, 0), bool)
            and stats.get(key, 0) == 0
            for key in ("documents_fetched", "document_bytes", "document_output_bytes")
        )
    )


def identified_route_results(value: Any, expected_count: int) -> bool:
    return (
        isinstance(value, list)
        and len(value) == expected_count
        and all(
            isinstance(result, dict)
            and isinstance(result.get("id"), str)
            and bool(result["id"])
            for result in value
        )
        and len({result["id"] for result in value}) == expected_count
    )


def proof_summary(kind: str, index_name: str, response: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
    stats = route_response_stats(response)
    if stats is None:
        raise ValueError("route response stats must be an object when present")
    diagnostics = response.get("diagnostics") or {}
    return {
        "kind": kind,
        "index_name": index_name,
        "endpoint": f"/v1/indexes/{index_name}/search/vector-index",
        "request": {
            "top_k": request_payload.get("top_k"),
            "ef_search": request_payload.get("ef_search"),
            "query_mode": request_payload.get("query_mode", "exact"),
            "quantized_index_name": request_payload.get("quantized_index_name", ""),
            "quantized_rerank_candidates": request_payload.get("quantized_rerank_candidates", 0),
        },
        "response": {
            "metric": response.get("metric"),
            "query_mode": response.get("query_mode"),
            "quantized_index_name": response.get("quantized_index_name", ""),
            "quantized_rerank_candidates": response.get("quantized_rerank_candidates", 0),
            "no_documents": bool(response.get("no_documents")),
            "result_count": len(response.get("results") or []),
            "result_ids": [item.get("id") for item in (response.get("results") or [])],
        },
        "route": str(diagnostics.get("route") or ""),
        "fallback_reason": fallback_reason(response),
        "documents_fetched": int_field(stats, "documents_fetched"),
        "document_bytes": int_field(stats, "document_bytes"),
        "quantized_scorer_active": int_field(stats, "quantized_scorer_active"),
        "quantized_score_calls": int_field(stats, "quantized_score_calls"),
        "quantized_rerank_candidates_observed": int_field(stats, "quantized_rerank_candidates"),
        "quantized_rerank_exact_score_calls": int_field(stats, "quantized_rerank_exact_score_calls"),
        "score_batch_calls": int_field(stats, "score_batch_calls"),
        "score_batch_optimized": int_field(stats, "score_batch_optimized"),
        "score_batch_fallback": int_field(stats, "score_batch_fallback"),
        "search_route_hnsw_search_pack": int_field(stats, "search_route_hnsw_search_pack"),
        "search_route_quantized_rerank": int_field(stats, "search_route_quantized_rerank"),
        "raw_response_file": f"route_proof_{kind}_response.json",
    }


def add_assert(assertions: list[dict[str, Any]], name: str, passed: bool, detail: str) -> None:
    assertions.append({"name": name, "passed": bool(passed), "detail": detail})


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_smoke_shape(dimension: int, documents: int, top_k: int, ef_search: int, rerank_candidates: int) -> None:
    if dimension <= 0 or top_k <= 0:
        raise ValueError("smoke dimension and top-k must be positive")
    if documents < top_k:
        raise ValueError("smoke documents must be at least smoke top-k")
    if ef_search < top_k:
        raise ValueError("ef-search must be at least smoke top-k")
    if rerank_candidates < top_k:
        raise ValueError("rerank candidates must be at least smoke top-k")


def smoke_documents(count: int, dimension: int) -> list[dict[str, Any]]:
    return [
        {
            "id": str(101 + row),
            "embedding": [((row * 17 + column * 31) % 257 - 128) / 128.0 for column in range(dimension)],
            "content": f"route proof {101 + row}",
        }
        for row in range(count)
    ]


def run_route_proof_smoke(
    state: HarnessState,
    *,
    base_url: str,
    index_prefix: str,
    m: int,
    ef_construction: int,
    ef_search: int,
    quantized_index_name: str,
    rerank_candidates: int,
    smoke_dimension: int,
    smoke_documents_count: int,
    smoke_top_k: int,
) -> dict[str, Any]:
    validate_smoke_shape(smoke_dimension, smoke_documents_count, smoke_top_k, ef_search, rerank_candidates)
    exact_index = f"{index_prefix}_exact_smoke"
    scalar_index = f"{index_prefix}_scalar_u8_smoke"
    documents = smoke_documents(smoke_documents_count, smoke_dimension)
    query = list(documents[0]["embedding"])
    optimized_scoring_expected = smoke_dimension >= 32
    create_exact = {
        "name": exact_index,
        "dimension": smoke_dimension,
        "metric": "cosine",
        "vector_index_options": {
            "strategy": "column_graph",
            "m": m,
            "ef_construction": ef_construction,
            "ef_search": ef_search,
        },
    }
    create_scalar = {
        "name": scalar_index,
        "dimension": smoke_dimension,
        "metric": "cosine",
        "vector_index_options": {
            "strategy": "column_graph",
            "m": m,
            "ef_construction": ef_construction,
            "ef_search": ef_search,
            "quantized_indexes": [{"name": quantized_index_name, "codec": "scalar_u8", "version": 1}],
        },
    }
    for payload in (create_exact, create_scalar):
        http_json("POST", f"{base_url.rstrip('/')}/v1/indexes", payload)
    for index in (exact_index, scalar_index):
        http_json("POST", index_url(base_url, index, "/documents/upsert"), {"documents": documents})
        optimize = http_json("POST", index_url(base_url, index, "/optimize"), {"vector_index_name": "embedding"})
        write_json(state.root / f"route_proof_{index}_optimize.json", optimize)

    exact_request = {
        "query_embedding": query,
        "top_k": smoke_top_k,
        "ef_search": ef_search,
        "query_mode": "exact",
    }
    scalar_request = {
        "query_embedding": query,
        "top_k": smoke_top_k,
        "ef_search": ef_search,
        "query_mode": "quantized_rerank",
        "quantized_index_name": quantized_index_name,
        "quantized_rerank_candidates": rerank_candidates,
    }
    exact_response = http_json("POST", index_url(base_url, exact_index, "/search/vector-index"), exact_request)
    scalar_response = http_json("POST", index_url(base_url, scalar_index, "/search/vector-index"), scalar_request)
    write_json(state.root / "route_proof_exact_response.json", exact_response)
    write_json(state.root / "route_proof_scalar_response.json", scalar_response)

    exact = proof_summary("exact", exact_index, exact_response, exact_request)
    scalar = proof_summary("scalar", scalar_index, scalar_response, scalar_request)
    assertions: list[dict[str, Any]] = []
    add_assert(
        assertions,
        "exact_route",
        exact["route"] == "exact_hnsw_search_pack_v1",
        f"route={exact['route']} want exact_hnsw_search_pack_v1",
    )
    add_assert(
        assertions,
        "exact_no_documents",
        exact["response"]["no_documents"] is True and exact["documents_fetched"] == 0 and exact["document_bytes"] == 0,
        f"no_documents={exact['response']['no_documents']} documents_fetched={exact['documents_fetched']} document_bytes={exact['document_bytes']}",
    )
    add_assert(
        assertions,
        "exact_no_fallback",
        exact["fallback_reason"] == "none",
        f"fallback_reason={exact['fallback_reason']}",
    )
    add_assert(
        assertions,
        "exact_result_count",
        exact["response"]["result_count"] == smoke_top_k,
        f"result_count={exact['response']['result_count']} want={smoke_top_k}",
    )
    add_assert(
        assertions,
        "exact_optimized_scoring",
        not optimized_scoring_expected or (exact["score_batch_optimized"] > 0 and exact["score_batch_fallback"] == 0),
        f"expected={optimized_scoring_expected} optimized={exact['score_batch_optimized']} fallback={exact['score_batch_fallback']}",
    )
    add_assert(
        assertions,
        "scalar_route",
        scalar["route"] == "quantized_rerank",
        f"route={scalar['route']} want quantized_rerank",
    )
    add_assert(
        assertions,
        "scalar_quantized_scorer_active",
        scalar["quantized_scorer_active"] == 1,
        f"quantized_scorer_active={scalar['quantized_scorer_active']} want 1",
    )
    add_assert(
        assertions,
        "scalar_no_documents",
        scalar["response"]["no_documents"] is True and scalar["documents_fetched"] == 0 and scalar["document_bytes"] == 0,
        f"no_documents={scalar['response']['no_documents']} documents_fetched={scalar['documents_fetched']} document_bytes={scalar['document_bytes']}",
    )
    add_assert(
        assertions,
        "scalar_no_fallback",
        scalar["fallback_reason"] == "none",
        f"fallback_reason={scalar['fallback_reason']}",
    )
    add_assert(
        assertions,
        "scalar_result_count",
        scalar["response"]["result_count"] == smoke_top_k,
        f"result_count={scalar['response']['result_count']} want={smoke_top_k}",
    )
    add_assert(
        assertions,
        "scalar_optimized_scoring",
        not optimized_scoring_expected or (scalar["score_batch_optimized"] > 0 and scalar["score_batch_fallback"] == 0),
        f"expected={optimized_scoring_expected} optimized={scalar['score_batch_optimized']} fallback={scalar['score_batch_fallback']}",
    )
    scalar_requested = int(scalar["response"].get("quantized_rerank_candidates") or rerank_candidates)
    scalar_exact_calls = int(scalar["quantized_rerank_exact_score_calls"])
    add_assert(
        assertions,
        "scalar_rerank_exact_calls_bounded",
        smoke_top_k <= scalar_exact_calls <= scalar_requested,
        f"exact_calls={scalar_exact_calls} want={smoke_top_k}..{scalar_requested}",
    )
    proof = {
        "schema_version": ROUTE_PROOF_SCHEMA,
        "generated_at": iso_now(),
        "base_url": base_url,
        "note": "Smoke route proof only; not throughput or public claim-quality benchmark evidence.",
        "shape": {
            "dimension": smoke_dimension,
            "documents": smoke_documents_count,
            "top_k": smoke_top_k,
            "ef_search": ef_search,
            "rerank_candidates": rerank_candidates,
            "optimized_scoring_expected": optimized_scoring_expected,
        },
        "exact_fp32": exact,
        "scalar_u8_rerank": scalar,
        "assertions": assertions,
        "passed": all(item["passed"] for item in assertions),
    }
    write_json(state.root / "route_proof.json", proof)
    state.route_proof = proof
    if not proof["passed"]:
        failed = [item for item in assertions if not item["passed"]]
        raise RuntimeError(f"route proof smoke failed: {failed}")
    return proof


def pythonpath_for(vectordbbench_dir: Path, gomap_root: Path) -> str:
    parts = [str(vectordbbench_dir), str(gomap_root / "clients" / "python" / "treedb_client" / "src")]
    existing = os.environ.get("PYTHONPATH")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def should_use_uv(args: argparse.Namespace) -> bool:
    if args.use_uv == "on":
        return True
    if args.use_uv == "off":
        return False
    return shutil.which("uv") is not None


def python_module_cmd(args: argparse.Namespace, module: str) -> list[str]:
    if should_use_uv(args):
        cmd = ["uv", "run", "--no-sync"]
        for dep in VDBBENCH_UV_DEPS:
            cmd.extend(["--with", dep])
        # Use uv's environment-selected Python so --with dependencies are
        # available inside the ephemeral run environment. Pin a Python for uv
        # with UV_PYTHON if needed.
        cmd.extend(["python", "-m", module])
        return cmd
    return [args.python, "-m", module]


def run_vdbbench_tests(
    state: HarnessState,
    *,
    args: argparse.Namespace,
    gomap_root: Path,
    vectordbbench_dir: Path | None,
) -> None:
    mode = args.run_tests
    if mode == "off":
        add_skip(state, "vectordbbench_tests", "TREEDB_VDBBENCH_RUN_TESTS=off")
        return
    if vectordbbench_dir is None or not vectordbbench_dir.exists():
        reason = "VECTORDBBENCH_DIR is not set or does not exist"
        if mode == "required":
            raise RuntimeError(reason)
        add_skip(state, "vectordbbench_tests", reason)
        return
    env = os.environ.copy()
    env["PYTHONPATH"] = pythonpath_for(vectordbbench_dir, gomap_root)
    raw_cmd = os.environ.get("TREEDB_VDBBENCH_TEST_CMD")
    if raw_cmd:
        cmd = shlex.split(raw_cmd)
    else:
        cmd = python_module_cmd(args, "pytest") + ["tests/test_treedb_cli.py", "tests/test_db_client_resolution.py", "-q"]
    run_command(state, "vectordbbench_tests", cmd, cwd=vectordbbench_dir, env=env, timeout=args.vdbbench_timeout, required=True)


def append_stage_flags(cmd: list[str], args: argparse.Namespace) -> None:
    if args.vdbbench_dry_run:
        cmd.extend(["--skip-load", "--skip-search-serial", "--skip-search-concurrent", "--dry-run"])
        return
    if args.skip_load:
        cmd.append("--skip-load")
    if args.skip_search_serial:
        cmd.append("--skip-search-serial")
    if args.skip_search_concurrent:
        cmd.append("--skip-search-concurrent")


def vdbbench_base_cmd(args: argparse.Namespace, base_url: str, index_name: str, command_name: str) -> list[str]:
    cmd = python_module_cmd(args, "vectordb_bench.cli.vectordbbench") + [
        command_name,
        "--base-url",
        base_url,
        "--index-name",
        index_name,
        "--timeout",
        str(args.client_timeout),
        "--m",
        str(args.m),
        "--ef-construction",
        str(args.ef_construction),
        "--ef-search",
        str(args.ef_search),
        "--case-type",
        args.case_type,
        "--k",
        str(args.k),
        "--num-concurrency",
        args.num_concurrency,
        "--concurrency-duration",
        str(args.concurrency_duration),
        "--db-label",
        args.db_label,
    ]
    append_stage_flags(cmd, args)
    if args.vdbbench_extra_args:
        cmd.extend(shlex.split(args.vdbbench_extra_args))
    return cmd


def vdbbench_row_env(args: argparse.Namespace, vectordbbench_dir: Path, gomap_root: Path, state: HarnessState, row: str = "") -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = pythonpath_for(vectordbbench_dir, gomap_root)
    env["RESULTS_LOCAL_DIR"] = str(state.root / "vdbbench-results" / row) if row else str(state.root / "vdbbench-results")
    env["LOG_FILE"] = str(state.root / "vdbbench.log")
    env["NUM_PER_BATCH"] = str(args.num_per_batch)
    if getattr(args, "lifecycle", False):
        env["TREEDB_LIFECYCLE_SIDECAR"] = str(state.root / "adapter-lifecycle.jsonl")
        env["TREEDB_LIFECYCLE_BOUNDARY_ACK"] = str(state.root / "lifecycle-boundary-diagnostics.json")
    return env


def vdbbench_result_files(root: Path) -> set[Path]:
    return set(root.rglob("result_*.json")) if root.exists() else set()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_identity(path: Path) -> dict[str, Any]:
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def lifecycle_config_sha256(manifest: dict[str, Any]) -> str:
    service = manifest.get("service")
    return canonical_sha256({
        "service_profile": service.get("profile") if isinstance(service, dict) else None,
        "service_command": service.get("command") if isinstance(service, dict) else None,
        "harness": manifest.get("harness"),
    })


def _artifact_file(root: Path, relative: Any, label: str, errors: list[str]) -> Path | None:
    if not isinstance(relative, str) or not relative:
        errors.append(f"{label} path must be a non-empty relative path")
        return None
    try:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            errors.append(f"{label} path escapes artifact root: {relative!r}")
            return None
        resolved_root = root.resolve()
        resolved = (resolved_root / path).resolve()
    except (OSError, ValueError) as exc:
        errors.append(f"{label} path is invalid: {exc}")
        return None
    try:
        resolved.relative_to(resolved_root)
    except ValueError:
        errors.append(f"{label} path escapes artifact root: {relative!r}")
        return None
    return resolved


def _nonnegative_int(value: Any, label: str, errors: list[str]) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        errors.append(f"{label} must be a non-negative integer")
        return None
    return value


def _object(value: Any, label: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{label} must be an object")
        return {}
    return value


def _utc_timestamp(value: Any, label: str, errors: list[str]) -> _dt.datetime | None:
    if not isinstance(value, str) or re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
        value,
    ) is None:
        errors.append(f"{label} must be an RFC3339 timestamp")
        return None
    try:
        parsed = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, OverflowError):
        errors.append(f"{label} must be an RFC3339 timestamp")
        return None
    if parsed.tzinfo is None:
        errors.append(f"{label} must include a timezone")
        return None
    try:
        return parsed.astimezone(_dt.timezone.utc)
    except (ValueError, OverflowError):
        errors.append(f"{label} must be an RFC3339 timestamp")
        return None


_UTC_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def _datetime_from_ns(value: int) -> _dt.datetime:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("nanosecond timestamp must be an integer")
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    try:
        return _UTC_EPOCH + _dt.timedelta(
            seconds=seconds, microseconds=nanoseconds // 1_000
        )
    except OverflowError as exc:
        raise ValueError("nanosecond timestamp is outside the supported UTC datetime range") from exc


def _utc_datetime_from_ns(value: Any, label: str, errors: list[str]) -> _dt.datetime | None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        errors.append(f"{label} must be a positive integer")
        return None
    try:
        return _datetime_from_ns(value)
    except ValueError:
        errors.append(f"{label} is outside the supported UTC datetime range")
        return None


def _strict_json_loads(value: str) -> Any:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number {constant}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key {key!r}")
            result[key] = item
        return result

    def finite_float(number: str) -> float:
        parsed = float(number)
        if not math.isfinite(parsed):
            raise ValueError(f"non-finite JSON number {number}")
        return parsed

    return json.loads(
        value,
        parse_constant=reject_constant,
        parse_float=finite_float,
        object_pairs_hook=reject_duplicate_keys,
    )


class LifecycleJournal:
    """Append lifecycle stages without rewriting prior evidence."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._sequence = 0
        if path.exists():
            raw = path.read_text(encoding="utf-8")
            if raw and not raw.endswith("\n"):
                raise ValueError(f"lifecycle journal {path} has a partial final record")
            for expected, line in enumerate(raw.splitlines()):
                event = _strict_json_loads(line)
                if not isinstance(event, dict) or event.get("sequence") != expected:
                    raise ValueError(f"lifecycle journal {path} has invalid sequence {expected}")
                self._sequence += 1

    def append(self, stage: str, state: dict[str, Any], *, timestamp: str | None = None) -> int:
        if stage not in LIFECYCLE_STAGES:
            raise ValueError(f"unknown lifecycle stage {stage!r}")
        if not isinstance(state, dict):
            raise TypeError("lifecycle state must be an object")
        with self._lock:
            sequence = self._sequence
            event = {
                "schema_version": LIFECYCLE_EVENT_SCHEMA,
                "sequence": sequence,
                "stage": stage,
                "timestamp": timestamp or iso_now(),
                "state": state,
            }
            payload = (json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n").encode()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                if os.write(fd, payload) != len(payload):
                    raise OSError("short lifecycle journal write")
                os.fsync(fd)
            finally:
                os.close(fd)
            self._sequence += 1
            return sequence


class DiagnosticsSampler:
    """Persist bounded service snapshots while the blocking benchmark runs."""

    def __init__(self, url: str, path: Path, interval: float, data_dir: Path):
        self.url = url
        self.path = path
        self.interval = interval
        self.data_dir = data_dir
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._stopped = False
        self._sample_lock = threading.Lock()

    def sample(self, *, boundary: str | None = None, boundary_timestamp_ns: int | None = None) -> dict[str, Any]:
        with self._sample_lock:
            wal_dir = self.data_dir / "maindb" / "wal"
            wal_files = []
            with contextlib.suppress(OSError):
                wal_files = [path for path in wal_dir.iterdir() if path.is_file()]
            wal_bytes = 0
            for path in wal_files:
                with contextlib.suppress(OSError):
                    wal_bytes += path.stat().st_size
            record = {
                "timestamp_ns": time.time_ns(),
                "snapshot": http_json("GET", self.url, timeout=2.0),
                "wal_filesystem": {
                    "path": str(wal_dir),
                    "files": len(wal_files),
                    "bytes": wal_bytes,
                },
            }
            if boundary is not None:
                record["boundary"] = boundary
                record["boundary_timestamp_ns"] = boundary_timestamp_ns
            payload = (json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                if os.write(fd, payload) != len(payload):
                    raise OSError("short diagnostics sample write")
            finally:
                os.close(fd)
            self.samples.append(record)
            return record

    def start(self) -> None:
        self.sample()

        def run() -> None:
            while not self._stop.wait(self.interval):
                with contextlib.suppress(Exception):
                    self.sample()

        self._thread = threading.Thread(target=run, name="treedb-lifecycle-diagnostics", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval * 2))
        with contextlib.suppress(Exception):
            self.sample()

    def at(self, timestamp_ns: int) -> dict[str, Any]:
        eligible = [sample for sample in self.samples if sample["timestamp_ns"] <= timestamp_ns]
        selected = eligible[-1] if eligible else (self.samples[0] if self.samples else None)
        return selected["snapshot"] if selected else {}

    def latest(self) -> dict[str, Any]:
        return self.samples[-1]["snapshot"] if self.samples else {}


def capture_lifecycle_boundary_diagnostics(
    sidecar: Path,
    acknowledgement: Path,
    sampler: DiagnosticsSampler,
    stop: threading.Event,
) -> None:
    """Synchronously sample each load/build boundary while the adapter is paused."""
    next_boundary = 0
    observed_size = -1
    while not stop.wait(0.1):
        try:
            current_size = sidecar.stat().st_size
        except OSError:
            continue
        if current_size <= observed_size:
            continue
        observed_size = current_size
        try:
            records = read_adapter_lifecycle_records(sidecar)
        except ValueError:
            continue
        while next_boundary < len(LIFECYCLE_DIAGNOSTIC_BOUNDARIES):
            boundary = LIFECYCLE_DIAGNOSTIC_BOUNDARIES[next_boundary]
            matches = [record for record in records if record.get("event") == boundary]
            if not matches:
                break
            if len(matches) != 1:
                raise ValueError(f"adapter lifecycle sidecar has duplicate {boundary} boundaries")
            boundary_ns = matches[0]["timestamp_ns"]
            sample = sampler.sample(boundary=boundary, boundary_timestamp_ns=boundary_ns)
            sample_ns = sample["timestamp_ns"]
            if sample_ns < boundary_ns:
                raise RuntimeError(f"{boundary} diagnostics sample predates the adapter boundary")
            temporary = acknowledgement.with_name(f".{acknowledgement.name}.{os.getpid()}.tmp")
            try:
                write_json(temporary, {
                    "boundary": boundary,
                    "boundary_timestamp_ns": boundary_ns,
                    "sample_timestamp_ns": sample_ns,
                })
                os.replace(temporary, acknowledgement)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
            next_boundary += 1
            if next_boundary == len(LIFECYCLE_DIAGNOSTIC_BOUNDARIES):
                return


def boundary_diagnostics_snapshot(
    boundary: str, boundary_ns: int, sampler: DiagnosticsSampler
) -> dict[str, Any]:
    matches = [
        record for record in sampler.samples
        if record.get("boundary") == boundary
        and record.get("boundary_timestamp_ns") == boundary_ns
        and isinstance(record.get("timestamp_ns"), int)
        and not isinstance(record.get("timestamp_ns"), bool)
        and record["timestamp_ns"] >= boundary_ns
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("snapshot"), dict):
        raise ValueError(f"{boundary} diagnostics has no exact sampled snapshot")
    return matches[0]["snapshot"]


def read_adapter_lifecycle_records(path: Path) -> list[dict[str, Any]]:
    """Read structurally complete sidecar records without inventing boundaries."""
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read adapter lifecycle sidecar: {exc}") from exc
    if not raw or not raw.endswith("\n"):
        raise ValueError("adapter lifecycle sidecar is empty or has a partial final record")
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        if not line:
            raise ValueError(f"adapter lifecycle sidecar line {line_number} is blank")
        try:
            record = _strict_json_loads(line)
        except ValueError as exc:
            raise ValueError(f"adapter lifecycle sidecar line {line_number} is invalid: {exc}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"adapter lifecycle sidecar line {line_number} must be an object")
        timestamp_ns = record.get("timestamp_ns")
        if not isinstance(timestamp_ns, int) or isinstance(timestamp_ns, bool) or timestamp_ns <= 0:
            raise ValueError(f"adapter lifecycle sidecar line {line_number} timestamp_ns must be a positive integer")
        try:
            _datetime_from_ns(timestamp_ns)
        except ValueError as exc:
            raise ValueError(
                f"adapter lifecycle sidecar line {line_number} timestamp_ns is outside "
                "the supported UTC datetime range"
            ) from exc
        records.append(record)

    return records


def read_adapter_lifecycle_sidecar(path: Path) -> dict[str, Any]:
    """Read the adapter's complete boundary stream, rejecting partial evidence."""
    records = read_adapter_lifecycle_records(path)
    events = [record.get("event") for record in records]
    tail = ["load_end", "optimize_start", "optimize_end", "cache_prime", "cache_warm"]
    if len(records) < 8 or events[:2] != ["reset", "load_start"] or events[-5:] != tail:
        raise ValueError("adapter lifecycle sidecar is missing required ordered boundaries")
    batches = records[2:-5]
    if not batches or any(record.get("event") != "batch_accepted" for record in batches):
        raise ValueError("adapter lifecycle sidecar must contain only accepted batches between load boundaries")
    reset_ns = records[0]["timestamp_ns"]
    load_start_ns = records[1]["timestamp_ns"]
    load_end_ns = records[-5]["timestamp_ns"]
    optimize_start_ns = records[-4]["timestamp_ns"]
    optimize_end_ns = records[-3]["timestamp_ns"]
    cache_prime_ns = records[-2]["timestamp_ns"]
    cache_warm_ns = records[-1]["timestamp_ns"]
    if not reset_ns <= load_start_ns <= load_end_ns <= optimize_start_ns <= optimize_end_ns < cache_prime_ns < cache_warm_ns:
        raise ValueError("adapter lifecycle boundary timestamps are out of order")
    if any(not load_start_ns <= record["timestamp_ns"] <= load_end_ns for record in batches):
        raise ValueError("adapter lifecycle batch timestamp falls outside the load boundaries")
    reset_response = records[0].get("response")
    optimize_response = records[-3].get("response")
    if not isinstance(reset_response, dict) or not isinstance(optimize_response, dict):
        raise ValueError("adapter lifecycle reset and optimize responses must be objects")
    client_sent = 0
    server_accepted = 0
    for record in batches:
        sent = record.get("client_sent")
        accepted = record.get("server_accepted")
        if (
            not isinstance(sent, int)
            or isinstance(sent, bool)
            or sent <= 0
            or not isinstance(accepted, int)
            or isinstance(accepted, bool)
            or accepted < 0
            or accepted > sent
        ):
            raise ValueError("adapter lifecycle batch counts are invalid")
        client_sent += sent
        server_accepted += accepted
    return {
        "records": records,
        "client_sent": client_sent,
        "server_accepted": server_accepted,
        "reset_response": reset_response,
        "reset_ns": reset_ns,
        "load_start_ns": load_start_ns,
        "load_end_ns": load_end_ns,
        "optimize_start_ns": optimize_start_ns,
        "optimize_end_ns": optimize_end_ns,
        "cache_prime_ns": cache_prime_ns,
        "cache_warm_ns": cache_warm_ns,
        "optimize_response": optimize_response,
    }


def _valid_go_int64(value: str) -> bool:
    unsigned = value
    negative = False
    if unsigned.startswith(("+", "-")):
        negative = unsigned[0] == "-"
        unsigned = unsigned[1:]
    if not unsigned:
        return False
    base = 10
    digits = unsigned
    prefixed = False
    if unsigned.startswith(("0b", "0B")):
        base, digits, prefixed = 2, unsigned[2:], True
    elif unsigned.startswith(("0o", "0O")):
        base, digits, prefixed = 8, unsigned[2:], True
    elif unsigned.startswith(("0x", "0X")):
        base, digits, prefixed = 16, unsigned[2:], True
    elif len(unsigned) > 1 and unsigned.startswith("0"):
        base, digits, prefixed = 8, unsigned[1:], True
    if prefixed and digits.startswith("_"):
        digits = digits[1:]
    if not digits or digits.startswith("_") or digits.endswith("_") or "__" in digits:
        return False
    allowed = {2: r"[01_]+", 8: r"[0-7_]+", 10: r"[0-9_]+", 16: r"[0-9a-fA-F_]+"}
    if re.fullmatch(allowed[base], digits) is None:
        return False
    try:
        number = int(digits.replace("_", ""), base)
    except ValueError:
        return False
    if negative:
        number = -number
    return -(1 << 63) <= number < (1 << 63)


def _valid_pprof_listen_address(value: str) -> bool:
    if not value:
        return True
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0 or value[closing + 1:closing + 2] != ":":
            return False
        host = value[1:closing]
        port = value[closing + 2:]
        if ":" in port:
            return False
    else:
        if value.count(":") != 1:
            return False
        host, port = value.rsplit(":", 1)
    if "%" in host or re.fullmatch(r"[0-9]{1,5}", port) is None or not 1 <= int(port) <= 65535:
        return False
    return loopback_host(host)


def _pprof_metadata(path: Path) -> bytes | None:
    try:
        process = subprocess.Popen(
            ("go", "tool", "pprof", "-raw", str(path)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return None
    prefix = bytearray()

    def drain() -> None:
        assert process.stdout is not None
        with process.stdout:
            while chunk := process.stdout.read(64 * 1024):
                if len(prefix) < 64 * 1024:
                    prefix.extend(chunk[:64 * 1024 - len(prefix)])

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        returncode = process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass
        process.wait()
        reader.join()
        return None
    reader.join()
    return bytes(prefix) if returncode == 0 else None


def _valid_profile_payload(kind: Any, path: Path, data: bytes) -> bool:
    if not isinstance(kind, str):
        return False
    if kind in PPROF_PROFILE_KINDS:
        if path.suffix != ".pprof" or not data.startswith(b"\x1f\x8b\x08"):
            return False
        try:
            with gzip.open(path, "rb") as source:
                saw_data = False
                while chunk := source.read(64 * 1024):
                    saw_data = True
            if not saw_data:
                return False
            metadata = _pprof_metadata(path)
            if metadata is None:
                return False
            lines = metadata.decode("utf-8").splitlines()
            samples_position = lines.index("Samples:")
            sample_tokens = lines[samples_position + 1].split()
            saw_sample = False
            for line in lines[samples_position + 2:]:
                if line in {"Locations", "Mappings"}:
                    break
                values, separator, _ = line.partition(":")
                if separator and len(values.split()) == len(sample_tokens):
                    try:
                        tuple(int(value) for value in values.split())
                    except ValueError:
                        continue
                    saw_sample = True
                    break
            if not saw_sample:
                return False
            sample_types = {token.removesuffix("[dflt]") for token in sample_tokens}
            default_types = {
                token.removesuffix("[dflt]") for token in sample_tokens if token.endswith("[dflt]")
            }
            if kind == "cpu":
                required = {"samples/count", "cpu/nanoseconds"}
                return "PeriodType: cpu nanoseconds" in lines and required <= sample_types
            if kind in {"heap", "allocs"}:
                required = {
                    "alloc_objects/count", "alloc_space/bytes", "inuse_objects/count", "inuse_space/bytes",
                }
                expected_default = "inuse_space/bytes" if kind == "heap" else "alloc_space/bytes"
                valid_defaults = (
                    {frozenset(), frozenset({expected_default})}
                    if kind == "heap"
                    else {frozenset({expected_default})}
                )
                return (
                    "PeriodType: space bytes" in lines
                    and required <= sample_types
                    and frozenset(default_types) in valid_defaults
                )
            required = {"contentions/count", "delay/nanoseconds"}
            return "PeriodType: contentions count" in lines and required <= sample_types
        except (OSError, EOFError, UnicodeError, ValueError, IndexError, zlib.error):
            return False
    if kind == "trace":
        header = re.match(rb"go 1\.[0-9]+ trace\x00\x00\x00", data)
        if path.suffix != ".out" or header is None or len(data) <= header.end():
            return False
        try:
            decoded = subprocess.run(
                ("go", "tool", "trace", "-d=parsed", str(path)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            return decoded.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False
    if kind == "perf":
        if path.suffix != ".data" or data[:8] not in {b"PERFILE2", b"2ELIFREP"}:
            return False
        byteorder = "little" if data.startswith(b"PERFILE2") else "big"
        try:
            file_size = path.stat().st_size
            with path.open("rb") as source:
                header = source.read(104)
                if len(header) != 104:
                    return False
                header_size = int.from_bytes(header[8:16], byteorder)
                attr_size = int.from_bytes(header[16:24], byteorder)
                if header_size < 104 or header_size > file_size or attr_size == 0:
                    return False
                attrs_size = int.from_bytes(header[32:40], byteorder)
                if attrs_size == 0 or attr_size > attrs_size or attrs_size % attr_size != 0:
                    return False
                sections = []
                for start in (24, 40, 56):
                    offset = int.from_bytes(header[start:start + 8], byteorder)
                    size = int.from_bytes(header[start + 8:start + 16], byteorder)
                    if size:
                        if offset < header_size or offset > file_size - size:
                            return False
                        sections.append((offset, offset + size))
                sections.sort()
                if any(left[1] > right[0] for left, right in zip(sections, sections[1:])):
                    return False
                data_offset = int.from_bytes(header[40:48], byteorder)
                data_size = int.from_bytes(header[48:56], byteorder)
                if data_size < 8:
                    return False
                source.seek(data_offset)
                remaining = data_size
                saw_sample = False
                while remaining:
                    if remaining < 8:
                        return False
                    event_header = source.read(8)
                    if len(event_header) != 8:
                        return False
                    event_type = int.from_bytes(event_header[:4], byteorder)
                    event_size = int.from_bytes(event_header[6:8], byteorder)
                    if event_size < 8 or event_size > remaining:
                        return False
                    if event_type == 9 and event_size > 8:  # PERF_RECORD_SAMPLE
                        saw_sample = True
                    source.seek(event_size - 8, os.SEEK_CUR)
                    remaining -= event_size
                if not saw_sample:
                    return False
            decoded = subprocess.run(
                ("perf", "script", "-i", str(path)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            return decoded.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False
    return False


def validate_lifecycle_artifact(root: Path) -> dict[str, Any]:
    """Validate a lifecycle extension of the existing artifact-v1 envelope."""
    root = root.resolve()
    errors: list[str] = []
    completion_errors: list[str] = []
    report: dict[str, Any] = {
        "schema_version": LIFECYCLE_VALIDATION_SCHEMA,
        "artifact_root": str(root),
        "analyzable": False,
        "complete": False,
        "result_status": None,
        "last_stage": None,
        "counts": None,
        "t_ready_seconds": None,
        "errors": errors,
        "completion_errors": completion_errors,
    }
    manifest_path = root / "manifest.json"
    try:
        manifest = _strict_json_loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        errors.append(f"cannot read manifest.json: {exc}")
        return report
    if not isinstance(manifest, dict):
        errors.append("manifest.json must contain an object")
        return report
    if manifest.get("schema_version") != ARTIFACT_SCHEMA:
        errors.append(f"manifest schema_version must be {ARTIFACT_SCHEMA!r}")
    lifecycle = manifest.get("lifecycle")
    if not isinstance(lifecycle, dict):
        errors.append("manifest.lifecycle must be an object")
        return report
    if lifecycle.get("schema_version") != LIFECYCLE_SCHEMA:
        errors.append(f"lifecycle schema_version must be {LIFECYCLE_SCHEMA!r}")

    expected_rows = _nonnegative_int(lifecycle.get("expected_rows"), "lifecycle.expected_rows", errors)
    if expected_rows == 0:
        errors.append("lifecycle.expected_rows must be positive")
    dataset = _object(lifecycle.get("dataset"), "lifecycle.dataset", errors)
    if not isinstance(dataset.get("name"), str) or not dataset.get("name"):
        errors.append("lifecycle.dataset.name must be non-empty")
    dataset_sha256 = dataset.get("sha256")
    if not isinstance(dataset_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", dataset_sha256):
        errors.append("lifecycle.dataset.sha256 must be a lowercase SHA-256")
    dimensions = _nonnegative_int(dataset.get("dimensions"), "lifecycle.dataset.dimensions", errors)
    if dimensions == 0:
        errors.append("lifecycle.dataset.dimensions must be positive")
    vectors = _nonnegative_int(dataset.get("vectors"), "lifecycle.dataset.vectors", errors)
    if expected_rows is not None and vectors is not None and vectors != expected_rows:
        errors.append("lifecycle.dataset.vectors must equal lifecycle.expected_rows")

    context = _object(manifest.get("context"), "manifest.context", errors)
    gomap = _object(context.get("gomap"), "manifest.context.gomap", errors)
    vectordbbench = _object(context.get("vectordbbench"), "manifest.context.vectordbbench", errors)
    service = _object(manifest.get("service"), "manifest.service", errors)
    binary = _object(service.get("binary"), "manifest.service.binary", errors)
    identity = _object(lifecycle.get("identity"), "lifecycle.identity", errors)
    harness = _object(manifest.get("harness"), "manifest.harness", errors)
    if harness.get("mode") != "vdbbench+lifecycle":
        errors.append("manifest.harness.mode must be 'vdbbench+lifecycle' for lifecycle artifacts")
    if manifest.get("route_proof") is not None:
        errors.append("lifecycle artifacts must not claim the independent route_proof.json smoke")
    manifest_vdbbench = manifest.get("vdbbench")
    if not isinstance(manifest_vdbbench, list):
        errors.append("manifest.vdbbench must be a list")
        manifest_vdbbench = []
    if service.get("profile") not in ("command_wal_durable", "command_wal_relaxed", "no_wal_fast"):
        errors.append("manifest.service.profile must name a canonical public profile")
    service_command = service.get("command")
    effective_dir = None
    effective_pprof = None
    if (
        not isinstance(service_command, list)
        or not service_command
        or any(not isinstance(argument, str) or not argument for argument in service_command)
    ):
        errors.append("manifest.service.command must be a non-empty argv list of strings")
    else:
        known_flags = {
            "addr", "dir", "profile", "pprof", "block-profile-rate", "mutex-profile-fraction",
        }
        profile_values = []
        effective_dir = "/tmp/treedb-document-service"
        effective_pprof = ""
        invalid_command = False
        parsing_flags = True
        position = 1
        while position < len(service_command):
            argument = service_command[position]
            if not parsing_flags:
                trailing_flag = argument[2:] if argument.startswith("--") else argument[1:]
                if argument.startswith("-") and trailing_flag.partition("=")[0] == "profile":
                    invalid_command = True
                position += 1
                continue
            if argument == "--":
                parsing_flags = False
                position += 1
                continue
            if not argument.startswith("-") or argument == "-":
                parsing_flags = False
                position += 1
                continue
            flag = argument[2:] if argument.startswith("--") else argument[1:]
            name, separator, inline_value = flag.partition("=")
            if name not in known_flags:
                invalid_command = True
                position += 1
                continue
            if separator:
                value = inline_value
                position += 1
            elif position + 1 < len(service_command):
                value = service_command[position + 1]
                position += 2
            else:
                invalid_command = True
                break
            if name in {"block-profile-rate", "mutex-profile-fraction"} and not _valid_go_int64(value):
                invalid_command = True
            if name == "profile":
                profile_values.append(value)
            elif name == "dir":
                effective_dir = value
            elif name == "pprof":
                effective_pprof = value
        if (
            invalid_command
            or not effective_dir
            or effective_pprof is None
            or not _valid_pprof_listen_address(effective_pprof)
            or len(profile_values) != 1
            or profile_values[0] != service.get("profile")
        ):
            errors.append("manifest.service.command has invalid flags or does not select exactly one matching profile")
    declared_data_dir = service.get("data_dir")
    if not isinstance(declared_data_dir, str) or not declared_data_dir:
        errors.append("manifest.service.data_dir must be a non-empty absolute path")
    elif effective_dir is not None:
        try:
            declared_path = Path(declared_data_dir)
            effective_path = Path(effective_dir)
            if not declared_path.is_absolute() or not effective_path.is_absolute():
                raise ValueError("declared and effective paths must be absolute")
            declared_resolved = declared_path.resolve(strict=False)
            effective_resolved = effective_path.resolve(strict=False)
            artifact_data_dir = root / "treedb-data"
            if artifact_data_dir.is_symlink():
                errors.append("manifest.service.data_dir must not be a symlink")
            expected_data_dir = artifact_data_dir.resolve(strict=False)
            if declared_resolved != effective_resolved:
                errors.append("manifest.service.data_dir does not match the effective service -dir")
            if declared_resolved != expected_data_dir or effective_resolved != expected_data_dir:
                errors.append("manifest.service.data_dir must be the artifact-owned treedb-data directory")
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(f"manifest.service.data_dir is invalid: {exc}")
    binary_path = binary.get("path")
    binary_digest = None
    if not isinstance(binary_path, str) or not binary_path:
        errors.append("manifest.service.binary.path must be a non-empty string")
    else:
        try:
            declared_binary = Path(binary_path)
            if not declared_binary.is_absolute():
                raise ValueError("path is not absolute")
            resolved_binary = declared_binary.resolve(strict=True)
            if not resolved_binary.is_file():
                raise ValueError("path is not a regular file")
            if not os.access(resolved_binary, os.R_OK | os.X_OK):
                raise PermissionError("path is not readable and executable")
            binary_digest = sha256_file(resolved_binary)
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(f"manifest service binary is unavailable or invalid: {exc}")
        if (
            isinstance(service_command, list)
            and service_command
            and isinstance(service_command[0], str)
            and service_command[0] != binary_path
        ):
            errors.append("manifest.service.command[0] must match manifest.service.binary.path")
    bound_index_name: str | None = None
    case_type = harness.get("case_type")
    if not isinstance(case_type, str) or not case_type.strip():
        errors.append("manifest.harness.case_type must be non-empty")
    elif case_type == "PerformanceCustomDataset":
        binding = lifecycle.get("task_config_binding")
        if not isinstance(binding, dict):
            completion_errors.append("custom case cannot complete without task_config dataset shape evidence")
        else:
            for key in ("result_file", "result_sha256", "task_config_sha256"):
                value = binding.get(key)
                if not isinstance(value, str) or not value:
                    errors.append(f"lifecycle.task_config_binding.{key} must be a non-empty string")
            candidates = [
                row.get("load_metrics")
                for row in manifest_vdbbench
                if isinstance(row, dict) and isinstance(row.get("load_metrics"), dict)
            ]
            matches = [
                item for item in candidates
                if item.get("result_file") == binding.get("result_file")
                and item.get("result_sha256") == binding.get("result_sha256")
                and item.get("task_config_sha256") == binding.get("task_config_sha256")
            ]
            if len(matches) != 1:
                errors.append("lifecycle task_config binding does not select one manifest VDBBench result")
            else:
                selected = matches[0]
                selected_index_name = selected.get("index_name")
                if not isinstance(selected_index_name, str) or not selected_index_name:
                    errors.append("bound manifest VDBBench result index_name must be a non-empty string")
                else:
                    bound_index_name = selected_index_name
                task_config = selected.get("task_config")
                if not isinstance(task_config, dict) or canonical_sha256(task_config) != binding.get("task_config_sha256"):
                    errors.append("lifecycle task_config binding checksum does not match canonical task_config")
                else:
                    try:
                        custom_vectors, custom_dimensions = custom_task_config_shape(task_config)
                        selected_dataset = custom_task_config_dataset_file(task_config)
                        selected_digest = sha256_file(selected_dataset)
                    except (OSError, RuntimeError, ValueError) as exc:
                        errors.append(f"custom task_config dataset evidence is invalid: {exc}")
                    else:
                        if custom_vectors != expected_rows or custom_vectors != vectors or custom_dimensions != dimensions:
                            errors.append("custom task_config dataset shape does not match lifecycle dataset")
                        if selected_digest != dataset.get("sha256"):
                            errors.append("custom task_config selected dataset checksum does not match lifecycle dataset")
                result_path = _artifact_file(root, binding.get("result_file"), "task_config result", errors)
                if result_path is not None:
                    try:
                        result_digest = sha256_file(result_path)
                        result_document = _strict_json_loads(result_path.read_text(encoding="utf-8"))
                    except (OSError, UnicodeError, ValueError) as exc:
                        errors.append(f"cannot read task_config result: {exc}")
                    else:
                        if result_digest != binding.get("result_sha256"):
                            errors.append("task_config result checksum does not match lifecycle binding")
                        result_rows = result_document.get("results") if isinstance(result_document, dict) else None
                        if not isinstance(result_rows, list):
                            errors.append("task_config result results must be a list")
                            result_rows = []
                        result_configs = [
                            item.get("task_config")
                            for item in result_rows
                            if isinstance(item, dict) and isinstance(item.get("task_config"), dict)
                            and canonical_sha256(item["task_config"]) == binding.get("task_config_sha256")
                        ]
                        if len(result_configs) != 1 or result_configs[0] != selected.get("task_config"):
                            errors.append("task_config result does not contain the uniquely bound manifest task_config")
    else:
        try:
            case_vectors = case_vector_count(case_type)
            case_dimensions = case_vector_dimensions(case_type)
        except ValueError as exc:
            completion_errors.append(str(exc))
        else:
            if (
                (expected_rows is not None and case_vectors != expected_rows)
                or (vectors is not None and case_vectors != vectors)
            ):
                completion_errors.append(
                    "standard case vector count does not match lifecycle.expected_rows/dataset.vectors"
                )
            if dimensions is not None and case_dimensions != dimensions:
                completion_errors.append("standard case dimensions do not match lifecycle.dataset.dimensions")
    concurrency = harness.get("num_concurrency")
    if isinstance(concurrency, int) and not isinstance(concurrency, bool):
        valid_concurrency = 0 < concurrency <= 999_999_999
    elif isinstance(concurrency, str):
        parts = [part.strip() for part in concurrency.split(",")]
        valid_concurrency = bool(parts) and all(
            re.fullmatch(r"[0-9]{1,9}", part) is not None and int(part) > 0 for part in parts
        )
    else:
        valid_concurrency = False
    if not valid_concurrency:
        errors.append("manifest.harness.num_concurrency must contain positive integers")
    for key in ("num_per_batch", "m", "ef_construction"):
        value = _nonnegative_int(harness.get(key), f"manifest.harness.{key}", errors)
        if value == 0:
            errors.append(f"manifest.harness.{key} must be positive")
    identities = (
        ("gomap_commit", gomap.get("commit"), 40),
        ("vectordbbench_commit", vectordbbench.get("commit"), 40),
        ("service_binary_sha256", binary.get("sha256"), 64),
    )
    for name, actual, width in identities:
        declared = identity.get(name)
        actual_valid = isinstance(actual, str) and re.fullmatch(rf"[0-9a-f]{{{width}}}", actual)
        declared_valid = isinstance(declared, str) and re.fullmatch(rf"[0-9a-f]{{{width}}}", declared)
        if not actual_valid:
            errors.append(f"manifest {name} is missing or invalid")
        if not declared_valid:
            errors.append(f"lifecycle identity {name} is missing or invalid")
        elif actual_valid and declared != actual:
            errors.append(f"lifecycle identity {name} does not match manifest")
    if binary_digest is not None:
        if binary.get("sha256") != binary_digest:
            errors.append("manifest service binary SHA-256 does not match current binary bytes")
        if identity.get("service_binary_sha256") != binary_digest:
            errors.append("lifecycle identity service_binary_sha256 does not match current binary bytes")
    if gomap.get("dirty") is not False or vectordbbench.get("dirty") is not False:
        errors.append("gomap and VectorDBBench checkouts must be clean")
    config_sha256 = lifecycle_config_sha256(manifest)
    if identity.get("config_sha256") != config_sha256:
        errors.append("lifecycle identity config_sha256 does not match manifest configuration")

    host = _object(context.get("host"), "manifest.context.host", errors)
    for key in ("logical_cpu_count", "physical_cpu_count", "memory_bytes"):
        value = _nonnegative_int(host.get(key), f"manifest context.host.{key}", errors)
        if value == 0:
            errors.append(f"manifest context.host.{key} must be positive")
    if not valid_storage_context(host.get("storage")):
        errors.append("manifest context.host.storage must contain method, device, filesystem, mount, and positive capacity")

    lifecycle_path = _artifact_file(root, lifecycle.get("file"), "lifecycle", errors)
    events: list[dict[str, Any]] = []
    if lifecycle_path is not None:
        try:
            raw_lifecycle = lifecycle_path.read_bytes()
        except OSError as exc:
            errors.append(f"cannot read lifecycle JSONL: {exc}")
        else:
            actual_hash = hashlib.sha256(raw_lifecycle).hexdigest()
            if lifecycle.get("sha256") != actual_hash:
                errors.append("lifecycle JSONL checksum mismatch")
            try:
                lifecycle_text = raw_lifecycle.decode("utf-8")
            except UnicodeDecodeError as exc:
                errors.append(f"lifecycle JSONL is not UTF-8: {exc}")
                lifecycle_text = ""
            for line_number, raw_line in enumerate(lifecycle_text.splitlines(), 1):
                if not raw_line.strip():
                    errors.append(f"lifecycle JSONL line {line_number} is blank")
                    continue
                try:
                    event = _strict_json_loads(raw_line)
                except ValueError as exc:
                    errors.append(f"lifecycle JSONL line {line_number} is invalid: {exc}")
                    continue
                if not isinstance(event, dict):
                    errors.append(f"lifecycle JSONL line {line_number} must be an object")
                    continue
                events.append(event)
    if not events:
        errors.append("lifecycle JSONL has no events")

    raw_by_path: dict[str, str] = {}
    raw_artifacts = lifecycle.get("raw_artifacts")
    if not isinstance(raw_artifacts, list):
        errors.append("lifecycle.raw_artifacts must be a list")
        raw_artifacts = []
    for position, artifact in enumerate(raw_artifacts):
        if not isinstance(artifact, dict):
            errors.append(f"raw artifact {position} must be an object")
            continue
        relative = artifact.get("path")
        path = _artifact_file(root, relative, f"raw artifact {position}", errors)
        expected_hash = artifact.get("sha256")
        if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            errors.append(f"raw artifact {position} has invalid SHA-256")
            continue
        if not isinstance(relative, str):
            continue
        if relative in raw_by_path:
            errors.append(f"raw artifact path is duplicated: {relative}")
            continue
        raw_by_path[str(relative)] = str(expected_hash)
        if path is None:
            continue
        try:
            actual_hash = sha256_file(path)
        except OSError as exc:
            errors.append(f"cannot read raw artifact {relative}: {exc}")
        else:
            if actual_hash != expected_hash:
                errors.append(f"raw artifact checksum mismatch: {relative}")

    diagnostics_records: list[dict[str, Any]] | None = None
    boundary_acknowledgement: dict[str, Any] | None = None
    adapter_records: list[dict[str, Any]] | None = None
    if "adapter-lifecycle.jsonl" in raw_by_path:
        try:
            adapter_records = read_adapter_lifecycle_records(root / "adapter-lifecycle.jsonl")
        except ValueError as exc:
            errors.append(f"cannot parse adapter lifecycle sidecar: {exc}")
        else:
            for line_number, record in enumerate(adapter_records, start=1):
                _utc_datetime_from_ns(
                    record.get("timestamp_ns"),
                    f"adapter lifecycle line {line_number} timestamp_ns",
                    errors,
                )
    if "diagnostics.jsonl" in raw_by_path:
        try:
            diagnostics_text = (root / "diagnostics.jsonl").read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            errors.append(f"cannot read lifecycle diagnostics: {exc}")
        else:
            if not diagnostics_text or not diagnostics_text.endswith("\n"):
                errors.append("lifecycle diagnostics is empty or has a partial final record")
            diagnostics_records = []
            for line_number, line in enumerate(diagnostics_text.splitlines(), start=1):
                try:
                    record = _strict_json_loads(line)
                except ValueError as exc:
                    errors.append(f"lifecycle diagnostics line {line_number} is invalid: {exc}")
                    continue
                if not isinstance(record, dict):
                    errors.append(f"lifecycle diagnostics line {line_number} must be an object")
                    continue
                _utc_datetime_from_ns(
                    record.get("timestamp_ns"),
                    f"lifecycle diagnostics line {line_number} timestamp_ns",
                    errors,
                )
                if not isinstance(record.get("snapshot"), dict):
                    errors.append(f"lifecycle diagnostics line {line_number} snapshot must be an object")
                if "boundary" in record or "boundary_timestamp_ns" in record:
                    boundary = record.get("boundary")
                    boundary_timestamp_ns = record.get("boundary_timestamp_ns")
                    if boundary not in LIFECYCLE_DIAGNOSTIC_BOUNDARIES:
                        errors.append(
                            f"lifecycle diagnostics line {line_number} has an unknown boundary"
                        )
                    _utc_datetime_from_ns(
                        boundary_timestamp_ns,
                        f"lifecycle diagnostics line {line_number} boundary_timestamp_ns",
                        errors,
                    )
                diagnostics_records.append(record)
    if "lifecycle-boundary-diagnostics.json" in raw_by_path:
        try:
            parsed_acknowledgement = _strict_json_loads(
                (root / "lifecycle-boundary-diagnostics.json").read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(f"cannot parse lifecycle boundary acknowledgement: {exc}")
        else:
            if not isinstance(parsed_acknowledgement, dict):
                errors.append("lifecycle boundary acknowledgement must be an object")
            else:
                boundary_acknowledgement = parsed_acknowledgement
                if boundary_acknowledgement.get("boundary") not in LIFECYCLE_DIAGNOSTIC_BOUNDARIES:
                    errors.append("lifecycle boundary acknowledgement has an unknown boundary")
                for key in ("boundary_timestamp_ns", "sample_timestamp_ns"):
                    _utc_datetime_from_ns(
                        boundary_acknowledgement.get(key),
                        f"lifecycle boundary acknowledgement {key}",
                        errors,
                    )

    sequence_events: dict[int, dict[str, Any]] = {}
    stage_events: dict[str, dict[str, Any]] = {}
    previous_sequence = -1
    previous_timestamp: _dt.datetime | None = None
    previous_series: dict[str, int] = {}
    counter_keys: set[str] | None = None
    for position, event in enumerate(events):
        prefix = f"lifecycle event {position}"
        if event.get("schema_version") != LIFECYCLE_EVENT_SCHEMA:
            errors.append(f"{prefix} schema_version must be {LIFECYCLE_EVENT_SCHEMA!r}")
        sequence = _nonnegative_int(event.get("sequence"), f"{prefix} sequence", errors)
        if sequence is not None:
            if sequence <= previous_sequence:
                errors.append(f"{prefix} sequence must increase")
            if sequence in sequence_events:
                errors.append(f"{prefix} sequence is duplicated")
            sequence_events[sequence] = event
            previous_sequence = sequence
        timestamp = _utc_timestamp(event.get("timestamp"), f"{prefix} timestamp", errors)
        if timestamp is not None:
            if previous_timestamp is not None and timestamp < previous_timestamp:
                errors.append(f"{prefix} timestamp decreased")
            previous_timestamp = timestamp
            event["_timestamp"] = timestamp
        stage = event.get("stage")
        if not isinstance(stage, str) or not stage:
            errors.append(f"{prefix} stage must be non-empty")
        elif stage in LIFECYCLE_STAGES:
            if stage in stage_events:
                errors.append(f"required lifecycle stage is duplicated: {stage}")
            stage_events[stage] = event
        state = event.get("state")
        if not isinstance(state, dict):
            errors.append(f"{prefix} state must be an object")
            continue
        if "index" in state:
            index = state.get("index")
            if not isinstance(index, dict):
                errors.append(f"{prefix} state.index must be an object")
            else:
                if "identity" in index and not isinstance(index.get("identity"), str):
                    errors.append(f"{prefix} state.index.identity must be a string")
                generation = index.get("asset_generation")
                if "asset_generation" in index and (
                    isinstance(generation, bool) or not isinstance(generation, int)
                ):
                    errors.append(f"{prefix} state.index.asset_generation must be an integer")
                if "status" in index and not isinstance(index.get("status"), str):
                    errors.append(f"{prefix} state.index.status must be a string")
        route = state.get("route") if "route" in state else None
        if "route" in state:
            if not isinstance(route, dict):
                errors.append(f"{prefix} state.route must be an object")
            else:
                for key in ("name", "fallback_reason", "index_identity"):
                    if key in route and not isinstance(route.get(key), str):
                        errors.append(f"{prefix} state.route.{key} must be a string")
                if "optimized" in route and not isinstance(route.get("optimized"), bool):
                    errors.append(f"{prefix} state.route.optimized must be a boolean")
                generation = route.get("index_asset_generation")
                if "index_asset_generation" in route and (
                    isinstance(generation, bool) or not isinstance(generation, int)
                ):
                    errors.append(f"{prefix} state.route.index_asset_generation must be an integer")
                for key in ("service_generation", "requested_top_k", "result_count"):
                    value = route.get(key)
                    if key in route and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
                        errors.append(f"{prefix} state.route.{key} must be a positive integer")
        if stage == "route_verify":
            if not isinstance(route, dict):
                errors.append(f"{prefix} route_verify must contain a route proof object")
            else:
                for key in (
                    "name", "fallback_reason", "optimized", "index_identity", "index_asset_generation",
                    "service_generation", "requested_top_k", "result_count",
                ):
                    if key not in route:
                        errors.append(f"{prefix} state.route is missing required field {key}")
        rows = state.get("rows")
        wal = state.get("wal")
        counters = state.get("counters")
        if not isinstance(rows, dict) or not isinstance(wal, dict) or not isinstance(counters, dict):
            errors.append(f"{prefix} state must contain rows, wal, and counters objects")
            continue
        current_counter_keys = set(counters)
        if counter_keys is None:
            counter_keys = current_counter_keys
        elif current_counter_keys != counter_keys:
            errors.append(f"{prefix} cumulative counter keys changed")
        series = {
            **{f"rows.{key}": rows.get(key) for key in ("client_sent", "server_accepted", "server_durable", "reopened")},
            **{f"wal.{key}": wal.get(key) for key in ("frontier", "bytes_written_total")},
            **{f"counters.{key}": value for key, value in counters.items()},
        }
        validated: dict[str, int] = {}
        for name, value in series.items():
            parsed = _nonnegative_int(value, f"{prefix} {name}", errors)
            if parsed is None:
                continue
            validated[name] = parsed
            if name in previous_series and parsed < previous_series[name]:
                errors.append(f"{prefix} {name} decreased")
            previous_series[name] = parsed
        sent = validated.get("rows.client_sent")
        accepted = validated.get("rows.server_accepted")
        durable = validated.get("rows.server_durable")
        reopened = validated.get("rows.reopened")
        if None not in (sent, accepted, durable, reopened) and not (reopened <= durable <= accepted <= sent):
            errors.append(f"{prefix} row counts violate reopened <= durable <= accepted <= sent")
    if counter_keys == set():
        errors.append("lifecycle requires a non-empty cumulative counter key set")

    for stage in LIFECYCLE_STAGES:
        if stage not in stage_events:
            completion_errors.append(f"missing required stage {stage}")
    stage_order = [
        LIFECYCLE_STAGES.index(event.get("stage"))
        for event in events
        if event.get("stage") in LIFECYCLE_STAGES
    ]
    if stage_order != sorted(stage_order):
        errors.append("known lifecycle stages are out of order")

    status = lifecycle.get("result_status")
    report["result_status"] = status
    if not isinstance(status, str) or status not in {"completed", "partial", "interrupted"}:
        errors.append("lifecycle.result_status must be completed, partial, or interrupted")
    if status != "completed":
        completion_errors.append(f"result_status is {status!r}, not 'completed'")
    raw_optimize_reference: tuple[str, int] | None = None
    if status == "completed":
        required_evidence = {
            "adapter-lifecycle.jsonl",
            "diagnostics.jsonl",
            "lifecycle-boundary-diagnostics.json",
            "lifecycle_count_response.json",
            "lifecycle_load_milestones.json",
            "lifecycle_route_response.json",
            "service.log",
        }
        for relative in sorted(required_evidence - raw_by_path.keys()):
            errors.append(f"completed lifecycle requires checksum-bound raw artifact {relative}")
        adapter = None
        if "adapter-lifecycle.jsonl" in raw_by_path:
            try:
                adapter = read_adapter_lifecycle_sidecar(root / "adapter-lifecycle.jsonl")
            except ValueError as exc:
                errors.append(f"completed adapter lifecycle sidecar is invalid: {exc}")
        if adapter is not None:
            optimize_response = adapter["optimize_response"]
            optimize_index = optimize_response.get("index")
            response_index_name = (
                optimize_index.get("name") if isinstance(optimize_index, dict) else None
            )
            expected_index_name = bound_index_name or response_index_name
            if not isinstance(response_index_name, str) or not response_index_name:
                errors.append("adapter optimize response index name must be a non-empty string")
            elif case_type == "PerformanceCustomDataset" and response_index_name != bound_index_name:
                errors.append("adapter optimize response index name does not match bound VDBBench result")
            else:
                try:
                    raw_identity, raw_generation, _ = lifecycle_ready_asset(
                        optimize_response, expected_index_name
                    )
                except RuntimeError as exc:
                    errors.append(f"adapter optimize response does not prove a ready index: {exc}")
                else:
                    raw_optimize_reference = (raw_identity, raw_generation)
            load_end_state = (stage_events.get("load_end") or {}).get("state")
            load_end_rows = load_end_state.get("rows") if isinstance(load_end_state, dict) else None
            final_state = events[-1].get("state") if events else None
            final_rows = final_state.get("rows") if isinstance(final_state, dict) else None
            for key in ("client_sent", "server_accepted"):
                adapter_count = adapter[key]
                if adapter_count != expected_rows:
                    errors.append(
                        f"adapter lifecycle cumulative {key} does not equal lifecycle.expected_rows"
                    )
                if not isinstance(load_end_rows, dict) or load_end_rows.get(key) != adapter_count:
                    errors.append(
                        f"adapter lifecycle cumulative {key} does not match stage load_end rows.{key}"
                    )
                if not isinstance(final_rows, dict) or final_rows.get(key) != adapter_count:
                    errors.append(
                        f"adapter lifecycle cumulative {key} does not match final rows.{key}"
                    )
            if "lifecycle_load_milestones.json" in raw_by_path:
                try:
                    milestone_document = _strict_json_loads(
                        (root / "lifecycle_load_milestones.json").read_text(encoding="utf-8")
                    )
                except (OSError, UnicodeError, ValueError) as exc:
                    errors.append(f"cannot parse lifecycle load milestones: {exc}")
                else:
                    try:
                        expected_milestones = lifecycle_load_milestone_document(adapter["records"])
                    except ValueError as exc:
                        errors.append(f"cannot reconstruct lifecycle load milestones: {exc}")
                    else:
                        if milestone_document != expected_milestones:
                            errors.append(
                                "lifecycle load milestones do not match the adapter lifecycle sidecar"
                            )
            diagnostic_state_builder = LifecycleStateBuilder()
            for position, boundary in enumerate(LIFECYCLE_DIAGNOSTIC_BOUNDARIES):
                boundary_ns = adapter[f"{boundary}_ns"]
                event_timestamp = (stage_events.get(boundary) or {}).get("_timestamp")
                expected_timestamp = _utc_datetime_from_ns(
                    boundary_ns, f"adapter {boundary} timestamp_ns", errors
                )
                if expected_timestamp is not None and event_timestamp != expected_timestamp:
                    errors.append(
                        f"stage {boundary} timestamp does not match adapter lifecycle boundary"
                    )
                boundary_records = [
                    record for record in (diagnostics_records or [])
                    if record.get("boundary") == boundary
                ]
                sample_timestamp_ns = (
                    boundary_records[0].get("timestamp_ns") if len(boundary_records) == 1 else None
                )
                next_boundary_ns = (
                    adapter[f"{LIFECYCLE_DIAGNOSTIC_BOUNDARIES[position + 1]}_ns"]
                    if position + 1 < len(LIFECYCLE_DIAGNOSTIC_BOUNDARIES)
                    else None
                )
                if (
                    len(boundary_records) != 1
                    or boundary_records[0].get("boundary_timestamp_ns") != boundary_ns
                    or not isinstance(sample_timestamp_ns, int)
                    or isinstance(sample_timestamp_ns, bool)
                    or sample_timestamp_ns < boundary_ns
                    or (next_boundary_ns is not None and sample_timestamp_ns >= next_boundary_ns)
                ):
                    errors.append(
                        f"stage {boundary} requires exactly one matching tagged diagnostics sample "
                        "at or after its boundary and before the next boundary"
                    )
                elif isinstance((stage_events.get(boundary) or {}).get("state"), dict):
                    event_state = stage_events[boundary]["state"]
                    snapshot = boundary_records[0].get("snapshot")
                    rows = event_state.get("rows")
                    if not isinstance(snapshot, dict) or not isinstance(rows, dict):
                        continue
                    expected_state = diagnostic_state_builder.build(
                        snapshot, rows
                    )
                    for field in ("wal", "counters"):
                        if event_state.get(field) != expected_state[field]:
                            errors.append(
                                f"stage {boundary} {field} does not match its tagged diagnostics snapshot"
                            )
            if boundary_acknowledgement is not None:
                warm_matches = [
                    record for record in (diagnostics_records or [])
                    if record.get("boundary") == "cache_warm"
                    and record.get("boundary_timestamp_ns") == adapter["cache_warm_ns"]
                ]
                if (
                    len(warm_matches) != 1
                    or boundary_acknowledgement.get("boundary") != "cache_warm"
                    or boundary_acknowledgement.get("boundary_timestamp_ns")
                    != adapter["cache_warm_ns"]
                    or boundary_acknowledgement.get("sample_timestamp_ns")
                    != warm_matches[0].get("timestamp_ns")
                ):
                    errors.append(
                        "lifecycle boundary acknowledgement does not match the cache_warm diagnostics sample"
                    )
        lifecycle_route_proof = manifest.get("lifecycle_route_proof")
        if lifecycle_route_proof != "lifecycle_route_response.json":
            completion_errors.append(
                "completed lifecycle must declare lifecycle_route_response.json route proof"
            )
        elif lifecycle_route_proof not in raw_by_path:
            completion_errors.append(
                "lifecycle_route_response.json must be a checksum-bound raw artifact"
            )
        lifecycle_count_proof = manifest.get("lifecycle_count_proof")
        if lifecycle_count_proof != "lifecycle_count_response.json":
            completion_errors.append(
                "completed lifecycle must declare lifecycle_count_response.json count proof"
            )
        elif lifecycle_count_proof not in raw_by_path:
            completion_errors.append(
                "lifecycle_count_response.json must be a checksum-bound raw artifact"
            )
    service_profile = service.get("profile")
    if (
        status == "completed"
        and isinstance(service_profile, str)
        and service_profile in {"command_wal_durable", "command_wal_relaxed"}
    ):
        for stage, row_count in (("load_end", "server_accepted"), ("drain_checkpoint", "server_durable")):
            state = (stage_events.get(stage) or {}).get("state")
            rows = state.get("rows") if isinstance(state, dict) else None
            wal = state.get("wal") if isinstance(state, dict) else None
            progress = rows.get(row_count) if isinstance(rows, dict) else None
            if isinstance(progress, int) and not isinstance(progress, bool) and progress > 0:
                for key in ("frontier", "bytes_written_total"):
                    value = wal.get(key) if isinstance(wal, dict) else None
                    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                        completion_errors.append(
                            f"stage {stage} requires positive wal.{key} after positive rows.{row_count}"
                        )
    if events:
        report["last_stage"] = events[-1].get("stage")
        if report["last_stage"] != "teardown":
            completion_errors.append("teardown must be the final lifecycle stage")
        final_state = events[-1].get("state")
        rows = final_state.get("rows") if isinstance(final_state, dict) else None
        if isinstance(rows, dict):
            report["counts"] = {key: rows.get(key) for key in ("client_sent", "server_accepted", "server_durable", "reopened")}

    if expected_rows is not None:
        for stage in ("reset", "load_start"):
            if stage not in stage_events:
                continue
            state = stage_events[stage].get("state")
            rows = state.get("rows") if isinstance(state, dict) else {}
            if not isinstance(rows, dict):
                rows = {}
            for key in ("client_sent", "server_accepted", "server_durable", "reopened"):
                if rows.get(key) != 0:
                    completion_errors.append(f"stage {stage} rows.{key} must be zero")
        count_stages = {
            "load_end": ("client_sent", "server_accepted"),
            "drain_checkpoint": ("client_sent", "server_accepted", "server_durable"),
            "exact_verify": ("client_sent", "server_accepted", "server_durable", "reopened"),
            "teardown": ("client_sent", "server_accepted", "server_durable", "reopened"),
        }
        for stage, keys in count_stages.items():
            if stage not in stage_events:
                continue
            state = stage_events[stage].get("state")
            rows = state.get("rows") if isinstance(state, dict) else {}
            if not isinstance(rows, dict):
                rows = {}
            for key in keys:
                if rows.get(key) != expected_rows:
                    completion_errors.append(f"stage {stage} rows.{key} does not equal expected_rows")
        for event in events:
            stage = event.get("stage")
            state = event.get("state")
            rows = state.get("rows") if isinstance(state, dict) else None
            if isinstance(rows, dict) and rows.get("reopened") != 0:
                completion_errors.append(f"stage {stage} rows.reopened must remain zero before cold reopen")
            if stage == "graceful_close":
                break

    index_reference: tuple[str, int] | None = None
    for stage in ("optimize_end", "cache_prime", "cache_warm", "graceful_close", "cold_open_ready", "exact_verify", "route_verify"):
        if stage not in stage_events:
            continue
        state = stage_events[stage].get("state")
        index = state.get("index") if isinstance(state, dict) else None
        if not isinstance(index, dict):
            index = {}
        index_identity = index.get("identity")
        index_generation = index.get("asset_generation")
        index_status = index.get("status")
        current = (index_identity, index_generation)
        if (
            not isinstance(current[0], str)
            or not current[0]
            or isinstance(current[1], bool)
            or not isinstance(current[1], int)
            or current[1] <= 0
            or index_status != "ready"
        ):
            completion_errors.append(f"stage {stage} lacks a ready index identity and asset generation")
        elif index_reference is None:
            index_reference = current
        else:
            if current[0] != index_reference[0]:
                completion_errors.append(f"index identity changed at stage {stage}")
            if current[1] != index_reference[1]:
                completion_errors.append(f"index asset generation changed at stage {stage}")
    if raw_optimize_reference is not None and index_reference != raw_optimize_reference:
        errors.append(
            "adapter optimize response index identity/generation does not match optimize_end"
        )

    database_snapshots: dict[str, tuple[str | None, int | None]] = {}
    for stage in ("graceful_close", "cold_open_ready"):
        event = stage_events.get(stage)
        if event is None:
            continue
        state = event.get("state")
        database = state.get("database") if isinstance(state, dict) else None
        if not isinstance(database, dict):
            errors.append(f"stage {stage} database must be an object")
            continue
        database_identity = database.get("identity")
        if not isinstance(database_identity, str) or not database_identity:
            errors.append(f"stage {stage} database.identity must be non-empty")
            database_identity = None
        commit = _nonnegative_int(database.get("commit_seq"), f"stage {stage} database.commit_seq", errors)
        database_snapshots[stage] = (database_identity, commit)
    close_database = database_snapshots.get("graceful_close")
    reopen_database = database_snapshots.get("cold_open_ready")
    if close_database is not None and reopen_database is not None:
        if close_database[0] is not None and reopen_database[0] != close_database[0]:
            completion_errors.append("cold reopen database.identity does not match graceful close")
        if close_database[1] is not None and reopen_database[1] is not None and reopen_database[1] != close_database[1]:
            completion_errors.append("cold reopen database.commit_seq does not match graceful close")

    route_state = (stage_events.get("route_verify") or {}).get("state")
    route = route_state.get("route") if isinstance(route_state, dict) else {}
    if not isinstance(route, dict):
        route = {}
    route_generation = route.get("index_asset_generation")
    if not (
        route.get("optimized") is True
        and route.get("fallback_reason") == "none"
        and isinstance(route.get("name"), str)
        and route.get("name") in OPTIMIZED_ROUTE_NAMES
        and index_reference is not None
        and route.get("index_identity") == index_reference[0]
        and not isinstance(route_generation, bool)
        and isinstance(route_generation, int)
        and route_generation == index_reference[1]
        and isinstance(route.get("service_generation"), int)
        and not isinstance(route.get("service_generation"), bool)
        and route.get("service_generation") > 0
        and isinstance(route.get("requested_top_k"), int)
        and not isinstance(route.get("requested_top_k"), bool)
        and route.get("requested_top_k") > 0
        and route.get("result_count") == route.get("requested_top_k")
    ):
        completion_errors.append("optimized route proof failed or used a stale index asset generation")

    if status == "completed" and manifest.get("lifecycle_route_proof") == "lifecycle_route_response.json":
        try:
            raw_route_response = _strict_json_loads(
                (root / "lifecycle_route_response.json").read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(f"cannot parse lifecycle route response: {exc}")
        else:
            if not isinstance(raw_route_response, dict):
                errors.append("lifecycle route response must be an object")
            else:
                raw_index = raw_route_response.get("index")
                raw_diagnostics = raw_route_response.get("diagnostics")
                raw_results = raw_route_response.get("results")
                raw_stats = route_response_stats(raw_route_response)
                valid_diagnostics = (
                    isinstance(raw_diagnostics, dict)
                    and isinstance(raw_diagnostics.get("route"), str)
                    and bool(raw_diagnostics.get("route"))
                    and isinstance(raw_diagnostics.get("fallback_reason"), str)
                    and bool(raw_diagnostics.get("fallback_reason"))
                )
                requested_top_k = route.get("requested_top_k")
                valid_results = (
                    isinstance(requested_top_k, int)
                    and not isinstance(requested_top_k, bool)
                    and identified_route_results(raw_results, requested_top_k)
                )
                if not isinstance(raw_index, dict):
                    errors.append("lifecycle route response index must be an object")
                if not valid_diagnostics:
                    errors.append("lifecycle route response diagnostics must name route and fallback status")
                if not valid_results:
                    errors.append("lifecycle route response results must be a list of identified result objects")
                if (
                    isinstance(raw_results, list)
                    and (
                        len(raw_results) != route.get("requested_top_k")
                        or len(raw_results) != route.get("result_count")
                    )
                ):
                    errors.append("lifecycle route response result count does not match route_verify")
                if raw_stats is None:
                    errors.append("lifecycle route response stats must be an object when present")
                if raw_route_response.get("no_documents") is not True:
                    errors.append("lifecycle route response must prove no-document search")
                if not no_document_guardrails(raw_route_response):
                    errors.append(
                        "lifecycle route response must prove zero-fetch no-document guardrails"
                    )
                if isinstance(raw_index, dict) and valid_diagnostics and valid_results and raw_stats is not None:
                    raw_generation = raw_index.get("generation")
                    raw_index_name = raw_index.get("name")
                    raw_vector_index_name = raw_route_response.get("vector_index_name")
                    if (
                        isinstance(raw_generation, bool)
                        or not isinstance(raw_generation, int)
                        or raw_generation <= 0
                        or raw_generation != route.get("service_generation")
                    ):
                        errors.append("lifecycle route response index generation does not match route_verify")
                    if (
                        not isinstance(raw_index_name, str)
                        or not raw_index_name
                        or not isinstance(raw_vector_index_name, str)
                        or not raw_vector_index_name
                        or f"{raw_index_name}:{raw_vector_index_name}" != route.get("index_identity")
                        or (bound_index_name is not None and raw_index_name != bound_index_name)
                    ):
                        errors.append("lifecycle route response index identity does not match route_verify")
                    raw_summary = proof_summary(
                        "lifecycle", "lifecycle", raw_route_response,
                        {"top_k": route.get("requested_top_k")},
                    )
                    if raw_summary["route"] != route.get("name"):
                        errors.append("lifecycle route response route does not match route_verify")
                    if raw_summary["fallback_reason"] != route.get("fallback_reason"):
                        errors.append("lifecycle route response fallback status does not match route_verify")

    if status == "completed" and manifest.get("lifecycle_count_proof") == "lifecycle_count_response.json":
        try:
            raw_count_response = _strict_json_loads(
                (root / "lifecycle_count_response.json").read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(f"cannot parse lifecycle count response: {exc}")
        else:
            raw_count_index = raw_count_response.get("index") if isinstance(raw_count_response, dict) else None
            raw_count = raw_count_response.get("count") if isinstance(raw_count_response, dict) else None
            count_generation = raw_count_index.get("generation") if isinstance(raw_count_index, dict) else None
            count_index_name = raw_count_index.get("name") if isinstance(raw_count_index, dict) else None
            count_vector_name = raw_count_index.get("vector_index_name") if isinstance(raw_count_index, dict) else None
            if not isinstance(raw_count_response, dict) or not isinstance(raw_count_index, dict):
                errors.append("lifecycle count response and index must be objects")
            elif (
                isinstance(raw_count, bool)
                or not isinstance(raw_count, int)
                or raw_count != expected_rows
                or isinstance(count_generation, bool)
                or not isinstance(count_generation, int)
                or count_generation != route.get("service_generation")
                or not isinstance(count_index_name, str)
                or not count_index_name
                or not isinstance(count_vector_name, str)
                or not count_vector_name
                or f"{count_index_name}:{count_vector_name}" != route.get("index_identity")
                or (bound_index_name is not None and count_index_name != bound_index_name)
            ):
                errors.append("lifecycle count response does not prove expected reopened rows and index identity")

    profiles = lifecycle.get("profiles")
    if not isinstance(profiles, list):
        errors.append("lifecycle.profiles must be a list")
        profiles = []
    if not profiles:
        completion_errors.append("completed lifecycle requires at least one profile")
    for position, profile in enumerate(profiles):
        if not isinstance(profile, dict):
            errors.append(f"profile {position} must be an object")
            continue
        relative = profile.get("path")
        before = profile.get("before_sequence")
        after = profile.get("after_sequence")
        kind = profile.get("kind")
        if (
            not isinstance(relative, str)
            or relative not in raw_by_path
            or profile.get("sha256") != raw_by_path.get(relative)
            or isinstance(before, bool)
            or isinstance(after, bool)
            or not isinstance(before, int)
            or not isinstance(after, int)
            or before not in sequence_events
            or after not in sequence_events
            or before >= after
        ):
            errors.append(f"profile state association {position} is invalid")
        profile_path = _artifact_file(root, relative, f"profile {position}", errors)
        if profile_path is not None:
            try:
                with profile_path.open("rb") as source:
                    profile_data = source.read(64)
            except OSError as exc:
                errors.append(f"cannot read profile {position}: {exc}")
            else:
                decoder = None
                if isinstance(kind, str):
                    if kind in PPROF_PROFILE_KINDS or kind == "trace":
                        decoder = "go"
                    elif kind == "perf":
                        decoder = "perf"
                if decoder is not None and shutil.which(decoder) is None:
                    errors.append(f"profile {position} requires unavailable native decoder: {decoder}")
                elif not _valid_profile_payload(kind, profile_path, profile_data):
                    errors.append(f"profile {position} content does not match a supported kind")

    if all(stage in stage_events for stage in ("load_start", "load_end", "drain_checkpoint", "optimize_end", "graceful_close", "cold_open_ready")):
        boundaries = [
            "load_start", "load_end", "drain_checkpoint", "optimize_end", "graceful_close", "cold_open_ready",
        ]
        timestamps = [stage_events[stage].get("_timestamp") for stage in boundaries]
        if all(isinstance(value, _dt.datetime) for value in timestamps):
            report["t_ready_seconds"] = (timestamps[-1] - timestamps[0]).total_seconds()
            if report["t_ready_seconds"] <= 0:
                completion_errors.append("T_ready must be strictly positive")

    report["analyzable"] = not errors
    report["complete"] = not errors and not completion_errors
    return report


def positive_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"canonical VDBBench result is missing positive {name}")
    return float(value)


def case_vector_count(case_type: str) -> int:
    match = re.search(r"(\d+)([KMG])$", case_type, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"cannot derive vector count from VDBBench case type {case_type!r}")
    count = int(match.group(1)) * {"K": 1_000, "M": 1_000_000, "G": 1_000_000_000}[match.group(2).upper()]
    if count <= 0:
        raise ValueError(f"cannot derive positive vector count from VDBBench case type {case_type!r}")
    return count


def case_vector_dimensions(case_type: str) -> int:
    match = re.fullmatch(r"Performance(\d+)D\d+[KMG]", case_type, flags=re.IGNORECASE)
    if match is None or int(match.group(1)) <= 0:
        raise ValueError(f"cannot derive positive dimensions from standard VDBBench case type {case_type!r}")
    return int(match.group(1))


def lifecycle_dataset_shape(args: argparse.Namespace) -> tuple[int, int]:
    if args.case_type == "PerformanceCustomDataset":
        return args.lifecycle_vectors, args.lifecycle_dimensions
    return case_vector_count(args.case_type), case_vector_dimensions(args.case_type)


def custom_task_config_dataset(task_config: dict[str, Any]) -> dict[str, Any]:
    current: Any = task_config
    for key in ("case_config", "custom_case", "dataset_config"):
        if not isinstance(current, dict) or not isinstance(current.get(key), dict):
            raise ValueError(f"custom task_config {key} must be an object")
        current = current[key]
    return current


def custom_task_config_shape(task_config: dict[str, Any]) -> tuple[int, int]:
    dataset = custom_task_config_dataset(task_config)
    values = []
    for key in ("size", "dim"):
        value = dataset.get(key)
        if isinstance(value, bool) or not (
            isinstance(value, int) or isinstance(value, str) and value.isascii() and value.isdigit()
        ):
            raise ValueError(f"custom task_config dataset {key} must be a positive integer")
        parsed = int(value)
        if parsed <= 0:
            raise ValueError(f"custom task_config dataset {key} must be a positive integer")
        values.append(parsed)
    return values[0], values[1]


def custom_task_config_dataset_file(task_config: dict[str, Any]) -> Path:
    dataset = custom_task_config_dataset(task_config)
    directory = dataset.get("dir")
    file_count = dataset.get("file_count")
    shuffled = dataset.get("use_shuffled")
    one_file = file_count == 1 or (
        isinstance(file_count, str) and file_count.isascii() and file_count == "1"
    )
    if not isinstance(directory, str) or not directory or not one_file or not isinstance(shuffled, bool):
        raise ValueError("custom task_config must identify one concrete training file")
    filename = "shuffle_train.parquet" if shuffled else "train.parquet"
    return (Path(directory).expanduser() / filename).resolve(strict=False)


def result_vector_count(task_config: dict[str, Any], case_type: str) -> tuple[int, str]:
    custom_case = task_config.get("case_config", {}).get("custom_case") or {}
    size = custom_case.get("dataset_config", {}).get("size")
    if (isinstance(size, int) and not isinstance(size, bool) and size > 0) or (isinstance(size, str) and size.isdigit() and int(size) > 0):
        return int(size), "task_config.case_config.custom_case.dataset_config.size"
    return case_vector_count(case_type), "case_type suffix"


def throughput_vector_count(metrics: dict[str, Any], expected: int) -> tuple[int, str]:
    inserted = metrics.get("inserted_count")
    if isinstance(inserted, bool) or not isinstance(inserted, int) or inserted < 0:
        raise ValueError("canonical VDBBench result is missing non-negative inserted_count")
    if inserted == 0:
        return expected, "expected dataset size; VDBBench performance sentinel inserted_count=0"
    if inserted != expected:
        raise ValueError(f"canonical VDBBench inserted_count {inserted} != expected dataset size {expected}")
    return inserted, "metrics.inserted_count"


def load_metrics_from_result(path: Path, index_name: str, case_type: str, artifact_root: Path) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read canonical VDBBench result {path}: {exc}") from exc
    matches = [
        item for item in result.get("results", [])
        if item.get("task_config", {}).get("db_config", {}).get("index_name") == index_name
    ]
    if len(matches) != 1:
        raise ValueError(f"canonical VDBBench result {path} has {len(matches)} entries for index {index_name!r}; expected one")
    if matches[0].get("label") != ":)":
        raise ValueError(f"canonical VDBBench result {path} did not report success")
    metrics = matches[0].get("metrics") or {}
    insert = positive_number(metrics.get("insert_duration"), "insert_duration")
    optimize = positive_number(metrics.get("optimize_duration"), "optimize_duration")
    load = positive_number(metrics.get("load_duration"), "load_duration")
    if not math.isclose(load, insert + optimize, rel_tol=0.0, abs_tol=0.0002):
        raise ValueError(f"canonical VDBBench result {path} has load_duration != insert_duration + optimize_duration")
    task_config = matches[0].get("task_config") or {}
    vectors, vector_source = result_vector_count(task_config, case_type)
    throughput_vectors, throughput_source = throughput_vector_count(metrics, vectors)
    return {
        "result_file": str(path.relative_to(artifact_root)),
        "result_sha256": sha256_file(path),
        "result_run_id": result.get("run_id"),
        "index_name": index_name,
        "case_type": case_type,
        "vector_count": vectors,
        "vector_count_source": vector_source,
        "inserted_count": metrics["inserted_count"],
        "throughput_vector_count": throughput_vectors,
        "throughput_vector_count_source": throughput_source,
        "insert_duration_seconds": insert,
        "offline_optimize_duration_seconds": optimize,
        "total_load_duration_seconds": load,
        "insert_vectors_per_second": throughput_vectors / insert,
        "task_config": task_config,
        "task_config_sha256": hashlib.sha256(
            json.dumps(task_config, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def capture_vdbbench_load_metrics(results_dir: Path, before: set[Path], index_name: str, case_type: str, artifact_root: Path) -> dict[str, Any]:
    candidates = sorted(vdbbench_result_files(results_dir) - before)
    if len(candidates) != 1:
        raise ValueError(f"expected exactly one new canonical VDBBench result for {index_name!r}, found {len(candidates)}")
    return load_metrics_from_result(candidates[0], index_name, case_type, artifact_root)


def run_vdbbench_rows(
    state: HarnessState,
    *,
    args: argparse.Namespace,
    gomap_root: Path,
    vectordbbench_dir: Path | None,
    base_url: str,
    index_prefix: str,
    sampler: DiagnosticsSampler | None = None,
) -> None:
    if not args.run_vdbbench:
        add_skip(state, "vectordbbench_rows", "run-vdbbench disabled; route-proof smoke still ran")
        return
    if vectordbbench_dir is None or not vectordbbench_dir.exists():
        raise RuntimeError("--run-vdbbench requires VECTORDBBENCH_DIR or --vectordbbench-dir")
    rows = [row.strip().lower() for row in args.rows.split(",") if row.strip()]
    row_specs = {
        "exact": ("treedbcolumngraphexact", f"{index_prefix}_exact_vdbbench"),
        "scalar": ("treedbscalaru8rerank", f"{index_prefix}_scalar_u8_vdbbench"),
    }
    for row in rows:
        if row not in row_specs:
            raise ValueError(f"unknown TreeDB VDBBench row {row!r}; allowed: exact,scalar")
        command_name, index_name = row_specs[row]
        env = vdbbench_row_env(args, vectordbbench_dir, gomap_root, state, row)
        results_dir = state.root / "vdbbench-results" / row
        cmd = vdbbench_base_cmd(args, base_url, index_name, command_name)
        before_results = vdbbench_result_files(results_dir)
        if row == "scalar":
            cmd.extend([
                "--quantized-index-name",
                args.quantized_index_name,
                "--quantized-rerank-candidates",
                str(args.rerank_candidates),
            ])
        capture_stop = threading.Event()
        capture_errors: list[BaseException] = []
        capture_thread: threading.Thread | None = None
        acknowledgement = state.root / "lifecycle-boundary-diagnostics.json"
        if args.lifecycle:
            if sampler is None:
                raise RuntimeError("lifecycle VDBBench run requires the diagnostics sampler")
            acknowledgement.unlink(missing_ok=True)

            def capture_boundary() -> None:
                try:
                    capture_lifecycle_boundary_diagnostics(
                        state.root / "adapter-lifecycle.jsonl",
                        acknowledgement,
                        sampler,
                        capture_stop,
                    )
                except BaseException as exc:  # noqa: BLE001 - propagate worker failure on caller thread
                    capture_errors.append(exc)

            capture_thread = threading.Thread(
                target=capture_boundary,
                name="treedb-lifecycle-boundaries",
                daemon=True,
            )
            capture_thread.start()
        try:
            record = run_command(
                state,
                f"vdbbench_{row}",
                cmd,
                cwd=vectordbbench_dir,
                env=env,
                timeout=args.vdbbench_timeout,
                required=True,
            )
        finally:
            capture_stop.set()
            if capture_thread is not None:
                capture_thread.join(timeout=2.0)
        if capture_thread is not None and capture_thread.is_alive():
            raise RuntimeError("lifecycle boundary diagnostics capture did not stop")
        if capture_errors:
            raise RuntimeError(f"lifecycle boundary diagnostics capture failed: {capture_errors[0]}") from capture_errors[0]
        if args.lifecycle and not acknowledgement.is_file():
            raise RuntimeError("VDBBench lifecycle ended without boundary diagnostics acknowledgement")
        row_record = {
            "row": row,
            "index_name": index_name,
            "command": record.command_string,
            "exit_code": record.exit_code,
            "results_dir": str(results_dir.relative_to(state.root)),
            "log_file": "vdbbench.log",
            "num_per_batch": args.num_per_batch,
        }
        if not args.vdbbench_dry_run and not args.skip_load:
            row_record["load_metrics"] = capture_vdbbench_load_metrics(
                results_dir, before_results, index_name, args.case_type, state.root
            )
        state.vdbbench.append(row_record)
        if "load_metrics" in row_record:
            write_json(state.root / "vdbbench_load_metrics.json", {
                "schema_version": "treedb-vectordbbench-load-metrics/v1",
                "rows": state.vdbbench,
                "note": "Durations and derived insert throughput are selected from the checksum-identified canonical VDBBench result JSON.",
            })


def iso_from_ns(timestamp_ns: int) -> str:
    return _datetime_from_ns(timestamp_ns).isoformat().replace("+00:00", "Z")


def snapshot_int(snapshot: dict[str, Any], section: str, key: str) -> int:
    values = snapshot.get(section)
    if not isinstance(values, dict):
        return 0
    value = values.get(key, 0)
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(0, parsed)


class LifecycleStateBuilder:
    def __init__(self):
        self.counters = {
            "commit_seq": 0,
            "wal_write_bytes_total": 0,
            "indexed_stage_docs_total": 0,
            "indexed_flush_docs_total": 0,
        }
        self.wal_frontier = 0
        self.wal_bytes = 0

    def build(
        self,
        snapshot: dict[str, Any],
        rows: dict[str, int],
        *,
        index: dict[str, Any] | None = None,
        database: dict[str, Any] | None = None,
        route: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        observed = {
            "commit_seq": snapshot_int(snapshot, "database", "treedb.commit_seq"),
            "wal_write_bytes_total": snapshot_int(
                snapshot, "database", "treedb.command_wal.write.bytes_total"
            ),
            "indexed_stage_docs_total": snapshot_int(
                snapshot, "collections", "treedb.collections.write_domain.indexed_stage.docs_total"
            ),
            "indexed_flush_docs_total": snapshot_int(
                snapshot, "collections", "treedb.collections.write_domain.indexed_flush.docs_total"
            ),
        }
        self.counters = {key: max(self.counters[key], observed[key]) for key in self.counters}
        self.wal_frontier = max(
            self.wal_frontier,
            snapshot_int(snapshot, "database", "treedb.command_wal.durable_wal_lsn"),
        )
        self.wal_bytes = max(self.wal_bytes, observed["wal_write_bytes_total"])
        state: dict[str, Any] = {
            "rows": dict(rows),
            "wal": {"frontier": self.wal_frontier, "bytes_written_total": self.wal_bytes},
            "counters": dict(self.counters),
        }
        if index is not None:
            state["index"] = index
        if database is not None:
            state["database"] = database
        if route is not None:
            state["route"] = route
        return state


def lifecycle_rows(sent: int = 0, accepted: int = 0, durable: int = 0, reopened: int = 0) -> dict[str, int]:
    return {
        "client_sent": sent,
        "server_accepted": accepted,
        "server_durable": durable,
        "reopened": reopened,
    }


def lifecycle_metadata(state: HarnessState, args: argparse.Namespace) -> dict[str, Any]:
    expected_rows, dimensions = lifecycle_dataset_shape(args)
    dataset_name = args.lifecycle_dataset_name or args.lifecycle_dataset_file.parent.name
    return {
        "schema_version": LIFECYCLE_SCHEMA,
        "result_status": "partial",
        "file": "lifecycle.jsonl",
        "expected_rows": expected_rows,
        "dataset": {
            "name": dataset_name,
            "sha256": sha256_file(args.lifecycle_dataset_file),
            "dimensions": dimensions,
            "vectors": expected_rows,
        },
        "raw_artifacts": [],
        "profiles": [],
    }


def lifecycle_raw_artifacts(state: HarnessState, paths: list[Path]) -> list[dict[str, str]]:
    artifacts = []
    for path in paths:
        if path.is_file():
            artifacts.append({
                "path": str(path.relative_to(state.root)),
                "sha256": sha256_file(path),
            })
    return artifacts


def lifecycle_load_milestone_document(records: list[dict[str, Any]]) -> dict[str, Any]:
    load_start = next(
        (record["timestamp_ns"] for record in records if record.get("event") == "load_start"),
        None,
    )
    if load_start is None:
        raise ValueError("adapter lifecycle sidecar has no load_start boundary")
    batches = sorted(
        (record for record in records if record.get("event") == "batch_accepted"),
        key=lambda record: record["timestamp_ns"],
    )
    accepted = 0
    sent = 0
    milestones = []
    for record in batches:
        batch_sent = record.get("client_sent")
        batch_accepted = record.get("server_accepted")
        if not all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in (batch_sent, batch_accepted)
        ):
            raise ValueError("adapter lifecycle batch counts are invalid")
        accepted += batch_accepted
        sent += batch_sent
        elapsed = max(0, record["timestamp_ns"] - load_start) / 1_000_000_000
        try:
            accepted_rate = accepted / elapsed if elapsed > 0 else None
        except OverflowError as exc:
            raise ValueError("adapter lifecycle counts exceed the supported finite milestone rate") from exc
        if accepted_rate is not None and not math.isfinite(accepted_rate):
            raise ValueError("adapter lifecycle counts exceed the supported finite milestone rate")
        milestones.append({
            "timestamp_ns": record["timestamp_ns"],
            "client_sent_cumulative": sent,
            "server_accepted_cumulative": accepted,
            "elapsed_seconds": elapsed,
            "accepted_vectors_per_second_cumulative": accepted_rate,
        })
    return {
        "schema_version": "treedb-vectordbbench-load-milestones/v1",
        "ordering": "batch completion timestamp; equal-size NUM_PER_BATCH milestones except the final batch",
        "milestones": milestones,
    }


def write_lifecycle_load_milestones(state: HarnessState, records: list[dict[str, Any]]) -> Path:
    path = state.root / "lifecycle_load_milestones.json"
    write_json(path, lifecycle_load_milestone_document(records))
    return path


def initialize_lifecycle_capture(
    state: HarnessState, args: argparse.Namespace, sampler: DiagnosticsSampler
) -> None:
    """Create analyzable partial ownership before the load can fail."""
    state.lifecycle = lifecycle_metadata(state, args)
    started_ns = state.lifecycle_started_ns or time.time_ns()
    LifecycleJournal(state.root / "lifecycle.jsonl").append(
        "startup",
        LifecycleStateBuilder().build(sampler.at(started_ns), lifecycle_rows()),
        timestamp=iso_from_ns(started_ns),
    )


def finalize_partial_lifecycle(
    state: HarnessState,
    args: argparse.Namespace,
    sampler: DiagnosticsSampler | None,
    *,
    result_status: str = "partial",
) -> None:
    """Retain only sidecar boundaries that are structurally present after failure."""
    if state.lifecycle is None:
        return
    if result_status not in {"partial", "interrupted"}:
        raise ValueError(f"unsupported incomplete lifecycle status {result_status!r}")
    state.lifecycle["result_status"] = result_status
    if sampler is not None:
        sampler.stop()
        state.diagnostics = list(sampler.samples)
    sidecar = state.root / "adapter-lifecycle.jsonl"
    state.lifecycle["raw_artifacts"] = lifecycle_raw_artifacts(state, [
        state.root / "diagnostics.jsonl",
        sidecar,
        state.root / "lifecycle-boundary-diagnostics.json",
        state.root / "service.log",
    ])
    if not sidecar.exists():
        return
    try:
        records = read_adapter_lifecycle_records(sidecar)
        journal = LifecycleJournal(state.root / "lifecycle.jsonl")
    except (OSError, ValueError):
        return
    try:
        milestone_path = write_lifecycle_load_milestones(state, records)
    except (OSError, ValueError):
        milestone_path = None
    state.lifecycle["raw_artifacts"] = lifecycle_raw_artifacts(state, [
        state.root / "diagnostics.jsonl",
        sidecar,
        state.root / "lifecycle-boundary-diagnostics.json",
        *([milestone_path] if milestone_path is not None else []),
        state.root / "service.log",
    ])
    existing = {
        event["stage"]
        for event in (
            _strict_json_loads(line)
            for line in (state.root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
        )
    }
    builder = LifecycleStateBuilder()
    accepted = 0
    sent = 0
    for record in records:
        event = record.get("event")
        if event == "batch_accepted":
            batch_sent = record.get("client_sent")
            batch_accepted = record.get("server_accepted")
            if all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in (batch_sent, batch_accepted)):
                sent += batch_sent
                accepted += batch_accepted
            continue
        stage = event if event in {
            "reset", "load_start", "load_end", "optimize_start", "optimize_end", "cache_prime", "cache_warm",
        } else None
        if stage is None or stage in existing:
            continue
        rows = lifecycle_rows()
        if stage in {"load_end", "optimize_start", "optimize_end", "cache_prime", "cache_warm"}:
            rows = lifecycle_rows(sent, accepted, accepted)
        if sampler is not None and stage in LIFECYCLE_DIAGNOSTIC_BOUNDARIES:
            try:
                snapshot = boundary_diagnostics_snapshot(stage, record["timestamp_ns"], sampler)
            except ValueError:
                snapshot = sampler.at(record["timestamp_ns"])
        else:
            snapshot = sampler.at(record["timestamp_ns"]) if sampler is not None else {}
        index = None
        if stage in {"optimize_end", "cache_prime", "cache_warm"}:
            optimize = next(
                (item.get("response") for item in records if item.get("event") == "optimize_end"), None
            )
            if not isinstance(optimize, dict):
                break
            try:
                identity, generation, _ = lifecycle_ready_asset(optimize, lifecycle_index_name(args))
            except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                break
            index = {"identity": identity, "asset_generation": generation, "status": "ready"}
        journal.append(
            stage, builder.build(snapshot, rows, index=index),
            timestamp=iso_from_ns(record["timestamp_ns"]),
        )
        existing.add(stage)


def fetch_file(url: str, path: Path, timeout: float = 30.0) -> None:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - loopback diagnostics only
        data = response.read()
    if not data:
        raise RuntimeError(f"empty response from {url}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def lifecycle_index_name(args: argparse.Namespace) -> str:
    row = next(row.strip().lower() for row in args.rows.split(",") if row.strip())
    suffix = "exact_vdbbench" if row == "exact" else "scalar_u8_vdbbench"
    return f"{args.index_prefix}_{suffix}"


def bind_lifecycle_task_config(
    state: HarnessState, args: argparse.Namespace, expected_rows: int, dimensions: int
) -> dict[str, str]:
    metrics = [row.get("load_metrics") for row in state.vdbbench if isinstance(row.get("load_metrics"), dict)]
    if len(metrics) != 1:
        raise RuntimeError("lifecycle requires exactly one canonical VDBBench load result")
    selected = metrics[0]
    task_config = selected.get("task_config")
    if not isinstance(task_config, dict):
        raise RuntimeError("canonical VDBBench result has no task_config object")
    if args.case_type != "PerformanceCustomDataset":
        raise RuntimeError("lifecycle requires PerformanceCustomDataset for checksum-bound task_config evidence")
    if custom_task_config_shape(task_config) != (expected_rows, dimensions):
        raise RuntimeError("custom VDBBench task_config shape does not match lifecycle dataset shape")
    if custom_task_config_dataset_file(task_config) != args.lifecycle_dataset_file.resolve():
        raise RuntimeError("custom VDBBench task_config does not select the checksum-bound lifecycle dataset file")
    binding = {
        "result_file": selected.get("result_file"),
        "result_sha256": selected.get("result_sha256"),
        "task_config_sha256": selected.get("task_config_sha256"),
    }
    if any(not isinstance(value, str) or not value for value in binding.values()):
        raise RuntimeError("canonical VDBBench result identity is incomplete")
    return binding


def run_loaded_route_proof(
    state: HarnessState,
    args: argparse.Namespace,
    index_name: str,
    index_identity: str,
    asset_generation: int,
    expected_service_generation: int | None = None,
) -> dict[str, Any]:
    opened = http_json("GET", index_url(args.base_url, index_name))
    if not isinstance(opened, dict):
        raise RuntimeError("cold-reopened index response must be an object")
    info = opened.get("index")
    if not isinstance(info, dict):
        raise RuntimeError("cold-reopened index response is missing index metadata")
    service_generation = info.get("generation")
    if isinstance(service_generation, bool) or not isinstance(service_generation, int) or service_generation <= 0:
        raise RuntimeError("cold-reopened index has no positive service generation")
    if expected_service_generation is not None and service_generation != expected_service_generation:
        raise RuntimeError(
            "cold-reopened index generation does not match the optimized column graph: "
            f"expected={expected_service_generation} actual={service_generation}"
        )
    expected_rows, dimensions = lifecycle_dataset_shape(args)
    request: dict[str, Any] = {
        "query_embedding": [1.0, *([0.0] * (dimensions - 1))],
        "top_k": min(args.k, expected_rows),
        "ef_search": max(args.ef_search, args.k),
        "query_mode": "exact",
        "expected_generation": service_generation,
    }
    row = next(row.strip().lower() for row in args.rows.split(",") if row.strip())
    expected_route = "exact_hnsw_search_pack_v1"
    if row == "scalar":
        expected_route = "quantized_rerank"
        request.update(
            query_mode="quantized_rerank",
            quantized_index_name=args.quantized_index_name,
            quantized_rerank_candidates=max(args.rerank_candidates, args.k),
        )
    response = http_json("POST", index_url(args.base_url, index_name, "/search/vector-index"), request)
    write_json(state.root / "lifecycle_route_response.json", response)
    if not isinstance(response, dict):
        raise RuntimeError("cold-reopen route proof response must be an object")
    response_index = response.get("index")
    diagnostics = response.get("diagnostics")
    if (
        not isinstance(response_index, dict)
        or isinstance(response_index.get("generation"), bool)
        or not isinstance(response_index.get("generation"), int)
        or response_index.get("generation") != service_generation
    ):
        raise RuntimeError("cold-reopen route proof response index generation is missing or stale")
    if (
        response_index.get("name") != index_name
        or response.get("vector_index_name") != index_identity.partition(":")[2]
    ):
        raise RuntimeError("cold-reopen route proof response index identity is missing or stale")
    if (
        not isinstance(diagnostics, dict)
        or not isinstance(diagnostics.get("route"), str)
        or not diagnostics.get("route")
        or not isinstance(diagnostics.get("fallback_reason"), str)
        or not diagnostics.get("fallback_reason")
    ):
        raise RuntimeError("cold-reopen route proof diagnostics must name route and fallback status")
    if response.get("no_documents") is not True:
        raise RuntimeError("cold-reopen route proof must prove no-document search")
    results = response.get("results")
    if not identified_route_results(results, request["top_k"]):
        raise RuntimeError(
            "cold-reopen route proof did not return exactly the requested results with nonempty string ids: "
            f"requested={request['top_k']} actual={len(results) if isinstance(results, list) else 'malformed'}"
        )
    if route_response_stats(response) is None:
        raise RuntimeError("cold-reopen route proof stats must be an object when present")
    if not no_document_guardrails(response):
        raise RuntimeError("cold-reopen route proof did not prove zero-fetch no-document guardrails")
    summary = proof_summary(row, index_name, response, request)
    route = {
        "name": summary["route"],
        "fallback_reason": summary["fallback_reason"],
        "optimized": summary["route"] == expected_route and summary["fallback_reason"] == "none",
        "index_identity": index_identity,
        "index_asset_generation": asset_generation,
        "service_generation": service_generation,
        "requested_top_k": request["top_k"],
        "result_count": len(results),
    }
    if not route["optimized"]:
        raise RuntimeError(f"cold-reopen route proof failed: {route}")
    return route


def lifecycle_ready_asset(optimize: dict[str, Any], index_name: str) -> tuple[str, int, int | None]:
    index_info = optimize.get("index")
    status = optimize.get("status")
    if not isinstance(index_info, dict) or not isinstance(status, dict):
        raise RuntimeError("optimize response is missing index/status evidence")
    vector_index_name = optimize.get("vector_index_name")
    strategy = index_info.get("vector_strategy")
    if (
        index_info.get("name") != index_name
        or not isinstance(vector_index_name, str)
        or not vector_index_name
        or status.get("strategy") != strategy
        or status.get("loaded") is not True
        or status.get("rebuild_needed") is not False
    ):
        raise RuntimeError(f"optimize response does not prove a ready durable asset: {optimize}")
    expected_service_generation: int | None = None
    if strategy == "column_graph":
        generation = index_info.get("generation")
        if (
            status.get("state") != "column_graph_loaded"
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation <= 0
        ):
            raise RuntimeError(f"optimize response does not prove a ready durable column graph: {optimize}")
        expected_service_generation = generation
    elif strategy == "native_runtime":
        generation = status.get("root_id")
        if (
            status.get("state") != "native_runtime"
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation <= 0
        ):
            raise RuntimeError(f"optimize response does not prove a ready native vector root: {optimize}")
    else:
        raise RuntimeError(f"optimize response has unsupported vector strategy: {strategy!r}")
    return f"{index_name}:{vector_index_name}", generation, expected_service_generation


def complete_lifecycle(
    state: HarnessState,
    args: argparse.Namespace,
    gomap_root: Path,
    service_bin: Path,
    service_proc: subprocess.Popen[str],
    sampler: DiagnosticsSampler,
) -> list[str]:
    adapter = read_adapter_lifecycle_sidecar(state.root / "adapter-lifecycle.jsonl")
    milestone_path = write_lifecycle_load_milestones(state, adapter["records"])
    sampler.stop()
    state.diagnostics.extend(sampler.samples)
    expected_rows, dimensions = lifecycle_dataset_shape(args)
    if adapter["client_sent"] != expected_rows or adapter["server_accepted"] != expected_rows:
        raise RuntimeError(f"adapter lifecycle counts do not equal expected rows: {adapter}")
    task_config_binding = bind_lifecycle_task_config(
        state, args, expected_rows, dimensions
    )
    optimize = adapter["optimize_response"]
    index_name = lifecycle_index_name(args)
    index_identity, asset_generation, expected_service_generation = lifecycle_ready_asset(
        optimize, index_name
    )
    index_state = {"identity": index_identity, "asset_generation": asset_generation, "status": "ready"}
    builder = LifecycleStateBuilder()
    journal = LifecycleJournal(state.root / "lifecycle.jsonl")
    zero = lifecycle_rows()
    loaded = lifecycle_rows(expected_rows, expected_rows, expected_rows, 0)
    load_start_snapshot = sampler.at(adapter["load_start_ns"])
    load_end_snapshot = boundary_diagnostics_snapshot("load_end", adapter["load_end_ns"], sampler)
    optimize_start_snapshot = boundary_diagnostics_snapshot(
        "optimize_start", adapter["optimize_start_ns"], sampler
    )
    optimize_end_snapshot = boundary_diagnostics_snapshot(
        "optimize_end", adapter["optimize_end_ns"], sampler
    )
    cache_prime_snapshot = boundary_diagnostics_snapshot(
        "cache_prime", adapter["cache_prime_ns"], sampler
    )
    cache_warm_snapshot = boundary_diagnostics_snapshot(
        "cache_warm", adapter["cache_warm_ns"], sampler
    )
    journal.append(
        "reset",
        builder.build(sampler.at(adapter["reset_ns"]), zero),
        timestamp=iso_from_ns(adapter["reset_ns"]),
    )
    journal.append(
        "load_start", builder.build(load_start_snapshot, zero),
        timestamp=iso_from_ns(adapter["load_start_ns"]),
    )
    journal.append(
        "load_end", builder.build(load_end_snapshot, loaded),
        timestamp=iso_from_ns(adapter["load_end_ns"]),
    )
    durable_lsn = snapshot_int(load_end_snapshot, "database", "treedb.command_wal.durable_wal_lsn")
    accepted_lsn = snapshot_int(load_end_snapshot, "database", "treedb.command_wal.live_accepted_max_lsn")
    wal_bytes = snapshot_int(load_end_snapshot, "database", "treedb.command_wal.write.bytes_total")
    if durable_lsn <= 0 or accepted_lsn <= 0 or durable_lsn < accepted_lsn or wal_bytes <= 0:
        raise RuntimeError(
            "command_wal_durable load boundary did not prove a positive drained WAL frontier: "
            f"durable={durable_lsn} accepted={accepted_lsn} bytes={wal_bytes}"
        )
    # Successful command_wal_durable insert responses prove all accepted batches
    # reached durable acknowledgement; this snapshot independently proves the
    # server's accepted frontier was drained before optimize began.
    journal.append(
        "drain_checkpoint", builder.build(load_end_snapshot, loaded),
        timestamp=iso_from_ns(adapter["load_end_ns"]),
    )
    journal.append(
        "optimize_start", builder.build(optimize_start_snapshot, loaded),
        timestamp=iso_from_ns(adapter["optimize_start_ns"]),
    )
    journal.append(
        "optimize_end", builder.build(optimize_end_snapshot, loaded, index=index_state),
        timestamp=iso_from_ns(adapter["optimize_end_ns"]),
    )
    journal.append(
        "cache_prime", builder.build(cache_prime_snapshot, loaded, index=index_state),
        timestamp=iso_from_ns(adapter["cache_prime_ns"]),
    )
    journal.append(
        "cache_warm", builder.build(cache_warm_snapshot, loaded, index=index_state),
        timestamp=iso_from_ns(adapter["cache_warm_ns"]),
    )

    profile_path = state.root / "profiles" / "optimize.heap.pprof"
    fetch_file(f"{args.diagnostics_url}/debug/pprof/heap?gc=1", profile_path)
    # Preserve the last live/pre-close telemetry record in the raw sampler stream.
    sampler.sample()
    database_identity = f"artifact-data:{canonical_sha256(str(args.data_dir.resolve()))}"
    close_process_group_cleanly(service_proc, graceful_timeout=args.service_close_timeout)
    close_completed_ns = time.time_ns()

    reopened_proc, health, command = start_service(
        state,
        gomap_root=gomap_root,
        service_bin=service_bin,
        data_dir=args.data_dir,
        host=args.host,
        port=args.port,
        profile=args.profile,
        health_timeout=args.health_timeout,
        pprof_addr=host_port(args.host, args.pprof_port),
        append_log=True,
    )
    try:
        state.health = health
        # This must be the first application-state request after cold open. The
        # health probe in start_service is read-only; no application mutation is
        # allowed before this snapshot establishes the post-Close durable state.
        reopen_snapshot = http_json("GET", f"{args.diagnostics_url}/debug/treedb/stats")
        reopen_database = {
            "identity": database_identity,
            "commit_seq": snapshot_int(reopen_snapshot, "database", "treedb.commit_seq"),
        }
        reopened_rows = lifecycle_rows(expected_rows, expected_rows, expected_rows, expected_rows)
        journal.append(
            "graceful_close",
            builder.build(reopen_snapshot, loaded, index=index_state, database=reopen_database),
            timestamp=iso_from_ns(close_completed_ns),
        )
        journal.append(
            "cold_open_ready",
            builder.build(reopen_snapshot, reopened_rows, index=index_state, database=reopen_database),
        )
        count = http_json("POST", index_url(args.base_url, index_name, "/documents/count"), {})
        write_json(state.root / "lifecycle_count_response.json", count)
        count_index = count.get("index") if isinstance(count, dict) else None
        if (
            not isinstance(count_index, dict)
            or count.get("count") != expected_rows
            or count_index.get("name") != index_name
            or count_index.get("vector_index_name") != index_identity.partition(":")[2]
            or isinstance(count_index.get("generation"), bool)
            or not isinstance(count_index.get("generation"), int)
            or count_index.get("generation") <= 0
            or (
                expected_service_generation is not None
                and count_index.get("generation") != expected_service_generation
            )
        ):
            raise RuntimeError(f"cold-reopen count mismatch: {count}")
        journal.append("exact_verify", builder.build(reopen_snapshot, reopened_rows, index=index_state))
        route = run_loaded_route_proof(
            state,
            args,
            index_name,
            index_identity,
            asset_generation,
            expected_service_generation,
        )
        journal.append(
            "route_verify",
            builder.build(reopen_snapshot, reopened_rows, index=index_state, route=route),
        )
        close_process_group_cleanly(reopened_proc, graceful_timeout=args.service_close_timeout)
        journal.append(
            "teardown",
            builder.build(reopen_snapshot, reopened_rows, index=index_state, database=reopen_database),
        )
    except BaseException:
        terminate_process_group(reopened_proc, graceful_timeout=args.service_close_timeout)
        raise

    profile_relative = str(profile_path.relative_to(state.root))
    profile_sha = sha256_file(profile_path)
    raw_artifacts = lifecycle_raw_artifacts(state, [
        profile_path,
        state.root / "diagnostics.jsonl",
        state.root / "adapter-lifecycle.jsonl",
        state.root / "lifecycle-boundary-diagnostics.json",
        milestone_path,
        state.root / "service.log",
        state.root / "lifecycle_count_response.json",
        state.root / "lifecycle_route_response.json",
    ])
    assert state.lifecycle is not None
    state.diagnostics = list(sampler.samples)
    state.lifecycle.update({
        "result_status": "completed",
        "task_config_binding": task_config_binding,
        "raw_artifacts": raw_artifacts,
        "profiles": [
            {
                "path": profile_relative,
                "sha256": profile_sha,
                "kind": "heap",
                "before_sequence": 8,
                "after_sequence": 9,
            }
        ],
    })
    return command


def write_readme(state: HarnessState, args: argparse.Namespace) -> None:
    if args.lifecycle:
        lines = [
            "# TreeDB VectorDBBench Lifecycle Artifact",
            "",
            "This artifact was produced by `scripts/treedb_vectordbbench_artifact.py`.",
            "It is a reproducibility artifact, not public claim-quality throughput evidence unless the caller ran and documented a quiet-host benchmark matrix.",
            "",
            f"- generated_at: `{iso_now()}`",
            "- manifest: `manifest.json`",
            "- lifecycle: `lifecycle.jsonl`",
            "- cold-reopen route proof: `lifecycle_route_response.json` (also embedded in the `route_verify` lifecycle stage)",
            "- service log: `service.log`",
            f"- data dir: `{args.data_dir}`",
            f"- VDBBench load batch: `{args.num_per_batch}` documents",
            "",
            "VDBBench TreeDB rows include Python/client/HTTP/service overhead and must not be reported as native Go `B/op` or `allocs/op` evidence.",
        ]
        (state.root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return
    proof = state.route_proof or {}
    exact = proof.get("exact_fp32", {})
    scalar = proof.get("scalar_u8_rerank", {})
    lines = [
        "# TreeDB VectorDBBench Artifact",
        "",
        "This artifact was produced by `scripts/treedb_vectordbbench_artifact.py`.",
        "It is a reproducibility and route-proof artifact, not public claim-quality throughput evidence unless the caller ran and documented a quiet-host benchmark matrix.",
        "",
        f"- generated_at: `{iso_now()}`",
        f"- manifest: `manifest.json`",
        f"- route proof: `{'skipped' if args.skip_route_proof else 'route_proof.json'}`",
        f"- service log: `service.log`",
        f"- data dir: `{args.data_dir}`",
        f"- VDBBench load batch: `{args.num_per_batch}` documents",
        f"- route-proof shape: `{args.smoke_documents} x {args.smoke_dimension}`, topK `{args.smoke_top_k}`, efSearch `{args.ef_search}`, rerank `{args.rerank_candidates}`",
        "",
        "## Route proof summary",
        "",
        "| row | route | fallback_reason | documents_fetched | optimized batches | fallback batches | rerank exact calls |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        f"| exact FP32 | `{exact.get('route', '')}` | `{exact.get('fallback_reason', '')}` | {exact.get('documents_fetched', '')} | {exact.get('score_batch_optimized', '')} | {exact.get('score_batch_fallback', '')} | {exact.get('quantized_rerank_exact_score_calls', '')} |",
        f"| scalar_u8 rerank{args.rerank_candidates} | `{scalar.get('route', '')}` | `{scalar.get('fallback_reason', '')}` | {scalar.get('documents_fetched', '')} | {scalar.get('score_batch_optimized', '')} | {scalar.get('score_batch_fallback', '')} | {scalar.get('quantized_rerank_exact_score_calls', '')} |",
        "",
        "VDBBench TreeDB rows include Python/client/HTTP/service overhead and must not be reported as native Go `B/op` or `allocs/op` evidence.",
    ]
    (state.root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def prepare_artifact_root(root: Path) -> None:
    if root.exists() and any(root.iterdir()):
        raise RuntimeError(
            f"artifact output directory must be new or empty for a fresh TreeDB data dir: {root}"
        )
    root.mkdir(parents=True, exist_ok=True)


def artifact_file_list(root: Path, limit: int = 500) -> tuple[list[str], bool]:
    files: list[str] = []
    truncated = False
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        # Fresh TreeDB data dirs are intentionally artifact-owned, but a real
        # VDBBench run can create many segment files. Keep the manifest readable
        # and point readers at the data dir instead of enumerating every file.
        if rel.parts and rel.parts[0] == "treedb-data":
            continue
        if len(files) >= limit:
            truncated = True
            continue
        files.append(str(rel))
    return files, truncated


def write_manifest(
    state: HarnessState,
    *,
    args: argparse.Namespace,
    context: dict[str, Any],
    service_command: list[str] | None,
) -> None:
    files, files_truncated = artifact_file_list(state.root)
    manifest = {
        "schema_version": ARTIFACT_SCHEMA,
        "generated_at": iso_now(),
        "artifact_root": str(state.root),
        "context": context,
        "service": {
            "base_url": args.base_url,
            "host": args.host,
            "port": args.port,
            "profile": args.profile,
            "data_dir": str(args.data_dir),
            "pid": state.service_pid,
            "command": service_command,
            "binary": state.service_binary,
            "health": state.health,
            "log": "service.log",
        },
        "harness": {
            "mode": "vdbbench+lifecycle" if args.lifecycle else (
                "vdbbench" if args.skip_route_proof else ("vdbbench+smoke" if args.run_vdbbench else "smoke")
            ),
            "rows": args.rows,
            "case_type": args.case_type,
            "k": args.k,
            "num_concurrency": args.num_concurrency,
            "concurrency_duration": args.concurrency_duration,
            "m": args.m,
            "ef_construction": args.ef_construction,
            "ef_search": args.ef_search,
            "smoke_dimension": args.smoke_dimension,
            "smoke_documents": args.smoke_documents,
            "smoke_top_k": args.smoke_top_k,
            "rerank_candidates": args.rerank_candidates,
            "num_per_batch": args.num_per_batch,
            "vdbbench_dry_run": args.vdbbench_dry_run,
        },
        "commands": [asdict(record) for record in state.commands],
        "vdbbench": state.vdbbench,
        "vdbbench_load_metrics": "vdbbench_load_metrics.json" if (state.root / "vdbbench_load_metrics.json").exists() else None,
        "route_proof": "route_proof.json" if state.route_proof else None,
        "skips": state.skips,
        "files": files,
        "files_truncated": files_truncated,
        "data_dir_note": "treedb-data is artifact-owned and intentionally not enumerated in files; see service.data_dir.",
    }
    if args.lifecycle:
        manifest["lifecycle_count_proof"] = (
            "lifecycle_count_response.json"
            if (state.root / "lifecycle_count_response.json").is_file()
            else None
        )
        manifest["lifecycle_route_proof"] = (
            "lifecycle_route_response.json"
            if (state.root / "lifecycle_route_response.json").is_file()
            else None
        )
    if state.lifecycle is not None:
        lifecycle = dict(state.lifecycle)
        lifecycle_path = state.root / str(lifecycle["file"])
        lifecycle["sha256"] = sha256_file(lifecycle_path)
        lifecycle["identity"] = {
            "gomap_commit": context.get("gomap", {}).get("commit"),
            "vectordbbench_commit": context.get("vectordbbench", {}).get("commit"),
            "service_binary_sha256": (state.service_binary or {}).get("sha256"),
        }
        manifest["lifecycle"] = lifecycle
        lifecycle["identity"]["config_sha256"] = lifecycle_config_sha256(manifest)
    write_json(state.root / "manifest.json", manifest)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=env_text("TREEDB_VDBBENCH_OUT", str(default_out_dir())), help="artifact output directory")
    parser.add_argument("--vectordbbench-dir", default=os.environ.get("VECTORDBBENCH_DIR", ""), help="VectorDBBench checkout")
    parser.add_argument("--host", default=env_text("TREEDB_VDBBENCH_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(env_text("TREEDB_VDBBENCH_PORT", "0")), help="service port; 0 chooses a free local port")
    parser.add_argument("--profile", default=env_text("TREEDB_VDBBENCH_PROFILE", "command_wal_durable"))
    parser.add_argument("--service-bin", default=os.environ.get("TREEDB_VDBBENCH_SERVICE_BIN", ""), help="existing treedb-document-service binary")
    parser.add_argument("--health-timeout", type=float, default=float(env_text("TREEDB_VDBBENCH_HEALTH_TIMEOUT", "60")))
    parser.add_argument("--python", default=env_text("TREEDB_VDBBENCH_PYTHON", sys.executable or "python3"))
    parser.add_argument("--use-uv", choices=["auto", "on", "off"], default=env_text("TREEDB_VDBBENCH_USE_UV", "auto"), help="use `uv run --with ...` for VectorDBBench Python commands")
    parser.add_argument("--run-vdbbench", action="store_true", default=env_flag("TREEDB_VDBBENCH_RUN_VDBBENCH", False), help="run selected VectorDBBench TreeDB rows")
    parser.add_argument("--rows", default=env_text("TREEDB_VDBBENCH_ROWS", "exact,scalar"), help="comma list: exact,scalar")
    parser.add_argument("--run-tests", choices=["off", "auto", "required"], default=env_text("TREEDB_VDBBENCH_RUN_TESTS", "auto"), help="run VectorDBBench TreeDB tests when checkout is available")
    parser.add_argument("--vdbbench-timeout", type=int, default=int(env_text("TREEDB_VDBBENCH_TIMEOUT", "36000")))
    parser.add_argument("--vdbbench-dry-run", action="store_true", default=env_flag("TREEDB_VDBBENCH_DRY_RUN", False), help="add --dry-run and skip load/search to VDBBench rows")
    parser.add_argument("--skip-load", action="store_true", default=env_flag("TREEDB_VDBBENCH_SKIP_LOAD", False))
    parser.add_argument("--skip-search-serial", action="store_true", default=env_flag("TREEDB_VDBBENCH_SKIP_SEARCH_SERIAL", False))
    parser.add_argument("--skip-search-concurrent", action="store_true", default=env_flag("TREEDB_VDBBENCH_SKIP_SEARCH_CONCURRENT", False))
    parser.add_argument("--vdbbench-extra-args", default=os.environ.get("TREEDB_VDBBENCH_EXTRA_ARGS", ""), help="extra args appended to each VDBBench row command")
    parser.add_argument("--case-type", default=env_text("TREEDB_VDBBENCH_CASE_TYPE", "Performance1536D50K"))
    parser.add_argument("--k", type=int, default=int(env_text("TREEDB_VDBBENCH_K", "10")))
    parser.add_argument("--num-concurrency", default=env_text("TREEDB_VDBBENCH_NUM_CONCURRENCY", "1,8,32"))
    parser.add_argument("--concurrency-duration", type=int, default=int(env_text("TREEDB_VDBBENCH_CONCURRENCY_DURATION", "30")))
    parser.add_argument("--db-label", default=env_text("TREEDB_VDBBENCH_DB_LABEL", f"treedb-vdbbench-{_dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"))
    parser.add_argument("--client-timeout", type=float, default=float(env_text("TREEDB_VDBBENCH_CLIENT_TIMEOUT", "30")))
    parser.add_argument("--m", type=int, default=int(env_text("TREEDB_VDBBENCH_M", "16")))
    parser.add_argument("--ef-construction", type=int, default=int(env_text("TREEDB_VDBBENCH_EF_CONSTRUCTION", "128")))
    parser.add_argument("--ef-search", type=int, default=int(env_text("TREEDB_VDBBENCH_EF_SEARCH", "128")))
    parser.add_argument("--quantized-index-name", default=env_text("TREEDB_VDBBENCH_QUANTIZED_INDEX_NAME", DEFAULT_QUANTIZED_INDEX_NAME))
    parser.add_argument("--rerank-candidates", type=int, default=int(env_text("TREEDB_VDBBENCH_RERANK_CANDIDATES", "32")))
    parser.add_argument("--num-per-batch", type=int, default=int(env_text("TREEDB_VDBBENCH_NUM_PER_BATCH", "1000")), help="TreeDB VectorDBBench documents per load batch")
    parser.add_argument("--smoke-dimension", type=int, default=int(env_text("TREEDB_VDBBENCH_SMOKE_DIMENSION", "2")))
    parser.add_argument("--smoke-documents", type=int, default=int(env_text("TREEDB_VDBBENCH_SMOKE_DOCUMENTS", "4")))
    parser.add_argument("--smoke-top-k", type=int, default=int(env_text("TREEDB_VDBBENCH_SMOKE_TOP_K", "2")))
    parser.add_argument("--skip-route-proof", action="store_true", default=env_flag("TREEDB_VDBBENCH_SKIP_ROUTE_PROOF", False), help="skip the independent route-proof smoke")
    parser.add_argument("--index-prefix", default=os.environ.get("TREEDB_VDBBENCH_INDEX_PREFIX", ""), help="unique benchmark index prefix")
    parser.add_argument("--validate-lifecycle", default="", help="validate an existing artifact-v1 lifecycle root and exit")
    parser.add_argument("--allow-partial", action="store_true", help="return success for an analyzable partial lifecycle validation")
    parser.add_argument("--lifecycle", action="store_true", help="capture the fail-closed load/build/reopen lifecycle for one VDBBench row")
    parser.add_argument("--lifecycle-dataset-file", default="", help="exact local training dataset file used by the lifecycle row")
    parser.add_argument("--lifecycle-dataset-name", default="", help="stable dataset label recorded with its checksum")
    parser.add_argument("--lifecycle-vectors", type=int, default=0, help="custom-case vector count, verified against canonical task_config")
    parser.add_argument("--lifecycle-dimensions", type=int, default=0, help="custom-case dimensions, verified against canonical task_config")
    parser.add_argument("--diagnostics-interval", type=float, default=5.0, help="seconds between lifecycle service/filesystem snapshots")
    parser.add_argument(
        "--service-close-timeout",
        type=float,
        default=300.0,
        help="seconds to allow each lifecycle service graceful close",
    )
    parser.add_argument("--self-test", action="store_true", help="run route-proof summarizer self-test and exit")
    args = parser.parse_args(argv)
    if args.self_test and args.validate_lifecycle:
        parser.error("self-test and validate-lifecycle are mutually exclusive")
    if args.allow_partial and not args.validate_lifecycle:
        parser.error("allow-partial requires --validate-lifecycle")
    if args.validate_lifecycle:
        args.validate_lifecycle = Path(args.validate_lifecycle).expanduser().resolve()
        return args
    if args.skip_route_proof and (
        not args.run_vdbbench
        or args.vdbbench_dry_run
        or args.skip_load
        or not any(row.strip() for row in args.rows.split(","))
    ):
        parser.error("skip-route-proof requires at least one non-dry-run, load-enabled VDBBench row")
    try:
        validate_smoke_shape(args.smoke_dimension, args.smoke_documents, args.smoke_top_k, args.ef_search, args.rerank_candidates)
    except ValueError as exc:
        parser.error(str(exc))
    if args.num_per_batch <= 0:
        parser.error("num-per-batch must be positive")
    if args.lifecycle:
        rows = [row.strip() for row in args.rows.split(",") if row.strip()]
        if not args.run_vdbbench or args.vdbbench_dry_run or args.skip_load or len(rows) != 1:
            parser.error("lifecycle requires exactly one real, load-enabled VDBBench row")
        if args.profile != "command_wal_durable":
            parser.error("lifecycle completion currently requires command_wal_durable")
        if not loopback_host(args.host):
            parser.error("lifecycle host must be a loopback address")
        if args.skip_search_serial or args.skip_search_concurrent:
            parser.error("lifecycle requires serial and concurrent VDBBench search phases")
        try:
            extra_args = shlex.split(args.vdbbench_extra_args)
        except ValueError as exc:
            parser.error(f"invalid vdbbench-extra-args: {exc}")
        overridden = sorted({
            argument.partition("=")[0]
            for argument in extra_args
            if argument.partition("=")[0] in VDBBENCH_OWNED_OPTIONS
        })
        if overridden:
            parser.error(
                "lifecycle VDBBench options must use dedicated harness arguments: "
                + ", ".join(overridden)
            )
        if not args.lifecycle_dataset_file:
            parser.error("lifecycle requires --lifecycle-dataset-file")
        if args.case_type != "PerformanceCustomDataset":
            parser.error("lifecycle requires PerformanceCustomDataset for checksum-bound dataset evidence")
        if args.lifecycle_vectors <= 0 or args.lifecycle_dimensions <= 0:
            parser.error("custom lifecycle requires positive --lifecycle-vectors and --lifecycle-dimensions")
        if args.diagnostics_interval <= 0:
            parser.error("diagnostics-interval must be positive")
        if args.service_close_timeout <= 0:
            parser.error("service-close-timeout must be positive")
    args.out = Path(args.out).expanduser().resolve()
    args.validate_lifecycle = None
    args.vectordbbench_dir = Path(args.vectordbbench_dir).expanduser().resolve() if args.vectordbbench_dir else None
    args.lifecycle_dataset_file = (
        Path(args.lifecycle_dataset_file).expanduser().resolve() if args.lifecycle_dataset_file else None
    )
    if args.lifecycle and (args.lifecycle_dataset_file is None or not args.lifecycle_dataset_file.is_file()):
        parser.error("lifecycle dataset file must exist and be a regular file")
    if args.port == 0:
        args.port = find_free_port(args.host)
    args.base_url = f"http://{host_port(args.host, args.port)}"
    args.pprof_port = 0
    if args.lifecycle:
        for _ in range(10):
            candidate = find_free_port(args.host)
            if candidate != args.port:
                args.pprof_port = candidate
                break
        if args.pprof_port == 0:
            parser.error("could not select a free pprof port distinct from the service port")
    args.diagnostics_url = f"http://{host_port(args.host, args.pprof_port)}" if args.lifecycle else ""
    if not args.index_prefix:
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        args.index_prefix = f"treedb_vdbbench_{stamp}_{os.getpid()}"
    # Keep collection names conservative across shells and downstream tooling.
    args.index_prefix = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in args.index_prefix)
    args.data_dir = args.out / "treedb-data"
    return args


def self_test() -> None:
    response = {
        "metric": "cosine",
        "query_mode": "quantized_rerank",
        "quantized_index_name": DEFAULT_QUANTIZED_INDEX_NAME,
        "quantized_rerank_candidates": 32,
        "no_documents": True,
        "results": [{"id": "101", "ordinal": 0, "score": 1.0}],
        "stats": {
            "documents_fetched": 0,
            "document_bytes": 0,
            "quantized_scorer_active": 1,
            "quantized_score_calls": 7,
            "quantized_rerank_candidates": 4,
            "quantized_rerank_exact_score_calls": 4,
            "search_route_quantized_rerank": 1,
        },
        "diagnostics": {"route": "quantized_rerank", "fallback_reason": "none"},
    }
    summary = proof_summary("scalar", "idx", response, {"top_k": 1, "query_mode": "quantized_rerank"})
    assert summary["route"] == "quantized_rerank"
    assert summary["documents_fetched"] == 0
    assert summary["quantized_scorer_active"] == 1
    assert summary["quantized_rerank_exact_score_calls"] == 4
    assert summary["score_batch_optimized"] == 0
    assert fallback_reason({"diagnostics": {}}) == "none"
    print("self-test passed")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    if args.validate_lifecycle is not None:
        result = validate_lifecycle_artifact(args.validate_lifecycle)
        print(json.dumps(result, indent=2, sort_keys=True))
        partial_ok = (
            args.allow_partial
            and result["analyzable"]
            and result["result_status"] in {"partial", "interrupted"}
        )
        return 0 if result["complete"] or partial_ok else 1

    gomap_root = repo_root()
    try:
        prepare_artifact_root(args.out)
    except Exception as exc:  # noqa: BLE001
        print(f"harness failed before start; error={exc}", file=sys.stderr)
        return 2
    state = HarnessState(root=args.out)
    context = collect_context(gomap_root, args.vectordbbench_dir, args.out)
    if args.lifecycle:
        host = context.get("host", {})
        total_memory = host.get("memory_bytes")
        if isinstance(total_memory, bool) or not isinstance(total_memory, int) or total_memory <= 0:
            print("harness failed before start; error=positive host memory size is unavailable", file=sys.stderr)
            return 2
        physical_cpus = host.get("physical_cpu_count")
        if isinstance(physical_cpus, bool) or not isinstance(physical_cpus, int) or physical_cpus <= 0:
            print("harness failed before start; error=positive physical CPU count is unavailable", file=sys.stderr)
            return 2
        if not valid_storage_context(host.get("storage")):
            print("harness failed before start; error=benchmark storage identity is unavailable", file=sys.stderr)
            return 2
        sources = (context.get("gomap"), context.get("vectordbbench"))
        if any(
            not isinstance(source, dict)
            or source.get("dirty") is not False
            or not isinstance(source.get("commit"), str)
            or re.fullmatch(r"[0-9a-f]{40}", source["commit"]) is None
            for source in sources
        ):
            print("harness failed before start; error=clean source commit identity is unavailable", file=sys.stderr)
            return 2
    service_proc: subprocess.Popen[str] | None = None
    service_command: list[str] | None = None
    sampler: DiagnosticsSampler | None = None
    try:
        service_bin = build_service(state, gomap_root, args.service_bin or None)
        state.service_binary = file_identity(service_bin)
        service_proc, health, service_command = start_service(
            state,
            gomap_root=gomap_root,
            service_bin=service_bin,
            data_dir=args.data_dir,
            host=args.host,
            port=args.port,
            profile=args.profile,
            health_timeout=args.health_timeout,
            pprof_addr=host_port(args.host, args.pprof_port) if args.lifecycle else "",
        )
        state.lifecycle_started_ns = time.time_ns()
        state.health = health
        write_json(args.out / "health.json", health)
        if args.lifecycle:
            sampler = DiagnosticsSampler(
                f"{args.diagnostics_url}/debug/treedb/stats",
                args.out / "diagnostics.jsonl",
                args.diagnostics_interval,
                args.data_dir,
            )
            sampler.start()
            initialize_lifecycle_capture(state, args, sampler)
        run_vdbbench_tests(state, args=args, gomap_root=gomap_root, vectordbbench_dir=args.vectordbbench_dir)
        run_vdbbench_rows(
            state,
            args=args,
            gomap_root=gomap_root,
            vectordbbench_dir=args.vectordbbench_dir,
            base_url=args.base_url,
            index_prefix=args.index_prefix,
            sampler=sampler,
        )
        if args.lifecycle:
            assert sampler is not None
            service_command = complete_lifecycle(
                state, args, gomap_root, service_bin, service_proc, sampler
            )
            service_proc = None
            sampler = None
        elif args.skip_route_proof:
            add_skip(state, "route_proof", "skip-route-proof requested")
        else:
            run_route_proof_smoke(
                state,
                base_url=args.base_url,
                index_prefix=args.index_prefix,
                m=args.m,
                ef_construction=args.ef_construction,
                ef_search=args.ef_search,
                quantized_index_name=args.quantized_index_name,
                rerank_candidates=args.rerank_candidates,
                smoke_dimension=args.smoke_dimension,
                smoke_documents_count=args.smoke_documents,
                smoke_top_k=args.smoke_top_k,
            )
        write_readme(state, args)
        write_manifest(state, args=args, context=context, service_command=service_command)
        print(f"artifact_root={args.out}")
        print(f"manifest={args.out / 'manifest.json'}")
        if args.lifecycle:
            print(f"lifecycle_route_proof={args.out / 'lifecycle_route_response.json'}")
        elif not args.skip_route_proof:
            print(f"route_proof={args.out / 'route_proof.json'}")
        return 0
    except KeyboardInterrupt:
        if not args.lifecycle:
            raise
        if service_proc is not None:
            terminate_process_group(service_proc, graceful_timeout=args.service_close_timeout)
            service_proc = None
        with contextlib.suppress(Exception):
            finalize_partial_lifecycle(state, args, sampler, result_status="interrupted")
            sampler = None
        with contextlib.suppress(Exception):
            write_manifest(state, args=args, context=context, service_command=service_command)
        print(f"harness interrupted; artifact_root={args.out}", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - write failure artifact before exiting
        error = {"error": str(exc), "traceback": traceback.format_exc(), "generated_at": iso_now()}
        write_json(args.out / "harness_error.json", error)
        if service_proc is not None:
            terminate_process_group(service_proc, graceful_timeout=args.service_close_timeout)
            service_proc = None
        with contextlib.suppress(Exception):
            finalize_partial_lifecycle(state, args, sampler)
            sampler = None
        with contextlib.suppress(Exception):
            write_manifest(state, args=args, context=context, service_command=service_command)
        print(f"harness failed; artifact_root={args.out}; error={exc}", file=sys.stderr)
        return 1
    finally:
        if sampler is not None:
            sampler.stop()
        if service_proc is not None:
            terminate_process_group(service_proc, graceful_timeout=args.service_close_timeout)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
