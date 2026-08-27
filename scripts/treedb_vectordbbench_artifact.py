#!/usr/bin/env python3
"""Capture a reproducible TreeDB VectorDBBench artifact.

The harness owns a fresh TreeDB data directory, starts
cmd/treedb-document-service, writes command/context manifests, runs a small
route-proof smoke, and can optionally run selected VectorDBBench TreeDB rows
from a checkout named by VECTORDBBENCH_DIR.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import hashlib
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
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ARTIFACT_SCHEMA = "treedb-vectordbbench-artifact/v1"
ROUTE_PROOF_SCHEMA = "treedb-vectordbbench-route-proof/v2"
LIFECYCLE_SCHEMA = "treedb-vectordbbench-lifecycle/v1"
LIFECYCLE_EVENT_SCHEMA = "treedb-vectordbbench-lifecycle-event/v1"
LIFECYCLE_VALIDATION_SCHEMA = "treedb-vectordbbench-lifecycle-validation/v1"
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


def collect_context(gomap_root: Path, vectordbbench_dir: Path | None) -> dict[str, Any]:
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
        },
        "gomap": git_context(gomap_root),
        "vectordbbench": git_context(vectordbbench_dir) if vectordbbench_dir else {"available": False, "reason": "VECTORDBBENCH_DIR not set"},
    }


def find_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
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


def terminate_process_group(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=5)


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
) -> tuple[subprocess.Popen[str], dict[str, Any], list[str]]:
    service_log = state.root / "service.log"
    cmd = [str(service_bin), "-dir", str(data_dir), "-addr", f"{host}:{port}", "-profile", profile]
    data_dir.mkdir(parents=True, exist_ok=True)
    log_fh = service_log.open("w", encoding="utf-8")
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
        health = wait_health(f"http://{host}:{port}", health_timeout)
    except Exception:
        terminate_process_group(proc)
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


def proof_summary(kind: str, index_name: str, response: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
    stats = response.get("stats") or {}
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
        "harness": manifest.get("harness"),
    })


def _artifact_file(root: Path, relative: Any, label: str, errors: list[str]) -> Path | None:
    if not isinstance(relative, str) or not relative:
        errors.append(f"{label} path must be a non-empty relative path")
        return None
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        errors.append(f"{label} path escapes artifact root: {relative!r}")
        return None
    resolved = (root / path).resolve()
    try:
        resolved.relative_to(root.resolve())
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
    if not isinstance(value, str):
        errors.append(f"{label} must be an RFC3339 timestamp")
        return None
    try:
        parsed = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{label} must be an RFC3339 timestamp")
        return None
    if parsed.tzinfo is None:
        errors.append(f"{label} must include a timezone")
        return None
    return parsed.astimezone(_dt.timezone.utc)


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
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
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
    if not re.fullmatch(r"[0-9a-f]{64}", str(dataset.get("sha256") or "")):
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
    if not isinstance(service.get("profile"), str) or not service["profile"].strip():
        errors.append("manifest.service.profile must be non-empty")
    case_type = harness.get("case_type")
    if not isinstance(case_type, str) or not case_type.strip():
        errors.append("manifest.harness.case_type must be non-empty")
    concurrency = harness.get("num_concurrency")
    if isinstance(concurrency, int) and not isinstance(concurrency, bool):
        valid_concurrency = concurrency > 0
    elif isinstance(concurrency, str):
        parts = [part.strip() for part in concurrency.split(",")]
        valid_concurrency = bool(parts) and all(part.isdigit() and int(part) > 0 for part in parts)
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
        if not re.fullmatch(rf"[0-9a-f]{{{width}}}", str(actual or "")):
            errors.append(f"manifest {name} is missing or invalid")
        elif declared != actual:
            errors.append(f"lifecycle identity {name} does not match manifest")
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
    if not isinstance(host.get("storage"), dict) or not host["storage"]:
        errors.append("manifest context.host.storage must describe the benchmark storage")

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
                    event = json.loads(raw_line)
                except json.JSONDecodeError as exc:
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
        if not re.fullmatch(r"[0-9a-f]{64}", str(expected_hash or "")):
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

    for stage in LIFECYCLE_STAGES:
        if stage not in stage_events:
            completion_errors.append(f"missing required stage {stage}")
    if all(stage in stage_events for stage in LIFECYCLE_STAGES):
        positions = [events.index(stage_events[stage]) for stage in LIFECYCLE_STAGES]
        if positions != sorted(positions):
            completion_errors.append("required lifecycle stages are out of order")

    status = lifecycle.get("result_status")
    report["result_status"] = status
    if status not in {"completed", "partial", "interrupted"}:
        errors.append("lifecycle.result_status must be completed, partial, or interrupted")
    if status != "completed":
        completion_errors.append(f"result_status is {status!r}, not 'completed'")
    if events:
        report["last_stage"] = events[-1].get("stage")
        final_state = events[-1].get("state")
        rows = final_state.get("rows") if isinstance(final_state, dict) else None
        if isinstance(rows, dict):
            report["counts"] = {key: rows.get(key) for key in ("client_sent", "server_accepted", "server_durable", "reopened")}

    if expected_rows is not None:
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

    index_reference: tuple[str, int] | None = None
    for stage in ("optimize_end", "cache_prime", "cache_warm", "graceful_close", "cold_open_ready", "exact_verify", "route_verify"):
        if stage not in stage_events:
            continue
        state = stage_events[stage].get("state")
        index = state.get("index") if isinstance(state, dict) else {}
        if not isinstance(index, dict):
            index = {}
        current = (index.get("identity"), index.get("asset_generation"))
        if (
            not isinstance(current[0], str)
            or not current[0]
            or isinstance(current[1], bool)
            or not isinstance(current[1], int)
            or current[1] <= 0
            or index.get("status") != "ready"
        ):
            completion_errors.append(f"stage {stage} lacks a ready index identity and asset generation")
        elif index_reference is None:
            index_reference = current
        else:
            if current[0] != index_reference[0]:
                completion_errors.append(f"index identity changed at stage {stage}")
            if current[1] != index_reference[1]:
                completion_errors.append(f"index asset generation changed at stage {stage}")

    close_state = (stage_events.get("graceful_close") or {}).get("state")
    reopen_state = (stage_events.get("cold_open_ready") or {}).get("state")
    close_db = close_state.get("database") if isinstance(close_state, dict) else {}
    reopen_db = reopen_state.get("database") if isinstance(reopen_state, dict) else {}
    if not isinstance(close_db, dict):
        close_db = {}
    if not isinstance(reopen_db, dict):
        reopen_db = {}
    if not isinstance(close_db.get("identity"), str) or not close_db.get("identity"):
        completion_errors.append("graceful_close database.identity is missing")
    elif reopen_db.get("identity") != close_db.get("identity"):
        completion_errors.append("cold reopen database.identity does not match graceful close")
    close_commit = _nonnegative_int(close_db.get("commit_seq"), "graceful_close database.commit_seq", completion_errors)
    reopen_commit = _nonnegative_int(reopen_db.get("commit_seq"), "cold_open_ready database.commit_seq", completion_errors)
    if close_commit is not None and reopen_commit is not None and reopen_commit != close_commit:
        completion_errors.append("cold reopen database.commit_seq does not match graceful close")

    route_state = (stage_events.get("route_verify") or {}).get("state")
    route = route_state.get("route") if isinstance(route_state, dict) else {}
    if not isinstance(route, dict):
        route = {}
    if not (
        route.get("optimized") is True
        and route.get("fallback_reason") == "none"
        and isinstance(route.get("name"), str)
        and route.get("name")
        and index_reference is not None
        and route.get("index_identity") == index_reference[0]
        and route.get("index_asset_generation") == index_reference[1]
    ):
        completion_errors.append("optimized route proof failed or used a stale index asset generation")

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
        if (
            not isinstance(relative, str)
            or relative not in raw_by_path
            or profile.get("sha256") != raw_by_path.get(str(relative))
            or before not in sequence_events
            or after not in sequence_events
            or isinstance(before, bool)
            or isinstance(after, bool)
            or not isinstance(before, int)
            or not isinstance(after, int)
            or before >= after
        ):
            errors.append(f"profile state association {position} is invalid")
        if not isinstance(profile.get("kind"), str) or not profile.get("kind"):
            errors.append(f"profile {position} kind must be non-empty")

    if all(stage in stage_events for stage in ("load_start", "load_end", "drain_checkpoint", "optimize_end", "graceful_close", "cold_open_ready")):
        boundaries = [
            "load_start", "load_end", "drain_checkpoint", "optimize_end", "graceful_close", "cold_open_ready",
        ]
        timestamps = [stage_events[stage].get("_timestamp") for stage in boundaries]
        if all(isinstance(value, _dt.datetime) for value in timestamps):
            report["t_ready_seconds"] = (timestamps[-1] - timestamps[0]).total_seconds()

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
        record = run_command(
            state,
            f"vdbbench_{row}",
            cmd,
            cwd=vectordbbench_dir,
            env=env,
            timeout=args.vdbbench_timeout,
            required=True,
        )
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


def write_readme(state: HarnessState, args: argparse.Namespace) -> None:
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
            "mode": "vdbbench" if args.skip_route_proof else ("vdbbench+smoke" if args.run_vdbbench else "smoke"),
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
    parser.add_argument("--self-test", action="store_true", help="run route-proof summarizer self-test and exit")
    args = parser.parse_args(argv)
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
    args.out = Path(args.out).expanduser().resolve()
    args.validate_lifecycle = None
    args.vectordbbench_dir = Path(args.vectordbbench_dir).expanduser().resolve() if args.vectordbbench_dir else None
    if args.port == 0:
        args.port = find_free_port(args.host)
    args.base_url = f"http://{args.host}:{args.port}"
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
    context = collect_context(gomap_root, args.vectordbbench_dir)
    service_proc: subprocess.Popen[str] | None = None
    service_command: list[str] | None = None
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
        )
        state.health = health
        write_json(args.out / "health.json", health)
        run_vdbbench_tests(state, args=args, gomap_root=gomap_root, vectordbbench_dir=args.vectordbbench_dir)
        run_vdbbench_rows(
            state,
            args=args,
            gomap_root=gomap_root,
            vectordbbench_dir=args.vectordbbench_dir,
            base_url=args.base_url,
            index_prefix=args.index_prefix,
        )
        if args.skip_route_proof:
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
        if not args.skip_route_proof:
            print(f"route_proof={args.out / 'route_proof.json'}")
        return 0
    except Exception as exc:  # noqa: BLE001 - write failure artifact before exiting
        error = {"error": str(exc), "traceback": traceback.format_exc(), "generated_at": iso_now()}
        write_json(args.out / "harness_error.json", error)
        with contextlib.suppress(Exception):
            write_manifest(state, args=args, context=context, service_command=service_command)
        print(f"harness failed; artifact_root={args.out}; error={exc}", file=sys.stderr)
        return 1
    finally:
        if service_proc is not None:
            terminate_process_group(service_proc)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
