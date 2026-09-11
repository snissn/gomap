#!/usr/bin/env python3
"""Fail-closed route probes for VectorDBBench search on an existing TreeDB index.

This helper owns one retained-index route envelope: exclusive lock, isolation
monitor, exact authorized service process, diagnostic and production probes,
their matching unmodified VectorDBBench --skip-load/--skip-drop-old commands,
service teardown, and checksum-bound evidence. It never creates, resets,
optimizes, or mutates an index; preexisting outputs and reordered runs fail.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
from datetime import datetime, timezone
from dataclasses import asdict, is_dataclass
import hashlib
import json
import math
import signal
import threading
import time
from urllib.parse import urlsplit
from urllib.request import urlopen
import re
from pathlib import Path
import subprocess
import sys
import os
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients" / "python" / "treedb_client" / "src"))

from treedb_client import TreeDBClient


def fail(message: str) -> None:
    raise ValueError(message)


def positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        fail(f"{name} must be a positive integer")
    return value


def jsonable(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(value), indent=2, sort_keys=True) + "\n")

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n"
    return hashlib.sha256(payload.encode()).hexdigest()


def dataset_file_checksums(dataset_dir: str) -> dict[str, str]:
    root = Path(dataset_dir)
    return {
        name: sha256_file(root / name)
        for name in ("test.parquet", "neighbors.parquet")
    }


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
def swap_used_bytes() -> int:
    fields: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, separator, value = line.partition(":")
        if separator and key in {"SwapTotal", "SwapFree"}:
            fields[key] = int(value.strip().split()[0]) * 1024
    if set(fields) != {"SwapTotal", "SwapFree"}:
        fail("/proc/meminfo did not expose swap counters")
    return fields["SwapTotal"] - fields["SwapFree"]


def competing_processes() -> list[dict[str, Any]]:
    result = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,args="], text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    rows = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(maxsplit=2)
        if len(fields) == 3:
            rows.append({"pid": int(fields[0]), "ppid": int(fields[1]), "command": fields[2]})
    owned = {os.getpid()}
    while True:
        descendants = {row["pid"] for row in rows if row["ppid"] in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    return [
        row for row in rows
        if row["pid"] not in owned
        and ("treedb-document-service" in row["command"].lower()
             or "vectordbbench" in row["command"].lower()
             or "vectordb_bench" in row["command"].lower())
    ]


def isolation_sample() -> dict[str, Any]:
    return {
        "timestamp": iso_now(),
        "swap_used_bytes": swap_used_bytes(),
        "competing_processes": competing_processes(),
    }


class IsolationMonitor:
    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.samples = [isolation_sample()]
        self.stop_event = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop_event.wait(self.interval):
                self.samples.append(isolation_sample())
        except BaseException as exc:
            self.error = exc
            self.stop_event.set()

    def stop(self) -> list[dict[str, Any]]:
        self.stop_event.set()
        self.thread.join()
        if self.error is not None:
            raise RuntimeError("search isolation monitor failed") from self.error
        self.samples.append(isolation_sample())
        return self.samples


def frozen_gomaxprocs() -> int:
    value = os.environ.get("GOMAXPROCS")
    if value != "12":
        fail("GOMAXPROCS must equal the frozen value 12")
    return 12


def service_argv(args: argparse.Namespace) -> list[str]:
    return [
        str(args.service_bin), "-dir", str(args.artifact_root / "treedb-data"),
        "-addr", args.service_addr, "-profile", "command_wal_durable",
    ]


def service_environment(args: argparse.Namespace) -> dict[str, str]:
    """Return the complete, non-inherited environment for the search service."""
    return {"GOMAXPROCS": str(args.gomaxprocs)}


def vdbbench_environment(
    args: argparse.Namespace, kind: str, result_root: Path,
) -> dict[str, str]:
    """Return the complete, non-inherited environment for VectorDBBench."""
    return {
        "GOMAXPROCS": str(args.gomaxprocs),
        "RESULTS_LOCAL_DIR": str(result_root),
        "PYTHONPATH": os.pathsep.join((
            str(args.vectordbbench_dir),
            str(ROOT / "clients" / "python" / "treedb_client" / "src"),
        )),
        "LOG_FILE": str(args.artifact_root / f"vdbbench-{args.route}-{kind}.log"),
        "NUM_PER_BATCH": "500",
    }


def wait_healthy(process: subprocess.Popen[Any], base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            fail(f"search service exited before health with {process.returncode}")
        try:
            with urlopen(f"{base_url.rstrip('/')}/v1/health", timeout=2):
                return
        except BaseException as exc:
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError("search service did not become healthy") from last_error


def stop_service(process: subprocess.Popen[Any]) -> int:
    if process.poll() is not None:
        fail(f"search service exited unexpectedly with {process.returncode}")
    os.killpg(process.pid, signal.SIGTERM)
    try:
        return_code = process.wait(timeout=30)
    except subprocess.TimeoutExpired as exc:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        raise RuntimeError("search service did not close within 30 seconds") from exc
    if return_code != 0:
        fail(f"search service did not close cleanly: exit={return_code}")
    return return_code


def append_command_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode()
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        if os.write(descriptor, payload) != len(payload):
            fail("short probe command ledger write")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_origin(
    args: argparse.Namespace,
    kind: str,
    result: Path,
    response: Path,
    output: Path,
    command_record: dict[str, Any],
    isolation_binding: dict[str, str],
    search_started_at: str,
    search_completed_at: str,
) -> None:
    write_json(output, {
        "schema_version": "treedb-construction-policy-4587-search-origin/v3",
        "run_id": args.run_id,
        "artifact_root": str(args.artifact_root),
        "manifest_sha256": args.manifest_sha256,
        "execution_commit": args.execution_commit,
        "dataset_sha256": args.dataset_sha256,
        "scale": args.scale,
        "role": args.role,
        "partition": args.partition,
        "ef_construction": args.ef_construction,
        "lifecycle_sha256": args.lifecycle_sha256,
        "lifecycle_started_at": args.lifecycle_started_at,
        "lifecycle_completed_at": args.lifecycle_completed_at,
        "kind": kind,
        "route": "exact" if args.route == "exact" else "scalar_u8_rerank",
        "result_sha256": sha256_file(result),
        "response_sha256": sha256_file(response),
        "index_metadata_sha256": sha256_file(args.metadata_out),
        "command_ledger_path": str(args.command_ledger.relative_to(args.artifact_root)),
        "command_sequence": command_record["sequence"],
        "command_record_sha256": canonical_sha256(command_record),
        "probe_started_at": command_record["probe_started_at"],
        "probe_completed_at": command_record["probe_completed_at"],
        "service_binary_sha256": args.service_binary_sha256,
        "service_argv": service_argv(args),
        "isolation": isolation_binding,
        "search_started_at": search_started_at,
        "search_completed_at": search_completed_at,
    })


def load_query(path: Path, dimensions: int) -> list[float]:
    value = json.loads(path.read_text())
    if isinstance(value, dict):
        value = value.get("query_embedding")
    if not isinstance(value, list) or len(value) != dimensions:
        fail(f"query must contain exactly {dimensions} coordinates")
    query = []
    for position, coordinate in enumerate(value):
        if isinstance(coordinate, bool) or not isinstance(coordinate, (int, float)) or not math.isfinite(coordinate):
            fail(f"query coordinate {position} must be finite")
        query.append(float(coordinate))
    return query


def validate_metadata(info: dict[str, Any], args: argparse.Namespace) -> None:
    expected = {
        "name": args.index_name,
        "dimension": args.dimensions,
        "metric": args.metric,
        "generation": args.expected_generation,
        "vector_strategy": "column_graph",
        "vector_m": args.m,
        "vector_ef_construction": args.ef_construction,
        "vector_ef_search": args.ef_search,
    }
    for key, wanted in expected.items():
        if info.get(key) != wanted:
            fail(f"existing index {key}={info.get(key)!r}, want {wanted!r}")
    quantized = info.get("quantized_indexes")
    if not isinstance(quantized, list) or not any(
        isinstance(item, dict) and item.get("name") == args.quantized_index_name for item in quantized
    ):
        fail(f"existing index is missing quantized score plane {args.quantized_index_name!r}")


def validate_diagnostic(response: dict[str, Any], args: argparse.Namespace) -> None:
    expected_mode = "exact" if args.route == "exact" else "quantized_rerank"
    expected_route = "exact_hnsw_search_pack_v1" if args.route == "exact" else "quantized_rerank"
    if response.get("no_documents") is not True:
        fail("diagnostic response did not use the no-document boundary")
    if response.get("query_mode") != expected_mode:
        fail("diagnostic response query mode does not match the requested route")
    results = response.get("results")
    if not isinstance(results, list) or len(results) != args.top_k:
        fail("diagnostic response result count does not equal topK")
    diagnostics = response.get("diagnostics")
    if not isinstance(diagnostics, dict):
        fail("diagnostic response is missing diagnostics")
    if (diagnostics.get("fallback_reason") or "none") != "none":
        fail("diagnostic response used a fallback")
    if diagnostics.get("route") != expected_route:
        fail("diagnostic response did not use the intended route")
    stats = response.get("stats")
    if not isinstance(stats, dict):
        fail("diagnostic response is missing stats")
    if not {"documents_fetched", "document_bytes"} <= set(stats):
        fail("diagnostic response is missing document counters")
    if stats["documents_fetched"] != 0 or stats["document_bytes"] != 0:
        fail("diagnostic response fetched or materialized documents")
    if args.route == "scalar_u8_rerank":
        calls = positive_int(stats.get("quantized_rerank_exact_score_calls"), "exact rerank calls")
        if not args.top_k <= calls <= args.effective_rerank_candidates:
            fail("diagnostic response exceeded the frozen exact-rerank boundary")


def validate_production(response: dict[str, Any], args: argparse.Namespace) -> None:
    if response.get("response_format") != "ids":
        fail("production response did not use the IDs-only transport")
    ids = response.get("ids")
    if not isinstance(ids, list) or len(ids) != args.top_k:
        fail("production response result count does not equal topK")


def vdbbench_command(args: argparse.Namespace, kind: str) -> list[str]:
    command = [
        str(args.python_executable), "-m", "vectordb_bench.cli.vectordbbench",
        "treedbcolumngraphexact" if args.route == "exact" else "treedbscalaru8rerank",
        "--base-url", args.base_url, "--index-name", args.index_name,
        "--skip-drop-old", "--skip-load", "--search-serial", "--search-concurrent",
        "--m", str(args.m), "--ef-construction", str(args.ef_construction),
        "--ef-search", str(args.ef_search), "--case-type", "PerformanceCustomDataset",
        "--k", str(args.top_k), "--num-concurrency", "32", "--concurrency-duration", "30",
        "--custom-case-name", f"treedb-4587-{args.scale}-{args.role}-{kind}-{args.route}",
        "--custom-dataset-name", args.dataset_name, "--custom-dataset-dir", str(args.dataset_dir),
        "--custom-dataset-size", str(args.scale), "--custom-dataset-dim", str(args.dimensions),
        "--custom-dataset-file-count", "1", "--query-embedding-encoding", "f32_le",
    ]
    if kind == "diagnostic":
        command.extend(["--stats-mode", "full_diagnostics", "--response-format", "full"])
    else:
        command.extend([
            "--stats-mode", "production", "--response-format", "ids",
            "--skip-vector-index-guards",
        ])
    if args.route == "scalar_u8_rerank":
        command.extend([
            "--quantized-index-name", args.quantized_index_name,
            "--quantized-rerank-candidates", str(args.rerank_candidates),
        ])
    return command


def run_vdbbench_command(
    args: argparse.Namespace, kind: str, response_path: Path,
    probe_started_at: str, probe_completed_at: str,
) -> tuple[Path, dict[str, Any]]:
    result_path = args.diagnostic_result if kind == "diagnostic" else args.production_result
    result_root = args.artifact_root / f"vdbbench-results-{args.route}-{kind}"
    if result_path.exists() or result_root.exists():
        fail(f"{kind} canonical result destination already exists")
    command = vdbbench_command(args, kind)
    dataset_files_before = dataset_file_checksums(args.dataset_dir)
    started_at = iso_now()
    env = vdbbench_environment(args, kind, result_root)
    bound_env = dict(env)
    completed = subprocess.run(command, cwd=args.vectordbbench_dir, env=env)
    dataset_files_after = dataset_file_checksums(args.dataset_dir)
    if dataset_files_after != dataset_files_before:
        fail(f"{kind} canonical VectorDBBench command changed its search dataset files")
    completed_at = iso_now()
    if completed.returncode != 0:
        fail(f"{kind} canonical VectorDBBench command failed with exit {completed.returncode}")
    generated = list(result_root.rglob("result_*.json"))
    if len(generated) != 1:
        fail(f"{kind} canonical VectorDBBench command must create exactly one result")
    result_path.parent.mkdir(parents=True, exist_ok=True)
    generated[0].replace(result_path)
    existing_records = args.command_ledger.read_text().splitlines() if args.command_ledger.exists() else []
    record = {
        "schema_version": "treedb-construction-policy-4587-probe-command/v5",
        "sequence": len(existing_records),
        "argv": [str(Path(__file__).resolve()), *sys.argv[1:]],
        "helper_sha256": sha256_file(Path(__file__).resolve()),
        "python_executable": str(Path(sys.executable).resolve()),
        "python_sha256": sha256_file(Path(sys.executable).resolve()),
        "vdbbench_argv": command,
        "vdbbench_env": bound_env,
        "kind": kind,
        "started_at": started_at,
        "completed_at": completed_at,
        "probe_started_at": probe_started_at,
        "probe_completed_at": probe_completed_at,
        "exit_code": completed.returncode,
        "query_sha256": sha256_file(args.query_json),
        "dataset_files_before_sha256": dataset_files_before,
        "dataset_files_after_sha256": dataset_files_after,
        "run_id": args.run_id,
        "route": args.route,
        "result_sha256": sha256_file(result_path),
        "response_sha256": sha256_file(response_path),
        "index_metadata_sha256": sha256_file(args.metadata_out),
    }
    append_command_record(args.command_ledger, record)
    return result_path, record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--query-json", required=True, type=Path)
    parser.add_argument("--metadata-out", required=True, type=Path)
    parser.add_argument("--diagnostic-response-out", required=True, type=Path)
    parser.add_argument("--production-response-out", required=True, type=Path)
    parser.add_argument("--diagnostic-result", required=True, type=Path)
    parser.add_argument("--production-result", required=True, type=Path)
    parser.add_argument("--diagnostic-origin-out", required=True, type=Path)
    parser.add_argument("--production-origin-out", required=True, type=Path)
    parser.add_argument("--command-ledger", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--execution-commit", required=True)
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument("--scale", required=True, type=int)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dataset-dir", required=True, type=Path)
    parser.add_argument("--vectordbbench-dir", required=True, type=Path)
    parser.add_argument("--role", required=True)
    parser.add_argument("--partition", required=True)
    parser.add_argument("--lifecycle-sha256", required=True)
    parser.add_argument("--lifecycle-started-at", required=True)
    parser.add_argument("--lifecycle-completed-at", required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--metric", default="cosine", choices=("cosine",))
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, required=True)
    parser.add_argument("--ef-search", type=int, default=192)
    parser.add_argument("--expected-generation", type=int, required=True)
    parser.add_argument("--route", choices=("exact", "scalar_u8_rerank"), required=True)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--quantized-index-name", default="embedding.scalar_u8.fast")
    parser.add_argument("--rerank-candidates", type=int, default=400)
    parser.add_argument("--effective-rerank-candidates", type=int, default=192)
    parser.add_argument("--timeout", type=float, default=3600.0)
    parser.add_argument("--service-bin", required=True, type=Path)
    parser.add_argument("--service-binary-sha256", required=True)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--python-sha256", required=True)
    parser.add_argument("--search-isolation-out", required=True, type=Path)
    parser.add_argument("--exclusive-lock", required=True, type=Path)
    parser.add_argument("--diagnostics-interval", type=float, default=5.0)
    parser.add_argument("--service-health-timeout", type=float, default=60.0)
    args = parser.parse_args()
    for name in ("dimensions", "m", "ef_construction", "ef_search", "expected_generation",
                 "top_k", "rerank_candidates", "effective_rerank_candidates", "scale"):
        positive_int(getattr(args, name), name)
    if args.diagnostics_interval <= 0 or args.service_health_timeout <= 0:
        fail("search diagnostics and health intervals must be positive")
    if args.effective_rerank_candidates > args.rerank_candidates:
        fail("effective rerank candidates cannot exceed the configured shortlist")
    args.artifact_root = args.artifact_root.resolve()
    for name in ("manifest_sha256", "dataset_sha256", "lifecycle_sha256",
                 "service_binary_sha256", "python_sha256"):
        if re.fullmatch(r"[0-9a-f]{64}", getattr(args, name)) is None:
            fail(f"{name} must be a lowercase SHA-256")
    if re.fullmatch(r"[0-9a-f]{40}", args.execution_commit) is None:
        fail("execution_commit must be a lowercase commit digest")
    if not args.run_id or not args.role or not args.partition:
        fail("run identity strings must be non-empty")
    parsed_url = urlsplit(args.base_url)
    if (parsed_url.scheme != "http"
            or parsed_url.hostname not in {"127.0.0.1", "::1", "localhost"}
            or parsed_url.port is None or parsed_url.path not in {"", "/"}
            or parsed_url.query or parsed_url.fragment):
        fail("base-url must be a loopback HTTP origin with an explicit port")
    host = parsed_url.hostname
    args.service_addr = f"[{host}]:{parsed_url.port}" if ":" in host else f"{host}:{parsed_url.port}"
    args.service_bin = args.service_bin.resolve()
    if not args.service_bin.is_file() or sha256_file(args.service_bin) != args.service_binary_sha256:
        fail("authorized search service binary checksum mismatch")
    if (not args.python_executable.is_absolute()
            or not args.python_executable.is_file()
            or sha256_file(args.python_executable) != args.python_sha256):
        fail("frozen VectorDBBench Python interpreter checksum mismatch")
    path_names = (
        "query_json", "metadata_out", "diagnostic_response_out",
        "production_response_out", "diagnostic_result", "production_result",
        "diagnostic_origin_out", "production_origin_out", "command_ledger",
        "search_isolation_out",
    )
    for name in path_names:
        path = getattr(args, name).resolve()
        try:
            path.relative_to(args.artifact_root)
        except ValueError:
            fail(f"search evidence path escapes artifact root: {path}")
        setattr(args, name, path)
    args.dataset_dir = args.dataset_dir.resolve()
    if not args.dataset_dir.is_dir():
        fail("dataset_dir must exist")
    args.vectordbbench_dir = args.vectordbbench_dir.resolve()
    if not args.vectordbbench_dir.is_dir():
        fail("vectordbbench_dir must exist")
    args.exclusive_lock = args.exclusive_lock.resolve()
    args.exclusive_lock.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = args.artifact_root / "manifest.json"
    if sha256_file(manifest_path) != args.manifest_sha256:
        fail("manifest_sha256 does not match artifact manifest")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("artifact_root") != str(args.artifact_root):
        fail("manifest artifact root does not match --artifact-root")
    service = manifest.get("service")
    binary = service.get("binary") if isinstance(service, dict) else None
    if (not isinstance(service, dict)
            or Path(service.get("data_dir", "")).resolve()
            != (args.artifact_root / "treedb-data").resolve()
            or service.get("base_url") != args.base_url
            or service.get("profile") != "command_wal_durable"
            or not isinstance(binary, dict)
            or binary.get("sha256") != args.service_binary_sha256):
        fail("manifest service identity does not match frozen search service")
    args.gomaxprocs = frozen_gomaxprocs()
    lifecycle = manifest.get("lifecycle")
    if not isinstance(lifecycle, dict) or lifecycle.get("sha256") != args.lifecycle_sha256:
        fail("lifecycle_sha256 does not match artifact manifest")
    lifecycle_path = args.artifact_root / lifecycle.get("file", "")
    if sha256_file(lifecycle_path) != args.lifecycle_sha256:
        fail("lifecycle_sha256 does not match lifecycle file")
    events = [json.loads(line) for line in lifecycle_path.read_text().splitlines()]
    starts = [event.get("timestamp") for event in events if event.get("stage") == "startup"]
    completed = [event.get("timestamp") for event in events if event.get("stage") == "teardown"]
    if starts != [args.lifecycle_started_at] or completed != [args.lifecycle_completed_at]:
        fail("lifecycle timestamps do not match lifecycle file")
    expected_records = 0 if args.route == "exact" else 2
    existing_records = args.command_ledger.read_text().splitlines() if args.command_ledger.exists() else []
    if len(existing_records) != expected_records:
        fail("route execution is not append-only or is out of order")
    outputs = (
        args.metadata_out, args.diagnostic_response_out, args.production_response_out,
        args.diagnostic_result, args.production_result, args.diagnostic_origin_out,
        args.production_origin_out, args.search_isolation_out,
        args.artifact_root / f"search-service-{args.route}.log",
        args.artifact_root / f"vdbbench-{args.route}-diagnostic.log",
        args.artifact_root / f"vdbbench-{args.route}-production.log",
        args.artifact_root / f"vdbbench-results-{args.route}-diagnostic",
        args.artifact_root / f"vdbbench-results-{args.route}-production",
    )
    for output in outputs:
        if output.exists():
            fail(f"search evidence output already exists: {output}")
    return args

def main() -> int:
    args = parse_args()
    query = load_query(args.query_json, args.dimensions)
    lock_file = args.exclusive_lock.open("a+")
    monitor: IsolationMonitor | None = None
    service: subprocess.Popen[Any] | None = None
    client: TreeDBClient | None = None
    service_log = None
    captured: list[tuple[str, Path, Path, Path, dict[str, Any]]] = []
    failure: BaseException | None = None
    lock_acquired_at = ""
    service_started_at = ""
    service_exit_code: int | None = None
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fail("frozen search exclusive lock is already held")
        lock_acquired_at = iso_now()
        monitor = IsolationMonitor(args.diagnostics_interval)
        search_started_at = monitor.samples[0]["timestamp"]
        log_path = args.artifact_root / f"search-service-{args.route}.log"
        service_log = log_path.open("xb")
        if (monitor.samples[0]["swap_used_bytes"] != 0
                or monitor.samples[0]["competing_processes"]):
            fail("search isolation is not clean before service launch")
        env = service_environment(args)
        service_started_at = iso_now()
        service = subprocess.Popen(
            service_argv(args), stdout=service_log, stderr=subprocess.STDOUT,
            env=env, start_new_session=True)
        wait_healthy(service, args.base_url, args.service_health_timeout)
        client = TreeDBClient(args.base_url, timeout=args.timeout)
        before = jsonable(client.open_index(args.index_name))
        validate_metadata(before, args)
        write_json(args.metadata_out, before)
        common = {
            "ef_search": args.ef_search,
            "query_mode": "exact" if args.route == "exact" else "quantized_rerank",
            "expected_generation": args.expected_generation,
            "query_embedding_encoding": "f32_le",
        }
        if args.route == "scalar_u8_rerank":
            common.update({
                "quantized_index_name": args.quantized_index_name,
                "quantized_rerank_candidates": args.rerank_candidates,
            })
        for kind, response_path, origin_path in (
            ("diagnostic", args.diagnostic_response_out, args.diagnostic_origin_out),
            ("production", args.production_response_out, args.production_origin_out),
        ):
            probe_started_at = iso_now()
            if kind == "diagnostic":
                response = jsonable(client.search_vector_index(
                    args.index_name, query, args.top_k, stats_mode="full_diagnostics",
                    response_format="full", **common))
                validate_diagnostic(response, args)
            else:
                response = jsonable(client.search_vector_index(
                    args.index_name, query, args.top_k, stats_mode="production",
                    response_format="ids", **common))
                validate_production(response, args)
            probe_completed_at = iso_now()
            write_json(response_path, response)
            result_path, command_record = run_vdbbench_command(
                args, kind, response_path, probe_started_at, probe_completed_at)
            captured.append((kind, result_path, response_path, origin_path, command_record))
        after = jsonable(client.open_index(args.index_name))
        validate_metadata(after, args)
        if after != before:
            fail("existing index identity changed during canonical search commands")
    except BaseException as exc:
        failure = exc
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()
        if service is not None:
            try:
                service_exit_code = stop_service(service)
            except BaseException as exc:
                failure = failure or exc
        service_completed_at = iso_now()
        samples: list[dict[str, Any]] = []
        if monitor is not None:
            try:
                samples = monitor.stop()
            except BaseException as exc:
                failure = failure or exc
        try:
            search_completed_at = samples[-1]["timestamp"] if samples else iso_now()
            if samples:
                isolation = {
                    "schema_version": "treedb-construction-policy-4587-search-isolation/v3",
                    "artifact_root": str(args.artifact_root),
                    "lock_path": str(args.exclusive_lock),
                    "lock_acquired_at": lock_acquired_at,
                    "coverage_completed_at": search_completed_at,
                    "gomaxprocs": args.gomaxprocs,
                    "service_environment": service_environment(args),
                    "service_binary_sha256": args.service_binary_sha256,
                    "service_argv": service_argv(args),
                    "service_started_at": service_started_at,
                    "service_completed_at": service_completed_at,
                    "service_exit_code": service_exit_code,
                    "samples": samples,
                }
                write_json(args.search_isolation_out, isolation)
                if any(
                    sample["swap_used_bytes"] != 0 or sample["competing_processes"]
                    for sample in samples
                ):
                    failure = failure or ValueError(
                        "search isolation observed swap or competing processes")
                isolation_binding = {
                    "path": args.search_isolation_out.relative_to(args.artifact_root).as_posix(),
                    "sha256": sha256_file(args.search_isolation_out),
                }
                if failure is None:
                    for kind, result, response, output, command in captured:
                        write_origin(
                            args, kind, result, response, output, command, isolation_binding,
                            search_started_at, search_completed_at)
        except BaseException as exc:
            failure = failure or exc
        with contextlib.suppress(OSError):
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()
        if service_log is not None:
            service_log.close()
    if failure is not None:
        raise failure
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"INVALID: {exc}")
        raise SystemExit(1) from exc
