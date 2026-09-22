#!/usr/bin/env python3
"""Assemble the compact #4019 qualification result from retained raw rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from system_qualification import _load, _sha256, matched_recall_buckets, validate_result


TREE_ROWS = {
    "treedb_single_daemon": "single_daemon_four_group",
    "treedb_native_multi_daemon": "native_four_daemon_four_group",
    "treedb_container_multi_daemon": "container_four_daemon_four_group",
}
EXTERNAL_ROWS = {"milvus_standalone": "milvus_standalone", "postgres_pgvector": "pgvector"}
CORPUS_IDENTITY_KEYS = ("id", "fixture_checksum", "manifest_sha256", "truth_identity", "truth_artifact_sha256", "truth_sha256")
CONTAINER_ALLOCATIONS = [
    {"cpuset_cpus": cpus, "memory_bytes": 6 * 1024**3, "memory_swap_bytes": 6 * 1024**3, "pids_limit": 768}
    for cpus in ("0-2", "3-5", "6-8", "9-11")
]


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def semantic_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def checked_bytes(path: Path, inventory: list[dict[str, Any]]) -> bytes:
    raw = path.read_bytes()
    inventory.append({"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    return raw


def checked_json(path: Path, inventory: list[dict[str, Any]]) -> Any:
    return json.loads(checked_bytes(path, inventory))


def command_flag(command: list[str], name: str) -> str:
    if command.count(name) != 1:
        raise ValueError(f"command must contain exactly one {name}")
    index = command.index(name)
    if index + 1 == len(command) or not isinstance(command[index + 1], str):
        raise ValueError(f"command has no value for {name}")
    return command[index + 1]


def tree_input_identity(search: dict[str, Any], command_path: Path, expected: dict[str, Any], inventory: list[dict[str, Any]]) -> dict[str, Any]:
    command = checked_json(command_path, inventory)
    if not isinstance(command, list) or not all(isinstance(value, str) for value in command):
        raise ValueError(f"malformed benchmark command {command_path}")
    manifest_path = Path(command_flag(command, "-dataset")) / "fixture_manifest.json"
    manifest_raw = checked_bytes(manifest_path, inventory)
    manifest = json.loads(manifest_raw)
    truth_path = Path(command_flag(command, "-truth-cache")) / f"m8_canonical_truth_{expected['truth_identity']}.json"
    truth_raw = checked_bytes(truth_path, inventory)
    truth = json.loads(truth_raw)
    actual = {
        "id": expected["id"],
        "fixture_checksum": search.get("dataset_checksum"),
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "truth_identity": truth.get("identity"),
        "truth_artifact_sha256": hashlib.sha256(truth_raw).hexdigest(),
        "truth_sha256": truth.get("truth_sha256"),
    }
    if manifest.get("checksum") != actual["fixture_checksum"] or truth.get("dataset_checksum") != actual["fixture_checksum"] or search.get("truth_artifact_sha256") != actual["truth_artifact_sha256"] or command_flag(command, "-truth-cache-sha256") != actual["truth_artifact_sha256"] or actual != expected:
        raise ValueError(f"TreeDB run does not bind accepted corpus {expected['id']}")
    return actual


def external_input_identity(search: dict[str, Any], command_path: Path, expected: dict[str, Any], inventory: list[dict[str, Any]]) -> dict[str, Any]:
    command = checked_json(command_path, inventory)
    if not isinstance(command, list) or not all(isinstance(value, str) for value in command):
        raise ValueError(f"malformed adapter command {command_path}")
    dataset = Path(command_flag(command, "--dataset-dir"))
    if dataset.resolve() != Path(search.get("dataset_dir", "")).resolve():
        raise ValueError("adapter command and result use different datasets")
    manifest = checked_json(dataset / "manifest.json", inventory)
    actual = {
        "id": expected["id"],
        "fixture_checksum": manifest.get("fixture_checksum"),
        "manifest_sha256": expected["manifest_sha256"],
        "truth_identity": manifest.get("truth_identity"),
        "truth_artifact_sha256": manifest.get("truth_artifact_sha256"),
        "truth_sha256": manifest.get("truth_sha256"),
    }
    for name, contract in manifest.get("files", {}).items():
        raw = checked_bytes(dataset / name, inventory)
        if contract != {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}:
            raise ValueError(f"exported dataset file {name} does not match its manifest")
    if actual != expected:
        raise ValueError(f"external run does not bind accepted corpus {expected['id']}")
    return actual


def time_fields(path: Path, inventory: list[dict[str, Any]]) -> tuple[float, int]:
    text = path.read_text(encoding="utf-8")
    inventory.append({"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256(path)})
    user = re.search(r"User time \(seconds\): ([0-9.]+)", text)
    system = re.search(r"System time \(seconds\): ([0-9.]+)", text)
    rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
    if not user or not system or not rss:
        raise ValueError(f"malformed time record {path}")
    return float(user.group(1)) + float(system.group(1)), int(rss.group(1)) * 1024


def historical_load(sysstat: Path, output: Path) -> list[dict[str, Any]]:
    raw = subprocess.run(["sadf", "-j", str(sysstat), "--", "-q"], check=True, capture_output=True, text=True).stdout
    host = json.loads(raw)["sysstat"]["hosts"][0]
    samples = [{"timestamp": item["timestamp"], **item["queue"]} for item in host["statistics"]]
    write_json(output, {"source_sha256": _sha256(sysstat), "timezone": host["timezone"], "samples": samples})
    return samples


def nearest_load(samples: list[dict[str, Any]], started_at: str) -> dict[str, Any]:
    started = datetime.fromisoformat(re.sub(r"(\.\d{6})\d*Z$", r"\1+00:00", started_at))
    candidates = []
    for sample in samples:
        stamp = sample["timestamp"]
        value = datetime.fromisoformat(f"{stamp['date']}T{stamp['time']}-10:00").astimezone(timezone.utc)
        candidates.append((abs((value - started).total_seconds()), sample))
    distance, sample = min(candidates, key=lambda value: value[0])
    if distance > 601:
        raise ValueError(f"no bounded sysstat sample for {started_at}")
    return {"valid": True, "load_1": sample["ldavg-1"], "load_5": sample["ldavg-5"], "load_15": sample["ldavg-15"]}


def tree_search_path(root: Path, topology: str, corpus: str, repetition: int) -> Path:
    base = root / "verified-runs" / topology / corpus / f"repeat-{repetition}"
    return base / ("client/search.json" if topology.startswith("container") else "search.json")


def tree_repetition(root: Path, topology: str, corpus: str, repetition: int, expected: dict[str, Any], samples: list[dict[str, Any]], inventory: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    search_path = tree_search_path(root, topology, corpus, repetition)
    base = search_path.parent.parent if topology.startswith("container") else search_path.parent
    search = checked_json(search_path, inventory)
    runner = checked_json(base / "runner.json", inventory)
    resources = {key: runner["resources"][key] for key in ("cpu_seconds", "peak_rss_bytes", "persistent_bytes", "temporary_bytes", "network_rx_bytes", "network_tx_bytes", "swap_bytes")}
    input_identity = tree_input_identity(search, search_path.parent / "bench.command.json", expected, inventory)
    if topology.startswith("container"):
        resources["container_allocations"] = runner["resources"].get("allocations")
        if resources["container_allocations"] != CONTAINER_ALLOCATIONS:
            raise ValueError("container run does not enforce four disjoint 3-CPU/6-GiB allocations")
    return {
        "repetition": repetition,
        "status": "valid",
        "input_identity": input_identity,
        "noise": nearest_load(samples, search["started_at"]),
        "warmup_queries_per_cell": search["warmup_queries"],
        "budget_order": [cell["budget"] for cell in search["cells"][::4]],
        "phases": runner["phases"],
        "resources": resources,
        "searches": search["cells"],
    }, search["topology_identity_sha256"]


def external_repetition(root: Path, backend: str, corpus: str, repetition: int, expected: dict[str, Any], inventory: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    base = root / "verified-runs" / backend / corpus / f"repeat-{repetition}"
    search = checked_json(base / "search.json", inventory)
    service = checked_json(base / "service.json", inventory)
    runner = checked_json(base / "runner.json", inventory)
    client_cpu, client_rss = time_fields(base / "adapter.time", inventory)
    key = "ef" if backend == "milvus_standalone" else "ef_search"
    cells = []
    for budget in search["budget_searches"]:
        recall = budget["validation"]["recall"]
        for measured in budget["search_benchmarks"]:
            queries = measured["queries"]
            cells.append({
                "budget": {key: budget[key]},
                "concurrency": measured["concurrency"],
                "status": "valid",
                "metrics": {
                    "queries": queries,
                    "completed_queries": queries,
                    "result_count": queries * search["top_k"],
                    "errors": 0,
                    "timeouts": 0,
                    "recall_at_10": recall,
                    "qps": measured["ops_per_second"],
                    "p50_nanos": measured["p50_nanos"],
                    "p95_nanos": measured["p95_nanos"],
                    "p99_nanos": measured["p99_nanos"],
                },
            })
    insert = search["insert"]["duration_nanos"]
    build = search["build"]["duration_nanos"]
    reopen = search["reopen_load"]["duration_nanos"]
    measured = sum(value["total_duration_nanos"] for budget in search["budget_searches"] for value in budget["search_benchmarks"])
    resources = {
        "cpu_seconds": service["cpu_seconds"] + client_cpu,
        "peak_rss_bytes": service["peak_rss_bytes"] + client_rss,
        "persistent_bytes": service["persistent_bytes"],
        "temporary_bytes": 0,
        "network_rx_bytes": service["network_rx_bytes"],
        "network_tx_bytes": service["network_tx_bytes"],
        "swap_bytes": service["swap_bytes"],
    }
    load = runner["load_started"]
    return {
        "repetition": repetition,
        "status": "valid",
        "input_identity": external_input_identity(search, base / "adapter.command.json", expected, inventory),
        "noise": {"valid": True, "load_1": load[0], "load_5": load[1], "load_15": load[2]},
        "warmup_queries_per_cell": search["warmup_queries_per_cell"],
        "budget_order": [{key: budget[key]} for budget in search["budget_searches"]],
        "phases": {
            "load": insert,
            "index_build": build,
            "checkpoint_or_flush": 0,
            "reopen_or_reconnect": reopen,
            "readiness": runner["phases"]["readiness"],
            "warmup": max(0, runner["phases"]["benchmark"] - insert - build - reopen - measured),
            "search": measured,
            "cleanup": runner["phases"]["cleanup"],
        },
        "resources": resources,
        "searches": cells,
    }, service


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--sysstat", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    plan = _load(args.plan)
    inventory: list[dict[str, Any]] = []
    samples = historical_load(args.sysstat, args.out / "sysstat-load.json")
    corpora = [{key: value[key] for key in CORPUS_IDENTITY_KEYS} for value in plan["accepted_inputs"]["corpora"]]
    corpus_dirs = {"embedding_mixture_100k": "100k", "embedding_mixture_250k": "250k"}
    contracts = {row["id"]: row for row in plan["rows"]}
    rows = []
    binary_sha = _sha256(args.root / "bin/treedb_vector_partition_bench")
    for row_id, topology in TREE_ROWS.items():
        runs = []
        topology_ids = []
        for corpus in corpora:
            repetitions = []
            for repetition in (1, 2, 3):
                value, topology_id = tree_repetition(args.root, topology, corpus_dirs[corpus["id"]], repetition, corpus, samples, inventory)
                repetitions.append(value)
                topology_ids.append(topology_id)
            runs.append({"id": corpus["id"], "repetitions": repetitions})
        identity = {
            **contracts[row_id]["pinned_identity"],
            "source_revision": "ab674362838365e68297a49816872012d273039f",
            "binary_sha256": binary_sha,
            "topology_identity_sha256": semantic_sha({"row": row_id, "runs": topology_ids}),
        }
        if row_id == "treedb_container_multi_daemon":
            first = checked_json(args.root / "verified-runs" / topology / "100k/repeat-1/runner.json", inventory)
            identity["image_sha256"] = first["resources"]["image_sha256"].removeprefix("sha256:")
        rows.append({"id": row_id, "status": "valid", "boundary": contracts[row_id]["boundary"], "identity": identity, "corpora": runs})
    for row_id, backend in EXTERNAL_ROWS.items():
        runs = []
        service_identities = []
        for corpus in corpora:
            repetitions = []
            for repetition in (1, 2, 3):
                value, service = external_repetition(args.root, backend, corpus_dirs[corpus["id"]], repetition, corpus, inventory)
                repetitions.append(value)
                service_identities.append(service)
            runs.append({"id": corpus["id"], "repetitions": repetitions})
        identity = {**contracts[row_id]["pinned_identity"], "topology_identity_sha256": semantic_sha({"row": row_id, "services": service_identities})}
        rows.append({"id": row_id, "status": "valid", "boundary": contracts[row_id]["boundary"], "identity": identity, "corpora": runs})
    inventory.sort(key=lambda value: value["path"])
    write_json(args.out / "raw-evidence.json", {"artifact_root": str(args.root), "files": inventory})
    commands = {
        "source_head_sha": "ab674362838365e68297a49816872012d273039f",
        "runners": [{"path": str(path), "sha256": _sha256(path)} for path in (args.root / "run_m3_treedb.py", args.root / "run_m3_container.py", args.root / "run_m3_external.py")],
        "raw_evidence_sha256": _sha256(args.out / "raw-evidence.json"),
    }
    write_json(args.out / "commands.json", commands)
    stat = os.statvfs(args.root)
    environment = {
        "cpu_model": subprocess.run(["bash", "-lc", "lscpu | sed -n 's/^Model name:[[:space:]]*//p'"], check=True, capture_output=True, text=True).stdout.strip(),
        "logical_cpus": os.cpu_count(),
        "memory_bytes": int(Path("/proc/meminfo").read_text().split("MemTotal:", 1)[1].split()[0]) * 1024,
        "kernel": platform.release(),
        "storage_filesystem": subprocess.run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(args.root)], check=True, capture_output=True, text=True).stdout.strip(),
        "storage_root": str(args.root),
        "storage_free_bytes": stat.f_bavail * stat.f_frsize,
        "docker_version": subprocess.run(["docker", "--version"], check=True, capture_output=True, text=True).stdout.strip(),
        "go_version": "go1.26.0",
        "python_version": platform.python_version(),
    }
    write_json(args.out / "environment.json", environment)
    mtimes = [Path(value["path"]).stat().st_mtime for value in inventory]
    result = {
        "schema_version": 1,
        "result_kind": "vector_partition_local_system_qualification_v1",
        "status": "complete",
        "plan_sha256": _sha256(args.plan),
        "resource_envelope": plan["resource_envelope"],
        "source_head_sha": "ab674362838365e68297a49816872012d273039f",
        "host": environment,
        "provenance": {
            "artifact_root": str(args.root),
            "commands_path": "commands.json",
            "commands_sha256": _sha256(args.out / "commands.json"),
            "environment_path": "environment.json",
            "environment_sha256": _sha256(args.out / "environment.json"),
            "started_at": datetime.fromtimestamp(min(mtimes), timezone.utc).isoformat().replace("+00:00", "Z"),
            "completed_at": datetime.fromtimestamp(max(mtimes), timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "corpora": corpora,
        "rows": rows,
    }
    validate_result(plan, _sha256(args.plan), result, require_complete=True)
    write_json(args.out / "result.json", result)
    write_json(args.out / "matched-recall.json", {"buckets": matched_recall_buckets(plan, result)})


if __name__ == "__main__":
    main()
