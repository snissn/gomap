#!/usr/bin/env python3
"""Run one serialized #4019 M3 TreeDB system row."""

from __future__ import annotations

import hashlib
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time


ROOT = Path("/mnt/fast4tb/gomap-4019-m3-system-matrix-ab674362")
BINARY = ROOT / "bin/treedb_vector_partition_bench"
REBINDER = ROOT / "bin/rebind_snapshot"
SOURCE_HEAD = "ab674362838365e68297a49816872012d273039f"
BINARY_SHA = "fe5f4e7c1d5371388699d1dde8a0f22f557e029a73f2a6804a6ae213da72e729"
REBINDER_SHA = "3d0d6bde7cd2fc7d120ea6e64c5d2873ff79499f7ca58ebe155226717894c681"
CAMPAIGN = Path("/mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0")
CORPORA = {
    "100k": {"dataset": CAMPAIGN / "100k/dataset", "database": ROOT / "100k/graph-overlap-020-db", "truth": CAMPAIGN / "100k/truth-cache", "truth_sha": "0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c"},
    "250k": {"dataset": CAMPAIGN / "250k/dataset", "database": ROOT / "250k/graph-overlap-020-db", "truth": CAMPAIGN / "250k/truth-cache", "truth_sha": "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e"},
}
GROUPS = ("group-a", "group-b", "group-c", "group-d")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def addresses(count: int) -> list[str]:
    listeners: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
        return [f"127.0.0.1:{listener.getsockname()[1]}" for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def directory_bytes(path: Path) -> int:
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def cgroup_resources() -> dict[str, object]:
    relative = Path(Path("/proc/self/cgroup").read_text(encoding="utf-8").strip().split("::", 1)[1].lstrip("/"))
    group = Path("/sys/fs/cgroup") / relative
    if group.suffix != ".scope":
        raise RuntimeError("M3 runner is not inside its required cgroup scope")
    cpu = dict(line.split() for line in (group / "cpu.stat").read_text(encoding="utf-8").splitlines())
    return {
        "cgroup": str(group),
        "cpu_seconds": int(cpu["usage_usec"]) / 1_000_000,
        "peak_rss_bytes": int((group / "memory.peak").read_text(encoding="utf-8")),
        "swap_bytes": int((group / "memory.swap.peak").read_text(encoding="utf-8")),
    }


def copy_database(source: Path, destination: Path) -> None:
    subprocess.run(["cp", "-a", "--reflink=auto", str(source), str(destination)], check=True)
    subprocess.run([str(REBINDER), str(destination)], check=True)


def assert_preflight() -> None:
    if digest(BINARY) != BINARY_SHA:
        raise RuntimeError("benchmark binary digest changed")
    if digest(REBINDER) != REBINDER_SHA:
        raise RuntimeError("snapshot rebinder digest changed")
    status = subprocess.run(["git", "-C", str(ROOT / "source"), "status", "--porcelain=v1"], check=True, capture_output=True, text=True).stdout
    head = subprocess.run(["git", "-C", str(ROOT / "source"), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    if status or head != SOURCE_HEAD:
        raise RuntimeError("source checkout changed")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if comm in ("go", "KaHIP") or comm.endswith(".test") or comm == BINARY.name[:15]:
            raise RuntimeError(f"unexpected live heavy process {entry.name} {comm}")


def node_config(run: Path, topology: str, corpus: dict[str, object], index: int, endpoints: dict[str, str], public: str, applied: dict[str, int]) -> Path:
    node = "single" if topology.startswith("single") else f"native-{GROUPS[index]}"
    state = run / f"state-{node}"
    database = run / f"database-{node}"
    state.mkdir()
    copy_database(corpus["database"], database)  # type: ignore[arg-type]
    local_groups = GROUPS if topology.startswith("single") else (GROUPS[index],)
    config = {
        "schema_version": 1,
        "result_kind": "vector_partition_system_node_config_v1",
        "assembly": "production_public_v1",
        "topology": topology,
        "node_id": node,
        "dataset_directory": str(corpus["dataset"]),
        "database_directory": str(database),
        "state_directory": str(state),
        "ready_path": str(state / "ready.json"),
        "local_groups": [{"group_id": group, "listen": endpoints[group]} for group in local_groups],
        "endpoints": endpoints,
        "group_applied_indexes": applied,
    }
    if index == 0:
        config["public_listen"] = public
    path = state / "node.json"
    write_json(path, config)
    return path


def wait_ready(configs: list[Path], processes: list[subprocess.Popen[bytes]]) -> list[dict[str, object]]:
    deadline = time.monotonic() + 300
    ready: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("system node exited before readiness")
        ready = []
        for config_path in configs:
            config = json.loads(config_path.read_text(encoding="utf-8"))
            path = Path(config["ready_path"])
            if not path.exists():
                break
            value = json.loads(path.read_text(encoding="utf-8"))
            if value.get("source_revision") != SOURCE_HEAD or value.get("vcs_modified") is not False or value.get("executable_sha256") != BINARY_SHA or value.get("m8_loopback") is not False or value.get("production_topology") is not True:
                raise RuntimeError("system node readiness provenance is invalid")
            ready.append(value)
        if len(ready) == len(configs):
            return ready
        time.sleep(.2)
    raise RuntimeError("system node readiness timed out")


def stop_nodes(ready: list[dict[str, object]], processes: list[subprocess.Popen[bytes]]) -> None:
    if ready:
        for value in ready:
            pid = value.get("pid")
            if not isinstance(pid, int):
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    else:
        for process in processes:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
    for process in processes:
        try:
            rc = process.wait(timeout=60)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("system node did not stop") from exc
        if rc != 0:
            raise RuntimeError(f"system node wrapper exited {rc}")


def run_one(topology: str, corpus_id: str, repetition: int) -> None:
    started = time.monotonic_ns()
    assert_preflight()
    corpus = CORPORA[corpus_id]
    run = ROOT / "verified-runs" / topology / corpus_id / f"repeat-{repetition}"
    run.mkdir(parents=True)
    listeners = addresses(5)
    endpoints = dict(zip(GROUPS, listeners[:4]))
    public = listeners[4]
    applied = {group: 1 for group in GROUPS}
    node_count = 1 if topology.startswith("single") else 4
    configs = [node_config(run, topology, corpus, index, endpoints, public, applied) for index in range(node_count)]
    loaded = time.monotonic_ns()
    topology_path = run / "topology.json"
    check = subprocess.run([str(BINARY), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)], capture_output=True, text=True)
    (run / "topology.stdout").write_text(check.stdout, encoding="utf-8")
    (run / "topology.stderr").write_text(check.stderr, encoding="utf-8")
    (run / "topology.rc").write_text(f"{check.returncode}\n", encoding="utf-8")
    if check.returncode != 0:
        raise RuntimeError(f"topology check failed: {check.stderr}")
    processes: list[subprocess.Popen[bytes]] = []
    files: list[object] = []
    ready: list[dict[str, object]] = []
    try:
        for config in configs:
            state = config.parent
            stdout = (state / "node.stdout").open("wb")
            stderr = (state / "node.stderr").open("wb")
            files.extend((stdout, stderr))
            command = ["/usr/bin/time", "-v", "-o", str(state / "node.time"), str(BINARY), "system-node", "-config", str(config)]
            environment = os.environ.copy()
            environment["TMPDIR"] = str(state)
            processes.append(subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment, start_new_session=True))
        ready = wait_ready(configs, processes)
        readied = time.monotonic_ns()
        probes = ("1,2,4,8,16", "16,8,4,2,1", "2,4,8,16,1")[repetition - 1]
        command = [
            "/usr/bin/time", "-v", "-o", str(run / "bench.time"), str(BINARY), "system-bench",
            "-endpoint", public, "-topology", str(topology_path), "-dataset", str(corpus["dataset"]),
            "-truth-cache", str(corpus["truth"]), "-truth-cache-sha256", str(corpus["truth_sha"]),
            "-probes", probes, "-concurrency", "1,8,32,64", "-top-k", "10", "-ef-search", "128",
            "-warmup", "1000", "-out", str(run / "search.json"),
        ]
        (run / "bench.command.json").write_text(json.dumps(command) + "\n", encoding="utf-8")
        with (run / "bench.stdout").open("wb") as stdout, (run / "bench.stderr").open("wb") as stderr:
            completed = subprocess.run(command, stdout=stdout, stderr=stderr)
        (run / "bench.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0 or not (run / "search.json").is_file():
            raise RuntimeError(f"system benchmark failed rc={completed.returncode}")
        benchmarked = time.monotonic_ns()
    finally:
        primary_failure = sys.exc_info()[0] is not None
        try:
            stop_nodes(ready, processes)
        except Exception as exc:
            if not primary_failure:
                raise
            print(f"M2 cleanup after primary failure: {exc}", file=sys.stderr, flush=True)
        finally:
            for file in files:
                file.close()  # type: ignore[union-attr]
    cleaned = time.monotonic_ns()
    search = json.loads((run / "search.json").read_text(encoding="utf-8"))
    measured = sum(cell["elapsed_nanos"] for cell in search["cells"])
    resources = cgroup_resources()
    persistent = sum(directory_bytes(Path(json.loads(path.read_text(encoding="utf-8"))["database_directory"])) for path in configs)
    resources.update({
        "persistent_bytes": persistent,
        "temporary_bytes": max(0, directory_bytes(run) - persistent),
        "network_rx_bytes": sum(cell["counters"]["candidate_bytes"] + cell["counters"]["response_bytes"] for cell in search["cells"]),
        "network_tx_bytes": sum(cell["counters"]["query_bytes"] + cell["counters"]["request_bytes"] for cell in search["cells"]),
    })
    write_json(run / "runner.json", {
        "phases": {
            "load": loaded - started, "index_build": 0, "checkpoint_or_flush": 0,
            "reopen_or_reconnect": readied - loaded, "readiness": readied - loaded,
            "warmup": max(0, benchmarked - readied - measured), "search": measured,
            "cleanup": cleaned - benchmarked,
        },
        "resources": resources,
        "started_monotonic_nanos": started,
        "completed_monotonic_nanos": cleaned,
    })
    print(f"complete topology={topology} corpus={corpus_id} repetition={repetition} result={run / 'search.json'}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("topology", choices=("single_daemon_four_group", "native_four_daemon_four_group"))
    parser.add_argument("corpus", choices=tuple(CORPORA))
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    assert_preflight()
    run_one(args.topology, args.corpus, args.repetition)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"M3 STOP: {exc}", file=sys.stderr, flush=True)
        raise
