#!/usr/bin/env python3
"""Run one serialized #4092 TreeDB serving-snapshot evidence row."""

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


ROOT = Path("/mnt/fast4tb/gomap-4092-serving-snapshot-ce5f0088")
BINARY = ROOT / "bin/treedb_vector_partition_bench"
REBINDER = ROOT / "bin/rebind_snapshot"
SOURCE_HEAD = "ce5f008855ef0a285cf8e69d5cba7d9b6b4bf56a"
BINARY_SHA = "f2fdd0ab69e2362df14c470a0d3a86485dec12fd29a1b01bff26477cd6e07cf0"
REBINDER_SHA = "dc6700ac2a0fb692f6239052988d679d2d5a1a96a6bf1411907684d56f2104e2"
CAMPAIGN = Path("/mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0")
CORPORA = {
    "250k": {
        "dataset": CAMPAIGN / "250k/dataset", "database": ROOT / "250k/graph-overlap-020-db",
        "truth": CAMPAIGN / "250k/truth-cache", "truth_sha": "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e",
        "fixture_sha": "14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025",
        "descriptor_sha": "c43fdc9c6901203daa5747124f74c845c49f4fb3f0ca731e5c919629cfb1e1f8",
    },
}
GROUPS = ("group-a", "group-b", "group-c", "group-d")
CPUSETS = ("0-2", "3-5", "6-8", "9-11")
CONTROLS = ("single",)


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
    for corpus in CORPORA.values():
        if digest(corpus["dataset"] / "fixture_manifest.json") != corpus["fixture_sha"]:  # type: ignore[operator]
            raise RuntimeError("fixture manifest digest changed")
        if digest(corpus["database"] / "vector_partition_variant_v1.json") != corpus["descriptor_sha"]:  # type: ignore[operator]
            raise RuntimeError("M3 descriptor digest changed")
        truth = list(corpus["truth"].glob("*.json"))  # type: ignore[union-attr]
        if len(truth) != 1 or digest(truth[0]) != corpus["truth_sha"]:
            raise RuntimeError("truth artifact digest changed")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if comm in ("go", "KaHIP") or comm.endswith(".test") or comm == BINARY.name[:15]:
            raise RuntimeError(f"unexpected live heavy process {entry.name} {comm}")


def node_config(run: Path, topology: str, corpus: dict[str, object], index: int, endpoints: dict[str, str], public: str, applied: dict[str, int], profile: bool) -> Path:
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
    if profile:
        profile_directory = state / "profiles"
        profile_directory.mkdir()
        config["profile_directory"] = str(profile_directory)
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


def run_one(control: str, repetition: int, profile_concurrency: int | None) -> None:
    started = time.monotonic_ns()
    assert_preflight()
    corpus_id = "250k"
    corpus = CORPORA[corpus_id]
    topology = "single_daemon_four_group" if control == "single" else "native_four_daemon_four_group"
    run = (ROOT / "profiles" / control / f"c{profile_concurrency}" if profile_concurrency else ROOT / "runs" / control / f"repeat-{repetition}")
    run.mkdir(parents=True)
    listeners = addresses(5)
    endpoints = dict(zip(GROUPS, listeners[:4]))
    public = listeners[4]
    applied = {group: 1 for group in GROUPS}
    node_count = 1 if topology.startswith("single") else 4
    configs = [node_config(run, topology, corpus, index, endpoints, public, applied, profile_concurrency is not None) for index in range(node_count)]
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
        for index, config in enumerate(configs):
            state = config.parent
            stdout = (state / "node.stdout").open("wb")
            stderr = (state / "node.stderr").open("wb")
            files.extend((stdout, stderr))
            command = ["/usr/bin/time", "-v", "-o", str(state / "node.time")]
            if control == "native-budgeted":
                command.extend(["taskset", "--cpu-list", CPUSETS[index], "env", "GOMAXPROCS=3"])
            command.extend([str(BINARY), "system-node", "-config", str(config)])
            environment = os.environ.copy()
            environment["TMPDIR"] = str(state)
            processes.append(subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment, start_new_session=True))
        ready = wait_ready(configs, processes)
        readied = time.monotonic_ns()
        concurrency = str(profile_concurrency) if profile_concurrency else "1,32"
        command = [
            "/usr/bin/time", "-v", "-o", str(run / "bench.time"),
        ]
        if control == "native-budgeted":
            command.extend(["taskset", "--cpu-list", CPUSETS[0], "env", "GOMAXPROCS=3"])
        command.extend([
            str(BINARY), "system-bench",
            "-endpoint", public, "-topology", str(topology_path), "-dataset", str(corpus["dataset"]),
            "-truth-cache", str(corpus["truth"]), "-truth-cache-sha256", str(corpus["truth_sha"]),
            "-probes", "2", "-concurrency", concurrency, "-top-k", "10", "-ef-search", "128",
            "-warmup", "1000", "-out", str(run / "search.json"),
        ])
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
        "control": control,
        "server_cpu_sets": list(CPUSETS[:node_count]) if control == "native-budgeted" else [],
        "client_cpu_set": CPUSETS[0] if control == "native-budgeted" else "",
        "profile_concurrency": profile_concurrency or 0,
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
    print(f"complete control={control} repetition={repetition} profile_concurrency={profile_concurrency or 0} result={run / 'search.json'}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("control", choices=CONTROLS)
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    parser.add_argument("--profile-concurrency", type=int, choices=(1, 32))
    args = parser.parse_args()
    if args.profile_concurrency and args.repetition != 1:
        parser.error("profile captures use repetition 1")
    assert_preflight()
    run_one(args.control, args.repetition, args.profile_concurrency)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"#4092 STOP: {exc}", file=sys.stderr, flush=True)
        raise
