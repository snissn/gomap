#!/usr/bin/env python3
"""Run one serialized #4091 250k ownership row."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


GROUPS = ("group-a", "group-b", "group-c", "group-d")
CPU_SETS = ("0-2", "3-5", "6-8", "9-11")
CONTAINER_IPS = ("172.30.91.10", "172.30.91.11", "172.30.91.12", "172.30.91.13")
GROUP_PORT = 47100
PUBLIC_PORT = 47101
MEMORY_BYTES = 6 << 30


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} is not a JSON object")
    return value


def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
    return subprocess.run(command, check=True, **kwargs)


def free_addresses(count: int) -> list[str]:
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


def no_heavy_process() -> None:
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if comm in ("go", "KaHIP") or comm.endswith(".test") or comm.startswith(("treedb_vector_", "treedb-vector-")):
            raise RuntimeError(f"unexpected live heavy process {entry.name} {comm}")


def preflight(root: Path, provenance: dict[str, object]) -> tuple[Path, Path]:
    source = root / "source"
    binary = root / "bin/treedb_vector_partition_bench"
    rebinder = root / "bin/rebind_snapshot"
    if sha256(binary) != provenance["binary_sha256"] or sha256(rebinder) != provenance["rebinder_sha256"]:
        raise RuntimeError("binary digest changed")
    head = run(["git", "-C", str(source), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = run(["git", "-C", str(source), "status", "--porcelain=v1"], capture_output=True, text=True).stdout
    if head != provenance["source_head"] or dirty:
        raise RuntimeError("source checkout changed")
    dataset = Path(str(provenance["dataset_directory"]))
    truth = Path(str(provenance["truth_directory"]))
    database = Path(str(provenance["m3_database_directory"]))
    descriptor = database / "vector_partition_variant_v1.json"
    truth_files = list(truth.glob("*.json"))
    if sha256(dataset / "fixture_manifest.json") != provenance["fixture_manifest_sha256"]:
        raise RuntimeError("fixture manifest changed")
    if len(truth_files) != 1 or sha256(truth_files[0]) != provenance["truth_sha256"]:
        raise RuntimeError("truth artifact changed")
    if sha256(descriptor) != provenance["m3_descriptor_sha256"]:
        raise RuntimeError("M3 descriptor changed")
    no_heavy_process()
    return binary, rebinder


def copy_database(source: Path, destination: Path, rebinder: Path) -> None:
    run(["cp", "-a", "--reflink=auto", str(source), str(destination)])
    run([str(rebinder), str(destination)])


def node_config(root: Path, run_dir: Path, provenance: dict[str, object], topology: str,
                index: int, endpoints: dict[str, str], public: str, rebinder: Path) -> Path:
    single = topology == "single"
    node_id = "single" if single else f"owned-{GROUPS[index]}"
    state = run_dir / f"state-{node_id}"
    database = run_dir / f"database-{node_id}"
    state.mkdir()
    copy_database(Path(str(provenance["m3_database_directory"])), database, rebinder)
    local = GROUPS if single else (GROUPS[index],)
    value: dict[str, object] = {
        "schema_version": 1,
        "result_kind": "vector_partition_system_node_config_v1",
        "assembly": "production_public_v1",
        "topology": "single_daemon_four_group" if single else f"{topology}_four_daemon_four_group",
        "node_id": node_id,
        "dataset_directory": provenance["dataset_directory"],
        "database_directory": str(database),
        "state_directory": str(state),
        "ready_path": str(state / "ready.json"),
        "local_groups": [{"group_id": group, "listen": endpoints[group]} for group in local],
        "endpoints": endpoints,
        "group_applied_indexes": {group: 1 for group in GROUPS},
    }
    if not single:
        value["runtime_ownership"] = {
            "cpu_set": CPU_SETS[index], "gomaxprocs": 3, "go_memory_limit_bytes": MEMORY_BYTES,
        }
    if index == 0:
        value["public_listen"] = public
    path = state / "node.json"
    write_json(path, value)
    return path


def validate_ready(path: Path, provenance: dict[str, object], ownership: str | None) -> dict[str, object]:
    ready = load_json(path)
    if ready.get("source_revision") != provenance["source_head"] or ready.get("vcs_modified") is not False:
        raise RuntimeError(f"{path} source provenance mismatch")
    if ready.get("executable_sha256") != provenance["binary_sha256"] or ready.get("production_topology") is not True or ready.get("m8_loopback") is not False:
        raise RuntimeError(f"{path} executable or route provenance mismatch")
    if ownership is not None:
        declared = ready.get("runtime_ownership")
        if not isinstance(declared, dict) or declared.get("cpu_set") != ownership or declared.get("gomaxprocs") != 3 or declared.get("go_memory_limit_bytes") != MEMORY_BYTES:
            raise RuntimeError(f"{path} declared ownership mismatch")
        if ready.get("effective_cpu_set") != ownership or ready.get("gomaxprocs") != 3 or ready.get("go_memory_limit") != MEMORY_BYTES:
            raise RuntimeError(f"{path} effective ownership mismatch")
    return ready


def wait_native(configs: list[Path], processes: list[subprocess.Popen], provenance: dict[str, object], single: bool) -> list[dict[str, object]]:
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("node exited before readiness")
        ready: list[dict[str, object]] = []
        for index, config_path in enumerate(configs):
            path = Path(str(load_json(config_path)["ready_path"]))
            if not path.exists():
                break
            ready.append(validate_ready(path, provenance, None if single else CPU_SETS[index]))
        if len(ready) == len(configs):
            return ready
        time.sleep(.2)
    raise RuntimeError("node readiness timed out")


def stop_native(ready: list[dict[str, object]], processes: list[subprocess.Popen]) -> None:
    for value in ready:
        try:
            os.kill(int(value["pid"]), signal.SIGTERM)
        except ProcessLookupError:
            pass
    for process in processes:
        if process.poll() is None and not ready:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        if process.wait(timeout=60) != 0:
            raise RuntimeError("node wrapper exited nonzero")


def validate_result(path: Path) -> None:
    result = load_json(path)
    cells = result.get("cells")
    if not isinstance(cells, list) or len(cells) != 2:
        raise RuntimeError("benchmark did not retain exactly c1/c32")
    for cell, concurrency in zip(cells, (1, 32)):
        metrics = cell.get("metrics")
        if not isinstance(metrics, dict) or cell.get("status") != "valid" or cell.get("concurrency") != concurrency:
            raise RuntimeError("benchmark cell shape is invalid")
        if cell.get("budget") != {"probes": 2} or metrics.get("completed_queries") != 1000 or metrics.get("errors") != 0 or metrics.get("timeouts") != 0 or float(metrics.get("recall_at_10", 0)) < .90:
            raise RuntimeError("benchmark cell failed correctness gate")


def bench_command(binary: Path, public: str, topology: Path, provenance: dict[str, object], out: Path) -> list[str]:
    return [
        str(binary), "system-bench", "-endpoint", public, "-topology", str(topology),
        "-dataset", str(provenance["dataset_directory"]), "-truth-cache", str(provenance["truth_directory"]),
        "-truth-cache-sha256", str(provenance["truth_sha256"]), "-probes", "2", "-concurrency", "1,32",
        "-top-k", "10", "-ef-search", "128", "-warmup", "1000", "-out", str(out),
    ]


def run_native(root: Path, provenance: dict[str, object], topology: str, repetition: int, binary: Path, rebinder: Path) -> None:
    single = topology == "single"
    run_dir = root / "runs" / topology / f"repeat-{repetition}"
    run_dir.mkdir(parents=True)
    addresses = free_addresses(5)
    endpoints = dict(zip(GROUPS, addresses[:4]))
    configs = [node_config(root, run_dir, provenance, topology, i, endpoints, addresses[4], rebinder) for i in range(1 if single else 4)]
    topology_path = run_dir / "topology.json"
    run([str(binary), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)])
    processes: list[subprocess.Popen] = []
    files = []
    ready: list[dict[str, object]] = []
    try:
        for config in configs:
            state = config.parent
            stdout = (state / "node.stdout").open("wb")
            stderr = (state / "node.stderr").open("wb")
            files.extend((stdout, stderr))
            command = ["/usr/bin/time", "-v", "-o", str(state / "node.time"), str(binary), "system-node", "-config", str(config)]
            processes.append(subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True, env={**os.environ, "TMPDIR": str(state)}))
        ready = wait_native(configs, processes, provenance, single)
        command = bench_command(binary, addresses[4], topology_path, provenance, run_dir / "search.json")
        if not single:
            command = ["taskset", "--cpu-list", CPU_SETS[0], "env", "GOMAXPROCS=3", *command]
        timed = ["/usr/bin/time", "-v", "-o", str(run_dir / "bench.time"), *command]
        write_json(run_dir / "bench.command.json", timed)
        with (run_dir / "bench.stdout").open("wb") as stdout, (run_dir / "bench.stderr").open("wb") as stderr:
            completed = subprocess.run(timed, stdout=stdout, stderr=stderr)
        (run_dir / "bench.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0:
            raise RuntimeError("system benchmark failed")
        validate_result(run_dir / "search.json")
    finally:
        primary = sys.exc_info()[0] is not None
        try:
            stop_native(ready, processes)
        except Exception:
            if not primary:
                raise
        finally:
            for file in files:
                file.close()


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def run_container(root: Path, provenance: dict[str, object], repetition: int, binary: Path, rebinder: Path) -> None:
    topology = "container"
    run_dir = root / "runs" / topology / f"repeat-{repetition}"
    run_dir.mkdir(parents=True)
    network = f"gomap4091-r{repetition}"
    names = [f"{network}-{group}" for group in GROUPS]
    endpoints = {group: f"{ip}:{GROUP_PORT}" for group, ip in zip(GROUPS, CONTAINER_IPS)}
    configs = [node_config(root, run_dir, provenance, topology, i, endpoints, f"0.0.0.0:{PUBLIC_PORT}", rebinder) for i in range(4)]
    topology_path = run_dir / "topology.json"
    run([str(binary), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)])
    docker("network", "create", "--internal", "--subnet", "172.30.91.0/24", network)
    started: list[str] = []
    try:
        for index, (config, name, ip) in enumerate(zip(configs, names, CONTAINER_IPS)):
            value = load_json(config)
            state = config.parent
            database = Path(str(value["database_directory"]))
            command = [
                "run", "-d", "--name", name, "--network", network, "--ip", ip,
                "--cpuset-cpus", CPU_SETS[index], "--memory", "6g", "--memory-swap", "6g", "--pids-limit", "768",
                "-e", f"TMPDIR={state}", "-v", f"{state}:{state}:rw", "-v", f"{database}:{database}:rw",
                "-v", f"{provenance['dataset_directory']}:{provenance['dataset_directory']}:ro",
            ]
            if index == 0:
                command.extend(["-v", f"{run_dir}:{run_dir}:rw", "-v", f"{provenance['truth_directory']}:{provenance['truth_directory']}:ro"])
            command.extend([str(provenance["container_image"]), "system-node", "-config", str(config)])
            docker(*command)
            started.append(name)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            ready = []
            for index, config in enumerate(configs):
                path = Path(str(load_json(config)["ready_path"]))
                if not path.exists():
                    break
                ready.append(validate_ready(path, provenance, CPU_SETS[index]))
            if len(ready) == 4:
                break
            if any(docker("inspect", "-f", "{{.State.Running}}", name).stdout.strip() != "true" for name in names):
                raise RuntimeError("container exited before readiness")
            time.sleep(.2)
        else:
            raise RuntimeError("container readiness timed out")
        inner = bench_command(Path("/treedb_vector_partition_bench"), f"{CONTAINER_IPS[0]}:{PUBLIC_PORT}", topology_path, provenance, run_dir / "search.json")
        command = ["docker", "exec", names[0], *map(str, inner)]
        timed = ["/usr/bin/time", "-v", "-o", str(run_dir / "bench.time"), *command]
        write_json(run_dir / "bench.command.json", timed)
        with (run_dir / "bench.stdout").open("wb") as stdout, (run_dir / "bench.stderr").open("wb") as stderr:
            completed = subprocess.run(timed, stdout=stdout, stderr=stderr)
        (run_dir / "bench.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0:
            raise RuntimeError("container benchmark failed")
        validate_result(run_dir / "search.json")
    finally:
        for name in started:
            logs = docker("logs", name, check=False)
            state = run_dir / f"state-owned-{name.rsplit('-', 2)[-2]}-{name.rsplit('-', 1)[-1]}"
            if state.is_dir():
                (state / "container.stdout").write_text(logs.stdout, encoding="utf-8")
                (state / "container.stderr").write_text(logs.stderr, encoding="utf-8")
            docker("rm", "-f", name, check=False)
        docker("network", "rm", network, check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("topology", choices=("single", "native", "container"))
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    provenance = load_json(args.root / "provenance.json")
    binary, rebinder = preflight(args.root, provenance)
    if args.topology == "container":
        run_container(args.root, provenance, args.repetition, binary, rebinder)
    else:
        run_native(args.root, provenance, args.topology, args.repetition, binary, rebinder)
    print(f"PASS topology={args.topology} repetition={args.repetition}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"#4091 STOP: {exc}", file=sys.stderr, flush=True)
        raise
