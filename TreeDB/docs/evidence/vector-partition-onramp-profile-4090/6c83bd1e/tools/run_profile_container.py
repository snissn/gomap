#!/usr/bin/env python3
"""Run one serialized #4090 TreeDB four-container profiling row."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import run_profile_treedb as common


IMAGE = "gomap-4090-vector-onramp:6c83bd1e"
IPS = ("172.30.19.10", "172.30.19.11", "172.30.19.12", "172.30.19.13")
CPUSETS = ("0-2", "3-5", "6-8", "9-11")
GROUP_PORT = 47100
PUBLIC_PORT = 47101


def docker(*args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=capture, text=True)


def wait_ready(configs: list[Path], names: list[str]) -> list[dict[str, object]]:
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        ready: list[dict[str, object]] = []
        for config_path, name in zip(configs, names):
            if docker("inspect", "-f", "{{.State.Running}}", name, capture=True).stdout.strip() != "true":
                raise RuntimeError(f"container {name} exited before readiness")
            config = json.loads(config_path.read_text(encoding="utf-8"))
            path = Path(config["ready_path"])
            if not path.exists():
                break
            value = json.loads(path.read_text(encoding="utf-8"))
            if value.get("source_revision") != common.SOURCE_HEAD or value.get("vcs_modified") is not False or value.get("executable_sha256") != common.BINARY_SHA or value.get("m8_loopback") is not False or value.get("production_topology") is not True:
                raise RuntimeError(f"container {name} readiness provenance is invalid")
            ready.append(value)
        if len(ready) == len(configs):
            return ready
        time.sleep(.2)
    raise RuntimeError("container readiness timed out")


def container_resources(names: list[str]) -> dict[str, object]:
    cpu_usec = peak = swap = 0
    ids: list[str] = []
    allocations: list[dict[str, object]] = []
    for name in names:
        inspect = json.loads(docker("inspect", name, capture=True).stdout)[0]
        ids.append(inspect["Id"])
        host = inspect["HostConfig"]
        allocations.append({"cpuset_cpus": host["CpusetCpus"], "memory_bytes": host["Memory"], "memory_swap_bytes": host["MemorySwap"], "pids_limit": host["PidsLimit"]})
        pid = int(inspect["State"]["Pid"])
        relative = Path((Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").strip().split("::", 1)[1]).lstrip("/"))
        group = Path("/sys/fs/cgroup") / relative
        cpu = dict(line.split() for line in (group / "cpu.stat").read_text(encoding="utf-8").splitlines())
        cpu_usec += int(cpu["usage_usec"])
        peak += int((group / "memory.peak").read_text(encoding="utf-8"))
        swap += int((group / "memory.swap.peak").read_text(encoding="utf-8"))
    return {"container_ids": ids, "allocations": allocations, "cpu_seconds": cpu_usec / 1_000_000, "peak_rss_bytes": peak, "swap_bytes": swap}


def run_one(repetition: int, profile_concurrency: int | None) -> None:
    started = time.monotonic_ns()
    common.assert_preflight()
    corpus_id = "250k"
    corpus = common.CORPORA[corpus_id]
    topology = "container_four_daemon_four_group"
    run = (common.ROOT / "profiles" / "container" / f"c{profile_concurrency}" if profile_concurrency else common.ROOT / "runs" / "container" / f"repeat-{repetition}")
    run.mkdir(parents=True)
    client = run / "client"
    client.mkdir()
    suffix = f"profile-c{profile_concurrency}" if profile_concurrency else f"r{repetition}"
    network = f"gomap4090-{corpus_id}-{suffix}"
    names = [f"{network}-{group}" for group in common.GROUPS]
    endpoints = {group: f"{ip}:{GROUP_PORT}" for group, ip in zip(common.GROUPS, IPS)}
    applied = {group: 1 for group in common.GROUPS}
    configs = [common.node_config(run, topology, corpus, index, endpoints, f"0.0.0.0:{PUBLIC_PORT}", applied, profile_concurrency is not None) for index in range(4)]
    loaded = time.monotonic_ns()
    topology_path = client / "topology.json"
    check = subprocess.run([str(common.BINARY), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)], capture_output=True, text=True)
    (client / "topology.stdout").write_text(check.stdout, encoding="utf-8")
    (client / "topology.stderr").write_text(check.stderr, encoding="utf-8")
    (client / "topology.rc").write_text(f"{check.returncode}\n", encoding="utf-8")
    if check.returncode != 0:
        raise RuntimeError(f"topology check failed: {check.stderr}")
    docker("network", "create", "--internal", "--subnet", "172.30.19.0/24", network)
    ready: list[dict[str, object]] = []
    resources: dict[str, object] = {}
    try:
        for index, (config, name, ip, cpuset) in enumerate(zip(configs, names, IPS, CPUSETS)):
            config_value = json.loads(config.read_text(encoding="utf-8"))
            state = config.parent
            database = Path(config_value["database_directory"])
            command = [
                "run", "-d", "--name", name, "--network", network, "--ip", ip,
                "--memory", "6g", "--memory-swap", "6g", "--pids-limit", "768", "--cpuset-cpus", cpuset,
                "-e", f"TMPDIR={state}", "-e", "GOMAXPROCS=3",
                "-v", f"{state}:{state}:rw", "-v", f"{database}:{database}:rw",
                "-v", f"{corpus['dataset']}:{corpus['dataset']}:ro",
            ]
            if index == 0:
                command.extend(["-v", f"{client}:{client}:rw", "-v", f"{corpus['truth']}:{corpus['truth']}:ro"])
            command.extend([IMAGE, "system-node", "-config", str(config)])
            docker(*command, capture=True)
        ready = wait_ready(configs, names)
        readied = time.monotonic_ns()
        concurrency = str(profile_concurrency) if profile_concurrency else "1,32"
        inner = [
            "/treedb_vector_partition_bench", "system-bench", "-endpoint", f"{IPS[0]}:{PUBLIC_PORT}",
            "-topology", str(topology_path), "-dataset", str(corpus["dataset"]),
            "-truth-cache", str(corpus["truth"]), "-truth-cache-sha256", str(corpus["truth_sha"]),
            "-probes", "2", "-concurrency", concurrency, "-top-k", "10", "-ef-search", "128",
            "-warmup", "1000", "-out", str(client / "search.json"),
        ]
        command = ["docker", "exec", "-e", "GOMAXPROCS=3", names[0], *inner]
        (client / "bench.command.json").write_text(json.dumps(command) + "\n", encoding="utf-8")
        with (client / "bench.stdout").open("wb") as stdout, (client / "bench.stderr").open("wb") as stderr:
            completed = subprocess.run(command, stdout=stdout, stderr=stderr)
        (client / "bench.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0 or not (client / "search.json").is_file():
            raise RuntimeError(f"container benchmark failed rc={completed.returncode}")
        benchmarked = time.monotonic_ns()
        resources = container_resources(names)
    finally:
        primary_failure = sys.exc_info()[0] is not None
        cleanup_errors: list[str] = []
        for name in names:
            result = docker("stop", "--time", "60", name, check=False, capture=True)
            if result.returncode not in (0, 1):
                cleanup_errors.append(f"stop {name}: {result.stderr.strip()}")
        for name in names:
            logs = docker("logs", name, check=False, capture=True)
            state = run / f"state-native-{name.rsplit('-', 2)[-2]}-{name.rsplit('-', 1)[-1]}"
            (state / "container.stdout").write_text(logs.stdout, encoding="utf-8")
            (state / "container.stderr").write_text(logs.stderr, encoding="utf-8")
            result = docker("rm", name, check=False, capture=True)
            if result.returncode not in (0, 1):
                cleanup_errors.append(f"rm {name}: {result.stderr.strip()}")
        result = docker("network", "rm", network, check=False, capture=True)
        if result.returncode not in (0, 1):
            cleanup_errors.append(f"network rm: {result.stderr.strip()}")
        if cleanup_errors and not primary_failure:
            raise RuntimeError("; ".join(cleanup_errors))
    cleaned = time.monotonic_ns()
    search = json.loads((client / "search.json").read_text(encoding="utf-8"))
    measured = sum(cell["elapsed_nanos"] for cell in search["cells"])
    persistent = sum(common.directory_bytes(Path(json.loads(path.read_text(encoding="utf-8"))["database_directory"])) for path in configs)
    resources.update({
        "image_sha256": docker("image", "inspect", "-f", "{{.Id}}", IMAGE, capture=True).stdout.strip(),
        "network": network, "profile_concurrency": profile_concurrency or 0, "persistent_bytes": persistent,
        "temporary_bytes": max(0, common.directory_bytes(run) - persistent),
        "network_rx_bytes": sum(cell["counters"]["candidate_bytes"] + cell["counters"]["response_bytes"] for cell in search["cells"]),
        "network_tx_bytes": sum(cell["counters"]["query_bytes"] + cell["counters"]["request_bytes"] for cell in search["cells"]),
    })
    common.write_json(run / "runner.json", {
        "phases": {"load": loaded - started, "index_build": 0, "checkpoint_or_flush": 0,
                   "reopen_or_reconnect": readied - loaded, "readiness": readied - loaded,
                   "warmup": max(0, benchmarked - readied - measured), "search": measured,
                   "cleanup": cleaned - benchmarked},
        "resources": resources, "started_monotonic_nanos": started, "completed_monotonic_nanos": cleaned,
    })
    print(f"complete control=container repetition={repetition} profile_concurrency={profile_concurrency or 0} result={client / 'search.json'}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    parser.add_argument("--profile-concurrency", type=int, choices=(1, 32))
    args = parser.parse_args()
    if args.profile_concurrency and args.repetition != 1:
        parser.error("profile captures use repetition 1")
    run_one(args.repetition, args.profile_concurrency)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"#4090 STOP: {exc}", file=sys.stderr, flush=True)
        raise
