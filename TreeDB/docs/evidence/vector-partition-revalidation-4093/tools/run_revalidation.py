#!/usr/bin/env python3
"""Run one serialized #4093 TreeDB topology repetition."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


GROUPS = ("group-a", "group-b", "group-c", "group-d")
TOPOLOGIES = ("single", "native", "container")
MODES = ("strict", "fast", "pinned")
CPU_SETS = ("0-2", "3-5", "6-8", "9-11")
MEMORY_BYTES = 6 << 30
SINGLE_MEMORY_BYTES = 24 << 30
CONTAINER_IPS = ("172.30.93.10", "172.30.93.11", "172.30.93.12", "172.30.93.13")
GROUP_PORT = 47300
PUBLIC_PORT = 47301
SEQUENCE = (
    ("single", 1), ("native", 1), ("container", 1),
    ("native", 2), ("container", 2), ("single", 2),
    ("container", 3), ("single", 3), ("native", 3),
)

SOURCE_ROOT = Path(__file__).resolve().parents[5]
BASE_RUNNER = SOURCE_ROOT / "TreeDB/docs/evidence/vector-partition-runtime-ownership-4091/tools/run_runtime_ownership.py"
SPEC = importlib.util.spec_from_file_location("runtime_ownership_runner", BASE_RUNNER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load the retained runtime-ownership runner")
base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(base)


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load(path: Path) -> dict[str, object]:
    return base.load_json(path)


def write(path: Path, value: object) -> None:
    base.write_json(path, value)


def expected_runtime(topology: str, index: int) -> tuple[str, int, int]:
    if topology == "single":
        return "0-11", 12, SINGLE_MEMORY_BYTES
    return CPU_SETS[index], 3, MEMORY_BYTES


def validate_ready(path: Path, provenance: dict[str, object], expected: tuple[str, int, int]) -> dict[str, object]:
    ready = base.validate_ready(path, provenance, None)
    cpu_set, gomaxprocs, memory = expected
    declared = ready.get("runtime_ownership")
    if (not isinstance(declared, dict) or declared.get("cpu_set") != cpu_set or
            declared.get("gomaxprocs") != gomaxprocs or declared.get("go_memory_limit_bytes") != memory or
            ready.get("effective_cpu_set") != cpu_set or ready.get("gomaxprocs") != gomaxprocs or
            ready.get("go_memory_limit") != memory):
        raise RuntimeError(f"{path} runtime ownership mismatch")
    return ready


def wait_ready(configs: list[Path], processes: list[subprocess.Popen[bytes]],
               provenance: dict[str, object], topology: str) -> list[dict[str, object]]:
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("node exited before readiness")
        ready: list[dict[str, object]] = []
        for index, config_path in enumerate(configs):
            path = Path(str(load(config_path)["ready_path"]))
            if not path.exists():
                break
            ready.append(validate_ready(path, provenance, expected_runtime(topology, index)))
        if len(ready) == len(configs):
            return ready
        time.sleep(.2)
    raise RuntimeError("node readiness timed out")


def configure(root: Path, run_dir: Path, provenance: dict[str, object], topology: str,
              repetition: int, index: int, endpoints: dict[str, str], public: str,
              rebinder: Path) -> Path:
    path = base.node_config(root, run_dir, provenance, topology, index, endpoints, public, rebinder)
    value = load(path)
    cpu_set, gomaxprocs, memory = expected_runtime(topology, index)
    value["capability_key_path"] = provenance["capability_key_path"]
    value["runtime_ownership"] = {
        "cpu_set": cpu_set, "gomaxprocs": gomaxprocs, "go_memory_limit_bytes": memory,
    }
    if repetition == 1:
        value["profile_directory"] = str(path.parent / "profiles")
    write(path, value)
    return path


def mode_order(topology: str, repetition: int) -> tuple[str, ...]:
    shift = (TOPOLOGIES.index(topology) + repetition - 1) % len(MODES)
    return MODES[shift:] + MODES[:shift]


def concurrency_order(topology: str, repetition: int, mode: str) -> str:
    return "1,32" if (TOPOLOGIES.index(topology) + MODES.index(mode) + repetition) % 2 else "32,1"


def bench_command(binary: Path, endpoint: str, topology_path: Path,
                  provenance: dict[str, object], out: Path, mode: str,
                  concurrency: str) -> list[str]:
    command = base.bench_command(binary, endpoint, topology_path, provenance, out)
    command[command.index("-concurrency") + 1] = concurrency
    command.extend(("-search-mode", mode, "-max-index-age", "1h", "-max-session-age", "2m"))
    return command


def validate_result(path: Path, mode: str) -> None:
    result = load(path)
    cells = result.get("cells")
    if (result.get("schema_version") != 1 or result.get("result_kind") != "vector_partition_system_bench_v1" or
            result.get("search_mode") != mode or not isinstance(cells, list) or len(cells) != 2):
        raise RuntimeError(f"{mode}: invalid result envelope")
    by_concurrency = {cell.get("concurrency"): cell for cell in cells if isinstance(cell, dict)}
    if set(by_concurrency) != {1, 32}:
        raise RuntimeError(f"{mode}: missing c1/c32 cells")
    for concurrency, cell in by_concurrency.items():
        metrics, counters = cell.get("metrics"), cell.get("counters")
        catalog = cell.get("catalog_reads", {}).get("total", {})
        strict = catalog.get("strict_search", {}).get("reads")
        refresh = catalog.get("serving_refresh", {}).get("reads")
        total = catalog.get("total", {})
        want_strict = 1000 if mode == "strict" else 0
        if (not isinstance(metrics, dict) or not isinstance(counters, dict) or cell.get("status") != "valid" or
                cell.get("search_mode") != mode or cell.get("budget") != {"probes": 2} or
                metrics.get("completed_queries") != 1000 or metrics.get("errors") != 0 or
                metrics.get("timeouts") != 0 or float(metrics.get("recall_at_10", 0)) < .90 or
                strict != want_strict or total.get("reads") != want_strict + refresh or
                total.get("log_barriers") != 0 or total.get("failures") != 0 or
                any(counters.get(key) != 0 for key in ("read_proofs", "generation_pins", "partition_opens")) or
                counters.get("snapshot_pins") != (0 if mode == "pinned" else 1000) or
                counters.get("session_pins") != (concurrency if mode == "pinned" else 0)):
            raise RuntimeError(f"{mode}/c{concurrency}: correctness or proof gate failed")
        if (mode == "strict") != (cell.get("fast_evidence") is None):
            raise RuntimeError(f"{mode}/c{concurrency}: freshness evidence mismatch")


def run_benches(binary: Path, endpoint: str, topology_path: Path, provenance: dict[str, object],
                run_dir: Path, topology: str, repetition: int, prefix: list[str]) -> None:
    for mode in mode_order(topology, repetition):
        concurrency = concurrency_order(topology, repetition, mode)
        out = run_dir / f"search-{mode}.json"
        command = bench_command(binary, endpoint, topology_path, provenance, out, mode, concurrency)
        timed = ["/usr/bin/time", "-v", "-o", str(run_dir / f"bench-{mode}.time"), *prefix, *command]
        write(run_dir / f"bench-{mode}.command.json", timed)
        with (run_dir / f"bench-{mode}.stdout").open("wb") as stdout, (run_dir / f"bench-{mode}.stderr").open("wb") as stderr:
            completed = subprocess.run(timed, stdout=stdout, stderr=stderr)
        (run_dir / f"bench-{mode}.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0:
            raise RuntimeError(f"{mode}: benchmark failed")
        validate_result(out, mode)


def remove_copied_databases(configs: list[Path]) -> None:
    for config in configs:
        database = Path(str(load(config)["database_directory"]))
        if database.name.startswith("database-") and database.parent == config.parent.parent:
            shutil.rmtree(database)
        else:
            raise RuntimeError(f"refusing to remove unexpected copied database {database}")


def run_native(root: Path, provenance: dict[str, object], topology: str, repetition: int,
               binary: Path, rebinder: Path, run_dir: Path) -> None:
    addresses = base.free_addresses(5)
    endpoints = dict(zip(GROUPS, addresses[:4]))
    count = 1 if topology == "single" else 4
    configs = [configure(root, run_dir, provenance, topology, repetition, index, endpoints, addresses[4], rebinder)
               for index in range(count)]
    topology_path = run_dir / "topology.json"
    base.run([str(binary), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)])
    processes: list[subprocess.Popen[bytes]] = []
    files: list[object] = []
    ready: list[dict[str, object]] = []
    try:
        for config in configs:
            state = config.parent
            stdout, stderr = (state / "node.stdout").open("wb"), (state / "node.stderr").open("wb")
            files.extend((stdout, stderr))
            command = ["/usr/bin/time", "-v", "-o", str(state / "node.time"), str(binary), "system-node", "-config", str(config)]
            processes.append(subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True,
                                               env={**os.environ, "TMPDIR": str(state)}))
        ready = wait_ready(configs, processes, provenance, topology)
        prefix = [] if topology == "single" else ["taskset", "--cpu-list", CPU_SETS[0], "env", "GOMAXPROCS=3"]
        run_benches(binary, addresses[4], topology_path, provenance, run_dir, topology, repetition, prefix)
    finally:
        primary = sys.exc_info()[0] is not None
        try:
            base.stop_native(ready, processes)
        except Exception:
            if not primary:
                raise
        finally:
            for file in files:
                file.close()  # type: ignore[union-attr]
    remove_copied_databases(configs)


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def run_container(root: Path, provenance: dict[str, object], repetition: int,
                  binary: Path, rebinder: Path, run_dir: Path) -> None:
    network = f"gomap4093-r{repetition}"
    names = [f"{network}-{group}" for group in GROUPS]
    endpoints = {group: f"{ip}:{GROUP_PORT}" for group, ip in zip(GROUPS, CONTAINER_IPS)}
    configs = [configure(root, run_dir, provenance, "container", repetition, index, endpoints,
                         f"0.0.0.0:{PUBLIC_PORT}", rebinder) for index in range(4)]
    topology_path = run_dir / "topology.json"
    base.run([str(binary), "system-check-topology", "-configs", ",".join(map(str, configs)), "-out", str(topology_path)])
    docker("network", "create", "--internal", "--subnet", "172.30.93.0/24", network)
    started: list[str] = []
    try:
        for index, (config, name, ip) in enumerate(zip(configs, names, CONTAINER_IPS)):
            value, state = load(config), config.parent
            database = Path(str(value["database_directory"]))
            command = [
                "run", "-d", "--name", name, "--network", network, "--ip", ip,
                "--user", f"{os.getuid()}:{os.getgid()}", "--cpuset-cpus", CPU_SETS[index],
                "--memory", "6g", "--memory-swap", "6g", "--pids-limit", "768",
                "-e", f"TMPDIR={state}", "-e", "GOMAXPROCS=3",
                "-v", f"{state}:{state}:rw", "-v", f"{database}:{database}:rw",
                "-v", f"{provenance['dataset_directory']}:{provenance['dataset_directory']}:ro",
                "-v", f"{provenance['capability_key_path']}:{provenance['capability_key_path']}:ro",
            ]
            if index == 0:
                command.extend(("-v", f"{run_dir}:{run_dir}:rw", "-v", f"{provenance['truth_directory']}:{provenance['truth_directory']}:ro"))
            command.extend((str(provenance["container_image"]), "system-node", "-config", str(config)))
            docker(*command)
            started.append(name)
        deadline = time.monotonic() + 300
        ready: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            ready = []
            for index, config in enumerate(configs):
                path = Path(str(load(config)["ready_path"]))
                if not path.exists():
                    break
                ready.append(validate_ready(path, provenance, expected_runtime("container", index)))
            if len(ready) == 4:
                break
            if any(docker("inspect", "-f", "{{.State.Running}}", name).stdout.strip() != "true" for name in names):
                raise RuntimeError("container exited before readiness")
            time.sleep(.2)
        else:
            raise RuntimeError("container readiness timed out")
        prefix = ["docker", "exec", "-e", "GOMAXPROCS=3", names[0]]
        run_benches(Path("/treedb_vector_partition_bench"), f"{CONTAINER_IPS[0]}:{PUBLIC_PORT}",
                    topology_path, provenance, run_dir, "container", repetition, prefix)
        write(run_dir / "container-resources.json", {
            "image_id": docker("image", "inspect", "-f", "{{.Id}}", str(provenance["container_image"])).stdout.strip(),
            "nodes": [json.loads(docker("inspect", name).stdout)[0]["HostConfig"] for name in names],
        })
    finally:
        primary = sys.exc_info()[0] is not None
        errors: list[str] = []
        for name in started:
            logs = docker("logs", name, check=False)
            state = run_dir / f"state-owned-{name.rsplit('-', 2)[-2]}-{name.rsplit('-', 1)[-1]}"
            if state.is_dir():
                (state / "container.stdout").write_text(logs.stdout, encoding="utf-8")
                (state / "container.stderr").write_text(logs.stderr, encoding="utf-8")
            stopped = docker("stop", "--time", "60", name, check=False)
            if stopped.returncode not in (0, 1):
                errors.append(stopped.stderr.strip())
            docker("rm", name, check=False)
        docker("network", "rm", network, check=False)
        if errors and not primary:
            raise RuntimeError("; ".join(errors))
    remove_copied_databases(configs)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("topology", choices=TOPOLOGIES)
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    provenance = load(args.root / "provenance.json")
    binary, rebinder = base.preflight(args.root, provenance)
    capability_key = Path(str(provenance.get("capability_key_path", "")))
    if (not capability_key.is_file() or
            base.sha256(capability_key) != provenance.get("capability_key_sha256")):
        raise RuntimeError("capability key changed")
    expected_sequence = SEQUENCE.index((args.topology, args.repetition)) + 1
    run_dir = args.root / "runs" / args.topology / f"repeat-{args.repetition}"
    run_dir.mkdir(parents=True)
    write(run_dir / "runner.json", {
        "schema_version": 1, "result_kind": "vector_partition_revalidation_run_v1",
        "sequence": expected_sequence, "topology": args.topology, "repetition": args.repetition,
        "mode_order": mode_order(args.topology, args.repetition),
        "concurrency_order": {mode: concurrency_order(args.topology, args.repetition, mode) for mode in MODES},
        "started_at": now(), "completed_at": None,
    })
    if args.topology == "container":
        run_container(args.root, provenance, args.repetition, binary, rebinder, run_dir)
    else:
        run_native(args.root, provenance, args.topology, args.repetition, binary, rebinder, run_dir)
    value = load(run_dir / "runner.json")
    value["completed_at"] = now()
    write(run_dir / "runner.json", value)
    print(f"PASS sequence={expected_sequence} topology={args.topology} repetition={args.repetition}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"#4093 STOP: {exc}", file=sys.stderr, flush=True)
        raise
