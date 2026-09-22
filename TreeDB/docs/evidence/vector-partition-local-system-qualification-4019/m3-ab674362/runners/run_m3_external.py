#!/usr/bin/env python3
"""Run one serialized pinned Milvus or pgvector #4019 M3 row."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time


ROOT = Path("/mnt/fast4tb/gomap-4019-m3-system-matrix-ab674362")
SOURCE = ROOT / "source"
VENV = ROOT / "venv/bin/python"
HEAD = "ab674362838365e68297a49816872012d273039f"
PG_IMAGE = "pgvector/pgvector:pg16@sha256:84a355869251af1a3379cfc9fa7b4dbf962c03f642a4bb7b339a203925071c43"
MILVUS_IMAGE = "milvusdb/milvus:v2.6.20@sha256:e514fced2aa26cf3b94e7de20986fe9e535159fde08f9934d245d0e1a909c18c"
ETCD_IMAGE = "quay.io/coreos/etcd:v3.5.25@sha256:dc2bdc588d2adc5272204a1fff7f1d89f31e8caacea78fdf509fd409d7162a9d"
MINIO_IMAGE = "minio/minio:RELEASE.2024-12-18T13-15-44Z@sha256:34c8e2f52a5984492555427fee07254c80036bdb7079bb91679232abd7a4fa20"
COMPOSE = ROOT / "external-pins/milvus-standalone-docker-compose.yml"
COMPOSE_SHA = "9e0e8187e197ce23d3da3e63c19bc20189782f96bacb97287f8fcee80ba628c3"
BUDGETS = ("16,32,64,128,256,512", "512,256,128,64,32,16", "32,64,128,256,512,16")


def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, **kwargs)  # type: ignore[arg-type]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def directory_bytes(path: Path) -> int:
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def assert_preflight() -> None:
    if digest(COMPOSE) != COMPOSE_SHA:
        raise RuntimeError("Milvus compose digest changed")
    if run(["git", "-C", str(SOURCE), "rev-parse", "HEAD"], capture_output=True).stdout.strip() != HEAD:
        raise RuntimeError("source revision changed")
    if run(["git", "-C", str(SOURCE), "status", "--porcelain=v1"], capture_output=True).stdout:
        raise RuntimeError("source checkout changed")
    names = run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True).stdout.splitlines()
    if any(name.startswith("gomap4019-m3-") for name in names):
        raise RuntimeError("another #4019 M3 service is live")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if comm in ("go", "KaHIP", "treedb_vector_p") or comm.endswith(".test"):
            raise RuntimeError(f"unexpected live heavy process {entry.name} {comm}")


def wait_ready(backend: str, endpoint: str) -> None:
    code = (
        "import sys,time\n"
        "last=None\n"
        "for _ in range(360):\n"
        " try:\n"
        + ("  import psycopg\n  c=psycopg.connect(sys.argv[1],autocommit=True); c.execute('select 1'); c.close()\n"
           if backend == "pgvector" else
           "  from pymilvus import MilvusClient\n  c=MilvusClient(uri=sys.argv[1],token='root:Milvus'); c.list_collections(); c.close()\n")
        + "  raise SystemExit(0)\n"
        " except Exception as exc:\n"
        "  last=exc; time.sleep(.5)\n"
        "raise SystemExit(f'service did not become ready: {last}')\n"
    )
    run([str(VENV), "-c", code, endpoint])


def container_resources(ids: list[str]) -> dict[str, object]:
    resources: list[dict[str, object]] = []
    peak = swap = network_rx = network_tx = 0
    cpu_seconds = 0.0
    for container in ids:
        value = json.loads(run(["docker", "inspect", container], capture_output=True).stdout)[0]
        pid = int(value["State"]["Pid"])
        relative = Path(Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").strip().split("::", 1)[1].lstrip("/"))
        group = Path("/sys/fs/cgroup") / relative
        memory_peak = int((group / "memory.peak").read_text(encoding="utf-8"))
        swap_peak = int((group / "memory.swap.peak").read_text(encoding="utf-8"))
        cpu = dict(line.split() for line in (group / "cpu.stat").read_text(encoding="utf-8").splitlines())
        cpu_value = int(cpu["usage_usec"]) / 1_000_000
        interfaces = (Path(f"/proc/{pid}/net/dev").read_text(encoding="utf-8").splitlines())[2:]
        rx_value = tx_value = 0
        for line in interfaces:
            _, raw = line.split(":", 1)
            fields = raw.split()
            rx_value += int(fields[0])
            tx_value += int(fields[8])
        peak += memory_peak
        swap += swap_peak
        cpu_seconds += cpu_value
        network_rx += rx_value
        network_tx += tx_value
        resources.append({
            "id": value["Id"], "image": value["Image"], "name": value["Name"].lstrip("/"),
            "memory_limit_bytes": int(value["HostConfig"]["Memory"]),
            "memory_swap_limit_bytes": int(value["HostConfig"]["MemorySwap"]),
            "cpuset_cpus": value["HostConfig"]["CpusetCpus"], "pids_limit": value["HostConfig"]["PidsLimit"],
            "peak_rss_bytes": memory_peak, "swap_peak_bytes": swap_peak,
            "cpu_seconds": cpu_value,
            "network_rx_bytes": rx_value, "network_tx_bytes": tx_value,
        })
    return {"containers": resources, "peak_rss_bytes": peak, "swap_bytes": swap, "cpu_seconds": cpu_seconds,
            "network_rx_bytes": network_rx, "network_tx_bytes": network_tx}


def persistent_bytes(backend: str, ids: list[str]) -> int:
    paths = {"pgvector": "/var/lib/postgresql/data", "etcd": "/etcd", "minio": "/minio_data", "standalone": "/var/lib/milvus"}
    total = 0
    for container in ids:
        inspect = json.loads(run(["docker", "inspect", container], capture_output=True).stdout)[0]
        service = "pgvector" if backend == "pgvector" else inspect["Config"]["Labels"]["com.docker.compose.service"]
        path = paths[service]
        raw = run(["docker", "run", "--rm", "--volumes-from", container, PG_IMAGE, "du", "-sb", path], capture_output=True).stdout.split()[0]
        total += int(raw)
    return total


def adapter_command(backend: str, corpus: str, repetition: int, endpoint: str, output: Path, storage: Path) -> list[str]:
    script = SOURCE / f"benchmarks/vector_db_compare/{'milvus_bench.py' if backend == 'milvus' else 'pgvector_bench.py'}"
    command = [
        str(VENV), str(script), "--dataset-dir", str(ROOT / f"datasets/{corpus}"),
        "--output", str(output), "--queries", "1000", "--validate-queries", "1000",
        "--top-k", "10", "--search-concurrency", "1,8,32,64", "--m", "16",
        "--ef-construction", "128", "--ef-search-budgets", BUDGETS[repetition - 1],
        "--warmup", "1000", "--min-recall", "0",
    ]
    if backend == "pgvector":
        command.extend(["--dsn", endpoint, "--schema", "gomap_vector_bench", "--table", "documents"])
    else:
        command.extend(["--uri", endpoint, "--token", "root:Milvus", "--collection", "gomap_vector_bench", "--index", "embedding_hnsw", "--storage-dir", str(storage)])
    return command


def pgvector_start(run_dir: Path, name: str) -> tuple[str, list[str], dict[str, object]]:
    port = free_port()
    storage = run_dir / "server"
    storage.mkdir()
    container = run([
        "docker", "run", "-d", "--name", name, "--memory", "20g", "--memory-swap", "20g",
        "--pids-limit", "2048", "--cpuset-cpus", "0-11", "-e", "POSTGRES_PASSWORD=postgres",
        "-e", "POSTGRES_DB=gomap_vector_bench", "-p", f"127.0.0.1:{port}:5432",
        "-v", f"{storage}:/var/lib/postgresql/data", PG_IMAGE, "-c", "max_connections=256",
    ], capture_output=True).stdout.strip()
    endpoint = f"postgresql://postgres:postgres@127.0.0.1:{port}/gomap_vector_bench?sslmode=disable"
    try:
        wait_ready("pgvector", endpoint)
        version_code = "import psycopg,sys; c=psycopg.connect(sys.argv[1]); print(c.execute(\"select default_version from pg_available_extensions where name='vector'\").fetchone()[0]); c.close()"
        pgvector_version = run([str(VENV), "-c", version_code, endpoint], capture_output=True).stdout.strip()
    except Exception:
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise
    return endpoint, [container], {"image": PG_IMAGE, "storage": str(storage), "port": port, "pgvector_version": pgvector_version}


def milvus_start(run_dir: Path, project: str) -> tuple[str, list[str], dict[str, object]]:
    ports = {name: free_port() for name in ("minio_api", "minio_console", "milvus", "health")}
    storage = run_dir / "server"
    storage.mkdir()
    text = COMPOSE.read_text(encoding="utf-8")
    text = text.replace("    container_name: milvus-etcd\n", "").replace("    container_name: milvus-minio\n", "").replace("    container_name: milvus-standalone\n", "")
    text = text.replace("quay.io/coreos/etcd:v3.5.25", ETCD_IMAGE).replace("minio/minio:RELEASE.2024-12-18T13-15-44Z", MINIO_IMAGE).replace("milvusdb/milvus:v2.6.20", MILVUS_IMAGE)
    text = text.replace('- "9001:9001"', f'- "127.0.0.1:{ports["minio_console"]}:9001"').replace('- "9000:9000"', f'- "127.0.0.1:{ports["minio_api"]}:9000"')
    text = text.replace('- "19530:19530"', f'- "127.0.0.1:{ports["milvus"]}:19530"').replace('- "9091:9091"', f'- "127.0.0.1:{ports["health"]}:9091"')
    text = text.split("\nnetworks:\n", 1)[0].rstrip() + "\n"
    compose = run_dir / "compose.yml"
    compose.write_text(text, encoding="utf-8")
    override = run_dir / "resources.yml"
    override.write_text(
        "services:\n"
        "  etcd:\n    mem_limit: 4g\n    memswap_limit: 4g\n    cpuset: '0-11'\n    pids_limit: 768\n"
        "  minio:\n    mem_limit: 4g\n    memswap_limit: 4g\n    cpuset: '0-11'\n    pids_limit: 768\n"
        "  standalone:\n    mem_limit: 12g\n    memswap_limit: 12g\n    cpuset: '0-11'\n    pids_limit: 2048\n",
        encoding="utf-8",
    )
    env = {**os.environ, "DOCKER_VOLUME_DIRECTORY": str(storage)}
    base = ["docker", "compose", "-p", project, "-f", str(compose), "-f", str(override)]
    run([*base, "config"], env=env, stdout=(run_dir / "compose.resolved.yml").open("w", encoding="utf-8"))
    try:
        run([*base, "up", "-d"], env=env)
        ids = run([*base, "ps", "-q"], env=env, capture_output=True).stdout.splitlines()
        if len(ids) != 3:
            raise RuntimeError(f"Milvus compose started {len(ids)} containers, want 3")
        endpoint = f"http://127.0.0.1:{ports['milvus']}"
        wait_ready("milvus", endpoint)
    except Exception:
        subprocess.run([*base, "down", "--remove-orphans"], env=env, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise
    return endpoint, ids, {"images": [ETCD_IMAGE, MINIO_IMAGE, MILVUS_IMAGE], "storage": str(storage), "ports": ports, "compose_base": base}


def cleanup(backend: str, run_dir: Path, identity: dict[str, object], ids: list[str]) -> None:
    for container in ids:
        logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True)
        (run_dir / f"container-{container[:12]}.stdout").write_text(logs.stdout, encoding="utf-8")
        (run_dir / f"container-{container[:12]}.stderr").write_text(logs.stderr, encoding="utf-8")
    if backend == "pgvector":
        subprocess.run(["docker", "rm", "-f", *ids], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        env = {**os.environ, "DOCKER_VOLUME_DIRECTORY": str(identity["storage"])}
        subprocess.run([*identity["compose_base"], "down", "--remove-orphans"], env=env, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # type: ignore[list-item]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("backend", choices=("milvus", "pgvector"))
    parser.add_argument("corpus", choices=("100k", "250k"))
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    assert_preflight()
    backend_dir = "milvus_standalone" if args.backend == "milvus" else "pgvector"
    run_dir = ROOT / "verified-runs" / backend_dir / args.corpus / f"repeat-{args.repetition}"
    run_dir.mkdir(parents=True)
    name = f"gomap4019-m3-{args.backend}-{args.corpus}-r{args.repetition}"
    started = time.monotonic_ns()
    load_started = os.getloadavg()
    ids: list[str] = []
    identity: dict[str, object] = {}
    try:
        if args.backend == "pgvector":
            endpoint, ids, identity = pgvector_start(run_dir, name)
        else:
            endpoint, ids, identity = milvus_start(run_dir, name)
        ready = time.monotonic_ns()
        output = run_dir / "search.json"
        command = adapter_command(args.backend, args.corpus, args.repetition, endpoint, output, Path(str(identity["storage"])))
        wrapped = [
            "systemd-run", "--user", "--scope", "--quiet", "-p", "MemoryMax=4G", "-p", "MemorySwapMax=0",
            "-p", "AllowedCPUs=0-11", "/usr/bin/time", "-v", "-o", str(run_dir / "adapter.time"), *command,
        ]
        write_json(run_dir / "adapter.command.json", wrapped)
        adapter_env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        with (run_dir / "adapter.stdout").open("w", encoding="utf-8") as stdout, (run_dir / "adapter.stderr").open("w", encoding="utf-8") as stderr:
            completed = subprocess.run(wrapped, stdout=stdout, stderr=stderr, text=True, env=adapter_env)
        (run_dir / "adapter.rc").write_text(f"{completed.returncode}\n", encoding="utf-8")
        if completed.returncode != 0 or not output.is_file():
            raise RuntimeError(f"{args.backend} adapter failed rc={completed.returncode}")
        value = json.loads(output.read_text(encoding="utf-8"))
        budgets = value.get("budget_searches")
        if not isinstance(budgets, list) or len(budgets) != 6:
            raise RuntimeError("adapter did not retain six EF budgets")
        if not any(float(row["validation"]["recall"]) >= .90 for row in budgets):
            raise RuntimeError("adapter retained no matched-recall budget")
        measured = time.monotonic_ns()
        resources = container_resources(ids)
        resources["persistent_bytes"] = persistent_bytes(args.backend, ids)
        retained_identity = {key: value for key, value in identity.items() if key != "compose_base"}
        write_json(run_dir / "service.json", {**retained_identity, **resources})
    finally:
        primary_failure = sys.exc_info()[0] is not None
        try:
            if ids:
                cleanup(args.backend, run_dir, identity, ids)
        except Exception as exc:
            if not primary_failure:
                raise
            print(f"cleanup after primary failure: {exc}", file=sys.stderr)
    completed_at = time.monotonic_ns()
    load_completed = os.getloadavg()
    write_json(run_dir / "runner.json", {
        "started_monotonic_nanos": started, "ready_monotonic_nanos": ready,
        "measured_monotonic_nanos": measured, "completed_monotonic_nanos": completed_at,
        "phases": {"readiness": ready - started, "benchmark": measured - ready, "cleanup": completed_at - measured},
        "load_started": load_started, "load_completed": load_completed,
        "source_revision": HEAD, "adapter_sha256": digest(Path(command[1])),
        "python": {"path": str(VENV), "sha256": digest(VENV), "version": run([str(VENV), "--version"], capture_output=True).stdout.strip()},
    })
    print(f"complete backend={args.backend} corpus={args.corpus} repetition={args.repetition}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"M3 STOP: {exc}", file=sys.stderr, flush=True)
        raise
