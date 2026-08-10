#!/usr/bin/env python3
"""Run one serialized #4096 strict-proof row using the #4091 harness."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True

OLD = Path(__file__).resolve().parents[2] / "vector-partition-runtime-ownership-4091/tools/run_runtime_ownership.py"
SPEC = importlib.util.spec_from_file_location("runtime_ownership_runner", OLD)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load #4091 runner")
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)

CAPABILITY_KEY: Path
CONTAINER_IMAGE: str
ORIGINAL_NODE_CONFIG = BASE.node_config
ORIGINAL_DOCKER = BASE.docker
ORIGINAL_VALIDATE_RESULT = BASE.validate_result


def node_config(root: Path, run_dir: Path, provenance: dict[str, object], topology: str,
                index: int, endpoints: dict[str, str], public: str, rebinder: Path) -> Path:
    path = ORIGINAL_NODE_CONFIG(root, run_dir, provenance, topology, index, endpoints, public, rebinder)
    value = BASE.load_json(path)
    value["capability_key_path"] = str(CAPABILITY_KEY)
    value["profile_directory"] = str(path.parent / "profiles")
    BASE.write_json(path, value)
    return path


def docker(*args: str, check: bool = True):
    values = list(args)
    if values and values[0] == "run":
        image = values.index(CONTAINER_IMAGE)
        values[image:image] = ["-v", f"{CAPABILITY_KEY}:{CAPABILITY_KEY}:ro"]
    elif values[:2] == ["rm", "-f"]:
        ORIGINAL_DOCKER("stop", "-t", "60", values[2], check=False)
    return ORIGINAL_DOCKER(*values, check=check)


def validate_result(path: Path) -> None:
    ORIGINAL_VALIDATE_RESULT(path)
    for cell in BASE.load_json(path)["cells"]:
        counters = cell.get("counters", {})
        catalog = cell.get("catalog_reads", {}).get("total", {})
        total, strict = catalog.get("total", {}), catalog.get("strict_search", {})
        refresh = catalog.get("serving_refresh", {})
        queries = cell["metrics"]["completed_queries"]
        if counters.get("snapshot_pins") != queries or any(counters.get(key) != 0 for key in ("read_proofs", "generation_pins", "partition_opens")):
            raise RuntimeError("strict request performed duplicate proof or request-side asset work")
        if strict.get("reads") != queries or total.get("reads") != queries + refresh.get("reads", -queries):
            raise RuntimeError("strict request did not retain exactly one no-log ingress proof")
        for stage in ("total", "operations_health", "strict_search", "serving_refresh", "coordinator_lifecycle", "shard_lifecycle", "unknown"):
            value = catalog.get(stage, {})
            if value.get("reads") != value.get("successes") or value.get("reads") != value.get("verify_leader_calls") or value.get("reads") != value.get("no_log_proofs") or value.get("failures") != 0 or value.get("log_barriers") != 0:
                raise RuntimeError("strict proof or background refresh was not successful and no-log")
        if any(catalog.get(stage, {}).get("reads") != 0 for stage in ("operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")):
            raise RuntimeError("strict request retained a duplicate catalog proof")


def preflight(root: Path, provenance: dict[str, object]):
    global CAPABILITY_KEY, CONTAINER_IMAGE
    binary, rebinder = BASE.preflight(root, provenance)
    CAPABILITY_KEY = Path(str(provenance.get("capability_key_path", ""))).resolve(strict=True)
    CONTAINER_IMAGE = str(provenance["container_image"])
    mode = CAPABILITY_KEY.stat()
    if CAPABILITY_KEY != (root / "capability.key").resolve() or not stat.S_ISREG(mode.st_mode) or stat.S_IMODE(mode.st_mode) != 0o600 or mode.st_size != 32 or BASE.sha256(CAPABILITY_KEY) != provenance.get("capability_key_sha256"):
        raise RuntimeError("strict capability key identity changed")
    for excluded in (Path(str(provenance["dataset_directory"])), Path(str(provenance["m3_database_directory"]))):
        if CAPABILITY_KEY.is_relative_to(excluded.resolve()):
            raise RuntimeError("strict capability key is inside a data root")
    return binary, rebinder


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("topology", choices=("single", "native", "container"))
    parser.add_argument("repetition", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    provenance = BASE.load_json(args.root / "provenance.json")
    binary, rebinder = preflight(args.root, provenance)
    BASE.node_config = node_config
    BASE.validate_result = validate_result
    BASE.docker = docker
    if args.topology == "container":
        BASE.run_container(args.root, provenance, args.repetition, binary, rebinder)
    else:
        BASE.run_native(args.root, provenance, args.topology, args.repetition, binary, rebinder)
    print(f"PASS topology={args.topology} repetition={args.repetition}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"#4096 STOP: {exc}", file=sys.stderr, flush=True)
        raise
