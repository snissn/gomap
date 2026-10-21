#!/usr/bin/env python3
"""Plan, then explicitly run prebuilt ordered-memory native measurements.

The default only prints a plan. No builds, automatic retries, resume, or sample
selection are performed. Each execution needs a new evidence directory.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any


PLAN_SCHEMA = "orderedmem-native-plan-v1"
RUN_SCHEMA = "orderedmem-native-run-v1"
RESULT_SCHEMA = "orderedmem-native-v1"
PROFILES = ("no_wal_fast", "command_wal_relaxed", "command_wal_durable")
ARMS = ("baseline", "cow-baseline", "candidate", "badger")
TEST_ARGS = ("-test.run=^TestOrderedMemNativeMeasurement$", "-test.count=1", "-test.timeout=30m")
ENV_WHITELIST = (
    "PATH", "TMPDIR", "GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GODEBUG",
    "GOTRACEBACK", "GOMEMPROFILERATE", "GOEXPERIMENT", "GODEBUGCPU", "TZ",
    "LD_LIBRARY_PATH", "CGO_ENABLED", "GOROOT", "GOTOOLCHAIN", "GOWORK",
)
ANCHORS = (
    ("commit1hot251", "commit", 5003, 1, 251, 1, 1, 0,
     "operations are independently acknowledged singleton commits"),
    ("batch16wide4096", "commit", 5003, 16, 4096, 1, 1, 0,
     "operations are independently acknowledged sixteen-mutation commits"),
    ("history256x64", "history", 200003, 1, 256, 64, 1, 0,
     "operations are historical point reads after untimed 256-key/depth-64 seeding"),
    ("range32wide4096depth4", "range", 20003, 1, 4096, 4, 1, 32,
     "operations are historical range requests; output rows are counted separately"),
    ("lifecycle5003batch16", "lifecycle", 5003, 16, 4096, 1, 1, 0,
     "operations are commits; checkpoints every 1000 commits and a held cut are explicit phases"),
    ("shared4hot251", "shared", 5003, 1, 251, 1, 4, 0,
     "operations are TOTAL independently acknowledged commits across four writers sharing 251 keys"),
    ("mixed20003", "mixed", 20003, 1, 256, 1, 1, 0,
     "operations are a deterministic 60/20/20 read/write/delete trace, not all commits"),
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def mappings(values: list[str], option: str) -> dict[str, str]:
    result = {}
    for value in values:
        label, separator, item = value.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z0-9_.-]+", label) or not item:
            raise ValueError(f"{option} expects LABEL=VALUE: {value!r}")
        if label in result:
            raise ValueError(f"duplicate {option} label: {label}")
        result[label] = item
    return result


def make_plan(args: argparse.Namespace) -> dict[str, Any]:
    if not math.isfinite(args.operations_scale) or args.operations_scale <= 0:
        raise ValueError("--operations-scale must be finite and positive")
    if args.repeat < 1:
        raise ValueError("--repeat must be positive")
    profiles = list(dict.fromkeys(args.profile or PROFILES))
    selected = set(args.case or [anchor[0] for anchor in ANCHORS])
    if selected - {anchor[0] for anchor in ANCHORS}:
        raise ValueError("unknown anchor selection")
    cells = []
    for repeat in range(args.repeat):
        for profile in profiles:
            eligible = list(ARMS if profile != "no_wal_fast" else ARMS[:3])
            rotation = repeat % len(eligible)
            order = eligible[rotation:] + eligible[:rotation]
            for anchor, case, count, batch, keys, depth, workers, width, unit in ANCHORS:
                if anchor not in selected:
                    continue
                scaled_count = count * args.operations_scale
                if not math.isfinite(scaled_count) or scaled_count > (1 << 63) - 1:
                    raise ValueError("scaled operation count exceeds signed 64-bit range")
                for arm in order:
                    config = {
                        "case": case, "engine": "badger" if arm == "badger" else "treedb",
                        "profile": profile,
                        "memtable_mode": {"baseline": "adaptive", "cow-baseline": "cow_btree", "candidate": "cow_ordered", "badger": ""}[arm],
                        "dir": "{CELL_DIR}/db", "operations": max(1, int(scaled_count)),
                        "batch_size": batch, "value_bytes": args.value_bytes,
                        "key_count": keys, "history_depth": depth, "workers": workers,
                        "range_width": width or 32, "flush_threshold": (1 << 20) if case == "lifecycle" else args.flush_threshold,
                        "checkpoint_every": 1000 if case == "lifecycle" else 0,
                        "hold_cut": case == "lifecycle", "seed": args.seed,
                        "pointer_threshold": args.pointer_threshold,
                        "retained_diagnostics": args.retained_diagnostics,
                    }
                    if args.capture_profiles:
                        config["profile_dir"] = "{CELL_DIR}/profiles"
                    cells.append({
                        "id": f"r{repeat:03d}-{profile}-{anchor}-{arm}",
                        "anchor": anchor, "repeat": repeat, "arm": arm,
                        "operation_unit": unit, "config": config,
                        "config_template_sha256": hashlib.sha256(canonical(config)).hexdigest(),
                    })
    return {
        "schema_version": PLAN_SCHEMA, "kind": "initial_bounded_diagnostic",
        "qualification": "not_qualified; bounded initial calibration, no acceptance thresholds",
        "minimum_final_pairs": 5,
        "repeat": args.repeat, "operations_scale": args.operations_scale,
        "instrumented": args.capture_profiles,
        "retained_diagnostics": args.retained_diagnostics,
        "trace_contract": "Trace hashes exclude engine, profile, memtable_mode and paths; include useful workload configuration and actual operation/key/value bytes. Shared writers use monotone timestamps after seed.",
        "notes": [
            "No no-WAL Badger cell is generated or scored.",
            "Foreground and whole cycle_ns are distinct measurements.",
            "Retention/profiling diagnostics are separate from ordinary timing; do not pool them.",
            "Lifecycle checkpoints use the declared operation count even when operations are scaled.",
            "Every planned cell is preserved; failed or missing cells make the run incomplete/not-qualified.",
            "Counts are initial calibration counts, not a final qualification campaign.",
        ],
        "cells": cells,
    }


def validate_plan(plan: Any) -> None:
    if not isinstance(plan, dict) or plan.get("schema_version") != PLAN_SCHEMA:
        raise ValueError(f"plan schema must be {PLAN_SCHEMA}")
    cells = plan.get("cells")
    if not isinstance(cells, list) or not cells:
        raise ValueError("plan must contain cells")
    seen = set()
    for cell in cells:
        identity = cell.get("id", "")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", identity) or identity in seen:
            raise ValueError(f"invalid or duplicate cell id: {identity!r}")
        seen.add(identity)
        if cell.get("arm") not in ARMS or type(cell.get("repeat")) is not int or cell["repeat"] < 0:
            raise ValueError(f"invalid arm/repeat in {identity}")
        cfg = cell.get("config", {})
        if cfg.get("case") not in {"commit", "history", "range", "lifecycle", "mixed", "shared"}:
            raise ValueError(f"invalid case in {identity}")
        if cfg.get("engine") not in {"treedb", "badger"} or cfg.get("profile") not in PROFILES:
            raise ValueError(f"invalid engine/profile in {identity}")
        if (cell["arm"] == "badger") != (cfg["engine"] == "badger"):
            raise ValueError(f"arm/engine mismatch in {identity}")
        if cfg["engine"] == "badger" and cfg["profile"] == "no_wal_fast":
            raise ValueError(f"no no-WAL Badger comparison is defined: {identity}")
        expected_mode = {"baseline": "adaptive", "cow-baseline": "cow_btree", "candidate": "cow_ordered", "badger": ""}[cell["arm"]]
        if cfg.get("memtable_mode") != expected_mode:
            raise ValueError(f"unexpected memtable route in {identity}")
        for field in ("operations", "batch_size", "value_bytes", "key_count", "history_depth", "workers"):
            value = cfg.get(field)
            if type(value) is not int or value < (16 if field == "value_bytes" else 1):
                raise ValueError(f"invalid {field} in {identity}")
        for field in ("range_width", "checkpoint_every", "seed", "pointer_threshold", "flush_threshold"):
            if type(cfg.get(field)) is not int or cfg[field] < (1 if field in {"flush_threshold", "range_width"} else 0):
                raise ValueError(f"invalid {field} in {identity}")
        for field in ("hold_cut", "retained_diagnostics"):
            if type(cfg.get(field)) is not bool:
                raise ValueError(f"invalid {field} in {identity}")
        if cfg["case"] == "range" and cfg["range_width"] < 1:
            raise ValueError(f"range_width must be positive in {identity}")
        if cfg["key_count"] < cfg["batch_size"]:
            raise ValueError(f"key_count must cover batch_size in {identity}")
        if cfg["seed"] > (1 << 64) - 1:
            raise ValueError(f"seed exceeds uint64 in {identity}")
        for field, maximum in (("badger_memtable_size", (1 << 63) - 1),
                               ("badger_value_threshold", (1 << 63) - 1),
                               ("operational_min_rotations", (1 << 64) - 1),
                               ("operational_min_payload_bytes", (1 << 64) - 1)):
            value = cfg.get(field, 0)
            if type(value) is not int or not 0 <= value <= maximum:
                raise ValueError(f"invalid optional {field} in {identity}")
        if not isinstance(cfg.get("dir"), str) or not cfg["dir"]:
            raise ValueError(f"missing database directory in {identity}")
        expected_hash = cell.get("config_template_sha256")
        if expected_hash and expected_hash != hashlib.sha256(canonical(cfg)).hexdigest():
            raise ValueError(f"config template hash mismatch in {identity}")


def source_inventory(label: str, root: Path, claimed: str | None) -> dict[str, Any]:
    root = root.resolve(strict=True)
    files = {}
    for directory, subdirs, names in os.walk(root):
        subdirs[:] = sorted(name for name in subdirs if name != ".git")
        for name in sorted(names):
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if (name.endswith(".go") or name in {"go.mod", "go.sum", "go.work", "go.work.sum", "experiment.mod", "experiment.sum"}
                    or relative.startswith("benchmarks/orderedmem_native/")) and path.is_file():
                files[relative] = digest_file(path)
    git = {}
    for key, command in (("head", ["git", "rev-parse", "HEAD"]), ("status", ["git", "status", "--porcelain=v1"])):
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
        git[key] = {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    return {"label": label, "dir": str(root), "claimed_source": claimed, "git": git,
            "files": files, "inventory_sha256": hashlib.sha256(canonical(files)).hexdigest(),
            "scope": "Go files, module/workspace files and orderedmem_native harness; inventory is not compiler-input attestation"}


def result_errors(result: Any, config: dict[str, Any]) -> list[str]:
    errors = []
    if not isinstance(result, dict):
        return ["result is not a JSON object"]
    if result.get("schema_version") != RESULT_SCHEMA:
        errors.append("unexpected result schema_version")
    if result.get("success") is not True:
        errors.append("result success is not true: " + str(result.get("error", "")))
    if not isinstance(result.get("trace_sha256"), str) or not re.fullmatch(r"[a-fA-F0-9]{64}", result["trace_sha256"]):
        errors.append("missing/invalid trace_sha256")
    reported = result.get("config")
    if not isinstance(reported, dict):
        errors.append("missing config object")
    else:
        for key, value in config.items():
            if reported.get(key) != value:
                errors.append(f"reported config mismatch: {key}")
    if not isinstance(result.get("runtime"), dict):
        errors.append("missing runtime object")
    for field in ("route", "reopened_route"):
        route = result.get(field)
        if not isinstance(route, dict):
            errors.append("missing " + field + " object")
            continue
        expected_route = {"engine": config.get("engine"), "acknowledgment": config.get("profile"),
                          "resolved_profile": "managed" if config.get("engine") == "badger" else config.get("profile")}
        for key, value in expected_route.items():
            if route.get(key) != value:
                errors.append(f"{field} mismatch: {key}")
        if route.get("memtable_mode", "") != config.get("memtable_mode", ""):
            errors.append(field + " mismatch: memtable_mode")
        if config.get("memtable_mode") == "cow_ordered" and route.get("logical_mvcc") is not True:
            errors.append(field + " does not confirm candidate logical_mvcc")
    if isinstance(result.get("route"), dict) and isinstance(result.get("reopened_route"), dict) and result["route"] != result["reopened_route"]:
        errors.append("reopened route differs from initial route")
    if not isinstance(result.get("verification"), dict):
        errors.append("missing verification object")
    elif result["verification"].get("success") is False or result["verification"].get("verified") is False:
        errors.append("verification explicitly reports failure")
    if isinstance(result.get("verification"), dict):
        required = ["foreground_outputs", "distinct_values_and_complete_history_after_reopen"]
        if config.get("hold_cut"):
            required.append("held_cut")
        if config.get("memtable_mode") == "cow_ordered":
            required.append("ordered_entries_observed")
        if config.get("operational_min_rotations", 0) > 0 or config.get("operational_min_payload_bytes", 0) > 0:
            required.append("operational_activation")
        for key in required:
            if result["verification"].get(key) is not True:
                errors.append("verification does not confirm " + key)
    if config.get("memtable_mode") == "cow_ordered":
        activation = result.get("activation_stats")
        if not isinstance(activation, dict):
            errors.append("candidate activation_stats missing")
        else:
            for key in ("treedb.cache.cow.ordered.logical_keys", "treedb.cache.cow.ordered.physical_versions"):
                value = activation.get(key)
                if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,20}", value) or not 0 < int(value) <= (1 << 64) - 1:
                    errors.append("candidate activation counter is not positive uint64: " + key)
    phases = result.get("phases")
    if not isinstance(phases, list) or not phases:
        errors.append("missing phases")
    else:
        names = set()
        for phase in phases:
            if not isinstance(phase, dict) or not isinstance(phase.get("name"), str):
                errors.append("invalid phase object/name")
                continue
            if phase["name"] in names:
                errors.append("duplicate phase name: " + phase["name"])
            names.add(phase["name"])
            for key in ("nanoseconds", "operations", "mutations", "output_records", "allocated_bytes", "allocations", "num_gc", "heap_alloc", "heap_objects"):
                if type(phase.get(key)) is not int or phase[key] < 0:
                    errors.append(f"invalid {phase['name']}.{key}")
    if type(result.get("cycle_ns")) is not int or result["cycle_ns"] < 0:
        errors.append("invalid cycle_ns")
    return errors


def execute(plan: dict[str, Any], args: argparse.Namespace) -> int:
    if not args.out:
        raise ValueError("--execute requires --out pointing to a fresh directory")
    binary_paths = mappings(args.binary, "--binary")
    sources = mappings(args.source, "--source")
    source_dirs = mappings(args.source_dir, "--source-dir")
    needed = {cell["arm"] for cell in plan["cells"]}
    if "cow-baseline" in needed:
        if "cow-baseline" not in binary_paths and "baseline" in binary_paths:
            binary_paths["cow-baseline"] = binary_paths["baseline"]
        if "cow-baseline" not in sources and "baseline" in sources:
            sources["cow-baseline"] = sources["baseline"]
    if needed - binary_paths.keys():
        raise ValueError("missing binary mappings: " + ", ".join(sorted(needed - binary_paths.keys())))
    if "badger" in needed and "badger" not in sources and "baseline" in sources:
        sources["badger"] = sources["baseline"]
    if needed - sources.keys():
        raise ValueError("missing source mappings: " + ", ".join(sorted(needed - sources.keys())))
    for label, source in sources.items():
        if not re.fullmatch(r"[a-fA-F0-9]{40,64}", source):
            raise ValueError(f"--source {label} requires a full hexadecimal source SHA")
    resolved_binaries = {}
    for label in needed:
        path = Path(binary_paths[label]).expanduser().resolve(strict=True)
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"binary is not an executable file: {path}")
        resolved_binaries[label] = path
    output = Path(args.out).expanduser().absolute()
    if output.exists():
        raise ValueError(f"refusing an existing evidence directory: {output}")
    output.mkdir(parents=True, exist_ok=False)
    atomic_json(output / "plan.json", plan)
    manifest = {
        "schema_version": RUN_SCHEMA, "created_at": utc_now(), "finished_at": None,
        "complete": False, "success": False, "qualification": "not_qualified; initial bounded diagnostic",
        "plan_sha256": digest_file(output / "plan.json"),
        "plan_canonical_sha256": hashlib.sha256(canonical(plan)).hexdigest(),
        "environment_whitelist": {key: os.environ[key] for key in ENV_WHITELIST if key in os.environ},
        "environment_policy": "child inherits environment; only the explicit whitelist plus harness variables is recorded; this is not a complete environment attestation",
        "binary_policy": "prebuilt executables copied once into evidence before execution; SHA256 records actual immutable run copies",
        "sources": sources, "source_inventories": {}, "binaries": {},
        "cells": [{"id": cell["id"], "status": "planned"} for cell in plan["cells"]],
    }
    manifest_path = output / "manifest.json"
    atomic_json(manifest_path, manifest)
    try:
        for label, root in source_dirs.items():
            inventory = source_inventory(label, Path(root), sources.get(label))
            atomic_json(output / "sources" / (label + ".json"), inventory)
            manifest["source_inventories"][label] = "sources/" + label + ".json"
        copies = {}
        (output / "binaries").mkdir()
        for label in sorted(needed):
            original = resolved_binaries[label]
            key = str(original)
            if key not in copies:
                saved = output / "binaries" / label
                shutil.copyfile(original, saved)
                saved.chmod(0o555)
                copies[key] = {"path": str(saved), "sha256": digest_file(saved), "bytes": saved.stat().st_size}
            manifest["binaries"][label] = {"input_path": str(original), **copies[key], "source": sources[label]}
        atomic_json(manifest_path, manifest)
        for index, cell in enumerate(plan["cells"]):
            directory = output / "cells" / cell["id"]
            directory.mkdir(parents=True, exist_ok=False)
            (directory / "tmp").mkdir()
            config = copy.deepcopy(cell["config"])
            # Saved plans cannot cause multiple cells to reuse an external DB.
            config["dir"] = str(directory / "db")
            if config.get("profile_dir"):
                config["profile_dir"] = str(directory / "profiles")
            config_path, result_path = directory / "config.json", directory / "result.json"
            atomic_json(config_path, config)
            command = [manifest["binaries"][cell["arm"]]["path"], *TEST_ARGS]
            environment = os.environ.copy()
            environment.update({"ORDEREDMEM_NATIVE_CONFIG": str(config_path), "ORDEREDMEM_NATIVE_OUT": str(result_path), "TMPDIR": str(directory / "tmp")})
            row = manifest["cells"][index]
            row.update({"status": "running", "started_at": utc_now(), "command": command,
                        "cwd": str(directory), "environment": {key: environment[key] for key in (*ENV_WHITELIST, "ORDEREDMEM_NATIVE_CONFIG", "ORDEREDMEM_NATIVE_OUT") if key in environment},
                        "config": config, "config_path": str(config_path.relative_to(output)),
                        "config_sha256": digest_file(config_path),
                        "config_canonical_sha256": hashlib.sha256(canonical(config)).hexdigest(),
                        "result_path": str(result_path.relative_to(output)), "stdout_path": str((directory / "stdout.txt").relative_to(output)),
                        "stderr_path": str((directory / "stderr.txt").relative_to(output))})
            atomic_json(directory / "command.json", {key: row[key] for key in ("command", "cwd", "environment", "config_sha256")})
            atomic_json(manifest_path, manifest)
            print(f"[{index + 1}/{len(plan['cells'])}] {cell['id']}", file=sys.stderr, flush=True)
            errors = []
            try:
                with (directory / "stdout.txt").open("wb") as stdout, (directory / "stderr.txt").open("wb") as stderr:
                    process = subprocess.run(command, cwd=directory, env=environment, stdout=stdout, stderr=stderr, check=False)
                row["returncode"] = process.returncode
                if process.returncode:
                    errors.append(f"process returncode {process.returncode}")
            except OSError as error:
                row["returncode"] = None
                errors.append(f"process launch failed: {error}")
            if result_path.is_file():
                row["result_sha256"] = digest_file(result_path)
                try:
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    errors.extend(result_errors(result, config))
                    row["result_success"] = result.get("success") if isinstance(result, dict) else None
                except (ValueError, OSError) as error:
                    errors.append(f"partial/invalid result JSON retained: {error}")
            else:
                errors.append("result JSON missing")
            row.update({"status": "failed" if errors else "success", "errors": errors, "finished_at": utc_now()})
            atomic_json(manifest_path, manifest)
        manifest["complete"] = True
        manifest["success"] = all(row["status"] == "success" for row in manifest["cells"])
        return 0 if manifest["success"] else 1
    except KeyboardInterrupt:
        manifest["interrupted"] = True
        for row in manifest["cells"]:
            if row["status"] == "running":
                row.update({"status": "interrupted", "finished_at": utc_now()})
        return 130
    except Exception as error:
        manifest["runner_error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        manifest["finished_at"] = utc_now()
        atomic_json(manifest_path, manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="explicitly execute a plan with prebuilt binaries")
    parser.add_argument("--out", help="fresh evidence directory; required only for execution")
    parser.add_argument("--plan", type=Path, help="preserved plan JSON; generation flags cannot be combined")
    parser.add_argument("--plan-out", type=Path, help="also write the exact plan JSON to this new file")
    parser.add_argument("--binary", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--source", action="append", default=[], metavar="LABEL=SHA")
    parser.add_argument("--source-dir", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--operations-scale", type=float, default=1.0)
    parser.add_argument("--profile", action="append", choices=PROFILES)
    parser.add_argument("--case", action="append", choices=[anchor[0] for anchor in ANCHORS])
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--flush-threshold", type=int, default=64 << 20)
    parser.add_argument("--pointer-threshold", type=int, default=256)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--retained-diagnostics", action="store_true")
    parser.add_argument("--capture-profiles", action="store_true", help="separate instrumented diagnostic plan")
    args = parser.parse_args()
    try:
        if args.plan:
            generation_flags = {"--repeat", "--operations-scale", "--profile", "--case", "--value-bytes", "--flush-threshold", "--pointer-threshold", "--seed", "--retained-diagnostics", "--capture-profiles"}
            if any(token.split("=", 1)[0] in generation_flags for token in sys.argv[1:]):
                raise ValueError("--plan preserves exact input; do not combine it with generation flags")
            plan = json.loads(args.plan.read_text(encoding="utf-8"))
        else:
            plan = make_plan(args)
        validate_plan(plan)
        if args.plan_out:
            with args.plan_out.open("x", encoding="utf-8") as stream:
                json.dump(plan, stream, indent=2, sort_keys=True, allow_nan=False)
                stream.write("\n")
        if not args.execute:
            if args.out or args.binary or args.source or args.source_dir:
                raise ValueError("execution mappings/--out require --execute; use --plan-out for planning")
            json.dump(plan, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
            sys.stdout.write("\n")
            return 0
        return execute(plan, args)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
