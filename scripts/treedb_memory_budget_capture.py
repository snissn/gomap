#!/usr/bin/env python3
"""Prepare, run and validate one fresh-process memory-budget evidence cell."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time

from treedb_memory_budget_overlay import COHORTS, generate

HARNESS = "TreeDB/memory_budget_bench_test.go"
PHASES = ["load_sync", "checkpoint", "owned_get_sweep", "reused_append_sweep",
          "read_gc1", "read_gc2", "updates_sync", "update_checkpoint",
          "verify_before_close", "verify_reopen"]
SOURCE_SUFFIXES = {".go", ".s", ".S", ".c", ".h", ".cc", ".cpp", ".syso", ".mod", ".sum", ".py"}
INPUT_FIELDS = ("GoFiles", "CgoFiles", "CFiles", "CXXFiles", "MFiles", "HFiles",
                "FFiles", "SFiles", "SysoFiles", "EmbedFiles", "TestGoFiles", "XTestGoFiles")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_identity(actual, expected):
    require(actual == expected, "wrong/changed source/input/overlay/binary identity")


def digest(path):
    with Path(path).open("rb") as stream:
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
        return result.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def source_identity(root):
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "-c", "-o",
                                     "--exclude-standard", "-z"]).decode().split("\0")
    sources = {name: digest(root / name) for name in sorted(set(names))
               if name and Path(name).suffix in SOURCE_SUFFIXES}
    return {"runtime_head": git(root, "rev-parse", "HEAD"),
            "treedb_tree": git(root, "rev-parse", "HEAD:TreeDB"),
            "harness_sha256": digest(root / HARNESS), "source_files": sources}


def json_stream(text):
    decoder = json.JSONDecoder()
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        yield value
        text = text.lstrip()[end:]


def compile_inputs(listing, replacements, goroot=None):
    paths = set()
    for package in json_stream(listing):
        require(not package.get("Error") and not package.get("DepsErrors"), "go list dependency error")
        for field in INPUT_FIELDS:
            for name in package.get(field, []):
                path = str((Path(package["Dir"]) / name).resolve())
                paths.add(replacements.get(path, path))
        module = package.get("Module", {})
        module = module.get("Replace", module)
        if module.get("GoMod"):
            paths.add(module["GoMod"])
    if goroot:
        paths.update(str(path) for path in (Path(goroot) / "pkg/tool").rglob("*") if path.is_file())
    return {path: digest(path) for path in sorted(paths)}


def environment(leaf, value_bytes, threshold, pilot):
    # An explicit small environment avoids publishing ambient credentials and
    # gives the child precisely the environment recorded in the freeze.
    names = ("PATH", "HOME", "TMPDIR", "GOCACHE", "GOPATH", "GOROOT", "GOMODCACHE",
             "CC", "CXX", "CGO_ENABLED", "GOMAXPROCS", "GOMEMLIMIT", "GOGC", "GODEBUG")
    env = {name: os.environ[name] for name in names if name in os.environ}
    env.update(GOWORK="off", GOENV="off", GOFLAGS="", GOTOOLCHAIN="local",
               TREEDB_MEMORY_LEAF_MIB=str(leaf), TREEDB_MEMORY_VALUE_BYTES=str(value_bytes),
               TREEDB_MEMORY_POINTER_THRESHOLD=str(threshold), TREEDB_MEMORY_PILOT="1" if pilot else "0")
    return env


def command(argv, root, env, destination, name):
    started = time.time_ns()
    with (destination / (name + ".stdout")).open("wb") as stdout, (destination / (name + ".stderr")).open("wb") as stderr:
        process = subprocess.Popen(argv, cwd=root, env=env, stdout=stdout, stderr=stderr)
        exit_code = process.wait()
    return {"argv": argv, "cwd": str(root), "environment": env, "pid": process.pid,
            "started_unix_ns": started, "finished_unix_ns": time.time_ns(), "exit_code": exit_code,
            "stdout_sha256": digest(destination / (name + ".stdout")),
            "stderr_sha256": digest(destination / (name + ".stderr"))}


def current_freeze_identity(prepared, freeze):
    root = Path(freeze["source_root"])
    identity = source_identity(root)
    identity["compile_inputs"] = {path: digest(path) for path in freeze["identity"]["compile_inputs"]}
    identity["binary_sha256"] = digest(prepared / "memory-budget.test")
    identity["overlay_files"] = {name: digest(prepared / "overlay" / name)
                                 for name in ("identity.json", "overlay.json", "db.go")}
    identity["go_binary_sha256"] = digest(freeze["go_binary"])
    return identity


def prepare(args):
    root, output = args.source_root.resolve(), args.output.resolve()
    require(not output.is_relative_to(root), "prepared artifacts must be outside source checkout")
    before = source_identity(root)
    if not args.pilot:
        require(not git(root, "status", "--porcelain", "--untracked-files=all"), "full preparation requires clean reviewed/landed checkout")
        require(args.runtime_head == before["runtime_head"], "wrong external runtime HEAD")
        require(args.harness_sha256 == before["harness_sha256"], "wrong external harness SHA")
    output.mkdir(parents=True, exist_ok=False)
    overlay = generate(root, output / "overlay", 64 - args.leaf_mib)
    env = environment(args.leaf_mib, args.value_bytes, args.threshold, args.pilot)
    go_binary = str((args.go or Path(shutil.which("go", path=env.get("PATH")))).resolve())
    probe_env = {key: value for key, value in env.items() if key != "GOROOT"}
    env["GOROOT"] = subprocess.check_output([go_binary, "env", "GOROOT"], env=probe_env, text=True).strip()
    go_hash = digest(go_binary)
    overlay_path = str(output / "overlay/overlay.json")
    list_argv = [go_binary, "list", "-deps", "-test", "-json", "-overlay", overlay_path, "./TreeDB"]
    listed = command(list_argv, root, env, output, "inputs")
    write_json(output / "inputs-process.json", listed)
    require(listed["exit_code"] == 0, "go list failed; preserve inputs logs")
    replacements = json.loads((output / "overlay/overlay.json").read_text())["Replace"]
    before["compile_inputs"] = compile_inputs((output / "inputs.stdout").read_text(), replacements, env["GOROOT"])
    build_argv = [go_binary, "test", "-c", "-overlay", overlay_path, "-o", str(output / "memory-budget.test"), "./TreeDB"]
    build = command(build_argv, root, env, output, "build")
    write_json(output / "build-process.json", build)
    require(build["exit_code"] == 0, "build failed; preserve build logs")
    listed_after = command(list_argv, root, env, output, "inputs-after")
    write_json(output / "inputs-after-process.json", listed_after)
    require(listed_after["exit_code"] == 0, "post-build go list failed; preserve inputs-after logs")
    check_identity(compile_inputs((output / "inputs-after.stdout").read_text(), replacements, env["GOROOT"]), before["compile_inputs"])
    require(source_identity(root) == {key: value for key, value in before.items() if key != "compile_inputs"}, "source changed during build")
    require(before["compile_inputs"] == {path: digest(path) for path in before["compile_inputs"]}, "compile input changed during build")
    require(digest(go_binary) == go_hash, "toolchain executable changed during build")
    before.update(binary_sha256=digest(output / "memory-budget.test"), go_binary_sha256=go_hash,
                  overlay_files={name: digest(output / "overlay" / name) for name in ("identity.json", "overlay.json", "db.go")})
    freeze = {"schema": "memory-budget-freeze-v1", "pilot": args.pilot, "source_root": str(root),
              "go_binary": go_binary, "identity": before, "overlay": overlay, "environment": env,
              "host": {"platform": platform.platform(), "hostname": platform.node()},
              "inputs_process": listed, "inputs_after_process": listed_after, "build_process": build}
    write_json(output / "freeze.json", freeze)
    print(digest(output / "freeze.json"))


def load_freeze(prepared, expected):
    freeze = json.loads((prepared / "freeze.json").read_text())
    require(freeze["schema"] == "memory-budget-freeze-v1", "wrong freeze schema")
    if not freeze["pilot"]:
        require(expected and digest(prepared / "freeze.json") == expected, "missing/wrong externally recorded freeze SHA")
    elif expected:
        require(digest(prepared / "freeze.json") == expected, "wrong pilot freeze SHA")
    check_identity(current_freeze_identity(prepared, freeze), freeze["identity"])
    for name, record in (("inputs", freeze["inputs_process"]), ("inputs-after", freeze["inputs_after_process"]), ("build", freeze["build_process"])):
        require(record["exit_code"] == 0, name + " failed")
        for stream in ("stdout", "stderr"):
            require(digest(prepared / (name + "." + stream)) == record[stream + "_sha256"], name + " log changed")
    return freeze


def packet_from(stderr):
    lines = [line.removeprefix("TREEDB_MEMORY_PACKET ") for line in stderr.splitlines()
             if line.startswith("TREEDB_MEMORY_PACKET ")]
    require(len(lines) == 1, "missing/duplicate workflow packet")
    return json.loads(lines[0])


def check_packet(packet, freeze, process, retained):
    env = freeze["environment"]
    pilot = freeze["pilot"]
    require(not retained or not pilot, "pilot cannot satisfy retained validation")
    require(packet["schema"] == "memory-budget-v1" and packet["pilot"] == pilot, "wrong packet mode")
    keys, updates = (8192, 8000) if pilot else (250000, 40000)
    leaf, size, threshold = (int(env[name]) for name in ("TREEDB_MEMORY_LEAF_MIB", "TREEDB_MEMORY_VALUE_BYTES", "TREEDB_MEMORY_POINTER_THRESHOLD"))
    require(leaf in COHORTS and size in (256, 4096) and threshold in (1, 1024), "unsupported frozen cohort")
    expected = {"keys": keys, "updates": updates, "key_bytes": 32, "shared_prefix_bytes": 24,
                "value_bytes": size, "pointer_threshold": threshold, "batch_size": 1000, "update_stride": 7919,
                "main_leaf_budget_bytes": leaf << 20, "main_frame_budget_bytes": (64 - leaf) << 20,
                "combined_configured_main_budget_bytes": 64 << 20, "negative_filter_bytes": 0,
                "crc_disabled": False, "side_store_limits_changed": False, "gc_is_diagnostic": True,
                "background_maintenance_disabled": True, "all_values_and_interleaved_misses_verified": True,
                "final_close_checked": True, "binary_sha256": freeze["identity"]["binary_sha256"],
                "process_id": process["pid"], "process_argv": process["argv"],
                "verified_pointer_entries": keys if threshold == 1 or size == 4096 else 0}
    for key, value in expected.items():
        require(type(packet.get(key)) is type(value) and packet[key] == value, "wrong/missing packet field: " + key)
    require(packet["configuration"] == {
        "command_wal": True, "command_wal_stats_scan": True, "keep_recent": 10000, "flush_threshold": 64 << 20,
        "outer_leaves_in_value_log": True, "leaf_prefix_compression": True, "columnar_leaves": True, "packed_value_ptr": True,
        "leaf_cache_entries": leaf * (1 << 20) // 4096 if leaf else -1,
        "background_checkpoint_interval": -1, "background_checkpoint_idle_duration": -1, "max_wal_bytes": -1,
        "background_index_vacuum_interval": -1, "disable_background_prune": True}, "wrong/missing frozen options")
    env_hash = hashlib.sha256("\0".join(sorted(key + "=" + value for key, value in env.items())).encode()).hexdigest()
    require(packet["environment_sha256"] == env_hash and process["environment"] == env, "wrong process environment")
    require(process["exit_code"] == 0, "workflow failed")
    require([phase["name"] for phase in packet["phases"]] == PHASES, "wrong/missing/duplicate phases")
    operations = [keys, 1, keys, keys, 0, 0, updates, 1, keys * 2, keys * 2]
    for phase, count in zip(packet["phases"], operations):
        require(phase["operations"] == count, "wrong phase operation count")
        require(phase["heap_bytes"] > 0 and phase["stats"] and phase["logical_file_bytes"], "missing owner/heap/storage sample")
        if count:
            require(phase["elapsed_ns"] > 0 and phase["before_stats"], "missing timed phase boundary")
            require(phase["allocated_bytes"] >= 0 and phase["allocations"] >= 0, "invalid allocation delta")
    for stats in [packet["initial_stats"], packet["closure_stats"], packet["reopen_stats"]] + [phase["stats"] for phase in packet["phases"]]:
        require(stats["treedb.vlog.read_integrity"] == "verify" and int(stats["treedb.negative_lookup_filter.active_bytes"]) == 0, "integrity/filter configuration drift")
        require(not stats.get("treedb.command_wal.stats_error"), "command-WAL stats unavailable")
        require(int(stats["treedb.process.read_path.outer_leaf.cache.capacity"]) * 4096 == leaf << 20, "wrong leaf limit")
        require(int(stats["treedb.vlog.grouped_frame_cache.budget_bytes"]) == (64 - leaf) << 20, "wrong frame limit")
        if leaf == 64:
            require(int(stats["treedb.vlog.grouped_frame_cache.capacity"]) == 0, "zero bytes did not disable frames")
        for name in ("heap_alloc_bytes", "peak_heap_alloc_bytes", "rss_bytes", "rss_hwm_bytes", "vlog_mmap_active_bytes"):
            require(int(stats["treedb.process.memory." + name]) >= 0, "missing process memory owner")
        for name in ("treedb.process.batch_arena.retained_bytes_estimate", "treedb.process.append_only.entry_pool_retained_bytes_estimate",
                     "treedb.process.append_only.mem_lease_entry_backing_bytes", "treedb.process.append_only.mutable_entry_backing_bytes",
                     "treedb.vlog.grouped_frame_cache.retained_bytes", "treedb.vlog.decode_scratch.small_pool.retained_bytes"):
            require(int(stats[name]) >= 0, "missing retained owner: " + name)
    closure = packet["closure_stats"]
    require(int(closure["treedb.command_wal.applied_lsn"]) >= int(closure["treedb.command_wal.live_accepted_max_lsn"]) > 0, "uncovered acknowledged LSN")
    require(packet["closed_files"], "missing closed storage inventory")
    if retained:
        require(packet["goos"] == "linux" and packet["rss_available"], "retained packet requires observed Linux RSS/HWM")
        require(int(closure["treedb.process.memory.rss_hwm_bytes"]) > 0, "missing observed RSS high water")


def run(args):
    prepared, output = args.prepared.resolve(), args.output.resolve()
    freeze = load_freeze(prepared, args.freeze_sha256)
    require(not output.is_relative_to(Path(freeze["source_root"])), "run artifacts must be outside source")
    output.mkdir(parents=True, exist_ok=False)
    before = current_freeze_identity(prepared, freeze)
    argv = [str(prepared / "memory-budget.test"), "-test.run=^$", "-test.bench=^BenchmarkMemoryBudgetWorkflow$",
            "-test.benchtime=1x", "-test.count=1", "-test.benchmem", "-test.v", "-test.timeout=60m"]
    process = command(argv, Path(freeze["source_root"]), freeze["environment"], output, "run")
    after = current_freeze_identity(prepared, freeze)
    record = {"schema": "memory-budget-run-v1", "freeze_sha256": digest(prepared / "freeze.json"),
              "before": before, "after": after, "process": process}
    write_json(output / "run.json", record)
    validate(args)


def validate(args):
    prepared, output = args.prepared.resolve(), args.output.resolve()
    freeze = load_freeze(prepared, args.freeze_sha256)
    record = json.loads((output / "run.json").read_text())
    require(record["schema"] == "memory-budget-run-v1", "wrong run schema")
    require(record["freeze_sha256"] == digest(prepared / "freeze.json"), "run freeze mismatch")
    require(record["before"] == record["after"] == freeze["identity"], "wrong/changed before/after source or binary identity")
    process = record["process"]
    require(process["cwd"] == freeze["source_root"], "wrong process cwd")
    require(process["argv"] == [str(prepared / "memory-budget.test"), "-test.run=^$", "-test.bench=^BenchmarkMemoryBudgetWorkflow$",
                                "-test.benchtime=1x", "-test.count=1", "-test.benchmem", "-test.v", "-test.timeout=60m"], "wrong workflow argv")
    for stream in ("stdout", "stderr"):
        require(digest(output / ("run." + stream)) == process[stream + "_sha256"], "changed " + stream)
    require("PASS" in (output / "run.stdout").read_text().splitlines(), "no process PASS")
    packet = packet_from((output / "run.stderr").read_text())
    check_packet(packet, freeze, process, args.retained)
    print("memory budget evidence validation PASS (" + ("retained cell" if args.retained else "diagnostic; no retained decision") + ")")


def self_check():
    # Identity and packet stream checks use the same fail-closed paths as runs.
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "input.go").write_text("package input\n")
        listing = json.dumps({"Dir": directory, "GoFiles": ["input.go"]})
        original = compile_inputs(listing, {})
        (root / "input.go").write_text("package changed\n")
        require(original != compile_inputs(listing, {}), "changed source accepted")
        for text in ("", "TREEDB_MEMORY_PACKET {}\nTREEDB_MEMORY_PACKET {}"):
            try:
                packet_from(text)
            except ValueError:
                pass
            else:
                raise AssertionError("invalid packet stream accepted")
        require(packet_from('TREEDB_MEMORY_PACKET {"pilot":true}') == {"pilot": True}, "packet parser failed")
    identity = {"runtime_head": "head", "treedb_tree": "tree", "harness_sha256": "harness",
                "binary_sha256": "binary", "source_files": {"input.go": "sha"}, "overlay_files": {"db.go": "sha"}}
    check_identity(identity, copy.deepcopy(identity))
    for field in identity:
        wrong = copy.deepcopy(identity)
        wrong[field] = "changed"
        try:
            check_identity(wrong, identity)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted wrong/changed identity: " + field)
    print("memory budget capture self-check PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--source-root", type=Path, required=True)
    prep.add_argument("--output", type=Path, required=True)
    prep.add_argument("--leaf-mib", type=int, choices=COHORTS, required=True)
    prep.add_argument("--value-bytes", type=int, choices=(256, 4096), required=True)
    prep.add_argument("--threshold", type=int, choices=(1, 1024), required=True)
    prep.add_argument("--pilot", action="store_true")
    prep.add_argument("--go", type=Path, help="actual Go 1.26+ executable; automatic toolchain switching is disabled")
    prep.add_argument("--runtime-head")
    prep.add_argument("--harness-sha256")
    for name in ("run", "validate"):
        sub = commands.add_parser(name)
        sub.add_argument("--prepared", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
        sub.add_argument("--freeze-sha256")
        sub.add_argument("--retained", action="store_true")
    commands.add_parser("self-check")
    args = parser.parse_args()
    try:
        if args.command == "self-check":
            self_check()
        else:
            globals()[args.command](args)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, "memory budget evidence FAIL: " + str(error) + "\n")
