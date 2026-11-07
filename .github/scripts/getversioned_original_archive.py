"""Authenticate the fixed original evidence archive; this alone grants no acceptance."""

import hashlib
import importlib.util
import json
import math
import subprocess
from pathlib import Path
import stat
import zipfile


MANIFEST_SHA256 = "9e9a1a7dca6c911b8afefbe910650bec14aa3aafa314d551997ca50f74ecafc7"
MAX_MEMBER = 128 * 1024 * 1024
MAX_EXPANDED = 1024 * 1024 * 1024


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def authenticate(manifest_path, archive_path):
    raw_manifest = Path(manifest_path).read_bytes()
    if hashlib.sha256(raw_manifest).hexdigest() != MANIFEST_SHA256:
        raise ValueError("manifest identity mismatch")
    manifest = json.loads(raw_manifest)
    archive = Path(archive_path)
    if archive.stat().st_size != manifest["artifact"]["bytes"]:
        raise ValueError("archive length mismatch")
    if sha256_file(archive) != manifest["artifact"]["sha256"]:
        raise ValueError("archive digest mismatch")
    seen = set()
    total = 0
    found = {}
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            name = info.filename
            components = name.rstrip("/").split("/")
            if (not name or name.startswith("/") or "\\" in name or
                    any(part in ("", ".", "..") for part in components) or
                    ":" in components[0] or name in seen):
                raise ValueError("unsafe or duplicate archive member")
            seen.add(name)
            if stat.S_ISLNK(info.external_attr >> 16):
                raise ValueError("archive symlink")
            if info.is_dir():
                continue
            if info.file_size > MAX_MEMBER:
                raise ValueError("archive member exceeds limit")
            total += info.file_size
            if total > MAX_EXPANDED:
                raise ValueError("archive exceeds expanded limit")
            h = hashlib.sha256()
            size = 0
            with zf.open(info) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    size += len(block)
                    h.update(block)
            if size != info.file_size:
                raise ValueError("member length mismatch")
            found[name] = {"bytes": size, "sha256": h.hexdigest()}
    required = ["primary/baseline-primary.test", "primary/candidate-primary.test", "primary/summary.json", "primary/samples.json", "primary/bindings.json", "primary/runtime-inputs-before.json", "primary/runtime-inputs-after.json", "environment.txt", "measured-binaries.json", "binary-sha256.txt", "summary.json", "baseline.txt", "candidate.txt"]
    required += ["measured-binaries/" + revision + "-" + package + ".test" for revision in ("baseline", "candidate") for package in ("db", "caching", "treedb")]
    if any(name not in found for name in required):
        raise ValueError("required authenticated archive member missing")
    if found["primary/summary.json"]["sha256"] != manifest["summary_sha256"]:
        raise ValueError("original PRIMARY summary digest differs")
    return found


def derive_manifest(manifest, found):
    """Use only digests derived from the already authenticated whole ZIP."""
    result = dict(manifest)
    result["raw_elf_sha256"] = {
        f"{revision}-{package}.test": found[f"measured-binaries/{revision}-{package}.test"]["sha256"]
        for revision in ("baseline", "candidate") for package in ("db", "caching", "treedb")
    }
    for revision in ("baseline", "candidate"):
        result[f"primary_{revision}_elf_sha256"] = found[f"primary/{revision}-primary.test"]["sha256"]
    return result


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # The raw checker defines dataclasses, whose module must be registered.
    import sys
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def require_scoped_primary(measured):
    if measured["gates"] != {"timing": True, "bytes": False, "allocations": True,
                              "candidate_primary_activation": True}:
        raise ValueError("unexpected PRIMARY gate result")
    if not math.isclose(measured["median_paired_delta_percent"], 0.05383210096482349, abs_tol=1e-6):
        raise ValueError("original PRIMARY paired value mismatch")
    if (measured["medians"]["baseline"]["B/op"] != 92138 or
            measured["medians"]["candidate"]["B/op"] != 92142.5 or
            measured["medians"]["baseline"]["allocs/op"] != 340 or
            measured["medians"]["candidate"]["allocs/op"] != 340):
        raise ValueError("original PRIMARY cost differs")


def verify_observations(archive_path, root, scratch, manifest):
    """Recompute original raw and PRIMARY predicates from all paired text rows."""
    root, scratch = Path(root), Path(scratch)
    raw = load_module("original_raw_checker", root / ".github/scripts/check_mvcc_raw_path_gate.py")
    primary = load_module("original_primary_checker", root / "scripts/mvcc_primary_cost_gate.py")
    with zipfile.ZipFile(archive_path) as zf:
        def text(name):
            return zf.read(name).decode("utf-8", errors="strict")

        scratch.mkdir(parents=True, exist_ok=True)
        for revision in ("baseline", "candidate"):
            (scratch / (revision + ".txt")).write_text(text(revision + ".txt"), encoding="utf-8")
        base = raw.parse_benchmarks(scratch / "baseline.txt", 8)
        candidate = raw.parse_benchmarks(scratch / "candidate.txt", 8)
        _, results = raw.evaluate(base, candidate, 5.0, 1.0, 64.0)
        tiny = raw.BENCHMARKS[-1]
        for row in results:
            if not row["bytes_pass"] or not row["allocs_pass"]:
                raise ValueError("non-timing raw predicate failed")
            if not row["timing_pass"]:
                raise ValueError("unexpected raw timing predicate")
        raw_delta = next(row["paired_ns_delta_percent"] for row in results if row["benchmark"] == tiny)
        if not math.isfinite(raw_delta):
            raise ValueError("original tiny-WAL paired value mismatch")
        original_raw = json.loads(text("summary.json"))
        if (original_raw.get("baseline_sha") != "200580bc80c1ebd9f3b1ed58addc3d5723601946"
                or original_raw.get("candidate_sha") != "bce7e35959fdfb821877163284f7627d1ebf906e"
                or original_raw.get("measurement_pass") is not True):
            raise ValueError("raw original source or verdict mismatch")
        original_rows = original_raw.get("results", [])
        if len(original_rows) != len(results):
            raise ValueError("raw original result inventory mismatch")
        for actual, retained in zip(results, original_rows):
            for key, value in actual.items():
                if retained.get(key) != value:
                    raise ValueError("raw original recomputation mismatch: " + key)
        for name, digest in manifest["raw_elf_sha256"].items():
            revision, package = name.removesuffix(".test").split("-", 1)
            if original_raw["binary_digests"][package][revision] != digest:
                raise ValueError("raw summary executable digest mismatch: " + name)
            if digest + "  " + name not in text("binary-sha256.txt").splitlines():
                raise ValueError("raw executable ledger missing: " + name)
        rows = []
        for pair in range(8):
            for revision in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                metrics = primary.parse_sample(text(f"primary/pair-{pair + 1}-{revision}.txt"))
                rows.append({"pair": pair, "revision": revision, "metrics": metrics})
        if rows != json.loads(text("primary/samples.json")):
            raise ValueError("PRIMARY samples differ from original pair text")
        measured = primary.evaluate(rows)
        require_scoped_primary(measured)
        summary = json.loads(text("primary/summary.json"))
        if summary.get("quiet") is not True or summary.get("measurement_pass") is not False:
            raise ValueError("original quiet or measurement verdict absent")
        for key in ("medians", "paired_deltas_percent", "median_paired_delta_percent", "gates"):
            if summary.get(key) != measured[key]:
                raise ValueError("PRIMARY recomputation mismatch: " + key)
        lifecycle = text("primary/candidate-public-lifecycle.txt")
        if ("--- PASS: " + primary.LIFECYCLE + " " not in lifecycle or "--- SKIP:" in lifecycle
                or "--- FAIL:" in lifecycle or "PASS" not in lifecycle.splitlines()):
            raise ValueError("original public lifecycle did not complete")
        process_observation = json.loads(text("primary/measured-processes.json"))
        if process_observation.get("busy") != [] or process_observation.get("aggregate_busy") is not False:
            raise ValueError("original measurement is not quiet")
        commands = json.loads(text("primary/commands.json"))
        by_label = {row["label"]: row for row in commands}
        if len(commands) != len(by_label):
            raise ValueError("duplicate original command receipt")
        required_labels = ["candidate-public-lifecycle", "candidate-guard-before", "candidate-guard-after",
                           "baseline-guard-after"] + [f"pair-{pair}-{revision}" for pair in range(1, 9)
                                                       for revision in ("baseline", "candidate")]
        for label in required_labels:
            record = by_label.get(label)
            if record is None or record["exit"] != 0:
                raise ValueError("original command missing or failed: " + label)
            body = zf.read("primary/" + record["output"])
            if hashlib.sha256(body).hexdigest() != record["output_sha256"]:
                raise ValueError("original command output digest mismatch: " + label)
        before = json.loads(text("primary/runtime-inputs-before.json"))
        after = json.loads(text("primary/runtime-inputs-after.json"))
        qualified = primary.qualify_runtime(measured, before, after, quiet=True)
        if (qualified["accepted"] is not False or qualified["bytes_disposition"] != "FAIL"
                or qualified["runtime_input_equivalent"] is not False
                or qualified["acceptance_gates"]["timing"] is not True
                or qualified["acceptance_gates"]["allocations"] is not True
                or qualified["acceptance_gates"]["candidate_primary_activation"] is not True):
            raise ValueError("original PRIMARY qualification differs from scoped failure")
        binding = json.loads(text("primary/bindings.json"))
        if (binding.get("baseline") != "200580bc80c1ebd9f3b1ed58addc3d5723601946"
                or binding.get("candidate") != "bce7e35959fdfb821877163284f7627d1ebf906e"
                or binding.get("samples") != 8 or binding.get("iterations") != 128):
            raise ValueError("PRIMARY source or iteration binding mismatch")
        for revision in ("baseline", "candidate"):
            record = binding["binaries"][revision]
            if (record["sha256"] != manifest[f"primary_{revision}_elf_sha256"]
                    or before[revision]["executable"]["sha256"] != record["sha256"]
                    or before[revision]["executable"]["bytes"] != record["bytes"]):
                raise ValueError("PRIMARY runtime executable mismatch")
            inputs = before[revision]["inputs"]
            if inputs["harness_sha256"] != binding["harness_sha256"] or inputs["runner_sha256"] != binding["runner_sha256"]:
                raise ValueError("PRIMARY runtime harness or runner mismatch")
        records = json.loads(text("measured-binaries.json"))
        if records.get("status") != "RETAINED_ACTUAL_MEASURED_EXECUTABLES" or len(records.get("files", [])) != 6:
            raise ValueError("raw measured executable inventory incomplete")
        for field, member in (("environment_sha256", "environment.txt"),
                              ("initial_digests_sha256", "binary-sha256.txt"),
                              ("checker_summary_sha256", "summary.json")):
            if records.get(field) != hashlib.sha256(zf.read(member)).hexdigest():
                raise ValueError("raw internal manifest digest mismatch: " + field)
        actual_records = {row["path"]: row for row in records["files"]}
        if len(actual_records) != 6:
            raise ValueError("duplicate raw executable record")
        for name, digest in manifest["raw_elf_sha256"].items():
            row = actual_records.get("measured-binaries/" + name)
            revision, package = name.removesuffix(".test").split("-", 1)
            expected_source = manifest["base"] if revision == "baseline" else manifest["original_head"]
            expected_tree = (subprocess.check_output(["git", "rev-parse", manifest["base"] + "^{tree}"], cwd=root, text=True).strip()
                             if revision == "baseline" else manifest["original_tree"])
            expected_package = "./TreeDB" if package == "treedb" else "./TreeDB/" + package
            if (row is None or row["sha256"] != digest or row["source_sha"] != expected_source or row["source_tree"] != expected_tree
                    or row["package"] != expected_package or row["build_environment"] != {"GOWORK": "off"}
                    or row["build_argv"][:4] != ["go", "test", "-c", "-trimpath"]
                    or len(row["build_argv"]) != 8 or row["build_argv"][4:6] != ["-buildvcs=false", "-o"]
                    or not row["build_argv"][6].endswith("/" + name) or row["build_argv"][7] != expected_package):
                raise ValueError("raw source/build input mismatch: " + name)
        environment = text("environment.txt").splitlines()
        for required in ("runs=8", "benchtime=2s", "batch_write_benchtime=1000x", "cpuset=0",
                         "gomaxprocs=1", "max_regression_percent=5", "max_bytes_regression_percent=1",
                         "max_bytes_regression_absolute=64", "go version go1.26.10 linux/amd64"):
            if required not in environment:
                raise ValueError("raw original environment incomplete: " + required)
        return {"raw": results, "primary": measured, "original_measurement": "FAIL"}
