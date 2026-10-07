"""C3-read artifact protocol. Construction only until independently reviewed."""
import datetime
import hashlib
import itertools
import json
import math
import os
import re
import stat
from pathlib import Path

SCHEMA = "gomap-c3-read-matched-v1"
PROFILES = {"no_wal_fast", "command_wal_relaxed", "command_wal_durable"}
MODES = {"append_only", "btree", "cow_btree"}
LAYOUTS = {"inline", "pointer"}
WORKLOADS = {"point", "group_all_versions", "concurrent"}
CONTROLS = {"GOROOT", "GOCACHE", "GOMODCACHE", "GOWORK", "GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOFLAGS", "TMPDIR"}
C3_PACKAGE = "github.com/snissn/gomap/TreeDB/mvcc/cowbench"
C3_PRODUCT = "github.com/snissn/gomap/TreeDB/mvcc"
C3_HARNESS_ROOTS = ("TreeDB/mvcc/cowbench/", "TreeDB/internal/cowbench/")
C3_FIXTURE = "TreeDB/mvcc/cowbench/cow_c3_public_bench_test.go"
C3_HARNESS_FILES = (C3_FIXTURE, "TreeDB/mvcc/cowbench/cow_public_admission_test.go",
                    "TreeDB/mvcc/cowbench/leak_test.go", "TreeDB/internal/cowbench/admission.go")
C4_PACKAGE = "github.com/snissn/gomap/TreeDB/mvcc/cowsustained"
HARNESS_PACKAGES = {"c3": C3_PACKAGE, "c4": C4_PACKAGE}
HARNESS_ROOTS = {"c3": C3_HARNESS_ROOTS,
                 "c4": ("TreeDB/mvcc/cowsustained/", "TreeDB/internal/cowbench/")}
HARNESS_FILES = {"c3": C3_HARNESS_FILES,
                 "c4": ("TreeDB/mvcc/cowsustained/cow_c4_public_bench_test.go",
                        "TreeDB/mvcc/cowsustained/cow_c4_public_fixture_test.go",
                        "TreeDB/mvcc/cowsustained/leak_test.go", "TreeDB/internal/cowbench/admission.go")}
C3_ITERATIONS, C3_WARMUP_ITERATIONS = 1024, 128

def variant_paths(variant, *, live=False):
    """Keep declared custody paths verbatim; offline packets need no live paths."""
    paths = {}
    for key in ("source", "binary", "manifest", "build_receipt"):
        value = variant[key]
        need(isinstance(value, str), "noncanonical variant path " + key)
        path = Path(value)
        need(path.is_absolute() and path.anchor == "/" and str(path) == value and ".." not in path.parts,
             "noncanonical variant path " + key)
        if live:
            need(path.exists() and not path.is_symlink() and path.resolve() == path,
                 "noncanonical live variant path " + key)
        paths[key] = path
    return paths

def variant_git_ids(variant):
    head, tree = variant["production_commit"], variant["production_git_tree"]
    need(isinstance(head, str) and isinstance(tree, str)
         and re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", head)
         and re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", tree)
         and len(head) == len(tree), "missing exact Git revision/tree")

def fixture_manifest(fixtures, ident):
    """Bind the declared workload bytes in each retained source inventory."""
    files = {item["path"]: item["sha256"] for item in ident["files"]}
    for fixture in fixtures:
        need(files.get(fixture["path"]) == fixture["sha256"],
             "fixture differs from frozen source: " + fixture["path"])

def validate_harness_fixtures(fixtures, suite="c3"):
    need(suite in HARNESS_ROOTS, "unknown ordinary build suite")
    message = "canonical " + suite.upper() + " fixture required"
    need(type(fixtures) is list and fixtures, message)
    paths = []
    for item in fixtures:
        need(type(item) is dict and set(item) == {"path", "sha256", "mode"}
             and type(item["path"]) is str
             and any(item["path"].startswith(root) for root in HARNESS_ROOTS[suite])
             and ".." not in Path(item["path"]).parts
             and str(Path(item["path"])) == item["path"]
             and type(item["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
             and type(item["mode"]) is int and item["mode"] in (0o644, 0o755),
             message)
        paths.append(item["path"])
    need(paths == sorted(set(paths)) and set(HARNESS_FILES[suite]) <= set(paths), message)

def harness_manifest(packages, closure, ident, source, environment, suite="c3"):
    """Bind all standalone harness inputs while allowing ordinary product deltas."""
    need(suite in HARNESS_ROOTS, "unknown ordinary build suite")
    roots = [p for p in packages if p.get("ImportPath") == C3_PRODUCT and not p.get("ForTest")]
    need(len(roots) == 1 and type(roots[0].get("Deps")) is list, "ordinary product dependency closure missing")
    products = set(roots[0]["Deps"]) | {C3_PRODUCT}
    product_packages = [p for p in packages if p.get("ImportPath") in products and not p.get("ForTest")]
    need({p["ImportPath"] for p in product_packages} == products, "incomplete ordinary product dependency closure")
    product_paths, _ = selected_input_paths(product_packages, source, environment)
    repo = {key[5:] for key in set(closure) - set(product_paths) if key.startswith("REPO/")}
    need(repo and all(any(path.startswith(root) for root in HARNESS_ROOTS[suite]) for path in repo),
         "undeclared repository test harness input")
    files = {f["path"]: f for f in ident["files"]}
    inventory = {path for path in files if any(path.startswith(root) for root in HARNESS_ROOTS[suite])}
    need(repo == inventory, "complete " + suite.upper() + " harness source/selected-input closure mismatch")
    result = []
    for path in sorted(inventory):
        item = closure["REPO/" + path]
        need(item["sha256"] == files[path]["sha256"] and item["mode"] in (0o644, 0o755)
             and ("100755" if item["mode"] & 0o111 else "100644") == files[path]["git_mode"],
             "harness input differs from Git source authority")
        result.append({"path": path, "sha256": item["sha256"], "mode": item["mode"]})
    validate_harness_fixtures(result, suite)
    return result

def matched_products(variants):
    """C3 compares two products; candidate-only construction has no such rule."""
    baseline, candidate = variants["baseline"], variants["candidate"]
    for key in ("production_commit", "production_git_tree", "source_tree_sha256", "binary_sha256"):
        need(baseline[key] != candidate[key], "matched products must have distinct " + key)
    left, right = variant_paths(baseline), variant_paths(candidate)
    for a in left.values():
        for b in right.values():
            need(not a.is_relative_to(b) and not b.is_relative_to(a), "matched products must have independent custody")
    a, b = left["build_receipt"].parent, right["build_receipt"].parent
    need(not a.is_relative_to(b) and not b.is_relative_to(a), "matched products must have independent build custody")

def validate_toolchain(value):
    need(set(value) == {"go_binary_sha256", "executables", "headers", "go_env"}
         and isinstance(value["go_binary_sha256"], str)
         and re.fullmatch(r"[0-9a-f]{64}", value["go_binary_sha256"]), "invalid Go toolchain inventory")
    records = value["executables"]
    need(isinstance(records, list) and records, "empty Go toolchain inventory")
    paths = []
    for item in records:
        need(set(item) == {"path", "sha256", "bytes", "mode"}, "invalid Go toolchain executable")
        path = Path(item["path"])
        need(not path.is_absolute() and str(path) == item["path"] and ".." not in path.parts
             and path.parts[:2] == ("pkg", "tool") and len(path.parts) >= 4,
             "invalid Go toolchain executable path")
        need(isinstance(item["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
             and type(item["bytes"]) is int and item["bytes"] > 0
             and type(item["mode"]) is int and 0 < item["mode"] <= 0o777 and item["mode"] & 0o111,
             "invalid Go toolchain executable identity")
        paths.append(item["path"])
    need(paths == sorted(set(paths)) and {"compile", "link", "asm"} <= {Path(p).name for p in paths},
         "incomplete/duplicate Go toolchain inventory")
    headers = value["headers"]
    need(isinstance(headers, list) and headers, "empty Go assembler header inventory")
    paths = []
    for item in headers:
        need(set(item) == {"path", "sha256", "bytes", "mode"}, "invalid Go assembler header")
        path = Path(item["path"])
        need(not path.is_absolute() and str(path) == item["path"] and ".." not in path.parts
             and path.parts[:2] == ("pkg", "include") and len(path.parts) >= 3,
             "invalid Go assembler header path")
        file_identity(item)
        paths.append(item["path"])
    need(paths == sorted(set(paths)) and {"pkg/include/textflag.h", "pkg/include/funcdata.h"} <= set(paths),
         "incomplete/duplicate Go assembler header inventory")
    go_env = value["go_env"]
    need(isinstance(go_env, dict) and set(go_env) == {"path", "sha256", "bytes", "mode"}
         and go_env["path"] == "go.env", "invalid GOROOT go.env inventory")
    file_identity(go_env)
    return digest(value)

def toolchain_inventory(goroot):
    """Observe every Go tool executable without executing or trusting the launcher."""
    root = Path(goroot)
    need(root.is_absolute() and not root.is_symlink() and root.resolve() == root, "noncanonical GOROOT")
    go, directory = root / "bin/go", root / "pkg/tool"
    need(go.is_file() and not go.is_symlink() and go.resolve() == go and go.stat().st_mode & 0o111,
         "invalid Go launcher custody")
    need(directory.is_dir() and not directory.is_symlink() and directory.resolve() == directory,
         "invalid Go tool directory custody")
    records = []
    for path in sorted(directory.rglob("*")):
        need(not path.is_symlink(), "symlink in Go tool directory")
        mode = path.stat().st_mode
        if stat.S_ISDIR(mode):
            continue
        need(stat.S_ISREG(mode), "nonregular Go tool input")
        if mode & 0o111:
            records.append({"path": path.relative_to(root).as_posix(), "sha256": sha(path),
                            "bytes": path.stat().st_size, "mode": stat.S_IMODE(mode)})
    directory = root / "pkg/include"
    need(directory.is_dir() and not directory.is_symlink() and directory.resolve() == directory,
         "invalid Go assembler include directory custody")
    headers = []
    for path in sorted(directory.rglob("*")):
        need(not path.is_symlink(), "symlink in Go assembler include directory")
        mode = path.stat().st_mode
        if stat.S_ISDIR(mode):
            continue
        need(stat.S_ISREG(mode), "nonregular Go assembler header")
        headers.append({"path": path.relative_to(root).as_posix(), "sha256": sha(path),
                        "bytes": path.stat().st_size, "mode": stat.S_IMODE(mode)})
    go_env = root / "go.env"
    need(go_env.is_file() and not go_env.is_symlink() and go_env.resolve() == go_env,
         "invalid GOROOT go.env custody")
    value = {"go_binary_sha256": sha(go), "executables": records, "headers": headers,
             "go_env": {"path": "go.env", "sha256": sha(go_env), "bytes": go_env.stat().st_size,
                        "mode": stat.S_IMODE(go_env.stat().st_mode)}}
    validate_toolchain(value)
    return value

def build_toolchain(build, frozen, inventory):
    need(build["go_version"] == frozen["go_version"]
         and build["go_binary_sha256"] == frozen["go_binary_sha256"], "toolchain mismatch")
    need(inventory["go_binary_sha256"] == frozen["go_binary_sha256"]
         and validate_toolchain(inventory) == build["toolchain_identity"] == frozen["toolchain_identity"],
         "Go toolchain inventory mismatch")

def validate_no_cgo(packages):
    need(packages and not any(package.get("CgoFiles") for package in packages), "compiled Cgo inputs forbidden")

def file_identity(item):
    need(isinstance(item.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
         and type(item.get("bytes")) is int and item["bytes"] >= 0
         and type(item.get("mode")) is int and 0 <= item["mode"] <= 0o777,
         "invalid persistent input identity")

def selected_input_paths(packages, source, environment):
    """Derive finite persistent inputs and recognized generated test-main inputs.

    Paths are retained for same-build custody; normalized keys separate repository
    product deltas from actual external compiler inputs and survive relocation.
    """
    source, goroot, cache = Path(source), Path(environment["GOROOT"]), Path(environment["GOCACHE"])
    files, generated = {}, []
    def add(key, path):
        path = Path(path)
        need(path.is_absolute() and ".." not in path.parts, "invalid selected input path")
        need(key not in files or files[key] == str(path), "conflicting selected input custody")
        files[key] = str(path)
    for package in packages:
        directory = Path(package["Dir"])
        module = package.get("Module", {})
        effective = module.get("Replace", module)
        external = not package.get("Standard") and not module.get("Main") and effective.get("Version")
        module_key = digest({k: effective[k] for k in ("Path", "Version", "Sum", "GoModSum") if k in effective})
        for category in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles", "SysoFiles", "EmbedFiles", "CompiledGoFiles"):
            for name in package.get(category, []):
                path = directory / name
                # go list -test emits a derived test main in GOCACHE. No ordinary
                # missing Go/header/assembly input is granted this exception.
                if (package.get("ImportPath", "").endswith(".test") and category in ("GoFiles", "CompiledGoFiles")
                        and Path(name).is_absolute() and path.is_relative_to(cache) and path.name.endswith("-d")):
                    generated.append({"package": package["ImportPath"], "category": category,
                                      "kind": "go-list-generated-test-main", "path": str(path)})
                    continue
                if path.is_relative_to(source):
                    key = "REPO/" + path.relative_to(source).as_posix()
                elif package.get("Standard") and path.is_relative_to(goroot):
                    key = "GOROOT/" + path.relative_to(goroot).as_posix()
                elif external and path.is_relative_to(Path(effective["Dir"])):
                    key = "MODULE/" + module_key + "/" + path.relative_to(Path(effective["Dir"])).as_posix()
                else:
                    need(False, "unrecognized persistent compiler input: " + str(path))
                add(key, path)
        if external:
            need(effective.get("GoMod"), "selected module metadata missing")
            add("MODULE_META/" + module_key + "/go.mod", effective["GoMod"])
    for name in ("go.mod", "go.sum"):
        add("REPO/" + name, source / name)
    need(files and any(key.startswith("GOROOT/") for key in files), "empty selected external input closure")
    return dict(sorted(files.items())), sorted(generated, key=lambda item: (item["package"], item["category"], item["path"]))

def selected_inputs(packages, source, environment):
    paths, generated = selected_input_paths(packages, source, environment)
    result = {}
    for key, name in paths.items():
        path = Path(name)
        need(path.is_file() and not path.is_symlink() and path.resolve() == path,
             "missing/nonregular/noncanonical persistent compiler input: " + name)
        result[key] = {"path": name, "sha256": sha(path), "bytes": path.stat().st_size,
                       "mode": stat.S_IMODE(path.stat().st_mode)}
    return result, generated

def build_inputs(build, frozen, packages, before, after, generated, source, ident=None):
    """Validate retained actual inputs offline, without reading original host paths."""
    paths, expected_generated = selected_input_paths(packages, source, build["environment"])
    need(set(before) == set(after) == set(paths), "selected persistent input closure mismatch")
    for key, name in paths.items():
        for closure in (before, after):
            item = closure[key]
            need(set(item) == {"path", "sha256", "bytes", "mode"} and item["path"] == name,
                 "selected persistent input custody mismatch")
            file_identity(item)
    need(before == after, "selected persistent inputs drift during build")
    need(generated == expected_generated, "unrecognized generated compiler inputs")
    external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")}
                for key, value in after.items() if not key.startswith("REPO/")}
    need(digest(external) == build["external_input_identity"] == frozen["external_input_identity"],
         "actual external compiler inputs differ")
    for field, artifact in (("compiled_inputs_before_sha256", "compiled_inputs_before"),
                            ("compiled_input_closure_sha256", "compiled_input_closure"),
                            ("generated_nonpersistent_inputs_sha256", "generated_nonpersistent_inputs")):
        need(build[field] == build["artifacts"][artifact]["sha256"], "unbound selected compiler input artifact")
    package = frozen.get("cases", [{}])[0].get("package")
    suites = {name: suite for suite, name in HARNESS_PACKAGES.items()}
    if package in suites:
        suite = suites[package]
        need(ident is not None, suite.upper() + " harness source authority missing")
        need(build.get("suite") == suite, "build/harness suite mismatch")
        harness = harness_manifest(packages, after, ident, source, build["environment"], suite)
        need(harness == frozen["fixtures"] and build.get("harness_input_identity") == digest(harness),
             "complete frozen " + suite.upper() + " harness mismatch")
    return external

def case_names(case):
    shape = "forced_pointer" if case["layout"] == "pointer" else "inline"
    leaf = "group_versions" if case["workload"] == "group_all_versions" else case["workload"]
    parts = (case["profile"], case["mode"], shape, leaf)
    return "-".join(parts), "/".join(("BenchmarkC3PublicReadAdmission",) + parts)

def need(value, message):
    if not value:
        raise ValueError(message)

def process_environment(controls):
    """Build the complete child environment without reading ambient settings."""
    need(set(controls) == CONTROLS and all(isinstance(v, str) for v in controls.values()),
         "missing/extra/non-string process controls")
    cache = Path(controls["GOMODCACHE"])
    need(cache.is_absolute() and str(cache) == controls["GOMODCACHE"] and ".." not in cache.parts
         and cache.parent.parent != Path("/"), "unresolved GOMODCACHE/GOPATH")
    return dict(controls, PATH=os.defpath, GOENV="off", GOTOOLCHAIN="local",
                GOPATH=str(cache.parent.parent), LC_ALL="C", CGO_ENABLED="0",
                GOAMD64="v1", GOEXPERIMENT="")

def validate_go_environment(observed, env):
    # Go's cfg.EnvFile reports the disabled GOENV=off setting as an empty
    # filename in `go env -json`; the actual process environment still is off.
    for key in ("GOROOT", "GOFLAGS", "GOWORK", "GOCACHE", "GOMODCACHE", "GOENV", "GOTOOLCHAIN", "GOPATH", "CGO_ENABLED", "GOAMD64", "GOEXPERIMENT"):
        expected = "" if key == "GOENV" else env[key]
        need(observed[key] == expected, "actual go env mismatch " + key)
    need(observed["GOOS"] == "linux" and observed["GOARCH"] == "amd64", "actual build platform mismatch")

def build_target(suite="c3"):
    need(suite in ("c3", "c4"), "unknown ordinary build suite")
    return "./" + HARNESS_PACKAGES[suite].removeprefix("github.com/snissn/gomap/")

def build_command(go, binary, suite="c3"):
    return [str(go), "test", "-c", "-trimpath", "-o", str(binary), build_target(suite)]

def module_command(go, suite="c3"):
    return [str(go), "list", "-compiled", "-deps", "-test", "-json", build_target(suite)]

def validate_build_command(build, go, binary, suite="c3"):
    need(type(build.get("exit_code")) is int and build["exit_code"] == 0
         and build.get("suite") == suite and build.get("command") == build_command(go, binary, suite), "actual ordinary build invocation mismatch")
    need(build.get("module_producer_command") == module_command(go, suite), "actual module producer invocation mismatch")

ACK_CONTRACT = "CommitRelaxed; durable WAL append/sync=1/1, relaxed=1/0, NoWAL=0/0; exact profile counts required"
TIMED_SCOPE = "actual calls, owned point output, borrowed complete EntryView scan, validation, clocks; seed/physical layout probes/stats/Close excluded"

def workload_contract(workload):
    need(workload in WORKLOADS, "unknown workload")
    return {"keys": ["c3-a", "c3-ab"], "seed_timestamps": [10, 20], "read_timestamp": 100,
            "value_bytes": 256, "value_byte": 99, "history_growth": False,
            "writers": 1, "point_readers": 0 if workload == "group_all_versions" else 1,
            "scan_readers": 0 if workload == "point" else 1,
            "writer_records_per_call": 1 if workload == "point" else 4,
            "concurrency": workload == "concurrent", "clock_calls_retained": True,
            "read_overlap_is_internal_preparation_proof": False, "physical_layout_records": 4,
            "layout_probes_outside_counter_boundaries": True}

def work_contract(case, phase):
    need(phase in ("warmup", "measured"), "unknown workload phase")
    return case["workload_contract"]

def workload_metrics(workload):
    contract = workload_contract(workload)
    point, scan = contract["point_readers"], contract["scan_readers"]
    return {"point_calls/op": point, "scan_calls/op": scan, "visited/op": 2 * scan,
            "output/op": point + 2 * scan}, ["writer"] + (["point"] if point else []) + (["scan"] if scan else [])

def metric_contract(profile, layout, workload, mode):
    need(profile in PROFILES and layout in LAYOUTS and mode in MODES, "unknown metric dimensions")
    work, latency = workload_metrics(workload)
    rules = {"ns/op": {"min": 1}, "B/op": {"min": 1}, "allocs/op": {"min": 1},
             "writer_ops/s": {"min": 0.000001}, "reader_ops/s": {"min": 0.000001},
             "readers_while_writer_active/op": {"min": 0, "max": 2} if workload == "concurrent" else {"eq": 0},
             "wal_appends/op": {"eq": 0 if profile == "no_wal_fast" else 1},
             "wal_syncs/op": {"eq": 1 if profile == "command_wal_durable" else 0},
             "close_ok": {"eq": 1}}
    rules.update({unit: {"eq": count} for unit, count in work.items()})
    rules.update({"layout_expected_records": {"eq": 4},
                  "ack_routing_before_ok": {"eq": 1}, "ack_routing_after_ok": {"eq": 1}})
    for phase in ("before", "after"):
        rules["layout_" + phase + "_inline"] = {"eq": 4 if layout == "inline" else 0}
        rules["layout_" + phase + "_pointer"] = {"eq": 4 if layout == "pointer" else 0}
        for name in ("wal_appends", "wal_syncs"):
            rules[name + "_" + phase] = {"eq": 0} if profile == "no_wal_fast" else {"min": 0, "integer": True}
    for name in ("snapshot_rotations/op", "rotated_shards/op", "enqueued_records/op"):
        rules[name] = {"eq": 0} if mode == "cow_btree" else {"min": 0}
    comparisons = {"ns/op": "lower", "B/op": "lower", "allocs/op": "lower",
                   "writer_ops/s": "higher", "reader_ops/s": "higher"}
    if workload == "concurrent":
        for group in ("point", "scan"):
            rules[group + "_phase_elapsed_ns"] = {"min": 1}
            rules[group + "_phase_ops/s"] = {"min": 0.000001}
            comparisons[group + "_phase_elapsed_ns"] = "lower"
            comparisons[group + "_phase_ops/s"] = "higher"
    for group in latency:
        for p in (50, 95, 99):
            name = f"{group}_p{p}_ns"
            rules[name] = {"min": 1}
            comparisons[name] = "lower"
    if mode == "cow_btree":
        for name in ("capture_calls_total", "prepare_calls_total", "publications_total"):
            rules["cow_" + name + "/op"] = {"min": 1}
        for name in ("total_bytes", "peak_bytes", "external_leases"):
            rules["cow_end_" + name] = {"min": 1, "integer": True}
        rules["cow_end_views"] = {"eq": 0}
        rules["cow_end_active_cuts"] = {"eq": 1}
    return rules, comparisons

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()

def identity(path):
    # Reuses the C2 sorted sha256 + two spaces + relative path identity primitive;
    # supports both list manifests and the root's sparse {path:{sha256,bytes}} form.
    data = json.loads(Path(path).read_text())
    files = data["files"]
    if isinstance(files, dict):
        files = [{"path": p, "sha256": v["sha256"] if isinstance(v, dict) else v}
                 for p, v in sorted(files.items())]
    need(files and len({f["path"] for f in files}) == len(files), "empty/duplicate source paths")
    for item in files:
        p = Path(item["path"])
        need(not p.is_absolute() and ".." not in p.parts and str(p) == item["path"], "unsafe source path")
        need(re.fullmatch(r"[0-9a-f]{64}", item["sha256"]), "invalid source hash")
    encoded = "".join(f'{f["sha256"]}  {f["path"]}\n' for f in sorted(files, key=lambda x: x["path"]))
    calculated = hashlib.sha256(encoded.encode()).hexdigest()
    # Other historical manifest digests may use another derivation; bind them
    # separately instead of silently treating them as this tree digest.
    return {"original_manifest": data, "manifest_sha256": sha(path), "files": files,
            "tree_sha256": calculated, "derivation": "sorted sha256 + two spaces + path + newline"}

def drift(source, ident):
    source = Path(source)
    expected = {f["path"] for f in ident["files"]}
    actual = {p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file() or p.is_symlink()}
    wrong = []
    for item in ident["files"]:
        p = source / item["path"]
        wrong_mode = p.is_file() and "git_mode" in item and ("100755" if p.stat().st_mode & 0o111 else "100644") != item["git_mode"]
        if p.is_symlink() or not p.is_file() or sha(p) != item["sha256"] or wrong_mode:
            wrong.append(item["path"])
    return wrong + ["EXTRA:" + p for p in sorted(actual - expected)]

def finite_number(value):
    # JSON booleans are integers in Python; admission must keep them distinct.
    # Arbitrarily large JSON integers are finite without a float conversion.
    return type(value) is int or (type(value) is float and math.isfinite(value))

def validate_cpu_affinity(mask):
    need(type(mask) is list and len(mask) >= 4
         and all(type(cpu) is int and cpu >= 0 for cpu in mask)
         and mask == sorted(set(mask)), "CPU affinity must be sorted distinct nonnegative integers with at least four CPUs")
    return mask

def cpu_affinity():
    need(callable(getattr(os, "sched_getaffinity", None)), "CPU affinity observation unavailable")
    try:
        mask = sorted(os.sched_getaffinity(0))
    except OSError as error:
        raise ValueError("CPU affinity observation unavailable") from error
    return validate_cpu_affinity(mask)

def config(path):
    c = json.loads(Path(path).read_text())
    need(c["schema"] == SCHEMA and c["status"] == "frozen-approved", "unfrozen configuration")
    need(isinstance(c["coordinator_acceptance"], str) and c["coordinator_acceptance"], "missing coordinator freeze acceptance")
    need(type(c["cycles"]) is int and c["cycles"] == 3 and c["order"] == ["baseline", "candidate", "candidate", "baseline"], "requires three ABBA cycles")
    need(type(c["timeout_seconds"]) is int and c["timeout_seconds"] > 0, "positive integer timeout required")
    need(c["environment"]["GOMAXPROCS"] == "4" and c["environment"]["GOWORK"] == "off", "runtime control mismatch")
    need(set(c["environment"]) == CONTROLS, "missing/extra explicit environment controls")
    need(all(isinstance(v, str) for v in c["environment"].values()), "unresolved environment controls")
    need(c["environment"]["GOFLAGS"] == "", "GOFLAGS must be empty")
    process_environment(c["environment"])
    need(isinstance(c["go_version"], str) and c["go_version"], "missing frozen Go version")
    for key in ("go_binary_sha256", "toolchain_identity", "external_input_identity"):
        need(isinstance(c[key], str) and re.fullmatch(r"[0-9a-f]{64}", c[key]), "missing frozen " + key)
    need(c["go_binary"] == str(Path(c["environment"]["GOROOT"]) / "bin/go"), "unbound Go launcher path")
    need(c["host"]["system"] == "Linux" and type(c["host"]["cpu_count"]) is int and c["host"]["cpu_count"] >= 4, "Linux host contract")
    validate_cpu_affinity(c["host"].get("cpu_affinity"))
    tmpdir = Path(c["environment"]["TMPDIR"])
    need(tmpdir.is_absolute() and str(tmpdir) == c["environment"]["TMPDIR"] and ".." not in tmpdir.parts, "unresolved TMPDIR")
    need(c["host"]["tmpdir"] == str(tmpdir) and type(c["host"]["tmpdir_device"]) is int and c["host"]["tmpdir_device"] >= 0, "unbound temporary database filesystem")
    for key in ("max_load1", "max_load5"):
        need(finite_number(c["host"][key]) and c["host"][key] > 0, "missing host admission bound")
    need(type(c["host"]["min_free_bytes"]) is int and c["host"]["min_free_bytes"] > 0, "missing host admission bound")
    noise = c["noise_policy"]
    for key in ("max_spread_fraction", "material_regression_fraction", "minimum_effect_fraction"):
        need(finite_number(noise[key]) and 0 < noise[key] < 1, "predeclare noise/regression bounds")
    need(noise["exclusions"] == "none; retain and stop on contamination", "no post-hoc exclusions")
    need(c["fixtures"] and c["comparison_metrics"] and c["cases"], "missing frozen fixture/metric/cases")
    validate_harness_fixtures(c["fixtures"])
    need(set(c["variants"]) == {"baseline", "candidate"}, "two exact variants required")
    for variant in c["variants"].values():
        variant_git_ids(variant)
        variant_paths(variant)
        for key in ("binary_sha256", "source_tree_sha256"):
            need(isinstance(variant[key], str) and re.fullmatch(r"[0-9a-f]{64}", variant[key]),
                 "missing exact product hash " + key)
    matched_products(c["variants"])
    cases = c["cases"]
    need(len({x["id"] for x in cases}) == len(cases), "duplicate case ids")
    coverage = {(x["profile"], x["layout"], x["workload"], x["mode"]) for x in cases}
    need(coverage == set(itertools.product(PROFILES, LAYOUTS, WORKLOADS, MODES)), "incomplete/extra matrix")
    need(len(cases) == len(coverage), "duplicate matrix cell")
    need(len({x["benchmark"] for x in cases}) == len(cases), "duplicate benchmark leaves")
    for x in cases:
        need(re.fullmatch(r"[A-Za-z0-9_.-]+", x["id"]), "unsafe case id")
        expected_id, expected_benchmark = case_names(x)
        need(x["id"] == expected_id and x["benchmark"] == expected_benchmark, "benchmark/id does not match case dimensions")
        need(x["package"] == C3_PACKAGE, "unexpected benchmark package")
        need(type(x["iterations"]) is int and x["iterations"] == C3_ITERATIONS, "canonical measured iterations required")
        need(type(x["warmup_iterations"]) is int and x["warmup_iterations"] == C3_WARMUP_ITERATIONS, "canonical warmup iterations required")
        need(x["workload_contract"] and x["comparable_metrics"] and x["timed_scope"] and x["ack_contract"], "missing work/ACK comparability")
        need(set(x["rules"]) == {"baseline", "candidate"}, "variant metric rules missing")
        need({"wal_appends/op", "wal_syncs/op"} <= set(x["comparable_metrics"]), "WAL comparability missing")
        expected_wal = {"command_wal_durable": (1, 1), "command_wal_relaxed": (1, 0), "no_wal_fast": (0, 0)}[x["profile"]]
        for variant in ("baseline", "candidate"):
            for unit, expected in zip(("wal_appends/op", "wal_syncs/op"), expected_wal):
                rule = x["rules"][variant].get(unit)
                need(isinstance(rule, dict) and set(rule) == {"eq"} and type(rule["eq"]) is int
                     and rule["eq"] == expected, "profile WAL exact rule mismatch")
        # Canonical JSON equality is type-sensitive (True must not equal 1),
        # recursively binds list elements, and rejects missing/extra fields.
        need(digest(x["workload_contract"]) == digest(workload_contract(x["workload"])),
             "literal workload contract mismatch")
        expected_work, expected_latency = workload_metrics(x["workload"])
        need(x["latency_groups"] == expected_latency, "literal workload latency groups mismatch")
        need(x["ack_contract"] == ACK_CONTRACT and x["timed_scope"] == TIMED_SCOPE,
             "literal workload ACK/timed scope mismatch")
        need(set(expected_work) <= set(x["comparable_metrics"]), "work-count comparability missing")
        for variant in ("baseline", "candidate"):
            for unit, expected in expected_work.items():
                rule = x["rules"][variant].get(unit)
                need(isinstance(rule, dict) and set(rule) == {"eq"} and type(rule["eq"]) is int
                     and rule["eq"] == expected, "literal workload count rule mismatch")
        for variant in ("baseline", "candidate"):
            expected_layout = {"layout_expected_records": 4, "ack_routing_before_ok": 1, "ack_routing_after_ok": 1}
            for phase in ("before", "after"):
                expected_layout["layout_" + phase + "_inline"] = 4 if x["layout"] == "inline" else 0
                expected_layout["layout_" + phase + "_pointer"] = 4 if x["layout"] == "pointer" else 0
                for counter in ("wal_appends", "wal_syncs"):
                    expected = {"eq": 0} if x["profile"] == "no_wal_fast" else {"min": 0, "integer": True}
                    rule = x["rules"][variant].get(counter + "_" + phase)
                    need(isinstance(rule, dict) and rule == expected
                         and (type(rule.get("eq")) is int if "eq" in expected
                              else type(rule.get("min")) is int and rule.get("integer") is True),
                         "actual WAL boundary rule mismatch")
            for unit, expected in expected_layout.items():
                rule = x["rules"][variant].get(unit)
                need(isinstance(rule, dict) and set(rule) == {"eq"} and type(rule["eq"]) is int and rule["eq"] == expected,
                     "actual layout/routing exact rule mismatch")
        units = set(x["rules"]["baseline"])
        need(units == set(x["rules"]["candidate"]), "unmatched metric sets")
        need({"ns/op", "B/op", "allocs/op"} <= units and x["latency_groups"], "allocation/latency metrics missing")
        for group in x["latency_groups"]:
            need({group + "_p" + str(p) + "_ns" for p in (50, 95, 99)} <= units, "missing active latency group")
        need(set(x["comparable_metrics"]) <= units and set(c["comparison_metrics"]) <= units, "unavailable comparison metric")
        need(set(c["comparison_metrics"]) <= set(x["comparison_metrics"]), "globally required comparison metric missing from case")
        need(set(x["comparison_metrics"]) <= units and all(v in ("lower", "higher") for v in x["comparison_metrics"].values()), "missing effect direction")
        for variant in ("baseline", "candidate"):
            for unit, rule in x["rules"][variant].items():
                need(set(rule) <= {"min", "max", "eq", "integer"} and ("eq" in rule or "min" in rule), "explicit zero/nonzero metric rule required")
                need(all(finite_number(v) and v >= 0 for k, v in rule.items() if k != "integer"), "invalid metric bounds")
        canonical_rules, canonical_directions = metric_contract(x["profile"], x["layout"], x["workload"], x["mode"])
        need(digest(x["comparison_metrics"]) == digest(canonical_directions), "canonical effect directions mismatch")
        for variant in ("baseline", "candidate"):
            need(digest(x["rules"][variant]) == digest(canonical_rules), "canonical operational rules mismatch")
    return c

def schedule(c):
    for case in c["cases"]:
        for variant in ("baseline", "candidate"):
            yield {"case": case["id"], "phase": "warmup", "cycle": 0, "slot": 0, "variant": variant}
        for cycle in range(1, 4):
            for slot, variant in enumerate(c["order"], 1):
                yield {"case": case["id"], "phase": "measured", "cycle": cycle, "slot": slot, "variant": variant}

def label(item):
    return f'{item["case"]}-{item["phase"]}-c{item["cycle"]}-s{item["slot"]}-{item["variant"]}'

def command(binary, case, item, timeout):
    # Go matches slash-separated benchmark components separately.
    pattern = "/".join("^" + re.escape(p) + "$" for p in case["benchmark"].split("/"))
    count = case["warmup_iterations"] if item["phase"] == "warmup" else case["iterations"]
    return [str(binary), "-test.run=^$", "-test.bench=" + pattern, f"-test.benchtime={count}x",
            "-test.benchmem", "-test.count=1", f"-test.timeout={timeout}s"]

def row(stdout, stderr, case, variant, phase):
    need(not Path(stderr).read_bytes(), "unexpected stderr")
    found, metadata, passed = [], {}, 0
    for line in Path(stdout).read_text().splitlines():
        if not line.strip():
            continue
        if line == "PASS":
            passed += 1
            continue
        matched = re.fullmatch(r"(goos|goarch|pkg|cpu): (.+)", line)
        if matched:
            need(matched[1] not in metadata, "duplicate Go metadata")
            metadata[matched[1]] = matched[2]
            continue
        matched = re.fullmatch(re.escape(case["benchmark"]) + r"-4\s+(\d+)\s+(.+)", line)
        need(matched is not None, "unexpected stdout: " + line[:160])
        tokens = matched[2].split()
        need(len(tokens) % 2 == 0, "malformed benchmark metric pairs")
        metrics = {}
        for number, unit in zip(tokens[::2], tokens[1::2]):
            need(unit not in metrics, "duplicate metric " + unit)
            value = float(number)
            need(math.isfinite(value) and value >= 0, "invalid metric " + unit)
            metrics[unit] = value
        rules = case["rules"][variant]
        need(set(metrics) == set(rules), "missing/extra metrics")
        for unit, rule in rules.items():
            value = metrics[unit]
            need("min" not in rule or value >= rule["min"], "underflow/zero " + unit)
            need("max" not in rule or value <= rule["max"], "overflow " + unit)
            need("eq" not in rule or value == rule["eq"], "unexpected counter " + unit)
            need(not rule.get("integer") or value.is_integer(), "noninteger counter " + unit)
        count = case["warmup_iterations"] if phase == "warmup" else case["iterations"]
        need(int(matched[1]) == count, "unexpected benchmark work count")
        for counter, unit in (("wal_appends", "wal_appends/op"), ("wal_syncs", "wal_syncs/op")):
            before, after = metrics[counter + "_before"], metrics[counter + "_after"]
            need(after >= before and after - before == metrics[unit] * count,
                 "actual WAL boundary/delta mismatch")
        for group in case["latency_groups"]:
            need(metrics[group + "_p50_ns"] <= metrics[group + "_p95_ns"] <= metrics[group + "_p99_ns"], "invalid latency ordering")
        found.append({"iterations": count, "metrics": metrics})
    need(passed == 1 and len(found) == 1, "missing/extra benchmark row or PASS")
    need(set(metadata) == {"goos", "goarch", "pkg", "cpu"} and metadata["goos"] == "linux", "missing/unexpected benchmark metadata")
    need(metadata["pkg"] == case["package"], "unexpected benchmark package")
    return dict(found[0], metadata=metadata)
