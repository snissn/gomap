"""C3-read artifact protocol. Construction only until independently reviewed."""
import datetime
import hashlib
import itertools
import json
import math
import os
import re
from pathlib import Path

SCHEMA = "gomap-c3-read-matched-v1"
PROFILES = {"no_wal_fast", "command_wal_relaxed", "command_wal_durable"}
MODES = {"append_only", "btree", "cow_btree"}
LAYOUTS = {"inline", "pointer"}
WORKLOADS = {"point", "group_all_versions", "concurrent"}
CONTROLS = {"GOROOT", "GOCACHE", "GOMODCACHE", "GOWORK", "GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOFLAGS", "TMPDIR"}
C3_FIXTURE = "TreeDB/mvcc/cow_c3_public_bench_test.go"

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
                GOPATH=str(cache.parent.parent), LC_ALL="C")

def validate_go_environment(observed, env):
    # Go's cfg.EnvFile reports the disabled GOENV=off setting as an empty
    # filename in `go env -json`; the actual process environment still is off.
    for key in ("GOROOT", "GOFLAGS", "GOWORK", "GOCACHE", "GOMODCACHE", "GOENV", "GOTOOLCHAIN", "GOPATH"):
        expected = "" if key == "GOENV" else env[key]
        need(observed[key] == expected, "actual go env mismatch " + key)
    need(observed["GOOS"] == "linux" and observed["GOARCH"] == "amd64", "actual build platform mismatch")

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

def config(path):
    c = json.loads(Path(path).read_text())
    need(c["schema"] == SCHEMA and c["status"] == "frozen-approved", "unfrozen configuration")
    need(isinstance(c["coordinator_acceptance"], str) and c["coordinator_acceptance"], "missing coordinator freeze acceptance")
    need(c["cycles"] == 3 and c["order"] == ["baseline", "candidate", "candidate", "baseline"], "requires three ABBA cycles")
    need(c["environment"]["GOMAXPROCS"] == "4" and c["environment"]["GOWORK"] == "off", "runtime control mismatch")
    need(set(c["environment"]) == CONTROLS, "missing/extra explicit environment controls")
    need(all(isinstance(v, str) for v in c["environment"].values()), "unresolved environment controls")
    need(c["environment"]["GOFLAGS"] == "", "GOFLAGS must be empty")
    process_environment(c["environment"])
    need(c["host"]["system"] == "Linux" and c["host"]["cpu_count"] >= 4, "Linux host contract")
    tmpdir = Path(c["environment"]["TMPDIR"])
    need(tmpdir.is_absolute() and str(tmpdir) == c["environment"]["TMPDIR"] and ".." not in tmpdir.parts, "unresolved TMPDIR")
    need(c["host"]["tmpdir"] == str(tmpdir) and type(c["host"]["tmpdir_device"]) is int and c["host"]["tmpdir_device"] >= 0, "unbound temporary database filesystem")
    for key in ("max_load1", "max_load5", "min_free_bytes"):
        need(math.isfinite(c["host"][key]) and c["host"][key] > 0, "missing host admission bound")
    noise = c["noise_policy"]
    for key in ("max_spread_fraction", "material_regression_fraction", "minimum_effect_fraction"):
        need(math.isfinite(noise[key]) and 0 < noise[key] < 1, "predeclare noise/regression bounds")
    need(noise["exclusions"] == "none; retain and stop on contamination", "no post-hoc exclusions")
    need(c["fixtures"] and c["comparison_metrics"] and c["cases"], "missing frozen fixture/metric/cases")
    need(isinstance(c["fixtures"], list) and len(c["fixtures"]) == 1
         and set(c["fixtures"][0]) == {"path", "sha256"}
         and c["fixtures"][0]["path"] == C3_FIXTURE
         and isinstance(c["fixtures"][0]["sha256"], str)
         and re.fullmatch(r"[0-9a-f]{64}", c["fixtures"][0]["sha256"]), "canonical C3 fixture required")
    need(set(c["variants"]) == {"baseline", "candidate"}, "two exact variants required")
    for variant in c["variants"].values():
        variant_git_ids(variant)
        variant_paths(variant)
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
        need(x["package"] == "github.com/snissn/gomap/TreeDB/mvcc", "unexpected benchmark package")
        need(type(x["iterations"]) is int and x["iterations"] > 0, "fixed iterations required")
        need(type(x["warmup_iterations"]) is int and x["warmup_iterations"] > 0, "separate warmup required")
        need(x["workload_contract"] and x["comparable_metrics"] and x["timed_scope"] and x["ack_contract"], "missing work/ACK comparability")
        need(set(x["rules"]) == {"baseline", "candidate"}, "variant metric rules missing")
        need({"wal_appends/op", "wal_syncs/op"} <= set(x["comparable_metrics"]), "WAL comparability missing")
        expected_wal = {"command_wal_durable": (1, 1), "command_wal_relaxed": (1, 0), "no_wal_fast": (0, 0)}[x["profile"]]
        for variant in ("baseline", "candidate"):
            for unit, expected in zip(("wal_appends/op", "wal_syncs/op"), expected_wal):
                rule = x["rules"][variant].get(unit)
                need(isinstance(rule, dict) and set(rule) == {"eq"} and type(rule["eq"]) is int
                     and rule["eq"] == expected, "profile WAL exact rule mismatch")
        units = set(x["rules"]["baseline"])
        need(units == set(x["rules"]["candidate"]), "unmatched metric sets")
        need({"ns/op", "B/op", "allocs/op"} <= units and x["latency_groups"], "allocation/latency metrics missing")
        for group in x["latency_groups"]:
            need({group + "_p" + str(p) + "_ns" for p in (50, 95, 99)} <= units, "missing active latency group")
        need(set(x["comparable_metrics"]) <= units and set(c["comparison_metrics"]) <= units, "unavailable comparison metric")
        need(set(x["comparison_metrics"]) <= units and all(v in ("lower", "higher") for v in x["comparison_metrics"].values()), "missing effect direction")
        for variant in ("baseline", "candidate"):
            for unit, rule in x["rules"][variant].items():
                need(set(rule) <= {"min", "max", "eq", "integer"} and ("eq" in rule or "min" in rule), "explicit zero/nonzero metric rule required")
                need(all(math.isfinite(v) and v >= 0 for k, v in rule.items() if k != "integer"), "invalid metric bounds")
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
        for group in case["latency_groups"]:
            need(metrics[group + "_p50_ns"] <= metrics[group + "_p95_ns"] <= metrics[group + "_p99_ns"], "invalid latency ordering")
        found.append({"iterations": count, "metrics": metrics})
    need(passed == 1 and len(found) == 1, "missing/extra benchmark row or PASS")
    need(set(metadata) == {"goos", "goarch", "pkg", "cpu"} and metadata["goos"] == "linux", "missing/unexpected benchmark metadata")
    need(metadata["pkg"] == case["package"], "unexpected benchmark package")
    return dict(found[0], metadata=metadata)
