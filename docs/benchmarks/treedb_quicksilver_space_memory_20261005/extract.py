#!/usr/bin/env python3
"""Extract this fixed provisional campaign; never collect or open a database."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median

HERE = Path(__file__).resolve().parent
INPUTS_SHA256 = "493763cef652bb341c2d3ab33d44447fa9638f68f9e121ff73fa01b7ff85438c"
BASE = "maintenance/maintenance-baseline-3m/"
FAILED = "maintenance/maintenance-baseline-3m-batch8192/"
DIAG = "maintenance/maintenance-3m-diagnostic/"
REBOUND = "maintenance/maintenance-3m-diagnostic-rebound/"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def verify_inputs(contract_bytes, raw):
    # Authenticate the contract and complete path inventory before parsing receipts.
    require(sha(contract_bytes) == INPUTS_SHA256, "fixed input contract changed")
    contract = json.loads(contract_bytes)
    require(set(raw) == set(contract["raw_files"]), "raw inventory differs")
    for name, digest in contract["raw_files"].items():
        require(sha(raw[name]) == digest, "raw bytes changed: " + name)
    for name, digest in contract["compressed_originals"].items():
        raw[name] = gzip.decompress(raw[name + ".gz"])
        require(sha(raw[name]) == digest, "original bytes changed: " + name)
    require(contract["publication_status"] == "PROVISIONAL" and contract["pending"],
            "this packet cannot declare final completion")
    return contract


def load(folder=HERE):
    raw_root = folder / "raw"
    require(not raw_root.is_symlink(), "raw root is a symlink")
    raw = {}
    for path in sorted(raw_root.rglob("*")):
        require(not path.is_symlink(), "raw symlink: " + str(path))
        if path.is_file():
            raw[path.relative_to(raw_root).as_posix()] = path.read_bytes()
    contract = verify_inputs((folder / "inputs.json").read_bytes(), raw)
    return contract, raw


def oracle(value, contract):
    require(value.get("verification_only") is True, "oracle is not verification-only")
    require(value.get("engine") == "treedb", "wrong oracle engine")
    for key, expected in contract["fixture"].items():
        require(value["config"].get(key) == expected, "oracle fixture differs: " + key)
    for key in ["verified_keys", "verified_misses"]:
        require(value.get(key) == contract[key], "incomplete oracle: " + key)
    return {key: value[key] for key in ["verified_keys", "verified_misses"]}


def census(value):
    domains = {name: {"apparent": 0, "allocated": 0} for name in
               ["dictionary", "outer_leaf", "user_value", "index", "metadata", "wal"]}
    seen = set()
    for row in value["files"]:
        name = row["path"]
        require(name not in seen and not Path(name).is_absolute() and ".." not in Path(name).parts,
                "invalid census path")
        seen.add(name)
        require(row["kind"] in domains, "unknown census domain")
        for key in ["apparent", "allocated"]:
            require(type(row[key]) is int and row[key] >= 0, "invalid census bytes")
            domains[row["kind"]][key] += row[key]
    out = {"label": value["label"], "domains": domains}
    for key in ["apparent", "allocated"]:
        total = sum(d[key] for d in domains.values())
        require(total == value[key], "census total differs")
        without_wal = total - domains["wal"][key]
        require(without_wal == value[key + "_excluding_wal"], "WAL-excluded census differs")
        out[key] = total
        out[key + "_excluding_wal"] = without_wal
    return out


def time_summary(text):
    rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
    elapsed = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)", text)
    seconds = None
    if elapsed:
        seconds = 0.0
        for part in elapsed[1].split(":"):
            seconds = seconds * 60 + float(part)
    return {"time_elapsed_seconds": seconds,
            "kernel_process_rss_hwm_bytes": int(rss[1]) * 1024 if rss else None}


def command_summary(command, stderr):
    seconds = command["finished"] - command["started"]
    require(math.isfinite(seconds) and seconds >= 0, "invalid command interval")
    status = "timed_out" if command.get("timed_out") else (
        "completed" if command["rc"] == 0 else "failed")
    samples = command.get("disk_samples", [])
    for sample in samples:
        require(command["started"] <= sample["time"] <= command["finished"], "disk sample outside command")
    out = {"status": status, "rc": command["rc"], "command": command["command"],
           "receipt_elapsed_seconds": seconds, "disk_sample_count": len(samples),
           "sampled_disk_hwm_including_wal": {
               key: max((s[key] for s in samples), default=None) for key in ["apparent", "allocated"]}}
    out.update(time_summary(stderr))
    return out


def rss_summary(rows, run=None, binary="unified-bench-baseline"):
    require(rows, "empty RSS observation")
    pids = {row["pid"] for row in rows}
    require(len(pids) == 1, "RSS summary requires one observed process")
    for row in rows:
        require(row["exe"].endswith("/" + binary), "unexpected RSS executable")
        if run:
            require(run["started"] <= row["time"] <= run["finished"], "RSS sample outside run")
        mem = row["memory"]
        require(all(type(v) is int and v >= 0 for v in mem.values()), "invalid RSS value")
        require(mem["VmHWM"] >= mem["VmRSS"], "RSS exceeds process HWM")
    peak = max(rows, key=lambda row: row["memory"]["VmRSS"])
    return {"pid": next(iter(pids)), "samples": len(rows),
            "observed_kernel_hwm_bytes": max(row["memory"]["VmHWM"] for row in rows),
            "sampled_rss_peak_bytes": peak["memory"]["VmRSS"],
            "peak_sample": peak,
            "independent_sampled_component_maxima": {
                key: max(row["memory"][key] for row in rows)
                for key in ["RssAnon", "RssFile", "RssShmem", "VmSwap"]}}


def public_matched(contract, raw):
    prefix = "public-matched/"
    bundle = prefix + "offsets-3m-matched/"
    read = lambda name: json.loads(raw[prefix + name])
    fixed = contract["public_matched"]
    manifest = read("qualified-manifest-offsets.json")
    require(raw[prefix + "qualified-manifest-offsets.json"] == raw[bundle + "manifest.json"],
            "public manifest copies differ")
    require(manifest["provisional"] is True and manifest["sources"] == fixed["sources"],
            "public frozen source identity differs")
    plan_bytes = raw[bundle + "plan.json"]
    require(sha(plan_bytes) == fixed["plan_sha256"] and
            plan_bytes == raw["pending-plans/offsets-3m-matched-plan.json"], "public plan differs")
    plan = json.loads(plan_bytes)
    require(plan["repeats"] == 3 and len(plan["cells"]) == 6 and
            len({c["label"] for c in plan["cells"]}) == 6, "public matrix dimensions differ")
    receipts = {}
    for name, receipt in manifest["receipts"].items():
        data = raw[prefix + receipt["path"]]
        require(sha(data) == receipt["sha256"] and data == raw[bundle + name + "-receipt.json"],
                "public receipt binding differs: " + name)
        receipts[name] = json.loads(data)
    observations = [json.loads(line) for line in raw[prefix + "offsets-3m-matched-memory.jsonl"].splitlines()]
    observed_count = 0
    for name, identity in fixed["sources"].items():
        source, build = receipts["source"][name], receipts["build"][name]
        require(source["head"] == build["head"] == identity["head"] and
                source["producer_script_sha256"] == fixed["collector_sha256"], "public producer differs")
        require(source["compiled_project_inputs"] and all(source["files"].get(p) == digest
                for p, digest in source["compiled_project_inputs"].items()), "public compiled source differs")
        require(build["binaries"][identity["binary"]]["binary_sha256"] == identity["binary_sha256"]
                and build["go_version"] == "go version go1.26.3 linux/amd64\n", "public build differs")
        require(all(b["rc"] == 0 and b["head"] == identity["head"] for b in build["builds"].values())
                and all(build["environment"][k] == manifest["build_env"][k]
                        for k in build["environment"]), "public build environment/status differs")
        require(receipts["native"][name]["libraries"] == manifest["libraries"], "public native receipt differs")
        require(receipts["runner"][name]["shared_host"] is True, "public runner qualification differs")
    phase_names = ["quicksilver_" + n for n in ["hits", "misses", "mixed", "concurrent"]]
    records = []
    for repeat in range(1, 4):
        for cell in plan["cells"]:
            path = bundle + str(repeat) + "-" + cell["label"] + "/"
            run = json.loads(raw[path + "run.json"])
            require(run["cell"] == cell and run["repeat"] == repeat and run["validated"] is True
                    and run["rc"] == 0 and cell["profiled"] is False, "public row identity/status differs")
            identity = fixed["sources"][cell["source"]]
            require(run["source"] == identity and run["manifest_sha256"] == sha(raw[bundle + "manifest.json"])
                    and run["plan_sha256"] == fixed["plan_sha256"]
                    and run["collector_sha256"] == fixed["collector_sha256"]
                    and run["env"] == fixed["environment"], "public row provenance differs")
            binary = "/mnt/fast4tb/quicksilver-space-memory-20261005/bin/" + identity["binary"]
            command = [binary, "-suite", "quicksilver", "-dbs", "treedb", "-profile", cell["profile"],
                       "-quicksilver-case", cell["case"], "-keys", str(cell["keys"]),
                       "-read-workers", str(cell["workers"]), "-quicksilver-reads", str(cell["reads"]),
                       "-quicksilver-updates", str(cell["updates"]), "-quicksilver-duration", cell["duration"],
                       "-quicksilver-read-batch", "64", "-quicksilver-commit", "auto", "-max-wall", "30m",
                       "-seed", str(cell["seed"]), "-quicksilver-mixture", cell["mixture"],
                       "-quicksilver-working-set", cell["working_set"], "-quicksilver-miss-percent", str(cell["miss_percent"])]
            require(run["command"] == command, "public command differs")
            resolution = run["native_resolution"]
            libraries = {p:v["sha256"] for p,v in manifest["libraries"].items()}
            ldd = raw[path + "ldd.stdout.txt"].decode()
            resolved_paths = set(re.findall(r"(?:=>\s+|^\s*)(/\S+)\s+\(", ldd, re.M))
            require(resolution["command"] == ["/usr/bin/ldd", binary] and resolution["rc"] == 0
                    and resolution["linkage"] == "dynamic" and resolution["libraries"] == libraries
                    and resolved_paths == set(libraries) and raw[path + "ldd.stderr.txt"] == b""
                    and resolution["loader_env"] == {"LD_LIBRARY_PATH": run["env"]["LD_LIBRARY_PATH"],
                                                     "LD_PRELOAD": "", "LD_AUDIT": ""}, "public loader proof differs")
            outputs = json.loads(raw[path + "stdout.json"])
            require(len(outputs) == 1, "public result inventory differs")
            value = outputs[0]
            require(value["engine"] == "treedb" and value["profiled"] is False and value["gomaxprocs"] == 12,
                    "public result configuration differs")
            config = dict(contract["fixture"], mixture=cell["mixture"], working_set=cell["working_set"],
                          miss_percent=cell["miss_percent"])
            require(all(value["config"].get(k) == v for k,v in config.items()), "public fixture differs")
            require(value["initial_verified_keys"] == value["verified_keys"] == 3000000
                    and value["initial_verified_misses"] == 6030000 and value["verified_misses"] == 6040000
                    and value["updated_keys"] == 40000
                    and value["mutations"] == {"updates":10000,"deletes":10000,"inserts":10000,
                                                "overwrite_targets":10000,"overwrite_sets":40000},
                    "public fixture oracle/mutations incomplete")
            require([p["name"] for p in value["phases"]] == phase_names, "public phase inventory differs")
            phases = []
            for phase in value["phases"]:
                require(all(math.isfinite(phase[k]) and phase[k] >= 0 for k in
                            ["ops", "seconds", "composition_seconds", "ops_per_sec", "p50_us", "p95_us",
                             "p99_us", "p999_us", "max_us", "process_allocated_bytes", "process_bytes_per_op"])
                        and 0 < phase["p99_us"] <= phase["p999_us"] <= phase["max_us"], "public phase metric invalid")
                require(phase["ops"] > 0 and phase["seconds"] > 0 and
                        math.isclose(phase["ops"] / phase["seconds"], phase["ops_per_sec"], rel_tol=1e-12)
                        and math.isclose(phase["process_allocated_bytes"] / phase["ops"],
                                         phase["process_bytes_per_op"], rel_tol=1e-12), "public phase ratio differs")
                require(phase["requested_present"] + phase["requested_absent"] == phase["ops"],
                        "public request count differs")
                if phase["name"] != "quicksilver_concurrent":
                    require(phase["ops"] == 6000000 and phase["observed_hits"] == phase["requested_present"],
                            "public read identity/count differs")
                phases.append({k:phase[k] for k in ["name", "ops", "seconds", "composition_seconds", "ops_per_sec",
                    "p50_us", "p95_us", "p99_us", "p999_us", "max_us", "process_allocated_bytes",
                    "process_bytes_per_op", "process_heap_alloc_before", "process_heap_alloc_after",
                    "process_gc_cycles", "requested_present", "requested_absent", "observed_hits"]})
            require([r["phase"] for r in run["miss_ratio_validation"]] == phase_names[2:], "miss screening inventory differs")
            for screen, phase in zip(run["miss_ratio_validation"], phases[2:]):
                expected = phase["ops"] * cell["miss_percent"] / 100
                tolerance = math.sqrt(phase["ops"] * math.log(2 / 1e-12) / 2)
                require(screen["alpha"] == 1e-12 and screen["configured_miss_percent"] == cell["miss_percent"]
                        and all(screen[k] == phase[k] for k in ["ops", "requested_present", "requested_absent"])
                        and math.isclose(screen["expected_absent"], expected, rel_tol=1e-12)
                        and math.isclose(screen["tolerance_requests"], tolerance, rel_tol=1e-12)
                        and abs(phase["requested_absent"] - expected) <= tolerance, "public miss screening differs")
            require(run["finished"] > run["started"] >=
                    receipts["build"][cell["source"]]["builds"]["native-build"]["finished"], "public time ordering differs")
            require(len(value["update_batch_ms"]) == 40 and len(value["checkpoint_ms"]) == 4,
                    "public maintenance guardrail inventory differs")
            guard = {k:value[k] for k in ["load_seconds", "initial_checkpoint_ms", "reopen_ms",
                     "final_checkpoint_ms", "final_reopen_ms", "update_batch_ms", "checkpoint_ms"]}
            guard.update(time_summary(raw[path + "stderr.log"].decode()))
            require(all(guard[k] is not None for k in ["time_elapsed_seconds", "kernel_process_rss_hwm_bytes"]),
                    "public elapsed/RSS summary missing")
            samples = [row for row in observations if run["started"] <= row["time"] <= run["finished"]
                       and row["exe"] == binary]
            observed_count += len(samples)
            observed_rss = rss_summary(samples, run, identity["binary"])
            file_totals = {}
            for boundary in ["initial_files", "final_files"]:
                domains = dict.fromkeys(["dictionary", "outer_leaf", "user_value", "index", "metadata", "wal"], 0)
                for filename, size in value[boundary].items():
                    require(not Path(filename).is_absolute() and ".." not in Path(filename).parts
                            and type(size) is int and size >= 0, "public file census differs")
                    domain = ("wal" if "/wal/" in filename else "dictionary" if filename.startswith("dictdb/")
                              else "outer_leaf" if "/leaf_vlog/" in filename else "user_value" if "/value_vlog/" in filename
                              else "index" if filename.endswith("/index.db") else "metadata")
                    domains[domain] += size
                file_totals[boundary] = {"domains":domains,"apparent_excluding_wal":sum(domains.values())-domains["wal"]}
            records.append({"repeat":repeat,"cell":cell,"command":command,"source":identity,
                "started":run["started"],"finished":run["finished"],"load_before":run["load_before"],
                "load_after":run["load_after"],"phases":phases,"guardrails":guard,"rss":observed_rss,
                "runtime_apparent_files":file_totals})
    require(len(records) == fixed["expected_rows"], "public row count differs")
    ordered = sorted(records, key=lambda r:r["started"])
    require(all(a["finished"] <= b["started"] for a,b in zip(ordered, ordered[1:])), "public commands overlap")
    require(observed_count == len(observations), "unbound public RSS sample")
    def spread(values):
        return {"median":median(values), "min":min(values), "max":max(values)}
    comparisons = []
    for profile, mixture in [("durable","primary"),("fast","primary"),("durable","holdout")]:
        sides = {s:[r for r in records if r["cell"]["source"] == s and
                 r["cell"]["profile"] == profile and r["cell"]["mixture"] == mixture] for s in fixed["sources"]}
        for phase_index, phase_name in enumerate(phase_names):
            metrics = {}
            for metric in ["ops_per_sec", "p99_us", "p999_us", "process_bytes_per_op"]:
                vals = {s:[r["phases"][phase_index][metric] for r in rows] for s,rows in sides.items()}
                metrics[metric] = {s:spread(v) for s,v in vals.items()}
                metrics[metric]["candidate_over_baseline_median"] = median(vals["candidate"]) / median(vals["baseline"])
                metrics[metric]["paired_candidate_over_baseline"] = spread([c/b for c,b in zip(vals["candidate"], vals["baseline"])])
            comparisons.append({"profile":profile,"mixture":mixture,"phase":phase_name,"metrics":metrics})
    return {"status":"VALIDATED_ALL", "acceptance":"PENDING", "expected_rows":18,"observed_rows":len(records),
            "manifest_provisional":True,"sources":fixed["sources"],"environment":fixed["environment"],
            "records":records,"comparisons":comparisons}


def extract(contract, raw):
    def read(name):
        return json.loads(raw[name])

    manifest = read("maintenance/qualified-manifest-baseline.json")
    run = read("maintenance/run.json")
    source = read("maintenance/receipts-baseline/source.json")
    build = read("maintenance/receipts-baseline/build.json")
    native = read("maintenance/receipts-baseline/native.json")
    equality = read("baseline-landed-source-equality.json")
    require(manifest["provisional"] is False, "baseline manifest not qualified")
    require(run["manifest_sha256"] == sha(raw["maintenance/qualified-manifest-baseline.json"]), "run manifest differs")
    require(run["plan_sha256"] == sha(raw["baseline-3m-profile-plan.json"]), "run plan differs")
    for receipt in manifest["receipts"].values():
        require(sha(raw["maintenance/" + receipt["path"]]) == receipt["sha256"], "manifest receipt differs")
    require(source["head"] == equality["built_source_head"] == contract["source_head"], "baseline source differs")
    require(equality["landed_merge"] == contract["landed_head"] and equality["entire_git_tree_equal"] is True
            and equality["built_tree"] == equality["landed_tree"], "landed source equality differs")
    for name, digest in source["compiled_project_inputs"].items():
        require(source["files"].get(name) == digest, "compiled input differs from frozen source")
    require(source["go_list_sha256"] == sha(raw["maintenance/receipts-baseline/go-list-inputs.jsonstream"]), "compile listing differs")
    require(source["producer_script_sha256"] == run["collector_sha256"], "collector identity differs")
    require(run["source"] == manifest["sources"]["baseline"], "run source differs")
    require(run["source"]["binary_sha256"] == contract["baseline_binary_sha256"]
            == build["binaries"]["unified-bench-baseline"]["binary_sha256"], "baseline binary differs")
    require(run["native_resolution"]["libraries"] == {k:v["sha256"] for k,v in native["libraries"].items()}, "runtime native libraries differ")
    require(run["rc"] == 0 and run["validated"] is True and run["cell"]["profiled"] is True, "baseline run invalid")
    reconstruction = read("maintenance/baseline-treemap-reconstruction.json")
    require(reconstruction["source_head"] == contract["source_head"]
            and reconstruction["identical"] is True
            and reconstruction["original_build_receipt_missing"] is True
            and reconstruction["original_binary_sha256"] == reconstruction["rebuilt_binary_sha256"]
            == contract["treemap_binary_sha256"], "maintenance reconstruction differs")
    for key in ["source_receipt", "baseline_build_receipt"]:
        receipt = reconstruction[key]
        require(sha(raw["maintenance/" + receipt["path"]]) == receipt["sha256"], "reconstruction source/build binding differs")
    baseline = read("baseline-3m-profile-stdout.json")[0]
    require(baseline["engine"] == "treedb" and baseline["profiled"] is True and baseline["gomaxprocs"] == 12, "baseline configuration differs")
    for key, expected in contract["fixture"].items():
        require(baseline["config"].get(key) == expected, "baseline fixture differs: " + key)
    require(baseline["verified_keys"] == contract["verified_keys"] and baseline["verified_misses"] == contract["verified_misses"], "baseline oracle incomplete")
    phases = []
    for phase in baseline["phases"]:
        require(phase["ops"] > 0 and phase["seconds"] > 0, "invalid phase denominator")
        require(math.isclose(phase["process_allocated_bytes"] / phase["ops"], phase["process_bytes_per_op"], rel_tol=1e-12), "allocation ratio differs")
        selected = {k:phase[k] for k in ["name", "ops", "seconds", "composition_seconds", "process_heap_alloc_before", "process_heap_alloc_after", "process_allocated_bytes", "process_mallocs", "process_bytes_per_op", "process_gc_cycles"]}
        selected["reported_cache_snapshots"] = {
            boundary: {k:v for k,v in phase[boundary].items()
                       if "grouped_frame_cache." in k and k.rsplit(".", 1)[-1] in
                       ["allocated_slots", "allocated_shards", "capacity", "entries", "retained_bytes", "budget_bytes"]}
            for boundary in ["stats_before", "stats_after"]}
        phases.append(selected)
    require([p["name"] for p in phases] == ["quicksilver_" + n for n in ["hits", "misses", "mixed", "concurrent"]], "phase inventory differs")
    commands = read(BASE + "commands.json")
    require([c["label"] for c in commands] == ["pre-maintenance-verify", "full", "full-verify", "exhaustive"], "maintenance command order differs")
    for name in ["pre-maintenance-verify", "full-verify"]:
        oracle(read(BASE + name + "-stdout.json"), contract)
    compact = read(BASE + "full-stdout.json")
    require(compact["mode"] == "full" and compact["dry_run"] is False, "Full mode differs")
    flags = {k:compact[k] for k in ["fully_compacted", "policy_fully_compacted", "byte_minimized"]}
    require(all(v is False for v in flags.values()), "historical completion flags changed")
    storage = [census(read(BASE + name + "-census.json")) for name in
               ["final-before-verify", "before-maintenance", "full-before-oracle", "full-after"]]
    require(commands[1]["rc"] == 0 and commands[1]["timed_out"] is False, "Full did not complete")
    require(commands[3]["timed_out"] is True and commands[3]["rc"] != 0
            and commands[3]["finished"] - commands[3]["started"] >= 1800, "Exhaustive timeout differs")
    maintenance = [dict(label=c["label"], **command_summary(c, raw[BASE + c["label"] + "-stderr.txt"].decode())) for c in [commands[1], commands[3]]]
    maintenance[0].update(flags=flags, remaining_debt=compact["remaining_debt"], phases=compact["phases"], leaf_gc=compact["leaf_generation_gc"])
    failed_commands = read(FAILED + "commands.json")
    oracle(read(FAILED + "pre-maintenance-verify-stdout.json"), contract)
    failed = command_summary(failed_commands[1], raw[FAILED + "full-stderr.txt"].decode())
    require(failed["status"] == "failed" and "-rewrite-batch-size" in failed["command"] and failed["command"][-1] == "8192", "failed batch experiment differs")
    require("incompatible duplicate stable identity" in raw[FAILED + "full-stderr.txt"].decode(), "failure diagnostic differs")
    after_failure = read(FAILED + "readonly-after-failure/receipt.json")
    require(after_failure["rc"] == 0 and after_failure["unchanged"] is True and after_failure["before"] == after_failure["after"], "failed copy changed during read-only oracle")
    failed["readonly_oracle"] = oracle(read(FAILED + "readonly-after-failure/stdout.json"), contract)
    original = read(DIAG + "original-unchanged.json")
    require(original["before"] == original["after"], "original changed during read-only diagnosis")
    original_oracle = oracle(read(DIAG + "failed-original-readonly-full-oracle-stdout.txt"), contract)
    oracle(read(REBOUND + "rebound-copy-readonly-full-oracle-stdout.txt"), contract)
    diagnostic_commands = read(REBOUND + "commands.json")
    profile = next(c for c in diagnostic_commands if c["label"] == "bounded-copy-cpu-diagnostic")
    require(profile["profile_complete"] is True and profile["intentionally_terminated"] is True and profile["cpu_sha256"] == contract["retained_large_profile"]["sha256"], "bounded CPU profile differs")
    cumulative = raw[REBOUND + "cpu-cumulative-stdout.txt"].decode()
    def cpu_share(symbol):
        lines = [line.split() for line in cumulative.splitlines() if line.endswith(symbol)]
        require(len(lines) == 1, "missing or ambiguous CPU symbol")
        return float(lines[0][-2].rstrip("%"))
    observations = [json.loads(line) for line in raw["maintenance/baseline-3m-profile-memory.jsonl"].splitlines()]
    setting_rows = [json.loads(line) for line in raw["maintenance/leaf-256m-3m-profile-memory.jsonl"].splitlines()]
    leaf_manifest, leaf_run = read("leaf256/qualified-manifest.json"), read("leaf256/run.json")
    require(leaf_manifest["provisional"] is False and leaf_manifest["receipts"] == manifest["receipts"]
            and leaf_manifest["sources"] == manifest["sources"], "leaf diagnostic producer chain differs")
    require(leaf_run["manifest_sha256"] == sha(raw["leaf256/qualified-manifest.json"])
            and leaf_run["plan_sha256"] == sha(raw["leaf256/plan.json"]), "leaf diagnostic receipt differs")
    require(leaf_run["source"] == run["source"] and leaf_run["collector_sha256"] == run["collector_sha256"]
            and leaf_run["native_resolution"] == run["native_resolution"], "leaf diagnostic runtime differs")
    require(leaf_run["rc"] == 0 and leaf_run["validated"] is True
            and leaf_run["env"] == dict(run["env"], TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_BYTES="268435456"),
            "leaf diagnostic environment differs")
    require({k:v for k,v in leaf_run["cell"].items() if k != "label"}
            == {k:v for k,v in run["cell"].items() if k != "label"}, "leaf diagnostic cell differs")
    leaf_stdout = read("leaf256/stdout.json")[0]
    require(leaf_stdout["config"] == baseline["config"] and leaf_stdout["profiled"] is True
            and leaf_stdout["engine"] == "treedb" and leaf_stdout["gomaxprocs"] == 12
            and leaf_stdout["verified_keys"] == contract["verified_keys"]
            and leaf_stdout["verified_misses"] == contract["verified_misses"], "leaf diagnostic fixture differs")
    pending_packets = {}
    for name, expected in contract["pending_packets"].items():
        plan = read("pending-plans/" + name)
        require(plan["repeats"] * len(plan["cells"]) == expected["expected_rows"], "pending plan row count differs")
        require(len({cell["label"] for cell in plan["cells"]}) == len(plan["cells"])
                and {cell["source"] for cell in plan["cells"]} == set(expected["sources"])
                and all(cell["profiled"] is expected["profiled"] for cell in plan["cells"]), "pending plan class differs")
        pending_packets[name] = dict(expected, observed_rows=0, status="PENDING",
                                     repeats=plan["repeats"], cells=plan["cells"],
                                     plan_sha256=sha(raw["pending-plans/" + name]))
    before, after = storage[1], storage[2]
    return {"schema_version": 1, "publication_status": "PROVISIONAL", "pending": contract["pending"],
            "inputs_sha256": INPUTS_SHA256, "raw_files": contract["raw_files"],
            "provenance": {"source_head": contract["source_head"], "landed_head": contract["landed_head"],
                           "baseline_binary_sha256": contract["baseline_binary_sha256"], "go_version": build["go_version"].strip(),
                           "collector_sha256": run["collector_sha256"], "environment": run["env"],
                           "compiled_project_input_count": len(source["compiled_project_inputs"]),
                           "maintenance_binary_sha256": contract["treemap_binary_sha256"],
                           "maintenance_binary_binding": "post-measurement bit-identical reconstruction; original build receipt missing"},
            "fixture": baseline["config"], "storage": storage, "pending_packets": pending_packets,
            "public_matched": public_matched(contract, raw),
            "full_reduction": {"apparent_excluding_wal_bytes": before["apparent_excluding_wal"]-after["apparent_excluding_wal"],
                               "percent": 100*(1-after["apparent_excluding_wal"]/before["apparent_excluding_wal"]),
                               "oracle_apparent_delta_bytes": storage[3]["apparent"]-after["apparent"],
                               "oracle_allocated_delta_bytes": storage[3]["allocated"]-after["allocated"]},
            "memory": {"profiled_baseline_phases": phases, "rss": rss_summary(observations, run),
                       "leaf256_diagnostic": {"environment": leaf_run["env"],
                                              "rss": rss_summary(setting_rows, leaf_run),
                                              "acceptance": "single profiled setting diagnostic; no matched public matrix"},
                       "harness_bytes": {k:baseline[k] for k in ["oracle_state_bytes", "distinct_tracking_bytes", "sample_bytes", "trace_bytes"]}},
            "maintenance": maintenance, "failed_batch8192": failed,
            "batch8192_input_states": [census(read(FAILED+n+"-census.json")) for n in ["final-before-verify", "before-maintenance"]],
            "readonly_original_oracle": original_oracle,
            "diagnostic": {"command": profile, "candidate_closure_cumulative_percent": cpu_share("github.com/snissn/gomap/TreeDB/db.(*DB).scanCandidateExternalReferencesWithCountsAndLimitsV1"),
                           "zstd_decode_cumulative_percent": cpu_share("github.com/snissn/compress/zstd.(*Decoder).DecodeAll"),
                           "retained_profile": contract["retained_large_profile"]}}


REPORT_PROSE_SHA256 = "7a49969c48b913e5bd3212f4e06cb8e6af5531f5f36c1ec1d734f9ba3e118bb1"


def report(result, template=None):
    # REPORT is the sole prose template; its fixed prose is separately authenticated.
    text = (HERE / "REPORT.md").read_text() if template is None else template
    pattern = re.compile(r"<!-- BEGIN (\w+) -->\n.*?\n<!-- END \1 -->", re.S)
    skeleton = pattern.sub(lambda match: "<!-- " + match[1] + " -->", text)
    require(sha(skeleton.encode()) == REPORT_PROSE_SHA256, "fixed report prose changed")
    def fmt(value):
        if value is None:
            return "unavailable"
        return f"{value:,}" if type(value) is int else str(value)
    def table(headers, rows):
        return "\n".join(["| " + " | ".join(headers) + " |",
                          "| " + " | ".join(["---"] + ["---:"] * (len(headers)-1)) + " |"] +
                         ["| " + " | ".join(fmt(v) for v in row) + " |" for row in rows])
    storage = result["storage"]
    reduction = result["full_reduction"]
    debt = result["maintenance"][0]["remaining_debt"]
    failed_states = result["batch8192_input_states"]
    rss = result["memory"]["rss"]
    mem = rss["peak_sample"]["memory"]
    source = result["provenance"]
    blocks = {
        "summary": f"Full reduced apparent WAL-excluded bytes by {reduction['percent']:.2f}%, from {storage[1]['apparent_excluding_wal']:,} to {storage[2]['apparent_excluding_wal']:,}.",
        "oracle_delta": f"The Full oracle changed total apparent bytes by {reduction['oracle_apparent_delta_bytes']:,} and allocated bytes by {reduction['oracle_allocated_delta_bytes']:,}. All-value verification covered 3,000,000 present keys and 6,040,000 misses.",
        "debt": f"Full reported leaf-GC debt of {debt['leaf_gc_bytes']:,} bytes in {debt['leaf_gc_generations']} generation. `fully_compacted=false`, `policy_fully_compacted=false`, and `byte_minimized=false` are preserved. Debt is not automatically safe to delete: recoverable roots and stable resources remain protected.",
        "failed": f"The failed 8192 attempt's initial writable oracle changed apparent WAL-excluded bytes from {failed_states[0]['apparent_excluding_wal']:,} to {failed_states[1]['apparent_excluding_wal']:,}.",
        "rss": f"The baseline observer retained {rss['samples']} samples of one process. Maximum observed kernel VmHWM was {rss['observed_kernel_hwm_bytes']:,} bytes; maximum sampled RSS was {rss['sampled_rss_peak_bytes']:,}. At that RSS sample, anonymous/file/shared bytes were {mem['RssAnon']:,} / {mem['RssFile']:,} / {mem['RssShmem']:,}. Independent component maxima remain separate in RESULTS.json and must not be added together.",
        "diagnostic": f"The diagnostic CPU profile attributed {result['diagnostic']['candidate_closure_cumulative_percent']:.2f}% cumulatively to candidate external-reference closure scans and {result['diagnostic']['zstd_decode_cumulative_percent']:.2f}% to nested Zstd decoding.",
        "source": f"Frozen source `{source['source_head']}` has whole-tree equality with landed `{source['landed_head']}`. The separate post-measurement reconstruction produced a bit-identical treemap binary (`{source['maintenance_binary_sha256']}`).",
        "pending": "\n".join("- " + item for item in result["pending"]),
    }
    leaf_rss = result["memory"]["leaf256_diagnostic"]["rss"]
    blocks["rss"] += f"\n\nThe separate 256MiB outer-leaf mapping-budget diagnostic retained {leaf_rss['samples']} samples, with observed kernel VmHWM {leaf_rss['observed_kernel_hwm_bytes']:,} bytes and sampled RSS peak {leaf_rss['sampled_rss_peak_bytes']:,}. Its frozen producer chain and full-fixture oracle are bound here. This single profiled setting run does not establish a repeatable memory or throughput improvement."
    domains = ["dictionary", "outer_leaf", "user_value", "index", "metadata", "wal"]
    rows = [[s["label"]] + [f"{s['domains'][n]['apparent']:,} / {s['domains'][n]['allocated']:,}" for n in domains] +
            [f"{s['apparent_excluding_wal']:,} / {s['allocated_excluding_wal']:,}"] for s in storage]
    blocks["storage_table"] = table(["State", "Dictionary", "Outer leaves", "User values", "Index", "Metadata", "WAL", "Total excluding WAL"], rows)
    attempts = [(m["label"], m) for m in result["maintenance"]] + [("restored-copy Full, batch 8192", result["failed_batch8192"])]
    rows = [[name, item["status"], item["time_elapsed_seconds"], f"{item['receipt_elapsed_seconds']:.3f}", item["kernel_process_rss_hwm_bytes"],
             f"{item['sampled_disk_hwm_including_wal']['apparent']:,} / {item['sampled_disk_hwm_including_wal']['allocated']:,}"] for name, item in attempts]
    blocks["maintenance_table"] = table(["Attempt", "Outcome", "GNU time elapsed (s)", "Receipt elapsed (s)", "Process RSS high-water bytes", "Sampled apparent / allocated disk maximum, including WAL"], rows)
    rows = [[p["name"], p["process_heap_alloc_before"], p["process_heap_alloc_after"], p["process_allocated_bytes"], f"{p['process_bytes_per_op']:.3f}"] for p in result["memory"]["profiled_baseline_phases"]]
    blocks["memory_table"] = table(["Profiled phase", "HeapAlloc before", "HeapAlloc after", "Allocated bytes in bracket", "Bytes per operation"], rows)
    blocks["planned_inputs"] = table(["Pending plan", "Expected rows", "Imported result rows", "Class"],
        [[name, packet["expected_rows"], packet["observed_rows"], "profiled structural diagnostic" if "structural" in name else
          ("profiled public pair" if packet["profiled"] else "unprofiled public pairs")]
         for name, packet in result["pending_packets"].items()])
    public = result["public_matched"]
    def interval(values, digits=3):
        return f"{median(values):,.{digits}f} [{min(values):,.{digits}f}, {max(values):,.{digits}f}]"
    def metric_interval(values, digits=3):
        return f"{values['median']:,.{digits}f} [{values['min']:,.{digits}f}, {values['max']:,.{digits}f}]"
    rows, tails = [], []
    for comparison in public["comparisons"]:
        label = comparison["profile"] + " " + comparison["mixture"]
        phase = comparison["phase"].removeprefix("quicksilver_")
        metric = comparison["metrics"]["ops_per_sec"]
        ratio = metric["paired_candidate_over_baseline"]
        rows.append([label, phase, metric_interval(metric["baseline"], 0), metric_interval(metric["candidate"], 0),
                     f"{100*(metric['candidate_over_baseline_median']-1):+.2f}%",
                     f"{100*(ratio['min']-1):+.2f}% to {100*(ratio['max']-1):+.2f}%"])
        if phase in ["mixed", "concurrent"]:
            row = [label, phase]
            for name in ["p99_us", "p999_us"]:
                m = comparison["metrics"][name]
                row.extend([metric_interval(m["baseline"]), metric_interval(m["candidate"]),
                            metric_interval(m["paired_candidate_over_baseline"])])
            tails.append(row)
    blocks["public_throughput"] = table(["Workload", "Phase", "Baseline ops/s", "Candidate ops/s",
        "Ratio of medians change", "Paired repeat change range"], rows)
    blocks["public_tails"] = table(["Workload", "Phase", "Baseline p99 us", "Candidate p99 us",
        "Paired p99 ratio", "Baseline p999 us", "Candidate p999 us", "Paired p999 ratio"], tails)
    rows, barriers = [], []
    for label in [r["cell"]["label"] for r in public["records"][:6]]:
        records = [r for r in public["records"] if r["cell"]["label"] == label]
        rows.append([label, interval([r["guardrails"]["load_seconds"] for r in records]),
            interval([r["guardrails"]["time_elapsed_seconds"] for r in records]),
            interval([r["guardrails"]["kernel_process_rss_hwm_bytes"] for r in records], 0),
            interval([r["runtime_apparent_files"]["final_files"]["apparent_excluding_wal"] for r in records], 0)])
        barriers.append([label] + [interval([r["guardrails"][name] for r in records]) for name in
            ["initial_checkpoint_ms", "final_checkpoint_ms", "reopen_ms", "final_reopen_ms"]] +
            [interval([max(r["guardrails"][name]) for r in records]) for name in ["checkpoint_ms", "update_batch_ms"]])
    blocks["public_guardrails"] = table(["Cell", "Load s", "Whole command GNU time s", "Process RSS high-water bytes",
        "Final pre-oracle apparent bytes excluding WAL"], rows)
    blocks["public_barriers"] = table(["Cell", "Initial checkpoint ms", "Final checkpoint ms", "Initial reopen ms",
        "Final reopen ms", "Maximum concurrent checkpoint ms", "Maximum update batch ms"], barriers)
    names = [match[1] for match in pattern.finditer(text)]
    require(len(names) == len(blocks) and set(names) == set(blocks), "report block inventory differs")
    return pattern.sub(lambda match: "<!-- BEGIN " + match[1] + " -->\n" + blocks[match[1]] + "\n<!-- END " + match[1] + " -->", text)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="compare generated bytes without writing")
    args=parser.parse_args()
    result=extract(*load())
    outputs={"RESULTS.json":encoded(result), "REPORT.md":report(result).encode()}
    for name, data in outputs.items():
        if args.check:
            require((HERE/name).read_bytes() == data, "generated output differs: " + name)
        else:
            (HERE/name).write_bytes(data)
    print("PROVISIONAL packet verified" if args.check else "PROVISIONAL packet generated")


if __name__ == "__main__":
    main()
