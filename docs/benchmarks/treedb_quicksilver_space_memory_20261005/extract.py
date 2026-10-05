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
INPUTS_SHA256 = "9b32f9ca7c62272097a434cb395dfe30dab0e0682efefecf1dcea9fe86b3a1a7"
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


def public_matched(contract, raw, profiled=False, diagnostic=False):
    prefix = "structural/" if diagnostic else ("public-profiles/" if profiled else "public-matched/")
    output = "offsets-3m-structural-diagnostic" if diagnostic else ("offsets-3m-profile-paired" if profiled else "offsets-3m-matched")
    bundle = prefix + output + "/"
    read = lambda name: json.loads(raw[prefix + name])
    fixed = contract["structural" if diagnostic else ("public_profiles" if profiled else "public_matched")]
    manifest_name = "diagnostic-manifest-offsets.json" if diagnostic else "qualified-manifest-offsets.json"
    manifest = read(manifest_name)
    require(raw[prefix + manifest_name] == raw[bundle + "manifest.json"],
            "public manifest copies differ")
    require(manifest["provisional"] is True and manifest["sources"] == fixed["sources"],
            "public frozen source identity differs")
    plan_bytes = raw[bundle + "plan.json"]
    require(sha(plan_bytes) == fixed["plan_sha256"] and
            plan_bytes == raw["pending-plans/" + output + "-plan.json"], "public plan differs")
    plan = json.loads(plan_bytes)
    require(plan["repeats"] == (1 if diagnostic else 3) and len(plan["cells"]) == (2 if profiled else 6) and
            len({c["label"] for c in plan["cells"]}) == len(plan["cells"]), "public matrix dimensions differ")
    receipts = {}
    for name, receipt in manifest["receipts"].items():
        data = raw[("public-matched/" if diagnostic else prefix) + receipt["path"]]
        require(sha(data) == receipt["sha256"] and data == raw[bundle + name + "-receipt.json"],
                "public receipt binding differs: " + name)
        receipts[name] = json.loads(data)
    observations = [json.loads(line) for line in raw[prefix + output + "-memory.jsonl"].splitlines()]
    observed_count = 0
    for name, identity in fixed["sources"].items():
        if diagnostic:
            name = name.removeprefix("diag-")
            identity = contract["public_matched"]["sources"][name]
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
    for repeat in range(1, plan["repeats"] + 1):
        for cell in plan["cells"]:
            path = bundle + str(repeat) + "-" + cell["label"] + "/"
            run = json.loads(raw[path + "run.json"])
            require(run["cell"] == cell and run["repeat"] == repeat and run["validated"] is True
                    and run["rc"] == 0 and cell["profiled"] is profiled, "public row identity/status differs")
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
            if profiled:
                command += ["-profile-dir", "/mnt/fast4tb/quicksilver-space-memory-20261005/" + output + "/" + str(repeat) + "-" + cell["label"]]
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
            require(value["engine"] == "treedb" and value["profiled"] is profiled and value["gomaxprocs"] == 12,
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
            if profiled:
                for selected, phase in zip(phases, value["phases"]):
                    selected["reported_cache_snapshots"] = {}
                    for boundary in ["stats_before", "stats_after"]:
                        stats = {k:v for k,v in phase[boundary].items() if "grouped_frame_cache." in k}
                        require(stats and all(math.isfinite(float(v)) and float(v) >= 0 for v in stats.values()),
                                "profile cache counters missing/invalid")
                        selected["reported_cache_snapshots"][boundary] = stats
                    for field in ["process_heap_alloc_before", "process_heap_alloc_after", "process_mallocs", "process_gc_pause_ns", "process_gc_cycles"]:
                        require(type(phase[field]) is int and phase[field] >= 0, "profile heap/allocation bracket invalid")
                        selected[field] = phase[field]
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
                    receipts["build"][cell["source"].removeprefix("diag-")]["builds"]["native-build"]["finished"], "public time ordering differs")
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
    if diagnostic:
        return structural_evidence(records, contract, raw)
    if profiled:
        return profile_evidence(records, contract, raw)
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
    return {"status":"VALIDATED_ALL", "acceptance":"HELD", "expected_rows":18,"observed_rows":len(records),
            "manifest_provisional":True,"sources":fixed["sources"],"environment":fixed["environment"],
            "records":records,"comparisons":comparisons}



def profile_evidence(records, contract, raw):
    prefix = "public-profiles/"
    read = lambda name: json.loads(raw[prefix + name])
    # These are the same frozen producers as the accepted unprofiled packet.
    for name in ["qualified-manifest-offsets.json"] + ["receipts-offsets/" + n + ".json" for n in ["source", "build", "native", "runner"]]:
        require(raw[prefix + name] == raw["public-matched/" + name], "profile/public frozen producer differs")
    inventory = read("m-profile-analysis-v2/inventory.json")
    profile_names = {"allocs_quicksilver_" + phase + "_treedb.pprof" for phase in ["hits", "misses", "mixed", "concurrent"]}
    profile_names |= {"cpu_quicksilver_" + phase + "_treedb.pprof" for phase in ["hits", "misses", "mixed", "concurrent"]}
    profile_names |= {"checkpoint_cpu_checkpoint_quicksilver_" + phase + "_treedb.pprof" for phase in ["initial", "final"]}
    profile_names |= {"block.pprof", "mutex.pprof", "trace.out"}
    expected = {"offsets-3m-profile-paired/" + str(r["repeat"]) + "-" + r["cell"]["label"] + "/" + name
                for r in records for name in profile_names}
    require(len(inventory) == len(expected) and {v["path"] for v in inventory} == expected
            and all(type(v["bytes"]) is int and v["bytes"] > 0 and re.fullmatch(r"[0-9a-f]{64}", v["sha256"]) for v in inventory),
            "retained native profile inventory differs")
    by_path = {v["path"]:v for v in inventory}
    analyses = read("m-profile-analysis-v2/analyses.json")
    require(analyses["go_sha256"] == contract["public_profiles"]["analysis_tool_sha256"], "profile analysis tool differs")
    expected_top = {"cpu_quicksilver_mixed_treedb.pprof", "allocs_quicksilver_mixed_treedb.pprof", "mutex.pprof", "block.pprof"}
    analyzed = set()
    for row in analyses["rows"]:
        command = row["command"]
        profile_path = command[-1].removeprefix("/mnt/fast4tb/quicksilver-space-memory-20261005/")
        profile_name = Path(profile_path).name
        require(profile_name in expected_top and profile_path in by_path and profile_path not in analyzed,
                "profile analysis inventory differs")
        record = next(r for r in records if profile_path.split("/")[1] == str(r["repeat"]) + "-" + r["cell"]["label"])
        identity = record["source"]
        expected_command = ["/home/mikers/.gvm/gos/go1.26.3/bin/go", "tool", "pprof", "-top", "-nodecount=20"]
        if profile_name.startswith("allocs_"):
            expected_command += ["-sample_index=alloc_space"]
        expected_command += [record["command"][0], "/mnt/fast4tb/quicksilver-space-memory-20261005/" + profile_path]
        output = "m-profile-analysis-v2/" + profile_path.split("/")[1] + "-" + profile_name + ".top.txt"
        require(command == expected_command and row["rc"] == 0 and row["started"] >= record["finished"]
                and row["finished"] > row["started"] and row["binary_sha256"] == identity["binary_sha256"]
                and row["profile_sha256"] == by_path[profile_path]["sha256"] and row["output"] == output
                and row["output_sha256"] == sha(raw[prefix + output]), "profile analysis provenance differs")
        analyzed.add(profile_path)
    require(len(analyzed) == 24, "profile top analyses incomplete")
    failure = "m-profile-analysis/1-baseline-durable-primary-profile-cpu_quicksilver_mixed_treedb.pprof.top.txt"
    require(b'does not match go tool version' in raw[prefix + failure], "original analysis failure lost")
    def spread(values):
        return {"median":median(values), "min":min(values), "max":max(values)}
    comparisons = []
    for i, phase in enumerate(records[0]["phases"]):
        metrics = {}
        for metric in ["process_heap_alloc_before", "process_heap_alloc_after", "process_bytes_per_op", "process_mallocs", "process_gc_cycles"]:
            vals = {side:[r["phases"][i][metric] for r in records if r["cell"]["source"] == side] for side in ["baseline", "candidate"]}
            metrics[metric] = {side:spread(v) for side,v in vals.items()}
            metrics[metric]["candidate_minus_baseline_median"] = median(vals["candidate"]) - median(vals["baseline"])
            metrics[metric]["paired_candidate_minus_baseline"] = spread([c-b for b,c in zip(vals["baseline"],vals["candidate"])])
        counters = {}
        for key in ["allocated_slots", "allocated_shards", "capacity", "entries", "retained_bytes", "budget_bytes"]:
            full_key = "treedb.vlog.grouped_frame_cache." + key
            counters[key] = {side:spread([int(r["phases"][i]["reported_cache_snapshots"]["stats_before"][full_key])
                for r in records if r["cell"]["source"] == side]) for side in ["baseline","candidate"]}
        comparisons.append({"phase":phase["name"], "metrics":metrics, "reported_vlog_cache_before":counters})
    return {"status":"VALIDATED_ALL", "acceptance":"EVIDENCE_ONLY", "observed_rows":6, "expected_rows":6,
            "records":records, "comparisons":comparisons, "native_profile_inventory":inventory,
            "top_analyses":analyses, "original_analysis_failure":failure,
            "heap_boundary":"explicit pre-profile GC followed by profile and reader setup; whole-process HeapAlloc",
            "timing_scope":"profiled diagnostic timings excluded from public performance acceptance"}


def structural_evidence(records, contract, raw):
    prefix = "structural/"
    read = lambda name: json.loads(raw[prefix + name])
    manifest = read("diagnostic-manifest-offsets.json")
    builds = read("5004-diagnostic-overlay/native-builds.json")
    overlay = read("5004-diagnostic-overlay/receipt.json")
    require(manifest["diagnostic_inputs"]["sha256"] == sha(raw[prefix + manifest["diagnostic_inputs"]["path"]])
            and overlay["generator_sha256"] == sha(raw[prefix + "5004-diagnostic-overlay/generate.py"]), "structural overlay method differs")
    source_receipt = json.loads(raw["public-matched/receipts-offsets/source.json"])
    for side, build in builds.items():
        identity = contract["structural"]["sources"]["diag-" + side]
        mapping_name = "5004-diagnostic-overlay/" + side + "-native-overlay.json"
        require(build["rc"] == 0 and build["source"] == contract["public_matched"]["sources"][side]
                and build["binary_sha256"] == identity["binary_sha256"]
                and build["receipt_sha256"] == sha(raw[prefix + "5004-diagnostic-overlay/receipt.json"])
                and build["overlay_manifest_sha256"] == sha(raw[prefix + mapping_name])
                and read(mapping_name)["Replace"] == build["overlay_mapping"], "structural native build differs")
        require(build["command"] == ["go", "build", "-p", "1", "-buildvcs=false", "-tags", "lmdb rocksdb", "-overlay",
                "/mnt/fast4tb/quicksilver-space-memory-20261005/" + mapping_name, "-o",
                "/mnt/fast4tb/quicksilver-space-memory-20261005/bin/" + identity["binary"], "./cmd/unified_bench"], "structural build command differs")
        for filename, relative in [("grouped_frame_cache.go", "TreeDB/internal/valuelog/grouped_frame_cache.go"), ("main.go", "cmd/unified_bench/main.go")]:
            binding = overlay["source_hashes"][side + "/" + filename]
            require(binding["original_sha256"] == source_receipt[side]["files"][relative]
                    and binding["overlay_sha256"] == sha(raw[prefix + "5004-diagnostic-overlay/" + side + "/" + filename + ".txt"]), "structural overlay source differs")
    by_pid = {r["rss"]["pid"]:r for r in records}
    require(len(by_pid) == 2 and all(r["started"] >= builds[r["cell"]["source"].removeprefix("diag-")]["finished"] for r in records), "structural process/build ordering differs")
    index = read("m-diagnostic-analysis/full-snapshot-index.json")
    require(len(index) == 16 and len({v["path"] for v in index}) == 16, "structural full snapshots incomplete")
    for record in records:
        cell = "1-" + record["cell"]["label"]
        snapshots = [v for v in index if v["cell"] == cell]
        require([(v["phase"],v["kind"]) for v in snapshots] == [(phase,kind) for phase in ["hits","misses","mixed","concurrent"] for kind in ["base","after"]], "structural snapshot order differs")
        require(all(a["mtime_ns"] < b["mtime_ns"] for a,b in zip(snapshots,snapshots[1:])), "structural snapshot timestamps unordered")
        for v in snapshots:
            require(v["pid"] == record["rss"]["pid"] and record["started"] <= v["mtime_ns"]/1e9 <= record["finished"]
                    and Path(v["path"]).name.startswith("quicksilver_allocs_" + v["kind"] + "_")
                    and len(raw[prefix + v["path"]]) == v["bytes"] and sha(raw[prefix + v["path"]]) == v["sha256"], "structural snapshot process/bytes differ")
    analyses = read("m-diagnostic-analysis/analyses.json")
    require(analyses["go_sha256"] == contract["structural"]["analysis_tool_sha256"] and len(analyses["rows"]) == 16, "structural analysis tool/inventory differs")
    by_digest = {v["sha256"]:v for v in index}
    outputs = set()
    sampled_heap = []
    for row in analyses["rows"]:
        v = by_digest[row["profile_sha256"]]
        record = by_pid[v["pid"]]
        output = "m-diagnostic-analysis/" + v["cell"] + "-" + v["phase"] + "-" + v["kind"] + ".top.txt"
        require(row["command"] == ["/home/mikers/.gvm/gos/go1.26.3/bin/go", "tool", "pprof", "-top", "-nodecount=30", "-sample_index=inuse_space", record["command"][0], "/mnt/fast4tb/quicksilver-space-memory-20261005/" + v["path"]]
                and row["rc"] == 0 and row["started"] >= record["finished"] and row["finished"] > row["started"]
                and row["binary_sha256"] == record["source"]["binary_sha256"] and row["output"] == output
                and row["output_sha256"] == sha(raw[prefix + output]) and output not in outputs, "structural full analysis provenance differs")
        outputs.add(output)
        text = raw[prefix + output].decode()
        require("Type: inuse_space" in text, "structural heap sample type differs")
        total = re.findall(r" of ([0-9.]+[kMGT]?B) total", text)
        require(len(total) == 1, "structural sampled heap total missing")
        symbols = {}
        for symbol in ["github.com/snissn/gomap/TreeDB/internal/valuelog.(*groupedFrameCache).store", "github.com/snissn/gomap/TreeDB/internal/valuelog.getDecodeScratch"]:
            lines = [line.split() for line in text.splitlines() if line.endswith(symbol)]
            require(len(lines) == 1, "structural sampled heap symbol missing")
            symbols[symbol] = {"flat":lines[0][0], "cumulative":lines[0][3]}
        sampled_heap.append({"cell":v["cell"],"phase":v["phase"],"kind":v["kind"],"reported_total":total[0],"symbols":symbols})
    rows = [json.loads(line) for line in raw[prefix + "offsets-3m-diagnostic-cache.jsonl"].splitlines()]
    require(len(rows) == 864, "structural per-cache inventory differs")
    groups, group = [], []
    for row in rows:
        record = by_pid[row["PID"]]
        require(row["Schema"] == 1 and record["started"] <= row["UnixNano"]/1e9 <= record["finished"], "structural cache process/time differs")
        st = row["Stats"]
        live, caps, empty = [row[k] for k in ["LiveK","AllSlotOffsetCapacity","EmptySlotOffsetCapacity"]]
        for hist, lower, upper in [(live,1,255),(caps,0,256),(empty,0,256)]:
            require(all(str(int(k)) == k and lower <= int(k) <= upper and type(n) is int and n >= 0 for k,n in hist.items()), "structural histogram invalid")
        require(all(type(v) is int and v >= 0 for v in st.values()) and sum(live.values()) == st["Entries"]
                and sum(caps.values()) == st["AllocatedSlots"] and sum(empty.values()) == st["AllocatedSlots"]-st["Entries"]
                and all(n <= caps.get(k,0) for k,n in empty.items()), "structural slot counts differ")
        inline = record["cell"]["source"] == "diag-baseline"
        offset_bytes = sum(int(k)*4*n for k,n in caps.items())
        require(row["Inline"] is inline and row["SlotSizeBytes"] == (1136 if inline else 136)
                and row["SlotStructBytes"] == st["AllocatedSlots"]*row["SlotSizeBytes"]
                and row["InlineOffsetBytes"] == (offset_bytes if inline else 0)
                and row["OffsetBackingBytes"] == (0 if inline else offset_bytes)
                and row["StructuralMetadataBytes"] == row["SlotStructBytes"]+row["OffsetBackingBytes"]
                and (not inline or all(int(k)==256 for k in caps)), "structural retained byte identity differs")
        if group and (row["PID"] != group[-1]["PID"] or row["UnixNano"]-group[-1]["UnixNano"] > 10000000):
            groups.append(group)
            group = []
        if group:
            require(row["UnixNano"] > group[-1]["UnixNano"], "structural cache captures unordered")
        group.append(row)
    groups.append(group)
    require(len(groups) == 20 and all(len({r["Cache"] for r in g}) == len(g) for g in groups), "structural contiguous capture grouping differs")
    fields = {"Hits":"hits","Misses":"misses","Stores":"stores","Evictions":"evictions","Releases":"releases",
        "Entries":"entries","Capacity":"capacity","AllocatedShards":"allocated_shards","AllocatedSlots":"allocated_slots",
        "RetainedBytes":"retained_bytes","SkippedDisabled":"skipped_disabled","SkippedOversize":"skipped_oversize",
        "SkippedBudget":"skipped_budget","SkippedContention":"skipped_contention"}
    qualified = []
    for record in records:
        own_groups = [g for g in groups if g[0]["PID"] == record["rss"]["pid"]]
        require(len(own_groups) == 10, "structural per-process captures incomplete")
        snapshots = [v for v in index if v["pid"] == record["rss"]["pid"]]
        for i, phase in enumerate(record["phases"]):
            # Select the unique contiguous pass between this phase's base/after full snapshots.
            matches = [g for g in own_groups if snapshots[2*i]["mtime_ns"] < g[0]["UnixNano"] <= g[-1]["UnixNano"] < snapshots[2*i+1]["mtime_ns"]]
            # CPU phase completion StatsAfter also occurs before the after snapshot; the first pass is StatsBefore.
            require(len(matches) == 2, "structural phase capture interval differs")
            g = matches[0]
            require(len({r["OwnerPath"] for r in g}) == len(g), "structural qualified owner paths repeat")
            stats = phase["reported_cache_snapshots"]["stats_before"]
            require(all(sum(r["Stats"][k] for r in g) == int(stats["treedb.vlog.grouped_frame_cache." + field]) for k,field in fields.items())
                    and {r["Stats"]["BudgetBytes"] for r in g} == {int(stats["treedb.vlog.grouped_frame_cache.budget_bytes"])}, "structural StatsBefore aggregate differs")
            histograms = {}
            for name in ["LiveK","AllSlotOffsetCapacity","EmptySlotOffsetCapacity"]:
                hist = {}
                for r in g:
                    for k,n in r[name].items():hist[k] = hist.get(k,0)+n
                histograms[name] = hist
            totals = {k:sum(r[k] for r in g) for k in ["SlotStructBytes","InlineOffsetBytes","OffsetBackingBytes","StructuralMetadataBytes"]}
            totals["empty_offset_capacity_bytes"] = sum(int(k)*4*n for k,n in histograms["EmptySlotOffsetCapacity"].items())
            qualified.append({"source":record["cell"]["source"],"phase":phase["name"],"pid":record["rss"]["pid"],
                "capture_start_ns":g[0]["UnixNano"],"capture_end_ns":g[-1]["UnixNano"],"cache_count":len(g),
                "stats_before":stats,"histograms":histograms,"structural_bytes":totals})
    return {"status":"VALIDATED_ALL", "acceptance":"STRUCTURAL_EVIDENCE_ONLY", "records":records,
            "per_cache_record_count":len(rows), "contiguous_capture_count":len(groups), "qualified_stats_before":qualified,
            "full_snapshot_index":index, "full_snapshot_analyses":analyses, "sampled_full_heap":sampled_heap,
            "aggregation_scope":"eight exact StatsBefore passes; unmatched concurrent-after captures excluded; no repeated-capture sums"}


def restored_maintenance(contract, raw):
    prefix = "restore-r2/"
    read = lambda name: json.loads(raw[prefix + name])
    fixed = contract["restore_r2"]
    manifest = read("provisional-manifest-restore-r2.json")
    require(manifest["provisional"] is True and manifest["sources"]["restore_r"]["head"] == fixed["head"],
            "restored maintenance source identity differs")
    for receipt in manifest["receipts"].values():
        require(sha(raw[prefix + receipt["path"]]) == receipt["sha256"], "restored receipt binding differs")
    source = read("receipts-restore-r2/source.json")
    build = read("receipts-restore-r2/build.json")
    native = read("receipts-restore-r2/native.json")
    source_inputs = read("restore-r2-source-inputs.json")
    equality = read("restore-r2-landed-source-equality.json")
    require(equality["measured_head"] == fixed["head"] and equality["landed_head"] == fixed["landed_head"]
            and equality["equal"] is True and equality["measured_tree"] == equality["landed_tree"] == fixed["tree"],
            "restored landed source differs")
    require(source["head"] == build["head"] == source_inputs["head"] == fixed["head"]
            and source["files"] == source_inputs["files"] and source["compiled_project_inputs"]
            and all(source["files"].get(p) == digest for p,digest in source["compiled_project_inputs"].items())
            and source["go_list_sha256"] == sha(raw[prefix + "receipts-restore-r2/go-list-inputs.jsonstream"])
            and source["freeze_script_sha256"] == sha(raw[prefix + "freeze-restore-r2.py"])
            and source["producer_script_sha256"] == source["files"]["scripts/unified_bench_quicksilver_capture.py"],
            "restored frozen compile inputs differ")
    require({name:value["binary_sha256"] for name,value in build["binaries"].items()} == fixed["binaries"]
            and all(step["rc"] == 0 and step["head"] == fixed["head"] for step in build["builds"].values())
            and native["libraries"] == manifest["libraries"]
            and build["restore_helper_source_sha256"] == fixed["restore_helper_source_sha256"]
            == sha(raw[prefix + "maintenance-diagnostic-overlay/rebind-copy.go.txt"]), "restored build/helper differs")
    causal = {}
    for label in ["red", "green"]:
        receipt = read("restore-r2-causal/" + label + "-receipt.json")
        require(receipt["head"] == fixed["head"] and receipt["rc"] == (1 if label == "red" else 0)
                and receipt["method_sha256"] == sha(raw[prefix + "restore-r2-causal.py"])
                and receipt["finished"] > receipt["started"], "restored causal receipt differs")
        for channel in ["stdout", "stderr"]:
            require(receipt[channel + "_sha256"] == sha(raw[prefix + "restore-r2-causal/" + label + "-" + channel + ".txt"]),
                    "restored causal log binding differs")
        require(all(source["files"][name] == digest for name,digest in receipt["test_sha256"].items()),
                "restored causal tests differ")
        expected_overlay = read("restore-r2-causal/red-overlay.json") if label == "red" else None
        require(receipt["overlay"] == expected_overlay, "restored causal overlay differs")
        production = "TreeDB/db/durable_root_snapshot_rebind.go"
        expected_production = (json.loads(raw["maintenance/receipts-baseline/source.json"])["files"][production]
                               if label == "red" else source["files"][production])
        require(receipt["production_sha256"] == expected_production, "restored causal production differs")
        text = raw[prefix + "restore-r2-causal/" + label + "-stdout.txt"].decode()
        for kind in ["dictionary", "template"]:
            for layout in ["manifest-v1", "directory-v2"]:
                require(("FAIL" if label == "red" else "PASS") +
                        ": TestRebindDurableRootSnapshotSideStoreNamespaceMatchesFreshAuthority/" + kind + "/" + layout in text,
                        "restored causal coverage incomplete")
        causal[label] = receipt
    packet = "restore-r2-batch-3m-8192-input/"
    derived = read(packet + "run.json")
    identity = read("maintenance-restore-r2-3m-batch8192/identity.json")
    copy = derived["derived_copy"]
    require(derived["source"] == identity["source"] == "restore_r" and copy["batch_size"] == 8192
            and copy["copy"] == identity["database"] and derived["env"] == identity["environment"]
            and identity["method_sha256"] == sha(raw[prefix + "maintenance-restore-r2-batch.py"])
            and identity["treemap_sha256"] == fixed["binaries"]["treemap-restore-r2"]
            and copy["restore_helper_sha256"] == fixed["binaries"]["rebind-owned-copy-restore-r2"]
            and derived["candidate_manifest"]["sha256"] == sha(raw[prefix + "provisional-manifest-restore-r2.json"]),
            "restored copy provenance differs")
    # This is inherited baseline metadata, not a newly timed R2 profiling run.
    original = json.loads(raw["maintenance/run.json"])
    inherited = {k:v for k,v in derived.items() if k not in ["candidate_manifest", "derived_copy", "source", "command"]}
    require(inherited == {k:v for k,v in original.items() if k not in ["source", "command"]}
            and derived["command"][1:] == original["command"][1:], "inherited baseline metadata differs")
    before_profile = json.loads(raw["baseline-3m-profile-stdout.json"])[0]
    inherited_profile = read(packet + "stdout.json")[0]
    require(inherited_profile["data_dir"] == copy["copy"] and before_profile["data_dir"] == copy["original"]
            and {k:v for k,v in inherited_profile.items() if k != "data_dir"}
            == {k:v for k,v in before_profile.items() if k != "data_dir"}, "inherited baseline stdout differs")
    readonly = read(packet + "readonly-after-restore/receipt.json")
    require(readonly["rc"] == 0 and readonly["unchanged"] is True and readonly["before"] == readonly["after"]
            and readonly["binary_sha256"] == fixed["readonly_binary_sha256"]
            and readonly["method_sha256"] == sha(raw[prefix + "verify-restore-r2-copy.py"]), "restored readonly proof differs")
    require(readonly["proof"] == read(packet + "readonly-after-restore/stdout.json"), "restored readonly output differs")
    oracle(readonly["proof"], contract)
    base = "maintenance-restore-r2-3m-batch8192/"
    commands = read(base + "commands.json")
    labels = ["pre-maintenance-verify", "full", "full-verify", "exhaustive", "exhaustive-verify",
              "exhaustive-second", "exhaustive-second-verify"]
    require([c["label"] for c in commands] == labels and
            all(c["rc"] == 0 and c["timed_out"] is False for c in commands)
            and all(a["finished"] <= b["started"] for a,b in zip(commands,commands[1:])), "restored command order/status differs")
    binary_root = "/mnt/fast4tb/quicksilver-space-memory-20261005/bin/"
    verify_command = [binary_root + "unified-bench-restore-r2"] + original["command"][1:-3] + ["-quicksilver-verify-dir", copy["copy"]]
    attempts = []
    for command in commands:
        label = command["label"]
        require(command["started"] >= max(b["finished"] for b in build["builds"].values()), "restored command predates build")
        if label.endswith("verify"):
            require(command["command"] == verify_command, "restored oracle command differs")
            oracle(read(base + label + "-stdout.json"), contract)
            continue
        compact = read(base + label + "-stdout.json")
        mode = label.split("-")[0]
        expected = [binary_root + "treemap-restore-r2", "compact", copy["copy"], "-rw", "-json", "-mode", mode,
                    "-sync-each-phase", "-leaf-pack-max-passes", "64", "-rewrite-batch-size", "8192"]
        require(command["command"] == expected and compact["mode"] == mode and compact["dry_run"] is False,
                "restored compact command/mode differs")
        flags = {k:compact[k] for k in ["fully_compacted", "policy_fully_compacted", "byte_minimized"]}
        require(all(v is False for v in flags.values()), "restored minimum/completion flags changed")
        cost = command_summary(command, raw[prefix + base + label + "-stderr.txt"].decode())
        require(cost["time_elapsed_seconds"] is not None and cost["kernel_process_rss_hwm_bytes"] is not None,
                "restored cost measurement missing")
        pre = census(read(base + label + "-before-oracle-census.json"))
        post = census(read(base + label + "-after-census.json"))
        attempts.append(dict(label=label, **cost, flags=flags, remaining_debt=compact["remaining_debt"],
                             phases=compact["phases"], pre_oracle=pre, post_oracle=post,
                             oracle_delta={key:post[key]-pre[key] for key in ["apparent","allocated"]}))
    return {"status":"MEASURED_PROVISIONAL", "source_head":fixed["head"], "landed_source":equality, "binaries":fixed["binaries"],
            "environment":identity["environment"], "causal":causal,"restored_copy":copy,
            "input_metadata":"inherited baseline run/stdout; not R2 profiling measurements",
            "readonly_restored_copy":readonly, "initial_states":[census(read(base + name + "-census.json"))
                for name in ["final-before-verify", "before-maintenance"]], "attempts":attempts}


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
            "public_profiles": public_matched(contract, raw, profiled=True),
            "structural": public_matched(contract, raw, profiled=True, diagnostic=True),
            "restored_maintenance": restored_maintenance(contract, raw),
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


REPORT_PROSE_SHA256 = "97ba7aa988e7a63330c4adfb7e9382902d92151f96f14569ef0202e4d7b6505c"


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
         for name, packet in result["pending_packets"].items()]) if result["pending_packets"] else "All three planned row inventories are populated; acceptance gates below remain open."
    restored = result["restored_maintenance"]
    rows = [[item["label"], item["time_elapsed_seconds"], f"{item['receipt_elapsed_seconds']:.3f}",
             item["kernel_process_rss_hwm_bytes"],
             f"{item['sampled_disk_hwm_including_wal']['apparent']:,} / {item['sampled_disk_hwm_including_wal']['allocated']:,}",
             item["disk_sample_count"]] for item in restored["attempts"]]
    blocks["restored_costs"] = table(["R2 attempt, batch 8192", "GNU time s", "Receipt s", "Process RSS high-water bytes",
                                    "Sampled disk maximum including WAL, apparent / allocated", "Disk samples"], rows)
    states = restored["initial_states"] + [state for item in restored["attempts"] for state in [item["pre_oracle"], item["post_oracle"]]]
    blocks["restored_storage"] = table(["R2 state", "Dictionary", "Outer leaves", "User values", "Index", "Metadata", "WAL", "Total excluding WAL"],
        [[s["label"]] + [f"{s['domains'][n]['apparent']:,} / {s['domains'][n]['allocated']:,}" for n in domains] +
         [f"{s['apparent_excluding_wal']:,} / {s['allocated_excluding_wal']:,}"] for s in states])
    blocks["restored_debt"] = table(["R2 attempt", "Leaf-GC debt bytes", "Leaf generations", "Rewrite segments", "Rewrite stale bytes", "Oracle apparent / allocated delta"],
        [[item["label"], item["remaining_debt"]["leaf_gc_bytes"], item["remaining_debt"]["leaf_gc_generations"],
          item["remaining_debt"]["value_log_rewrite_segments"], item["remaining_debt"]["value_log_rewrite_bytes"],
          f"{item['oracle_delta']['apparent']:,} / {item['oracle_delta']['allocated']:,}"] for item in restored["attempts"]])
    public = result["public_matched"]
    def interval(values, digits=3):
        return f"{median(values):,.{digits}f} [{min(values):,.{digits}f}, {max(values):,.{digits}f}]"
    def metric_interval(values, digits=3):
        return f"{values['median']:,.{digits}f} [{values['min']:,.{digits}f}, {values['max']:,.{digits}f}]"
    profiles = result["public_profiles"]
    rows, cache_rows = [], []
    for comparison in profiles["comparisons"]:
        heap = comparison["metrics"]["process_heap_alloc_before"]
        after = comparison["metrics"]["process_heap_alloc_after"]
        allocation = comparison["metrics"]["process_bytes_per_op"]
        rows.append([comparison["phase"].removeprefix("quicksilver_"), metric_interval(heap["baseline"], 0),
            metric_interval(heap["candidate"], 0), heap["candidate_minus_baseline_median"],
            metric_interval(heap["paired_candidate_minus_baseline"], 0), metric_interval(after["baseline"], 0),
            metric_interval(after["candidate"], 0), metric_interval(allocation["baseline"]), metric_interval(allocation["candidate"])])
        counters = comparison["reported_vlog_cache_before"]
        for side in ["baseline", "candidate"]:
            cache_rows.append([comparison["phase"].removeprefix("quicksilver_") + " " + side] +
                [metric_interval(counters[key][side], 0) for key in ["allocated_slots", "allocated_shards", "capacity", "entries", "retained_bytes", "budget_bytes"]])
    blocks["profile_heap"] = table(["Profiled phase", "Baseline HeapAlloc before B", "Candidate HeapAlloc before B", "Difference of medians B",
        "Paired candidate-minus-baseline B", "Baseline HeapAlloc after B", "Candidate HeapAlloc after B", "Baseline allocated B/op", "Candidate allocated B/op"], rows)
    blocks["profile_cache"] = table(["StatsBefore, value-log grouped cache", "Allocated slots", "Allocated shards", "Capacity", "Entries", "Retained payload B", "Budget B"], cache_rows)
    rows = []
    for side in ["baseline", "candidate"]:
        records = [r for r in profiles["records"] if r["cell"]["source"] == side]
        rows.append([side, interval([r["guardrails"]["kernel_process_rss_hwm_bytes"] for r in records], 0),
            interval([r["rss"]["sampled_rss_peak_bytes"] for r in records], 0),
            interval([r["rss"]["peak_sample"]["memory"]["RssAnon"] for r in records], 0),
            interval([r["rss"]["peak_sample"]["memory"]["RssFile"] for r in records], 0)])
    blocks["profile_rss"] = table(["Profiled source", "GNU process RSS high-water B", "Sampled RSS peak B", "Anonymous B at sampled peak", "File-backed B at sampled peak"], rows)
    structural = result["structural"]
    rows = []
    for group in structural["qualified_stats_before"]:
        hist = group["histograms"]
        stats = group["stats_before"]
        vals = group["structural_bytes"]
        entries = int(stats["treedb.vlog.grouped_frame_cache.entries"])
        rows.append([group["source"] + " " + group["phase"].removeprefix("quicksilver_"), group["cache_count"],
            int(stats["treedb.vlog.grouped_frame_cache.allocated_slots"]), entries,
            f"{100*hist['LiveK'].get('2',0)/entries:.2f}%", vals["SlotStructBytes"], vals["OffsetBackingBytes"],
            vals["StructuralMetadataBytes"], vals["empty_offset_capacity_bytes"]])
    blocks["structural_bytes"] = table(["Independent StatsBefore pass", "Caches", "Allocated slots", "Live entries", "Live K=2 fraction",
        "Slot struct B", "Separate offset backing B", "Structural metadata B", "Empty-slot offset capacity B"], rows)
    rows = [[v["cell"].removeprefix("1-") + " " + v["phase"], v["reported_total"],
             v["symbols"]["github.com/snissn/gomap/TreeDB/internal/valuelog.(*groupedFrameCache).store"]["flat"],
             v["symbols"]["github.com/snissn/gomap/TreeDB/internal/valuelog.getDecodeScratch"]["flat"]]
            for v in structural["sampled_full_heap"] if v["kind"] == "base"]
    blocks["structural_heap"] = table(["Full pre-phase GC snapshot", "Sampled inuse-space total", "Grouped cache store flat", "Decode scratch flat"], rows)
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
