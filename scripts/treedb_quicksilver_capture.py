#!/usr/bin/env python3
"""Run/validate the canonical workflow with the existing package-build freeze."""
import argparse
import copy
import hashlib
import json
from pathlib import Path

import treedb_memory_budget_capture as memory

HARNESS = "TreeDB/quicksilver_workflow_bench_test.go"
PREFIX = "TREEDB_QUICKSILVER_PACKET "
LATENCY_SAMPLE_STRIDE = 17
PHASES = ["load_sync", "initial_checkpoint", "verify_initial", "owned_warm_reads"]
PHASES += [name for part in range(1, 5) for name in (f"updates_{part}", f"checkpoint_{part}")]
PHASES += ["verify_before_close", "verify_reopen"]


def workload(environment, controls):
    pilot = environment["TREEDB_MEMORY_PILOT"] == "1"
    keys, updates, reads = (8192, 8000, 6400) if pilot else (250000, 40000, 256000)
    filter_bytes = (((keys + 1) * 10 + 63) // 64 * 8) if controls["filter"] == "on" else 0
    return pilot, keys, updates, reads, filter_bytes


def run_environment(freeze, controls):
    _, _, _, _, filter_bytes = workload(freeze["environment"], controls)
    return dict(freeze["environment"], TREEDB_QUICKSILVER_MISS_PERCENT=str(controls["miss_percent"]),
                TREEDB_QUICKSILVER_READ_BATCH=str(controls["read_batch"]),
                TREEDB_QUICKSILVER_DISTRIBUTION=controls["distribution"],
                TREEDB_QUICKSILVER_FILTER_BYTES=str(filter_bytes))


def argv(prepared):
    return [str(prepared / "memory-budget.test"), "-test.run=^$", "-test.bench=^BenchmarkQuicksilverWorkflow$",
            "-test.benchtime=1x", "-test.count=1", "-test.benchmem", "-test.v", "-test.timeout=60m"]


def packet_from(stderr):
    lines = [line[len(PREFIX):] for line in stderr.splitlines() if line.startswith(PREFIX)]
    memory.require(len(lines) == 1, "missing/duplicate canonical packet")
    return json.loads(lines[0])


def quantile(samples, fraction):
    return sorted(samples)[min(int(len(samples) * fraction), len(samples) - 1)]


def positive_int(value):
    return type(value) is int and value > 0


def check_io(pair, required):
    memory.require(set(pair) == {"before", "after"}, "incomplete process IO pair")
    names = {"rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"}
    for sample in pair.values():
        memory.require(type(sample.get("supported")) is bool and type(sample.get("counters")) is dict, "invalid process IO support")
        counters = sample["counters"]
        memory.require(all(type(n) is int and n >= 0 for n in counters.values()), "invalid process IO counters")
        memory.require(sample["supported"] or counters == {}, "fabricated unsupported process IO")
        memory.require(not required or (sample["supported"] and names <= set(counters)), "missing retained Linux process IO")
        memory.require(not sample["supported"] or names <= set(counters), "incomplete supported process IO")
    memory.require(pair["before"]["supported"] == pair["after"]["supported"], "changed process IO support")
    memory.require(all(pair["after"]["counters"].get(k, -1) >= v for k, v in pair["before"]["counters"].items()), "reversed process IO counters")


def check_update_observations(packet, updates, retained):
    samples = packet["update_ack_samples_ns"]
    count = updates // 1000
    memory.require(type(samples) is list and len(samples) == count and all(positive_int(n) for n in samples), "invalid acknowledgement samples")
    for key, value in {"update_ack_count": count, "update_ack_batch_ops": 1000,
                       "update_ack_latency_unit": "ns per 1000-key WriteSync", "update_ack_ns": sum(samples),
                       "update_ack_p99_ns": quantile(samples, .99), "update_ack_p999_ns": quantile(samples, .999),
                       "update_ack_max_ns": max(samples), "final_checkpoint_includes_reader_join": True,
                       "process_io_scope": "kernel process counters; includes concurrent owner and helper work, not device writes"}.items():
        memory.require(type(packet.get(key)) is type(value) and packet[key] == value, "wrong/missing update field: " + key)
    per_phase = count // 4
    for part in range(4):
        memory.require(sum(samples[part * per_phase:(part + 1) * per_phase]) <= packet["phases"][4 + part * 2]["elapsed_ns"], "acknowledgements exceed update phase")
    reader = packet["concurrent_owned_reads"]
    expected = {"api": "owned Get", "readers": 1, "key_domain": "present even keys; permutation 7919", "allowed_generations": [0, 1],
                "sample_stride": LATENCY_SAMPLE_STRIDE, "sample_capacity": 65536, "full_values_validated": True, "joined": True, "error": ""}
    for key, value in expected.items():
        memory.require(type(reader.get(key)) is type(value) and reader[key] == value, "wrong/missing concurrent reader field: " + key)
    memory.require(all(type(generation) is int for generation in reader["allowed_generations"]), "invalid generation types")
    raw = reader["samples_ns"]
    memory.require(positive_int(reader["reads"]) and type(raw) is list and
                   len(raw) == (reader["reads"] + LATENCY_SAMPLE_STRIDE - 1) // LATENCY_SAMPLE_STRIDE and
                   0 < len(raw) <= reader["sample_capacity"] and all(positive_int(n) for n in raw), "incomplete concurrent read samples")
    memory.require(positive_int(reader["elapsed_ns"]) and positive_int(reader["max_ns"]) and
                   type(reader["p99_ns"]) is int and type(reader["p999_ns"]) is int and
                   reader["p99_ns"] == quantile(raw, .99) and reader["p999_ns"] == quantile(raw, .999) and
                   max(raw) <= reader["max_ns"] <= reader["elapsed_ns"], "invalid concurrent read tails")
    memory.require(reader["elapsed_ns"] >= sum(phase["elapsed_ns"] for phase in packet["phases"][4:12]), "concurrent reader missed update/checkpoint interval")
    check_io(packet["process_io"], retained)
    memory.require(set(packet["phase_process_io"]) == set(PHASES), "incomplete phase process IO")
    process = packet["process_io"]
    previous = process["before"]
    for name in PHASES:
        pair = packet["phase_process_io"][name]
        check_io(pair, retained)
        memory.require(pair["before"]["supported"] == process["before"]["supported"], "incoherent phase/process IO support")
        if previous["supported"]:
            for counter in process["before"]["counters"]:
                memory.require(counter in pair["before"]["counters"] and counter in pair["after"]["counters"] and
                               previous["counters"][counter] <= pair["before"]["counters"][counter] <=
                               pair["after"]["counters"][counter] <= process["after"]["counters"][counter],
                               "uncontained/chronologically reversed phase IO: " + counter)
        previous = pair["after"]


def extension_negative_packets(packet):
    cases = []
    for key in ("update_ack_samples_ns", "update_ack_count", "concurrent_owned_reads", "process_io", "phase_process_io"):
        wrong = copy.deepcopy(packet); del wrong[key]; cases.append(wrong)
    for key, value in (("update_ack_samples_ns", packet["update_ack_samples_ns"][:-1]),
                       ("update_ack_count", True), ("update_ack_ns", 1), ("update_ack_p999_ns", 0),
                       ("update_ack_max_ns", 0), ("update_ack_batch_ops", 1), ("update_ack_latency_unit", "ns per key")):
        wrong = copy.deepcopy(packet); wrong[key] = value; cases.append(wrong)
    for value in (False, 0, -1, 1.5):
        wrong = copy.deepcopy(packet); wrong["update_ack_samples_ns"][0] = value; cases.append(wrong)
    wrong = copy.deepcopy(packet); wrong["phases"][4]["elapsed_ns"] = 1; cases.append(wrong)
    for key, value in (("reads", 0), ("samples_ns", []), ("sample_stride", 1), ("sample_capacity", 1),
                       ("max_ns", 0), ("p99_ns", 0), ("full_values_validated", False), ("joined", False),
                       ("error", "injected"), ("allowed_generations", [1]), ("elapsed_ns", 1), ("api", "GetMany64")):
        wrong = copy.deepcopy(packet); wrong["concurrent_owned_reads"][key] = value; cases.append(wrong)
    wrong = copy.deepcopy(packet); wrong["phase_process_io"].pop(PHASES[0]); cases.append(wrong)
    wrong = copy.deepcopy(packet); wrong["process_io"]["before"] = {"supported": False, "counters": {"rchar": 0}}; cases.append(wrong)
    wrong = copy.deepcopy(packet)
    wrong["concurrent_owned_reads"]["elapsed_ns"] = sum(phase["elapsed_ns"] for phase in packet["phases"][4:11])
    cases.append(wrong)
    wrong = copy.deepcopy(packet)
    wrong["phase_process_io"][PHASES[4]]["before"]["supported"] = not wrong["process_io"]["before"]["supported"]
    cases.append(wrong)
    # Coherent supported fixtures isolate containment/chronology rejection even
    # on Darwin, where actual observations are explicitly unsupported.
    for before, after in ((1000, 1001), (0, 1)):
        wrong = copy.deepcopy(packet)
        names = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes")
        wrong["process_io"] = {"before": {"supported": True, "counters": dict.fromkeys(names, 0)},
                               "after": {"supported": True, "counters": dict.fromkeys(names, 100)}}
        for i, name in enumerate(PHASES):
            wrong["phase_process_io"][name] = {
                "before": {"supported": True, "counters": dict.fromkeys(names, i * 2 + 2)},
                "after": {"supported": True, "counters": dict.fromkeys(names, i * 2 + 3)}}
        pair = wrong["phase_process_io"][PHASES[6]]
        pair["before"]["counters"]["rchar"], pair["after"]["counters"]["rchar"] = before, after
        cases.append(wrong)
    if "configuration" in packet:
        for key, value in packet["configuration"].items():
            wrong = copy.deepcopy(packet)
            wrong["configuration"][key] = 1 if type(value) is bool else float(value)
            cases.append(wrong)
        wrong = copy.deepcopy(packet); wrong["configuration"]["unexpected"] = True; cases.append(wrong)
        wrong = copy.deepcopy(packet); del wrong["configuration"]["command_wal"]; cases.append(wrong)
    return cases


def check_configuration(configuration, leaf):
    expected = {
        "command_wal": True, "command_wal_stats_scan": True, "keep_recent": 10000, "flush_threshold": 64 << 20,
        "outer_leaves_in_value_log": True, "leaf_prefix_compression": True, "columnar_leaves": True, "packed_value_ptr": True,
        "leaf_cache_entries": leaf * (1 << 20) // 4096 if leaf else -1,
        "background_checkpoint_interval": -1, "background_checkpoint_idle_duration": -1, "max_wal_bytes": -1,
        "background_index_vacuum_interval": -1, "disable_background_prune": True}
    memory.require(type(configuration) is dict and set(configuration) == set(expected), "wrong canonical option keys")
    for key, value in expected.items():
        memory.require(type(configuration[key]) is type(value) and configuration[key] == value, "wrong canonical option: " + key)


def check_packet(packet, freeze, process, controls, retained):
    env = freeze["environment"]
    pilot, keys, updates, reads, filter_bytes = workload(env, controls)
    memory.require(not retained or not pilot, "pilot cannot satisfy retained validation")
    leaf, size, threshold = (int(env[name]) for name in ("TREEDB_MEMORY_LEAF_MIB", "TREEDB_MEMORY_VALUE_BYTES", "TREEDB_MEMORY_POINTER_THRESHOLD"))
    batch, miss = controls["read_batch"], controls["miss_percent"]
    expected = {"schema": "quicksilver-workflow-v1", "pilot": pilot, "keys": keys, "updates": updates,
                "read_keys": reads, "key_bytes": 32, "shared_prefix_bytes": 24, "value_bytes": size,
                "pointer_threshold": threshold, "verified_pointer_entries": keys if threshold == 1 or size == 4096 else 0,
                "miss_percent": miss, "distribution": controls["distribution"], "read_batch": batch,
                "negative_filter_bytes": filter_bytes, "main_leaf_budget_bytes": leaf << 20,
                "main_frame_budget_bytes": (64 - leaf) << 20, "combined_configured_main_budget_bytes": 64 << 20,
                "batch_size": 1000, "update_stride": 7919, "update_checkpoints": 4, "sentinel_keys": 1,
                "read_api_requests": reads // batch, "read_hits": reads * (100 - miss) // 100,
                "read_misses": reads * miss // 100, "query_table_bytes": reads * 8,
                "latency_samples": (reads // batch + LATENCY_SAMPLE_STRIDE - 1) // LATENCY_SAMPLE_STRIDE,
                "latency_sample_stride_requests": LATENCY_SAMPLE_STRIDE,
                "latency_unit": "ns per owned API request", "throughput_includes_crc32_consumption": True,
                "crc_disabled": False, "background_maintenance_disabled": True, "side_store_limits_changed": False,
                "all_values_and_interleaved_misses_verified": True, "present_empty_verified": True, "final_close_checked": True,
                "binary_sha256": freeze["identity"]["binary_sha256"], "process_id": process["pid"], "process_argv": process["argv"]}
    for key, value in expected.items():
        memory.require(type(packet.get(key)) is type(value) and packet[key] == value, "wrong/missing canonical field: " + key)
    check_configuration(packet["configuration"], leaf)
    actual_env = run_environment(freeze, controls)
    env_hash = hashlib.sha256("\0".join(sorted(key + "=" + value for key, value in actual_env.items())).encode()).hexdigest()
    memory.require(process["environment"] == actual_env and packet["environment_sha256"] == env_hash, "wrong canonical environment")
    memory.require(packet["gomaxprocs"] == int(actual_env.get("GOMAXPROCS", packet["gomaxprocs"])) and
                   packet["gomemlimit"] == actual_env.get("GOMEMLIMIT", ""), "wrong process resource settings")
    memory.require(packet["warm_state"] == "after checkpoint, persisted-placement scan and full value/miss verification", "wrong warm state")
    memory.require([phase["name"] for phase in packet["phases"]] == PHASES, "incomplete/duplicate canonical phases")
    operations = [keys, 1, keys * 2 + 1, reads] + [count for _ in range(4) for count in (updates // 4, 1)] + [keys * 2 + 1] * 2
    for phase, count in zip(packet["phases"], operations):
        memory.require(type(phase["operations"]) is int and phase["operations"] == count, "wrong phase count")
        for name in ("elapsed_ns", "heap_bytes"):
            memory.require(type(phase[name]) is int and phase[name] > 0, "missing positive phase sample: " + name)
        for name in ("allocated_bytes", "allocations"):
            memory.require(type(phase[name]) is int and phase[name] >= 0, "invalid phase allocation: " + name)
    check_update_observations(packet, updates, retained)
    memory.check_observations(packet, leaf, filter_bytes, (packet["post_reopen_gc_stats"],))
    memory.require(type(packet["read_checksum"]) is int and packet["read_checksum"] > 0, "missing consumed read checksum")
    memory.require(0 < packet["read_p99_ns"] <= packet["read_p999_ns"] <= packet["read_max_ns"] <= packet["phases"][3]["elapsed_ns"], "invalid read latency samples")
    memory.require(type(packet["reopen_ns"]) is int and packet["reopen_ns"] > 0 and
                   type(packet["post_reopen_gc_heap_bytes"]) is int and packet["post_reopen_gc_heap_bytes"] > 0, "missing lifecycle/heap observations")
    closure = packet["closure_stats"]
    memory.require(int(closure["treedb.command_wal.applied_lsn"]) >= int(closure["treedb.command_wal.live_accepted_max_lsn"]) > 0, "uncovered acknowledged LSN")
    if retained:
        memory.require(packet["goos"] == "linux" and all(int(closure["treedb.process.memory." + name]) > 0
                       for name in ("rss_bytes", "rss_hwm_bytes")), "retained cell requires observed Linux RSS/HWM")


def load(args):
    prepared = args.prepared.resolve()
    freeze = memory.load_freeze(prepared, args.freeze_sha256)
    harness_hash = freeze["identity"]["source_files"][HARNESS]
    input_path = str(Path(freeze["source_root"]) / HARNESS)
    memory.require(freeze["identity"]["compile_inputs"].get(input_path) == harness_hash, "canonical harness was not a compiled input")
    if not freeze["pilot"]:
        memory.require(args.runtime_head == freeze["identity"]["runtime_head"] and
                       args.harness_sha256 == harness_hash, "wrong/missing external canonical identity")
    return prepared, freeze, harness_hash


def check_record(record, freeze, prepared, output, retained):
    memory.require(record["schema"] == "quicksilver-workflow-run-v1" and record["freeze_sha256"] == memory.digest(prepared / "freeze.json"), "wrong canonical run/freeze")
    memory.require(record["before"] == record["after"] == freeze["identity"], "source/input/binary changed")
    memory.require(record["harness_sha256"] == freeze["identity"]["source_files"][HARNESS], "wrong canonical harness")
    controls = record["controls"]
    memory.require(type(controls["miss_percent"]) is int and controls["miss_percent"] in (0, 50, 90, 99) and
                   type(controls["read_batch"]) is int and controls["read_batch"] in (1, 64) and
                   controls["distribution"] in ("uniform", "zipf") and controls["filter"] in ("off", "on"), "invalid canonical controls")
    process = record["process"]
    memory.require(process["argv"] == argv(prepared) and process["cwd"] == freeze["source_root"] and
                   process["exit_code"] == 0, "wrong/failed canonical process")
    for stream in ("stdout", "stderr"):
        memory.require(memory.digest(output / ("run." + stream)) == process[stream + "_sha256"], "changed canonical " + stream)
    memory.require("PASS" in (output / "run.stdout").read_text().splitlines(), "missing canonical PASS")
    packet = packet_from((output / "run.stderr").read_text())
    check_packet(packet, freeze, process, controls, retained)
    return packet


def run(args):
    prepared, freeze, harness_hash = load(args)
    output = args.output.resolve()
    memory.require(not output.is_relative_to(Path(freeze["source_root"])), "run output must be outside source")
    output.mkdir(parents=True, exist_ok=False)
    controls = {key: getattr(args, key) for key in ("miss_percent", "read_batch", "distribution", "filter")}
    record = {"schema": "quicksilver-workflow-run-v1", "complete": False, "grant": args.grant,
              "freeze_sha256": memory.digest(prepared / "freeze.json"), "harness_sha256": harness_hash,
              "controls": controls, "before": memory.current_freeze_identity(prepared, freeze)}
    memory.require(freeze["pilot"] or args.grant, "full collection requires coordinator timing grant")
    memory.write_json(output / "run.json", record)
    try:
        record["process"] = memory.command(argv(prepared), Path(freeze["source_root"]), run_environment(freeze, controls), output, "run")
        record["after"] = memory.current_freeze_identity(prepared, freeze)
        check_record(record, freeze, prepared, output, not freeze["pilot"])
        record["complete"] = True
    except BaseException as error:
        record["failure"] = repr(error)
        raise
    finally:
        memory.write_json(output / "run.json", record)
    print("canonical workflow validation PASS (" + ("pilot; no retained claim" if freeze["pilot"] else "retained cell") + ")")


def validate(args):
    prepared, freeze, _ = load(args)
    output = args.output.resolve()
    record = json.loads((output / "run.json").read_text())
    memory.require(record["complete"] is True and not record.get("failure"), "incomplete canonical capture")
    packet = check_record(record, freeze, prepared, output, args.retained)
    if args.negative_checks:
        wrong = copy.deepcopy(packet)
        wrong["phases"].pop()
        cases = [wrong] + extension_negative_packets(packet)
        for key, value in (("all_values_and_interleaved_misses_verified", False), ("binary_sha256", "wrong"), ("latency_samples", 0)):
            wrong = copy.deepcopy(packet)
            wrong[key] = value
            cases.append(wrong)
        for wrong in cases:
            try:
                check_packet(wrong, freeze, record["process"], record["controls"], args.retained)
            except (ValueError, KeyError):
                pass
            else:
                raise AssertionError("incomplete/stale canonical proof accepted")
    print("canonical workflow validation PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--prepared", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--freeze-sha256")
        command.add_argument("--runtime-head")
        command.add_argument("--harness-sha256")
        if name == "run":
            command.add_argument("--miss-percent", type=int, choices=(0, 50, 90, 99), required=True)
            command.add_argument("--read-batch", type=int, choices=(1, 64), required=True)
            command.add_argument("--distribution", choices=("uniform", "zipf"), required=True)
            command.add_argument("--filter", choices=("off", "on"), required=True)
            command.add_argument("--grant")
        else:
            command.add_argument("--retained", action="store_true")
            command.add_argument("--negative-checks", action="store_true")
    args = parser.parse_args()
    (run if args.command == "run" else validate)(args)
