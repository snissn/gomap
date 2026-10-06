"""Reparse immutable raw rows; descriptive ABBA effects, no automatic acceptance."""
import argparse
import json
from pathlib import Path
import statistics
from protocol import command, config, digest, identity, label, need, process_environment, row, schedule, sha, write, validate_go_environment, fixture_manifest
from collect import host_gate, selected_protocol, invocation_command
from build import verify_git_receipt

def summary(values):
    low, high = min(values), max(values)
    median = statistics.median(values)
    return {"values": values, "median": median, "min": low, "max": high,
            "spread_fraction": (high - low) / median if median else (0 if high == 0 else None)}

def validate_packet(packet, suite="c3-read"):
    """Shared retained build/source/process/host validation; no live execution."""
    packet = Path(packet).resolve()
    selected, selected_scripts = selected_protocol(suite)
    need(not (packet / "failure.json").exists(), "failed capture retained; no graceful analysis fallback")
    c = selected.config(packet / "config.json")
    env = process_environment(c["environment"])
    need(json.loads((packet / "environment.json").read_text()) ==
         {"effective_controls": c["environment"], "effective_process_environment": env}, "captured process environment mismatch")
    completion = json.loads((packet / "completion.json").read_text())
    for key, file in (("config_sha256", "config.json"), ("receipts_sha256", "receipts.json"), ("script_identity_sha256", "script-identity.json")):
        need(sha(packet / file) == completion[key], "packet identity mismatch " + file)
    script_hashes = json.loads((packet / "script-identity.json").read_text())
    need(set(script_hashes) == set(selected_scripts), "missing/extra retained tooling scripts")
    for name, value in script_hashes.items():
        need(sha(packet / name) == value, "collector/analyzer script drift")
    for variant in ("baseline", "candidate"):
        declaration = c["variants"][variant]
        receipt_path = packet / (variant + "-build-receipt.json")
        need(sha(receipt_path) == declaration["build_receipt_sha256"], "build receipt drift")
        build = json.loads(receipt_path.read_text())
        need(build["binary_sha256"] == declaration["binary_sha256"] and build["source_tree_sha256"] == declaration["source_tree_sha256"], "unbound build receipt")
        need(build["environment"] == c["environment"] and build["race"] is False and build["build_tags"] == [], "build controls drift")
        need(build.get("effective_process_environment") == env, "build process environment mismatch")
        artifacts = json.loads((packet / (variant + "-build-artifacts.json")).read_text())
        required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "generated_nonpersistent_inputs", "git_source"}
        need(set(artifacts) == set(build["artifacts"]) == required, "missing/extra build provenance map")
        for name, artifact in artifacts.items():
            need(artifact["path"] == variant + "-" + name + ".raw" and artifact["sha256"] == build["artifacts"][name]["sha256"], "artifact receipt binding drift")
            need(sha(packet / artifact["path"]) == artifact["sha256"], "build provenance drift")
        go_env = json.loads((packet / (variant + "-go_env.raw")).read_text())
        validate_go_environment(go_env, env)
        observed = identity(packet / (variant + "-source-manifest.json"))
        need(observed == json.loads((packet / (variant + "-identity.json")).read_text()), "source identity drift")
        need(observed["manifest_sha256"] == declaration["manifest_sha256"] and observed["tree_sha256"] == declaration["source_tree_sha256"], "unbound source manifest")
        need(observed["original_manifest"]["git_head"] == declaration["production_commit"] and observed["original_manifest"]["git_tree"] == declaration["production_git_tree"], "unbound production Git revision/tree")
        verify_git_receipt(None, observed["original_manifest"], json.loads((packet / (variant + "-git_source.raw")).read_text()))
        fixture_manifest(c["fixtures"], observed)
        source_files = {item["path"]: item["sha256"] for item in observed["files"]}
        for name, value in script_hashes.items():
            need(source_files.get("scripts/cow_c3_read/" + name) == value, "retained tooling differs from frozen source: " + name)
        need(not json.loads((packet / (variant + "-source-before.json")).read_text())["drift"], "initial source drift")
        need(not json.loads((packet / (variant + "-build-source-after.json")).read_text())["drift"], "build source-after drift")
    receipts = json.loads((packet / "receipts.json").read_text())
    planned = list(selected.schedule(c))
    need(len(receipts) == len(planned) == completion["runs"], "missing/extra runs")
    cases = {x["id"]: x for x in c["cases"]}
    rows = []
    for r, expected in zip(receipts, planned):
        name = label(expected)
        need(r["label"] == name and all(r[k] == v for k, v in expected.items()), "schedule/order mismatch")
        need(r["exit_code"] == 0 and not r["timed_out"] and not r["validation_error"] and not any(r["source_drift_after"].values()), "failed/drifted run")
        need(r["binary_sha256"] == c["variants"][r["variant"]]["binary_sha256"], "binary hash mismatch")
        for suffix, key in ((".stdout", "stdout_sha256"), (".stderr", "stderr_sha256")):
            need(sha(packet / (name + suffix)) == r[key], "raw stream drift")
        case = cases[r["case"]]
        raw_directory = r.get("raw_directory") if suite == "c4-sustained" else None
        if suite == "c4-sustained":
            need(type(raw_directory) is str and Path(raw_directory).is_absolute() and Path(raw_directory).name == name + "-lifecycle", "unbound raw directory")
        need(r["command"] == invocation_command(selected, c["variants"][r["variant"]]["binary"], case, expected, c["timeout_seconds"], raw_directory), "invocation mismatch")
        need(r["elapsed_seconds"] > 0 and r["child_max_rss_kib"] > 0 and r["child_user_seconds"] >= 0 and r["child_system_seconds"] >= 0, "invalid process measurements")
        need(r["work_contract_sha256"] == digest(case["workload_contract"]), "workload contract mismatch")
        parsed = selected.row(packet / (name + ".stdout"), packet / (name + ".stderr"), case, r["variant"], r["phase"])
        need(parsed == r["row"], "cached row differs from raw stream")
        for direction in ("before", "after"):
            name_prefix = name + "-" + direction
            captured = json.loads((packet / (name_prefix + "-host.json")).read_text())
            need(captured == r[direction], "host receipt mismatch")
            need(captured["source_path"] == c["variants"][r["variant"]]["source"], "host source path mismatch")
            host_gate(captured, c["host"])
            for source in ("meminfo", "cpuinfo", "mounts", "processes"):
                need(sha(packet / (name_prefix + "-" + source + ".txt")) == captured[source + "_sha256"], "host snapshot drift")
        rows.append(dict(expected, **parsed, rss_kib=r["child_max_rss_kib"]))
    return c, rows, receipts, completion


def main():
    p = argparse.ArgumentParser()
    p.add_argument("packet", type=Path)
    args = p.parse_args()
    packet = args.packet.resolve()
    c, rows, receipts, completion = validate_packet(packet)
    results = []
    need(len({digest(r["metadata"]) for r in rows}) == 1, "benchmark host/package metadata differs")
    for case in c["cases"]:
        group = [r for r in rows if r["case"] == case["id"] and r["phase"] == "measured"]
        need(len(group) == 12, "missing three ABBA cycles")
        for unit in case["comparable_metrics"]:
            need(len({r["metrics"][unit] for r in group}) == 1, "incomparable work metric " + unit)
        item = {"case": case["id"], "profile": case["profile"], "layout": case["layout"],
                "mode": case["mode"], "workload": case["workload"], "metrics": {}}
        for unit, direction in case["comparison_metrics"].items():
            a = [r["metrics"][unit] for r in group if r["variant"] == "baseline"]
            b = [r["metrics"][unit] for r in group if r["variant"] == "candidate"]
            ratios = []
            for cycle in range(1, 4):
                cycle_rows = [r for r in group if r["cycle"] == cycle]
                av = statistics.mean(r["metrics"][unit] for r in cycle_rows if r["variant"] == "baseline")
                bv = statistics.mean(r["metrics"][unit] for r in cycle_rows if r["variant"] == "candidate")
                ratios.append(bv / av if av else (1 if bv == 0 else None))
            sa, sb = summary(a), summary(b)
            inconclusive = any(s["spread_fraction"] is None or s["spread_fraction"] > c["noise_policy"]["max_spread_fraction"] for s in (sa, sb))
            effect = statistics.median(ratios) - 1 if all(v is not None for v in ratios) else None
            adverse = effect if direction == "lower" or effect is None else -effect
            item["metrics"][unit] = {"baseline": sa, "candidate": sb, "cycle_ratios": ratios, "better_direction": direction,
                "median_change_fraction": effect, "noisy": inconclusive,
                "material_regression_flag": adverse is None or adverse > c["noise_policy"]["material_regression_fraction"],
                "improvement_candidate": not inconclusive and adverse is not None and adverse < -c["noise_policy"]["minimum_effect_fraction"]}
        results.append(item)
    write(packet / "rows.json", rows)
    write(packet / "matched-summary.json", results)
    write(packet / "analysis-validation.json", {"runs": len(rows), "measured_runs": len(c["cases"]) * 12,
          "warmup_runs": len(c["cases"]) * 2, "cases": len(c["cases"]), "raw_rows_and_hashes_verified": True,
          "scope": "C3-read only; descriptive six samples/variant and three cycle ratios; no statistical significance or automatic acceptance",
          "claim": "Every flagged regression/inconclusive case requires coordinator disposition; no C4/M7/parent qualification"})
    print(json.dumps({"runs": len(rows), "cases": len(results)}))

if __name__ == "__main__":
    main()
