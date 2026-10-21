#!/usr/bin/env python3
"""Describe every saved cell and paired ratios; never turn diagnostics into qualification."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
from typing import Any


METRICS = ("nanoseconds", "operations", "mutations", "output_records", "allocated_bytes", "allocations", "num_gc", "heap_alloc", "heap_objects")
PROCESS_METRICS = ("user_cpu_ns", "system_cpu_ns", "voluntary_switches", "involuntary_switches")
IO_METRICS = ("read_bytes", "write_bytes", "read_chars", "write_chars")
COMPARISONS = (("baseline", "candidate"), ("cow-baseline", "candidate"), ("badger", "candidate"), ("baseline", "badger"), ("cow-baseline", "badger"))
ADAPTIVE_MEMTABLE_MODES = frozenset({"append_only", "hash_sorted", "btree"})


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def describe(values: list[float]) -> dict[str, Any] | None:
    if not values:
        return None
    return {"samples": len(values), "median": statistics.median(values), "min": min(values), "max": max(values), "values": values}


def pair_stats(pairs: list[tuple[float, float]]) -> dict[str, Any]:
    valid = [(left, right) for left, right in pairs if number(left) and number(right)]
    ratios = [left / right for left, right in valid if right > 0]
    return {
        "pairs": len(valid), "left": describe([left for left, _ in valid]),
        "right": describe([right for _, right in valid]),
        "paired_left_over_right": describe(ratios),
        "paired_right_minus_left": describe_signed([right - left for left, right in valid]),
        "right_wins": sum(right < left for left, right in valid),
        "ties": sum(right == left for left, right in valid),
        "left_wins": sum(left < right for left, right in valid),
        "zero_right_denominators": sum(right == 0 for _, right in valid),
    }


def describe_signed(values: list[float]) -> dict[str, Any] | None:
    return describe(values)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError("artifact path escapes evidence directory: " + relative)
    return path


def memtable_route_errors(config: dict[str, Any], route: dict[str, Any]) -> list[str]:
    errors = []
    requested = config.get("memtable_mode", "")
    if route.get("requested_memtable_mode") != requested:
        errors.append("requested_memtable_mode differs from config")
    actual = route.get("memtable_mode", "")
    if config.get("engine") == "treedb" and requested == "adaptive":
        if actual not in ADAPTIVE_MEMTABLE_MODES:
            errors.append("memtable_mode is not a production adaptive resolution")
        expected_config = "adaptive"
    else:
        if actual != requested:
            errors.append("memtable_mode differs from explicit selector")
        expected_config = "fixed" if config.get("engine") == "treedb" else ""
    if route.get("memtable_mode_config", "") != expected_config:
        errors.append("memtable_mode_config differs from requested routing")
    return errors


def reopened_route_matches(config: dict[str, Any], initial: dict[str, Any], reopened: dict[str, Any]) -> bool:
    ignored = {"memtable_mode"} if config.get("engine") == "treedb" and config.get("memtable_mode") == "adaptive" else set()
    return {key: value for key, value in initial.items() if key not in ignored} == {
        key: value for key, value in reopened.items() if key not in ignored}


def validate_result(result: Any, row: dict[str, Any]) -> list[str]:
    errors = []
    if not isinstance(result, dict):
        return ["result is not an object"]
    if result.get("schema_version") != "orderedmem-native-v1" or result.get("success") is not True:
        errors.append("result schema/success invalid")
    trace = result.get("trace_sha256")
    if not isinstance(trace, str) or len(trace) != 64 or any(char not in "0123456789abcdefABCDEF" for char in trace):
        errors.append("trace_sha256 invalid")
    reported = result.get("config", {})
    if not isinstance(reported, dict) or any(reported.get(key) != value for key, value in row.get("config", {}).items()):
        errors.append("reported config differs from saved execution config")
    phases = result.get("phases")
    if not isinstance(phases, list) or not phases:
        errors.append("phases missing")
    else:
        seen = set()
        for phase in phases:
            if not isinstance(phase, dict) or not isinstance(phase.get("name"), str):
                errors.append("invalid phase")
                continue
            if phase["name"] in seen:
                errors.append("duplicate phase: " + phase["name"])
            seen.add(phase["name"])
            for key in METRICS:
                if not number(phase.get(key)):
                    errors.append(f"invalid {phase['name']}.{key}")
    if not number(result.get("cycle_ns")):
        errors.append("cycle_ns invalid")
    for field in ("runtime", "verification"):
        if not isinstance(result.get(field), dict):
            errors.append(field + " missing")
    config = row.get("config", {})
    for field, maximum in (("badger_memtable_size", (1 << 63) - 1),
                           ("badger_value_threshold", (1 << 63) - 1),
                           ("operational_min_rotations", (1 << 64) - 1),
                           ("operational_min_payload_bytes", (1 << 64) - 1)):
        value = config.get(field, 0)
        if type(value) is not int or not 0 <= value <= maximum:
            errors.append("invalid optional config field: " + field)
    for field in ("route", "reopened_route"):
        route = result.get(field)
        if not isinstance(route, dict):
            errors.append(field + " missing")
            continue
        expected_route = {"engine": config.get("engine"), "acknowledgment": config.get("profile"),
                          "resolved_profile": "managed" if config.get("engine") == "badger" else config.get("profile")}
        for key, value in expected_route.items():
            if route.get(key) != value:
                errors.append(f"{field} mismatch: {key}")
        errors.extend(field + ": " + error for error in memtable_route_errors(config, route))
        if config.get("memtable_mode") == "cow_ordered" and route.get("logical_mvcc") is not True:
            errors.append(field + " does not confirm candidate logical_mvcc")
    if isinstance(result.get("route"), dict) and isinstance(result.get("reopened_route"), dict) and not reopened_route_matches(config, result["route"], result["reopened_route"]):
        errors.append("reopened route differs from initial route")
    verification = result.get("verification")
    if isinstance(verification, dict) and (verification.get("success") is False or verification.get("verified") is False):
        errors.append("verification explicitly reports failure")
    if isinstance(verification, dict):
        required = ["foreground_outputs", "distinct_values_and_complete_history_after_reopen"]
        if config.get("hold_cut"):
            required.append("held_cut")
        if config.get("memtable_mode") == "cow_ordered":
            required.append("ordered_entries_observed")
        if any(type(config.get(field, 0)) is int and config.get(field, 0) > 0
               for field in ("operational_min_rotations", "operational_min_payload_bytes")):
            required.append("operational_activation")
        for key in required:
            if verification.get(key) is not True:
                errors.append("verification does not confirm " + key)
    if config.get("memtable_mode") == "cow_ordered":
        activation = result.get("activation_stats")
        if not isinstance(activation, dict):
            errors.append("candidate activation_stats missing")
        else:
            for key in ("treedb.cache.cow.ordered.logical_keys", "treedb.cache.cow.ordered.physical_versions"):
                value = activation.get(key)
                if not isinstance(value, str) or not 1 <= len(value) <= 20 or any(char not in "0123456789" for char in value) or not 0 < int(value) <= (1 << 64) - 1:
                    errors.append("candidate activation counter is not positive uint64: " + key)
    return errors


def workload(config: dict[str, Any]) -> dict[str, Any]:
    excluded = {"engine", "profile", "memtable_mode", "dir", "profile_dir", "badger_memtable_size", "badger_value_threshold"}
    return {key: value for key, value in config.items() if key not in excluded}


def summarize(root: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    plan_bytes = (root / "plan.json").read_bytes()
    plan = json.loads(plan_bytes)
    manifest = load_json(root / "manifest.json")
    if plan.get("schema_version") != "orderedmem-native-plan-v1" or manifest.get("schema_version") != "orderedmem-native-run-v1":
        raise ValueError("unsupported plan/manifest schema")
    issues = []
    if manifest.get("plan_sha256") != hashlib.sha256(plan_bytes).hexdigest():
        issues.append("plan SHA256 differs from manifest")
    if manifest.get("plan_canonical_sha256") != hashlib.sha256(canonical(plan)).hexdigest():
        issues.append("canonical plan SHA256 differs from manifest")
    if manifest.get("complete") is not True:
        issues.append("runner manifest is incomplete")
    if manifest.get("success") is not True:
        issues.append("runner manifest does not report overall success")
    rows = {}
    for row in manifest.get("cells", []):
        if row.get("id") in rows:
            issues.append("duplicate manifest cell: " + str(row.get("id")))
        rows[row.get("id")] = row
    planned_ids = {cell["id"] for cell in plan["cells"]}
    extra = sorted(set(rows) - planned_ids)
    if extra:
        issues.append("unplanned manifest cells: " + ", ".join(extra))
    cells = []
    valid = {}
    groups = defaultdict(dict)
    for cell in plan["cells"]:
        identity = cell["id"]
        row = rows.get(identity)
        errors = []
        result = None
        if row is None:
            errors.append("planned cell missing from manifest")
        else:
            if row.get("status") != "success" or row.get("returncode") != 0:
                errors.append(f"status={row.get('status')} returncode={row.get('returncode')}")
            errors.extend(row.get("errors", []))
            cfg = row.get("config", {})
            if row.get("config_canonical_sha256") != hashlib.sha256(canonical(cfg)).hexdigest():
                errors.append("canonical execution config SHA256 mismatch")
            try:
                config_bytes = safe_path(root, row.get("config_path", "")).read_bytes()
                if row.get("config_sha256") != hashlib.sha256(config_bytes).hexdigest():
                    errors.append("execution config file SHA256 mismatch")
                if json.loads(config_bytes) != cfg:
                    errors.append("execution config file differs from manifest object")
            except (ValueError, OSError, TypeError) as error:
                errors.append("missing/invalid config file: " + str(error))
            expected = {key: value for key, value in cell["config"].items() if key not in {"dir", "profile_dir"}}
            if any(cfg.get(key) != value for key, value in expected.items()):
                errors.append("execution config differs from planned config")
            try:
                result_path = safe_path(root, row.get("result_path", ""))
                payload = result_path.read_bytes()
                if row.get("result_sha256") != hashlib.sha256(payload).hexdigest():
                    errors.append("result SHA256 differs from manifest")
                result = json.loads(payload)
                errors.extend(validate_result(result, row))
            except (ValueError, OSError, TypeError) as error:
                errors.append("missing/partial/invalid result retained: " + str(error))
        entry = {"id": identity, "anchor": cell["anchor"], "repeat": cell["repeat"], "arm": cell["arm"],
                 "profile": cell["config"]["profile"], "valid": not errors, "errors": errors,
                 "status": row.get("status") if row else "missing",
                 "result_path": row.get("result_path") if row else None}
        if isinstance(result, dict):
            entry.update({key: result.get(key) for key in ("trace_sha256", "cycle_ns", "runtime", "route", "reopened_route", "verification", "activation_stats", "phases", "retained", "files_before_close", "files_after_close", "files_before_foreground", "files_after_foreground", "stats_before", "stats_after", "stats_persisted", "stats_released", "notes")})
        cells.append(entry)
        key = (cell["anchor"], cell["config"]["profile"], cell["repeat"])
        if cell["arm"] in groups[key]:
            issues.append("duplicate planned pair position: " + identity)
        groups[key][cell["arm"]] = cell
        if not errors:
            valid[identity] = (result, row["config"])
    if any(not cell["valid"] for cell in cells):
        issues.append("one or more planned cells are failed, missing, or invalid")
    buckets = defaultdict(lambda: {"pair_ids": [], "excluded_pairs": [], "cycle": [], "phases": defaultdict(lambda: defaultdict(list)), "phase_availability": defaultdict(list)})
    for (anchor, profile, repeat), arms in sorted(groups.items()):
        for left, right in COMPARISONS:
            if left not in arms or right not in arms:
                continue
            bucket = buckets[(anchor, profile, left, right)]
            left_id, right_id = arms[left]["id"], arms[right]["id"]
            absent = [identity for identity in (left_id, right_id) if identity not in valid]
            if absent:
                bucket["excluded_pairs"].append({"repeat": repeat, "ids": [left_id, right_id], "reason": "invalid cells", "invalid_ids": absent})
                continue
            (a, acfg), (b, bcfg) = valid[left_id], valid[right_id]
            if workload(acfg) != workload(bcfg) or a["trace_sha256"].lower() != b["trace_sha256"].lower():
                reason = "workload config or trace SHA256 mismatch"
                bucket["excluded_pairs"].append({"repeat": repeat, "ids": [left_id, right_id], "reason": reason})
                issues.append(f"{anchor}/{profile}/repeat={repeat}/{left}:{right}: {reason}")
                continue
            bucket["pair_ids"].append({"repeat": repeat, "left": left_id, "right": right_id})
            bucket["cycle"].append((a["cycle_ns"], b["cycle_ns"]))
            aphases = {phase["name"]: phase for phase in a["phases"]}
            bphases = {phase["name"]: phase for phase in b["phases"]}
            for name in sorted(aphases.keys() | bphases.keys()):
                if name not in aphases or name not in bphases:
                    bucket["phase_availability"][name].append({"repeat": repeat, "left_present": name in aphases, "right_present": name in bphases})
                    continue
                pa, pb = aphases[name], bphases[name]
                if any(pa[field] != pb[field] for field in ("operations", "mutations", "output_records")):
                    mismatch = {"repeat": repeat, "left_operations": pa["operations"], "right_operations": pb["operations"],
                                "left_mutations": pa["mutations"], "right_mutations": pb["mutations"],
                                "left_output_records": pa["output_records"], "right_output_records": pb["output_records"]}
                    bucket["phase_availability"][name].append(mismatch)
                    issues.append(f"{anchor}/{profile}/repeat={repeat}/{left}:{right}/{name}: phase useful-work counts differ")
                    continue
                for metric in METRICS:
                    bucket["phases"][name][metric].append((pa[metric], pb[metric]))
                ap, bp = pa.get("process_delta", {}), pb.get("process_delta", {})
                if isinstance(ap, dict) and isinstance(bp, dict):
                    process_metrics = PROCESS_METRICS
                    if ap.get("io_available") is True and bp.get("io_available") is True:
                        process_metrics += IO_METRICS
                    for metric in process_metrics:
                        av, bv = ap.get(metric), bp.get(metric)
                        if number(av) and number(bv):
                            bucket["phases"][name]["process_delta." + metric].append((av, bv))
                for denominator in ("operations", "mutations", "output_records"):
                    if pa[denominator] > 0 and pb[denominator] > 0:
                        for metric in ("nanoseconds", "allocated_bytes", "allocations"):
                            bucket["phases"][name][metric + "_per_" + denominator].append((pa[metric] / pa[denominator], pb[metric] / pb[denominator]))
                for metric in ("p50", "p95", "p99", "max", "samples"):
                    av, bv = pa.get("latency_ns", {}).get(metric), pb.get("latency_ns", {}).get(metric)
                    if number(av) and number(bv):
                        bucket["phases"][name]["latency_ns." + metric].append((av, bv))
    comparisons = []
    for (anchor, profile, left, right), bucket in sorted(buckets.items()):
        comparisons.append({
            "anchor": anchor, "profile": profile, "left": left, "right": right,
            "ratio_definition": "per matched repeat left elapsed / right elapsed, then median; >1 means right faster",
            "minimum_final_pairs": plan.get("minimum_final_pairs", 5),
            "meets_minimum_final_pairs": len(bucket["pair_ids"]) >= plan.get("minimum_final_pairs", 5),
            "pair_ids": bucket["pair_ids"], "excluded_pairs": bucket["excluded_pairs"],
            "cycle_ns": pair_stats(bucket["cycle"]),
            "phases": {name: {metric: pair_stats(pairs) for metric, pairs in sorted(metrics.items())} for name, metrics in sorted(bucket["phases"].items())},
            "unmatched_phases": dict(bucket["phase_availability"]),
        })
    return {
        "schema_version": "orderedmem-native-summary-v1", "evidence_dir": str(root),
        "run_completeness": "incomplete" if issues else "complete",
        "qualification": "not_qualified",
        "qualification_reason": "Initial bounded calibration and descriptive paired statistics; no acceptance thresholds or final qualification declared.",
        "instrumented": plan.get("instrumented", False), "retained_diagnostics": plan.get("retained_diagnostics", False),
        "planned_cells": len(plan["cells"]), "valid_cells": len(valid), "issues": issues,
        "sources": manifest.get("sources"), "binaries": manifest.get("binaries"),
        "source_inventories": manifest.get("source_inventories"),
        "cells": cells, "comparisons": comparisons,
        "notes": [
            "No sample dropping or reruns; every planned cell and excluded pair appears above.",
            "Complete-pair results remain descriptive when the overall run is incomplete.",
            "Cycle and per-phase results are separate; no ratio of separate medians is used.",
            "Allocation/GC/heap statistics describe their recorded phase boundaries; HeapAlloc is not retained live memory.",
            "Retained diagnostics are preserved per cell and are not mixed into ordinary phase totals.",
            "Quantile comparisons are paired per-cell quantiles, not pooled operation-level percentiles.",
            "Process CPU/context-switch/I/O deltas include engine background work. I/O is paired only when both samples report io_available; kernel accounting does not prove durable-device traffic.",
            "Source SHA labels and optional file inventories record declared identities; they do not attest compiler inputs.",
            "No no-WAL Badger comparison is generated.",
        ],
    }


def markdown(summary: dict[str, Any]) -> str:
    lines = [f"Run: **{summary['run_completeness']}; not qualified**. {summary['valid_cells']}/{summary['planned_cells']} planned cells valid.", "", summary["qualification_reason"], ""]
    lines.append(f"Profiling enabled: {summary['instrumented']}; retained diagnostics enabled: {summary['retained_diagnostics']}. These diagnostic modes must not be pooled with ordinary timing.")
    for issue in summary["issues"]:
        lines.append("- " + issue)
    lines.extend(["", "Ratios are medians of matched per-repeat left/right elapsed ratios; greater than one means the right arm is faster. Observed ranges are sample ranges, not confidence intervals.", "", "| Anchor | Profile | Left / right | Boundary | Pairs | Median ratio | Observed ratio range | Right wins/ties/left wins |", "| --- | --- | --- | --- | ---: | ---: | --- | --- |"])
    for comparison in summary["comparisons"]:
        boundaries = [("cycle", comparison["cycle_ns"])]
        for name, metrics in comparison["phases"].items():
            boundaries.append((name, metrics["nanoseconds"]))
        for boundary, stats in boundaries:
            ratios = stats["paired_left_over_right"]
            ratio = f"{ratios['median']:.6f}" if ratios else "n/a"
            observed = f"{ratios['min']:.6f}–{ratios['max']:.6f}" if ratios else "n/a"
            lines.append(f"| {comparison['anchor']} | {comparison['profile']} | {comparison['left']} / {comparison['right']} | {boundary} | {stats['pairs']} | {ratio} | {observed} | {stats['right_wins']}/{stats['ties']}/{stats['left_wins']} |")
    lines.extend(["", "Per-phase allocation and GC medians below are absolute matched-sample values; ratios in the JSON are computed per pair first.", "", "| Anchor | Profile | Left / right | Phase | Bytes/op left → right | Allocations/op left → right | GC count left → right |", "| --- | --- | --- | --- | --- | --- | --- |"])
    def absolute_pair(stats: dict[str, Any] | None) -> str:
        if not stats or stats["left"] is None or stats["right"] is None:
            return "n/a"
        return f"{stats['left']['median']:.3f} → {stats['right']['median']:.3f}"
    for comparison in summary["comparisons"]:
        for name, metrics in comparison["phases"].items():
            lines.append(f"| {comparison['anchor']} | {comparison['profile']} | {comparison['left']} / {comparison['right']} | {name} | {absolute_pair(metrics.get('allocated_bytes_per_operations'))} | {absolute_pair(metrics.get('allocations_per_operations'))} | {absolute_pair(metrics.get('num_gc'))} |")
    lines.extend(["", "Per-phase heap, counts, latency quantiles, raw values, failed cells and excluded pairs are retained in the JSON summary. Foreground and cycle must be assessed separately. Allocation/op uses each phase's own operations field; zero-operation phases have n/a.", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_dir", type=Path)
    parser.add_argument("--json-out", type=Path, help="new JSON summary file")
    parser.add_argument("--markdown-out", type=Path, help="new Markdown summary file")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()
    try:
        summary = summarize(args.evidence_dir)
        if args.json_out:
            with args.json_out.open("x", encoding="utf-8") as stream:
                json.dump(summary, stream, indent=2, sort_keys=True, allow_nan=False)
                stream.write("\n")
        report = markdown(summary)
        if args.markdown_out:
            with args.markdown_out.open("x", encoding="utf-8") as stream:
                stream.write(report)
        if args.format == "json":
            json.dump(summary, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
            sys.stdout.write("\n")
        else:
            sys.stdout.write(report)
        return 1 if summary["run_completeness"] != "complete" else 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
