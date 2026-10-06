"""Write a NON-RUNNABLE draft for the worker's known fixture, no collection."""
import argparse
import itertools
from pathlib import Path
from protocol import SCHEMA, write

def draft():
    cases = []
    for profile, layout, workload, mode in itertools.product(
            ("command_wal_durable", "command_wal_relaxed", "no_wal_fast"),
            ("inline", "pointer"), ("point", "group_all_versions", "concurrent"),
            ("cow_btree", "append_only", "btree")):
        shape = "forced_pointer" if layout == "pointer" else "inline"
        leaf = "group_versions" if workload == "group_all_versions" else workload
        point = 0 if workload == "group_all_versions" else 1
        scan = 0 if workload == "point" else 1
        latency = ["writer"] + (["point"] if point else []) + (["scan"] if scan else [])
        rules = {"ns/op": {"min": 1}, "B/op": {"min": 1}, "allocs/op": {"min": 1},
                 "writer_ops/s": {"min": 0.000001}, "reader_ops/s": {"min": 0.000001},
                 "point_calls/op": {"eq": point}, "scan_calls/op": {"eq": scan},
                 "visited/op": {"eq": 2 * scan}, "output/op": {"eq": point + 2 * scan},
                 "readers_while_writer_active/op": {"min": 0, "max": 2} if workload == "concurrent" else {"eq": 0},
                 "wal_appends/op": {"eq": 0 if profile == "no_wal_fast" else 1},
                 "wal_syncs/op": {"eq": 1 if profile == "command_wal_durable" else 0},
                 "close_ok": {"eq": 1}}
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
        cases.append({"id": "-".join((profile, mode, shape, leaf)), "profile": profile,
            "layout": layout, "workload": workload, "mode": mode,
            "benchmark": "/".join(("BenchmarkC3PublicReadAdmission", profile, mode, shape, leaf)),
            "package": "github.com/snissn/gomap/TreeDB/mvcc", "iterations": 1024, "warmup_iterations": 128,
            "latency_groups": latency, "rules": {"baseline": rules, "candidate": rules},
            "comparable_metrics": ["point_calls/op", "scan_calls/op", "visited/op", "output/op", "wal_appends/op", "wal_syncs/op"],
            "comparison_metrics": comparisons,
            "ack_contract": "CommitRelaxed; durable WAL append/sync=1/1, relaxed=1/0, NoWAL=0/0; exact profile counts required",
            "timed_scope": "actual calls, owned point output, borrowed complete EntryView scan, validation, clocks; seed/stats/Close excluded",
            "workload_contract": {"keys": ["c3-a", "c3-ab"], "seed_timestamps": [10, 20], "read_timestamp": 100,
                "value_bytes": 256, "value_byte": 99, "history_growth": False,
                "writers": 1, "point_readers": point, "scan_readers": scan,
                "writer_records_per_call": 1 if workload == "point" else 4,
                "concurrency": workload == "concurrent", "clock_calls_retained": True,
                "read_overlap_is_internal_preparation_proof": False}})
    variants = {v: {"production_commit": None, "production_git_tree": None, "source": None, "manifest": None, "manifest_sha256": None,
        "source_tree_sha256": None, "binary": None, "binary_sha256": None,
        "build_receipt": None, "build_receipt_sha256": None} for v in ("baseline", "candidate")}
    return {"schema": SCHEMA, "status": "draft-unfrozen", "coordinator_acceptance": None,
        "scope": "C3-read milestone only; no M7/C4/parent qualification", "cycles": 3,
        "order": ["baseline", "candidate", "candidate", "baseline"], "timeout_seconds": 300,
        "go_binary": None, "go_binary_sha256": None, "go_version": None,
        "environment": {"GOMAXPROCS": "4", "GOWORK": "off", "GOROOT": None,
            "GOGC": "100", "GOMEMLIMIT": "off", "GOFLAGS": "", "GOCACHE": None, "GOMODCACHE": None, "TMPDIR": None},
        "host": {"system": "Linux", "node": None, "machine": "x86_64", "release": None,
            "cpu_count": None, "max_load1": None, "max_load5": None, "min_free_bytes": None, "tmpdir": None, "tmpdir_device": None},
        "noise_policy": {"max_spread_fraction": None, "material_regression_fraction": None,
            "minimum_effect_fraction": None, "exclusions": "none; retain and stop on contamination"},
        "comparison_metrics": ["ns/op", "B/op", "allocs/op", "writer_ops/s", "reader_ops/s"],
        "fixtures": [{"path": "TreeDB/mvcc/cow_c3_public_bench_test.go", "sha256": None}],
        "variants": variants, "cases": cases}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    write(args.out, draft())
