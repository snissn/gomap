"""Write a NON-RUNNABLE draft for the worker's known fixture, no collection."""
import argparse
import itertools
from pathlib import Path
from protocol import SCHEMA, write, workload_contract, workload_metrics, metric_contract, ACK_CONTRACT, TIMED_SCOPE

def draft():
    cases = []
    for profile, layout, workload, mode in itertools.product(
            ("command_wal_durable", "command_wal_relaxed", "no_wal_fast"),
            ("inline", "pointer"), ("point", "group_all_versions", "concurrent"),
            ("cow_btree", "append_only", "btree")):
        shape = "forced_pointer" if layout == "pointer" else "inline"
        leaf = "group_versions" if workload == "group_all_versions" else workload
        work, latency = workload_metrics(workload)
        rules, comparisons = metric_contract(profile, layout, workload, mode)
        cases.append({"id": "-".join((profile, mode, shape, leaf)), "profile": profile,
            "layout": layout, "workload": workload, "mode": mode,
            "benchmark": "/".join(("BenchmarkC3PublicReadAdmission", profile, mode, shape, leaf)),
            "package": "github.com/snissn/gomap/TreeDB/mvcc", "iterations": 1024, "warmup_iterations": 128,
            "latency_groups": latency, "rules": {"baseline": rules, "candidate": rules},
            "comparable_metrics": ["point_calls/op", "scan_calls/op", "visited/op", "output/op", "wal_appends/op", "wal_syncs/op"],
            "comparison_metrics": comparisons,
            "ack_contract": ACK_CONTRACT, "timed_scope": TIMED_SCOPE,
            "workload_contract": workload_contract(workload)})
    variants = {v: {"production_commit": None, "production_git_tree": None, "source": None, "manifest": None, "manifest_sha256": None,
        "source_tree_sha256": None, "binary": None, "binary_sha256": None,
        "build_receipt": None, "build_receipt_sha256": None} for v in ("baseline", "candidate")}
    return {"schema": SCHEMA, "status": "draft-unfrozen", "coordinator_acceptance": None,
        "scope": "C3-read milestone only; no M7/C4/parent qualification", "cycles": 3,
        "order": ["baseline", "candidate", "candidate", "baseline"], "timeout_seconds": 300,
        "go_binary": None, "go_binary_sha256": None, "go_version": None, "toolchain_identity": None, "external_input_identity": None,
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
