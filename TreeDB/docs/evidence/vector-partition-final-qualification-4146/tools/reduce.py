#!/usr/bin/env python3
"""Fail-closed reducer for the bounded #4146 TreeDB-only matrix."""

import argparse
import hashlib
import json
from pathlib import Path
import statistics

CORPORA = ("100k", "250k")
EFS = (64, 80, 96, 128)
CONCURRENCY = (1, 32)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reduce_matrix(root):
    inputs, corpora = {}, {}
    for corpus in CORPORA:
        curve, corpus_identity = {}, None
        for ef in EFS:
            cells = {c: [] for c in CONCURRENCY}
            for repetition in (1, 2, 3):
                path = root / corpus / f"repeat-{repetition}" / f"search-ef{ef}.json"
                value = json.loads(path.read_text(encoding="utf-8"))
                inputs[str(path.relative_to(root))] = sha256(path)
                identity = (value.get("dataset_checksum"), value.get("truth_artifact_sha256"))
                if (value.get("schema_version") != 1 or
                        value.get("result_kind") != "vector_partition_system_bench_v1" or
                        value.get("top_k") != 10 or value.get("ef_search") != ef or
                        value.get("warmup_queries") != 1000 or value.get("search_mode") != "strict" or
                        not all(isinstance(item, str) and item for item in identity) or
                        corpus_identity not in (None, identity) or len(value.get("cells", [])) != 2):
                    raise ValueError(f"invalid report identity: {path}")
                corpus_identity = identity
                seen = set()
                for cell in value["cells"]:
                    concurrency = cell.get("concurrency")
                    metrics, counters = cell.get("metrics", {}), cell.get("counters", {})
                    if (concurrency not in CONCURRENCY or concurrency in seen or
                            cell.get("status") != "valid" or metrics.get("queries") != 1000 or
                            metrics.get("completed_queries") != 1000 or metrics.get("errors") != 0 or
                            metrics.get("timeouts") != 0 or counters.get("retries") != 0 or
                            counters.get("redirects") != 0 or cell.get("budget", {}).get("probes") != 2):
                        raise ValueError(f"invalid cell: {path}")
                    seen.add(concurrency)
                    cells[concurrency].append(cell)
                if seen != set(CONCURRENCY):
                    raise ValueError(f"incomplete concurrency coordinates: {path}")
            row = {"concurrency": {}}
            invariant = None
            for concurrency, values in cells.items():
                signatures = {(v["metrics"]["recall_at_10"], v["counters"]["candidates"],
                               v["counters"]["edges"], v["counters"]["requests"],
                               v["counters"]["selected_partitions"], v["counters"]["selected_groups"])
                              for v in values}
                if len(signatures) != 1 or invariant not in (None, next(iter(signatures))):
                    raise ValueError(f"deterministic work drift: {corpus} EF{ef}")
                invariant = next(iter(signatures))
                metrics = values[0]["metrics"]
                row["recall_at_10"] = metrics["recall_at_10"]
                row["candidates"], row["edges"] = invariant[1], invariant[2]
                row["requests"], row["selected_partitions"], row["selected_groups"] = invariant[3:]
                row["concurrency"][str(concurrency)] = {
                    key: statistics.median(v["metrics"][source] for v in values)
                    for key, source in (("qps", "qps"), ("p50_nanos", "p50_nanos"),
                                        ("p95_nanos", "p95_nanos"), ("p99_nanos", "p99_nanos"))
                }
                row["concurrency"][str(concurrency)]["qps_min"] = min(v["metrics"]["qps"] for v in values)
                row["concurrency"][str(concurrency)]["qps_max"] = max(v["metrics"]["qps"] for v in values)
            curve[str(ef)] = row
        corpora[corpus] = {"selected_ef": 96, "selected": curve["96"], "curve": curve}
    return {
        "schema_version": 1,
        "result_kind": "treedb_vector_partition_final_qualification_4146_v1",
        "source_revision": "939c71b6357c41d569af681fd7b95aea705978a4",
        "selected_ef": 96,
        "selected_probes": 2,
        "gates": {
            "recall_at_10_gte_0_9500": corpora["250k"]["selected"]["recall_at_10"] >= .95,
            "c1_qps_gte_2000": corpora["250k"]["selected"]["concurrency"]["1"]["qps"] >= 2000,
            "c32_qps_gte_9000": corpora["250k"]["selected"]["concurrency"]["32"]["qps"] >= 9000,
            "c1_p95_lte_700000": corpora["250k"]["selected"]["concurrency"]["1"]["p95_nanos"] <= 700000,
            "c32_p95_lte_7250000": corpora["250k"]["selected"]["concurrency"]["32"]["p95_nanos"] <= 7250000,
            "zero_query_failures": True,
        },
        "corpora": corpora,
        "inputs": inputs,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = reduce_matrix(args.runs)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
