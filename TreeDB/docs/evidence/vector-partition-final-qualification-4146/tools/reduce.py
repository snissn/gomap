#!/usr/bin/env python3
"""Fail-closed reducer for the bounded #4146 TreeDB-only matrix."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics

CORPORA = ("100k", "250k")
EFS = (64, 80, 96, 128)
CONCURRENCY = (1, 32)
CORPUS_IDENTITIES = {
    "100k": ("ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95",
             "0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c"),
    "250k": ("d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69",
             "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e"),
}
ASSET_IDENTITIES = {
    "100k": {"artifact_sha256": "3916da3febc7c5a1ecad39488ee259e63103eda1dc6b27231a530e2169e9808b",
             "manifest_integrity_digest": "3e394c80c4a5c4cb422b7d0d089a5f411bdd0af3d89ab6cafed6a2920a831cdd",
             "ready_set_digest": "523b1cc40138714b270c8c299c285bd2b223753f60447b41f605fe19e8431bb3",
             "partitions": 16},
    "250k": {"artifact_sha256": "b07ab6272598447ee517d41665305af776ba806bb94033046b687e283a786040",
             "manifest_integrity_digest": "3d9409fa68c5264491071eea8e2dad4e0b8a090dc05d7bed83784821237b529f",
             "ready_set_digest": "5b5035c84952111ddaa95da92fbb919f87a60bbccfeff02faea03610fd4ffd1e",
             "partitions": 40},
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def is_hex(value, length):
    return isinstance(value, str) and len(value) == length and all(character in "0123456789abcdef" for character in value)


def topology_digest(value):
    canonical = dict(value)
    canonical["topology_identity_sha256"] = ""
    raw = json.dumps(canonical, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(raw).hexdigest()


def reduce_matrix(root):
    inputs, corpora, build_identity, generation_identity = {}, {}, None, None
    for corpus in CORPORA:
        curve, corpus_identity = {}, None
        for ef in EFS:
            cells = {c: [] for c in CONCURRENCY}
            report_shas = set()
            for repetition in (1, 2, 3):
                run = root / corpus / f"repeat-{repetition}"
                ready_path, topology_path = run / "state/ready.json", run / "topology.json"
                descriptor_path = run / "database/vector_partition_variant_v1.json"
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
                topology = json.loads(topology_path.read_text(encoding="utf-8"))
                descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
                candidate_build = (ready.get("source_revision"), ready.get("executable_sha256"))
                if (ready.get("result_kind") != "vector_partition_system_node_ready_v1" or
                        ready.get("vcs_modified") is not False or not is_hex(candidate_build[0], 40) or
                        not is_hex(candidate_build[1], 64) or build_identity not in (None, candidate_build) or
                        topology.get("result_kind") != "vector_partition_system_topology_v1" or
                        not is_hex(topology.get("topology_identity_sha256"), 64) or
                        topology_digest(topology) != topology["topology_identity_sha256"] or
                        descriptor.get("schema_version") != 6 or
                        descriptor.get("result_kind") != "m3_persistent_variant_descriptor_v6" or
                        descriptor.get("fixture_checksum") != CORPUS_IDENTITIES[corpus][0] or
                        any(descriptor.get(key) != expected for key, expected in ASSET_IDENTITIES[corpus].items())):
                    raise ValueError(f"invalid run provenance: {run}")
                build_identity = candidate_build
                inputs[str(ready_path.relative_to(root))] = sha256(ready_path)
                inputs[str(topology_path.relative_to(root))] = sha256(topology_path)
                inputs[str(descriptor_path.relative_to(root))] = sha256(descriptor_path)
                path = root / corpus / f"repeat-{repetition}" / f"search-ef{ef}.json"
                value = json.loads(path.read_text(encoding="utf-8"))
                report_sha = sha256(path)
                inputs[str(path.relative_to(root))] = report_sha
                identity = (value.get("dataset_checksum"), value.get("truth_artifact_sha256"))
                if (value.get("schema_version") != 1 or
                        value.get("result_kind") != "vector_partition_system_bench_v1" or
                        value.get("top_k") != 10 or value.get("ef_search") != ef or
                        value.get("warmup_queries") != 1000 or value.get("search_mode") != "strict" or
                        not all(isinstance(item, str) and item for item in identity) or
                        identity != CORPUS_IDENTITIES[corpus] or report_sha in report_shas or
                        value.get("topology_identity_sha256") != topology["topology_identity_sha256"] or
                        corpus_identity not in (None, identity) or len(value.get("cells", [])) != 2):
                    raise ValueError(f"invalid report identity: {path}")
                report_shas.add(report_sha)
                corpus_identity = identity
                seen = set()
                for cell in value["cells"]:
                    concurrency = cell.get("concurrency")
                    metrics, counters = cell.get("metrics", {}), cell.get("counters", {})
                    numeric = [metrics.get(key) for key in
                               ("recall_at_10", "qps", "p50_nanos", "p95_nanos", "p99_nanos")]
                    generation_value = cell.get("generation")
                    generation = json.dumps(generation_value, sort_keys=True, separators=(",", ":"))
                    if (concurrency not in CONCURRENCY or concurrency in seen or
                            cell.get("status") != "valid" or metrics.get("queries") != 1000 or
                            metrics.get("completed_queries") != 1000 or metrics.get("errors") != 0 or
                            metrics.get("timeouts") != 0 or counters.get("retries") != 0 or
                            counters.get("redirects") != 0 or cell.get("budget", {}).get("probes") != 2 or
                            any(isinstance(item, bool) or not isinstance(item, (int, float)) or
                                not math.isfinite(item) for item in numeric) or
                            not 0 <= numeric[0] <= 1 or any(item < 0 for item in numeric[1:]) or
                            not numeric[2] <= numeric[3] <= numeric[4] or
                            generation_value != {"Index": "embedding_graph", "Generation": 1} or
                            generation_identity not in (None, generation)):
                        raise ValueError(f"invalid cell: {path}")
                    generation_identity = generation
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
        "source_revision": build_identity[0],
        "executable_sha256": build_identity[1],
        "generation": json.loads(generation_identity),
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
