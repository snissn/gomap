#!/usr/bin/env python3
"""Bind a #4142 run to current source and frozen input descriptors."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


MEASURED_SOURCE_HEAD = "21a57f937f88ff7b3b2746848efa40433a84389d"
FROZEN_INPUT_HEAD = "eed54bc0b9ec3b705e9170be26ab069bdc9b9771"
FROZEN_CAMPAIGN_SHA256 = "c20f11bb38898fd0d5907330bec3df80db29df14e44962a8766502e757849aa2"
FROZEN_DESCRIPTOR_SHA256 = "e840bfc02328be416356b4ea080d3aa2381fb7a4388341313e04ea970c41ec4c"
FROZEN_DATASET_SHA256 = "14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025"
FROZEN_TRUTH_SHA256 = "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e"
FROZEN_GRAPH_SHA256 = "57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d"
FROZEN_CALIBRATION_SHA256 = "077ec68492638dfe4f3cd589e125a769149130666533491e50143767f28ea46f"
FROZEN_HOLDOUT_SHA256 = "b25cc80df7d03294949f3ce3ef70f14e10692d1127d14e45b9081e07e8196e28"
FROZEN_QUERY_UNION_SHA256 = "b1c32e2197c96b83093960b247b9a8eac730c9527f14fa7691c116b77d679a63"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_vcs(binary: Path) -> dict[str, str]:
    output = subprocess.check_output(("go", "version", "-m", str(binary)), text=True)
    values: dict[str, str] = {}
    for line in output.splitlines():
        field = line.strip().removeprefix("build\t")
        if field.startswith("vcs.") and "=" in field:
            key, value = field.split("=", 1)
            values[key] = value
    return values


def query_union(calibration: Path, holdout: Path) -> str:
    values = [json.loads(path.read_text(encoding="utf-8")) for path in (calibration, holdout)]
    if any(value.get("schema") != "vector_partition_4105_query_split_v1" for value in values):
        raise SystemExit("query split schema is invalid")
    ordinals = [ordinal for value in values for ordinal in value.get("ordinals", [])]
    if sorted(ordinals) != list(range(1000)):
        raise SystemExit("query splits are not a complete disjoint frozen union")
    canonical = json.dumps(sorted(ordinals), separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-evidence", type=Path, required=True, help="#4027 compact evidence root")
    parser.add_argument("--source-checkout", type=Path, required=True, help="clean measured-source checkout")
    parser.add_argument("--binary", type=Path, required=True, help="binary built from --source-checkout")
    parser.add_argument("--dataset-manifest", type=Path, required=True)
    parser.add_argument("--truth-artifact", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, required=True)
    parser.add_argument("--holdout", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    campaign = args.source_evidence / "campaign.json"
    descriptor = args.source_evidence / "m3-descriptors/250k/graph-overlap-020/vector_partition_variant_v1.json"
    for path in (campaign, descriptor, args.dataset_manifest, args.truth_artifact, args.calibration, args.holdout):
        if not path.is_file():
            raise SystemExit(f"missing frozen input: {path}")
    source_head = subprocess.check_output(("git", "-C", str(args.source_checkout), "rev-parse", "HEAD"), text=True).strip()
    source_dirty = subprocess.check_output(("git", "-C", str(args.source_checkout), "status", "--porcelain=v1"), text=True).strip()
    if source_dirty:
        raise SystemExit("measured source checkout is dirty")
    if not args.binary.is_file():
        raise SystemExit(f"missing measured binary: {args.binary}")
    source = json.loads(campaign.read_text(encoding="utf-8"))
    descriptor_value = json.loads(descriptor.read_text(encoding="utf-8"))
    vcs = build_vcs(args.binary)
    campaign_sha = digest(campaign)
    descriptor_sha = digest(descriptor)
    dataset_sha = digest(args.dataset_manifest)
    truth_sha = digest(args.truth_artifact)
    calibration_sha = digest(args.calibration)
    holdout_sha = digest(args.holdout)
    query_union_sha = query_union(args.calibration, args.holdout)
    ready = (
        source_head == MEASURED_SOURCE_HEAD
        and source.get("head_sha") == FROZEN_INPUT_HEAD
        and descriptor_value.get("head_sha") == FROZEN_INPUT_HEAD
        and campaign_sha == FROZEN_CAMPAIGN_SHA256
        and descriptor_sha == FROZEN_DESCRIPTOR_SHA256
        and dataset_sha == FROZEN_DATASET_SHA256
        and truth_sha == FROZEN_TRUTH_SHA256
        and calibration_sha == FROZEN_CALIBRATION_SHA256
        and holdout_sha == FROZEN_HOLDOUT_SHA256
        and query_union_sha == FROZEN_QUERY_UNION_SHA256
        and descriptor_value.get("fixture_checksum") == "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69"
        and descriptor_value.get("graph_artifact_sha256") == FROZEN_GRAPH_SHA256
        and vcs.get("vcs.revision") == source_head
        and vcs.get("vcs.modified") == "false"
    )
    result = {
        "schema_version": 1,
        "result_kind": "vector_partition_locality_matrix_preflight_v1",
        "source_head": source_head,
        "binary_sha256": digest(args.binary),
        "frozen_head": source["head_sha"],
        "campaign_sha256": campaign_sha,
        "descriptor_sha256": descriptor_sha,
        "descriptor_head": descriptor_value.get("head_sha"),
        "dataset_sha256": dataset_sha,
        "truth_sha256": truth_sha,
        "graph_sha256": descriptor_value.get("graph_artifact_sha256"),
        "query_union_sha256": query_union_sha,
        "binary_vcs_revision": vcs.get("vcs.revision"),
        "binary_vcs_modified": vcs.get("vcs.modified"),
        "status": "ready" if ready else "blocked_source_identity",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if result["status"] != "ready":
        raise SystemExit("source identity differs from #4140 measured source; rebuild exact assets before matrix execution")


if __name__ == "__main__":
    main()
