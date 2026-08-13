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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-evidence", type=Path, required=True, help="#4027 compact evidence root")
    parser.add_argument("--source-checkout", type=Path, required=True, help="clean measured-source checkout")
    parser.add_argument("--binary", type=Path, required=True, help="binary built from --source-checkout")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    campaign = args.source_evidence / "campaign.json"
    descriptor = args.source_evidence / "m3-descriptors/250k/graph-overlap-020/vector_partition_variant_v1.json"
    for path in (campaign, descriptor):
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
    ready = (
        source_head == MEASURED_SOURCE_HEAD
        and source.get("head_sha") == FROZEN_INPUT_HEAD
        and descriptor_value.get("head_sha") == FROZEN_INPUT_HEAD
        and campaign_sha == FROZEN_CAMPAIGN_SHA256
        and descriptor_sha == FROZEN_DESCRIPTOR_SHA256
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
