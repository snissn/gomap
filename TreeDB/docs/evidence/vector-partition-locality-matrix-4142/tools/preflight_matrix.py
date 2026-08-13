#!/usr/bin/env python3
"""Bind a #4142 run to current source and frozen input descriptors."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-evidence", type=Path, required=True, help="#4027 compact evidence root")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    campaign = args.source_evidence / "campaign.json"
    descriptor = args.source_evidence / "m3-descriptors/250k/graph-overlap-020/vector_partition_variant_v1.json"
    for path in (campaign, descriptor):
        if not path.is_file():
            raise SystemExit(f"missing frozen input: {path}")
    current = subprocess.check_output(("git", "rev-parse", "HEAD"), text=True).strip()
    source = json.loads(campaign.read_text(encoding="utf-8"))
    descriptor_value = json.loads(descriptor.read_text(encoding="utf-8"))
    result = {
        "schema_version": 1,
        "result_kind": "vector_partition_locality_matrix_preflight_v1",
        "current_head": current,
        "frozen_head": source["head_sha"],
        "campaign_sha256": digest(campaign),
        "descriptor_sha256": digest(descriptor),
        "descriptor_head": descriptor_value.get("head_sha"),
        "status": "ready" if current == source["head_sha"] == descriptor_value.get("head_sha") else "blocked_source_identity",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if result["status"] != "ready":
        raise SystemExit("source identity differs from frozen qualification; rebuild exact assets before matrix execution")


if __name__ == "__main__":
    main()
