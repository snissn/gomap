"""Shared helpers for vector database comparison benchmarks."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np

DOCUMENT_ID = re.compile(r"doc-(\d{6})$")


def parse_ints(raw: str) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value < 1:
            raise ValueError("concurrency values must be at least 1")
        if value not in seen:
            values.append(value)
            seen.add(value)
    if not values:
        raise ValueError("at least one concurrency value is required")
    return sorted(values)


def parse_ordered_ints(raw: str) -> list[int]:
    requested = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not requested or any(value < 1 for value in requested) or len(requested) != len(set(requested)):
        raise ValueError("ordered values must be distinct positive integers")
    return requested


def load_exact_truth(dataset_dir: Path, manifest: dict[str, Any]) -> list[set[int]]:
    name = manifest.get("exact_truth_file")
    count = manifest.get("exact_truth_queries")
    top_k = manifest.get("top_k")
    file_contract = manifest.get("files", {}).get(name) if isinstance(name, str) else None
    if not isinstance(name, str) or not name or not isinstance(count, int) or count < 1 or not isinstance(top_k, int) or top_k < 1:
        raise ValueError("manifest exact truth contract is incomplete")
    path = dataset_dir / name
    raw = path.read_bytes()
    if not isinstance(file_contract, dict) or file_contract.get("bytes") != len(raw) or file_contract.get("sha256") != hashlib.sha256(raw).hexdigest():
        raise ValueError("exact truth file does not match manifest")
    rows: list[set[int]] = []
    for index, line in enumerate(raw.splitlines()):
        value = json.loads(line)
        if value.get("query_id") != f"query-{index:06d}":
            raise ValueError("exact truth query IDs are not canonical and ordered")
        ids = value.get("document_ids")
        if ids is None and isinstance(value.get("neighbors"), list):
            ids = [neighbor.get("document_id") for neighbor in value["neighbors"]]
        if not isinstance(ids, list) or len(ids) != top_k:
            raise ValueError("exact truth row has the wrong neighbor count")
        ordinals: set[int] = set()
        for document_id in ids:
            match = DOCUMENT_ID.fullmatch(document_id) if isinstance(document_id, str) else None
            if match is None:
                raise ValueError("exact truth has a noncanonical document ID")
            ordinal = int(match.group(1))
            if ordinal >= manifest.get("docs", 0):
                raise ValueError("exact truth document ID is out of range")
            ordinals.add(ordinal + 1)
        if len(ordinals) != top_k:
            raise ValueError("exact truth row has duplicate document IDs")
        rows.append(ordinals)
    if len(rows) != count:
        raise ValueError("exact truth row count does not match manifest")
    return rows


def percentile(sorted_values: list[int], p: float) -> int:
    if not sorted_values:
        return 0
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    idx = math.ceil(p * len(sorted_values)) - 1
    return sorted_values[max(0, min(idx, len(sorted_values) - 1))]


def phase(start: float) -> dict[str, Any]:
    seconds = time.perf_counter() - start
    return {"duration_nanos": int(seconds * 1_000_000_000), "seconds": seconds}


def load_vectors(path: Path, rows: int, dims: int) -> np.ndarray:
    data = np.fromfile(path, dtype="<f4")
    expected = rows * dims
    if data.size != expected:
        raise ValueError(f"{path} has {data.size} float32s, want {expected}")
    return data.reshape(rows, dims)


def max_rss_bytes() -> int | None:
    try:
        import resource

        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return int(value)
        return int(value) * 1024
    except (ImportError, OSError, AttributeError):
        return None
