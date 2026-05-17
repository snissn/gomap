"""Shared helpers for vector database comparison benchmarks."""

from __future__ import annotations

import math
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


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
