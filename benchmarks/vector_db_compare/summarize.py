#!/usr/bin/env python3
"""Render TreeDB vs SQLite+Vectorlite benchmark JSON as Markdown."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load(path: str) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"read benchmark result {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"parse benchmark result {path}: {exc}") from exc


def micros(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.2f}ms"
    return f"{value:.0f}us"


def bytes_human(value: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024 or unit == "PB":
            return f"{value:.2f}{unit}" if unit != "B" else f"{value:.0f}B"
        value /= 1024


def storage_bytes(result: dict[str, Any]) -> int:
    if result.get("backend") == "sqlite_vectorlite":
        return int(result["storage_after_build"]["total_bytes"])
    return int(result["storage_after_compact"]["total_bytes"])


def bytes_per_doc(result: dict[str, Any]) -> float:
    if result.get("backend") == "sqlite_vectorlite":
        return float(result["storage_after_build"]["bytes_per_doc"])
    return float(result["storage_after_compact"]["bytes_per_doc"])


def insert_seconds(result: dict[str, Any]) -> float:
    return float(result["insert"]["seconds"])


def build_seconds(result: dict[str, Any]) -> float:
    if result.get("backend") == "sqlite_vectorlite":
        return float(result["build"]["seconds"])
    return float(result["rebuild"]["seconds"])


def reopen_seconds(result: dict[str, Any]) -> float:
    return float(result["reopen_load"]["seconds"])


def recall(result: dict[str, Any]) -> float:
    return float(result["validation"]["recall"])


def search_by_concurrency(result: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {int(row["concurrency"]): row for row in result["search_benchmarks"]}


def backend_name(result: dict[str, Any]) -> str:
    if result.get("backend") == "sqlite_vectorlite":
        return "SQLite+Vectorlite HNSW"
    if result.get("backend") in (None, "treedb"):
        return "TreeDB native HNSW"
    raise ValueError(f"unknown backend {result.get('backend')!r}")


def index_memory(result: dict[str, Any]) -> str:
    memory = result.get("memory", {})
    if "index_bytes_memory" in memory:
        return bytes_human(float(memory["index_bytes_memory"]))
    return "n/a"


def process_rss(result: dict[str, Any]) -> str:
    memory = result.get("memory", {})
    if "max_rss_bytes" in memory:
        return bytes_human(float(memory["max_rss_bytes"]))
    return "n/a"


def render(treedb: dict[str, Any], vectorlite: dict[str, Any]) -> str:
    results = [treedb, vectorlite]
    lines: list[str] = []
    lines.append("# TreeDB vs SQLite+Vectorlite Vector Benchmark")
    lines.append("")
    lines.append("Both systems use persistent database files, close/reopen before validation/search, cosine distance, HNSW ANN search, and the same TreeDB-exported vectors and query set.")
    lines.append("")
    lines.append("## Build, Recall, Storage")
    lines.append("")
    lines.append("| Backend | Insert | Index build | Reopen/load | Recall@K | Storage | Storage/doc | TreeDB index memory | Python process max RSS |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        lines.append(
            "| {backend} | {insert:.3f}s | {build:.3f}s | {reopen:.3f}s | {recall:.4f} | {storage} | {per_doc:.1f}B | {index_memory} | {process_rss} |".format(
                backend=backend_name(result),
                insert=insert_seconds(result),
                build=build_seconds(result),
                reopen=reopen_seconds(result),
                recall=recall(result),
                storage=bytes_human(storage_bytes(result)),
                per_doc=bytes_per_doc(result),
                index_memory=index_memory(result),
                process_rss=process_rss(result),
            )
        )
    lines.append("")
    lines.append("## Search")
    lines.append("")
    lines.append("| Backend | Concurrency | Avg | P50 | P95 | P99 | Ops/sec |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        for row in result["search_benchmarks"]:
            lines.append(
                "| {backend} | {concurrency} | {avg} | {p50} | {p95} | {p99} | {ops:.1f} |".format(
                    backend=backend_name(result),
                    concurrency=row["concurrency"],
                    avg=micros(float(row["avg_micros"])),
                    p50=micros(float(row["p50_nanos"]) / 1000),
                    p95=micros(float(row["p95_nanos"]) / 1000),
                    p99=micros(float(row["p99_nanos"]) / 1000),
                    ops=float(row["ops_per_second"]),
                )
            )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- sqlite-vec is intentionally not used here because upstream sqlite-vec is brute-force today; ANN support is tracked as future work.")
    lines.append("- SQLite+Vectorlite stores the SQLite table and its HNSW index file under the benchmark DB directory; storage includes both.")
    lines.append("- TreeDB storage is the reopened benchmark datastore reported by `treedb_vector_search_demo`.")
    lines.append("- Memory columns are intentionally separated: TreeDB reports native vector-index memory, while SQLite+Vectorlite reports whole Python benchmark process max RSS.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--treedb", required=True)
    parser.add_argument("--vectorlite", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    text = render(load(args.treedb), load(args.vectorlite))
    Path(args.output).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
