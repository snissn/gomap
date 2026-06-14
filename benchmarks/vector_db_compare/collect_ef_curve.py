#!/usr/bin/env python3
"""Collect vector DB comparison EF-sweep results into CSV and Markdown."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from pathlib import Path
from typing import Any


def load_summarize_module() -> Any:
    path = Path(__file__).with_name("summarize.py")
    spec = importlib.util.spec_from_file_location("vector_db_compare_summarize", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


summarize = load_summarize_module()


CSV_FIELDS = [
    "ef_search",
    "backend",
    "backend_label",
    "search_mode",
    "concurrency",
    "recall",
    "ops_per_second",
    "avg_micros",
    "p50_micros",
    "p95_micros",
    "p99_micros",
    "top_k",
    "docs",
    "dimensions",
    "queries",
    "m",
    "ef_construction",
    "quantized_codec",
    "quantized_index_name",
    "quantized_rerank_candidates",
    "avg_candidates",
    "avg_quantized_score_calls",
    "avg_quantized_rerank_candidates",
    "avg_quantized_rerank_exact_score_calls",
    "avg_vector_bytes",
    "avg_norm_bytes",
    "result_path",
]


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"parse {path}: {exc}") from exc


def numeric(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def integer(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def rows_from_result(path: Path, result: dict[str, Any]) -> list[dict[str, Any]]:
    result_ef_search = integer(result.get("ef_search"))
    result_recall = numeric(result.get("validation", {}).get("recall"))
    rows: list[dict[str, Any]] = []
    for search_row in result.get("search_benchmarks", []):
        row_mode = summarize.search_row_mode(result, search_row)
        rows.append(
            {
                "ef_search": integer(search_row.get("ef_search"), result_ef_search),
                "backend": str(result.get("backend") or "treedb"),
                "backend_label": summarize.backend_name(result),
                "search_mode": row_mode,
                "concurrency": integer(search_row.get("concurrency")),
                "recall": numeric(search_row.get("recall"), result_recall),
                "ops_per_second": numeric(search_row.get("ops_per_second")),
                "avg_micros": numeric(search_row.get("avg_micros")),
                "p50_micros": numeric(search_row.get("p50_nanos")) / 1000,
                "p95_micros": numeric(search_row.get("p95_nanos")) / 1000,
                "p99_micros": numeric(search_row.get("p99_nanos")) / 1000,
                "top_k": integer(result.get("top_k")),
                "docs": integer(result.get("docs")),
                "dimensions": integer(result.get("dimensions")),
                "queries": integer(result.get("queries")),
                "m": integer(result.get("m")),
                "ef_construction": integer(result.get("ef_construction")),
                "quantized_codec": str(search_row.get("quantized_codec") or result.get("quantized_codec") or ""),
                "quantized_index_name": str(search_row.get("quantized_index_name") or result.get("quantized_index_name") or ""),
                "quantized_rerank_candidates": integer(
                    search_row.get("quantized_rerank_candidates", result.get("quantized_rerank_candidates"))
                ),
                "avg_candidates": numeric(search_row.get("avg_candidates")),
                "avg_quantized_score_calls": numeric(search_row.get("avg_quantized_score_calls")),
                "avg_quantized_rerank_candidates": numeric(search_row.get("avg_quantized_rerank_candidates")),
                "avg_quantized_rerank_exact_score_calls": numeric(search_row.get("avg_quantized_rerank_exact_score_calls")),
                "avg_vector_bytes": numeric(search_row.get("avg_vector_bytes")),
                "avg_norm_bytes": numeric(search_row.get("avg_norm_bytes")),
                "result_path": str(path),
            }
        )
    return rows


def discover_result_paths(run_dir: Path) -> list[Path]:
    paths = []
    for path in run_dir.glob("*.json"):
        if path.name in {"dataset_export.json", "vectorlite.stdout.json", "pgvector.stdout.json", "mongodb.stdout.json"}:
            continue
        paths.append(path)
    for path in run_dir.glob("ef*/*.json"):
        if path.name in {"dataset_export.json", "vectorlite.stdout.json", "pgvector.stdout.json", "mongodb.stdout.json"}:
            continue
        paths.append(path)
    return sorted(paths)


def collect_rows(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in discover_result_paths(run_dir):
        result = load_json(path)
        if "search_benchmarks" not in result or "validation" not in result:
            continue
        rows.extend(rows_from_result(path, result))
    return sorted(rows, key=lambda row: (row["concurrency"], row["backend_label"], row["search_mode"], row["ef_search"]))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def fmt_float(value: Any, digits: int = 4) -> str:
    return f"{numeric(value):.{digits}f}"


def render_markdown(rows: list[dict[str, Any]], run_dir: Path) -> str:
    lines = [
        "# Vector EF Curve",
        "",
        f"- run_dir: `{run_dir}`",
        f"- points: `{len(rows)}`",
        "",
    ]
    if not rows:
        lines.append("No benchmark result rows found.")
        lines.append("")
        return "\n".join(lines)

    concurrencies = sorted({integer(row["concurrency"]) for row in rows})
    for concurrency in concurrencies:
        lines.append(f"## Concurrency {concurrency}")
        lines.append("")
        lines.append("| Backend | Search mode | ef | Recall@K | Ops/sec | P95 | P99 | Avg candidates |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in [item for item in rows if integer(item["concurrency"]) == concurrency]:
            lines.append(
                "| {backend} | {mode} | {ef} | {recall} | {ops:.1f} | {p95:.0f}us | {p99:.0f}us | {candidates:.1f} |".format(
                    backend=row["backend_label"],
                    mode=row["search_mode"],
                    ef=row["ef_search"],
                    recall=fmt_float(row["recall"]),
                    ops=numeric(row["ops_per_second"]),
                    p95=numeric(row["p95_micros"]),
                    p99=numeric(row["p99_micros"]),
                    candidates=numeric(row["avg_candidates"]),
                )
            )
        lines.append("")

    lines.append("## Use")
    lines.append("")
    lines.append("- Plot `ops_per_second` against `recall`, grouped by `backend_label` and `search_mode`.")
    lines.append("- Treat `ef_search` as the curve-control knob, not as the comparison axis for claims.")
    lines.append("- Compare engines by matched recall buckets, then inspect latency and TreeDB counters.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, help="EF curve run directory containing ef*/ result subdirectories.")
    parser.add_argument("--csv", default="", help="Output CSV path. Defaults to <run-dir>/curve.csv.")
    parser.add_argument("--markdown", default="", help="Output Markdown path. Defaults to <run-dir>/curve.md.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    rows = collect_rows(run_dir)
    csv_path = Path(args.csv).expanduser().resolve() if args.csv else run_dir / "curve.csv"
    markdown_path = Path(args.markdown).expanduser().resolve() if args.markdown else run_dir / "curve.md"
    write_csv(csv_path, rows)
    markdown = render_markdown(rows, run_dir)
    markdown_path.write_text(markdown, encoding="utf-8")
    print(markdown)


if __name__ == "__main__":
    main()
