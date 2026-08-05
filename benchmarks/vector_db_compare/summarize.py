#!/usr/bin/env python3
"""Render vector database benchmark JSON as Markdown."""

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


def storage_record(result: dict[str, Any]) -> dict[str, Any]:
    if "storage_after_build" in result:
        return result["storage_after_build"]
    if "storage_after_index_vacuum" in result:
        return result["storage_after_index_vacuum"]
    if "storage_after_close" in result:
        return result["storage_after_close"]
    if "storage_after_compact" in result:
        return result["storage_after_compact"]
    if "storage" in result:
        return result["storage"]
    raise ValueError(f"{result_label(result)} missing storage result")


def storage_bytes(result: dict[str, Any]) -> int:
    return int(storage_record(result)["total_bytes"])


def storage_display(result: dict[str, Any]) -> str:
    if storage_record(result).get("unavailable_reason"):
        return "n/a"
    label = bytes_human(storage_bytes(result))
    if storage_record(result).get("total_bytes_excludes_vector_search_index"):
        return f"{label}*"
    return label


def bytes_per_doc_display(result: dict[str, Any]) -> str:
    if storage_record(result).get("unavailable_reason"):
        return "n/a"
    return f"{float(storage_record(result)['bytes_per_doc']):.1f}B"


def insert_seconds(result: dict[str, Any]) -> float:
    return float(result["insert"]["seconds"])


def build_seconds(result: dict[str, Any]) -> float:
    if "build" in result:
        return float(result["build"]["seconds"])
    if "rebuild" in result:
        return float(result["rebuild"]["seconds"])
    raise ValueError(f"{result_label(result)} missing build/rebuild phase")


def reopen_seconds(result: dict[str, Any]) -> float:
    return float(result["reopen_load"]["seconds"])


def recall(result: dict[str, Any]) -> float:
    return float(result["validation"]["recall"])


def search_by_concurrency(result: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {int(row["concurrency"]): row for row in result["search_benchmarks"]}


def backend_name(result: dict[str, Any]) -> str:
    backend = result.get("backend")
    if backend in (None, "", "treedb"):
        return "TreeDB native HNSW"
    if backend in (
        "treedb_column_graph",
        "treedb_column_graph_quantized_only",
        "treedb_column_graph_quantized_rerank",
        "treedb_column_graph_scalar_u8_quantized_only",
        "treedb_column_graph_scalar_u8_quantized_rerank",
        "treedb_column_graph_rabitq_1bit_quantized_only",
        "treedb_column_graph_rabitq_1bit_quantized_rerank",
    ):
        return "TreeDB column-store graph HNSW"
    if backend == "sqlite_vectorlite":
        return "SQLite+Vectorlite HNSW"
    if backend == "pgvector":
        return "PostgreSQL+pgvector HNSW"
    if backend == "milvus":
        return "Milvus Standalone HNSW"
    if backend == "mongodb_vector_search":
        return "MongoDB Vector Search HNSW"
    raise ValueError(f"unknown backend {backend!r}")


def quantized_codec(result: dict[str, Any], row: dict[str, Any] | None = None) -> str:
    if row:
        codec = str(row.get("quantized_codec") or "").strip()
        if codec:
            return codec
    codec = str(result.get("quantized_codec") or "").strip()
    if codec:
        return codec
    backend = str(result.get("backend") or "")
    if "rabitq_1bit" in backend:
        return "rabitq_1bit"
    if "scalar_u8" in backend:
        return "scalar_u8"
    index_name = str(result.get("quantized_index_name") or "")
    if "rabitq_1bit" in index_name:
        return "rabitq_1bit"
    if "scalar_u8" in index_name:
        return "scalar_u8"
    if backend in ("treedb_column_graph_quantized_only", "treedb_column_graph_quantized_rerank"):
        return "scalar_u8"
    return ""


def label_quantized_mode(mode: str, codec: str) -> str:
    if mode in ("quantized_only", "quantized_rerank") and codec:
        return f"{codec} {mode}"
    return mode


def search_mode(result: dict[str, Any]) -> str:
    backend = result.get("backend")
    mode = str(result.get("query_mode") or "").strip()
    if backend in (None, "", "treedb"):
        return "exact/default" if mode in ("", "exact") else mode
    if backend == "treedb_column_graph":
        return "exact/default" if mode in ("", "exact") else mode
    if backend in ("treedb_column_graph_quantized_only", "treedb_column_graph_scalar_u8_quantized_only", "treedb_column_graph_rabitq_1bit_quantized_only"):
        return label_quantized_mode(mode or "quantized_only", quantized_codec(result))
    if backend in ("treedb_column_graph_quantized_rerank", "treedb_column_graph_scalar_u8_quantized_rerank", "treedb_column_graph_rabitq_1bit_quantized_rerank"):
        return label_quantized_mode(mode or "quantized_rerank", quantized_codec(result))
    if backend in ("pgvector", "milvus"):
        return "full-vector HNSW"
    if backend in ("sqlite_vectorlite", "mongodb_vector_search"):
        return "full-vector HNSW"
    return mode or "HNSW"


def search_row_mode(result: dict[str, Any], row: dict[str, Any]) -> str:
    backend = result.get("backend")
    if backend in ("pgvector", "milvus"):
        return "full-vector HNSW"
    mode = str(row.get("query_mode") or result.get("query_mode") or "").strip()
    if backend in (None, "", "treedb", "treedb_column_graph") and mode in ("", "exact"):
        return "exact/default"
    return label_quantized_mode(mode, quantized_codec(result, row)) or search_mode(result)


def result_label(result: dict[str, Any]) -> str:
    backend = result.get("backend")
    if backend:
        return f"result backend={backend!r}"
    dataset = result.get("dataset_dir")
    if dataset:
        return f"result dataset_dir={dataset!r}"
    return "result"


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


def render(results: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# Vector Database Benchmark")
    lines.append("")
    lines.append("All reported systems use persistent database files or server-side durable storage, close/reopen or reconnect before validation/search, cosine distance, HNSW ANN search, and the same TreeDB-exported vectors and query set. TreeDB quantized rows are explicit TreeDB column_graph query modes with named scalar_u8 or rabitq_1bit score planes; PostgreSQL+pgvector and Milvus remain full-vector HNSW anchors.")
    lines.append("")
    lines.append("## Build, Recall, Storage")
    lines.append("")
    lines.append("| Backend | Search mode | Insert | Index build | Reopen/load | Recall@K | Storage | Storage/doc | TreeDB index memory | Process max RSS |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        lines.append(
            "| {backend} | {mode} | {insert:.3f}s | {build:.3f}s | {reopen:.3f}s | {recall:.4f} | {storage} | {per_doc} | {index_memory} | {process_rss} |".format(
                backend=backend_name(result),
                mode=search_mode(result),
                insert=insert_seconds(result),
                build=build_seconds(result),
                reopen=reopen_seconds(result),
                recall=recall(result),
                storage=storage_display(result),
                per_doc=bytes_per_doc_display(result),
                index_memory=index_memory(result),
                process_rss=process_rss(result),
            )
        )
    lines.append("")
    lines.append("## Search")
    lines.append("")
    lines.append("| Backend | Search mode | Concurrency | Avg | P50 | P95 | P99 | Ops/sec |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        for row in result["search_benchmarks"]:
            lines.append(
                "| {backend} | {mode} | {concurrency} | {avg} | {p50} | {p95} | {p99} | {ops:.1f} |".format(
                    backend=backend_name(result),
                    mode=search_row_mode(result, row),
                    concurrency=row["concurrency"],
                    avg=micros(float(row["avg_micros"])),
                    p50=micros(float(row["p50_nanos"]) / 1000),
                    p95=micros(float(row["p95_nanos"]) / 1000),
                    p99=micros(float(row["p99_nanos"]) / 1000),
                    ops=float(row["ops_per_second"]),
                )
            )
    lines.append("")
    counter_fields = [
        "avg_candidates",
        "avg_quantized_score_calls",
        "avg_quantized_code_bytes",
        "avg_quantized_rerank_candidates",
        "avg_quantized_rerank_exact_score_calls",
        "avg_vector_bytes",
        "avg_norm_bytes",
    ]
    counter_rows = [
        (result, row)
        for result in results
        if str(result.get("backend") or "treedb").startswith("treedb")
        for row in result.get("search_benchmarks", [])
        if any(field in row for field in counter_fields)
    ]
    if counter_rows:
        lines.append("## TreeDB Search Counters")
        lines.append("")
        lines.append("| Backend | Search mode | Concurrency | Avg candidates | Avg quantized score calls | Avg quantized code bytes | Avg quantized rerank candidates | Avg exact rerank score calls | Avg vector bytes | Avg norm bytes |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for result, row in counter_rows:
            lines.append(
                "| {backend} | {mode} | {concurrency} | {candidates:.1f} | {qscore:.1f} | {qbytes:.1f} | {qrerank:.1f} | {qexact:.1f} | {vbytes:.1f} | {nbytes:.1f} |".format(
                    backend=backend_name(result),
                    mode=search_row_mode(result, row),
                    concurrency=row["concurrency"],
                    candidates=float(row.get("avg_candidates", 0)),
                    qscore=float(row.get("avg_quantized_score_calls", 0)),
                    qbytes=float(row.get("avg_quantized_code_bytes", 0)),
                    qrerank=float(row.get("avg_quantized_rerank_candidates", 0)),
                    qexact=float(row.get("avg_quantized_rerank_exact_score_calls", 0)),
                    vbytes=float(row.get("avg_vector_bytes", 0)),
                    nbytes=float(row.get("avg_norm_bytes", 0)),
                )
            )
        lines.append("")
    guardrail_fields = [
        "avg_documents_fetched",
        "avg_response_owned_result_allocs",
        "avg_search_route_hnsw_search_pack",
        "avg_search_route_quantized_only",
        "avg_search_route_quantized_rerank",
        "avg_search_route_column_graph_prepared",
        "avg_search_route_column_graph_fallback",
        "avg_graph_row_fallbacks",
        "avg_typed_column_fallbacks",
        "avg_vector_scratch_decodes",
    ]
    guardrail_rows = [
        (result, row)
        for result in results
        if str(result.get("backend") or "treedb").startswith("treedb")
        for row in result.get("search_benchmarks", [])
        if any(field in row for field in guardrail_fields)
    ]
    if guardrail_rows:
        lines.append("## TreeDB Search Guardrails")
        lines.append("")
        lines.append("| Backend | Search mode | Concurrency | Avg docs fetched | Avg response-owned allocs | Route hnsw_pack | Route qonly | Route qrerank | Route prepared | Route fallback | Graph fallbacks | Typed-column fallbacks | Vector scratch decodes |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for result, row in guardrail_rows:
            lines.append(
                "| {backend} | {mode} | {concurrency} | {docs:.1f} | {owned:.1f} | {hnsw:.1f} | {qonly:.1f} | {qrerank:.1f} | {prepared:.1f} | {fallback:.1f} | {graph:.1f} | {typed:.1f} | {scratch:.1f} |".format(
                    backend=backend_name(result),
                    mode=search_row_mode(result, row),
                    concurrency=row["concurrency"],
                    docs=float(row.get("avg_documents_fetched", 0)),
                    owned=float(row.get("avg_response_owned_result_allocs", 0)),
                    hnsw=float(row.get("avg_search_route_hnsw_search_pack", 0)),
                    qonly=float(row.get("avg_search_route_quantized_only", 0)),
                    qrerank=float(row.get("avg_search_route_quantized_rerank", 0)),
                    prepared=float(row.get("avg_search_route_column_graph_prepared", 0)),
                    fallback=float(row.get("avg_search_route_column_graph_fallback", 0)),
                    graph=float(row.get("avg_graph_row_fallbacks", 0)),
                    typed=float(row.get("avg_typed_column_fallbacks", 0)),
                    scratch=float(row.get("avg_vector_scratch_decodes", 0)),
                )
            )
        lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- sqlite-vec is intentionally not used here because upstream sqlite-vec is brute-force today; ANN support is tracked as future work.")
    lines.append("- SQLite+Vectorlite stores the SQLite table and its HNSW index file under the benchmark DB directory; storage includes both.")
    lines.append("- PostgreSQL+pgvector storage uses the benchmark table's `pg_total_relation_size`, including its full-vector HNSW index.")
    lines.append("- PostgreSQL+pgvector is not quantized by this harness: no halfvec, binary quantize, SQL rerank, byte-code scoring, or custom operator class is used.")
    lines.append("- Milvus storage includes the standalone server, etcd, and MinIO files under the supplied compose data directory.")
    lines.append("- MongoDB is included only when run against a MongoDB Vector Search deployment, such as Atlas or local Atlas with `mongot`; plain `mongod` is not a vector-search comparator.")
    lines.append("- MongoDB storage marked with `*` uses `collStats` collection storage and ordinary index bytes; MongoDB Vector Search index bytes are not exposed by this harness.")
    lines.append("- TreeDB storage uses the post-close, post-index-vacuum retained datastore when reported by `treedb_vector_search_demo`; raw pre-close and pre-vacuum storage fields remain in the JSON.")
    lines.append("- TreeDB quantized rows declare a named quantized score plane (`scalar_u8` or `rabitq_1bit`) and select it through explicit `query_mode`; exact/default TreeDB rows do not declare or use quantized assets.")
    lines.append("- Memory columns are intentionally separated: TreeDB reports native vector-index memory when available, while Python-backed comparator harnesses report whole benchmark process max RSS.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", action="append", default=[], help="Benchmark result JSON. Can be repeated.")
    parser.add_argument("--treedb", help="Legacy TreeDB result JSON")
    parser.add_argument("--vectorlite", help="Legacy SQLite+Vectorlite result JSON")
    parser.add_argument("--pgvector", help="Legacy PostgreSQL+pgvector result JSON")
    parser.add_argument("--mongodb", help="Legacy MongoDB Vector Search result JSON")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    paths = list(args.result)
    for legacy in [args.treedb, args.vectorlite, args.pgvector, args.mongodb]:
        if legacy:
            paths.append(legacy)
    if not paths:
        raise SystemExit("at least one input is required: pass --result, or one of --treedb/--vectorlite/--pgvector/--mongodb")
    deduped = []
    seen = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    paths = deduped
    try:
        text = render([load(path) for path in paths])
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        raise SystemExit(f"invalid benchmark result: {exc}") from exc
    Path(args.output).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
