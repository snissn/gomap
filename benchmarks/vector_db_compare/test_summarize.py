#!/usr/bin/env python3
"""Focused tests for vector DB comparison summary rendering."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


_SPEC = importlib.util.spec_from_file_location("summarize", Path(__file__).with_name("summarize.py"))
assert _SPEC is not None and _SPEC.loader is not None
summarize = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(summarize)


def result(backend: str, query_mode: str = "", quantized_codec: str = "") -> dict:
    row = {
        "concurrency": 1,
        "queries": 4,
        "query_mode": query_mode,
        "quantized_codec": quantized_codec,
        "total_duration_nanos": 1000,
        "avg_nanos": 250,
        "avg_micros": 0.25,
        "ops_per_second": 4000,
        "p50_nanos": 200,
        "p95_nanos": 300,
        "p99_nanos": 400,
        "avg_candidates": 32,
        "avg_quantized_score_calls": 0,
        "avg_quantized_code_bytes": 0,
        "avg_quantized_rerank_candidates": 0,
        "avg_quantized_rerank_exact_score_calls": 0,
        "avg_vector_bytes": 128,
        "avg_norm_bytes": 32,
        "avg_documents_fetched": 0,
        "avg_response_owned_result_allocs": 0,
        "avg_search_route_hnsw_search_pack": 1,
        "avg_search_route_quantized_only": 0,
        "avg_search_route_quantized_rerank": 0,
        "avg_search_route_column_graph_prepared": 0,
        "avg_search_route_column_graph_fallback": 0,
        "avg_graph_row_fallbacks": 0,
        "avg_typed_column_fallbacks": 0,
        "avg_vector_scratch_decodes": 0,
    }
    if query_mode == "quantized_only":
        row.update(
            {
                "avg_quantized_score_calls": 40,
                "avg_quantized_code_bytes": 5120,
                "avg_vector_bytes": 0,
                "avg_norm_bytes": 0,
                "avg_search_route_hnsw_search_pack": 0,
                "avg_search_route_quantized_only": 1,
            }
        )
    if query_mode == "quantized_rerank":
        row.update(
            {
                "avg_quantized_score_calls": 40,
                "avg_quantized_code_bytes": 5120,
                "avg_quantized_rerank_candidates": 16,
                "avg_quantized_rerank_exact_score_calls": 16,
                "avg_search_route_hnsw_search_pack": 0,
                "avg_search_route_quantized_rerank": 1,
            }
        )
    return {
        "backend": backend,
        "engine": backend,
        "query_mode": query_mode,
        "quantized_codec": quantized_codec,
        "docs": 64,
        "dimensions": 8,
        "queries": 4,
        "top_k": 3,
        "insert": {"seconds": 0.1},
        "rebuild": {"seconds": 0.2},
        "reopen_load": {"seconds": 0.03},
        "validation": {"recall": 0.95},
        "search_benchmarks": [row],
        "storage_after_index_vacuum": {"total_bytes": 4096, "bytes_per_doc": 64, "domains": {}, "files": 1},
        "memory": {"index_bytes_memory": 1024},
    }


class SummaryRenderTests(unittest.TestCase):
    def test_treedb_quantized_modes_and_pgvector_anchor_are_distinct(self) -> None:
        rendered = summarize.render(
            [
                result("treedb_column_graph", "exact"),
                result("treedb_column_graph_scalar_u8_quantized_only", "quantized_only", "scalar_u8"),
                result("treedb_column_graph_rabitq_1bit_quantized_only", "quantized_only", "rabitq_1bit"),
                result("treedb_column_graph_rabitq_1bit_quantized_rerank", "quantized_rerank", "rabitq_1bit"),
                {
                    **result("pgvector"),
                    "build": {"seconds": 0.25},
                    "storage_after_build": {"total_bytes": 8192, "bytes_per_doc": 128, "domains": {}, "files": 0},
                },
                {
                    **result("milvus"),
                    "build": {"seconds": 0.3},
                    "storage_after_build": {"total_bytes": 0, "bytes_per_doc": 0, "domains": {}, "files": 0, "unavailable_reason": "external service"},
                },
            ]
        )
        self.assertIn("| Backend | Search mode | Insert |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | exact/default |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | scalar_u8 quantized_only |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | rabitq_1bit quantized_only |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | rabitq_1bit quantized_rerank |", rendered)
        self.assertIn("| PostgreSQL+pgvector HNSW | full-vector HNSW |", rendered)
        self.assertIn("| Milvus Standalone HNSW | full-vector HNSW |", rendered)
        self.assertIn("| 0.9500 | n/a | n/a |", rendered)
        self.assertIn("## TreeDB Search Counters", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | scalar_u8 quantized_only | 1 | 32.0 | 40.0 | 5120.0 | 0.0 | 0.0 | 0.0 | 0.0 |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | rabitq_1bit quantized_rerank | 1 | 32.0 | 40.0 | 5120.0 | 16.0 | 16.0 | 128.0 | 32.0 |", rendered)
        self.assertIn("## TreeDB Search Guardrails", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | exact/default | 1 | 0.0 | 0.0 | 1.0 | 0.0 | 0.0 |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | scalar_u8 quantized_only | 1 | 0.0 | 0.0 | 0.0 | 1.0 | 0.0 |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | rabitq_1bit quantized_only | 1 | 0.0 | 0.0 | 0.0 | 1.0 | 0.0 |", rendered)
        self.assertIn("| TreeDB column-store graph HNSW | rabitq_1bit quantized_rerank | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 1.0 |", rendered)
        self.assertIn("PostgreSQL+pgvector is not quantized", rendered)


if __name__ == "__main__":
    unittest.main()
