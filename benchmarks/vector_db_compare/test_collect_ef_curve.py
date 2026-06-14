#!/usr/bin/env python3
"""Focused tests for vector EF-curve collection."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


_SPEC = importlib.util.spec_from_file_location("collect_ef_curve", Path(__file__).with_name("collect_ef_curve.py"))
assert _SPEC is not None and _SPEC.loader is not None
collect_ef_curve = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(collect_ef_curve)


def result(ef_search: int, backend: str = "treedb_column_graph_scalar_u8_quantized_rerank") -> dict:
    return {
        "backend": backend,
        "engine": backend,
        "query_mode": "quantized_rerank",
        "quantized_codec": "scalar_u8",
        "quantized_index_name": "embedding.scalar_u8.fast",
        "quantized_rerank_candidates": 32,
        "docs": 100,
        "dimensions": 16,
        "queries": 20,
        "top_k": 10,
        "m": 16,
        "ef_construction": 128,
        "ef_search": ef_search,
        "validation": {"recall": 0.91},
        "search_benchmarks": [
            {
                "concurrency": 32,
                "queries": 20,
                "query_mode": "quantized_rerank",
                "quantized_codec": "scalar_u8",
                "quantized_index_name": "embedding.scalar_u8.fast",
                "quantized_rerank_candidates": 32,
                "ops_per_second": 1234.5,
                "avg_micros": 812.3,
                "p50_nanos": 700000,
                "p95_nanos": 900000,
                "p99_nanos": 1100000,
                "avg_candidates": 64,
                "avg_quantized_score_calls": 60,
                "avg_quantized_rerank_candidates": 32,
                "avg_quantized_rerank_exact_score_calls": 32,
                "avg_vector_bytes": 2048,
                "avg_norm_bytes": 128,
            }
        ],
    }


class EfCurveCollectorTests(unittest.TestCase):
    def test_collects_ef_child_results_to_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ef8").mkdir()
            (root / "ef32").mkdir()
            (root / "ef8" / "treedb_column_graph_scalar_u8_quantized_rerank.json").write_text(
                json.dumps(result(8)),
                encoding="utf-8",
            )
            (root / "ef32" / "treedb_column_graph_scalar_u8_quantized_rerank.json").write_text(
                json.dumps(result(32)),
                encoding="utf-8",
            )
            (root / "ef32" / "dataset_export.json").write_text("{}", encoding="utf-8")

            rows = collect_ef_curve.collect_rows(root)

        self.assertEqual([row["ef_search"] for row in rows], [8, 32])
        self.assertEqual(rows[0]["backend_label"], "TreeDB column-store graph HNSW")
        self.assertEqual(rows[0]["search_mode"], "scalar_u8 quantized_rerank")
        self.assertEqual(rows[0]["concurrency"], 32)
        self.assertEqual(rows[0]["recall"], 0.91)
        self.assertEqual(rows[0]["ops_per_second"], 1234.5)
        self.assertEqual(rows[0]["p95_micros"], 900.0)

    def test_render_markdown_points_to_matched_recall_usage(self) -> None:
        markdown = collect_ef_curve.render_markdown(
            collect_ef_curve.rows_from_result(Path("/tmp/ef8/result.json"), result(8)),
            Path("/tmp/curve"),
        )
        self.assertIn("# Vector EF Curve", markdown)
        self.assertIn("| TreeDB column-store graph HNSW | scalar_u8 quantized_rerank | 8 | 0.9100 | 1234.5 | 900us |", markdown)
        self.assertIn("Compare engines by matched recall buckets", markdown)


if __name__ == "__main__":
    unittest.main()
