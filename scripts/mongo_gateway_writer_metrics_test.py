#!/usr/bin/env python3
import csv
import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "mongo_gateway_writer_metrics",
    ROOT / "mongo_gateway_writer_metrics.py",
)
writer_metrics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(writer_metrics)


class WriterMetricsTests(unittest.TestCase):
    def test_missing_and_malformed_json_warn_and_keep_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            bad_json = out_dir / "bad.json"
            bad_json.write_text("{not json", encoding="utf-8")
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tmissing\t100\t0\tmissing.json\n"
                "treedb\tbad\t100\t0\tbad.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            warnings = stderr.getvalue()
            self.assertIn("missing.json", warnings)
            self.assertIn("bad.json", warnings)
            with output.open(newline="") as out_file:
                rows = list(csv.reader(out_file, delimiter="\t"))
            self.assertEqual(rows, [writer_metrics.COLUMNS])

    def test_schema_invalid_json_warns_and_keeps_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "array.json").write_text("[]", encoding="utf-8")
            (out_dir / "bad_phases.json").write_text('{"phases":{}}', encoding="utf-8")
            (out_dir / "null_phase.json").write_text('{"target":"treedb","phases":[null]}', encoding="utf-8")
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tarray\t100\t0\tarray.json\n"
                "treedb\tbad_phases\t100\t0\tbad_phases.json\n"
                "treedb\tnull_phase\t100\t0\tnull_phase.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            warnings = stderr.getvalue()
            self.assertIn("top-level JSON is not an object", warnings)
            self.assertIn("phases is not a list", warnings)
            self.assertIn("non-object phase", warnings)
            with output.open(newline="") as out_file:
                rows = list(csv.reader(out_file, delimiter="\t"))
            self.assertEqual(rows, [writer_metrics.COLUMNS])

    def test_exact_integer_composites_and_invalid_present_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            huge = "900719925474099312345"
            raw = out_dir / "run.json"
            raw.write_text(
                json.dumps({
                    "target": "treedb",
                    "documents": 100,
                    "secondary_indexes": 0,
                    "phases": [{
                        "name": "concurrent_id_update_set_w8",
                        "ops_per_sec": 1,
                        "sampled_ns_per_op": 2,
                        "driver_calls": 3,
                        "treedb_stats_delta": {
                            "treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total": huge,
                            "treedb.collections.write_domain.collection_root_base_mismatch_total": "bad",
                            "treedb.collections.write_domain.indexed_flush.root_base_mismatch_total": "1",
                        },
                    }],
                }),
                encoding="utf-8",
            )
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tcfg\t100\t0\trun.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            with output.open(newline="") as out_file:
                rows = list(csv.DictReader(out_file, delimiter="\t"))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["backpressure_sync_total"], huge)
            self.assertEqual(rows[0]["root_mismatch_total"], "")
            self.assertIn("root_mismatch_total", stderr.getvalue())


if __name__ == "__main__":
    raise SystemExit(unittest.main())
