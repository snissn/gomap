#!/usr/bin/env python3
import csv
import importlib.util
import io
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


if __name__ == "__main__":
    raise SystemExit(unittest.main())
