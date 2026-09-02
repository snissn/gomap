#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import load_exact_truth, parse_ordered_ints


class CommonTest(unittest.TestCase):
    def test_loads_manifest_bound_exact_truth(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            contents = b'{"query_id":"query-000000","document_ids":["doc-000001","doc-000000"]}\n'
            (root / "exact.jsonl").write_bytes(contents)
            manifest = {
                "docs": 2,
                "top_k": 2,
                "exact_truth_file": "exact.jsonl",
                "exact_truth_queries": 1,
                "files": {"exact.jsonl": {"bytes": len(contents), "sha256": hashlib.sha256(contents).hexdigest()}},
            }
            self.assertEqual(load_exact_truth(root, manifest), [{1, 2}])
            (root / "exact.jsonl").write_text(json.dumps({"query_id": "query-000000", "document_ids": ["doc-000000"]}) + "\n")
            with self.assertRaisesRegex(ValueError, "does not match manifest"):
                load_exact_truth(root, manifest)

    def test_ordered_budgets_preserve_order_and_reject_duplicates(self) -> None:
        self.assertEqual(parse_ordered_ints("512,16,128"), [512, 16, 128])
        with self.assertRaises(ValueError):
            parse_ordered_ints("16,16")


if __name__ == "__main__":
    unittest.main()
