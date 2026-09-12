"""Small fail-closed checks; no service, protected holdout or corpus collection."""
import copy
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest

import numpy as np

import minima_cohere_native_diagnostic as diagnostic
import minima_qdrant_runner as frozen


class NativeCohereDiagnosticTests(unittest.TestCase):
    def test_real_dimension_independent_oracle_and_scalar_membership(self):
        vectors = np.zeros((16, 768), dtype=np.float32)
        vectors[:, 0] = 1
        vectors[:, 767] = np.arange(16)
        query = np.zeros((1, 768), dtype=np.float32)
        query[0, 767] = 1
        truth = diagnostic.exact_truth(vectors, query, [4, 16])
        self.assertEqual(truth["16"][0][0], "row-000015")  # Not ordinal/first-coordinate oracle.
        self.assertTrue(all((int(row[4:]) * 7919) % 16 < 4 for row in truth["4"][0]))
        document = diagnostic.make_document(vectors, 3, 16, True)
        self.assertEqual(len(document["embedding"]), 768)
        self.assertEqual(document["embedding"][-1], 3)
        self.assertEqual(document["meta"]["user_id"], f"{(3 * 7919) % 16:06d}")
        self.assertTrue(document["content"].endswith(":updated"))

    def test_no_clobber_and_provenance_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileExistsError):
                diagnostic.Run({"run_dir": directory})
        plan = {"schema": diagnostic.SCHEMA, "product_commit": "a" * 40,
                "dataset_files_sha256": {"documents": "b" * 64}}
        diagnostic.validate_plan(plan, copy.deepcopy(plan))
        changed = copy.deepcopy(plan)
        changed["dataset_files_sha256"]["documents"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "frozen plan differs"):
            diagnostic.validate_plan(plan, changed)

    def test_hashing_quantiles_and_frozen_minima_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture"
            path.write_bytes(b"abc")
            self.assertEqual(diagnostic.digest(path), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(diagnostic.quantiles(list(range(1, 101)))["p95_ns"], 95)
        self.assertEqual(diagnostic.counts(500000), [4096, 4097, 5000, 50000, 500000])
        self.assertIsNone(diagnostic.predicate(500000, 500000))
        self.assertEqual(diagnostic.predicate(500000, 4097)["value"], "004097")
        self.assertIn("0x6", frozen.GENERATOR)
        self.assertNotEqual(diagnostic.SCHEMA, frozen.MEASURED_SCHEMA)

    def test_shutdown_history_import_origin_and_zero_vectors_fail_closed(self):
        lifetime = {"pid": 123, "linux_process_identity": "123:456",
                    "exit": {"pid": 123, "linux_process_identity": "123:456", "exit_code": 0, "availability": "measured"},
                    "terminal_work": {"cleanup_completed": True, "shutdown_failures": 0,
                                      "contract_version": diagnostic.existing.SERVICE_CONTRACT, "work": {"pid": 123}}}
        diagnostic.validate_shutdowns([lifetime], 1)
        lifetime["terminal_work"]["shutdown_failures"] = 1
        with self.assertRaisesRegex(RuntimeError, "clean verified shutdown"):
            diagnostic.validate_shutdowns([lifetime], 1)
        diagnostic.validate_imports(Path(diagnostic.__file__).resolve().parents[2])
        with self.assertRaisesRegex(ValueError, "outside the frozen source"):
            diagnostic.validate_imports(Path("/nonexistent-frozen-tree"))
        run = object.__new__(diagnostic.Run)
        run.plan, run.updated, run.vectors = {"rows": 16}, set(), np.ones((16, 768), dtype=np.float32)
        document = diagnostic.make_document(run.vectors, 0, 16)
        document["embedding"] = [0.0] * 768
        with self.assertRaisesRegex(RuntimeError, "zero/nonfinite vector norm"):
            run.check_documents([SimpleNamespace(**document)], ["row-000000"])


if __name__ == "__main__":
    unittest.main()
