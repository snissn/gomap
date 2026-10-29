import unittest

from treedb_client import BenchmarkVectorIndexOptions
from treedb_client._native import _dense_response
from treedb_client.errors import TreeDBProtocolError
from treedb_client.models import VectorIndexMaintenanceStatus


class RetiredStrategyTests(unittest.TestCase):
    def test_strategy_admission_covers_dataclass_and_mapping(self):
        self.assertEqual(BenchmarkVectorIndexOptions().to_dict(), {})
        self.assertEqual(BenchmarkVectorIndexOptions.from_dict({"strategy": "column_graph"}).to_dict(), {"strategy": "column_graph"})
        for value in ("native_runtime", "unknown"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    BenchmarkVectorIndexOptions(strategy=value)
                with self.assertRaises(ValueError):
                    BenchmarkVectorIndexOptions.from_dict({"strategy": value})

    def test_maintenance_requires_modern_identity(self):
        self.assertEqual(VectorIndexMaintenanceStatus.from_dict({"strategy": "column_graph"}).strategy, "column_graph")
        for value in (None, "", "native_runtime", "unknown"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                VectorIndexMaintenanceStatus.from_dict({"strategy": value})

    def test_legacy_wire_version_rejected(self):
        with self.assertRaises(TreeDBProtocolError):
            _dense_response(b"", 1, version=1)
