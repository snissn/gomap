from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parents[2] / "clients/python/treedb_client/src"))
import minima_qdrant_runner as common
import minima_treedb_runner as runner


class FakeClient:
    def __init__(self, response: object) -> None:
        self.response = response
        self.call: tuple[object, ...] | None = None

    def query_by_embedding(self, *args: object, **kwargs: object) -> object:
        self.call = (*args, kwargs)
        return self.response


class MinimaTreeDBRunnerTest(unittest.TestCase):
    def workload(self, response: object) -> runner.TreeDBMinimaRunner:
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.client = FakeClient(response)
        workload.collection = "owned"
        workload.config = {"top_k": 5}
        workload.ef_search = 64
        workload.specs = {"mixed": {"name": "mixed", "filter": "user_id+fpath", "user_id": "u", "fpath": "/a"}}
        workload.queries = {"mixed": {"vector": [1.0, 0.0]}}
        workload.evidence = common.Evidence({"corpora": [{"name": "mixed"}]})
        workload.route_evidence = {}
        return workload

    def response(self, **changes: object) -> object:
        values = {
            "route": "ann", "native_base_plus_live_delta": True, "exact_fallbacks": 0,
            "full_document_scan_fallbacks": 0, "documents": [SimpleNamespace(
                id="d", content="c", score=1.0, meta={"user_id": "u", "fpath": "/a"})],
        }
        values.update(changes)
        return SimpleNamespace(**values)

    def test_search_uses_public_filtered_ann_and_captures_actual_interval(self) -> None:
        workload = self.workload(self.response())
        interval: dict[str, int] = {}
        ids, scores = workload.search("timed", "mixed", interval)
        self.assertEqual((ids, scores), (["d"], [1.0]))
        self.assertLess(interval["started_monotonic_ns"], interval["ended_monotonic_ns"])
        call = workload.client.call
        assert call is not None
        self.assertEqual(call[:3], ("owned", [1.0, 0.0], 5))
        self.assertEqual(call[3], {"operator": "AND", "conditions": [
            {"field": "meta.user_id", "operator": "==", "value": "u"},
            {"field": "meta.fpath", "operator": "==", "value": "/a"},
        ]})
        self.assertEqual(call[4], {"route": "ann", "ef_search": 64})
        self.assertIs(workload.route_evidence["mixed"], workload.client.response)

    def test_search_rejects_hidden_exact_fallback(self) -> None:
        workload = self.workload(self.response(exact_fallbacks=1))
        with self.assertRaisesRegex(RuntimeError, "left required native route"):
            workload.search("final", "mixed")
        self.assertNotIn("mixed", workload.route_evidence)

    def test_document_mapping_keeps_minima_fields_in_public_meta(self) -> None:
        document = {"id": "d", "content": "c", "vector": [1.0, 0.0], "user_id": "u", "fpath": "/a"}
        self.assertEqual(runner.service_document(document), {
            "id": "d", "content": "c", "embedding": [1.0, 0.0],
            "meta": {"user_id": "u", "fpath": "/a"},
        })


if __name__ == "__main__":
    unittest.main()
