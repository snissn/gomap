from __future__ import annotations

import unittest

import _support  # noqa: F401
from treedb_client import (
    Document,
    HybridFusionOptions,
    HybridSearchPlan,
    HybridSearchRequest,
    HybridSearchResponse,
    IndexCapabilities,
    IndexInfo,
    KeywordSearchRequest,
    KeywordSearchResponse,
)


class DocumentModelTests(unittest.TestCase):
    def test_document_write_serialization_omits_response_score(self) -> None:
        doc = Document(
            id="doc-1",
            content="body",
            embedding=[1, 2.5],
            meta={"repo": "snissn/gomap", "nested": {"line": 7}},
            score=0.99,
        )

        payload = doc.to_dict()

        self.assertEqual(payload["id"], "doc-1")
        self.assertEqual(payload["content"], "body")
        self.assertEqual(payload["embedding"], [1.0, 2.5])
        self.assertEqual(payload["meta"]["nested"]["line"], 7)
        self.assertNotIn("score", payload)

    def test_document_response_deserialization_preserves_score_and_defaults(self) -> None:
        doc = Document.from_dict({"id": "doc-1", "score": 0.25})

        self.assertEqual(doc.id, "doc-1")
        self.assertEqual(doc.content, "")
        self.assertIsNone(doc.embedding)
        self.assertEqual(doc.meta, {})
        self.assertEqual(doc.score, 0.25)
        self.assertEqual(doc.to_dict(include_score=True), {"id": "doc-1", "score": 0.25})

    def test_document_rejects_unknown_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported field"):
            Document.from_dict({"id": "doc-1", "metadata": {"repo": "gomap"}})


class IndexModelTests(unittest.TestCase):
    def test_index_info_round_trip(self) -> None:
        info = IndexInfo.from_dict(
            {
                "name": "docs",
                "dimension": 2,
                "metric": "cosine",
                "generation": 1,
                "contract_version": "treedb-document-service/v1alpha1",
                "embedding_field": "embedding",
                "vector_index_name": "embedding",
                "text_field": "content",
                "text_index_name": "content",
                "document_type": "treedb_document_service_v1",
                "capabilities": {
                    "dense_vector_search": True,
                    "exact_dense_scoring": True,
                    "metadata_filters": True,
                    "keyword_search": True,
                    "hybrid_search": True,
                    "keyword_metadata_filters": False,
                    "hybrid_metadata_filters": False,
                },
            }
        )

        self.assertEqual(info.name, "docs")
        self.assertEqual(info.dimension, 2)
        self.assertIsInstance(info.capabilities, IndexCapabilities)
        self.assertEqual(info.embedding_field, "embedding")
        self.assertEqual(info.vector_index_name, "embedding")
        self.assertEqual(info.text_field, "content")
        self.assertEqual(info.text_index_name, "content")
        self.assertTrue(info.capabilities.metadata_filters)
        self.assertTrue(info.capabilities.keyword_search)
        self.assertTrue(info.capabilities.hybrid_search)
        self.assertFalse(info.capabilities.keyword_metadata_filters)
        self.assertFalse(info.capabilities.hybrid_metadata_filters)
        self.assertEqual(info.to_dict()["capabilities"]["hybrid_search"], True)

    def test_index_capabilities_preserve_unknown_fields(self) -> None:
        capabilities = IndexCapabilities.from_dict(
            {
                "dense_vector_search": True,
                "exact_dense_scoring": True,
                "metadata_filters": True,
                "keyword_search": True,
                "hybrid_search": False,
                "keyword_metadata_filters": False,
                "hybrid_metadata_filters": False,
                "future_capability": "kept",
            }
        )

        self.assertEqual(capabilities.extra["future_capability"], "kept")
        self.assertEqual(capabilities.to_dict()["future_capability"], "kept")


class KeywordHybridModelTests(unittest.TestCase):
    def test_keyword_request_serializes_supported_fields_and_filter(self) -> None:
        request = KeywordSearchRequest(
            expected_generation=3,
            query="refund policy",
            top_k=10,
            operator="or",
            candidate_limit=100,
            max_postings_scanned=1000,
            filter={"field": "meta.repo", "operator": "$eq", "value": "gomap"},
            return_embedding=True,
        )

        self.assertEqual(
            request.to_dict(),
            {
                "query": "refund policy",
                "top_k": 10,
                "return_embedding": True,
                "expected_generation": 3,
                "operator": "or",
                "candidate_limit": 100,
                "max_postings_scanned": 1000,
                "filter": {"field": "meta.repo", "operator": "==", "value": "gomap"},
            },
        )

    def test_keyword_response_parses_stats_and_unknowns(self) -> None:
        index = _sample_index()
        response = KeywordSearchResponse.from_dict(
            {
                "index": index,
                "text_index": "content",
                "documents": [{"id": "doc-1", "content": "refund", "score": 1.5}],
                "stats": {"query_terms": 1, "documents_fetched": 1, "future_stat": 12},
                "future_top_level": {"ok": True},
            }
        )

        self.assertEqual(response.text_index, "content")
        self.assertEqual(response.documents[0].score, 1.5)
        self.assertEqual(response.stats.query_terms, 1)
        self.assertEqual(response.stats.extra["future_stat"], 12)
        self.assertEqual(response.extra["future_top_level"], {"ok": True})

    def test_hybrid_request_serializes_fusion_options(self) -> None:
        request = HybridSearchRequest(
            expected_generation=4,
            query="refund policy",
            query_embedding=(0.1, 0.2),
            top_k=5,
            candidate_limit=50,
            text_candidate_limit=25,
            vector_candidate_limit=30,
            ef_search=64,
            fusion=HybridFusionOptions(
                method="rrf",
                rrf_k=60,
                tie_policy="fused_score_best_rank_source_order_id",
                source_order=["text", "vector"],
            ),
            return_embedding=False,
        )

        payload = request.to_dict()

        self.assertEqual(payload["query_embedding"], [0.1, 0.2])
        self.assertEqual(payload["fusion"]["method"], "rrf")
        self.assertEqual(payload["fusion"]["source_order"], ["text", "vector"])
        self.assertEqual(payload["text_candidate_limit"], 25)
        self.assertEqual(payload["return_embedding"], False)

    def test_hybrid_response_parses_plan_snapshot_stats(self) -> None:
        response = HybridSearchResponse.from_dict(
            {
                "index": _sample_index(),
                "text_index": "content",
                "vector_index": "embedding",
                "documents": [{"id": "doc-1", "meta": {"_treedb_search": {"type": "hybrid"}}, "score": 0.03}],
                "plan": {"fusion_method": "rrf", "final_top_k": 5, "future_plan": "kept"},
                "snapshot": {"consistency": "current_snapshot", "commit_seq": 9},
                "stats": {"fusion_both": 1, "documents_fetched": 5},
            }
        )

        self.assertEqual(response.text_index, "content")
        self.assertEqual(response.vector_index, "embedding")
        self.assertIsInstance(response.plan, HybridSearchPlan)
        self.assertEqual(response.plan.fusion_method, "rrf")
        self.assertEqual(response.plan.extra["future_plan"], "kept")
        self.assertEqual(response.snapshot.commit_seq, 9)
        self.assertEqual(response.stats.documents_fetched, 5)
        self.assertEqual(response.documents[0].meta["_treedb_search"]["type"], "hybrid")


def _sample_index() -> dict[str, object]:
    return {
        "name": "docs",
        "dimension": 2,
        "metric": "cosine",
        "generation": 1,
        "contract_version": "treedb-document-service/v1alpha1",
        "embedding_field": "embedding",
        "vector_index_name": "embedding",
        "text_field": "content",
        "text_index_name": "content",
        "document_type": "treedb_document_service_v1",
        "capabilities": {
            "dense_vector_search": True,
            "exact_dense_scoring": True,
            "metadata_filters": True,
            "keyword_search": True,
            "hybrid_search": True,
            "keyword_metadata_filters": False,
            "hybrid_metadata_filters": False,
        },
    }


if __name__ == "__main__":
    unittest.main()
