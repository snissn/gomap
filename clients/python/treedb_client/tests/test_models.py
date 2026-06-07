from __future__ import annotations

import unittest

import _support  # noqa: F401
from treedb_client import Document, IndexCapabilities, IndexInfo


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
                "document_type": "treedb_document_service_v1",
                "capabilities": {
                    "dense_vector_search": True,
                    "exact_dense_scoring": True,
                    "metadata_filters": True,
                    "keyword_search": False,
                    "hybrid_search": False,
                },
            }
        )

        self.assertEqual(info.name, "docs")
        self.assertEqual(info.dimension, 2)
        self.assertIsInstance(info.capabilities, IndexCapabilities)
        self.assertTrue(info.capabilities.metadata_filters)
        self.assertFalse(info.capabilities.keyword_search)
        self.assertEqual(info.to_dict()["capabilities"]["hybrid_search"], False)


if __name__ == "__main__":
    unittest.main()
