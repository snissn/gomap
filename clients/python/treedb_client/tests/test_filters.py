from __future__ import annotations

import unittest

import _support  # noqa: F401
from treedb_client import Document, Filter, InvalidFilterError, TreeDBClientError, normalize_filter
from treedb_client.filters import _document_matches_filter


class FilterConversionTests(unittest.TestCase):
    def test_leaf_filter_normalizes_operator_aliases(self) -> None:
        self.assertEqual(
            normalize_filter({"field": "meta.repo", "operator": "$eq", "value": "snissn/gomap"}),
            {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
        )
        self.assertEqual(
            normalize_filter({"field": "language", "operator": "NOT_IN", "value": ("python", "ruby")}),
            {"field": "language", "operator": "not in", "value": ["python", "ruby"]},
        )

    def test_boolean_filter_normalizes_conditions(self) -> None:
        filt = Filter(
            operator="AND",
            conditions=[
                Filter(field="meta.repo", operator="==", value="snissn/gomap"),
                {"field": "meta.start_line", "operator": ">=", "value": 100},
            ],
        )

        self.assertEqual(
            normalize_filter(filt),
            {
                "operator": "AND",
                "conditions": [
                    {"field": "meta.repo", "operator": "==", "value": "snissn/gomap"},
                    {"field": "meta.start_line", "operator": ">=", "value": 100},
                ],
            },
        )

    def test_not_requires_one_condition(self) -> None:
        with self.assertRaisesRegex(InvalidFilterError, "exactly one"):
            normalize_filter({"operator": "NOT", "conditions": []})

    def test_unsupported_operator_fails_closed(self) -> None:
        with self.assertRaisesRegex(InvalidFilterError, "unsupported filter operator"):
            normalize_filter({"field": "meta.repo", "operator": "contains", "value": "gomap"})

    def test_membership_requires_array(self) -> None:
        with self.assertRaisesRegex(InvalidFilterError, "requires an array"):
            normalize_filter({"field": "meta.repo", "operator": "in", "value": "gomap"})

    def test_top_level_embedding_filter_is_rejected(self) -> None:
        with self.assertRaisesRegex(InvalidFilterError, "embedding filters are unsupported"):
            normalize_filter({"field": "embedding", "operator": "==", "value": [1.0, 0.0]})

    def test_embedding_named_metadata_paths_are_allowed(self) -> None:
        self.assertEqual(
            normalize_filter({"field": "meta.embedding.provider", "operator": "==", "value": "openai"}),
            {"field": "meta.embedding.provider", "operator": "==", "value": "openai"},
        )
        self.assertEqual(
            normalize_filter({"field": "embedding.model", "operator": "==", "value": "text-embedding-3-small"}),
            {"field": "embedding.model", "operator": "==", "value": "text-embedding-3-small"},
        )

    def test_unknown_filter_keys_are_rejected_before_http(self) -> None:
        with self.assertRaisesRegex(InvalidFilterError, "unsupported field"):
            normalize_filter({"field": "meta.repo", "operator": "==", "value": "gomap", "scan": True})

    def test_invalid_filter_is_client_error(self) -> None:
        self.assertTrue(issubclass(InvalidFilterError, TreeDBClientError))

    def test_response_document_matching_mirrors_service_filter_semantics(self) -> None:
        document = Document(
            id="doc-1",
            content="hello",
            meta={"tenant": "a", "rank": 42.0, "nested": {"tags": ["go", "database"]}},
        )
        matching = (
            {"field": "id", "operator": "==", "value": "doc-1"},
            {"field": "content", "operator": "==", "value": "hello"},
            {"field": "meta.nested.tags", "operator": "in", "value": ["database"]},
            {"field": "rank", "operator": ">=", "value": 42},
            {"operator": "AND", "conditions": [
                {"field": "tenant", "operator": "==", "value": "a"},
                {"operator": "NOT", "conditions": [
                    {"field": "rank", "operator": "<", "value": 42},
                ]},
            ]},
        )
        for filter_value in matching:
            with self.subTest(matching=filter_value):
                self.assertTrue(_document_matches_filter(document, normalize_filter(filter_value)))
        for filter_value in (
            {"field": "tenant", "operator": "==", "value": "b"},
            {"field": "missing", "operator": "!=", "value": "x"},
            {"field": "rank", "operator": ">", "value": "1"},
            {"field": "rank", "operator": "==", "value": True},
        ):
            with self.subTest(rejected=filter_value):
                self.assertFalse(_document_matches_filter(document, normalize_filter(filter_value)))

    def test_response_document_matching_preserves_integer_precision(self) -> None:
        lower = 1 << 53
        document = Document(id="large", meta={"number": lower + 1})
        for name, filter_value, want in (
            ("exact", {"field": "number", "operator": "==", "value": lower + 1}, True),
            ("adjacent integer", {"field": "number", "operator": "==", "value": lower}, False),
            ("adjacent float", {"field": "number", "operator": "==", "value": float(lower)}, False),
            ("greater", {"field": "number", "operator": ">", "value": lower}, True),
            ("not less", {"field": "number", "operator": "<", "value": lower + 1}, False),
            ("membership", {"field": "number", "operator": "in", "value": [lower, lower + 1]}, True),
            ("adjacent membership", {"field": "number", "operator": "in", "value": [lower]}, False),
            ("not in", {"field": "number", "operator": "not in", "value": [lower]}, True),
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    _document_matches_filter(document, normalize_filter(filter_value)), want,
                )


if __name__ == "__main__":
    unittest.main()
