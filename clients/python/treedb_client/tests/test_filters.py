from __future__ import annotations

import unittest

import _support  # noqa: F401
from treedb_client import Filter, InvalidFilterError, TreeDBClientError, normalize_filter


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


if __name__ == "__main__":
    unittest.main()
