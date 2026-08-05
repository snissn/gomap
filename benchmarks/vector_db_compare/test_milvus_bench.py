#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path
from unittest import mock

try:
    import numpy as np
except ModuleNotFoundError:
    np = None

sys.path.insert(0, str(Path(__file__).parent))
if np is not None:
    import milvus_bench


class FakeSchema:
    def __init__(self) -> None:
        self.fields: list[dict[str, object]] = []

    def add_field(self, **kwargs: object) -> None:
        self.fields.append(kwargs)


class FakeIndex:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def add_index(self, **kwargs: object) -> None:
        self.values = kwargs


class FakeClient:
    def __init__(self) -> None:
        self.schema = FakeSchema()
        self.index = FakeIndex()
        self.inserted: list[dict[str, object]] = []

    def has_collection(self, **_: object) -> bool:
        return False

    def create_schema(self, **_: object) -> FakeSchema:
        return self.schema

    def create_collection(self, **_: object) -> None:
        pass

    def insert(self, *, data: list[dict[str, object]], **_: object) -> None:
        self.inserted.extend(data)

    def flush(self, **_: object) -> None:
        pass

    def prepare_index_params(self) -> FakeIndex:
        return self.index

    def create_index(self, **_: object) -> None:
        pass

    def close(self) -> None:
        pass

    def search(self, **_: object) -> list[list[dict[str, int]]]:
        return [[{"id": 2}, {"id": 1}]]

    def describe_index(self, **_: object) -> dict[str, str]:
        return {"index_type": "HNSW", "metric_type": "COSINE", "M": "16", "efConstruction": "128"}


class FakeDataType:
    INT64 = "INT64"
    FLOAT_VECTOR = "FLOAT_VECTOR"


@unittest.skipIf(np is None, "numpy is not installed")
class MilvusBenchTest(unittest.TestCase):
    def test_builds_hnsw_and_returns_canonical_ids(self) -> None:
        client = FakeClient()
        args = argparse.Namespace(
            collection="bench",
            index="embedding_hnsw",
            allow_drop_collection=False,
            created_collection=False,
            insert_batch=2,
            m=16,
            ef_construction=128,
            ef_search=64,
        )
        docs = np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
        with mock.patch.object(milvus_bench, "new_client", return_value=client):
            milvus_bench.build_database(args, {"docs": 2, "dimensions": 2}, docs, FakeDataType)
        self.assertEqual([row["id"] for row in client.inserted], [1, 2])
        self.assertEqual(
            client.index.values,
            {
                "field_name": "embedding",
                "index_name": "embedding_hnsw",
                "index_type": "HNSW",
                "metric_type": "COSINE",
                "params": {"M": 16, "efConstruction": 128},
            },
        )
        self.assertEqual(milvus_bench.search_one(client, args, docs[0], 2), [2, 1])
        self.assertEqual(milvus_bench.verify_hnsw_index(client, args)["index_type"], "HNSW")
        self.assertIn("unavailable_reason", milvus_bench.storage_usage(Path("/does/not/exist"), 2))


if __name__ == "__main__":
    unittest.main()
