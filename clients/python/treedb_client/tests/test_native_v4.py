import copy
import json
import struct
import unittest
from dataclasses import asdict, fields, replace
from types import SimpleNamespace
from unittest import mock

import _support
from treedb_client import BenchmarkVectorIndexOptions, IndexInfo, TreeDBClient
from treedb_client.errors import IndexUnavailableError, TreeDBProtocolError
from treedb_client._native import (
    _HEADER,
    _NativeConnection,
    _dense_normalized_diagnostic_matches,
    _dense_normalized_options,
    _dense_normalized_route_identity,
    _dense_normalized_route_matches,
    _dense_response,
    _dense_score_plane,
    _dense_work,
    _read_uint,
    _section,
    _sections,
    _uint,
    _vector,
)


REPRESENTATION = "cosine_normalized_f32_v1"
QUANTIZED = "embedding.scalar_u8.fast"


def _route_identity(*, mode=1, route=2, flags=0, rerank=0, results=1, quantized=False):
    values = [
        1, 1, mode, route, int(quantized), int(quantized), flags,
        7, 1, 1, 3, 1, 3, 2,
        1, 64, rerank, results,
        int(quantized), 2 if quantized else 0,
        1, 8,
        int(quantized), int(quantized), 8 if quantized else 0,
        results if flags & 1 else 0, results * 8 if flags & 1 else 0, 1 if results and flags & 1 else 0,
    ]
    name = QUANTIZED.encode() if quantized else b""
    return b"".join(_uint(value) for value in values) + _uint(len(name)) + name


def _bytes(value):
    return _uint(len(value)) + value


def _response_body(*, embedding=False, quantized=False, hostile_section=b""):
    document = b'{"id":"a","embedding":[0,1]}' if embedding else b'{"id":"a"}'
    meta = _uint(1) + _uint(1) + struct.pack("<d", 1.0)
    return (
        _section(102, _vector([b"a"]))
        + _section(103, _vector([document]))
        + _section(130, meta)
        + _section(
            138,
            _route_identity(
                mode=2 if quantized else 1,
                route=3 if quantized else 2,
                flags=1 if embedding else 0,
                rerank=64 if quantized else 0,
                quantized=quantized,
            ),
        )
        + hostile_section
    )


def _index_info():
    return SimpleNamespace(
        name="a",
        dimension=2,
        generation=1,
        vector_strategy="column_graph",
        vector_representation=REPRESENTATION,
        metric="cosine",
        vector_ef_search=64,
        extra={"typed_input": True},
        capabilities=SimpleNamespace(typed_dense_quantized_rerank=True),
        quantized_indexes=[
            SimpleNamespace(
                name=QUANTIZED,
                codec="scalar_u8",
                version=1,
                scalar_u8_calibration=None,
            )
        ],
    )


class NativeDenseV4Tests(unittest.TestCase):
    def test_explicit_representation_models(self):
        options = BenchmarkVectorIndexOptions.from_dict({"representation": REPRESENTATION})
        self.assertEqual(options.representation, REPRESENTATION)
        self.assertEqual(options.to_dict(), {"representation": REPRESENTATION})
        raw = {
            "name": "a", "dimension": 2, "metric": "cosine", "generation": 1,
            "contract_version": "v", "embedding_field": "embedding", "document_type": "typed",
            "capabilities": {}, "vector_representation": REPRESENTATION,
        }
        info = IndexInfo.from_dict(raw)
        self.assertEqual(info.vector_representation, REPRESENTATION)
        self.assertNotIn("vector_representation", info.extra)

    def test_score_plane_wire_v2_keeps_public_proof_v1_and_binds_packed_rerank(self):
        values = [
            2, 7, 2, 2, 3, 1, 0, 1, 64, 64, 1, 1, 1, 1, 1, 1,
            1, 2, 1, 0, 0, 8, 0, 7, 1, 2, 2,
            1, 1, 8, 0,
        ]
        raw = (
            b"".join(_uint(value) for value in values)
            + _bytes(b"") + _bytes(QUANTIZED.encode()) + _bytes(b"scalar_u8")
            + (b"\x01\x01\x01\x03" * 2)
        )
        decoded = _dense_score_plane(raw, 2)
        self.assertEqual(decoded["version"], 1)
        self.assertEqual(decoded["packed_score_batch_calls"], 1)
        hostile = bytearray(raw)
        prefix = b"".join(_uint(value) for value in values)
        forbidden = list(values)
        forbidden[30] = 1
        hostile = b"".join(_uint(value) for value in forbidden) + raw[len(prefix):]
        with self.assertRaisesRegex(TreeDBProtocolError, "packed"):
            _dense_score_plane(hostile, 2)

    def test_v4_production_error_allows_only_error_metadata(self):
        error = _uint(1) + b"\x00" + _bytes(b"failed")
        payload = _section(2, error) + _section(999, b"", critical=False)
        connection = _NativeConnection("127.0.0.1:2", 1)
        connection.socket = mock.Mock()
        header = _HEADER.pack(b"TDB1", 40, 1, 1, 6, 0, 0, 1, len(payload))
        with mock.patch.object(connection, "_read", side_effect=(header, payload)):
            with self.assertRaisesRegex(TreeDBProtocolError, "unexpected normalized dense error section"):
                connection._round_trip(1, b"", 2, 10**12, dense_version=4)

    def test_native_v4_is_disjoint_and_does_not_rescore_returned_embedding(self):
        client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        self.addCleanup(client.close)

        def command(command_id, version, sections, capability):
            self.assertEqual((command_id, version, capability), (64, 4, "dense_vector_search_versions"))
            parsed = _sections(sections, {4, 129, 137, 139}, {137, 139})
            self.assertNotIn(139, parsed)
            self.assertEqual(parsed[137], _dense_normalized_options("exact", None, 0))
            request = parsed[129]
            offset = 0
            size, offset = _read_uint(request, offset)
            offset += size
            _, offset = _read_uint(request, offset)
            resolved_ef, _ = _read_uint(request, offset)
            self.assertEqual(resolved_ef, 64)
            return _response_body(embedding=True)

        with mock.patch.object(client._native, "command", side_effect=command):
            response = client.query_by_embedding(
                "a", [1, 0], 1, return_embedding=True, index_info=_index_info()
            )
        self.assertEqual(response.native_command_version, 4)
        self.assertEqual(response.documents[0].embedding, [0.0, 1.0])
        self.assertIsNotNone(response.route_identity)
        self.assertIsNone(response.dense_work)
        self.assertIsNone(response.score_plane)

    def test_native_v4_quantized_defaults_e_and_r_and_rejects_hostile_inventory(self):
        client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        self.addCleanup(client.close)

        def command(command_id, version, sections, capability):
            self.assertEqual(version, 4)
            parsed = _sections(sections, {4, 129, 137, 139}, {137, 139})
            self.assertEqual(
                parsed[137],
                _dense_normalized_options("quantized_rerank", QUANTIZED, 64),
            )
            return _response_body(quantized=True)

        with mock.patch.object(client._native, "command", side_effect=command):
            response = client.query_by_embedding(
                "a", [1, 0], 1, query_mode="quantized_rerank",
                quantized_index_name=QUANTIZED, index_info=_index_info(),
            )
        self.assertEqual(response.route_identity.rerank_candidates, 64)
        hostile = _response_body(hostile_section=_section(134, b""))
        with self.assertRaisesRegex(TreeDBProtocolError, "inventory"):
            _dense_response(
                hostile, 1, version=4, query_mode="exact", ef_search=64,
                query_dimension=2, expected_generation=1,
            )
        good = _section(138, _route_identity())
        bad = _section(138, _route_identity()[:-2] + b"\x09\x00")
        corrupt = _response_body().replace(good, bad)
        with self.assertRaises(TreeDBProtocolError):
            _dense_response(
                corrupt, 1, version=4, query_mode="exact", ef_search=64,
                query_dimension=2, expected_generation=1,
            )

    def test_v4_route_receipt_rejects_missing_owner_and_score_invariants(self):
        def matches(identity, *, mode="exact", result_count=1, rerank=0, name=None):
            return _dense_normalized_route_matches(
                identity,
                query_mode=mode,
                quantized_index_name=name,
                top_k=1,
                ef_search=64,
                rerank_candidates=rerank,
                result_count=result_count,
                query_dimension=2,
                expected_generation=1,
                return_embedding=False,
                diagnostics=False,
                filter_requested=False,
            )

        exact = _dense_normalized_route_identity(_route_identity())
        self.assertTrue(matches(exact))
        hostile = (
            replace(exact, current_coverage_lsn=0),
            replace(exact, current_manifest_checksum=4),
            replace(exact, fp32_score_calls=0, fp32_vector_bytes_read=0),
            replace(
                exact,
                packed_score_calls=1,
                packed_score_candidates=2,
                packed_vector_bytes_read=16,
            ),
        )
        for identity in hostile:
            with self.subTest(identity=identity):
                self.assertFalse(matches(identity))

        empty = replace(
            exact,
            execution_route="typed_empty",
            result_count=0,
            fp32_score_calls=0,
            fp32_vector_bytes_read=0,
        )
        self.assertTrue(matches(empty, result_count=0))
        self.assertFalse(matches(
            replace(empty, fp32_score_calls=1, fp32_vector_bytes_read=8),
            result_count=0,
        ))

        quantized_exact = replace(
            _dense_normalized_route_identity(
                _route_identity(mode=2, route=2, rerank=64, quantized=True)
            ),
            quantized_score_calls=0,
            quantized_code_bytes_read=0,
        )
        self.assertTrue(matches(
            quantized_exact,
            mode="quantized_rerank",
            rerank=64,
            name=QUANTIZED,
        ))
        self.assertFalse(matches(
            replace(quantized_exact, quantized_score_calls=1, quantized_code_bytes_read=2),
            mode="quantized_rerank",
            rerank=64,
            name=QUANTIZED,
        ))
        for route in ("typed_exact", "typed_hnsw"):
            suffix = replace(
                quantized_exact, execution_route=route,
                current_manifest_generation=2, current_manifest_checksum=4,
                packed_score_calls=0, packed_score_candidates=0, packed_vector_bytes_read=0,
                quantized_score_calls=int(route == "typed_hnsw"),
                quantized_code_bytes_read=2 if route == "typed_hnsw" else 0,
            )
            with self.subTest(suffix_route=route):
                self.assertTrue(matches(suffix, mode="quantized_rerank", rerank=64, name=QUANTIZED))
                for hostile in (
                    replace(suffix, current_manifest_generation=1, current_manifest_checksum=3),
                    replace(suffix, packed_score_calls=1),
                    replace(suffix, packed_score_candidates=1, packed_vector_bytes_read=8),
                    replace(suffix, packed_vector_bytes_read=8),
                ):
                    self.assertFalse(matches(hostile, mode="quantized_rerank", rerank=64, name=QUANTIZED))

    def test_v4_diagnostics_bind_receipt_to_the_same_snapshot_owner(self):
        raw = bytes.fromhex(
            (_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex")
            .read_text()
            .strip()
        )
        work = _dense_work(raw)
        filter_work = replace(
            work.graph.filter,
            **{
                field.name: False if field.name in ("attempted", "completed") else 0
                for field in fields(work.graph.filter)
            },
        )
        work = replace(
            work,
            graph=replace(work.graph, base_ann_scored=1, filter=filter_work),
        )
        identity = _dense_normalized_route_identity(_route_identity(flags=2))

        def matches(candidate_work):
            return _dense_normalized_diagnostic_matches(
                identity,
                candidate_work,
                None,
                query_mode="exact",
                quantized_index_name=None,
                top_k=1,
                ef_search=64,
                rerank_candidates=0,
                result_count=1,
                query_dimension=2,
                expected_generation=1,
                filter_requested=False,
            )

        self.assertTrue(matches(work))
        split_owner = replace(
            work,
            graph=replace(
                work.graph,
                snapshot=replace(work.graph.snapshot, schema_hash=8),
            ),
        )
        self.assertFalse(matches(split_owner))

    def test_v4_exact_error_work_is_reclassified_as_consistency_unavailable(self):
        raw = bytes.fromhex(
            (_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex")
            .read_text()
            .strip()
        )
        work = _dense_work(raw)

        native = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        self.addCleanup(native.close)
        remote = TreeDBProtocolError("native error 7: interrupted", dense_work=work)
        with mock.patch.object(native._native, "command", side_effect=remote), \
             self.assertRaisesRegex(TreeDBProtocolError, "consistency-unavailable") as caught:
            native.query_by_embedding(
                "a", [1, 0], 1, diagnostics=True, index_info=_index_info()
            )
        self.assertEqual(caught.exception.dense_work, work)
        self.assertIs(caught.exception.__cause__, remote)

        http = TreeDBClient("http://127.0.0.1:1")
        self.addCleanup(http.close)
        remote = IndexUnavailableError("index_unavailable", "interrupted", dense_work=work)
        with mock.patch.object(http, "_request", side_effect=remote), \
             self.assertRaisesRegex(TreeDBProtocolError, "consistency-unavailable") as caught:
            http.query_by_embedding(
                "a", [1, 0], 1, diagnostics=True, index_info=_index_info()
            )
        self.assertEqual(caught.exception.dense_work, work)
        self.assertIs(caught.exception.__cause__, remote)

        malformed = TreeDBProtocolError("invalid native dense work proof", dense_work=work)
        with mock.patch.object(native._native, "command", side_effect=malformed), \
             self.assertRaisesRegex(TreeDBProtocolError, "invalid native dense work proof") as caught:
            native.query_by_embedding(
                "a", [1, 0], 1, diagnostics=True, index_info=_index_info()
            )
        self.assertIs(caught.exception, malformed)

    def test_http_normalized_request_and_receipt_do_not_rescore(self):
        from test_client import FixtureServer, SAMPLE_INDEX

        raw_index = copy.deepcopy(SAMPLE_INDEX)
        raw_index.update(typed_input=True, vector_representation=REPRESENTATION)
        raw_index["capabilities"]["typed_dense_quantized_rerank"] = True
        info = IndexInfo.from_dict(raw_index)
        payload = {
            "index": raw_index,
            "documents": [{"id": "a", "embedding": [0, 1], "score": 1.0}],
            "metric": "cosine",
            "route": "ann",
            "exact": False,
            "candidates": 1,
            "route_identity": asdict(_dense_normalized_route_identity(_route_identity(flags=1))),
        }
        route = ("POST", "/v1/indexes/docs/search/vector")
        with FixtureServer({route: (200, payload, 0)}) as server:
            client = TreeDBClient(server.base_url)
            try:
                response = client.query_by_embedding(
                    "docs", [1, 0], 1, return_embedding=True, index_info=info,
                )
            finally:
                client.close()
        request = json.loads(server.records[0]["body"])
        self.assertEqual(request["vector_representation"], REPRESENTATION)
        self.assertEqual(request["expected_generation"], info.generation)
        self.assertEqual(request["ef_search"], info.vector_ef_search)
        self.assertEqual(request["query_mode"], "exact")
        self.assertNotIn("diagnostics", request)
        self.assertEqual(response.documents[0].embedding, [0.0, 1.0])


if __name__ == "__main__":
    unittest.main()
