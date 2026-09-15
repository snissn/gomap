import unittest
import socket
import json
import copy
from dataclasses import replace
import struct
from dataclasses import asdict, FrozenInstanceError
from types import SimpleNamespace
from unittest import mock

import _support
from treedb_client import TreeDBClient
from treedb_client.errors import TreeDBConfigError, TreeDBProtocolError, TreeDBTimeoutError, TreeDBTransportError, UnsupportedError
from treedb_client._native import _dense_work, _dense_quantized_options, _dense_score_plane
from treedb_client._dense_work import DenseSearchWork, dense_document_ids_valid
from treedb_client.client import _decode_json_body
from treedb_client._native import _HEADER, _NativeConnection, _dense_request, _dense_response, _decode_vector, _read_uint, _section, _sections, _string_map, _uint, _vector, _typed_upsert_request, _typed_upsert_response


def _bytes_for_test(value):
    raw = value.encode("utf-8")
    return _uint(len(raw)) + raw


class NativeCodecTests(unittest.TestCase):
    def test_typed_quantized_dense_uses_v3_and_owned_score_plane(self):
        raw_work = bytes.fromhex((_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex").read_text().strip())
        work_values, work_offset = [], 0
        for _ in range(38):
            value, work_offset = _read_uint(raw_work, work_offset)
            work_values.append(value)
        work_values[2] = 3  # the public quantized rerank score plane uses typed_hnsw work.
        work_values[3] = 1  # dense-work base ANN scoring equals the quantized score calls.
        work_values[9] = 1  # dense-work base result IDs equal exact-base score calls.
        work_values[1] &= ~((1 << 3) | (1 << 4))
        work_values[10:19] = [0] * 9  # the baseline request has no declared filter.
        work_values[34] = len(b'{"id":"a"}')
        raw_work = b"".join(_uint(value) for value in work_values)
        meta = bytes.fromhex("0301000001000000000000f03f")
        values = [1, 7, 2, 2, 3, 1, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 2, 1, 0, 0, 8, 0, 7, 1, 2, 2]
        score_plane = b"".join(_uint(value) for value in values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
        score_plane += b"\x01\x01\x01\x03" * 2

        def response_body(document):
            response_work_values = list(work_values)
            response_work_values[34] = len(document)
            return (
                _section(102, _vector([b"a"])) + _section(103, _vector([document]))
                + _section(130, meta)
                + _section(134, b"".join(_uint(value) for value in response_work_values))
                + _section(136, score_plane)
            )

        body = response_body(b'{"id":"a"}')
        info = SimpleNamespace(
            name="a", dimension=2, generation=2, vector_strategy="column_graph", metric="cosine",
            extra={"typed_input": True},
            capabilities=SimpleNamespace(typed_dense_quantized_rerank=True),
            quantized_indexes=[SimpleNamespace(name="embedding.scalar_u8.public", codec="scalar_u8", version=1, scalar_u8_calibration=None)],
        )
        client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        try:
            def command(command_id, version, sections, capability):
                self.assertEqual((command_id, version, capability), (64, 3, "dense_vector_search_versions"))
                parsed = _sections(sections, {4, 129, 135})
                self.assertIn(135, parsed)
                self.assertEqual(parsed[135], _dense_quantized_options("quantized_rerank", "embedding.scalar_u8.public", 0))
                return body
            with mock.patch.object(client._native, "command", side_effect=command):
                response = client.query_by_embedding("a", [1, 0], 1, query_mode="quantized_rerank",
                                                     quantized_index_name="embedding.scalar_u8.public", index_info=info)
            self.assertEqual(response.native_command_version, 3)
            self.assertIsNotNone(response.score_plane)
            self.assertEqual(response.score_plane.quantized_index_name, "embedding.scalar_u8.public")
            embedded_body = response_body(b'{"id":"a","embedding":[1,0]}')
            with mock.patch.object(client._native, "command", return_value=embedded_body):
                embedded_response = client.query_by_embedding(
                    "a", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", index_info=info,
                    return_embedding=True,
                )
            self.assertEqual(embedded_response.documents[0].embedding, [1.0, 0.0])
            for name, candidate_body, return_embedding in (
                ("unrequested", embedded_body, False),
                ("missing", body, True),
                ("wrong dimension", response_body(b'{"id":"a","embedding":[1]}'), True),
                ("nonfinite", response_body(b'{"id":"a","embedding":[NaN,0]}'), True),
            ):
                with self.subTest(native_embedding_echo=name), \
                     mock.patch.object(client._native, "command", return_value=candidate_body), \
                     self.assertRaisesRegex(TreeDBProtocolError, "return_embedding") as caught:
                    client.query_by_embedding(
                        "a", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", index_info=info,
                        return_embedding=return_embedding,
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)
            hostile_work_values = list(work_values)
            hostile_work_values[3:5] = [2, 0]
            hostile_work_values[7], hostile_work_values[9] = 2, 2
            hostile_plane_values = list(values)
            hostile_plane_values[10:16] = [2, 2, 2, 2, 2, 2]
            hostile_plane_values[16:18] = [2, 4]
            hostile_plane_values[18], hostile_plane_values[21] = 2, 16
            hostile_plane = b"".join(_uint(value) for value in hostile_plane_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            hostile_plane += b"\x01\x01\x01\x03" * 2
            hostile_body = (
                _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                _section(130, meta) + _section(134, b"".join(_uint(value) for value in hostile_work_values)) +
                _section(136, hostile_plane)
            )
            _dense_response(
                hostile_body, 1, version=3, query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                ef_search=0, query_dimension=2,
            )
            hostile_work_values[4] = 1
            with self.assertRaisesRegex(TreeDBProtocolError, "score-plane proof"):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in hostile_work_values)) +
                    _section(136, hostile_plane),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2,
                )
            native_error = _section(2, _uint(1) + b"\x00" + _bytes_for_test("native error"))
            pre_owner_values = list(work_values)
            pre_owner_values[1] = (1 << 1) | (1 << 3) | (1 << 4) | (1 << 5)
            pre_owner_values[2:10] = [0] * 8
            pre_owner_values[10:19] = [1] * 9
            pre_owner_values[31:38] = [0] * 7
            native_work_only_cases = [("pre-owner", pre_owner_values, False)]
            for route, tag in (("typed_empty", 1), ("typed_exact", 2), ("typed_hnsw", 3)):
                candidate = list(pre_owner_values)
                candidate[2] = tag
                native_work_only_cases.append((route, candidate, True))
            scored = list(pre_owner_values)
            scored[3] = 1
            native_work_only_cases.append(("route-empty score", scored, True))
            output_attempted = list(pre_owner_values)
            output_attempted[1] |= 1 << 6
            native_work_only_cases.append(("output attempted", output_attempted, True))
            for name, candidate, rejected in native_work_only_cases:
                candidate_raw = b"".join(_uint(value) for value in candidate)
                candidate_payload = native_error + _section(134, candidate_raw)
                work_only_client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
                work_only_client._native.socket = mock.Mock()
                work_only_client._native.capabilities["dense_vector_search_versions"] = "3"
                candidate_header = _HEADER.pack(b"TDB1", 40, 1, 0, 6, 0, 0, 1, len(candidate_payload))
                expected = "proof does not match the request" if rejected else "native error"
                with self.subTest(native_work_only=name), \
                     mock.patch.object(work_only_client._native, "_read", side_effect=(candidate_header, candidate_payload)), \
                     self.assertRaisesRegex(TreeDBProtocolError, expected) as caught:
                    work_only_client.query_by_embedding(
                        "a", [1, 0], 1,
                        filter={"field": "meta.repo", "operator": "==", "value": "gomap"},
                        query_mode="quantized_rerank", quantized_index_name="embedding.scalar_u8.public", index_info=info,
                    )
                self.assertEqual(caught.exception.dense_work, _dense_work(candidate_raw))
                self.assertIsNone(caught.exception.score_plane)
                work_only_client.close()

            proof_only_payload = native_error + _section(136, score_plane)
            proof_only_client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
            proof_only_client._native.socket = mock.Mock()
            proof_only_client._native.capabilities["dense_vector_search_versions"] = "3"
            proof_only_header = _HEADER.pack(b"TDB1", 40, 1, 0, 6, 0, 0, 1, len(proof_only_payload))
            with mock.patch.object(proof_only_client._native, "_read", side_effect=(proof_only_header, proof_only_payload)), \
                 self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                proof_only_client.query_by_embedding(
                    "a", [1, 0], 1, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", index_info=info,
                )
            self.assertIsNone(caught.exception.dense_work)
            self.assertEqual(caught.exception.score_plane, response.score_plane)
            proof_only_client.close()

            post_search_values = list(work_values)
            post_search_values[1] &= ~((1 << 0) | (1 << 6) | (1 << 7))
            post_search_values[31:38] = [0] * 7
            partial_fetch_values = list(post_search_values)
            partial_fetch_values[1] |= 1 << 6
            partial_fetch_values[31], partial_fetch_values[35], partial_fetch_values[37] = 1, 1, 1
            completed_missing_values = list(post_search_values)
            completed_missing_values[1] |= (1 << 6) | (1 << 7)
            completed_missing_values[31], completed_missing_values[33] = 1, 1
            graph_incomplete_values = list(post_search_values)
            graph_incomplete_values[1] &= ~(1 << 2)
            proof_incomplete_values = list(values)
            proof_incomplete_values[1] &= ~(1 << 1)
            proof_incomplete = bytearray(
                b"".join(_uint(value) for value in proof_incomplete_values)
                + _bytes_for_test("incomplete")
                + _bytes_for_test("embedding.scalar_u8.public")
                + _bytes_for_test("scalar_u8")
                + b"\x01\x01\x01\x03" * 2
            )
            wrong_route_values = list(post_search_values)
            wrong_route_values[2] = 2
            requested_mismatch_values = list(post_search_values)
            requested_mismatch_values[1] |= 1 << 6
            incomplete_route_values = list(graph_incomplete_values)
            incomplete_route_values[2] = 2
            empty_graph_typed_empty = (list(graph_incomplete_values), bytearray(proof_incomplete))
            empty_graph_typed_empty[0][2], empty_graph_typed_empty[1][4] = 0, 1
            empty_graph_typed_exact = (list(graph_incomplete_values), bytearray(proof_incomplete))
            empty_graph_typed_exact[0][2], empty_graph_typed_exact[1][4] = 0, 2
            wrong_bytes = bytearray(score_plane)
            wrong_bytes[17] -= 1
            proof_unavailable = bytearray(proof_incomplete)
            proof_unavailable[1] &= ~(1 << 0)
            graph_unavailable_values = [1] + [0] * 37
            proof_snapshot_unavailable_values = list(values)
            proof_snapshot_unavailable_values[1] &= ~((1 << 1) | (1 << 2))
            proof_snapshot_unavailable_values[23:27] = [0] * 4
            proof_snapshot_unavailable = (
                b"".join(_uint(value) for value in proof_snapshot_unavailable_values)
                + _bytes_for_test("incomplete") + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
                + b"\x00" * 8
            )
            graph_snapshot_unavailable_values = list(graph_incomplete_values)
            graph_snapshot_unavailable_values[1] &= ~(1 << 5)
            graph_snapshot_unavailable_values[19:31] = [0] * 12
            sibling_counter_proof = bytearray(proof_incomplete)
            sibling_counter_proof[16] += 1
            missing_reason_proof = bytearray(score_plane)
            missing_reason_proof[1] &= ~(1 << 1)
            with self.assertRaisesRegex(TreeDBProtocolError, "invalid native dense score-plane proof") as caught:
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}']))
                    + _section(130, meta)
                    + _section(134, b"".join(_uint(value) for value in graph_incomplete_values))
                    + _section(136, bytes(missing_reason_proof)),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2,
                )
            self.assertEqual(caught.exception.dense_work, _dense_work(
                b"".join(_uint(value) for value in graph_incomplete_values)
            ))
            self.assertIsNone(caught.exception.score_plane)
            native_completion_cases = (
                ("post-search before fetch", post_search_values, score_plane, False),
                ("partial fetch", partial_fetch_values, score_plane, False),
                ("completed fetch missing", completed_missing_values, score_plane, False),
                ("proof complete graph incomplete", graph_incomplete_values, score_plane, True),
                ("graph complete proof incomplete", post_search_values, bytes(proof_incomplete), True),
                ("route", wrong_route_values, score_plane, True),
                ("requested", requested_mismatch_values, score_plane, True),
                ("proof unavailable", post_search_values, bytes(proof_unavailable), True),
                ("graph unavailable", graph_unavailable_values, bytes(proof_incomplete), True),
                ("proof snapshot unavailable", post_search_values, proof_snapshot_unavailable, True),
                ("graph snapshot unavailable", graph_snapshot_unavailable_values, bytes(proof_incomplete), True),
                ("incomplete sibling score counters", graph_incomplete_values, bytes(sibling_counter_proof), True),
                ("incomplete route", incomplete_route_values, bytes(proof_incomplete), True),
                ("empty graph typed-empty proof", empty_graph_typed_empty[0], bytes(empty_graph_typed_empty[1]), True),
                ("empty graph typed-exact proof", empty_graph_typed_exact[0], bytes(empty_graph_typed_exact[1]), True),
                ("bytes", post_search_values, bytes(wrong_bytes), True),
            )
            for name, candidate_work, candidate_proof, rejected in native_completion_cases:
                candidate_work_raw = b"".join(_uint(value) for value in candidate_work)
                candidate_payload = native_error + _section(134, candidate_work_raw) + _section(136, candidate_proof)
                completion_client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
                completion_client._native.socket = mock.Mock()
                completion_client._native.capabilities["dense_vector_search_versions"] = "3"
                candidate_header = _HEADER.pack(b"TDB1", 40, 1, 0, 6, 0, 0, 1, len(candidate_payload))
                expected = "proof does not match the request" if rejected else "native error"
                with self.subTest(native_completion_prefix=name), \
                     mock.patch.object(completion_client._native, "_read", side_effect=(candidate_header, candidate_payload)), \
                     self.assertRaisesRegex(TreeDBProtocolError, expected) as caught:
                    completion_client.query_by_embedding(
                        "a", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", index_info=info,
                    )
                self.assertEqual(caught.exception.dense_work, _dense_work(candidate_work_raw))
                self.assertEqual(caught.exception.score_plane, type(response.score_plane).from_dict(_dense_score_plane(candidate_proof)))
                completion_client.close()
            for name, changed in (
                ("index name", replace(response.score_plane, quantized_index_name="embedding.scalar_u8.other")),
                ("top k", replace(response.score_plane, requested_top_k=2)),
                ("EF", replace(response.score_plane, requested_ef_search=1)),
                ("rerank limit", replace(response.score_plane, requested_rerank_candidates=2)),
                ("future generation", replace(response.score_plane,
                    snapshot=replace(response.score_plane.snapshot, schema_generation=3))),
            ):
                failure = TreeDBProtocolError("native error", dense_work=response.dense_work, score_plane=changed)
                with self.subTest(native_error_request_binding=name), \
                     mock.patch.object(client._native, "command", side_effect=failure), \
                     self.assertRaisesRegex(TreeDBProtocolError, "proof does not match the request") as caught:
                    client.query_by_embedding(
                        "a", [1, 0], 1, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", index_info=info,
                        expected_generation=2,
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)
            reversed_work_values = list(work_values)
            reversed_work_values[21:23] = [100, 1]
            reversed_plane_values = list(values)
            reversed_plane_values[25:27] = [100, 1]
            reversed_plane = b"".join(_uint(value) for value in reversed_plane_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            reversed_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in reversed_work_values)) +
                    _section(136, reversed_plane),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2,
                )
            reversed_manifest_work_values = list(work_values)
            reversed_manifest_work_values[23], reversed_manifest_work_values[27] = 2, 1
            reversed_manifest_plane = score_plane[:-8] + b"\x02\x01\x01\x03\x01\x01\x01\x03"
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in reversed_manifest_work_values)) +
                    _section(136, reversed_manifest_plane),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2, expected_generation=2,
                )
            newer_generation_work_values = list(work_values)
            newer_generation_work_values[20] = 3
            newer_generation_values = list(values)
            newer_generation_values[24] = 3
            newer_generation_plane = b"".join(_uint(value) for value in newer_generation_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            newer_generation_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in newer_generation_work_values)) +
                    _section(136, newer_generation_plane),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2, expected_generation=2,
                )
            boundary_document = b'{"id":"\\u001ca"}'
            boundary_work_values = list(work_values)
            boundary_work_values[34] = len(boundary_document)
            boundary = _dense_response(
                _section(102, _vector([b"\x1ca"])) + _section(103, _vector([boundary_document])) +
                _section(130, meta) + _section(134, b"".join(_uint(value) for value in boundary_work_values)) +
                _section(136, score_plane),
                1, version=3, query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                ef_search=0, query_dimension=2, expected_generation=2,
            )
            self.assertEqual(boundary[0], [b"\x1ca"])
            for name, offset in (("generation", -8), ("version", -6), ("checksum", -5)):
                incomplete_manifest_plane = bytearray(score_plane)
                incomplete_manifest_plane[offset] = 0
                with self.subTest(incomplete_manifest=name), self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                        _section(130, meta) + _section(134, raw_work) +
                        _section(136, bytes(incomplete_manifest_plane)),
                        1, version=3, query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                        ef_search=0, query_dimension=2,
                    )
            filtered_work_values = list(work_values)
            filtered_work_values[1] |= (1 << 3) | (1 << 4)
            filtered_work_values[4] = 1
            filtered_work_values[10] = 4097
            filtered_body = (
                _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                _section(130, meta) + _section(134, b"".join(_uint(value) for value in filtered_work_values)) +
                _section(136, score_plane)
            )
            _dense_response(
                filtered_body, 1, version=3, query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                ef_search=0, query_dimension=2, filter_requested=True,
            )
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    body, 1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2, filter_requested=True,
                )
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    filtered_body, 1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2,
                )
            underfilled_work_values = list(filtered_work_values)
            underfilled_work_values[10] = 2
            underfilled_values = list(values)
            underfilled_values[7] = 2
            underfilled_values[10:13] = [2, 2, 2]
            underfilled_plane = b"".join(_uint(value) for value in underfilled_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            underfilled_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in underfilled_work_values)) +
                    _section(136, underfilled_plane),
                    2, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2, filter_requested=True,
                )
            over_scored_work_values = list(filtered_work_values)
            over_scored_work_values[2] = 2  # typed_exact
            over_scored_work_values[3] = 0
            over_scored_work_values[7] = over_scored_work_values[9] = 2
            over_scored_work_values[10] = 1
            over_scored_values = list(values)
            over_scored_values[4] = 2  # typed_exact
            over_scored_values[13:19] = [0, 0, 0, 0, 0, 0]
            over_scored_values[20], over_scored_values[21] = 2, 16
            over_scored_plane = b"".join(_uint(value) for value in over_scored_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            over_scored_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in over_scored_work_values)) +
                    _section(136, over_scored_plane),
                    1, version=3, query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public", quantized_rerank_candidates=0,
                    ef_search=0, query_dimension=2, filter_requested=True,
                )
            for route_tag, score_calls in ((4, 1), (3, 0)):  # typed_hnsw and zero-work rerank proofs are invalid.
                invalid_values = list(values)
                invalid_values[4] = route_tag
                invalid_values[16] = score_calls
                invalid_plane = b"".join(_uint(value) for value in invalid_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
                invalid_plane += b"\x01\x01\x01\x03" * 2
                with self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                        _section(130, meta) + _section(134, raw_work) + _section(136, invalid_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
            hash_values = list(values)
            hash_values[6] = 1
            hash_plane = b"".join(_uint(value) for value in hash_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            hash_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, raw_work) + _section(136, hash_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            underfill_work_values = list(work_values)
            underfill_work_values[31:38] = [0, 0, 0, 0, 0, 0, 0]
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([])) + _section(103, _vector([])) +
                    _section(130, bytes([3, 0, 0, 0, 0])) + _section(134, b"".join(_uint(value) for value in underfill_work_values)) + _section(136, score_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )

            base_id_values = list(work_values)
            base_id_values[9] = 0
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in base_id_values)) + _section(136, score_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            wrong_work_values = list(work_values)
            wrong_work_values[2] = 2  # typed_exact contradicts the quantized rerank score plane.
            wrong_work = b"".join(_uint(value) for value in wrong_work_values)
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, wrong_work) + _section(136, score_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            for field, value in ((17, 0), (21, 0), (22, 1)):
                byte_values = list(values)
                byte_values[field] = value
                byte_plane = b"".join(_uint(value) for value in byte_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
                byte_plane += b"\x01\x01\x01\x03" * 2
                with self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                        _section(130, meta) + _section(134, raw_work) + _section(136, byte_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
            counter_values = list(values)
            counter_values[16] = 2  # contradict dense-work base_ann_scored=1.
            counter_plane = b"".join(_uint(value) for value in counter_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            counter_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, raw_work) + _section(136, counter_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            candidate_work_values = list(work_values)
            candidate_work_values[4] = 2  # base candidates cannot exceed quantized score calls.
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in candidate_work_values)) + _section(136, score_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            overflow_docs = _vector([b"a", b"b"])
            overflow_payloads = _vector([b'{"id":"a"}', b'{"id":"b"}'])
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, overflow_docs) + _section(103, overflow_payloads) +
                    _section(130, meta) + _section(134, raw_work) + _section(136, score_plane),
                    2,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            invalid_result_meta = {
                "candidate count": bytes([3, 2, 0, 0, 1]) + meta[5:],
                "nonfinite score": bytes([3, 1, 0, 0, 1]) + struct.pack("<d", float("nan")),
                "out-of-range score": bytes([3, 1, 0, 0, 1]) + struct.pack("<d", 100),
            }
            for name, candidate_meta in invalid_result_meta.items():
                with self.subTest(name=name), self.assertRaises(TreeDBProtocolError) as caught:
                    _dense_response(
                        _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                        _section(130, candidate_meta) + _section(134, raw_work) + _section(136, score_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
                self.assertEqual(caught.exception.dense_work, _dense_work(raw_work))
                self.assertIsNotNone(caught.exception.score_plane)
                self.assertEqual(caught.exception.score_plane.quantized_index_name, "embedding.scalar_u8.public")
            for name, item_id in (
                ("empty", b""),
                ("whitespace only", b" \t"),
                ("leading whitespace", b" a"),
                ("trailing whitespace", b"a "),
                ("invalid UTF-8", b"\xff"),
            ):
                with self.subTest(invalid_id=name), self.assertRaises(TreeDBProtocolError) as caught:
                    _dense_response(
                        _section(102, _vector([item_id])) + _section(103, _vector([b'{}'])) +
                        _section(130, meta) + _section(134, raw_work) + _section(136, score_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)
            ordered_work_values = list(work_values)
            ordered_work_values[3] = ordered_work_values[7] = ordered_work_values[9] = 2
            ordered_work_values[31:38] = [2, 2, 0, 4, 2, 2, 2]
            ordered_plane_values = list(values)
            for index in (7, 10, 11, 12, 13, 14, 15, 16, 18):
                ordered_plane_values[index] = 2
            ordered_plane_values[17], ordered_plane_values[21] = 4, 16
            ordered_plane = b"".join(_uint(value) for value in ordered_plane_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            ordered_plane += b"\x01\x01\x01\x03" * 2
            for name, ids, scores in (
                ("ascending score", [b"a", b"b"], (0.1, 0.9)),
                ("descending ID tie", [b"b", b"a"], (0.5, 0.5)),
            ):
                out_of_order_meta = bytes([3, 2, 0, 0, 2]) + struct.pack("<2d", *scores)
                with self.subTest(name=name), self.assertRaises(TreeDBProtocolError) as caught:
                    _dense_response(
                        _section(102, _vector(ids)) + _section(103, _vector([b"{}", b"{}"])) +
                        _section(130, out_of_order_meta) + _section(134, b"".join(_uint(value) for value in ordered_work_values)) + _section(136, ordered_plane),
                        2,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
                self.assertIsNotNone(caught.exception.dense_work)
                self.assertIsNotNone(caught.exception.score_plane)
            exact_work_values = list(work_values)
            exact_work_values[2], exact_work_values[3], exact_work_values[6], exact_work_values[7], exact_work_values[9] = 2, 0, 1, 0, 0
            exact_work_values[31:38] = [0, 0, 0, 0, 0, 0, 0]
            exact_work = b"".join(_uint(value) for value in exact_work_values)
            exact_values = list(values)
            exact_values[4], exact_values[16], exact_values[17], exact_values[18], exact_values[19], exact_values[20], exact_values[22] = 2, 0, 0, 0, 1, 0, 8
            exact_plane = b"".join(_uint(value) for value in exact_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            exact_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([])) + _section(103, _vector([])) +
                    _section(130, bytes([3, 0, 0, 0, 0])) + _section(134, exact_work) + _section(136, exact_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            inconsistent_values = list(values)
            inconsistent_values[18] = 0  # actual rerank is not backed by exact-base scoring.
            inconsistent_values[20] = 1
            inconsistent_plane = b"".join(_uint(value) for value in inconsistent_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            inconsistent_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, raw_work) + _section(136, inconsistent_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=0,
                    query_dimension=2,
                )
            capped_values = list(values)
            capped_values[9] = 2
            capped_values[12] = 3
            capped_values[15] = 3
            capped_plane = b"".join(_uint(value) for value in capped_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            capped_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, raw_work) + _section(136, capped_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=2,
                    ef_search=0,
                    query_dimension=2,
                )
            for field, value in ((12, 2), (14, 2), (10, 2)):
                width_values = list(values)
                width_values[10] = 1
                width_values[11] = 1
                width_values[12] = 1
                width_values[13] = 1
                width_values[14] = 1
                width_values[15] = 1
                width_values[18] = 1
                width_values[field] = value
                width_plane = b"".join(_uint(value) for value in width_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
                width_plane += b"\x01\x01\x01\x03" * 2
                with self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                        _section(130, meta) + _section(134, raw_work) + _section(136, width_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
            for field in (16, 19, 20):
                route_values = list(values)
                route_values[4] = 1  # typed_empty
                route_values[16] = route_values[19] = route_values[20] = 0
                route_values[field] = 1
                route_plane = b"".join(_uint(value) for value in route_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
                route_plane += b"\x01\x01\x01\x03" * 2
                empty_work_values = list(work_values)
                empty_work_values[2] = 1
                empty_work_values[3] = 0
                empty_work_values[31:38] = [0, 0, 0, 0, 0, 0, 0]
                empty_work = b"".join(_uint(value) for value in empty_work_values)
                empty_meta = bytes([3]) + b"\x00" * 4
                with self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([])) + _section(103, _vector([])) +
                        _section(130, empty_meta) + _section(134, empty_work) + _section(136, route_plane),
                        0,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                    )
            empty_values = list(values)
            empty_values[4] = 1  # typed_empty
            empty_values[10:23] = [0] * 13
            empty_values[10:13] = [1, 1, 1]  # planning precedes removal of shadowed filter matches.
            empty_plane = b"".join(_uint(value) for value in empty_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            empty_plane += b"\x01\x01\x01\x03" * 2
            valid_empty_work_values = list(work_values)
            valid_empty_work_values[1] |= (1 << 3) | (1 << 4)
            valid_empty_work_values[2] = 1
            valid_empty_work_values[3:19] = [0] * 16
            valid_empty_work_values[31:38] = [0] * 7
            valid_empty_work = b"".join(_uint(value) for value in valid_empty_work_values)
            _dense_response(
                _section(102, _vector([])) + _section(103, _vector([])) +
                _section(130, bytes([3]) + b"\x00" * 4) + _section(134, valid_empty_work) + _section(136, empty_plane),
                1,
                version=3,
                query_mode="quantized_rerank",
                quantized_index_name="embedding.scalar_u8.public",
                quantized_rerank_candidates=0,
                ef_search=0,
                query_dimension=2,
                filter_requested=True,
            )
            for filter_flags, eligible_rows in ((0, 0), (1 << 3, 0), ((1 << 3) | (1 << 4), 1)):
                invalid_empty_work_values = list(valid_empty_work_values)
                invalid_empty_work_values[1] &= ~((1 << 3) | (1 << 4))
                invalid_empty_work_values[1] |= filter_flags
                invalid_empty_work_values[10] = eligible_rows
                with self.assertRaises(TreeDBProtocolError):
                    _dense_response(
                        _section(102, _vector([])) + _section(103, _vector([])) +
                        _section(130, bytes([3]) + b"\x00" * 4) +
                        _section(134, b"".join(_uint(value) for value in invalid_empty_work_values)) + _section(136, empty_plane),
                        1,
                        version=3,
                        query_mode="quantized_rerank",
                        quantized_index_name="embedding.scalar_u8.public",
                        quantized_rerank_candidates=0,
                        ef_search=0,
                        query_dimension=2,
                        filter_requested=True,
                    )
            shortcut_work_values = list(work_values)
            shortcut_work_values[2] = 2
            shortcut_work_values[3] = shortcut_work_values[7] = shortcut_work_values[9] = 0
            shortcut_work_values[6] = 1
            shortcut_proof_values = list(values)
            shortcut_proof_values[4] = 2  # typed_exact
            shortcut_proof_values[8] = 1  # explicit EF bound
            shortcut_proof_values[10:13] = [100, 100, 100]
            shortcut_proof_values[13:19] = [0, 0, 0, 0, 0, 0]
            shortcut_proof_values[19:23] = [1, 0, 0, 8]
            shortcut_plane = b"".join(_uint(value) for value in shortcut_proof_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            shortcut_plane += b"\x01\x01\x01\x03" * 2
            with self.assertRaises(TreeDBProtocolError):
                _dense_response(
                    _section(102, _vector([b"a"])) + _section(103, _vector([b'{"id":"a"}'])) +
                    _section(130, meta) + _section(134, b"".join(_uint(value) for value in shortcut_work_values)) + _section(136, shortcut_plane),
                    1,
                    version=3,
                    query_mode="quantized_rerank",
                    quantized_index_name="embedding.scalar_u8.public",
                    quantized_rerank_candidates=0,
                    ef_search=1,
                    query_dimension=2,
                )
            incomplete_values = list(values)
            incomplete_values[1] = 1  # available proof, incomplete execution, unavailable snapshot.
            incomplete_plane = b"".join(_uint(value) for value in incomplete_values) + b"\x00" + _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8")
            incomplete_plane += b"\x00" * 8
            parsed_incomplete = _dense_score_plane(incomplete_plane)
            self.assertTrue(parsed_incomplete["available"])
            self.assertFalse(parsed_incomplete["completed"])
            self.assertFalse(parsed_incomplete["snapshot"]["available"])
        finally:
            client.close()

    def test_dense_document_ids_match_go_trim_space_and_utf8(self):
        self.assertTrue(dense_document_ids_valid(["a", b"b"]))
        for codepoint in range(0x1C, 0x20):
            separator = chr(codepoint)
            with self.subTest(go_non_space=hex(codepoint)):
                self.assertTrue(dense_document_ids_valid([separator + "a", "a" + separator]))
        for value in ("", " a", "a ", "\u2000a", "a\u3000", "\ud800", b"\xff"):
            with self.subTest(value=value):
                self.assertFalse(dense_document_ids_valid([value]))
        self.assertFalse(dense_document_ids_valid(["a", b"a"]))

    def test_typed_upsert_golden_residual_and_validation(self):
        info = SimpleNamespace(dimension=2, generation=1, scalar_fields=[])
        row = {"id": "a", "content": "text", "embedding": [1, 0], "meta": {"extra": "owned"}}
        payload, ids = _typed_upsert_request("a", [row], info)
        sections = _sections(payload, {102, 103, 131})
        self.assertEqual(sections[131].hex(), "01610101020000803f000000000107636f6e74656e740474657874")
        self.assertEqual(ids, ["a"])
        self.assertEqual(json.loads(_decode_vector(sections[103], 1)[0]), {"id": "a", "meta": {"extra": "owned"}})
        info.scalar_fields = [SimpleNamespace(field="meta.extra", value_type="string")]
        payload, _ = _typed_upsert_request("a", [row], info)
        self.assertEqual(json.loads(_decode_vector(_sections(payload, {102, 103, 131})[103], 1)[0]), {"id": "a", "meta": {}})
        self.assertEqual(row["meta"]["extra"], "owned")
        for rows in ([], [row, row], [dict(row, embedding=[1])], [dict(row, embedding=[float("nan"), 0])], [dict(row, meta={})]):
            with self.subTest(rows=rows), self.assertRaises(TreeDBConfigError):
                _typed_upsert_request("a", rows, info)
        self.assertEqual(_typed_upsert_response(_section(132, b"\x01\x02\x01\x01"), 1, 2), (1, 1))
        for raw in (b"", b"\x02\x02\x01\x01", b"\x01\x02\x00\x01", b"\x01\x02\x01\x01\x00"):
            with self.subTest(raw=raw), self.assertRaises(TreeDBProtocolError):
                _typed_upsert_response(_section(132, raw), 1, 2)

    def test_native_address_rejects_hostnames_before_networking(self):
        with mock.patch("socket.getaddrinfo", side_effect=AssertionError("resolver called")):
            for address in ("localhost:12", "example.org:12", "[localhost]:12", "[::1:12", "::1:12", "[fe80::1%eth0]:12"):
                with self.subTest(address=address), self.assertRaises(TreeDBConfigError):
                    _NativeConnection(address, 1)

    def test_numeric_connect_uses_one_socket_and_remaining_deadline(self):
        for address, family, endpoint in (("127.0.0.1:12", socket.AF_INET, ("127.0.0.1", 12)),
                                          ("[::1]:12", socket.AF_INET6, ("::1", 12, 0, 0))):
            with self.subTest(address=address):
                sock = mock.Mock()
                sock.connect.side_effect = TimeoutError("connect deadline")
                connection = _NativeConnection(address, 1)
                with mock.patch("socket.getaddrinfo", side_effect=AssertionError("resolver called")), \
                     mock.patch("socket.create_connection", side_effect=AssertionError("multi-address dial called")), \
                     mock.patch("socket.socket", return_value=sock) as create, \
                     mock.patch("time.monotonic", side_effect=[10.0, 10.25]):
                    with self.assertRaises(TreeDBTimeoutError):
                        connection.command(50, 1, b"", "get_many_versions")
                create.assert_called_once_with(family, socket.SOCK_STREAM)
                sock.settimeout.assert_called_once_with(.75)
                sock.connect.assert_called_once_with(endpoint)
                sock.close.assert_called_once()
                self.assertTrue(connection.closed)

    def test_expired_connect_budget_closes_socket_without_connecting(self):
        connection = _NativeConnection("127.0.0.1:12", 1)
        sock = mock.Mock()
        with mock.patch("socket.socket", return_value=sock), \
             mock.patch("time.monotonic", side_effect=[10.0, 11.0]):
            with self.assertRaises(TreeDBTimeoutError):
                connection.command(50, 1, b"", "get_many_versions")
        sock.connect.assert_not_called()
        sock.close.assert_called_once()
        self.assertTrue(connection.closed)

    def test_frame_and_dense_request_goldens(self):
        self.assertEqual(_HEADER.pack(b"TDB1", 40, 1, 0, 1, 0, 0, 1, 0).hex(),
                         "54444231280001000000010000000000000000000000000001000000000000000000000000000000")
        self.assertEqual(_dense_request("a", [1, 0], 1, 8, 2, True, None).hex(),
                         "016101080201020000803f0000000000")
        self.assertEqual(_vector([b"a", b"bc"]).hex(), "020102616263")

    def test_typed_response_golden_and_version_rejection(self):
        meta = bytes.fromhex("0201000001000000000000f03f")
        raw_work = bytes.fromhex((_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex").read_text().strip())
        sections = _section(102, _vector([b"a"])) + _section(103, _vector([b"{}"])) + _section(134, raw_work)
        self.assertEqual(_dense_response(sections + _section(130, meta), 1), ([b"a"], [b"{}"], (1.0,), 1, _dense_work(raw_work)))
        for tag in (0, 1, 3):
            with self.subTest(tag=tag), self.assertRaises(TreeDBProtocolError):
                _dense_response(sections + _section(130, bytes([tag]) + meta[1:]), 1)
        with self.assertRaises(TreeDBProtocolError):
            _dense_response(sections + _section(130, meta), 0)

    def test_dense_work_strict_owned_and_error_envelopes(self):
        raw = bytes.fromhex((_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex").read_text().strip())
        work = _dense_work(raw)
        data = asdict(work)
        self.assertEqual(DenseSearchWork.from_dict(data), work)
        data["graph"]["snapshot"]["base_manifest"]["format"] = "changed"
        self.assertEqual(work.graph.snapshot.base_manifest.format, "tcs1")
        with self.assertRaises(FrozenInstanceError):
            work.graph.route = "bad"
        for cut in range(len(raw)):
            with self.subTest(cut=cut), self.assertRaises(TreeDBProtocolError):
                _dense_work(raw[:cut])
        for candidate in (raw + b"\x00", b"\x02" + raw[1:], b"\x81\x00" + raw[1:], b"\xff" * 10 + raw[1:]):
            with self.assertRaises(TreeDBProtocolError):
                _dense_work(candidate)
        for action in (lambda d: d.pop("version"), lambda d: d.update(extra=0), lambda d: d.update(version=True),
                       lambda d: d["graph"].update(base_edges=-1), lambda d: d["graph"].update(base_edges=1 << 64),
                       lambda d: d["output"].pop("missing"), lambda d: d["graph"].update(route="ann"),
                       lambda d: d["graph"]["snapshot"].update(base_coverage_lsn=100, current_coverage_lsn=1),
                       lambda d: d["graph"]["snapshot"].update(schema_generation=0),
                       lambda d: d["graph"]["snapshot"].update(base_coverage_lsn=0),
                       lambda d: d["graph"]["snapshot"]["base_manifest"].update(generation=0),
                       lambda d: d["graph"]["snapshot"]["base_manifest"].update(version=0),
                       lambda d: d["graph"]["snapshot"]["base_manifest"].update(version=2),
                       lambda d: d["graph"]["snapshot"]["base_manifest"].update(checksum=0)):
            candidate = copy.deepcopy(asdict(work))
            action(candidate)
            with self.assertRaises((ValueError, TypeError)):
                DenseSearchWork.from_dict(candidate)
        envelope = json.dumps({"error": {"code": "index_unavailable", "message": "budget", "dense_work": asdict(work)}}).encode()
        client = TreeDBClient("http://127.0.0.1:1")
        try:
            error = client._decode_error(503, envelope, dense_proof=True)
            self.assertEqual(error.dense_work, work)
            duplicate = envelope.replace(b'"version": 1', b'"version": 1, "version": 1')
            with self.assertRaises(TreeDBProtocolError):
                client._decode_error(503, duplicate, dense_proof=True)
            self.assertEqual(_decode_json_body(b'{"legacy":1,"legacy":2}', status_code=200), {"legacy": 2})
            with mock.patch("treedb_client.client.json.loads", wraps=json.loads) as parse:
                self.assertEqual(client._decode_success(200, b'{"legacy":1,"legacy":2}'), {"legacy": 2})
                self.assertNotIn("object_pairs_hook", parse.call_args.kwargs)
            with mock.patch("treedb_client.client.json.loads", wraps=json.loads) as parse:
                error = client._decode_error(503, duplicate)
                self.assertIsNone(error.dense_work)
                self.assertNotIn("object_pairs_hook", parse.call_args.kwargs)
        finally:
            client.close()

        # Malformed proof siblings cannot erase independently owned valid proof.
        error_payload = _uint(1) + b"\x00" + _bytes_for_test("original")
        proof_values = [1, 1, 2, 2, 0, 1, 0] + [0] * 20
        valid_plane = b"".join(map(_uint, proof_values)) + _bytes_for_test("incomplete")
        valid_plane += _bytes_for_test("embedding.scalar_u8.public") + _bytes_for_test("scalar_u8") + b"\x00" * 8
        from treedb_client._dense_work import DenseScorePlaneProof
        proof = DenseScorePlaneProof.from_dict(_dense_score_plane(valid_plane))
        for name, work_raw, plane_raw, want_work, want_plane in (
            ("score plane", raw, b"\x01", work, None),
            ("dense work", b"\x01", valid_plane, None, proof),
            ("both proofs", b"\x01", b"\x01", None, None),
        ):
            payload = _section(2, error_payload) + _section(134, work_raw) + _section(136, plane_raw)
            connection = _NativeConnection("127.0.0.1:2", 1)
            connection.socket = mock.Mock()
            header = _HEADER.pack(b"TDB1", 40, 1, 0, 6, 0, 0, 1, len(payload))
            with self.subTest(malformed=name), mock.patch.object(connection, "_read", side_effect=(header, payload)), self.assertRaises(TreeDBProtocolError) as caught:
                connection._round_trip(1, b"", 2, 10**12, dense_proof=True, dense_version=3)
            self.assertEqual(caught.exception.dense_work, want_work)
            self.assertEqual(caught.exception.score_plane, want_plane)
            self.assertIsInstance(caught.exception.__cause__, TreeDBProtocolError)

        proof_sections = _section(134, raw) + _section(136, valid_plane)
        unknown_critical = _uint(999) + _uint(1) + _uint(0)
        for name, payload in (
            ("missing error metadata", proof_sections),
            ("duplicate error metadata", _section(2, error_payload) * 2 + proof_sections),
            ("invalid retry flag", _section(2, _uint(1) + b"\x02\x00") + proof_sections),
            ("truncated error metadata", _section(2, b"") + proof_sections),
            ("unknown critical sibling", _section(2, error_payload) + proof_sections + unknown_critical),
        ):
            connection = _NativeConnection("127.0.0.1:2", 1)
            connection.socket = mock.Mock()
            header = _HEADER.pack(b"TDB1", 40, 1, 0, 6, 0, 0, 1, len(payload))
            with self.subTest(malformed_error=name), mock.patch.object(connection, "_read", side_effect=(header, payload)), self.assertRaises(TreeDBProtocolError) as caught:
                connection._round_trip(1, b"", 2, 10**12, dense_proof=True, dense_version=3)
            self.assertEqual(caught.exception.dense_work, work)
            self.assertEqual(caught.exception.score_plane, proof)

        # Existing exceptions retain decoded proof if client document parsing fails.
        body = _section(102, _vector([b"a"])) + _section(103, _vector([b"{}"])) + _section(130, bytes.fromhex("0201000001000000000000f03f")) + _section(134, raw)
        client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        # Aggregate service generation may exceed the captured vector generation
        # after text-index recreation. The service owns expected-generation admission.
        info = SimpleNamespace(name="a", dimension=2, generation=2, vector_strategy="column_graph", metric="cosine", extra={"typed_input": True})
        try:
            with mock.patch.object(client._native, "command", return_value=body), self.assertRaises(TreeDBProtocolError) as caught:
                client.query_by_embedding("a", [1, 0], 1, index_info=info)
            self.assertEqual(caught.exception.dense_work, work)
            self.assertIsNotNone(caught.exception.__cause__)
        finally:
            client.close()

    def test_dense_document_overflow_preserves_proof(self):
        from test_client import FixtureServer, SAMPLE_INDEX
        raw_work = bytes.fromhex((_support.REPO_ROOT / "TreeDB/nativewire/testdata/dense_work_v1.hex").read_text().strip())
        document = {"id": "a", "embedding": [10 ** 400]}
        raw_doc = json.dumps(document).encode()
        values, offset = [], 0
        for _ in range(38):
            value, offset = _read_uint(raw_work, offset)
            values.append(value)
        values[34] = len(raw_doc)
        raw_work = b"".join(map(_uint, values))
        work = _dense_work(raw_work)
        body = _section(102, _vector([b"a"])) + _section(103, _vector([raw_doc])) + _section(130, bytes.fromhex("0201000001000000000000f03f")) + _section(134, raw_work)
        client = TreeDBClient("http://127.0.0.1:1", native_address="127.0.0.1:2")
        info = SimpleNamespace(name="a", dimension=2, generation=1, vector_strategy="column_graph", metric="cosine", extra={"typed_input": True})
        try:
            with mock.patch.object(client._native, "command", return_value=body), self.assertRaises(TreeDBProtocolError) as caught:
                client.query_by_embedding("a", [1, 0], 1, index_info=info)
            self.assertEqual(caught.exception.dense_work, work)
            self.assertIsInstance(caught.exception.__cause__, OverflowError)
        finally:
            client.close()
        payload = dict(index=dict(SAMPLE_INDEX, typed_input=True), documents=[document], metric="cosine", exact=False,
                       candidates=1, route="ann", dense_work=asdict(work))
        with FixtureServer({("POST", "/v1/indexes/docs/search/vector"): (200, payload, 0)}) as server:
            client = TreeDBClient(server.base_url)
            try:
                with mock.patch("treedb_client.client.json.loads", wraps=json.loads) as parse, self.assertRaises(TreeDBProtocolError) as caught:
                    client.query_by_embedding("docs", [1, 0], 1)
                self.assertEqual(parse.call_count, 1)
                self.assertIn("object_pairs_hook", parse.call_args.kwargs)
                self.assertEqual(caught.exception.dense_work, work)
                self.assertIsInstance(caught.exception.__cause__, OverflowError)
            finally:
                client.close()

    def test_malformed_bounded_codecs(self):
        for value in (b"", b"\x80", b"\x80\x00", b"\xff" * 10):
            with self.subTest(value=value), self.assertRaises(TreeDBProtocolError):
                _read_uint(value, 0)
        for value in (b"\x02\x01a", b"\x01\xff", b"\x01\x00x"):
            with self.subTest(value=value), self.assertRaises(TreeDBProtocolError):
                _decode_vector(value, 1)
        for value in (_section(3, b"") * 2, b"\x03\x02\x00", b"\x7f\x01\x00"):
            with self.subTest(value=value), self.assertRaises(TreeDBProtocolError):
                _sections(value, {3})
        with self.assertRaises(TreeDBProtocolError):
            _string_map(b"\x01\x02a")

    def test_native_mutations_do_not_fall_back_to_http(self):
        client = TreeDBClient("http://localhost:1", native_address="127.0.0.1:2")
        self.addCleanup(client.close)
        with mock.patch.object(client, "_request", side_effect=AssertionError("HTTP fallback")):
            with self.assertRaises(TreeDBConfigError):
                client.upsert_documents("a", [])
            for call in (lambda: client.delete_documents("a", []),
                         lambda: client.delete_by_filter("a", {"field": "meta.x", "operator": "==", "value": "x"})):
                with self.assertRaises(UnsupportedError):
                    call()
            with self.assertRaises(TreeDBConfigError):
                client.query_by_embedding("a", [1], 1)

    def test_invalid_query_and_scalar_bounds(self):
        for query in ([float("inf")], [1e100], []):
            with self.subTest(query=query), self.assertRaises(TreeDBConfigError):
                _dense_request("a", query, 1, 0, 1, False, None)
        with self.assertRaises(TreeDBConfigError):
            _uint(1 << 64)

    def test_connection_errors_close_without_retry(self):
        for incoming, error in ((b"", TreeDBTransportError),
                                (_HEADER.pack(b"TDB1", 40, 1, 0, 2, 0, 0, 99, 0), TreeDBProtocolError),
                                (_HEADER.pack(b"TDB1", 40, 1, 0, 2, 0, 0, 1, 1 << 30), TreeDBProtocolError),
                                (TimeoutError("deadline"), TreeDBTimeoutError)):
            with self.subTest(incoming=incoming):
                sock = mock.Mock()
                sock.recv.side_effect = [incoming] if not isinstance(incoming, Exception) else incoming
                connection = _NativeConnection("127.0.0.1:1", 1)
                with mock.patch("socket.socket", return_value=sock) as dial:
                    with self.assertRaises(error):
                        connection.command(50, 1, b"", "get_many_versions")
                    self.assertTrue(connection.closed)
                    sock.close.assert_called_once()
                    with self.assertRaises(TreeDBTransportError):
                        connection.command(50, 1, b"", "get_many_versions")
                    dial.assert_called_once()

    def test_busy_waiter_deadline_does_not_close_holder(self):
        connection = _NativeConnection("127.0.0.1:1", .01)
        connection.lock.acquire()
        try:
            with self.assertRaises(TreeDBTimeoutError):
                connection.command(50, 1, b"", "get_many_versions")
            self.assertFalse(connection.closed)
        finally:
            connection.lock.release()


if __name__ == "__main__":
    unittest.main()
