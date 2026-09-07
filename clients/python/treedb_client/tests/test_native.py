import unittest
import socket
from unittest import mock

import _support
from treedb_client import TreeDBClient
from treedb_client.errors import TreeDBConfigError, TreeDBProtocolError, TreeDBTimeoutError, TreeDBTransportError, UnsupportedError
from treedb_client._native import _HEADER, _NativeConnection, _dense_request, _dense_response, _decode_vector, _read_uint, _section, _sections, _string_map, _uint, _vector


class NativeCodecTests(unittest.TestCase):
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
        sections = _section(102, _vector([b"a"])) + _section(103, _vector([b"{}"]))
        self.assertEqual(_dense_response(sections + _section(130, meta), 1), ([b"a"], [b"{}"], (1.0,), 1))
        for tag in (0, 1, 3):
            with self.subTest(tag=tag), self.assertRaises(TreeDBProtocolError):
                _dense_response(sections + _section(130, bytes([tag]) + meta[1:]), 1)
        with self.assertRaises(TreeDBProtocolError):
            _dense_response(sections + _section(130, meta), 0)

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
            for call in (lambda: client.upsert_documents("a", []), lambda: client.delete_documents("a", []),
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
