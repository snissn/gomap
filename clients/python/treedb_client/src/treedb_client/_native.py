"""Private bounded native-wire transport for TreeDBClient (no HTTP fallback)."""

import socket
import math
import struct
import threading
import time

from .errors import TreeDBConfigError, TreeDBProtocolError, TreeDBTimeoutError, TreeDBTransportError

_HEADER = struct.Struct("<4sHHHHIQQQ")
_MAX_FRAME = 16 << 20


def _uint(value):
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 1 << 64:
        raise TreeDBConfigError("native integer is outside uint64")
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def _read_uint(data, offset):
    value = 0
    start = offset
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise TreeDBProtocolError("truncated native varint")
        byte = data[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise TreeDBProtocolError("overflowed native varint")
        value |= (byte & 127) << shift
        if byte < 128:
            if offset - start > 1 and byte == 0:
                raise TreeDBProtocolError("nonminimal native varint")
            return value, offset
    raise TreeDBProtocolError("overflowed native varint")


def _bytes(value):
    return _uint(len(value)) + value


def _section(section_id, value):
    return _uint(section_id) + b"\x00" + _bytes(value)


def _sections(body, known):
    result = {}
    offset = 0
    count = 0
    while offset < len(body):
        section_id, offset = _read_uint(body, offset)
        flags, offset = _read_uint(body, offset)
        size, offset = _read_uint(body, offset)
        count += 1
        if count > 64 or flags & ~1 or size > len(body) - offset:
            raise TreeDBProtocolError("invalid native section bounds or flags")
        if section_id in result:
            raise TreeDBProtocolError("duplicate native section")
        if section_id not in known and flags & 1:
            raise TreeDBProtocolError("unknown critical native section")
        result[section_id] = body[offset:offset + size]
        offset += size
    return result


def _vector(values):
    return _uint(len(values)) + b"".join(_uint(len(v)) for v in values) + b"".join(values)


def _decode_vector(data, expected):
    count, offset = _read_uint(data, 0)
    if count != expected or count > len(data) - offset:
        raise TreeDBProtocolError("native vector count mismatch")
    lengths = []
    for _ in range(count):
        size, offset = _read_uint(data, offset)
        lengths.append(size)
    if sum(lengths) != len(data) - offset:
        raise TreeDBProtocolError("native vector payload mismatch")
    values = []
    for size in lengths:
        values.append(data[offset:offset + size])
        offset += size
    return values


def _string_map(data):
    count, offset = _read_uint(data, 0)
    if count > 64:
        raise TreeDBProtocolError("too many native capabilities")
    result = {}
    for _ in range(count):
        pair = []
        for _ in range(2):
            size, offset = _read_uint(data, offset)
            if size > len(data) - offset:
                raise TreeDBProtocolError("truncated native capability")
            pair.append(data[offset:offset + size].decode("utf-8", errors="strict"))
            offset += size
        if pair[0] in result:
            raise TreeDBProtocolError("duplicate native capability")
        result[pair[0]] = pair[1]
    if offset != len(data):
        raise TreeDBProtocolError("trailing native capabilities")
    return result


def _dense_request(index, query, top_k, ef_search, generation, return_embedding, filter):
    if top_k <= 0 or not query or len(query) > 65536:
        raise TreeDBConfigError("native dense query and positive top_k required")
    values = [float(v) for v in query]
    if not all(math.isfinite(v) for v in values):
        raise TreeDBConfigError("native dense query must be finite")
    try:
        packed = struct.pack(f"<{len(values)}f", *values)
    except (OverflowError, struct.error) as exc:
        raise TreeDBConfigError("native dense query exceeds float32") from exc
    leaves = []

    def visit(node, depth):
        if depth > 16:
            raise TreeDBConfigError("native filter depth exceeds 16")
        if node["operator"] == "AND":
            for child in node["conditions"]:
                visit(child, depth + 1)
        else:
            if node["operator"] not in ("==", ">", ">=", "<", "<=") or len(leaves) >= 64:
                raise TreeDBConfigError("unsupported native filter")
            leaves.append(node)

    if filter is not None:
        visit(filter, 0)
    out = bytearray(_bytes(index.encode("utf-8")) + _uint(top_k) + _uint(ef_search) + _uint(generation))
    out.append(bool(return_embedding))
    out.extend(_uint(len(values)) + packed + _uint(len(leaves)))
    for leaf in leaves:
        out.extend(_bytes(leaf["field"].encode("utf-8")))
        out.append({"==": 1, ">": 2, ">=": 3, "<": 4, "<=": 5}[leaf["operator"]])
        value = leaf["value"]
        if isinstance(value, str):
            out.extend(b"\x01" + _bytes(value.encode("utf-8")))
        elif isinstance(value, bool):
            out.extend(bytes((2, value)))
        elif isinstance(value, int) and -(1 << 63) <= value < 1 << 63:
            out.extend(b"\x03" + _uint((value << 1) ^ (value >> 63)))
        elif isinstance(value, float) and math.isfinite(value):
            out.extend(b"\x04" + struct.pack("<d", value))
        else:
            raise TreeDBConfigError("unsupported native scalar value")
    return bytes(out)


def _dense_response(body, top_k):
    sections = _sections(body, {102, 103, 130})
    if not {102, 103, 130} <= sections.keys():
        raise TreeDBProtocolError("native dense response sections missing")
    meta = sections[130]
    if not meta or meta[0] != 2:
        raise TreeDBProtocolError("typed dense v2 route tag missing")
    candidates, offset = _read_uint(meta, 1)
    exact, offset = _read_uint(meta, offset)
    scan, offset = _read_uint(meta, offset)
    count, offset = _read_uint(meta, offset)
    if exact or scan or count > top_k or candidates < count or len(meta) - offset != count * 8:
        raise TreeDBProtocolError("native dense response bounds or route mismatch")
    scores = struct.unpack(f"<{count}d", meta[offset:])
    if not all(math.isfinite(score) for score in scores):
        raise TreeDBProtocolError("native dense response has nonfinite score")
    return _decode_vector(sections[102], count), _decode_vector(sections[103], count), scores, candidates


class _NativeConnection:
    def __init__(self, address, timeout):
        if timeout is None or timeout <= 0:
            raise TreeDBConfigError("native transport requires a positive timeout")
        try:
            host, port = address.rsplit(":", 1)
            port = int(port)
            if not host or not 0 < port <= 65535:
                raise ValueError()
        except (AttributeError, ValueError):
            raise TreeDBConfigError("native_address must be host:port") from None
        self.address = (host.strip("[]"), port)
        self.timeout = timeout
        self.socket = None
        self.closed = False
        self.request_id = 0
        self.limit = _MAX_FRAME
        self.capabilities = {}
        self.lock = threading.Lock()

    def close(self):
        self.closed = True
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def _read(self, count, deadline):
        result = bytearray()
        while len(result) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("native response deadline expired")
            self.socket.settimeout(remaining)
            part = self.socket.recv(count - len(result))
            if not part:
                raise OSError("native connection closed")
            result.extend(part)
        return bytes(result)

    def _round_trip(self, frame_type, body, response_type, deadline):
        if len(body) + _HEADER.size > self.limit:
            raise TreeDBConfigError("native request exceeds frame limit")
        self.request_id += 1
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("native request deadline expired")
        self.socket.settimeout(remaining)
        self.socket.sendall(_HEADER.pack(b"TDB1", 40, 1, 0, frame_type, 0, 0, self.request_id, len(body)) + body)
        magic, size, major, minor, kind, flags, stream, request, count = _HEADER.unpack(self._read(40, deadline))
        if (magic, size, major, minor, flags, stream, request) != (b"TDB1", 40, 1, 0, 0, 0, self.request_id) or count + 40 > self.limit:
            raise TreeDBProtocolError("invalid native response header")
        payload = self._read(count, deadline)
        if kind == 6:
            sections = _sections(payload, {2})
            if 2 not in sections:
                raise TreeDBProtocolError("native error section missing")
            error = sections[2]
            code, offset = _read_uint(error, 0)
            if offset >= len(error) or error[offset] > 1:
                raise TreeDBProtocolError("invalid native error retry flag")
            size, offset = _read_uint(error, offset + 1)
            if size != len(error) - offset:
                raise TreeDBProtocolError("invalid native error message")
            raise TreeDBProtocolError(f"native error {code}: {error[offset:].decode('utf-8', errors='replace')}")
        if kind != response_type:
            raise TreeDBProtocolError("unexpected native response frame")
        return payload

    def command(self, command_id, version, sections, capability):
        # One connection, one bounded round trip; errors never replay a request.
        deadline = time.monotonic() + self.timeout
        if not self.lock.acquire(timeout=self.timeout):
            raise TreeDBTimeoutError("native connection is busy")
        try:
            if self.closed:
                raise TreeDBTransportError("native connection is closed")
            try:
                if self.socket is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("native connection deadline expired")
                    self.socket = socket.create_connection(self.address, remaining)
                    hello = _sections(self._round_trip(1, b"", 2, deadline), {3})
                    if 3 not in hello:
                        raise TreeDBProtocolError("native hello capabilities missing")
                    self.capabilities = _string_map(hello[3])
                    if self.capabilities.get("protocol") != "treedb-native-wire":
                        raise TreeDBProtocolError("wrong native protocol capability")
                    server_limit = int(self.capabilities.get("max_frame_size", "0"))
                    if server_limit < 40:
                        raise TreeDBProtocolError("native frame capability missing")
                    self.limit = min(self.limit, server_limit)
                if str(version) not in self.capabilities.get(capability, "").split(","):
                    raise TreeDBProtocolError(f"native capability {capability}/{version} unavailable")
                body = _section(1, _uint(command_id) + _uint(version) + b"\x00") + sections
                return self._round_trip(3, body, 4, deadline)
            except TimeoutError as exc:
                self.close()
                raise TreeDBTimeoutError(str(exc)) from exc
            except OSError as exc:
                self.close()
                raise TreeDBTransportError(str(exc)) from exc
            except (ValueError, UnicodeError) as exc:
                self.close()
                raise TreeDBProtocolError(str(exc)) from exc
            except TreeDBProtocolError:
                self.close()
                raise
        finally:
            self.lock.release()
