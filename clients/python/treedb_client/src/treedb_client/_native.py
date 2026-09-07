"""Private bounded native-wire transport for TreeDBClient (no HTTP fallback)."""

import socket
import ipaddress
import math
import struct
import threading
import time
import copy
import json
from collections.abc import Mapping

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


def _typed_upsert_request(index, documents, info):
    """Serialize declared carriers directly; JSON contains residual fields only."""
    from .models import Document
    rows, dims = len(documents), info.dimension
    if rows <= 0 or dims <= 0 or dims > 65536 or rows * dims * 4 > _MAX_FRAME:
        raise TreeDBConfigError("typed batch dimensions exceed frame bounds")
    fields = [field.field for field in info.scalar_fields]
    if len(set(fields)) != len(fields) or any(field.value_type != "string" or not field.field.startswith("meta.") for field in info.scalar_fields):
        raise TreeDBConfigError("typed scalar schema requires unique declared metadata strings")
    ids, residuals = [], []
    columns = [("content", [])] + [(field, []) for field in fields]
    packed = bytearray(rows * dims * 4)
    packer = struct.Struct("<" + "f" * dims)
    for i, document in enumerate(documents):
        if isinstance(document, Document):
            name, content, embedding, meta = document.id, document.content, document.embedding, document.meta
            compact = document.embedding_f32_le_b64
        elif isinstance(document, Mapping):
            if set(document) - {"id", "content", "embedding", "meta", "score", "embedding_f32_le_b64"}:
                raise TreeDBConfigError("unknown document field")
            name, content, embedding, meta = document.get("id"), document.get("content", ""), document.get("embedding"), document.get("meta", {})
            compact = document.get("embedding_f32_le_b64")
        else:
            raise TreeDBConfigError("typed document must be Document or Mapping")
        if not isinstance(name, str) or not name or not isinstance(content, str) or not isinstance(meta, Mapping):
            raise TreeDBConfigError("invalid typed document ID/content/meta")
        if compact is not None or embedding is None or len(embedding) != dims:
            raise TreeDBConfigError("native upsert requires dimension-matched numeric embedding, not base64")
        try:
            if not all(math.isfinite(v) for v in embedding):
                raise ValueError("nonfinite embedding")
            packer.pack_into(packed, i * dims * 4, *embedding)
        except (ValueError, TypeError, OverflowError, struct.error) as exc:
            raise TreeDBConfigError("invalid FP32 embedding") from exc
        ids.append(name)
        columns[0][1].append(content)
        residual = copy.deepcopy(dict(meta))
        for n, field in enumerate(fields):
            parent = residual
            path = field[5:].split(".")
            for part in path[:-1]:
                parent = parent.get(part) if isinstance(parent, Mapping) else None
            if not isinstance(parent, dict) or not isinstance(parent.get(path[-1]), str):
                raise TreeDBConfigError("missing declared string scalar")
            columns[n + 1][1].append(parent.pop(path[-1]))
        try:
            residuals.append(json.dumps({"id": name, "meta": residual}, separators=(",", ":"), allow_nan=False).encode("utf-8"))
        except (ValueError, TypeError) as exc:
            raise TreeDBConfigError("invalid residual metadata") from exc
    if len(set(ids)) != rows:
        raise TreeDBConfigError("duplicate typed document IDs")
    payload = bytearray(_bytes(index.encode("utf-8")) + _uint(info.generation) + _uint(rows) + _uint(dims))
    payload.extend(packed)
    payload.extend(_uint(len(columns)))
    for name, values in columns:
        payload.extend(_bytes(name.encode("utf-8")))
        for value in values:
            payload.extend(_bytes(value.encode("utf-8")))
    sections = _section(131, payload) + _section(102, _vector([name.encode("utf-8") for name in ids])) + _section(103, _vector(residuals))
    if len(sections) + 128 > _MAX_FRAME:
        raise TreeDBConfigError("typed batch exceeds frame bounds")
    return sections, ids


def _typed_upsert_response(body, generation, rows):
    sections = _sections(body, {132})
    if 132 not in sections:
        raise TreeDBProtocolError("typed upsert response missing")
    raw, offset, values = sections[132], 0, []
    for _ in range(4):
        value, offset = _read_uint(raw, offset)
        values.append(value)
    if offset != len(raw) or values[0] != generation or values[1] != rows or values[2] + values[3] != rows:
        raise TreeDBProtocolError("typed upsert response generation/count mismatch")
    return values[2], values[3]


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
            if address.startswith("["):
                host, port = address[1:].split("]:")
                ip = ipaddress.IPv6Address(host)
            else:
                host, port = address.split(":")
                ip = ipaddress.IPv4Address(host)
            port = int(port)
            if "%" in host or not 0 < port <= 65535:
                raise ValueError()
        except (AttributeError, ValueError):
            raise TreeDBConfigError("native_address requires numeric IPv4:port or [IPv6]:port without a zone") from None
        self.family = socket.AF_INET if ip.version == 4 else socket.AF_INET6
        self.address = (str(ip), port) if ip.version == 4 else (str(ip), port, 0, 0)
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
                    # Numeric literal + one explicit family: no resolver or
                    # per-address retry can escape the single request deadline.
                    self.socket = socket.socket(self.family, socket.SOCK_STREAM)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("native connection deadline expired")
                    self.socket.settimeout(remaining)
                    self.socket.connect(self.address)
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
