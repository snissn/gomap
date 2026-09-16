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
_CRITICAL_SECTION_IDS = frozenset((134, 135, 136))


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


def _section(section_id, value, *, critical=None):
    if critical is None:
        critical = section_id in _CRITICAL_SECTION_IDS
    return _uint(section_id) + _uint(1 if critical else 0) + _bytes(value)


def _section_items(body):
    result = []
    offset = 0
    count = 0
    while offset < len(body):
        section_id, offset = _read_uint(body, offset)
        flags, offset = _read_uint(body, offset)
        size, offset = _read_uint(body, offset)
        count += 1
        if count > 64 or flags & ~1 or size > len(body) - offset:
            raise TreeDBProtocolError("invalid native section bounds or flags")
        result.append((section_id, flags, body[offset:offset + size]))
        offset += size
    return result


def _sections(body, known, required_critical=()):
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
        if section_id in required_critical and flags != 1:
            raise TreeDBProtocolError("required native section is not critical")
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


def _dense_request(index, query, top_k, ef_search, generation, return_embedding, filter,
                   query_mode="exact", quantized_index_name=None, quantized_rerank_candidates=0):
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


def _dense_quantized_options(query_mode, quantized_index_name, quantized_rerank_candidates):
    if query_mode != "quantized_rerank" or not isinstance(quantized_index_name, str) or not quantized_index_name:
        raise TreeDBConfigError("native quantized dense options require query_mode=quantized_rerank and an index name")
    if type(quantized_rerank_candidates) is not int or quantized_rerank_candidates < 0 or quantized_rerank_candidates >= 1 << 63:
        raise TreeDBConfigError("native quantized rerank candidates must be a non-negative integer")
    return _uint(1) + _uint(1) + _bytes(quantized_index_name.encode("utf-8")) + _uint(quantized_rerank_candidates)


def _dense_work(raw):
    from ._dense_work import DenseSearchWork
    values, offset = [], 0
    for _ in range(38):
        value, offset = _read_uint(raw, offset)
        values.append(value)
    if offset != len(raw) or values[1] > 255 or values[2] > 3 or values[24] > 1 or values[28] > 1:
        raise TreeDBProtocolError("invalid native dense work fields or length")
    flag = lambda bit: bool(values[1] & (1 << bit))
    manifest = lambda start: dict(zip(("generation", "format", "version", "checksum"),
                                      (values[start], ("", "tcs1")[values[start + 1]], values[start + 2], values[start + 3])))
    filter = dict(zip(("eligible_rows", "source_ids", "source_bytes", "inspected_entries", "mapping_work_charged",
                       "retained_bytes", "scratch_id_bytes", "scratch_rows", "ordinal_growth_peak_bytes"), values[10:19]))
    filter.update(attempted=flag(3), completed=flag(4))
    snapshot = dict(zip(("schema_hash", "schema_generation", "base_coverage_lsn", "current_coverage_lsn"), values[19:23]))
    snapshot.update(available=flag(5), base_manifest=manifest(23), current_manifest=manifest(27))
    graph = dict(zip(("base_ann_scored", "base_candidates", "base_edges", "delta_scored", "exact_base_scored", "base_shadowed", "base_result_ids"), values[3:10]))
    graph.update(available=flag(1), completed=flag(2), route=("", "typed_empty", "typed_exact", "typed_hnsw")[values[2]], filter=filter, snapshot=snapshot)
    output = dict(zip(("requested", "fetched", "missing", "output_bytes", "retained_payload_fetches", "json_reconstruction_rows", "typed_column_rows"), values[31:38]))
    output.update(attempted=flag(6), completed=flag(7))
    try:
        return DenseSearchWork.from_dict(dict(version=values[0], completed=flag(0), graph=graph, output=output))
    except (ValueError, TypeError, KeyError) as exc:
        raise TreeDBProtocolError("invalid native dense work proof") from exc


def _dense_score_plane(raw):
    values, offset = [], 0
    for _ in range(27):
        value, offset = _read_uint(raw, offset)
        values.append(value)
    if values[0] != 1 or values[1] > 7 or values[2] not in (1, 2, 3) or values[3] not in (1, 2, 3) or values[4] > 4 or values[5] > 65535:
        raise TreeDBProtocolError("invalid native dense score-plane proof")

    def read_string():
        nonlocal offset
        size, offset = _read_uint(raw, offset)
        if size > len(raw) - offset:
            raise TreeDBProtocolError("truncated native dense score-plane string")
        value = raw[offset:offset + size].decode("utf-8", errors="strict")
        offset += size
        return value

    reason, name, codec = read_string(), read_string(), read_string()
    manifests = []
    for _ in range(2):
        generation, offset = _read_uint(raw, offset)
        fmt, offset = _read_uint(raw, offset)
        version, offset = _read_uint(raw, offset)
        checksum, offset = _read_uint(raw, offset)
        if fmt > 1 or version > 65535:
            raise TreeDBProtocolError("invalid native dense score-plane manifest")
        manifests.append({"generation": generation, "format": ("", "tcs1")[fmt], "version": version, "checksum": checksum})
    if offset != len(raw):
        raise TreeDBProtocolError("trailing native dense score-plane proof")
    modes = ("", "exact", "quantized_rerank", "quantized_only")
    routes = ("", "typed_empty", "typed_exact", "quantized_rerank", "typed_hnsw")
    snapshot = {
        "available": bool(values[1] & 4), "schema_hash": values[23], "schema_generation": values[24],
        "base_manifest": manifests[0], "current_manifest": manifests[1],
        "base_coverage_lsn": values[25], "current_coverage_lsn": values[26],
    }
    return {
        "version": values[0], "available": bool(values[1] & 1), "completed": bool(values[1] & 2),
        "requested_mode": modes[values[2]], "effective_mode": modes[values[3]], "route": routes[values[4]],
        "reason": reason, "quantized_index_name": name, "quantized_codec": codec,
        "quantized_version": values[5], "quantized_config_hash": values[6],
        "requested_top_k": values[7], "requested_ef_search": values[8], "requested_rerank_candidates": values[9],
        "normalized_candidate_width": values[10], "raw_candidate_width": values[11], "rerank_candidate_cap": values[12],
        "raw_retained_candidates": values[13], "live_shortlist_candidates": values[14], "actual_rerank_candidates": values[15],
        "quantized_score_calls": values[16], "quantized_code_bytes_read": values[17],
        "exact_base_rerank_score_calls": values[18], "exact_suffix_score_calls": values[19], "exact_small_filter_score_calls": values[20],
        "exact_base_vector_bytes_read": values[21], "exact_suffix_vector_bytes_read": values[22], "snapshot": snapshot,
    }


def _dense_results_ordered(ids, scores):
    return all(
        scores[i - 1] > scores[i]
        or (scores[i - 1] == scores[i] and ids[i - 1] < ids[i])
        for i in range(1, len(ids))
    )


def _dense_response(body, top_k, version=2, *, query_mode=None, quantized_index_name=None,
                    quantized_rerank_candidates=0, ef_search=None, query_dimension=None,
                    expected_generation=None, filter_requested=False):
    if version not in (1, 2, 3):
        raise TreeDBProtocolError(f"unsupported native dense response version {version}")
    known = {102, 103, 130, 134}
    if version == 3:
        known.add(136)
    required_critical = {134} | ({136} if version == 3 else set())
    sections = _sections(body, known, required_critical)
    work = None
    score_plane = None
    if version == 3 and 136 in sections:
        from ._dense_work import (
            DenseScorePlaneProof,
            dense_cosine_scores_valid,
            dense_document_ids_valid,
            dense_quantized_response_work_matches,
            dense_score_plane_byte_counters_match,
        )
        try:
            score_plane = DenseScorePlaneProof.from_dict(_dense_score_plane(sections[136]))
        except (TreeDBProtocolError, ValueError, TypeError, KeyError, UnicodeError) as exc:
            score_plane_error = exc
        else:
            score_plane_error = None
    else:
        score_plane_error = None
    if 134 in sections:
        try:
            work = _dense_work(sections[134])
        except TreeDBProtocolError as exc:
            raise TreeDBProtocolError("invalid native dense work proof", score_plane=score_plane) from exc
    required = {102, 103, 130, 134} | ({136} if version == 3 else set())
    if not required <= sections.keys():
        raise TreeDBProtocolError("native dense response sections missing", dense_work=work, score_plane=score_plane)
    if score_plane_error is not None:
        raise TreeDBProtocolError("invalid native dense score-plane proof", dense_work=work) from score_plane_error
    meta = sections[130]
    expected_tag = 3 if version == 3 else 2
    if not meta or meta[0] != expected_tag:
        raise TreeDBProtocolError("typed dense v2 route tag missing", dense_work=work, score_plane=score_plane)
    try:
        candidates, offset = _read_uint(meta, 1)
        exact, offset = _read_uint(meta, offset)
        scan, offset = _read_uint(meta, offset)
        count, offset = _read_uint(meta, offset)
    except TreeDBProtocolError as exc:
        raise TreeDBProtocolError("invalid native dense response metadata", dense_work=work, score_plane=score_plane) from exc
    if exact or scan or count > top_k or candidates < count or (version == 3 and candidates != count) or len(meta) - offset != count * 8:
        raise TreeDBProtocolError("native dense response bounds or route mismatch", dense_work=work, score_plane=score_plane)
    scores = struct.unpack(f"<{count}d", meta[offset:])
    if not all(math.isfinite(score) for score in scores):
        raise TreeDBProtocolError("native dense response has nonfinite score", dense_work=work, score_plane=score_plane)
    if version == 3 and not dense_cosine_scores_valid(scores):
        raise TreeDBProtocolError("native dense response has an invalid cosine score", dense_work=work, score_plane=score_plane)
    try:
        ids, docs = _decode_vector(sections[102], count), _decode_vector(sections[103], count)
    except TreeDBProtocolError as exc:
        raise TreeDBProtocolError("invalid native dense result vectors", dense_work=work, score_plane=score_plane) from exc
    if version == 3 and not dense_document_ids_valid(ids):
        raise TreeDBProtocolError("native dense response has invalid or duplicate IDs", dense_work=work, score_plane=score_plane)
    if version == 3 and not _dense_results_ordered(ids, scores):
        raise TreeDBProtocolError("native dense response is not in score and ID order", dense_work=work, score_plane=score_plane)
    if version == 3:
        if (not score_plane.available or not score_plane.completed or not score_plane.snapshot.available
                or score_plane.requested_mode != (query_mode or "quantized_rerank")
                or score_plane.effective_mode != "quantized_rerank"
                or (expected_generation is not None and (
                    type(expected_generation) is not int
                    or not 0 < expected_generation < 1 << 64
                    or score_plane.snapshot.schema_generation > expected_generation
                ))
                or score_plane.route not in ("typed_empty", "typed_exact", "quantized_rerank")
                or not dense_score_plane_byte_counters_match(score_plane, query_dimension)
                or not dense_quantized_response_work_matches(
                    work, score_plane, top_k, count, filter_requested
                )
                or (score_plane.route == "typed_empty" and count != 0)
                or (score_plane.route in ("typed_exact", "quantized_rerank") and count != min(top_k, score_plane.exact_base_rerank_score_calls + score_plane.exact_small_filter_score_calls + score_plane.exact_suffix_score_calls))
                or (score_plane.route == "quantized_rerank" and (
                    score_plane.actual_rerank_candidates > score_plane.rerank_candidate_cap
                    or score_plane.live_shortlist_candidates > score_plane.raw_retained_candidates
                    or score_plane.raw_retained_candidates > score_plane.raw_candidate_width
                    or score_plane.actual_rerank_candidates > score_plane.live_shortlist_candidates
                    or (quantized_rerank_candidates and score_plane.rerank_candidate_cap > quantized_rerank_candidates)
                ))
                or score_plane.quantized_index_name != (quantized_index_name or score_plane.quantized_index_name)
                or score_plane.requested_top_k != top_k
                or (ef_search is not None and score_plane.requested_ef_search != ef_search)
                or (ef_search is not None and score_plane.normalized_candidate_width > max(top_k, ef_search))
                or score_plane.requested_rerank_candidates != quantized_rerank_candidates):
            raise TreeDBProtocolError("native dense score-plane proof does not match the request", dense_work=work, score_plane=score_plane)
    if not work.completed or work.output.fetched != count or work.output.output_bytes != sum(map(len, docs)):
        raise TreeDBProtocolError("native dense work does not match documents", dense_work=work, score_plane=score_plane)
    if version == 3:
        return ids, docs, scores, candidates, work, score_plane
    return ids, docs, scores, candidates, work


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

    def _round_trip(self, frame_type, body, response_type, deadline, *, dense_proof=False, dense_version=0):
        if len(body) + _HEADER.size > self.limit:
            raise TreeDBConfigError("native request exceeds frame limit")
        self.request_id += 1
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("native request deadline expired")
        self.socket.settimeout(remaining)
        self.socket.sendall(_HEADER.pack(b"TDB1", 40, 1, 1, frame_type, 0, 0, self.request_id, len(body)) + body)
        magic, size, major, minor, kind, flags, stream, request, count = _HEADER.unpack(self._read(40, deadline))
        if (magic, size, major, minor, flags, stream, request) != (b"TDB1", 40, 1, 1, 0, 0, self.request_id) or count + 40 > self.limit:
            raise TreeDBProtocolError("invalid native response header")
        payload = self._read(count, deadline)
        if kind == 6:
            if dense_proof and dense_version == 0:
                dense_version = 2
            known = {2}
            if dense_proof:
                known.add(134)
                if dense_version == 3:
                    known.add(136)
            sections = {}
            duplicates = set()
            section_error = None
            for section_id, section_flags, section_value in _section_items(payload):
                if section_id in sections:
                    duplicates.add(section_id)
                    if section_error is None:
                        section_error = TreeDBProtocolError("duplicate native section")
                    continue
                if section_id not in known and section_flags & 1 and section_error is None:
                    section_error = TreeDBProtocolError("unknown critical native section")
                required_critical = (
                    section_id == 134 and dense_proof
                    or section_id == 136 and dense_version == 3
                )
                if required_critical and section_flags != 1 and section_error is None:
                    section_error = TreeDBProtocolError("required native section is not critical")
                sections[section_id] = section_value
            work = score_plane = None
            work_error = score_plane_error = None
            if dense_proof and 134 in sections and 134 not in duplicates:
                try:
                    work = _dense_work(sections[134])
                except TreeDBProtocolError as exc:
                    work_error = exc
            if dense_version == 3 and 136 in sections and 136 not in duplicates:
                from ._dense_work import DenseScorePlaneProof
                try:
                    score_plane = DenseScorePlaneProof.from_dict(_dense_score_plane(sections[136]))
                except (ValueError, TypeError, KeyError, UnicodeError, TreeDBProtocolError) as exc:
                    score_plane_error = exc
            if section_error is not None:
                raise TreeDBProtocolError(str(section_error), dense_work=work, score_plane=score_plane) from section_error
            if 134 in sections and not dense_proof:
                raise TreeDBProtocolError("unexpected native dense error work", score_plane=score_plane)
            if 2 not in sections:
                raise TreeDBProtocolError("native error section missing", dense_work=work, score_plane=score_plane)
            error = sections[2]
            try:
                code, offset = _read_uint(error, 0)
                if offset >= len(error) or error[offset] > 1:
                    raise TreeDBProtocolError("invalid native error retry flag")
                size, offset = _read_uint(error, offset + 1)
                if size != len(error) - offset:
                    raise TreeDBProtocolError("invalid native error message")
            except TreeDBProtocolError as exc:
                raise TreeDBProtocolError(str(exc), dense_work=work, score_plane=score_plane) from exc
            if 136 in sections and dense_version != 3:
                raise TreeDBProtocolError("unexpected native dense score-plane proof", dense_work=work)
            if work_error is not None:
                raise TreeDBProtocolError("invalid native dense work proof", score_plane=score_plane) from work_error
            if score_plane_error is not None:
                raise TreeDBProtocolError("invalid native dense score-plane proof", dense_work=work) from score_plane_error
            raise TreeDBProtocolError(f"native error {code}: {error[offset:].decode('utf-8', errors='replace')}", dense_work=work, score_plane=score_plane)
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
                dense = command_id == 64 and version in (2, 3)
                return self._round_trip(3, body, 4, deadline, dense_proof=dense, dense_version=version if dense else 0)
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
