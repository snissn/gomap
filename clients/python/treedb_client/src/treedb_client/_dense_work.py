"""Owned dense-work v1 values shared by HTTP and native decoding."""

import math
from dataclasses import dataclass


def _values(data, integers=(), booleans=(), strings=(), nested=()):
    if not isinstance(data, dict) or set(data) != set(integers + booleans + strings + nested):
        raise ValueError("dense work fields are missing or unknown")
    out = {}
    for name in integers:
        value = data[name]
        if type(value) is not int or not 0 <= value < 1 << 64:
            raise ValueError(f"dense work {name} must be uint64")
        out[name] = value
    for name in booleans:
        if type(data[name]) is not bool:
            raise ValueError(f"dense work {name} must be bool")
        out[name] = data[name]
    for name in strings:
        if not isinstance(data[name], str):
            raise ValueError(f"dense work {name} must be a string")
        out[name] = data[name]
    return out


@dataclass(frozen=True)
class DenseManifestWork:
    generation: int
    format: str
    version: int
    checksum: int

    @classmethod
    def from_dict(cls, data):
        out = cls(**_values(data, ("generation", "version", "checksum"), strings=("format",)))
        if out.format not in ("", "tcs1") or out.version > 65535:
            raise ValueError("unsupported dense manifest identity")
        return out

    def is_complete(self):
        return self.generation != 0 and self.version == 1 and self.checksum != 0


@dataclass(frozen=True)
class DenseSnapshotWork:
    available: bool
    schema_hash: int
    schema_generation: int
    base_manifest: DenseManifestWork
    current_manifest: DenseManifestWork
    base_coverage_lsn: int
    current_coverage_lsn: int

    @classmethod
    def from_dict(cls, data):
        values = _values(data, ("schema_hash", "schema_generation", "base_coverage_lsn", "current_coverage_lsn"),
                         ("available",), nested=("base_manifest", "current_manifest"))
        base = DenseManifestWork.from_dict(data["base_manifest"])
        current = DenseManifestWork.from_dict(data["current_manifest"])
        if not values["available"] and (any(values.values()) or any(vars(base).values()) or any(vars(current).values())):
            raise ValueError("unavailable dense snapshot carries identity")
        if values["available"] and (not base.is_complete() or not current.is_complete()):
            raise ValueError("available dense snapshot has an incomplete manifest identity")
        if values["available"] and (values["schema_hash"] == 0 or values["schema_generation"] == 0):
            raise ValueError("available dense snapshot has no schema identity")
        if values["available"] and values["base_coverage_lsn"] == 0:
            raise ValueError("available dense snapshot has no coverage")
        if values["available"] and values["current_coverage_lsn"] < values["base_coverage_lsn"]:
            raise ValueError("dense snapshot coverage is reversed")
        if values["available"] and current.generation < base.generation:
            raise ValueError("dense snapshot manifest generation is reversed")
        if values["available"] and current.generation == base.generation and (
                base.generation,
                base.format or "tcs1",
                base.version,
                base.checksum,
        ) != (
                current.generation,
                current.format or "tcs1",
                current.version,
                current.checksum,
        ):
            raise ValueError("equal dense snapshot manifest generations have different identities")
        if (values["available"] and current.generation == base.generation
                and values["current_coverage_lsn"] != values["base_coverage_lsn"]):
            raise ValueError("equal dense snapshot manifest identities have different coverage")
        return cls(**values, base_manifest=base, current_manifest=current)


@dataclass(frozen=True)
class DenseFilterWork:
    attempted: bool
    completed: bool
    eligible_rows: int
    source_ids: int
    source_bytes: int
    inspected_entries: int
    mapping_work_charged: int
    retained_bytes: int
    scratch_id_bytes: int
    scratch_rows: int
    ordinal_growth_peak_bytes: int

    @classmethod
    def from_dict(cls, data):
        values = _values(data, ("eligible_rows", "source_ids", "source_bytes", "inspected_entries", "mapping_work_charged",
                                "retained_bytes", "scratch_id_bytes", "scratch_rows", "ordinal_growth_peak_bytes"), ("attempted", "completed"))
        if not values["attempted"] and any(values.values()):
            raise ValueError("unattempted dense filter carries work")
        return cls(**values)


@dataclass(frozen=True)
class DenseGraphWork:
    available: bool
    completed: bool
    route: str
    base_ann_scored: int
    base_candidates: int
    base_edges: int
    delta_scored: int
    exact_base_scored: int
    base_shadowed: int
    base_result_ids: int
    filter: DenseFilterWork
    snapshot: DenseSnapshotWork

    @classmethod
    def from_dict(cls, data):
        values = _values(data, ("base_ann_scored", "base_candidates", "base_edges", "delta_scored", "exact_base_scored", "base_shadowed", "base_result_ids"),
                         ("available", "completed"), ("route",), ("filter", "snapshot"))
        filter = DenseFilterWork.from_dict(data["filter"])
        snapshot = DenseSnapshotWork.from_dict(data["snapshot"])
        if values["route"] not in ("", "typed_empty", "typed_exact", "typed_hnsw"):
            raise ValueError("unknown dense graph route")
        if not values["available"] and (any(values.values()) or filter.attempted or snapshot.available):
            raise ValueError("unavailable dense graph carries work")
        if values["completed"] and (not snapshot.available or not values["route"] or (filter.attempted and not filter.completed)):
            raise ValueError("completed dense graph lacks captured work")
        if (snapshot.available and snapshot.current_manifest.generation == snapshot.base_manifest.generation
                and (values["delta_scored"] or values["base_shadowed"])):
            raise ValueError("unchanged dense graph snapshot carries suffix work")
        return cls(**values, filter=filter, snapshot=snapshot)


@dataclass(frozen=True)
class DenseScorePlaneProof:
    """Owned, sibling proof for the typed dense quantized score plane."""

    version: int
    available: bool
    completed: bool
    requested_mode: str
    effective_mode: str
    route: str
    reason: str
    quantized_index_name: str
    quantized_codec: str
    quantized_version: int
    quantized_config_hash: int
    requested_top_k: int
    requested_ef_search: int
    requested_rerank_candidates: int
    normalized_candidate_width: int
    raw_candidate_width: int
    rerank_candidate_cap: int
    raw_retained_candidates: int
    live_shortlist_candidates: int
    actual_rerank_candidates: int
    quantized_score_calls: int
    quantized_code_bytes_read: int
    exact_base_rerank_score_calls: int
    exact_suffix_score_calls: int
    exact_small_filter_score_calls: int
    exact_base_vector_bytes_read: int
    exact_suffix_vector_bytes_read: int
    snapshot: DenseSnapshotWork
    packed_score_batch_calls: int = 0
    packed_score_candidates: int = 0
    packed_vector_bytes_read: int = 0
    forbidden_stable_score_calls: int = 0

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            raise ValueError("dense score-plane proof must be an object")
        has_packed_counters = any(data.get(name, 0) for name in (
            "packed_score_batch_calls", "packed_score_candidates", "packed_vector_bytes_read",
            "forbidden_stable_score_calls",
        ))
        required = {
            "version", "available", "completed", "requested_mode", "effective_mode", "route",
            "requested_top_k", "requested_ef_search", "requested_rerank_candidates",
            "normalized_candidate_width", "raw_candidate_width", "rerank_candidate_cap",
            "raw_retained_candidates", "live_shortlist_candidates", "actual_rerank_candidates",
            "quantized_score_calls", "quantized_code_bytes_read", "exact_base_rerank_score_calls",
            "exact_suffix_score_calls", "exact_small_filter_score_calls", "exact_base_vector_bytes_read",
            "exact_suffix_vector_bytes_read", "snapshot",
        }
        optional = {
            "reason", "quantized_index_name", "quantized_codec", "quantized_version", "quantized_config_hash",
            "packed_score_batch_calls", "packed_score_candidates", "packed_vector_bytes_read",
            "forbidden_stable_score_calls",
        }
        if set(data) - required - optional or not required <= set(data):
            raise ValueError("dense score-plane proof fields are missing or unknown")
        values = {}
        for name in required - {"snapshot", "requested_mode", "effective_mode", "route"}:
            value = data[name]
            if name in {"available", "completed"}:
                if type(value) is not bool:
                    raise ValueError(f"dense score-plane {name} must be bool")
            elif type(value) is not int or not 0 <= value < 1 << 64:
                raise ValueError(f"dense score-plane {name} must be uint64")
            values[name] = value
        for name in ("requested_mode", "effective_mode", "route"):
            if not isinstance(data[name], str):
                raise ValueError(f"dense score-plane {name} must be a string")
            values[name] = data[name]
        for name in ("reason", "quantized_index_name", "quantized_codec"):
            value = data.get(name, "")
            if not isinstance(value, str):
                raise ValueError(f"dense score-plane {name} must be a string")
            values[name] = value
        for name in (
            "quantized_version", "quantized_config_hash", "packed_score_batch_calls",
            "packed_score_candidates", "packed_vector_bytes_read", "forbidden_stable_score_calls",
        ):
            value = data.get(name, 0)
            if type(value) is not int or not 0 <= value < 1 << 64:
                raise ValueError(f"dense score-plane {name} must be uint64")
            values[name] = value
        if values["quantized_version"] > 65535:
            raise ValueError("dense score-plane quantized_version must be uint16")
        if values["version"] != 1 or values["requested_mode"] not in ("exact", "quantized_rerank", "quantized_only") \
                or values["effective_mode"] not in ("exact", "quantized_rerank", "quantized_only") \
                or values["route"] not in ("", "typed_empty", "typed_exact", "quantized_rerank", "typed_hnsw"):
            raise ValueError("unsupported dense score-plane proof")
        if values["requested_mode"] == "quantized_rerank" or values["effective_mode"] == "quantized_rerank":
            if not values["quantized_index_name"] or values["quantized_codec"] != "scalar_u8" or values["quantized_version"] != 1 or values["quantized_config_hash"] != 0:
                raise ValueError("dense score-plane proof requires the legacy scalar_u8/v1 codec")
        snapshot = DenseSnapshotWork.from_dict(data["snapshot"])
        if values["completed"] and (
            not values["available"]
            or not snapshot.available
            or not values["route"]
            or values["reason"]
        ):
            raise ValueError("completed dense score-plane proof lacks captured work")
        if values["available"] and not values["completed"] and not values["reason"]:
            raise ValueError("incomplete dense score-plane proof has no reason")
        if (snapshot.available and snapshot.current_manifest.generation == snapshot.base_manifest.generation
                and (values["exact_suffix_score_calls"] or values["exact_suffix_vector_bytes_read"]
                     or values["raw_candidate_width"] != values["normalized_candidate_width"])):
            raise ValueError("unchanged dense score-plane snapshot carries suffix work")
        if values["completed"] and values["route"] == "quantized_rerank" and values["quantized_score_calls"] == 0:
            raise ValueError("completed quantized rerank proof has no quantized score calls")
        if values["completed"]:
            expected_cap = min(values["normalized_candidate_width"], values["requested_rerank_candidates"] or values["normalized_candidate_width"])
            if (
                (values["requested_ef_search"] != 0 and values["normalized_candidate_width"] > max(values["requested_top_k"], values["requested_ef_search"]))
                or values["rerank_candidate_cap"] != expected_cap
                or values["normalized_candidate_width"] > values["raw_candidate_width"]
                or (
                    values["normalized_candidate_width"] == 0
                    and (values["raw_candidate_width"] != 0 or values["exact_small_filter_score_calls"] != 0)
                )
                or values["raw_retained_candidates"] > values["quantized_score_calls"]
                or values["raw_retained_candidates"] > values["raw_candidate_width"]
                or values["live_shortlist_candidates"] > values["raw_retained_candidates"]
                or values["live_shortlist_candidates"] > values["normalized_candidate_width"]
                or values["actual_rerank_candidates"] > values["live_shortlist_candidates"]
                or values["actual_rerank_candidates"] > values["rerank_candidate_cap"]
                or values["actual_rerank_candidates"] != values["exact_base_rerank_score_calls"]
            ):
                raise ValueError("completed dense score-plane proof planning counters are inconsistent")
            if values["route"] == "quantized_rerank":
                if (
                    values["normalized_candidate_width"] == 0
                    or values["raw_candidate_width"] == 0
                    or values["rerank_candidate_cap"] == 0
                    or values["exact_small_filter_score_calls"] != 0
                    or values["actual_rerank_candidates"] != min(values["live_shortlist_candidates"], values["rerank_candidate_cap"])
                ):
                    raise ValueError("completed dense score-plane proof quantized rerank counters are inconsistent")
            elif values["route"] == "typed_empty" and (
                values["quantized_score_calls"] != 0
                or values["exact_suffix_score_calls"] != 0
                or values["exact_small_filter_score_calls"] != 0
                or values["raw_retained_candidates"] != 0
                or values["live_shortlist_candidates"] != 0
                or values["actual_rerank_candidates"] != 0
                or values["exact_base_rerank_score_calls"] != 0
            ):
                raise ValueError("completed dense score-plane proof typed-empty counters are inconsistent")
            elif values["route"] == "typed_exact" and (
                values["quantized_score_calls"] != 0
                or values["raw_retained_candidates"] != 0
                or values["live_shortlist_candidates"] != 0
                or values["actual_rerank_candidates"] != 0
                or values["exact_base_rerank_score_calls"] != 0
            ):
                raise ValueError("completed dense score-plane proof typed-exact counters are inconsistent")
        packed_candidates = values["exact_base_rerank_score_calls"] + values["exact_small_filter_score_calls"]
        if has_packed_counters and (
            values["forbidden_stable_score_calls"] != 0
            or values["packed_score_candidates"] != packed_candidates
            or values["packed_vector_bytes_read"] != values["exact_base_vector_bytes_read"]
            or values["packed_score_batch_calls"] != (1 if packed_candidates else 0)
        ):
            raise ValueError("dense score-plane packed scorer counters are inconsistent")
        return cls(**values, snapshot=snapshot)


def optional_dense_score_plane(data):
    return None if data is None else DenseScorePlaneProof.from_dict(data)


def dense_score_plane_byte_counters_match(proof, dimension):
    if proof is None or type(dimension) is not int or dimension <= 0:
        return False
    exact_base_calls = proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
    bytes_per_exact = dimension * 4
    return (
        proof.quantized_code_bytes_read == proof.quantized_score_calls * dimension
        and proof.exact_base_vector_bytes_read == exact_base_calls * bytes_per_exact
        and proof.exact_suffix_vector_bytes_read == proof.exact_suffix_score_calls * bytes_per_exact
    )


def dense_score_plane_packed_counters_match(proof, dimension):
    if proof is None or type(dimension) is not int or dimension <= 0:
        return False
    candidates = proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
    return (
        proof.forbidden_stable_score_calls == 0
        and proof.packed_score_candidates == candidates
        and proof.packed_vector_bytes_read == candidates * dimension * 4
        and proof.packed_score_batch_calls == (1 if candidates else 0)
    )


_DENSE_TYPED_SCALAR_EXACT_LIMIT = 4096
_DENSE_COSINE_SCORE_TOLERANCE = 1e-6


def dense_cosine_scores_valid(scores):
    return all(
        isinstance(score, (int, float))
        and not isinstance(score, bool)
        and math.isfinite(score)
        and -1 - _DENSE_COSINE_SCORE_TOLERANCE <= score <= 1 + _DENSE_COSINE_SCORE_TOLERANCE
        for score in scores
    )


def dense_document_ids_valid(ids):
    """Match producer ID admission and require uniqueness across result rows."""
    seen = set()
    for value in ids:
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeError:
                return False
        elif isinstance(value, str):
            try:
                value.encode("utf-8", errors="strict")
            except UnicodeError:
                return False
        else:
            return False
        if not value:
            return False
        if _go_unicode_space(value[0]) or _go_unicode_space(value[-1]) or value in seen:
            return False
        seen.add(value)
    return True


def _go_unicode_space(value):
    """Mirror unicode.IsSpace, which backs Go strings.TrimSpace."""
    codepoint = ord(value)
    return (
        codepoint in (0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0, 0x1680,
                      0x2028, 0x2029, 0x202F, 0x205F, 0x3000)
        or 0x2000 <= codepoint <= 0x200A
    )


def dense_score_plane_route_matches_graph(graph_route, proof_route):
    expected_route = {
        "typed_empty": "typed_empty",
        "typed_exact": "typed_exact",
        "quantized_rerank": "typed_hnsw",
    }.get(proof_route)
    return expected_route is not None and graph_route == expected_route


def dense_score_plane_prefix_route_matches_graph(graph_route, proof_route):
    return (proof_route == "quantized_rerank" if not graph_route
            else dense_score_plane_route_matches_graph(graph_route, proof_route))


def dense_failure_score_counters_match_graph(graph, proof):
    if graph is None or proof is None:
        return False
    exact_base_calls = proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
    return (
        exact_base_calls < 1 << 64
        and dense_score_plane_prefix_candidate_counters_match(proof)
        and graph.base_candidates <= proof.quantized_score_calls
        and (proof.normalized_candidate_width != 0 or graph.base_shadowed == 0)
        and proof.quantized_score_calls == graph.base_ann_scored
        and exact_base_calls == graph.exact_base_scored
        and exact_base_calls == graph.base_result_ids
        and proof.exact_suffix_score_calls == graph.delta_scored
    )


def dense_completed_graph_result_count(proof, top_k):
    if proof is None or not proof.completed or type(top_k) is not int or top_k < 0:
        return None
    if proof.route == "typed_empty":
        return 0
    if proof.route not in ("typed_exact", "quantized_rerank"):
        return None
    return min(top_k, proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls + proof.exact_suffix_score_calls)


def dense_score_plane_rerank_counters_match(proof):
    if proof is None or not proof.completed:
        return False
    if not dense_score_plane_prefix_candidate_counters_match(proof):
        return False
    if proof.route == "quantized_rerank":
        return (
            proof.normalized_candidate_width != 0
            and proof.raw_candidate_width != 0
            and proof.rerank_candidate_cap != 0
            and proof.exact_small_filter_score_calls == 0
            and proof.actual_rerank_candidates == min(proof.live_shortlist_candidates, proof.rerank_candidate_cap)
        )
    if proof.route == "typed_empty":
        return not any((
            proof.quantized_score_calls, proof.exact_suffix_score_calls, proof.exact_small_filter_score_calls,
            proof.raw_retained_candidates, proof.live_shortlist_candidates, proof.actual_rerank_candidates,
            proof.exact_base_rerank_score_calls,
        ))
    if proof.route == "typed_exact":
        return not any((
            proof.quantized_score_calls, proof.raw_retained_candidates, proof.live_shortlist_candidates,
            proof.actual_rerank_candidates, proof.exact_base_rerank_score_calls,
        ))
    return False


def dense_score_plane_prefix_candidate_counters_match(proof):
    if proof is None:
        return False
    if proof.requested_ef_search and proof.normalized_candidate_width > max(proof.requested_top_k, proof.requested_ef_search):
        return False
    expected_cap = min(proof.normalized_candidate_width, proof.requested_rerank_candidates or proof.normalized_candidate_width)
    return not (
        proof.rerank_candidate_cap != expected_cap
        or proof.normalized_candidate_width > proof.raw_candidate_width
        or (
            proof.normalized_candidate_width == 0
            and (proof.raw_candidate_width != 0 or proof.exact_small_filter_score_calls != 0)
        )
        or proof.raw_retained_candidates > proof.quantized_score_calls
        or proof.raw_retained_candidates > proof.raw_candidate_width
        or proof.live_shortlist_candidates > proof.raw_retained_candidates
        or proof.live_shortlist_candidates > proof.normalized_candidate_width
        or proof.actual_rerank_candidates > proof.live_shortlist_candidates
        or proof.actual_rerank_candidates > proof.rerank_candidate_cap
        or proof.actual_rerank_candidates != proof.exact_base_rerank_score_calls
    )


def dense_quantized_completed_graph_matches(work, proof, top_k, result_count, filter_requested):
    if (
        work is None
        or proof is None
        or type(top_k) is not int
        or top_k <= 0
        or type(result_count) is not int
        or result_count < 0
        or type(filter_requested) is not bool
        or not proof.available
        or not proof.completed
        or not proof.snapshot.available
        or not dense_score_plane_rerank_counters_match(proof)
    ):
        return False
    graph = work.graph
    exact_base_calls = proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
    exact_score_calls = exact_base_calls + proof.exact_suffix_score_calls
    if (
        not graph.available
        or not graph.completed
        or not dense_score_plane_route_matches_graph(graph.route, proof.route)
        or graph.snapshot != proof.snapshot
        or graph.filter.attempted != filter_requested
        or (graph.filter.attempted and not graph.filter.completed)
        or (filter_requested and result_count != min(top_k, graph.filter.eligible_rows))
        or graph.base_edges != 0
        or (proof.normalized_candidate_width == 0 and graph.base_shadowed != 0)
        or (not filter_requested and proof.exact_small_filter_score_calls != 0)
        or (filter_requested and (
            exact_score_calls > graph.filter.eligible_rows
            or (proof.route == "typed_exact" and exact_score_calls != graph.filter.eligible_rows)
        ))
        or proof.quantized_score_calls != graph.base_ann_scored
        or graph.base_candidates > proof.quantized_score_calls
        or exact_base_calls != graph.exact_base_scored
        or graph.base_result_ids != exact_base_calls
        or proof.exact_suffix_score_calls != graph.delta_scored
        or result_count > exact_score_calls
    ):
        return False
    if proof.route == "typed_empty":
        return (
            result_count == 0
            and graph.filter.attempted
            and graph.filter.completed
            and graph.filter.eligible_rows == 0
            and graph.base_shadowed == 0
        )
    if result_count != min(top_k, exact_score_calls):
        return False
    if proof.route == "typed_exact":
        if not filter_requested:
            return (
                proof.normalized_candidate_width == 0
                and proof.raw_candidate_width == 0
                and proof.rerank_candidate_cap == 0
                and graph.base_shadowed == 0
            )
        return (
            graph.filter.eligible_rows > 0
            and (
                proof.normalized_candidate_width == 0
                or graph.filter.eligible_rows <= _DENSE_TYPED_SCALAR_EXACT_LIMIT
            )
        )
    # Minimal traversal suppresses base_candidates only for unfiltered work.
    return (
        (not filter_requested or graph.filter.eligible_rows > _DENSE_TYPED_SCALAR_EXACT_LIMIT)
        and ((filter_requested and proof.raw_retained_candidates <= graph.base_candidates)
             or (not filter_requested and graph.base_candidates == 0))
        and graph.base_shadowed <= proof.raw_retained_candidates
        and proof.live_shortlist_candidates
        == min(proof.normalized_candidate_width, proof.raw_retained_candidates - graph.base_shadowed)
    )


def dense_failure_output_matches_completed_graph(output, result_count):
    if output is None or type(result_count) is not int or result_count < 0:
        return False
    if not output.attempted:
        return not any(vars(output).values())
    if (
        output.requested != result_count
        or output.fetched + output.missing > output.requested
        or output.retained_payload_fetches + output.missing > result_count
        or output.fetched > output.retained_payload_fetches
        or output.json_reconstruction_rows != output.fetched
        or output.typed_column_rows > output.retained_payload_fetches
        or (output.fetched == 0 and output.output_bytes != 0)
    ):
        return False
    return not output.completed or (
        output.retained_payload_fetches + output.missing == result_count
        and output.fetched == output.retained_payload_fetches
    )


def dense_quantized_response_work_matches(work, proof, top_k, result_count, filter_requested):
    """Bind the public quantized response to one producer-owned route model."""

    if not dense_quantized_completed_graph_matches(work, proof, top_k, result_count, filter_requested) or not work.completed:
        return False
    output = work.output
    return (
        output.completed
        and dense_failure_output_matches_completed_graph(output, result_count)
        and output.fetched == result_count
        and output.retained_payload_fetches == result_count
        and output.json_reconstruction_rows == result_count
        and output.typed_column_rows <= result_count
        and (output.output_bytes == 0) == (result_count == 0)
    )


@dataclass(frozen=True)
class DenseOutputWork:
    attempted: bool
    completed: bool
    requested: int
    fetched: int
    missing: int
    output_bytes: int
    retained_payload_fetches: int
    json_reconstruction_rows: int
    typed_column_rows: int

    @classmethod
    def from_dict(cls, data):
        values = _values(data, ("requested", "fetched", "missing", "output_bytes", "retained_payload_fetches", "json_reconstruction_rows", "typed_column_rows"),
                         ("attempted", "completed"))
        if not values["attempted"] and any(values.values()):
            raise ValueError("unattempted dense output carries work")
        if values["fetched"] + values["missing"] > values["requested"]:
            raise ValueError("dense output rows exceed requested rows")
        return cls(**values)


@dataclass(frozen=True)
class DenseSearchWork:
    version: int
    completed: bool
    graph: DenseGraphWork
    output: DenseOutputWork

    @classmethod
    def from_dict(cls, data):
        values = _values(data, ("version",), ("completed",), nested=("graph", "output"))
        if values["version"] != 1:
            raise ValueError("unsupported dense work version")
        graph = DenseGraphWork.from_dict(data["graph"])
        output = DenseOutputWork.from_dict(data["output"])
        if values["completed"] and (not graph.completed or not output.completed or output.missing or output.fetched != output.requested):
            raise ValueError("completed dense service lacks graph/output work")
        return cls(**values, graph=graph, output=output)


def optional_dense_work(data):
    # Null/missing is unavailable, never a zero-filled proof.
    return None if data is None else DenseSearchWork.from_dict(data)
