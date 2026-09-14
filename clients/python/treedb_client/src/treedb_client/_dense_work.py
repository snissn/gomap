"""Owned dense-work v1 values shared by HTTP and native decoding."""

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

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            raise ValueError("dense score-plane proof must be an object")
        required = {
            "version", "available", "completed", "requested_mode", "effective_mode", "route",
            "requested_top_k", "requested_ef_search", "requested_rerank_candidates",
            "normalized_candidate_width", "raw_candidate_width", "rerank_candidate_cap",
            "raw_retained_candidates", "live_shortlist_candidates", "actual_rerank_candidates",
            "quantized_score_calls", "quantized_code_bytes_read", "exact_base_rerank_score_calls",
            "exact_suffix_score_calls", "exact_small_filter_score_calls", "exact_base_vector_bytes_read",
            "exact_suffix_vector_bytes_read", "snapshot",
        }
        optional = {"reason", "quantized_index_name", "quantized_codec", "quantized_version", "quantized_config_hash"}
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
        for name in ("quantized_version", "quantized_config_hash"):
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
        if values["completed"] and (not values["available"] or not snapshot.available or not values["route"]):
            raise ValueError("completed dense score-plane proof lacks captured work")
        if values["completed"] and values["route"] == "quantized_rerank" and values["quantized_score_calls"] == 0:
            raise ValueError("completed quantized rerank proof has no quantized score calls")
        if values["completed"]:
            if values["route"] == "quantized_rerank":
                if (
                    (values["requested_ef_search"] != 0 and values["normalized_candidate_width"] > max(values["requested_top_k"], values["requested_ef_search"]))
                    or
                    values["rerank_candidate_cap"] > values["normalized_candidate_width"]
                    or values["live_shortlist_candidates"] > values["normalized_candidate_width"]
                    or values["normalized_candidate_width"] > values["raw_candidate_width"]
                    or values["exact_small_filter_score_calls"] != 0
                    or values["actual_rerank_candidates"] != values["exact_base_rerank_score_calls"]
                    or values["actual_rerank_candidates"] != min(values["live_shortlist_candidates"], values["rerank_candidate_cap"])
                ):
                    raise ValueError("completed dense score-plane proof quantized rerank counters are inconsistent")
            elif values["route"] == "typed_empty" and (
                values["quantized_score_calls"] != 0
                or values["exact_suffix_score_calls"] != 0
                or values["exact_small_filter_score_calls"] != 0
                or values["actual_rerank_candidates"] != 0
                or values["exact_base_rerank_score_calls"] != 0
            ):
                raise ValueError("completed dense score-plane proof typed-empty counters are inconsistent")
            elif values["route"] == "typed_exact" and (
                values["quantized_score_calls"] != 0
                or values["actual_rerank_candidates"] != 0
                or values["exact_base_rerank_score_calls"] != 0
            ):
                raise ValueError("completed dense score-plane proof typed-exact counters are inconsistent")
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
