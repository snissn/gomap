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
