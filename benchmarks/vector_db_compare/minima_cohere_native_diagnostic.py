#!/usr/bin/env python3
"""Nonqualifying Minima-style public/native lifecycle using existing Cohere FP32 exports.

Separate from the frozen 8D Minima/M5 schema. No download or protected holdout access.
Freeze this reviewed harness before collection; use --freeze, then --run with its SHA256.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import threading
import time

import numpy as np

import minima_treedb_runner as existing
from treedb_client import _dense_work as dense_contract

SCHEMA = "treedb_minima_cohere_native_diagnostic/v2"
RSS_ARTIFACT_SCHEMA = "treedb_cohere_768_rss_boundary/v3"
QUANTIZED_RSS_ARTIFACT_SCHEMA = "treedb_cohere_768_sq8_rss_boundary/v1"
QUANTIZED_PROFILE_NAME = "minima_sq8"
RSS_RECALL_TARGET = .90
RSS_CONTROLS = [32, 64, 128, 256, 512, 1024, 2048]
RSS_CALIBRATION_QUERIES = list(range(100))
RSS_REVALIDATION_QUERIES = list(range(100, 200))
RSS_SELECTION_PROTOCOL = "lowest_control_passing_both_fixed_query_sets/v1"
GIB = 1 << 30
FROZEN_JSON_MAX_BYTES = 1 << 20
EVIDENCE_JSON_MAX_BYTES = 64 << 20


def quantized_representation_arm():
    return {
        "schema": "treedb_cohere_quantized_representation/v1",
        "source_vectors": "fp32",
        "query_vectors": "fp32",
        "truth_scores": "canonical_fp32_cosine",
        "query_mode": "quantized_rerank",
        "quantized_index_name": QUANTIZED_PROFILE_NAME,
        "codec": "scalar_u8",
        "version": 1,
        "calibration": "legacy",
        "quantized_config_hash": 0,
        "vector_m": 16,
        "requested_rerank_policy": "R=E_at_each_predeclared_coordinate",
    }


def validate_quantized_options(query_mode, index_name, rss_only, rows):
    if query_mode == "exact":
        if index_name is not None:
            raise ValueError("exact Cohere mode does not accept a quantized index")
        return
    if query_mode != "quantized_rerank" or index_name != QUANTIZED_PROFILE_NAME:
        raise ValueError("quantized Cohere mode requires the minima_sq8 profile")
    if rss_only and rows != 500000:
        raise ValueError("quantized RSS requires the frozen 500000-row export")


def _coordinate_rows(quality):
    for split in ("calibration", "revalidation"):
        for row in quality[split]["curve"]:
            row.update(ef_search=row["control"], rerank_candidates=row["control"])
    selected = quality["selected_control"]
    quality["selected_coordinate"] = None if selected is None else {
        "ef_search": selected, "rerank_candidates": selected,
    }
    return quality


def fixed_set_quality_valid(quality, controls=RSS_CONTROLS,
                            calibration_queries=RSS_CALIBRATION_QUERIES,
                            revalidation_queries=RSS_REVALIDATION_QUERIES,
                            target=RSS_RECALL_TARGET, *, quantized_coordinates=False):
    """Validate the shared lowest-coordinate/two-fixed-set quality contract."""
    try:
        calibration = quality["calibration"]
        revalidation = quality["revalidation"]
        first, second = calibration["curve"], revalidation["curve"]
        selected = quality["selected_control"]
        split_keys = ({"queries", "curve"}, {"queries", "curve", "passed"})
        row_keys = {"control", "mean_recall_at_10", "mean_ndcg_at_10",
                    "per_query", "per_query_ndcg_at_10"}
        if quantized_coordinates:
            row_keys |= {"ef_search", "rerank_candidates"}
        if (quality["selection_protocol"] != RSS_SELECTION_PROTOCOL
                or not same_json(quality["target_mean_recall_at_10"], target)
                or not isinstance(calibration, dict) or not isinstance(revalidation, dict)
                or set(calibration) != split_keys[0] or set(revalidation) != split_keys[1]
                or not same_json(calibration["queries"], list(calibration_queries))
                or not same_json(revalidation["queries"], list(revalidation_queries))
                or revalidation["passed"] is not True
                or not first or len(first) != len(second)
                or not same_json([row["control"] for row in first], controls[:len(first)])
                or not same_json([row["control"] for row in second], controls[:len(second)])
                or selected != first[-1]["control"]
                or type(selected) is not int
                or (quantized_coordinates and not same_json(quality["selected_coordinate"], {
                        "ef_search": selected, "rerank_candidates": selected,
                    }))):
            return False
        for left, right in zip(first, second):
            control = left["control"]
            if (type(control) is not int or type(right["control"]) is not int
                    or right["control"] != control or set(left) != row_keys or set(right) != row_keys):
                return False
            if quantized_coordinates and (
                    any(type(row[name]) is not int for row in (left, right)
                        for name in ("ef_search", "rerank_candidates"))
                    or left["ef_search"] != control or left["rerank_candidates"] != control
                    or right["ef_search"] != control or right["rerank_candidates"] != control):
                return False
            for row, queries in ((left, calibration_queries), (right, revalidation_queries)):
                recalls = row["per_query"]
                ndcgs = row["per_query_ndcg_at_10"]
                if (type(row["mean_recall_at_10"]) not in (int, float)
                        or type(row["mean_ndcg_at_10"]) not in (int, float)
                        or len(recalls) != len(queries) or len(ndcgs) != len(queries)
                        or not all(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1
                                   for value in recalls + ndcgs)
                        or not math.isclose(row["mean_recall_at_10"], sum(recalls) / len(recalls),
                                            rel_tol=0.0, abs_tol=1e-12)
                        or not math.isclose(row["mean_ndcg_at_10"], sum(ndcgs) / len(ndcgs),
                                            rel_tol=0.0, abs_tol=1e-12)):
                    return False
        if any(left["mean_recall_at_10"] >= target and right["mean_recall_at_10"] >= target
               for left, right in zip(first[:-1], second[:-1])):
            return False
        return first[-1]["mean_recall_at_10"] >= target and second[-1]["mean_recall_at_10"] >= target
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return False


def quantized_quality_valid(quality, controls=RSS_CONTROLS,
                            calibration_queries=RSS_CALIBRATION_QUERIES,
                            revalidation_queries=RSS_REVALIDATION_QUERIES,
                            target=RSS_RECALL_TARGET):
    return fixed_set_quality_valid(
        quality, controls, calibration_queries, revalidation_queries, target,
        quantized_coordinates=True,
    )


def select_quantized_cohorts(search, truth, eligible_counts, rows, controls,
                             calibration_queries, revalidation_queries, target):
    """Select each ANN cohort independently; small filtered cohorts stay exact."""
    quality = {}
    queries = list(calibration_queries) + list(revalidation_queries)
    if not queries or max(queries) >= len(next(iter(truth.values()))):
        raise ValueError("quantized Cohere fixed query sets exceed the frozen oracle")
    for eligible in sorted(eligible_counts, key=lambda value: (value != rows, value)):
        expected = truth[str(eligible)]
        if eligible <= 4096 and eligible != rows:
            control = controls[0]
            per_query = [search(eligible, control, query) == expected[query] for query in queries]
            quality[str(eligible)] = {
                "selection_protocol": "typed_exact_correctness_at_first_predeclared_coordinate/v1",
                "target_mean_recall_at_10": 1.0,
                "selected_control": control if all(per_query) else None,
                "selected_coordinate": ({"ef_search": control, "rerank_candidates": control}
                                        if all(per_query) else None),
                "calibration": {"queries": list(calibration_queries), "curve": []},
                "revalidation": {"queries": list(revalidation_queries), "curve": [],
                                 "passed": all(per_query)},
                "exact_query_matches": per_query,
            }
            continue
        selected = calibrate_ann_control(
            lambda control, query, eligible=eligible: search(eligible, control, query),
            expected, controls, calibration_queries, revalidation_queries, target,
        )
        quality[str(eligible)] = _coordinate_rows(selected)
    return quality


def validate_quantized_response(response, rows, eligible, ef, filter_requested, generation):
    work, proof = getattr(response, "dense_work", None), getattr(response, "score_plane", None)
    expected_route = ("typed_empty" if eligible == 0 else "typed_exact"
                      if filter_requested and eligible <= 4096 else "quantized_rerank")
    try:
        expected_width = min(eligible, max(10, ef))
        exact_calls = (proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
                       + proof.exact_suffix_score_calls)
        route_work_valid = {
            "typed_empty": (
                proof.quantized_score_calls == 0 and exact_calls == 0
                and proof.raw_retained_candidates == 0 and proof.live_shortlist_candidates == 0
                and proof.actual_rerank_candidates == 0 and work.graph.base_candidates == 0
                and work.graph.base_shadowed == 0 and work.output.output_bytes == 0
            ),
            "typed_exact": (
                proof.quantized_score_calls == 0 and proof.exact_base_rerank_score_calls == 0
                and exact_calls == eligible and proof.raw_retained_candidates == 0
                and proof.live_shortlist_candidates == 0 and proof.actual_rerank_candidates == 0
                and work.graph.base_candidates == 0 and work.graph.base_shadowed == 0
            ),
            "quantized_rerank": (
                proof.quantized_score_calls > 0
                and proof.raw_retained_candidates <= proof.quantized_score_calls
                and work.graph.base_shadowed <= proof.raw_retained_candidates
                and proof.live_shortlist_candidates
                    == min(proof.normalized_candidate_width,
                           proof.raw_retained_candidates - work.graph.base_shadowed)
                and proof.actual_rerank_candidates
                    == min(proof.live_shortlist_candidates, proof.rerank_candidate_cap)
                and proof.actual_rerank_candidates == proof.exact_base_rerank_score_calls
            ),
        }[expected_route]
        valid = (
            type(rows) is int and rows > 0 and type(eligible) is int and 0 <= eligible <= rows
            and type(ef) is int and ef > 0 and type(filter_requested) is bool
            and type(generation) is int and generation > 0
            and response.native_command_version == 3
            and response.index.generation == generation
            and response.index.vector_strategy == "column_graph"
            and response.route == "ann"
            and response.exact_fallbacks == 0
            and response.full_document_scan_fallbacks == 0
            and work is not None and proof is not None
            and work.version == 1 and work.completed
            and work.graph.available and work.graph.completed
            and work.output.attempted and work.output.completed and work.output.missing == 0
            and work.output.requested == len(response.documents)
            and proof.version == 1 and proof.available and proof.completed
            and proof.requested_mode == "quantized_rerank"
            and proof.effective_mode == "quantized_rerank"
            and proof.route == expected_route and not proof.reason
            and proof.quantized_index_name == QUANTIZED_PROFILE_NAME
            and proof.quantized_codec == "scalar_u8" and proof.quantized_version == 1
            and proof.quantized_config_hash == 0
            and proof.requested_top_k == 10
            and proof.requested_ef_search == ef
            and proof.requested_rerank_candidates == ef
            and proof.normalized_candidate_width == expected_width
            and proof.normalized_candidate_width <= proof.raw_candidate_width
            and proof.raw_candidate_width <= eligible
            and proof.rerank_candidate_cap == min(eligible, expected_width, ef)
            and existing.quantized_snapshot_valid(proof.snapshot, generation)
            and work.graph.snapshot == proof.snapshot
            and route_work_valid
            and (not filter_requested or work.graph.filter.eligible_rows == eligible)
            and ((filter_requested
                  and proof.raw_retained_candidates <= work.graph.base_candidates
                  and work.graph.base_candidates <= proof.quantized_score_calls)
                 or (not filter_requested and work.graph.base_candidates == 0))
            and ((len(response.documents) == 0 and work.output.output_bytes == 0)
                 or (len(response.documents) > 0 and work.output.output_bytes > 0))
            and dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            and dense_contract.dense_quantized_response_work_matches(
                work, proof, 10, len(response.documents), filter_requested,
            )
        )
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise RuntimeError("quantized proof does not bind the Cohere request and current producer owner")


def quantized_upsert_state(state, rows, updated):
    """Apply one authored upsert, advancing its owner only when content changes."""
    try:
        rows = frozenset(rows)
        if (not rows or any(type(row) is not int or row < 0 for row in rows)
                or type(updated) is not bool
                or type(state["owner_advance"]) is not int or state["owner_advance"] < 0
                or type(state["folded"]) is not bool):
            return None
        current_updated = frozenset(state["updated"])
        current_touched = frozenset(state["touched"])
        current_deleted = frozenset(state["deleted"])
        changed = frozenset(
            row for row in rows
            if row in current_deleted or ((row in current_updated) != updated)
        )
        published = bool(changed)
        return {
            "updated": ((current_updated | rows) if updated else (current_updated - rows)),
            "touched": current_touched | changed,
            "deleted": current_deleted - rows,
            "owner_advance": state["owner_advance"] + int(published),
            "folded": state["folded"],
        }
    except (KeyError, TypeError, ValueError):
        return None


def quantized_lifecycle_states(initial_updated, initial_touched, initial_deleted,
                               initial_advance, mutation_windows,
                               request_start_ns, request_end_ns, folded=False):
    """Enumerate whole-batch states that can linearize inside one query interval."""
    if (type(initial_advance) is not int or initial_advance < 0
            or type(request_start_ns) is not int or type(request_end_ns) is not int
            or request_start_ns > request_end_ns or type(folded) is not bool):
        return []
    states = [{
        "updated": frozenset(initial_updated), "touched": frozenset(initial_touched),
        "deleted": frozenset(initial_deleted), "owner_advance": initial_advance,
        "folded": folded,
    }]
    mandatory_prefix = 0
    try:
        ordered_windows = sorted(mutation_windows, key=lambda item: item["started_monotonic_ns"])
    except (KeyError, TypeError, ValueError):
        return []
    for window in ordered_windows:
        try:
            started, ended, success = (window["started_monotonic_ns"],
                                       window.get("ended_monotonic_ns"), window.get("success"))
            rows = frozenset(window["rows"])
            updated, published = window["updated"], window["published"]
            if (type(started) is not int or started < 0
                    or (ended is not None and (type(ended) is not int or ended < started))
                    or (success is not None and type(success) is not bool) or not rows
                    or any(type(row) is not int or row < 0 for row in rows)
                    or type(updated) is not bool or type(published) is not bool
                    or ((ended is None) != (success is None))):
                return []
            if started > request_end_ns:
                break
            if success is False:
                continue
            prior = states[-1]
            current = quantized_upsert_state(prior, rows, updated)
            if current is None or (current["owner_advance"] != prior["owner_advance"]) != published:
                return []
            if published:
                states.append(current)
            if (published and success is True and ended is not None
                    and ended <= request_start_ns):
                mandatory_prefix = len(states) - 1
        except (KeyError, TypeError, ValueError):
            return []
    return states[mandatory_prefix:]


def quantized_snapshot_identity(snapshot):
    try:
        manifest = lambda value: {
            "generation": value.generation, "format": value.format,
            "version": value.version, "checksum": value.checksum,
        }
        return {
            "schema_hash": snapshot.schema_hash,
            "schema_generation": snapshot.schema_generation,
            "base_manifest": manifest(snapshot.base_manifest),
            "current_manifest": manifest(snapshot.current_manifest),
            "base_coverage_lsn": snapshot.base_coverage_lsn,
            "current_coverage_lsn": snapshot.current_coverage_lsn,
        }
    except (AttributeError, TypeError):
        return None


def quantized_snapshot_matches_state(snapshot, initial, state):
    current = quantized_snapshot_identity(snapshot)
    if current is None or initial is None:
        return False
    try:
        generation = initial["current_manifest"]["generation"] + state["owner_advance"]
        coverage = initial["current_coverage_lsn"] + state["owner_advance"]
        valid = (
            generation < 1 << 64 and coverage < 1 << 64
            and current["schema_hash"] == initial["schema_hash"]
            and current["schema_generation"] == initial["schema_generation"]
            and current["current_manifest"]["generation"] == generation
            and current["current_coverage_lsn"] == coverage
        )
        if state["folded"]:
            return valid and current["base_manifest"] == current["current_manifest"] \
                and current["base_coverage_lsn"] == current["current_coverage_lsn"]
        return valid and current["base_manifest"] == initial["base_manifest"] \
            and current["base_coverage_lsn"] == initial["base_coverage_lsn"]
    except (KeyError, TypeError):
        return False


def quantized_state_widths(state, rows, eligible, ef):
    try:
        if (type(rows) is not int or rows <= 0 or type(eligible) is not int
                or not 0 <= eligible <= rows or type(ef) is not int or ef <= 0
                or type(state["owner_advance"]) is not int or state["owner_advance"] < 0
                or type(state["folded"]) is not bool):
            return None
        if any(type(ordinal) is not int or not 0 <= ordinal < rows
               for values in (state["updated"], state["touched"], state["deleted"])
               for ordinal in values):
            return None
        shadowed = 0 if state["folded"] else sum(
            1 for row in state["touched"] if row < rows and (row * 7919) % rows < eligible
        )
        effective = min(eligible, max(10, ef))
        return effective, min(eligible, effective + shadowed), min(eligible, effective, ef), shadowed
    except (KeyError, TypeError):
        return None


def quantized_response_matches_state(response, state, rows, eligible, ef, initial_snapshot):
    widths = quantized_state_widths(state, rows, eligible, ef)
    if widths is None:
        return False
    effective, raw, rerank_cap, shadow_allowance = widths
    try:
        return (
            (response.score_plane.normalized_candidate_width,
             response.score_plane.raw_candidate_width,
             response.score_plane.rerank_candidate_cap) == (effective, raw, rerank_cap)
            and response.dense_work.graph.base_shadowed <= shadow_allowance
            and quantized_snapshot_matches_state(response.score_plane.snapshot, initial_snapshot, state)
            and quantized_response_projection_matches_state(response.documents, state)
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def quantized_projection_matches_update_states(
        documents, initial_updated, mutation_windows, request_start_ns, request_end_ns):
    """Compatibility helper for focused atomic-projection tests."""
    return any(quantized_response_projection_matches_state(documents, state)
               for state in quantized_lifecycle_states(
                   initial_updated, (), (), 0, mutation_windows, request_start_ns, request_end_ns,
               ))


def quantized_response_projection_matches_state(documents, state):
    try:
        return all(
            document.content == f"minima-cohere:{int(document.id.removeprefix('row-'))}"
            + (":updated" if int(document.id.removeprefix("row-")) in state["updated"] else "")
            and document.id == f"row-{int(document.id.removeprefix('row-')):06d}"
            and int(document.id.removeprefix("row-")) not in state["deleted"]
            for document in documents
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def same_json(left, right):
    try:
        return canonical(left) == canonical(right)
    except (TypeError, ValueError):
        return False


def strict_json_object(path, label, maximum_bytes=FROZEN_JSON_MAX_BYTES):
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(maximum_bytes + 1)
        if len(raw) > maximum_bytes:
            raise ValueError(f"{label} exceeds {maximum_bytes} bytes")

        def reject_duplicates(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"{label} contains duplicate key {key!r}")
                result[key] = value
            return result

        def reject_constant(value):
            raise ValueError(f"{label} contains nonfinite {value}")

        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates,
                           parse_constant=reject_constant)
        if not isinstance(value, dict) or not value:
            raise ValueError(f"{label} must be a nonempty JSON object")
        return value, raw
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {label}: {exc}") from exc


def bytes_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_frozen_json(path, value, label):
    raw = canonical(value)
    if len(raw) > FROZEN_JSON_MAX_BYTES:
        raise ValueError(f"{label} exceeds {FROZEN_JSON_MAX_BYTES} bytes")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("xb") as stream:
        stream.write(raw)
    return bytes_digest(raw)


def counts(rows):
    if rows == 512:
        return [64, 65, 128, 256, 512]
    return sorted({min(rows, n) for n in (4096, 4097, 5000, 50000, rows)})


def predicate(rows, eligible):
    return None if eligible == rows else {"field": "meta.user_id", "operator": "<", "value": f"{eligible:06d}"}


def make_document(vectors, row, total, updated=False):
    return {"id": f"row-{row:06d}", "content": f"minima-cohere:{row}" + (":updated" if updated else ""),
            "embedding": vectors[row].tolist(),
            "meta": {"user_id": f"{(row * 7919) % total:06d}", "fpath": f"/cohere/{row // 256:06d}.txt"}}


def exact_truth(vectors, queries, eligible_counts):
    """Bounded smoke oracle only; the 500K run consumes the existing exhaustive oracle."""
    normalized = np.asarray(vectors, dtype=np.float64)
    normalized /= np.linalg.norm(normalized, axis=1)[:, None]
    normalized_queries = np.asarray(queries, dtype=np.float64)
    normalized_queries /= np.linalg.norm(normalized_queries, axis=1)[:, None]
    ranks = (np.arange(len(vectors)) * 7919) % len(vectors)
    scores = normalized_queries @ normalized.T
    result = {}
    for eligible in eligible_counts:
        rows = np.flatnonzero(ranks < eligible)
        result[str(eligible)] = [[f"row-{int(row):06d}" for row in rows[np.lexsort((rows, -values[rows]))[:10]]]
                                for values in scores]
    return result


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "total_ns": sum(values),
            **{name + "_ns": values[math.ceil(len(values) * fraction) - 1]
               for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def dataset_identity(dataset, rows):
    dataset = Path(dataset).resolve()
    manifest = json.loads((dataset / "manifest.json").read_text())
    if (manifest.get("dimensions"), manifest.get("top_k"), manifest.get("exact_train_query_overlap")) != (768, 10, 0):
        raise ValueError("expected the existing real 768D top-10 nonoverlapping diagnostic export")
    if rows not in (512, 500000) or rows > manifest["rows"] or math.gcd(rows, 7919) != 1:
        raise ValueError("only real-prefix 512-row smoke or 500000-row diagnostic runs are supported")
    query_count = 4 if rows == 512 else 200
    if query_count > manifest["query_count"]:
        raise ValueError("not enough exported queries")
    if rows == 500000 and (manifest["rows"], manifest["query_count"]) != (500000, 200):
        raise ValueError("diagnostic oracle requires exactly 500000 exported rows and 200 queries")
    files = {}
    for name, expected_size in (("documents", manifest["rows"] * 768 * 4),
                                ("queries", manifest["query_count"] * 768 * 4), ("truth", None)):
        path = dataset / (name + (".json" if name == "truth" else ".f32"))
        if expected_size is not None and path.stat().st_size != expected_size:
            raise ValueError(f"{name} size does not match dataset manifest")
        files[name] = digest(path)
        if files[name] != manifest[name + "_sha256"]:
            raise ValueError(f"{name} hash does not match dataset manifest")
    return dataset, manifest, files, query_count


def calibrate_ann_control(search, truth, controls, calibration_queries, revalidation_queries, target):
    if (not controls or controls != sorted(set(controls)) or not calibration_queries or not revalidation_queries
            or set(calibration_queries) & set(revalidation_queries) or not 0 < target <= 1):
        raise ValueError("invalid ANN quality calibration contract")

    def metrics(control, queries):
        recalls, ndcgs = [], []
        for query in queries:
            expected = truth[query]
            actual = search(control, query)
            if len(actual) != len(expected) or len(set(actual)) != len(actual):
                raise RuntimeError("ANN result cardinality differs from exact truth")
            recalls.append(len(set(actual) & set(expected)) / len(expected))
            ndcgs.append(binary_ndcg(actual, expected))
        return recalls, ndcgs

    calibration, revalidation, selected = [], [], None
    for control in controls:
        first, first_ndcg = metrics(control, calibration_queries)
        second, second_ndcg = metrics(control, revalidation_queries)
        calibration.append({"control": control, "mean_recall_at_10": sum(first) / len(first),
                            "mean_ndcg_at_10": sum(first_ndcg) / len(first_ndcg),
                            "per_query": first, "per_query_ndcg_at_10": first_ndcg})
        revalidation.append({"control": control, "mean_recall_at_10": sum(second) / len(second),
                             "mean_ndcg_at_10": sum(second_ndcg) / len(second_ndcg),
                             "per_query": second, "per_query_ndcg_at_10": second_ndcg})
        if calibration[-1]["mean_recall_at_10"] >= target and revalidation[-1]["mean_recall_at_10"] >= target:
            selected = control
            break
    return {
        "selection_protocol": RSS_SELECTION_PROTOCOL,
        "target_mean_recall_at_10": target, "selected_control": selected,
        "calibration": {"queries": list(calibration_queries), "curve": calibration},
        "revalidation": {"queries": list(revalidation_queries), "curve": revalidation,
                         "passed": selected is not None},
    }


def binary_ndcg(actual, expected):
    relevant = set(expected)
    ideal = sum(1 / math.log2(rank + 2) for rank in range(len(expected)))
    return sum(1 / math.log2(rank + 2) for rank, item in enumerate(actual) if item in relevant) / ideal


def construction_calibration_contract(ef_construction):
    return {
        "schema": "treedb_column_graph_construction_calibration/v1",
        "ef_construction": ef_construction, "control_ef_construction": 128,
        "max_absolute_recall_loss": .002, "max_absolute_binary_ndcg_loss": .002,
        "max_selected_route_regression": {"qps": .05, "p95": .05, "p99": .05},
    }


def rss_comparison_contract(plan):
    return {
        "schema": "cohere_500k_768d_matched_rss/v3", "rows": plan["rows"],
        "dimensions": plan["dimensions"], "metric": "cosine", "top_k": plan["top_k"],
        "dataset_manifest_sha256": plan["dataset_manifest_sha256"],
        "dataset_files_sha256": plan["dataset_files_sha256"],
        "logical_ids": "row-<six-digit-ordinal>",
        "document": "id, content, FP32 embedding, nested meta.user_id and meta.fpath",
        "scalar_indexes": ["meta.fpath:string", "meta.user_id:string"],
        "query_filter": "none (all 500000 rows eligible)",
        "batch_size": plan["batch_size"], "durability_visibility": "durable_and_visible_before_ack",
        "cpu_affinity": plan["cpu_affinity"], "host_memory_bytes": plan["host_memory_bytes"],
        "treedb_go_runtime": plan["treedb_go_runtime"],
        "host_resource_identity": plan["host_resource_identity"],
        "platform": plan["platform"], "quality_metric": "mean_recall_at_10",
        "quality_target": plan["rss_recall_target"],
        "quality_selection_protocol": RSS_SELECTION_PROTOCOL,
        "ann_controls": {
            "treedb_ef_search": plan["rss_controls"],
            "qdrant_hnsw_ef": plan["rss_controls"],
        },
        "calibration_queries": plan["rss_calibration_queries"],
        "revalidation_queries": plan["rss_revalidation_queries"],
        "rss_boundary": "fresh_server_and_backend_through_initial_ann_ready_and_quality_gated_query",
        "rss_scope": "server_process_lifetime_VmHWM_including_resident_mappings",
    }


def rss_comparison_contract_valid(contract):
    """Validate the immutable matched-RSS contract without trusting a sibling arm."""
    try:
        sha256_valid = lambda value: (
            isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        )
        expected_keys = {
            "schema", "rows", "dimensions", "metric", "top_k",
            "dataset_manifest_sha256", "dataset_files_sha256", "logical_ids", "document",
            "scalar_indexes", "query_filter", "batch_size", "durability_visibility",
            "cpu_affinity", "host_memory_bytes", "treedb_go_runtime",
            "host_resource_identity", "platform", "quality_metric", "quality_target",
            "quality_selection_protocol", "ann_controls", "calibration_queries",
            "revalidation_queries", "rss_boundary", "rss_scope",
        }
        hashes = contract["dataset_files_sha256"]
        affinity = contract["cpu_affinity"]
        host = contract["host_resource_identity"]
        runtime = contract["treedb_go_runtime"]
        return (
            isinstance(contract, dict) and set(contract) == expected_keys
            and contract["schema"] == "cohere_500k_768d_matched_rss/v3"
            and all(type(contract[name]) is int for name in (
                "rows", "dimensions", "top_k", "batch_size", "host_memory_bytes",
            ))
            and (contract["rows"], contract["dimensions"], contract["top_k"],
                 contract["batch_size"]) == (500000, 768, 10, 256)
            and contract["host_memory_bytes"] > 0
            and contract["metric"] == "cosine"
            and sha256_valid(contract["dataset_manifest_sha256"])
            and isinstance(hashes, dict) and set(hashes) == {"documents", "queries", "truth"}
            and all(sha256_valid(value) for value in hashes.values())
            and same_json(contract["scalar_indexes"], ["meta.fpath:string", "meta.user_id:string"])
            and isinstance(affinity, list) and affinity
            and all(type(cpu) is int and cpu >= 0 for cpu in affinity)
            and affinity == sorted(set(affinity))
            and isinstance(runtime, dict) and set(runtime) == {"GOMAXPROCS", "GOGC", "GOMEMLIMIT"}
            and all(isinstance(value, str) for value in runtime.values())
            and isinstance(host, dict)
            and set(host) == {"machine_id", "boot_id", "page_size_bytes", "numa_mems",
                              "cgroup_membership", "cgroup_limits"}
            and all(isinstance(host[name], str) and host[name]
                    for name in ("machine_id", "boot_id", "numa_mems", "cgroup_membership"))
            and type(host["page_size_bytes"]) is int and host["page_size_bytes"] > 0
            and isinstance(host["cgroup_limits"], dict)
            and all(isinstance(key, str) and isinstance(value, str)
                    for key, value in host["cgroup_limits"].items())
            and isinstance(contract["platform"], str) and contract["platform"]
            and same_json(contract["quality_target"], RSS_RECALL_TARGET)
            and contract["quality_selection_protocol"] == RSS_SELECTION_PROTOCOL
            and same_json(contract["ann_controls"], {
                "treedb_ef_search": RSS_CONTROLS, "qdrant_hnsw_ef": RSS_CONTROLS,
            })
            and same_json(contract["calibration_queries"], RSS_CALIBRATION_QUERIES)
            and same_json(contract["revalidation_queries"], RSS_REVALIDATION_QUERIES)
            and contract["logical_ids"] == "row-<six-digit-ordinal>"
            and contract["document"] == "id, content, FP32 embedding, nested meta.user_id and meta.fpath"
            and contract["query_filter"] == "none (all 500000 rows eligible)"
            and contract["durability_visibility"] == "durable_and_visible_before_ack"
            and contract["quality_metric"] == "mean_recall_at_10"
            and contract["rss_boundary"]
                == "fresh_server_and_backend_through_initial_ann_ready_and_quality_gated_query"
            and contract["rss_scope"]
                == "server_process_lifetime_VmHWM_including_resident_mappings"
        )
    except (KeyError, TypeError, ValueError):
        return False


def treedb_service_environment(plan):
    child = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
    child.update({key: value for key, value in plan["treedb_go_runtime"].items() if value})
    return child


def host_resource_identity():
    cgroup = Path("/proc/self/cgroup").read_text().strip()
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
    unified = next((line.split("::", 1)[1] for line in cgroup.splitlines() if line.startswith("0::")), None)
    limits = {}
    if unified is not None:
        root, current = Path("/sys/fs/cgroup"), Path("/sys/fs/cgroup") / unified.lstrip("/")
        if not current.resolve().is_relative_to(root):
            raise RuntimeError("cgroup path escaped its mount")
        while True:
            for name in ("cpu.max", "cpuset.cpus.effective", "cpuset.mems.effective",
                         "memory.high", "memory.max", "memory.swap.max"):
                path = current / name
                if path.is_file():
                    limits[f"{current.relative_to(root)}/{name}"] = path.read_text().strip()
            if current == root:
                break
            current = current.parent
    identity = {
        "machine_id": Path("/etc/machine-id").read_text().strip(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "page_size_bytes": os.sysconf("SC_PAGE_SIZE"), "numa_mems": status.get("Mems_allowed_list", "").strip(),
        "cgroup_membership": cgroup, "cgroup_limits": limits,
    }
    if not identity["machine_id"] or not identity["boot_id"] or not identity["numa_mems"]:
        raise RuntimeError("host resource identity is incomplete")
    return identity


def process_peak_at_boundary(pid, expected_identity, expected_affinity):
    sample = existing.common.process_peak_rss(pid)
    identity = existing.common.linux_process_identity(pid)
    try:
        affinity = sorted(os.sched_getaffinity(pid)) if type(pid) is int and pid > 0 else []
    except (OSError, ProcessLookupError):
        affinity = []
    if (sample.get("availability") != "measured" or sample.get("bytes") is None
            or identity != expected_identity or sample.get("process_identity") != expected_identity
            or affinity != expected_affinity):
        return {**sample, "availability": "unavailable", "bytes": None,
                "reason": "server process identity, affinity, or VmHWM changed/unavailable at boundary"}
    return sample


def finalize_rss_artifact(artifact, failure):
    if failure:
        artifact["state"] = "uncalibrated"
        artifact["reasons"].append(f"terminal failure: {failure}")
    return artifact


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    query_mode = getattr(args, "query_mode", "exact")
    quantized_index_name = getattr(args, "quantized_index_name", None)
    rss_only = getattr(args, "rss_only", False)
    validate_quantized_options(query_mode, quantized_index_name, rss_only, args.rows)
    harness = existing.repository_commit()
    dataset, manifest, files, query_count = dataset_identity(args.dataset, args.rows)
    if rss_only and args.rows != 500000:
        raise ValueError("matched RSS mode requires the frozen 500000-row export")
    existing.service_binary_build_provenance(args.service_bin, args.product_commit)
    root_tree = lambda path: subprocess.check_output(["git", "rev-parse", "HEAD:" + path], cwd=source, text=True).strip()
    product_tree = lambda path: subprocess.check_output(["git", "rev-parse", args.product_commit + ":" + path], cwd=source, text=True).strip()
    serving, serving_raw = strict_json_object(args.serving, "column_graph serving limits")
    plan = {"schema": SCHEMA, "qualification": "not_evaluated", "mode": "smoke" if args.rows == 512 else "diagnostic",
            "harness_commit": harness, "harness_source_sha256": digest(Path(__file__)),
            "harness_trees": {path: root_tree(path) for path in ("benchmarks/vector_db_compare", "clients/python/treedb_client")},
            "product_commit": args.product_commit, "service_bin": str(args.service_bin.resolve()),
            "product_trees": {path: product_tree(path) for path in ("TreeDB", "cmd/treedb-document-service", "internal", "go.mod", "go.sum")},
            "service_sha256": digest(args.service_bin), "dataset": str(dataset),
            "dataset_manifest_sha256": digest(dataset / "manifest.json"), "dataset_files_sha256": files,
            "serving": serving, "serving_sha256": bytes_digest(serving_raw),
            "serving_path": str(args.serving.resolve()),
            "run_dir": str(args.run_dir.resolve()), "rows": args.rows, "dimensions": 768, "queries": query_count,
            "top_k": 10, "batch_size": 256, "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "rss_only": rss_only, "rss_recall_target": RSS_RECALL_TARGET,
            "ef_construction": args.ef_construction,
            "construction_decisions": args.construction_decisions,
            "construction_calibration_contract": construction_calibration_contract(args.ef_construction),
            "rss_controls": RSS_CONTROLS,
            "rss_calibration_queries": RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": RSS_REVALIDATION_QUERIES,
            "overlap_eligible": counts(args.rows)[1],
            "eligible_counts": counts(args.rows), "reader_concurrency": 4, "writer_calls": 8,
            "scalar_shape": "dispersed unique rank=(row*7919)%rows; user_id range, fpath equality for lifecycle delete",
            "writer_shape": "eight 256-row same-ID replacements after full initial ingestion; fixed live row count",
            "url": args.url, "native_address": args.native_address, "diagnostics_url": args.diagnostics_url,
            "operation_timeout_s": 600, "wall_limit_s": 2700, "minimum_free_bytes": 10 * GIB,
            "maximum_output_bytes": 11 * GIB, "maximum_combined_rss_bytes": 24 * GIB,
            "gomaxprocs": os.environ.get("GOMAXPROCS", ""),
            "treedb_go_runtime": {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")},
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "host_memory_bytes": existing.common.memory_bytes(),
            "host_resource_identity": host_resource_identity(),
            "blas_threads": {key: os.environ.get(key, "") for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")},
            "python": os.sys.version, "numpy": np.__version__, "platform": platform.platform(),
            "query_usage": ("observed calibration 0..99 and observed revalidation 100..199; "
                            "both fixed sets select the control; neither is a holdout" if rss_only
                            else "diagnostic queries, not final holdout"),
            "infrastructure": "INFRASTRUCTURE_UNAVAILABLE: runner: shared workstation, serialized quiet window; persistent cache and local artifact storage"}
    if query_mode == "quantized_rerank":
        plan.update(
            query_mode=query_mode,
            quantized_index_name=quantized_index_name,
            vector_m=16,
            representation_arm=quantized_representation_arm(),
            quantized_coordinate_policy="ordered_ef_grid_with_requested_rerank_candidates_equal_ef",
            query_usage=("observed fixed sets 0..99 and 100..199 both select every >4096 cohort; "
                         "all rows is selected once as the unfiltered cohort; filtered <=4096 is typed-exact correctness"
                         if args.rows == 500000 else
                         "four observed queries select the unfiltered 512-row cohort; smaller filtered cohorts are typed-exact"),
        )
    return plan


def validate_plan(plan, expected):
    if canonical(plan) != canonical(expected):
        raise ValueError("frozen plan differs from current source/binary/dataset/configuration/environment")


def validate_imports(source):
    for imported, directory in ((existing, "benchmarks/vector_db_compare"),
                                (existing.common, "benchmarks/vector_db_compare"),
                                (dense_contract, "clients/python/treedb_client/src"),
                                (existing.TreeDBClient, "clients/python/treedb_client/src")):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / directory):
            raise ValueError("imported runner/client is outside the frozen source tree")


def validate_shutdowns(lifetimes, expected_count):
    if len(lifetimes) != expected_count:
        raise RuntimeError("owned service lifetime count mismatch")
    for lifetime in lifetimes:
        terminal, exited = lifetime.get("terminal_work", {}), lifetime.get("exit", {})
        if (terminal.get("cleanup_completed") is not True or terminal.get("shutdown_failures") != 0
                or terminal.get("contract_version") != existing.SERVICE_CONTRACT
                or terminal.get("work", {}).get("pid") != lifetime["pid"]
                or exited.get("pid") != lifetime["pid"] or exited.get("exit_code") != 0
                or exited.get("availability") != "measured"
                or exited.get("linux_process_identity") != lifetime["linux_process_identity"]):
            raise RuntimeError("owned service did not complete a clean verified shutdown")


class Run:
    def __init__(self, plan):
        self.plan = plan
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)  # Never reuse or overwrite a prior run.
        self.events = (self.output / "events.jsonl").open("x", buffering=1)
        self.lock, self.overlap_lock, self.cancel = threading.Lock(), threading.Lock(), threading.Event()
        self.failure = None
        self.controller = existing.ServiceController(Path(plan["service_bin"]), plan["url"], self.output / "db",
            "command_wal_durable", 600, 120, diagnostics_url=plan["diagnostics_url"],
            block_profile_rate=0, mutex_profile_fraction=0, native_address=plan["native_address"], measured=True,
            environment=treedb_service_environment(plan),
            construction_decisions=plan["construction_decisions"])
        self.clients = existing.ThreadLocalClients(plan["url"], plan["operation_timeout_s"], self.controller)
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(plan["rows"], 768))
        self.queries = np.memmap(data / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768))
        self.truth = (json.loads((data / "truth.json").read_text()) if plan["mode"] == "diagnostic"
                      else exact_truth(self.vectors, self.queries, plan["eligible_counts"]))
        for eligible in plan["eligible_counts"]:
            if len(self.truth[str(eligible)]) != plan["queries"]:
                raise ValueError("oracle query cardinality mismatch")
        self.updated = set()
        self.overlap_initial = frozenset()
        self.overlap_initial_touched = frozenset()
        self.overlap_initial_deleted = frozenset()
        self.overlap_initial_advance = 0
        self.overlap_batches = []
        self.quantized_touched = set()
        self.quantized_deleted = set()
        self.quantized_owner_advance = 0
        self.quantized_folded = False
        self.quantized_initial_snapshot = None
        self.quantized_owner_snapshots = {}
        self.info = None
        self.initial_graph_build = None
        self.quantized_requests = []
        self.quantized_quality = {}
        self.selected_efs = {}

    def emit(self, event, **fields):
        with self.lock:
            self.events.write(canonical({"event": event, **fields}).decode())

    def timed(self, phase, call, **fields):
        if self.failure:
            raise RuntimeError(self.failure)
        start = time.monotonic_ns()
        try:
            result = call()
        except BaseException as exc:
            self.emit("call", phase=phase, start_ns=start, duration_ns=time.monotonic_ns() - start,
                      outcome="error", error=f"{type(exc).__name__}: {exc}", **fields)
            raise
        end = time.monotonic_ns()
        self.emit("call", phase=phase, start_ns=start, end_ns=end, duration_ns=end - start, outcome="completed", **fields)
        return result

    def check_resources(self):
        free = shutil.disk_usage(self.output).free
        size = existing.common.disk_bytes(self.output)
        process = self.controller.process
        rss = 0
        for pid in (os.getpid(), process.pid if process else None):
            if pid is not None:
                try:
                    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                        if line.startswith("VmRSS:"):
                            rss += int(line.split()[1]) * 1024
                except FileNotFoundError:
                    pass
        elapsed = time.monotonic() - self.started
        self.emit("resource", elapsed_s=elapsed, free_bytes=free, output_bytes=size, combined_rss_bytes=rss)
        if (free < self.plan["minimum_free_bytes"] or size > self.plan["maximum_output_bytes"]
                or rss > self.plan["maximum_combined_rss_bytes"] or elapsed > self.plan["wall_limit_s"]):
            raise RuntimeError("frozen disk/RAM/wall budget exceeded")

    def guard(self):
        while not self.cancel.wait(1):
            if not self.failure:
                try:
                    self.check_resources()
                except BaseException as exc:
                    self.failure = f"resource guard: {exc}"
                    self.emit("guard_failed", error=self.failure)
            if self.failure:
                process = self.controller.process
                if process and existing.common.linux_process_identity(process.pid) == self.controller._owned_identity:
                    os.kill(process.pid, signal.SIGTERM)

    def ensure(self):
        vector_index_options = {"strategy": "column_graph", "ef_construction": self.plan["ef_construction"]}
        if self.plan.get("query_mode") == "quantized_rerank":
            vector_index_options["m"] = self.plan["vector_m"]
            vector_index_options["quantized_indexes"] = [{
                "name": self.plan["quantized_index_name"], "codec": "scalar_u8", "version": 1,
            }]
        info = self.clients.ensure_index("minima_cohere", 768, "cosine", typed_input=True,
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"}, {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options=vector_index_options)
        if (info.name != "minima_cohere" or info.dimension != 768 or info.metric != "cosine"
                or info.contract_version != existing.SERVICE_CONTRACT
                or info.embedding_field != "embedding" or info.vector_index_name != "embedding"
                or info.text_field != "content" or info.text_index_name != "content"
                or info.document_type != "treedb_document_service_v1"
                or info.vector_strategy != "column_graph"
                or info.vector_m != (self.plan["vector_m"] if self.plan.get("query_mode") == "quantized_rerank" else 16)
                or info.vector_ef_construction != self.plan["ef_construction"]
                or info.extra.get("typed_input") is not True
                or {(f.field, f.value_type) for f in info.scalar_fields} != {("meta.user_id", "string"), ("meta.fpath", "string")}):
            raise RuntimeError("public collection schema differs from frozen typed schema")
        if self.plan.get("query_mode") == "quantized_rerank":
            if not existing.quantized_effective_index_valid(
                    info, "minima_cohere", 768, "cosine", self.plan["ef_construction"]):
                raise RuntimeError("public collection quantized profile differs from frozen minima_sq8")
            self.emit("quantized_index", observed=info.to_dict())
        self.info = info
        return info

    def optimize(self, action):
        result = self.clients.optimize_index("minima_cohere", column_graph_action=action,
            column_graph_serving=self.plan["serving"] if action in ("build", "ensure") else None,
            expected_generation=self.info.generation)
        if action == "fold" and self.plan.get("query_mode") == "quantized_rerank":
            with self.overlap_lock:
                self.quantized_folded = True
        return result

    def record_quantized_delete(self, rows):
        if self.plan.get("query_mode") != "quantized_rerank":
            return
        with self.overlap_lock:
            self.quantized_owner_advance += 1
            self.quantized_touched.update(rows)
            self.quantized_deleted.update(rows)
            self.updated.difference_update(rows)

    def _quantized_upsert_state_locked(self, rows, updated):
        prior = {
            "updated": frozenset(self.updated),
            "touched": frozenset(self.quantized_touched),
            "deleted": frozenset(self.quantized_deleted),
            "owner_advance": self.quantized_owner_advance,
            "folded": self.quantized_folded,
        }
        current = quantized_upsert_state(prior, rows, updated)
        if current is None:
            raise RuntimeError("invalid authored quantized upsert transition")
        return prior, current

    def _apply_quantized_upsert_state_locked(self, state):
        self.updated = set(state["updated"])
        self.quantized_touched = set(state["touched"])
        self.quantized_deleted = set(state["deleted"])
        self.quantized_owner_advance = state["owner_advance"]

    def quantized_allowed_states(self, writer_active, request_start_ns, request_end_ns):
        with self.overlap_lock:
            if writer_active:
                return quantized_lifecycle_states(
                    self.overlap_initial, self.overlap_initial_touched,
                    self.overlap_initial_deleted, self.overlap_initial_advance,
                    [dict(window) for window in self.overlap_batches],
                    request_start_ns, request_end_ns,
                )
            return [{
                "updated": frozenset(self.updated),
                "touched": frozenset(self.quantized_touched),
                "deleted": frozenset(self.quantized_deleted),
                "owner_advance": self.quantized_owner_advance,
                "folded": self.quantized_folded,
            }]

    def validate_quantized_lifecycle(self, response, record, writer_active, eligible, ef):
        states = self.quantized_allowed_states(
            writer_active, record["started_monotonic_ns"], record["ended_monotonic_ns"],
        )
        snapshot = quantized_snapshot_identity(response.score_plane.snapshot)
        if snapshot is None:
            raise RuntimeError("quantized response omitted its lifecycle owner identity")
        with self.overlap_lock:
            initial = self.quantized_initial_snapshot
            if initial is None:
                if (len(states) != 1 or states[0]["owner_advance"] != 0 or states[0]["folded"]
                        or snapshot["base_manifest"] != snapshot["current_manifest"]
                        or snapshot["base_coverage_lsn"] != snapshot["current_coverage_lsn"]):
                    raise RuntimeError("quantized response did not establish one initial graph owner")
                initial = snapshot
            matched = []
            for state in states:
                key = (state["owner_advance"], state["folded"])
                prior = self.quantized_owner_snapshots.get(key)
                if ((prior is None or same_json(prior, snapshot))
                        and quantized_response_matches_state(
                            response, state, self.plan["rows"], eligible, ef, initial,
                        )):
                    matched.append(state)
            if len(matched) != 1:
                raise RuntimeError("quantized response does not identify one exact projection/width/owner lifecycle state")
            if self.quantized_initial_snapshot is None:
                self.quantized_initial_snapshot = initial
            state = matched[0]
            self.quantized_owner_snapshots[(state["owner_advance"], state["folded"])] = snapshot
        record["lifecycle_state"] = {
            "owner_advance": state["owner_advance"], "folded": state["folded"],
            "shadow_allowance": quantized_state_widths(
                state, self.plan["rows"], eligible, ef,
            )[3],
        }
        return state

    def upsert(self, rows, phase, updated=False, record_quantized=False, record_overlap=False):
        batch = [make_document(self.vectors, row, self.plan["rows"], updated) for row in rows]
        window, expected_state = None, None
        if record_quantized or record_overlap:
            with self.overlap_lock:
                prior, expected_state = self._quantized_upsert_state_locked(rows, updated)
                published = expected_state["owner_advance"] != prior["owner_advance"]
                if record_overlap:
                    window = {
                        "started_monotonic_ns": time.monotonic_ns(), "ended_monotonic_ns": None,
                        "rows": tuple(rows), "updated": updated,
                        "published": published, "success": None,
                    }
                    self.overlap_batches.append(window)
        try:
            response = self.timed(
                phase,
                lambda: self.clients.native.upsert_documents(
                    "minima_cohere", batch, index_info=self.info),
                first_row=rows[0], rows=len(rows),
            )
            if response.upserted != len(rows) or response.ids != [d["id"] for d in batch]:
                raise RuntimeError("public upsert completion mismatch")
        except BaseException:
            if window is not None:
                with self.overlap_lock:
                    window.update(ended_monotonic_ns=time.monotonic_ns(), success=False)
            raise
        if expected_state is not None:
            with self.overlap_lock:
                prior, current = self._quantized_upsert_state_locked(rows, updated)
                if current != expected_state:
                    raise RuntimeError("authored quantized upsert state changed during the single-writer call")
                self._apply_quantized_upsert_state_locked(current)
                if window is not None:
                    window.update(ended_monotonic_ns=time.monotonic_ns(), success=True)
        return response

    def search(self, phase, eligible, ef, query, writer_active=False, query_filter=None):
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        selected_filter = predicate(self.plan["rows"], eligible) if query_filter is None else query_filter
        record = None
        if quantized:
            with self.lock:
                record = {
                    "request_sequence": len(self.quantized_requests) + 1,
                    "phase": phase, "eligible": eligible, "query": query,
                    "requested_ef_search": ef, "requested_rerank_candidates": ef,
                    "command_version": 3, "expected_generation": self.info.generation,
                    "started_monotonic_ns": time.monotonic_ns(), "outcome": "error", "results": [],
                }
                self.quantized_requests.append(record)
        response = None
        try:
            response = self.timed(phase, lambda: self.clients.native.query_by_embedding(
                "minima_cohere", self.queries[query].tolist(), 10, selected_filter,
                route="ann", ef_search=ef, index_info=self.info,
                **({"query_mode": "quantized_rerank", "quantized_index_name": QUANTIZED_PROFILE_NAME,
                    "quantized_rerank_candidates": ef} if quantized else {})),
                eligible=eligible, ef=ef, query=query, writer_active=writer_active)
            if record is not None:
                record["ended_monotonic_ns"] = time.monotonic_ns()
                proof = getattr(response, "dense_work", None)
                score_plane = getattr(response, "score_plane", None)
                if proof is not None:
                    record["dense_work"] = asdict(proof)
                if score_plane is not None:
                    record["score_plane"] = asdict(score_plane)
            if quantized:
                validate_quantized_response(response, self.plan["rows"], eligible, ef,
                                            selected_filter is not None, self.info.generation)
            if ((not quantized and response.native_command_version != 2)
                or response.index.generation != self.info.generation
                or response.index.vector_strategy != "column_graph" or response.route != "ann"
                or response.exact_fallbacks or response.full_document_scan_fallbacks or response.dense_work is None):
                raise RuntimeError("search left the required producer-backed native typed route")
            if eligible != self.plan["rows"] and (not response.dense_work.graph.filter.completed
                                                   or response.dense_work.graph.filter.eligible_rows != eligible):
                raise RuntimeError("producer scalar-filter membership differs from declared population")
            ids = [doc.id for doc in response.documents]
            if len(ids) != min(10, eligible) or len(set(ids)) != len(ids):
                raise RuntimeError("unexpected search result cardinality")
            results = [] if quantized else None
            scores = [] if quantized else None
            for doc in response.documents:
                row = int(doc.id.removeprefix("row-"))
                if not 0 <= row < self.plan["rows"] or (row * 7919) % self.plan["rows"] >= eligible:
                    raise RuntimeError("cross-filter search result")
                if doc.meta.get("user_id") != f"{(row * 7919) % self.plan['rows']:06d}" or doc.score is None:
                    raise RuntimeError("missing scalar/score projection")
                # Correctness-only scoring is outside the public API timer.
                vector = np.asarray(self.vectors[row], dtype=np.float64)
                query_vector = np.asarray(self.queries[query], dtype=np.float64)
                score = np.dot(vector, query_vector) / (np.linalg.norm(vector) * np.linalg.norm(query_vector))
                if not math.isfinite(doc.score) or abs(score - doc.score) > 2e-5:
                    raise RuntimeError("returned score differs from independent full-vector cosine")
                if quantized:
                    expected_meta = {"user_id": f"{(row * 7919) % self.plan['rows']:06d}",
                                     "fpath": f"/cohere/{row // 256:06d}.txt"}
                    if doc.meta != expected_meta:
                        raise RuntimeError("quantized result omitted or changed a full document projection")
                    scores.append(float(doc.score))
                    results.append({"id": doc.id, "content": doc.content,
                                    "meta": doc.meta, "score": float(doc.score)})
            if quantized:
                self.validate_quantized_lifecycle(response, record, writer_active, eligible, ef)
            if quantized and list(zip((-score for score in scores), ids)) != sorted(zip((-score for score in scores), ids)):
                raise RuntimeError("quantized results are not canonically ordered by score then ID")
            truth = [] if eligible == 0 else self.truth[str(eligible)][query]
            if (quantized and response.score_plane.route in ("typed_exact", "typed_empty")
                    and ids != truth):
                raise RuntimeError("typed-exact quantized result rank differs from frozen eligible truth")
            recall = len(set(ids) & set(truth)) / len(truth) if truth else (1.0 if not ids else 0.0)
            ndcg = binary_ndcg(ids, truth) if truth else (1.0 if not ids else 0.0)
            event = {"phase": phase, "eligible": eligible, "ef": ef, "query": query, "ids": ids,
                     "recall": recall, "ndcg_at_10": ndcg, "writer_active": writer_active,
                     "dense_work": record["dense_work"] if quantized else asdict(response.dense_work)}
            if quantized:
                event["rerank_candidates"] = ef
                event["score_plane"] = record["score_plane"]
                record.update(outcome="success", results=results, recall=recall, ndcg_at_10=ndcg)
            self.emit("search_result", **event)
            return response
        except BaseException as exc:
            if record is not None:
                record.setdefault("ended_monotonic_ns", time.monotonic_ns())
                for name in ("dense_work", "score_plane"):
                    value = getattr(response, name, None) if response is not None else None
                    if value is None:
                        value = getattr(exc, name, None)
                    if value is not None and name not in record:
                        record[name] = asdict(value)
                record["error"] = f"{type(exc).__name__}: {exc}"
                self.emit("quantized_request", request=record)
            raise

    def check_documents(self, docs, expected_ids):
        if [None if doc is None else doc.id for doc in docs] != expected_ids:
            raise RuntimeError("public materialization ID ordering/missing mismatch")
        for doc in docs:
            if doc is None:
                continue
            row = int(doc.id.removeprefix("row-"))
            expected = make_document(self.vectors, row, self.plan["rows"], row in self.updated)
            if doc.content != expected["content"] or doc.meta != expected["meta"]:
                raise RuntimeError("public materialization payload mismatch")
            actual = np.asarray(doc.embedding, dtype=np.float64)
            want = np.asarray(self.vectors[row], dtype=np.float64)
            if actual.shape != (768,) or not np.isfinite(actual).all():
                raise RuntimeError("public materialization vector shape mismatch")
            actual_norm, want_norm = np.linalg.norm(actual), np.linalg.norm(want)
            if not (math.isfinite(actual_norm) and actual_norm > 0 and math.isfinite(want_norm) and want_norm > 0):
                raise RuntimeError("public materialization zero/nonfinite vector norm")
            actual /= actual_norm
            want /= want_norm
            if np.max(np.abs(actual - want)) > 1e-6:
                raise RuntimeError("public materialization full-vector mismatch")

    def overlap(self):
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        if quantized:
            with self.overlap_lock:
                self.overlap_initial = frozenset(self.updated)
                self.overlap_initial_touched = frozenset(self.quantized_touched)
                self.overlap_initial_deleted = frozenset(self.quantized_deleted)
                self.overlap_initial_advance = self.quantized_owner_advance
                self.overlap_batches.clear()
        barrier = threading.Barrier(5)
        active = threading.Event()
        active.set()
        def reader(worker):
            barrier.wait(timeout=30)
            for n in range(64):
                eligible = self.plan["overlap_eligible"]
                ef = self.selected_efs.get(str(eligible), self.plan["overlap_ef"])
                self.search("overlap_search", eligible, ef,
                            (n * 4 + worker) % self.plan["queries"], active.is_set())
        def writer():
            barrier.wait(timeout=30)
            try:
                for n in range(8):
                    start = (self.plan["rows"] - 256 * (n + 1)) % self.plan["rows"]
                    rows = list(range(start, start + 256))
                    self.upsert(rows, "overlap_replace", updated=True,
                                record_quantized=quantized,
                                record_overlap=quantized)
                    if not quantized:
                        with self.lock:
                            self.updated.update(rows)
                            self.overlap_batches.append((time.monotonic_ns(), frozenset(rows)))
            finally:
                active.clear()
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(reader, n) for n in range(4)] + [pool.submit(writer)]
            for future in futures:
                future.result()

    def lifecycle(self):
        rows = list(range(256))
        ids = [f"row-{row:06d}" for row in rows]
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        self.upsert(rows, "explicit_update", updated=True, record_quantized=quantized)
        if not quantized:
            self.updated.update(rows)
        docs = self.timed("native_get_many", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        deleted = self.timed("http_delete_file", lambda: self.clients.delete_by_filter("minima_cohere",
            {"field": "meta.fpath", "operator": "==", "value": "/cohere/000000.txt"}, expected_generation=self.info.generation))
        if deleted.deleted != len(rows):
            raise RuntimeError("file delete cardinality mismatch")
        self.record_quantized_delete(rows)
        docs = self.timed("native_deleted_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, [None] * len(ids))
        self.upsert(rows, "reindex_replacement", updated=True, record_quantized=quantized)
        docs = self.timed("native_reinsert_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        empty_filter = {"field": "meta.user_id", "operator": "==", "value": "missing-user"}
        if self.plan.get("query_mode") == "quantized_rerank":
            response = self.search("empty_user", 0, self.selected_efs[str(self.plan["rows"])], 0,
                                   query_filter=empty_filter)
        else:
            response = self.timed("empty_user", lambda: self.clients.native.query_by_embedding(
                "minima_cohere", self.queries[0].tolist(), 10, empty_filter,
                route="ann", ef_search=512, index_info=self.info))
        if response.documents:
            raise RuntimeError("empty-user query returned documents")

    def scroll(self):
        after, seen = None, 0
        while True:
            result = self.clients.filter_documents("minima_cohere", limit=256, return_embedding=True,
                after_id=after, cursor_page=True, expected_generation=self.info.generation)
            expected = [f"row-{row:06d}" for row in range(seen, min(seen + 256, self.plan["rows"]))]
            self.check_documents(result.documents, expected)
            seen += len(result.documents)
            if result.exhausted:
                break
            if not result.next_after_id or result.next_after_id == after or not result.documents:
                raise RuntimeError("public verification cursor did not advance")
            after = result.next_after_id
        if seen != self.plan["rows"]:
            raise RuntimeError("public full-state count mismatch")
        self.emit("full_state_verified", rows=seen, vectors_checked=seen, normalized_full_vector_tolerance=1e-6)

    def rss_boundary(self):
        truth = self.truth[str(self.plan["rows"])]

        def search(ef, query):
            response = self.search("rss_quality", self.plan["rows"], ef, query)
            return [document.id for document in response.documents]

        quality = calibrate_ann_control(
            search, truth, self.plan["rss_controls"], self.plan["rss_calibration_queries"],
            self.plan["rss_revalidation_queries"], self.plan["rss_recall_target"],
        )
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        if quantized:
            quality = _coordinate_rows(quality)
        process = self.controller.process
        rss = process_peak_at_boundary(
            process.pid if process else None, self.controller._owned_identity, self.plan["cpu_affinity"],
        )
        reasons = []
        if not quality["revalidation"]["passed"]:
            reasons.append("no TreeDB EF passed both fixed recall query sets")
        if rss.get("availability") != "measured":
            reasons.append("TreeDB server VmHWM unavailable or process drifted")
        artifact = {
            "schema": QUANTIZED_RSS_ARTIFACT_SCHEMA if quantized else RSS_ARTIFACT_SCHEMA,
            "state": "calibrated" if not reasons else "uncalibrated",
            "backend": "treedb", "comparison_contract": rss_comparison_contract(self.plan),
            "construction_calibration_contract": self.plan["construction_calibration_contract"],
            "quality": {**quality, "control_name": "ef_search", "exact_mode": False},
            "rss": rss, "reasons": reasons,
            "readiness": {"graph_action": "build", "successful_ann_queries": sum(
                len(row["per_query"]) for row in quality["calibration"]["curve"]
            ) + sum(len(row["per_query"]) for row in quality["revalidation"]["curve"]),
                "effective_index": {"m": self.info.vector_m, "ef_construction": self.info.vector_ef_construction},
                "column_graph_build": asdict(self.initial_graph_build.status.column_graph_build),
            },
            "provenance": {key: self.plan[key] for key in (
                "harness_commit", "harness_source_sha256", "harness_trees", "product_commit",
                "product_trees", "service_sha256", "dataset_manifest_sha256", "dataset_files_sha256",
                "serving_sha256",
            )},
        }
        if quantized:
            artifact["representation_arm"] = quantized_representation_arm()
            artifact["observed_execution"] = {
                "schema": "treedb_cohere_sq8_execution/v1",
                "native_command_version": 3,
                "requests": self.quantized_requests,
            }
            artifact["readiness"]["effective_index"] = self.info.to_dict()
        return artifact

    def execute(self):
        self.started = time.monotonic()
        monitor = threading.Thread(target=self.guard, daemon=True)
        self.emit("plan", plan=self.plan)
        monitor.start()
        failed, rss_artifact = None, None
        try:
            self.timed("service_start", self.controller.start)
            self.timed("schema_ensure", self.ensure)
            for start in range(0, self.plan["rows"], 256):
                self.upsert(list(range(start, min(start + 256, self.plan["rows"]))), "initial_durable_ingest")
            self.initial_graph_build = self.timed("initial_graph_build", lambda: self.optimize("build"))
            decisions = self.initial_graph_build.status.column_graph_build.construction_decisions
            if (decisions is not None) != self.plan["construction_decisions"]:
                raise RuntimeError("construction decision observer response differs from frozen mode")
            if self.plan["rss_only"]:
                rss_artifact = self.rss_boundary()
                if rss_artifact["reasons"]:
                    raise RuntimeError("; ".join(rss_artifact["reasons"]))
            elif self.plan.get("query_mode") == "quantized_rerank":
                cold_populations = set()

                def quality_search(eligible, ef, query):
                    phase = "predicate_first_quality_query" if eligible not in cold_populations else "quality_selection"
                    cold_populations.add(eligible)
                    return [doc.id for doc in self.search(phase, eligible, ef, query).documents]

                if self.plan["rows"] == 500000:
                    self.quantized_quality = select_quantized_cohorts(
                        quality_search,
                        self.truth, self.plan["eligible_counts"], self.plan["rows"], self.plan["rss_controls"],
                        self.plan["rss_calibration_queries"], self.plan["rss_revalidation_queries"],
                        self.plan["rss_recall_target"],
                    )
                else:
                    smoke_queries = list(range(self.plan["queries"]))
                    self.quantized_quality = select_quantized_cohorts(
                        quality_search,
                        self.truth, self.plan["eligible_counts"], self.plan["rows"], self.plan["rss_controls"],
                        smoke_queries[:2], smoke_queries[2:], 1.0,
                    )
                failed_cohorts = [eligible for eligible, row in self.quantized_quality.items()
                                  if row["selected_coordinate"] is None
                                  or row["revalidation"].get("passed") is not True]
                self.emit("quantized_quality", cohorts=self.quantized_quality)
                if failed_cohorts:
                    raise RuntimeError("quantized fixed-set quality failed for cohorts " + ",".join(failed_cohorts))
                self.selected_efs = {eligible: row["selected_coordinate"]["ef_search"]
                                     for eligible, row in self.quantized_quality.items()}
                # Selection is now frozen. Later diagnostic/lifecycle calls only reuse these coordinates.
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("fixed_coordinate_curve", eligible, self.selected_efs[str(eligible)], query)
                self.overlap()
                self.lifecycle()
                self.timed("pre_close_fold", lambda: self.optimize("fold"))
                self.timed("close", self.clients.close)
                validate_shutdowns(self.controller.lifetimes, 1)
                self.timed("reopen", self.controller.start)
                self.timed("idempotent_ensure", self.ensure)
                self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("post_reopen_curve", eligible, self.selected_efs[str(eligible)], query)
                self.timed("verification_only_full_scroll", self.scroll)
            else:
                # Cache-cold means first request for this predicate, not OS/disk-cold.
                for eligible in self.plan["eligible_counts"]:
                    self.search("predicate_first_query", eligible, 512, 0)
                    for ef in self.plan["efs"]:
                        for query in range(self.plan["queries"]):
                            self.search("warm_curve", eligible, ef, query)
                self.overlap()
                self.lifecycle()
                self.timed("pre_close_fold", lambda: self.optimize("fold"))
                self.timed("close", self.clients.close)
                validate_shutdowns(self.controller.lifetimes, 1)
                self.timed("reopen", self.controller.start)
                self.timed("idempotent_ensure", self.ensure)
                self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("post_reopen_curve", eligible, 512, query)
                self.timed("verification_only_full_scroll", self.scroll)
            if self.failure:
                raise RuntimeError(self.failure)
        except BaseException as exc:
            failed = f"{type(exc).__name__}: {exc}"
            if self.plan.get("query_mode") == "quantized_rerank" and self.quantized_requests:
                last = self.quantized_requests[-1]
                if last.get("outcome") == "error" and "error" not in last:
                    last["error"] = failed
                self.emit("quantized_failure_evidence", requests=self.quantized_requests)
            self.emit("failure", error=failed)
        finally:
            try:
                self.clients.close()
                validate_shutdowns(self.controller.lifetimes, 1 if self.plan["rss_only"] else 2)
            except BaseException as exc:
                failed = failed or f"shutdown: {exc}"
            self.cancel.set()
            monitor.join(timeout=5)
            failed = failed or self.failure
            try:
                if monitor.is_alive():
                    raise RuntimeError("resource guard did not stop")
                self.check_resources()
            except BaseException as exc:
                failed = failed or f"final resource guard: {exc}"
            if rss_artifact is not None:
                rss_artifact = finalize_rss_artifact(rss_artifact, failed)
                self.emit("rss_boundary", artifact=rss_artifact)
            self.emit("terminal", lifecycle_complete=failed is None, qualification="not_evaluated", error=failed,
                      process_lifetimes=self.controller.lifetimes, final_disk_bytes=existing.common.disk_bytes(self.output / "db"))
            self.events.close()
            if rss_artifact is not None:
                (self.output / "rss.json").write_bytes(canonical(rss_artifact))
        return int(failed is not None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--service-bin", required=True, type=Path)
    parser.add_argument("--product-commit", required=True)
    parser.add_argument("--serving", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--rows", type=int, choices=[512, 500000], default=500000)
    parser.add_argument("--rss-only", action="store_true")
    parser.add_argument("--query-mode", choices=("exact", "quantized_rerank"), default="exact")
    parser.add_argument("--quantized-index-name")
    parser.add_argument("--ef-construction", type=int, choices=(32, 64, 96, 128), default=128)
    parser.add_argument("--construction-decisions", action="store_true")
    parser.add_argument("--url", default="http://127.0.0.1:17420")
    parser.add_argument("--native-address", default="127.0.0.1:17422")
    parser.add_argument("--diagnostics-url", default="http://127.0.0.1:17421")
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        print(write_frozen_json(args.freeze, plan, "frozen Cohere run plan"), args.freeze)
        return 0
    frozen, frozen_raw = strict_json_object(args.run, "frozen Cohere run plan")
    if not args.expected_plan_sha256 or bytes_digest(frozen_raw) != args.expected_plan_sha256:
        raise ValueError("externally pinned plan hash required")
    validate_plan(frozen, plan)
    if shutil.disk_usage(args.run_dir.parent).free < plan["minimum_free_bytes"] + GIB:
        raise RuntimeError("insufficient disk headroom before starting service")
    return Run(frozen).execute()


if __name__ == "__main__":
    raise SystemExit(main())
