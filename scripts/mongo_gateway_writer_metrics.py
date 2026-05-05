#!/usr/bin/env python3
import csv
import json
import os
import re
import sys


COLUMNS = [
    "target",
    "config",
    "documents",
    "secondary_indexes",
    "writers",
    "phase",
    "ops_per_sec",
    "sampled_ns_per_op",
    "driver_calls",
    "treedb_drain_ms",
    "drain_indexed_flush_calls_total",
    "drain_coalesced_flush_batches_total",
    "drain_primary_only_drains_total",
    "publish_delta_group_calls_per_doc",
    "root_apply_calls_per_doc",
    "roots_per_publish",
    "read_only_prepare_calls_per_doc",
    "read_only_prepare_ns_per_doc",
    "read_only_prepare_ns_per_plan",
    "read_only_prepare_ops_per_doc",
    "read_only_prepare_leaf_spans_per_plan",
    "read_only_prepare_worker_targets_per_plan",
    "read_only_prepare_worker_ranges_per_plan",
    "read_only_prepare_worker_max_ops_per_plan",
    "primary_root_publishes_per_doc",
    "primary_root_delta_entries_per_doc",
    "primary_root_delta_bytes_per_doc",
    "indexed_flush_units_per_batch",
    "indexed_flush_docs_per_batch",
    "coalesced_batch_units_per_batch",
    "coalesced_batch_docs_per_batch",
    "coalesced_batch_bytes_per_batch",
    "raw_root_delta_entries_per_doc",
    "raw_root_delta_bytes_per_doc",
    "raw_root_delta_tombstones_per_doc",
    "raw_primary_root_delta_entries_per_doc",
    "raw_primary_root_delta_bytes_per_doc",
    "raw_primary_root_delta_tombstones_per_doc",
    "raw_template_root_delta_entries_per_doc",
    "raw_template_root_delta_bytes_per_doc",
    "raw_template_root_delta_tombstones_per_doc",
    "raw_index_state_root_delta_entries_per_doc",
    "raw_index_state_root_delta_bytes_per_doc",
    "raw_index_state_root_delta_tombstones_per_doc",
    "raw_secondary_root_delta_entries_per_doc",
    "raw_secondary_root_delta_bytes_per_doc",
    "raw_secondary_root_delta_tombstones_per_doc",
    "final_root_delta_entries_per_doc",
    "final_root_delta_bytes_per_doc",
    "final_root_delta_tombstones_per_doc",
    "final_primary_root_delta_entries_per_doc",
    "final_primary_root_delta_bytes_per_doc",
    "final_primary_root_delta_tombstones_per_doc",
    "final_template_root_delta_entries_per_doc",
    "final_template_root_delta_bytes_per_doc",
    "final_template_root_delta_tombstones_per_doc",
    "final_index_state_root_delta_entries_per_doc",
    "final_index_state_root_delta_bytes_per_doc",
    "final_index_state_root_delta_tombstones_per_doc",
    "final_secondary_root_delta_entries_per_doc",
    "final_secondary_root_delta_bytes_per_doc",
    "final_secondary_root_delta_tombstones_per_doc",
    "squashed_root_delta_entries_per_doc",
    "net_zero_root_batches_per_doc",
    "net_zero_root_plans_per_doc",
    "skipped_secondary_roots_per_doc",
    "primary_only_duplicate_ids_coalesced_per_doc",
    "primary_only_drains_per_doc",
    "primary_only_drain_docs_per_drain",
    "leaf_log_node_loads_per_doc",
    "leaf_log_pages_written_per_doc",
    "leaf_log_read_bytes_per_doc",
    "leaf_log_write_bytes_per_doc",
    "backpressure_sync_total",
    "root_mismatch_total",
    "read_only_prepare_calls_total",
    "read_only_prepare_worker_targets_total",
    "read_only_prepare_worker_ranges_total",
    "root_delta_plan_raw_unit_primary_entries_total",
    "root_delta_plan_raw_unit_secondary_entries_total",
    "root_delta_plan_final_primary_entries_total",
    "root_delta_plan_final_secondary_entries_total",
    "root_delta_plan_squashed_entries_total",
    "coalesced_flush_net_zero_batches_total",
    "primary_only_duplicate_ids_coalesced_total",
    "primary_only_drains_total",
    "raw_json",
]

METRIC_COLUMNS = {
    "publish_delta_group_calls_per_doc": "publish_delta_group_calls/doc",
    "root_apply_calls_per_doc": "root_apply_calls/doc",
    "roots_per_publish": "roots/publish",
    "read_only_prepare_calls_per_doc": "read_only_prepare_calls/doc",
    "read_only_prepare_ns_per_doc": "read_only_prepare_ns/doc",
    "read_only_prepare_ns_per_plan": "read_only_prepare_ns/plan",
    "read_only_prepare_ops_per_doc": "read_only_prepare_ops/doc",
    "read_only_prepare_leaf_spans_per_plan": "read_only_prepare_leaf_spans/plan",
    "read_only_prepare_worker_targets_per_plan": "read_only_prepare_worker_targets/plan",
    "read_only_prepare_worker_ranges_per_plan": "read_only_prepare_worker_ranges/plan",
    "read_only_prepare_worker_max_ops_per_plan": "read_only_prepare_worker_max_ops/plan",
    "primary_root_publishes_per_doc": "primary_root_publishes/doc",
    "primary_root_delta_entries_per_doc": "primary_root_delta_entries/doc",
    "primary_root_delta_bytes_per_doc": "primary_root_delta_bytes/doc",
    "indexed_flush_units_per_batch": "indexed_flush_units/batch",
    "indexed_flush_docs_per_batch": "indexed_flush_docs/batch",
    "coalesced_batch_units_per_batch": "coalesced_batch_units/batch",
    "coalesced_batch_docs_per_batch": "coalesced_batch_docs/batch",
    "coalesced_batch_bytes_per_batch": "coalesced_batch_bytes/batch",
    "raw_root_delta_entries_per_doc": "raw_root_delta_entries/doc",
    "raw_root_delta_bytes_per_doc": "raw_root_delta_bytes/doc",
    "raw_root_delta_tombstones_per_doc": "raw_root_delta_tombstones/doc",
    "raw_primary_root_delta_entries_per_doc": "raw_primary_root_delta_entries/doc",
    "raw_primary_root_delta_bytes_per_doc": "raw_primary_root_delta_bytes/doc",
    "raw_primary_root_delta_tombstones_per_doc": "raw_primary_root_delta_tombstones/doc",
    "raw_template_root_delta_entries_per_doc": "raw_template_root_delta_entries/doc",
    "raw_template_root_delta_bytes_per_doc": "raw_template_root_delta_bytes/doc",
    "raw_template_root_delta_tombstones_per_doc": "raw_template_root_delta_tombstones/doc",
    "raw_index_state_root_delta_entries_per_doc": "raw_index_state_root_delta_entries/doc",
    "raw_index_state_root_delta_bytes_per_doc": "raw_index_state_root_delta_bytes/doc",
    "raw_index_state_root_delta_tombstones_per_doc": "raw_index_state_root_delta_tombstones/doc",
    "raw_secondary_root_delta_entries_per_doc": "raw_secondary_root_delta_entries/doc",
    "raw_secondary_root_delta_bytes_per_doc": "raw_secondary_root_delta_bytes/doc",
    "raw_secondary_root_delta_tombstones_per_doc": "raw_secondary_root_delta_tombstones/doc",
    "final_root_delta_entries_per_doc": "final_root_delta_entries/doc",
    "final_root_delta_bytes_per_doc": "final_root_delta_bytes/doc",
    "final_root_delta_tombstones_per_doc": "final_root_delta_tombstones/doc",
    "final_primary_root_delta_entries_per_doc": "final_primary_root_delta_entries/doc",
    "final_primary_root_delta_bytes_per_doc": "final_primary_root_delta_bytes/doc",
    "final_primary_root_delta_tombstones_per_doc": "final_primary_root_delta_tombstones/doc",
    "final_template_root_delta_entries_per_doc": "final_template_root_delta_entries/doc",
    "final_template_root_delta_bytes_per_doc": "final_template_root_delta_bytes/doc",
    "final_template_root_delta_tombstones_per_doc": "final_template_root_delta_tombstones/doc",
    "final_index_state_root_delta_entries_per_doc": "final_index_state_root_delta_entries/doc",
    "final_index_state_root_delta_bytes_per_doc": "final_index_state_root_delta_bytes/doc",
    "final_index_state_root_delta_tombstones_per_doc": "final_index_state_root_delta_tombstones/doc",
    "final_secondary_root_delta_entries_per_doc": "final_secondary_root_delta_entries/doc",
    "final_secondary_root_delta_bytes_per_doc": "final_secondary_root_delta_bytes/doc",
    "final_secondary_root_delta_tombstones_per_doc": "final_secondary_root_delta_tombstones/doc",
    "squashed_root_delta_entries_per_doc": "squashed_root_delta_entries/doc",
    "net_zero_root_batches_per_doc": "net_zero_root_batches/doc",
    "net_zero_root_plans_per_doc": "net_zero_root_plans/doc",
    "skipped_secondary_roots_per_doc": "skipped_secondary_roots/doc",
    "primary_only_duplicate_ids_coalesced_per_doc": "primary_only_duplicate_ids_coalesced/doc",
    "primary_only_drains_per_doc": "primary_only_drains/doc",
    "primary_only_drain_docs_per_drain": "primary_only_drain_docs/drain",
    "leaf_log_node_loads_per_doc": "leaf_log_node_loads/doc",
    "leaf_log_pages_written_per_doc": "leaf_log_pages_written/doc",
    "leaf_log_read_bytes_per_doc": "leaf_log_read_bytes/doc",
    "leaf_log_write_bytes_per_doc": "leaf_log_write_bytes/doc",
}

WRITER_PHASE_RE = re.compile(r"^concurrent_id_update_set_w([0-9]+)$")


def fmt(value):
    if value is None or value == "":
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return format(value, ".12g")
    return str(value)


def parse_int_counter(value):
    if value is None or value == "":
        return 0, False
    try:
        text = str(value).strip()
        if not re.fullmatch(r"[+-]?[0-9]+", text):
            return 0, False
        return int(text, 10), True
    except (TypeError, ValueError):
        return 0, False


def delta_count(delta, keys, label):
    found = False
    total = 0
    for key in keys:
        if key not in delta:
            continue
        found = True
        value, ok = parse_int_counter(delta.get(key))
        if not ok:
            print(
                f"warning: skipping {label}: TreeDB delta {key}={delta.get(key)!r} is not an integer",
                file=sys.stderr,
            )
            return ""
        total += value
    if not found:
        return ""
    return fmt(total)


def load_result(raw_path):
    try:
        with open(raw_path) as raw_file:
            result = json.load(raw_file)
    except OSError as err:
        print(f"warning: skipping raw JSON {raw_path}: {err}", file=sys.stderr)
        return None
    except json.JSONDecodeError as err:
        print(f"warning: skipping raw JSON {raw_path}: {err}", file=sys.stderr)
        return None
    if not isinstance(result, dict):
        print(f"warning: skipping raw JSON {raw_path}: top-level JSON is not an object", file=sys.stderr)
        return None
    phases = result.get("phases")
    if phases is not None and not isinstance(phases, list):
        print(f"warning: skipping raw JSON {raw_path}: phases is not a list", file=sys.stderr)
        return None
    return result


def write_writer_metrics(out_dir, matrix_path, writer_metrics_path):
    with open(writer_metrics_path, "w", newline="") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        with open(matrix_path, newline="") as matrix_file:
            matrix = csv.DictReader(matrix_file, delimiter="\t")
            for row in matrix:
                raw_json = row.get("raw_json", "")
                if not raw_json:
                    continue
                raw_path = raw_json if os.path.isabs(raw_json) else os.path.join(out_dir, raw_json)
                result = load_result(raw_path)
                if result is None:
                    continue
                target = row.get("target") or result.get("target", "")
                config = row.get("config", "")
                documents = row.get("documents") or str(result.get("documents", ""))
                secondary_indexes = row.get("secondary_indexes") or str(result.get("secondary_indexes", ""))
                for phase in result.get("phases") or []:
                    if not isinstance(phase, dict):
                        print(f"warning: skipping non-object phase in raw JSON {raw_path}", file=sys.stderr)
                        continue
                    phase_name = phase.get("name", "")
                    match = WRITER_PHASE_RE.match(phase_name)
                    if not match:
                        continue
                    metrics = phase.get("treedb_metrics") or {}
                    if not isinstance(metrics, dict):
                        metrics = {}
                    delta = phase.get("treedb_stats_delta") or {}
                    if not isinstance(delta, dict):
                        delta = {}
                    drain_delta = phase.get("treedb_drain_stats_delta") or {}
                    if not isinstance(drain_delta, dict):
                        drain_delta = {}
                    out = {
                        "target": target,
                        "config": config,
                        "documents": documents,
                        "secondary_indexes": secondary_indexes,
                        "writers": match.group(1),
                        "phase": phase_name,
                        "ops_per_sec": fmt(phase.get("ops_per_sec")),
                        "sampled_ns_per_op": fmt(phase.get("sampled_ns_per_op")),
                        "driver_calls": fmt(phase.get("driver_calls")),
                        "treedb_drain_ms": fmt(phase.get("treedb_drain_ms")),
                        "raw_json": raw_json,
                    }
                    for column, metric_name in METRIC_COLUMNS.items():
                        out[column] = fmt(metrics.get(metric_name))
                    out["drain_indexed_flush_calls_total"] = delta_count(drain_delta, [
                        "treedb.collections.write_domain.indexed_flush.calls_total",
                    ], "drain_indexed_flush_calls_total")
                    out["drain_coalesced_flush_batches_total"] = delta_count(drain_delta, [
                        "treedb.collections.write_domain.coalesced_flush_batch.batches_total",
                    ], "drain_coalesced_flush_batches_total")
                    out["drain_primary_only_drains_total"] = delta_count(drain_delta, [
                        "treedb.collections.write_domain.primary_only.drains_total",
                    ], "drain_primary_only_drains_total")
                    out["backpressure_sync_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total",
                    ], "backpressure_sync_total")
                    out["root_mismatch_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.collection_root_base_mismatch_total",
                        "treedb.collections.write_domain.indexed_flush.root_base_mismatch_total",
                        "treedb.collections.write_domain.coordinator_requeue_on_mismatch_total",
                    ], "root_mismatch_total")
                    out["read_only_prepare_calls_total"] = delta_count(delta, [
                        "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_calls_total",
                    ], "read_only_prepare_calls_total")
                    out["read_only_prepare_worker_targets_total"] = delta_count(delta, [
                        "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_worker_targets_total",
                    ], "read_only_prepare_worker_targets_total")
                    out["read_only_prepare_worker_ranges_total"] = delta_count(delta, [
                        "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_worker_ranges_total",
                    ], "read_only_prepare_worker_ranges_total")
                    out["root_delta_plan_raw_unit_primary_entries_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.root_delta_plan.raw_unit.primary.entries_total",
                    ], "root_delta_plan_raw_unit_primary_entries_total")
                    out["root_delta_plan_raw_unit_secondary_entries_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.root_delta_plan.raw_unit.secondary.entries_total",
                    ], "root_delta_plan_raw_unit_secondary_entries_total")
                    out["root_delta_plan_final_primary_entries_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.root_delta_plan.final.primary.entries_total",
                    ], "root_delta_plan_final_primary_entries_total")
                    out["root_delta_plan_final_secondary_entries_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.root_delta_plan.final.secondary.entries_total",
                    ], "root_delta_plan_final_secondary_entries_total")
                    out["root_delta_plan_squashed_entries_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.root_delta_plan.squashed_entries_total",
                    ], "root_delta_plan_squashed_entries_total")
                    out["coalesced_flush_net_zero_batches_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.coalesced_flush_batch.net_zero_batches_total",
                    ], "coalesced_flush_net_zero_batches_total")
                    out["primary_only_duplicate_ids_coalesced_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.primary_only.duplicate_ids_coalesced_total",
                    ], "primary_only_duplicate_ids_coalesced_total")
                    out["primary_only_drains_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.primary_only.drains_total",
                    ], "primary_only_drains_total")
                    writer.writerow(out)


def main(argv):
    if len(argv) != 4:
        print("usage: mongo_gateway_writer_metrics.py OUT_DIR MATRIX_TSV WRITER_METRICS_TSV", file=sys.stderr)
        return 2
    write_writer_metrics(argv[1], argv[2], argv[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
