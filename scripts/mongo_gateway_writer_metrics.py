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
    "publish_delta_group_calls_per_doc",
    "root_apply_calls_per_doc",
    "roots_per_publish",
    "primary_root_publishes_per_doc",
    "primary_root_delta_entries_per_doc",
    "primary_root_delta_bytes_per_doc",
    "indexed_flush_units_per_batch",
    "indexed_flush_docs_per_batch",
    "leaf_log_node_loads_per_doc",
    "leaf_log_pages_written_per_doc",
    "leaf_log_read_bytes_per_doc",
    "leaf_log_write_bytes_per_doc",
    "backpressure_sync_total",
    "root_mismatch_total",
    "raw_json",
]

METRIC_COLUMNS = {
    "publish_delta_group_calls_per_doc": "publish_delta_group_calls/doc",
    "root_apply_calls_per_doc": "root_apply_calls/doc",
    "roots_per_publish": "roots/publish",
    "primary_root_publishes_per_doc": "primary_root_publishes/doc",
    "primary_root_delta_entries_per_doc": "primary_root_delta_entries/doc",
    "primary_root_delta_bytes_per_doc": "primary_root_delta_bytes/doc",
    "indexed_flush_units_per_batch": "indexed_flush_units/batch",
    "indexed_flush_docs_per_batch": "indexed_flush_docs/batch",
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


def parse_number(value):
    if value is None or value == "":
        return 0.0, False
    try:
        return float(value), True
    except (TypeError, ValueError):
        return 0.0, False


def delta_count(delta, keys):
    found = False
    parsed = False
    total = 0.0
    for key in keys:
        if key not in delta:
            continue
        found = True
        value, ok = parse_number(delta.get(key))
        if ok:
            parsed = True
            total += value
    if not found or not parsed:
        return ""
    return fmt(total)


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
                with open(raw_path) as raw_file:
                    result = json.load(raw_file)
                target = row.get("target") or result.get("target", "")
                config = row.get("config", "")
                documents = row.get("documents") or str(result.get("documents", ""))
                secondary_indexes = row.get("secondary_indexes") or str(result.get("secondary_indexes", ""))
                for phase in result.get("phases") or []:
                    phase_name = phase.get("name", "")
                    match = WRITER_PHASE_RE.match(phase_name)
                    if not match:
                        continue
                    metrics = phase.get("treedb_metrics") or {}
                    delta = phase.get("treedb_stats_delta") or {}
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
                        "raw_json": raw_json,
                    }
                    for column, metric_name in METRIC_COLUMNS.items():
                        out[column] = fmt(metrics.get(metric_name))
                    out["backpressure_sync_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total",
                    ])
                    out["root_mismatch_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.collection_root_base_mismatch_total",
                        "treedb.collections.write_domain.indexed_flush.root_base_mismatch_total",
                        "treedb.collections.write_domain.coordinator_requeue_on_mismatch_total",
                    ])
                    writer.writerow(out)


def main(argv):
    if len(argv) != 4:
        print("usage: mongo_gateway_writer_metrics.py OUT_DIR MATRIX_TSV WRITER_METRICS_TSV", file=sys.stderr)
        return 2
    write_writer_metrics(argv[1], argv[2], argv[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
