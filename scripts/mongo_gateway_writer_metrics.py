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
                    ], "backpressure_sync_total")
                    out["root_mismatch_total"] = delta_count(delta, [
                        "treedb.collections.write_domain.collection_root_base_mismatch_total",
                        "treedb.collections.write_domain.indexed_flush.root_base_mismatch_total",
                        "treedb.collections.write_domain.coordinator_requeue_on_mismatch_total",
                    ], "root_mismatch_total")
                    writer.writerow(out)


def main(argv):
    if len(argv) != 4:
        print("usage: mongo_gateway_writer_metrics.py OUT_DIR MATRIX_TSV WRITER_METRICS_TSV", file=sys.stderr)
        return 2
    write_writer_metrics(argv[1], argv[2], argv[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
