#!/usr/bin/env python3
"""Emit non-mutating TreeDB JSONBench storage compression audits.

The filesystem/frame-level checks do not mutate the DB, do not run compaction,
and do not exclude durable TreeDB storage from the reported size basis. When a
JSONBench result identifies a collection, the audit also runs a read-only
retained-payload path check through TreeDB's collection decoder. Command WAL
remains a separate subtree so downstream reports can keep using durable bytes
excluding only command WAL.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shlex
import struct
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


RECORD_HEADER_SIZE = 20
FRAME_HEADER_SIZE = 12
RECORD_FLAG_GROUPED = 1 << 0
FRAME_FLAG_COMPRESSED = 1 << 0
JSONBENCH_DECLARED_PATHS = [
    "time_us",
    "kind",
    "did",
    "commit.operation",
    "commit.collection",
]


def resolve_main_dir(db_dir: Path) -> Path:
    db_dir = db_dir.expanduser().resolve()
    candidates = [db_dir]
    if db_dir.name != "maindb":
        candidates.append(db_dir / "maindb")
    for candidate in candidates:
        if candidate.is_dir() and (candidate / "index.db").exists():
            return candidate
    raise SystemExit(f"could not resolve TreeDB maindb under {db_dir}")


def iter_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    if path.is_file():
        return [path]
    files: list[Path] = []
    for root, _, names in os.walk(path):
        for name in names:
            files.append(Path(root) / name)
    return sorted(files)


def gzip_oracle(main_dir: Path, level: int) -> dict[str, Any]:
    subtrees = [(".", main_dir)]
    for name in ("leaf_vlog", "value_vlog", "column_assets", "index.db", "wal"):
        subtrees.append((name, main_dir / name))

    rows: list[dict[str, Any]] = []
    for name, path in subtrees:
        raw = 0
        gz = 0
        files = iter_files(path)
        for file_path in files:
            data = file_path.read_bytes()
            raw += len(data)
            gz += len(gzip.compress(data, compresslevel=level))
        rows.append(
            {
                "subtree": name,
                "path": str(path),
                "files": len(files),
                "raw_bytes": raw,
                "gzip_bytes": gz,
                "gzip_to_raw_ratio": (gz / raw) if raw else None,
            }
        )
    return {"gzip_level": level, "subtrees": rows}


def value_log_files(log_dir: Path) -> list[Path]:
    if not log_dir.is_dir():
        return []
    return sorted(
        p
        for p in log_dir.iterdir()
        if p.is_file() and p.name.startswith("value-") and p.name.endswith(".log")
    )


def frame_mode(frame_flags: int, reserved: int, dict_id: int) -> str:
    if frame_flags & FRAME_FLAG_COMPRESSED == 0:
        return "grouped_raw"
    if dict_id:
        return "grouped_dict"
    if reserved == 1:
        return "grouped_block_snappy"
    if reserved == 2:
        return "grouped_block_lz4"
    if reserved == 3:
        return "grouped_block_zstd"
    if reserved == 0:
        return "grouped_block_none"
    return f"grouped_block_codec_{reserved}"


def mode_codec(mode: str) -> str:
    if mode == "raw_record" or mode == "grouped_raw":
        return "raw"
    if mode == "grouped_dict":
        return "dict"
    if mode.startswith("grouped_block_"):
        return mode.removeprefix("grouped_block_")
    return "unknown"


def empty_mode_stats() -> dict[str, Any]:
    return {
        "frames": 0,
        "records": 0,
        "raw_payload_bytes": 0,
        "stored_payload_bytes": 0,
        "record_body_bytes": 0,
    }


def scan_log_frames(log_dir: Path) -> dict[str, Any]:
    modes: dict[str, dict[str, int]] = defaultdict(empty_mode_stats)
    stats = {
        "files": 0,
        "file_bytes": 0,
        "records": 0,
        "grouped_frames": 0,
        "grouped_records": 0,
        "raw_records": 0,
        "raw_payload_bytes": 0,
        "stored_payload_bytes": 0,
        "truncated_files": 0,
        "parse_errors": 0,
    }
    samples: list[dict[str, Any]] = []

    for path in value_log_files(log_dir):
        stats["files"] += 1
        data = path.read_bytes()
        stats["file_bytes"] += len(data)
        pos = 0
        while pos + RECORD_HEADER_SIZE <= len(data):
            flags = data[pos + 5]
            body_len = struct.unpack_from("<I", data, pos + 16)[0]
            body_start = pos + RECORD_HEADER_SIZE
            body_end = body_start + body_len
            if body_end > len(data):
                stats["truncated_files"] += 1
                break
            body = data[body_start:body_end]
            stats["records"] += 1
            if flags & RECORD_FLAG_GROUPED == 0:
                mode = modes["raw_record"]
                mode["frames"] += 1
                mode["records"] += 1
                mode["raw_payload_bytes"] += body_len
                mode["stored_payload_bytes"] += body_len
                mode["record_body_bytes"] += body_len
                stats["raw_records"] += 1
                stats["raw_payload_bytes"] += body_len
                stats["stored_payload_bytes"] += body_len
                if len(samples) < 12:
                    samples.append(
                        {
                            "path": str(path),
                            "offset": pos,
                            "mode": "raw_record",
                            "raw_payload_bytes": body_len,
                            "stored_payload_bytes": body_len,
                        }
                    )
                pos = body_end
                continue

            if len(body) < FRAME_HEADER_SIZE:
                stats["parse_errors"] += 1
                pos = body_end
                continue
            version = body[0]
            frame_flags = body[1]
            k = body[2]
            reserved = body[3]
            dict_id = struct.unpack_from("<Q", body, 4)[0]
            offsets_start = FRAME_HEADER_SIZE + k * 8
            offsets_end = offsets_start + (k + 1) * 4
            if version != 1 or k == 0 or offsets_end > len(body):
                stats["parse_errors"] += 1
                pos = body_end
                continue
            raw_payload = struct.unpack_from("<I", body, offsets_start + k * 4)[0]
            stored_payload = len(body) - offsets_end
            name = frame_mode(frame_flags, reserved, dict_id)
            mode = modes[name]
            mode["frames"] += 1
            mode["records"] += k
            mode["raw_payload_bytes"] += raw_payload
            mode["stored_payload_bytes"] += stored_payload
            mode["record_body_bytes"] += body_len

            stats["grouped_frames"] += 1
            stats["grouped_records"] += k
            stats["raw_payload_bytes"] += raw_payload
            stats["stored_payload_bytes"] += stored_payload
            if len(samples) < 12 and (name == "grouped_raw" or raw_payload == stored_payload):
                samples.append(
                    {
                        "path": str(path),
                        "offset": pos,
                        "mode": name,
                        "raw_payload_bytes": raw_payload,
                        "stored_payload_bytes": stored_payload,
                        "records": k,
                    }
                )
            pos = body_end
        if pos < len(data) and pos + RECORD_HEADER_SIZE > len(data):
            stats["truncated_files"] += 1

    raw = stats["raw_payload_bytes"]
    stored = stats["stored_payload_bytes"]
    raw_mode_bytes = modes.get("raw_record", {}).get("raw_payload_bytes", 0) + modes.get(
        "grouped_raw", {}
    ).get("raw_payload_bytes", 0)
    mode_rows: dict[str, dict[str, Any]] = {}
    for name, mode_stats in sorted(modes.items()):
        enriched = dict(mode_stats)
        frames = enriched["frames"]
        records = enriched["records"]
        raw_bytes = enriched["raw_payload_bytes"]
        stored_bytes = enriched["stored_payload_bytes"]
        enriched["codec"] = mode_codec(name)
        enriched["records_per_frame"] = (records / frames) if frames else None
        enriched["raw_payload_bytes_per_frame"] = (raw_bytes / frames) if frames else None
        enriched["stored_payload_bytes_per_frame"] = (stored_bytes / frames) if frames else None
        enriched["stored_to_raw_ratio"] = (stored_bytes / raw_bytes) if raw_bytes else None
        mode_rows[name] = enriched
    return {
        **stats,
        "stored_to_raw_ratio": (stored / raw) if raw else None,
        "raw_mode_payload_bytes": raw_mode_bytes,
        "raw_mode_payload_fraction": (raw_mode_bytes / raw) if raw else None,
        "modes": mode_rows,
        "raw_frame_samples": samples,
    }


def vlog_frame_audit(main_dir: Path) -> dict[str, Any]:
    leaf = scan_log_frames(main_dir / "leaf_vlog")
    value = scan_log_frames(main_dir / "value_vlog")
    return {
        "leaf_vlog": leaf,
        "value_vlog": value,
        "gates": {
            "value_vlog_raw_mode_payload_bytes": value["raw_mode_payload_bytes"],
            "value_vlog_raw_mode_payload_fraction": value["raw_mode_payload_fraction"],
            "leaf_vlog_raw_mode_payload_bytes": leaf["raw_mode_payload_bytes"],
            "leaf_vlog_raw_mode_payload_fraction": leaf["raw_mode_payload_fraction"],
        },
    }


def column_section_audit(main_dir: Path, gzip_level: int) -> dict[str, Any]:
    root = main_dir / "column_assets"
    files = []
    for path in iter_files(root):
        data = path.read_bytes()
        gz = gzip.compress(data, compresslevel=gzip_level)
        files.append(
            {
                "path": str(path),
                "name": path.name,
                "bytes": len(data),
                "gzip_bytes": len(gz),
                "gzip_to_raw_ratio": (len(gz) / len(data)) if data else None,
            }
        )
    total = sum(row["bytes"] for row in files)
    gzip_total = sum(row["gzip_bytes"] for row in files)
    return {
        "status": "filesystem_oracle_only",
        "reason": "section-level typed-column descriptors require JSONBench/typed-column metadata; this audit reports file-level compression headroom",
        "files": files,
        "total_bytes": total,
        "gzip_bytes": gzip_total,
        "gzip_to_raw_ratio": (gzip_total / total) if total else None,
    }


def compression_fields(data: Any, prefix: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if "compression" in str(key).lower():
                out.append({"path": path, "value": value})
            out.extend(compression_fields(value, path))
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            out.extend(compression_fields(value, f"{prefix}[{idx}]"))
    return out


def load_result_json(result_json: Path | None) -> Any | None:
    if result_json is None:
        return None
    return json.loads(result_json.read_text())


def load_result_compression_summary(result_json: Path | None, data: Any | None = None) -> dict[str, Any] | None:
    if result_json is None:
        return None
    if data is None:
        data = load_result_json(result_json)
    fields = compression_fields(data)
    raw = json.dumps(fields, sort_keys=True).lower()
    silent_none = "requested=none" in raw or "actual=none" in raw or '"none"' in raw
    return {
        "path": str(result_json),
        "compression_fields": fields,
        "silent_none_suspected": silent_none,
    }


def fields_named(data: Any, names: set[str], prefix: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if str(key) in names:
                out.append({"path": path, "value": value})
            out.extend(fields_named(value, names, path))
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            out.extend(fields_named(value, names, f"{prefix}[{idx}]"))
    return out


def jsonbench_cells(data: Any, prefix: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if str(key) == "jsonbench_cells" and isinstance(value, list):
                for idx, cell in enumerate(value):
                    if isinstance(cell, dict):
                        out.append({"path": f"{path}[{idx}]", "value": cell})
            out.extend(jsonbench_cells(value, path))
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            out.extend(jsonbench_cells(value, f"{prefix}[{idx}]"))
    return out


def retained_status_value_inactive(value: Any) -> bool:
    text = str(value).lower()
    return text == "" or "inactive" in text or text in {"none", "not_configured"}


def retained_status_cell_findings(
    cells: list[dict[str, Any]],
    value_names: set[str],
    status_names: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    missing: list[dict[str, Any]] = []
    inactive: list[dict[str, Any]] = []
    for cell_info in cells:
        path = str(cell_info["path"])
        cell = cell_info["value"]
        present_values = [name for name in sorted(value_names) if name in cell]
        present_statuses = [name for name in sorted(status_names) if name in cell]
        if not present_values or not present_statuses:
            missing.append(
                {
                    "path": path,
                    "missing_value_fields": sorted(value_names - set(present_values)),
                    "missing_status_fields": sorted(status_names - set(present_statuses)),
                }
            )
        for name in present_values + present_statuses:
            value = cell.get(name)
            if retained_status_value_inactive(value):
                inactive.append({"path": f"{path}.{name}", "value": value})
    return missing, inactive


def retained_status_summary(result_json: Path | None, data: Any | None = None) -> dict[str, Any]:
    if result_json is None:
        return {
            "path": None,
            "retained_payload_encoding_status_missing": True,
            "retained_payload_encoding_inactive": True,
            "retained_payload_compression_status_missing": True,
            "retained_payload_compression_inactive": True,
            "reason": "no result.json supplied; retained encoding/compression status cannot be proven active",
        }
    if data is None:
        data = load_result_json(result_json)
    cells = jsonbench_cells(data)
    encoding_fields = fields_named(data, {"retained_payload_encoding"})
    encoding_status_fields = fields_named(data, {"retained_payload_encoding_status"})
    compression_fields = fields_named(
        data,
        {"retained_payload_compression", "retained_payload_compression_policy"},
    )
    compression_status_fields = fields_named(data, {"retained_payload_compression_status"})

    def inactive(fields: list[dict[str, Any]]) -> bool:
        if not fields:
            return True
        for field in fields:
            if retained_status_value_inactive(field.get("value", "")):
                return True
        return False

    encoding_missing_cells, encoding_inactive_cells = retained_status_cell_findings(
        cells,
        {"retained_payload_encoding"},
        {"retained_payload_encoding_status"},
    )
    compression_missing_cells, compression_inactive_cells = retained_status_cell_findings(
        cells,
        {"retained_payload_compression", "retained_payload_compression_policy"},
        {"retained_payload_compression_status"},
    )
    if cells:
        encoding_missing = bool(encoding_missing_cells)
        encoding_inactive = encoding_missing or bool(encoding_inactive_cells)
        compression_missing = bool(compression_missing_cells)
        compression_inactive = compression_missing or bool(compression_inactive_cells)
    else:
        encoding_missing = not encoding_fields or not encoding_status_fields
        encoding_inactive = inactive(encoding_fields) or inactive(encoding_status_fields)
        compression_missing = not compression_fields or not compression_status_fields
        compression_inactive = inactive(compression_fields) or inactive(compression_status_fields)

    return {
        "path": str(result_json),
        "jsonbench_cell_count": len(cells),
        "retained_payload_encoding_fields": encoding_fields,
        "retained_payload_encoding_status_fields": encoding_status_fields,
        "retained_payload_encoding_missing_cells": encoding_missing_cells,
        "retained_payload_encoding_inactive_cells": encoding_inactive_cells,
        "retained_payload_encoding_status_missing": encoding_missing,
        "retained_payload_encoding_inactive": encoding_inactive,
        "retained_payload_compression_fields": compression_fields,
        "retained_payload_compression_status_fields": compression_status_fields,
        "retained_payload_compression_missing_cells": compression_missing_cells,
        "retained_payload_compression_inactive_cells": compression_inactive_cells,
        "retained_payload_compression_status_missing": compression_missing,
        "retained_payload_compression_inactive": compression_inactive,
    }


def enrich_retained_status_from_audit(status: dict[str, Any], retained_audit: dict[str, Any]) -> dict[str, Any]:
    if not retained_audit:
        return status

    def append_field(bucket: str, name: str) -> Any:
        value = retained_audit.get(name)
        if value is None:
            return None
        status.setdefault(bucket, []).append({"path": f"retained_payload_audit.{name}", "value": value})
        return value

    encoding = append_field("retained_payload_encoding_fields", "retained_payload_encoding")
    encoding_status = append_field("retained_payload_encoding_status_fields", "retained_payload_encoding_status")
    compression = append_field("retained_payload_compression_fields", "retained_payload_compression")
    compression_policy = append_field("retained_payload_compression_fields", "retained_payload_compression_policy")
    compression_status = append_field("retained_payload_compression_status_fields", "retained_payload_compression_status")

    if encoding is not None and encoding_status is not None:
        status["retained_payload_encoding_status_missing"] = False
        status["retained_payload_encoding_inactive"] = retained_status_value_inactive(encoding) or retained_status_value_inactive(encoding_status)
    if compression is not None and compression_policy is not None and compression_status is not None:
        status["retained_payload_compression_status_missing"] = False
        status["retained_payload_compression_inactive"] = (
            retained_status_value_inactive(compression)
            or retained_status_value_inactive(compression_policy)
            or retained_status_value_inactive(compression_status)
        )
    if (
        not status.get("retained_payload_encoding_status_missing")
        and not status.get("retained_payload_encoding_inactive")
        and not status.get("retained_payload_compression_status_missing")
        and not status.get("retained_payload_compression_inactive")
    ):
        status["retained_payload_status_source"] = "retained_payload_audit"
    return status


def result_collection_name(data: Any | None) -> str | None:
    if not isinstance(data, dict):
        return None
    for key in ("collection", "collection_name"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("collection", "collection_name"):
        for field in fields_named(data, {key}):
            value = field.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def retained_payload_audit_stub(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "not_available_from_filesystem_audit",
        "required_for_final_claim": True,
        "reason": "path-aware retained-payload audit must decode retained rows and verify declared JSON paths are absent while reconstruction hash passes",
        "declared_paths": JSONBENCH_DECLARED_PATHS,
        "report_status": status,
    }


def run_retained_payload_audit(args: argparse.Namespace, result_data: Any | None, retained_status: dict[str, Any]) -> dict[str, Any]:
    if getattr(args, "skip_retained_payload_audit", False):
        retained = retained_payload_audit_stub(retained_status)
        retained["reason"] = "retained payload audit was explicitly skipped"
        return retained
    collection = result_collection_name(result_data)
    if collection is None:
        retained = retained_payload_audit_stub(retained_status)
        retained["reason"] = "no collection name found in result.json; retained payload audit command was not run"
        return retained

    paths = JSONBENCH_DECLARED_PATHS
    audit_cmd = getattr(args, "retained_payload_audit_cmd", None)
    if audit_cmd:
        command = shlex.split(audit_cmd)
        cwd = None
    else:
        repo_root = Path(__file__).resolve().parent.parent
        command = ["go", "run", "./cmd/treedb_retained_payload_audit"]
        cwd = str(repo_root)
    command.extend(
        [
            "-db-dir",
            str(Path(args.db_dir).expanduser().resolve()),
            "-collection",
            collection,
            "-paths",
            ",".join(paths),
        ]
    )
    limit = int(getattr(args, "retained_payload_audit_limit", 0) or 0)
    if limit > 0:
        command.extend(["-max-documents", str(limit)])

    try:
        completed = subprocess.run(command, cwd=cwd, check=False, text=True, capture_output=True)
    except Exception as exc:  # pragma: no cover - defensive CLI failure path.
        return {
            "status": "failed",
            "required_for_final_claim": True,
            "reason": "retained payload audit command failed before producing JSON",
            "declared_paths": paths,
            "collection": collection,
            "command": command,
            "errors": [str(exc)],
            "report_status": retained_status,
        }

    try:
        retained = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {
            "status": "failed",
            "required_for_final_claim": True,
            "reason": "retained payload audit command did not produce JSON",
            "declared_paths": paths,
            "collection": collection,
            "command": command,
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
            "report_status": retained_status,
        }
    if not isinstance(retained, dict):
        retained = {"status": "failed", "errors": ["retained payload audit JSON was not an object"]}
    retained.setdefault("status", "failed" if completed.returncode else "passed")
    retained.setdefault("collection", collection)
    retained.setdefault("declared_paths", paths)
    retained["required_for_final_claim"] = True
    retained["command"] = command
    retained["returncode"] = completed.returncode
    if completed.stderr:
        retained["stderr"] = completed.stderr[-4000:]
    if completed.returncode != 0:
        retained.setdefault("reason", "retained payload audit command returned non-zero")
    retained["report_status"] = retained_status
    return retained


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def ratio_text(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def write_md(path: Path, report: dict[str, Any]) -> None:
    gzip_rows = report["gzip_oracle"]["subtrees"]
    frame = report["vlog_frame_audit"]
    column = report["column_section_audit"]
    retained = report["retained_payload_audit"]
    retained_status = report["retained_payload_status_audit"]
    lines = [
        "# TreeDB JSONBench Storage Compression Audit",
        "",
        f"- DB dir: `{report['db_dir']}`",
        f"- main dir: `{report['main_dir']}`",
        f"- generated at unix: `{report['generated_at_unix']}`",
        "",
        "## gzip oracle",
        "",
        "| subtree | files | raw bytes | gzip bytes | gzip/raw |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in gzip_rows:
        lines.append(
            f"| `{row['subtree']}` | {row['files']} | {row['raw_bytes']} | {row['gzip_bytes']} | {ratio_text(row['gzip_to_raw_ratio'])} |"
        )

    lines.extend(
        [
            "",
            "## vlog frame audit",
            "",
            "| log | records | raw payload | stored payload | raw-mode bytes | raw-mode fraction | stored/raw |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for name in ("leaf_vlog", "value_vlog"):
        row = frame[name]
        lines.append(
            f"| `{name}` | {row['records']} | {row['raw_payload_bytes']} | {row['stored_payload_bytes']} | {row['raw_mode_payload_bytes']} | {ratio_text(row['raw_mode_payload_fraction'])} | {ratio_text(row['stored_to_raw_ratio'])} |"
        )

    for name in ("leaf_vlog", "value_vlog"):
        row = frame[name]
        lines.extend(["", f"### {name} modes", "", "| mode | codec | frames | records | records/frame | raw payload | raw/frame | stored payload | stored/frame | stored/raw |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
        for mode, stats in row["modes"].items():
            lines.append(
                f"| `{mode}` | `{stats['codec']}` | {stats['frames']} | {stats['records']} | {ratio_text(stats['records_per_frame'])} | {stats['raw_payload_bytes']} | {ratio_text(stats['raw_payload_bytes_per_frame'])} | {stats['stored_payload_bytes']} | {ratio_text(stats['stored_payload_bytes_per_frame'])} | {ratio_text(stats['stored_to_raw_ratio'])} |"
            )

    lines.extend(
        [
            "",
            "## column section audit",
            "",
            f"- status: `{column['status']}`",
            f"- reason: {column['reason']}",
            f"- total bytes: `{column['total_bytes']}`",
            f"- gzip bytes: `{column['gzip_bytes']}`",
            f"- gzip/raw: `{ratio_text(column['gzip_to_raw_ratio'])}`",
            "",
            "## retained payload audit",
            "",
            f"- status: `{retained['status']}`",
            f"- final claim requires path-aware retained payload audit: `{retained['required_for_final_claim']}`",
            f"- reason: {retained.get('reason', 'n/a')}",
            f"- checked rows: `{retained.get('checked_rows', 'n/a')}`",
            f"- retained payload bytes: `{retained.get('retained_payload_bytes', 'n/a')}`",
            f"- declared paths: `{', '.join(retained.get('declared_paths', []))}`",
            f"- retained encoding status missing: `{retained_status['retained_payload_encoding_status_missing']}`",
            f"- retained encoding inactive: `{retained_status['retained_payload_encoding_inactive']}`",
            f"- retained compression status missing: `{retained_status['retained_payload_compression_status_missing']}`",
            f"- retained compression inactive: `{retained_status['retained_payload_compression_inactive']}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    main_dir = resolve_main_dir(Path(args.db_dir))
    result_json = Path(args.result_json).expanduser().resolve() if args.result_json else None
    result_data = load_result_json(result_json) if result_json else None
    retained_status = retained_status_summary(result_json, result_data)
    retained_audit = run_retained_payload_audit(args, result_data, retained_status)
    retained_status = enrich_retained_status_from_audit(retained_status, retained_audit)
    retained_audit["report_status"] = retained_status
    return {
        "schema": "treedb_jsonbench_storage_audit_v1",
        "db_dir": str(Path(args.db_dir).expanduser().resolve()),
        "main_dir": str(main_dir),
        "generated_at_unix": int(time.time()),
        "gzip_oracle": gzip_oracle(main_dir, args.gzip_level),
        "vlog_frame_audit": vlog_frame_audit(main_dir),
        "column_section_audit": column_section_audit(main_dir, args.gzip_level),
        "retained_payload_status_audit": retained_status,
        "retained_payload_audit": retained_audit,
        "result_compression_summary": load_result_compression_summary(result_json, result_data),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir", required=True, help="TreeDB DB dir or maindb dir")
    parser.add_argument("--result-json", help="Optional JSONBench result.json for compression policy fields")
    parser.add_argument("--out-dir", help="Directory for audit JSON/Markdown artifacts")
    parser.add_argument("--gzip-level", type=int, default=6)
    parser.add_argument("--json-only", action="store_true", help="Print combined JSON to stdout")
    parser.add_argument("--retained-payload-audit-cmd", help="Command used to run the retained-payload audit; defaults to go run ./cmd/treedb_retained_payload_audit")
    parser.add_argument("--retained-payload-audit-limit", type=int, default=0, help="Maximum retained rows to audit; zero audits all rows")
    parser.add_argument("--skip-retained-payload-audit", action="store_true", help="Skip the path-aware retained-payload audit")
    args = parser.parse_args()

    report = build_report(args)
    if args.json_only or not args.out_dir:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(out_dir / "compression_audit.json", report)
    write_md(out_dir / "compression_audit.md", report)
    write_json(out_dir / "gzip_oracle.json", report["gzip_oracle"])
    write_json(out_dir / "vlog_frame_audit.json", report["vlog_frame_audit"])
    write_json(out_dir / "column_section_audit.json", report["column_section_audit"])
    write_json(out_dir / "retained_payload_status_audit.json", report["retained_payload_status_audit"])
    write_json(out_dir / "retained_payload_audit.json", report["retained_payload_audit"])
    print(out_dir / "compression_audit.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
