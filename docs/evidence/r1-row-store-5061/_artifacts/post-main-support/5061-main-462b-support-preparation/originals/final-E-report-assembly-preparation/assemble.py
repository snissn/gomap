#!/usr/bin/env python3
"""Byte-preserving private E report assembly; no capture/qualification side effects."""
import argparse
import hashlib
import json
import re
import string
from pathlib import Path, PurePosixPath

ARTIFACT_PREFIX = "../../../../docs/evidence/r1-row-store-5061/_artifacts/"
HISTORICAL_SHA = "e636404bed9bb61be7543428dbc97504bc8511228467cdfab07c21701acfde8b"
HISTORICAL_HEAD = "4c9f21a51d304534619e8f70155a5dea51c5989b"
FORMATTERS = {
    "A": "b2e386b4ec939978b9de1759ff6a2b564abf2afc0ecd136171823302b1fdefa4",
    "C": "52d94e91dcfd696c1d8aaa29f3a39ff0da010e5d238bf10818d802832a66323d",
    "D": "022c45de44e96014de9ef343b639be4abd92189152efd411314b829b54c4da58",
}
FIELDS = ("source_commit", "landed_commit", "runtime_sha256", "harness_sha256",
          "fixture_sha256", "config_sha256", "binary_sha256", "packet_sha256")
RECEIPTS = ("source_receipt", "validation_receipt", "decision_receipt")


def refuse(message):
    raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def resolve(base, name):
    if not isinstance(name, str) or not name:
        refuse("missing input path")
    path = Path(name)
    return path if path.is_absolute() else base / path


def verified_file(base, record):
    if not isinstance(record, dict):
        refuse("input record must supply path and sha256")
    path = resolve(base, record.get("path"))
    data = path.read_bytes()
    if digest(data) != record.get("sha256"):
        refuse(f"input hash mismatch: {path}")
    return data


def value(value, length, synthetic):
    if synthetic and value is None:
        return "UNSUPPLIED — NONQUALIFYING"
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{%d}" % length, value):
        refuse("missing or invalid observed commit/digest")
    return value


def receipt(base, record, synthetic):
    if synthetic and record is None:
        return "UNSUPPLIED — NONQUALIFYING"
    verified_file(base, record)
    artifact = record.get("artifact_path")
    if not isinstance(artifact, str) or not artifact or "\\" in artifact:
        refuse("receipt requires an artifact-relative publication path")
    parts = PurePosixPath(artifact).parts
    if artifact.startswith("/") or ".." in parts or ":" in artifact or "\n" in artifact or any(c in artifact for c in "[]()<>|"):
        refuse("unsafe artifact-relative publication path")
    return f"[{artifact}]({ARTIFACT_PREFIX}{artifact}) (`{record['sha256']}`)"


def assemble(config_path, template_path, synthetic=False):
    base = config_path.parent
    config = json.loads(config_path.read_text())
    expected_mode = "synthetic-format-only" if synthetic else "root-supplied-evidence"
    if config.get("mode") != expected_mode:
        refuse("mode must match invocation; synthetic config cannot become actual evidence")
    if config.get("artifact_link_prefix") != ARTIFACT_PREFIX:
        refuse("wrong artifact root: artifacts belong outside TreeDB")
    historical = verified_file(base, config["historical_report"])
    if digest(historical) != HISTORICAL_SHA:
        refuse("historical report changed; reassess before replacing the frozen snapshot")
    actual_main = value(config.get("actual_main"), 40, synthetic)
    actual_tree = value(config.get("actual_main_tree"), 40, synthetic)
    landing = receipt(base, config.get("landing_receipt"), synthetic)
    public_replay = receipt(base, config.get("public_replay_receipt"), synthetic)
    interpretation = verified_file(base, config["interpretation"]).decode("utf-8")
    comparison = verified_file(base, config["A_comparison"]).decode("utf-8")
    fragments = {}
    lines = ["| Evidence | Source / landing | Runtime / harness | Fixture / config | Binary / packet | Original authority receipts |",
             "| --- | --- | --- | --- | --- | --- |"]
    for name in ("A", "C", "D"):
        group = config["evidence"][name]
        formatter = verified_file(base, group["formatter"])
        if digest(formatter) != FORMATTERS[name]:
            refuse(f"{name} formatter is not the frozen reviewed helper")
        fragments[name] = verified_file(base, group["markdown"]).decode("utf-8")
        pins = {key: value(group.get(key), 40 if key.endswith("commit") else 64, synthetic) for key in FIELDS}
        authority = "<br>".join(receipt(base, group.get(key), synthetic) for key in RECEIPTS)
        cells = [name, f"`{pins['source_commit']}`<br>`{pins['landed_commit']}`",
                 f"`{pins['runtime_sha256']}`<br>`{pins['harness_sha256']}`",
                 f"`{pins['fixture_sha256']}`<br>`{pins['config_sha256']}`",
                 f"`{pins['binary_sha256']}`<br>`{pins['packet_sha256']}`", authority]
        lines.append("| " + " | ".join(cells) + " |")
    banner = ("**SYNTHETIC FORMAT ONLY — NO FINAL CAPTURE, LANDING, PUBLIC REPLAY OR ACCEPTANCE.**\n\n"
              if synthetic else "")
    context = dict(banner=banner, actual_main=actual_main, actual_tree=actual_tree,
                   landing=landing, public_replay=public_replay, source_table="\n".join(lines),
                   interpretation=interpretation, A_comparison=comparison,
                   historical_head=HISTORICAL_HEAD, historical_sha=HISTORICAL_SHA,
                   historical=historical.decode("utf-8"), **fragments)
    rendered = string.Template(template_path.read_text()).substitute(context).encode("utf-8")
    # Substitution is one pass: no fragment/history contents are rewritten or rebound.
    for text in [historical.decode("utf-8"), interpretation, comparison, *fragments.values()]:
        if text.encode("utf-8") not in rendered:
            refuse("verbatim fragment/history preservation failed")
    return rendered


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--template", type=Path, default=Path(__file__).with_name("report-template.md"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--format-only", action="store_true")
    args = parser.parse_args()
    try:
        result = assemble(args.manifest, args.template, args.format_only)
        with args.out.open("xb") as output:
            output.write(result)
    except (ValueError, KeyError, OSError, UnicodeError, json.JSONDecodeError) as error:
        parser.exit(1, f"refused: {error}\n")
    print(json.dumps({"status": "SYNTHETIC_FORMAT_ONLY_NONQUALIFYING" if args.format_only else "ASSEMBLED_SUPPLIED_BYTES_NOT_ACCEPTANCE",
                      "output": str(args.out), "sha256": digest(result), "bytes": len(result)}))


if __name__ == "__main__":
    main()
