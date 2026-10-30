#!/usr/bin/env python3
"""Validate retained, typed deterministic fixture snapshots (not YCSB timings)."""
import argparse
import base64
import json
import sys

IDENTITY = ("schema_version", "source_id", "fixture_id", "profile", "target", "format")

def fail(message):
    raise ValueError(message)

def read(path):
    def unique(pairs):
        result = {}
        for key, val in pairs:
            if key in result:
                fail("duplicate JSON object key")
            result[key] = val
        return result
    with open(path, encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=unique)

def typed(v, fmt):
    if not isinstance(v, dict):
        fail("field value must be typed")
    if fmt == "json":
        if set(v) != {"type", "value"} or v["type"] != "string" or not isinstance(v["value"], str):
            fail("JSON value must be a typed string")
        decoded = base64.b64decode(v["value"], validate=True)
        if base64.b64encode(decoded).decode() != v["value"]:
            fail("noncanonical JSON base64 string")
    elif fmt == "bson":
        if set(v) != {"type", "subtype", "base64"} or v["type"] != "binary" or v["subtype"] != 0:
            fail("BSON value must be subtype-0 binary")
        raw = base64.b64decode(v["base64"], validate=True)
        if base64.b64encode(raw).decode() != v["base64"]:
            fail("noncanonical binary encoding")
    else:
        fail("unsupported representation")

def rows(value, fmt):
    if not isinstance(value, list):
        fail("rows must be a list")
    out = {}
    for row in value:
        if not isinstance(row, dict) or set(row) != {"id", "fields"} or not isinstance(row["id"], str) or not row["id"]:
            fail("invalid row")
        if row["id"] in out or not isinstance(row["fields"], dict) or not row["fields"]:
            fail("duplicate row or invalid fields")
        for name, val in row["fields"].items():
            if not isinstance(name, str) or not name:
                fail("invalid field name")
            typed(val, fmt)
        out[row["id"]] = row["fields"]
    return out

def validate(expected, observations):
    if any(k not in expected for k in IDENTITY) or type(expected["schema_version"]) is not int or expected["schema_version"] != 1:
        fail("invalid expected identity")
    if expected["target"] not in ("native", "mongo") or expected["profile"] not in ("command_wal_durable", "command_wal_relaxed", "no_wal_fast"):
        fail("invalid target or profile")
    if not all(isinstance(expected[k], str) and expected[k] for k in IDENTITY[1:]):
        fail("missing identity")
    initial = rows(expected.get("load"), expected["format"])
    if len(initial) != 64:
        fail("expected exactly 64 load records")
    final = {k: dict(v) for k, v in initial.items()}
    updates = expected.get("updates")
    if not isinstance(updates, list) or len(updates) != 128:
        fail("expected exactly two updates for each record")
    per_id = {k: [] for k in initial}
    for update in updates:
        if not isinstance(update, dict) or set(update) != {"id", "worker", "sequence", "field", "value"}:
            fail("invalid update intent")
        key = update["id"]
        if key not in initial or update["field"] not in initial[key] or type(update["worker"]) is not int or type(update["sequence"]) is not int or not 0 <= update["worker"] < 4:
            fail("invalid update key, worker or field")
        typed(update["value"], expected["format"])
        per_id[key].append(update)
        final[key][update["field"]] = update["value"]
    ownership = [0, 0, 0, 0]
    for group in per_id.values():
        if len(group) != 2 or [u["sequence"] for u in group] != [1, 2] or group[0]["worker"] != group[1]["worker"]:
            fail("updates must be sequential and disjoint per key")
        ownership[group[0]["worker"]] += 1
    if ownership != [16, 16, 16, 16]:
        fail("workers must own 16 keys each")
    seen = set()
    for observed in observations:
        if any(observed.get(k) != expected[k] for k in IDENTITY):
            fail("snapshot identity mismatch")
        stage = observed.get("stage")
        if stage not in ("load", "updated", "reopened") or stage in seen:
            fail("invalid or duplicate stage")
        seen.add(stage)
        if rows(observed.get("rows"), expected["format"]) != (initial if stage == "load" else final):
            fail("snapshot state differs at " + stage)
        outcomes = observed.get("operations")
        if not isinstance(outcomes, list):
            fail("missing individual operation outcomes")
        wanted = ({(key, 0) for key in initial} if stage == "load" else
                  {(u["id"], u["sequence"]) for u in updates} if stage == "updated" else set())
        got = set()
        for result in outcomes:
            if not isinstance(result, dict) or set(result) != {"id", "sequence", "acknowledged", "readback_ok", "error"}:
                fail("invalid operation outcome")
            if not isinstance(result["id"], str) or type(result["sequence"]) is not int:
                fail("invalid operation identity")
            token = (result["id"], result["sequence"])
            if token in got or result["acknowledged"] is not True or result["readback_ok"] is not True or result["error"] != "":
                fail("duplicate, failed or unacknowledged operation")
            got.add(token)
        if got != wanted:
            fail("missing or unexpected operation outcomes")
    if not {"load", "updated", "reopened"}.issubset(seen):
        fail("missing required lifecycle snapshot")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", required=True)
    parser.add_argument("--observed", required=True, action="append")
    args = parser.parse_args()
    try:
        validate(read(args.expected), [read(p) for p in args.observed])
    except (ValueError, TypeError, KeyError, OSError, json.JSONDecodeError) as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    print("PASS: exact typed fixture state and lifecycle snapshots match")
    return 0

if __name__ == "__main__":
    sys.exit(main())
