"""Strict JSON and type-preserving copy comparisons for retained YCSB evidence."""
import json
import math


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError(f"nonfinite JSON number {value}")


def finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        reject_constant(value)
    return number


def read(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream, object_pairs_hook=unique_object,
                         parse_constant=reject_constant, parse_float=finite_float)


def canonical(value):
    # Python == aliases True/1 and 1/1.0 recursively. JSON encoding retains
    # those distinctions while deliberately ignoring object member order.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def same(left, right):
    return canonical(left) == canonical(right)
