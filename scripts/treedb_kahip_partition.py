#!/usr/bin/env python3
"""Pinned offline KaHIP 3.25 adapter for TreeDB's validated JSON contract."""

import base64
import csv
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import sys

# KaHIP's Python extension reads OpenMP settings during import. Force a single
# worker so its offline output identity does not depend on host scheduling.
os.environ["OMP_NUM_THREADS"] = "1"

WHEEL_SHA256 = "e6ea76524e9fc01b27e6f5c5f00b7eec71c94cbd1e84678ce2a14d64dfc9eda4"
RECORD_SHA256 = "7ff011253147286fcebc9185573662bf31dbcfbab1944f9b4940032f49ea5217"


def pinned_kahip():
    distribution = importlib.metadata.distribution("kahip")
    record = next(path for path in distribution.files if str(path).endswith("RECORD"))
    record_bytes = distribution.locate_file(record).read_bytes()
    record_hash = hashlib.sha256(record_bytes).hexdigest()
    if (
        distribution.version != "3.25"
        or distribution.metadata["License"] != "MIT"
        or record_hash != RECORD_SHA256
    ):
        raise SystemExit("requires pinned kahip==3.25 MIT distribution")
    extension_path = None
    for path, digest, size in csv.reader(record_bytes.decode("utf-8").splitlines()):
        if not digest:
            continue
        algorithm, encoded = digest.split("=", 1)
        if algorithm != "sha256" or not size.isdecimal():
            raise SystemExit("unsupported KaHIP RECORD entry")
        payload = distribution.locate_file(path).read_bytes()
        expected = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        if len(payload) != int(size) or hashlib.sha256(payload).digest() != expected:
            raise SystemExit("KaHIP RECORD payload integrity mismatch")
        if str(path).startswith("kahip/kahip."):
            extension_path = distribution.locate_file(path)
    if extension_path is None:
        raise SystemExit("pinned KaHIP native extension is missing")
    return extension_path


if len(sys.argv) != 3:
    raise SystemExit("usage: treedb_kahip_partition.py INPUT OUTPUT")
extension_path = pinned_kahip()
spec = importlib.util.spec_from_file_location("kahip.kahip", extension_path)
if spec is None or spec.loader is None:
    raise SystemExit("pinned KaHIP native extension is invalid")
kahip = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kahip)
with open(sys.argv[1], encoding="utf-8") as input_file:
    artifact = json.load(input_file)
config = artifact["config"]
partitions = config["partitions"]
seed = config["seed"]
if (
    config["imbalance"] != 0.05
    or type(partitions) is not int
    or partitions < 1
    or partitions > 16_384
    or type(seed) is not int
    or seed < -2_147_483_648
    or seed > 2_147_483_647
):
    raise SystemExit("KaHIP request identity/configuration mismatch")
nodes = len(artifact["ids"])
neighbors = artifact["graph"]["neighbors"]
if nodes == 0 or nodes > 1_000_000 or partitions > nodes or len(neighbors) != nodes:
    raise SystemExit("invalid graph")
rows = [set() for _ in range(nodes)]
directed = 0
for source, targets in enumerate(neighbors):
    directed += len(targets)
    # V1's canonical offline envelope is one million vectors at degree 16.
    # Do not accept the broader graph-builder reservation (64M edges).
    if directed > 16_000_000:
        raise SystemExit("selected KaHIP directed-edge envelope exceeded")
    for target in targets:
        if not isinstance(target, int) or target < 0 or target >= nodes or target == source:
            raise SystemExit("invalid graph edge")
        rows[source].add(target)
        rows[target].add(source)
xadj = [0]
adjncy = []
for row in rows:
    adjncy.extend(sorted(row))
    xadj.append(len(adjncy))
_, assignment = kahip.kaffpa(
    [1] * nodes,
    xadj,
    [1] * len(adjncy),
    adjncy,
    partitions,
    config["imbalance"],
    False,
    seed,
    1,  # KaHIP ECO mode
)
if len(assignment) != nodes or any(not isinstance(partition, int) or partition < 0 or partition >= partitions for partition in assignment):
    raise SystemExit("invalid KaHIP assignment")
loads = [0] * partitions
for partition in assignment:
    loads[partition] += 1
if max(loads) > artifact["metrics"]["cap"] or min(loads) < 1:
    raise SystemExit("KaHIP assignment violates capacity")
directed_cut = sum(
    assignment[source] != assignment[target]
    for source, targets in enumerate(neighbors)
    for target in targets
)
artifact["assignment"] = assignment
artifact["backend"] = "kahip_python_3.25_eco_symmetrized_v1_seed_%d" % seed
artifact["backend_license"] = "MIT; kahip==3.25; wheel_sha256=%s; record_sha256=%s; ECO; epsilon=0.05; symmetrized_unweighted_v1" % (WHEEL_SHA256, RECORD_SHA256)
artifact["metrics"]["edge_cut"] = directed_cut
artifact["metrics"]["max_partition_size"] = max(loads)
with open(sys.argv[2], "w", encoding="utf-8") as output_file:
    json.dump(artifact, output_file, separators=(",", ":"))
