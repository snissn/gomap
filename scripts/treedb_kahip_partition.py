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
HOME_PACKING_POLICY = "kahip_3_25_eco_induced_home_symmetrized_epsilon0_v1"


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
with open(sys.argv[1], "rb") as input_file:
    input_bytes = input_file.read()
request_sha256 = hashlib.sha256(input_bytes).hexdigest()
document = json.loads(input_bytes)
del input_bytes
packing = document.get("kind") == HOME_PACKING_POLICY
if "kind" in document and not packing:
    raise SystemExit("unsupported KaHIP request kind")
artifact = document["parent"] if packing else document
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

if packing:
    plan = document["plan"]
    count = plan["packs_per_domain"]
    assignment = artifact["assignment"]
    if (
        type(count) is not int or count < 1
        or plan["logical_domains"] != partitions
        or plan["partitions"] != partitions * count
        or plan["partitions"] > 16_384
        or plan["vectors"] != nodes
        or len(assignment) != nodes
    ):
        raise SystemExit("invalid home packing plan")
    domains = [[] for _ in range(partitions)]
    directed = 0
    for source, domain in enumerate(assignment):
        if type(domain) is not int or not 0 <= domain < partitions:
            raise SystemExit("invalid logical home")
        domains[domain].append(source)  # ascending parent ordinal, never a new source
        directed += len(neighbors[source])
        if directed > 16_000_000:
            raise SystemExit("selected KaHIP directed-edge envelope exceeded")
        if any(type(target) is not int or target < 0 or target >= nodes or target == source for target in neighbors[source]):
            raise SystemExit("invalid graph edge")
    homes = [-1] * nodes
    for domain, ordinals in enumerate(domains):
        if len(ordinals) < count:
            raise SystemExit("too few homes for nonempty physical packs")
        if count == 1:
            labels = [0] * len(ordinals)
        else:
            # Only this domain's projection/CSR is live. A cross-domain edge
            # contributes nothing; no queries, vectors or truth enter the solve.
            local = {ordinal: index for index, ordinal in enumerate(ordinals)}
            rows = [set() for _ in ordinals]
            for source in ordinals:
                left = local[source]
                for target in neighbors[source]:
                    if assignment[target] == domain:
                        right = local[target]
                        rows[left].add(right)
                        rows[right].add(left)
            xadj, adjncy = [0], []
            for row in rows:
                adjncy.extend(sorted(row))
                xadj.append(len(adjncy))
            del rows, row, local
            _, labels = kahip.kaffpa([1] * len(ordinals), xadj, [1] * len(adjncy), adjncy, count, 0.0, False, seed, 1)
            del xadj, adjncy
        if len(labels) != len(ordinals) or any(type(label) is not int or not 0 <= label < count for label in labels):
            raise SystemExit("invalid KaHIP home labels")
        loads = [0] * count
        for ordinal, label in zip(ordinals, labels):
            loads[label] += 1
            homes[ordinal] = domain * count + label
        cap = (len(ordinals) + count - 1) // count
        if min(loads) < 1 or max(loads) > min(cap, plan["home_capacity"], plan["overlap_capacity"], plan["max_memberships_per_pack"]):
            raise SystemExit("KaHIP home packing violates exact capacity")
    with open(sys.argv[2], "w", encoding="utf-8") as output_file:
        json.dump({"request_sha256": request_sha256, "policy": HOME_PACKING_POLICY, "homes": homes}, output_file, separators=(",", ":"))
    raise SystemExit(0)

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
