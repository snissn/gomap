#!/usr/bin/env python3
"""Pinned offline KaHIP 3.25 adapter for TreeDB's validated JSON contract."""
import json, os, sys
# KaHIP's Python extension reads OpenMP settings during import. Force a single
# worker so its offline output identity does not depend on host scheduling.
os.environ["OMP_NUM_THREADS"] = "1"
import kahip

if len(sys.argv) != 3 or kahip.__version__ != "3.25":
    raise SystemExit("requires kahip Python module version 3.25")
with open(sys.argv[1], encoding="utf-8") as f: a = json.load(f)
c = a["config"]
if c["imbalance"] != 0.05 or c["partitions"] < 1:
    raise SystemExit("KaHIP request identity/configuration mismatch")
n = len(a["ids"])
if n == 0 or n > 1_000_000 or len(a["graph"]["neighbors"]) != n:
    raise SystemExit("invalid graph")
rows=[set() for _ in range(n)]
directed=0
for u, ns in enumerate(a["graph"]["neighbors"]):
    directed += len(ns)
    # V1's canonical offline envelope is one million vectors at degree 16.
    # Do not accept the broader graph-builder reservation (64M edges).
    if directed > 16_000_000: raise SystemExit("selected KaHIP directed-edge envelope exceeded")
    for v in ns:
        if not isinstance(v, int) or v < 0 or v >= n or v == u: raise SystemExit("invalid graph edge")
        rows[u].add(v); rows[v].add(u)
xadj=[0]; adjncy=[]
for ns in rows:
    ns=sorted(ns)
    adjncy.extend(ns); xadj.append(len(adjncy))
cut, assignment = kahip.kaffpa([1]*n, xadj, [1]*len(adjncy), adjncy, c["partitions"], c["imbalance"], False, c["seed"], kahip.ECO)
if len(assignment) != n or any(not isinstance(p,int) or p < 0 or p >= c["partitions"] for p in assignment): raise SystemExit("invalid KaHIP assignment")
loads=[0]*c["partitions"]
for p in assignment: loads[p]+=1
if max(loads) > a["metrics"]["cap"] or min(loads) < 1: raise SystemExit("KaHIP assignment violates capacity")
directed_cut=sum(assignment[u]!=assignment[v] for u,ns in enumerate(a["graph"]["neighbors"]) for v in ns)
a["assignment"]=assignment
a["backend"]="kahip_python_3.25_eco_symmetrized_v1_seed_%d" % c["seed"]
a["backend_license"]="MIT; Python module kahip==3.25; ECO; epsilon=0.05; symmetrized_unweighted_v1"
a["metrics"]["edge_cut"]=directed_cut
a["metrics"]["max_partition_size"]=max(loads)
with open(sys.argv[2],"w",encoding="utf-8") as f: json.dump(a,f,separators=(",",":"))
