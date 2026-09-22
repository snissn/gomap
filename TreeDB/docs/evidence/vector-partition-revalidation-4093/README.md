# Vector partition hot-path revalidation (#4093)

Status: **qualified** at source head
`6e406e43ec2a6228e84d672a488768e32e6f0b53`.

This is the focused TreeDB revalidation authorized after the #4090–#4098
hot-path overhaul. It does not regenerate the frozen five-system comparison
matrix. The frozen 250k Milvus and pgvector rows remain the competitive rubric:
404.4 QPS / 3.0 ms at c1 and 1,595.3 QPS / 44 ms at c32 for Milvus, with the
pgvector c32 throughput ceiling of 1,595.3 QPS.

## Frozen shape

- unchanged 250k graph-overlap corpus and exact truth;
- p2, EF128, router candidates 256, top-k 10, four groups;
- single daemon, native four daemon, and container four daemon;
- strict, bounded-fast, and pinned-session search independently;
- c1 and c32, 1,000 measured queries per cell;
- three serialized, order-balanced repetitions per topology.

All 54 cells completed 1,000/1,000 queries with recall `0.9359`, zero query
errors/timeouts/retries/redirects, and identical selected partitions, groups,
RPC count, candidates, edges, query bytes, and response bytes. Strict search
retained exactly one catalog proof per query. Fast and pinned search retained
zero per-query catalog or data-group consensus reads; pinned search retained
one session pin per worker and zero per-query snapshot pins.

## Median matched-recall results

| Search | Topology | c1 QPS / p95 | c32 QPS / p95 |
|---|---|---:|---:|
| strict | single | 732.6 / 1.826 ms | 3,471.1 / 15.433 ms |
| strict | native 4-daemon | 767.3 / 1.571 ms | 2,372.6 / 22.348 ms |
| strict | container 4-daemon | 796.0 / 1.555 ms | 2,410.9 / 21.457 ms |
| bounded fast | single | 772.4 / 1.518 ms | 3,479.1 / 16.743 ms |
| bounded fast | native 4-daemon | 794.2 / 1.562 ms | 2,505.4 / 23.160 ms |
| bounded fast | container 4-daemon | 819.7 / 1.476 ms | 2,379.0 / 23.525 ms |
| pinned session | single | 782.1 / 1.577 ms | 3,407.4 / 17.576 ms |
| pinned session | native 4-daemon | 778.2 / 1.632 ms | 2,272.7 / 25.415 ms |
| pinned session | container 4-daemon | 828.6 / 1.427 ms | 2,523.7 / 23.462 ms |

Every median clears the predeclared focused floors: at least 404.4 QPS and at
most 5 ms p95 at c1; at least 1,595.3 QPS and at most 50 ms p95 at c32. The
strict single/container controls also improve by more than the allowed 3%
regression envelope against the pre-overhaul retained baseline.

Pinned c1 native/container p95 medians differ by 14.4% (1.632 vs 1.427 ms),
while QPS differs by 6.1%, mean client time differs by 6.5%, and the retained
three-repetition p95 ranges overlap. The reducer records that bounded tail
variance as an attributed exception; both cells remain well inside the 5 ms
absolute gate. No repetition was discarded or substituted.

## Where the wall time remains

Strict c1 median timings below are microseconds/query. They are nested timing
scopes, not additive columns.

| Topology | client total | encode + write + decode | public + service adapters | RPC | network | generation open | shard search | merge |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| single | 1,362 | 24 | 63 | 1,085 | 385 | 59 | 628 | 3.7 |
| native | 1,300 | 22 | 49 | 1,057 | 377 | 48 | 619 | 3.5 |
| container | 1,253 | 19 | 46 | 1,018 | 350 | 46 | 608 | 3.5 |

The removable encode/write/decode/public/service on-ramp is 6.4% of strict c1
for single, 5.5% for native, and 5.1% for container; fast and pinned are
2.4–3.1%. No individual removable stage reaches 5%. The remaining distributed
c32 gap versus single is therefore a real RPC/network/scheduling topology tax,
not a hidden serialization, allocation, lifecycle reconstruction, or
per-query consensus tax. Physical `request_bytes` are retained separately:
distributed requests carry 12,838 additional bytes across 1,834 RPCs because
of topology-specific wire identity, while semantic work remains exact-equal.

## Evidence

- [`summary.json`](summary.json) is the fail-closed, compact result and binds
  every raw search result, process record, topology, readiness record, and
  first-repetition profile by SHA-256.
- [`provenance.json`](provenance.json) binds the exact source, executable,
  container image, M3 descriptor, fixture, truth, and capability key.
- [`tools/run_revalidation.py`](tools/run_revalidation.py) owns the predeclared
  serialized schedule; [`tools/reduce_revalidation.py`](tools/reduce_revalidation.py)
  enforces the qualification contract.
- Raw evidence remains at
  `/mnt/fast4tb/gomap-4093-vector-revalidation-evidence-6e406e43e` (1.4 GB).
  It is deliberately not committed.

The full five-system matrix remains frozen pending explicit owner approval.
