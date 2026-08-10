# #4091 runtime ownership evidence

This compact packet is the exact-head evidence for #4091. It proves that four
same-host native daemons with explicit, disjoint CPU and Go-runtime ownership
match the equivalently bounded four-container control within 10% at c1 and
c32. It does not rerun or replace the frozen five-system gold matrix.

The 29 GiB logical scratch root containing reflinked databases, binaries, logs,
and raw run artifacts remains outside git at
`/mnt/fast4tb/gomap-4091-runtime-ownership-evidence-678d54b3`.
`RAW_EXTERNAL_SHA256SUMS` binds the omitted inputs and results; this committed
packet is intentionally only tens of kilobytes.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `4afe78e8090d3d35b7cebbff857e1ef2a8538ff1` |
| Instrumented head | `678d54b3152099a730acb2a691ffff8329a67a5d` |
| Benchmark binary SHA-256 | `c83e6ca2496d7fdc8b642193512df08727751312a536ff81176f3dff5f812e6b` |
| Container image | `sha256:2163f2723a85f3c31210b1e7fac8a4d3b0f7690577eb52ca826908cfdcf87537` |
| M3 artifact SHA-256 | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| M3 descriptor SHA-256 | `0e268e571f91c5a23d3f0da1ed5b40893af4bf1ffa42e639751afaaf05de68ec` |
| Fixture checksum | `d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69` |
| Truth artifact SHA-256 | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |

The exact 250k graph-overlap M3 rebuild passed once in 15:18.60 with peak RSS
2,231,636 KiB. All nine search rows were then run serially on one 12-logical-
CPU host: single, native four-daemon, and container four-daemon, each repeated
three times. The four-daemon rows assigned `0-2`, `3-5`, `6-8`, and `9-11`,
with `GOMAXPROCS=3` and a 6 GiB Go memory limit per daemon.

## Qualified matched-recall result

Every cell completed 1,000/1,000 queries with recall 0.9247, generation 1,
zero errors, zero timeouts, zero retries, and zero redirects.

| Topology | c1 QPS / p95 | c32 QPS / p95 |
| --- | ---: | ---: |
| Single daemon | 668.69 / 1.84 ms | 3,043.81 / 16.30 ms |
| Native four-daemon | 533.94 / 2.63 ms | 1,714.93 / 27.73 ms |
| Container four-daemon | 553.90 / 2.55 ms | 1,615.48 / 30.21 ms |

The native/container median ratios are:

| Cell | Native/container QPS | Native/container p95 | Gate |
| --- | ---: | ---: | --- |
| c1 | 0.9640 (-3.60%) | 1.0292 (+2.92%) | PASS |
| c32 | 1.0616 (+6.16%) | 0.9179 (-8.21%) | PASS |

The reducer requires every ratio to stay in `[0.9, 1.1]`. Because both cells
pass, the predeclared conditional profile rerun was not needed.

Selected partitions, selected groups, requests, RPCs, candidates, edges,
query bytes, service request bytes, candidate bytes, service response bytes,
generation, recall, and all proof counts are exact across all nine rows. Each
cell performed 3,834 successful no-log catalog proofs and zero log barriers.
The outer public frame totals differ by at most 0.3% because they include
topology-specific request metadata; the canonical service payload bytes are
identical.

## Runtime attribution

Native and container work is also closely matched. At c1, aggregate CPU time
is 2.867 s native versus 2.793 s container and allocated bytes differ by
0.03%. At c32, aggregate CPU time is 3.051 s versus 2.965 s and allocated bytes
differ by 0.02%. Context-switch, GC-count, heap, RSS, and peak-RSS evidence is
retained in `comparison.json`. Aggregate four-process peak RSS is about
3.78 GiB in both shapes (roughly 945 MiB per daemon); the single process is
about 959 MiB.

The remaining distributed tax is therefore not a native/container ownership
mismatch. Native retains 79.85% of single QPS at c1 and 56.34% at c32, with
p95 ratios of 1.42x and 1.70x. Container shows the same class of residual.
That shared residual remains owned by the already-open downstream graph:
#4096 removes per-request strict proof reconstruction, #4098 adds pinned fast
sessions, and #4097 removes avoidable wire allocation/copy work.

## Frozen rubric context

Against the unchanged 250k gold table, the current native row is directionally
24.1x/28.3x the old c1/c32 QPS and the container row is 13.8x/12.8x. The
current single row is also 1.28x/1.04x the focused #4092 snapshot row, so the
single control did not regress. These are graph-progress comparisons only:
Milvus, pgvector, and the old TreeDB rows were not rerun at this head.

## Validation and reproduction

`benchmarks/vector_db_compare/runtime_ownership.py` rejects missing or reused
topology identities, overlapping database roots, mismatched readiness or
runtime ownership, stale client commands, invalid raw latency/QPS, recall or
generation drift, changed logical work, changed proof counts, and parity or
control regressions. `comparison.json` is its machine-readable output.

Validate this committed packet with:

```sh
(cd TreeDB/docs/evidence/vector-partition-runtime-ownership-4091/678d54b3 && sha256sum -c SHA256SUMS)
(cd benchmarks/vector_db_compare && python3 -m unittest test_runtime_ownership.py test_topology_tax.py)
```

On the evidence host, validate and reduce the omitted raw artifacts with:

```sh
(cd /mnt/fast4tb/gomap-4091-runtime-ownership-evidence-678d54b3 && sha256sum -c /mnt/fast4tb/gomap-4091-runtime-ownership/TreeDB/docs/evidence/vector-partition-runtime-ownership-4091/678d54b3/RAW_EXTERNAL_SHA256SUMS)
python3 benchmarks/vector_db_compare/runtime_ownership.py \
  --root /mnt/fast4tb/gomap-4091-runtime-ownership-evidence-678d54b3 \
  --out /tmp/runtime-ownership-comparison.json
```

## Claim boundary

#4091 establishes production/operator CPU and Go-runtime ownership and closes
the native/container mismatch. It does not change topology, routing,
index/search, wire, or Raft proof semantics, and it does not claim that the
remaining single-to-four-daemon tax is solved.
