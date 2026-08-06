# #4019 M2 TreeDB topology-tax baseline

This directory retains the first bounded, trustworthy comparison of the
production public TreeDB vector route in the single-daemon/four-group and
native four-daemon/four-group topologies. It is an M2 baseline, not the full
five-row M3 qualification and not multi-host evidence.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `3773f82ec2da793ca79f71bb4a442ce8bbcd1317` |
| Measured head | `95c60cbef0b0cb824a74a29e9304784e76745d9d` |
| Benchmark binary SHA-256 | `b8d12f98778698ed74db4a905e2bb6b2925840702664beb6fab9e402c4f913d1` |
| M3 artifact SHA-256 | `3916da3febc7c5a1ecad39488ee259e63103eda1dc6b27231a530e2169e9808b` |
| Toolchain | `go1.26.0 linux/amd64`, `vcs.modified=false` |
| Fixture checksum | `ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95` |
| Truth artifact SHA-256 | `0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c` |

The host was Linux 6.8.0-124-generic on one Intel Core i5-11400F socket with
six physical cores, 12 logical CPUs, one NUMA node, and 31 GiB RAM. Evidence
and copied database roots were on the same `/mnt/fast4tb` NVMe filesystem.

The exact-head M3 graph-overlap asset uses 100,000 vectors, 16 partitions,
overlap 0.20, source/router caps 100,000/120,000, and seed 4017. It binds the
pinned KaHIP Python and adapter digests in `m3/vector_partition_variant_v1.json`.
Its descriptor accurately records that this build exhausted its 20,000
memberships as deterministic filler (zero useful replicas), so both edge-cut
counters are zero; this M2 topology-tax baseline makes no graph-cut-benefit
claim.

## Method

Each topology ran three serialized repetitions. Every repetition started fresh
production nodes from an ordinary copied M3 snapshot, waited for checked
readiness, and exercised only the bounded TCP `OperationsV1.Search` route.
The existing `RebindDurableRootSnapshotV1` API rebound each copied snapshot to
its new durable root before opening it; the retained helper does not bypass
normal open or identity validation. The matrix was p2/p16 by c1/c8, 1,000
queries per cell, top-k 10, EF128, and 1,000 warmup queries per cell. The
reducer requires every cell to complete, recall@10 at least 0.90, exact truth
and generation identity, coherent raw durations, exact percentile/QPS
reconstruction, positive stage timing, and zero errors/timeouts.
The topology config's `group_applied_indexes` are readiness floors. Each
`ready.json` records the later applied index observed after startup, so those
values are expected to be equal to or greater than the config floors.

The initial attempt against the older accepted #4027 M3 asset correctly failed
new resource-identity validation after #4014. The asset was rebuilt once at the
measured head. A subsequent copy attempt correctly failed because copying
changes the durable root identity; the final runner used the explicit supported
rebind API. Those startup failures remain preserved in the full local evidence
root and were not included in the measured repetitions.

## Results

Every cell completed 1,000/1,000 queries with zero errors, timeouts, retries,
or redirects. Recall and work counters were identical across topologies:
p2 recall was 0.9810 with 2 selected partitions, 1.836 selected groups/RPCs,
3,805.903 candidates, and 8,402.652 HNSW edges per query; p16 recall was
0.9829 with 16 partitions, 4 groups/RPCs, 28,409.473 candidates, and
67,624.979 edges per query.

| Cell | Single median QPS | Native median QPS | Native / single | Single p95 ms | Native p95 ms | Native / single |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| p2 c1 | 43.195 | 32.572 | 0.754 | 31.537 | 50.783 | 1.610 |
| p2 c8 | 100.801 | 71.719 | 0.711 | 107.234 | 147.990 | 1.380 |
| p16 c1 | 31.772 | 29.489 | 0.928 | 49.317 | 41.976 | 0.851 |
| p16 c8 | 74.018 | 47.676 | 0.644 | 191.779 | 243.395 | 1.269 |

`topology-tax.json` retains the minimum and maximum beside every recall, QPS,
and p95 median; the table stays median-only for readability. Per-shard stage
timings such as RPC, generation-open, and shard search are summed across the
selected shards for each query, while `total` is coordinator wall time.
Consequently, summed stage work may exceed `total` when shards run concurrently.

The topology tax is material at c8 and is not hidden as parity. The retained
stage evidence attributes most of the delta to generation-open/RPC work. For
p2 c1, median generation-open time was 15.258 ms/query single versus 21.561
ms/query native; RPC was 16.367 versus 22.688 ms/query. At p16 c8 those pairs
were 141.827 versus 184.612 ms/query and 150.679 versus 191.026 ms/query.
Network and shard-HNSW timing did not explain the gap. This is a scoped baseline
for later regressions and M3 interpretation, not a release threshold.

## Evidence and reproduction

- `topology-tax.json` is the fail-closed reduction; `runs/` retains all six raw
  matrices, checked topology identities, ready artifacts, exact commands, exit
  status, and resource timings. The reduction records the clean source revision
  and benchmark executable SHA256, validates each ready artifact against that
  identity and its retained node config/roots, and binds each ready SHA256. It
  also validates and binds each exact `/usr/bin/time` client command, successful
  exit record, and timing wrapper against the attested executable path.
- `inputs/` retains the fixture manifest and canonical truth cache.
- `m3/` retains the exact build command/time, build report, and schema-v5
  descriptor. `build/` retains the binary SHA and `go version -m` output.
- `run_m2.py` and `tools/rebind_snapshot/` retain the external runner and
snapshot-helper sources. The committed runner includes review-only preflight
and failure-cleanup hardening made after measurement; it does not change the
commands or retained results. The current `system-bench` also fail-closes before
fixture work unless every shard endpoint reports the node-config digest already
retained in `topology.json`; the measured hot loop is unchanged. It writes scratch results beneath
  `verified-runs/`; publication copied those files byte-for-byte to the
  committed `runs/` paths bound by `SHA256SUMS`.
- `tools/rebind_snapshot` is built only inside the scratch root, where its
  `../../source` replacement resolves to the ordinary checkout staged by the
  runner. It requires the same Go 1.26 driver recorded in `build/`.
- `SHA256SUMS` binds every retained file except itself.

Validate the committed evidence from the repository root:

```sh
(cd TreeDB/docs/evidence/vector-partition-local-system-qualification-4019/m2-95c60cbe && sha256sum -c SHA256SUMS)
(cd benchmarks/vector_db_compare && python3 -m unittest test_topology_tax.py)
```

The full local bundle, including copied database roots and the two rejected
startup attempts, remains at
`/mnt/fast4tb/gomap-4019-m2-topology-tax-evidence-95c60cbe`. The committed
directory is sufficient to re-run the reducer and inspect all claimed raw
measurements, but it is not a self-contained server replay bundle because the
large copied TreeDB roots and benchmark executable are omitted.

## Limitations

- One Linux host and one deterministic 100k synthetic corpus.
- Same-host TCP only; no bridge/container or multi-host claim.
- M2 covers two TreeDB topologies and p2/p16 at c1/c8. The committed M3 plan
  still owns all five comparator rows, full budget/concurrency sweeps, resource
  envelopes, and external relative ranking.
