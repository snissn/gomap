# #4092 immutable serving snapshot and no-log proof evidence

This 348 KiB packet is the focused exact-head evidence for #4092. It proves
that TreeDB can validate catalog authority without appending a Raft entry and
that pinning an already-published immutable serving snapshot is a sub-microsecond
operation. It does not rerun or replace the frozen five-system gold matrix.

The 4.3 GiB scratch root containing copied databases, binaries, raw pprof files,
and traces remains outside the repository at
`/mnt/fast4tb/gomap-4092-serving-snapshot-ce5f0088`. The two raw SHA inventories
bind those omitted files.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `1ddf7bfef0ba374ace10460ac2072da9c53b56df` |
| Instrumented head | `ce5f008855ef0a285cf8e69d5cba7d9b6b4bf56a` |
| Benchmark binary SHA-256 | `f2fdd0ab69e2362df14c470a0d3a86485dec12fd29a1b01bff26477cd6e07cf0` |
| M3 artifact SHA-256 | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| M3 descriptor SHA-256 | `c43fdc9c6901203daa5747124f74c845c49f4fb3f0ca731e5c919629cfb1e1f8` |
| Fixture checksum | `d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69` |
| Truth artifact SHA-256 | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |
| Toolchain | `go1.26.0 linux/amd64`, `CGO_ENABLED=0`, `vcs.modified=false` |

The host was Linux 6.8.0-124-generic on one Intel Core i5-11400F socket with
six physical cores, 12 logical CPUs, one NUMA node, and 31 GiB RAM. The exact
250k graph-overlap M3 rebuild completed once with exit 0 in 13:38.08 and peak
RSS 2,257,996 KiB.

## Focused result

One single-daemon production-public row measured p2, EF128, c256, top-k 10,
1,000 truth-bound queries at c1 and c32. Result IDs, recall, selected groups,
selected partitions, candidates, edges, retries, redirects, errors, timeouts,
generation, fixture, truth, and topology all remained unchanged.

| Cell | #4090 before | #4092 current | QPS gain | p95 before/current |
| --- | ---: | ---: | ---: | ---: |
| c1 | 38.75 QPS | 523.00 QPS | 13.50x | 43.70 / 3.97 ms |
| c32 | 163.31 QPS | 2,915.97 QPS | 17.86x | 242.39 / 17.25 ms |

Each cell performed 3,834 catalog validations. All 3,834 were no-log proofs;
`LogBarrier` count was zero and the catalog Raft log index stayed 10 before and
after each cell. At c1, summed catalog work fell from 31.363 ms/query to 0.135
ms/query. At c32 it fell from 235.796 ms/query to 6.649 ms/query of concurrent
aggregate work.

These are one-shot diagnostic values, not promotion medians. Against the frozen
250k rubric, the current row is already 1.29x/2.20x Milvus QPS at c1/c32 and
1.78x/1.83x pgvector QPS, while retaining TreeDB recall 0.9247. The comparison
is directional because the external systems were not rerun at this head.

## Snapshot pin substrate

The one-second microbenchmark ran three times:

| Operation | Median | Allocation |
| --- | ---: | ---: |
| Published snapshot pin | 402.8 ns/op | 48 B/op, 1 alloc/op |
| Fresh no-log authority capture | 17.179 us/op | 7,263 B/op, 95 allocs/op |

Pinning is 97.66% lower in time and 99.34% lower in allocated bytes than fresh
authority capture. Publication, replacement, invalidation, old-pin drain,
proof renewal, expiry, leadership transition, partial-build cleanup, and race
behavior are covered by the focused normal and race suites.

## Remaining boundary

#4092 deliberately builds the authority and publication substrate. The ordinary
strict request still invokes fresh no-log authority and generation paths; #4096
owns switching that request to exactly one immutable snapshot pin. Therefore
this packet does not claim one pin/search or zero request-side reconstruction
yet.

The retained CPU and allocation profiles confirm the residual target. Startup
and request work still reaches lifecycle checkpoint reduction, manifest JSON
encoding/integrity, generation pin/open, and physical row-reader preparation.
Those paths dominate the profile while local HNSW remains small. This is the
specific work #4096 can remove without changing routing, index shape, network
topology, recall, or fault semantics.

## Reproduction

`tools/run_profile_treedb.py` is the exact bounded runner. It validates source,
binary, M3 descriptor, fixture, truth, topology, readiness, and absence of a
competing heavy process; reflink copies are rebound with the included existing
`RebindDurableRootSnapshotV1` helper. It runs only the focused single-daemon
c1/c32 row or one c1/c32 profile capture.

`focused/search.json` is the complete normal result. `profiles/*/search.json`
bind the raw profile captures. `profile-summaries/` contains derived CPU,
allocation, block, mutex, and c32 trace summaries. `comparison.json` is the
compact machine-readable comparison and explicit claim boundary.

Validate the committed packet with:

```sh
(cd TreeDB/docs/evidence/vector-partition-serving-snapshot-4092/ce5f0088 && sha256sum -c SHA256SUMS)
```

On the evidence host, validate the omitted raw artifacts with:

```sh
(cd /mnt/fast4tb/gomap-4092-serving-snapshot-ce5f0088 && sha256sum -c /mnt/fast4tb/gomap-4092-serving-snapshot/TreeDB/docs/evidence/vector-partition-serving-snapshot-4092/ce5f0088/RAW_EXTERNAL_SHA256SUMS)
(cd /mnt/fast4tb/gomap-4092-serving-snapshot-ce5f0088 && sha256sum -c /mnt/fast4tb/gomap-4092-serving-snapshot/TreeDB/docs/evidence/vector-partition-serving-snapshot-4092/ce5f0088/RAW_PROFILE_SHA256SUMS)
```
