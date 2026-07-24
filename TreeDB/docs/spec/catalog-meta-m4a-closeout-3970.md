# Replicated Catalog/Meta M4A Closeout Evidence (#3970)

This evidence closes the generic catalog/meta authority required by
[issue #3970](https://github.com/snissn/gomap/issues/3970). It does not claim
live membership changes, rebalance/migration, cross-shard transactions,
vector-specific lifecycle state, or generic one-group versus N-group production
scale results.

## Contract and failure matrix

| Surface | Evidence | Conservative result |
| --- | --- | --- |
| Deterministic record/command | Canonical round trip, duplicate/unknown/truncated input, digest, identity, and bounded decoder tests | One epoch has one canonical record and digest; invalid input fails before publication. |
| Committed authority | `CatalogMetaRaftProviderV1` has the only usable apply/restore capabilities | Local files, static route adapters, and follower-local calls cannot activate ownership. |
| Three fixed peers | Real HashiCorp in-memory transports and stores exercise follower refusal, leader loss, new-leader commit, convergence, rejoin, snapshot, and reopen | The next generation is committed by the elected meta leader and replicas never move backward. |
| Availability and cancellation | Isolated voter, pre-enqueue cancellation, and blocked post-enqueue apply | No quorum is unavailable; only a post-enqueue cancellation is reported as commit-ambiguous. |
| Snapshot and backup archive | Canonical snapshot bytes include the applied index, record, and exact last command | Snapshot/reopen/rejoin and opaque backup round trips preserve epoch, digest, retry identity, placement, and feature floors without mixed publication. |
| Voter capabilities | Every fixed peer must declare `FeatureCatalogMetaAuthority` at the supported floor | Unsupported voters fail provider open before bootstrap/reopen and before local apply or route admission. |
| Owner admission | The replicated dispatcher re-resolves complete request metadata before registry lookup | Stale/missing proof and all route identity mismatches fail before owner mutation. |
| Nativewire and Mongo | Real single-node meta Raft, dynamic route providers, shared submit, mutation, and routed-read proof matrices | Adapters carry the locally applied proof and reject stale/missing proof before local success. |
| Crash/model safety | 64 deterministic seeds by 64 replay, apply, snapshot, and restart steps plus concurrent readers | Versions are monotonic and readers observe only complete generations. |

The static `nativewire.CatalogClusterRouteProvider` and legacy
`raftcluster.NewGroupRoutedSubmitter` remain bootstrap/test compatibility
adapters. The replicated path is
`NewCatalogMetaClusterRouteProvider` plus
`NewCatalogMetaGroupRoutedSubmitter`; the latter requires complete route
revalidation and has no nil-validator mode.

## Format and capacity accounting

The v1 command limit is 1 MiB and the snapshot limit is 3 MiB. Strings are
limited to 128 bytes and digests to 64 hexadecimal bytes. A catalog is limited
to 128 groups, 64 members per group, 64 required features, 4,096 collection
placements, and 16,384 aggregate token partitions. JSON nesting and all
objects, arrays, numeric tokens, string bytes, duplicate keys, and total bytes
are preflighted before typed decoding can allocate catalog slices.

`CatalogMetaStatusV1.RetainedWireBytes` reports the retained canonical record
plus exact command bytes. Lookup, proof validation, and status read the resolved
catalog held by the current generation and do not perform a meta-group network
round trip.

## Reproducible correctness gates

Run from the repository root:

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement ./TreeDB/internal/raftcluster ./TreeDB/internal/raftfsm ./TreeDB/nativewire ./TreeDB/mongo_gateway -run 'Test.*(CatalogMeta|CatalogVersion|FeatureFloor|StaleCatalog|CatalogSnapshot).*' -count=1
GOWORK=off go test -race ./TreeDB/internal/raftplacement ./TreeDB/internal/raftcluster ./TreeDB/nativewire -run 'Test.*(CatalogMeta|CatalogVersion|FeatureFloor|StaleCatalog).*' -count=1
GOWORK=off go test ./TreeDB/internal/raftplacement ./TreeDB/internal/raftcluster ./TreeDB/internal/raftfsm ./TreeDB/nativewire ./TreeDB/mongo_gateway -count=1
GOWORK=off go test -race ./TreeDB/internal/raftplacement ./TreeDB/internal/raftcluster ./TreeDB/internal/raftfsm ./TreeDB/nativewire ./TreeDB/mongo_gateway -count=1
GOWORK=off go vet ./TreeDB/internal/raftplacement ./TreeDB/internal/raftcluster ./TreeDB/internal/raftfsm ./TreeDB/nativewire ./TreeDB/mongo_gateway
GOWORK=off go test ./TreeDB/internal/raftplacement -run '^$' -fuzz '^FuzzDecodeCatalogMetaCommandV1$' -fuzztime=10s
```

The focused command passed in all five packages. Full non-race and full race
runs of the same five affected packages also passed. The required focused race
run passed, `go vet` reported no findings, and the 10-second decoder fuzz run
completed 291,404 executions without a failure.

## Base/head steady-state comparison

The identical comparison command was run at base `7590f5c2f` and implementation
head `c772dbe8e`:

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement \
  -run '^$' \
  -bench 'BenchmarkCatalogMeta(StatusAndRoute|EncodeDecode)$' \
  -benchmem -count=10
benchstat catalog-meta-base.txt catalog-meta-head.txt
```

Linux/amd64, Intel i5-11400F:

| Benchmark | Base | Head | Result |
| --- | ---: | ---: | --- |
| Status and route | 264.5 ns/op | 258.3 ns/op | no significant difference, `p=0.078`, `n=10` |
| Encode/decode | 77.23 us/op | 78.10 us/op | no significant difference, `p=0.063`, `n=10` |
| Status and route bytes | 32 B/op | 32 B/op | unchanged |
| Status and route allocations | 1 alloc/op | 1 alloc/op | unchanged |
| Encode/decode bytes | 40.67 KiB/op | 40.67 KiB/op | unchanged |
| Encode/decode allocations | 1,052 allocs/op | 1,052 allocs/op | unchanged |

No material regression was detected in the compatibility rows. These are local
microbenchmarks, not production scale evidence.

## New operation and maximum-shape measurements

The implementation-only matrix was captured at `c772dbe8e` with:

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement \
  -run '^$' \
  -bench 'BenchmarkCatalogMeta(StatusRouteAdmissionMatrix|EncodeDecodeApplyMatrix|SnapshotArchiveInstallWarmReopen)$' \
  -benchmem -benchtime=1x -count=1
```

| Shape / operation | Time | Bytes/op | Allocs/op | Capacity metric |
| --- | ---: | ---: | ---: | ---: |
| small status | 1.007 us | 0 B | 0 | 1,329 retained wire B |
| small route | 15.125 us | 32 B | 1 | 1,329 retained wire B |
| small owner admission | 11.759 us | 32 B | 1 | 1,329 retained wire B |
| maximum status | 6.729 us | 24 B | 1 | 1,278,713 retained wire B |
| maximum route | 10.071 us | 48 B | 1 | 1,278,713 retained wire B |
| maximum owner admission | 9.289 us | 48 B | 1 | 1,278,713 retained wire B |
| small encode, 685-byte command | 76.930 us | 23,160 B | 516 | 685 command B |
| small decode, 685-byte command | 129.602 us | 43,384 B | 1,054 | 685 command B |
| small fresh apply, 685-byte command | 151.469 us | 71,760 B | 1,583 | 685 command B |
| maximum encode, 639,377-byte command | 28.621 ms | 18,610,416 B | 414,123 | 639,377 command B |
| maximum decode, 639,377-byte command | 65.148 ms | 36,577,048 B | 848,729 | 639,377 command B |
| maximum fresh apply, 639,377-byte command | 115.655 ms | 55,999,240 B | 1,262,840 | 639,377 command B |
| small snapshot archive, 1,836-byte snapshot | 40.522 us | 15,880 B | 73 | 1,836 snapshot B |
| small snapshot install | 249.210 us | 131,248 B | 2,641 | 1,836 snapshot B |
| small warm reopen/status/route | 479.048 us | 131,280 B | 2,642 | 1,836 snapshot B |
| maximum snapshot archive, 1,705,012-byte snapshot | 10.121 ms | 11,126,152 B | 82 | 1,705,012 snapshot B |
| maximum snapshot install | 183.617 ms | 106,442,000 B | 2,111,578 | 1,705,012 snapshot B |
| maximum warm reopen/status/route | 179.071 ms | 104,342,768 B | 2,111,551 | 1,705,012 snapshot B |

The "maximum" fixture exercises the declared 4,096-placement catalog limit.
The new rows have no base equivalent because the replicated M4A operations did
not exist at the base commit. They use one iteration and are directional
capacity evidence, not confidence-interval performance claims.
