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
| Topology transition boundary | Table-driven owner/member/mode/partition/removal refusals, permitted metadata/addition cases, live forward-snapshot refusal, and real-cluster failover | Existing topology is immutable until an explicit migration workflow; a refused epoch-valid owner move does not publish or flip routes, while leader hints, compatible feature metadata, and additive entries can advance. |
| Committed authority | `CatalogMetaRaftProviderV1` has the only usable apply/restore capabilities | Local files, static route adapters, and follower-local calls cannot activate ownership. |
| Three fixed peers | Three real `CatalogMetaAuthorityV1` instances over HashiCorp in-memory transports and durable stores exercise follower refusal, leader loss, new-leader commit, convergence, rejoin, snapshot, and reopen | Every authority preserves exact identity, feature floors, route decisions, retry identity, and monotonic generations. |
| Availability and cancellation | Isolated voter, pre-enqueue cancellation, and blocked post-enqueue apply | No quorum is unavailable; only a post-enqueue cancellation is reported as commit-ambiguous. |
| Snapshot and backup archive | A leader exports a bounded checksummed HashiCorp snapshot; a fresh cluster restores it through HashiCorp Raft `Restore` | Restore propagates to all three authorities and survives reopen/failover/rejoin; follower, corrupt, and live rollback attempts fail closed. |
| Voter capabilities | Every fixed peer must declare `FeatureCatalogMetaAuthority` at the supported floor | Unsupported voters fail provider open before bootstrap/reopen and before local apply or route admission. |
| Owner admission | The replicated dispatcher re-resolves complete request metadata before registry lookup | Stale/missing proof and all route identity mismatches fail before owner mutation. |
| Nativewire and Mongo | Real single-node meta Raft, dynamic route providers, shared submit, mutation, and routed-read proof matrices | Adapters carry the locally applied proof and reject stale/missing proof before local success. |
| Crash/model safety | 64 deterministic seeds by 64 replay, apply, snapshot, and restart steps plus concurrent readers | Versions are monotonic and readers observe only complete generations. |

`nativewire.CatalogRouteResolverV1` is a read-only bootstrap/inspection helper.
Its method is not `ClusterRoute`, so it cannot satisfy the production
`ClusterRouteProvider` interface. The only exported routed-dispatcher
constructor is `NewCatalogMetaGroupRoutedSubmitter`, which requires complete
route revalidation and has no nil-validator or proof-only mode. Permissive
static route/provider adapters are confined to `_test.go` files. The legacy
Mongo benchmark scaffold has no replicated catalog authority and therefore
uses an always-fail-closed validator.

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
GOWORK=off go test ./cmd/mongo_gateway_bench ./TreeDB/docs -count=1
```

The focused command passed in all five packages. Full non-race and full race
runs of the same five affected packages also passed. The required focused race
run passed and `go vet` reported no findings. The docs and Mongo benchmark
command packages passed their tests. The 10-second decoder fuzz run completed
145,934 executions without a failure.

## Base/head steady-state comparison

The actual issue base is `a2d7bd558`, not the later feature merge
`7590f5c2f`. The base has no catalog-meta implementation or catalog benchmark
harness, so no catalog row is presented as a base/head comparison. The
identical pre-existing read-coordinator and concrete Raft-bridge benchmarks
were instead run in detached base and implementation-head (`b446c7091`)
worktrees:

```sh
GOWORK=off go test ./TreeDB/internal/raftcluster -run '^$' \
  -bench '^BenchmarkInternalGroupRoutedReadIndexCoordinatorScaffold$' \
  -benchmem -benchtime=2s -count=10
GOWORK=off go test ./TreeDB/nativewire -run '^$' \
  -bench '^BenchmarkRaftClusterSubmitterConcreteBridgeUpdateBSONSet$' \
  -benchmem -count=10
```

Linux/amd64, Intel i5-11400F:

| Identical pre-existing benchmark | Base | Head | Allocation result | Statistical result |
| --- | ---: | ---: | ---: | --- |
| Internal routed read-index scaffold | 55.59 ns/op | 56.04 ns/op | 0 B/op, 0 allocs/op at both | +0.81%, `p=0.000`, `n=10` |
| Concrete Raft bridge UpdateBSONSet | 9.546 ms/op | 10.264 ms/op | 244.7/244.6 KiB, 923.0/923.5 allocs; no significant difference | time `p=0.165`, bytes `p=0.436`, allocs `p=0.162`, `n=10` |

The read scaffold source is byte-for-byte unchanged between these commits; the
measured sub-nanosecond shift is reported but is not attributed to catalog-meta
code. The bridge timing and allocation distributions show no significant
difference. Its custom `ops_total` metric is the benchmark calibration
iteration count (`b.N`), not an operation outcome, so it is not used as a
performance result. These are local microbenchmarks, not production-scale
evidence.

## Implementation-only operation measurements

All catalog rows are explicitly implementation-only. Ten-sample medians at
`b446c7091` were captured with:

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement ./TreeDB/nativewire \
  ./TreeDB/mongo_gateway -run '^$' \
  -bench 'BenchmarkCatalogMeta(Route|Decode|NativewireAdmission|MongoMutationAdmission)$' \
  -benchmem -count=10
```

| Operation | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Catalog route only | 251.4 ns | 32 B | 1 |
| Catalog command decode only | 77.67 us | 40.67 KiB | 1,052 |
| Guarded owner dispatcher | 480.5 ns | 104 B | 4 |
| Nativewire admission + route + guarded dispatch | 1.185 us | 264 B | 9 |
| Nativewire routed-read admission | 466.5 ns | 96 B | 3 |
| Mongo wire mutation admission + route + guarded dispatch | 43.20 us | 45.62 KiB | 356 |

The route and decode benchmark names describe exactly what their timed bodies
do. Nativewire and Mongo setup uses a real committed
`CatalogMetaAuthorityV1`; the timed owner is a no-op/recording data-group
submitter, so these rows measure adapter and admission overhead, not data
storage or network consensus.

## Maximum-shape directional measurements

The maximum-shape matrix is also implementation-only and uses one iteration:

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement -run '^$' \
  -bench 'BenchmarkCatalogMeta(StatusRouteAdmissionMatrix|EncodeDecodeApplyMatrix|SnapshotArchiveInstallStatusRoute)$' \
  -benchmem -benchtime=1x -count=1
```

| Shape / operation | Time | Bytes/op | Allocs/op | Capacity metric |
| --- | ---: | ---: | ---: | ---: |
| small status | 2.944 us | 0 B | 0 | 1,329 retained wire B |
| small route | 36.833 us | 32 B | 1 | 1,329 retained wire B |
| small owner admission | 29.316 us | 32 B | 1 | 1,329 retained wire B |
| maximum status | 23.790 us | 24 B | 1 | 1,278,713 retained wire B |
| maximum route | 27.555 us | 48 B | 1 | 1,278,713 retained wire B |
| maximum owner admission | 16.207 us | 48 B | 1 | 1,278,713 retained wire B |
| small encode, 685-byte command | 166.429 us | 23,160 B | 516 | 685 command B |
| small decode, 685-byte command | 285.332 us | 45,496 B | 1,063 | 685 command B |
| small fresh apply, 685-byte command | 345.084 us | 71,760 B | 1,583 | 685 command B |
| maximum encode, 639,377-byte command | 64.348 ms | 16,513,104 B | 414,100 | 639,377 command B |
| maximum decode, 639,377-byte command | 195.941 ms | 34,479,880 B | 848,705 | 639,377 command B |
| maximum fresh apply, 639,377-byte command | 367.260 ms | 58,098,768 B | 1,262,871 | 639,377 command B |
| small snapshot export, 1,836-byte snapshot | 73.824 us | 15,880 B | 73 | 1,836 snapshot B |
| small snapshot install | 529.548 us | 131,248 B | 2,641 | 1,836 snapshot B |
| small snapshot install/status/route | 669.988 us | 126,112 B | 2,635 | 1,836 snapshot B |
| maximum snapshot export, 1,705,012-byte snapshot | 16.980 ms | 11,126,168 B | 82 | 1,705,012 snapshot B |
| maximum snapshot install | 330.792 ms | 106,442,112 B | 2,111,579 | 1,705,012 snapshot B |
| maximum snapshot install/status/route | 229.307 ms | 104,342,784 B | 2,111,551 | 1,705,012 snapshot B |

The "maximum" fixture exercises the declared 4,096-placement catalog limit.
One-iteration timings are directional capacity evidence, not
confidence-interval performance claims. The real Hashicorp backup/restore path
and persisted provider reopen are correctness-tested rather than presented as
microbenchmarks; the snapshot install/status/route rows do not measure provider
restart.
