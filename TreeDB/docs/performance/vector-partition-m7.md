# Vector partition M7 lifecycle microbenchmarks

Date: 2026-07-25

Host: Linux/amd64, 11th Gen Intel Core i5-11400F @ 2.60GHz, 12 logical CPUs
Scope: bounded in-process lifecycle primitives only; M7 remains experimental.

## Reproduction

```sh
GOWORK=off go test ./TreeDB/internal/raftplacement \
  -run '^$' \
  -bench '^BenchmarkVectorPartitionLifecycle' \
  -benchtime=10000x \
  -count=3 \
  -benchmem
```

The benchmark uses one bounded group and the smallest valid lifecycle identity.
It is not a network, disk, Raft quorum, builder, or upload measurement.

## Result

| Primitive | ns/op range | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| canonical begin-build command encode | 1,974-2,085 | 1,401-1,405 | 7 | deterministic wire encoding |
| pure begin-build record apply | 15,628-15,810 | 6,416-6,417 | 49 | validation plus canonical record construction; no catalog lock or Raft submit |
| one-group ready-set digest verification | 2,413-2,476 | 1,201 | 11 | canonical identity/group/readiness digest |
| two-generation mutation-proof selection | 131.0-132.4 | 0 | 0 | pending invalidation preferred over prepared candidate |

The test uses fixed iterations rather than a duration target, so these ranges
are reproducible microbenchmark samples rather than a throughput acceptance
claim. The encoded command and snapshot paths remain bounded by
`MaxVectorPartitionLifecycleCommandBytesV1` (1 MiB),
`MaxVectorPartitionLifecycleRecordBytesV1` (1 MiB), and
`MaxCatalogMetaSnapshotBytesV1` (8 MiB); catalog status reports retained wire
bytes for records, while `VectorPartitionLifecycleMutationFencesV1` and
`VectorPartitionCollectionMutationBarriersV1` expose the bounded pending-fence
operator state.

## Source-capture and mutation ordering

The build coordinator reads `BuildSourceMutationEpochV1` before it captures
group read/apply proofs, then supplies that epoch to `BeginBuildV1`. A relevant
nativewire mutation first publishes a collection-keyed barrier through the
catalog/meta Raft owner. The barrier rejects a build already in source capture,
and begin-build rejects an epoch that the mutation advanced after capture.
Once begin-build commits, later distinct mutations remain blocked through
build, stage, and prepare. The mutation barrier is keyed by the deterministic
command/idempotency identity, survives snapshot/restore, rejects distinct
concurrent mutations, and retains the 64 most recent completed identities per
collection as a bounded durable exact-replay window. Older identities rely on
the data Raft layer's durable idempotency result and may conservatively reopen
catalog admission before that result is returned. The barrier is released only
after the data bridge proves commit plus deterministic local apply, independent
of the requested response acknowledgment, and all per-index invalidation
confirmations.

Post-commit confirmation uses a bounded internal context detached from client
cancellation and deadlines. Search admission likewise cannot consult the raw
replica-local catalog cache: the serving adapter performs a quorum-verified
meta-Raft leader fence and requires the local catalog applied index to have
caught up before validating the active generation. Followers without a routed
proof and replicas behind that fence fail closed.

## Explicitly unavailable phases

M7 has no production generation-builder orchestration, remote asset upload
pipeline, or multi-group source-proof capture harness in this repository.
Therefore there is no honest measurement for build, stage/upload, source-proof
capture duration, catalog-meta Raft quorum submit, or multi-node
failover/rejoin latency.
Those phases are **not implemented/measured**, and this document must not be
read as production readiness evidence. The implemented fail-closed lifecycle
seams are command encoding/reduction, catalog-authority fencing, nativewire and
Mongo mutation admission/confirmation, snapshot restoration, and cleanup
guards.
