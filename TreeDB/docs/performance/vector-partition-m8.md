# Vector partition M8 production multi-group closeout

Date: 2026-07-25

Measured production-code head: `9f3cb7c6f8d5aa8283fe2342d9f341cbdbebab48` (the evidence-document commit is subsequent and is not the measured code).

Base M7 merge: `03a5508e5df33ceaf2920839a670fb16652f633d`

Status: experimental/off; north-star gates did not pass

## Executive result

M8 supplies a real production-shaped proof topology: distinct three-node
HashiCorp Raft data groups, a real three-node catalog-meta lifecycle group,
distributed leaders, production commit/read/apply proofs, serialized loopback
TCP M5 services, bounded M6 fanout, and M7 activation. The unavailable-endpoint
fault and the broader lifecycle/failure test matrix return no partial or stale
success. Placement balance passes. Persistent asset bytes and peak RSS were
observed, but no configured process-resource limit was compared, so resource
bounds remain measured/not-bounded rather than a pass.

M8 does **not** enable graph routing. Exact all-partition HNSW results did not
match the exhaustive collection oracle, the retained 1M query had zero
recall@10, and no lower-probe row reached the 0.90 recall target. Overlap `0.20`
and stable-hash attribution remain explicitly unsupported. Consequently recall,
probe reduction, matched-recall QPS, matched-recall tail latency, and overlap
storage gates are red or unavailable rather than inferred.

The linked measured blocker is per-request router reopen and full 1M-membership
manifest verification. The differential allocation profile measured about
67.8 GB allocated during the query/fault boundary; 52.7 GB (77.7%) is directly
attributed to `encoding/json.Marshal`, with manifest validation, lifecycle
reduction, memberships, and physical asset reads dominating the remainder.
CPU is led by SHA-256 (20.7%) and JSON struct encoding (9.1% flat, 40.8%
cumulative). This is an evidence-backed optimization owner, not an M8 passing
claim.

## Gate ledger

| Gate | Result | Evidence |
| --- | --- | --- |
| Exhaustive correctness | **FAIL** | every 1M all-partition cell failed exact ID/rank parity |
| Recall >= 0.90 | **FAIL** | retained 1M query recall@10 was 0 in every cell |
| Median probes <= 25% at target recall | **FAIL** | no row reached target recall |
| End-to-end QPS >= 1.15x exhaustive at matched recall | **FAIL** | no qualifying matched-recall row |
| p95 no worse than exhaustive at matched recall | **FAIL** | no qualifying matched-recall row |
| Balance epsilon 0.05 | **PASS** | max 63,292 rows; hard cap 65,625 |
| Overlap 0.20 bytes <= 1.35x | **FAIL** | overlap assets are not materialized and are reported unsupported |
| Failure honesty | **PASS** | unavailable endpoint returned error, zero neighbors, zero groups |
| Resource bounds | **MEASURED, NOT BOUNDED** | 188,536,448 persistent bytes and peak RSS 1,162,182,656 bytes were observed; no configured resource limit was compared, so this is not a pass |
| Existing behavior | **INCOMPLETE** | full normal suite and all M8-relevant race suites pass; full collections race command crashes in Go 1.26 runtime during unrelated `testing.AllocsPerRun` |

This ledger deliberately treats unsupported required evidence as failure. A
stage-only win or a configuration sweep without multiple queries does not
satisfy a matched-recall or concurrency gate.

## Evidence identities

### Checked-in 10k CI shape

| Field | Value |
| --- | --- |
| fixture / checksum | `deterministic_10000` / `22d9c6d2af058193ca5b4fa13df86e8ecf6214b9a62a6366098a4e4a720d87e8` |
| corpus / queries / dimensions | 10,000 / 128 / 16 |
| topology | 2 data groups x 3 nodes, plus 3-node catalog-meta group |
| partitions / probes / overlap | 4 / 1, 2, 4 / 0 measured; 0.20 unsupported |
| local `ef_search` / concurrency | 64, 128, 512, 4096 / 1, 16, 64 |
| rows | 36 measured plus one unsupported overlap row |
| persistent bytes / peak RSS | 1,752,856 / 100,642,816 |

The best all-partition row used `ef_search=4096`: recall@10 was
`0.96171875` at every concurrency, but exact ordered ID parity remained false.
At concurrency 1 it measured 27.34 QPS and 45.51 ms p95; concurrency 16
measured 51.81 QPS and 384.06 ms p95; concurrency 64 measured 52.57 QPS and
1,265.25 ms p95. The best quarter-probe recall was only `0.24140625`, so no
25%-probe row qualified for the matched-recall QPS/tail gates.

### Retained 1M deep shape

| Field | Value |
| --- | --- |
| fixture | `deterministic_1000000` |
| generator / arithmetic | `treedb_vector_partition_fixture_v2` / `ieee754_binary64_explicit_fma_v1` |
| checksum | `bf69a8ba38cf82da5632f3f0f47d0b768a149888f51d2c7d98e13ed1ff7b897c` |
| corpus / queries / dimensions | 1,000,000 / 1 / 16 |
| metric / seed / top-k | cosine / 1 / 10 |
| topology | 4 data groups x 3 nodes, plus 3-node catalog-meta group |
| distributed leaders | `node-a`, `node-b`, `node-c`, `node-a` |
| partitions / probes | 16 / 1, 2, 4, 8, 16 |
| overlap | 0 measured; 0.20 explicitly unsupported |
| local `ef_search` | 64, 128, 512, 4096 |
| configured concurrency | 1, 16, 64 |
| rows | 60 measured plus one unsupported overlap row |
| timed boundary | persisted router open/search, M6 planning/fanout/dedupe/merge, TCP M5, production Raft proof, local HNSW, and response validation |

The retained fixture contains one query. Its `concurrency=1,16,64` rows prove
configuration acceptance only and are not a concurrency throughput comparison.
The 10k corpus has 128 queries and owns concurrency-shaped evidence.

Every deep measured row had recall@10 `0`. QPS ranged from approximately
`0.507` to `0.539`, and p95 was approximately `1.856` to `1.971` seconds. The
similar timing across probe and `ef_search` budgets is consistent with the
profiled router-open/verification bottleneck.

## Production topology and lifecycle proof

Each data group retained commit, read, and applied index `3`, production proof
kind, three distinct node IDs, its selected leader, and nonzero endpoint hits.
The catalog-meta group activated the complete ready set with digest
`cb28c8d023014f6949901ca3de918ca6a9bcbbdd4f585ce8ee0cfc382bf8bf95`.
The retained 1M database was opened read-only; only placement labels were
cloned into the ephemeral four-group topology.

Lifecycle and recovery tests cover build/readiness/prepare/activate, mutation
invalidation, stale identity rejection, abort and cutover, catalog snapshot
round trip, fresh-authority backup restore, failover, reopen, follower rejoin,
and cleanup state transitions. Data-Raft tests cover follower rejection,
strong-read rejection on followers, failover, snapshot/rejoin, log compaction,
and unavailable meta quorum.

## Failure matrix

| Class | Result |
| --- | --- |
| follower request / not-leader redirect | bounded rejection/retry tests pass |
| data or catalog leader failover | Raft failover/read-proof tests pass |
| unavailable group endpoint | **PASS**, error with no partial neighbors/groups |
| unavailable catalog-meta quorum | fail-closed test passes |
| timeout / cancellation / peer disconnect | bounded cancellation tests pass |
| stale, mixed, missing, or corrupt generation/proof | coordinator and lifecycle rejection tests pass |
| mutation invalidation | pre-mutation fail-closed lifecycle tests pass |
| rebuild, abort, prepare, and cutover | workflow and authority tests pass |
| close/reopen, snapshot/rejoin, backup/restore | catalog/data Raft tests pass |

The benchmark injects endpoint loss after all measured rows. CPU, block, mutex,
trace, and cumulative allocation profiles therefore cover both ordinary query
work and that fault boundary.

## Verification

The required non-race command passed on the report-integrated head across
collections, placement, Raft cluster/FSM, native wire, Mongo gateway, and the
benchmark package. Default race runs passed for Raft cluster, native wire, and
the benchmark package. The focused collections vector-partition race suite
also passed:

```sh
GOWORK=off go test -race ./TreeDB/collections \
  -run 'Test.*VectorPartition' -count=1
```

The required full collections race package did not complete. It reproduced
the same segmentation fault twice inside Go 1.26's runtime while
`testing.AllocsPerRun` called `runtime.GOMAXPROCS(1)` from the unrelated
`TestColumnPhysicalAssetSerialScanNumericProjectionHasZeroAllocsM13A`; there
was no race-detector report and no M8 frame. Forcing `GOMAXPROCS=1` avoided that
runtime transition but invalidated unrelated tests that deliberately expect
parallel query policies. This is disclosed as an incomplete full-suite gate,
not counted as passing M8 evidence.

## Profile and resource findings

The deep artifact captured `cpu.pprof`, `heap.pprof`, `allocs.pprof`,
`allocs_baseline.pprof`, `block.pprof`, `mutex.pprof`, and `trace.out`. Compare
allocations with:

```sh
go tool pprof -top -alloc_space \
  -base profiles/allocs_baseline.pprof profiles/allocs.pprof
```

The dominant allocation paths are:

| Path | Differential allocation |
| --- | ---: |
| `encoding/json.Marshal` | 52,734 MB |
| manifest encode-with-context | 4,157 MB flat, 37,386 MB cumulative |
| membership materialization | 3,727 MB |
| membership slice cloning | 1,862 MB |
| lifecycle checkpoint encoding | 1,397 MB |
| physical asset reads | 1,381 MB |

These profiles identify repeated persistent router validation as the narrow
performance owner. They do not justify bypassing integrity checks; remediation
must retain snapshot/generation identity, cancellation, and fail-closed reader
pin semantics.

The retained M3 record separately reports 188,427,800 mapped bytes,
200,546,033 derived physical bytes, and 1,023,827,968 resident bytes after the
build. Its build peak RSS was 2,793,459,712 bytes. The M8 process measured peak
RSS independently but does not claim a second mmap sample or a configured
process-resource ceiling. Those observations are retained as provenance only;
they do not satisfy the resource-bounds gate.

## Reproduction

Run the checked-in CI shape from the repository root:

```sh
OUT=$(mktemp -d /tmp/treedb_vector_partition_m8_10k_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_partition_bench \
  -mode production_multi_group \
  -dataset /path/to/fixture \
  -out "$OUT" \
  -raft-groups 2 -raft-nodes-per-group 3 -partitions 4 \
  -probes 1,2,4 -overlap 0,.20 \
  -top-k 10 -recall-target .90 \
  -concurrency 1,16,64 -warmup 1 \
  -ef-search 64,128,512,4096 -seed 1 \
  -profiles "$OUT/profiles" -format text
```

For the retained deep shape, add
`-m8-existing-db /path/to/read-only/m3-db`, use four groups and 16 partitions,
and sweep probes `1,2,4,8,16`. The existing database is never rebuilt or
deleted. Every artifact embeds its exact invocation, commits, dirty state, host,
mounts, fixture checksum, budgets, topology, gate ledger, and profile paths.

## Retained records

| Record | Path | SHA-256 |
| --- | --- | --- |
| clean deep JSON | `/mnt/fast4tb/tmp/treedb_vector_partition_m8_refresh1m_20260725_231810/vector_partition_m8_9f3cb7c6f8d5.json` | `14a3e98ae8d4ab74bb4edb78e6858d8ce7e3d7733eebd3136b1321c0eda236b1` |
| clean 10k JSON | `/mnt/fast4tb/tmp/treedb_vector_partition_m8_refresh10k_20260725_231532/vector_partition_m8_9f3cb7c6f8d5.json` | `ef141fea8e8cb1e80235423a486920a499f9d9683fa72ec33fbbc28cf0fb784e` |
| CPU profile | `.../profiles/cpu.pprof` | `98b30c3104f486a28a2b4f0862988d29ac58ff440ee7e90b0d0600b3419059fb` |
| allocation baseline | `.../profiles/allocs_baseline.pprof` | `2c6c095da269c9ac1f7310fb762ae6ca78b3d13ea5e3d427ce5b13ada21ad37b` |
| allocation final | `.../profiles/allocs.pprof` | `0303957d60b2564417a30c582ed6c4043679cabd2eb7f39d3187cd5aa4d838ad` |
| execution trace | `.../profiles/trace.out` | `7144327c5f3986f29910811986d1fe8162c42f1eab26a407ef84f1888e4bb57b` |

The JSON artifact is clean exact-head evidence. Profile paths are host-local
retained records, not portable repository assets; their hashes make later
identity checks deterministic.

## Comparison boundary and disposition

This report compares only TreeDB's own exhaustive oracle and graph-routed
production-shaped loopback path on the stated host and retained fixtures. It
makes no paper-QPS, multi-host, billion-vector, online mutation freshness,
serving-replica, live repartitioning, partial-result, or distributed document
fetch claim.

The implemented topology, failure behavior, runner, and evidence remain useful
as an experimental foundation. Enablement stays off. Closing #3917 requires
explicit user acceptance of this narrower outcome; it does not convert red
gates into passes. The measured follow-up owner is to avoid full manifest
decode/validation/digest reconstruction per shard request while preserving
generation and integrity guarantees. Overlap materialization and stable-hash
attribution remain separately deferred.
