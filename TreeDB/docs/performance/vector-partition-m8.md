# Vector partition M8 production multi-group closeout

Date: 2026-07-26

Historical M8 closeout code head: `9f3cb7c6f8d5aa8283fe2342d9f341cbdbebab48`
(the evidence-document commit is subsequent and is not the measured code).

Historical M7 base merge: `03a5508e5df33ceaf2920839a670fb16652f633d`

Status: experimental/off; the #3982 final local matrix is complete and leaves
enablement off with measured follow-up owners

## #3982 final local gate disposition

Measured production-code head:
`8ad06a6e95423c8992638965230862e1ce917d30` (the documentation commit is
subsequent). Base: `a11060f91534958e119ba79284d201027c11e040`.

The strict schema-3 matrix materialized and executed all three required
immutable descriptors sequentially in fresh OS processes: graph/disjoint,
graph/overlap `0.20`, and stable-ID-hash/disjoint. The overlap descriptor is not
a qualified required variant because it realized only 8,096 of the requested
200,000 extra memberships. The matrix used one declared 1M-vector fixture with
32 queries, 16 dimensions, cosine distance, `top_k=10`, probes `1,2,4,8,16`,
`ef_search=64,4096`, concurrency `1,16`, four three-node data-Raft groups, and
the three-node catalog-meta group. This is single-host loopback
production-shaped evidence; multi-host qualification remains #3983. It is not
an external-system comparison.

Each schema-3 descriptor binds a manifest-authoritative canonical build
identity covering the fixture, source graph, assignment, overlap policy,
backend/source configuration, and the full vector-index definition. All three
share index-definition digest
`c51c99cdf93b98f5e0d22f7a4464c14c3f51f2563e33d3bd300f3fccab5955fc`,
including cosine metric, FP32 encoding, 16 dimensions, strategy, schema
generation, and partition-local HNSW `M=16` / construction/search settings.
All variants share graph artifact
`3c7a5665803b2f8f32f0187376b31faa74b7b712d8b7d94b28aea7114db6f556`.
The two graph-assignment variants also share that full assignment artifact and
router-model digest
`5c5492555c8ca7c5ff1b92e1bf07542130d12c5663ca6eb93ac6bb2b4b2074c4`;
the stable-ID control has full artifact
`7a8ec9915de7acc6035024f3fc363c76678e8b27c529a2cba8a9861e764a49ad`
while retaining the same source graph. Database paths are provenance only and
do not participate in content identity, so retained descriptors remain valid
after directory relocation.

The balance gate reads its hard cap from the manifest-integrity-covered
persisted overlap-policy capacity. It does not recompute a source-row-only
epsilon at measurement time; #4001 still owns choosing and materializing a
feasible `0.20` overlap policy.

Artifact:
`/mnt/fast4tb/tmp/gomap-3982-router-budget-matrix-root-8kZWas/run/matrix/vector_partition_m8_matrix_8ad06a6e9542_04f97bc80f2d.json`

Artifact SHA-256:
`d3347736200332cd2a81333a9053899f725eb66309a3b3ca3743376e60d030d2`

Measured benchmark binary SHA-256:
`86645edacdfeda86d00160d32de47a6bc44c712948757aae6d110dc5ead8d0d9`
(built from the clean detached measured head with `go build -buildvcs=false`).

Fixture checksum:
`71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f`

The final disposition is `experimental_gate_failures` /
`enablement_off_follow_up_required`. This is a completed narrow local gate, not
an enablement claim and not a review or performance waiver.

| Gate | Result | Final evidence |
| --- | --- | --- |
| Required variants | **FAIL** | all three immutable descriptors executed, but graph/overlap realized `8,096 / 200,000` requested memberships |
| Exhaustive correctness | **FAIL** | the actual all-partition coordinator response does not have canonical-oracle ID/score parity; the independent exact partition union still has ID/score parity and isolates the loss to partition-local HNSW |
| Failure honesty | **PASS** | unavailable endpoint returns no partial neighbors/groups |
| Recall >= 0.90 | **FAIL** | graph all-partition recall@10 is `0.7125`; overlap is `0.715625` |
| Median probes <= 25% | **FAIL** | graph exact representative routing retains only `0.25625` recall at 4/16 probes |
| Matched-recall QPS | **FAIL** | no <=4-probe graph row reaches target recall |
| Matched-recall p95 | **FAIL** | no <=4-probe graph row reaches target recall |
| Coupled graph acceptance | **FAIL** | neither graph variant passes recall, probe reduction, matched-recall QPS, and matched-recall tail together |
| Balance epsilon 0.05 | **PASS** | disjoint max `63,292`; overlap max `63,918`; manifest-covered persisted capacity `65,625` |
| Overlap bytes < 1.35x | **FAIL** | raw `285,168,176 / 282,881,928 = 1.0080819868x`, but only `4.048%` of the requested overlap budget materialized, so the ratio is not qualified |
| Resource bounds | **PASS** | fresh-process 4 GiB RSS and 512 MiB asset ceilings plus actual coordinator/shard request maxima pass |
| Existing behavior | **PENDING** | latest-head required normal/race/hosted suites own final PR readiness |

Corpus-exclusive fresh-process peak RSS was `1,610,182,656`, `1,759,191,040`,
and `1,642,389,504` bytes for graph/disjoint, graph/overlap, and stable hash,
respectively, below the configured 4 GiB ceiling. This supersedes the earlier
sequential-process high-water attribution; the blocked matrix parent does not
materialize or retain a second fixture corpus. Aggregate shard concurrency was
configured as eight workers per request times 16 clients (`128`) and observed
at `64`. The production coordinator request budget was the actual `256`
retained router representatives for every variant, below the configured
`1,000,000` ceiling. The `-router-candidates 1024` flag controls attribution;
it is not the request budget. The topology maximum includes a separate
stopped-group fault request, so the resource ledger covers both measured cells
and the endpoint-loss fault.
Successful exhaustive preflight and configured warmup requests have a separate
untimed resource boundary and participate in the same maximum calculation.
Their per-variant wall times were `21,227,422,286`, `20,872,708,413`, and
`21,325,331,988` ns; each selected all `16` partitions at `ef_search=4096`
and used `4` requests / `4` RPCs. The largest untimed coordinator request was
`2,352` bytes, with `4,205,376` candidate bytes, `7,232` response bytes, and a
maximum `588`-byte shard request across `4` partitions.

The stopped-group fault selected all `16` partitions at `ef_search=4096`,
attempted `4` requests and `4` RPCs, and returned zero candidates and response
bytes. Its
maximum coordinator request was `2,344` bytes, its maximum shard request was
`586` bytes across at most `4` partitions, and per-variant fault wall times
were `15,970,857`, `28,464,324`, and `15,091,465` ns.
Query-wide selected partitions reached `16`; actual generated shard
requests contained at most `4` partitions against both 32-partition request
ceilings. Retry and redirect ceilings are the per-shard-task limit multiplied by
the maximum observed four-task fanout (`4` each; `0` observed). Persistent
assets were `282,881,928`, `285,168,176`, and `282,385,488` bytes for graph
disjoint, graph overlap, and stable hash respectively. Each variant retained
CPU, allocation baseline/final, heap, block, mutex, and trace profiles under
`/mnt/fast4tb/tmp/gomap-3982-router-budget-matrix-root-8kZWas/run/profiles/`.
The bounded aggregate candidate ceiling was `134,217,728` bytes, with a maximum
observed request value of `4,207,808` bytes. The slowest actual completed
request was the successful stable-hash preflight at `21,325,331,988` ns
against the `30,000,000,000` ns hard limit.

At all 16 partitions, exact representative routing recall is `1.0`, while
partition-local HNSW owns the remaining loss: `0.7125` graph/disjoint,
`0.715625` graph/overlap, and `0.778125` stable hash at `ef_search=4096`.
Increasing graph `ef_search` from 64 to 4096 did not change recall. At four
probes, exact graph representative routing itself retains only `0.25625`, so a
local-HNSW-only change cannot satisfy the quarter-probe target.

Measured successors preserve these distinct owners:

- #3998: graph partition/router locality at the quarter-probe budget;
- #3999: partition-local HNSW reachability and recall at the 1M shape;
- #4001: reconcile the requested overlap treatment with the balance/capacity model.

Reproduction command (the retained DB descriptors are immutable inputs):

```sh
./bin/treedb_vector_partition_bench \
  -mode production_multi_group \
  -dataset /mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture \
  -out /mnt/fast4tb/tmp/gomap-3982-router-budget-matrix-root-8kZWas/run/matrix \
  -partitions 16 -raft-groups 4 -raft-nodes-per-group 3 \
  -probes 1,2,4,8,16 -top-k 10 -concurrency 1,16 -warmup 1 \
  -ef-search 64,4096 -router-candidates 1024 \
  -profiles /mnt/fast4tb/tmp/gomap-3982-router-budget-matrix-root-8kZWas/run/profiles \
  -m8-max-rss-bytes 4294967296 \
  -m8-max-persistent-asset-bytes 536870912 \
  -m8-variant-dbs /mnt/fast4tb/tmp/treedb_3982_identity3_1m_5060_vXlTip/db_graph_disjoint,/mnt/fast4tb/tmp/treedb_3982_identity3_1m_5060_vXlTip/db_graph_overlap,/mnt/fast4tb/tmp/treedb_3982_identity3_1m_5060_vXlTip/db_stable \
  -format text
```

The sections below retain the earlier M8 closeout and #3980 attribution
history; their unsupported-row and pre-fix performance statements are
historical rather than the current #3982 disposition.

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
manifest verification. The fresh differential allocation profile measured
67,964.72 MB allocated during the query/fault boundary; 52,974.66 MB (77.94%) is directly
attributed to `encoding/json.Marshal`, with manifest validation, lifecycle
reduction, memberships, and physical asset reads dominating the remainder.
CPU is led by SHA-256 (20.98%) and JSON struct encoding (8.38% flat, 39.99%
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
| Resource bounds | **MEASURED, NOT BOUNDED** | 188,536,448 persistent bytes and peak RSS 1,244,614,656 bytes were observed; no configured resource limit was compared, so this is not a pass |
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
| persistent bytes / peak RSS | 1,752,856 / 117,641,216 |

The best all-partition row used `ef_search=4096`: recall@10 was
`0.96171875` at every concurrency, but exact ordered ID parity remained false.
At concurrency 1 it measured 28.55 QPS and 41.91 ms p95; concurrency 16
measured 54.73 QPS and 299.56 ms p95; concurrency 64 measured 52.34 QPS and
1,272.30 ms p95. The best quarter-probe recall was only `0.24140625`, so no
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
`0.512` to `0.540`, and p95 ranged from approximately `1.851` to `1.952`
seconds. Similar timing across probe and `ef_search` budgets is consistent with the
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
| `encoding/json.Marshal` | 52,974.66 MB |
| manifest encode-with-context | 4,191.84 MB flat, 37,710.54 MB cumulative |
| membership materialization | 3,727.32 MB |
| membership slice cloning | 1,862.41 MB |
| lifecycle checkpoint encoding | 1,397.28 MB |
| physical asset reads | 1,403.95 MB |

These profiles identify repeated persistent router validation as the narrow
performance owner. They do not justify bypassing integrity checks; remediation
must retain snapshot/generation identity, cancellation, and fail-closed reader
pin semantics.

## #3980 exactness and recall-attribution continuation

Measured production-code head: `3b52711665297c7396f1f86238840dee1ea2897b`
(the documentation commit is subsequent). Status remains **experimental/off**.
The historical continuation below emitted schema `2`, result kind
`m8_production_multi_group_evidence_v2`, and persists zero recall explicitly.

The executable score contract is
`fp32_normalized_cosine_binary64_accum_score_desc_stable_id_asc_best_duplicate_v1`:
query and source vectors are normalized in FP32, their dot product is
accumulated left-to-right with explicit separately rounded binary64 products
and additions, the result is rounded once to FP32,
duplicate stable IDs retain their best score, and results sort by descending
score then ascending stable ID. The full-source oracle, generation-pinned exact
partition scan, and published HNSW candidate rescoring share this contract.

The clean 10k diagnostic used two data groups, four partitions, probes `1,4`,
`ef_search=64,4096`, one measured concurrency, and 128 queries. The canonical
source oracle and exhaustive exact partition union matched with recall `1.0`
and exact ID/score parity in every row. Coordinator merge/transport also had
exact ID/score parity. At four probes, exact and approximate representative
routing both retained recall `1.0`; remaining recall was `0.25625` at
`ef_search=64` and `0.96953125` at `ef_search=4096`, wholly owned by
partition-local HNSW. At one probe, exact representative routing retained
`0.25234375`; local HNSW reduced it further to `0.07109375` or `0.24375`.

The retained 1M diagnostic used four data groups, 16 partitions, probes `1,16`,
`ef_search=64,4096`, and the declared single smoke query. Its canonical source
oracle and exhaustive exact union again matched at recall `1.0` with exact
ID/score parity. At 16 probes, both representative-routing stages retained
recall `1.0`, but partition-local HNSW and end-to-end recall were explicitly
`0` at both search budgets. Coordinator ID/score parity remained exact. At one
probe, representative routing itself retained no truth IDs. The former 1M zero
is therefore explained: it is not caused by source identity, partition
membership, the score contract, Raft/TCP transport, dedupe, or coordinator
merge; its all-partition owner is partition-local HNSW.

| Continuation record | Path | SHA-256 |
| --- | --- | --- |
| clean 10k attribution JSON | `/mnt/fast4tb/tmp/treedb_vector_partition_3980_10k_clean_fvQhPj/vector_partition_m8_3b5271166529_0f33a1583ce3.json` | `c9d37aba290284b742f9e8189b429031146226a8cf9801370bd15754b80c81dc` |
| clean retained 1M attribution JSON | `/mnt/fast4tb/tmp/treedb_vector_partition_3980_1m_clean_Wadji9/vector_partition_m8_3b5271166529_55786c770e98.json` | `9afad07fb8daf374076fe9fa630106ffdf6241ae746344349eb1885d37cdfbd1` |

These two diagnostic records predate the runner-ordering repair and performed
their exhaustive mmap-backed attribution scans before the timed sweep. They
remain valid for exactness and recall-loss ownership, but their QPS, latency,
profile, and peak-RSS fields are not clean performance evidence. The current
runner completes the measured query/fault profile and snapshots peak RSS before
opening the attribution harness. The peak-RSS scope still includes the bounded
top-k coordinator results retained from measured cells for later parity checks;
performance acceptance requires a new capture.

These runs diagnose ownership; they do not pass the original exhaustive-HNSW,
representative-recall, overlap, resource-bound, or matched-QPS enablement gates.
Issue #3981 owns generation-pinned request-session caching. Issue #3982 owns
remediation or explicit disposition of the local-HNSW loss plus the sole final
local gate.

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
| CPU profile | `.../profiles/cpu.pprof` | `46a106805b3795267bd50c868bec5a7c2d4023f445ca03254c857445ca62c963` |
| allocation baseline | `.../profiles/allocs_baseline.pprof` | `fc68e9ea269a4e4d56ded21ac9aa005e3bddeac4744ed0e713c8a1905e8fea11` |
| allocation final | `.../profiles/allocs.pprof` | `56c21400d0b4176f27f7be459c69f4be8db1dd889525dfa2869edc7e9e8ee755` |
| execution trace | `.../profiles/trace.out` | `13470d84b203738c49f7bba87c8fa52b80e0da0453d17b871e13a9347e54fe1e` |

The JSON artifact is clean exact-head evidence. Profile paths are host-local
retained records, not portable repository assets; their hashes make later
identity checks deterministic.

## #3999 partition-local HNSW reachability refresh

The measured implementation head `2ec108a625419a3a3f372a0eb5dca29c433052f5`
rebuilds only persistent partition-local packs with a bounded, deterministic
layer-0 navigation overlay. It preserves the `2*M` layer-0 cap (32 at M=16)
while providing a route from the pack entry to every local row; global
column-graph and router construction are unchanged. A subsequent
documentation-only record commit does not change this implementation or its
runtime behavior.

The authoritative uncontended evidence uses the fresh graph/disjoint 1M DB
`/mnt/fast4tb/tmp/issue3999_m3_lk3imU/db_graph_disjoint`. The all-16-partition
M8 `ef_search=4096` cell recorded recall@10 and local-HNSW recall@10 `0.975`,
exact representative-routing recall `1.0`, QPS `11.158`, and p50/p95/p99
`87.586/111.463/121.120ms`. Its measured query wall clock was `20.251s`
(`133.31s` command wall clock; `19.609s` build).

The profiled artifact is
`/mnt/fast4tb/tmp/issue3999_m8_2ec_UxtWPM/vector_partition_m8_2ec108a62541_ca6f595a28e8.json`
(SHA-256 `84bb62aeb9cebcc4c6bba733bfacbc68f6a3a46be4709264e723d748f777c36c`).
It records `dirty=false` and `partition_pack_reachability=pass`: exactly 16
diagnostics bind to the 16 manifest partition loads, every diagnostic `rows`
matches its load, the diagnostic/load/retained-variant totals are each
`1,000,000`, and every pack has `reachable_rows == rows` and
`traversal_roots == 1`. Resource bounds pass with peak RSS
`1,731,821,568 / 4,294,967,296` bytes and persistent assets
`282,881,928 / 536,870,912` bytes.

The seven captured profile hashes are: allocation baseline
`aba4330b3185456dba3c34fe3dc14a4073742a88e3efe510b8470e1d85b23b74`,
allocation final `f9c5e3141e60c702e5cbe79d23e67245402334d5819c6b419e2b2399b4d10b13`,
CPU `f809683540eaac0051ee4271b945aad74ff71508fbafff1c21a354b44325338f`,
heap `a5748e085dbc7734f8ee5365632e8fbcacede12853d7588315ccbabf4b105116`,
block `a641bf71369e47f80f53517ffce258202b1f0bdaebf53ffea9cb145b7a037bd7`,
mutex `881ea506a541d1742651a4fa3155937df01146e03e68e4b54bd4f231a3b744b8`,
and trace `5a15d8304b5f7bf73b3a9c713bfe95fb7197b75d7414bfb2e6ff2496c47a3c6c`.

The report remains `experimental_gate_failures`: probe reduction is #3998,
overlap storage is #4001, while exhaustive-correctness, end-to-end QPS, and
tail-latency gates are still unsatisfied M8-system evidence rather than
partition-local reachability/recall failures.

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
