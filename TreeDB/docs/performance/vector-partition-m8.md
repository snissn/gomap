# Vector partition M8 production multi-group closeout

## Frozen real-embedding fixtures (#4753)

`import-fixture` admits existing FP32LE row-major data into the same M3/M8
build, truth, serving, and retained-replay paths. It does not change placement,
R/kRt routing, the accepted Vamana profile, or any qualification gate. The
procedural `embedding_mixture` fixture is still synthetic; importing real data
does not by itself establish real-data performance or scaling.

The admitted source on LAN host `mikers@192.168.0.111` is the retained
`YoKONCy/Cohere-1M-wikipedia-768d` mirror, revision
`de34a7af7f436d7aceb4fecdda01490e552efdde`. Its source receipt and raw files
are under `/home/mikers/gomap-q5-evidence/precollection-20260915/`.
It is **not proven identical** to the unavailable historical VDBBench export.
Although the mirror README says normalized, these raw files are not unit norm.
The importer explicitly L2-normalizes in binary64 with FMA, rounds to FP32,
and uses those same bytes for building and canonical truth. No supplied
benchmark truth or IDs are reused; selected documents have stable ordinal IDs
`doc-%06d`, and duplicate corpus vectors retain separate IDs.

Frozen initial slice: train rows `[0,100000)`, test rows `[200,712)`, 768
dimensions. The query slice avoids the documented earlier first 200 queries;
this is not a claim that it has never been inspected by any other experiment.
Run only after fresh host/resource admission; **no AWS/cloud execution**.

```sh
./treedb_vector_partition_bench import-fixture \
  -train /home/mikers/gomap-q5-evidence/precollection-20260915/source/cohere_train.f32 \
  -test /home/mikers/gomap-q5-evidence/precollection-20260915/source/cohere_test.f32 \
  -train-sha256 d4bd7224f525ff1722ef4a77d0437a01c5f2c624857ead009f0a9c0e8e167961 \
  -test-sha256 03139cf22783b7bac9278264cfc515682a268ca14162e0c850618eebfb811ab3 \
  -source YoKONCy/Cohere-1M-wikipedia-768d \
  -source-revision de34a7af7f436d7aceb4fecdda01490e552efdde \
  -fixture cohere-wikipedia-100k-768-query200-711 \
  -train-rows 1000000 -test-rows 1000 -train-offset 0 -test-offset 200 \
  -vectors 100000 -queries 512 -dimensions 768 -seed 4017 \
  -max-checksum-visits 51200000 -out /path/to/fresh/fixture

./treedb_vector_partition_bench generate-truth-cache \
  -dataset /path/to/fresh/fixture -out /path/to/fresh/truth \
  -seed 4017 -top-k 10 -max-exact-truth-visits 102400000
```

Import streams and hashes each entire pinned source file but retains only the
selected rows. Shape and byte/work caps precede allocation. It refuses zero or
non-finite selected vectors, source hash/length mismatches, symlink files,
and exact cross-split duplicates after normalization/FP32 rounding. Corpus
duplicates are allowed. This is an exact-vector leakage check, not semantic
deduplication or proof of independent sampling.

The portable fixture contains only `fixture_manifest.json`, `documents.f32`,
and `queries.f32`. The manifest records source hashes/revision/slices,
normalization, output hashes/sizes, and the existing vector/query/truth
checksum. Runtime loading verifies the selected files without renormalizing.
External truth generation admits both the existing binary64 checksum pass and
the canonical FP32 truth pass; its visit cap covers both. External truth-cache
identity additionally binds the file hashes and source selection, preventing
query-only consumers from reusing stale truth after a manifest/file change.
The manifest is published last; a failed import is not a valid fixture and
existing outputs are never overwritten. Historical M0 locality capture remains
procedural-only: its split/capture schema does not bind external query files.
Fixed-fixture calibration and final-qualification commands also keep their
existing eligibility restrictions.
Use the ordinary retained M3/M8 paths for this new, separately declared packet.

M3 source, router-source, partition-local and M8 source ingestion share the
same JSON batcher: at most 8,192 rows and 32 MiB of encoded IDs/documents plus
their per-row command-WAL length fields. The byte bound leaves headroom below
the unchanged 64 MiB command-frame cap; a single over-cap row is refused.
High-dimensional real embeddings can require more publications than synthetic
fixtures because decimal JSON is larger than the FP32 source. Batch splitting
preserves source ordinals, IDs, values and durability; it changes setup costs,
so retain actual build costs and never reuse older build timings as current.
The loader reuses bounded batch slice headers and clears consumed document
references after each synchronous insert; it does not retain all encoded rows.
The fixture memory planner's row-based bound remains conservative.

## R all-level router evidence boundary (#4773)

The retained #4773 producer used report schema 6/result kind
`m8_production_multi_group_evidence_v6`, explicitly binding
`global_all_level_spherical_krt_hierarchical_C_v4`, global budget B, actual
representative count, score budget C and probes P. Defaults are B=256 and
C=1024. CLI controls are `-router-global-budget` and `-router-score-budget`;
incompatible settings are refused, not clamped. System-node config version 3
carries the same server-owned C/P coordinates. Router v3 treats each domain as
a virtual container, emits genuine
top-level bucket centroids, and defaults to fanout 64 and minimum cluster size
250. Old v2 formats and retained reports remain historical and cannot replay as
v3 evidence.

Ordinary rows retain actual router score calls, distinct visits and edges,
including failed work. A budget below the root count is an explicit
`router_score_budget_exhausted` row in the report, measurement transcript,
matrix, and retained replay; it preserves observed counters/timing but contains
no partial result or fabricated quality/throughput measurement. The production
hierarchical route always scores one root per domain, so it has no distinct
candidate-coverage refusal. Optional historical flat-HNSW policy diagnostics
retain their candidate-coverage and score-budget refusals instead of aborting
the artifact. Exact attribution is fully charged and never a fallback.
The renamed meaning of historical internal Go fields called `RouterCandidates`
is C in this schema, not a distinct representative count. Use the explicit
semantics/version and score-call counters when interpreting records.

Allocation ownership: construction owns normalized source/member buffers and
bounded tree/centroid scratch; publication owns encoded records and pack build
buffers. A reopened router borrows centers from its pinned prepared FP32 plane,
plus canonical mapping/path metadata, rather than retaining another vector
plane. Live owners explicitly clone any vectors they must own. Query scratch is
bounded by the representative count and pending hierarchy groups; returned
routes are caller-owned. No query exports the corpus or retains diagnostics.
The ordinary router benchmark uses real immutable M8 assets with setup outside
timing; it measures router cost only, not public-service or complete ANN QPS.

No R performance or 100K-to-250K scaling improvement is asserted here. Retained
qualification must freeze reviewed product/harness identities, use eligible
comparable hosts and record build/open/query wall/CPU, allocation, memory and
bytes alongside exact/approximate routing quality. L/M and final #4753 own
local navigability, membership feasibility and matched-recall scaling.

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

## Historical canonical partition-local HNSW readiness (#4744)

The superseded #4744 materialization built one explicit
`canonical_hnsw_m18_ef_construction_256` partition-local graph. Its construction
uses the standard `M` outgoing selection, `2M` layer-0 / `M` upper reciprocal
caps, and preserves the full descending `SEARCH-LAYER` working set. It emits a
membership-bound version-5 native pack with no repair/auxiliary topology and no
ordinal reseed when the query frontier empties. Manifest version 6 records the
graph variant explicitly; publication, recovery, and production open reject
historical or mixed variants instead of inferring identity from matching
parameters. The corrected retained structured250K result reached recall@10
`0.9072` at P2/EF96 with routed truth availability `1.0`; #4787 therefore
replaced this profile rather than tuning it.

The existing M8 producer now emits report/transcript schema 7, binding that
exact graph identity and the local score-call budget, records total and
per-query maximum combined native traversal and canonical result-rescore
calls, and validates them against the configured cap. This is harness
readiness, not retained evidence: the structured-250K, full-query `P<=2`, `EF<=96`,
recall@10 `>=0.95` gate must run only from the reviewed landed product and
harness identity.

## Connectivity-preserving partition-local Vamana qualification (#4787)

Production materialization now uses
`connectivity_preserving_vamana_r64_l256_alpha_1_2`: pack version 6, `R=64`,
`L=256`, alpha `1.0` then `1.2`, and a degree-preserving entry-reachability
pass. The exact-head structured250K retained qualification at
`eb4e754f81f114dd31110f772eee7501a38e25ce` searched all 1,000 queries and
reached recall@10 `0.9816` at both concurrency 1 and 32 with routed truth
availability `1.0`. All 250,000 rows were entry-reachable; 16 packs occupied
207,241,896 durable bytes, maximum load was 15,668 under the 18,750 bound, and
the measured M8 peak was 1,188,319,232 bytes with zero swap I/O. Exact-union,
canonical-score, failure-honesty, replay, and concurrency-invariance gates
passed. This qualifies the local graph only; membership feasibility and the
final scaling decision remain with #4775 and #4753.

## Opt-in graph-quality attribution (#4744)

`-m8-quality-diagnostics` extends the existing `production_multi_group` M8
producer and retained replay for top-k values from 1 through 10. The flag
changes no serving router, graph construction, public query option, or default.
Omission retains the historical subset-enumeration method and omits the new
JSON fields; old receipts are not silently reinterpreted as new observations.

The new `quality_diagnostics` object contains, for every query:

- Minimum cost for **at least h** exact truth hits, independently for logical
  domains and physical packs. The mask dynamic program has at most 1024 states,
  runs once per query and cost interpretation, and is reused across probe/EF
  cells. All packs bound to a selected domain contribute to its physical cost.
- A no-coarsening domain order derived from each pack's best eligible **actual
  member**, retained during the existing exhaustive pack pass. This is not an
  order reconstructed from just global top-k truth or nearest centroids.
- Exact and approximate representative routes, their truth masks, physical
  costs, and signed gains/losses. Alternative routes need not form nested sets.
- Available and returned truth masks for the static local search; optional
  independently observed scored/per-pack-retained masks; and a separate mask
  attached from the **measured coordinator output**. A mask is not inferred from
  an aggregate recall value. Local attribution is not concurrent-live evidence.

`-m8-quality-trace-queries N` samples the first N queries (0..8) through the
existing prepared-pack attribution path, outside serving timers. Zero leaves
scored/retained masks absent, not zero. An actual pack's edge/row structure must
pass a conservative event/memory bound before detailed traces or owned ID maps
are allocated. Unavailable or oversized tracing rejects the selected diagnostic;
it never silently removes queries from the sample. Trace storage and cached ID
strings are released with the attribution harness. No per-request tracing or
second canonical vector corpus is introduced into ordinary serving.

The work planner charges both dynamic programs, mask preparation, curve
extraction, per-cell retained routes, exact bests, and optional trace/ID storage.
It removes the binomial restriction **only for the explicitly selected method**.
The strict retained consumer binds the flags, query/truth digests, generation,
model, masks and physical costs, then reproduces them from the pinned assets.
Unknown modes, incomplete pack ownership, malformed costs, contradictory masks,
missing selected evidence and source/query changes fail closed.

### Cost interpretation

Independent domain and pack optima are not a joint feasibility certificate. For
truth masks `[1023,31,992]` with pack costs `[8,1,1]`, one domain can cover ten
hits and two packs can independently cover ten hits, but at most **one domain
and two packs simultaneously** permits only five. The private, bounded joint
oracle is tested against exhaustive subsets. The quality-attribution producer
continues to report the independent curves and actual route costs; it does not
silently reinterpret them as joint feasibility.

The separate retained-asset preflight is enabled only by the paired
`-m8-membership-probes` and `-m8-membership-pack-limit` flags. It reuses the
trusted truth loader, final shard-generation memberships, pack ownership, and
the joint DP before any timed child process. For every query it also exhaustively
enumerates singles/pairs (`P<=2`), requires the same optimum as the DP, expands
selected domains to all owned physical packs, and persists the exact witness.
The artifact and replay bind the retained build, manifest, ready set, shard
generation, membership digest, truth artifact, actual encoded pack bytes,
limits, and recall target. A ceiling below the target fails before serving
measurement. Passing proves only that retained membership can satisfy the
declared joint limits; router and local-search loss remain measured separately.

### Commands and evidence boundary

A bounded correctness/development run using the existing checked-in generator:

```sh
GOWORK=off go build -o /tmp/treedb_vector_partition_bench ./cmd/treedb_vector_partition_bench
/tmp/treedb_vector_partition_bench \
  -dataset testdata/vector_partition_10k -out /tmp/m8-quality-4744 \
  -mode production_multi_group -partitions 4 -raft-groups 2 \
  -overlap 0 -probes 1,2,4 -top-k 10 -ef-search 32 -concurrency 1 \
  -router-candidates 64 -m8-quality-diagnostics
```

This is a development command, not a representative acceptance or a multi-host
Raft benchmark. Retained qualification must use the existing clean-source,
executable, descriptor, truth-cache and command provenance requirements. Trace
sampling may need a smaller number of cells to fit the conservative work cap;
reduce the declared diagnostic matrix before execution, not the retained query
population after seeing failures.

Before any treatment or final scaling result, #4744 still requires a recorded
current-host baseline and frozen calibration/evaluation identities, 100K/250K
95%-recall operating region, physical-cost constraints, uncertainty policy and
numeric improvement/guardrail decision. Merging instrumentation or passing the
96-row integration test does not complete that empirical gate or release Raft.
The [#4744 baseline preregistration](../spec/artifacts/vector-partition-4744-baseline-plan.md)
is **PLANNED / NO MEASUREMENT**. Its source packet may land before collection;
the baseline gate remains owned by open #4744 until the retained evidence is
accepted, and #4745 remains blocked on that acceptance.

To recover fresh query identities without changing a qualification corpus,
keep its generator, seed, vector count and dimensions fixed and use the existing
fixture command's optional `-query-ordinal-offset`:

```sh
/tmp/treedb_vector_partition_bench generate-fixture \
  -out /tmp/m8-fresh-query-fixture -generator treedb_vector_partition_embedding_mixture_v1 \
  -vectors 16 -queries 4 -dimensions 8 -seed 4016 \
  -query-ordinal-offset 1000
```

This tiny example is a correctness check, not the frozen baseline population.
Use the actual generator identity from the retained manifest (the CLI rejects
unknown identities). The qualification query range is half-open
`[query_ordinal_offset, query_ordinal_offset + queries)`; corpus ordinals remain
unchanged. Freeze disjoint calibration/evaluation ranges before observing their
outcomes. The default zero is omitted from JSON and preserves existing fixture
bytes, checksums and truth-cache identities. The legacy generator rejects a
nonzero offset. Negative or overflowing ranges reject before fixture allocation.
Fresh query bytes produce a new checksum and truth-cache identity: regenerate
canonical truth, and do not reuse a descriptor bound to the old fixture checksum.
This does not change the historical `validate-qualification` campaign or relax
its retained descriptor/truth checks.

For a current-contract retained child report, `replay-m8-report` reuses the
existing production validators without the historical campaign's fixture
whitelist. It accepts only the existing M8 report schema, not a new campaign
format. All artifacts must be under a canonical retained root, with the clean
source checkout at `ROOT/source`. The retained benchmark must have matching clean
embedded VCS metadata. The existing command verifier requires child `-out` and
`-m8-matrix-out` to equal the report directory, plus matching
`-m8-existing-db`, `-profiles`, `-m8-matrix-profiles`, source and explicit caps.
Captured production profiles are required; replay keeps the existing 4 GiB RSS,
2 GiB asset and fixture-specific exact-truth caps.
Current replay reports and explicitly selected diagnostic transcripts have a
64 MiB retained-file cap; ordinary transcripts and historical qualification
retain their 2 MiB transcript, 16 MiB matrix and 1 MiB index caps.

```sh
/tmp/treedb_vector_partition_bench replay-m8-report \
  -root "$RETAINED_ROOT" -report "$RETAINED_REPORT" \
  -report-sha256 "$REPORT_SHA256" -fixture-sha256 "$FIXTURE_SHA256" \
  -command-sha256 "$COMMAND_SHA256" -executable-sha256 "$EXECUTABLE_SHA256" \
  -variant-descriptor-sha256 "$VARIANT_DESCRIPTOR_SHA256" \
  -truth-artifact-sha256 "$TRUTH_ARTIFACT_SHA256" \
  -truth-content-sha256 "$TRUTH_CONTENT_SHA256"
```

Fixture and argv pins are SHA256 of Go `json.Marshal(fixtureManifest)` and
`json.Marshal(report.Command)` respectively (no newline), frozen independently
before measurement. Pin the generated fixture and canonical truth identities
before observing benchmark outcomes; freeze receipt/file hashes at publication.
Do not derive all seven trusted pins from the report being checked. In
particular, a truth-cache checksum alone does not freeze the query offset.
The variant pin hashes `m3VariantDescriptorJSONV1` bytes (indented JSON plus
newline), freezing the complete descriptor including router model/configuration,
not only its M2 `ArtifactSHA256`. The actual descriptor and source are
independently reopened and compared with the pinned report.

Success prints `REPLAY_ACCEPTED_NOT_QUALIFICATION`: measured failures remain
failures. This validates retained child evidence, not baseline acceptance,
cross-variant overlap-storage conclusions, repeat/noise policy, held-out quality
or historical campaign qualification. A successful bounded rehearsal is not the
100K/250K empirical packet.

Focused verification:

```sh
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^TestM8(Quality|Coverage|NoCoarsening|ObservedTruth)' -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^Test(GenerateFixture|FixtureQueryOrdinalOffset|LocalHNSWAttributionCalibration)' -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^TestReplayM8Report|^TestM8QualityRetainedShortfall' -count=1
GOWORK=off go test -race ./cmd/treedb_vector_partition_bench -run '^TestM8(Quality|Coverage|NoCoarsening|ObservedTruth)' -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^$' -bench '^BenchmarkM8CoverageCost' -benchmem -count=5
```

The coverage benchmark measures oracle CPU/allocation cost, **not ANN QPS**.
All proposed performance comparisons retain identical datasets, score contracts,
timer boundaries and populations. Query time is never derived from inverse
concurrent throughput or combined write/search cycles.

## Same-candidate router policy diagnostics (#4745)

`-m8-router-policy-diagnostics` requires `-m8-quality-diagnostics`. It calls
`CompareRankingPoliciesForDiagnosticsV1` on the existing immutable router owner,
**outside the measured serving windows**, and compares three reducers on one
returned candidate set: minimum distance, frequency, and frequency-first then
distance. No public query policy, graph, persisted model, write placement, or
Raft behavior changes. The default remains distance-only with no policy hashing,
voting, diagnostic copies or traces.

The hybrid prepends the most frequent domain, then retains the original distance
order of every other domain. It does not swap two positions. Votes count unique
returned representative identities, not graph visits, source anchors, member
counts or physical pack copies. Equal frequency uses nearest distance then domain
ID; representative-distance ties use the immutable model ordinal. Different
centroids can share source anchors or coordinates and remain different votes.

`-m8-router-policy-width W` selects the nearest W collected representatives
before voting; zero uses the persisted model size. Exact-reference collection
scores **all representatives**, then takes nearest W. Approximate collection is
the retained historical flat-HNSW collector with returned width W, traversal
beam E equal to the persisted model size, and strict score-call ceiling C. It
is not the production hierarchical route and neither clamps C nor retries with
a larger budget. Returned width is not the number of vectors scored. The
historical commands above retain their original removed flag names and pre-R
semantics; they are provenance, not current CLI examples.

A comparison preserves a permutation-invariant candidate-set digest separately
from the collected sequence digest. Both bind model, query, score convention,
collection mode, budget and width. Duplicate identities with conflicting scores
are invalid. A valid set reaching too few domains is retained as
`candidate_coverage_shortfall`, including its work and candidate identity, but
**no partial route**. A traversal exhausting C is retained as
`router_score_budget_exhausted`, including the fully charged score-call count,
but no partial candidate set or policy result. No retry, probe escalation or
exact fallback fills gaps. Queries are not removed from the report because a
policy cannot supply a route.

M8 caches the immutable exact/approximate comparisons once per query and probe
coordinate across local-EF and concurrency cells. Cache lookup still hashes the full
query/truth population to reject changed inputs. Preflight therefore charges
`2*queries*probes*EF-coordinates*concurrency-coordinates` population rechecks per
variant: the conservative strict-replay maximum, including explicit replay and
its attribution pass. The producer's smaller count is covered by that envelope;
collector/reducer work remains charged once per probe. Each recheck includes
query conversion/hash input, bounded truth-ID/score bytes and fixed digest
headers. Dimension-sized scratch is charged separately from model-sized scratch.
These are conservative admission work units, not measured CPU instructions.
The retained-row validation and serialization bound is separate.

It records all three truth
masks, signed gained/lost bits, and **all expanded physical pack costs** using
#4744's common canonical truth and membership. These are coverage diagnostics,
not complete ANN search results under a new policy. Real local-search/QPS
promotion remains #4750/#4753 work. A refused comparison has no inferred mask.
The existing actual local/coordinator search remains on the ordinary route.

The work planner separately charges full exact representative scoring, bounded
approximate traversal, sorting/hashing, owned scalar results, and serialized
report copies. Retained models use their actual validated representative count,
read alongside domain counts under one open owner. New models use the existing
M8 builder's default representative bound, not unrelated simulation flags.
For a retained graph without a tighter preflight degree fact, the edge bound is
conservatively `min(C,N)*(N+9)`; a large diagnostic may therefore require a
smaller predeclared query/probe matrix. No cost or measured query is omitted after
results are observed. There is no new full-corpus vector copy or second ANN
engine. Returned policy prefixes use small owned buffers rather than retaining
all-domain temporary arrays.

The selected flags propagate to child commands and report/transcript identity.
Strict retained replay opens fresh owners and recollects candidates and policies,
including shortfalls, before accepting the report. A digest alone is not trusted
as proof that the source actually produced those candidates. Old unselected
reports remain unchanged and cannot silently contain new policy observations.

### Development and validation commands

For the same bounded development fixture shown above, add
`-m8-router-policy-diagnostics -m8-router-policy-width 4`. Keep the initial
matrix small, for example `-probes 1,2 -ef-search 32 -concurrency 1` and no local
trace sampling. This is not a new qualified workload or permission to tune on
previously opened final queries. The normal resource preflight still applies.

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestVectorPartitionRouter' -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^TestM8(RouterPolicy|Quality)' -count=1
GOWORK=off go test -race ./TreeDB/collections -run '^TestVectorPartitionRouter' -count=1
GOWORK=off go test -race ./cmd/treedb_vector_partition_bench -run '^TestM8RouterPolicy' -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^$' \
  -bench '^BenchmarkM8Router(OrdinaryPath|PolicyPrepared)V1$' -benchmem -count=5
```

`BenchmarkM8RouterOrdinaryPathV1` is the same prepared-router operation on the base
and candidate source. `BenchmarkM8RouterPolicyPreparedV1` measures a feature-
enabled collection plus three reducers and hashes, not a cache hit. Setup is
outside both timers. Neither benchmark includes local pack ANN, document fetch,
networking or Python; neither rate is end-user ANN QPS. The pure reducer benchmark
separately measures ranking/allocation cost without any representative search.
Publish every repetition and the exact source/toolchain/timer boundaries.

Policy work preflight charges both the once-per-probe candidate comparisons and
repeated query/truth checks, retained-row validation and scalar serialization
across EF/concurrency cells. A cache hit removes representative search work,
not the remaining bookkeeping or owned output cost.

The shared approximate collector allocates its owned scalar candidate copy once
at the bounded native return width. It does not grow an escaping append buffer
per query or borrow a pooled native slice after releasing its scratch owner.
Compare `BenchmarkM8RouterOrdinaryPathV1` against the parent and retain allocation
counts as well as latency; helper extraction alone is not an overhead waiver.

#### Draft review repairs for #4756

Nearest-width, representative-identity, domain-distance and domain-frequency
ordering reuse the existing cancellation-aware bounded merge sort. Diagnostic
preparation polls through large intermediate copies and returns no policy routes
on cancellation, allowing the owner read lock to be released. The extra bounded
sort scratch is included in the model-memory envelope. This changes no ordinary
serving reducer or selected candidate/digest definition.

Regression commands:
```sh
GOWORK=off go test ./TreeDB/collections -run '^TestVectorPartitionRouterPolicy(NearestWidthSortCancellation|ReductionCancellationNoPartial)$'
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run '^TestM8RouterPolicyResourcePlan'
```

A prior draft's work bound omitted repeated population revalidation. Old checks
on that bound do not qualify a new expanded diagnostic matrix; rerun current
preflight rather than weakening the cap. Cached comparisons still reject input
mutation and remain outside ordinary serving timers.

The representative admission test checks the combined attribution, quality and
policy work at N=100K/250K, Q=512 and d=128. It preserves the 200M work cap:
C256 at width64/P2/EF96 must refuse, while the finite C128 cell must admit.
The typed one-cell receipt test uses the actual producer and strict transcript
reader under the unchanged 64MiB diagnostic cap. Neither test is empirical
policy evidence or permission to expand the matrix after observing outcomes.

Raw M0 captures optionally include each complete prepared pack's existing
identity-neutral SHA256. Only the 32 membership-digest bytes are zeroed; levels,
vectors, IDs, CSR, auxiliary data and all other headers/sections remain covered.
Older captures without these hashes are readable but cannot prove full geometry
parity. An empty-ordinal split can capture geometry without query outcomes; it
still fails the locality reader's unchanged nonempty-trace admission. Analysis
executable identity remains distinct from each original descriptor's bindings.
