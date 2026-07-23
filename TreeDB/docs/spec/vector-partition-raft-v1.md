# Vector-partitioned ANN over Raft: M1 durable-manifest contract

Status: **M1 durable identity and lifecycle contract** for #3908/#3910.
M1 persists and validates generation manifests; it does not build, route, or
search ANN packs.

## M1 durable lifecycle

Each ready manifest stores typed `ColumnAssetRef`s, not paths. Every partition
asset has an explicit logical `partition_id`; every declared logical partition
must have at least one asset, while the router is a separate asset. Asset refs,
checksums, lengths, and aggregate referenced bytes are bounded and canonical.
The active pointer pins one ready generation. `Deactivate` durably changes that
generation to retained/prepared-only; deletion then permits column-asset GC.
Corrupt pointers, filenames, manifests, or catalog-group mappings fail closed.

`VectorPartitionStatusV1.Ready` means the stored generation is complete.
`Active` additionally requires it to be the active pointer target and source
valid. M1 provides explicit in-process reader-pin handles; status and deletion
observe those pins. `SnapshotReferences` and `CatalogReferences` remain
trusted caller-supplied cleanup authority inputs: M1 does not infer external
backup or catalog reachability. M7 owns durable catalog/cutover derivation.
Raft archives are self-contained copies and do not pin live side-store files
after export.

## Identity and placement

Canonical TreeDB documents and embeddings are authoritative. The `_id` token
route determines their canonical Raft-group owner. A **logical vector
partition** is separate, derived, rebuildable neighborhood membership used only
to select ANN search packs. A vector may appear in multiple partition-local
packs only as an overlap membership; that is neither a Raft replica nor an
additional authoritative document write.

Terminology is intentionally non-interchangeable:

| Term | Meaning |
| --- | --- |
| Raft replica | a consensus copy of one canonical group |
| overlap membership | derived vector ID and score assets in another ANN partition |
| serving replica | an optional read-serving copy; distinct from both above |

## Generation and consistency

Every artifact carries separate immutable identities: `vector_index_id` names
the index definition; `source_snapshot_generation` names the source vector
snapshot; `partition_generation` names derived memberships/packs; and
`router_generation` names representatives/router. Router, membership,
representative, and partition-local HNSW artifacts MUST bind all four. States
are `building`, `ready`, `cutover`, and `invalidated`.
`ready` requires every referenced artifact to be present, checksum-valid, and
length-capped before allocation on its declared owner. A `cutover` is atomic
over the complete generation.

V1 is snapshot-bound: insert, delete, embedding replacement, index-definition
change, source snapshot replacement, or any committed mutation affecting a
referenced vector invalidates the generation before the mutation becomes
searchable. Until full rebuild and cutover, distributed search MUST fail closed;
it MUST NOT mix generations or return partial top-k. A Raft read-index only
proves the group applied point; it does not make an older snapshot-built
generation fresh.

V1 distributed responses contain response-owned stable IDs and scores only.
Missing owner, stale generation, malformed/capped asset, unsupported
consistency, or shard failure is an explicit error with no incomplete result.

## M0 oracle and evidence boundary

`cmd/treedb_vector_partition_bench` is a sequential **simulation**. It emits
`result_kind=simulation_only` and `production_evidence=false`; its Markdown
artifacts repeat that it is not production Raft evidence. It records exact
global top-k, partition oracle, representative routing, exact partition-local
search, real TreeDB partition-local HNSW, and end-to-end simulation separately.

The HNSW stage deterministically derives one collection per logical modulo
partition in a temporary TreeDB. The harness uses the public lifecycle:
`SaveFormatConfig` with `RequiredFeatureCommandWALV1`, `db.Open`,
`NewCollectionManager`, JSON `float32_vector` typed-column ownership,
`VectorIndexStrategyColumnGraph`, `InsertBatch`, `Flush`,
`RebuildVectorIndex`, and response-owned `SearchVectorIndex` results. It builds
these derived collections and indexes once per harness run only when that stage
is selected. For each query it uses exact representative routing to choose
logical partitions, searches their prepared local indexes, then merges by
cosine distance followed by stable document ID.

Every local response MUST report
`SearchRouteHNSWSearchPack=1`, route
`exact_hnsw_search_pack_v1`, an active pack, and zero pack fallbacks. The
harness fails instead of accepting another route. Repeated overlap rows reuse
a cache whose key contains the query, top-k, and sorted partition-ID set.
Artifacts distinguish logical, executed, and cached searches, and serialize
all HNSW evidence counters even when their value is explicitly zero.
This is real TreeDB local-HNSW loss attribution, but its temporary modulo
partitioning, sequential coordinator, and absent network/Raft path remain
non-production simulation. With every partition probed, the partition oracle
MUST equal global exact top-k under ordered distance and stable-ID comparison.

The checked-in fixture manifest at `testdata/vector_partition_10k/` uses
generator `treedb_vector_partition_fixture_v2` and arithmetic contract
`ieee754_binary64_explicit_fma_v1`. Normalization, exact-oracle dot products,
and representative-routing dot products use explicit `math.FMA` accumulation
so compiler- or architecture-dependent multiply-add contraction cannot alter
the generated fixture or truth. Its stable checksum binds the exact IEEE-754
binary64 bits of every generated vector and query, then the ordered exact-truth
IDs and distance bits. Exact-truth checksum binding covers top-10 for
fixtures with at least ten vectors and every available neighbor for smaller
fixtures. The generated corpus includes an explicit duplicate/tie case,
clusters, and boundary shape. Tests resolve the
single root fixture from `cmd/treedb_vector_partition_bench`. The manifest read
itself has a 64 KiB bound. `-seed` MUST equal the manifest generation seed;
the result repeats that bound seed and its fixed-width bit pattern is part of
the artifact basename. The basename also binds the full fixture checksum,
partition count, probes, exact overlap bits, top-k, and a full SHA-256 over the
canonical selected-stage set, so evidence from different datasets, partition
configurations, or independently selected stage sets cannot overwrite another
row.
Combined vector/query counts, dimensions, partition count, and result metrics
are validated before allocating large work buffers or building TreeDB evidence.
The JSON schema version is strict; unknown versions, non-finite metrics, and
provenance other than exact 40-hex SHAs are rejected by the executable contract.

`-max-fixture-bytes` caps a conservative model of simultaneous live,
benchmark-owned material rather than only raw vector elements. The preflight
model includes contiguous generated `float64` vector/query matrices and row
headers, bounded exact and selected top-k candidates, representative routing,
whole-partition HNSW JSON serialization plus decoded JSON/vector batch material,
HNSW query merge storage, and the persistent cross-overlap HNSW result cache.
It applies 25 percent allocation slack. It explicitly excludes TreeDB engine
and index internals, Go runtime/GC metadata, and CLI/artifact encoding. The
artifact records the requested cap, modeled peak, and this scope. Checksum
validation consumes the already generated matrices and does not regenerate a
second full fixture.

An independent fixed work gate rejects more than 200,000,000 modeled
benchmark-owned vector/query corpus visits before fixture generation or
evidence. One visit is one vector considered for one query. The model counts
checksum exact truth once, the mandatory global truth pass for every
probe/overlap artifact row, and each enabled exhaustive or representative
routing corpus pass. It excludes TreeDB HNSW engine-internal search work. The
canonical `10k x 128`, three-probe, two-overlap, all-stage shape has 67 corpus
passes, or 85,760,000 visits, and remains within the gate.

The schema reserves every descendant evidence family even when M0 emits
`measurement_status=simulation_not_measured` and explicit finite zero values:
build wall/CPU/RSS/temp/final bytes; balance/cut/overlap; representatives and
router latency; fanout/RPC/bytes/shard/merge/failure counters; and QPS,
percentiles, recall@1/10/100, allocations, resident/mapped bytes. Artifact
provenance is the real `HEAD` and `merge-base HEAD origin/main`, a bounded
`pull_request.base.sha` and `pull_request.head.sha` pair from
`GITHUB_EVENT_PATH` in shallow PR CI, or explicit `GITHUB_SHA`/`BASE_SHA`
overrides when no pull-request event is present. A bounded pull-request event
pair overrides both environment values, ensuring GitHub's synthetic merge
`GITHUB_SHA` is never recorded as the candidate head; empty or malformed
provenance is rejected.

## Parent invariant matrix

| Parent invariant | M0 executable evidence | Owner after M0 |
| --- | --- | --- |
| `_id` ownership differs from ANN membership | `TestDocsVectorPartitionRaftM0Contract` | M1/M5 |
| overlap is not Raft replication | docs contract test | M3/M7 |
| four generation identities, ready/cutover/invalidation | docs contract test | M1/M7 |
| snapshot mutation fails closed | docs contract test | M7 |
| IDs/scores only; no partial top-k | docs contract test | M5/M6 |
| exact all-partition parity and stable ties | `TestTruthOracleTieOrderingAndAllPartitionParity` | M2--M6 |
| real local HNSW and fail-closed route evidence | `TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth` | M3/M5 |
| cap-before-allocation/malformed input | `TestMalformedCapAndFiniteInputsRejectBeforeSimulation` | M1--M8 |
| simulation is not production Raft evidence | `TestCanonicalRunWritesJSONAndMarkdown` | M8 |

## Provenance ledger

| Source | Allowed use | Code copied/adapted |
| --- | --- | --- |
| arXiv:2403.01797 | algorithm statements and evaluation vocabulary | none |
| `larsgottesbueren/gp-ann` | reference/oracle only; GitHub declares no license | none |
| TreeDB existing dependencies | separately licensed, pinned modules only | none newly introduced by M0 |

M0 makes no paper speedup claim. It implements no production partition format,
partitioner, overlap membership, kRt, RPC/coordinator, Raft catalog, or routed
Raft read. M1 owns persisted generation/placement; M2 partitions; M3 overlap
and local packs; M4 router; M5 routed reads; M6 merge/fanout; M7 lifecycle; M8
real multi-group matched-recall evidence.

## M1 durable generation record

`collections.VectorPartitionManifestV1` is the M1 binary record, with format
`vector_partition_manifest_v1`. It binds a collection, the SHA-256 digest of
the existing `VectorIndexDefinition`, source base generation/checksum/schema
and row count, derived partition generation, and a complete logical
partition-to-Raft-group mapping. Logical partition IDs are dense `[0,n)` and
are not `_id` token partitions; multiple logical partitions can name one Raft
group.

The record separates `building` from `ready`. A building record has no router
or ready-set reference and is never active. A ready record requires a matching
router generation and canonical SHA-256 ready-set digest over length-prefixed
placement and asset descriptors, including the router's required canonical
`partition_id=0`. It contains exactly one disjoint membership
for every source ordinal and separately records bounded overlap memberships.
Raw `VectorPartitionStoreV1.Publish` is fail-closed: both building and ready
publication require collection authority, which verifies the current source
identity and referenced assets while holding the collection mutation authority.
Collection-authorized publication then takes the root storage barrier; deletion,
deactivation, reader pins, and snapshots use that same barrier. A durable
deletion tombstone rejects both building and ready retries before any temporary
file, link, or rename, so a deleting generation cannot be resurrected.
All decoded count-derived allocations, strings, assets, memberships and record
bytes are capped before allocation; unknown/trailing records fail closed.

Local publication writes and syncs the generation, renames it, then (only for a
ready generation) write-sync-renames an active pointer. Readers therefore see
an old complete pointer or a new complete pointer, never a partial generation.
Raft snapshot archives include `db/vector_partitions`. Deletion requires an
explicit proof that the generation is inactive and has no reader pin, snapshot
reference, or catalog reference; M1 does not infer those external references.
Before removing the generation manifest, deletion durably replaces it with a
bounded, versioned, canonical, checksummed reclaim journal containing every
original asset ref. If an original ref shares a physical segment with a live
column-manifest ref, the mixed-segment rewrite write-sync-renames the journal
with those superseded live refs and syncs its directory before the remap may be
published. A persistence error, or cancellation observed before remap
publication, leaves the old manifest mapping authoritative. Recovery retries
with both original and superseded refs, and retains the journal until all of
their segment debt is physically absent; the durable-root fallback generation
can therefore delay, but never bypass, deletion.

### V1 format and bounds

The authoritative on-disk record is binary `VPM1` (big-endian magic
`0x56504d31`, version `2`), followed by fixed-order length-prefixed fields;
there are no tagged optional fields. The JSON form is an inspection/exchange
encoding of that same record: unknown fields, a second JSON value, trailing
bytes, and non-canonical ordering fail closed. This is a pre-alpha on-disk
format: old database directories need not be readable by new binaries. Version
2 adds a whole-record SHA-256 integrity digest covering identity, policy,
generations, placement, every membership family, and asset descriptors; this
is separate from the ready-set asset contract.

| Record area | Required content | Validation boundary |
| --- | --- | --- |
| identity | collection, index name, SHA-256 index-definition digest; source generation/checksum/schema/row count; partition and router generations | exact live TVIS/base identity; ready router generation equals partition generation |
| placement | dense logical `partition_id` to one Raft group, with many logical partitions allowed per group | IDs are exactly `[0, partition_count)` and canonical |
| memberships | one disjoint membership per source ordinal; bounded overlap and representatives | ordinal/partition coverage, sorted order, per-vector and per-partition caps |
| assets | typed `ColumnAssetRef`, length, CRC, SHA-256 and logical asset ID for each partition plus router | references are namespace-bound, unique, streamed and checksum-verified before every collection-authorized publication; router partition ID is exactly zero |
| ready set | SHA-256 over canonical placements, partition assets and router descriptor | mismatches, mixed router/generation, or missing partition asset reject |

Default decode limits are 16 MiB encoded bytes, 65,536 partitions, 1,048,576
source rows, and 2,097,152 aggregate memberships across disjoint, overlap,
and representative lists (so the declared 1M-row fixture plus 20% overlap and
representatives is representable), 262,144 assets, 4 KiB strings, 16
memberships per vector, and
65,536 representatives per partition. A single asset is capped at 8 GiB and
total referenced bytes at 16 GiB. Count-derived allocations are checked against
these limits before allocation.

### Lifecycle, publication, and cleanup authority

| Transition | Durable order | Observable result |
| --- | --- | --- |
| build -> retained building | write generation temp, `fsync`, link/rename, directory `fsync` | complete non-active building record only |
| build -> ready active | validate live source and producer stable-resource set; stream assets; persist generation; then write/`fsync`/rename active pointer and directory | old complete active pointer or new complete active pointer, never a partial manifest |
| active -> retired | persist retired marker and directory sync, then remove active marker and sync | prepared-only generation is not active |
| retired -> deleting | require inactive plus zero reader pins and caller-proved zero snapshot/catalog references; persist checksummed reclaim journal before removing manifest | retryable reclaim debt survives crash/reopen |
| deleting -> absent | journal superseded refs before any mixed-segment remap; GC physical segment debt; remove journal only when all original and superseded segments are absent | incomplete fallback is retried, never treated as deletion |

The storage barrier is canonical-root scoped (including symlink aliases) and
serializes publication, deletion, reader acquisition, and snapshot export.
`AcquireVectorPartitionReaderPinV1` returns an idempotently released handle;
`VectorPartitionStatusV1` reports its count and deletion rechecks it under the
same barrier. `SnapshotReferences` and `CatalogReferences` are deliberately
external, caller-supplied proofs in M1. M7, not M1, owns their durable catalog
derivation and cluster cutover authority.

### Snapshot and portability semantics

Raft export copies `db/vector_partitions` together with column assets while
holding the same root barrier. Restore replaces the side-store namespace, so a
restored manifest’s typed refs resolve against the archived assets instead of
the prior target directory. Snapshot archives are copies: exporting an archive
does not create a live reader pin or a durable catalog reference. File names
are opaque SHA-256-derived identities; manifest fields, not host paths, are
portable across restored DB roots.

On Windows, M1 can open an already restored `vector_partitions` namespace but
fails closed for creation, publication, activation, deletion, and reclaim
journaling. Generic directory sync and link/remove name-persistence proofs are
not available there; write-through rename alone is insufficient. This is an
explicit platform capability boundary, not a claim of Unix directory durability.

### M1 evidence boundary

M1 correctness tests publish a genuinely authorized ready generation and prove
that its manifest, active pointer, router asset, and partition assets survive
close/reopen and Raft snapshot/install before `OpenActive` succeeds. The scale
benchmark is narrower: `synthetic_ready_manifest_scale_v1` derives a test-only
ready manifest from a valid template, expands only its memberships, and keeps
fixture and authority construction outside the timer. It does not expose a
production authorization bypass and is not production Raft or ANN evidence.

The evidence ledger reports encoded VPM1 manifest bytes separately from full
snapshot archive bytes. Only the former is used for the metadata-bytes/vector
gate; the latter also includes the archived side-store namespace and referenced
assets. Full-process maximum RSS includes compile and setup, while `B/op` and
allocations/op are scoped to the timed benchmark operation.

`cmd/treedb_vector_dataset_export` also supports a declared 1M-vector local
corpus (`-docs 1000000`) within its pre-allocation byte caps. Its manifest pins
vector/query checksums, dimensions, metric, query set, and an exhaustive
distance-then-ID top-k truth stream for a declared leading query prefix.
`-truth-queries` defaults to all exported queries when omitted for standalone
compatibility, accepts `0..queries`, and is recorded as
`exact_truth_queries`. Explicit zero emits an empty truth stream while
retaining all exported query vectors. The fixed 200,000,000 comparison gate
applies to `docs * truth_queries`, not every exported benchmark query. The
exporter materializes document vectors once in a
contiguous `float32` corpus, precomputes their squared norms, and reuses those
bytes for vector/JSON output, query selection, and exact truth. Truth selection
retains only bounded top-k candidates. Its modeled truth peak cap includes the
document corpus, norm array, generator/query scratch, top-k candidates/results,
conservative encoded truth-row growth, and a 1 MiB per-row encoding allowance;
combined output vector bytes remain separately capped. A feasible declared 1M
export uses one bounded truth query:
`GOWORK=off go run ./cmd/treedb_vector_dataset_export -out "$OUT" -docs 1000000 -queries 1 -dims 64 -top-k 10`.
It is a corpus export contract, not a claim that M0 has run a 1M-vector
exhaustive top-k benchmark.

## Verification

```sh
GOWORK=off go test ./cmd/treedb_vector_partition_bench ./cmd/treedb_vector_dataset_export ./TreeDB/docs -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run 'Test.*(Fixture|Truth|Oracle|Manifest|Deterministic|Malformed|Cap).*' -count=1
```
