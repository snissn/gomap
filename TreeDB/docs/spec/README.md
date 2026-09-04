# TreeDB Specification (Canonical)

This folder is the canonical specification set for TreeDB.

It is intended to be complete enough to:
- review current behavior,
- guide refactors and improvement proposals,
- support reimplementation in another language.

## Status and Compatibility

TreeDB is pre-alpha.

- Public APIs may change.
- On-disk formats may change.
- Cross-version on-disk compatibility is not guaranteed.

When behavior or format changes, update this spec and the test matrix in `TreeDB/docs/spec/verification.md` in the same change.

When native-wire protocol IDs, command schemas, feature gates, or deterministic
entry formats change, update the native-wire spec, implementation guidelines,
roadmap, schema registry or drift tests, golden fixtures, and verification matrix
in the same change.

## Scope

This spec covers the public `treedb` engine and its backend/index implementation:

- index file layout (`index.db`),
- value-log pointer model and value-log record format,
- commit log (WAL/journal) semantics,
- cached-mode write path, checkpoint, and recovery,
- value-log GC and rewrite lifecycle,
- API-level behavioral contracts (durability, iteration, locking, concurrency).

HashDB is out of scope.

## Normative Language

The key words `MUST`, `MUST NOT`, `SHOULD`, and `MAY` are used as follows:

- `MUST` / `MUST NOT`: required for conformance to the current TreeDB behavior.
- `SHOULD`: recommended behavior; deviations require explicit rationale.
- `MAY`: optional behavior.

Given pre-alpha status, this is a living spec that tracks implementation.

## Core Invariants

1. Value-log records are persistent storage, not ephemeral WAL data.
2. Value pointers are long-lived and remain valid until segments become unreachable and are removed by GC or rewrite.
3. WAL (commit log) and value log are decoupled.
4. WAL can be disabled in cached mode; value-log pointers remain valid storage references.
5. Recovery must be coherent across cached/backend open paths.

## Document Map

### Practical guides (non-normative)

- `TreeDB/docs/guides/README.md`
  - index for typed-storage quickstarts, performance profiling, and vector typed-column guidance.
- `TreeDB/docs/guides/collections-quickstart.md`
  - runnable hybrid collection smoke plus document-only, typed-row, typed-column, and hybrid layout examples.
- `TreeDB/docs/guides/typed-storage-performance.md`
  - storage-mode decision guide, aggregate benchmark/profile commands, counter interpretation, troubleshooting, and playbooks.
- `TreeDB/docs/guides/vector-search-typed-column.md`
  - dense vector typed-column placement, column graph demo/benchmarks, search-vs-fetch timing boundaries, and caveats.
- `TreeDB/docs/guides/hybrid-search.md`
  - user-facing `SearchHybrid` index creation and query examples, mode-selection guidance, counters, and caveats.

### Canonical specs

- `TreeDB/docs/spec/minima-native-execution.md`
  - #4614/#4615 typed Minima implementation contract, ownership/reuse decisions,
    mutable column-graph durability gates, and bounded-versus-full evidence.
- `TreeDB/docs/spec/vector-partition-v1-contract.md`
  - #4013 admission contract separating snapshot-bound exact partition-union
    correctness from recall-qualified representative and partition-local HNSW
    evidence, with lifecycle, error, and IDs/scores-only boundaries.
- `TreeDB/docs/spec/vector-partition-raft-v1.md`
  - #3908 M0/M1/M4 contracts for derived vector-partition identity, durable
    generation lifecycle, deterministic persisted representative routing,
    simulation/local-path evidence boundaries, and clean-room provenance.
- `TreeDB/docs/spec/vector-partition-coordinator-v1.md`
  - #3915 M6 contract for bounded transport-neutral fanout, strict M5 proof
    validation, deterministic stable-ID merge, all-or-error cancellation, and
    scoped local-service evidence.
- `TreeDB/docs/spec/vector-partition-m8-production-topology.md`
  - #3917 M8 real multi-group Raft/TCP/lifecycle integration contract,
    retained-asset boundary, operator model, benchmark evidence rules, and
    experimental/off enablement gate.

- `TreeDB/docs/spec/architecture.md`
  - system model, components, directory layout, side stores, lock model.
- `TreeDB/docs/spec/contracts.md`
  - API-level behavioral contracts (reads/writes, iteration, snapshots, concurrency, locking).
- `TreeDB/docs/spec/conditional-kv-adapter-readiness.md`
  - downstream adapter closeout for native `EntryRevision` cache tokens,
    conditional transaction conflict mapping, command-WAL/fail-closed surfaces,
    and unsupported Badger-style feature errors.
- `TreeDB/docs/spec/dgraph-mvcc-readiness-3673.md`
  - reusable external-MVCC conformance harness, exact supported/unsupported
    Dgraph Alpha boundary, merged-main module-pin policy, correctness matrix,
    and pinned raw/MVCC performance evidence for issue #3673.
- `TreeDB/docs/spec/concurrency-paradigms.md`
  - complete concurrency mechanism inventory, lock/worker topology, and option/flag matrix for perf/refactor audits.
- `TreeDB/docs/spec/storage-format.md`
  - on-disk encodings for pages, node layouts, pointers, value-log records/frames, commit-log segments.
- `TreeDB/docs/spec/collections-document-formats.md`
  - collection document-format encodings, including JSON and template-v1
    template-root storage.
- `TreeDB/docs/spec/collections-write-domain.md`
  - indexed collection write-domain visibility, async flush, backpressure, and
    durability-boundary semantics.
- `TreeDB/docs/spec/collection-text-search.md`
  - collection-native text index metadata, analyzer, root naming/policies,
    versioned postings/text-state/text-stats storage, backfill/drop, write-path
    maintenance, bounded SearchText postings scans, BM25F-style ranking,
    top-K document fetch, and fail-closed search guardrails.
- `TreeDB/docs/spec/collection-text-v2-contract.md`
  - issue #2623 text-v2 production contract: B-tree-native storage boundaries,
    v1/v2 version and rollout vocabulary, reserved v2 roots, required counters,
    production benchmark matrix, target envelope, and fail-closed rollout gates.
- `TreeDB/docs/spec/write-path-and-durability.md`
  - write pipeline and durability semantics for all durability modes.
- `TreeDB/docs/spec/command-wal-durable-write-contract.md`
  - current cached command-WAL `Write`/`WriteSync` ordering, logical versus
    physical sync counters, crash boundaries, and the M3 optimization guardrail.
- `TreeDB/docs/spec/durable-wal-cleanup-proof-3682.md`
  - exact durable-root/WAL-lineage authority for ordinary command-WAL segment
    cleanup, destructive-surface inventory, retryable namespace debt, metrics,
    and focused evidence for issue #3682.
- `TreeDB/docs/spec/publication-readability-3245.md`
  - issue #3245 publication/readability map for collection catalog metadata and
    root descriptors, ordered/system-root publication, value-log pointer
    publication, snapshot acquisition, and `AppliedCommandLSN` visibility.
- `TreeDB/docs/spec/authority-inventory.md`
  - generated issue #3677 authority map for external resource fields, producers,
    identity/frontier evidence, namespace operations, recovery validation,
    deletion ownership, quarantines, and adjacent issue boundaries.
- `TreeDB/docs/spec/recoverable-root-set-maintenance-3681.md`
  - issue #3681 destructive-maintenance authority, checked call-site inventory,
    stale-plan behavior, adjacent-issue boundaries, and convergence contract.
- `TreeDB/docs/spec/recovery.md`
  - open-time recovery pipeline, replay ordering, truncated tail behavior, failure modes.
- `TreeDB/docs/spec/user-command-wal.md`
  - target user-command WAL, applied-LSN checkpointing, command support policy,
    and PR milestones for durable-at-ack mutation recovery.
- `TreeDB/docs/spec/user-command-wal-test-migration.md`
  - PR1 test migration inventory mapping legacy raw WAL invariants to typed
    command-frame coverage.
- `TreeDB/docs/spec/command-wal-legacy-surface-inventory.md`
  - issue #3613 M0 inventory and guardrail contract for removing or
    quarantining legacy cached WAL / redo-journal public surfaces while
    retaining command-WAL as the current durable write log.
- `TreeDB/docs/spec/value-log-lifecycle.md`
  - retention, GC, rewrite, and operational lifecycle of value-log segments.
- `TreeDB/docs/spec/leaf-generation-pack-pl01-closeout-3638.md`
  - issue #3638 two-phase publication state machine, portable page-durability
    contract, deterministic failure boundary, and count=5 performance closeout.
- `TreeDB/docs/spec/backup-restore.md`
  - restorable file set, live backup barrier, restore validation, and
    quarantine/purge requirements for command-WAL external refs.
- `TreeDB/docs/spec/native-wire-protocol.md`
  - native binary protocol v1 for code that advertises native-wire support;
    distributed/Raft behavior remains target behavior until cluster mode lands.
- `TreeDB/docs/spec/verification.md`
  - invariants mapped to tests and benchmark harnesses.
- `TreeDB/docs/spec/column-graph-native-vector-search.md`
  - user-facing quickstart, benchmark matrix, demo, and caveats for explicit
    `column_graph` vector indexes that search through the native physical column
    row reader.
- `TreeDB/docs/spec/hybrid-search-contract.md`
  - issue #2502 hybrid lexical + vector search contract for query/options,
    shared candidates/results, rank fusion, scalar filter strategy vocabulary,
    snapshot/epoch consistency, counters, and fail-closed behavior.
- `TreeDB/docs/spec/quantized-vector-index.md`
  - issue #1926/#2454/#2481 scalar_u8, `rabitq_1bit`, and prototype
    `brq_1bit` quantized score-plane semantics, fail-closed query modes, exact
    rerank behavior, benchmark/storage evidence, and future-work boundaries.
- `TreeDB/docs/spec/scalar-u8-alpha-default-gate-2845.md`
  - issue #2845 no-promote decision for per-granule-alpha `scalar_u8` as the
    new-index default, including count=10 10k x 768 gate evidence, storage/rebuild
    measurements, and explicit legacy/opt-in policy.
- `TreeDB/docs/spec/vector-search-closeout-2483.md`
  - issue #2483 final vector-search docs closeout index for accepted exact FP32,
    scalar_u8, RaBitQ, and BRQ evidence, #2487 snapshot rows, no-promote
    caveats, and #2494 crossover-pending status.
- `TreeDB/docs/spec/vector-partition-m2.md`
  - issue #3911 clean-room offline dense-ball graph sketch, deterministic
    balanced reference backend, artifact validation, and M0 builder command.
- `TreeDB/docs/spec/quantized-prepared-hnsw-closeout-2588.md`
  - issue #2588 closeout for the #2584 prepared HNSW quantized fast-path stack,
    including #2591 10k x 768 gate rows, exact FP32 guardrails, promoted
    `scalar_u8` prepared traversal, and promoted `rabitq_1bit` prepared pack
    traversal evidence.
- `TreeDB/docs/spec/rabitq-1bit-v1.md`
  - issue #2449/#2451/#2452 `rabitq_1bit` v1 codec identity, packed-bit
    storage shape, deterministic reference rotation, pure-Go query/scoring APIs,
    search-mode guardrails, and non-goals.
- `TreeDB/docs/spec/rabitq-closeout-2454.md`
  - issue #2454 RaBitQ closeout for user-visible query modes, asset/storage
    recap, exact/scalar_u8/RaBitQ benchmark workflow, representative recall /
    storage / performance rows, profile artifact requirements, and #2453
    no-acceleration caveat.
- `TreeDB/docs/spec/rabitq-performance-lane-closeout-2482.md`
  - issue #2482 RaBitQ performance-lane closeout for #2476/#2477 promoted
    Sublane A evidence, #2478/#2479 no-promote decisions, `rabitq_1bit` v1
    hard invariants, scalar/exact guardrail interpretation, and later #2480/#2481
    completed Sublane B status.
- `TreeDB/docs/spec/brq-1bit-v1.md`
  - issue #2480/#2481 selected codec contract and lower-level prototype for
    `brq_1bit` v1, including the new codec identity/version, durable packed-code
    schema, LSB0 word view, query `uint4` bit-product score semantics,
    fail-closed validation plan, oracle/golden/runtime test requirements, public
    counter names, and promotion gates.
- `TreeDB/docs/spec/typed-column-graph-search-prepared-views.md`
  - issue #2036 role-specific prepared runtime-view and admission policy for
    current-format typed-column graph search.
- `TreeDB/docs/spec/typed-column-graph-search-admission.md`
  - issue #2044 optimized-state readiness/admission table and docs-lint gate for
    current-format graph-search typed-column state roles.
- `TreeDB/docs/spec/typed-column-graph-search-benchmark-matrix.md`
  - issue #2037 benchmark truth-matrix labels, timing boundaries, placeholder
    prepared rows, and required graph-search report counters.

## Target Gates (Normative Target, Not Current Behavior)

- `TreeDB/docs/spec/user-command-wal.md`
  - active target contract for extending WAL coverage to current deterministic
    user commands and future command admission policy.
  - This supersedes the collection-specific physical/root-delta WAL target for
    future implementation work.
- `TreeDB/docs/spec/collection-wal-durability-plan.md`
  - deprecated target contract and historical design record for collection
    WAL/root-group durability.
  - Do not expand this plan feature-by-feature; use
    `user-command-wal.md` for new WAL implementation work.
  - Downstream specs may cite this document for historical external-ref and
    recovery risk analysis, but new durable-at-ack planning should cite
    `user-command-wal.md`.

## Design Proposals (Non-Normative)

- `TreeDB/docs/spec/delete-range-spans-design-2711.md`
  - issue #2711 design gate for cached TreeDB `DeleteRange` range spans,
    covering point reads, iteration, snapshots, command-WAL replay,
    checkpoint publication, value-log reachability, and benchmark gates.
- `TreeDB/docs/spec/collections-native-fastpath-proposal.md`
  - draft target architecture for rewriting the cached collections execution
    path around native root domains, vectorized probes, and native grouped
    publish.
- `TreeDB/docs/spec/native-ann-vector-index.md`
  - draft target architecture and implementation plan for turning the
    collection ANN vector graph into a native persisted TreeDB secondary index.
- `TreeDB/docs/spec/collections-native-fastpath-roadmap.md`
  - draft implementation roadmap for the native cached collections rewrite,
    including PR slices, acceptance criteria, and performance gates.
- `TreeDB/docs/spec/collections-native-fastpath-execution-playbook.md`
  - pre-execution operating guide for the rewrite, including frozen benchmark
    inputs, artifact discipline, PR deliverables, and go/no-go checks.
- `TreeDB/docs/spec/collections-native-fastpath-baseline-template.md`
  - baseline capture template for freezing the main-based rewrite starting
    point, benchmark commands, artifacts, and gate policy.
- `TreeDB/docs/spec/collections-native-fastpath-pr-note-template.md`
  - required PR note template for rewrite phases, including benchmark tables,
    artifact references, and go/no-go decisions.
- `TreeDB/docs/spec/collections-native-fastpath-baseline-2026-04-25.md`
  - refreshed pre-`R0` baseline note for issue `#768`, including exact
    main/oracle SHAs, artifact dirs, commands, and benchmark baselines.
- `TreeDB/docs/spec/native-wire-implementation-guidelines.md`
  - implementation playbook for keeping codecs, schema IDs, validation,
    conformance tests, benchmarks, and future Raft entry handling aligned with
    the native wire protocol.
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
  - draft query feature roadmap and distributed/Raft sequencing policy for the
    native protocol.
- `TreeDB/docs/spec/raftplacement.md`
  - issue #3046 collection-level placement catalog, pure route resolver, and
    simulation-only token-ring validation contract for early multi-group Raft
    slices.
- `TreeDB/docs/spec/native-wire-r1-closeout.md`
  - R1 single-node native server performance closeout, including direct vs
    native microbenchmarks, workload profile commands, artifacts, findings, and
    deferred optimization targets.
- `TreeDB/docs/spec/native-wire-r2-closeout.md`
  - R2 deterministic command-entry closeout, including replicated-command
    fixtures, canonical validation, digest stability, benchmarks, and deferred
    Raft apply work.
- `TreeDB/docs/spec/column-graph-native-reconstruction-inventory.md`
  - issue #1646 V0 inventory for rebuilding legacy native vector graph search
    on typed-storage reader/cache APIs without carrying forward decoded
    full-graph behavior as the product path.
- `TreeDB/docs/spec/typed-storage-naming.md`
  - issue #1750 PR 0 naming scaffold establishing `typed storage` as the
    umbrella for typed-row and typed-column physical storage, with legacy
    compatibility-name inventory and derived-accelerator classifications.
- `TreeDB/docs/spec/typed-column-transplant.md`
  - issue #1753 implementation note for the non-authoritative
    `TreeDB/internal/typedcolumn` data-plane transplant from
    `experiments/colgranule`.
- `TreeDB/docs/spec/typed-column-adapter.md`
  - issues #1754/#1755/#1756 implementation note for the adapter from TreeDB
    typed-storage field metadata and #1736 mapped resources to the transplanted
    typed-column data plane, including opt-in durable scalar and fixed-dimension
    vector publication and reconstruction.
- `TreeDB/docs/spec/typed-storage-closeout-1758.md`
  - issue #1758 closeout evidence for the typed-storage stack through #1757/#1781,
    including child-ticket status, benchmark/test evidence, naming audit
    classification, and the #1736 COW-maintenance handoff facts.
- `TreeDB/docs/spec/typed-asset-maintenance-1788.md`
  - issue #1788 implementation contract for typed-row plus typed-column COW
    reachability, active mappedresource pin protection, GC, and rewrite.
- `TreeDB/docs/spec/typed-column-schema-evolution.md`
  - issue #1789 pre-alpha policy for typed-column image/descriptor/manifest
    schema evolution, fail-closed mismatch handling, rebuild-vs-migrate
    decisions, future migration tooling requirements, and allocation/performance
    evidence for format changes.
- `TreeDB/docs/spec/typed-column-layout-capabilities.md`
  - issue #1838 layout/codec capability contract keyed by logical type,
    physical typedcolumn type, encoding, compression, and wrappers; documents
    optional raw int64, validation boundaries, direct-view eligibility metadata,
    and unsupported/fallback reason codes.
- `TreeDB/docs/spec/typed-column-optimized-consumer-capabilities.md`
  - issue #2047 optimized-consumer capability tier matrix for every current
    typed-column logical/physical pair, including graph-search admission baseline
    links for #2044 and direct-view certifier scope links for #2046.
- `TreeDB/docs/spec/quantized-asset-schema.md`
  - issue #1932 quantized asset role descriptors, fail-closed identity/shape
    validation, prepared ordinal access APIs, scratch contracts, non-goals, and
    footprint benchmark evidence.
- `TreeDB/docs/spec/typed-column-graph-search-prepared-views.md`
  - issue #2036 role-specific prepared runtime-view matrix, hot-loop boundaries,
    graph-row fallback prohibition, future type admission gate, and #2037/#2044
    evidence requirements for typed-column graph search.
- `TreeDB/docs/spec/typed-column-graph-search-admission.md`
  - issue #2044 repo-owned optimized-state admission/readiness table, current
    row statuses, legacy fallback rows, and docs-lint enforcement contract for
    graph-search typed-column state.
- `TreeDB/docs/spec/typed-column-direct-view-alignment.md`
  - issue #1893 aligned fixed-width direct-view safety contract, all-type/storage
    owner classification, absolute-offset alignment rule, fallback/deferred
    policy, counter vocabulary, and benchmark harness expectations for #1886.
- `TreeDB/docs/spec/typed-column-direct-view-closeout-1899.md`
  - issue #1899 final #1886 closeout matrix and evidence structure for
    typed-column fixed-width scalar/vector direct-view coverage, with physical
    row assets deferred to #1897 and adjacency direct views deferred to #1901.
- `TreeDB/docs/spec/typed-column-production-jsonbench-plan.md`
  - planning note for promoting `experiments/colgranule` sort order, granule
    metadata, codecs/compression, q1-q5 kernels, q2 real-predicate grouped
    distinct execution, aggregate metadata, multipart visibility, lifecycle
    accounting, and JSONBench reporting into production `TreeDB/collections`,
    with direct query performance as the first-class target.
- `TreeDB/docs/spec/typed-column-uint32-list-adjacency-quarantine.md`
  - issue #1989 quarantine/removal contract for using vector-index state
    `uint32_list` adjacency on the primary path while isolating legacy
    graph-specific adjacency-source storage.
- `TreeDB/docs/spec/typed-column-list-adjacency-benchmark-1990.md`
  - issue #1990 benchmark/profile closeout proving corrected vector-index state
    `uint32_list` / `raw_uint32_offsets_list` adjacency preserves or improves
    integrated search while removing legacy adjacency-source scratch decodes.
- `TreeDB/docs/spec/search-native-graph-benchmark-closeout-1970.md`
  - issue #1970 benchmark/threshold closeout for integrated search-native matrix
    evidence and default decisions including indexed scoring default-off. Its
    result-ID fallback evidence is historical before #2010/#2013.
- `TreeDB/docs/spec/typed-column-uint32-list-semantics.md`
  - issue #1984 first-class `uint32_list` semantic contract, including
    `uint32[]` / conceptual `Array(UInt32)` aliases, `raw_uint32_offsets_list`
    as the physical encoding, `rows+1` sentinel offsets, validation invariants,
    length-only offset-substream behavior, v1 deferrals, and legacy
    `adjacency_list` classification.
- `TreeDB/docs/spec/vector-index-state-manifest.md`
  - issue #1986 v1 vector-index state control-record contract: record home,
    index/base identity, typed-column asset refs by logical type plus physical
    encoding, fail-closed validation, and legacy graph-record fallback policy.

## Canonical Ownership

| Concept | Canonical owner | Supporting docs |
|---|---|---|
| Durability mode matrix | `write-path-and-durability.md` | `contracts.md`, public README and supporting docs summarize only. |
| Current collection flush-boundary durability | `collections-write-domain.md` | `contracts.md`, `verification.md`. |
| Target user-command WAL contract | `user-command-wal.md` | `write-path-and-durability.md`, `recovery.md`, `verification.md`. |
| Deprecated collection root-delta WAL target | `collection-wal-durability-plan.md` | Historical external-ref and recovery risk analysis only. |
| Current recovery algorithm | `recovery.md` | `storage-format.md`, `verification.md`. |
| Durable bytes and file names | `storage-format.md` | `recovery.md`, `architecture.md`, `backup-restore.md`. |
| Value-log and split leaf-log lifecycle | `value-log-lifecycle.md` | `storage-format.md`, `user-command-wal.md`, `collection-wal-durability-plan.md` for historical external-ref context. |
| Command-WAL external refs and side files | `user-command-wal.md` | `value-log-lifecycle.md`, future typed-storage docs. |
| Public API semantics | `contracts.md` | `write-path-and-durability.md`, `collections-write-domain.md`. |
| Native-wire ack policies | `native-wire-protocol.md` | `user-command-wal.md`, `native-query-raft-roadmap.md`. |
| Raft/local apply layering | `native-query-raft-roadmap.md` | `native-wire-protocol.md`, `user-command-wal.md`. |
| Single-group Raft provider/storage boundary | `raftcluster.md` | `native-query-raft-roadmap.md`, `storage-format.md`, `user-command-wal.md`. |
| Collection-level Raft placement catalog and token-ring simulation | `raftplacement.md` | `native-query-raft-roadmap.md`, `raftcluster.md`. |
| Verification mapping | `verification.md` | all normative specs. |

## Terminology Ownership

TreeDB-wide storage terms (`value log`, `leaf log`, `commit log`,
`ValuePtr`) are owned by `storage-format.md` and `value-log-lifecycle.md`.
Typed physical storage vocabulary (`typed storage`, `typed-row storage`,
`typed-column storage`, `typed_row_asset`, `typed_column_part`,
`retained_document`, `document_payload`, and `derived_accelerator`) is owned by
`typed-storage-naming.md`; the #1753 typed-column transplant scope is recorded
in `typed-column-transplant.md`, the #1754/#1755 adapter/publication seam is
recorded in `typed-column-adapter.md`, the #1758 closeout evidence/handoff is
recorded in `typed-storage-closeout-1758.md`, and current typed-row plus
typed-column maintenance behavior is recorded in
`typed-asset-maintenance-1788.md`, typed-column schema/version evolution
policy is owned by `typed-column-schema-evolution.md`, optimized-consumer tier
classification is owned by `typed-column-optimized-consumer-capabilities.md`,
quantized asset role descriptors and ordinal-reader contracts are owned by
`quantized-asset-schema.md`; `brq-1bit-v1.md` owns the selected #2480/#2481
`brq_1bit` codec contract and lower-level prototype boundary; graph-search
prepared runtime-view shapes and hot-loop boundaries are owned by
`typed-column-graph-search-prepared-views.md`, graph-search optimized-state
readiness/admission status is owned by
`typed-column-graph-search-admission.md`, #1886 direct-view closeout evidence is owned by
`typed-column-direct-view-closeout-1899.md`, and `uint32_list` adjacency
quarantine/audit ownership is recorded in
`typed-column-uint32-list-adjacency-quarantine.md`; first-class
`uint32_list` logical semantics are owned by
`typed-column-uint32-list-semantics.md` until #1985 lands runtime primitive
support. Vector-index derived-state control records and typed-column asset ref
identity are owned by `vector-index-state-manifest.md`.
User-command WAL lifecycle terms (`CommandEnvelope`, `LSN`, `AppliedLSN`, `WAL-supported`,
`WAL-rejected`, `WAL-off-only`) are owned by `user-command-wal.md`. Deprecated collection root-delta WAL terms
(`CollectionSeq`, `WALLSN`, `root group`, `applied watermark`) remain defined in
`collection-wal-durability-plan.md` for historical design context only.

## Open Questions Index

Open questions remain in their owner documents, but this index records where
blocking questions live:

- User-command WAL durability/recovery questions:
  `user-command-wal.md`.
- Native-wire protocol questions: `native-wire-protocol.md`.
- Raft sequencing and local recoverability questions:
  `native-query-raft-roadmap.md`, `raftcluster.md`, and `raftplacement.md`.
- Typed-storage persistence and historical roadmap questions:
  `typed-storage-naming.md`, `typed-column-transplant.md`,
  `typed-column-adapter.md`, `typed-storage-closeout-1758.md`,
  `typed-asset-maintenance-1788.md`, `typed-column-schema-evolution.md`,
  `quantized-asset-schema.md`, and `GOMAP_TREEDB_COLUMN_STORE_RFC.md`.

A blocking implementation question must be listed in this index and in its
owner document. Non-blocking future-extension questions must be labeled as
such.

## Required Spec Files

Docs lint treats this list as a manifest:

- `TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md`
- `TreeDB/docs/spec/COMPRESSION_TECHNOLOGY_SPEC.md`
- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/collection-text-v2-contract.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/docs/spec/raftcluster.md`
- `TreeDB/docs/spec/raftplacement.md`
- `TreeDB/docs/spec/typed-column-schema-evolution.md`
- `TreeDB/docs/spec/vector-partition-coordinator-v1.md`

## Relationship to Existing Docs

The legacy TreeDB docs in `docs/TREEDB_*.md` remain useful supporting material.
For architecture/behavior/format authority, prefer this folder.
