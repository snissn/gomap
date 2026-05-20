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

- `TreeDB/docs/spec/architecture.md`
  - system model, components, directory layout, side stores, lock model.
- `TreeDB/docs/spec/contracts.md`
  - API-level behavioral contracts (reads/writes, iteration, snapshots, concurrency, locking).
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
- `TreeDB/docs/spec/write-path-and-durability.md`
  - write pipeline and durability semantics for all durability modes.
- `TreeDB/docs/spec/recovery.md`
  - open-time recovery pipeline, replay ordering, truncated tail behavior, failure modes.
- `TreeDB/docs/spec/user-command-wal.md`
  - target user-command WAL, applied-LSN checkpointing, command support policy,
    and PR milestones for durable-at-ack mutation recovery.
- `TreeDB/docs/spec/user-command-wal-test-migration.md`
  - PR1 test migration inventory mapping legacy raw WAL invariants to typed
    command-frame coverage.
- `TreeDB/docs/spec/value-log-lifecycle.md`
  - retention, GC, rewrite, and operational lifecycle of value-log segments.
- `TreeDB/docs/spec/backup-restore.md`
  - restorable file set, live backup barrier, restore validation, and
    quarantine/purge requirements for command-WAL external refs.
- `TreeDB/docs/spec/native-wire-protocol.md`
  - native binary protocol v1 for code that advertises native-wire support;
    distributed/Raft behavior remains target behavior until cluster mode lands.
- `TreeDB/docs/spec/verification.md`
  - invariants mapped to tests and benchmark harnesses.

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

- `TreeDB/docs/spec/collections-native-fastpath-proposal.md`
  - draft target architecture for rewriting the cached collections execution
    path around native root domains, vectorized probes, and native grouped
    publish.
- `TreeDB/docs/spec/native-ann-vector-index.md`
  - draft target architecture and implementation plan for turning the
    collection ANN vector graph into a native persisted TreeDB secondary index.
- `TreeDB/docs/spec/collections-column-vector-contract-seam.md`
  - current contract seam for explicit `column_graph` vector indexes; documents
    physical column asset loading, status/fallback behavior, and remaining
    build/rebuild plus mutation-maintenance milestones.
- `TreeDB/docs/spec/collections-column-graph-vector-search.md`
  - user-facing quickstart, demo, benchmark scripts, Deep1B dataset guidance,
    current evidence, and caveats for explicit `column_graph` vector indexes.
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
- `TreeDB/docs/spec/native-wire-r1-closeout.md`
  - R1 single-node native server performance closeout, including direct vs
    native microbenchmarks, workload profile commands, artifacts, findings, and
    deferred optimization targets.
- `TreeDB/docs/spec/native-wire-r2-closeout.md`
  - R2 deterministic command-entry closeout, including replicated-command
    fixtures, canonical validation, digest stability, benchmarks, and deferred
    Raft apply work.

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
| Command-WAL external refs and side files | `user-command-wal.md` | `value-log-lifecycle.md`, future column-store docs. |
| Public API semantics | `contracts.md` | `write-path-and-durability.md`, `collections-write-domain.md`. |
| Native-wire ack policies | `native-wire-protocol.md` | `user-command-wal.md`, `native-query-raft-roadmap.md`. |
| Raft/local apply layering | `native-query-raft-roadmap.md` | `native-wire-protocol.md`, `user-command-wal.md`. |
| Verification mapping | `verification.md` | all normative specs. |

## Terminology Ownership

TreeDB-wide storage terms (`value log`, `leaf log`, `commit log`,
`ValuePtr`) are owned by `storage-format.md` and `value-log-lifecycle.md`.
User-command WAL lifecycle terms (`CommandEnvelope`, `LSN`, `AppliedLSN`,
`WAL-supported`, `WAL-rejected`, `WAL-off-only`) are owned by
`user-command-wal.md`. Deprecated collection root-delta WAL terms
(`CollectionSeq`, `WALLSN`, `root group`, `applied watermark`) remain defined in
`collection-wal-durability-plan.md` for historical design context only.

## Open Questions Index

Open questions remain in their owner documents, but this index records where
blocking questions live:

- User-command WAL durability/recovery questions:
  `user-command-wal.md`.
- Native-wire protocol questions: `native-wire-protocol.md`.
- Raft sequencing and local recoverability questions:
  `native-query-raft-roadmap.md`.
- Column-store persistence questions:
  `GOMAP_TREEDB_COLUMN_STORE_RFC.md`.

A blocking implementation question must be listed in this index and in its
owner document. Non-blocking future-extension questions must be labeled as
such.

## Required Spec Files

Docs lint treats this list as a manifest:

- `TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md`
- `TreeDB/docs/spec/COMPRESSION_TECHNOLOGY_SPEC.md`
- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/verification.md`

## Relationship to Existing Docs

The legacy TreeDB docs in `docs/TREEDB_*.md` remain useful supporting material.
For architecture/behavior/format authority, prefer this folder.
