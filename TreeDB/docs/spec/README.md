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
roadmap, schema registry or drift tests, golden vectors, and verification matrix
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
- `TreeDB/docs/spec/value-log-lifecycle.md`
  - retention, GC, rewrite, and operational lifecycle of value-log segments.
- `TreeDB/docs/spec/verification.md`
  - invariants mapped to tests and benchmark harnesses.

## Design Proposals (Non-Normative)

- `TreeDB/docs/spec/collections-native-fastpath-proposal.md`
  - draft target architecture for rewriting the cached collections execution
    path around native root domains, vectorized probes, and native grouped
    publish.
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
- `TreeDB/docs/spec/native-wire-protocol.md`
  - draft native binary network protocol for TreeDB collections, including
    framing, command payloads, explicit ack/consistency policies, and the
    boundary between wire requests and future deterministic Raft command
    entries.
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

## Relationship to Existing Docs

The legacy TreeDB docs in `docs/TREEDB_*.md` remain useful supporting material.
For architecture/behavior/format authority, prefer this folder.
