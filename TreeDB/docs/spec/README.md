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
- `TreeDB/docs/spec/outer-leaf-modes.md`
  - mode matrix for `v1`, `v1_leaflog`, `v2_blockptr`, and `v2_fenceptr`, plus parity requirements and benchmark workflow.
- `TreeDB/docs/spec/write-path-and-durability.md`
  - write pipeline and durability semantics for all durability modes.
- `TreeDB/docs/spec/recovery.md`
  - open-time recovery pipeline, replay ordering, truncated tail behavior, failure modes.
- `TreeDB/docs/spec/value-log-lifecycle.md`
  - retention, GC, rewrite, and operational lifecycle of value-log segments.
- `TreeDB/docs/spec/verification.md`
  - invariants mapped to tests and benchmark harnesses.

## Relationship to Existing Docs

The legacy TreeDB docs in `docs/TREEDB_*.md` remain useful supporting material.
For architecture/behavior/format authority, prefer this folder.
