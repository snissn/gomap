# Pebble Compatibility Contract (TreeDB Backend)

This document defines the compatibility target for `pebblecompat`.

Status: pre-alpha, compatibility-focused, not a full drop-in replacement for
`github.com/cockroachdb/pebble` yet.

## Compatibility Goal

Enable downstream systems that are Pebble-shaped at the storage boundary to run
on TreeDB with equivalent observable behavior for required call paths.

## Current Guarantees

`pebblecompat` currently guarantees:

- Deterministic `Batch.Repr()` + `DB.ApplyBatchRepr(...)` replay for supported kinds.
- Seeded randomized differential, replay-across-reopen, and batch-segmentation invariance coverage for supported point-op `ApplyBatchRepr` streams.
- `Batch.AddInternalKey` replay support for `Set`, `SetWithDelete`, and no-op
  internal kinds `13` (historical noop) and `17` (separator).
- Point-key operations (`Set/Get/Delete/DeleteRange/Merge/SingleDelete/DeleteSized`).
- `Merge` evaluates through the configured `Options.Merger` (defaults to
  `pebble.DefaultMerger`) for both compat replay and shadow Pebble parity.
- Range-key write operations (`RangeKeySet/RangeKeyUnset/RangeKeyDelete`).
- `ScanInternal(...)` callback compatibility surface for point/range metadata.
- Native `*pebble.Iterator`, `*pebble.Snapshot`, and indexed batch read APIs via
  an in-memory Pebble shadow mirror kept in sync with writes.
- Ingest support for:
  - local sstable adaptation,
  - local external-file adaptation (descriptor prevalidation before mutation;
    non-local descriptors can be staged through `Options.ExternalFileResolver`,
    otherwise return `ErrExternalFileUnsupported`),
  - TreeDB-native shared object format (`.pcobj`) export/import.
- `IngestAndExcise` supports mixed local SST, `.pcobj`, and compat-local
  shared-meta path inputs in one call, with optional fallback staging through
  `Options.SharedMetaResolver` for opaque shared descriptors.
  Excise is applied once and object-backed ingest preserves non-overlapping
  existing range fragments.
- Differential ingest/excise overlap-matrix tests cover disjoint, partial overlap,
  full overlap, and boundary-touch spans for local SST, local external-file
  excise+ingest flow, and compat-local shared-meta path ingest.
- `Checkpoint(destDir, ...)` directory export via TreeDB checkpoint plus recursive
  filesystem copy; `WithFlushedWAL` is accepted, while non-empty
  `WithRestrictToSpans` is rejected with `ErrCheckpointOptionUnsupported`.
- `Flush()` enforces a durable boundary by checkpointing TreeDB and then issuing a
  blocking flush on the shadow Pebble engine for closer observable parity.
- Operational/introspection APIs are exposed through delegation to the shadow
  Pebble engine (`Metrics`, `SSTables`, `EstimateDiskUsage*`, `ScanStatistics`,
  `Compact`, `AsyncFlush`, `Download`, format/version APIs, provider hooks).

## Hard Non-Goals (Current State)

These are not currently provided and must not be assumed by consumers:

- Full package-level drop-in replacement for `github.com/cockroachdb/pebble`
  (type identity mismatch remains).
- Full internal-sequence iterator parity with Pebble internals (the current
  shadow mirror provides user-visible iterator/snapshot semantics, not full
  internal key/sequence identity).
- Full Pebble internal merge-record lifecycle parity (current implementation
  eagerly resolves merge operands during apply).
- Full TreeDB-native operational parity (current operational APIs are delegated
  to the shadow Pebble engine).
- Provider-backed `[]pebble.SharedSSTMeta` direct ingest path (without caller
  staging through resolver hooks).
- Full Pebble checkpoint option parity is not implemented:
  `WithRestrictToSpans` remains unsupported and option-specific semantics beyond
  accepting `WithFlushedWAL` are not implemented.

## Required Full-Feature End State

For “full feature complete” status, all of the following must be true:

1. Method-level parity matrix has no required method marked missing.
2. Deterministic batch replay invariants hold under fuzz + crash/reopen tests.
3. Shared ingest parity includes provider-backed `SharedSSTMeta` resolver path.
4. Iterator/snapshot semantics are equivalent for required downstream call sites.
5. Operational methods expected by downstream users are implemented or explicitly
   mapped with deterministic, documented behavior.
6. Performance gates and reliability gates pass in CI.

## Error Policy

- Unsupported features must return deterministic typed errors.
- No silent no-op behavior for unsupported compatibility paths.
- Compatibility-breaking behavior changes require updating this file and the
  parity matrix in the same change.

## Source of Truth

- Method parity status: `pebblecompat/PARITY_MATRIX.md`.
- Execution plan: GitHub tracking issue created from this contract.
