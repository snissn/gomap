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
- Point-key operations (`Set/Get/Delete/DeleteRange/Merge/SingleDelete/DeleteSized`).
- Range-key write operations (`RangeKeySet/RangeKeyUnset/RangeKeyDelete`).
- `ScanInternal(...)` callback compatibility surface for point/range metadata.
- Native `*pebble.Iterator`, `*pebble.Snapshot`, and indexed batch read APIs via
  an in-memory Pebble shadow mirror kept in sync with writes.
- Ingest support for:
  - local sstable adaptation,
  - local external-file adaptation,
  - TreeDB-native shared object format (`.pcobj`) export/import.
- `IngestAndExcise` on `.pcobj`, including split-preservation of non-overlapping
  existing range fragments.
- `Checkpoint(destDir, ...)` directory export via TreeDB checkpoint plus recursive
  filesystem copy.
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
- Full TreeDB-native operational parity (current operational APIs are delegated
  to the shadow Pebble engine).
- Provider-backed `[]pebble.SharedSSTMeta` ingest path.
- Full Pebble checkpoint option parity (`WithRestrictToSpans`, option-specific
  semantics) is not implemented.

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
