# Pebblecompat Parity Matrix

Legend:

- `full`: implemented with intended semantics for current scope.
- `partial`: implemented with caveats/different internals.
- `missing`: not implemented in `pebblecompat` yet.

## DB Method Parity

| Method | Status | Notes | Target Phase |
|---|---|---|---|
| `Open` | partial | `pebblecompat.Options`, not full Pebble options surface. | 0/4 |
| `Close` | full | TreeDB close semantics. | done |
| `Get` | full | Returns `ErrNotFound` parity behavior. | done |
| `Set` | full | Via batch apply. | done |
| `Delete` | full | Via batch apply. | done |
| `DeleteSized` | partial | Tombstone semantics mapped; sizing hints not Pebble-internal. | 2 |
| `SingleDelete` | partial | Mapped to stored tombstone kind semantics. | 2 |
| `Merge` | partial | Uses compatibility merge behavior; not full merger configurability. | 2 |
| `DeleteRange` | full | Persisted as range-delete log + point effects. | done |
| `RangeKeySet` | full | Persisted in range-op log. | done |
| `RangeKeyUnset` | full | Persisted in range-op log. | done |
| `RangeKeyDelete` | full | Persisted in range-op log. | done |
| `LogData` | partial | Sequence-reserving no-op. | 2 |
| `Apply` | full | Repr-backed deterministic apply path. | done |
| `ApplyNoSyncWait` | partial | Equivalent to apply in current wrapper. | 2/4 |
| `ApplyBatchRepr` | full | Deterministic parsing/apply for supported op kinds. | done |
| `NewBatch` | full | Compatibility batch wrapper. | done |
| `NewBatchWithSize` | partial | Hint accepted; not full Pebble memory model. | 2 |
| `Flush` | partial | Checkpoint boundary mapping. | 4 |
| `Checkpoint(destDir, ...)` | partial | TreeDB checkpoint + filesystem copy; restrict-to-spans and full Pebble option parity not implemented. | 4 |
| `Ingest` | partial | Supports sstable adaptation + `.pcobj` fast path. | 3 |
| `IngestWithStats` | partial | Coarse stats parity. | 3 |
| `IngestExternalFiles` | partial | Local path adaptation only; provider/shared gaps. | 3 |
| `IngestAndExcise` | partial | Shared meta missing; `.pcobj` excise supported incl. fragment split. | 3 |
| `ScanInternal` | partial | Compatibility reconstruction, not native Pebble LSM internals. | 1/3 |
| `ExportSharedObject` (compat extension) | full | TreeDB-native immutable transfer object. | done |
| `IngestSharedObject` (compat extension) | full | Buffered/chunked apply path. | done |
| `NewIter` | partial | Exposed via Pebble shadow mirror; user-visible semantics only. | 1 |
| `NewIterWithContext` | partial | Exposed via Pebble shadow mirror; user-visible semantics only. | 1 |
| `NewSnapshot` | partial | Exposed via Pebble shadow mirror; sequence identity differs from Pebble internals. | 1 |
| `NewIndexedBatch` | partial | Indexed read semantics via Pebble shadow mirror. | 1 |
| `NewIndexedBatchWithSize` | partial | Indexed read semantics via Pebble shadow mirror. | 1 |
| `Compact` | partial | Delegates to shadow Pebble compaction (not TreeDB-native compaction). | 4 |
| `Metrics` | partial | Delegates to shadow Pebble metrics. | 4 |
| `EstimateDiskUsage` | partial | Delegates to shadow Pebble estimates. | 4 |
| `EstimateDiskUsageByBackingType` | partial | Delegates to shadow Pebble estimates. | 4 |
| `SSTables` | partial | Delegates to shadow Pebble metadata. | 4 |
| `ScanStatistics` | partial | Delegates to shadow Pebble statistics. | 4 |
| `AsyncFlush` | partial | Delegates to shadow Pebble async flush. | 4 |
| `Download` | partial | Delegates to shadow Pebble provider path. | 4 |
| `ObjProvider` | partial | Exposes shadow Pebble obj provider. | 3/4 |
| `FormatMajorVersion` | partial | Delegates to shadow Pebble format state. | 4 |
| `RatchetFormatMajorVersion` | partial | Delegates to shadow Pebble ratchet path. | 4 |
| `SetCreatorID` | partial | Delegates to shadow Pebble provider config. | 4 |
| `NewEventuallyFileOnlySnapshot` | partial | Delegates to shadow Pebble EFOS. | 4 |

## Batch Method Parity

| Method | Status | Notes | Target Phase |
|---|---|---|---|
| `Set` | full | | done |
| `Delete` | full | | done |
| `Merge` | full | Recorded and resolved during apply. | done |
| `DeleteSized` | partial | Kind preserved; Pebble-internal sizing heuristics differ. | 2 |
| `SingleDelete` | partial | Kind preserved; compaction semantics differ internally. | 2 |
| `DeleteRange` | full | | done |
| `RangeKeySet` | full | | done |
| `RangeKeyUnset` | full | | done |
| `RangeKeyDelete` | full | | done |
| `LogData` | partial | Seq reservation semantics only. | 2 |
| `Apply` | full | Batch append semantics. | done |
| `Commit` | full | Routes through DB apply. | done |
| `SyncWait` | partial | No async commit pipeline currently. | 4 |
| `Reader` | full | Exposes Pebble batch reader. | done |
| `Repr` | full | Stable copy returned. | done |
| `SetRepr` | full | | done |
| `Count` | full | | done |
| `Len` | full | | done |
| `Empty` | full | | done |
| `Reset` | full | | done |
| `Close` | full | | done |
| Indexed read APIs (`Get`, `NewIter` on indexed batch) | partial | Implemented; semantics rely on shadow mirror parity. | 1 |
| `CommitStats` | partial | Surface exposed; stats do not match Pebble commit-pipeline internals. | 2 |
| `AddInternalKey` | missing | Not implemented. | 2 |

## Acceptance Criteria for “Full”

A method may be promoted from `partial`/`missing` to `full` only when:

1. Differential tests vs Pebble pass for that behavior class.
2. Crash/recovery tests (where applicable) pass.
3. Contract updates are included in this file and `COMPATIBILITY.md`.
