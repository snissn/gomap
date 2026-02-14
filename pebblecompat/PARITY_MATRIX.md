# Pebblecompat Parity Matrix

Legend:

- `full`: implemented with intended semantics for current scope.
- `partial`: implemented with caveats/different internals.
- `missing`: not implemented in `pebblecompat` yet.

## DB Method Parity

| Method | Status | Notes | Target Phase |
|---|---|---|---|
| `Open` | partial | `pebblecompat.Options` surface (including `InternalPrefix`, `Merger`, and optional resolver hooks), not full Pebble options surface. | 0/4 |
| `Close` | full | TreeDB close semantics. | done |
| `Get` | full | Returns `ErrNotFound` parity behavior. | done |
| `Set` | full | Via batch apply. | done |
| `Delete` | full | Via batch apply. | done |
| `DeleteSized` | partial | Tombstone semantics mapped; sizing hints not Pebble-internal. | 2 |
| `SingleDelete` | partial | Mapped to stored tombstone kind semantics. | 2 |
| `Merge` | partial | Uses configured `Options.Merger` (default `pebble.DefaultMerger`) for TreeDB replay and shadow parity. Still differs from full Pebble internals because merges are eagerly resolved during apply. | 2 |
| `DeleteRange` | full | Persisted as range-delete log + point effects. | done |
| `RangeKeySet` | full | Persisted in range-op log. | done |
| `RangeKeyUnset` | full | Persisted in range-op log. | done |
| `RangeKeyDelete` | full | Persisted in range-op log. | done |
| `LogData` | partial | Sequence-reserving no-op. | 2 |
| `Apply` | full | Repr-backed deterministic apply path. | done |
| `ApplyNoSyncWait` | partial | Equivalent to apply in current wrapper. | 2/4 |
| `ApplyBatchRepr` | full | Deterministic parsing/apply for supported op kinds; seeded randomized differential/reopen/segmentation replay tests cover supported point-op streams. | done |
| `NewBatch` | full | Compatibility batch wrapper. | done |
| `NewBatchWithSize` | partial | Hint accepted; not full Pebble memory model. | 2 |
| `Flush` | partial | Flush now performs a TreeDB checkpoint and then a blocking shadow Pebble flush for closer observable parity; internal memtable/L0 semantics still differ. | 4 |
| `Checkpoint(destDir, ...)` | partial | TreeDB checkpoint + filesystem copy; `WithFlushedWAL` is accepted (no-op under current architecture), while non-empty `WithRestrictToSpans` remains explicitly rejected with `ErrCheckpointOptionUnsupported`. | 4 |
| `Ingest` | partial | Supports sstable adaptation + `.pcobj` fast path. | 3 |
| `IngestWithStats` | partial | Coarse stats parity. | 3 |
| `IngestExternalFiles` | partial | Local path adaptation plus optional `ExternalFileResolver` hook for non-local descriptors (e.g. `Locator`-backed) staged to local files. Descriptors are prevalidated before mutation; unresolved/failed resolver paths return `ErrExternalFileUnsupported`. Differential tests cover local external file ingest after excise across disjoint/partial/full/boundary-touch spans. | 3 |
| `IngestAndExcise` | partial | Supports local SST paths, `.pcobj` paths, compat-local `SharedSSTMeta` backings, and optional `SharedMetaResolver` fallback for opaque/shared descriptors staged to local `.pcobj`/`.sst` files. Excise is applied once and object-backed inputs are ingested before SST inputs deterministically; provider-backed direct ingest remains unsupported without staging. Differential overlap-matrix tests cover disjoint/partial/full/boundary-touch spans for local SST and shared-meta paths. | 3 |
| `ScanInternal` | partial | Compatibility reconstruction, not native Pebble LSM internals. | 1/3 |
| `ExportSharedObject` (compat extension) | full | TreeDB-native immutable transfer object. | done |
| `IngestSharedObject` (compat extension) | full | Buffered/chunked apply path. | done |
| `NewIter` | partial | Exposed via Pebble shadow mirror; user-visible semantics only. | 1 |
| `NewIterWithContext` | partial | Exposed via Pebble shadow mirror; user-visible semantics only. | 1 |
| `NewSnapshot` | partial | Exposed via Pebble shadow mirror; sequence identity differs from Pebble internals. | 1 |
| `NewIndexedBatch` | partial | Indexed read semantics via Pebble shadow mirror. | 1 |
| `NewIndexedBatchWithSize` | partial | Indexed read semantics via Pebble shadow mirror. | 1 |
| `Compact` | full | Delegates directly to Pebble shadow compaction API. | 4 |
| `Metrics` | full | Delegates directly to Pebble shadow metrics API. | 4 |
| `EstimateDiskUsage` | full | Delegates directly to Pebble shadow estimate API. | 4 |
| `EstimateDiskUsageByBackingType` | full | Delegates directly to Pebble shadow estimate API. | 4 |
| `SSTables` | full | Delegates directly to Pebble shadow sstable metadata API. | 4 |
| `ScanStatistics` | full | Delegates directly to Pebble shadow scan statistics API. | 4 |
| `AsyncFlush` | full | Delegates directly to Pebble shadow async flush API. | 4 |
| `Download` | full | Delegates directly to Pebble shadow download API. | 4 |
| `ObjProvider` | full | Delegates directly to Pebble shadow object provider. | 3/4 |
| `FormatMajorVersion` | full | Delegates directly to Pebble shadow format version API. | 4 |
| `RatchetFormatMajorVersion` | full | Delegates directly to Pebble shadow format ratchet API. | 4 |
| `SetCreatorID` | full | Delegates directly to Pebble shadow creator-id API. | 4 |
| `NewEventuallyFileOnlySnapshot` | full | Delegates directly to Pebble shadow EFOS API. | 4 |

## Batch Method Parity

| Method | Status | Notes | Target Phase |
|---|---|---|---|
| `Set` | full | | done |
| `Delete` | full | | done |
| `Merge` | full | Recorded and resolved during apply with the configured DB merger. | done |
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
| `AddInternalKey` | partial | Delegates to underlying Pebble batch; replay covers `Set`, `SetWithDelete`, and no-op internal kinds `13` (historical noop) and `17` (separator), with other internal kinds still dependent on apply-path coverage. | 2 |

## Acceptance Criteria for “Full”

A method may be promoted from `partial`/`missing` to `full` only when:

1. Differential tests vs Pebble pass for that behavior class.
2. Crash/recovery tests (where applicable) pass.
3. Contract updates are included in this file and `COMPATIBILITY.md`.
