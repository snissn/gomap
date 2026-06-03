# Typed-column lifecycle contract (#1954)

TreeDB production column assets use the collection column manifest as the
reachability catalog. Maintenance must fail closed before deleting or remapping
bytes whenever roots are incomplete, corrupt, ambiguous, quarantined, or held by
active readers.

## Colgranule reuse map

| colgranule file/function | production target file/API | decision | divergence / evidence |
| --- | --- | --- | --- |
| `experiments/colgranule/lifecycle.go` `ColumnAssetLifecycleState` | `TreeDB/collections/column_asset_lifecycle.go` report roots, `ColumnAssetReachabilitySource` | adapted | Production uses source masks plus segment intervals instead of a single state enum so active/recovery/pin/quarantine sources can overlap. Tests cover active/recovery, prepared-query, pending/prepared/quarantine, mappedresource, snapshot, GC, and rewrite roots. |
| `PlanColumnAssetReachability` / ref-delta accounting | `PlanColumnAssetReachability`, `ColumnAssetGC`, `ColumnAssetRewrite` | ported/adapted | Production preserves colgranule's conservative delete classification with interval-union byte accounting, unknown/missing/out-of-bounds fail-closed checks, and mixed-segment rewrite debt. |
| `lifecycle_view.go` summary/scratch planner | production reachability summary path | adapted | Production scans the B-tree manifest snapshot directly and keeps non-detailed GC dry-runs summary-only to avoid retaining per-ref entries; source byte counters and manifest-catalog bytes provide the JSONBench report contract. |
| `asset_manager.go` pins/quarantine/rewrite debt | `column_asset_lifecycle_registry.go`, `column_asset_reachability.go` | adapted | Production has process-local lifecycle pin sets, prepared/pending/quarantine registries, automatic mappedresource pins, and logical segment quarantine. Durable quarantine/pending registry remains a fail-closed future extension. |
| `asset_store.go` segment identity/checksum/range validation | `column_asset_manager.go`, read-cache integrity, reachability segment listing | ported/adapted | Production validates namespace/file/offset/length/checksum through `ColumnAssetRef`; symlink/dir/non-regular/missing/out-of-bounds segments are incomplete or retained. |
| `collection_manifest.go` manifest as catalog | `column_manifest_format.go`, `column_physical_scan.go` | ported/adapted | Production manifest records cover row parts, typed-column parts, aggregate metadata, dictionary codes, int64 sidecars, graph refs, and vector-index state refs. Manifest/catalog bytes are accounted separately from referenced asset bytes. |
| `control_plane_binary.go` fixed-record views | production manifest snapshot scan | intentionally not copied | Production manifests already live in TreeDB roots, not experiment envelope files. The scan path now exposes manifest-catalog byte counts without adding a second decode to maintenance consumers. |

## Lifecycle state mapping

| colgranule state | production mapping |
| --- | --- |
| `prepared` | `prepared_asset` lifecycle registry or explicit prepared refs; protected source. |
| `process_visible` | process-local mappedresource pins and prepared-query pin sets; protected source. |
| `pending_publish` | pending-publish registry or explicit pending refs; protected source. |
| `root_published` | active manifest root descriptor and collection catalog identity. |
| `recovery_authoritative` | recovery-authoritative manifest identity; must match active identity for current physical scans. |
| `active` | active manifest refs from the current snapshot view. |
| `superseded` | caller-supplied superseded refs are passed as reclaimable candidates until a newer root and snapshot fence make deletion safe. |
| `cleanup_safe` | complete whole-segment candidate with no protected, unknown, quarantine, or out-of-bounds bytes. |
| `snapshot_pinned` | explicit pinned refs and conservative older-snapshot candidate protection when exact old roots are unavailable. |
| `reclaimable` | complete canonical whole segment whose bytes are all candidate-only. |
| `deleting` | `ColumnAssetGC` remove phase after a complete plan and maintenance-readiness recheck. |
| `quarantined` | logical quarantine refs/segments; protected and reported as retained debt until released or future durable purge proves safety. |

## Report byte classes

`ColumnAssetLifecycleReport.Bytes` keeps manifest/catalog bytes separate from
referenced asset bytes and exposes overlapping safety classes:

- `manifest_catalog_bytes`: bytes in manifest identity and catalog records.
- `referenced_asset_bytes`: unique `ColumnAssetRef.Length` bytes in the plan.
- `live_bytes`: active-manifest referenced bytes.
- `stale_bytes`: candidate/superseded bytes.
- `protected_bytes`: unique bytes protected by any non-candidate source.
- `rewrite_debt_bytes`: reclaimable bytes inside mixed or unknown segments.
- `reclaimable_bytes`: whole-segment bytes eligible for GC in a complete plan.
- `active_pin_bytes`: explicit lifecycle pin bytes plus mappedresource pinned bytes.
- `pending_publish_bytes`, `prepared_asset_bytes`, `prepared_query_bytes`,
  `snapshot_pinned_bytes`, `mappedresource_pinned_bytes`, and
  `quarantine_bytes`: source-specific protection counters.

These counters intentionally overlap; they are diagnostics, not a storage-size
sum.

## Whole-DB durable storage label

Typed-column benchmark reports expose `durable_storage_bytes_wal_excluded` for
steady-state comparisons. The label is report-only and subtracts only valid
command WAL segment files named `wal/commit-l<lane>-<seq>.log` (numeric lane,
non-zero sequence) from `db_total_bytes`. Durable payload and index stores
remain counted, including ordinary `value_vlog`, split
`leaf_vlog`, `index.db`, isolated `column_assets/`, and manifest/control bytes.
Manifest/control bytes stay separately labeled from referenced column asset
bytes; the WAL-excluded label must not imply value-log or leaf-log bytes are
ephemeral.

Corrected 100k external context preserved in the #1955/#2165 reporting thread:
full-retained-json with the full prepared typed-column layout/owner,
reconstruction valid, 70,726,513 total bytes, 11,966,546 typed-column-part
bytes, 13,667,330 column asset bytes, 4,194,304 primary-index bytes, and
49,639,046 WAL bytes. Treat those figures as context for PR/report wording, not
as a replacement for fresh normal-profile #1955 evidence.

## Destructive consumer gates

`ColumnAssetGC` and `ColumnAssetRewrite` augment caller-supplied refs with the
shared lifecycle pin/registry snapshot before planning. Destructive consumers
must still require a complete reachability plan. Unknown segments, missing
segments, out-of-bounds refs, unconvertible mappedresource pins, quarantine
segment uncertainty, ambiguous publish state, and stale snapshot fences retain
bytes or return `ErrColumnAssetReachabilityIncomplete`.
