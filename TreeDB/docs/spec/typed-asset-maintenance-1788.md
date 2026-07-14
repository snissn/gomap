# Typed Asset Maintenance Contract (#1788)

Status: implementation note for issue #1788, the post-#1744/#1758 row+column
copy-on-write maintenance pass. The #1954 typed-column lifecycle reuse/state
mapping and JSONBench report contract are documented in
[`typed-column-lifecycle-1954.md`](typed-column-lifecycle-1954.md).

TreeDB typed assets are immutable copy-on-write assets stored under the
compatibility `column_assets` manager. Maintenance operates on asset refs and
segment inventory; it must not scan logical rows or decode full typed-column
part images merely to compute reachability.

## Reachability roots

Typed asset reachability includes all refs exposed by the column manifest view:

- typed-row `tcs1_part_image` refs;
- typed-column `tcs1_typed_column_part` refs;
- derived `tcs1_aggregate_metadata`, `tcs1_dictionary_codes`, and
  `tcs1_int64_values` refs;
- vector-index state refs recorded in `TVIS` vector-index state records;
- legacy vector graph derived refs recorded in vector-graph manifest records;
- explicit maintenance candidates;
- pending-publish, prepared, prepared-query, quarantine, and snapshot-pinned
  refs supplied by callers or the shared lifecycle registry;
- logical quarantine segment records;
- active process-local `mappedresource` pins converted from typed-row and
  typed-column resource keys.

Active and recovery-authoritative manifest refs are both treated as protected.
Candidate-only refs are reclaimable. Any unknown source, malformed ref,
non-canonical segment, missing segment, out-of-bounds range, or unconvertible
active pin makes the plan incomplete and destructive maintenance fails closed.

## Active mappedresource pins

Every `mappedresource.Manager` contributes to a process-wide active pin summary.
`PlanColumnAssetReachability`, `ColumnAssetGC`, and `ColumnAssetRewrite` filter
that summary by the collection's column-asset root and typed asset namespace,
then automatically add convertible `typed_row_asset` / `typed_column_asset` keys
as pinned refs. File-backed pins must carry either `ResourceRoot` or
`ResourcePath`; `AcquireFileRange` fills `ResourcePath` from the opened path, and
the column-asset read cache fills `ResourceRoot` for bytes it registers. Pins
from another DB root are ignored even when namespaces are reused in the same
process. A pin whose typed asset class, root, and namespace are relevant but
whose key cannot be converted to a `ColumnAssetRef` is not ignored: it sets
`MappedResources.UnconvertiblePins` and marks the plan incomplete.

The maintenance plan reports active handle accounting:

- `ActiveHandles`;
- `ActiveMappedBytes`;
- `ActiveHeapCopyBytes`;
- `ActiveDerivedMetadataBytes`;
- `PinnedRefs` and `PinnedBytes`;
- `UnconvertiblePins`;
- cumulative mappedresource `DeniedResources` and `FallbackReads`.

## Destructive actions

`ColumnAssetGC` may delete only canonical whole segments whose bytes are wholly
reclaimable and whose plan is complete. Mixed live/dead segments become rewrite
debt and are retained.

`ColumnAssetRewrite` may copy protected manifest refs out of complete mixed
segments and publish a remapped manifest. It skips any mixed segment that also
contains refs protected by non-manifest sources such as snapshots, pending or
prepared refs, or active mappedresource pins. Rewrite preserves logical asset
identity: kind, namespace, generation, part id, length, and checksum are
unchanged while file id and offset may change.

On platforms with exact relative-namespace persistence, standalone vector/HNSW
rebuild and `ColumnAssetRewrite` acquire DB-scoped stable-resource authority
before mutating a segment. Authority includes the exact child and parent
handles, physical frontier, and namespace durability obligations. Physical
identity may be coalesced, but every logical ref remains an immutable
obligation through publication. The active rebuild closure includes adjacency
state, inverse norms, row refs, document ids, quantized assets, and the HNSW
search pack; physical-graph and legacy-adjacency sources must be added to that
closure if a future writer emits them again.

Authority remains held until the publication backend returns. A command-WAL
retry prepares and pins its successor before releasing the prior attempt, so a
failed replacement leaves the prior authority intact. Failure releases pins
exactly once but retains copied or newly appended segments as persistent GC
orphans; failure cleanup must not remove a pathname that may have been rebound.

Explicit stable-authority operations and destructive rewrite fail with the
typed unsupported-platform error before visibility when exact namespace
persistence is unavailable. Until strict stable-authority activation in the
publication closeout, ordinary vector rebuild retains its legacy compatibility
path on those platforms and makes no stable-resource certification claim.

Maintenance never deletes or rewrites bytes protected by active handles,
snapshots, pending/prepared/prepared-query state, quarantine records, or
uncertain protection state. Releasing the handle, snapshot, or lifecycle lease
permits a later plan to classify the segment normally.

## Non-goals and boundaries

This contract does not introduce query routing, vector-search switching, public
API cleanup, or a new search algorithm. Vector-index state assets, legacy vector
graph assets, and aggregate/dictionary/int64 sidecars remain derived refs tied
to their owning manifest generation. Value-log and leaf-log lifecycle remain
covered by their existing maintenance contracts. It also does not grant
root-candidate authority, discharge command-WAL-prefix durability work, or
declare rebuilt assets query-ready.
