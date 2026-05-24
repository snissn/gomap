# Typed Asset Maintenance Contract (#1788)

Status: implementation note for issue #1788, the post-#1744/#1758 row+column
copy-on-write maintenance pass.

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
- vector graph derived refs recorded in vector-graph manifest records;
- explicit maintenance candidates;
- pending-publish, prepared, and snapshot-pinned refs supplied by callers;
- active process-local `mappedresource` pins converted from typed-row and
  typed-column resource keys.

Active and recovery-authoritative manifest refs are both treated as protected.
Candidate-only refs are reclaimable. Any unknown source, malformed ref,
non-canonical segment, missing segment, out-of-bounds range, or unconvertible
active pin makes the plan incomplete and destructive maintenance fails closed.

## Active mappedresource pins

Every `mappedresource.Manager` contributes to a process-wide active pin summary.
`PlanColumnAssetReachability`, `ColumnAssetGC`, and `ColumnAssetRewrite` filter
that summary by typed asset namespace and automatically add convertible
`typed_row_asset` / `typed_column_asset` keys as pinned refs. A pin whose typed
asset class and namespace are relevant but whose key cannot be converted to a
`ColumnAssetRef` is not ignored: it sets `MappedResources.UnconvertiblePins` and
marks the plan incomplete.

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

Maintenance never deletes or rewrites bytes protected by active handles,
snapshots, pending/prepared state, or uncertain protection state. Releasing the
handle or snapshot permits a later plan to classify the segment normally.

## Non-goals and boundaries

This contract does not introduce query routing, vector-search switching, public
API cleanup, or a new authoritative vector graph owner. Vector graph assets and
aggregate/dictionary/int64 sidecars remain derived refs tied to their owning
manifest generation. Value-log and leaf-log lifecycle remain covered by their
existing maintenance contracts.
