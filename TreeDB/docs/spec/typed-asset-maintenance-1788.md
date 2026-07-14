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

The production central column publish path captures the exact open `.tca`
segment after its existing content sync and before close. All row image, typed
part, aggregate metadata, dictionary-code, and int64-value refs in that batch
are retained as immutable logical obligations in one coalesced stable-resource
set at the greatest per-class byte frontiers. The unclaimed set remains
owned only through the command publish boundary; collection-root/candidate
authority is deliberately left to #3679. Query-ready assets remain rebuildable
and non-authoritative.

Within that set, compatibility row-image `tcs1_part_image` refs are
column-manifest assets (`column.manifest_asset_ref`), while
`tcs1_typed_column_part` refs are typed multipart assets
(`column.typed_multipart_ref`). Sharing one physical `.tca` identity does not
collapse those distinct reachability obligations: the set retains two authority
descriptors and two pins over one observed physical identity.

Those producer pins and destructive GC/rewrite cleanup share one DB-scoped
identity registry. A pinned identity cannot acquire a delete lease. Deletion
opens the exact parent and child, validates that the retained child is still
linked at the canonical name, and uses a parent-handle-relative unlink. TreeDB
serializes its own column namespace mutations with the collection mutation and
segment locks. An external actor that can rebind a name after retained-link
validation is outside this in-process serialization boundary and remains
explicit hardening work.

## Windows capability boundary

Windows does not currently have a supported exact retained-parent namespace
persistence primitive. Production central-column writes therefore remain
fail-closed with `rootpublication.ErrNamespacePersistenceUnsupported`; they do
not publish manifest metadata or a manifest root and do not retain identity
pins. The Windows core CI shard explicitly runs the focused first-publish
capability test for this contract.

The complete `github.com/snissn/gomap/TreeDB/collections` and
`github.com/snissn/gomap/TreeDB/documentservice` package suites are unsupported
on Windows until a real namespace persistence primitive exists. Their
functional suite coverage remains on Linux and macOS. Windows CI continues to
run the TreeDB root and DB suites, the dedicated caching shards, and every other
package; the two exact package exclusions are checked and logged by the
centralized Windows core package router.

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
snapshots, pending/prepared/prepared-query state, quarantine records, or
uncertain protection state. Releasing the handle, snapshot, or lifecycle lease
permits a later plan to classify the segment normally.

## Non-goals and boundaries

This contract does not introduce query routing, vector-search switching, public
API cleanup, or a new search algorithm. Vector-index state assets, legacy vector
graph assets, and aggregate/dictionary/int64 sidecars remain derived refs tied
to their owning manifest generation. Value-log and leaf-log lifecycle remain
covered by their existing maintenance contracts.

The standalone `tcs1_hnsw_search_pack` rebuild writer does not use the central
batch session yet. Its token classification remains fail-closed inventory, but
production capture/composite authority for that separate writer is remaining
issue `#3677` work rather than evidence supplied by the central-batch
implementation.
