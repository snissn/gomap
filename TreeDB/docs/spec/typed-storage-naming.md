# TreeDB Typed Storage Naming

This document is the PR 0 naming scaffold for issue #1750 under the #1744
typed-storage tracker. It is intentionally a vocabulary and inventory contract
only. It does not define a new durable on-disk format, remove legacy public
APIs, alter query planning, or change runtime behavior.

## Naming Contract

`typed storage` is the umbrella name for TreeDB's non-document physical storage
for schema-declared typed fields. Do not use "column store" as the umbrella name
for that superset.

Required vocabulary:

| Term | Meaning |
| --- | --- |
| `typed storage` | Umbrella subsystem for typed-row storage and typed-column storage. |
| `typed-row storage` / `typed_row_asset` | Current row-record physical asset path for declared typed values. |
| `typed-column storage` / `typed_column_part` | Future true sectioned, column-major part assets transplanted from `experiments/colgranule`. |
| `retained document` / `document_payload` | Flexible document owner or retained residual payload used for reconstruction. |
| `derived accelerator` | Non-authoritative duplicate, index, cache, or metadata derived from an authoritative owner. |

Authoritative field owners are only:

- `retained_document` / `document_payload`;
- `typed_row_asset`;
- `typed_column_part`.

`derived_accelerator` is a classification only. It is not an authoritative field
owner and must not silently become a second source of truth for a logical field.

## PR 1 Layout Resolver Contract

Issue #1751 adds the pure-metadata resolver surface that turns collection
metadata into explicit typed-storage ownership rows. The implementation names are
canonical for this seam:

- `TypedStorageLayout`
- `TypedStorageFieldOwner`
- `TypedStorageOwnerRetainedDocument`
- `TypedStorageOwnerRowAsset`
- `TypedStorageOwnerColumnPart`
- `TypedStorageAssetClass`
- `TypedStorageAssetClassDerivedAccelerator`
- `ResolveTypedStorageLayout`
- `NormalizeTypedStorageLayout`

Compatibility normalization keeps `ColumnStoreConfig` as input metadata. Existing
declared columns with no explicit typed-storage owner resolve to
`typed_row_asset`. `ColumnRetainedPayloadFull` is compatibility duplication in
`document_payload`; it does not make the retained document a second
authoritative owner for declared typed fields. `typed_column_part` ownership may
be represented as a placeholder, but reads and publication must fail closed until
the typed-column format/publication work lands.

The resolver is pure metadata only: it must not perform filesystem IO, mmap,
section decode, DB mutation, asset open, publication, query planning, or durable
typed-column format work.

## Current Derived Accelerator Classifications

These existing assets are derived accelerators unless a future format explicitly
promotes them and updates the typed-storage ownership contract:

| Asset family | Classification | Authoritative owner relationship |
| --- | --- | --- |
| dictionary-code assets | `derived_accelerator` | Derived from a typed-row or typed-column authoritative value owner. |
| int64-values assets | `derived_accelerator` | Derived from a typed-row or typed-column authoritative value owner. |
| aggregate metadata | `derived_accelerator` | Derived metadata tied to an authoritative owner/generation. |
| vector graph assets | `derived_accelerator` | Derived graph/search structure tied to vector field ownership. |
| read caches and decoded metadata caches | `derived_accelerator` | Runtime acceleration over authoritative storage. |

## Legacy Name Inventory

Audit command used for the initial inventory:

```sh
rg -n "ColumnStore|column store|column-store" TreeDB experiments docs
```

The repository currently has many legacy `ColumnStore*`, "column store", and
"column-store" references. This table classifies the major current groups. A PR
that touches a specific legacy occurrence must include the exact touched
occurrence in its PR inventory.

| Path | Symbol/text | Current meaning | Classification | Action | Deferral reason |
| --- | --- | --- | --- | --- | --- |
| `TreeDB/collections/api.go` | `ColumnStoreConfig`, `ColumnStoreColumn`, `ColumnStoreValueType`, retained-payload options | Public compatibility configuration for declared typed fields and retained payload behavior. | compatibility-retained | Keep as compatibility input; #1751 should normalize it through typed-storage layout names. | Public API compatibility; do not remove in PR 0. |
| `TreeDB/collections/column_store.go` and related publish/reconstruction files | `ColumnStore*` implementation names and comments | Production declared-field extraction, retained-payload handling, reconstruction, and publication control plane currently attached to typed-row assets. | compatibility-retained | Document as typed-storage/typed-row behavior; rename/wrap later under #1751/#1752. | Avoid behavior or public API change in PR 0. |
| `TreeDB/collections/column_physical_asset.go`, `column_physical_*.go`, `column_physical_row_reader.go` | column physical asset / `TCPA` wording | Current row-record physical asset for declared typed values. | rename-now when touched as typed-row terminology | Treat as `typed_row_asset`; broad rename deferred to #1752. | Current PR is naming scaffold only. |
| `TreeDB/collections/column_asset_manager.go`, `column_asset_reachability.go`, `column_asset_gc.go`, `column_asset_rewrite.go` | column asset manager / reachability / GC / rewrite wording | Typed physical asset manager rooted in current typed-row assets and future typed-column refs. | compatibility-retained | Keep behavior; align terminology as typed-storage resource management in follow-ups. | #1736/#1755 own deeper resource and maintenance integration. |
| `TreeDB/collections/column_dictionary_codes_asset.go` | dictionary-code asset names | Derived code sidecar for declared values. | derived accelerator | Keep as non-authoritative sidecar; require owner/generation association in later work. | Query integration and metadata rules belong to later #1744 children. |
| `TreeDB/collections/column_int64_values_asset.go` | int64-values asset names | Derived values sidecar for declared values. | derived accelerator | Keep as non-authoritative sidecar; require owner/generation association in later work. | Query integration and metadata rules belong to later #1744 children. |
| `TreeDB/collections/column_vector_graph_*.go`, `vector_index*.go` | vector graph / native column wording | Derived vector graph/search structures and row readers over current assets. | derived accelerator | Keep as non-authoritative accelerator; future dense typed-column sections are #1756. | Do not change vector product path in PR 0. |
| `experiments/colgranule/**` | `ColumnStoreOptions`, `ColumnPart*`, column/granule descriptors | Coherent experimental typed-column data plane to transplant. | true typed-column terminology | Preserve for #1753 transplant; do not rename in PR 0. | Avoid thrashing the experiment before transplant. |
| `TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md` | broad "column-store" roadmap wording | Historical/pre-typed-storage RFC that predates the umbrella rename. | deferred | Keep as historical supporting material until a dedicated docs rewrite. | Large historical document; changing it here would obscure PR 0. |
| `TreeDB/docs/spec/column-graph-native-*.md` | column-store-native vector wording | Historical/vector tracker docs for rebuilding native vector search. | deferred | Keep historical references; future vector typed-column docs belong to #1756. | Do not change vector path in PR 0. |
| `TreeDB/docs/spec/storage-format.md`, `docs/TREEDB_STORAGE_FORMAT.md`, backup/recovery/verification docs | future/physical column-store asset wording | Storage-format and recovery docs describing existing/future typed physical assets. | deferred | Update after typed-storage layout/publication work clarifies exact durable terms. | Avoid changing format claims before #1753/#1755. |

## PR 0 Runtime Boundary

This naming scaffold must preserve existing runtime behavior:

- no durable typed-column part format;
- no `ColumnStoreConfig` removal;
- no public API break;
- no query planner change;
- no production data-path behavior change;
- no #1736 resource-manager behavior change.
