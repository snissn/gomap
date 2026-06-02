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
| `typed-column storage` / `typed_column_part` | Opt-in true sectioned, column-major part assets transplanted from `experiments/colgranule` (scalar durable publication starts in #1755; fixed-dimension vector sections start in #1756). |
| `retained document` / `document_payload` | Flexible document owner or retained residual payload used for reconstruction. |
| `derived accelerator` | Non-authoritative duplicate, index, cache, or metadata derived from an authoritative owner. |

Authoritative field owners are only:

- `retained_document` / `document_payload`;
- `typed_row_asset`;
- `typed_column_part`.

`derived_accelerator` is a classification only. It is not an authoritative field
owner and must not silently become a second source of truth for a logical field.

## Code Vocabulary Contract

The naming regression tests compare this table to Go constants so docs cannot
drift from the runtime vocabulary strings.

| Go symbol or alias | Documented string | Scope |
| --- | --- | --- |
| `TypedStorageOwnerRetainedDocument` | `retained_document` | Authoritative retained-document owner. |
| `document_payload` alias | `document_payload` | Retained-payload/document-owner wording. |
| `TypedStorageOwnerRowAsset` | `typed_row_asset` | Authoritative typed-row asset owner. |
| `TypedStorageOwnerColumnPart` | `typed_column_part` | Authoritative typed-column part owner. |
| `TypedStorageAssetClassDerivedAccelerator` | `derived_accelerator` | Non-authoritative sidecar classification. |
| `ColumnStoreValueBool` | `bool` | Legacy public declared-type compatibility name. |
| `ColumnStoreValueInt64` | `int64` | Legacy public declared-type compatibility name. |
| `ColumnStoreValueFloat32` | `float32` | Legacy public declared-type compatibility name. |
| `ColumnStoreValueDouble` | `double` | Legacy public declared-type compatibility name; docs may also say `float64`. |
| `ColumnStoreValueString` | `string` | Legacy public declared-type compatibility name. |
| `ColumnStoreValueInt8` | `int8` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueUint8` | `uint8` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueInt16` | `int16` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueUint16` | `uint16` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueInt32` | `int32` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueUint32` | `uint32` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueUint64` | `uint64` | Quantization-ready primitive scalar compatibility name. |
| `ColumnStoreValueFloat16` | `float16` | Storage-only raw IEEE binary16 bits compatibility name. |
| `ColumnStoreValueBFloat16` | `bfloat16` | Storage-only raw bfloat16 bits compatibility name. |
| `ColumnStoreValueUint8Vector` | `uint8_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueInt8Vector` | `int8_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueUint16Vector` | `uint16_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueInt16Vector` | `int16_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueUint32Vector` | `uint32_vector` | Dense row-major numeric vector compatibility name; distinct from `adjacency_list`. |
| `ColumnStoreValueInt32Vector` | `int32_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueUint64Vector` | `uint64_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueInt64Vector` | `int64_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueFloat16Vector` | `float16_vector` | Dense row-major raw float16-bit vector compatibility name. |
| `ColumnStoreValueBFloat16Vector` | `bfloat16_vector` | Dense row-major raw bfloat16-bit vector compatibility name. |
| `ColumnStoreValueFloat32Vector` | `float32_vector` | Legacy public declared-type compatibility name. |
| `ColumnStoreValueFloat64Vector` | `float64_vector` | Dense row-major numeric vector compatibility name. |
| `ColumnStoreValueByteVector` | `byte_vector` | Fixed row-byte vector compatibility name. |
| `ColumnStoreValuePackedBitVector` | `packed_bit_vector` | Packed 1-bit code vector compatibility name. |
| `ColumnStoreValuePackedUint2Vector` | `packed_uint2_vector` | Packed 2-bit code vector compatibility name. |
| `ColumnStoreValuePackedUint4Vector` | `packed_uint4_vector` | Packed 4-bit code vector compatibility name. |
| `ColumnStoreValueUint32List` | `uint32_list` | Generic integer-list declared-type compatibility name. |
| `ColumnStoreValueBytes` | `bytes` | Generic opaque byte-payload declared-type compatibility name. |
| `ColumnStoreValueAdjacencyList` | `adjacency_list` | Legacy public declared-type compatibility name. |

## `uint32_list` Compatibility Naming Strategy (#1984)

The first-class generic integer-list logical type is `uint32_list`, with
`uint32[]` as a conceptual spelling alias and `Array(UInt32)` as the ClickHouse
reference model. Its v1 physical encoding is `raw_uint32_offsets_list`: explicit
sentinel offsets (`rows+1`, `offsets[0] == 0`) plus flattened little-endian
`uint32` values.

The preferred public compatibility symbol is `ColumnStoreValueUint32List` with
documented string `uint32_list`. Issue #1985 adds the runtime
writer/reader/direct-view implementation and updates the code vocabulary table,
naming regression tests, adapter admission, and conformance evidence in the same
PR.

`ColumnStoreValueAdjacencyList` remains the legacy graph-adjacency compatibility
name. It must not be reused as the generic `uint32_list` primitive, and
`adjacency_list` must remain classified as consumer-specific/legacy rather than a
first-class datastore list type.

## `bytes` Compatibility Naming Strategy (#2010)

The first-class generic opaque binary logical type is `bytes`; its v1 physical
encoding is `raw_bytes_offsets`: explicit sentinel offsets (`rows+1`,
`offsets[0] == 0`) plus exact concatenated byte payloads. The preferred public
compatibility symbol is `ColumnStoreValueBytes` with documented string `bytes`.
The name is intentionally consumer-neutral and must not be specialized to vector
search result IDs, graph row IDs, or text/string semantics.

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
be represented in metadata; after #1755/#1756 and the #1929 primitive scalar
admission, scalar bool/int64/float32/double/string, non-null primitive
int8/uint8/int16/uint16/int32/uint32/uint64/float16/bfloat16 owners, and
fixed-dimension `float32_vector` owners have opt-in durable publication and
reconstruction, while unsupported value types still fail closed.

The resolver is pure metadata only: it must not perform filesystem IO, mmap,
section decode, DB mutation, asset open, publication, query planning, or durable
typed-column format work.

## PR 2 Umbrella Rename Cleanup

Issue #1752 is a naming cleanup only. It must not start the colgranule
transplant, durable typed-column part format, query-planner rewrite, production
storage behavior change, or #1736 resource-manager work.

After PR 2, new umbrella docs, comments, tests, status labels, and debug output
should use `typed storage`, `TypedStorage*`, `typed-row`, or `typed-column`
terminology. Public/exported `ColumnStore*` names remain compatibility-retained
unless a future PR adds wrappers/aliases and a deprecation plan.

Remaining legacy names must fall into one of these classes:

| Class | Allowed occurrences | Reason |
| --- | --- | --- |
| compatibility-retained | Public API/config names such as `ColumnStoreConfig`, durable manifest/config fields, and compatibility tests that prove existing users still compile. | Avoid public API and metadata breakage while typed-storage names are introduced internally. |
| true typed-column terminology | `experiments/colgranule` and future sectioned, column-major `typed_column_part` data-plane terms. | Preserve the coherent typed-column data plane for #1753 transplant work. |
| deferred | Historical RFCs, vector-search reconstruction notes, and large implementation files deferred to #1752 follow-ups. | Avoid broad mechanical churn or format claims before typed-column publication exists. |

## PR 3 Typed-Column Data-Plane Transplant

Issue #1753 introduces `TreeDB/internal/typedcolumn` as an internal,
non-authoritative transplant of the `experiments/colgranule` data plane. This is
true typed-column terminology, but it is not production publication and does not
make `typed_column_part` an authoritative owner yet. The transplant deliberately
uses package-local `Options`/`Batch` names instead of adding new public or
internal `ColumnStore*` umbrella names.

## PR 4 Typed-Column Adapter

Issue #1754 introduces `TreeDB/collections/typed_column_adapter.go` as an
adapter seam from TreeDB typed-storage metadata and #1736 resource handles to
`TreeDB/internal/typedcolumn`. It may use `ColumnStoreValueType` as
compatibility input, but existing `ColumnStoreConfig` metadata still resolves to
`typed_row_asset` unless a column explicitly selects `typed_column_part`.

## PR 5 Durable Scalar Typed-Column Publication

Issues #1755/#1756 let explicit scalar and fixed-dimension `float32_vector`
`typed_column_part` owners publish durable `tcs1_typed_column_part` assets and
reconstruct retained-payload documents after reopen. A compatibility
`typed_row_asset`/`TCPA` asset remains present per mutation generation as the
row-ID/tombstone locator and owner of any `typed_row_asset` fields. The invariant
remains one authoritative owner per logical field per generation: retained
document, typed-row asset, or typed-column part.

The #1756 path is intentionally not the native vector graph/query switch.
Production adjacency publication and predicate scan / query integration remain
later #1744 children (#1757 for the scalar predicate MVP).

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
rg -n "ColumnStore|column store|column-store" TreeDB docs experiments
```

The repository currently has many legacy `ColumnStore*`, "column store", and
"column-store" references. This table classifies the major current groups. A PR
that touches a specific legacy occurrence must include the exact touched
occurrence in its PR inventory.

`TestTypedStorageLegacyNameAllowlistIsComplete` is the executable inventory for
this contract: every match from the audit command must map to one of the
classifications below, and line/occurrence count drift must update the
classification explanation in the PR.

| Path | Symbol/text | Current meaning | Classification | Action | Deferral reason |
| --- | --- | --- | --- | --- | --- |
| `TreeDB/collections/api.go` | `ColumnStoreConfig`, `ColumnStoreColumn`, `ColumnStoreValueType`, retained-payload options | Public compatibility configuration for declared typed fields and retained payload behavior. | compatibility-retained | Keep as compatibility input normalized by the typed-storage layout resolver. | Public API compatibility; do not remove in PR 2. |
| `TreeDB/collections/column_store.go` and related publish/reconstruction files | `ColumnStore*` implementation names and comments | Production declared-field extraction, retained-payload handling, reconstruction, and publication control plane currently attached to typed-row assets. | compatibility-retained | Document as typed-storage/typed-row behavior; rename/wrap incrementally after the public compatibility seam is stable. | Avoid behavior or public API change in PR 2. |
| `TreeDB/collections/column_physical_asset.go`, `column_physical_*.go`, `column_physical_row_reader.go` | column physical asset / `TCPA` wording | Current row-record physical asset for declared typed values. | deferred | Treat as `typed_row_asset`; broad file/symbol rename remains deferred until it can be split without obscuring behavior changes. | Large implementation surface; PR 2 only cleans safe umbrella names. |
| `TreeDB/collections/column_asset_manager.go`, `column_asset_reachability.go`, `column_asset_gc.go`, `column_asset_rewrite.go` | column asset manager / reachability / GC / rewrite wording | Typed physical asset manager rooted in current typed-row assets and future typed-column refs. | compatibility-retained | Keep behavior; align terminology as typed-storage resource management in follow-ups. | #1736/#1755 own deeper resource and maintenance integration. |
| `TreeDB/collections/column_dictionary_codes_asset.go` | dictionary-code asset names | Derived code sidecar for declared values. | derived accelerator | Keep as non-authoritative sidecar; require owner/generation association in later work. | Query integration and metadata rules belong to later #1744 children. |
| `TreeDB/collections/column_int64_values_asset.go` | int64-values asset names | Derived values sidecar for declared values. | derived accelerator | Keep as non-authoritative sidecar; require owner/generation association in later work. | Query integration and metadata rules belong to later #1744 children. |
| `TreeDB/collections/column_vector_graph_*.go`, `vector_index*.go` | vector graph / native column wording | Derived vector graph/search structures and row readers over current assets. | derived accelerator | Keep as non-authoritative accelerator; future dense typed-column sections are #1756. | Do not change vector product path in PR 0. |
| `experiments/colgranule/**` | `ColumnStoreOptions`, `ColumnPart*`, column/granule descriptors | Coherent experimental typed-column data plane to transplant. | true typed-column terminology | Preserve for #1753 transplant; do not rename in PR 0. | Avoid thrashing the experiment before transplant. |
| `TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md` | broad "column-store" roadmap wording | Historical/pre-typed-storage RFC that predates the umbrella rename. | deferred | Keep as historical supporting material until a dedicated docs rewrite. | Large historical document; changing it here would obscure PR 0. |
| `TreeDB/docs/spec/column-graph-native-*.md` | column-store-native vector wording | Historical/vector tracker docs for rebuilding native vector search. | deferred | Keep historical references; future vector typed-column docs belong to #1756. | Do not change vector path in PR 0. |
| `TreeDB/docs/guides/**` | current public compatibility config and value-type symbols in copy/paste examples | User-facing typed-storage quickstarts and benchmark guides that must show current runnable APIs while describing them as compatibility-retained/pre-alpha. | compatibility-retained | Keep examples accurate; surrounding prose should use typed-storage, typed-row, typed-column, and retained-document wording. | Public examples must compile against current APIs without implying stable names. |
| `TreeDB/docs/spec/storage-format.md`, `docs/TREEDB_STORAGE_FORMAT.md`, backup/recovery/verification docs | typed asset manager and `column_assets` compatibility directory wording | Storage-format and recovery docs describing existing/future typed physical assets. | compatibility-retained | Keep the on-disk directory name; use typed-storage wording around it. | Directory and manifest names are durable compatibility metadata. |

## PR 0 Runtime Boundary

This naming scaffold must preserve existing runtime behavior:

- no durable typed-column part format;
- no `ColumnStoreConfig` removal;
- no public API break;
- no query planner change;
- no production data-path behavior change;
- no #1736 resource-manager behavior change.

## Naming Regression Test Boundary

Issue #1773 adds regression coverage for this naming contract only. It is
limited to docs, tests, and process evidence: repo-scan allowlist updates,
spec link checks, and code-vocabulary alignment checks. A future exception that
needs production behavior, public API, storage format, query planning, or
publication changes must move to a separate implementation tracker and document
why this naming guard is insufficient.
