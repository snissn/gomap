# Production Typed-Column JSONBench Plan

Status: planning note for promoting the `experiments/colgranule` design into
production TreeDB collection typed-column storage.

TreeDB is pre-alpha. This plan may change on-disk formats and public collection
query APIs. If an implementation changes persisted typed-column formats, update
the relevant specs and tests in the same change; do not add complex migration
scaffolding unless the specific change requires it.

## Goal

Build the production collection typed-column path around the same feature set proven
in `experiments/colgranule`, then make JSONBench q1 through q5 exercise those
features through production APIs.

The priority is not just to make prepared, repeated-query benchmark rows fast.
The one-shot direct API must become fast and clear enough that ad-hoc physical
queries do not pay avoidable setup, mapping, allocation, or document
materialization costs.

JSONBench parity must use real query predicates over stored columns. In
particular, q2 must store full `kind`, `operation`, `collection` (reported as
`event`), and `did` values, then execute `kind = commit` and
`operation = create` predicates at query time. Do not regain ClickHouse-like
numbers by write-time masking,
sentinel substitution, or benchmark-specific q2 pre-filtering.

## Terms

Use these terms consistently in code, docs, benchmark names, and diagnostics.

### Storage Containers

#### Collection Typed Storage

The production collection feature configured by `ColumnStoreConfig` in
`TreeDB/collections/column_store.go`.

It is not one file. It is the collection-level configuration and publish/query
machinery that decides which declared JSON fields become typed physical storage.
On disk, the physical data is published as column asset manager records and
manifest records.

Important code:

- `TreeDB/collections/column_store.go`
- `TreeDB/collections/column_publish_write.go`
- `TreeDB/collections/column_manifest_format.go`

#### Column Asset

A durable byte object owned by the production column asset manager.

It is stored as a byte range in a segment file, not necessarily as a whole file.
The address is a `ColumnAssetRef`:

- `Kind`: storage format, for example `tcs1_typed_column_part`
- `Namespace`: asset-manager namespace
- `Generation`: column manifest generation
- `PartID`: logical part id inside the generation
- `FileID`: segment file id
- `Offset`: byte offset inside the segment file
- `Length`: byte count
- `Checksum`: payload checksum

Segment files live under:

```text
<column-asset-root>/<namespace>/assets/segments/segment-%06d.tca
```

The current constants are:

- row asset part id: `columnPhysicalRowAssetPartID == 1`
- primary typed-column part id: `typedColumnPartAssetPartID == 2`
- default segment file id: `columnAssetM12ASegmentFileID == 1`
- direct-view segment file ids start at `1 << 20`

Important code:

- `TreeDB/collections/column_publish_plan.go`
- `TreeDB/collections/column_asset_manager.go`
- `TreeDB/collections/column_manifest_format.go`

#### Typed-Row Asset

The current production row-oriented compatibility asset.

On disk it is a `ColumnAssetKindTCS1PartImage` asset, written by
`writeColumnPhysicalAssetToManager`, usually with part id `1`. It is a byte
range in a column asset segment file. Its binary format is the `TCPA` physical
asset format in `TreeDB/collections/column_physical_asset.go`.

It stores:

- document id bytes for each row
- deleted/tombstone state for each row
- declared column values owned by the row-asset projection
- enough data for physical row scans, visibility, and reconstruction

Rows:

- row count is the number of rows in the publish batch for that generation
- insert/update/delete operations all publish row assets
- row order is the row asset order, not a declared sort-key order

Size:

- variable header plus variable row/document/value payloads
- zero-row-column parts with fixed-width document ids can use the TCPA V7
  `fixed_id` row encoding, which stores fixed-width ids and derives deleted
  state from the asset operation
- zero-row-column parts with dense 8-byte big-endian uint64 document ids can use
  the TCPA V8 `dense_id_range` row encoding, which stores only the base id and
  derives every row id by ordinal
- `ColumnPreparedAsset.Bytes` and `ColumnAssetRef.Length` are the authoritative
  stored byte counts

Important code:

- `TreeDB/collections/column_physical_asset.go`
- `TreeDB/collections/column_publish_write.go`
- `TreeDB/collections/typed_column_publication.go`

#### Typed-Column Part

A production column-oriented physical part for fields owned by
`typed_column_part`.

On disk it is a `ColumnAssetKindTCS1TypedColumnPart` asset, usually with part id
`2`. The payload is a typed-column `ColumnPartImage` byte image. The asset is a
byte range in a column asset segment file.

It stores:

- one shared physical row order
- a reserved internal primary-id column named `__treedb_primary_id`
- one encoded column payload per declared typed-column field
- descriptors, granules, row locators, optional dictionaries, optional stats,
  pruning metadata, and layout-contract metadata

Rows:

- one typed-column part image currently covers the rows in one publish batch
- all columns in the part have the same row count
- rows are split into granules; default granule size is `8192` rows
- the last granule may contain fewer rows

Current production support:

- when every declared sort-key column is owned by `typed_column_part`, is ascending,
  non-null, uses a supported bool/int64/string carrier, and has at most
  `typedColumnPartSortKeyMaxColumns == 8` columns, publication sorts typed-column
  rows by that logical `ColumnStoreConfig.SortKey`
- mixed-owner sort-key layouts keep typed-column parts in `__treedb_primary_id`
  order and do not advertise typed-column sort metadata
- typed-column-owned unsupported, nullable, descending, or wider-than-8 sort-key
  layouts fail closed until their ordering semantics are specified
- production typed-column physical queries validate typed-column part marks and
  use equality sorted-prefix pruning for supported `SortKey` predicates, with
  explicit diagnostics and fallback reasons; compatibility dictionary-code/
  int64-value asset query paths and JSONBench per-column asset paths still do not
  consume typed-column part marks

Important code:

- `TreeDB/collections/typed_column_adapter.go`
- `TreeDB/collections/typed_column_publication.go`
- `TreeDB/internal/typedcolumn/part.go`
- `TreeDB/internal/typedcolumn/part_image.go`

#### Column Part Image

The serialized bytes of one typed-column part.

It is not a separate filesystem file by itself. In production it is the payload
of a `tcs1_typed_column_part` column asset. In the reference experiment it is
the payload stored by the in-memory or workspace asset store as
`tcs1_part_image`.

The image begins with a manifest/directory, then named sections. Sections are
8-byte aligned in the current internal typed-column format.

Rows and size:

- `ColumnPartImage.Rows` is the row count for the whole part
- `ColumnPartImage.TotalBytes()` is the serialized image size
- section sizes are visible through `ColumnPartImage.Sections`

Important code:

- `TreeDB/internal/typedcolumn/part_image.go`
- `TreeDB/internal/typedcolumn/part_image_decode.go`
- `experiments/colgranule/part_image.go`
- `experiments/colgranule/part_image_decode.go`

#### Section

A contiguous named byte range inside a column part image.

It is not a file and it is not a manifest record. It is an offset/length entry
inside the column part image directory.

Current section fields include:

- `Kind`, for example `column_data`, `sort_key_marks`, or `dictionaries`
- `Category`, for byte accounting
- `Column`, for column-owned sections
- `Offset` and `Length`, byte range inside the image
- optional `Rows`, `Granules`, `Blocks`, `Encoding`, `Compression`, and
  `RawBytes` for compressed sections

Common section kinds:

| Section kind | What it contains |
| --- | --- |
| `descriptor` | Part-level row count, granule descriptors, and column block descriptors. |
| `sort_key_metadata` | Sort-key column definitions for the part. |
| `sort_key_marks` | Per-granule sort-key prefix bounds. |
| `row_locators` | Mapping from logical primary id to physical row/granule position. |
| `dictionaries` | Distinct string values for low-cardinality string columns. |
| `aggregate_metadata` | Typed-column aggregate summaries, including the experiment's per-granule metadata model. |
| `column_stats` | Column-level statistics used by typed-column scans. |
| `pruning_metadata` | Additional pruning metadata for typed-column scans. |
| `layout_contract` | Direct-view certification metadata. |
| `column_data` | Encoded payload bytes for a fixed-width or scalar column. |
| `column_offsets` / `column_values` | Split offsets/value payloads for variable-length list-like columns. |

Important code:

- `TreeDB/internal/typedcolumn/part_image.go`
- `TreeDB/internal/typedcolumn/section_reader.go`

### Row Layout Terms

#### Physical Row Number

The zero-based row position inside one physical part.

Every column payload in a typed-column part uses the same physical row number.
This is why a q2-style kernel can read `operation_code[row]` and `did_code[row]`
without reconstructing a JSON document.

Physical row numbers are local to a part. A multipart query must combine
`Generation`, `PartID`, and row number, then apply visibility rules.

#### Logical Primary ID

The stable integer id used internally to identify a row/document version.

In the typed-column adapter it is stored in the reserved column
`__treedb_primary_id`. It is used for deterministic sort tie-breaking, row
locator lookup, and visibility joins back to row-oriented physical assets.

Important code:

- `TreeDB/collections/typed_column_adapter.go`

#### Row Locator

A lookup entry from logical primary id to physical location inside one
typed-column part.

It is not a file. In a typed-column part image it is serialized in the
`row_locators` section. Each entry records:

- primary id
- part id
- physical row number
- granule ordinal
- row number inside that granule

Rows and size:

- there is one locator per row in the typed-column part
- size is proportional to row count
- the authoritative serialized size is the `row_locators` section length

Important code:

- `TreeDB/internal/typedcolumn/part.go`
- `TreeDB/internal/typedcolumn/part_image.go`

#### Sort Key

The declared column order that should determine physical row order inside a
typed-column part.

Example JSONBench sort keys:

- `time_us`
- `(kind, operation, collection, did, time_us)`

Rows:

- the sort key orders rows inside one part by logical column values, not by
  arbitrary per-part dictionary code assignment
- dictionary-code carriers may only be used for sort comparison when their code
  ordering is proven to preserve the declared logical value ordering, or when
  comparison consults the dictionary values explicitly
- nullable, missing, and default values must have a documented ordering before
  a column can participate in a persisted sort key
- equal sort-key values must tie-break by logical primary id so the order is
  deterministic

Current production support:

- `ColumnStoreConfig.SortKey` is validated and hashed
- production typed-column publishing physically sorts by the declared SortKey
  when the full key is typed-column-owned, ascending, non-null, uses a
  supported bool/int64/string carrier, and has at most
  `typedColumnPartSortKeyMaxColumns == 8` columns
- mixed-owner layouts fall back to `__treedb_primary_id` order and do not
  advertise typed-column SortKey metadata
- typed-column-owned nullable, descending, unsupported, or wider-than-8 keys
  fail closed until their ordering semantics are specified

Important code:

- reference: `experiments/colgranule/part.go`
- internal data plane: `TreeDB/internal/typedcolumn/part.go`
- production adapter support: `TreeDB/collections/typed_column_adapter.go`

#### Granule

A contiguous range of physical rows inside a typed-column part.

It is not a file and not an asset. It is a row-range descriptor plus associated
metadata. It is the unit we want to use for pruning and metadata-driven query
shortcuts.

Rows:

- default maximum rows per granule: `8192`
- final granule may be shorter
- adaptive mark sizing can choose a smaller rows-per-granule target
- each granule has `FirstRow`, `RowCount`, `VisibleRows`, and related bounds

Size:

- granule descriptors are part of the `descriptor` section
- sort-key mark data for a granule is in the `sort_key_marks` section
- encoded column bytes for rows in a granule are in column `column_data` payloads
  when codec blocks align with granules

Important code:

- `TreeDB/internal/typedcolumn/part.go`
- `TreeDB/internal/typedcolumn/granule.go`
- `TreeDB/internal/typedcolumn/adaptive_mark.go`
- `experiments/colgranule/part.go`

#### Sort-Key Mark

Per-granule metadata that summarizes the sort-key range covered by that
granule.

It is not a file. In a typed-column part image it is stored in the
`sort_key_marks` section. Each mark has the same row count as its granule and
contains prefix summaries. For a sort key `(a, b, c)`, the mark can summarize:

- prefix `(a)`
- prefix `(a, b)`
- prefix `(a, b, c)`

Each prefix summary has a lower bound and an upper-exclusive bound. A query with
a compatible range predicate can skip a granule when the requested range cannot
overlap the mark.

Rows and size:

- one mark per granule
- rows per mark equals rows in that granule
- size is proportional to number of granules times number of sort-key prefixes
  and stored bound values
- the authoritative serialized size is the `sort_key_marks` section length

Current production limitation:

- the internal typed-column format has marks
- the production JSONBench dictionary/int64 query path does not yet use marks
  for prefix pruning

Important code:

- `TreeDB/internal/typedcolumn/predicate.go`
- `TreeDB/internal/typedcolumn/part.go`
- `experiments/colgranule/predicate.go`

### Column Payload Terms

#### Dictionary

The distinct string table for one low-cardinality string column.

It is not row-aligned. If a column has 819,200 rows but only four distinct
values, the dictionary has four entries. Row payloads store small integer codes
that refer to entries in this table.

Production compatibility asset shape:

- the dictionary is stored inside the `tcs1_dictionary_codes` asset header
  before the row-aligned code payload
- there is one such asset per declared dictionary string column, when the row
  batch is eligible

Typed-column part image shape:

- dictionaries are stored in the `dictionaries` section
- dictionary sections may be Snappy/LZ4-compressed in current typed-column image
  version 4, with raw-byte metadata used for fail-closed decode validation
- dictionary-encoded rows use low-cardinality integer code payloads in column
  data sections

Size:

- variable, based on cardinality and string bytes
- not proportional to row count except through higher cardinality

Important code:

- `TreeDB/collections/column_dictionary_codes_asset.go`
- `TreeDB/internal/typedcolumn/part_image.go`
- `experiments/colgranule/part_image.go`

#### Dictionary Code Payload

The row-aligned integer-code array for one dictionary string column.

This is the term to use instead of the ambiguous phrase "dictionary sidecar."
When discussing a concrete implementation, say which storage shape is meant:

- production compatibility dictionary-code asset
- typed-column part image column data section

Production compatibility asset shape:

- asset kind: `ColumnAssetKindTCS1DictionaryCodes`
- format magic: `TCDC`
- stored as a byte range in a column asset segment file
- one asset per eligible declared dictionary string column
- manifest key is by generation, part id, and column name
- current published part id is the row asset part id, `1`

Rows:

- one `uint32` code per row
- the asset is currently only built when every row in the batch is non-deleted
  and the column value is present, non-null, and string-typed
- if any row is deleted, missing, null, or incompatible, the compatibility asset
  is not published for that column

Size:

- variable header and dictionary strings
- padding to 4-byte payload alignment
- payload size is exactly `4 * row_count` bytes
- total stored size is `ColumnAssetRef.Length`

Typed-column part image shape:

- low-cardinality code bytes are column payload bytes inside a `column_data`
  section for that column
- dictionary entries are in the `dictionaries` section
- compact code blocks can use narrower integer encodings, and the column-data
  section compression records the requested codec separately from each block's
  actual keep-if-smaller result
- codec blocks may be smaller or larger than granules, but JSONBench should
  normally start with one codec block per granule

Important code:

- production: `TreeDB/collections/column_dictionary_codes_asset.go`
- direct/prepared use: `TreeDB/collections/column_dictionary_query.go`
- typed-column section model: `TreeDB/internal/typedcolumn/part_image.go`

#### Int64 Value Payload

The row-aligned integer payload for one non-null int64 column.

This is the term to use instead of the ambiguous phrase "int64 sidecar." When
discussing a concrete implementation, say which storage shape is meant:

- production compatibility int64-value asset
- typed-column part image column data section

Production compatibility asset shape:

- asset kind: `ColumnAssetKindTCS1Int64Values`
- format magic: `TCI8`
- stored as a byte range in a column asset segment file
- one asset per eligible declared non-null int64 column
- manifest key is by generation, part id, and column name
- current published part id is the row asset part id, `1`

Rows:

- one `int64` value per row
- the asset is currently only built when every row in the batch is non-deleted
  and the value is present, non-null, and int64-typed
- if any row is deleted, missing, null, or incompatible, the compatibility asset
  is not published for that column

Size:

- variable header
- padding to 8-byte payload alignment
- payload size is exactly `8 * row_count` bytes
- total stored size is `ColumnAssetRef.Length`
- on little-endian aligned hosts, query code can direct-view the payload as a
  `[]int64`; otherwise it copies into an owned slice

Typed-column part image shape:

- raw or encoded int64 bytes are inside a `column_data` section for the column
- encoding may be raw int64, delta varint, double-delta varint, nullable int64,
  or another supported typed-column encoding
- compression may be none, Snappy, LZ4, or future typed-column compression modes

Important code:

- production: `TreeDB/collections/column_int64_values_asset.go`
- direct/prepared use: `TreeDB/collections/column_int64_query.go`
- typed-column section model: `TreeDB/internal/typedcolumn/granule.go`

#### Codec Block

The encoded payload for a contiguous range of rows in one column.

It is not a file. In a typed-column part it is represented by a
`ColumnBlockDescriptor` plus an `EncodedGranule` payload. Serialized bytes live
inside that column's `column_data` section, unless the column uses split
sections such as offsets/values for variable-length list data.

Rows:

- default codec block rows equals rows per granule
- a column can set `CodecBlockRows` to split differently
- each block records `FirstRow`, `RowCount`, `FirstGranule`, and `LastGranule`

Size:

- `RawBytes` is the uncompressed logical encoding size
- `StoredBytes` is the actual stored payload size after optional compression
- the payload range is recorded in the part image and block descriptor

Important code:

- `TreeDB/internal/typedcolumn/part.go`
- `TreeDB/internal/typedcolumn/granule.go`

#### Encoded Granule

The in-memory representation of one encoded column block payload.

Despite the name, it can represent a codec block. When codec blocks align with
granules, one encoded granule maps to one physical granule. If codec block rows
are configured differently, one encoded granule may span a different row range.

Fields that matter for query performance:

- `Rows`
- `Encoding`
- `Compression`
- `RawBytes`
- `StoredBytes`
- `PayloadRef`
- `Payload`
- optional min/max values

Current payload refs are inline inside the part image. A future format could
add external payload refs, but this plan should keep JSONBench q1-q5 on
sectioned in-image payloads first.

Important code:

- `TreeDB/internal/typedcolumn/granule.go`

### Metadata Terms

#### Aggregate Metadata

Precomputed summaries used to answer or accelerate aggregate queries.

Production compatibility asset shape:

- asset kind: `ColumnAssetKindTCS1AggregateMetadata`
- format magic: `TCAM`
- stored as a byte range in a column asset segment file
- grouped by one string column and, for group-hour/min/max/span metadata, one
  int64 value column
- v2 stores exact predicate coverage for equality and small IN-list string predicates
- v3 keeps that predicate coverage and allows grouped count metadata without a
  value column, for q1-style group-count requests
- v4 stores an hour bucket per entry for q3-style `group-hour-count` metadata
- entries store group and count, with hour populated for grouped-hour counts and
  min/max populated for int64 value metadata
- typed-column-owned aggregate metadata is emitted per typed-column granule; older v1/no-predicate assets were per part

Rows and size:

- `Rows` is the full input row count for the part
- `Entries` is the number of groups
- size is variable: header plus group string bytes plus count/min/max per group
- total stored size is `ColumnAssetRef.Length`

Reference experiment target:

- metadata can be per granule
- metadata can be predicate-qualified, for example only rows where
  `kind=commit`, `operation=create`, and `collection=post`
- q4b/q5 can scan metadata instead of data rows when the metadata definition
  exactly covers the query predicate and measure

Important code:

- production: `TreeDB/collections/column_aggregate_metadata_asset.go`
- production query path: `TreeDB/collections/column_aggregate_metadata_query.go`
- reference: `experiments/colgranule/aggregate_metadata.go`

#### Predicate-Qualified Aggregate Metadata

Aggregate metadata whose input rows are restricted by a declared predicate at
build time.

Example:

```text
group by did_code
measures: count, min(time_us), max(time_us)
predicate: kind_code = commit AND operation_code = create AND collection_code = post
scope: granule
```

This is not the same as applying a runtime query predicate to generic aggregate
metadata. The metadata definition itself must include the predicate, and query
planning must only use it when the requested predicate is covered exactly.

Rows:

- metadata rows are summary entries, not source rows
- source row count remains the typed-column part or granule row count
- matched row count is the number of rows satisfying the metadata predicate

Current production status:

- `ColumnAggregateMetadata` can declare exact predicate coverage.
- Production aggregate metadata assets persist predicate coverage and query planning only uses them when runtime predicates, group key, value column when present, and aggregate kind match exactly.
- Metadata diagnostics report metadata entries read separately from matched source-row counts; metadata paths keep `RowsScanned == 0` because they do not decode data rows.

Important code:

- `experiments/colgranule/aggregate_metadata.go`
- `TreeDB/collections/column_aggregate_metadata_asset.go`

#### Pruning Metadata

Any metadata used to prove that a row range, granule, block, part, or asset
cannot match a query before decoding row payloads.

Examples:

- sort-key marks
- min/max statistics
- row-count and visible-row-count summaries
- typed-column pruning metadata sections

It is not one storage format. Always name the concrete storage shape in
diagnostics, for example `sort_key_marks` section or `column_stats` section.

### Query Mode Terms

#### Direct Query

A one-shot call to `Collection.RunColumnPhysicalQuery`.

It includes:

- request validation
- manifest/snapshot lookup
- asset-ref discovery
- asset opening or cache lookup
- predicate planning
- scan/reduce execution
- result shaping

This is the first performance priority. JSONBench q1-q5 direct runs must avoid
JSON document reconstruction and should avoid repeated dictionary/global-code
setup when the same work can be done in one direct physical plan.

Important code:

- `TreeDB/collections/column_physical_query.go`

#### Prepared Query

A reusable runner from `Collection.PrepareColumnPhysicalQuery`.

It amortizes setup across repeated `Run` calls over a pinned or validated
physical state. Prepared mode may cache decoded dictionaries, block plans,
direct-view certifications, and reusable scratch. It must not be the only fast
path reported for JSONBench.

Important code:

- `TreeDB/collections/column_physical_query.go`
- `TreeDB/collections/typed_column_prepared_state.go`
- `TreeDB/collections/typed_column_prepared_int64.go`

#### Compatibility Per-Column Asset

A transitional production asset derived from the row-oriented `TCPA` row asset
for one column.

Current examples:

- `tcs1_dictionary_codes` for one dictionary string column
- `tcs1_int64_values` for one non-null int64 column

These assets are useful fast paths, but they are not the final typed-column part
model because each column is stored as a separate asset and lacks shared
sort-key marks, per-granule descriptors, and sectioned row-aligned metadata.

Avoid calling these "sidecars" without the exact asset kind. Prefer:

- `compatibility dictionary-code asset`
- `compatibility int64-value asset`

#### Typed-Column Part Section

A named byte range inside one `tcs1_typed_column_part` asset.

This is the final integration target for JSONBench q1-q5 because it keeps all
declared columns in one shared physical row order and can carry sort-key marks,
granule descriptors, dictionaries, aggregate metadata, and compressed payloads
as one image.

Avoid calling this a "sidecar." Prefer:

- `typed-column dictionary section`
- `typed-column kind_code column_data section`
- `typed-column sort_key_marks section`
- `typed-column aggregate_metadata section`

## Source Map

### Reference experiment

`experiments/colgranule` is the reference design and performance sandbox.

| Area | Files |
| --- | --- |
| Part builder, physical sort order, granules, row locators | `experiments/colgranule/part.go` |
| Sort-key marks and range pruning | `experiments/colgranule/predicate.go` |
| Adaptive granule sizing | `experiments/colgranule/adaptive_mark.go` |
| Column codecs and compression | `experiments/colgranule/granule.go`, `typed.go` |
| Sectioned column part image | `experiments/colgranule/part_image.go`, `part_image_decode.go` |
| Predicate-qualified aggregate metadata | `experiments/colgranule/aggregate_metadata.go` |
| JSONBench q1-q5 kernels and sort-order variants | `experiments/colgranule/jsonbench_part_queries.go`, `jsonbench_queries.go`, `jsonbench_bench_test.go` |
| Production comparison benchmark | `experiments/colgranule/jsonbench_treedb_columnstore_bench_test.go` |
| Base/delta/tombstone part-set visibility and compaction planning | `experiments/colgranule/part_set.go`, `collection_manifest.go`, `mutation_adapter.go` |
| Workspace, TCS1 assets, reachability, rewrite debt, quarantine | `experiments/colgranule/workspace.go`, `tcs1.go`, `asset_manager.go`, `lifecycle.go`, `lifecycle_view.go` |

### Internal typed-column package

`TreeDB/internal/typedcolumn` already contains much of the experiment data plane.
It is not the production owner by itself.

| Area | Files |
| --- | --- |
| Transplant scope | `TreeDB/internal/typedcolumn/doc.go`, `TreeDB/docs/spec/typed-column-transplant.md` |
| Part builder, granules, ascending sort-key marks | `TreeDB/internal/typedcolumn/part.go`, `predicate.go` |
| Sectioned images and TCS1 header | `TreeDB/internal/typedcolumn/part_image.go`, `part_image_decode.go`, `tcs1.go` |
| Codecs, compression, nullable wrappers, bool/string/int64/vector/adjacency layouts | `TreeDB/internal/typedcolumn/granule.go`, `typed.go`, `nullable_test.go`, `dense.go`, `fixed_width.go`, `raw_uint32_offsets_list.go` |
| Aggregate metadata and pruning metadata | `TreeDB/internal/typedcolumn/aggregate_metadata.go`, `pruning.go`, `stats.go` |
| Data-plane part-set visibility | `TreeDB/internal/typedcolumn/part_set.go` |

### Production collection typed storage

`TreeDB/collections` is the production integration target.

| Area | Files |
| --- | --- |
| User configuration and schema hashing | `TreeDB/collections/column_store.go` |
| Adapter from collection fields to typed-column parts | `TreeDB/collections/typed_column_adapter.go`, `typed_column_publication.go` |
| Physical query API, direct and prepared entry points | `TreeDB/collections/column_physical_query.go` |
| Current dictionary string query path | `TreeDB/collections/column_dictionary_query.go`, `column_physical_predicate.go` |
| Current int64 query path | `TreeDB/collections/column_int64_query.go`, `column_dict_int64_query.go`, `column_dict_hour_query.go` |
| Current aggregate metadata query path | `TreeDB/collections/column_aggregate_metadata_query.go`, `column_aggregate_metadata_asset.go` |
| Current per-column compatibility assets | `TreeDB/collections/column_dictionary_codes_asset.go`, `column_int64_values_asset.go` |
| Typed-column scalar scans and prepared state | `TreeDB/collections/typed_column_int64_scan.go`, `typed_column_string_scan.go`, `typed_column_bool_scan.go`, `typed_column_prepared_state.go`, `typed_column_prepared_int64.go` |
| Asset lifecycle | `TreeDB/collections/column_asset_manager.go`, `column_asset_reachability.go`, `column_asset_rewrite.go`, `column_asset_gc.go` |

### JSONBench harnesses

There are two different JSONBench-related locations.

| Location | Role |
| --- | --- |
| `experiments/colgranule/jsonbench_treedb_columnstore_bench_test.go` | In-repo synthetic benchmark that compares the reference experiment kernels with the current production collection physical query path. |
| `$JSONBENCH_TREEDB_DIR` or `<jsonbench-checkout>/treedb` | Local checkout of the external JSONBench TreeDB harness. This is the canonical benchmark/reporting home for cross-database JSONBench runs; repository state is authoritative, not any one local path. Current external work has transitional TreeDB column-store rows; this plan targets the final production typed-column cells and must keep direct, prepared, and metadata rows unambiguous. |

The external harness entry points are under `$JSONBENCH_TREEDB_DIR`:

- `run_matrix.sh`
- `cmd/jsonbench_treedb/run.go`
- `cmd/jsonbench_treedb/queries.go`
- `README.md`

Current transitional external labels include `column-store`,
`column-store-prepared`, and `column-store-prepared-metadata` once the prepared
layout split lands. Those labels are acceptable as interim reference rows, but
final production typed-column reports must still expose the same dimensions as
`column-direct`, `column-prepared`, `column-direct-metadata`, and
`column-prepared-metadata` or provide explicit aliases.

Draft label mapping:

| Transitional label | Production dimension |
| --- | --- |
| `column-store` | `column-direct` data scan |
| `column-store-prepared` | `column-prepared` data scan |
| `column-store-prepared-metadata` | `column-prepared-metadata` for q4/q5 metadata Top-K |

## Current Gap Summary

| Feature family | Reference experiment | Internal typedcolumn | Production collections status |
| --- | --- | --- | --- |
| Physical sort key | Rows are physically sorted by declared sort key. | Supports ascending physical sort and marks; descending is rejected today. | `typed_column_part` publication persists and validates ascending non-null bool/int64/string `ColumnStoreConfig.SortKey` order when the full key is typed-column-owned and has at most `typedColumnPartSortKeyMaxColumns == 8` columns; mixed-owner keys fall back to synthetic primary-id order without sorted metadata, while typed-column-owned unsupported/nullable/descending/wider-than-8 keys fail closed. Compatibility per-column assets use insertion/current row order. |
| Granules and marks | Per-granule descriptors and sort-key prefix marks support pruning. | Present in the data plane. | Production typed-column physical queries validate typed-column part marks and use equality sorted-prefix pruning for supported SortKey predicates, with explicit diagnostics/fallback reasons. Compatibility dictionary-code/int64-value asset query paths still do not consume typed-column part marks. |
| Sectioned column part image | One row-aligned image contains descriptors, marks, locators, dictionaries, metadata, and payloads. | Present, with extra stats/pruning/layout sections. | Published for `typed_column_part` owners, but JSONBench string/int64 production query paths still largely use compatibility per-column assets. |
| Encodings and compression | Delta, double-delta, nullable, bool bitpack/RLE, compact string codes, adaptive codec block sizing, Snappy/LZ4 keep-if-smaller. | Mostly present or extended. | Production JSONBench-style physical queries do not consistently consume the typed-column codec/block model. |
| q1-q5 kernels | Dense low-allocation kernels, fused q2, q2 sorted-prefix grouped distinct, q4 time-order early stop, q4 prefix-pruned scan, q4/q5 metadata paths. | General primitives exist, but JSONBench kernels are not production APIs. | Typed-column direct q1/q2/q3/q4a/q4b/q5 now execute over typed-column sections with explicit diagnostics: q1 uses dense dictionary-code grouped count for predicate-free event/collection counts, q2 uses sorted grouped-distinct streaming over `(collection,did)` when the physical SortKey validates, q3 uses dense dictionary-code/int64 grouped-hour reduction with real predicates, q4a uses `time_us` physical order to decode only the Top-K prefix granules, q4b uses sorted-prefix mark pruning plus TopK shaping, and q5 uses dense dictionary-code/int64 span reduction with TopK shaping. |
| Predicate-qualified aggregate metadata | Per-granule metadata can be built only for rows matching declared predicates. | Present in the data plane. | Production `ColumnAggregateMetadata` declares exact predicate coverage; v4 aggregate metadata assets persist that coverage, emit typed-column-owned summaries per granule, support grouped count metadata without a value column for q1-style requests and grouped-hour count metadata for q3-style requests, and direct/prepared metadata paths require exact predicate/group/measure compatibility before scanning metadata entries. |
| Multipart visibility and compaction | Base/delta/tombstones, latest-row visibility, part-set scans, compaction planning. | Data-plane subset exists. | Production has mutation manifests and visibility fallback, but optimized physical predicates, aggregate metadata, and prepared paths are mostly insert-only. |
| Lifecycle control plane | Workspace inventory, prepared assets, reachability/reclaim/rewrite debt/quarantine planning. | Mostly deferred from internal typedcolumn. | Production has real asset manager/publish/GC/rewrite plumbing, but not a sorted typed-column part-set lifecycle model with full JSONBench diagnostics. |

## Priority Plan

### P0: Freeze vocabulary, baselines, and parity tests

Deliverables:

- Add or update docs so the terms in this note are used consistently.
- Preserve current benchmark baselines for direct and prepared production queries.
- Add parity tests that compare production typed-column q1-q5 results against a
  simple row-scan implementation on the same input.
- Keep the experiment benchmarks runnable until each feature is represented by
  production tests and benchmarks.

Acceptance criteria:

- A reader can identify which benchmark row uses experiment code, production
  direct code, production prepared code, or external JSONBench row-scan code.
- q1-q5 result hashes match between row scan, production direct, and production
  prepared modes for deterministic fixtures.
- Reports show direct setup time, direct scan/reduce time, prepared setup time,
  prepared hot-run time, bytes read, rows scanned, rows reduced, and allocations.

### P1: Make the typed-column part the production physical substrate for JSONBench fields

Deliverables:

- For JSONBench-style scalar/string fields, publish all required fields into one
  row-aligned `typed_column_part` image for the generation.
- Keep the typed-row asset for row identity, tombstones, and compatibility, but
  do not route q1-q5 optimized scans through separate per-column compatibility
  assets once the typed-column path is available.
- Make the manifest clearly state which production query source was used:
  typed-column part image, typed-row asset, per-column compatibility asset, or
  document fallback.

Acceptance criteria:

- q1-q5 direct production queries can execute without reconstructing JSON
  documents.
- Diagnostics name typed-column sections and row counts, not vague "sidecars".
- Missing or stale typed-column assets fail closed or fall back only through an
  explicit, measured fallback path.

### P2: Wire real physical sort order

Deliverables:

- Pass `ColumnStoreConfig.SortKey` into the typed-column adapter instead of
  always using the synthetic primary-id order.
- Sort each typed-column part by the configured sort key's logical values, then
  by logical primary key, then by stable input order if needed.
- Define and test nullable/missing/default ordering for every supported sort-key
  value type before allowing that type in persisted sort metadata.
- Ensure dictionary-backed string sort keys compare by logical string value or by
  a dictionary code space whose ordering is explicitly certified as equivalent.
- Persist sort-key metadata in the column part image and manifest.
- Start with ascending sort keys. Add descending support only after the mark
  semantics and tests cover it.

Acceptance criteria:

- A collection declared with `SortKey: time_us` physically stores typed-column
  part rows in ascending `time_us` order.
- A collection declared with the ClickHouse-style key
  `(kind, operation, collection, did, time_us)` physically stores that order for
  both q2 grouped-distinct and q4b prefix-scan layouts.
- Reopen tests verify that sort metadata and row locators survive checkpoint and
  reopen.
- Direct q4 time-order early-stop is a later P3/P4 acceptance gate; #1948 only
  provides the persisted/validated physical order that those planners can consume.

Implementation note for #1948: P2 persists and validates the physical typed-column
part SortKey contract for at most `typedColumnPartSortKeyMaxColumns == 8`
ascending non-null bool/int64/string columns. String sort keys use adapter
dictionaries whose codes are assigned in logical bytewise ascending order and are
certified in adapter metadata; nullable/defaulted, descending, and wider-than-8
SortKey columns fail closed until their ordering semantics are specified. q4
early-stop remains deferred to the P3/P4 mark/pruning/kernel work rather than
being claimed by the full-scan typed-column query path.

### P3: Promote granule marks and sorted-prefix pruning into production queries

Deliverables:

- Store and validate per-granule sort-key marks in production typed-column parts.
- Add a production pruning plan for sort-key prefix ranges.
- Add diagnostics for granules considered, granules skipped by marks, granules
  decoded, rows scanned, and bytes decoded.
- Make q2 plan over the ClickHouse-style sort key for the `kind=commit`,
  `operation=create` prefix and expose sorted grouped-distinct streaming
  readiness over `(collection, did)`. #1949 keeps direct q2 execution on the
  existing exact grouped-distinct reducer; #1950 owns replacing that reducer with
  the streaming kernel that consumes this readiness contract.
- Make q4b use the ClickHouse-style sort key to prune on
  `kind=commit`, `operation=create`, and `collection=app.bsky.feed.post`.

Acceptance criteria:

- q2 direct production query uses sorted-prefix planning for the
  `kind/operation` predicate, exposes readiness/fallback diagnostics for exact
  streaming grouped distinct over `collection/did`, and continues to return exact
  grouped-distinct results until #1950 swaps in the streaming reducer.
- q4b direct production query skips granules when marks prove they cannot match.
- Mark-pruning and sorted-prefix result hashes match full-scan result hashes.
- Tests cover empty ranges, boundary equality, multi-column prefixes, missing
  marks, corrupt/stale marks, unsupported descending marks, and uncertified
  dictionary/code ordering.

### P4: Implement production q1-q5 direct kernels first

Direct kernels are first-class. Prepared kernels may reuse the same plan builder,
but direct performance is the priority for this phase.

Deliverables:

- q1: grouped count by event/collection over dictionary codes.
- q2: one fused pass that applies real `kind=commit` and `operation=create`
  predicates, returns both row count and exact distinct user count by event, and
  uses the sorted-prefix/grouped-distinct plan when the physical sort key proves
  it is available.
- q3: filtered event/hour grouped count.
- q4a: top-3 earliest posters using `time_us` physical order and early stop.
- q4b: top-3 earliest posters using ClickHouse-style sort order and mark-pruned
  prefix scan.
- q5: top-3 user activity spans using min/max time per user.

Acceptance criteria:

- Direct q1-q5 scan typed-column sections, not JSON documents.
- q2 is one physical query shape, not two independent physical calls glued
  together at the benchmark layer, and not a write-time masked benchmark
  shortcut.
- Hot loops avoid per-row map lookups and avoid per-row string conversions.
- Each query has parity tests, allocation benchmarks, and CPU/alloc profiles.

### P5: Add predicate-qualified aggregate metadata to production

Status: partially implemented. Production `ColumnAggregateMetadata` supports exact predicate declarations, v4 `TCAM` assets persist predicate coverage and per-entry hour buckets, typed-column-owned metadata is built per granule, direct/prepared q3/q4/q5 metadata requests fail closed unless predicates/group/value/aggregate shape match exactly, grouped count metadata can answer q1-style direct/prepared group-count requests by summing persisted `Count` entries, and grouped-hour count metadata can answer q3-style direct/prepared requests by summing persisted `(group, hour)` entries.

Deliverables:

- Extend `ColumnAggregateMetadata` or add a new production metadata declaration
  that can express:
  - group key: `did` or `collection`
  - measures: count, group-hour count, min `time_us`, max `time_us`
  - predicates: `kind=commit`, `operation=create`,
    `collection=app.bsky.feed.post` or a bounded feed-collection `IN` list
- Build the metadata per granule for the typed-column part.
- Add direct and prepared query paths for q4 and q5 that can answer from metadata
  without decoding data rows.
- Add direct and prepared query paths for q1 grouped counts that can answer from
  metadata without decoding data rows.

Acceptance criteria:

- q4 metadata and q5 metadata rows scan metadata entries, not data rows.
- Predicate metadata result hashes match full direct q4/q5 hashes.
- Metadata invalidation is explicit when mutation parts, stale parts, or
  compaction make the metadata unsafe.

### P6: Bring production encoding, compression, and adaptive granule controls to parity

Deliverables:

- Make production string, bool, int64, nullable, and low-cardinality fields use
  the typed-column codec capability model.
- Support raw int64, delta varint, double-delta varint, nullable int64,
  bool bitpack/RLE, compact dictionary codes, and compression keep-if-smaller
  choices where supported by `typedcolumn`.
- Expose adaptive rows-per-granule controls for benchmark and production tuning.
- Add per-codec profile rows so compression cost, decompression cost, and storage
  savings are visible independently.

Acceptance criteria:

- The benchmark matrix can compare compression off, default compression, and
  alternative codec layouts for q1-q5.
- Compression does not hide allocation regressions in direct queries.
- Unsupported codecs fail closed with an explicit diagnostic.

### P7: Make multipart visibility and compaction compatible with optimized queries

Deliverables:

- Compose base parts, delta parts, and tombstones into a latest-visible typed
  row set for direct q1-q5.
- Preserve optimized query paths when mutation parts exist where practical.
- Implement sorted multipart merge for q4 time-order early stop and mark-pruned
  q4b. This must handle duplicate users across parts and tombstoned or
  superseded rows.
- Rebuild or invalidate aggregate metadata during compaction.

Acceptance criteria:

- Direct q1-q5 return correct results over inserts, updates, deletes, checkpoint,
  reopen, and compaction.
- Mutation-bearing manifests do not silently fall back to document scans.
- Diagnostics show whether a query used insert-only fast path, latest-visible
  typed-column merge, or explicit fallback.

### P8: Close the lifecycle and asset-management gap

Deliverables:

- Integrate typed-column part images, aggregate metadata, and related sections
  into production reachability, rewrite, GC, quarantine, and prepared-asset
  accounting.
- Expose lifecycle diagnostics for live bytes, stale bytes, rewrite debt,
  protected references, active mapped resources, and prepared query pins.

Acceptance criteria:

- No query-visible typed-column part is deleted while reachable by a manifest,
  snapshot, prepared runner, or mapped-resource handle.
- Compaction and rewrite preserve q1-q5 parity and direct/prepared diagnostics.
- Failure and quarantine tests cover corrupt typed-column part images and corrupt
  metadata.

### P9: Add production JSONBench cells and reports

Deliverables:

- In the external JSONBench TreeDB harness, add explicit TreeDB typed-column
  cells instead of overloading or relabeling row-scan/template cells.
- In gomap, keep a smaller in-repo synthetic benchmark that exercises the same
  production query modes without depending on external JSONBench data.
- Generate benchmark reports that show direct and prepared rows separately.

Acceptance criteria:

- JSONBench reports can answer: which query, which sort layout, which storage
  source, direct or prepared, metadata or data scan, compression mode, and
  mutation mode.
- q1-q5 have stable result hashes across row-scan baseline, production direct,
  and production prepared modes.
- The benchmark harness can run smoke, 100k, 1m, and 10m scales with consistent
  artifact names.

## JSONBench Production Matrix

The production matrix should make every dimension explicit.

### Query definitions

| Query | Required production behavior |
| --- | --- |
| q1 | Count rows grouped by event/collection. |
| q2 | Store full `kind`, `operation`, `collection` (reported as `event`), and `did`; at query time apply `kind=commit` and `operation=create`, then count rows and exact distinct users grouped by event/collection. |
| q3 | For `kind=commit`, `operation=create`, and event in the JSONBench event list, count rows grouped by event and hour of day. |
| q4a | For post creates, return the three users with earliest post time using `time_us` physical order and early stop. |
| q4b | For post creates, return the three users with earliest post time using ClickHouse-style physical order plus sort-key mark pruning. |
| q5 | For post creates, return the three users with largest `max(time_us) - min(time_us)` activity span. |

### Storage layouts

| Layout name | Sort key | Purpose |
| --- | --- | --- |
| `typed-column-primary-id-control` | synthetic primary id | Control layout. It should be correct but should not claim sort-order optimizations. |
| `typed-column-time` | `time_us` | q4a early-stop layout and a useful q1/q2/q3/q5 control. |
| `typed-column-filter-user-time` | `kind`, `operation`, `collection`, `did`, `time_us` | q2 sorted-prefix grouped-distinct layout, q4b mark-pruned prefix-scan layout, and ClickHouse-order comparison. |
| `typed-column-unsorted-legacy-assets` | current compatibility row order | Legacy production comparison only. Do not use it as the target implementation. |

### Execution modes

| Mode | API | What is timed | Required label |
| --- | --- | --- | --- |
| Row-scan baseline | External JSONBench current public collection scan | Full query over JSON/template rows | `row-scan` |
| Production direct data scan | `RunColumnPhysicalQuery` | One complete one-shot physical query | `column-direct` |
| Production prepared data scan | `PrepareColumnPhysicalQuery` once, then `Run` | Hot repeated run; setup reported separately | `column-prepared` |
| Production direct metadata | `RunColumnPhysicalQuery` with aggregate metadata | One complete metadata query | `column-direct-metadata` |
| Production prepared metadata | Prepared runner with aggregate metadata | Hot repeated metadata run; setup reported separately | `column-prepared-metadata` |

Reports must never merge direct and prepared rows. Prepared rows can show the
steady-state service ceiling; direct rows show ad-hoc query performance and are
the primary optimization target in this plan.

Current production prepared runners may build exact reusable summaries during
setup for predicate-compatible q1 and q3 typed-column scans. Hot prepared `Run`
calls can then answer those queries without scanning data rows while preserving
the same result hash and reducer cardinality diagnostics as the direct path.
This is a prepared-service ceiling, not a substitute for direct or persisted
summary work; benchmark reports must still keep direct q1/q3 rows visible.
For q1-style grouped counts and q3-style grouped-hour counts, production
metadata can now persist per-typed-column-granule summaries and answer
direct/prepared metadata requests by reading metadata entries.

## Direct vs Prepared Contract

Direct and prepared share logical query semantics, result shape, diagnostics, and
fail-closed rules. They differ only in lifetime and where setup cost is paid.

Direct query requirements:

- A direct query plans and executes one request against one snapshot.
- It may build a short-lived plan, dictionary translations, pruning plan, and
  scratch buffers, but it should avoid repeated work inside the same call.
- It must expose setup, scan/decode, reduce, and result-shaping timing.
- It must not reconstruct JSON documents for optimized q1-q5 paths.
- It is the first performance priority.

Prepared query requirements:

- A prepared query pins or validates the physical state it needs, builds reusable
  translations and decoders, and exposes a `Run` method for repeated execution.
- Prepared setup time must be reported separately from prepared hot-run time.
- Prepared runners must fail closed or refresh explicitly when manifest state,
  asset identity, schema hash, sort metadata, aggregate metadata, or visibility
  state no longer matches.
- Prepared rows are allowed to outperform direct rows, but they must not be used
  to hide poor direct performance.

Top-K result shaping should avoid materializing the full candidate result set
when the query only needs a bounded Top-K. For q5 dense-span scans, keep the
candidate counter diagnostic, but feed candidates directly into the bounded
Top-K reducer instead of appending every user span before trimming.

Shared implementation preference:

- Build one planner that can produce either a direct one-shot execution plan or a
  prepared reusable plan.
- Keep result-shaping code shared so direct/prepared parity is tested by the same
  result hash.
- Keep diagnostics shared so reports can compare modes without guessing.

## Profiling Loop

Every priority step should follow the same loop.

1. Record a baseline.
2. Add the smallest feature slice.
3. Run parity tests.
4. Run microbenchmarks with `-benchmem`.
5. Capture CPU and allocation profiles.
6. Optimize the direct path.
7. Compare direct and prepared rows.
8. Record the artifact paths and decision in the PR note.

Suggested local commands:

```sh
# Reference experiment benchmark smoke.
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench 'BenchmarkJSONBench(EncodedPartQueries|ColumnStoreCompare)' \
  -benchmem

# Production collection physical query and typed-column benchmarks.
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'Benchmark(ColumnPhysical|TypedColumn|JSONBench)' \
  -benchmem

# Focused profiles for one production benchmark once it exists.
OUT=/tmp/treedb_column_jsonbench_profile_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkJSONBenchProductionTypedColumn/q2/direct' \
  -benchmem \
  -cpuprofile "$OUT/cpu.pprof" \
  -memprofile "$OUT/allocs.pprof"

JSONBENCH_TREEDB_DIR=${JSONBENCH_TREEDB_DIR:-../JSONBench/treedb}
(
  cd "$JSONBENCH_TREEDB_DIR"
  DATA_DIR=./testdata/bluesky SUBSET_ROWS=6 TRIES=1 ./run_matrix.sh
)

# External JSONBench 100k working run.
JSONBENCH_TREEDB_DIR=${JSONBENCH_TREEDB_DIR:-../JSONBench/treedb}
(
  cd "$JSONBENCH_TREEDB_DIR"
  DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=100000 TRIES=3 ./run_matrix.sh
)
```

Required metrics:

- wall time, ns/op, rows/s, dataset rows/s;
- B/op and allocs/op;
- setup time, scan/decode time, reduce time, result-shape time;
- physical bytes read, mapped bytes, heap-copy bytes, decoded bytes;
- rows scanned, rows reduced, result groups;
- granules considered, skipped, decoded;
- sort-key mark hit/miss counts;
- aggregate metadata entries read;
- compression ratio, compressed bytes, decompressed bytes, compression CPU, and
  decompression CPU;
- fallback reason when the optimized typed-column path is not used.

## Implementation Rules

- Do not route production q1-q5 through `experiments/colgranule`; port or adapt
  the required behavior through `TreeDB/internal/typedcolumn` and
  `TreeDB/collections`.
- Do not claim `ColumnStoreConfig.SortKey` is active for a query unless the
  physical typed-column part is actually sorted by that key and the query
  validates the metadata it relies on.
- Do not describe a storage source as a "sidecar" in new docs or diagnostics
  without naming the exact asset or section.
- Do not report prepared-only JSONBench results as the typed-column result.
  Direct and prepared rows must stay separate.
- Do not use q2 write-time masking, empty sentinels, or benchmark-specific
  pre-filtered `event`/`did` payloads to simulate `kind=commit` and
  `operation=create`. Store full predicate columns and execute real predicates.
- Do not treat aggregate metadata as valid when mutation parts, stale manifests,
  compaction, or schema changes make the metadata unsafe.
- Do not optimize by skipping checksum, schema, lifetime, or section-boundary
  validation unless the benchmark row explicitly selects a documented read
  integrity mode.

## First Work Items

1. Add production q1-q5 parity fixtures and result hashes.
2. Add a production JSONBench synthetic benchmark under `TreeDB/collections`
   rather than only under `experiments/colgranule`.
3. Wire `ColumnStoreConfig.SortKey` into typed-column publication for ascending
   int64/string-code sort carriers.
4. Add q2 direct fused count plus distinct over typed-column dictionary codes,
   with real `kind`/`operation` predicates and no write-time sentinel masking.
5. Wire sorted-prefix q2 planning over
   `(kind, operation, collection, did, time_us)` so direct q2 stops rebuilding a
   global high-cardinality `did` map when the sort contract is available.
6. Add q4a direct/prepared early-stop over `typed-column-time`.
7. Add sort-key mark diagnostics and q4b prefix pruning.
8. Add predicate-qualified aggregate metadata declaration and q1/q3/q4/q5
   metadata direct queries.
9. Add or alias external JSONBench `column-direct`, `column-prepared`,
   `column-direct-metadata`, and `column-prepared-metadata` cells, preserving
   compatibility with transitional `column-store*` labels where needed.
