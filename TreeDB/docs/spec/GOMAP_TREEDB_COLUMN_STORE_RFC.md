# RFC: Column-Store Collections for gomap TreeDB

Status: proposal, non-normative.

Production persistent column-store collection writes are blocked until
`TreeDB/docs/spec/collection-wal-durability-plan.md` M7 sign-off links to green
M1-M6 collection WAL evidence. Before that gate, column-store work is limited
to docs, benchmarks, pure codecs, filters/search packages, and isolated
encode/decode tests that do not publish persistent collection roots. Persistent
column-store APIs, column part descriptor roots, secondary indexes pointing at
column-store rows, column-file side refs in published roots, and crash/reopen
safety claims for column-store writes are blocked.

Target TreeDB source studied:
`https://github.com/snissn/gomap/tree/874737704bd8cdcd1add40c5d2316f03544b1219`

Compression reference: `COMPRESSION_TECHNOLOGY_SPEC.md` in this directory

ClickHouse source snapshot studied:
`https://github.com/ClickHouse/ClickHouse/commit/ad347dbafb074ccf13790b5045b25708a975fb77`

## 1. Summary

Add an optional column-store storage layout for TreeDB collections. The new
layout keeps TreeDB's existing B+Tree, catalog roots, snapshots, value log,
dictionary store, and collection manager, but changes the durable document-data
root for selected collections from row-document values to immutable,
column-oriented parts. Each part stores row-aligned column granules in a
dedicated column file class and stores small B-tree descriptors for discovery,
visibility, and point lookup.

The desired end state is a TreeDB collection type that can:

- ingest rows from JSON, BSON, template-v1, or direct column vectors;
- store each declared field as typed, nullable column streams;
- apply ClickHouse-style codec pipelines per column and substream;
- scan projected columns without materializing full documents;
- support direct borrowed vector APIs for tight loops;
- preserve existing primary-key and secondary-index semantics, including
  unique/nonunique secondary indexes for column-store collections;
- write updates as replacement column-store data, not as a permanent row-store
  overlay;
- require fixed schemas for column-store collections in `TCS1`;
- expose ClickHouse-style fast filters and haystack search algorithms through
  reusable collection-level interfaces, so row-store, template-v1, BSON, and
  column-store collections can all use them where their data layout can supply
  typed vectors or byte haystacks;
- use TreeDB's copy-on-write root publish, snapshots, column-file/value-log
  lifecycle discipline, and benchmark discipline;
- use test-driven milestones with explicit throughput and compression gates.

This should be built as an additive collection storage mode, not as a rewrite
of the base B-tree.

## 2. Current TreeDB Facts This Proposal Relies On

The gomap repository already has most of the low-level pieces needed for this
design:

- TreeDB is a persistent ordered key/value store with B+Tree pages in
  `Dir/maindb/index.db`.
- The public TreeDB wrapper opens `maindb/`, `dictdb/`, and optional template
  side stores.
- Values larger than a threshold can live in a persistent append-only value
  log and be referenced by `page.ValuePtr`.
- The value log is durable storage, not an ephemeral WAL. Pointers are valid
  across reopen and are reclaimed only by reachability GC or rewrite.
- Cached mode writes through memtables plus journal and later publishes B-tree
  roots.
- Collections already have multiple ordered roots per logical collection:
  primary, templates, optional index-state, and secondary indexes.
- Collection writes already publish root groups with one system-root descriptor
  update.
- The current indexed collection write-domain is flush-boundary durable, not
  durable-at-ack. Pending mutable, queued, or publishing collection writes are
  visible in-process, but crash durability is established by `Collection.Flush`,
  `CollectionManager.FlushAll`, close, or a synchronous publish path.
- Collection storage policies already distinguish fast pager leaves from
  compressed value-log leaves:
  `RootStorageFast` and `RootStorageCompressed`.
- TreeDB already stores B-tree outer leaves in the value log as `TOL2` blocks
  with restart points, checksums, optional snappy/lz4, and typed blob-ref
  entries.
- B-tree leaf pages already have columnar metadata encodings for keys and
  values inside a 4 KiB page.
- Value-log grouped frames already support raw, block compression, dictionary
  zstd compression, K selection, read integrity options, dictionary
  persistence, and autotune.
- Existing benchmarks report docs/sec, ns/doc, bytes/doc, allocation counts,
  phase timings, maintenance size, value-log rewrite/GC, and SQLite baselines.

The column-store collection should reuse these contracts instead of inventing a
separate storage engine.

## 3. Goals

1. Preserve TreeDB's single-writer/multi-reader snapshot model.
2. Keep the B-tree as the transactional root catalog, locator, secondary-index,
   filter-index, and visibility mechanism.
3. Store immutable column parts in a dedicated persistent column file class.
4. Make projected scans much faster and lower-allocation than materializing
   JSON/BSON/template documents.
5. Improve compacted bytes/doc for typed collection shapes, especially
   repeated fields, low-cardinality strings, booleans, nullable columns,
   counters, timestamps, and monotonic ids.
6. Apply ClickHouse compression practices where they fit TreeDB:
   granules, marks, substreams, per-column codecs, codec-pipeline validation,
   generic-only structural streams, min/max compressed-block sizing,
   recompression during maintenance, and sparse/default-value encoding.
7. Provide a direct API for row loops, vector loops, and codec development.
8. Make secondary indexes first-class for column-store collections. Direct
   vector ingestion, row ingestion, updates, deletes, compaction, and snapshot
   reads must preserve the same index semantics as existing row collections.
9. Make updates rewrite changed data into column-store delta parts immediately,
   then merge those delta parts into normal parts during maintenance.
10. Make fast filters and byte-haystack search algorithms reusable across
   collection types through small adapters, not hard-coded to the column-store
   scan path.
11. Use fixed schemas for column-store collections. Flexible/lazy schemas are a
   future collection mode, not part of `TCS1`.
12. Align column-store compression with TreeDB's existing compression system
   instead of creating a disconnected codec stack.
13. Provide a roadmap where every PR has correctness tests and explicit
   performance gates.

## 4. Non-Goals

- Do not replace the B-tree.
- Do not make all TreeDB collections columnar by default.
- Do not attempt SQL query planning in the first implementation.
- Do not require old DB directories to migrate. TreeDB is pre-alpha, and this
  proposal may add new on-disk formats.
- Do not rewrite an entire collection or large base part for a point update.
  A point update may write a small replacement column-store delta part plus
  tombstones for old row locators, then maintenance rewrites compacted base
  parts later.
- Do not keep a permanent row-document overlay for column-store collections.
  A transient row object may exist while computing an update, but the durable
  representation of changed rows must be column-store data.
- Do not lazily create columns for undeclared fields in `TCS1`. Flexible
  schemas may be designed later as a separate format or compatibility layer.
- Do not force one operating-system file per field for every write shape. PR1
  should create a dedicated column-store file class, but the manifest may pack
  small update-delta parts to avoid pathological file counts.
- Do not require every ClickHouse codec in PR1. The format should allow them,
  but implementation should be staged.

## 5. Desired End State

### 5.1 User-Facing Collection Model

Add an opt-in column-store mode to collection metadata:

```go
type CollectionStorageLayout string

const (
    CollectionStorageRow      CollectionStorageLayout = ""
    CollectionStorageColumnar CollectionStorageLayout = "columnar-v1"
)

type ColumnSchemaMode string

const (
    ColumnSchemaFixed ColumnSchemaMode = "fixed"
)

type CollectionOptions struct {
    // existing fields...
    StorageLayout CollectionStorageLayout `json:"storage_layout,omitempty"`
    ColumnStore   ColumnStoreOptions      `json:"column_store,omitempty"`
}
```

Column-store collections require an explicit schema, even if documents are
ingested from JSON/BSON/template-v1:

```go
type ColumnStoreOptions struct {
    SchemaVersion uint32
    SchemaMode    ColumnSchemaMode
    Columns       []ColumnDefinition
    PrimaryOrder  []string
    PartPolicy    ColumnPartPolicy
    Compression   ColumnCompressionPolicy
    UpdatePolicy  ColumnUpdatePolicy
    FastFilters   []FastFilterDefinition
}

type ColumnDefinition struct {
    Name         string
    Path         string
    Type         ColumnType
    Nullable     bool
    Default      ColumnDefault
    Compression  ColumnCompressionBinding
    IndexHints   ColumnIndexHints
}

type ColumnCompressionBinding struct {
    Mode       ColumnCompressionMode // auto, pinned, off
    Pinned     []ColumnCodecSpec
    Candidates []ColumnCodecSpec
}

type ColumnCompressionMode string

const (
    ColumnCompressionAuto   ColumnCompressionMode = "auto"
    ColumnCompressionPinned ColumnCompressionMode = "pinned"
    ColumnCompressionOff    ColumnCompressionMode = "off"
)

type ColumnUpdatePolicy struct {
    MicroBatchMaxDelay    time.Duration
    CompactAfterDeltaRows int
}

type FastFilterDefinition struct {
    Name        string
    Columns     []string
    Kind        FastFilterKind // minmax, set, bloom_filter, tokenbf_v1, ngrambf_v1, text
    GranuleRows int
    Args        map[string]uint64
}
```

`SchemaMode` must be `fixed` for `TCS1`. Every stored column must be declared
up front with a stable column id, type, nullability, default, compression
binding, and index hints. Row ingestion may ignore undeclared fields or reject
them according to collection options, but it must not create new physical
columns lazily.

`Compression.Mode` defaults to `auto`. Auto mode lets the column-store codec
selector choose per-column and per-block pipelines from the collection/profile
candidate set. `pinned` mode restricts the column to the explicitly listed
pipeline(s), and `off` stores raw blocks except for mandatory structural
encodings such as bitmaps. This gives schemas good defaults while still
allowing a column owner to pin `LZ4`, `ZSTD`, a zstd dictionary profile, or a
typed transform pipeline when benchmarks justify it.

Flexible schemas are deliberately deferred. If TreeDB later supports lazily
created columns, that should be a separate schema mode with explicit migration,
query, and storage-compatibility rules.

`ColumnPartPolicy` is authoritative for update-delta physical sizing, including
`UpdateDeltaMaxRows`, `UpdateDeltaMaxBytes`, and update-delta granule and codec
block limits. `ColumnUpdatePolicy` only controls foreground batching and
compaction triggers; collection opening must reject future options that set
conflicting physical delta limits in both places.

The collection can still accept row documents through `InsertBatch`, but the
preferred high-throughput API is direct column ingestion:

```go
type ColumnBatch struct {
    RowCount int
    Columns  []ColumnVector
}

func (c *Collection) InsertColumnBatch(ids [][]byte, batch ColumnBatch) error
```

The row APIs remain available:

- `Get`
- `ScanDocuments`
- `FindByIndexRange`
- existing JSON materialization helpers

The new APIs expose columnar access:

```go
type ColumnProjection struct {
    Columns []string
}

type ColumnScanOptions struct {
    Projection ColumnProjection
    LowerID    []byte
    UpperID    []byte
    Limit      int
    Predicate  ColumnPredicate
    BatchRows  int
}

type BorrowedColumnBatch struct {
    RowIDs   [][]byte
    Columns []BorrowedColumnVector
}

func (c *Collection) ScanColumnBatches(
    opts ColumnScanOptions,
    fn func(BorrowedColumnBatch) (bool, error),
) error
```

The borrowed batch contract should mirror existing borrowed collection APIs:
slices are valid only until the callback returns and must not be retained or
modified.

### 5.2 Developer-Facing Codec API

Expose a low-level codec surface under an internal package first:

```go
type CodecKind uint8

type CodecSpec struct {
    Kind CodecKind
    Args []uint64
}

type ColumnCodec interface {
    Kind() CodecKind
    Flags() CodecFlags
    MaxEncodedLen(rawLen int) int
    Encode(dst []byte, col ColumnVector) ([]byte, CodecBlockMeta, error)
    Decode(dst ColumnVectorBuilder, encoded []byte, meta CodecBlockMeta) error
}
```

Column codecs should be testable without a DB. Every owned format must have:

- exact-byte golden tests;
- round-trip tests;
- fuzz tests for corrupt input;
- benchmark fixtures that report raw bytes, encoded bytes, decode rows/sec,
  encode rows/sec, and allocations.

## 6. Storage Design

### 6.1 Roots

A column-store collection uses the same root-group machinery as current
collections, with additional roots:

```text
<collection>/primary            existing primary root, id -> row_locator
<collection>/columns/parts      immutable part descriptors
<collection>/columns/granules   optional part/granule range marks
<collection>/columns/locator    optional auxiliary primary-id locator map
<collection>/columns/deletes    tombstones and delete bitmap descriptors
<collection>/columns/schema     schema evolution descriptors
<collection>/columns/counts     exact count and visibility metadata
<collection>/columns/filters/<name> optional persistent fast-filter roots
<collection>/templates          existing template-v1 root when needed
<collection>/index-state        existing row-index-state root when needed
<collection>/index/<name>       existing secondary roots, column-store aware
```

The column-store root-kind inventory is part of the collection WAL contract.
Every root above is either published in the same collection WAL transaction as
the corresponding mutation or is absent from the first implementation. PR1 uses
the existing secondary-root naming convention, `<collection>/index/<name>`.
`<collection>/secondary/<name>` is not a second persistent naming scheme unless
a later migration explicitly introduces it and updates the WAL root-kind mapping.

The first implementation should keep the existing primary root as
`id -> row-locator`. A row locator is small and B-tree-friendly:

```text
row_locator :=
    format_version  u8
    part_id         u64
    row_ordinal     u32 or u64
    granule_ordinal u32 optional, derivable from the part descriptor
    flags           u8
```

The primary root preserves point lookup, uniqueness checks, and ordered id
range scans without scanning part manifests. Later, if primary ids are dense
and append-ordered, the locator root can be made optional by deriving ranges
from part descriptors.

The authoritative key-to-row mapping is the primary B-tree entry
`id -> row_locator`. The row locator names the immutable part and row ordinal.
The part descriptor maps row ordinals to logical granules through its
`Granules` array. A reader can derive `granule_ordinal` by binary-searching that
array when the locator does not store the optional cached ordinal.

The optional `columns/granules` root is a scan accelerator, not the visibility
authority. It stores coarse mark entries such as:

```text
column_granule_mark_key :=
    collection name prefix
    id_lower
    part_id u64 big-endian
    granule_ordinal u32 big-endian

column_granule_mark_value :=
    id_upper_exclusive
    first_row
    row_count
    created_commit_seq
    visible_row_count
```

Because update-delta parts can overlap base-part key ranges, scan plans that use
`columns/granules` must still apply primary-locator visibility, tombstones, or
commit-sequence rules before returning rows. PR1 may start with primary-root
range scans grouped by `(part_id, granule_ordinal)` for correctness, then add
`columns/granules` when dense scans need fewer primary-root reads.

### 6.1.1 Secondary Index Contract

Secondary indexes must work for column-store collections from the first indexed
milestone. The implementation should keep the existing secondary B-tree root
format and semantics first, then optimize payloads after parity is proven.

Contract:

- secondary index definitions remain collection metadata, independent of the
  physical row/column storage layout;
- row ingestion extracts indexed fields while building the `ColumnBatch`;
- direct vector ingestion builds index runs from the input vectors when an
  index expression references declared columns;
- unsupported index expressions may materialize only the fields needed to
  compute the index expression, not the full document;
- index keys keep current ordering and uniqueness semantics, typically
  `<encoded index value>|<primary id>` for nonunique indexes and
  `<encoded index value>` with conflict checks for unique indexes;
- index values should remain document ids in PR1, with an optional
  `row_locator` payload later as a point-read accelerator;
- index scans return primary ids, then the primary locator root maps ids to
  visible column rows for projected column reads;
- direct column scans may use a secondary index result as a row-id filter and
  decode only matching projected granules;
- updates publish old-index-entry deletes and new-index-entry inserts in the
  same root group as primary locator changes, column delta-part descriptors,
  delete bitmaps, and system-root metadata;
- compaction rewrites column parts but must not change logical secondary index
  contents unless primary ids or indexed values change.

This gives column-store collections the same correctness surface as existing
collections while allowing better physical plans later. After parity, low
cardinality secondary indexes can grow compressed posting-list or bitmap
payloads, but that is an optimization of the secondary root value, not a new
logical index model.

### 6.2 Part Descriptors

Column data is immutable. A write publishes one or more parts and installs
descriptors in a B-tree root.

Part descriptor key:

```text
collection_part_key :=
    collection name prefix
    part_id u64 big-endian
```

Part descriptor value:

```text
ColumnPartDescriptor {
    Version          uint8
    PartID           uint64
    PartKind         uint8 // base, insert delta, update delta, compacted
    SchemaVersion    uint32
    CreationWALLSN   uint64
    CreationCollectionSeq uint64
    ManifestRef      optional ColumnFileRef
    SideRefDigest    bytes optional
    CodecRegistryVersion uint32 optional
    RowCount         uint32
    VisibleRowCount  uint32
    DeletedRowCount  uint32
    IDLower          []byte
    IDUpperExclusive []byte
    CreatedCommitSeq uint64
    Supersedes       []RowLocatorRange optional
    Granules         []GranuleDescriptor
    Columns          []ColumnPartColumnDescriptor
    DeleteBitmapRef  optional ColumnFileRef
    DictionaryRefs   []ColumnFileRef or []DictionaryID optional
    Stats            PartStats
}
```

Granules are row ranges within a part:

```text
GranuleDescriptor {
    FirstRow      uint32
    RowCount      uint32
    VisibleRows   uint32
    DeletedRows   uint32
    IDLower       []byte
    IDUpper       []byte
    MarkOrdinal   uint32
}
```

Column descriptors contain file references to compressed streams:

```text
ColumnPartColumnDescriptor {
    ColumnID      uint32
    Type          ColumnType
    Substreams    []ColumnSubstreamDescriptor
    MinMax        optional typed min/max per granule
    NullCount     []uint32
    DefaultCount  []uint32
    CompressedBytes   uint64
    UncompressedBytes uint64
}

ColumnSubstreamDescriptor {
    SubstreamKind  uint8
    FileRef        ColumnFileRef
    BlockDirectory []ColumnBlockDirectoryEntry
}

ColumnFileRef {
    FileID         uint64
    RelativePath   string
    Offset         uint64
    Size           uint64
    ChecksumCRC32C uint32
}
```

A `ColumnPartDescriptor` that references external bytes must be WAL-side-ref
closed. The descriptor or its manifest must allow recovery to enumerate every
`ColumnFileRef`, delete bitmap ref, filter ref, dictionary ref, and external
compression metadata ref without executing user code. `SideRefDigest`, when
present, is a digest of the canonical sorted required side-ref set for the
descriptor graph and must match the collection WAL transaction's side-ref
closure.

### 6.3 Column File Layout

PR1 should create a dedicated column-store file class instead of hiding column
payloads inside ordinary value-log records. This avoids a migration later,
improves observability, and makes schema-change cleanup easier.

Column files use a staged prepare namespace before publication:

```text
Dir/maindb/columns/
    .prepare/
        txn-<wallsn>/
            part-<part-id>/
                manifest.tcs1
                ...
```

The prepare manifest names the final relative path, size, checksum, `PartID`,
`FileID`, schema version, compression pipeline, dictionary ids, and owning
collection id for every file/range. Recovery owns both prepare and final
namespaces. Files in either namespace that are not reachable from active roots,
pending collection WAL, snapshots, or live collection read views are reclaimed or
quarantined by the column file GC policy. `PartID` and `FileID` values observed
in unclassified prepare/final directories remain reserved until recovery
classifies them.

The recommended physical layout is manifest-driven:

```text
Dir/maindb/columns/
    <collection-id>/
        schema-<schema-version>/
            part-<part-id>/
                manifest.tcs1
                col-<column-id>-values.tcs1
                col-<column-id>-nullmap.tcs1
                col-<column-id>-offsets.tcs1
                col-<column-id>-chars.tcs1
                filters/
                deletes.tdbm
```

For columns with many substreams or very large payloads, the same manifest can
use a column subfolder form such as `col-<column-id>/values.tcs1`; the stable
`column_id`, not the user-facing field name, should still be the path key.

Use stable `column_id` values in file names, not field paths. User-facing
field names may be renamed; stable ids keep old parts readable and let schema
evolution map old physical columns to new logical names.

Columns are logically distinct streams. For a schema such as
`{primary key, metric_1, metric_2}`, `metric_1` and `metric_2` should have
separate column stream descriptors and, for compacted/base parts, separate
column or substream files. The manifest, not path conventions alone, remains
authoritative: it records each column/substream file, byte range, compression
pipeline, checksums, marks, and row ranges.

Small update-delta parts may pack multiple columns or substreams into one
`TCS1` container file to avoid excessive inode and file-descriptor churn. The
same manifest format should describe both layouts:

- compacted/base part: prefer separate files per column/substream for
  observability and selective IO;
- small update-delta part: allow packed multi-column files;
- maintenance compaction: rewrite packed deltas into normal separated base
  part files.

Column files must still use TreeDB's existing durability and lifecycle
principles:

- use the same reachability model as value-log pointers;
- integrate with TreeDB's lifecycle tooling or provide equivalent segment/file
  GC and rewrite;
- expose bytes-by-class stats for column payloads, filters, marks, and
  tombstones;
- preserve WAL/recovery ordering: no root may publish a pointer to unreadable
  column bytes;
- keep `TCS1` payload compression independent from outer frame compression.

Envelope:

```text
TCS1 record payload :=
    magic              byte[4] = "TCS1"
    version            u8 = 1
    flags              u8
    column_id          u32 little-endian
    logical_type       u16 little-endian
    row_count          u32 little-endian
    granule_count      u32 little-endian
    substream_kind     u8
    pipeline_count     u16 little-endian
    pipeline_table     repeated CodecPipelineDescriptor
    block_count        u32 little-endian
    block_directory    repeated ColumnBlockDirectoryEntry
    block_payloads     bytes
```

Pipeline table:

```text
CodecPipelineDescriptor {
    PipelineID   uint16
    CodecCount   uint8
    Codecs       repeated CodecSpec
}
```

The stream must always include a raw pipeline:

```text
PipelineID=0, Codecs=[NONE]
```

Additional pipelines represent the column-selected default and any adaptive
alternatives. A block chooses one pipeline by id. This is required because
compression admission can reject one block as incompressible while keeping
compression for neighboring blocks in the same column stream.

Block directory:

```text
ColumnBlockDirectoryEntry {
    FirstRow          uint32
    RowCount          uint32
    FirstGranule      uint32
    GranuleCount      uint32
    PipelineID        uint16
    Reserved          uint16
    CodecOffset       uint64
    CodecSize         uint32
    RawSize           uint32
    ChecksumCRC32C    uint32
    MinMaxOffset      uint32 optional by flags
}
```

The column file reference records a checksum for the referenced byte range.
`ChecksumCRC32C` in each block is still useful because a reader may decode one
block after slicing a larger column file. This avoids requiring a full column
stream scan to validate one projected block. A later format can use xxhash128
or CityHash128 if evidence shows CRC32C is too weak for the block slice use
case.

`TCS1` column files should store already-encoded block payloads directly.
Double compression must be treated as a bug unless an explicit class-level
dictionary experiment demonstrates a workload where an outer dictionary layer
improves wall time.

### 6.4 Granules, Blocks, and Marks

Borrow these ClickHouse defaults unless benchmark evidence changes them:

- default granule: 8192 rows;
- minimum compression block target: 64 KiB raw;
- maximum compression block target: 1 MiB raw;
- compact/write-small parts may group several columns into one packed `TCS1`
  file, but mature parts should favor independent column/substream files;
- marks record row range plus compressed offset and decoded offset.

Chunking has four separate units:

- part: the immutable maintenance and publication unit;
- granule: the smallest unit for metadata skipping and row-count accounting;
- codec block: the smallest unit for decompression;
- column file or file range: the physical IO/reference unit.

Granules and codec blocks should usually align, but the format must allow one
granule to contain multiple codec blocks for very wide strings and one codec
block to contain several tiny granules for narrow booleans.

Logical granules are part-wide and row-aligned across all columns. For granule
ordinal `g` in a part, every column and substream refers to the same
`FirstRow`, `RowCount`, `IDLower`, and `IDUpper` from the part descriptor. This
lets one row mask, delete bitmap, secondary-index result, or fast-filter result
apply across all projected columns.

Codec blocks are different: they are per-column-substream physical decode
units. `MinCodecBlockRawBytes` and `MaxCodecBlockRawBytes` are targets for one
substream's raw bytes, not a global constraint across every column in the row
granule. A narrow bool substream may encode one block that covers many logical
granules. A large string `chars` substream may split one logical granule across
several codec blocks. The block directory's row range and granule range map each
physical block back to the part-wide logical granules.

The minimum block size is an efficiency target, not a correctness invariant. A
small bool column does not force a large string column to wait, and a huge
string column does not force the bool column to split. The maximum block size is
a stronger latency and memory guard for a single substream; when one wide
substream would exceed it, only that substream splits its codec blocks. The
logical granule boundary can remain stable for the other columns.

Initial chunk builder policy:

```go
type ColumnPartPolicy struct {
    TargetPartRows                  int
    MaxPartRows                     int
    TargetGranuleRows               int
    MinCodecBlockRawBytes           int
    MaxCodecBlockRawBytes           int
    UpdateDeltaTargetGranuleRows    int
    UpdateDeltaMinCodecBlockRawBytes int
    UpdateDeltaMaxCodecBlockRawBytes int
    UpdateDeltaMaxRows              int
    UpdateDeltaMaxBytes             int64
}
```

Suggested defaults:

- `TargetGranuleRows = 8192`;
- `MinCodecBlockRawBytes = 64 KiB`;
- `MaxCodecBlockRawBytes = 1 MiB`;
- update-delta parts use smaller limits and are compacted sooner.

For update-delta parts, smaller limits means smaller logical row granules and
smaller per-substream codec-block ceilings so `Get` immediately after an update
does not routinely decode an 8192-row or 1 MiB block. The minimum block target
is advisory and may be ignored for tiny deltas. Initial experimental defaults:

- `UpdateDeltaTargetGranuleRows = 512`;
- `UpdateDeltaMinCodecBlockRawBytes = 0` or `16 KiB`, selected by benchmarks;
- `UpdateDeltaMaxCodecBlockRawBytes = 128 KiB`;
- `UpdateDeltaMaxRows = 4096`;
- `UpdateDeltaMaxBytes = 8 MiB`.

Compaction should trigger on delta age, total bytes, part fan-in for an id
range, or update-followed-by-read regression, not only on raw size.

The builder should close a logical granule when it reaches `TargetGranuleRows`
or when a row-aligned granule would make projected reads too coarse for a wide
column. It should close or split codec blocks independently per substream when
that substream reaches its byte targets. It may coalesce very narrow substreams
across several logical granules until the minimum raw-byte target where doing so
does not harm skipping, because the logical granule metadata still exists.

Too-small chunks waste marks and compression ratio; too-large chunks hurt point
reads, update-followed-by-read latency, and predicate exclusion. These tradeoffs
must be benchmarked rather than treated as fixed constants.

### 6.5 Substreams

Each logical column can have multiple physical substreams:

| Logical type | Substreams |
|---|---|
| nullable scalar | `nullmap`, `values` |
| bool | `nullmap`, `bits` or `values` |
| integer/date/time | `nullmap`, `values` |
| float | `nullmap`, `values` |
| string bytes | `nullmap`, `offsets`, `chars`, optional `dictionary` |
| low-cardinality string | `nullmap`, `keys`, `dictionary` |
| array | `nullmap`, `offsets`, element substreams |
| object/map later | `paths`, `offsets`, typed value substreams |

Specialized numeric codecs must never be applied to structural substreams such
as null maps, offsets, lengths, dictionary keys, or sparse-default positions
unless the codec is explicitly defined for that structural stream. This follows
ClickHouse's `only_generic` rule for `NullMap`, `ArraySizes`, `StringSizes`,
`DictionaryIndexes`, and `SparseOffsets`.

### 6.6 Physical Vector Encoding

All integer values in `TCS1` are little-endian unless a substream explicitly
states otherwise. Column vectors use these canonical raw layouts before codecs
run:

```text
fixed_width_values :=
    value[0] value[1] ... value[n-1]

nullmap :=
    bitset, one bit per row, 1 means null

bool_values :=
    bitset, one bit per row, 1 means true

string_offsets :=
    uint32_le offsets[n+1], offsets[0] = 0

string_chars :=
    byte[offsets[n]]

dictionary_keys :=
    uint8/uint16/uint32 keys[n], width selected by cardinality

dictionary_values :=
    string_offsets + string_chars for distinct values in key order

sparse_default :=
    default_value
    non_default_count uint32
    positions_delta_varint[non_default_count]
    non_default_values substream
```

The default fixed-width alignment is byte-packed with no padding between
values. Codecs that need a wider interpretation, such as `Delta(8)` or
`T64`, receive the logical width from the column type and must reject
misaligned raw byte counts.

String values are never stored as per-row length-prefixed blobs in the column
format. They must use offsets plus chars, or dictionary keys plus dictionary
values, so projected scans can skip char bytes when evaluating null/default or
dictionary predicates.

### 6.7 Mutable Writes, Updates, and Deletes

Column parts are immutable: do not patch compressed blocks in place. But an
update must still rewrite the changed rows into column-store format immediately.
The durable update representation is a replacement column delta part plus
visibility changes, not a permanent row-oriented overlay.

An update-delta part is a normal immutable column part with a short lifecycle.
`Delta` describes why the part exists and how aggressively maintenance should
compact it; it does not mean a field-level patch, a row-oriented overlay, or a
read-time pointer diff.

PR1 update-delta parts must contain complete replacement rows for the changed
primary ids:

- write every declared column for each changed row, including columns whose
  values did not change;
- use the same fixed schema, substreams, codecs, checksums, granules, block
  directory, and part descriptor shape as insert/base parts;
- sort rows by primary id inside the part;
- update the primary root so each changed id points directly to the replacement
  row locator;
- hide old locators for changed rows with tombstones/delete bitmaps;
- update secondary indexes for changed rows in the same root group.

Partial-column deltas can be studied later, but they are out of scope for the
first format because they complicate row reconstruction, secondary-index
updates, compaction, and crash recovery. If an update API receives a partial
mutation, the writer must read the old row's declared columns and build a
complete replacement `ColumnBatch` before publishing.

Update model:

1. Insert batches build immutable base or insert-delta parts.
2. An update resolves current `id -> row_locator` entries and reads the old
   declared-column values needed to produce complete replacement rows. An
   optimizer may first read only predicate/update columns to identify changed
   rows, but it must fetch all declared columns for rows it actually rewrites.
3. The updater builds a replacement `ColumnBatch` for changed rows.
4. The part builder writes that batch as one or more update-delta column parts
   using the same `TCS1` substreams, codecs, checksums, marks, and statistics
   as normal parts.
5. The write publishes, in one collection WAL transaction and one root group:
   - new update-delta part descriptors;
   - all required column side refs for those parts;
   - primary locator changes from old rows to replacement rows for changed ids;
   - delete bitmap/tombstone entries that hide old row locators for changed ids;
   - secondary index deletes for old indexed values from changed rows;
   - secondary index inserts for new indexed values from changed rows;
   - count/visibility metadata deltas when present;
   - the applied collection watermark update in the backend publish commit.
6. Reads see the newest primary locator for each id and treat tombstoned
   locators as invisible.
7. Maintenance compacts base parts, insert deltas, update deltas, and delete
   bitmaps into new base parts.
8. The compaction publish removes or supersedes the compacted source part
   descriptors and their superseded delete/mark metadata from the new active
   descriptor roots, while old snapshot roots keep those descriptors reachable
   for existing readers.
9. Old parts become unreachable from active roots and are reclaimed by column
   file GC or rewrite after snapshots release them.

Small foreground updates can batch in memory briefly to amortize part-builder
overhead, but the crash-recoverable representation must be columnar. A row
object may exist only as a transient planning/update-evaluation value.

Update pseudocode:

```text
update_rows(snapshot, ids, update_fn):
    old_rows = read_all_declared_columns(snapshot, ids)
    new_rows = apply_update_fn(old_rows)
    changed = filter_rows_where_new_row_differs(old_rows, new_rows)
    if changed.empty:
        return

    changed_old_rows = old_rows.only(changed.ids)
    changed_new_rows = new_rows.only(changed.ids)
    old_index_values = compute_secondary_values(changed_old_rows, affected_indexes)
    new_index_values = compute_secondary_values(changed_new_rows, affected_indexes)

    delta_parts = build_column_parts(changed.ids, changed.column_batch,
                                    kind=update_delta)
    root_group = new_root_group()
    root_group.add_part_descriptors(delta_parts)
    root_group.add_primary_locator_puts(delta_parts.locators)
    root_group.add_delete_tombstones(changed_old_rows.locators)
    root_group.add_secondary_deletes(old_index_values, changed.ids)
    root_group.add_secondary_puts(new_index_values, changed.ids)
    publish(root_group)
```

Tombstones, delete bitmaps, and secondary-index deletes must be derived from
`changed_old_rows`, not from every requested id. A no-op row in a mixed update
request must keep its original locator and secondary-index entries visible.

Deletes are the same visibility mechanism without replacement rows: publish
tombstones/delete bitmap updates, remove secondary index entries, and leave
the old column bytes to snapshot-safe GC/rewrite after compaction.

`Visibility changes` means root-level metadata changes, not a read-time pointer
diff chain. The primary root should map each id directly to its newest visible
row locator. Delete bitmaps/tombstones hide old locators during scans and
snapshot reads. A reader should not have to chase an unbounded list of pointer
diffs to reconstruct the current row.

The update design needs an explicit experimental gate. There are at least
three plausible physical strategies:

1. update-delta parts for changed rows only;
2. rewrite the affected column granule(s) and publish new locators;
3. hybrid: use update-delta parts for scattered writes and granule rewrite for
   dense updates.

The RFC chooses update-delta parts as the first implementation because it
keeps foreground writes bounded and preserves immutable base parts. Milestone 6
must benchmark this choice against granule rewrite on:

- random point update throughput;
- dense range update throughput;
- point read immediately after update;
- projected scan after 1 percent, 10 percent, and 30 percent churn;
- secondary-index update cost;
- compaction catch-up time and reclaimed bytes;
- delta-part count and maximum visible part fan-in per id range.

If update-delta parts make read amplification unacceptable before compaction,
the implementation should switch to the hybrid policy rather than hard-coding
delta parts forever.

### 6.8 Schema Evolution

Schema changes create new schema versions. A part is always written with one
schema version. Readers resolve missing columns by column defaults and
materialize old parts through a versioned schema adapter.

Rules:

- adding nullable or defaulted columns is metadata-only for old parts;
- dropping a column hides it from projections but does not rewrite old parts
  until maintenance;
- changing type creates a new column id and an adapter if safe;
- codec changes affect only new parts until recompression maintenance.

## 7. Compression Design

### 7.1 Codec Pipeline Model

Use ClickHouse's separation between transforms and generic compressors:

```text
typed transform(s) -> generic compression -> optional encryption later
```

Initial pipeline validation:

- `NONE` cannot be combined with other codecs.
- Generic compression must be the last non-encryption stage.
- Encryption, if added later, must be last.
- Type-specific transforms require compatible logical types.
- Structural substreams are generic-only unless explicitly whitelisted.
- Experimental codecs require an explicit option.

Persist the complete codec spec in the column part descriptor. Do not rely only
on method bytes because parameters such as zstd level, T64 mode, or inferred
width must survive reopen and maintenance.

Encoding pseudocode:

```text
encode_column_block(raw_vector, candidate_pipelines):
    best = raw_pipeline
    best_bytes = raw_vector.bytes

    for pipeline in candidate_pipelines excluding raw:
        if !pipeline.compatible(raw_vector.type, raw_vector.substream):
            continue
        tmp = raw_vector.bytes
        for codec in pipeline.codecs:
            tmp = codec.encode(tmp, raw_vector.logical_meta)
        if admission_accepts(raw_len=len(raw_vector.bytes),
                             encoded_len=len(tmp),
                             header_len=pipeline_header_cost):
            if len(tmp) < len(best_bytes):
                best = pipeline
                best_bytes = tmp

    return best.pipeline_id, best_bytes
```

Decoding pseudocode:

```text
decode_column_block(encoded, pipeline_id, dst_vector):
    pipeline = stream.pipeline_table[pipeline_id]
    tmp = encoded
    for codec in reverse(pipeline.codecs):
        tmp = codec.decode(tmp, dst_vector.logical_meta)
    append tmp into dst_vector
```

Transform codecs operate on raw substream bytes and typed metadata. They do
not allocate Go objects per row. Generic codecs operate on byte slices.

### 7.2 Proposed Codec IDs

Use a TreeDB-specific method byte space for `TCS1`; do not reuse ClickHouse
method bytes as persistent identifiers unless the payload is byte-compatible.

| ID | Codec | Stage | PR target |
|---:|---|---|---|
| `0x00` | `NONE` | generic/raw | PR1 |
| `0x01` | `SNAPPY` | generic | PR2 |
| `0x02` | `LZ4` | generic | PR2 |
| `0x03` | `ZSTD` | generic | PR2 |
| `0x04` | `ZSTD_DICT` | generic with dictionary id | PR2/PR3 |
| `0x10` | `BITPACK` | typed/structural | PR3 |
| `0x11` | `RLE` | typed/structural | PR3 |
| `0x12` | `DELTA` | numeric transform | PR3 |
| `0x13` | `DOUBLE_DELTA` | numeric transform | PR4 |
| `0x14` | `GCD` | numeric transform | PR4 |
| `0x15` | `T64` | integer transform | PR4 |
| `0x20` | `GORILLA` | float transform | PR5 |
| `0x21` | `FPC` | float transform | PR5 |
| `0x22` | `ALP` | experimental float transform | PR6 |
| `0x30` | `DICT_STRING` | string transform | PR3 |
| `0x31` | `SPARSE_DEFAULT` | sparse/default transform | PR3 |

These codec ids should map onto a canonical TreeDB compression registry, not a
one-off column-store implementation. TreeDB already has production paths for
snappy, lz4, zstd, zstd dictionaries, dictionary persistence in `dictdb/`,
no-expansion admission, and autotune/backoff. The column-store implementation
should initially wrap those routines behind the `TCS1` codec interface, then
promote the shared pieces into a single internal compression package that can
serve value-log frames, column parts, outer leaves, and future collection
filters.

Dictionary compression is especially important to keep unified. A `ZSTD_DICT`
codec must persist enough dictionary identity to decode after reopen. The
dictionary bytes should live in the existing dictionary store or a compatible
successor, and dictionary selection should support payload classes such as
`column/<collection>/<column_id>/<substream>` rather than only one global
value-log class.

Decode stability is part of the WAL side-ref contract. Codec ids, codec
parameters, codec registry version, dictionary ids, dictionary registry version,
and external compression metadata refs must be persisted in the descriptor or
manifest. Any external dictionary or compression metadata object required to
decode a published column payload is a required collection WAL side ref and must
remain protected until every dependent payload is unreachable.

### 7.3 ClickHouse Codec Application

Apply these ClickHouse techniques directly:

- `Delta`: for integer/date/time columns where adjacent values are smooth.
- `DoubleDelta`: for timestamp-like or monotonically increasing series.
- `GCD`: for integer/decimal values with common scaling factors.
- `T64`: for integer ranges whose high bits are stable within a block.
- `Gorilla`/`FPC`: for slowly changing float metrics.
- `ALP`: experimental for decimal-like floats after the codec framework is
  proven.
- `Sparse`: when default/null ratio exceeds a threshold. Start with
  ClickHouse's `0.9375` default ratio as a candidate, then tune.
- `ZSTD`: for cold/compacted parts or columns with high ratio wins.
- `LZ4`: default low-CPU generic codec for hot parts.
- `SNAPPY`: compatible with current TreeDB value-log block-compression
  defaults and useful as a low-risk hot-ingest baseline.
- `ZSTD_DICT`: for repeated strings, template-like values, marks/descriptors,
  or columns whose samples train a stable dictionary.

TreeDB should not blindly port every algorithm before the storage format is
stable. The staged path should first prove the codec pipeline and block
directory with `NONE`, `LZ4`, `ZSTD`, bool bitpacking, integer delta, strings,
and sparse/default encoding.

### 7.4 Compression Selection

Selection priority:

1. Column-level `Compression` binding in `ColumnDefinition`.
2. Collection-level `ColumnCompressionPolicy`.
3. Profile default (`fast`, `wal_on_fast`, `durable`, `bench`).
4. Engine default.

Suggested defaults:

- Hot ingest: `LZ4` or `NONE` if sampled data is incompressible.
- Compacted parts: `ZSTD(1..3)` plus typed transforms where profitable.
- Marks/descriptors: `ZSTD(3)` equivalent or TreeDB's existing zstd fastest
  dictionary path if a dictionary wins.
- Structural substreams: `LZ4`/`ZSTD`, bitpack/RLE for null maps and booleans.

Selection should be block-local but policy-driven:

- the collection chooses candidate pipelines;
- the part builder samples early blocks to seed a selector;
- every block can still fall back to raw through `PipelineID=0`;
- maintenance can rewrite old parts with stronger candidate pipelines;
- a high-entropy block can bypass compression without changing the column's
  configured default.

The selector should combine TreeDB's current value-log compression chooser with
a ClickHouse-inspired exploration/exploitation model:

- keep the existing no-expansion admission, minimum-size checks,
  high-entropy probes, and PAUSED/probe state;
- treat candidate pipelines as arms with observed encode ns, decode ns,
  compressed bytes, and kept/attempted ratio;
- score an arm by estimated saved IO time minus encode/decode CPU cost, with
  profile-specific weights for hot ingest versus cold storage;
- reserve a small exploration budget so the selector can detect data-shape
  changes;
- never explore outside a column's pinned pipeline list;
- persist selector stats at part or collection scope only when doing so
  improves reopen behavior in benchmarks.

Do not decide upfront whether the existing TreeDB chooser or the bandit-style
selector is the final architecture. Milestone 2 should ship the simpler shared
TreeDB chooser. Milestone 4 or 8 should compare it against the bandit-style
selector with deterministic fixtures before making it canonical.

Maintenance should support recompression:

```go
type ColumnRecompressionPolicy struct {
    MinPartAge        time.Duration
    MinPartBytes      int64
    TargetProfile     string // "hot", "warm", "cold"
    CodecOverrides    map[string][]CodecSpec
}
```

This is the TreeDB equivalent of ClickHouse TTL recompression and server-level
compression selectors.

### 7.5 Compression Admission

Never keep compressed bytes when they expand data. Also require a minimum
savings margin to avoid storing barely-smaller blocks that cost CPU:

```text
keep = encoded_size + header_size + min_savings < raw_size
```

Start with existing TreeDB practices:

- minimum payload bytes before generic compression;
- minimum savings bytes;
- high-entropy probes for LZ4-like paths;
- PAUSED/probe state for repeated incompressible blocks;
- wall-time autotune for hot value-log compression.

For column parts, add per-column block statistics:

- attempted blocks;
- kept blocks;
- raw bytes;
- compressed bytes;
- encode ns;
- decode ns;
- selected codec;
- rejected reason.

## 8. Read Path

### 8.1 Point Lookup

Point lookup by id:

1. Read `id -> row_locator` from the primary B-tree.
2. Check delete bitmap/tombstone visibility for the locator.
3. Read the part descriptor from descriptor cache or B-tree.
4. Decode only projected columns for the row's granule.
5. Materialize a row document only if the caller used row APIs.

The first implementation may decode a whole column block to return one row.
That is acceptable if point lookup remains guarded by benchmarks, but it should
not be the only planned path. ClickHouse gets much of its selective-read
performance from sparse primary indexes, marks, mark caches, and uncompressed
block caches; TreeDB should combine those ideas with KV-specific locators.

Point-read optimization ladder:

- decoded block cache keyed by `(part_id, column_id, block_ordinal)`;
- single-row fast paths for uncompressed fixed-width blocks;
- direct offset reads for fixed-width raw blocks without decoding neighboring
  rows;
- small row-group blocks for update-delta parts and point-read-heavy
  collections;
- optional row mini-cache keyed by `(part_id, row_ordinal, projection_hash)`;
- row materialization cache for hot point reads;
- optional small-row inline cache in primary locator value;
- adaptive part policy that reduces block size when point-read and
  update-followed-by-read benchmarks regress.

The point-read benchmark suite must report cold point reads, warm point reads,
update-followed-by-read, and mixed point/scan workloads. A projected scan win
is not enough if `Get` becomes too slow for collection users.

### 8.2 Column Scan

Column scan:

1. Snapshot collection catalog.
2. Enumerate visible parts by id range and predicate metadata.
3. Use min/max, null count, default count, and optional bloom/zone maps to skip
   granules.
4. Build the row-visibility mask for surviving granules from delete bitmaps,
   update-delta tombstones, primary id bounds, and row-id filters produced by
   secondary indexes or fast filters.
5. Decode only projected columns for granules with at least one candidate row.
6. Apply the row-visibility mask to the decoded batch.
7. Call the borrowed callback with the filtered vector batch.

The borrowed callback must never observe tombstoned rows or rows outside the
primary id range, secondary-index result, or fast-filter row set.

Update-delta parts are normal column parts for reads. They may be smaller and
more numerous than compacted base parts, but they use the same marks,
substreams, codecs, and visibility rules.

A vector batch should expose:

- typed value slices for fixed-width types;
- string offsets and char bytes for string columns;
- null bitmap;
- row id vector;
- release callback for pooled decode buffers.

### 8.3 Predicate Pushdown

Initial pushdown:

- primary id range;
- equality/range on secondary index through existing index roots;
- min/max per granule for typed columns;
- null-only/non-null checks;
- default-only checks for sparse columns.

Later pushdown:

- dictionary membership for low-cardinality strings;
- set indexes for low-cardinality granules;
- bloom filters for high-cardinality exact membership;
- token bloom filters for tokenized text, tags, and arrays;
- ngram bloom filters for substring and LIKE-style predicates;
- text/posting-list filters for workloads that need fewer false positives than
  bloom filters;
- vectorized predicate evaluation on decoded batches;
- conjunctive predicate planning across secondary indexes and column stats.

### 8.4 Fast Filters and Reusable Haystack Search

ClickHouse has several fast pruning/search families worth importing as
algorithms, not as SQL-only features:

- `minmax`: store comparable min/max per granule and reject impossible ranges;
- `set`: store the distinct values for a granule until a configured maximum,
  then mark the granule unknown;
- `bloom_filter`: probabilistic exact-membership filter for scalar or byte
  values;
- `tokenbf_v1`: tokenize text-like inputs and bloom-filter the tokens;
- `ngrambf_v1` and `sparse_grams`: bloom-filter byte ngrams for substring and
  LIKE-style predicates;
- `text`: dictionary plus posting-list style index for workloads that need
  stronger text filtering than bloom filters;
- byte-haystack searchers: compiled single-needle and multi-needle substring
  search, following ClickHouse's `StringSearcher`, `Volnitsky`, and
  `MultiVolnitsky` style of precomputing search state and scanning many
  haystacks with low per-row overhead.

The `set` filter should start as a distinct-value membership index, not a
histogram. Store enough metadata to make it operationally useful:

- `row_count`;
- `distinct_count`;
- `null_count`;
- `truncated/unknown` flag when distinct values exceed the configured maximum.

Per-value counts are not required for PR1. If workloads need count estimates,
add a separate count-min sketch, top-k, or posting-list/bitmap index rather
than overloading the set filter.

TreeDB should expose these through a reusable internal filter/search package
that can serve every collection layout:

```go
type ValueSource interface {
    RowCount() int
    TypedVector(column string) (ColumnVector, bool)
    ByteHaystack(column string, row int, dst []byte) ([]byte, bool)
}

type FastFilter interface {
    BuildGranule(src ValueSource, rows RowRange) (FastFilterGranule, error)
    MayMatch(granule FastFilterGranule, pred Predicate) TriState
    ApplyBatch(src ValueSource, pred Predicate, dst *Bitset) error
}

type ByteSearcher interface {
    Match(haystack []byte) bool
    MatchBatch(src ValueSource, column string, dst *Bitset) error
}
```

Column-store collections can build filters directly from column vectors and
store filter granules beside part descriptors or in `TCS1` side streams.
Row-store, BSON, and template-v1 collections can build the same filter granules
from row iteration, extracted fields, or whole-document byte haystacks. The
algorithm package should not know which layout produced the values.

Haystack search must be generic over "bytes to search", not limited to string
columns:

- string and `[]byte` columns expose their raw bytes;
- arrays/tags expose each element as a token stream for token bloom and
  multi-search;
- template-v1 or BSON rows can expose one field, a projected subdocument, or
  the whole encoded document as the haystack;
- scalar values may expose a canonical byte encoding only for explicit
  "encoded contains" predicates. Numeric range/equality predicates should use
  typed filters, not accidental decimal-string substring semantics.

Recommended implementation sequence:

1. Add shared `Bitset`, `TriState`, `ValueSource`, and predicate interfaces.
2. Implement min/max and set filters for typed vectors.
3. Implement exact bloom filters for scalar and byte values.
4. Implement token and ngram extractors that accept any byte haystack source.
5. Add compiled byte searchers for single-needle and multi-needle predicates.
6. Wire column-store part pruning first, then reuse the same package for
   row/template/BSON collection scan acceleration.

The important design point is that filter building and batch evaluation should
consume typed vectors when available, but fall back to row extraction or byte
haystack adapters without changing the algorithm implementation.

### 8.5 Fast Counts

TreeDB should support a fast exact `count(*)` path for column-store
collections. The design should not depend on scanning a hidden column.

Maintain count metadata at multiple levels:

- collection root aggregate: visible row count for the snapshot root;
- part descriptor: physical rows, rows visible when the part was published or
  compacted, deleted rows;
- granule descriptor: row count and deleted count;
- optional secondary/filter metadata: exact posting counts only when the index
  format can prove exactness.

For `count(*)` with no predicate, a reader should use the snapshot's collection
aggregate. A part-derived exact count is allowed only when the reader applies
the same snapshot visibility state as a row scan: the active part set, delete
or tombstone roots, update-delta tombstones, and the snapshot watermark. Raw
immutable part `VisibleRowCount` totals are exact only for snapshots whose
manifest proves no external visibility state can hide rows from those parts.
Otherwise they are diagnostic metadata or an upper bound, not an exact answer.

Inserts increment the aggregate, deletes decrement it, one-row replacement
updates leave it unchanged, and compaction must preserve it. For simple
predicates, min/max, set, bloom, and secondary indexes may prune work, but
approximate filters must not return an exact count without verifying rows or
using an exact posting/bitmap index.

Persisted count and visibility aggregates are root/system deltas. Inserts,
deletes, update-delta publishes, compaction, and recompression must publish
count/visibility changes in the same collection WAL transaction and backend root
group as primary locators, part descriptors, delete bitmaps, filters, and
secondary-index changes.

This is analogous to ClickHouse's ability to answer trivial counts from part
metadata, but it must respect TreeDB snapshots and delete bitmaps.

## 9. Write Path

### 9.1 Row Ingestion

For `InsertBatch(ids, documents)`:

1. Parse/validate documents through the existing format path.
2. Extract typed schema columns into a `ColumnBatch`.
3. Build secondary index runs using existing planner logic.
4. Build one or more column parts.
5. Build primary locator run.
6. Publish roots as one ordered root group.

For `InsertColumnBatch`:

1. Validate ids and vector lengths.
2. Validate types and null/default bitmaps.
3. Build secondary index state from direct vectors when the index field is
   projected; fall back to row materialization only for unsupported expressions.
4. Publish the same roots.

### 9.2 Update Write Path

For `Update` or callback-based mutation APIs:

1. Resolve target ids through the primary root under a snapshot.
2. Decode predicate/update columns and affected secondary-index columns to
   identify rows whose stored values actually change.
3. Fetch every declared column for each changed row, including untouched
   columns, from the current row locators.
4. Apply the mutation and build a complete replacement `ColumnBatch` for only
   the rows that actually changed.
5. Write the complete replacement rows as update-delta column parts.
6. Publish primary locator updates, delete tombstones for old locators,
   secondary index deletes, secondary index puts, and part descriptors in one
   root group.

For bulk updates that touch many adjacent ids, the writer may build larger
delta parts directly. For scattered point updates, the writer may buffer a
bounded micro-batch before the durable publish. In both cases, once the update
is durable, the changed row bytes live in the column-store format.

### 9.3 Part Builder

Part builder steps:

1. Sort rows by primary id unless input is declared sorted and verified.
2. Choose part row count from `ColumnPartPolicy`.
3. Split into granules.
4. Build per-column substreams.
5. Choose codec pipeline per substream.
6. Encode blocks, applying compression admission.
7. Write column files or packed update-delta containers under
   `Dir/maindb/columns/`.
8. Build part descriptor and primary locator run.
9. Return ordered root publish inputs.

### 9.4 Parallelism

Borrow ClickHouse's ordered parallel compression pattern:

- column blocks are encoded independently in a worker pool;
- each block has a sequence number;
- column file writes preserve deterministic descriptor order;
- bounded memory prevents compression workers from outrunning the writer;
- a direct encode-into-destination path is allowed when there is no pending
  earlier block.

Use TreeDB's existing benchmark and backpressure style. Do not start with
unbounded goroutine-per-column behavior.

### 9.5 WAL and Durability Integration

Durability decision: the column-store buildout should make collection WAL
semantics work for both column-store and non-column-store collections. Do not
build a column-store-only WAL, and do not defer this until after mutable
column-store writes are advertised as production-ready.

The current collection write-domain contract is flush-boundary durable:
acknowledged writes that remain in mutable, queued, or publishing collection
state are visible in-process but are not crash-durable until `Flush`,
`FlushAll`, close, or a synchronous publish barrier succeeds. That may be a
documented contract today, but it is too easy to misuse once column-store writes
also create external column files. A root that references missing column bytes,
or a recovered column file with no reachable root, is worse than a simple lost
buffered row write.

The prerequisite is a shared collection commit protocol. It can be implemented
as a replayable collection mutation log, by moving collection root-group
publication onto the existing backend WAL/commit-log path, or by an equivalent
manifest protocol. The protocol must cover row-store and column-store
collections so secondary indexes, primary roots, template/index-state roots,
delete roots, and column file references have one recovery story.

Column-store writes still follow TreeDB's lower-level durability model:
WAL/journal and payload storage are decoupled. Column file references should
follow the same durable-pointer rules as value-log pointers.

WAL-on write ordering:

1. Build column parts, filters, marks, and descriptors.
2. Write `TCS1` payloads, filter files, delete bitmap files, and dictionary
   files through the collection WAL side-ref prepare protocol.
3. Fsync files, atomically rename temp files to final paths, and fsync parent
   directories before the collection WAL commit marker may reference them.
4. Append the collection WAL transaction that describes root mutations, primary
   locator changes, granule/mark changes, secondary index changes,
   delete/tombstone changes, count/visibility metadata, part descriptor
   additions, and every required external side ref.
5. Validate that the declared side-ref set matches the complete canonical
   transitive closure embedded in part descriptors, manifests, filter
   descriptors, delete bitmap refs, dictionary refs, compression metadata refs,
   granule roots, and root-delta values.
6. Publish the root group only after the column bytes are readable at the
   required durability boundary.
7. On recovery, replay either the complete root group or none of it.

Column-store side refs use the same lifecycle as collection WAL side refs:

- temp-only files with no committed WAL/root reference are orphan-prepared and
  may be quarantined or deleted after recovery;
- a complete WAL-on transaction that references a missing or corrupt column
  file, filter file, delete bitmap, or dictionary is a recovery error, not a
  skipped column-store write;
- optional/rebuildable filters may be absent, but a published filter root entry
  that names external bytes makes those bytes required side refs. Missing
  optional filters degrade to "filter absent"; they must never produce
  false-negative reads;
- before root publication, required column side refs are retained by the
  collection WAL protected side-ref index;
- after root publication, WAL-only protection may be released only after the
  applied collection watermark is durable, root descriptors are durable, and the
  column-file reachability tracker or a full reachability scan has incorporated
  the published roots;
- cleanup must retain files referenced by unapplied WAL, published roots, old
  snapshots, and active iterators.

WAL-off relaxed mode may acknowledge writes according to current TreeDB
semantics, but it must not weaken pointer safety: a visible root must never
reference missing column bytes after normal reopen within that mode's existing
guarantees.

Minimum shared collection durability deliverables:

- an explicit durable boundary for collection writes in API docs and tests;
- a commit record or manifest entry that names every root mutation in the
  collection root group;
- external file references in the same commit record when column files are
  involved;
- validation that declared external refs match the canonical refs embedded in
  column descriptors and root deltas;
- recovery that replays complete committed root groups and ignores incomplete
  ones;
- cleanup of prepared but unpublished column files, and retention of files
  referenced by WAL, roots, snapshots, or active iterators;
- row-store indexed insert/update/delete recovery tests with secondary indexes;
- column-store insert/update/delete recovery tests with secondary indexes and
  column files;
- read-only open with unapplied collection WAL must fail with a recovery-required
  error in production mode, rather than silently hiding acknowledged column-store
  writes.

The recommended sequencing is option c from the planning discussion: make WAL
semantics work for both column-store and non-column-store collections as part of
this buildout. Option b creates a second durability model for collections.
Option d leaves a known sharp edge under a feature that adds more side files.
Option a is acceptable only if it is treated as the first column-store
milestone, not as unrelated work.

Project-management hard gate: Milestone 0.5 blocks most column-store
implementation. Before the shared collection durability story is resolved, the
project may proceed with documentation, benchmark fixtures, pure codec packages,
filter/search packages, and isolated `TCS1` encode/decode experiments that do
not publish collection roots. It should not merge production or public-facing
column-store collection behavior that writes persistent column files, publishes
part descriptors, exposes mutable column-store collection APIs, wires secondary
indexes to column-store rows, or claims crash/reopen safety. Those pieces must
land on top of the shared collection commit/recovery protocol.

The column-store roadmap must reserve tests for:

- crash before root publish after writing column bytes;
- crash after commit-log append before root publish;
- crash during update-delta publish with secondary index changes;
- crash during compaction publish;
- recovery with WAL enabled and WAL disabled profiles;
- column file GC/rewrite while old snapshots still reference old column parts.

## 10. Caches and Metadata

Add small bounded caches:

- part descriptor cache by `part_id`;
- mark cache by `(part_id, column_id)`;
- decoded block cache by `(part_id, column_id, block_ordinal)`;
- dictionary cache by dictionary id;
- schema adapter cache by `(from_version, to_version)`.

ClickHouse practice to copy:

- marks are small and worth compressing on disk but caching decoded in memory;
- uncompressed block cache should be optional and bounded;
- seekable readers need compressed offset plus offset inside decoded block;
- raw/NONE blocks can alias payload bytes and avoid copies when safe.

## 11. Maintenance

Column-store maintenance has three jobs:

1. Compact small parts, update-delta parts, and delete bitmaps into larger
   base parts.
2. Recompress hot parts into warm/cold codecs.
3. Reclaim old column files or packed delta containers through GC/rewrite.

Part compaction algorithm:

```text
compact_column_parts(snapshot, source_parts, target_policy):
    acquire snapshot pins for source column files
    build merge iterator over base parts, delta parts, and delete bitmaps
    emit new row-aligned ColumnBatch chunks
    encode chunks into new parts using target codec policy
    publish new descriptors, primary locators, delete metadata, and system root
    remove/supersede source part descriptors and obsolete delete/mark metadata
        from the new active descriptor roots in the same root group
    keep source parts reachable through old snapshot roots only
    let column file GC/rewrite reclaim old files or packed containers
```

Column compaction, delete-bitmap compaction, and recompression that create new
external column files must publish through collection WAL. The transaction must
include new side refs, new descriptors, active descriptor supersession or
deletion of source descriptors, obsolete delete/mark/filter metadata removal,
count/visibility metadata preservation, and unchanged logical secondary-index
state in one atomic root group.

The compaction publish must make the active descriptor roots a replacement
manifest for the affected key ranges, not an append-only list of all historical
parts. New scans must enumerate the new compacted descriptors and must not also
enumerate the source base/delta descriptors that fed the compaction. Existing
snapshots remain safe because they still reference the old roots that contain
the source descriptors; GC/rewrite can reclaim source files only after those
snapshot roots drain.

A part descriptor must record enough source stats to make maintenance
observable:

- rows copied;
- rows deleted;
- raw bytes read;
- compressed bytes read;
- raw bytes written;
- compressed bytes written;
- codec transitions;
- elapsed encode/decode/write time.

## 12. Test Strategy

### 12.1 Format and Codec Tests

Required tests for every format PR:

- exact-byte golden files for block headers and descriptors;
- round-trip encode/decode for every type;
- corrupt block fuzzing;
- unknown codec rejection;
- checksum mismatch rejection;
- endian stability;
- max length and overflow checks;
- old schema/new schema adapter tests.

### 12.2 Collection Correctness Tests

Required collection tests:

- create/open/reopen column-store collection;
- `InsertBatch` row ingestion parity with row-store collection;
- `InsertColumnBatch` direct ingestion;
- point `Get` parity;
- `ScanDocuments` parity;
- `ScanColumnBatches` projection parity;
- secondary unique and nonunique index parity;
- delete bitmap and update-delta-part correctness;
- secondary index correctness after column-store updates and deletes;
- WAL/recovery correctness for column-part publishes;
- exact count metadata correctness across insert, update, delete, compaction,
  reopen, and active snapshots;
- compaction while snapshots are open;
- column file GC does not remove reachable column parts;
- offline rewrite preserves column locators;
- schema add/drop/default behavior.

### 12.3 Fuzz and Property Tests

Property tests:

- random schemas, random batches, random nulls/defaults;
- compare column-store results with an in-memory row map;
- random updates/deletes and compaction checkpoints;
- random codec pipelines constrained by validation rules;
- projection/predicate equivalence with full materialization.

### 12.4 Benchmark Harness

Add a deterministic suite:

```bash
go run ./cmd/columnstore_bench -suite columnstore -validate
```

Required output:

- docs/sec;
- ns/doc;
- scan rows/sec by projection;
- point reads/sec, cold and warm;
- update ops/sec;
- update-followed-by-read ops/sec;
- exact count latency;
- encoded bytes/doc;
- raw bytes/doc;
- compression ratio;
- kept/attempted compression blocks by codec;
- encode MB/sec;
- decode MB/sec;
- allocations/op;
- part count and granule count;
- update-delta part count and maximum visible fan-in;
- column WAL bytes/row and side-ref metadata bytes/row;
- side-ref closure validation refs/sec;
- recovery rows/sec and file refs/sec for 1K, 100K, and 1M row fixtures;
- orphan prepare/final cleanup time and files/sec;
- GC protected-byte debt by payload, filter, delete bitmap, dictionary,
  manifest, and compression metadata class;
- read-only recovery-required scan latency without full column payload parsing;
- maintenance rewrite stats.

Extend canonical collections:

```bash
make bench-collections-canonical
```

with new rows:

- `treedb_columnstore_collection_0_indexes`
- `treedb_columnstore_collection_1_indexes`
- `treedb_columnstore_collection_2_indexes`
- `treedb_columnstore_collection_3_indexes`
- `treedb_columnstore_direct_vectors`
- `treedb_columnstore_update_then_get`
- `treedb_columnstore_count_star`

### 12.5 JSONBench Integration

Add a TreeDB column-store lane to the external JSONBench TreeDB harness
(`~/dev/snissn/JSONBench/treedb`) once the column-store scan API can express the
five Bluesky analytics queries. This is a comparison benchmark, not the first
correctness gate, and it must follow JSONBench rules.

The comparison target is benchmark-shape compatibility with the ClickHouse JSON
store setup in `clickhouse/ddl.sql` and `clickhouse/queries.sql`, not
byte-for-byte ClickHouse storage compatibility. TreeDB should include a declared
column-store schema mode that mirrors the ClickHouse-declared JSON paths:

- `kind`;
- `commit.operation`;
- `commit.collection`;
- `did`;
- `time_us`;
- the original JSON document, or a reconstructable equivalent, so the result can
  be labeled as retaining JSON structure.

Required harness changes:

- extend `treedb/run_matrix.sh` and `treedb/cmd/jsonbench_treedb` with a
  `columnstore` format or storage mode in addition to the existing `json` and
  `template-v1` modes;
- load the same NDJSON rows used by JSONBench at `1m`, `10m`, `100m`, and
  optionally `1000m` scales;
- run full-document row-store/template-v1 baselines and the column-store
  fixed-schema mode from the same harness;
- label the column-store schema, primary/sort order, secondary indexes, codec
  profile, granule settings, durability profile, and query-cache policy in the
  result JSON;
- disable query result caching and document any block/mark cache warmup policy;
- write one `result.json` per TreeDB cell and a machine-readable `report.json`
  that can import local DuckDB and ClickHouse result directories.

The column-store lane must implement the five canonical JSONBench queries:

- `q1`: count by `commit.collection`;
- `q2`: filtered count plus exact distinct `did` by collection for
  `kind = commit` and `operation = create`;
- `q3`: filtered hourly count for post, repost, and like collections using
  `time_us`;
- `q4`: first post timestamp per user;
- `q5`: activity span per user.

Required result fields:

- system, engine/layout, scale, requested rows, loaded rows, and source dataset;
- query attempts, best seconds, median seconds, rows scanned, result row count,
  and deterministic result hash;
- total bytes, data bytes, index bytes, bytes per row, file count, part count,
  granule count, and retained JSON structure flag;
- codec choices and compression stats for every declared column;
- load time, checkpoint time, optional compaction time, and maintenance stats;
- imported DuckDB and ClickHouse rows in the aggregate report when local result
  directories are supplied.

Correctness gates:

- `DATA_DIR=./testdata/bluesky SUBSET_ROWS=6 TRIES=1 ./run_matrix.sh` continues
  to pass as a smoke fixture;
- `SCALES=1m DATA_DIR="$HOME/data/bluesky" TRIES=3 ./run_matrix.sh` writes
  TreeDB column-store results for `q1` through `q5`;
- column-store result hashes match the existing TreeDB row-store/template-v1
  query implementations for every query and scale being compared;
- imported local DuckDB and ClickHouse result rows produce the same ordered
  answer sets on the `1m` fixture before performance numbers are interpreted.

Performance gates:

- no ClickHouse-relative product gate in the first column-store release;
- each JSONBench query must report TreeDB row-store/template-v1 and TreeDB
  column-store timings from the same run so the column-store delta is visible;
- projected column-store queries that only need declared paths should beat the
  TreeDB full-document row-store baseline on `1m` and `10m`;
- storage reporting must distinguish retained JSON bytes from declared-column
  bytes so projection-only results are not mistaken for a full JSON storage
  comparison.

### 12.6 Completeness Audit

Every implementation PR should update a traceability checklist that connects
design text to tests and gates. The checklist should answer:

- which RFC sections the PR implements or changes;
- which exact tests cover those sections;
- which benchmark rows/gates changed;
- whether secondary indexes, WAL/recovery, compression stats, and count
  metadata are affected;
- whether the change introduces a new on-disk format byte or only new policy;
- whether old snapshots, column file GC/rewrite, and compaction remain safe.

No milestone should be considered complete unless every deliverable has at
least one correctness test and every risky performance claim has a benchmark
row.

## 13. Roadmap and Gates

All gates are relative to the same host and same run unless explicitly marked
deterministic. Use `benchstat` with `-count=5` for Go microbenchmarks and
machine-readable validation for deterministic suites.

Traceability matrix:

| Design area | RFC sections | Milestones | Required evidence |
|---|---|---|---|
| Fixed schema and APIs | 5.1, 6.8 | M1, M3 | schema golden tests, row/direct ingestion parity |
| Physical layout and chunking | 6.2-6.6 | M1, M2, M3 | descriptor golden tests, chunk-size benchmarks |
| Compression registry and chooser | 7.1-7.5 | M2, M4, M8 | codec tests, chooser stats, ratio/throughput gates |
| Point reads and scans | 8.1-8.3 | M5 | point-read, scan, allocation, cache gates |
| Fast filters and haystack search | 8.4 | M5 | false-negative tests, adapter parity, search benchmarks |
| Fast counts | 8.5 | M5, M6 | exact count tests and count latency gates |
| Collection durability | 2, 9.5 | M0.5, M1, M6 | row/column recovery tests, pointer-safety tests |
| Writes, updates, WAL | 6.7, 9.1-9.5 | M0.5, M6 | recovery tests, update/read churn benchmarks |
| Maintenance and GC | 11 | M6, M8 | snapshot compaction tests, reclaimed-byte gates |
| JSONBench analytics | 12.5 | M0, M8 | JSONBench TreeDB lane, result parity, imported ClickHouse/DuckDB reports |

### Milestone 0: Baseline and Fixtures

Deliverables:

- freeze representative datasets:
  - collection benchmark document shape from existing tests;
  - monotonic timestamp/int shape;
  - low-cardinality strings;
  - sparse nullable fields;
  - high-entropy strings;
  - update/delete churn shape;
  - update-followed-by-read shape;
  - count-star shape;
- add `cmd/columnstore_bench` skeleton with row-store/template-v1 baselines;
- inventory the existing `~/dev/snissn/JSONBench/treedb` harness and define the
  column-store result fields needed to compare with local ClickHouse/DuckDB
  JSONBench runs;
- add machine-readable result schema and guardrail checks.

Tests:

- runner config parsing;
- result schema validation;
- guardrail failures for missing baseline rows.
- JSONBench smoke fixture remains runnable through
  `DATA_DIR=./testdata/bluesky SUBSET_ROWS=6 TRIES=1 ./run_matrix.sh`.

Performance gates:

- no product gate yet;
- benchmark runner overhead under 1 percent on a no-op fixture;
- results include bytes/doc and docs/sec for every configured row.
- results include point-read, update, update-followed-by-read, and count
  latency placeholders even before product gates turn on.

### Milestone 0.5: Blocking Collection Durability Prerequisite

This milestone is a hard boundary for project planning. Milestone 1 and later
may be designed in parallel, but production implementation of persistent
column-store collection behavior should not proceed past isolated format,
codec, filter, and benchmark work until this milestone passes.

Deliverables:

- current collection flush-boundary durability contract captured as executable
  tests;
- shared collection commit protocol for root-group publication, or an explicit
  integration with the existing backend WAL/commit-log path;
- commit metadata that can include primary roots, secondary roots,
  template/index-state roots, delete roots, part descriptors, and external file
  references;
- explicit column root-kind inventory aligned with the collection WAL spec;
- side-ref closure validator for descriptors, manifests, filters, delete
  bitmaps, dictionaries, compression metadata, and root-delta payloads;
- `.prepare/txn-<wallsn>/part-<part-id>/` column file prepare/finalize
  protocol;
- `PartID` and `FileID` allocator recovery from roots, pending WAL, and prepare
  directories;
- production read-only open behavior that fails recovery-required when unapplied
  collection WAL exists;
- recovery path that replays complete committed collection root groups and
  ignores incomplete ones;
- cleanup path for prepared but unpublished column files.

Tests:

- checkpoint does not accidentally promise durability for unflushed
  collection-local writes;
- close and reopen preserves flushed row-store collection writes with secondary
  indexes;
- crash/reopen replays or discards a row-store indexed insert/update/delete as
  one root group;
- crash/reopen never exposes a root that references a missing external file;
- prepared but unpublished column files are reclaimed or quarantined after
  recovery.
- descriptor side-ref closure mismatch fails before publish/replay;
- missing or corrupt manifest, substream, filter, delete bitmap, dictionary, or
  compression metadata side refs fail recovery for complete transactions;
- read-only open with unapplied collection WAL returns recovery-required;
- `PartID` and `FileID` allocators do not reuse ids from pending or unclassified
  prepared/orphan parts.

Gates:

- production column-store collection implementation is blocked until this
  milestone passes, except for isolated format/codec/filter/benchmark work that
  does not publish collection roots;
- column-store mutable writes remain experimental until this milestone passes;
- WAL-on collection write throughput regression is measured against the current
  row-store indexed path before optimizing the column format around it.

### Milestone 1: On-Disk Descriptor and Raw Column Parts

Deliverables:

- `TCS1` envelope and part descriptor structs;
- `Dir/maindb/columns/` file class with manifest-driven part layout;
- fixed schema metadata with explicit rejection of lazy column creation;
- raw `NONE` fixed-width scalar columns;
- primary `id -> row_locator` root with row-ordinal to granule mapping;
- part descriptor root and optional granule-mark root contract;
- reopen support.

Tests:

- exact-byte descriptor golden tests;
- fixed-schema validation tests;
- scalar round trips for bool/int64/float64/string-id placeholders;
- locator-to-granule mapping tests across projected columns;
- reopen parity;
- column file GC reachable-part protection.

Gates:

- raw column-store insert throughput at least 70 percent of row-store
  template-v1 zero-index insert throughput on the same fixture;
- raw fixed-width projected scan at least 2.0x faster than `ScanDocuments`
  materializing template-v1 documents;
- raw column bytes/doc no more than 1.25x template-v1 zero-index bytes/doc
  before compression.

### Milestone 2: Generic Compression Blocks

Deliverables:

- codec pipeline registry;
- `NONE`, `SNAPPY`, `LZ4`, `ZSTD`, and `ZSTD_DICT` wrappers around the shared
  TreeDB compression registry where possible;
- simple TreeDB-style compression chooser with no-expansion admission;
- block directory, per-block checksums, min/max raw/compressed accounting;
- direct decode into caller-owned vector buffers;
- no-expansion admission.

Tests:

- codec golden tests;
- corrupt input tests;
- unknown codec tests;
- generic-only structural stream validation;
- direct decode buffer reuse tests.
- zstd dictionary id persists and decodes after reopen.

Gates:

- high-entropy fixture stores raw or near-raw with compression attempt rate
  bounded under 10 percent after warmup;
- compressible fixed-width fixture achieves compressed/raw ratio <= 0.55 with
  LZ4 and <= 0.40 with ZSTD;
- snappy hot-ingest throughput is measured as a baseline against LZ4 and raw;
- zstd dictionary fixture beats non-dictionary zstd by >= 10 percent bytes/doc
  without losing more than 15 percent decode throughput;
- decode projected fixed-width scan remains at least 1.5x faster than
  row-document scan;
- compression-off ceiling loses no more than 10 percent insert throughput from
  Milestone 1.

### Milestone 3: Strings, Nulls, Booleans, and Sparse Defaults

Deliverables:

- null map substream;
- bool bitpack/RLE;
- string offsets/chars substreams;
- low-cardinality string dictionary blocks;
- sparse/default encoding with a threshold starting at 0.9375;
- JSON/BSON/template-v1 row ingestion into typed vectors.

Tests:

- null/default parity;
- string offset corruption fuzzing;
- low-cardinality dictionary round trip;
- sparse threshold selection;
- row ingestion parity against current collection APIs.

Gates:

- boolean columns encode to <= 0.08 bytes/value before generic compression;
- sparse default fixture stores <= 0.20x raw bytes when defaults >= 95 percent;
- low-cardinality string fixture stores <= 0.35x raw bytes after dictionary
  plus generic compression;
- row-ingest indexed insert throughput at least 60 percent of template-v1
  indexed insert throughput, while direct-vector insert reaches at least 90
  percent of template-v1 zero-index throughput.

### Milestone 4: Integer Time-Series Codecs

Deliverables:

- Delta;
- DoubleDelta;
- GCD;
- T64 byte mode;
- codec selection heuristics by column stats;
- experimental bandit-style selector behind a feature flag or benchmark-only
  path;
- maintenance recompression from hot defaults to cold codecs.

Tests:

- exact-byte tests for each owned transform;
- width inference tests;
- overflow/wrap tests;
- structural-stream rejection tests;
- recompression preserves query results.
- selector comparison report against the Milestone 2 TreeDB-style chooser.

Gates:

- monotonic timestamp fixture ratio <= 0.10 with DoubleDelta + generic
  compression;
- scaled decimal/integer fixture ratio <= 0.25 with GCD + generic compression;
- narrow-range integer fixture ratio <= 0.20 with T64 + generic compression;
- encode throughput for these transforms at least 250 MB/sec per core on the
  deterministic microbench fixture or no worse than 25 percent below generic
  compression for the same ratio class.
- bandit-style selector is not promoted unless it improves weighted
  ratio/throughput score on at least two deterministic fixtures without
  regressing high-entropy fallback.

### Milestone 5: Projection and Predicate Pushdown

Deliverables:

- `ScanColumnBatches`;
- min/max granule pruning;
- null/default pruning;
- secondary-index-to-column-batch scan path;
- shared fast-filter interfaces for typed vectors and byte haystacks;
- set and exact bloom filters;
- token and ngram bloom filters for byte-haystack predicates;
- compiled single-needle and multi-needle byte searchers;
- exact fast `count(*)` from snapshot/part metadata;
- decoded block cache;
- mark cache.

Tests:

- projected scan parity;
- range predicate parity;
- secondary index scan parity for row ingestion and direct vector ingestion;
- secondary-index-to-column projection parity;
- fast-filter false-negative tests;
- filter adapter parity across column-store, row-store, template-v1, and BSON
  fixtures where the same logical fields exist;
- byte-haystack search parity for string fields, byte fields, arrays/tags, and
  whole-document adapters;
- limit behavior;
- borrowed-slice lifetime tests;
- cache hit/miss accounting tests.
- exact count tests across insert, update, delete, compaction, reopen, and
  active snapshots, including uncompacted delete roots and update-delta
  tombstones.

Gates:

- one-column full scan >= 3.0x row-document scan rows/sec;
- three-column full scan >= 2.0x row-document scan rows/sec;
- range predicate with 90 percent granule pruning >= 5.0x unpruned scan
  rows/sec;
- negative exact-membership bloom probes prune >= 90 percent of eligible
  granules at the configured false-positive target on the deterministic
  fixture;
- multi-needle byte search over a batch is at least 2.0x faster than compiling
  per row for the same haystacks and needles;
- unfiltered `count(*)` is O(visible parts) or better and at least 20x faster
  than scanning one projected column on the count-star fixture;
- stale immutable part descriptor totals are never used as exact `count(*)`
  results when snapshot delete or update-delta visibility can hide rows;
- projected scan allocations <= 0.25 allocations per 1000 rows in borrowed API
  steady state.

### Milestone 6: Updates, Deletes, and Delta-Part Compaction

Deliverables:

- update-delta column parts;
- experimental granule-rewrite and hybrid update benchmarks, even if not
  productized;
- delete bitmap handling;
- compaction from base parts, delta parts, and delete bitmaps into new base
  parts;
- collection WAL transaction classes for update-delta, delete, compaction,
  delete-bitmap compaction, and recompression publishes;
- active descriptor-root deletion/supersession of compacted source parts and
  obsolete delete/mark metadata;
- snapshot-safe publish and GC integration;
- online and offline maintenance hooks.

Tests:

- randomized update/delete parity;
- updated rows are durably stored in column parts after reopen, with no
  permanent row-overlay dependency;
- compaction with active snapshots;
- post-compaction scans do not return duplicate rows from both compacted parts
  and source parts;
- crash/reopen around compaction publish;
- crash/reopen around recompression publish;
- missing/corrupt side-ref closure member fails recovery before publishing roots;
- column file GC/rewrite after compaction;
- unique secondary correctness after updates;
- crash/reopen around update-delta publish with secondary index changes.

Gates:

- foreground random update throughput with update-delta column parts at least
  70 percent of current row-store update path for the same indexed shape;
- update-delta part count is bounded by the configured micro-batch policy and
  does not grow one durable part per row under the default bulk-update path;
- point read immediately after update is no worse than 2.0x clean-dataset point
  read p50 and p95 on the deterministic churn fixture;
- projected scan after 10 percent churn is no worse than 1.5x clean-dataset
  scan p50 before compaction, and returns to within 1.15x after compaction;
- compacted bytes/doc after 30 percent churn no more than 1.20x compacted
  insert-only bytes/doc;
- compaction reclaims at least 80 percent of bytes made unreachable in a
  deterministic churn fixture.

### Milestone 7: Float Codecs and Experimental ALP

Deliverables:

- Gorilla;
- FPC;
- optional ALP behind `AllowExperimentalColumnCodecs`;
- float codec selector.

Tests:

- NaN/Inf/negative-zero behavior;
- exact round trips for Float32/Float64;
- ALP fallback-to-raw cases;
- codec selector avoids bad float codecs.

Gates:

- slowly changing float fixture ratio <= 0.35 with Gorilla or FPC;
- decimal-like float fixture ratio <= 0.25 with ALP where exact
  integerization applies;
- random float fixture stores raw/generic and does not waste more than 10
  percent insert CPU after warmup.

### Milestone 8: Production Profiles and Canonical Comparison

Deliverables:

- profile defaults for durable/fast/wal_on_fast/bench;
- persisted `format.json` extension for column-store format knobs;
- canonical benchmark integration;
- JSONBench TreeDB column-store lane in `~/dev/snissn/JSONBench/treedb`;
- operational docs.

Tests:

- profile normalization;
- env override conflict checks;
- format config load/apply;
- canonical report guardrails;
- JSONBench `q1` through `q5` result-hash parity against existing TreeDB
  row-store/template-v1 query implementations;
- JSONBench local report import for DuckDB and ClickHouse result directories.

Gates:

- two-index direct-vector column-store insert >= 80 percent of template-v1
  two-index insert docs/sec;
- two-index row-ingest column-store insert >= 65 percent of template-v1
  two-index insert docs/sec;
- compacted bytes/doc <= 75 percent of template-v1 compacted bytes/doc on the
  typed benchmark fixture;
- projected analytical scans >= 3x row-store scans;
- high-entropy fixture compacted bytes/doc <= 1.10x row-store bytes/doc;
- JSONBench `1m` column-store run completes for `q1` through `q5`, writes
  machine-readable `result.json` and `report.json`, and reports row-store and
  column-store timings from the same run;
- JSONBench projected column-store queries that only need declared paths beat
  the TreeDB full-document row-store baseline on `1m` and `10m`.

## 14. Best Practices to Import from ClickHouse

1. Use granules and marks as first-class storage objects.
2. Store enough mark metadata to seek without scanning prior compressed blocks.
3. Separate logical columns into physical substreams.
4. Strip specialized codecs from structural substreams.
5. Persist codec descriptions, not just method bytes.
6. Validate codec pipelines before writing data.
7. Use fast defaults for hot data and stronger compression during maintenance.
8. Keep block sizes bounded: small enough for selective reads, large enough for
   compression ratio.
9. Cache marks and optionally cache decoded blocks.
10. Decode directly into caller buffers when the caller consumes a whole block.
11. Avoid copies for raw/NONE blocks when lifetime makes aliasing safe.
12. Parallelize compression but preserve deterministic write order.
13. Treat sparse/default-heavy columns as a storage kind, not merely a codec.
14. Recompress through maintenance instead of making foreground writes choose
    cold-storage codecs.
15. Keep operational stats for raw bytes, compressed bytes, codec choices,
    skipped granules, and cache hit rates.
16. Keep skip indexes and byte-haystack searchers as reusable algorithms with
    storage-layout adapters.
17. Use set, bloom, token bloom, ngram bloom, and text/posting-list filters as
    staged pruning layers before decoding full column blocks.
18. Reuse and improve TreeDB's canonical compression machinery rather than
    building an isolated column-store codec stack.
19. Keep exact row-count metadata in descriptors so trivial counts avoid data
    scans.
20. Treat WAL/recovery ordering as part of the storage format, not an
    integration afterthought.

## 15. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Point lookup becomes slower because a whole block is decoded for one row. | Primary locator root, decoded block cache, optional inline hot-row cache, and explicit point-read gates. |
| Random updates cause write amplification or read amplification. | Write changed rows into bounded update-delta column parts, batch scattered point updates where allowed, benchmark against granule rewrite/hybrid strategies, and compact asynchronously. |
| Compression burns CPU on incompressible data. | No-expansion admission, high-entropy probes, PAUSED/probe autotune, and per-column stats. |
| Column-store compression diverges from existing TreeDB compression. | Start with wrappers around TreeDB's snappy/lz4/zstd/zstd-dict routines and migrate toward one canonical compression registry. |
| WAL integration is underspecified. | Block persistent column writes on side-ref closure, prepare/finalize, allocator recovery, read-only recovery-required behavior, root-group publish ordering, and recovery tests. |
| Schema evolution makes old parts hard to read. | Version every part and use schema adapters with golden tests. |
| Flexible schemas create unpredictable columns and compression choices. | Keep `TCS1` fixed-schema only; defer lazy/flexible schema mode to a separate design. |
| Secondary indexes duplicate too much data. | Keep current secondary root format first; later consider optional row-locator payloads, compressed posting-list values, or bitmap payloads for low-cardinality shapes. |
| Secondary indexes drift from column-store visibility after updates. | Publish secondary deletes/inserts, primary locator changes, part descriptors, and delete bitmaps in the same root group; test crash/reopen and active snapshots. |
| Compaction leaves source parts active. | Publish compacted descriptors and descriptor-root deletes/supersession for all source parts through collection WAL in one root group; test post-compaction scans for duplicate/stale rows and delayed GC with active snapshots. |
| Fast filters produce wrong answers. | Treat filters as pruning only, use tri-state `MayMatch`, forbid false negatives in property tests, and always verify matches on decoded rows or exact secondary entries. |
| Haystack search semantics become confusing for non-string types. | Require explicit byte-haystack adapters; use typed filters for numeric equality/range and reserve encoded-byte substring search for explicit predicates. |
| Fast count metadata becomes stale. | Publish count deltas in the same root group as inserts/deletes and verify across update, compaction, reopen, and snapshots. |
| Column file GC deletes live column parts. | Descriptors and locators are normal roots; GC reachability must scan column file references, with tests. |
| Borrowed vector API is unsafe for general users. | Provide safe materializing APIs and document borrowed lifetime like existing borrowed APIs. |
| ClickHouse codecs are overfit for analytical time series. | Stage codecs and keep generic compression as the baseline; require dataset-specific gates before enabling selectors. |
| Format churn blocks development. | TreeDB is pre-alpha; isolate format in `TCS1` and exact-byte tests. |

## 16. Open Questions

1. Should the primary root map id to row locator only, or should it also keep a
   small inline materialized row for hot point reads?
2. Should part descriptors live entirely in a B-tree value, or should large
   mark arrays be stored in the value log with descriptor pointers?
3. Should column parts be sorted by primary id only, or allow alternate sort
   keys per collection?
4. Should secondary index values stay id-only forever, or should a later format
   add optional row-locator payloads as a point-read cache?
5. Should compaction preserve part-level physical clustering by primary id,
   insertion order, or a configured sort key?
6. Which ZSTD implementation should be used for generic column blocks:
   existing `snissn/compress/zstd`, `klauspost/compress/zstd`, or both behind
   a small interface?
7. Should `TCS1` use CRC32C only, or add a 128-bit checksum for compressed
   block payloads after the format proves useful?
8. Which fast-filter metadata belongs inside `TCS1` side streams versus shared
   `filters/<name>` roots used by non-column collection layouts?
9. Which byte-haystack adapters should be enabled by default for BSON and
   template-v1: field-only, whole-document, or both behind explicit options?
10. Should compacted/base parts always use one file per column substream, or
    should very narrow columns be grouped by default?
11. Should the canonical compression registry live under the existing value-log
    compression packages, the existing internal compression package, or a new
    shared package consumed by both?
12. What churn threshold should switch updates from delta parts to granule
    rewrite or hybrid rewrite?

## 17. Adversarial Review and Applied Fixes

This section records the review pass performed before finalizing this draft.

Finding: The first naive design would store compressed column blocks directly
behind primary ids, which would make updates rewrite blocks and break snapshot
efficiency.
Fix: The proposal uses immutable base parts, update-delta column parts,
delete bitmaps, and maintenance compaction. Changed rows are rewritten into
column-store format immediately, while old compressed blocks remain immutable
until compaction and GC.

Finding: A pure column part directory would make point lookup require part
searches.
Fix: The proposal keeps an explicit primary `id -> row_locator` B-tree root.

Finding: Reusing ClickHouse method bytes would imply byte compatibility the Go
implementation may not provide.
Fix: The proposal uses TreeDB-specific codec ids while importing algorithms and
validation rules.

Finding: Applying `Delta`, `T64`, or float codecs to null maps and offsets
would corrupt structural streams.
Fix: The proposal imports ClickHouse's generic-only structural substream rule.

Finding: A column scan API could accidentally make borrowed slices escape or be
used after callback return.
Fix: The proposal defines both safe materialized APIs and borrowed callback
APIs with explicit lifetime rules.

Finding: Compression-only gates could reward small output even when throughput
collapses.
Fix: Every milestone combines compression gates with throughput and allocation
gates.

Finding: The existing template-v1 layout is already very compact on benchmark
fixtures, so absolute size claims could be misleading.
Fix: Gates are relative to same-run baselines and include high-entropy fallback
requirements.

Finding: Column file checksums might make block checksums look duplicative.
Fix: The proposal keeps per-block CRC32C because projected reads may slice a
larger column file, but leaves a stronger checksum as an open question.

Finding: A direct column-store path could bypass secondary index correctness.
Fix: Direct vector insertion still builds existing secondary root runs and
preserves current unique/nonunique index semantics.

Finding: Existing value-log compression and column-store compression could
double-compress the same bytes.
Fix: `TCS1` column files should store their encoded block payloads directly.
The column file class must not apply an outer compression layer unless an
explicit class-level dictionary experiment proves a wall-time and bytes win.

Finding: Secondary indexes could work for inserts but become stale after
column-store updates.
Fix: Updates now publish old secondary-entry deletes, new secondary-entry
puts, primary locator changes, delta-part descriptors, and tombstones in one
root group.

Finding: Adding ClickHouse-style fast filters only to column parts would leave
row-store, BSON, and template-v1 collections unable to benefit.
Fix: The RFC adds shared filter/search interfaces with typed-vector and
byte-haystack adapters so the same minmax, set, bloom, token, ngram, text, and
searcher algorithms can serve multiple layouts.

Finding: Generalizing string haystack algorithms to other types can create
bad semantics if numeric values are searched as decimal strings by accident.
Fix: Haystack adapters are explicit. String/bytes/document/token predicates
use byte searchers; numeric equality and range use typed filters.

Finding: A fixed-schema column store could drift into flexible schema behavior
if row ingestion silently creates columns for new paths.
Fix: `TCS1` now requires fixed schemas and declares flexible/lazy schemas
out-of-scope for this format.

Finding: Delaying the physical column file/folder could make a later migration
harder and reduce observability.
Fix: PR1 now creates a dedicated column file class under `Dir/maindb/columns/`,
uses stable `column_id` paths, and keeps the manifest flexible enough to pack
small update-delta parts when file count would dominate.

Finding: Update-delta parts may be correct but too slow for update-heavy
workloads.
Fix: Milestone 6 now requires experiments against granule rewrite and hybrid
rewrite, plus update-followed-by-read and churned-scan gates.

Finding: A new column compression system could fork away from TreeDB's existing
snappy/lz4/zstd/zstd-dictionary work.
Fix: The RFC now requires a shared TreeDB compression registry path and treats
the bandit-style selector as an experiment against the current TreeDB chooser.

Finding: Fast filters and part row counts could be approximate but accidentally
used for exact answers.
Fix: Approximate filters remain pruning-only, while `count(*)` uses the exact
snapshot aggregate, visibility-adjusted part/granule metadata, or verifies
rows. Raw immutable part totals are not exact when delete or update-delta
visibility can hide rows.

## 18. Completion Criteria for the Proposal

This RFC is complete when a reviewer can answer:

- what the end-state API looks like;
- how the B-tree remains central;
- where column bytes are stored;
- how compression is represented and selected;
- how fixed schemas, auto compression, and pinned per-column compression work;
- how ClickHouse practices are mapped into TreeDB;
- how secondary indexes work with column-store inserts, scans, updates,
  deletes, and compaction;
- how reusable fast filters and byte-haystack searchers apply to column-store
  and other collection layouts;
- how fast counts are maintained without scanning data;
- how the column-store plan is tested through the JSONBench Bluesky workload and
  compared with local ClickHouse/DuckDB JSONBench result files;
- how WAL/recovery ordering protects column-part pointers;
- how reads, writes, updates, deletes, compaction, and GC work;
- what tests are required;
- what performance and compression gates apply to every milestone.
