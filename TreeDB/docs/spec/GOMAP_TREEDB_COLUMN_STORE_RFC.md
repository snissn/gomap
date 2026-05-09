# TreeDB Column Store RFC

Status: canonical placeholder for column-store persistence scope. Detailed
format and execution design remains future work.

Production persistent column-store collection writes are blocked until
`TreeDB/docs/spec/collection-wal-durability-plan.md` M7 sign-off links to green
M1-M6 collection WAL evidence. Before that gate, column-store work is limited
to docs, benchmarks, pure codecs, filters/search packages, and isolated
encode/decode tests that do not publish persistent collection roots. Persistent
column-store APIs, column part descriptor roots, secondary indexes pointing at
column-store rows, column-file side refs in published roots, and crash/reopen
safety claims for column-store writes are blocked.

This file exists so the spec index and docs-lint manifest have a stable owner
for column-store persistence questions. When a full RFC lands, it must keep the
blocker paragraph above unchanged or explicitly update the WAL gate, verification
matrix, and spec README in the same change.

## Persistent Descriptor Identity

Persistent column-store roots remain blocked, but any future descriptor format
must use stable identity and generation guards:

```text
ColumnPartDescriptorV1 {
    PartID                         uuid128 or uint128
    PartGeneration                 uint64
    OwnerCollectionUID             uuid128
    CollectionGeneration           uint64
    SchemaEpoch                    uint64
    ColumnSchemaDigest             bytes32
    CompressionDescriptorDigest    bytes32
    CodecRegistryVersion           uint64
    DictionaryUIDs                 repeated uuid128
    DictionaryGenerations          repeated uint64
    CreatedByCollectionSeq         uint64
    SupersededByCollectionSeq      uint64 optional
    CompactionEpoch                uint64 optional
    RowCount                       uint64
    PrimaryKeyRange                optional
    MinMaxStatsDigest              bytes32 optional
    SideRefDigest                  bytes32
}
```

Delete, filter, locator, count, and visibility roots must reference
`PartID + PartGeneration`, not bare `PartID`. Compaction/recompression must
create new part IDs or increment part generation and publish source supersession
plus target descriptors in one collection WAL maintenance transaction.
