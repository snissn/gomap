# Vector-index row reference state (#1993)

TreeDB column-graph search now publishes vector-index state for ordinal-to-base-row
references. This is separate from returned document IDs.

## Healthy path

For each rebuilt `column_graph` index, TVIS may contain four `row_refs` assets:

- `base_row_ref/generation`
- `base_row_ref/part_id`
- `base_row_ref/row_index`
- `base_row_ref/applied_command_lsn`

Each asset has logical type `int64` and physical encoding `raw_int64` in a
`TCS1` typed-column part. The row count matches the vector-index row count, and
asset schema hashes are derived from the base collection's column-store config.

The row reference tuple is a `DocumentRowRef` coordinate into the base physical
row assets. Open/rebuild validation checks state identity, schema/type/encoding,
asset refs, row count, and bounds against the active base manifest. Vector index
status remains cheap: it performs manifest/ref/layer/schema checks, not full
payload validation.

## Search usage

The typed-column vector source first uses `row_refs` state to map HNSW ordinals
to base typed-column rows. If row-ref state is absent, legacy graph row ID scans
remain an explicit compatibility fallback.

Top-K result fetches still copy returned IDs from graph row ID bytes. This
preserves exact opaque binary document IDs without redefining arbitrary bytes as
strings. When documents are materialized, row refs from vector-index state are
used directly when available, avoiding an ID-to-row-ref locator lookup.

## Opaque document IDs

TreeDB does not currently have a first-class typed-column opaque `bytes` scalar
for exact binary document IDs. Until that exists, graph row ID bytes remain the
compatibility source for returned IDs. A follow-up should add a generic opaque
bytes primitive before vector-index state attempts to own exact arbitrary binary
IDs.
