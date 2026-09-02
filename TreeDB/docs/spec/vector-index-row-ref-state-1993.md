# Vector-index row reference state (#1993)

TreeDB column-graph search publishes vector-index state for ordinal-to-base-row
references. This remains separate from returned document IDs, which are now
owned by vector-index `document_ids` bytes state.

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

Top-K document materialization uses row refs from vector-index state directly
when available, avoiding an ID-to-row-ref locator lookup. Returned IDs are
fetched from `document_ids` typed-column bytes state on the healthy path; legacy
graph row ID bytes are compatibility fallback only.

## Opaque document IDs

Document IDs are opaque bytes, not strings. The document-ID state consumer uses
the generic `bytes` / `raw_bytes_offsets` primitive so non-UTF-8 bytes and
embedded NUL bytes stay exact at the typed-column layer. Graph row ID bytes
remain compatibility or quarantine records until #2014 can retire or shrink that
old payload dependency.
