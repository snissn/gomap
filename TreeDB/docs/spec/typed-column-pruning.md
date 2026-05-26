# Typed-Column Pruning/Index Metadata (#1841)

Status: pre-alpha internal contract for immutable `typed_column_part` pruning
metadata. This is a generic envelope with type-specific payloads. The first
payload is a non-null `int64` value-row index used by prepared int64 predicate
aggregate planning.

## Contract

A `typed_column_part` may contain one `pruning_metadata` TCS1 section. The
section is optional: missing metadata is an explicit fallback to existing
min/max pruning and value decoding. Present metadata is validated before use and
is bound to the same immutable part descriptor and writer-certified layout
contract as column data.

Each column payload is wrapped in a generic envelope containing:

- envelope version;
- part id;
- column name, physical type, encoding, compression;
- part row count and block count;
- null/default counts;
- payload kind;
- advertised pruning operations;
- payload length and checksum.

Readers must fail closed on checksum, identity, row-count, block identity,
null/default, ordering, or min/max mismatches. Unsupported or missing payloads
fall back explicitly. Hot row loops consume concrete block-local `RowSelection`
plans and must not call generic capability interfaces per row.

## Initial payload: `int64_value_rows_v1`

`int64_value_rows_v1` is emitted for non-null `int64` columns with uncompressed
int64 encodings (`raw_int64`, `delta_varint`, or `double_delta_varint`). Nullable
or defaultable carriers are deliberately not indexed by this payload.

The payload stores:

- a block map (`block index`, `first row`, `row count`, optional min/max);
- one `(value, row)` entry for every part row, sorted by `(value, row)`.

The payload advertises:

- `pruning.equality`;
- `pruning.ordered_range`.

Prepared planning binary-searches entries for equality/range predicates and
returns immutable block-local `RowSelection` candidates. These selections compose
with #1844 visibility/delete/null/default masks. For current int64 aggregate
execution they narrow or eliminate value decoding but do not bypass column-data
read-integrity validation for matching blocks.

## Capability gates

Use requires all of the following:

1. semantic capability support in `TreeDB/internal/columnsemantics` for the
   logical operation (`pruning.equality` or `pruning.ordered_range`);
2. layout capability support in `TreeDB/internal/columnlayout` for value-row
   pruning;
3. writer-certified `PruningCertified` layout contract flag;
4. envelope/payload validation against the descriptor and decoded column block
   metadata.

Bool/string/float/vector/adjacency payloads are intentionally deferred. They
must define type-native value semantics rather than reusing int64 carrier
ordering or aggregates. In particular, current scalar `float32`/`double` raw
`int64` bit carriers never advertise `pruning.equality` or
`pruning.ordered_range`: equality and range pruning need a native float-domain
contract for NaN payloads, signed zero, infinities, and precision/rounding before
any payload can be trusted.

## Diagnostics and fallback

Prepared int64 sessions decode pruning metadata during prepare when requested.
Diagnostics count decoded metadata bytes and preserve existing
`PruningCertified` certification counters. Missing metadata records a fallback
reason on prepared column state; corrupt/stale metadata returns a validation
error rather than silently accepting unsafe candidates.

## On-disk compatibility

TreeDB is pre-alpha. Adding this section changes newly written TCS1 images; old
images without pruning metadata remain readable and use fallback planning.
