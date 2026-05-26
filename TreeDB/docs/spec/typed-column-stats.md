# Typed-Column Stats Framework (#1840)

Status: internal pre-alpha format. New binaries may require rebuilding old test
DBs when typed-column image formats change, but readers treat absent stats as a
safe decode fallback.

## Envelope

Typed-column stats use a generic envelope and type-specific payloads.

The envelope binds:

- stats section/envelope version;
- immutable typed-column part id and column name;
- physical typedcolumn type, encoding, compression, row count, and block count;
- row-count contracts: total rows, null rows, default rows, visible rows, and
  value rows are distinct fields;
- advertised logical operations, for example `aggregate.sum` and `stats.sum`;
- advertised selection shapes, currently `full_block` and optionally `all_rows`;
- payload kind, payload length, and payload CRC32/IEEE checksum.

A prepared reader may consume stats only after normal asset integrity policy and
writer layout certification have accepted the typed-column part. Present but
invalid stats are corruption/staleness and fail closed. Missing stats are not
corruption; optimized paths fall back to decoding.

## Int64 payload v1

The initial payload is `int64_count_sum_min_max_v1` for non-null logical `int64`
columns with typedcolumn physical `int64`. Collection adapter columns whose
physical carrier is `int64` but whose logical value type is not `int64` (for
example float raw-bit carriers and the hidden primary-id carrier) disable this
payload at build time and still require semantic/layout gates at read time. The
payload records part-level and block-level:

- row/value counts;
- null/default/visible counts (zero/null-free for the current value payload);
- checked int64 sum plus a `sum_valid` bit;
- min/max copied from the validated block granule metadata.

Prepared int64 count/sum/avg scans use this payload only when a block is
full-covered by the predicate and no visibility/mutation mask is required. This
covers no-filter/all-match and clustered full-covered range blocks. Partial,
random, sparse, hotspot, nullable/default, or mutation-visible blocks use the
existing typedkernel/direct-view/streaming fallback path. The envelope may still
advertise the `all_rows` shape for exact count operations when part-level sum
overflows; sum/avg additionally require `sum_valid`.

Overflow behavior is explicit: if a block sum overflows int64 during stats
construction, that block does not advertise a usable sum and selected scans fall
back to the checked reducer, which returns the existing overflow error. If a
part-level sum overflows, block stats can still be used independently, and final
result accumulation remains checked.

## Future payload rules

The envelope is type-neutral; payload semantics are not.

- Bool payloads should expose true/false/null counts, not broad scalar range.
- String/dictionary payloads may expose dictionary-id stats only under stable
  dictionary identity. Lexical range stats require dictionary-order and collation
  proof.
- Float payloads require native float layout and explicit NaN, signed-zero,
  infinity, precision, and sum/min/max rules. Raw int64 bit patterns must not use
  int64 numeric stats and must not advertise `aggregate.sum`, `aggregate.avg`,
  `aggregate.min`, `aggregate.max`, `stats.min_max`, or `stats.sum`. A future
  float payload must state whether NaNs are propagated, ignored, bucketed, or
  rejected; how `+0`/`-0` affect min/max; how infinities participate in ranges
  and aggregates; and what accumulator/result precision and rounding policy is
  used.
- Vector and adjacency payloads must be vector/graph-specific metadata or index
  sketches and must reject scalar aggregate assumptions.

Future payloads must declare their supported operations and selection shapes in
the envelope and must be admitted through the semantic/layout capability gates.
