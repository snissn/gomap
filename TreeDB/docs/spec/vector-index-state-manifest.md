# Vector-Index State Manifest (#1986)

Status: v2 control-record contract for durable vector-index derived state. This
record is separate from authoritative collection field/part records and from the
legacy `column_graph` graph-record trailers.

TreeDB is pre-alpha. Old graph records without vector-index state are treated as
legacy fallback/rebuild-needed inputs rather than migrated in place.

## Home and ownership

Vector-index state records live in the collection column-manifest root under a
separate control keyspace:

```text
\x06vector-index-state/v1/index/<index_name>
```

The record value uses magic `TVIS` and current version `2` (`1` remains a
read-only compatibility format). The keyspace is a
vector-index control layer: it is not a base field owner, not a synthetic user
column, and not a graph-specific adjacency-source trailer. The active manifest
checksum includes the state record for durability, while the state record's
`base_manifest_checksum` is computed over the authoritative base collection
manifest records with vector-index derived records excluded.

Current legacy `column_graph` records may still exist so old pre-alpha graph
assets can be searched or rebuilt through compatibility paths. New derived
state should be described here and should reference generic typed-column assets.

## Record identity

Each current state record contains:

- index identity: index name, field, metric, vector encoding, dimensions, `M`,
  `efConstruction`, and `efSearch`;
- graph/state shape: row count by vector ordinal;
- base identity: base manifest generation, base manifest checksum, and base
  schema hash;
- expected HNSW adjacency layer count for validating typed adjacency assets even
  when legacy graph-specific adjacency-source trailers are absent;
- zero or more typed-column asset refs.

A reader validates the state identity against the declared vector-index
definition and the current active base manifest before trusting any asset ref.
Generation, checksum, schema, index-definition, or row-count mismatches fail
closed and require rebuild/fallback instead of silently reading stale bytes.

## Typed-column asset references

Each asset ref records:

- role and asset id;
- logical typed-column type;
- physical encoding;
- row count;
- source schema/hash identity;
- immutable typed-column asset ref: kind, namespace, generation, part id, file id,
  offset, length, checksum, and byte count.

Defined v1 roles and type contracts are:

| Role | Logical type | Physical encoding | Owner |
| --- | --- | --- | --- |
| `adjacency` | `uint32_list` | `raw_uint32_offsets_list` | HNSW adjacency consumers (#1987/#1988). |
| `inverse_norm` | `float32` | `raw_float32` | cosine inverse norms (#1992). |
| `normalized_vectors` | `float32_vector` | `raw_float32_vector` | optional derived normalized vectors (#1977). |
| `row_refs` | decided by follow-up | decided by follow-up | row/document refs (#1993). |

The role contract deliberately names generic typed-column primitives. HNSW layer
semantics, neighbor ordinal bounds, graph traversal, deleted-row policy, and row
ref interpretation stay above the typed-column datastore layer.

## Validation and fail-closed behavior

Opening/status validation must reject:

- malformed record magic/version or trailing bytes;
- invalid index name/field/metric/encoding/dimensions/settings;
- base manifest generation, checksum, or schema mismatch;
- state row count mismatch with the graph/control layer;
- duplicate `(role, asset_id)` refs;
- unknown roles, except explicitly versioned follow-ups;
- wrong logical type or physical encoding for a known role;
- typed-column refs whose kind, namespace, generation, row count, byte count, or
  checksum identity does not match the state/base identity;
- missing, out-of-bounds, or corrupt referenced assets.

Old `column_graph` graph records without a `TVIS` state record are legacy
compatibility. They may still be used by current fallback readers, but new
vector-index derived assets should not be added to the graph-record trailer
format.
