# Collection Column-Backed Vector Index Seam

Status: implemented as a product-contract seam, not as a complete persisted
column-backed vector-search product path.

## Current Reality

TreeDB currently has two separate pieces of vector-index infrastructure:

- The public collection vector-index lifecycle uses collection metadata plus
  native TreeDB vector-index roots. This path can declare, build, save, reopen,
  load, and query the native/runtime ANN graph.
- `ColumnVectorGraph` is the lower-level column-shaped search kernel. It
  searches immutable row-major vector, inverse-norm, and CSR adjacency columns
  with caller-owned scratch. It does not fetch full documents in the traversal,
  scoring, or top-k kernel.

The full product path for a persisted column-backed vector index still depends
on physical column-store support that writes, publishes, reopens, and scans the
vector, inverse-norm, and adjacency-list assets through the collection column
manifest/root lifecycle.

## `column_graph` Strategy

Collection vector-index metadata now accepts an explicit strategy:

```json
{
  "name": "embedding",
  "field": "embedding",
  "metric": "cosine",
  "dimensions": 128,
  "strategy": "column_graph"
}
```

The omitted/default strategy remains the existing native/runtime path. Native
metadata continues to omit `strategy` so older normalized metadata remains
stable.

Selecting `column_graph` is intentionally not a request to build a native HNSW
root. TreeDB skips native runtime registration, native vector-root descriptor
creation, and native write-maintenance for explicit `column_graph` indexes. The
strategy reports status through the normal vector-index operational APIs instead
of silently returning native results.

## Status Contract

`VectorIndexStatus` and `VectorIndexLoadStatus` report the selected strategy and
column-graph availability:

- `Strategy`: `native_runtime` or `column_graph`.
- `NativeRuntimeUsed`: true only when the native/runtime graph is the active
  path.
- `ColumnGraphLoaded`: true only after a real column-backed graph is loaded.
- `ColumnGraphUnavailableReason`: precise reason the column graph cannot load.
- `PhysicalColumnAssetsSupported`: true only when physical column assets are
  available through the loader boundary.
- `RebuildNeeded`: true when the index cannot safely serve the selected path.

Current `column_graph` unavailable reasons include:

- `physical_column_asset_support_missing`
- `column_graph_manifest_root_missing`
- `column_graph_manifest_root_mismatch`
- `column_graph_requires_cosine`
- `column_graph_requires_float32`

These reasons are also mirrored in `ExactFallbackReason` so existing callers
that only inspect the older field still fail closed.

## Loader Boundary

`ColumnVectorGraphIndexLoader` is the seam future physical column-store work
must satisfy. A real loader must read vector, inverse-norm, and adjacency-list
column assets referenced by the collection column manifest and construct an
immutable `ColumnVectorGraph`.

The current default loader returns
`physical_column_asset_support_missing`. It does not create a vector-only
sidecar format and does not bypass the column-store manifest/root architecture.

## Remaining Milestones

Before `column_graph` can become the default product path, TreeDB still needs:

1. Physical column asset writing for vector, inverse-norm, and adjacency-list
   columns during vector-index build/rebuild.
2. Durable publication of those assets through the collection column
   manifest/root lifecycle.
3. Reopen/scanner support that loads those published assets into
   `ColumnVectorGraphColumns`.
4. Mutation policy for inserts, updates, and deletes: maintain the graph, mark
   it stale, or rebuild it explicitly.
5. Public query wiring that uses `ColumnVectorGraph` for top-k selection and
   materializes documents only after the graph kernel chooses result IDs.

## What This Proves

This seam proves that a normal collection can declare a column-backed vector
index strategy and get explicit operational status without accidentally using
the native graph path. It does not prove persisted column asset search, disk
layout, compression, or end-to-end column-backed query performance. Those remain
the responsibility of the physical column asset milestones above.
