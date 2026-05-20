# Collection Column-Backed Vector Index Seam

Status: implemented as a product-contract seam with real physical column-asset
loading when vector, inverse-norm, and adjacency-list assets already exist.

## Current Reality

TreeDB currently has two vector-index paths:

- The public collection vector-index lifecycle uses collection metadata plus
  native TreeDB vector-index roots. This path can declare, build, save, reopen,
  load, and query the native/runtime ANN graph.
- Explicit `column_graph` indexes load vector, inverse-norm, and adjacency-list
  data from physical column-store assets referenced by the collection column
  manifest/root state, then construct an immutable `ColumnVectorGraph`.

`ColumnVectorGraph` searches immutable row-major vector, inverse-norm, and CSR
adjacency columns with caller-owned scratch. It does not fetch full documents in
the traversal, scoring, or top-k kernel. Public `SearchVectorIndex` materializes
documents only after the graph kernel has selected top-k IDs.

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

No vector-only sidecar persistence format is created. The default
`column_graph` loader only uses the physical column asset scanner and the
collection column manifest/root lifecycle.

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

- `column_graph_strategy_not_selected`
- `physical_column_asset_support_missing`
- `column_graph_manifest_root_missing`
- `column_graph_manifest_root_mismatch`
- `column_graph_requires_cosine`
- `column_graph_requires_float32`
- `column_graph_physical_schema_mismatch`
- `column_graph_requires_insert_only_manifest`
- `column_graph_empty`
- `column_graph_physical_graph_invalid`

These reasons are also mirrored in `ExactFallbackReason` so existing callers
that only inspect the older field still fail closed.

## Loader Boundary

`ColumnVectorGraphIndexLoader` is the seam the physical column-store path
satisfies. The default loader reads vector, inverse-norm, and adjacency-list
column assets referenced by the collection column manifest and constructs an
immutable `ColumnVectorGraph`.

The loader currently requires an insert-only physical manifest. If the active
manifest includes mutation parts, or if the expected vector/invNorm/adjacency
columns are missing or incompatible, status reports a precise unavailable or
rebuild-needed reason instead of silently serving misleading results.

## Remaining Milestones

Before `column_graph` can become the default vector-index product path, TreeDB
still needs:

1. Vector-index build/rebuild plumbing that derives inverse norms and adjacency
   graph columns and publishes them as normal physical column assets.
2. Mutation policy for inserts, updates, and deletes: maintain the graph, mark
   it stale, or rebuild it explicitly.
3. Dynamic overlay or rebuild integration for mutation-bearing manifests.
4. Optional caching/open-handle lifecycle so repeated public
   `SearchVectorIndex` calls do not have to rescan physical assets each time.

## What This Proves

This seam proves that a normal collection can declare a column-backed vector
index strategy, reopen physical column assets through the durable
manifest/root lifecycle, load a `ColumnVectorGraph`, and serve public
`SearchVectorIndex` calls without accidentally using the native graph path.

It does not yet prove that normal vector-index build/rebuild creates all graph
assets from ordinary vector documents, and it does not claim mutation-bearing
manifests are maintained in place. Those remain explicit follow-on milestones.
