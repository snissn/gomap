# TreeDB Column Graph Vector Demo

`treedb_column_graph_demo` is a small product-path demo for the explicit
`column_graph` vector-index strategy. It creates a collection, writes vector,
inverse-norm, and adjacency-list columns as normal column-store assets, closes
and reopens the database, then searches through `Collection.SearchVectorIndex`.

Run it with:

```sh
GOWORK=off go run ./cmd/treedb_column_graph_demo -json
```

The demo intentionally keeps the graph tiny. It proves the lifecycle boundary:

1. Open a TreeDB database with command WAL enabled.
2. Create a collection with column-store columns for:
   - `embedding` as `float32_vector`
   - `embedding_inv_norm` as `float32`
   - `embedding_neighbors` as `adjacency_list`
3. Declare a vector index with `Strategy: column_graph`.
4. Insert documents that include the three graph columns.
5. Checkpoint, close, and reopen.
6. Search through `Collection.SearchVectorIndex`.
7. Print status showing whether the physical column graph was loaded.

Current caveat: normal vector-index `BuildVectorIndex` does not yet derive and
publish inverse-norm or adjacency-list columns from ordinary vector documents.
Until that build/rebuild seam lands, this demo writes those columns explicitly
so the loader can prove the persisted column-asset path without adding a
vector-only sidecar format.
