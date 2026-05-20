# Column Graph Vector Search

Status: pre-alpha. The `column_graph` strategy is explicit and feature-gated so
TreeDB can prove persisted column graph assets without replacing the native
vector index by default. This is not column-store-native search yet.

## What It Is

TreeDB has two vector-index paths today:

- Native/runtime vector indexes: the established collection lifecycle for
  ordinary users. `BuildVectorIndex` builds an ANN graph from document vectors,
  persists native vector-index roots, and reloads those roots on reopen.
- Column graph vector indexes: an explicit `column_graph` strategy that loads
  vector, inverse-norm, and adjacency-list columns from physical column-store
  assets, decodes them into an immutable in-memory `ColumnVectorGraph`, runs
  graph search over that in-memory graph, then materializes documents after
  top-k selection.

The column graph path does not create a vector-only sidecar format. It uses the
normal collection column manifest/root lifecycle and the physical column asset
scanner.

## What It Is Not

This path does not search directly over TreeDB column-store granules, resident
column readers, marks, or decoded-block caches. The current flow is:

```text
physical column assets on disk
-> manifest/root lookup and physical scan
-> decode into Go slices
-> in-memory ColumnVectorGraph.SearchCosine
```

The target future flow for column-store-native vector search is different:

```text
column-store-native graph reader/cache
-> fetch vector rows and adjacency through generic column APIs
-> search without materializing a full decoded graph copy
```

Until that exists, treat the search-kernel benchmarks below as an in-memory
ceiling after column-asset decode, not as proof of native column-store search.

## Quickstart

The current product seam expects the graph columns to already exist. Normal
vector-index build/rebuild does not yet derive inverse norms or adjacency lists
from ordinary vector documents.

```go
meta := collections.CollectionMeta{
	Name: "docs",
	Options: collections.CollectionOptions{
		ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{
				{
					Name:       "embedding",
					Path:       "embedding",
					ValueType:  collections.ColumnStoreValueFloat32Vector,
					VectorDims: 3,
				},
				{
					Name:      "embedding_inv_norm",
					Path:      "embedding_inv_norm",
					ValueType: collections.ColumnStoreValueFloat32,
				},
				{
					Name:      "embedding_neighbors",
					Path:      "embedding_neighbors",
					ValueType: collections.ColumnStoreValueAdjacencyList,
				},
			},
			RetainedPayload: collections.ColumnRetainedPayloadFull,
			AssetManager: &collections.ColumnAssetManagerConfig{
				Kind:              collections.ColumnAssetManagerValueLogShaped,
				IsolatedNamespace: true,
				Namespace:         "vector_graph_columns",
			},
		},
	},
	VectorIndexes: []collections.VectorIndexDefinition{{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     collections.VectorMetricCosine,
		Dimensions: 3,
		M:          16,
		EfSearch:   128,
		Strategy:   collections.VectorIndexStrategyColumnGraph,
	}},
}
```

This quickstart uses a tiny 3-dimensional graph so the example document is
literal rather than abbreviated. Production and benchmark configurations should
set `VectorDims` and `Dimensions` to the actual embedding width.

Documents must include the source vector plus graph side columns until the
column-graph build/rebuild seam lands:

```json
{
  "title": "example",
  "embedding": [0.1, 0.2, 0.3],
  "embedding_inv_norm": 2.6726,
  "embedding_neighbors": [17, 44, 91]
}
```

After insert and checkpoint, reopen the DB and query the declared index:

```go
results, trace, err := col.SearchVectorIndex(
	"embedding",
	query,
	collections.VectorIndexSearchOptions{
		TopK:                 10,
		EfSearch:             128,
		DisableExactFallback: true,
	},
)
```

Check `trace.Strategy`, `trace.ExactFallbackReason`, and
`Collection.VectorIndexStatus("embedding")` when integrating. A loaded physical
column graph reports `ColumnGraphLoaded=true`,
`PhysicalColumnAssetsSupported=true`, and `RebuildNeeded=false`.

## Demo

Run the small product-path demo:

```sh
GOWORK=off go run ./cmd/treedb_column_graph_demo -json
```

The demo creates a database, inserts a tiny graph, checkpoints, reopens, and
searches through the public collection API. It intentionally writes the
inverse-norm and adjacency columns in the documents so it can prove the
persisted column-asset loader without adding fake vector persistence. Search is
still performed over the decoded in-memory `ColumnVectorGraph`, not directly
over column-store granules. By default it requires the persisted `column_graph`
path; pass `-allow-fallback` when you want the demo to print exact-fallback
diagnostics instead of failing closed.

## Real Public Dataset

For public large-vector evidence, use the Yandex Research Deep1B/DEEP dataset.
Yandex describes Deep1B as 96-dimensional L2-normalized image embeddings and
publishes 10M and 1B base-vector downloads plus query vectors and ground truth:
https://research.yandex.com/blog/benchmarks-for-billion-scale-similarity-search

TreeDB does not store that dataset in the repository. The opt-in benchmark
script downloads it into a cache directory when missing:

```sh
COLUMN_VECTOR_DEEP1B_DIR=$HOME/.cache/gomap/deep1b \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
RUN_10M=false \
BENCHTIME=500ms \
COUNT=1 \
scripts/bench_column_vector_deep1b.sh
```

Set `RUN_10M=true` for the larger 10M shape. The Deep1B benchmarks currently
live under `experiments/colgranule` and measure persisted column-vector graph
evidence. They are deliberately opt-in because downloading and scanning public
datasets is not CI-safe.

## Main-Path Benchmark Script

For the collection product path, run:

```sh
scripts/bench_column_graph_main_path.sh
```

The script records output under `/tmp/gomap_column_graph_main_path_*` and runs:

```sh
TREEDB_VECTOR_BENCH_DOCS=10000 \
TREEDB_VECTOR_BENCH_DIMS=64 \
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkCollectionVectorIndexColumnGraphMainPath' \
  -benchmem \
  -benchtime=500ms \
  -count=3
```

This is a synthetic benchmark. It exercises real collection metadata, physical
column assets, manifest/root reopen, decode into an in-memory
`ColumnVectorGraph`, in-memory graph search, and public document
materialization variants. It does not measure direct search over native
column-store reader APIs.

## Current Evidence

Local run on Apple M3, darwin/arm64:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkCollectionVectorIndexColumnGraphMainPath' \
  -benchmem \
  -benchtime=500ms \
  -count=3
```

Shape: 10,000 documents, 64 dimensions, degree 16, `TopK=10`,
`EfSearch=128`, physical column graph assets 4,246,700 bytes.

Representative results:

| Benchmark | Result |
| --- | ---: |
| Decode persisted column assets into in-memory graph | 15.7-16.4 ms/op |
| In-memory graph search after column decode | 10.47-10.57 us/search, 0 B/op, 0 allocs/op |
| Parallel in-memory graph search after column decode | 2.10-2.36 us/op, 0 B/op, 0 allocs/op |
| In-memory graph search plus TreeDB document fetch | 0.47 ms/search, 0 allocs/op after warmed buffer |
| Public `SearchVectorIndex`, currently reload/decode per call | 5.1-5.4 ms/search, about 21.7 MB/op, 30,463 allocs/op |
| Cold DB/open plus column decode plus in-memory search plus document fetch | usually 17-18 ms/op after one slower first sample |

Interpretation: the reopened `ColumnVectorGraph` kernel is only the current
in-memory ceiling after column-asset decode. It is zero-allocation with warmed
scratch, but it is not column-store-native search. The public API path is
slower today because it reloads/scans/decodes physical assets for each call
before materializing documents. The correct follow-on is a generic
column-store-native graph reader/cache, not a vector-specific storage shortcut.

## Best Practices

- Use `Strategy: column_graph` explicitly. Do not rely on it as the default
  vector-index product path yet.
- Keep canonical documents in the row store. Treat column vectors, inverse
  norms, and adjacency lists as derived assets.
- Keep graph traversal document-fetch-free. Materialize documents only after
  top-k IDs are selected.
- Treat loaded `ColumnVectorGraph` instances as immutable shared state. Use one
  `ColumnVectorGraphSearchScratch` per goroutine for lower-level kernel calls.
- Do not treat `ColumnVectorGraph.SearchCosine` as native column-store search.
  It searches decoded in-memory slices loaded from column assets.
- Expect mutation-bearing physical manifests to report rebuild-needed until the
  dynamic overlay or rebuild-maintenance seam lands.
- For final storage numbers, compact through TreeDB's normal storage
  maintenance path rather than hand-deleting column assets.

## Current Caveats

- `BuildVectorIndex` still builds the native vector-index path. It does not yet
  synthesize column graph assets.
- The column graph loader currently requires insert-only physical manifests.
  Inserts, updates, or deletes after a graph is published mark the graph
  unavailable/rebuild-needed rather than maintaining it in place.
- Repeated public searches currently reload the physical graph each call. Reuse
  a loaded `ColumnVectorGraph` directly in lower-level benchmarks when measuring
  the in-memory search-kernel ceiling after column-asset decode.
- True column-store-native search still needs a generic column-store graph
  reader/cache that can traverse adjacency and score vector rows without
  materializing a full decoded `ColumnVectorGraph`.
- Deep1B benchmark scripts are opt-in and should not be added to routine CI.
