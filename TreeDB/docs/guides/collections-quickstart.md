# TreeDB Collections Typed-Storage Quickstart

This guide shows how to start with TreeDB collection storage modes and how to
validate the current typed-column int64 aggregate path. It is written for the
current pre-alpha codebase, not as a stable public API promise.

> **Pre-alpha caveat:** collection APIs, typed-storage metadata, and on-disk
> formats may change. Use disposable benchmark directories and rebuild them when
> switching branches. Current typed-storage writes require the command-WAL v1
> feature in the DB format config.

All shell commands below are copy/paste runnable from the repository root unless
the section explicitly says **illustrative snippet**.

## Storage modes in one table

| Workload/data shape | Recommended layout | Why |
| --- | --- | --- |
| Flexible document, point reads | Retained document | Simplest, schema-flexible, no typed-storage ownership decisions. |
| Declared scalar fields, point reconstruction | Typed-row asset | Compact typed row path while keeping reconstruction straightforward. |
| Int64 range aggregate/filter | Typed-column part | Min/max pruning and aggregate scan can avoid document and row materialization. |
| Vector search payloads | Typed-column dense section | Contiguous vector bytes, mmap direct-view capable only after certification plus lifetime/range/endian/length/absolute-offset/actual-pointer alignment validation. |
| Mixed metadata + vector | Typed-column vectors plus typed-row or retained-document metadata | Separates candidate scoring from final fetch and keeps metadata placement tied to query shape. |

## Field-ownership best practice

One logical field should have one authoritative owner in a generation:

- `retained_document` / `document_payload` for flexible fields and residual JSON;
- `typed_row_asset` for declared fields that are mostly reconstructed by point
  get or row-shaped reads;
- `typed_column_part` for declared fields that benefit from column scans,
  aggregates, or dense vector sections.

Avoid overlapping authoritative ownership. Compatibility retained-payload modes
may duplicate bytes for reconstruction, but do not treat duplicated retained
payload as a second source of truth for a typed field.

### Schema-aware indexed writes

Storage ownership and input encoding are separate decisions. A JSON-based
`InsertBatch` can publish typed assets while still parsing JSON to obtain their
values. For indexed RAG data, supply the declared strings and FP32 vectors
directly; keep residual JSON free of the declared fields. Do not stringify a
vector merely to pass it through a document adapter, and do not keep a second
authoritative JSON copy of indexed metadata.

The [Minima native contract](../spec/minima-native-execution.md) selects
typed-row strings for `content`, `meta.user_id`, and `meta.fpath`, and a fixed
FP32 typed column for `embedding`. A complete typed batch supplies every declared
field; unsupported types, null/missing values, dimensional mismatches, and
overlapping retained fields must be rejected before admission. Binary transport
alone does not establish this contract: the write planner and WAL replay must
consume those same typed inputs.

Current behavior: for durable-at-acknowledgement writes, open the DB with the explicit
`command_wal_durable` production profile. Merely enabling the command-WAL format
feature on a low-level unprofiled backend is not an equivalent durability
boundary. See [write-path and durability](../spec/write-path-and-durability.md).

Illustrative snippet for an already-created collection with declared column
names `embedding`, `content`, `user`, and `path` (the latter two have paths
`meta.user_id` and `meta.fpath`), non-column retained JSON, and an 8-D vector:

```go
ids := [][]byte{[]byte("chunk-1")}
retained := [][]byte{[]byte(`{"id":"chunk-1","source":"manual"}`)}
columns := []collections.TypedColumnBatch{
    {Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
    {Name: "content", Strings: []string{"TreeDB stores indexed values natively."}},
    {Name: "user", Strings: []string{"reader-1"}},
    {Name: "path", Strings: []string{"manual/intro"}},
}
_, stats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
if err != nil {
    return err
}
_ = stats // Inspect ingestion statistics separately from graph readiness.

columns[1].Strings[0] = "Updated native indexed content."
results, err := col.ReplaceTypedBatch(ids, retained, columns)
if err != nil {
    return err
}
_ = results // Per-ID Matched/Modified; missing IDs are not inserted.
```

Column carriers align by row with `ids`; column `Name` identifies the declared
column, not its JSON path. Supply every declared column on replacement, including
unchanged ones. Inputs are borrowed only for the call and may be reused after
return; do not mutate them concurrently with the call. Ordinary duplicate and
unique-index checks still apply. Typed ingestion is not an assertion that a
collection is empty, nor a shortcut around those checks. Graph build/readiness
is a separate operation; these calls alone do not promise mutable ANN serving.

For document-shaped applications, the JSON/template examples below remain valid.
Choose them for their document contract, not as proof of JSON-free indexing.
See the [typed-storage performance guide](typed-storage-performance.md) for
separate load, mutation, query, and output measurement boundaries.

## Illustrative metadata snippets

These snippets show the shape of the current metadata. The runnable end-to-end
program in the next section is the copy/paste smoke test.

### Document-only collection

```go
meta := &collections.CollectionMeta{
    Name: "docs",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
    },
}
```

Use this when schema flexibility and point reads matter more than typed-column
scan/aggregate paths.

For RAG-style ingestion of long text into a document-only collection, use the
built-in chunking seam (`IngestChunkedDocument`) instead of pre-splitting
callers; see [Document chunking](document-chunking.md).

Embeddings come from the pluggable embedder seam; dimension validation against
the target vector index happens at ingest time and fails closed before any
write. See [Document embedding](document-embedding.md).
For one-call chunk → embed → index ingestion with bounded concurrency, see
[One-call document ingestion](document-ingestion.md).

### Typed-row collection

```go
meta := &collections.CollectionMeta{
    Name: "events_row",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
            Columns: []collections.ColumnStoreColumn{
                {Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
                {Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset, Dictionary: true},
            },
            SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
        },
    },
}
```

Use this for declared fields that are reconstructed by point reads or row-shaped
access. Existing `ColumnStoreConfig` fields with no explicit owner normalize to
`typed_row_asset` for compatibility.

### Typed-column collection

```go
meta := &collections.CollectionMeta{
    Name: "events_column",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
            Columns: []collections.ColumnStoreColumn{
                {Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
            },
            SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
        },
    },
}
```

Use this when a declared field is queried through a typed-column scan/aggregate
or a dense vector section. Non-nullable int64 predicate count/sum/avg is the
current scalar aggregate path.

### Hybrid collection

```go
meta := &collections.CollectionMeta{
    Name: "events",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
            Columns: []collections.ColumnStoreColumn{
                {Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
                {Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset, Dictionary: true},
            },
            SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
        },
    },
}
```

This is the recommended starter for event-style data: keep the range/aggregate
column in `typed_column_part`, keep small metadata in typed-row or retained JSON,
and fetch the final document only after a candidate/aggregate phase.

## Runnable hybrid quickstart

This program creates a hybrid collection, inserts fixture rows, runs a point get,
runs a typed-column int64 range aggregate, checkpoints, reopens, and proves the
stored document is still readable.

```sh
cat >/tmp/treedb_typed_storage_quickstart.go <<'EOF'
package main

import (
	"fmt"
	"log"
	"os"

	collections "github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func main() {
	dir, err := os.MkdirTemp("", "treedb-typed-storage-quickstart-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
		RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV2},
	}); err != nil {
		log.Fatal(err)
	}

	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)

	meta := &collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Columns: []collections.ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
					{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset, Dictionary: true},
				},
				SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
			},
		},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		log.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		log.Fatal(err)
	}

	ids := [][]byte{[]byte("e-1"), []byte("e-2"), []byte("e-3")}
	docs := [][]byte{
		[]byte(`{"time_us":10,"kind":"alpha","message":"first"}`),
		[]byte(`{"time_us":11,"kind":"beta","message":"second"}`),
		[]byte(`{"time_us":20,"kind":"alpha","message":"third"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		log.Fatal(err)
	}

	got, err := col.Get([]byte("e-2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("point_get=%s\n", got)

	agg, err := col.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{
		Column: "time_us",
		Kind:   collections.TypedColumnInt64PredicateRange,
		Low:    10,
		High:   11,
		ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegrityCachedVerify,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("count=%d sum=%d avg=%.1f rows_scanned=%d rows_matched=%d blocks_pruned=%d mapped_bytes=%d decoded_bytes=%d docs=%d rows=%d\n",
		agg.Count, agg.Sum, agg.Avg,
		agg.Diagnostics.RowsScanned, agg.Diagnostics.RowsMatched, agg.Diagnostics.BlocksPruned,
		agg.Diagnostics.MappedBytes, agg.Diagnostics.DecodedHeapCopyBytes,
		agg.Diagnostics.DocumentMaterializations, agg.Diagnostics.RowMaterializations,
	)

	if err := mgr.FlushAll(); err != nil {
		log.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer reopened.Close()
	reopenedCol, err := collections.NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		log.Fatal(err)
	}
	reopenedDoc, err := reopenedCol.Get([]byte("e-1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("reopened_get=%s\n", reopenedDoc)
}
EOF

go run /tmp/treedb_typed_storage_quickstart.go
```

Expected output shape:

```text
point_get={"time_us":11,"kind":"beta","message":"second"}
count=2 sum=21 avg=10.5 rows_scanned=3 rows_matched=2 blocks_pruned=0 mapped_bytes=... decoded_bytes=... docs=0 rows=0
reopened_get={"time_us":10,"kind":"alpha","message":"first"}
```

Interpretation:

- `point_get` fetches the full reconstructed document by document ID.
- `count/sum/avg` comes from the typed-column int64 aggregate request.
- `docs=0 rows=0` means the typed-column aggregate did not materialize public
  documents or typed rows in the timed aggregate path.
- `mapped_bytes` and `decoded_bytes` are useful profiling counters, not
  durability guarantees. On tiny fixtures they mainly prove the counters are
  wired.
- The checkpoint/reopen block is included because durability examples should
  cross a persisted boundary.

## Runnable package benchmark

Use the package benchmark when you want repeatable counters for the current
int64 predicate aggregate path:

```sh
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  ./TreeDB/collections
```

Expected output includes landed #1808 matrix rows for `path_typed_column_part`
and `path_document_full_scan_fallback`, with counters such as:

```text
... rows_4096/dist_clustered_monotonic/path_typed_column_part/shape_selective_range_1pct/predicate_count_sum_avg ... rows_scanned/op ... rows_matched/op ... mapped_bytes/op ... decoded_bytes/op ... document_materializations/op ... row_materializations/op ... B/op ... allocs/op
... rows_4096/dist_clustered_monotonic/path_document_full_scan_fallback/shape_selective_range_1pct/predicate_count_sum_avg ... document_materializations/op ... row_materializations/op ...
```

Use a longer command for less noisy numbers:

```sh
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=100x \
  -count=5 \
  ./TreeDB/collections
```

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `column-store writes require command WAL` | The DB format config did not require command-WAL v2. | For current typed-storage examples, call `backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV2}})` before opening a new DB directory. |
| Aggregate returns unsupported/fail-closed | Field is not a non-nullable int64 `typed_column_part`, metadata is stale/corrupt, or a currently unsupported nullable path was requested. | Check `ColumnStoreColumn.Owner`, `ValueType`, and `Nullable`; do not silently fall back to document reconstruction when a direct typed-column path must fail closed. |
| Reopened DB misses recent collection writes | The example did not flush/checkpoint before close, or durability mode differs. | Use `CollectionManager.FlushAll()` and `DB.Checkpoint()` in examples where durability matters. |
| Benchmark looks slower than expected | Setup/open/checksum/metadata work may be included, row counts may be tiny, or the shape may not prune. | Separate setup from timed query loops; use stable row counts/seeds; inspect `mapped_bytes/op`, `decoded_bytes/op`, `blocks_pruned/op`, and allocation counters. |
| Retained JSON contains fields you expected to be stripped | Retained-payload policy or owner choice does not match the intended model. | Use `ColumnRetainedPayloadNonColumn` for residual payload plus typed fields, and avoid overlapping authoritative owners. |

## Best-practice checklist

- Start with retained documents unless you have declared fields and a measured
  query/reconstruction need.
- Put one logical field under one authoritative owner; avoid overlapping
  retained-document, typed-row, and typed-column ownership.
- Use typed-row for row-shaped declared metadata and typed-column for scan,
  aggregate, or vector payloads.
- Keep vector payloads out of retained JSON for search-heavy workloads when a
  dense typed-column section is the intended search data plane.
- For vector APIs that return documents without embedding echo, use
  `ColumnRetainedPayloadNonColumn` plus
  `ProjectionOrientedVectorDocumentFetchPreset` so final documents exclude the
  vector field; request full documents/embeddings only as an explicit comparison
  or compatibility path.
- Separate candidate search/aggregate from final document fetch.
- Use `cached_verify` or `verify` for correctness-oriented validation; treat
  `skip_checksums` only as an unsafe ceiling benchmark.
- Include `FlushAll`, `Checkpoint`, close, and reopen in examples where
  durability matters.
- Keep benchmark row counts, query shapes, and seeds stable; do not optimize only
  for clustered/selective data.
