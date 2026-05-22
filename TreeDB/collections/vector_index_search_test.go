package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSearchVectorIndexColumnGraphNativeReaderReopenV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	query := []float32{0, 0.2, 1}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	if got.Stats.Candidates == 0 || got.Stats.CandidateFetches == 0 || got.Stats.ResultFetches < uint64(len(got.Results)) {
		t.Fatalf("stats=%+v want public search to expose non-zero native graph traversal/result accounting", got.Stats)
	}
	if got.Stats.DocumentsFetched != 0 {
		t.Fatalf("DocumentsFetched=%d want no document fetch without IncludeDocuments", got.Stats.DocumentsFetched)
	}
	if got.Results[0].Document != nil {
		t.Fatalf("document materialized without IncludeDocuments: %q", got.Results[0].Document)
	}
}

func TestSearchVectorIndexColumnGraphResultIDsAreCapacityIsolatedV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{1, 0, 0},
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	secondID := append([]byte(nil), got.Results[1].ID...)
	_ = append(got.Results[0].ID, '!')
	if !bytes.Equal(got.Results[1].ID, secondID) {
		t.Fatalf("second result ID changed after appending to first result ID: got %q want %q", got.Results[1].ID, secondID)
	}
}

func TestSearchVectorIndexByteAccountingRejectsOverflowV4(t *testing.T) {
	total, err := addVectorIndexSearchByteTotal(3, 4, 10, "document")
	if err != nil || total != 7 {
		t.Fatalf("addVectorIndexSearchByteTotal=%d, %v want 7, nil", total, err)
	}
	if _, err := addVectorIndexSearchByteTotal(math.MaxInt-1, 1, math.MaxInt, "document"); err != nil {
		t.Fatalf("max int edge add failed: %v", err)
	}
	if _, err := addVectorIndexSearchByteTotal(8, 3, 10, "document"); err == nil {
		t.Fatalf("addVectorIndexSearchByteTotal overflow err=nil want failure")
	}
	if total, err := multiplyVectorIndexSearchByteTotal(3, 4, 12, "document"); err != nil || total != 12 {
		t.Fatalf("multiplyVectorIndexSearchByteTotal=%d, %v want 12, nil", total, err)
	}
	if _, err := multiplyVectorIndexSearchByteTotal(math.MaxInt, 2, math.MaxInt, "document"); err == nil {
		t.Fatalf("multiplyVectorIndexSearchByteTotal overflow err=nil want failure")
	}
	got, err := vectorIndexSearchResultIDBytesLimit([]columnVectorGraphNativeSearchResult{
		{ID: []byte("abc")},
		{ID: []byte("de")},
	}, 5)
	if err != nil || got != 5 {
		t.Fatalf("vectorIndexSearchResultIDBytesLimit=%d, %v want 5, nil", got, err)
	}
	_, err = vectorIndexSearchResultIDBytesLimit([]columnVectorGraphNativeSearchResult{
		{ID: []byte("abc")},
		{ID: []byte("def")},
	}, 5)
	if err == nil {
		t.Fatalf("vectorIndexSearchResultIDBytesLimit overflow err=nil want failure")
	}
}

func TestSearchVectorIndexColumnGraphMaterializesDocumentsAfterTopKV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	got, err := col.SearchVectorIndex(opts)
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-c")
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) {
		t.Fatalf("DocumentsFetched=%d want %d", got.Stats.DocumentsFetched, len(got.Results))
	}
	documentBefore := append([]byte(nil), got.Results[0].Document...)
	if _, err := col.SearchVectorIndex(opts); err != nil {
		t.Fatalf("second SearchVectorIndex: %v", err)
	}
	if !bytes.Equal(got.Results[0].Document, documentBefore) {
		t.Fatalf("top result document changed after a later search; want response-owned bytes")
	}
}

func TestSearchVectorIndexFlushesBufferedWritesBeforeSnapshotV4(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:                          DocumentFormatJSON,
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			DisableBufferedIndexedAsyncFlush:        true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 0,
		},
		Indexes: []IndexDefinition{{
			Name:      "kind",
			Field:     "kind",
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("doc-a")},
		[][]byte{[]byte(`{"kind":"vector","embedding":[1,0,0]}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got == 0 {
		t.Fatalf("PendingDocuments=%d want buffered write before search", got)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: "embedding_graph",
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("SearchVectorIndex err=%v want search unavailable", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonPhysicalColumnAssetSupportMissing {
		t.Fatalf("status=%+v want column_graph unavailable response", got.Status)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got != 0 {
		t.Fatalf("PendingDocuments after SearchVectorIndex=%d want flushed before snapshot", got)
	}
}

func TestOpenVectorIndexSearcherFetchesDocumentsFromBoundSnapshotV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a after opening searcher: %v", err)
	}
	if live, err := col.Get([]byte("doc-a")); err != nil || live != nil {
		t.Fatalf("live Get doc-a after delete=%q err=%v want missing", live, err)
	}

	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("Search after delete on bound searcher: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("top result id=%q want doc-a from bound graph snapshot", got.Results[0].ID)
	}
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-a")
	if got.Stats.DocumentsFetched != 1 {
		t.Fatalf("DocumentsFetched=%d want 1", got.Stats.DocumentsFetched)
	}
}

func TestOpenVectorIndexSearcherReusesNativeReaderV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{0, 0, 1}, TopK: 2, EfSearch: len(rows)}
	first, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("first Search: %v", err)
	}
	second, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("second Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, first, def.Name, 2)
	assertColumnGraphSearchResponseLoadedV4(t, second, def.Name, 2)
	if len(first.Results) != len(second.Results) {
		t.Fatalf("second results=%d want %d", len(second.Results), len(first.Results))
	}
	for i := range first.Results {
		if !bytes.Equal(first.Results[i].ID, second.Results[i].ID) || first.Results[i].Ordinal != second.Results[i].Ordinal || first.Results[i].Score != second.Results[i].Score {
			t.Fatalf("second result[%d]=%+v want %+v", i, second.Results[i], first.Results[i])
		}
	}
	if second.Stats.OpenGranulesRead != first.Stats.OpenGranulesRead || second.Stats.OpenPhysicalBytesRead != first.Stats.OpenPhysicalBytesRead {
		t.Fatalf("open stats changed first=(%d,%d) second=(%d,%d); want stable bound-reader setup telemetry", first.Stats.OpenGranulesRead, first.Stats.OpenPhysicalBytesRead, second.Stats.OpenGranulesRead, second.Stats.OpenPhysicalBytesRead)
	}
	// Physical read/cache counters may be zero when the segment cache is warm;
	// logical fetch/search counters prove the bound reader is reused without
	// depending on cold-cache telemetry.
	if first.Stats.RowFetches == 0 || second.Stats.RowFetches == 0 {
		t.Fatalf("row fetch stats first=%d second=%d want non-zero per-search deltas", first.Stats.RowFetches, second.Stats.RowFetches)
	}
	if first.Stats.Candidates == 0 || second.Stats.Candidates == 0 {
		t.Fatalf("candidate stats first=%d second=%d want non-zero searches", first.Stats.Candidates, second.Stats.Candidates)
	}
}

func assertVectorIndexSearchDocumentDIDV4(tb testing.TB, document []byte, want string) {
	tb.Helper()
	var got struct {
		DID string `json:"did"`
	}
	if err := json.Unmarshal(document, &got); err != nil {
		tb.Fatalf("document=%q is not valid JSON: %v", document, err)
	}
	if got.DID != want {
		tb.Fatalf("document did=%q want %q in %q", got.DID, want, document)
	}
}

func TestSearchVectorIndexColumnGraphUnavailableStatusV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want rebuild-needed failure")
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded || got.Status.Loaded {
		t.Fatalf("status=%+v want rebuild-needed fail-closed status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unavailable index", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexColumnGraphReaderOpenFailureDowngradesLoadedStatusV4(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithManifestRowsV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	}, 3)
	defer func() { _ = d.Close() }()

	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded {
		t.Fatalf("test setup status=%+v want loaded before reader row-count validation", status)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex err=%v want unavailable wrapping manifest mismatch", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded || got.Status.Loaded {
		t.Fatalf("status=%+v want fail-closed rebuild-needed status", got.Status)
	}
	if got.Status.Reason != VectorIndexReasonColumnGraphAssetMismatch {
		t.Fatalf("status reason=%q want %q", got.Status.Reason, VectorIndexReasonColumnGraphAssetMismatch)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on reader open failure", got.Path, len(got.Results))
	}
}

func TestColumnGraphVectorIndexStatusUsesCallerSnapshotV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()

	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	current, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus current: %v", err)
	}
	if current.State != VectorIndexStateColumnGraphLoaded || !current.Loaded {
		t.Fatalf("current status=%+v want loaded after rebuild", current)
	}

	old, err := col.columnGraphVectorIndexStatusAtSnapshot(def.Name, oldSnap)
	if err != nil {
		t.Fatalf("columnGraphVectorIndexStatusAtSnapshot: %v", err)
	}
	if old.State != VectorIndexStateColumnGraphRebuildNeeded || !old.RebuildNeeded || old.Loaded {
		t.Fatalf("old snapshot status=%+v want rebuild-needed from caller snapshot", old)
	}
}

func TestColumnGraphVectorIndexStatusRejectsNilInputsV4(t *testing.T) {
	var nilCollection *Collection
	if _, err := nilCollection.columnGraphVectorIndexStatus("embedding_graph"); !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection status err=%v want errCollectionNil", err)
	}
	if _, err := nilCollection.columnGraphVectorIndexStatusAtSnapshot("embedding_graph", nil); !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection snapshot status err=%v want errCollectionNil", err)
	}
	emptyCollection := &Collection{}
	if _, err := emptyCollection.columnGraphVectorIndexStatus("embedding_graph"); !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("nil db status err=%v want errCollectionDBNil", err)
	}

	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.columnGraphVectorIndexStatusAtSnapshot(def.Name, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil snapshot status err=%v want ErrClosed", err)
	}
}

func TestSearchVectorIndexColumnGraphStaleAfterMutationV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-c", vector: []float32{0, 0, 1}},
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{0, 0, 1},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want stale/rebuild-needed failure")
	}
	if !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex stale err=%v want manifest mismatch wrapping", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded {
		t.Fatalf("status=%+v want stale graph to require rebuild", got.Status)
	}
	if got.Status.Reason != VectorIndexReasonColumnGraphUnsupportedVisibility {
		t.Fatalf("status reason=%q want %q", got.Status.Reason, VectorIndexReasonColumnGraphUnsupportedVisibility)
	}
}

func TestSearchVectorIndexColumnGraphMutationMatrixFailsClosedV5(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(testing.TB, *Collection)
		query  []float32
	}{
		{
			name: "insert_after_build",
			mutate: func(tb testing.TB, col *Collection) {
				insertColumnGraphRebuildRowsV2A(tb, col, []columnGraphRebuildInputRowV2A{
					{id: "doc-c", vector: []float32{0, 0, 1}},
				})
			},
			query: []float32{0, 0, 1},
		},
		{
			name: "vector_update",
			mutate: func(tb testing.TB, col *Collection) {
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
			},
			query: []float32{0, 0, 1},
		},
		{
			name: "non_vector_payload_update",
			mutate: func(tb testing.TB, col *Collection) {
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-a", []float32{1, 0, 0}, "payload-updated")
			},
			query: []float32{1, 0, 0},
		},
		{
			name: "delete",
			mutate: func(tb testing.TB, col *Collection) {
				deleted, err := col.DeleteDocument([]byte("doc-a"))
				if err != nil {
					tb.Fatalf("DeleteDocument: %v", err)
				}
				if !deleted {
					tb.Fatalf("DeleteDocument deleted=false want true")
				}
			},
			query: []float32{1, 0, 0},
		},
		{
			name: "mixed_sequential_batch",
			mutate: func(tb testing.TB, col *Collection) {
				insertColumnGraphRebuildRowsV2A(tb, col, []columnGraphRebuildInputRowV2A{
					{id: "doc-c", vector: []float32{0, 0, 1}},
				})
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-b", []float32{1, 0, 0}, "vector-updated")
				deleted, err := col.DeleteDocument([]byte("doc-a"))
				if err != nil {
					tb.Fatalf("DeleteDocument: %v", err)
				}
				if !deleted {
					tb.Fatalf("DeleteDocument deleted=false want true")
				}
			},
			query: []float32{1, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := []columnGraphRebuildInputRowV2A{
				{id: "doc-a", vector: []float32{1, 0, 0}},
				{id: "doc-b", vector: []float32{0, 1, 0}},
			}
			_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
			defer func() { _ = d.Close() }()
			status, err := col.RebuildVectorIndex(def.Name)
			if err != nil {
				t.Fatalf("RebuildVectorIndex: %v", err)
			}
			assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

			tt.mutate(t, col)

			assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)
			got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
				IndexName:        def.Name,
				Query:            tt.query,
				TopK:             2,
				EfSearch:         len(rows) + 1,
				IncludeDocuments: true,
				MaxDecodedBlocks: 1,
			})
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
				t.Fatalf("SearchVectorIndex err=%v want unavailable stale manifest mismatch", err)
			}
			if got.Path != "" || len(got.Results) != 0 || got.Stats.DocumentsFetched != 0 {
				t.Fatalf("response path=%q results=%d docs=%d want no search results before rebuild", got.Path, len(got.Results), got.Stats.DocumentsFetched)
			}
			assertColumnGraphUnsupportedVisibilitySearchStatusV5(t, got.Status, def.Name)
		})
	}
}

func TestSearchVectorIndexColumnGraphMutationStaleStatusSurvivesReopenV5(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	updateColumnGraphRebuildJSONDocumentV5(t, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
	assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertColumnGraphUnsupportedVisibilityStatusV5(t, reopenedCol, def.Name)
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             1,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex reopen err=%v want unavailable stale manifest mismatch", err)
	}
	assertColumnGraphUnsupportedVisibilitySearchStatusV5(t, got.Status, def.Name)
}

func TestSearchVectorIndexColumnGraphSnapshotBoundSearcherSurvivesLaterMutationV5(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	updateColumnGraphRebuildJSONDocumentV5(t, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
	assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)

	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("snapshot-bound Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if string(got.Results[0].ID) != "doc-a" ||
		!bytes.Contains(got.Results[0].Document, []byte(`"did":"doc-a"`)) ||
		bytes.Contains(got.Results[0].Document, []byte(`"note":"vector-updated"`)) {
		t.Fatalf("snapshot result id=%q doc=%s want old consistent generation", got.Results[0].ID, got.Results[0].Document)
	}
}

func TestSearchVectorIndexNativeRuntimeDoesNotFallbackToColumnGraphV4(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_native",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyNativeRuntime,
		}},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: "embedding_native",
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want explicit native-runtime unsupported status")
	}
	if got.Status.State != VectorIndexStateNativeRuntime || got.Status.Reason != VectorIndexReasonNativeRuntime {
		t.Fatalf("status=%+v want native runtime status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no column graph fallback", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexColumnGraphUsesSnapshotMetadataV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutated := false
	for i := range col.meta.VectorIndexes {
		if col.meta.VectorIndexes[i].Name == def.Name {
			col.meta.VectorIndexes[i].Metric = VectorMetric(255)
			col.meta.VectorIndexes[i].Strategy = VectorIndexStrategyNativeRuntime
			mutated = true
		}
	}
	if !mutated {
		t.Fatalf("test setup missing vector index %q", def.Name)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex after handle metadata drift: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if !bytes.Equal(got.Results[0].ID, []byte("doc-a")) {
		t.Fatalf("top result id=%q want doc-a from snapshot catalog metadata", got.Results[0].ID)
	}
}

func TestSearchVectorIndexColumnGraphRejectsUnsupportedMetricV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotVectorIndexForTestV4(t, d, col, def.Name, func(def *VectorIndexDefinition) {
		def.Metric = VectorMetric(255)
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "supports only \"cosine\"") {
		t.Fatalf("SearchVectorIndex err=%v want unsupported metric search-unavailable error", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonColumnGraphUnsupportedMetric {
		t.Fatalf("status=%+v want unsupported metric unavailable status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unsupported metric", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexRejectsUnsupportedSnapshotStrategyV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotVectorIndexForTestV4(t, d, col, def.Name, func(def *VectorIndexDefinition) {
		def.Strategy = VectorIndexStrategy("decoded_graph")
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "unsupported strategy") {
		t.Fatalf("SearchVectorIndex err=%v want unsupported strategy search-unavailable error", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonUnsupportedStrategy {
		t.Fatalf("status=%+v want unsupported strategy unavailable status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unsupported strategy", got.Path, len(got.Results))
	}
}

func mutateCurrentSnapshotVectorIndexForTestV4(tb testing.TB, d *backenddb.DB, col *Collection, name string, mutate func(*VectorIndexDefinition)) {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		tb.Fatal("catalogForSnapshot returned nil")
	}
	for i := range catalog.meta.VectorIndexes {
		if catalog.meta.VectorIndexes[i].Name == name {
			mutate(&catalog.meta.VectorIndexes[i])
			return
		}
	}
	tb.Fatalf("test setup missing vector index %q in snapshot catalog", name)
}

func assertColumnGraphSearchResponseLoadedV4(tb testing.TB, got VectorIndexSearchResponse, name string, wantResults int) {
	tb.Helper()
	if got.IndexName != name || got.Strategy != VectorIndexStrategyColumnGraph {
		tb.Fatalf("response index=%q strategy=%q want %q/%q", got.IndexName, got.Strategy, name, VectorIndexStrategyColumnGraph)
	}
	if got.Path != VectorIndexSearchPathColumnGraphNativeReader {
		tb.Fatalf("path=%q want %q", got.Path, VectorIndexSearchPathColumnGraphNativeReader)
	}
	if got.Status.State != VectorIndexStateColumnGraphLoaded || !got.Status.Loaded || got.Status.RebuildNeeded {
		tb.Fatalf("status=%+v want loaded column graph", got.Status)
	}
	if len(got.Results) != wantResults {
		tb.Fatalf("results=%d want %d", len(got.Results), wantResults)
	}
}

func assertVectorIndexSearchResultsV4(tb testing.TB, got []VectorIndexSearchResult, want []columnVectorGraphNativeSearchResult, wantDocs bool) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("results=%d want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].ID, want[i].ID) || got[i].Ordinal != want[i].Ordinal || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want id=%q ordinal=%d score=%v", i, got[i], want[i].ID, want[i].Ordinal, want[i].Score)
		}
		if wantDocs && len(got[i].Document) == 0 {
			tb.Fatalf("result[%d] missing materialized document", i)
		}
	}
}

func assertColumnGraphUnsupportedVisibilityStatusV5(tb testing.TB, col *Collection, name string) {
	tb.Helper()
	status, err := col.VectorIndexStatus(name)
	if err != nil {
		tb.Fatalf("VectorIndexStatus: %v", err)
	}
	assertColumnGraphUnsupportedVisibilitySearchStatusV5(tb, status, name)
}

func assertColumnGraphUnsupportedVisibilitySearchStatusV5(tb testing.TB, status VectorIndexStatus, name string) {
	tb.Helper()
	if status.Name != name || status.Strategy != VectorIndexStrategyColumnGraph {
		tb.Fatalf("status=%+v want column_graph index %q", status, name)
	}
	if status.State != VectorIndexStateColumnGraphRebuildNeeded ||
		status.Reason != VectorIndexReasonColumnGraphUnsupportedVisibility ||
		!status.RebuildNeeded ||
		status.Loaded {
		tb.Fatalf("status=%+v want rebuild-needed unsupported visibility", status)
	}
}

func updateColumnGraphRebuildJSONDocumentV5(tb testing.TB, col *Collection, id string, vector []float32, note string) {
	tb.Helper()
	replacement, err := json.Marshal(map[string]any{
		"time_us":   int64(100),
		"kind":      "vector",
		"did":       id,
		"embedding": vector,
		"note":      note,
	})
	if err != nil {
		tb.Fatalf("json.Marshal replacement: %v", err)
	}
	matched, modified, err := col.Update([]byte(id), func(current []byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		tb.Fatalf("Update %q: %v", id, err)
	}
	if !matched || !modified {
		tb.Fatalf("Update %q matched=%t modified=%t want true/true", id, matched, modified)
	}
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		MaxDecodedBlocks: 1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	// Sample deterministic telemetry before the timed loop so reporting does not
	// add per-iteration work to the public one-shot search benchmark.
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	// The steady-state benchmark times only Search. Metrics come from a warmed
	// pre-timer search so telemetry accumulation cannot hide throughput drift.
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	// Document materialization remains in the timed loop; metric collection does
	// not, so allocs/op reflects the public API path instead of test bookkeeping.
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6(b *testing.B) {
	const (
		rows = 1024
		dims = 128
		m    = 16
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	statsSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("stats OpenVectorIndexSearcher: %v", err)
	}
	readerStats := statsSearcher.reader.Stats()
	stats := VectorIndexSearchStats{
		OpenGranulesRead:      uint64(readerStats.OpenGranulesRead),
		OpenPhysicalBytesRead: readerStats.OpenPhysicalBytesRead,
		MaxResidentBytes:      readerStats.MaxResidentBytes,
	}
	if err := statsSearcher.Close(); err != nil {
		b.Fatalf("Close stats searcher: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
			IndexName:        def.Name,
			MaxDecodedBlocks: 1,
		})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += searcher.reader.RowCount()
		if err := searcher.Close(); err != nil {
			b.Fatalf("Close searcher: %v", err)
		}
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func reportVectorIndexSearchBenchMetricsV4(b *testing.B, n int, stats VectorIndexSearchStats, includeOpenPerOp bool) {
	b.Helper()
	if n <= 0 {
		return
	}
	// Callers pass one representative search/setup sample captured outside the
	// timer; these labels are intentionally per-search or per-open, not averaged
	// over b.N. Keep aggregation out of the hot benchmark loop.
	b.ReportMetric(float64(stats.Candidates), "candidates/search")
	b.ReportMetric(float64(stats.Edges), "edges/search")
	if stats.Candidates > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(stats.CandidateFetches), "candidate_fetches/search")
	b.ReportMetric(float64(stats.ExpansionFetches), "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches), "result_fetches/search")
	b.ReportMetric(float64(stats.RowFetches), "row_fetches/search")
	b.ReportMetric(float64(stats.BatchFetches), "batch_fetches/search")
	b.ReportMetric(float64(stats.RowsFetched), "rows_fetched/search")
	b.ReportMetric(float64(stats.CacheHits), "cache_hits/search")
	b.ReportMetric(float64(stats.CacheMisses), "cache_misses/search")
	if cacheLookups := stats.CacheHits + stats.CacheMisses; cacheLookups > 0 {
		b.ReportMetric(float64(stats.CacheHits)/float64(cacheLookups), "cache_hit_ratio")
	}
	b.ReportMetric(float64(stats.DecodedBlocks), "decoded_blocks/search")
	b.ReportMetric(float64(stats.GranulesTouched), "granules_touched/search")
	b.ReportMetric(float64(stats.PhysicalBytesRead), "physical_B/search")
	b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	if includeOpenPerOp {
		b.ReportMetric(float64(stats.OpenGranulesRead), "open_granules/op")
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead), "open_physical_B/op")
	}
	if stats.OpenGranulesRead > 0 {
		b.ReportMetric(float64(stats.OpenGranulesRead), "max_open_granules")
	}
	if stats.OpenPhysicalBytesRead > 0 {
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead), "max_open_physical_B")
	}
	b.ReportMetric(float64(stats.DocumentsFetched), "docs_fetched/search")
}

var vectorSearchBenchSinkOrdinalV4 int
