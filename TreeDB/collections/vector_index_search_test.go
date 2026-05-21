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
			Strategy:   VectorIndexStrategyNativeRuntime,
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
	if got.Status.State != VectorIndexStateNativeRuntime || got.Status.Reason != VectorIndexReasonNativeRuntime {
		t.Fatalf("status=%+v want native_runtime unavailable response", got.Status)
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
			col.meta.VectorIndexes[i].Metric = VectorMetric("dot")
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
		def.Metric = VectorMetric("dot")
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
	if _, err := col.SearchVectorIndex(opts); err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
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
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
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
	if _, err := col.SearchVectorIndex(opts); err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
}

func reportVectorIndexSearchBenchMetricsV4(b *testing.B, n int, stats VectorIndexSearchStats) {
	b.Helper()
	if n <= 0 {
		return
	}
	b.ReportMetric(float64(stats.Candidates)/float64(n), "candidates/search")
	b.ReportMetric(float64(stats.Edges)/float64(n), "edges/search")
	if stats.Candidates > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(stats.CandidateFetches)/float64(n), "candidate_fetches/search")
	b.ReportMetric(float64(stats.ExpansionFetches)/float64(n), "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches)/float64(n), "result_fetches/search")
	b.ReportMetric(float64(stats.DocumentsFetched)/float64(n), "docs_fetched/search")
}

var vectorSearchBenchSinkOrdinalV4 int
