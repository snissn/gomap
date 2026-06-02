package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestVectorIndexSearchStatsModePublicMapping2126(t *testing.T) {
	tests := []struct {
		name string
		mode VectorIndexSearchStatsMode
		want columnVectorGraphNativeSearchStatsMode
	}{
		{name: "default_preserves_full_diagnostics", mode: VectorIndexSearchStatsModeDefault, want: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		{name: "full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics, want: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		{name: "minimal", mode: VectorIndexSearchStatsModeMinimal, want: columnVectorGraphNativeSearchStatsModeMinimal},
		{name: "production_alias", mode: VectorIndexSearchStatsModeProduction, want: columnVectorGraphNativeSearchStatsModeMinimal},
		{name: "benchmark_debug", mode: VectorIndexSearchStatsModeBenchmarkDebug, want: columnVectorGraphNativeSearchStatsModeBenchmarkDebug},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := columnVectorGraphNativeSearchStatsModeFromPublic(tt.mode)
			if err != nil {
				t.Fatalf("columnVectorGraphNativeSearchStatsModeFromPublic(%q): %v", tt.mode, err)
			}
			if got != tt.want {
				t.Fatalf("columnVectorGraphNativeSearchStatsModeFromPublic(%q)=%s want %s", tt.mode, got, tt.want)
			}
		})
	}
	if got, err := columnVectorGraphNativeSearchStatsModeFromPublic(VectorIndexSearchStatsMode("debug_everything")); err == nil || got != columnVectorGraphNativeSearchStatsModeDefault {
		t.Fatalf("unsupported mode got=(%s,%v) want default mode with error", got, err)
	}
}

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
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, reopened, "docs")
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		t.Fatalf("graph manifest record %q missing", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		t.Fatalf("healthy rebuilt graph has physical row asset %+v; want TVIS/base typed-column state only", graph.AssetRef)
	}
	if got.Stats.Candidates == 0 || got.Stats.CandidateFetches == 0 || got.Stats.ResultFetches < uint64(len(got.Results)) {
		t.Fatalf("stats=%+v want public search to expose non-zero native graph traversal/result accounting", got.Stats)
	}
	if got.Stats.CandidateRows != uint64(len(rows)) || got.Stats.VisitedNodes < got.Stats.Candidates || got.Stats.VisitedEdges != got.Stats.Edges || got.Stats.VectorBytesRead == 0 || got.Stats.AdjacencyBytesRead == 0 {
		t.Fatalf("stats=%+v want public operation-specific candidate row, non-undercounting visited graph, vector-byte, and adjacency-byte counters", got.Stats)
	}
	if got.Stats.AdjacencyPreparedCSRMmapDirectViews+got.Stats.AdjacencyTypedListMmapDirectViews+got.Stats.AdjacencyTypedListHeapCopyTypedViews+got.Stats.AdjacencyTypedListScratchDecodes == 0 || got.Stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("stats=%+v want public search to expose prepared/state adjacency and no legacy fallback on healthy state", got.Stats)
	}
	if got.Stats.GraphRows != 0 || got.Stats.RowFetches != 0 || got.Stats.BatchFetches != 0 || got.Stats.RowsFetched != 0 || got.Stats.PhysicalBytesRead != 0 || got.Stats.OpenPhysicalBytesRead != 0 {
		t.Fatalf("stats=%+v want zero graph row payload residency/reads on healthy current-format search", got.Stats)
	}
	if got.Stats.TypedColumnFallbacks != 0 || got.Stats.RowRefVectorSourceLegacyGraphIDs != 0 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.NormSourceFallbacks != 0 {
		t.Fatalf("stats=%+v want TVIS/base typed-column sources without graph row fallback", got.Stats)
	}
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if got.Stats.PreparedScoreCalls == 0 || got.Stats.PreparedScoreCalls != got.Stats.CandidateFetches {
			t.Fatalf("stats=%+v want prepared scoring to cover every candidate fetch", got.Stats)
		}
		if got.Stats.VectorPreparedDirectViews != got.Stats.CandidateFetches || got.Stats.NormPreparedDirectViews != got.Stats.CandidateFetches {
			t.Fatalf("stats=%+v want prepared vector/norm direct views for every scored candidate", got.Stats)
		}
		if got.Stats.VectorPreparedIdentityMappings+got.Stats.VectorPreparedRowRefMappings != got.Stats.CandidateFetches || got.Stats.ScoreFloat64Fallbacks != 0 {
			t.Fatalf("stats=%+v want prepared mapping coverage and no rare float64 score fallback", got.Stats)
		}
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
	if got.Stats.DocumentRowRefStateFetches != uint64(len(got.Results)) || got.Stats.DocumentRowRefLookupFallbacks != 0 || got.Stats.DocumentRowLocatorLookups != 0 || got.Stats.DocumentRowLocatorMisses != 0 || got.Stats.DocumentPointRowFetches != uint64(len(got.Results)) || got.Stats.DocumentPointRowDecodes != uint64(len(got.Results)) {
		t.Fatalf("stats=%+v want vector-index row refs and point row fetch per document", got.Stats)
	}
	if got.Stats.DocumentRowRefFallbackScans != 0 || got.Stats.DocumentVisibilityScans != 0 || got.Stats.DocumentVisibilityRowsScanned != 0 {
		t.Fatalf("stats=%+v want IncludeDocuments to avoid row-ref scan fallback on supported manifest", got.Stats)
	}
	documentBefore := append([]byte(nil), got.Results[0].Document...)
	if _, err := col.SearchVectorIndex(opts); err != nil {
		t.Fatalf("second SearchVectorIndex: %v", err)
	}
	if !bytes.Equal(got.Results[0].Document, documentBefore) {
		t.Fatalf("top result document changed after a later search; want response-owned bytes")
	}
}

func TestSearchVectorIndexColumnGraphProjectedDocuments1875(t *testing.T) {
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

	full, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex full documents: %v", err)
	}
	projected, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:            def.Name,
		Query:                []float32{0, 0, 1},
		TopK:                 2,
		EfSearch:             len(rows),
		IncludeDocuments:     true,
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
		MaxDecodedBlocks:     1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex projected documents: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, projected, def.Name, 2)
	if projected.Stats.DocumentsFetched != uint64(len(projected.Results)) || projected.Stats.DocumentFieldsSkipped == 0 || projected.Stats.DocumentFieldsReconstructed == 0 {
		t.Fatalf("projected stats=%+v want fetched docs, skipped fields, reconstructed fields", projected.Stats)
	}
	if projected.Stats.DocumentOutputBytes != projected.Stats.DocumentBytes || projected.Stats.DocumentBytes >= full.Stats.DocumentBytes {
		t.Fatalf("projected stats=%+v full stats=%+v want output bytes below full document bytes", projected.Stats, full.Stats)
	}
	for i := range projected.Results {
		var doc map[string]any
		if err := json.Unmarshal(projected.Results[i].Document, &doc); err != nil {
			t.Fatalf("projected document[%d]=%q invalid JSON: %v", i, projected.Results[i].Document, err)
		}
		if _, ok := doc["embedding"]; ok {
			t.Fatalf("projected document[%d]=%s retained embedding", i, projected.Results[i].Document)
		}
		did, didOK := doc["did"].(string)
		kind, kindOK := doc["kind"].(string)
		if !didOK || did == "" || !kindOK || kind != "vector" {
			t.Fatalf("projected document[%d]=%v want selected metadata fields", i, doc)
		}
	}
	secondBefore := append([]byte(nil), projected.Results[1].Document...)
	_ = append(projected.Results[0].Document, '!')
	if !bytes.Equal(projected.Results[1].Document, secondBefore) {
		t.Fatalf("projected response documents share capacity: second=%s want %s", projected.Results[1].Document, secondBefore)
	}
	projected.Results[0].Document[0] = 'X'
	fresh, err := col.Get(projected.Results[0].ID)
	if err != nil {
		t.Fatalf("Get after mutating projected response: %v", err)
	}
	if len(fresh) == 0 || fresh[0] == 'X' {
		t.Fatalf("mutating projected response affected stored document: %s", fresh)
	}
}

func TestVectorIndexSearcherProjectedDocumentsSnapshotBound1875(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a after opening searcher: %v", err)
	}
	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:                []float32{1, 0, 0},
		TopK:                 1,
		EfSearch:             len(rows),
		IncludeDocuments:     true,
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
	})
	if err != nil {
		t.Fatalf("Search projected after delete on bound searcher: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("results=%+v want snapshot-bound doc-a", got.Results)
	}
	var doc map[string]any
	if err := json.Unmarshal(got.Results[0].Document, &doc); err != nil {
		t.Fatalf("projected snapshot document invalid JSON: %v", err)
	}
	if doc["did"] != "doc-a" {
		t.Fatalf("projected snapshot doc=%v want old doc-a", doc)
	}
	if _, ok := doc["embedding"]; ok {
		t.Fatalf("projected snapshot document retained embedding: %s", got.Results[0].Document)
	}
	if got.Stats.DocumentPointRowFetches != 1 || got.Stats.DocumentFieldsSkipped == 0 {
		t.Fatalf("stats=%+v want row-ref point fetch and skipped projection field", got.Stats)
	}
}

func TestSearchVectorIndexProjectionRequiresIncludeDocuments1875(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	_, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:            def.Name,
		Query:                []float32{1, 0, 0},
		TopK:                 1,
		EfSearch:             len(rows),
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
	})
	if err == nil || !strings.Contains(err.Error(), "IncludeDocuments") {
		t.Fatalf("SearchVectorIndex projection without IncludeDocuments err=%v want fail closed", err)
	}
}

func TestSearchVectorIndexColumnGraphRetainedFullDocumentsFallbackV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 0)
	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.RetainedPayload = ColumnRetainedPayloadFull
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("results=%+v want doc-a", got.Results)
	}
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-a")
	if got.Stats.DocumentRowRefUnsupported != 1 || got.Stats.DocumentPointRowFetches != 0 || got.Stats.DocumentVisibilityScans != 0 || got.Stats.DocumentsFetched != 1 {
		t.Fatalf("stats=%+v want retained-full fallback without row-ref point fetch", got.Stats)
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
	if got.Stats.DocumentRowRefStateFetches != 1 || got.Stats.DocumentRowRefLookupFallbacks != 0 || got.Stats.DocumentRowLocatorLookups != 0 || got.Stats.DocumentPointRowFetches != 1 || got.Stats.DocumentPointRowDecodes != 1 || got.Stats.DocumentRowRefFallbackScans != 0 || got.Stats.DocumentVisibilityScans != 0 {
		t.Fatalf("stats=%+v want prepared IncludeDocuments to use vector-index row refs and point fetches", got.Stats)
	}
	if searcher.documentView == nil || searcher.documentView.assetScopeKind != mappedresource.ScopePreparedSearch {
		t.Fatalf("document view=%+v want prepared_search scope", searcher.documentView)
	}
	again, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("second Search after delete on bound searcher: %v", err)
	}
	if !bytes.Equal(again.Results[0].Document, got.Results[0].Document) {
		t.Fatalf("second document=%s want %s", again.Results[0].Document, got.Results[0].Document)
	}
	if again.Stats.DocumentAssetFileOpens != 0 {
		t.Fatalf("second stats=%+v want prepared document read cache reuse without file opens", again.Stats)
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
	// Physical read/cache counters and generic row fetches may be zero when the
	// planner-backed segment/block cache is warm; logical candidate counters prove
	// the bound reader is reused without depending on cold-cache telemetry.
	if first.Stats.CandidateFetches == 0 || second.Stats.CandidateFetches == 0 {
		t.Fatalf("candidate fetch stats first=%d second=%d want non-zero per-search deltas", first.Stats.CandidateFetches, second.Stats.CandidateFetches)
	}
	if first.Stats.Candidates == 0 || second.Stats.Candidates == 0 {
		t.Fatalf("candidate stats first=%d second=%d want non-zero searches", first.Stats.Candidates, second.Stats.Candidates)
	}
}

func TestVectorIndexSearcherSearchWithBufferResultEquivalenceAndZeroAllocs2124(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	ownedSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher owned: %v", err)
	}
	defer func() { _ = ownedSearcher.Close() }()
	bufferedSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher buffered: %v", err)
	}
	defer func() { _ = bufferedSearcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{0.6, 0.4, 0}, TopK: 3, EfSearch: len(rows)}
	owned, err := ownedSearcher.Search(opts)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, opts.TopK)

	var buffer VectorIndexSearchBuffer
	buffered, err := bufferedSearcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, buffered, def.Name, opts.TopK)
	assertVectorIndexSearchResponsesEquivalentNoDocs2124(t, buffered, owned)
	assertVectorIndexSearchResultIDStatsContract2124(t, buffered.Stats, owned.Stats)

	if _, err := bufferedSearcher.SearchWithBuffer(opts, &buffer); err != nil {
		t.Fatalf("warm SearchWithBuffer for allocation check: %v", err)
	}
	var sink int
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := bufferedSearcher.SearchWithBuffer(opts, &buffer)
		if err != nil {
			panic(err)
		}
		if len(got.Results) != opts.TopK {
			panic("unexpected SearchWithBuffer result count")
		}
		sink += len(got.Results) + got.Results[0].Ordinal
	})
	if allocs != 0 {
		t.Fatalf("SearchWithBuffer steady-state allocs=%v want 0", allocs)
	}
	if sink == 0 {
		t.Fatal("allocation check did not consume results")
	}
}

func TestVectorIndexSearcherSearchWithBufferReuseGrowShrinkNoStaleIDs1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-long-a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
		{id: "cc", vector: []float32{0, 0, 1}},
		{id: "dddd", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	growOpts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 3, EfSearch: len(rows)}
	for i := 0; i < 3; i++ {
		got, err := searcher.SearchWithBuffer(growOpts, &buffer)
		if err != nil {
			t.Fatalf("SearchWithBuffer grow iteration %d: %v", i, err)
		}
		assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 3)
		assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
		for j, result := range got.Results {
			if cap(result.ID) != len(result.ID) {
				t.Fatalf("grow iteration %d result[%d] id len/cap=%d/%d want cap isolated", i, j, len(result.ID), cap(result.ID))
			}
		}
	}

	if len(buffer.results) != 3 {
		t.Fatalf("test setup buffer results=%d want 3 before shrink", len(buffer.results))
	}
	buffer.results[1].Document = []byte("stale-document")
	buffer.results[2].Score = 99

	shrinkOpts := VectorIndexSearcherSearchOptions{Query: []float32{0, 1, 0}, TopK: 1, EfSearch: len(rows)}
	shrunk, err := searcher.SearchWithBuffer(shrinkOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer shrink: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, shrunk, def.Name, 1)
	assertVectorIndexSearchResultsV4(t, shrunk.Results, exactColumnGraphTopKForTest(t, rows, shrinkOpts.Query, shrinkOpts.TopK), false)
	if gotID := string(shrunk.Results[0].ID); gotID != "b" {
		t.Fatalf("shrunk top id=%q want b without stale bytes from longer prior IDs", gotID)
	}
	if cap(shrunk.Results[0].ID) != len(shrunk.Results[0].ID) {
		t.Fatalf("shrunk id len/cap=%d/%d want cap isolated", len(shrunk.Results[0].ID), cap(shrunk.Results[0].ID))
	}
	allResults := buffer.results[:cap(buffer.results)]
	for i := len(buffer.results); i < 3 && i < len(allResults); i++ {
		if allResults[i].ID != nil || allResults[i].Ordinal != 0 || allResults[i].Score != 0 || allResults[i].Document != nil {
			t.Fatalf("shrunk stale tail result[%d]=%+v want cleared", i, allResults[i])
		}
	}

	regrown, err := searcher.SearchWithBuffer(growOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer regrow: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, regrown, def.Name, 3)
	assertVectorIndexSearchResultsV4(t, regrown.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
}

func TestVectorIndexSearcherSearchWithBufferOversizedReuseClearsCurrentView2124(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(24, 3)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	wideOpts := VectorIndexSearcherSearchOptions{Query: rows[0].vector, TopK: len(rows), EfSearch: len(rows)}
	wide, err := searcher.SearchWithBuffer(wideOpts, &buffer)
	if err != nil {
		t.Fatalf("wide SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, wide, def.Name, len(rows))
	if cap(buffer.results) <= 1*2+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("test setup cap=%d want oversized for shrink-to-one path", cap(buffer.results))
	}
	for i := 1; i < len(buffer.results); i++ {
		buffer.results[i].Document = []byte("stale-document")
		buffer.results[i].Score = 99
	}

	shrinkOpts := VectorIndexSearcherSearchOptions{Query: rows[1].vector, TopK: 1, EfSearch: len(rows)}
	shrunk, err := searcher.SearchWithBuffer(shrinkOpts, &buffer)
	if err != nil {
		t.Fatalf("shrink SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, shrunk, def.Name, 1)
	if cap(shrunk.Results) != 1 || cap(buffer.results) != 1 {
		t.Fatalf("shrunk result cap response=%d buffer=%d want new exact backing for oversized reuse", cap(shrunk.Results), cap(buffer.results))
	}
	if shrunk.Results[0].Document != nil || shrunk.Results[0].Score == 99 {
		t.Fatalf("shrunk current result retained stale state: %+v", shrunk.Results[0])
	}
}

func TestVectorIndexSearcherSearchWithBufferDoesNotMutateSearchResponse1961(t *testing.T) {
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
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows)}
	owned, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, 2)
	ownedID := append([]byte(nil), owned.Results[0].ID...)

	var buffer VectorIndexSearchBuffer
	buffered, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, buffered, def.Name, 2)
	buffered.Results[0].ID[0] = 'X'
	if !bytes.Equal(owned.Results[0].ID, ownedID) {
		t.Fatalf("Search response ID changed after mutating SearchWithBuffer response: got %q want %q", owned.Results[0].ID, ownedID)
	}

	if _, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{0, 1, 0}, TopK: 1, EfSearch: len(rows)}, &buffer); err != nil {
		t.Fatalf("second SearchWithBuffer: %v", err)
	}
	if !bytes.Equal(owned.Results[0].ID, ownedID) {
		t.Fatalf("Search response ID changed after reusing buffer: got %q want %q", owned.Results[0].ID, ownedID)
	}
}

func TestVectorIndexSearcherSearchWithBufferParallelIndependentBuffers1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0}
	topK := 2
	want := exactColumnGraphTopKForTest(t, rows, query, topK)
	const workers = 4
	const iterations = 20
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				errs <- fmt.Sprintf("worker %d OpenVectorIndexSearcher: %v", worker, err)
				return
			}
			defer func() { _ = searcher.Close() }()
			var buffer VectorIndexSearchBuffer
			opts := VectorIndexSearcherSearchOptions{Query: query, TopK: topK, EfSearch: len(rows)}
			for i := 0; i < iterations; i++ {
				got, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					errs <- fmt.Sprintf("worker %d iteration %d SearchWithBuffer: %v", worker, i, err)
					return
				}
				if len(got.Results) != len(want) {
					errs <- fmt.Sprintf("worker %d iteration %d results=%d want %d", worker, i, len(got.Results), len(want))
					return
				}
				for j := range want {
					if !bytes.Equal(got.Results[j].ID, want[j].ID) || math.Abs(got.Results[j].Score-want[j].Score) > 1e-6 {
						errs <- fmt.Sprintf("worker %d iteration %d result[%d]=%+v want id=%q score=%v", worker, i, j, got.Results[j], want[j].ID, want[j].Score)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestVectorIndexSearcherSearchWithBufferErrorResetsBuffer1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	validOpts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows)}
	if got, err := searcher.SearchWithBuffer(validOpts, &buffer); err != nil || len(got.Results) != 2 {
		t.Fatalf("initial SearchWithBuffer results=%d err=%v want 2, nil", len(got.Results), err)
	}

	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), IncludeDocuments: true}, &buffer)
	if err == nil || !strings.Contains(err.Error(), "IncludeDocuments") {
		t.Fatalf("SearchWithBuffer IncludeDocuments err=%v want documented no-document failure", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("IncludeDocuments error left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
	if got.IndexName != def.Name || got.Status.State != VectorIndexStateColumnGraphLoaded {
		t.Fatalf("error response metadata=%+v status=%+v want loaded search metadata", got, got.Status)
	}

	got, err = searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, EfSearch: len(rows)}, &buffer)
	if err == nil || !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("SearchWithBuffer dimension err=%v want query dimension mismatch", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("dimension error left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}

	got, err = searcher.SearchWithBuffer(validOpts, &buffer)
	if err != nil {
		t.Fatalf("valid SearchWithBuffer after errors: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
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
		if !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want id=%q ordinal=%d score=%v", i, got[i], want[i].ID, want[i].Ordinal, want[i].Score)
		}
		if wantDocs && len(got[i].Document) == 0 {
			tb.Fatalf("result[%d] missing materialized document", i)
		}
	}
}

func assertVectorIndexSearchResponsesEquivalentNoDocs2124(tb testing.TB, got, want VectorIndexSearchResponse) {
	tb.Helper()
	if got.IndexName != want.IndexName || got.Strategy != want.Strategy || got.Path != want.Path || got.Status != want.Status {
		tb.Fatalf("response metadata got=%+v status=%+v want=%+v status=%+v", got, got.Status, want, want.Status)
	}
	if len(got.Results) != len(want.Results) {
		tb.Fatalf("results=%d want %d", len(got.Results), len(want.Results))
	}
	for i := range want.Results {
		if !bytes.Equal(got.Results[i].ID, want.Results[i].ID) || got.Results[i].Ordinal != want.Results[i].Ordinal || math.Abs(got.Results[i].Score-want.Results[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want %+v", i, got.Results[i], want.Results[i])
		}
		if len(got.Results[i].Document) != 0 {
			tb.Fatalf("result[%d] document len=%d want no-document reusable response", i, len(got.Results[i].Document))
		}
	}
}

func assertVectorIndexSearchResultIDStatsContract2124(tb testing.TB, got, want VectorIndexSearchStats) {
	tb.Helper()
	checks := []struct {
		name string
		got  uint64
		want uint64
	}{
		{name: "candidate_rows", got: got.CandidateRows, want: want.CandidateRows},
		{name: "candidates", got: got.Candidates, want: want.Candidates},
		{name: "edges", got: got.Edges, want: want.Edges},
		{name: "visited_nodes", got: got.VisitedNodes, want: want.VisitedNodes},
		{name: "visited_edges", got: got.VisitedEdges, want: want.VisitedEdges},
		{name: "vector_bytes_read", got: got.VectorBytesRead, want: want.VectorBytesRead},
		{name: "norm_bytes_read", got: got.NormBytesRead, want: want.NormBytesRead},
		{name: "adjacency_bytes_read", got: got.AdjacencyBytesRead, want: want.AdjacencyBytesRead},
		{name: "candidate_fetches", got: got.CandidateFetches, want: want.CandidateFetches},
		{name: "expansion_fetches", got: got.ExpansionFetches, want: want.ExpansionFetches},
		{name: "result_fetches", got: got.ResultFetches, want: want.ResultFetches},
		{name: "prepared_graph_search_views", got: got.PreparedGraphSearchViews, want: want.PreparedGraphSearchViews},
		{name: "result_id_typed_bytes_state", got: got.ResultIDTypedBytesState, want: want.ResultIDTypedBytesState},
		{name: "result_id_graph_fallbacks", got: got.ResultIDGraphFallbacks, want: want.ResultIDGraphFallbacks},
		{name: "row_ref_state_result_refs", got: got.RowRefStateResultRefs, want: want.RowRefStateResultRefs},
		{name: "graph_row_fallbacks", got: got.GraphRowFallbacks, want: want.GraphRowFallbacks},
		{name: "typed_column_fallbacks", got: got.TypedColumnFallbacks, want: want.TypedColumnFallbacks},
		{name: "vector_scratch_decodes", got: got.VectorScratchDecodes, want: want.VectorScratchDecodes},
		{name: "norm_scratch_decodes", got: got.NormScratchDecodes, want: want.NormScratchDecodes},
		{name: "adjacency_scratch_decodes", got: got.AdjacencyScratchDecodes, want: want.AdjacencyScratchDecodes},
	}
	for _, check := range checks {
		if check.got != check.want {
			tb.Fatalf("stats %s=%d want %d; got=%+v wantStats=%+v", check.name, check.got, check.want, got, want)
		}
	}
	if got.GraphRows != 0 || got.ResultIDGraphFallbacks != 0 || got.GraphRowFallbacks != 0 || got.VectorScratchDecodes != 0 || got.NormScratchDecodes != 0 || got.AdjacencyScratchDecodes != 0 || got.TypedColumnFallbacks != 0 {
		tb.Fatalf("stats=%+v want healthy typed-column result-ID path without graph-row/scratch fallbacks", got)
	}
	if got.ResultIDTypedBytesState == 0 || got.ResultIDPreparedBytesViews == 0 || got.RowRefVectorSourceState == 0 {
		tb.Fatalf("stats=%+v want typed-column result-ID and row-ref state", got)
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

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
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
		IndexName:            def.Name,
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     true,
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
		MaxDecodedBlocks:     1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
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

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsV4(b *testing.B) {
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
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
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
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     true,
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, false, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4StatsMode2126(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderStatsModes2126(b, false, DocumentFetchOptions{})
}

func BenchmarkVectorSearchPublicSearchSerialTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, true, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, true, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, includeDocuments, fetchOptions, VectorIndexSearchStatsModeDefault)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderStatsModes2126(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	b.Helper()
	for _, tc := range []struct {
		name string
		mode VectorIndexSearchStatsMode
	}{
		{name: "stats=full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics},
		{name: "stats=production", mode: VectorIndexSearchStatsModeProduction},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, includeDocuments, fetchOptions, tc.mode)
		})
	}
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions, statsMode VectorIndexSearchStatsMode) {
	b.Helper()
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
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
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     includeDocuments,
		DocumentFetchOptions: fetchOptions,
		StatsMode:            statsMode,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		if includeDocuments {
			vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
		} else {
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
	}
	b.StopTimer()
	reportVectorIndexSearchStatsModeBenchMetric2126(b, statsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkVectorSearchReusableBufferSerialTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
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
	var buffer VectorIndexSearchBuffer
	if _, err := searcher.SearchWithBuffer(opts, &buffer); err != nil {
		b.Fatalf("warm SearchWithBuffer: %v", err)
	}
	measuredStats, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		b.Fatalf("measure SearchWithBuffer stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column reusable-buffer benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.SearchWithBuffer(opts, &buffer)
		if err != nil {
			b.Fatalf("SearchWithBuffer: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, false, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4StatsMode2126(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelStatsModes2126(b, false, DocumentFetchOptions{})
}

func BenchmarkVectorSearchPublicSearchParallelTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithDocumentsV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, true, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithDocumentsExcludeEmbedding1875(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, true, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, includeDocuments, fetchOptions, VectorIndexSearchStatsModeDefault)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelStatsModes2126(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	b.Helper()
	for _, tc := range []struct {
		name string
		mode VectorIndexSearchStatsMode
	}{
		{name: "stats=full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics},
		{name: "stats=production", mode: VectorIndexSearchStatsModeProduction},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, includeDocuments, fetchOptions, tc.mode)
		})
	}
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions, statsMode VectorIndexSearchStatsMode) {
	b.Helper()
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
	if workers < 1 {
		workers = 1
	}
	previousGOMAXPROCS := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousGOMAXPROCS)
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     includeDocuments,
		DocumentFetchOptions: fetchOptions,
		StatsMode:            statsMode,
	}
	type preparedWorker struct {
		searcher *VectorIndexSearcher
	}
	benchWorkers := make([]preparedWorker, workers)
	for i := range benchWorkers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func() { _ = searcher.Close() }()
		if _, err := searcher.Search(opts); err != nil {
			b.Fatalf("warm Search worker %d: %v", i, err)
		}
		benchWorkers[i] = preparedWorker{searcher: searcher}
	}
	measuredStats, err := benchWorkers[0].searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column parallel benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	var nextWorker atomic.Uint64
	var sink atomic.Int64
	var firstErr atomic.Value
	var failed atomic.Bool
	recordParallelErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerIndex := int(nextWorker.Add(1)) - 1
		if workerIndex < 0 || workerIndex >= len(benchWorkers) {
			recordParallelErr("parallel worker requested more than %d prepared searchers", workers)
			for pb.Next() {
			}
			return
		}
		searcher := benchWorkers[workerIndex].searcher
		var localSink int64
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, err := searcher.Search(opts)
			if err != nil {
				recordParallelErr("Search: %v", err)
				continue
			}
			if len(got.Results) == 0 {
				recordParallelErr("Search returned no results")
				continue
			}
			if includeDocuments {
				localSink += int64(len(got.Results[0].Document))
			} else {
				localSink += int64(got.Results[0].Ordinal)
			}
		}
		sink.Add(localSink)
	})
	b.StopTimer()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, col.columnVectorGraphSharedPreparedSearchCacheSnapshot(), workers)
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
	reportVectorIndexSearchStatsModeBenchMetric2126(b, statsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkVectorSearchReusableBufferParallelTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
	if workers < 1 {
		workers = 1
	}
	previousGOMAXPROCS := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousGOMAXPROCS)
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	type preparedWorker struct {
		searcher *VectorIndexSearcher
		buffer   *VectorIndexSearchBuffer
	}
	type paddedBuffer struct {
		buffer VectorIndexSearchBuffer
		_      [128]byte
	}
	benchWorkers := make([]preparedWorker, workers)
	buffers := make([]paddedBuffer, workers)
	for i := range benchWorkers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func() { _ = searcher.Close() }()
		buffer := &buffers[i].buffer
		benchWorkers[i] = preparedWorker{searcher: searcher, buffer: buffer}
		if _, err := searcher.SearchWithBuffer(opts, buffer); err != nil {
			b.Fatalf("warm SearchWithBuffer worker %d: %v", i, err)
		}
	}
	measuredStats, err := benchWorkers[0].searcher.SearchWithBuffer(opts, benchWorkers[0].buffer)
	if err != nil {
		b.Fatalf("measure SearchWithBuffer stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column reusable-buffer parallel benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	var nextWorker atomic.Uint64
	var sink atomic.Int64
	var firstErr atomic.Value
	var failed atomic.Bool
	recordParallelErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerIndex := int(nextWorker.Add(1)) - 1
		if workerIndex < 0 || workerIndex >= len(benchWorkers) {
			recordParallelErr("parallel worker requested more than %d prepared searchers", workers)
			for pb.Next() {
			}
			return
		}
		worker := &benchWorkers[workerIndex]
		var localSink int64
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, err := worker.searcher.SearchWithBuffer(opts, worker.buffer)
			if err != nil {
				recordParallelErr("SearchWithBuffer: %v", err)
				continue
			}
			if len(got.Results) == 0 {
				recordParallelErr("SearchWithBuffer returned no results")
				continue
			}
			localSink += int64(got.Results[0].Ordinal)
		}
		sink.Add(localSink)
	})
	b.StopTimer()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, col.columnVectorGraphSharedPreparedSearchCacheSnapshot(), workers)
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
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
		GraphRows:             uint64(readerStats.Rows),
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

func reportVectorIndexSearchStatsModeBenchMetric2126(b *testing.B, mode VectorIndexSearchStatsMode) {
	b.Helper()
	switch mode {
	case VectorIndexSearchStatsModeDefault:
		b.ReportMetric(1, "stats_mode_full_diagnostics")
	case VectorIndexSearchStatsModeMinimal:
		b.ReportMetric(1, "stats_mode_minimal")
	case VectorIndexSearchStatsModeProduction:
		b.ReportMetric(1, "stats_mode_production")
	case VectorIndexSearchStatsModeFullDiagnostics:
		b.ReportMetric(1, "stats_mode_full_diagnostics")
	case VectorIndexSearchStatsModeBenchmarkDebug:
		b.ReportMetric(1, "stats_mode_benchmark_debug")
	}
}

func reportVectorIndexSearchBenchMetricsV4(b *testing.B, n int, stats VectorIndexSearchStats, includeOpenPerOp bool) {
	b.Helper()
	if n <= 0 {
		return
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
	// Callers pass one representative search/setup sample captured outside the
	// timer; these labels are intentionally per-search or per-open, not averaged
	// over b.N. Keep aggregation out of the hot benchmark loop.
	b.ReportMetric(float64(stats.GraphRows), "graph_rows")
	b.ReportMetric(float64(stats.CandidateRows), "candidate_rows/search")
	b.ReportMetric(float64(stats.Candidates), "candidates/search")
	b.ReportMetric(float64(stats.Edges), "edges/search")
	b.ReportMetric(float64(stats.VisitedNodes), "visited_nodes/search")
	b.ReportMetric(float64(stats.VisitedEdges), "visited_edges/search")
	b.ReportMetric(float64(stats.VectorBytesRead), "vector_B/search")
	b.ReportMetric(float64(stats.NormBytesRead), "norm_B/search")
	b.ReportMetric(float64(stats.AdjacencyBytesRead), "adjacency_B/search")
	if stats.Candidates > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(stats.CandidateFetches), "candidate_fetches/search")
	b.ReportMetric(float64(stats.ScoreBatchCalls), "score_batch_calls/search")
	b.ReportMetric(float64(stats.ScoreBatchCandidates), "score_batch_candidates/search")
	b.ReportMetric(float64(stats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(stats.ScoreBatchOptimizedCalls), "score_batch_optimized/search")
	b.ReportMetric(float64(stats.ScoreBatchScalarFallbackCalls), "score_batch_fallback/search")
	b.ReportMetric(float64(stats.PreparedScoreCalls), "prepared_score_calls/search")
	b.ReportMetric(float64(stats.ScoreFloat64Fallbacks), "score_float64_fallbacks/search")
	if stats.ScoreBatchCalls > 0 {
		b.ReportMetric(float64(stats.ScoreBatchCandidates)/float64(stats.ScoreBatchCalls), "score_batch_avg_tile_size")
	}
	if stats.BenchmarkDebugSearches > 0 {
		reportVectorIndexSearchBenchmarkDebugMetrics2105(b, stats)
	}
	b.ReportMetric(float64(stats.ExpansionFetches), "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches), "result_fetches/search")
	b.ReportMetric(float64(stats.VectorDirectViews), "vector_direct_views/search")
	b.ReportMetric(float64(stats.VectorMmapDirectViews), "vector_mmap_direct/search")
	b.ReportMetric(float64(stats.VectorHeapCopyTypedViews), "vector_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes), "vector_scratch_decode/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes), "vector_scratch_decodes/search")
	b.ReportMetric(float64(stats.VectorPreparedDirectViews), "vector_prepared_direct/search")
	b.ReportMetric(float64(stats.VectorPreparedIdentityMappings), "vector_prepared_identity_mapping/search")
	b.ReportMetric(float64(stats.VectorPreparedRowRefMappings), "vector_prepared_row_ref_mapping/search")
	b.ReportMetric(float64(stats.VectorCertificationFailures), "vector_certification_failures/search")
	b.ReportMetric(float64(stats.VectorAbsoluteOffsetUnaligned), "vector_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.VectorActualPointerUnaligned), "vector_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.VectorStaleHandles), "vector_stale_handles/search")
	b.ReportMetric(float64(stats.AdjacencyDirectViews), "adjacency_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyMmapDirectViews), "adjacency_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyHeapCopyTypedViews), "adjacency_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.AdjacencyPreparedCSRDirectViews), "adjacency_prepared_csr_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyPreparedCSRMmapDirectViews), "adjacency_prepared_csr_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListDirectViews), "adjacency_typed_list_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListMmapDirectViews), "adjacency_typed_list_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListHeapCopyTypedViews), "adjacency_typed_list_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListScratchDecodes), "adjacency_typed_list_scratch_decodes/search")
	b.ReportMetric(float64(stats.AdjacencyLegacyFallbacks), "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencySourceUnavailable), "adjacency_source_unavailable/search")
	b.ReportMetric(float64(stats.AdjacencySourceFallbacks), "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencyCertificationFailures), "adjacency_certification_failures/search")
	b.ReportMetric(float64(stats.AdjacencyValidationFailures), "adjacency_validation_failures/search")
	b.ReportMetric(float64(stats.AdjacencyAbsoluteOffsetUnaligned), "adjacency_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.AdjacencyActualPointerUnaligned), "adjacency_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.AdjacencyStaleHandles), "adjacency_stale_handles/search")
	b.ReportMetric(float64(stats.AdjacencyScratchDecodes), "adjacency_scratch_decode/search")
	b.ReportMetric(float64(stats.AdjacencyScratchDecodes), "adjacency_scratch_decodes/search")
	b.ReportMetric(float64(stats.NormDirectViews), "norm_direct_views/search")
	b.ReportMetric(float64(stats.NormMmapDirectViews), "norm_mmap_direct/search")
	b.ReportMetric(float64(stats.NormHeapCopyTypedViews), "norm_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.NormScratchDecodes), "norm_scratch_decode/search")
	b.ReportMetric(float64(stats.NormScratchDecodes), "norm_scratch_decodes/search")
	b.ReportMetric(float64(stats.NormPreparedDirectViews), "norm_prepared_direct/search")
	b.ReportMetric(float64(stats.NormSourceUnavailable), "norm_source_unavailable/search")
	b.ReportMetric(float64(stats.NormSourceFallbacks), "norm_source_fallbacks/search")
	b.ReportMetric(float64(stats.NormValidationFailures), "norm_validation_failures/search")
	b.ReportMetric(float64(stats.NormAbsoluteOffsetUnaligned), "norm_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.NormActualPointerUnaligned), "norm_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.NormStaleHandles), "norm_stale_handles/search")
	b.ReportMetric(float64(stats.NormMappedBytes), "norm_mapped_B")
	b.ReportMetric(float64(stats.NormHeapCopyBytes), "norm_heap_copy_B")
	b.ReportMetric(float64(stats.NormDecodedBytes), "norm_decoded_B")
	b.ReportMetric(float64(stats.NormActiveHandles), "norm_active_handles")
	b.ReportMetric(float64(stats.NormDeniedResources), "norm_denied_resources")
	if stats.VectorMmapDirectViews > 0 {
		b.ReportMetric(1, "typed_column_vector_source_mmap")
	}
	if stats.VectorHeapCopyTypedViews > 0 {
		b.ReportMetric(1, "typed_column_vector_source_heap_copy")
	}
	if stats.VectorScratchDecodes > 0 && stats.TypedColumnDecodedBytes > 0 {
		b.ReportMetric(1, "typed_column_vector_source_scratch")
	}
	if stats.TypedColumnFallbacks > 0 {
		b.ReportMetric(1, "typed_column_vector_source_fallback")
	}
	b.ReportMetric(float64(stats.TypedColumnMappedBytes), "typed_column_mapped_B")
	b.ReportMetric(float64(stats.TypedColumnHeapCopyBytes), "typed_column_heap_copy_B")
	b.ReportMetric(float64(stats.TypedColumnDecodedBytes), "typed_column_decoded_derived_B")
	b.ReportMetric(float64(stats.TypedColumnActiveHandles), "typed_column_active_handles")
	b.ReportMetric(float64(stats.TypedColumnDeniedResources), "typed_column_denied_resources")
	b.ReportMetric(float64(stats.TypedColumnFallbacks), "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceState), "row_ref_vector_source_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceLegacyGraphIDs), "row_ref_vector_source_legacy_graph_ids/search")
	b.ReportMetric(float64(stats.RowRefStatePreparedViews), "row_ref_state_prepared_views/search")
	b.ReportMetric(float64(stats.RowRefStateMmapDirectFields), "row_ref_state_mmap_direct_fields/search")
	b.ReportMetric(float64(stats.RowRefStateResultRefs), "row_ref_state_result_refs/search")
	b.ReportMetric(float64(stats.RowRefStateSourceUnavailable), "row_ref_state_source_unavailable/search")
	b.ReportMetric(float64(stats.RowRefStateSourceFallbacks), "row_ref_state_source_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDPreparedBytesViews), "result_id_prepared_bytes_views/search")
	b.ReportMetric(float64(stats.ResultIDTypedBytesState), "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(stats.ResultIDGraphFallbacks), "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDStateValidationFailures), "result_id_state_validation_failures/search")
	b.ReportMetric(float64(stats.PreparedGraphSearchViews), "prepared_graph_search_views/search")
	b.ReportMetric(float64(stats.GraphRowFallbacks), "graph_row_fallbacks/search")
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
	b.ReportMetric(float64(stats.DocumentsMissing), "docs_missing/search")
	b.ReportMetric(float64(stats.DocumentBytes), "doc_B/search")
	b.ReportMetric(float64(stats.DocumentOutputBytes), "output_B/search")
	b.ReportMetric(float64(stats.DocumentFieldsReconstructed), "fields_reconstructed/search")
	b.ReportMetric(float64(stats.DocumentFieldsSkipped), "fields_skipped/search")
	b.ReportMetric(float64(stats.DocumentFetchNanos), "doc_fetch_ns/search")
	b.ReportMetric(float64(stats.DocumentRetainedFetches), "doc_retained_fetches/search")
	b.ReportMetric(float64(stats.DocumentRetainedBytes), "doc_retained_B/search")
	b.ReportMetric(float64(stats.DocumentVisibilityScans), "doc_visibility_scans/search")
	b.ReportMetric(float64(stats.DocumentVisibilityRowsScanned), "doc_visibility_rows_scanned/search")
	b.ReportMetric(float64(stats.DocumentVisibilityRows), "doc_visibility_rows/search")
	b.ReportMetric(float64(stats.DocumentVisibilityPhysicalBytes), "doc_visibility_physical_B/search")
	b.ReportMetric(float64(stats.DocumentVisibilityNanos), "doc_visibility_ns/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnRows), "doc_typed_column_rows/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnCacheHits), "doc_typed_column_cache_hits/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnCacheMisses), "doc_typed_column_cache_misses/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnPartLoads), "doc_typed_column_part_loads/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnPartDecodes), "doc_typed_column_part_decodes/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnNanos), "doc_typed_column_ns/search")
	b.ReportMetric(float64(stats.DocumentJSONReconstructionRows), "doc_json_reconstruction_rows/search")
	b.ReportMetric(float64(stats.DocumentJSONReconstructionNanos), "doc_json_reconstruction_ns/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorBuilds), "doc_row_locator_builds/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorLookups), "doc_row_locator_lookups/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorMisses), "doc_row_locator_misses/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorRowsScanned), "doc_row_locator_rows_scanned/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorPhysicalBytes), "doc_row_locator_physical_B/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorNanos), "doc_row_locator_ns/search")
	b.ReportMetric(float64(stats.DocumentRowRefStateFetches), "doc_row_ref_state_fetches/search")
	b.ReportMetric(float64(stats.DocumentRowRefLookupFallbacks), "doc_row_ref_lookup_fallbacks/search")
	b.ReportMetric(float64(stats.DocumentPointRowFetches), "doc_point_row_fetches/search")
	b.ReportMetric(float64(stats.DocumentPointRowDecodes), "doc_point_row_decodes/search")
	b.ReportMetric(float64(stats.DocumentRowRefFallbackScans), "doc_row_ref_fallback_scans/search")
	b.ReportMetric(float64(stats.DocumentRowRefUnsupported), "doc_row_ref_unsupported/search")
	b.ReportMetric(float64(stats.DocumentRowRefValidationFailures), "doc_row_ref_validation_failures/search")
	b.ReportMetric(float64(stats.DocumentAssetMmapHits), "doc_asset_mmap_hits/search")
	b.ReportMetric(float64(stats.DocumentAssetReadAtFallbacks), "doc_asset_readat_fallbacks/search")
	b.ReportMetric(float64(stats.DocumentAssetFileOpens), "doc_asset_file_opens/search")
	b.ReportMetric(float64(stats.DocumentAssetFileCloses), "doc_asset_file_closes/search")
	b.ReportMetric(float64(stats.DocumentAssetActiveHandles), "doc_asset_active_handles")
}

var vectorSearchBenchSinkOrdinalV4 int
