package collections

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestProjectionOrientedVectorDocumentFetchPresetOutput1903(t *testing.T) {
	preset, err := ProjectionOrientedVectorDocumentFetchPreset(VectorIndexDefinition{Field: "embedding"})
	if err != nil {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	if !preset.IncludeDocuments {
		t.Fatalf("IncludeDocuments=false want true")
	}
	if got, want := preset.DocumentFetchOptions.ExcludePaths, []string{"embedding"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ExcludePaths=%v want %v", got, want)
	}
	if len(preset.DocumentFetchOptions.IncludePaths) != 0 || preset.DocumentFetchOptions.Format != "" || preset.DocumentFetchOptions.ColumnAssetReadIntegrity != "" {
		t.Fatalf("unexpected fetch options: %+v", preset.DocumentFetchOptions)
	}

	searchOpts := VectorIndexSearchOptions{
		IncludeDocuments: false,
		DocumentFetchOptions: DocumentFetchOptions{
			IncludePaths:             []string{"title"},
			Format:                   DocumentFormatBSON,
			ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums,
		},
	}
	preset.ApplyToSearchOptions(&searchOpts)
	if !searchOpts.IncludeDocuments ||
		!reflect.DeepEqual(searchOpts.DocumentFetchOptions.ExcludePaths, []string{"embedding"}) ||
		len(searchOpts.DocumentFetchOptions.IncludePaths) != 0 ||
		searchOpts.DocumentFetchOptions.Format != DocumentFormatBSON ||
		searchOpts.DocumentFetchOptions.ColumnAssetReadIntegrity != ColumnAssetReadIntegritySkipChecksums {
		t.Fatalf("applied VectorIndexSearchOptions=%+v", searchOpts)
	}

	searcherOpts := VectorIndexSearcherSearchOptions{
		IncludeDocuments: false,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths:             []string{"old"},
			Format:                   DocumentFormatTemplateV1,
			ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify,
		},
	}
	preset.ApplyToSearcherSearchOptions(&searcherOpts)
	if !searcherOpts.IncludeDocuments ||
		!reflect.DeepEqual(searcherOpts.DocumentFetchOptions.ExcludePaths, []string{"embedding"}) ||
		searcherOpts.DocumentFetchOptions.Format != DocumentFormatTemplateV1 ||
		searcherOpts.DocumentFetchOptions.ColumnAssetReadIntegrity != ColumnAssetReadIntegrityCachedVerify {
		t.Fatalf("applied VectorIndexSearcherSearchOptions=%+v", searcherOpts)
	}

	preset.DocumentFetchOptions.ExcludePaths[0] = "mutated"
	if searchOpts.DocumentFetchOptions.ExcludePaths[0] != "embedding" || searcherOpts.DocumentFetchOptions.ExcludePaths[0] != "embedding" {
		t.Fatalf("applied options alias preset exclude slice: search=%v searcher=%v", searchOpts.DocumentFetchOptions.ExcludePaths, searcherOpts.DocumentFetchOptions.ExcludePaths)
	}
}

func TestProjectionOrientedVectorDocumentFetchPresetRejectsUnsupportedField1903(t *testing.T) {
	if _, err := ProjectionOrientedVectorDocumentFetchPreset(VectorIndexDefinition{}); err == nil || !strings.Contains(err.Error(), "path cannot be empty") {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset(empty def) err=%v want missing field failure", err)
	}
	for _, tc := range []struct {
		name      string
		field     string
		wantError string
	}{
		{name: "missing", field: "", wantError: "path cannot be empty"},
		{name: "nested", field: "embedding.values", wantError: "nested paths are not supported"},
		{name: "trimmed", field: " embedding", wantError: "leading or trailing spaces"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := ProjectionOrientedVectorDocumentFetchPresetForField(tc.field)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("ProjectionOrientedVectorDocumentFetchPresetForField(%q) err=%v want %q", tc.field, err, tc.wantError)
			}
		})
	}
}

func TestProjectionOrientedVectorDocumentFetchPresetPreservesDefaultDocuments1903(t *testing.T) {
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

	base := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	}
	withoutDocs, err := col.SearchVectorIndex(base)
	if err != nil {
		t.Fatalf("SearchVectorIndex without documents: %v", err)
	}
	if len(withoutDocs.Results) == 0 || len(withoutDocs.Results[0].Document) != 0 || withoutDocs.Stats.DocumentsFetched != 0 {
		t.Fatalf("withoutDocs results/stats=%+v/%+v want existing no-document default", withoutDocs.Results, withoutDocs.Stats)
	}

	fullOpts := base
	fullOpts.IncludeDocuments = true
	full, err := col.SearchVectorIndex(fullOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex full documents: %v", err)
	}
	assertSearchDocumentsHaveField1903(t, full, "embedding", true)

	preset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	projectedOpts := base
	preset.ApplyToSearchOptions(&projectedOpts)
	projected, err := col.SearchVectorIndex(projectedOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex preset projection: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, projected, def.Name, 2)
	assertSearchDocumentsHaveField1903(t, projected, "embedding", false)
	assertSearchDocumentsHaveField1903(t, projected, "did", true)
	if projected.Stats.DocumentsFetched != uint64(len(projected.Results)) || projected.Stats.DocumentFieldsSkipped == 0 || projected.Stats.DocumentOutputBytes >= full.Stats.DocumentOutputBytes {
		t.Fatalf("projected stats=%+v full stats=%+v want fetched projection below full output", projected.Stats, full.Stats)
	}
}

func TestProjectionOrientedVectorDocumentFetchPresetCustomVectorField1903(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	cfg := testColumnStoreConfig(nil)
	cfg.Columns = append(append([]ColumnStoreColumn(nil), cfg.Columns...), ColumnStoreColumn{
		Name:       "other_embedding",
		Path:       "other_embedding",
		Owner:      TypedStorageOwnerColumnPart,
		ValueType:  ColumnStoreValueFloat32Vector,
		VectorDims: 2,
	})
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "other_embedding_graph",
		Field:      "other_embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          1,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
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
	insertColumnGraphRebuildDualVectorRowsV2A(t, col, []columnGraphRebuildDualInputRowV2A{
		{id: "doc-a", embedding: []float32{1, 0}, otherEmbedding: []float32{0, 1}},
		{id: "doc-b", embedding: []float32{0, 1}, otherEmbedding: []float32{1, 0}},
	})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	preset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	opts := VectorIndexSearcherSearchOptions{
		Query:    []float32{0, 1},
		TopK:     2,
		EfSearch: 2,
	}
	preset.ApplyToSearcherSearchOptions(&opts)
	got, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("Search preset projection: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertSearchDocumentsHaveField1903(t, got, "other_embedding", false)
	assertSearchDocumentsHaveField1903(t, got, "embedding", true)
	assertSearchDocumentsHaveField1903(t, got, "did", true)
}

func assertSearchDocumentsHaveField1903(tb testing.TB, got VectorIndexSearchResponse, field string, want bool) {
	tb.Helper()
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) {
		tb.Fatalf("stats=%+v want one fetched document per result", got.Stats)
	}
	for i, result := range got.Results {
		var doc map[string]any
		if err := json.Unmarshal(result.Document, &doc); err != nil {
			tb.Fatalf("document[%d]=%q invalid JSON: %v", i, result.Document, err)
		}
		_, ok := doc[field]
		if ok != want {
			tb.Fatalf("document[%d]=%v field %q present=%t want %t", i, doc, field, ok, want)
		}
	}
}
