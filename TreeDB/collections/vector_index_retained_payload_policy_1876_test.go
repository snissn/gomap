package collections

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type retainedPayloadPolicyFixtureShape1876 struct {
	rows      int
	dims      int
	m         int
	topK      int
	efSearch  int
	bodyBytes int
}

type retainedPayloadPolicySearchMode1876 struct {
	name string
	opts DocumentFetchOptions
}

type retainedPayloadPolicyFixture1876 struct {
	dir                  string
	db                   *backenddb.DB
	col                  *Collection
	def                  VectorIndexDefinition
	shape                retainedPayloadPolicyFixtureShape1876
	policy               ColumnRetainedPayloadPolicy
	input                []columnGraphRebuildInputRowV2A
	docs                 [][]byte
	query                []float32
	inputDocumentBytes   int64
	retainedPayloadBytes int64
	storage              retainedPayloadPolicyStorage1876
}

type retainedPayloadPolicyStorage1876 struct {
	columnAssetBytes int64
	graphAssetBytes  int64
	dbDirBytes       int64
}

func TestSearchVectorIndexColumnGraphRetainedPayloadPolicyMatrix1876(t *testing.T) {
	shape := retainedPayloadPolicyFixtureShape1876{rows: 12, dims: 6, m: 4, topK: 3, efSearch: 12, bodyBytes: 64}
	for _, policy := range retainedPayloadPolicies1876() {
		policy := policy
		t.Run(retainedPayloadPolicyName1876(policy), func(t *testing.T) {
			fixture := openRetainedPayloadPolicyFixture1876(t, shape, policy, false)
			defer func() { _ = fixture.db.Close() }()
			for _, mode := range retainedPayloadSearchModes1876(t, fixture.def) {
				mode := mode
				t.Run(mode.name, func(t *testing.T) {
					got, err := fixture.col.SearchVectorIndex(VectorIndexSearchOptions{
						IndexName:            fixture.def.Name,
						Query:                fixture.query,
						TopK:                 fixture.shape.topK,
						EfSearch:             fixture.shape.efSearch,
						IncludeDocuments:     true,
						DocumentFetchOptions: mode.opts,
						MaxDecodedBlocks:     1,
					})
					if err != nil {
						t.Fatalf("SearchVectorIndex: %v", err)
					}
					assertColumnGraphSearchResponseLoadedV4(t, got, fixture.def.Name, fixture.shape.topK)
					assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, fixture.input, fixture.query, fixture.shape.topK), true)
					assertRetainedPayloadPolicySearchDocuments1876(t, got, policy, mode, fixture.docs)
				})
			}
		})
	}
}

func TestSearchVectorIndexColumnGraphRetainedFullDocumentsReopenValueLogPointers1876(t *testing.T) {
	shape := retainedPayloadPolicyFixtureShape1876{rows: 16, dims: 8, m: 4, topK: 3, efSearch: 16, bodyBytes: 96}
	fixture := openRetainedPayloadPolicyFixture1876(t, shape, ColumnRetainedPayloadFull, true)
	if err := fixture.db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openRetainedPayloadPolicyDB1876(t, fixture.dir, true)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        fixture.def.Name,
		Query:            fixture.query,
		TopK:             fixture.shape.topK,
		EfSearch:         fixture.shape.efSearch,
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex reopen: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, fixture.def.Name, fixture.shape.topK)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, fixture.input, fixture.query, fixture.shape.topK), true)
	assertRetainedPayloadPolicySearchDocuments1876(t, got, ColumnRetainedPayloadFull, retainedPayloadPolicySearchMode1876{name: "full_documents_comparison"}, fixture.docs)
	if got.Stats.DocumentRetainedBytes == 0 || got.Stats.DocumentRowRefUnsupported == 0 {
		t.Fatalf("stats=%+v want retained-full fetch from primary value-log-backed payload after reopen", got.Stats)
	}
}

func BenchmarkOpenVectorIndexSearcherColumnGraphRetainedPayloadPolicy1876(b *testing.B) {
	// Issue #1876 retained-payload policy matrix fixture shape:
	// 1024 rows, 128-dim vectors, topK=10, efSearch=128, with a non-column
	// retained body plus retained_tag so ColumnRetainedPayloadNone storage and
	// semantics are visible in final document fetch results and byte accounting.
	shape := retainedPayloadPolicyFixtureShape1876{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, bodyBytes: 192}
	for _, policy := range retainedPayloadPolicies1876() {
		policy := policy
		b.Run(retainedPayloadPolicyName1876(policy), func(b *testing.B) {
			fixture := openRetainedPayloadPolicyFixture1876(b, shape, policy, false)
			defer func() { _ = fixture.db.Close() }()
			for _, mode := range retainedPayloadSearchModes1876(b, fixture.def) {
				mode := mode
				b.Run(mode.name, func(b *testing.B) {
					benchmarkOpenVectorIndexSearcherRetainedPayloadPolicy1876(b, fixture, mode)
				})
			}
		})
	}
}

func BenchmarkOpenVectorIndexSearcherProjectionOrientedFetchPreset1903(b *testing.B) {
	// Issue #1903 focused helper route: compare manual #1876 preferred projection
	// construction with the named preset over the same non-column retained-payload
	// fixture. Preset construction is intentionally outside the timed search loop.
	shape := retainedPayloadPolicyFixtureShape1876{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, bodyBytes: 192}
	for _, tc := range []struct {
		name      string
		configure func(VectorIndexDefinition) (VectorIndexSearcherSearchOptions, error)
	}{
		{
			name: "manual_non_column_exclude_embedding",
			configure: func(VectorIndexDefinition) (VectorIndexSearcherSearchOptions, error) {
				return VectorIndexSearcherSearchOptions{IncludeDocuments: true, DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}}}, nil
			},
		},
		{
			name: "preset_non_column_exclude_embedding",
			configure: func(def VectorIndexDefinition) (VectorIndexSearcherSearchOptions, error) {
				preset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
				if err != nil {
					return VectorIndexSearcherSearchOptions{}, err
				}
				var opts VectorIndexSearcherSearchOptions
				preset.ApplyToSearcherSearchOptions(&opts)
				return opts, nil
			},
		},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherProjectionOrientedFetchPreset1903(b, shape, tc.configure)
		})
	}
}

func benchmarkOpenVectorIndexSearcherProjectionOrientedFetchPreset1903(b *testing.B, shape retainedPayloadPolicyFixtureShape1876, configure func(VectorIndexDefinition) (VectorIndexSearcherSearchOptions, error)) {
	b.Helper()
	fixture := openRetainedPayloadPolicyFixture1876(b, shape, ColumnRetainedPayloadNonColumn, false)
	defer func() { _ = fixture.db.Close() }()
	searcher, err := fixture.col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	opts, err := configure(fixture.def)
	if err != nil {
		b.Fatalf("configure preset: %v", err)
	}
	opts.Query = fixture.query
	opts.TopK = fixture.shape.topK
	opts.EfSearch = fixture.shape.efSearch
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measured, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search: %v", err)
	}
	stats := measured.Stats
	b.ReportMetric(float64(stats.DocumentsFetched), "docs/search")
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
	reportRetainedPayloadPolicyFixtureMetrics1876(b, fixture, retainedPayloadPolicySearchMode1876{name: "preferred_exclude_embedding", opts: opts.DocumentFetchOptions})
}

func benchmarkOpenVectorIndexSearcherRetainedPayloadPolicy1876(b *testing.B, fixture retainedPayloadPolicyFixture1876, mode retainedPayloadPolicySearchMode1876) {
	b.Helper()
	searcher, err := fixture.col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	opts := VectorIndexSearcherSearchOptions{
		Query:                fixture.query,
		TopK:                 fixture.shape.topK,
		EfSearch:             fixture.shape.efSearch,
		IncludeDocuments:     true,
		DocumentFetchOptions: mode.opts,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measured, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search: %v", err)
	}
	stats := measured.Stats
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
	reportRetainedPayloadPolicyFixtureMetrics1876(b, fixture, mode)
}

func retainedPayloadPolicies1876() []ColumnRetainedPayloadPolicy {
	return []ColumnRetainedPayloadPolicy{
		ColumnRetainedPayloadNonColumn,
		ColumnRetainedPayloadFull,
		ColumnRetainedPayloadNone,
	}
}

func retainedPayloadSearchModes1876(tb testing.TB, def VectorIndexDefinition) []retainedPayloadPolicySearchMode1876 {
	tb.Helper()
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		tb.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	return []retainedPayloadPolicySearchMode1876{
		{name: "preferred_exclude_embedding", opts: fetchPreset.DocumentFetchOptions},
		{name: "full_documents_comparison"},
	}
}

func retainedPayloadPolicyName1876(policy ColumnRetainedPayloadPolicy) string {
	switch policy {
	case ColumnRetainedPayloadNonColumn:
		return "non_column"
	case ColumnRetainedPayloadFull:
		return "full"
	case ColumnRetainedPayloadNone:
		return "none"
	default:
		return string(policy)
	}
}

func openRetainedPayloadPolicyFixture1876(tb testing.TB, shape retainedPayloadPolicyFixtureShape1876, policy ColumnRetainedPayloadPolicy, forceValueLogPointers bool) retainedPayloadPolicyFixture1876 {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 || shape.topK <= 0 || shape.topK > shape.rows {
		tb.Fatalf("invalid fixture shape: %+v", shape)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openRetainedPayloadPolicyDB1876(tb, dir, forceValueLogPointers)
	def := columnGraphRebuildVectorIndexDefinitionV2A(shape.dims, shape.m)
	cfg := columnGraphRebuildColumnStoreConfigV2A(shape.dims)
	cfg.RetainedPayload = policy
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	input, ids, docs, inputBytes := retainedPayloadPolicyRowsAndDocuments1876(tb, shape)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	storage, err := collectRetainedPayloadPolicyStorage1876(col, dir, def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("collect storage: %v", err)
	}
	retainedBytes := retainedPayloadPolicyBytes1876(tb, *cfg, docs)
	queryOrdinal := 37
	if queryOrdinal >= len(input) {
		queryOrdinal = len(input) / 2
	}
	return retainedPayloadPolicyFixture1876{
		dir:                  dir,
		db:                   d,
		col:                  col,
		def:                  def,
		shape:                shape,
		policy:               policy,
		input:                input,
		docs:                 docs,
		query:                append([]float32(nil), input[queryOrdinal].vector...),
		inputDocumentBytes:   inputBytes,
		retainedPayloadBytes: retainedBytes,
		storage:              storage,
	}
}

func openRetainedPayloadPolicyDB1876(tb testing.TB, dir string, forceValueLogPointers bool) *backenddb.DB {
	tb.Helper()
	opts := backenddb.Options{Dir: dir, DisableBackgroundPrune: true}
	if forceValueLogPointers {
		opts.ValueLog = backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}
	}
	d, err := backenddb.Open(opts)
	if err != nil {
		tb.Fatalf("Open DB: %v", err)
	}
	return d
}

func retainedPayloadPolicyRowsAndDocuments1876(tb testing.TB, shape retainedPayloadPolicyFixtureShape1876) ([]columnGraphRebuildInputRowV2A, [][]byte, [][]byte, int64) {
	tb.Helper()
	input := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	ids := make([][]byte, len(input))
	docs := make([][]byte, len(input))
	var total int64
	for i, row := range input {
		bodyUnit := fmt.Sprintf("body-%04d-%02d|", i, i%17)
		body := strings.Repeat(bodyUnit, shape.bodyBytes/len(bodyUnit)+1)
		body = body[:shape.bodyBytes]
		raw, err := json.Marshal(map[string]any{
			"time_us":      int64(i + 1),
			"kind":         fmt.Sprintf("vector-%02d", i%8),
			"did":          row.id,
			"embedding":    row.vector,
			"body":         body,
			"retained_tag": fmt.Sprintf("retained-%02d", i%13),
		})
		if err != nil {
			tb.Fatalf("json.Marshal row %q: %v", row.id, err)
		}
		ids[i] = []byte(row.id)
		docs[i] = raw
		total += int64(len(raw))
	}
	return input, ids, docs, total
}

func retainedPayloadPolicyBytes1876(tb testing.TB, cfg ColumnStoreConfig, docs [][]byte) int64 {
	tb.Helper()
	var total int64
	for i, doc := range docs {
		retained, err := columnRetainedPayloadFromJSONDocument(cfg, doc)
		if err != nil {
			tb.Fatalf("columnRetainedPayloadFromJSONDocument row %d: %v", i, err)
		}
		total += int64(len(retained))
	}
	return total
}

func collectRetainedPayloadPolicyStorage1876(col *Collection, dir, indexName string) (retainedPayloadPolicyStorage1876, error) {
	var storage retainedPayloadPolicyStorage1876
	dbDirBytes, err := dirSize(dir)
	if err != nil {
		return storage, err
	}
	storage.dbDirBytes = dbDirBytes
	if col == nil || col.db == nil {
		return storage, errCollectionNil
	}
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		return storage, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		return storage, err
	}
	if catalog == nil || catalog.meta.Options.ColumnStore == nil {
		return storage, nil
	}
	cfg := catalog.meta.Options.ColumnStore
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return storage, err
	}
	manifest, _, _, err := decodeColumnManifestSnapshotViewForScan(records, cfg.AssetManager.Namespace)
	if err != nil {
		return storage, err
	}
	for _, part := range manifest.Parts {
		storage.columnAssetBytes += part.Bytes
	}
	for _, metadata := range manifest.AggregateMetadata {
		storage.columnAssetBytes += metadata.Bytes
	}
	for _, dictionary := range manifest.DictionaryCodes {
		storage.columnAssetBytes += dictionary.Bytes
	}
	for _, values := range manifest.Int64Values {
		storage.columnAssetBytes += values.Bytes
	}
	if graphRecord, ok := findColumnVectorGraphManifestRecord(records, indexName); ok {
		graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
		if err != nil {
			return storage, err
		}
		storage.graphAssetBytes = graph.AssetBytes
	}
	return storage, nil
}

func assertRetainedPayloadPolicySearchDocuments1876(tb testing.TB, got VectorIndexSearchResponse, policy ColumnRetainedPayloadPolicy, mode retainedPayloadPolicySearchMode1876, sourceDocs [][]byte) {
	tb.Helper()
	projected := documentFetchOptionsHasProjection(mode.opts)
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) || got.Stats.DocumentOutputBytes == 0 {
		tb.Fatalf("stats=%+v want document fetch/output accounting", got.Stats)
	}
	if projected && got.Stats.DocumentFieldsSkipped == 0 {
		tb.Fatalf("projected stats=%+v want skipped embedding field", got.Stats)
	}
	switch policy {
	case ColumnRetainedPayloadFull:
		if got.Stats.DocumentRowRefUnsupported == 0 || got.Stats.DocumentPointRowFetches != 0 || got.Stats.DocumentJSONReconstructionRows != 0 {
			tb.Fatalf("retained-full stats=%+v want primary retained-payload fetch fallback", got.Stats)
		}
	case ColumnRetainedPayloadNonColumn, ColumnRetainedPayloadNone:
		if got.Stats.DocumentRowRefUnsupported != 0 || got.Stats.DocumentPointRowFetches != uint64(len(got.Results)) || got.Stats.DocumentJSONReconstructionRows != uint64(len(got.Results)) {
			tb.Fatalf("reconstructing stats=%+v want row-ref point fetch and JSON reconstruction per result", got.Stats)
		}
	}
	if policy == ColumnRetainedPayloadNone && got.Stats.DocumentRetainedBytes != uint64(2*len(got.Results)) {
		tb.Fatalf("retained-none bytes=%d want {} per result", got.Stats.DocumentRetainedBytes)
	}
	expectedByID := retainedPayloadPolicyExpectedDocsByID1876(tb, sourceDocs)
	for i, result := range got.Results {
		var doc map[string]any
		if err := json.Unmarshal(result.Document, &doc); err != nil {
			tb.Fatalf("document[%d]=%q invalid JSON: %v", i, result.Document, err)
		}
		id := string(result.ID)
		expected, ok := expectedByID[id]
		if !ok {
			tb.Fatalf("document[%d] id=%q missing expected source document", i, id)
		}
		if doc["did"] != expected["did"] || doc["did"] != id {
			tb.Fatalf("document[%d]=%v want did=%q", i, doc, id)
		}
		if doc["kind"] != expected["kind"] || doc["time_us"] != expected["time_us"] {
			tb.Fatalf("document[%d]=%v want kind=%v time_us=%v", i, doc, expected["kind"], expected["time_us"])
		}
		_, hasEmbedding := doc["embedding"]
		if projected && hasEmbedding {
			tb.Fatalf("projected document[%d]=%v retained embedding", i, doc)
		}
		if !projected && !reflect.DeepEqual(doc["embedding"], expected["embedding"]) {
			tb.Fatalf("full document[%d] embedding mismatch", i)
		}
		_, hasBody := doc["body"]
		_, hasRetainedTag := doc["retained_tag"]
		if policy == ColumnRetainedPayloadNone {
			if hasBody || hasRetainedTag {
				tb.Fatalf("retained-none document[%d]=%v retained non-column fields", i, doc)
			}
		} else if doc["body"] != expected["body"] || doc["retained_tag"] != expected["retained_tag"] {
			tb.Fatalf("policy %q document[%d]=%v want body/tag from source row", policy, i, doc)
		}
	}
}

func retainedPayloadPolicyExpectedDocsByID1876(tb testing.TB, docs [][]byte) map[string]map[string]any {
	tb.Helper()
	expectedByID := make(map[string]map[string]any, len(docs))
	for i, raw := range docs {
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			tb.Fatalf("source document[%d]=%q invalid JSON: %v", i, raw, err)
		}
		id, ok := doc["did"].(string)
		if !ok || id == "" {
			tb.Fatalf("source document[%d]=%v missing did", i, doc)
		}
		expectedByID[id] = doc
	}
	return expectedByID
}

func reportRetainedPayloadPolicyFixtureMetrics1876(b *testing.B, fixture retainedPayloadPolicyFixture1876, mode retainedPayloadPolicySearchMode1876) {
	b.Helper()
	b.ReportMetric(float64(fixture.shape.rows), "rows")
	b.ReportMetric(float64(fixture.shape.dims), "dims")
	b.ReportMetric(float64(fixture.shape.topK), "top_k")
	b.ReportMetric(float64(fixture.shape.efSearch), "ef_search")
	b.ReportMetric(float64(fixture.shape.bodyBytes), "retained_body_B/doc")
	if documentFetchOptionsHasProjection(mode.opts) {
		b.ReportMetric(1, "exclude_embedding_projection")
	}
	b.ReportMetric(float64(fixture.retainedPayloadBytes), "retained_payload_B_total")
	if fixture.shape.rows > 0 {
		retainedPayloadBytesPerDoc := float64(fixture.retainedPayloadBytes) / float64(fixture.shape.rows)
		b.ReportMetric(retainedPayloadBytesPerDoc, "retained_payload_B/doc")
		b.ReportMetric(retainedPayloadBytesPerDoc, "retained_payload_B_total/per_doc")
	}
	b.ReportMetric(float64(fixture.inputDocumentBytes), "input_doc_B_total")
	b.ReportMetric(float64(fixture.storage.columnAssetBytes), "column_asset_B_total")
	b.ReportMetric(float64(fixture.storage.graphAssetBytes), "graph_asset_B_total")
	b.ReportMetric(float64(fixture.storage.dbDirBytes), "db_dir_B_total")
	if fixture.inputDocumentBytes > 0 {
		b.ReportMetric(float64(fixture.storage.dbDirBytes)/float64(fixture.inputDocumentBytes), "write_amplification")
	}
}
