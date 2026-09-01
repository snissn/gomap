package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestInsertBatchWithStatsValidatedFloat32ProjectionOwnsAndPublishesVectors(t *testing.T) {
	dir, d, col := openValidatedFloat32ProjectionCollection(t, 3)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	vectors := [][]float32{{1, 0, 0}, {0, 1, 0}}
	ids := [][]byte{[]byte("a"), []byte("b")}
	documents := validatedFloat32ProjectionDocuments(t, ids, vectors)

	gotIDs, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids, documents, "embedding", VectorMetricCosine, vectors)
	if err != nil {
		t.Fatalf("InsertBatchWithStatsValidatedFloat32Projection: %v", err)
	}
	if len(gotIDs) != 2 || stats.ColumnPublishValidatedFloat32ProjectionRows != 2 || stats.ColumnPublishDocumentExtraction != 0 {
		t.Fatalf("ids=%q stats=%+v want two trusted rows and zero extraction", gotIDs, stats)
	}
	vectors[0][0], vectors[1][1] = 9, 9

	if _, fallback, err := col.InsertBatchWithStats(
		[][]byte{[]byte("fallback")},
		validatedFloat32ProjectionDocuments(t, [][]byte{[]byte("fallback")}, [][]float32{{0, 0, 1}}),
	); err != nil {
		t.Fatalf("ordinary InsertBatchWithStats: %v", err)
	} else if fallback.ColumnPublishValidatedFloat32ProjectionRows != 0 || fallback.ColumnPublishRows != 1 {
		t.Fatalf("ordinary insert stats=%+v want parser fallback", fallback)
	}
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(
		[][]byte{[]byte("a")},
		validatedFloat32ProjectionDocuments(t, [][]byte{[]byte("a")}, [][]float32{{1, 0, 0}}),
		"embedding", VectorMetricCosine, [][]float32{{1, 0, 0}},
	); !IsDuplicateKeyError(err) {
		t.Fatalf("duplicate error=%v want duplicate classification", err)
	}
	want := map[string][]float32{"a": {1, 0, 0}, "b": {0, 1, 0}, "fallback": {0, 0, 1}}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	search, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, 0, 0}, TopK: 1, EfSearch: 3})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(search.Results) != 1 || !bytes.Equal(search.Results[0].ID, []byte("a")) {
		t.Fatalf("search results=%+v want vector a", search.Results)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	for id := range want {
		raw, err := reopenedCol.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get(%q): %v", id, err)
		}
		if !bytes.Contains(raw, []byte(`"embedding"`)) {
			t.Fatalf("Get(%q)=%s missing canonical embedding", id, raw)
		}
	}
	reopenedSearch, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, 0, 0}, TopK: 1, EfSearch: 3})
	if err != nil {
		t.Fatalf("SearchVectorIndex reopen: %v", err)
	}
	if len(reopenedSearch.Results) != 1 || !bytes.Equal(reopenedSearch.Results[0].ID, []byte("a")) {
		t.Fatalf("reopen search results=%+v want vector a", reopenedSearch.Results)
	}
}

func TestInsertBatchWithStatsValidatedFloat32ProjectionFailsBeforeCommandWAL(t *testing.T) {
	_, d, col := openValidatedFloat32ProjectionCollection(t, 3)
	defer func() { _ = d.Close() }()
	baseLSN := d.State().AppliedCommandLSN
	document := validatedFloat32ProjectionDocuments(t, [][]byte{[]byte("bad")}, [][]float32{{1, 0, 0}})
	tests := []struct {
		name    string
		ids     [][]byte
		docs    [][]byte
		column  string
		metric  VectorMetric
		vectors [][]float32
	}{
		{name: "row count", ids: [][]byte{[]byte("bad")}, docs: document, column: "embedding", metric: VectorMetricCosine},
		{name: "column", ids: [][]byte{[]byte("bad")}, docs: document, column: "other", metric: VectorMetricCosine, vectors: [][]float32{{1, 0, 0}}},
		{name: "metric", ids: [][]byte{[]byte("bad")}, docs: document, column: "embedding", metric: VectorMetricInnerProduct, vectors: [][]float32{{1, 0, 0}}},
		{name: "dimensions", ids: [][]byte{[]byte("bad")}, docs: document, column: "embedding", metric: VectorMetricCosine, vectors: [][]float32{{1, 0}}},
		{name: "non finite", ids: [][]byte{[]byte("bad")}, docs: document, column: "embedding", metric: VectorMetricCosine, vectors: [][]float32{{1, float32(math.Inf(1)), 0}}},
		{name: "zero cosine vector", ids: [][]byte{[]byte("bad")}, docs: document, column: "embedding", metric: VectorMetricCosine, vectors: [][]float32{{0, 0, 0}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(tc.ids, tc.docs, tc.column, tc.metric, tc.vectors); err == nil {
				t.Fatal("validated projection insert succeeded")
			}
			if got := d.State().AppliedCommandLSN; got != baseLSN {
				t.Fatalf("AppliedCommandLSN=%d want unchanged %d", got, baseLSN)
			}
			if got, err := col.Get([]byte("bad")); err != nil || got != nil {
				t.Fatalf("rejected document=%s err=%v", got, err)
			}
		})
	}

	t.Run("non column collection", func(t *testing.T) {
		dir := t.TempDir()
		if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
			t.Fatalf("SaveFormatConfig: %v", err)
		}
		nonColumnDB := openCollectionCommandWALDB(t, dir)
		defer func() { _ = nonColumnDB.Close() }()
		manager := NewCollectionManager(nonColumnDB)
		if _, err := manager.CreateCollection(&CollectionMeta{Name: "plain", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		nonColumn, err := manager.OpenCollection("plain")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		lsn := nonColumnDB.State().AppliedCommandLSN
		if _, _, err := nonColumn.InsertBatchWithStatsValidatedFloat32Projection(
			[][]byte{[]byte("bad")}, document, "embedding", VectorMetricCosine, [][]float32{{1, 0, 0}},
		); err == nil {
			t.Fatal("validated projection insert succeeded")
		}
		if got := nonColumnDB.State().AppliedCommandLSN; got != lsn {
			t.Fatalf("AppliedCommandLSN=%d want unchanged %d", got, lsn)
		}
		if got, err := nonColumn.Get([]byte("bad")); err != nil || got != nil {
			t.Fatalf("rejected document=%s err=%v", got, err)
		}
	})
}

func TestValidateTrustedFloat32ProjectionMetaRejectsSchemaDrift(t *testing.T) {
	base := CollectionMeta{
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{{
				Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3,
			}},
			RetainedPayload:         ColumnRetainedPayloadFull,
			RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		}},
		VectorIndexes: []VectorIndexDefinition{{Field: "embedding", Dimensions: 3, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}},
	}
	projection, err := newTrustedFloat32Projection([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0,0]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 0, 0}})
	if err != nil {
		t.Fatalf("newTrustedFloat32Projection: %v", err)
	}
	documents := []columnWriteDocument{{ID: []byte("a"), Document: []byte(`{"embedding":[1,0,0]}`)}}
	tests := []struct {
		name   string
		mutate func(*CollectionMeta)
	}{
		{name: "disabled", mutate: func(meta *CollectionMeta) { meta.Options.ColumnStore.Enabled = false }},
		{name: "no column config", mutate: func(meta *CollectionMeta) { meta.Options.ColumnStore = nil }},
		{name: "retained payload none", mutate: func(meta *CollectionMeta) {
			meta.Options.ColumnStore.RetainedPayload = ColumnRetainedPayloadNone
			meta.Options.ColumnStore.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingNone
		}},
		{name: "path", mutate: func(meta *CollectionMeta) { meta.Options.ColumnStore.Columns[0].Path = "other" }},
		{name: "type", mutate: func(meta *CollectionMeta) { meta.Options.ColumnStore.Columns[0].ValueType = ColumnStoreValueDouble }},
		{name: "owner", mutate: func(meta *CollectionMeta) { meta.Options.ColumnStore.Columns[0].Owner = TypedStorageOwnerRowAsset }},
		{name: "extra column", mutate: func(meta *CollectionMeta) {
			meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, ColumnStoreColumn{Name: "other", Path: "other", ValueType: ColumnStoreValueString})
		}},
		{name: "graph field", mutate: func(meta *CollectionMeta) { meta.VectorIndexes[0].Field = "other" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := copyCollectionMeta(base)
			tc.mutate(&meta)
			if err := validateTrustedFloat32ProjectionMeta(meta, projection); err == nil {
				t.Fatal("schema drift accepted")
			}
		})
	}
	if err := validateTrustedFloat32ProjectionMeta(base, projection); err != nil {
		t.Fatalf("matching metadata rejected: %v", err)
	}
	if err := applyTrustedFloat32Projection([][]byte{[]byte("a")}, documents, projection); err != nil {
		t.Fatalf("applyTrustedFloat32Projection: %v", err)
	}
}

func TestColumnRetainedPayloadJSONTopLevelColumn(t *testing.T) {
	cfg := ColumnStoreConfig{
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Columns:         []ColumnStoreColumn{{Name: "embedding", Path: "embedding"}},
	}
	for _, tc := range []struct {
		name     string
		document string
		want     string
		wantErr  bool
	}{
		{
			name:     "large declared array omitted",
			document: `{"content":"kept","embedding":[1,2,3],"meta":{"row":1,"source":"test"}}`,
			want:     `{"content":"kept","meta":{"row":1,"source":"test"}}`,
		},
		{
			name:     "duplicate declared member omitted",
			document: `{"embedding":[1],"embedding":[2],"content":"kept"}`,
			want:     `{"content":"kept"}`,
		},
		{name: "malformed fails closed", document: `{"embedding":[1]`, wantErr: true},
		{name: "non-object fails closed", document: `null`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := columnRetainedPayloadJSONFromJSONDocument(cfg, []byte(tc.document))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("columnRetainedPayloadJSONFromJSONDocument() error=nil want failure; retained=%s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("columnRetainedPayloadJSONFromJSONDocument: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("retained=%s want %s", got, tc.want)
			}
		})
	}

	nested := cfg
	nested.Columns = []ColumnStoreColumn{{Name: "embedding_values", Path: "embedding.values"}}
	got, err := columnRetainedPayloadJSONFromJSONDocument(nested, []byte(`{"embedding":{"values":[1,2],"keep":3},"content":"kept"}`))
	if err != nil {
		t.Fatalf("nested columnRetainedPayloadJSONFromJSONDocument: %v", err)
	}
	if want := `{"content":"kept","embedding":{"keep":3}}`; string(got) != want {
		t.Fatalf("nested retained=%s want %s", got, want)
	}
}

func openValidatedFloat32ProjectionCollection(t testing.TB, dims int) (string, *backenddb.DB, *Collection) {
	return openValidatedFloat32ProjectionCollectionWithPolicy(t, dims, ColumnRetainedPayloadNonColumn)
}

func openValidatedFloat32ProjectionCollectionWithPolicy(t testing.TB, dims int, retainedPayload ColumnRetainedPayloadPolicy) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatJSON,
			DisableBufferedIndexedAsyncFlush: true,
			ColumnStore: &ColumnStoreConfig{
				Enabled:                 true,
				Columns:                 []ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: dims}},
				RetainedPayload:         retainedPayload,
				RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			},
		},
		VectorIndexes: []VectorIndexDefinition{{Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dims, M: 2, Strategy: VectorIndexStrategyColumnGraph}},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	return dir, d, col
}

func BenchmarkInsertBatchValidatedFloat32ProjectionCohere768RetainedPayload(b *testing.B) {
	const rowsPerBatch = 32
	for _, policy := range []ColumnRetainedPayloadPolicy{ColumnRetainedPayloadFull, ColumnRetainedPayloadNonColumn} {
		policy := policy
		b.Run(string(policy)+"_json", func(b *testing.B) {
			_, d, col := openValidatedFloat32ProjectionCollectionWithPolicy(b, 768, policy)
			defer func() { _ = d.Close() }()
			batches := make([]struct {
				ids       [][]byte
				documents [][]byte
				vectors   [][]float32
			}, b.N)
			var inputBytes int64
			for batch := range batches {
				batches[batch].ids = make([][]byte, rowsPerBatch)
				batches[batch].documents = make([][]byte, rowsPerBatch)
				batches[batch].vectors = make([][]float32, rowsPerBatch)
				for row := range rowsPerBatch {
					id := fmt.Sprintf("cohere-%d-%d", batch, row)
					vector := make([]float32, 768)
					for dim := range vector {
						vector[dim] = float32(((batch*rowsPerBatch+row+dim)%127)-63) / 64
					}
					raw, err := json.Marshal(map[string]any{
						"id": id, "content": "cohere shaped retained payload", "embedding": vector,
						"meta": map[string]any{"source": "benchmark", "row": batch*rowsPerBatch + row},
					})
					if err != nil {
						b.Fatalf("marshal document: %v", err)
					}
					batches[batch].ids[row] = []byte(id)
					batches[batch].documents[row] = raw
					batches[batch].vectors[row] = vector
					if batch == 0 {
						inputBytes += int64(len(raw))
					}
				}
			}
			cfg := col.Meta().Options.ColumnStore.copy()
			var retainedBytes int64
			for _, document := range batches[0].documents {
				retained, err := ColumnRetainedPayloadFromJSONDocument(cfg, document)
				if err != nil {
					b.Fatalf("prepare retained payload: %v", err)
				}
				retainedBytes += int64(len(retained))
			}
			b.ReportAllocs()
			b.SetBytes(inputBytes)
			var retainedPrepare time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(
					batches[i].ids, batches[i].documents, "embedding", VectorMetricCosine, batches[i].vectors,
				)
				if err != nil {
					b.Fatalf("InsertBatchWithStatsValidatedFloat32Projection: %v", err)
				}
				retainedPrepare += stats.RetainedPayloadPrepare
			}
			b.StopTimer()
			if err := col.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
			if b.N > 0 {
				b.ReportMetric(float64(retainedPrepare.Nanoseconds())/float64(b.N), "retained_prepare_ns/op")
			}
			b.ReportMetric(float64(inputBytes), "input_B/op")
			b.ReportMetric(float64(retainedBytes), "retained_B/op")
			b.ReportMetric(float64(rowsPerBatch*768*4), "typed_column_B/op")
		})
	}
}

func validatedFloat32ProjectionDocuments(t testing.TB, ids [][]byte, vectors [][]float32) [][]byte {
	t.Helper()
	documents := make([][]byte, len(ids))
	for i := range ids {
		raw, err := json.Marshal(map[string]any{"id": string(ids[i]), "embedding": vectors[i]})
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		documents[i] = raw
	}
	return documents
}
