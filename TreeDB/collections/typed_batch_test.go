package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedMinimaAmbiguousMutationNotRetried(t *testing.T) {
	if !isRetriableCollectionMutationError(ErrConcurrentMutation) {
		t.Fatal("ordinary conflict must remain retryable")
	}
	if isRetriableCollectionMutationError(commitAmbiguousError("typed publication", ErrConcurrentMutation)) {
		t.Fatal("ambiguous mutation must not be retried")
	}
}

func TestTypedMinimaCatalogPostSyncAmbiguity(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	d := openTypedMinimaDB(t, dir)
	injected := errors.New("catalog post-sync cut")
	var fired atomic.Bool
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync && fired.CompareAndSwap(false, true) {
			return injected
		}
		return nil
	})
	meta := typedMinimaCollectionMeta()
	_, err := NewCollectionManager(d).CreateCollection(&meta)
	restore()
	if !fired.Load() || !errors.Is(err, ErrCommitAmbiguous) || !errors.Is(err, injected) {
		t.Fatalf("catalog cut fired=%t err=%v", fired.Load(), err)
	}
	_ = d.Close()
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	if _, err := NewCollectionManager(reopened).OpenCollection("minima"); err != nil {
		t.Fatal(err)
	}
}

func TestTypedMinimaCrashAndPublicationCuts(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_MINIMA_CRASH_DIR"); dir != "" {
		d := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(d).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("GOMAP_TYPED_MINIMA_CRASH_MODE")
		injected := errors.New("typed command WAL injected failure")
		var fired atomic.Bool
		ids := [][]byte{[]byte("b"), []byte("a")}
		retained := [][]byte{[]byte(`{"id":"b"}`), []byte(`{"id":"a"}`)}
		columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha", "beta"}}, {Name: "user", Strings: []string{"u1", "u2"}}, {Name: "path", Strings: []string{"file1", "file2"}}}
		if strings.HasPrefix(mode, "replace_") || strings.HasPrefix(mode, "delete_") {
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if mode != "ack" {
			point := durabilitycut.BeforeDependencyAppend
			if strings.HasSuffix(mode, "after_sync") {
				point = durabilitycut.AfterDependencyFileSync
			}
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point && fired.CompareAndSwap(false, true) {
					return injected
				}
				return nil
			})
		}
		switch mode {
		case "replace_after_sync":
			columns[1].Strings[0] = "gamma"
			_, err = col.ReplaceTypedBatch(ids, retained, columns)
		case "delete_after_sync":
			_, err = col.DeleteBatch(ids)
		default:
			_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
		}
		if mode == "ack" {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if !fired.Load() || !errors.Is(err, injected) {
				t.Fatalf("cut fired=%t err=%v", fired.Load(), err)
			}
			if strings.HasSuffix(mode, "after_sync") && !errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("post-sync error is not commit ambiguous: %v", err)
			}
			if mode == "before_append" && errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("preappend error is ambiguous: %v", err)
			}
		}
		os.Exit(0) // Deliberately bypass Close, Flush and all deferred cleanup.
	}
	for _, mode := range []string{"ack", "before_append", "after_sync", "replace_after_sync", "delete_after_sync"} {
		t.Run(mode, func(t *testing.T) {
			dir, d, _ := openTypedMinimaCollection(t)
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedMinimaCrashAndPublicationCuts$")
			cmd.Env = append(os.Environ(), "GOMAP_TYPED_MINIMA_CRASH_DIR="+dir, "GOMAP_TYPED_MINIMA_CRASH_MODE="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			reopened := openTypedMinimaDB(t, dir)
			defer reopened.Close()
			col, err := NewCollectionManager(reopened).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "replace_after_sync" {
				raw, err := col.Get([]byte("b"))
				if err != nil || !strings.Contains(string(raw), `"content":"gamma"`) {
					t.Fatalf("replacement output: %s %v", raw, err)
				}
				for _, query := range []string{"alpha", "gamma"} {
					found, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: query, TopK: 2})
					want := 0
					if query == "gamma" {
						want = 1
					}
					if err != nil || len(found.Results) != want {
						t.Fatalf("replacement text %s %+v %v", query, found, err)
					}
				}
				return
			}
			if mode != "before_append" && mode != "delete_after_sync" {
				assertTypedMinimaRows(t, col)
				return
			}
			for _, id := range []string{"a", "b"} {
				if doc, err := col.Get([]byte(id)); err != nil || doc != nil {
					t.Fatal("preappend failure installed document")
				}
			}
			found, err := col.FindByIndex("user", "u1")
			if err != nil || len(found) != 0 {
				t.Fatalf("partial scalar state: %q %v", found, err)
			}
			text, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 1})
			if err != nil || len(text.Results) != 0 {
				t.Fatalf("partial text state: %+v %v", text, err)
			}
		})
	}
}

func TestTypedMinimaUnsupportedScalarSibling(t *testing.T) {
	_, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	for _, columnOwner := range []bool{false, true} {
		meta := col.Meta()
		cfg := meta.Options.ColumnStore.copy()
		meta.Options.ColumnStore = &cfg
		if columnOwner {
			cfg.Columns[1].Owner = TypedStorageOwnerColumnPart
		} else {
			cfg.Columns[1].Nullable = true
		}
		if columnStoreTypedScalarIndexesSupported(meta) {
			t.Fatal("accepted unsupported old-state schema")
		}
		for _, op := range []ColumnPublishOperation{ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete} {
			if err := requireColumnStoreWriteOperationSupported(meta, op); err == nil {
				t.Fatalf("accepted unsupported sibling operation %v", op)
			}
		}
	}
}

func TestTypedMinimaVectorOnlyReplay(t *testing.T) {
	for _, mode := range []string{"no_index", "aliased_path", "multiple_indexes"} {
		t.Run(mode, func(t *testing.T) {
			meta := typedMinimaCollectionMeta()
			meta.Options.ColumnStore.Columns = meta.Options.ColumnStore.Columns[:1]
			meta.Indexes, meta.TextIndexes = nil, nil
			switch mode {
			case "no_index":
				meta.VectorIndexes = nil
			case "aliased_path":
				meta.Options.ColumnStore.Columns[0].Name = "vector"
			case "multiple_indexes":
				other := meta.VectorIndexes[0]
				other.Name = "other_graph"
				meta.VectorIndexes = append(meta.VectorIndexes, other)
			}
			dir, d, col := openTypedMinimaCollectionMeta(t, meta)
			defer d.Close()
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			replayDir := t.TempDir()
			copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
			_, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, []TypedColumnBatch{{Name: meta.Options.ColumnStore.Columns[0].Name, Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}})
			if err != nil {
				t.Fatal(err)
			}
			for _, frame := range collectionCommandWALFrames(t, dir) {
				if frame.LSN > 1 {
					writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
				}
			}
			reopened := openTypedMinimaDB(t, replayDir)
			defer reopened.Close()
			replayed, err := NewCollectionManager(reopened).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []*Collection{col, replayed} {
				raw, err := c.Get([]byte("a"))
				if err != nil || !strings.Contains(string(raw), `"embedding":[1,0,0,0,0,0,0,0]`) {
					t.Fatalf("typed vector output %s err=%v", raw, err)
				}
			}
		})
	}
}

func TestTypedMinimaReplaySemanticValidation(t *testing.T) {
	_, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	ids, retained := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
	p, err := newTrustedTypedProjection(col.Meta(), ids, retained, columns)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*commitlog.CollectionTypedBatchPayload){
		"schema_hash": func(p *commitlog.CollectionTypedBatchPayload) { p.SchemaHash++ },
		"collection":  func(p *commitlog.CollectionTypedBatchPayload) { p.Collection = "other" },
		"utf8":        func(p *commitlog.CollectionTypedBatchPayload) { p.Documents[0].Values[0].String = string([]byte{255}) },
		"zero_cosine": func(p *commitlog.CollectionTypedBatchPayload) { p.Documents[0].Values[1].Vector = make([]float32, 8) },
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := typedCommandPayload(col.Meta(), []commitlog.CollectionDocument{{ID: ids[0], Document: retained[0]}}, p)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&payload)
			encoded, err := commitlog.EncodeCollectionTypedBatchPayload(payload)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := commitlog.DecodeCollectionTypedBatchPayload(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := typedProjectionFromPayload(col.Meta(), decoded); err == nil {
				t.Fatal("accepted invalid replay values")
			}
		})
	}
}

func TestTypedMinimaAdmissionValidation(t *testing.T) {
	_, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	type input struct {
		meta          CollectionMeta
		ids, retained [][]byte
		columns       []TypedColumnBatch
	}
	for name, mutate := range map[string]func(*input){
		"retained_count":   func(in *input) { in.retained = nil },
		"column_count":     func(in *input) { in.columns = in.columns[:3] },
		"unknown":          func(in *input) { in.columns[0].Name = "unknown" },
		"duplicate_column": func(in *input) { in.columns[1].Name = "embedding" },
		"mixed_carrier":    func(in *input) { in.columns[0].Strings = []string{"wrong"} },
		"wrong_type":       func(in *input) { in.columns[1].Strings = nil; in.columns[1].Float32Vectors = [][]float32{{1}} },
		"string_count":     func(in *input) { in.columns[1].Strings = nil },
		"vector_count":     func(in *input) { in.columns[0].Float32Vectors = nil },
		"dimension":        func(in *input) { in.columns[0].Float32Vectors[0] = []float32{1} },
		"huge_dimensions": func(in *input) {
			in.meta.Options.ColumnStore.Columns[0].VectorDims = int(^uint(0) >> 1)
			in.meta.VectorIndexes[0].Dimensions = int(^uint(0) >> 1)
		},
		"nan":                         func(in *input) { in.columns[0].Float32Vectors[0][0] = float32(math.NaN()) },
		"infinity":                    func(in *input) { in.columns[0].Float32Vectors[0][0] = float32(math.Inf(1)) },
		"zero_cosine":                 func(in *input) { in.columns[0].Float32Vectors[0][0] = 0 },
		"invalid_utf8":                func(in *input) { in.columns[1].Strings[0] = string([]byte{255}) },
		"empty_id":                    func(in *input) { in.ids[0] = nil },
		"duplicate_id":                func(in *input) { in.ids = append(in.ids, in.ids[0]); in.retained = append(in.retained, in.retained[0]) },
		"retained_null":               func(in *input) { in.retained[0] = []byte(`null`) },
		"retained_declared_null":      func(in *input) { in.retained[0] = []byte(`{"id":"a","content":null}`) },
		"retained_ancestor_null":      func(in *input) { in.retained[0] = []byte(`{"id":"a","meta":null}`) },
		"retained_declared_value":     func(in *input) { in.retained[0] = []byte(`{"id":"a","content":"alpha"}`) },
		"retained_wrong_id":           func(in *input) { in.retained[0] = []byte(`{"id":"wrong"}`) },
		"retained_duplicate_id":       func(in *input) { in.retained[0] = []byte(`{"id":"a","id":"other"}`) },
		"retained_escaped_id":         func(in *input) { in.retained[0] = []byte(`{"id":"a","\u0069d":"other"}`) },
		"retained_escaped_column":     func(in *input) { in.retained[0] = []byte(`{"id":"a","\u0063ontent":"shadow"}`) },
		"retained_duplicate_ancestor": func(in *input) { in.retained[0] = []byte(`{"id":"a","meta":{},"meta":{"user_id":"shadow"}}`) },
		"nullable_schema":             func(in *input) { in.meta.Options.ColumnStore.Columns[1].Nullable = true },
	} {
		t.Run(name, func(t *testing.T) {
			meta := col.Meta()
			cfg := meta.Options.ColumnStore.copy()
			meta.Options.ColumnStore = &cfg
			meta.VectorIndexes = append([]VectorIndexDefinition(nil), meta.VectorIndexes...)
			in := input{meta: meta, ids: [][]byte{[]byte("a")}, retained: [][]byte{[]byte(`{"id":"a"}`)}, columns: []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}}
			mutate(&in)
			if _, err := newTrustedTypedProjection(in.meta, in.ids, in.retained, in.columns); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func openTypedMinimaCollection(t testing.TB) (string, *backenddb.DB, *Collection) {
	return openTypedMinimaCollectionMeta(t, typedMinimaCollectionMeta())
}

func typedMinimaCollectionMeta() CollectionMeta {
	return CollectionMeta{Name: "minima", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, DisableBufferedIndexedAsyncFlush: true, ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{
		{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, Owner: TypedStorageOwnerColumnPart, VectorDims: 8},
		{Name: "content", Path: "content", ValueType: ColumnStoreValueString},
		{Name: "user", Path: "meta.user_id", ValueType: ColumnStoreValueString},
		{Name: "path", Path: "meta.fpath", ValueType: ColumnStoreValueString},
	}}}, Indexes: []IndexDefinition{{Name: "user", Field: "meta.user_id", ValueType: IndexValueString}, {Name: "path", Field: "meta.fpath", ValueType: IndexValueString}}, TextIndexes: []TextIndexDefinition{{Name: "content", Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple}}, VectorIndexes: []VectorIndexDefinition{{Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 8, M: 2, Strategy: VectorIndexStrategyColumnGraph}}}
}

func openTypedMinimaCollectionMeta(t testing.TB, meta CollectionMeta) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	d := openTypedMinimaDB(t, dir)
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		d.Close()
		t.Fatal(err)
	}
	c, err := NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		d.Close()
		t.Fatal(err)
	}
	return dir, d, c
}

func openTypedMinimaDB(t testing.TB, dir string) *backenddb.DB {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestTypedMinimaInsertAndReplay(t *testing.T) {
	dir, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	replayDir := t.TempDir()
	copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
	ids := [][]byte{[]byte("b"), []byte("a")}
	retained := [][]byte{[]byte(`{"id":"b"}`), []byte(`{"id":"a"}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha", "beta"}}, {Name: "user", Strings: []string{"u1", "u2"}}, {Name: "path", Strings: []string{"file1", "file2"}}}
	_, stats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ColumnPublishDocumentExtraction != 0 {
		t.Fatalf("typed insert extracted JSON: %+v", stats)
	}
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > 1 {
			writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
		}
	}
	assertTypedMinimaRows(t, col)
	reopened := openTypedMinimaDB(t, replayDir)
	defer reopened.Close()
	replayed, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	assertTypedMinimaRows(t, replayed)
	if err := reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointReopen := openTypedMinimaDB(t, replayDir)
	defer checkpointReopen.Close()
	checkpointCollection, err := NewCollectionManager(checkpointReopen).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	assertTypedMinimaRows(t, checkpointCollection)
}

func TestTypedMinimaAdHocRuntimeAdmission(t *testing.T) {
	dir, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	index, err := newVectorIndex(col, VectorIndexOptions{Name: "ad_hoc", Field: "embedding", Metric: VectorMetricCosine, M: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := col.RegisterVectorIndex(index); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	before := countCollectionCommandWALFrames(t, dir)
	ids, retained := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
	for _, c := range []*Collection{col, other} {
		if _, _, err := c.InsertTypedBatchWithStats(ids, retained, columns); err == nil {
			t.Fatal("accepted runtime insert")
		}
		if _, err := c.ReplaceTypedBatch(ids, retained, columns); err == nil {
			t.Fatal("accepted runtime replacement")
		}
	}
	if got := countCollectionCommandWALFrames(t, dir); got != before {
		t.Fatalf("rejection appended WAL: %d -> %d", before, got)
	}
	col.UnregisterVectorIndex(index.name)
	if _, _, err := other.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.InsertTypedBatchWithStats(ids, retained, columns); err == nil {
		t.Fatal("accepted existing document ID")
	}
}

func assertTypedMinimaRows(t *testing.T, col *Collection) {
	t.Helper()
	ids, err := col.FindByIndex("user", "u1")
	if err != nil || len(ids) != 1 || string(ids[0]) != "b" {
		t.Fatalf("scalar ids=%q error=%v", ids, err)
	}
	result, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 2})
	if err != nil || len(result.Results) != 1 || string(result.Results[0].DocumentID) != "b" {
		t.Fatalf("text result=%+v error=%v", result, err)
	}
	got, err := col.Get([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"id":"b","embedding":[1,0,0,0,0,0,0,0],"content":"alpha","meta":{"user_id":"u1","fpath":"file1"}}`))
}

func TestTypedMinimaGenericMutations(t *testing.T) {
	for _, operation := range []string{"update", "update_batch", "typed_replace", "source_replace", "delete", "delete_batch"} {
		t.Run(operation, func(t *testing.T) {
			_, d, col := openTypedMinimaCollection(t)
			defer d.Close()
			_, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, []TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
				{Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			replacement := []byte(`{"id":"a","embedding":[0,1,0,0,0,0,0,0],"content":"beta","meta":{"user_id":"u2","fpath":"file2"}}`)
			callback := func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, []byte(`{"id":"a","embedding":[1,0,0,0,0,0,0,0],"content":"alpha","meta":{"user_id":"u1","fpath":"file1"}}`))
				return replacement, true, nil
			}
			switch operation {
			case "update":
				_, _, err = col.Update([]byte("a"), callback)
			case "update_batch":
				_, err = col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("a"), Update: callback}})
			case "source_replace":
				_, err = col.replaceSourceDocumentsWithCommandWALIntent([][]byte{[]byte("a")}, [][]byte{[]byte("a")}, [][]byte{replacement}, nil, nil)
			case "typed_replace":
				var results []UpdateBatchResult
				results, err = col.ReplaceTypedBatch([][]byte{[]byte("a"), []byte("missing")}, [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"missing"}`)}, []TypedColumnBatch{
					{Name: "embedding", Float32Vectors: [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}, {1, 0, 0, 0, 0, 0, 0, 0}}},
					{Name: "content", Strings: []string{"beta", "missing"}}, {Name: "user", Strings: []string{"u2", "missing"}}, {Name: "path", Strings: []string{"file2", "missing"}},
				})
				if err == nil && (len(results) != 2 || !results[0].Matched || !results[0].Modified || results[1].Matched) {
					t.Fatalf("typed results: %+v", results)
				}
			case "delete":
				err = col.Delete([]byte("a"))
			case "delete_batch":
				_, err = col.DeleteBatch([][]byte{[]byte("a")})
			}
			if err != nil {
				t.Fatal(err)
			}
			ids, err := col.FindByIndex("user", "u1")
			if err != nil || len(ids) != 0 {
				t.Fatalf("old scalar: %q %v", ids, err)
			}
			text, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 2})
			if err != nil || len(text.Results) != 0 {
				t.Fatalf("old text: %+v %v", text, err)
			}
			if operation == "update" || operation == "update_batch" || operation == "typed_replace" || operation == "source_replace" {
				ids, err = col.FindByIndex("user", "u2")
				if err != nil || len(ids) != 1 {
					t.Fatalf("new scalar: %q %v", ids, err)
				}
				text, err = col.SearchText(TextSearchOptions{IndexName: "content", Query: "beta", TopK: 2})
				if err != nil || len(text.Results) != 1 {
					t.Fatalf("new text: %+v %v", text, err)
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err == nil || !strings.Contains(err.Error(), "requires insert-only base physical refs") {
					t.Fatalf("M2 mutable graph gate changed: %v", err)
				}
				view, err := col.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				fetched, err := view.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{IncludePaths: []string{"embedding"}})
				closeErr := view.Close()
				if err != nil || closeErr != nil || len(fetched.Results) != 1 || fetched.Stats.TypedColumnRows != 1 {
					t.Fatalf("typed vector readback=%+v err=%v close=%v", fetched, err, closeErr)
				}
				assertJSONEqualM13C(t, fetched.Results[0].Document, []byte(`{"embedding":[0,1,0,0,0,0,0,0]}`))
			}
		})
	}
}

func TestTypedMinimaReplacementReplayAndNoop(t *testing.T) {
	dir, d, col := openTypedMinimaCollection(t)
	defer d.Close()
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	replayDir := t.TempDir()
	copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
	ids, retained := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	results, err := col.ReplaceTypedBatch(ids, retained, columns)
	if err != nil || len(results) != 1 || !results[0].Matched || results[0].Modified {
		t.Fatalf("noop results=%+v err=%v", results, err)
	}
	columns[0].Float32Vectors[0][0], columns[0].Float32Vectors[0][1] = 0, 1
	columns[1].Strings[0], columns[2].Strings[0], columns[3].Strings[0] = "beta", "u2", "file2"
	results, err = col.ReplaceTypedBatch(ids, retained, columns)
	if err != nil || !results[0].Modified {
		t.Fatalf("replace=%+v err=%v", results, err)
	}
	// Caller buffers are reusable immediately after acknowledgement.
	columns[0].Float32Vectors[0][1] = 99
	retained[0][2] = 'X'
	ids[0][0] = 'X'
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > 1 {
			writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
		}
	}
	reopened := openTypedMinimaDB(t, replayDir)
	defer reopened.Close()
	replayed, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Collection{col, replayed} {
		got, err := c.Get([]byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqualM13C(t, got, []byte(`{"id":"a","embedding":[0,1,0,0,0,0,0,0],"content":"beta","meta":{"user_id":"u2","fpath":"file2"}}`))
		for _, user := range []string{"u1", "u2"} {
			found, err := c.FindByIndex("user", user)
			want := 0
			if user == "u2" {
				want = 1
			}
			if err != nil || len(found) != want {
				t.Fatalf("user %s: %q %v", user, found, err)
			}
		}
		for _, query := range []string{"alpha", "beta"} {
			found, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: query, TopK: 2})
			want := 0
			if query == "beta" {
				want = 1
			}
			if err != nil || len(found.Results) != want {
				t.Fatalf("query %s: %+v %v", query, found, err)
			}
		}
	}
}

func TestTypedMinimaReplacementSignedZero(t *testing.T) {
	for _, initialBits := range []uint32{0, 0x80000000} {
		t.Run(fmt.Sprintf("initial=%08x", initialBits), func(t *testing.T) {
			dir, d, col := openTypedMinimaCollection(t)
			defer d.Close()
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			replayDir := t.TempDir()
			copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
			ids, retained := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, math.Float32frombits(initialBits), 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			wantBits := initialBits ^ 0x80000000
			columns[0].Float32Vectors[0][1] = math.Float32frombits(wantBits)
			results, err := col.ReplaceTypedBatch(ids, retained, columns)
			if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
				t.Fatalf("signed-zero replacement=%+v err=%v", results, err)
			}
			results, err = col.ReplaceTypedBatch(ids, retained, columns)
			if err != nil || len(results) != 1 || !results[0].Matched || results[0].Modified {
				t.Fatalf("identical-bit replacement=%+v err=%v", results, err)
			}
			for _, frame := range collectionCommandWALFrames(t, dir) {
				if frame.LSN > 1 {
					writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
				}
			}
			reopened := openTypedMinimaDB(t, replayDir)
			defer reopened.Close()
			replayed, err := NewCollectionManager(reopened).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []*Collection{col, replayed} {
				got, err := c.Get(ids[0])
				if err != nil {
					t.Fatal(err)
				}
				var document struct {
					Embedding []float32 `json:"embedding"`
				}
				if err := json.Unmarshal(got, &document); err != nil {
					t.Fatal(err)
				}
				if len(document.Embedding) != 8 || math.Float32bits(document.Embedding[1]) != wantBits {
					t.Fatalf("readback lost signed zero: %s, want bits %08x", got, wantBits)
				}
			}
		})
	}
}

func TestTypedMinimaInFlightPublicationMutation(t *testing.T) {
	for _, operation := range []string{"replace", "delete"} {
		t.Run(operation, func(t *testing.T) {
			_, d, col := openTypedMinimaCollection(t)
			defer d.Close()
			ids, retained := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error { once.Do(func() { close(entered); <-release }); return nil })
			defer restore()
			flushDone := make(chan error, 1)
			go func() { flushDone <- col.Flush() }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("publication did not reach preparation hook")
			}
			mutationDone := make(chan error, 1)
			mutationStarted := make(chan struct{})
			go func() {
				close(mutationStarted)
				if operation == "delete" {
					mutationDone <- col.Delete([]byte("a"))
					return
				}
				columns[1].Strings[0] = "beta"
				_, err := col.ReplaceTypedBatch(ids, retained, columns)
				mutationDone <- err
			}()
			<-mutationStarted
			select {
			case err := <-mutationDone:
				t.Fatalf("mutation passed paused publication: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case err := <-flushDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("flush stuck")
			}
			select {
			case err := <-mutationDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("mutation stuck")
			}
			found, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 1})
			if err != nil || len(found.Results) != 0 {
				t.Fatalf("old text=%+v err=%v", found, err)
			}
		})
	}
}

func TestTypedMinimaUniqueAdmission(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.Indexes = append(meta.Indexes, IndexDefinition{Name: "unique_user", Field: "meta.user_id", ValueType: IndexValueString, Unique: true})
	_, d, col := openTypedMinimaCollectionMeta(t, meta)
	defer d.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns); err == nil {
		t.Fatal("accepted duplicate unique value")
	}
	columns[2].Strings[0] = "u2"
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	columns[2].Strings[0] = "u1"
	if _, err := col.ReplaceTypedBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)}, columns); err == nil {
		t.Fatal("accepted conflicting replacement")
	}
	ids, err := col.FindByIndex("unique_user", "u2")
	if err != nil || len(ids) != 1 || string(ids[0]) != "b" {
		t.Fatalf("failed replacement changed state: %q %v", ids, err)
	}
}
