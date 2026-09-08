package collections

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestWorkStatsIndexedJSONProducers(t *testing.T) {
	for _, path := range []string{"tag", "nested.tag"} {
		t.Run(path, func(t *testing.T) {
			doc := []byte(`{"tag":"alpha","nested":{"tag":"alpha"}}`)
			before := workstats.Read()
			runtimes := []indexRuntime{{def: indexDefinition{name: "tag", field: path, valueType: IndexValueString}, path: strings.Split(path, ".")}}
			if _, err := orderedIndexStateForDocument(doc, runtimes, collectionOptions{}); err != nil {
				t.Fatal(err)
			}
			def := TextIndexDefinition{Name: "text", Fields: []TextIndexField{{Field: path}}}
			if analysis, err := analyzeTextIndexDocument(def, doc); err != nil || len(analysis.Fields) != 1 {
				t.Fatalf("analysis=%+v err=%v", analysis, err)
			}
			cfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{{Name: "tag", Path: path, ValueType: ColumnStoreValueString}}}
			// A late parser error must count its attempted prefix, never the untouched tail.
			_, err := extractColumnDeclaredRowsFromJSONDocuments(cfg, []columnWriteDocument{{ID: []byte("a"), Document: doc}, {ID: []byte("b"), Document: []byte(`{`)}, {ID: []byte("c"), Document: doc}})
			if err == nil {
				t.Fatal("invalid second row accepted")
			}
			after := workstats.Read()
			if after.IndexedJSON.ScalarRows-before.IndexedJSON.ScalarRows != 1 || after.IndexedJSON.TextRows-before.IndexedJSON.TextRows != 1 || after.IndexedJSON.ColumnRows-before.IndexedJSON.ColumnRows != 2 {
				t.Fatalf("before=%+v after=%+v", before.IndexedJSON, after.IndexedJSON)
			}
		})
	}
	before := workstats.Read()
	materializer := &StoredDocumentJSONMaterializer{documentFormat: DocumentFormatJSON}
	if vector, ok, err := vectorFromStoredDocument(materializer, []byte(`{"embedding":[1,0]}`), []string{"embedding"}); err != nil || !ok || len(vector) != 2 {
		t.Fatalf("vector=%v ok=%t err=%v", vector, ok, err)
	}
	raw, err := bson.Marshal(bson.M{"tag": "alpha", "embedding": bson.A{1.0, 0.0}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = vectorFromStoredDocument(&StoredDocumentJSONMaterializer{documentFormat: DocumentFormatBSON}, raw, []string{"embedding"}); err != nil {
		t.Fatal(err)
	}
	if _, err = materializeTextBackfillDocumentJSON(raw, collectionOptions{documentFormat: DocumentFormatBSON}); err != nil {
		t.Fatal(err)
	}
	idx := &VectorIndex{scalarDefinitions: []IndexDefinition{{Name: "tag", Field: "tag", ValueType: IndexValueString}}, scalarRuntimes: []indexRuntime{{def: indexDefinition{name: "tag", field: "tag", valueType: IndexValueString}, path: []string{"tag"}}}}
	if row, err := idx.nativeScalarRow(materializer, []byte(`{"tag":"alpha"}`)); err != nil || len(row) != 1 {
		t.Fatalf("native scalar=%v err=%v", row, err)
	}
	after := workstats.Read()
	if after.IndexedJSON.ScalarRows-before.IndexedJSON.ScalarRows != 1 {
		t.Fatal("native scalar extraction was not observed")
	}
	if after.IndexedJSON.VectorRows-before.IndexedJSON.VectorRows != 1 || after.IndexedJSON.MaterializationRows-before.IndexedJSON.MaterializationRows != 1 {
		t.Fatalf("before=%+v after=%+v", before.IndexedJSON, after.IndexedJSON)
	}
}

func TestWorkStatsTypedStartupReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_WORK_STATS_REPLAY_DIR"); dir != "" {
		before := workstats.Read()
		if before.Replay.FramesAttempted != 0 || before.Replay.TypedRowsDecoded != 0 {
			t.Fatal("helper did work before backend open")
		}
		d := openTypedMinimaDB(t, dir)
		// The first observation is before any live manager or service exists.
		after := workstats.Read()
		if after.Replay.FramesApplied == 0 || after.Replay.TypedPayloadFrames == 0 || after.Replay.TypedRowsDecoded != 2 || after.Replay.FrameErrors != 0 || after.Typed.ScalarRows == 0 || after.Typed.TextRows == 0 || after.IndexedJSON != (workstats.IndexedJSONStats{}) {
			t.Fatalf("startup work=%+v", after)
		}
		if after.OriginUnixNano != before.OriginUnixNano || after.PID != before.PID || after.OriginUnixNano > before.SnapshotUnixNano {
			t.Fatal("recovery reset observation origin")
		}
		col, err := NewCollectionManager(d).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		assertTypedMinimaRows(t, col)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	dir, d, _ := openTypedMinimaCollection(t)
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Reuse the existing real command-WAL post-sync process-cut fixture.
	writer := exec.Command(os.Args[0], "-test.run=^TestTypedMinimaCrashAndPublicationCuts$")
	writer.Env = append(os.Environ(), "GOMAP_TYPED_MINIMA_CRASH_DIR="+dir, "GOMAP_TYPED_MINIMA_CRASH_MODE=after_sync")
	if out, err := writer.CombinedOutput(); err != nil {
		t.Fatalf("crash writer: %v\n%s", err, out)
	}
	reader := exec.Command(os.Args[0], "-test.run=^TestWorkStatsTypedStartupReplay$")
	reader.Env = append(os.Environ(), "GOMAP_WORK_STATS_REPLAY_DIR="+dir)
	if out, err := reader.CombinedOutput(); err != nil {
		t.Fatalf("startup observer: %v\n%s", err, out)
	}
}

func TestWorkStatsLegacyProjectionDistinction(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.Options.ColumnStore.Columns = meta.Options.ColumnStore.Columns[:1]
	meta.Indexes, meta.TextIndexes = nil, nil
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	meta = col.Meta()
	payload := commitlog.CollectionTypedBatchPayload{Collection: meta.Name, SchemaHash: meta.Options.ColumnStore.SchemaHash, LegacyProjection: true,
		Columns:   []commitlog.CollectionTypedColumn{{Name: "embedding", Type: commitlog.CollectionTypedFloat32Vector, Dimensions: 8}},
		Documents: []commitlog.CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{"id":"a"}`), Values: []commitlog.CollectionTypedValue{{Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}}}}}}
	before := workstats.Read()
	projection, _, _, err := typedProjectionFromPayload(meta, payload)
	if err != nil || !projection.legacyTyped {
		t.Fatalf("legacy projection=%+v err=%v", projection, err)
	}
	after := workstats.Read()
	if after.Replay.LegacyProjectionRowsDecoded-before.Replay.LegacyProjectionRowsDecoded != 1 || after.Replay.TypedRowsDecoded != before.Replay.TypedRowsDecoded || after.IndexedJSON != before.IndexedJSON {
		t.Fatalf("legacy payload mislabeled: before=%+v after=%+v", before, after)
	}
}
