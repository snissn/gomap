package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func seedTypedMetadata4769(t testing.TB, rows, dims int) (string, *backenddb.DB, *Collection, [][]byte) {
	t.Helper()
	meta := cosineNormalizedF32V1TestMeta()
	meta.Options.ColumnStore.Columns[0].VectorDims = dims
	meta.VectorIndexes[0].Dimensions = dims
	return seedTypedMetadataWithMeta4769(t, rows, dims, meta)
}

func seedTypedMetadataWithMeta4769(t testing.TB, rows, dims int, meta CollectionMeta) (string, *backenddb.DB, *Collection, [][]byte) {
	t.Helper()
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	ids, retained := make([][]byte, rows), make([][]byte, rows)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	for i := range rows {
		ids[i] = []byte(fmt.Sprintf("metadata-%03d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q,"meta":{"extra":{"flag":true}}}`, ids[i]))
		v := make([]float32, dims)
		v[i%dims] = 3
		v[(i+1)%dims] = 4
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, v)
		columns[1].Strings = append(columns[1].Strings, "immutable searchable content")
		columns[2].Strings = append(columns[2].Strings, "old")
		columns[3].Strings = append(columns[3].Strings, "source")
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return dir, db, col, ids
}

func TestTypedMetadataPlannerStringBoundary4769(t *testing.T) {
	_, db, col, ids := seedTypedMetadata4769(t, 1, 8)
	defer db.Close()
	// The shared planner validates against its captured schema even when the
	// caller has not run public-handle validation first.
	for _, value := range []any{42, nil, []string{"wrong type"}} {
		plan, _, _, err := col.buildTypedMetadataPlan(ids, map[string]any{"meta.user_id": value}, nil, nil)
		if plan != nil {
			plan.close()
		}
		if !errors.Is(err, ErrTypedMetadataInvalid) {
			t.Fatalf("value=%#v error=%v", value, err)
		}
	}
	plan, _, result, err := col.buildTypedMetadataPlan(ids, map[string]any{"meta.user_id": ""}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ModifiedCount != 1 {
		plan.close()
		t.Fatalf("empty string modification count=%d", result.ModifiedCount)
	}
	j := typedStringColumnIndex(plan.meta.Options.ColumnStore.Columns, "meta.user_id")
	value := plan.metadataDocuments[0].declaredValues[j]
	plan.close()
	if !reflect.DeepEqual(value, columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: ""}) {
		t.Fatalf("empty string must be a present non-null value: %+v", value)
	}
	for _, modified := range []int{1, 0} {
		out, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": ""}, nil, metadataGeneration4769(col))
		if err != nil || out.ModifiedCount != modified {
			t.Fatalf("empty string update=%+v err=%v want modified=%d", out, err, modified)
		}
	}
	raw, err := col.Get(ids[0])
	if err != nil || !bytes.Contains(raw, []byte(`"user_id":""`)) {
		t.Fatalf("empty string reconstruction=%s err=%v", raw, err)
	}
}

func TestTypedMetadataCanceledResolutionDoesNotRead4769(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No DB/snapshot is available: cancellation must win before any scan or
	// preserved-row fetch tries to use one.
	col := new(Collection)
	rows := []columnPhysicalVisibleRow{{Preserved: &columnRowCoordinates{Generation: 1}}}
	if err := col.resolveColumnMetadataRows(ctx, nil, nil, ColumnStoreConfig{}, nil, ColumnAssetReadIntegrityVerify, rows); !errors.Is(err, context.Canceled) {
		t.Fatalf("metadata resolution=%v", err)
	}
	if _, _, err := col.materializeColumnStoreCompactionRows(ctx, columnStoreCompactionState{}, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("compaction scan=%v", err)
	}
}

func TestTypedMetadataPreservedReadIntegrity4769(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mode              ColumnAssetReadIntegrity
		warm              bool
		wantChecksumError bool
	}{
		{"default", "", false, true},
		{"verify", ColumnAssetReadIntegrityVerify, false, true},
		{"skip_checksums", ColumnAssetReadIntegritySkipChecksums, false, false},
		{"cached_verify_cold", ColumnAssetReadIntegrityCachedVerify, false, true},
		{"cached_verify_warm", ColumnAssetReadIntegrityCachedVerify, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetColumnAssetVerifiedChecksumCacheForTest(t)
			_, db, col, ids := seedTypedMetadata4769(t, 1, 8)
			defer db.Close()
			if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "new"}, nil, metadataGeneration4769(col)); err != nil {
				t.Fatal(err)
			}
			state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer closeState()
			view := newCollectionReadViewAtSnapshot(col, state.snap, state.catalog, false, "")
			defer view.Close()
			physical, err := view.materializerColumnSnapshotView(state.cfg)
			if err != nil {
				t.Fatal(err)
			}
			var ref ColumnAssetRef
			for _, asset := range physical.AssetRefs {
				if asset.Reason == ColumnPublishOperationInsert {
					ref = asset.Ref
					break
				}
			}
			root := db.ColumnAssetRootDir()
			raw, err := readColumnPhysicalAssetFromManager(root, ref)
			if err != nil {
				t.Fatal(err)
			}
			offset := bytes.Index(raw, []byte("immutable searchable content"))
			if offset < 0 {
				t.Fatal("preserved full row has no content payload")
			}
			path, err := columnAssetSegmentPath(root, ref)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if tc.warm {
				if !columnAssetVerifiedChecksumFileIdentityFromFile(file).valid {
					t.Skip("cached verify reuse requires stable column asset file identity")
				}
				if _, err := col.scanColumnPhysicalVisibleRowsWithReadIntegrity(nil, tc.mode); err != nil {
					t.Fatal(err)
				}
			}
			// Change only a valid string byte, not a structural field. Cached
			// verification intentionally trusts previously verified immutable files.
			if _, err := file.WriteAt([]byte("I"), int64(ref.Offset)+int64(offset)); err != nil {
				t.Fatal(err)
			}
			if tc.warm {
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			}
			check := func(route string, err error) {
				t.Helper()
				if tc.wantChecksumError {
					if err == nil || !strings.Contains(err.Error(), "checksum") {
						t.Fatalf("%s error=%v, want checksum rejection", route, err)
					}
				} else if err != nil {
					t.Fatalf("%s: %v", route, err)
				}
			}
			visible, err := col.scanColumnPhysicalVisibleRowsWithReadIntegrity([]string{"content"}, tc.mode)
			check("visible rows", err)
			if err == nil && (len(visible.Rows) != 1 || visible.Rows[0].Preserved == nil || visible.Rows[0].Values[0].String != "Immutable searchable content") {
				t.Fatalf("preserved content not resolved: %+v", visible.Rows)
			}
			// This is the compaction read phase, before its graph-specific
			// publication policy; no corrupted data is published by this test.
			rows, _, err := col.materializeColumnStoreCompactionRows(context.Background(), state, tc.mode)
			check("compaction rows", err)
			if err == nil && len(rows) != 1 {
				t.Fatalf("compaction rows=%d", len(rows))
			}
			if _, _, _, err := col.latestColumnPhysicalVisibleRowAtSnapshot(state.snap, state.catalog, ids[0], nil); err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("default point read must remain strict: %v", err)
			}
		})
	}
}

func TestTypedMetadataVectorDocumentIdentity4769(t *testing.T) {
	for _, representation := range []VectorIndexRepresentation{"", VectorIndexRepresentationCosineNormalizedF32V1} {
		name := string(representation)
		if name == "" {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			meta := cosineNormalizedF32V1TestMeta()
			meta.VectorIndexes[0].Representation = representation
			dir, db, col, ids := seedTypedMetadataWithMeta4769(t, 3, 8, meta)
			defer func() { db.Close() }()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			query := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{3, 4, 0, 0, 0, 0, 0, 0}, TopK: 3, EfSearch: 8, IncludeDocuments: true}
			before, err := col.SearchVectorIndex(query)
			if err != nil {
				t.Fatalf("before metadata: %v", err)
			}
			pinned, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: query.IndexName})
			if err != nil {
				t.Fatal(err)
			}
			defer pinned.Close()
			pinnedView, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer pinnedView.Close()
			oldRefs, err := pinnedView.LookupDocumentRowRefsByID(ids[:1], DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// One preserved scoring row is in the base and another in the mutable
			// suffix. Keep its content/vector unchanged for exact score comparison.
			replacement := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{0, 3, 4, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"immutable searchable content"}}, {Name: "user", Strings: []string{"old"}}, {Name: "path", Strings: []string{"source"}}}
			if _, err := col.ReplaceTypedBatch(ids[1:2], [][]byte{[]byte(fmt.Sprintf(`{"id":%q,"meta":{"extra":{"flag":true}}}`, ids[1]))}, replacement); err != nil {
				t.Fatal(err)
			}
			if _, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.user_id": "new"}, nil, metadataGeneration4769(col)); err != nil {
				t.Fatal(err)
			}
			current, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			_, staleErr := current.FetchDocumentsByRowRef([]DocumentRowRef{oldRefs.Results[0].RowRef}, DocumentFetchOptions{})
			current.Close()
			if staleErr == nil {
				t.Fatal("explicit stale row-ref request must still fail closed")
			}
			if _, err := col.SearchVectorIndex(query); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("legacy admission before rebuild: %v", err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatalf("rebuild after metadata: %v", err)
			}
			checkLatest := func(stage string) {
				t.Helper()
				for _, project := range []bool{false, true} {
					opts := query
					if project {
						opts.DocumentFetchOptions.ExcludePaths = []string{"embedding"}
					}
					after, err := col.SearchVectorIndex(opts)
					if err != nil {
						t.Fatalf("%s metadata document fetch: %v", stage, err)
					}
					if len(before.Results) != 3 || len(after.Results) != 3 || after.Stats.DocumentRowLocatorLookups != 3 || after.Stats.DocumentVisibilityScans != 0 {
						t.Fatalf("%s result/point-lookup budget: %+v", stage, after)
					}
					for i, row := range after.Results {
						if !bytes.Equal(row.ID, before.Results[i].ID) || row.Score != before.Results[i].Score {
							t.Fatal("metadata changed scoring")
						}
						want := "old"
						if bytes.Equal(row.ID, ids[0]) || bytes.Equal(row.ID, ids[1]) {
							want = "new"
						}
						if !bytes.Contains(row.Document, []byte(fmt.Sprintf(`"user_id":%q`, want))) || (project && bytes.Contains(row.Document, []byte(`"embedding"`))) {
							t.Fatalf("%s document=%s want user_id=%s projected=%t", stage, row.Document, want, project)
						}
					}
				}
				searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: query.IndexName})
				if err != nil {
					t.Fatal(err)
				}
				result, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: query.Query, TopK: 3, EfSearch: 8})
				searcher.Close()
				if err != nil || result.Stats.DocumentsFetched != 0 || result.Stats.DocumentRowLocatorLookups != 0 {
					t.Fatalf("%s no-document work=%+v err=%v", stage, result.Stats, err)
				}
			}
			checkLatest("rebuilt")
			old, err := pinned.Search(VectorIndexSearcherSearchOptions{Query: query.Query, TopK: 3, EfSearch: 8, IncludeDocuments: true})
			if err != nil || !reflect.DeepEqual(old.Results, before.Results) {
				t.Fatalf("pinned search changed: %+v error=%v", old.Results, err)
			}
			pinned.Close()
			pinnedView.Close()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTypedMinimaDB(t, dir)
			col, err = NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			checkLatest("reopened")
		})
	}
}

func TestTypedMetadataNormalizedColdFoldRaceAndGC4769(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := cosineNormalizedF32V1TestMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	dir, db, col, ids := seedTypedMetadataWithMeta4769(t, 8, 8, meta)
	defer func() { db.Close() }()
	ctx := context.Background()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", serving); err != nil {
		t.Fatal(err)
	}
	// Keep a scored suffix row as well as immutable-base rows.
	replacement := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{0, 3, 4, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"replacement content"}}, {Name: "user", Strings: []string{"old"}}, {Name: "path", Strings: []string{"source"}}}
	if _, err := col.ReplaceTypedBatch(ids[1:2], [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, ids[1]))}, replacement); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{3, 4, 0, 0, 0, 0, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.legacy", QuantizedRerankCandidates: 8, TopK: 8, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeProduction}
	search := func(acl string) (VectorIndexSearchResponse, *CollectionReadView) {
		t.Helper()
		opts := query
		if acl != "" {
			opts.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "user", Value: acl}
		}
		var buffer VectorIndexSearchBuffer
		result, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		return result, view
	}
	before, pinned := search("")
	defer pinned.Close()
	oldDocs, err := pinned.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.user_id": "new"}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	after, view := search("")
	view.Close()
	if !reflect.DeepEqual(before.Results, after.Results) || after.Stats.NormBytesRead != 0 || after.Stats.ColumnGraphWork.ScorePlane.ExactSuffixScoreCalls != 1 {
		t.Fatalf("metadata changed normalized scoring: before=%+v after=%+v", before, after)
	}
	pinned.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", serving); err != nil {
		t.Fatal(err)
	}
	after, view = search("")
	view.Close()
	if !reflect.DeepEqual(before.Results, after.Results) || col.typedGraphPublicationSnapshot().lastMetadataGeneration == 0 {
		t.Fatal("cold metadata lost scoring or invalidation frontier")
	}
	allowed, held := search("new")
	defer held.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("cold ACL=%+v", allowed.Results)
	}
	heldDocs, err := held.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// A new reference after capture cannot be installed against retired full rows.
	err = col.foldTypedGraph(ctx, serving.Owners.Cold, serving.FoldRows, serving.CandidateOutput, func() error {
		_, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.user_id": "raced"}, nil, metadataGeneration4769(col))
		return err
	})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("racing fold=%v", err)
	}
	if err := col.FoldColumnGraphServing(ctx, "embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if _, err := col.ColumnAssetGC(ctx, ColumnAssetGCOptions{Detailed: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ValueLogGC(ctx, backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	}
	stillHeld, err := held.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	if err != nil || !reflect.DeepEqual(heldDocs.Results, stillHeld.Results) {
		t.Fatalf("GC changed pinned metadata: %v", err)
	}
	allowed, view = search("raced")
	if len(allowed.Results) != 2 {
		t.Fatalf("folded ACL=%+v", allowed.Results)
	}
	docs, err := view.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	view.Close()
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range docs.Results {
		var old, current struct {
			Content   string
			Embedding []float32
		}
		if err := json.Unmarshal(oldDocs.Results[i].Document, &old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(row.Document, &current); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(old, current) {
			t.Fatal("fold changed canonical content/vector bits")
		}
	}
	// Ordinary replacement and deletion must supersede metadata references.
	if _, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.user_id": "again"}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	if _, err := col.ReplaceTypedBatch(ids[:1], [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, ids[0]))}, replacement); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	allowed, view = search("again")
	view.Close()
	if len(allowed.Results) != 0 {
		t.Fatalf("superseded metadata remained visible: %+v", allowed.Results)
	}
}

func TestTypedMetadataReplayRejectsProtectedAfterimages4769(t *testing.T) {
	_, db, col, ids := seedTypedMetadata4769(t, 1, 8)
	defer db.Close()
	plan, payload, _, err := col.buildTypedMetadataPlan(ids, map[string]any{"meta.user_id": "new"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan.close()
	for _, field := range []string{"content", "embedding", "meta.chunk_parent", "meta.user_id"} {
		t.Run(field, func(t *testing.T) {
			var object map[string]any
			if err := decodeTypedMetadataJSON(payload.Documents[0].Retained, &object); err != nil {
				t.Fatal(err)
			}
			if _, err := applyTypedMetadataPath(object, field, "forbidden", false); err != nil {
				t.Fatal(err)
			}
			corrupt := payload
			corrupt.Documents = append([]commitlog.CollectionTypedMetadataDocument(nil), payload.Documents...)
			corrupt.Documents[0].Retained, err = json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			p, _, _, err := col.buildTypedMetadataPlan(ids, nil, nil, &corrupt)
			if p != nil {
				p.close()
			}
			if err == nil {
				t.Fatal("accepted forbidden afterimage")
			}
		})
	}
}

func TestTypedMetadataReplayRejectsNonColumnCollection4769(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{Name: "ordinary"})
	db := openCollectionCommandWALDB(t, dir)
	defer db.Close()
	col, err := NewCollectionManager(db).OpenCollection("ordinary")
	if err != nil {
		t.Fatal(err)
	}
	payload := commitlog.CollectionTypedMetadataPayload{Collection: "ordinary", SchemaHash: 1, Documents: []commitlog.CollectionTypedMetadataDocument{{ID: []byte("a"), Retained: []byte(`{"id":"a"}`)}}}
	plan, _, _, err := col.buildTypedMetadataPlan([][]byte{[]byte("a")}, nil, nil, &payload)
	if plan != nil {
		plan.close()
	}
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("non-column replay=%v", err)
	}
}

func TestTypedMetadataInvalidBatchRollback4769(t *testing.T) {
	_, db, col, ids := seedTypedMetadata4769(t, 2, 8)
	defer db.Close()
	cases := []struct {
		name  string
		set   map[string]any
		unset []string
	}{
		{"nonmeta", map[string]any{"content": "bad"}, nil},
		{"ancestor", map[string]any{"meta.extra": map[string]any{}, "meta.extra-child": true, "meta.extra.flag": false}, nil},
		{"overlap", map[string]any{"meta.user_id": "new"}, []string{"meta.user_id"}},
		{"type", map[string]any{"meta.user_id": 42}, nil},
		{"unset_required", nil, []string{"meta.user_id"}},
		{"chunk", map[string]any{"meta.chunk_parent": "bad"}, nil},
		{"path", map[string]any{"meta..bad": "bad"}, nil},
		{"traverse_scalar", map[string]any{"meta.extra.flag.x": false}, nil},
	}
	seq, root := dbCommitSeqAndSystemRoot(db)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := col.UpdateTypedMetadataByID(ids, tc.set, tc.unset, metadataGeneration4769(col)); !errors.Is(err, ErrTypedMetadataInvalid) {
				t.Fatalf("err=%v", err)
			}
			if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(db); nextSeq != seq || nextRoot != root {
				t.Fatal("rejected batch published")
			}
		})
	}
	if _, err := col.UpdateTypedMetadataByID([][]byte{ids[0], ids[0]}, map[string]any{}, nil, metadataGeneration4769(col)); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("duplicate=%v", err)
	}
	if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{}, nil, metadataGeneration4769(col)+1); !errors.Is(err, ErrHybridSearchStaleIndex) {
		t.Fatalf("generation=%v", err)
	}
	// A later ID's incompatible residual shape cannot partially update the first ID.
	if _, err := col.UpdateTypedMetadataByID(ids[1:], map[string]any{"meta.extra": false}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	seq, root = dbCommitSeqAndSystemRoot(db)
	if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "new", "meta.extra.flag": false}, nil, metadataGeneration4769(col)); !errors.Is(err, ErrTypedMetadataInvalid) {
		t.Fatalf("batch=%v", err)
	}
	if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(db); nextSeq != seq || nextRoot != root {
		t.Fatal("partially applied rejected batch")
	}
	raw, err := col.Get(ids[0])
	if err != nil || !bytes.Contains(raw, []byte(`"user_id":"old"`)) {
		t.Fatalf("old first=%s err=%v", raw, err)
	}
}

func TestTypedMetadataRejectsNestedInvalidUTF8BeforePublication4769(t *testing.T) {
	_, db, col, ids := seedTypedMetadata4769(t, 1, 8)
	defer db.Close()
	bad := string([]byte{0xff})
	type namedString string
	cycle := map[string]any{}
	cycle["self"] = cycle
	before, err := col.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	seq, root := dbCommitSeqAndSystemRoot(db)
	for name, value := range map[string]any{
		"string":       bad,
		"object_value": map[string]any{"name": bad},
		"object_key":   map[string]any{bad: "value"},
		"array":        []any{"valid", map[string]any{"nested": []any{bad}}},
		"typed_map":    map[string]string{"name": bad},
		"typed_slice":  []namedString{namedString(bad)},
		"pointer":      &bad,
		"struct":       struct{ Name string }{bad},
		"cycle":        cycle,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.profile": value}, nil, metadataGeneration4769(col)); !errors.Is(err, ErrTypedMetadataInvalid) {
				t.Fatalf("invalid nested metadata error=%v", err)
			}
			if gotSeq, gotRoot := dbCommitSeqAndSystemRoot(db); gotSeq != seq || gotRoot != root {
				t.Fatal("invalid metadata advanced publication")
			}
			if after, err := col.Get(ids[0]); err != nil || !bytes.Equal(after, before) {
				t.Fatalf("invalid metadata changed document: %s error=%v", after, err)
			}
		})
	}
	valid := map[string]any{"meta.profile": map[string]any{"名字": []any{"日本語", "�", nil, true, json.Number("9007199254740993")}}}
	for _, modified := range []int{1, 0} {
		out, err := col.UpdateTypedMetadataByID(ids, valid, nil, metadataGeneration4769(col))
		if err != nil || out.ModifiedCount != modified {
			t.Fatalf("valid Unicode update=%+v err=%v want modified=%d", out, err, modified)
		}
	}
}

func TestTypedMetadataWALRecovery4769(t *testing.T) {
	dir, db, col, ids := seedTypedMetadata4769(t, 2, 8)
	defer func() { db.Close() }()
	before, err := col.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	seq, root := dbCommitSeqAndSystemRoot(db)
	preIntent := errors.New("metadata before WAL append failure")
	restorePrepare := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyAppend {
			return preIntent
		}
		return nil
	})
	_, err = col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "not-published"}, nil, metadataGeneration4769(col))
	restorePrepare()
	if !errors.Is(err, preIntent) || errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("pre-intent failure=%v", err)
	}
	if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(db); nextSeq != seq || nextRoot != root {
		t.Fatal("pre-intent failure changed authority")
	}
	injected := errors.New("metadata after durable intent failure")
	var fired atomic.Bool
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync && fired.CompareAndSwap(false, true) {
			return injected
		}
		return nil
	})
	defer func() {
		if restore != nil {
			restore()
		}
	}()
	_, err = col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "replayed", "meta.extra.flag": false}, nil, metadataGeneration4769(col))
	if !errors.Is(err, ErrCommitAmbiguous) || !errors.Is(err, injected) {
		t.Fatalf("post-WAL fault=%v", err)
	}
	restore()
	restore = nil
	if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(db); nextSeq != seq || nextRoot != root {
		t.Fatal("failed output changed authority")
	}
	if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{}, nil, metadataGeneration4769(col)); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("no-op bypassed recovery fence: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	var oldDoc struct {
		Embedding []float32
		Content   string
	}
	if err := json.Unmarshal(before, &oldDoc); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		raw, err := col.Get(id)
		if err != nil || !bytes.Contains(raw, []byte(`"user_id":"replayed"`)) {
			t.Fatalf("replay=%s err=%v", raw, err)
		}
	}
	raw, err := col.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Embedding []float32
		Content   string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, oldDoc) {
		t.Fatal("replay changed vector/content")
	}
}

func TestTypedMetadataDimensionIndependentPayload4769(t *testing.T) {
	for _, rows := range []int{1, 32, 128} {
		var want int
		for _, dims := range []int{8, 768} {
			t.Run(fmt.Sprintf("rows%d/dims%d", rows, dims), func(t *testing.T) {
				_, db, col, ids := seedTypedMetadata4769(t, rows, dims)
				defer db.Close()
				plan, payload, result, err := col.buildTypedMetadataPlan(ids, map[string]any{"meta.user_id": "changed"}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer plan.close()
				if result.ModifiedCount != rows {
					t.Fatalf("modified=%d", result.ModifiedCount)
				}
				raw, err := commitlog.EncodeCollectionTypedMetadataPayload(payload)
				if err != nil {
					t.Fatal(err)
				}
				if dims == 8 {
					want = len(raw)
				} else if len(raw) != want {
					t.Fatalf("WAL payload grew with dimension: %d vs %d", len(raw), want)
				}
				for _, doc := range plan.metadataDocuments {
					if doc.preserved == nil {
						t.Fatal("missing preserved authority")
					}
					for _, value := range doc.declaredValues {
						if value.Float32Vector != nil || value.DenseNumericVector != nil {
							t.Fatal("metadata plan copied a vector")
						}
					}
				}
				t.Logf("rows=%d dims=%d WAL_payload_B=%d", rows, dims, len(raw))
				if _, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "changed"}, nil, metadataGeneration4769(col)); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTypedMetadataBaseSuffixServingAndFold4769(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	// One ordinary full update supplies a scored suffix row beside seven base rows.
	suffix := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[1:2]}, {Name: "content", Strings: columns[1].Strings[1:2]}, {Name: "user", Strings: columns[2].Strings[1:2]}, {Name: "path", Strings: []string{"suffix"}}}
	if _, err := col.ReplaceTypedBatch(ids[1:2], retained[1:2], suffix); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 8, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeProduction}
	search := func(path string) (VectorIndexSearchResponse, *CollectionReadView) {
		t.Helper()
		q := query
		if path != "" {
			q.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: path}
		}
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		return response, view
	}
	before, pinned := search("")
	defer pinned.Close()
	warm, warmView := search("source")
	warmView.Close()
	if len(warm.Results) != 7 {
		t.Fatalf("warm=%+v", warm.Results)
	}
	state := col.typedGraphPublicationSnapshot()
	if len(state.rows) != 1 {
		t.Fatalf("suffix rows=%d", len(state.rows))
	}
	result, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.fpath": "allowed"}, nil, metadataGeneration4769(col))
	if err != nil || result != (TypedMetadataUpdateResult{2, 2}) {
		t.Fatalf("metadata=%+v err=%v", result, err)
	}
	next := col.typedGraphPublicationSnapshot()
	if len(next.rows) != 1 || &next.rows[0] != &state.rows[0] || (len(state.invNorms) > 0 && &next.invNorms[0] != &state.invNorms[0]) || next.lastMetadataGeneration == 0 {
		t.Fatal("metadata changed scoring state or missed frontier")
	}
	after, view := search("")
	view.Close()
	if !reflect.DeepEqual(before.Results, after.Results) {
		t.Fatalf("ranking changed: before=%+v after=%+v", before.Results, after.Results)
	}
	allowed, view := search("allowed")
	defer view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("allowed=%+v", allowed.Results)
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(allowed.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range fetched.Results {
		if !bytes.Contains(row.Document, []byte(`"fpath":"allowed"`)) {
			t.Fatalf("stale fetch=%s", row.Document)
		}
	}
	old, err := pinned.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(old.Results[0].Document, []byte("allowed")) {
		t.Fatal("pinned reader changed")
	}
	view.Close()
	pinned.Close()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	if col.typedGraphPublicationSnapshot().lastMetadataGeneration != 0 {
		t.Fatal("fold retained metadata frontier")
	}
	allowed, view = search("allowed")
	view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("fold filter=%+v", allowed.Results)
	}
	if _, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.fpath": "after-fold"}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	allowed, view = search("after-fold")
	view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("after fold update=%+v", allowed.Results)
	}
}

func metadataGeneration4769(c *Collection) uint64 {
	var generation uint64
	meta := c.Meta()
	for _, d := range meta.VectorIndexes {
		generation = max(generation, d.SchemaGeneration)
	}
	for _, d := range meta.TextIndexes {
		generation = max(generation, d.SchemaGeneration)
	}
	return max(generation, 1)
}

func TestTypedMetadataUpdateNoVectorRewrite4769(t *testing.T) {
	dir, d, col := openTypedMinimaCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a","meta":{"extra":{"flag":true}}}`), []byte(`{"id":"b","meta":{"extra":{"flag":true}}}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original a", "original b"}}, {Name: "user", Strings: []string{"old", "old"}}, {Name: "path", Strings: []string{"p", "p"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	old, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	beforeRefs, err := old.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	generation := metadataGeneration4769(col)
	for _, user := range []string{"new", "newer"} {
		result, err := col.UpdateTypedMetadataByID(append(ids, []byte("missing")), map[string]any{"meta.user_id": user, "meta.extra.flag": false}, nil, generation)
		if err != nil || result != (TypedMetadataUpdateResult{2, 2}) {
			t.Fatalf("update=%+v err=%v", result, err)
		}
		view, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		_, err = view.visitDocumentScoringRowRefsByID(ids, func(id []byte, ref DocumentRowRef, found bool) error {
			i := 0
			if string(id) == "b" {
				i = 1
			}
			if !found || !reflect.DeepEqual(ref, beforeRefs.Results[i].RowRef) {
				t.Errorf("scoring authority changed: %+v vs %+v", ref, beforeRefs.Results[i].RowRef)
			}
			return nil
		})
		view.Close()
		if err != nil {
			t.Fatal(err)
		}
		for i, id := range ids {
			raw, err := col.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Content   string         `json:"content"`
				Embedding []float32      `json:"embedding"`
				Meta      map[string]any `json:"meta"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.Content != columns[1].Strings[i] || !reflect.DeepEqual(got.Embedding, columns[0].Float32Vectors[i]) || got.Meta["user_id"] != user {
				t.Fatalf("changed nonmetadata or wrong metadata: %s", raw)
			}
		}
	}
	seq, root := dbCommitSeqAndSystemRoot(d)
	result, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "newer", "meta.extra.flag": false}, []string{"meta.absent"}, generation)
	if err != nil || result != (TypedMetadataUpdateResult{2, 0}) {
		t.Fatalf("noop=%+v err=%v", result, err)
	}
	if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(d); nextSeq != seq || nextRoot != root {
		t.Fatal("no-op published")
	}
	oldResult, err := old.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range oldResult.Results {
		if !bytes.Contains(r.Document, []byte(`"user_id":"old"`)) {
			t.Fatalf("pinned metadata changed: %s", r.Document)
		}
	}
	old.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := col.Get(ids[0])
	if err != nil || !bytes.Contains(raw, []byte(`"user_id":"newer"`)) || !bytes.Contains(raw, []byte(`"original a"`)) {
		t.Fatalf("reopen=%s err=%v", raw, err)
	}
}
