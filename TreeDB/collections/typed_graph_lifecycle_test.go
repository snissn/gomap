package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// The subprocess exits after the public durable acknowledgement, without Close
// or Flush. This is a process-crash replay test, not a power-loss simulation.
func TestTypedGraphLifecyclePublicMutationAndReopen(t *testing.T) {
	mutate := func(t *testing.T, col *Collection, operation string) {
		t.Helper()
		id := "base"
		if operation == "insert" {
			id = "new"
		}
		ids, retained := [][]byte{[]byte(id)}, [][]byte{[]byte(`{"id":"` + id + `"}`)}
		columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
		var err error
		switch operation {
		case "insert":
			_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
		case "replace":
			_, err = col.ReplaceTypedBatch(ids, retained, columns)
		case "reinsert":
			if err = col.Delete(ids[0]); err == nil {
				_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
			}
		case "delete":
			err = col.Delete(ids[0])
		default:
			t.Fatalf("unknown mutation %q", operation)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if dir := os.Getenv("GOMAP_TYPED_GRAPH_LIFECYCLE_CRASH_DIR"); dir != "" {
		var indexedJSON atomic.Uint64
		if os.Getenv("GOMAP_TYPED_GRAPH_LIFECYCLE_SERVING") == "1" {
			setColumnVectorGraphCanonicalRowsTestHook(func() { indexedJSON.Add(1) })
		}
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("GOMAP_TYPED_GRAPH_LIFECYCLE_SERVING") == "1" {
			if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
				t.Fatal(err)
			}
		}
		mutate(t, col, os.Getenv("GOMAP_TYPED_GRAPH_LIFECYCLE_OPERATION"))
		if indexedJSON.Load() != 0 {
			t.Fatal("selected acknowledged mutation entered indexed JSON extraction")
		}
		os.Exit(0)
	}
	for _, boundary := range []string{"live", "crash_reopen", "rebuild", "serving_crash_reopen"} {
		for _, operation := range []string{"insert", "replace", "delete", "reinsert"} {
			t.Run(boundary+"/"+operation, func(t *testing.T) {
				var publicScans atomic.Uint64
				if boundary == "serving_crash_reopen" {
					restore := setColumnVectorGraphCanonicalRowsTestHook(func() { publicScans.Add(1) })
					defer restore()
				}
				dir, db, col := openTypedMinimaCollection(t)
				defer func() { _ = db.Close() }()
				ids := [][]byte{[]byte("base"), []byte("other")}
				retained := [][]byte{[]byte(`{"id":"base"}`), []byte(`{"id":"other"}`)}
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 1, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original", "other"}}, {Name: "user", Strings: []string{"tenant", "tenant"}}, {Name: "path", Strings: []string{"source", "source"}}}
				if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
					t.Fatal(err)
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
				if boundary == "crash_reopen" || boundary == "serving_crash_reopen" {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command(os.Args[0], "-test.run=^TestTypedGraphLifecyclePublicMutationAndReopen$")
					cmd.Env = append(os.Environ(), "GOMAP_TYPED_GRAPH_LIFECYCLE_CRASH_DIR="+dir, "GOMAP_TYPED_GRAPH_LIFECYCLE_OPERATION="+operation)
					if boundary == "serving_crash_reopen" {
						cmd.Env = append(cmd.Env, "GOMAP_TYPED_GRAPH_LIFECYCLE_SERVING=1")
					}
					if output, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("crash helper: %v\n%s", err, output)
					}
					db = openTypedMinimaDB(t, dir)
					var err error
					col, err = NewCollectionManager(db).OpenCollection("minima")
					if err != nil {
						t.Fatal(err)
					}
				} else {
					mutate(t, col, operation)
				}
				wantID := "base"
				if operation == "insert" {
					wantID = "new"
				}
				doc, err := col.Get([]byte(wantID))
				if err != nil || (operation == "delete" && doc != nil) || (operation != "delete" && !bytes.Contains(doc, []byte(`"content":"changed"`))) {
					t.Fatalf("authoritative mutation missing: document=%s err=%v", doc, err)
				}
				if operation == "delete" {
					wantID = "other"
				}
				if boundary == "serving_crash_reopen" {
					if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
						t.Fatal(err)
					}
					for _, filtered := range []bool{false, true} {
						query := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
						if filtered {
							query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "source"}
						}
						var buffer VectorIndexSearchBuffer
						response, view, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
						if err != nil {
							t.Fatal(err)
						}
						fetched, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
						closeErr := view.Close()
						if fetchErr != nil || closeErr != nil || len(fetched.Results) != 1 || string(fetched.Results[0].ID) != wantID {
							t.Fatalf("public recovered results=%+v fetch=%v close=%v", fetched.Results, fetchErr, closeErr)
						}
						if operation != "delete" && !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"changed"`)) {
							t.Fatalf("stale document: %s", fetched.Results[0].Document)
						}
					}
					if publicScans.Load() != 0 {
						t.Fatal("public recovery entered indexed JSON extraction")
					}
					return
				}
				if boundary == "rebuild" {
					assertTypedGraphLifecycleRebuildSource(t, col)
					var canonicalScans atomic.Uint64
					restore := setColumnVectorGraphCanonicalRowsTestHook(func() { canonicalScans.Add(1) })
					defer restore()
					if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
						t.Fatalf("mutated typed rebuild: %v", err)
					}
					if got := canonicalScans.Load(); got != 0 {
						t.Fatalf("typed rebuild entered canonical JSON extraction %d times", got)
					}
				}
				response, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, IncludeDocuments: true})
				if boundary != "rebuild" {
					// M3 serving remains gated until durable base/current closure
					// and incremental publication are implemented. The original
					// desired public-result failure is retained in milestone-1.
					if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "manifest mismatch") {
						t.Fatalf("expected closed mutable graph identity gate, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("acknowledged %s must be graph-searchable after %s: %v", operation, boundary, err)
				}
				if len(response.Results) != 1 || string(response.Results[0].ID) != wantID || (operation != "delete" && response.Results[0].Score < .999) {
					t.Fatalf("stale graph result: %+v want=%s", response.Results, wantID)
				}
				if operation != "delete" && !bytes.Contains(response.Results[0].Document, []byte(`"content":"changed"`)) {
					t.Fatalf("stale materialized document: %s", response.Results[0].Document)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = openTypedMinimaDB(t, dir)
				col, err = NewCollectionManager(db).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				reopened, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, IncludeDocuments: true})
				if err != nil || len(reopened.Results) != 1 || string(reopened.Results[0].ID) != wantID || reopened.Results[0].Score != response.Results[0].Score || !bytes.Equal(reopened.Results[0].Document, response.Results[0].Document) {
					t.Fatalf("mixed-source reopen result=%+v err=%v", reopened.Results, err)
				}
			})
		}
	}
}

func TestTypedGraphLifecycleEmptyRebuild(t *testing.T) {
	for _, allDeleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_deleted=%v", allDeleted), func(t *testing.T) {
			dir, db, col := openTypedMinimaCollection(t)
			defer func() { db.Close() }()
			if allDeleted {
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
					t.Fatal(err)
				}
				if err := col.Delete([]byte("base")); err != nil {
					t.Fatal(err)
				}
				assertTypedGraphLifecycleRebuildSource(t, col)
			}
			var canonicalScans atomic.Uint64
			restore := setColumnVectorGraphCanonicalRowsTestHook(func() { canonicalScans.Add(1) })
			defer restore()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatalf("repeat empty rebuild: %v", err)
			}
			if canonicalScans.Load() != 0 {
				t.Fatal("empty typed rebuild entered JSON source")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTypedMinimaDB(t, dir)
			var err error
			col, err = NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatalf("repeat empty rebuild after reopen: %v", err)
			}
			response, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 1})
			if err != nil || len(response.Results) != 0 {
				t.Fatalf("empty rebuilt graph results=%v err=%v", response.Results, err)
			}
		})
	}
}

func TestTypedGraphLifecycleCloseFirstBufferedFlush(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer func() { _ = db.Close() }()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"first buffered write"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	// No graph rebuild, read, or explicit Flush initializes asset ownership.
	if err := db.Close(); err != nil {
		t.Fatalf("Close must flush the acknowledged typed batch: %v", err)
	}
	if id := columnAssetLifecycleRegistryProcessDBID(db); id != 0 {
		t.Fatalf("closed DB retained registry identity %d", id)
	}
	db = openTypedMinimaDB(t, dir)
	var err error
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	document, err := col.Get([]byte("a"))
	if err != nil || !bytes.Contains(document, []byte("first buffered write")) {
		t.Fatalf("acknowledged row after Close/reopen: %s, %v", document, err)
	}
}

func TestTypedGraphLifecycleRegistryManagerCloseRace(t *testing.T) {
	_, db, _ := openTypedMinimaCollection(t)
	defer db.Close()
	id := columnAssetLifecycleRegistryProcessDBID(db)
	if id == 0 {
		t.Fatal("open manager has no registry cleanup owner")
	}
	_ = NewCollectionManager(db)
	if got := columnAssetLifecycleRegistryProcessDBID(db); got != id {
		t.Fatalf("managers split registry identity: %d / %d", id, got)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_ = NewCollectionManager(db)
		}()
	}
	close(start)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	closed := NewCollectionManager(db)
	if !closed.isClosing() || closed.closeUnregister != nil || closed.commandWALRawUnregister != nil {
		t.Fatal("failed closed construction retained manager hooks")
	}
	if err := ensureColumnAssetLifecycleRegistryDB(db); err == nil {
		t.Fatal("closed backend admitted registry ownership")
	}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	if _, found := columnAssetLifecycleProcessRegistries.dbIDs[db]; found {
		t.Fatal("closed manager race leaked registry DB identity")
	}
	for _, record := range columnAssetLifecycleProcessRegistries.records {
		if record.Scope.dbID == id {
			t.Fatalf("closed manager race leaked registry record %d", record.ID)
		}
	}
}

// Source-only diagnostic: fixture writes, flush and snapshot acquisition are
// outside the timer; mapped part opening, owned builder rows and Close are in it.
func BenchmarkTypedGraphLifecycleRebuildSource(b *testing.B) {
	for _, mutated := range []bool{false, true} {
		b.Run(fmt.Sprintf("mutated=%v", mutated), func(b *testing.B) {
			_, db, col := openTypedMinimaCollection(b)
			defer db.Close()
			const count = 512
			ids, retained := make([][]byte, count), make([][]byte, count)
			vectors, strings := make([][]float32, count), make([]string, count)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("row-%04d", i))
				retained[i] = []byte(fmt.Sprintf(`{"id":"%s"}`, ids[i]))
				vectors[i] = []float32{1, float32(i + 1), 0, 0, 0, 0, 0, 0}
				strings[i] = "original"
			}
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: vectors}, {Name: "content", Strings: strings}, {Name: "user", Strings: strings}, {Name: "path", Strings: strings}}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				b.Fatal(err)
			}
			if mutated {
				for i := range columns {
					if i == 0 {
						columns[i].Float32Vectors = [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}}
					} else {
						columns[i].Strings = []string{"changed"}
					}
				}
				if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], columns); err != nil {
					b.Fatal(err)
				}
				if err := col.Delete(ids[1]); err != nil {
					b.Fatal(err)
				}
			}
			view, err := col.OpenCollectionReadView()
			if err != nil {
				b.Fatal(err)
			}
			defer view.Close()
			catalog := view.catalog
			records, err := loadColumnManifestRecordsFromRoot(view.snapshot, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)))
			if err != nil {
				b.Fatal(err)
			}
			manifest, err := decodeColumnManifestSnapshotForScan(records)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, source, used, err := col.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(view.snapshot, catalog, *catalog.meta.Options.ColumnStore, records, manifest, catalog.meta.VectorIndexes[0])
				want := count
				if mutated {
					want--
				}
				if err != nil || !used || len(rows) != want {
					b.Fatalf("rows=%d used=%v err=%v", len(rows), used, err)
				}
				if err := source.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func assertTypedGraphLifecycleRebuildSource(t *testing.T, col *Collection) {
	t.Helper()
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	catalog := view.catalog
	records, err := loadColumnManifestRecordsFromRoot(view.snapshot, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatal(err)
	}
	def := catalog.meta.VectorIndexes[0]
	rows, source, used, err := col.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(view.snapshot, catalog, *catalog.meta.Options.ColumnStore, records, manifest, def)
	if err != nil || !used || source == nil || source.closed {
		t.Fatalf("latest typed source used=%v source=%v err=%v", used, source, err)
	}
	defer source.Close()
	usedGenerations := make(map[uint64]bool)
	for _, row := range rows {
		_, err := view.visitDocumentRowRefsByID([][]byte{row.ID}, func(_ []byte, ref DocumentRowRef, found bool) error {
			got := row.BaseRowRef
			if !found || got.Generation != ref.Generation || got.PartID != ref.PartID || got.RowIndex != ref.RowIndex || got.AppliedCommandLSN != ref.AppliedCommandLSN {
				t.Fatalf("builder ref=%+v latest=%+v found=%v", got, ref, found)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		usedGenerations[row.BaseRowRef.Generation] = true
		borrowed := false
		for _, part := range source.parts {
			if part.generation == row.BaseRowRef.Generation {
				borrowed = &row.Vector[0] == &part.values[row.BaseRowRef.RowIndex*def.Dimensions]
			}
		}
		if !borrowed {
			t.Fatalf("row %q vector was not borrowed from its typed mapping", row.ID)
		}
	}
	if len(source.parts) != len(usedGenerations) {
		t.Fatalf("mapped %d generations for %d live generations", len(source.parts), len(usedGenerations))
	}
	if err := source.Close(); err != nil || !source.closed {
		t.Fatalf("source close: closed=%v err=%v", source.closed, err)
	}
	// Corrupt/missing locator state cannot fall back to indexed JSON. These
	// catalog copies are private test inputs; no persisted roots are changed.
	_, mutations, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, catalog.meta.Options.ColumnStore.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if mutations != 0 {
		badRoots := []uint64{0}
		if len(rows) != 0 {
			badRoots = append(badRoots, catalog.rootID(collectionPrimaryRootName(catalog.meta.Name)))
		}
		for _, locatorRoot := range badRoots {
			badCatalog := *catalog
			badCatalog.roots = maps.Clone(catalog.roots)
			badCatalog.roots[collectionColumnRowLocatorRootName(catalog.meta.Name)] = locatorRoot
			_, badSource, _, err := col.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(view.snapshot, &badCatalog, *catalog.meta.Options.ColumnStore, records, manifest, def)
			if badSource != nil {
				badSource.Close()
			}
			if err == nil {
				t.Fatalf("invalid locator root=%d admitted", locatorRoot)
			}
		}
	}
	if len(rows) != 0 {
		if err := validateColumnVectorGraphEmptyTypedSource(view.snapshot, catalog); err == nil {
			t.Fatal("empty-source proof accepted a live primary")
		}
		badCatalog := *catalog
		badCatalog.roots = maps.Clone(catalog.roots)
		badCatalog.roots[collectionPrimaryRootName(catalog.meta.Name)] = 0
		if err := validateColumnVectorGraphEmptyTypedSource(view.snapshot, &badCatalog); err == nil || !strings.Contains(err.Error(), collectionColumnRowLocatorRootName(catalog.meta.Name)) {
			t.Fatalf("empty-source proof accepted extra locator: %v", err)
		}
		_, badSource, _, err := col.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(view.snapshot, &badCatalog, *catalog.meta.Options.ColumnStore, records, manifest, def)
		if badSource != nil {
			badSource.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "locator without primary") {
			t.Fatalf("extra locator entries: %v", err)
		}
	}
}
