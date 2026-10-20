package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// These are ordinary document-backed native_runtime graphs, not imported
// ColumnGraph metadata. The initial native Create refusal is the first RED.
// The same witness must eventually retain old document/graph cuts while public
// CRUD immediately changes current search, and recover an actual kind103.
func TestNativeDocumentVectorPublicLifecycle(t *testing.T) {
	const childDirEnv = "GOMAP_NATIVE_DOCUMENT_VECTOR_DIR"
	const childFormatEnv = "GOMAP_NATIVE_DOCUMENT_VECTOR_FORMAT"
	const marker = "DOCUMENT VECTOR original103 durable and physically unapplied"
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		if os.Getenv(childDirEnv) != "" && os.Getenv(childFormatEnv) != string(format) {
			continue
		}
		t.Run(string(format), func(t *testing.T) {
			dir := os.Getenv(childDirEnv)
			child := dir != ""
			if !child {
				dir = t.TempDir()
			}
			opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff
			def := collections.VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 4, Strategy: collections.VectorIndexStrategyNativeRuntime}
			indexOptions := collections.VectorIndexOptions{Name: def.Name, Field: def.Field, Metric: def.Metric, Dimensions: def.Dimensions, M: def.M}
			var database *DB
			var backend *backenddb.DB
			var manager *collections.CollectionManager
			var col *collections.Collection
			var held *collections.CollectionReadView
			var heldGraph *collections.VectorIndexSearcher
			closeDB := func() {
				t.Helper()
				if heldGraph != nil {
					if err := heldGraph.Close(); err != nil {
						t.Fatal(err)
					}
					heldGraph = nil
				}
				if held != nil {
					if err := held.Close(); err != nil {
						t.Fatal(err)
					}
					held = nil
				}
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Fatal(err)
					}
					manager = nil
				}
				if database != nil {
					cache, registry := database.cached, backend.ValueLogIdentityPinRegistry()
					if err := database.Close(); err != nil {
						t.Fatal(err)
					}
					database = nil
					requirePublicCOWFinalZero(t, cache)
					if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
						t.Fatalf("document/vector resources remain: %+v", stats)
					}
				}
			}
			t.Cleanup(closeDB)
			openDB := func() {
				t.Helper()
				var err error
				database, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err = database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = collections.NewCollectionManager(backend)
			}
			document := func(id string, x, y float64) []byte {
				if format == collections.DocumentFormatBSON {
					return nativeBSONDocument(t, bson.D{{Key: "embedding", Value: bson.A{x, y}}, {Key: "name", Value: id}})
				}
				return []byte(fmt.Sprintf(`{"embedding":[%g,%g],"name":%q}`, x, y, id))
			}
			seed := [][]byte{document("a", 1, 0), document("b", 0, 1), document("c", .8, .2)}
			openDB()
			if child {
				var err error
				col, err = manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				assigned, physical := backend.CommandWALNextLSN(), indexedCRUDAppliedLSN(t, backend)
				index, err := col.BuildVectorIndex(indexOptions)
				if err != nil {
					t.Fatalf("public BuildVectorIndex: %v", err)
				}
				if status, err := index.SaveSnapshot(); err != nil || !status.Loaded {
					t.Fatalf("public SaveSnapshot: %+v %v", status, err)
				}
				if backend.CommandWALNextLSN() != assigned+1 || indexedCRUDAppliedLSN(t, backend) != physical {
					t.Fatal("original103 must assign once above the unchanged physical source")
				}
				requireNativeDocumentVectorFrame(t, opts, assigned)
				fmt.Println(marker)
				os.Exit(97)
			}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: format}, VectorIndexes: []collections.VectorIndexDefinition{def}}); err != nil {
				t.Fatalf("activated ordinary document-vector Create: %v", err)
			}
			var err error
			col, err = manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if ids, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, seed); err != nil || len(ids) != 3 {
				t.Fatalf("public document-vector InsertBatch: %q %v", ids, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeDB()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeDocumentVectorPublicLifecycle$", "-test.v")
			cmd.Env = append(os.Environ(), childDirEnv+"="+dir, childFormatEnv+"="+string(format))
			output, err := cmd.CombinedOutput()
			t.Logf("original103 child:\n%s", output)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("actual original103 producer crash: %v context=%v\n%s", err, ctx.Err(), output)
			}
			beforeReplay := workstats.Read().Replay
			openDB()
			afterReplay := workstats.Read().Replay
			if afterReplay.FramesApplied-beforeReplay.FramesApplied != 1 || afterReplay.FrameErrors != beforeReplay.FrameErrors {
				t.Fatalf("actual original103 replay: %+v -> %+v", beforeReplay, afterReplay)
			}
			col, err = manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			loaded, status, err := col.LoadVectorIndexSnapshot(indexOptions)
			if err != nil || loaded == nil || !status.Loaded || status.ExactFallbackReason != "" {
				t.Fatalf("load original graph: %+v %v", status, err)
			}
			// The existing Close-bearing searcher owns the old capture. An ordinary
			// VectorIndex has no Close and must not secretly pin a native cut.
			heldGraph, err = col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: def.Name})
			if err != nil {
				t.Fatalf("open snapshot-bound ordinary HNSW searcher: %v", err)
			}
			held, err = col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			checkOld := func() {
				t.Helper()
				rows, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, collections.DocumentFetchOptions{})
				if err != nil || len(rows.Results) != 4 {
					t.Fatalf("held documents: %+v %v", rows, err)
				}
				for i, want := range seed {
					if !rows.Results[i].Found || !bytes.Equal(rows.Results[i].Document, want) {
						t.Fatalf("held row%d: %+v", i, rows.Results[i])
					}
				}
				if rows.Results[3].Found {
					t.Fatal("held document cut observed later insert")
				}
				found, err := heldGraph.Search(collections.VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction})
				if err != nil || len(found.Results) != 1 || string(found.Results[0].ID) != "a" || found.Path != collections.VectorIndexSearchPathNativeRuntime {
					t.Fatalf("held graph changed: %+v %v", found, err)
				}
			}
			var buffer collections.VectorIndexSearchBuffer
			checkCurrent := func(want string) {
				t.Helper()
				found, err := col.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction}, &buffer)
				if err != nil || len(found.Results) != 1 || string(found.Results[0].ID) != want || found.Path != collections.VectorIndexSearchPathNativeRuntime || found.Stats.SearchRouteNativeRuntime == 0 {
					t.Fatalf("current native HNSW want=%s: %+v %v", want, found, err)
				}
			}
			checkOld()
			checkCurrent("a")
			updated := document("a", -1, 0)
			var matched, modified bool
			if format == collections.DocumentFormatBSON {
				matched, modified, err = col.UpdateBSONSet([]byte("a"), []collections.BSONSetField{{Key: "embedding", Value: nativeBSONQuery(t, bson.A{float64(-1), float64(0)})}})
			} else {
				matched, modified, err = col.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
					if !bytes.Equal(current, seed[0]) {
						return nil, false, fmt.Errorf("callback read %s want %s", current, seed[0])
					}
					return bytes.Clone(updated), true, nil
				})
			}
			if err != nil || !matched || !modified {
				t.Fatalf("ordinary document-vector update: %v %v %v", matched, modified, err)
			}
			checkCurrent("c") // No rebuild/Flush between mutation and search.
			checkOld()
			if deleted, err := col.DeleteDocument([]byte("c")); err != nil || !deleted {
				t.Fatalf("ordinary document-vector delete: %v %v", deleted, err)
			}
			checkCurrent("b")
			inserted := document("d", .9, .1)
			if _, err := col.InsertBatch([][]byte{[]byte("d")}, [][]byte{inserted}); err != nil {
				t.Fatal(err)
			}
			checkCurrent("d")
			for _, row := range []struct {
				id   string
				want []byte
			}{{"a", updated}, {"b", seed[1]}, {"c", nil}, {"d", inserted}} {
				if got, err := col.Get([]byte(row.id)); err != nil || !bytes.Equal(got, row.want) {
					t.Fatalf("current document %s=%x want=%x: %v", row.id, got, row.want, err)
				}
			}
			if _, err := col.RebuildVectorIndex(def.Name); err != nil {
				t.Fatalf("explicit ordinary native rebuild: %v", err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkCurrent("d")
			checkOld()
			closeDB()
			beforeReplay = workstats.Read().Replay
			openDB()
			afterReplay = workstats.Read().Replay
			if afterReplay.FramesApplied != beforeReplay.FramesApplied || afterReplay.FrameErrors != beforeReplay.FrameErrors {
				t.Fatalf("checkpointed ordinary graph replayed twice: %+v -> %+v", beforeReplay, afterReplay)
			}
			col, err = manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			loaded, status, err = col.LoadVectorIndexSnapshot(indexOptions)
			if err != nil || loaded == nil || !status.Loaded || status.ExactFallbackReason != "" {
				t.Fatalf("reopen graph: %+v %v", status, err)
			}
			if found, _, err := loaded.Search([]float32{1, 0}, collections.VectorIndexSearchOptions{TopK: 1, EfSearch: 8, DisableExactFallback: true}); err != nil || len(found) != 1 || string(found[0].DocumentID) != "d" {
				t.Fatalf("reopen graph survivors: %+v %v", found, err)
			}
			checkCurrent("d")
			closeDB()
		})
	}
}

func requireNativeDocumentVectorFrame(t *testing.T, opts Options, assigned uint64) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			frame, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if frame.LSN != assigned {
				continue
			}
			if found || frame.Kind != commitlog.CommandKindCollectionRebuildVectorIndex || frame.Scope != commitlog.CommandScopeCollection || frame.PayloadFormat != commitlog.PayloadFormatCollectionRebuildVectorIndexV1 {
				t.Fatalf("original103 assigned frame: %+v", frame)
			}
			payload, err := commitlog.DecodeCollectionRebuildVectorIndexPayload(frame.Payload)
			if err != nil || payload.Collection != "docs" || payload.IndexName != "embedding" {
				t.Fatalf("original103 payload: %+v %v", payload, err)
			}
			found = true
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatalf("original103 missing assigned frame%d", assigned)
	}
}
