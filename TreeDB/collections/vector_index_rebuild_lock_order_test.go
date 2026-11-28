package collections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A raw barrier may retain a selected write domain even after its pending writes
// have drained. It must be able to take mutation ownership before either rebuild
// capture or final publication acquires that ownership.
// TryLock turns the otherwise unbounded lock cycle into a deterministic failure.
func TestVectorIndexRebuildDrainsRawBarriersBeforeMutation(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalized_%t/empty_%t", normalized, empty), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				if normalized {
					meta = cosineNormalizedF32V1TestMeta()
				}
				_, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				if !empty {
					if _, _, err := col.InsertTypedBatchWithStats(
						[][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)},
						cosineNormalizedF32V1TestColumns([]float32{1, 0, 0, 0, 0, 0, 0, 0}),
					); err != nil {
						t.Fatal(err)
					}
				}
				var calls atomic.Int32
				unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
					calls.Add(1)
					if !col.writeDomain.mutationMu.TryLock() {
						return errors.New("rebuild entered raw barrier while owning collection mutation")
					}
					col.writeDomain.mutationMu.Unlock()
					return nil
				})
				defer unregister()
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
				if got := calls.Load(); got != 2 {
					t.Fatalf("raw barrier calls=%d; want capture and final handoff drains, no nested drain", got)
				}
			})
		}
	}
}

func TestVectorIndexRebuildCheckpointPublicationHandoff(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, changedSource := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalized_%t/changed_source_%t", normalized, changedSource), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				if normalized {
					meta = cosineNormalizedF32V1TestMeta()
				}
				dir, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				put := func(id string) {
					t.Helper()
					if _, _, err := col.InsertTypedBatchWithStats(
						[][]byte{[]byte(id)}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, id))},
						cosineNormalizedF32V1TestColumns([]float32{1, 0, 0, 0, 0, 0, 0, 0}),
					); err != nil {
						t.Fatal(err)
					}
				}
				put("a")
				constructed, resumeConstruction := make(chan struct{}), make(chan struct{})
				resumeBuild := sync.OnceFunc(func() { close(resumeConstruction) })
				defer resumeBuild()
				restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
					close(constructed)
					<-resumeConstruction
				})
				defer restoreBuild()
				handedOff, resumePublication := make(chan struct{}), make(chan struct{})
				resumePublish := sync.OnceFunc(func() { close(resumePublication) })
				defer resumePublish()
				restoreHandoff := setColumnVectorGraphRebuildAfterMutationReleaseTestHook(func(current *Collection) {
					if current == col {
						close(handedOff)
						<-resumePublication
					}
				})
				defer restoreHandoff()
				wait := func(done <-chan struct{}, stage string) {
					t.Helper()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatalf("timeout waiting for %s", stage)
					}
				}
				rebuilt := make(chan error, 1)
				go func() { _, err := col.RebuildVectorIndex("embedding_graph"); rebuilt <- err }()
				wait(constructed, "paused construction")
				// A sibling/raw command must not be serialized behind graph construction.
				published := make(chan error, 1)
				go func() { published <- db.Set([]byte("unrelated"), []byte("retained")) }()
				select {
				case err := <-published:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("graph construction held global publication ownership")
				}
				selected, tryMutation := make(chan struct{}), make(chan struct{})
				allowMutation := sync.OnceFunc(func() { close(tryMutation) })
				var barrierCalls atomic.Int32
				unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
					if barrierCalls.Add(1) == 1 {
						close(selected)
						<-tryMutation
					}
					if !col.writeDomain.mutationMu.TryLock() {
						return errors.New("checkpoint barrier could not acquire released rebuild mutation")
					}
					col.writeDomain.mutationMu.Unlock()
					return nil
				})
				defer func() { allowMutation(); unregister() }()
				checkpointed := make(chan error, 1)
				go func() { checkpointed <- db.Checkpoint() }()
				wait(selected, "checkpoint raw-barrier ownership")
				resumeBuild()
				wait(handedOff, "rebuild mutation release")
				allowMutation()
				select {
				case err := <-checkpointed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("checkpoint did not complete through rebuild handoff")
				}
				if changedSource {
					put("b")
					// Finish the accepted source command before measuring rejection;
					// the final raw drain may legitimately publish lower commands.
					if err := col.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				beforeCommit, beforeSystem := dbCommitSeqAndSystemRoot(db)
				beforeFrames := countCollectionCommandWALFrames(t, dir)
				resumePublish()
				select {
				case err := <-rebuilt:
					if changedSource && (err == nil || (!errors.Is(err, ErrConcurrentMutation) && !strings.Contains(err.Error(), `[stage=root_descriptor_system_delta diff=manifest_progress]`))) {
						t.Fatalf("changed source rebuild=%v; want concurrent-mutation rejection", err)
					}
					if !changedSource && err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("rebuild did not finish after checkpoint")
				}
				if changedSource {
					commit, system := dbCommitSeqAndSystemRoot(db)
					if commit != beforeCommit || system != beforeSystem || countCollectionCommandWALFrames(t, dir) != beforeFrames {
						t.Fatal("source rejection changed a root or appended a rebuild frame")
					}
				}
				restoreBuild()
				restoreHandoff()
				unregister()
				if changedSource {
					if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
						t.Fatalf("fresh rebuild after source rejection: %v", err)
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				reopened := openTypedMinimaDB(t, dir)
				defer reopened.Close()
				current, err := NewCollectionManager(reopened).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				if normalized {
					// Lock handoff coverage above runs on every host; normalized
					// search additionally requires production holder authority.
					requireTypedGraphPublicServingTest(t)
					if err := current.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
						t.Fatal(err)
					}
				}
				query := VectorIndexSearchOptions{
					IndexName: "embedding_graph", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 2, EfSearch: 8,
				}
				var result VectorIndexSearchResponse
				if normalized {
					query.StatsMode = VectorIndexSearchStatsModeProduction
					query.QueryMode = VectorIndexQueryModeExact
					var buffer VectorIndexSearchBuffer
					var view *CollectionReadView
					result, view, err = current.SearchVectorIndexWithBufferReadView(query, &buffer)
					if view != nil {
						defer view.Close()
					}
				} else {
					result, err = current.SearchVectorIndex(query)
				}
				want := 1
				if changedSource {
					want = 2
				}
				if err != nil || len(result.Results) != want {
					t.Fatalf("reopened graph results=%+v err=%v; want%d documents", result.Results, err, want)
				}
				if got, err := reopened.Get([]byte("unrelated")); err != nil || string(got) != "retained" {
					t.Fatalf("reopened raw prefix=%q err=%v", got, err)
				}
			})
		}
	}
}

func TestVectorIndexRebuildReleasesRawWhileMutationIsBusy(t *testing.T) {
	for _, busyAt := range []int32{1, 2} {
		t.Run(fmt.Sprintf("barrier_%d", busyAt), func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			contended := make(chan struct{})
			var calls atomic.Int32
			var locked atomic.Bool
			release := func() {
				if locked.CompareAndSwap(true, false) {
					col.writeDomain.mutationMu.Unlock()
				}
			}
			unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
				if calls.Add(1) == busyAt {
					// Another mutation owner survives the completed raw drain.
					// Rebuild must release raw before waiting on that owner.
					col.writeDomain.mutationMu.Lock()
					locked.Store(true)
					close(contended)
				}
				return nil
			})
			defer func() { release(); unregister() }()
			rebuilt := make(chan error, 1)
			go func() { _, err := col.RebuildVectorIndex("embedding_graph"); rebuilt <- err }()
			select {
			case <-contended:
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild did not reach mutation contention")
			}
			published := make(chan error, 1)
			go func() { published <- db.Set([]byte("unrelated"), []byte("retained")) }()
			select {
			case err := <-published:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild waited on mutation while retaining raw")
			}
			release()
			select {
			case err := <-rebuilt:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild did not finish after mutation release")
			}
		})
	}
}

// A queued schema writer must not turn final rebuild publication into a nested
// schema-read acquisition. The immutable build keeps its original schema cut.
func TestColumnGraphRebuildCompletesWithQueuedSchemaWriter5275(t *testing.T) {
	_, db, col, def := openColumnGraphRebuildTestCollectionV2A(t, 2, 2, []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}},
	})
	defer db.Close()
	building, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	restore := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { close(building); <-resume })
	defer restore()
	rebuilt := make(chan error, 1)
	go func() { _, err := col.RebuildVectorIndex(def.Name); rebuilt <- err }()
	select {
	case <-building:
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild did not reach construction")
	}
	coord := col.collectionSchemaCoordinator()
	writerDone := make(chan struct{})
	go func() { coord.schemaMu.Lock(); coord.schemaMu.Unlock(); close(writerDone) }()
	waitForLockState(t, func() bool {
		if coord.schemaMu.TryRLock() {
			coord.schemaMu.RUnlock()
			return false
		}
		return true
	}, "schema writer to queue behind rebuild")
	release()
	select {
	case err := <-rebuilt:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild deadlocked with queued schema writer")
	}
	select {
	case <-writerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("schema writer did not acquire after rebuild")
	}
}

func openRepoChunks5921TestDB(t *testing.T, maxBufferedDocs int) (string, *backenddb.DB, func() error, *CollectionManager, *Collection) {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 512
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	backend, closeFn, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := NewCollectionManager(backend)
	colOpts := CollectionOptions{
		DocumentFormat:          DocumentFormatJSON,
		DataRootStoragePolicy:   RootStorageDefault,
		IndexStateStoragePolicy: RootStorageDefault,
		ColumnStore: &ColumnStoreConfig{
			Enabled:                 true,
			RetainedPayload:         ColumnRetainedPayloadNonColumn,
			RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			Columns: []ColumnStoreColumn{{
				Name:       "embedding",
				Path:       "embedding",
				Owner:      TypedStorageOwnerColumnPart,
				ValueType:  ColumnStoreValueFloat32Vector,
				VectorDims: 8,
			}},
		},
	}
	if maxBufferedDocs > 0 {
		colOpts.BufferedIndexedWriteMaxDocuments = maxBufferedDocs
	}
	meta := &CollectionMeta{
		Name:    "repo_chunks",
		Options: colOpts,
		Indexes: []IndexDefinition{
			{Name: "top_dir", Field: "meta.top_dir", ValueType: IndexValueString},
			{Name: "sub_dir", Field: "meta.sub_dir", ValueType: IndexValueString},
			{Name: "ext", Field: "meta.ext", ValueType: IndexValueString},
			{Name: "lang", Field: "meta.lang", ValueType: IndexValueString},
			{Name: "path", Field: "path", ValueType: IndexValueString},
			{Name: "chunk_parent", Field: "chunk_parent", ValueType: IndexValueString},
			{Name: "updated_at", Field: "meta.updated_at", ValueType: IndexValueInt64},
		},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			Analyzer:       TextAnalyzerSimple,
			StorePositions: true,
			Fields: []TextIndexField{
				{Field: "title", Weight: 3.0},
				{Field: "heading", Weight: 2.5},
				{Field: "path", Weight: 2.0},
				{Field: "body", Weight: 1.0},
			},
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Dimensions: 8,
			Metric:     VectorMetricCosine,
			Strategy:   VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedVectorIndexDefinition{{
				Name:    "embedding_scalar_u8",
				Codec:   QuantizedVectorCodecScalarU8,
				Version: 1,
			}},
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = closeFn()
		t.Fatalf("CreateCollection(repo_chunks): %v", err)
	}
	col, err := mgr.OpenCollection("repo_chunks")
	if err != nil {
		_ = closeFn()
		t.Fatalf("OpenCollection(repo_chunks): %v", err)
	}
	return dir, backend, closeFn, mgr, col
}

func makeRepoChunks5921Batch(prefix string, parents, chunksPerParent int, bodyPad string) ([][]byte, [][]byte, [][]float32, [][]byte) {
	total := parents * chunksPerParent
	ids := make([][]byte, 0, total)
	docs := make([][]byte, 0, total)
	vecs := make([][]float32, 0, total)
	parentIDs := make([][]byte, 0, parents)
	for p := 0; p < parents; p++ {
		parent := fmt.Sprintf("repo-file-%s-very-long-parent-path-%04d", prefix, p)
		parentIDs = append(parentIDs, []byte(parent))
		for c := 0; c < chunksPerParent; c++ {
			id := fmt.Sprintf("%s#%d", parent, c)
			vec := []float32{
				float32((p%7)+1) / 8.0,
				float32((c%5)+1) / 8.0,
				0.25, 0.5, 0.125, 0.375, 0.625, 0.75,
			}
			docObj := map[string]any{
				"title":         fmt.Sprintf("Title %s %d %d", prefix, p, c),
				"heading":       fmt.Sprintf("Section %d", c),
				"path":          fmt.Sprintf("docs/%s/file_%04d.md", prefix, p),
				"chunk_parent":  parent,
				"chunk_ordinal": c,
				"chunk_kind":    "chunk",
				"body":          fmt.Sprintf("quantum lexical vector chunk %s %d %d %s", prefix, p, c, bodyPad),
				"meta": map[string]any{
					"top_dir":    "docs",
					"sub_dir":    prefix,
					"ext":        ".md",
					"lang":       "markdown",
					"updated_at": int64(1700000000 + p*10 + c),
				},
				"embedding": vec,
			}
			raw, err := json.Marshal(docObj)
			if err != nil {
				panic(err)
			}
			ids = append(ids, []byte(id))
			docs = append(docs, raw)
			vecs = append(vecs, vec)
		}
	}
	return ids, docs, vecs, parentIDs
}

func TestVectorIndexRebuildTextV2DocMapRace5921(t *testing.T) {
	t.Run("case1_sequential_write_delete_insert_rebuild", func(t *testing.T) {
		_, _, closeFn, _, col := openRepoChunks5921TestDB(t, 4)
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		ids1, docs1, vecs1, parents1 := makeRepoChunks5921Batch("v1", 6, 2, "alpha beta")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			t.Fatalf("initial insert: %v", err)
		}
		if _, err := col.DeleteChunkedDocuments(parents1[:3]); err != nil {
			t.Fatalf("DeleteChunkedDocuments: %v", err)
		}
		ids2, docs2, vecs2, _ := makeRepoChunks5921Batch("v2", 6, 2, "gamma delta")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids2, docs2, "embedding", VectorMetricCosine, vecs2); err != nil {
			t.Fatalf("second insert: %v", err)
		}
		var asyncRunningAtHandoff atomic.Bool
		restoreHandoff := setColumnVectorGraphRebuildAfterMutationReleaseTestHook(func(current *Collection) {
			if current == col && col.writeDomain != nil && col.writeDomain.indexedAsyncFlushRunning() {
				asyncRunningAtHandoff.Store(true)
			}
		})
		defer restoreHandoff()
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatalf("RebuildVectorIndex after sequential write/delete/insert: %v", err)
		}
		if asyncRunningAtHandoff.Load() {
			t.Fatalf("indexedAsyncFlush was still running at preparePublication handoff")
		}
	})

	t.Run("case2_concurrent_maintain_text_index_serialized", func(t *testing.T) {
		_, _, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		ids1, docs1, vecs1, parents1 := makeRepoChunks5921Batch("m1", 8, 2, "alpha beta")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			t.Fatalf("initial insert: %v", err)
		}
		if _, err := col.DeleteChunkedDocuments(parents1[:4]); err != nil {
			t.Fatalf("DeleteChunkedDocuments: %v", err)
		}
		ids2, docs2, vecs2, _ := makeRepoChunks5921Batch("m2", 4, 2, "gamma delta")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids2, docs2, "embedding", VectorMetricCosine, vecs2); err != nil {
			t.Fatalf("second insert: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}
		building, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		defer release()
		restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
			close(building)
			<-resume
		})
		defer restoreBuild()
		rebuildErrCh := make(chan error, 1)
		go func() {
			_, err := col.RebuildVectorIndex("embedding_graph")
			rebuildErrCh <- err
		}()
		select {
		case <-building:
		case <-time.After(5 * time.Second):
			t.Fatal("rebuild did not reach build hook")
		}
		maintErrCh := make(chan error, 1)
		go func() {
			_, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Force: true})
			maintErrCh <- err
		}()
		coord := col.collectionSchemaCoordinator()
		waitForLockState(t, func() bool {
			if coord.schemaMu.TryRLock() {
				coord.schemaMu.RUnlock()
				return false
			}
			return true
		}, "MaintainTextIndex to queue behind RebuildVectorIndex schemaMu.RLock")
		release()
		select {
		case err := <-rebuildErrCh:
			if err != nil {
				t.Fatalf("RebuildVectorIndex with concurrent MaintainTextIndex: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("RebuildVectorIndex timed out")
		}
		select {
		case err := <-maintErrCh:
			if err != nil {
				t.Fatalf("MaintainTextIndex after RebuildVectorIndex: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("MaintainTextIndex timed out")
		}
	})

	t.Run("case3a_online_vlog_rewrite_docmap_only_during_build", func(t *testing.T) {
		dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
		// Insert enough chunks with long document IDs so text-v2-docmap/lexical
		// produces a block > ~4 KiB (pointerized into value_vlog per
		// collectionValueLogInlineThresholdResolverForRoot).
		ids1, docs1, vecs1, parents1 := makeRepoChunks5921Batch("r1", 64, 2, "x")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			_ = closeFn()
			t.Fatalf("initial insert: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			_ = closeFn()
			t.Fatalf("FlushAll(1): %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			_ = closeFn()
			t.Fatalf("Checkpoint(1): %v", err)
		}
		// Close and reopen (matching an incremental treedb-repo-mcp sync) so
		// value_vlog allocates fresh segments for the post-reopen
		// DeleteChunkedDocuments docmap update while surviving primary records
		// stay in the pre-reopen segments.
		if err := closeFn(); err != nil {
			t.Fatalf("close(1): %v", err)
		}
		var err error
		db, closeFn, mgr, col = reopenRepoChunks5921TestDB(t, dir)
		closed := false
		defer func() {
			if !closed {
				if err := closeFn(); err != nil {
					t.Fatalf("close db: %v", err)
				}
			}
		}()
		if _, err := col.DeleteChunkedDocuments(parents1[:2]); err != nil {
			t.Fatalf("DeleteChunkedDocuments: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			t.Fatalf("FlushAll(2): %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(2): %v", err)
		}
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		cat, err := loadCollectionCatalog(snap, "repo_chunks")
		if err != nil || cat == nil {
			_ = snap.Close()
			t.Fatalf("loadCollectionCatalog: cat=%v err=%v", cat, err)
		}
		docMapRootName := collectionTextV2DocMapRootName("repo_chunks", "lexical")
		beforeDocMapRoot := cat.rootID(docMapRootName)
		primaryFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, cat.rootID(collectionPrimaryRootName("repo_chunks")))
		docMapFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, beforeDocMapRoot)
		_ = snap.Close()
		var docMapOnlyFileIDs []uint32
		for fid := range docMapFileIDs {
			if _, inPrimary := primaryFileIDs[fid]; !inPrimary {
				docMapOnlyFileIDs = append(docMapOnlyFileIDs, fid)
			}
		}
		if len(docMapOnlyFileIDs) == 0 {
			t.Fatalf("expected text-v2-docmap/lexical to have a distinct value_vlog segment from primary (primary=%v docmap=%v)", primaryFileIDs, docMapFileIDs)
		}
		var buildCalls atomic.Int32
		var rewriteStats backenddb.ValueLogRewriteStats
		var rewriteErr error
		restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
			if buildCalls.Add(1) == 1 {
				rewriteStats, rewriteErr = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
					SourceFileIDs: docMapOnlyFileIDs,
					BatchSize:     16,
					SyncEachBatch: true,
				})
			}
		})
		defer restoreBuild()
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatalf("RebuildVectorIndex failed after online ValueLogRewriteOnline on text-v2-docmap: %v", err)
		}
		if rewriteErr != nil {
			t.Fatalf("ValueLogRewriteOnline(docmap-only) during build failed: %v", rewriteErr)
		}
		if rewriteStats.RecordsCopied == 0 {
			t.Fatalf("expected ValueLogRewriteOnline(docmap-only) to copy >=1 record, got %+v", rewriteStats)
		}
		if got := buildCalls.Load(); got != 1 {
			t.Fatalf("Option B fast path expected 1 build attempt for text-v2-docmap rewrite, got %d", got)
		}
		// Verify the cached catalog after publication has the fresh post-rewrite
		// text-v2-docmap root ID (not the stale pre-rewrite root ID).
		postSnap := db.AcquireSnapshot()
		if postSnap == nil {
			t.Fatal("AcquireSnapshot(post) returned nil")
		}
		postCat, err := loadCollectionCatalog(postSnap, "repo_chunks")
		_ = postSnap.Close()
		if err != nil || postCat == nil {
			t.Fatalf("loadCollectionCatalog(post): cat=%v err=%v", postCat, err)
		}
		afterDocMapRoot := postCat.rootID(docMapRootName)
		if afterDocMapRoot == 0 || afterDocMapRoot == beforeDocMapRoot {
			t.Fatalf("expected text-v2-docmap root to advance via ValueLogRewriteOnline: before=%d after=%d", beforeDocMapRoot, afterDocMapRoot)
		}
		col.catalogMu.RLock()
		cachedCat := col.catalog
		col.catalogMu.RUnlock()
		if cachedCat == nil || cachedCat.rootID(docMapRootName) != afterDocMapRoot {
			gotRoot := uint64(0)
			if cachedCat != nil {
				gotRoot = cachedCat.rootID(docMapRootName)
			}
			t.Fatalf("cached catalog retained stale text-v2-docmap root=%d; want %d", gotRoot, afterDocMapRoot)
		}

		survivingID := string(ids1[4])
		deletedID := string(ids1[0])
		survivingVec := vecs1[4]
		verifyRepoChunks5921SearchAndGC(t, db, col, "r1", survivingID, deletedID, survivingVec)
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(post-rebuild): %v", err)
		}
		if err := closeFn(); err != nil {
			t.Fatalf("close(post-rebuild): %v", err)
		}
		closed = true
		reopenDB, reopenClose, _, reopenCol := reopenRepoChunks5921TestDB(t, dir)
		defer func() {
			if err := reopenClose(); err != nil {
				t.Fatalf("close(reopened): %v", err)
			}
		}()
		verifyRepoChunks5921SearchAndGC(t, reopenDB, reopenCol, "r1", survivingID, deletedID, survivingVec)
	})

	t.Run("case3b_online_vlog_rewrite_primary_retry_and_gc", func(t *testing.T) {
		t.Run("repo_chunks_non_normalized", func(t *testing.T) {
			dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
			closed := false
			defer func() {
				if !closed {
					if err := closeFn(); err != nil {
						t.Fatalf("close db: %v", err)
					}
				}
			}()
			ids1, docs1, vecs1, parents1 := makeRepoChunks5921Batch("r2", 12, 2, "x")
			if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
				t.Fatalf("initial insert: %v", err)
			}
			if _, err := col.DeleteChunkedDocuments(parents1[:2]); err != nil {
				t.Fatalf("DeleteChunkedDocuments: %v", err)
			}
			if err := mgr.FlushAll(); err != nil {
				t.Fatalf("FlushAll: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			var buildCalls atomic.Int32
			var rewriteStats backenddb.ValueLogRewriteStats
			var rewriteErr error
			restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
				if buildCalls.Add(1) == 1 {
					rewriteStats, rewriteErr = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
						BatchSize:     16,
						SyncEachBatch: true,
					})
				}
			})
			defer restoreBuild()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatalf("RebuildVectorIndex failed after online ValueLogRewriteOnline on primary: %v", err)
			}
			if rewriteErr != nil {
				t.Fatalf("ValueLogRewriteOnline during build failed: %v", rewriteErr)
			}
			if rewriteStats.RecordsCopied == 0 {
				t.Fatalf("expected ValueLogRewriteOnline to copy >=1 record, got %+v", rewriteStats)
			}
			if got := buildCalls.Load(); got != 2 {
				t.Fatalf("Option C expected 2 build attempts after single primary physical rewrite, got %d", got)
			}
			survivingID := string(ids1[4])
			deletedID := string(ids1[0])
			survivingVec := vecs1[4]
			verifyRepoChunks5921SearchAndGC(t, db, col, "r2", survivingID, deletedID, survivingVec)
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(post-rebuild): %v", err)
			}
			if err := closeFn(); err != nil {
				t.Fatalf("close(post-rebuild): %v", err)
			}
			closed = true
			reopenDB, reopenClose, _, reopenCol := reopenRepoChunks5921TestDB(t, dir)
			defer func() {
				if err := reopenClose(); err != nil {
					t.Fatalf("close(reopened): %v", err)
				}
			}()
			verifyRepoChunks5921SearchAndGC(t, reopenDB, reopenCol, "r2", survivingID, deletedID, survivingVec)
		})

		t.Run("cosine_normalized_f32_v1_base_prepared_cleanup", func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			opts.IndexOuterLeavesInValueLog = true
			opts.ValueLog.PointerThreshold = 512
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			db, closeFn, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					if err := closeFn(); err != nil {
						t.Fatalf("close db: %v", err)
					}
				}
			}()
			meta := cosineNormalizedF32V1TestMeta()
			mgr := NewCollectionManager(db)
			if _, err := mgr.CreateCollection(&meta); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection(meta.Name)
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
			docs := [][]byte{
				[]byte(`{"id":"a"}`),
				[]byte(`{"id":"b"}`),
				[]byte(`{"id":"c"}`),
			}
			vecs := [][]float32{
				{1, 0, 0, 0, 0, 0, 0, 0},
				{0, 1, 0, 0, 0, 0, 0, 0},
				{0, 0, 1, 0, 0, 0, 0, 0},
			}
			columns := []TypedColumnBatch{
				{Name: "embedding", Float32Vectors: vecs},
				{Name: "content", Strings: []string{"alpha", "beta", "gamma"}},
				{Name: "user", Strings: []string{"u1", "u2", "u3"}},
				{Name: "path", Strings: []string{"file1", "file2", "file3"}},
			}
			if _, _, err := col.InsertTypedBatchWithStats(ids, docs, columns); err != nil {
				t.Fatalf("InsertTypedBatchWithStats: %v", err)
			}
			if err := mgr.FlushAll(); err != nil {
				t.Fatalf("FlushAll: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			var buildCalls atomic.Int32
			var rewriteErr error
			var rewriteStats backenddb.ValueLogRewriteStats
			restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
				if buildCalls.Add(1) == 1 {
					rewriteStats, rewriteErr = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
						BatchSize:     16,
						SyncEachBatch: true,
					})
				}
			})
			defer restoreBuild()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatalf("RebuildVectorIndex(normalized) failed after physical primary rewrite: %v", err)
			}
			if rewriteErr != nil {
				t.Fatalf("ValueLogRewriteOnline: %v", rewriteErr)
			}
			if rewriteStats.RecordsCopied == 0 {
				t.Fatalf("expected ValueLogRewriteOnline to copy >=1 record, got %+v", rewriteStats)
			}
			if got := buildCalls.Load(); got != 2 {
				t.Fatalf("normalized rebuild expected 2 attempts after single primary physical rewrite, got %d", got)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(post-rebuild): %v", err)
			}
			if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
				t.Fatalf("CloseVectorIndexPreparedSearchCache: %v", err)
			}
			if rootpublication.StableRelativeNamespaceSupported() {
				gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
				if err != nil {
					t.Fatalf("ColumnAssetGC after normalized retry failed: %v", err)
				}
				if gcStats.Plan.Segments.Unknown != 0 {
					t.Fatalf("ColumnAssetGC after normalized retry reported unknown_segments=%d; want 0 (plan=%+v)", gcStats.Plan.Segments.Unknown, gcStats.Plan.Segments)
				}
			}
			if err := closeFn(); err != nil {
				t.Fatalf("close(post-rebuild): %v", err)
			}
			closed = true
			reopenDB, reopenClose, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("reopen OpenBackendWithCachedLeafLog: %v", err)
			}
			defer func() {
				if err := reopenClose(); err != nil {
					t.Fatalf("close(reopened): %v", err)
				}
			}()
			reopenCol, err := NewCollectionManager(reopenDB).OpenCollection(meta.Name)
			if err != nil {
				t.Fatalf("reopen OpenCollection: %v", err)
			}
			if rootpublication.StableRelativeNamespaceSupported() {
				gcAfterReopen, err := reopenCol.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
				if err != nil {
					t.Fatalf("ColumnAssetGC after reopen failed: %v", err)
				}
				if gcAfterReopen.Plan.Segments.Unknown != 0 {
					t.Fatalf("ColumnAssetGC after reopen reported unknown_segments=%d; want 0", gcAfterReopen.Plan.Segments.Unknown)
				}
			}
		})
	})

	t.Run("case3c_bounded_retry_exhaustion_on_repeated_physical_rewrite", func(t *testing.T) {
		dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		ids1, docs1, vecs1, _ := makeRepoChunks5921Batch("r3", 6, 2, "x")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			t.Fatalf("initial insert: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		beforeFrames := countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb"))
		var buildCalls atomic.Int32
		restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
			buildCalls.Add(1)
			_, _ = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
				BatchSize:     16,
				SyncEachBatch: true,
			})
		})
		defer restoreBuild()
		_, rebuildErr := col.RebuildVectorIndex("embedding_graph")
		if rebuildErr == nil || !errors.Is(rebuildErr, ErrConcurrentMutation) {
			t.Fatalf("repeated physical rewrite rebuildErr=%v; want ErrConcurrentMutation", rebuildErr)
		}
		if got := buildCalls.Load(); got != maxColumnGraphRebuildPhysicalRootRewriteAttempts {
			t.Fatalf("buildCalls=%d; want bounded cap %d", got, maxColumnGraphRebuildPhysicalRootRewriteAttempts)
		}
		if gotFrames := countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb")); gotFrames != beforeFrames {
			t.Fatalf("exhausted rebuild appended CommandWAL frames: before=%d after=%d", beforeFrames, gotFrames)
		}
		gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{DryRun: !rootpublication.StableRelativeNamespaceSupported()})
		if err != nil || gcStats.Plan.Segments.Unknown != 0 {
			t.Fatalf("ColumnAssetGC after retry exhaustion: err=%v unknown_segments=%d", err, gcStats.Plan.Segments.Unknown)
		}
	})

	t.Run("case3d_online_vlog_rewrite_docmap_after_prepare_before_writeMu", func(t *testing.T) {
		dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
		ids1, docs1, vecs1, parents1 := makeRepoChunks5921Batch("r4", 64, 2, "x")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			_ = closeFn()
			t.Fatalf("initial insert: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			_ = closeFn()
			t.Fatalf("FlushAll(1): %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			_ = closeFn()
			t.Fatalf("Checkpoint(1): %v", err)
		}
		if err := closeFn(); err != nil {
			t.Fatalf("close(1): %v", err)
		}
		db, closeFn, mgr, col = reopenRepoChunks5921TestDB(t, dir)
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		if _, err := col.DeleteChunkedDocuments(parents1[:2]); err != nil {
			t.Fatalf("DeleteChunkedDocuments: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			t.Fatalf("FlushAll(2): %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(2): %v", err)
		}
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		cat, err := loadCollectionCatalog(snap, "repo_chunks")
		if err != nil || cat == nil {
			_ = snap.Close()
			t.Fatalf("loadCollectionCatalog: cat=%v err=%v", cat, err)
		}
		docMapRootName := collectionTextV2DocMapRootName("repo_chunks", "lexical")
		beforeDocMapRoot := cat.rootID(docMapRootName)
		primaryFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, cat.rootID(collectionPrimaryRootName("repo_chunks")))
		docMapFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, beforeDocMapRoot)
		_ = snap.Close()
		var docMapOnlyFileIDs []uint32
		for fid := range docMapFileIDs {
			if _, inPrimary := primaryFileIDs[fid]; !inPrimary {
				docMapOnlyFileIDs = append(docMapOnlyFileIDs, fid)
			}
		}
		if len(docMapOnlyFileIDs) == 0 {
			t.Fatalf("expected text-v2-docmap/lexical to have a distinct value_vlog segment from primary (primary=%v docmap=%v)", primaryFileIDs, docMapFileIDs)
		}
		var publishHookCalls atomic.Int32
		var rewriteStats backenddb.ValueLogRewriteStats
		var rewriteErr error
		restoreBeforePublish := setColumnVectorGraphRebuildBeforePublishTestHook(func() {
			if publishHookCalls.Add(1) == 1 {
				rewriteStats, rewriteErr = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
					SourceFileIDs: docMapOnlyFileIDs,
					BatchSize:     16,
					SyncEachBatch: true,
				})
			}
		})
		defer restoreBeforePublish()
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatalf("RebuildVectorIndex failed after pre-writeMu ValueLogRewriteOnline on text-v2-docmap: %v", err)
		}
		if rewriteErr != nil {
			t.Fatalf("ValueLogRewriteOnline(docmap-only) before writeMu failed: %v", rewriteErr)
		}
		if rewriteStats.RecordsCopied == 0 {
			t.Fatalf("expected ValueLogRewriteOnline(docmap-only) to copy >=1 record, got %+v", rewriteStats)
		}
		if got := publishHookCalls.Load(); got != 1 {
			t.Fatalf("expected 1 publish attempt, got %d", got)
		}
		postSnap := db.AcquireSnapshot()
		if postSnap == nil {
			t.Fatal("AcquireSnapshot(post) returned nil")
		}
		postCat, err := loadCollectionCatalog(postSnap, "repo_chunks")
		_ = postSnap.Close()
		if err != nil || postCat == nil {
			t.Fatalf("loadCollectionCatalog(post): cat=%v err=%v", postCat, err)
		}
		afterDocMapRoot := postCat.rootID(docMapRootName)
		if afterDocMapRoot == 0 || afterDocMapRoot == beforeDocMapRoot {
			t.Fatalf("expected text-v2-docmap root to advance via pre-writeMu ValueLogRewriteOnline: before=%d after=%d", beforeDocMapRoot, afterDocMapRoot)
		}
		col.catalogMu.RLock()
		cachedCat := col.catalog
		col.catalogMu.RUnlock()
		if cachedCat == nil || cachedCat.rootID(docMapRootName) != afterDocMapRoot {
			gotRoot := uint64(0)
			if cachedCat != nil {
				gotRoot = cachedCat.rootID(docMapRootName)
			}
			t.Fatalf("cached catalog retained stale text-v2-docmap root=%d after pre-writeMu rewrite; want %d", gotRoot, afterDocMapRoot)
		}
		verifyRepoChunks5921SearchAndGC(t, db, col, "r4", string(ids1[4]), string(ids1[0]), vecs1[4])
	})

	t.Run("case3e_typed_graph_fold_refreshes_catalog_after_intervening_text_root_rewrite", func(t *testing.T) {
		dir := t.TempDir()
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
		opts.IndexOuterLeavesInValueLog = true
		opts.ValueLog.PointerThreshold = 512
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		opts.BackgroundIndexVacuumInterval = -1
		db, closeFn, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
		}
		meta := typedMinimaCollectionMeta()
		meta.VectorIndexes[0].M = 16
		meta.TextIndexes = []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			Analyzer:       TextAnalyzerSimple,
			StorePositions: true,
			Fields: []TextIndexField{
				{Field: "content", Weight: 1.0},
			},
		}}
		mgr := NewCollectionManager(db)
		if _, err := mgr.CreateCollection(&meta); err != nil {
			_ = closeFn()
			t.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection(meta.Name)
		if err != nil {
			_ = closeFn()
			t.Fatalf("OpenCollection: %v", err)
		}
		const n = 96
		ids := make([][]byte, n)
		docs := make([][]byte, n)
		vecs := make([][]float32, n)
		contents := make([]string, n)
		users := make([]string, n)
		paths := make([]string, n)
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("typed-fold-long-document-identifier-prefix-%05d", i)
			ids[i] = []byte(id)
			contents[i] = fmt.Sprintf("quantum lexical fold content document %d", i)
			users[i] = fmt.Sprintf("u%d", i%8)
			paths[i] = "source"
			vecs[i] = vectorBenchmarkEmbedding(i, 8)
			docs[i] = []byte(fmt.Sprintf(`{"id":%q}`, id))
		}
		cols := []TypedColumnBatch{
			{Name: "embedding", Float32Vectors: vecs},
			{Name: "content", Strings: contents},
			{Name: "user", Strings: users},
			{Name: "path", Strings: paths},
		}
		if _, _, err := col.InsertTypedBatchWithStats(ids, docs, cols); err != nil {
			_ = closeFn()
			t.Fatalf("InsertTypedBatchWithStats: %v", err)
		}
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			_ = closeFn()
			t.Fatalf("initial RebuildVectorIndex: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			_ = closeFn()
			t.Fatalf("Checkpoint(1): %v", err)
		}
		if err := closeFn(); err != nil {
			t.Fatalf("close(1): %v", err)
		}
		db, closeFn, err = treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			t.Fatalf("reopen OpenBackendWithCachedLeafLog: %v", err)
		}
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		col, err = NewCollectionManager(db).OpenCollection(meta.Name)
		if err != nil {
			t.Fatalf("reopen OpenCollection: %v", err)
		}
		if err := col.Delete(ids[0]); err != nil {
			t.Fatalf("Delete(ids[0]): %v", err)
		}
		if err := col.Flush(); err != nil {
			t.Fatalf("Flush(2): %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(2): %v", err)
		}
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		cat, err := loadCollectionCatalog(snap, meta.Name)
		if err != nil || cat == nil {
			_ = snap.Close()
			t.Fatalf("loadCollectionCatalog: cat=%v err=%v", cat, err)
		}
		docMapRootName := collectionTextV2DocMapRootName(meta.Name, "lexical")
		beforeDocMapRoot := cat.rootID(docMapRootName)
		primaryFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, cat.rootID(collectionPrimaryRootName(meta.Name)))
		docMapFileIDs := collectRootValueLogFileIDsFor5921Test(t, snap, beforeDocMapRoot)
		_ = snap.Close()
		var docMapOnlyFileIDs []uint32
		for fid := range docMapFileIDs {
			if _, inPrimary := primaryFileIDs[fid]; !inPrimary {
				docMapOnlyFileIDs = append(docMapOnlyFileIDs, fid)
			}
		}
		if len(docMapOnlyFileIDs) == 0 {
			t.Fatalf("expected distinct value_vlog segment for text-v2-docmap (primary=%v docmap=%v)", primaryFileIDs, docMapFileIDs)
		}
		var rewriteStats backenddb.ValueLogRewriteStats
		var hookCalls atomic.Int32
		restoreFoldHook := setTypedGraphFoldBeforePublishTestHook(func() error {
			if hookCalls.Add(1) == 1 {
				var rewriteErr error
				rewriteStats, rewriteErr = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
					SourceFileIDs: docMapOnlyFileIDs,
					BatchSize:     16,
					SyncEachBatch: true,
				})
				return rewriteErr
			}
			return nil
		})
		defer restoreFoldHook()
		err = col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 256, typedGraphFoldTestAssetLimits(), nil)
		if err != nil {
			t.Fatalf("foldTypedGraph failed after online ValueLogRewriteOnline on text-v2-docmap: %v", err)
		}
		if got := hookCalls.Load(); got != 1 {
			t.Fatalf("typedGraphFoldBeforePublishTestHook calls=%d; want 1", got)
		}
		if rewriteStats.RecordsCopied == 0 {
			t.Fatalf("expected ValueLogRewriteOnline(docmap-only) to copy >=1 record, got %+v", rewriteStats)
		}
		postSnap := db.AcquireSnapshot()
		if postSnap == nil {
			t.Fatal("AcquireSnapshot(post) returned nil")
		}
		postCat, err := loadCollectionCatalog(postSnap, meta.Name)
		_ = postSnap.Close()
		if err != nil || postCat == nil {
			t.Fatalf("loadCollectionCatalog(post): cat=%v err=%v", postCat, err)
		}
		afterDocMapRoot := postCat.rootID(docMapRootName)
		if afterDocMapRoot == 0 || afterDocMapRoot == beforeDocMapRoot {
			t.Fatalf("expected text-v2-docmap root to advance during fold: before=%d after=%d", beforeDocMapRoot, afterDocMapRoot)
		}
		col.catalogMu.RLock()
		cachedCat := col.catalog
		col.catalogMu.RUnlock()
		if cachedCat == nil || cachedCat.rootID(docMapRootName) != afterDocMapRoot {
			gotRoot := uint64(0)
			if cachedCat != nil {
				gotRoot = cachedCat.rootID(docMapRootName)
			}
			t.Fatalf("cached catalog retained stale text-v2-docmap root=%d after fold; want %d", gotRoot, afterDocMapRoot)
		}
	})

	t.Run("case3f_unpublished_owned_segment_cleanup_fails_closed_on_rebound_identity_or_pin", func(t *testing.T) {
		if !rootpublication.StableRelativeNamespaceSupported() {
			t.Skip("unpublished owned segment cleanup requires stable relative namespace support")
		}
		segDir := t.TempDir()
		parent, err := os.Open(segDir)
		if err != nil {
			t.Fatalf("open segDir: %v", err)
		}
		parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
		_ = parent.Close()
		if err != nil {
			t.Fatalf("StableIdentityFromFile(parent): %v", err)
		}
		registry := rootpublication.NewIdentityPinRegistry()

		// 1. Rebound file identity at the same pathname must NOT be deleted.
		reboundName := columnAssetSegmentFileName(1)
		reboundPath := filepath.Join(segDir, reboundName)
		origPayload := []byte("original-owned-segment-bytes")
		if err := os.WriteFile(reboundPath, origPayload, 0o600); err != nil {
			t.Fatalf("write original: %v", err)
		}
		origFile, err := os.Open(reboundPath)
		if err != nil {
			t.Fatalf("open original: %v", err)
		}
		origIdentity, err := rootpublication.StableIdentityFromFile(origFile)
		_ = origFile.Close()
		if err != nil {
			t.Fatalf("StableIdentityFromFile(orig): %v", err)
		}
		// Replace path with a brand-new inode of identical size.
		replacementPayload := []byte("replaced-live-segment-bytes!")
		tmpPath := filepath.Join(segDir, "replacement.tmp")
		if err := os.WriteFile(tmpPath, replacementPayload, 0o600); err != nil {
			t.Fatalf("write replacement: %v", err)
		}
		if err := os.Rename(tmpPath, reboundPath); err != nil {
			t.Fatalf("rename replacement: %v", err)
		}
		if err := deleteUnpublishedRebuildOwnedSegment(registry, unpublishedRebuildOwnedSegmentTarget{
			assetPath:      reboundPath,
			segmentDir:     segDir,
			fileName:       reboundName,
			parentIdentity: parentIdentity,
			childIdentity:  origIdentity,
			expectedSize:   int64(len(origPayload)),
		}); err != nil {
			t.Fatalf("deleteUnpublishedRebuildOwnedSegment(rebound): %v", err)
		}
		got, err := os.ReadFile(reboundPath)
		if err != nil || string(got) != string(replacementPayload) {
			t.Fatalf("rebound replacement file was modified/deleted: got=%q err=%v", got, err)
		}

		// 2. Actively pinned physical identity must NOT be deleted; once unpinned,
		// exact-identity deletion removes it cleanly.
		pinnedName := columnAssetSegmentFileName(2)
		pinnedPath := filepath.Join(segDir, pinnedName)
		pinnedPayload := []byte("owned-segment-with-active-pin")
		if err := os.WriteFile(pinnedPath, pinnedPayload, 0o600); err != nil {
			t.Fatalf("write pinned: %v", err)
		}
		pinnedFile, err := os.Open(pinnedPath)
		if err != nil {
			t.Fatalf("open pinned: %v", err)
		}
		pinnedIdentity, err := rootpublication.StableIdentityFromFile(pinnedFile)
		_ = pinnedFile.Close()
		if err != nil {
			t.Fatalf("StableIdentityFromFile(pinned): %v", err)
		}
		if err := registry.Observe(pinnedIdentity); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		pin, err := registry.Pin(pinnedIdentity)
		if err != nil {
			t.Fatalf("Pin: %v", err)
		}
		target := unpublishedRebuildOwnedSegmentTarget{
			assetPath:      pinnedPath,
			segmentDir:     segDir,
			fileName:       pinnedName,
			parentIdentity: parentIdentity,
			childIdentity:  pinnedIdentity,
			expectedSize:   int64(len(pinnedPayload)),
		}
		if err := deleteUnpublishedRebuildOwnedSegment(registry, target); !errors.Is(err, rootpublication.ErrResourcePinned) {
			t.Fatalf("deleteUnpublishedRebuildOwnedSegment(pinned)=%v; want ErrResourcePinned", err)
		}
		if _, err := os.Stat(pinnedPath); err != nil {
			t.Fatalf("pinned segment should still exist: %v", err)
		}
		pin.Release()
		if err := registry.Unobserve(pinnedIdentity); err != nil {
			t.Fatalf("Unobserve: %v", err)
		}
		if err := deleteUnpublishedRebuildOwnedSegment(registry, target); err != nil {
			t.Fatalf("deleteUnpublishedRebuildOwnedSegment(unpinned): %v", err)
		}
		if _, err := os.Stat(pinnedPath); !os.IsNotExist(err) {
			t.Fatalf("unpinned owned segment should be deleted, stat err=%v", err)
		}
	})

	t.Run("case3g_retry_rejects_intervening_logical_write_between_attempts", func(t *testing.T) {
		dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
		defer func() {
			if err := closeFn(); err != nil {
				t.Fatalf("close db: %v", err)
			}
		}()
		ids1, docs1, vecs1, _ := makeRepoChunks5921Batch("r7", 6, 2, "x")
		if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
			t.Fatalf("initial insert: %v", err)
		}
		if err := mgr.FlushAll(); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		var buildCalls atomic.Int32
		restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
			if buildCalls.Add(1) == 1 {
				_, _ = db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
					BatchSize:     16,
					SyncEachBatch: true,
				})
			}
		})
		defer restoreBuild()
		var betweenErr error
		var beforeFrames int
		restoreRetry := setColumnVectorGraphRebuildBeforeRetryTestHook(func(attempt int) {
			if attempt == 1 {
				ids2, docs2, vecs2, _ := makeRepoChunks5921Batch("r7b", 1, 2, "between")
				_, _, betweenErr = col.InsertBatchWithStatsValidatedFloat32Projection(ids2, docs2, "embedding", VectorMetricCosine, vecs2)
				if betweenErr == nil {
					betweenErr = mgr.FlushAll()
				}
				beforeFrames = countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb"))
			}
		})
		defer restoreRetry()
		_, rebuildErr := col.RebuildVectorIndex("embedding_graph")
		if betweenErr != nil {
			t.Fatalf("intervening logical write between retry attempts failed: %v", betweenErr)
		}
		if !errors.Is(rebuildErr, ErrConcurrentMutation) {
			t.Fatalf("rebuild with logical write between retry attempts returned %v; want ErrConcurrentMutation", rebuildErr)
		}
		if got := buildCalls.Load(); got != 1 {
			t.Fatalf("retry after logical write must fail before attempt 2 build: buildCalls=%d, want 1", got)
		}
		if gotFrames := countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb")); gotFrames != beforeFrames {
			t.Fatalf("rejected retry appended CommandWAL frames: before=%d after=%d", beforeFrames, gotFrames)
		}
	})

	t.Run("case4_negative_controls_concurrent_logical_mutations_fail_closed", func(t *testing.T) {
		for _, op := range []string{"insert", "update", "delete"} {
			t.Run("repo_chunks/"+op, func(t *testing.T) {
				dir, db, closeFn, mgr, col := openRepoChunks5921TestDB(t, 0)
				defer func() {
					if err := closeFn(); err != nil {
						t.Fatalf("close db: %v", err)
					}
				}()
				ids1, docs1, vecs1, _ := makeRepoChunks5921Batch("neg", 4, 2, "initial")
				if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids1, docs1, "embedding", VectorMetricCosine, vecs1); err != nil {
					t.Fatalf("initial insert: %v", err)
				}
				if err := mgr.FlushAll(); err != nil {
					t.Fatalf("FlushAll: %v", err)
				}
				var buildCalls atomic.Int32
				restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
					buildCalls.Add(1)
				})
				defer restoreBuild()

				var mutErr error
				var beforeCommit, beforeSystem uint64
				var beforeFrames int
				var mutated atomic.Bool
				restoreHandoff := setColumnVectorGraphRebuildAfterMutationReleaseTestHook(func(current *Collection) {
					if current != col || !mutated.CompareAndSwap(false, true) {
						return
					}
					switch op {
					case "insert":
						ids2, docs2, vecs2, _ := makeRepoChunks5921Batch("neg2", 1, 2, "concurrent")
						_, _, mutErr = col.InsertBatchWithStatsValidatedFloat32Projection(ids2, docs2, "embedding", VectorMetricCosine, vecs2)
					case "update":
						updatedDoc := strings.Replace(string(docs1[0]), "initial", "mutated", 1)
						_, _, mutErr = col.Update(ids1[0], func(_ []byte) ([]byte, bool, error) {
							return []byte(updatedDoc), true, nil
						})
					case "delete":
						mutErr = col.Delete(ids1[0])
					}
					if mutErr == nil {
						mutErr = mgr.FlushAll()
					}
					beforeCommit, beforeSystem = dbCommitSeqAndSystemRoot(db)
					beforeFrames = countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb"))
				})
				defer restoreHandoff()

				_, rebuildErr := col.RebuildVectorIndex("embedding_graph")
				if mutErr != nil {
					t.Fatalf("concurrent %s failed: %v", op, mutErr)
				}
				if rebuildErr == nil || (!errors.Is(rebuildErr, ErrConcurrentMutation) && !strings.Contains(rebuildErr.Error(), `[stage=root_descriptor_system_delta diff=manifest_progress]`)) {
					t.Fatalf("concurrent %s rebuildErr=%v; want concurrent-mutation rejection", op, rebuildErr)
				}
				if got := buildCalls.Load(); got != 1 {
					t.Fatalf("concurrent %s must NOT retry rebuild: buildCalls=%d, want 1", op, got)
				}
				commit, system := dbCommitSeqAndSystemRoot(db)
				if commit != beforeCommit || system != beforeSystem || countCollectionCommandWALFrames(t, filepath.Join(dir, "maindb")) != beforeFrames {
					t.Fatalf("concurrent %s rejection mutated root or appended CommandWAL frame", op)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
				gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{DryRun: !rootpublication.StableRelativeNamespaceSupported()})
				if err != nil || gcStats.Plan.Segments.Unknown != 0 {
					t.Fatalf("ColumnAssetGC after concurrent %s rejection: err=%v unknown_segments=%d", op, err, gcStats.Plan.Segments.Unknown)
				}
			})
		}

		t.Run("cosine_normalized_f32_v1/insert_cleans_base_prepared", func(t *testing.T) {
			meta := cosineNormalizedF32V1TestMeta()
			dir, db, col := openTypedMinimaCollectionMeta(t, meta)
			defer func() {
				if err := db.Close(); err != nil {
					t.Fatalf("close db: %v", err)
				}
			}()
			if _, _, err := col.InsertTypedBatchWithStats(
				[][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)},
				cosineNormalizedF32V1TestColumns([]float32{1, 0, 0, 0, 0, 0, 0, 0}),
			); err != nil {
				t.Fatal(err)
			}
			var buildCalls atomic.Int32
			restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { buildCalls.Add(1) })
			defer restoreBuild()
			var mutErr error
			var beforeFrames int
			restoreHandoff := setColumnVectorGraphRebuildAfterMutationReleaseTestHook(func(current *Collection) {
				if current == col {
					_, _, mutErr = col.InsertTypedBatchWithStats(
						[][]byte{[]byte("b")}, [][]byte{[]byte(`{"id":"b"}`)},
						cosineNormalizedF32V1TestColumns([]float32{0, 1, 0, 0, 0, 0, 0, 0}),
					)
					if mutErr == nil {
						mutErr = col.Flush()
					}
					beforeFrames = countCollectionCommandWALFrames(t, dir)
				}
			})
			defer restoreHandoff()
			_, rebuildErr := col.RebuildVectorIndex("embedding_graph")
			if mutErr != nil {
				t.Fatalf("concurrent insert: %v", mutErr)
			}
			if rebuildErr == nil || (!errors.Is(rebuildErr, ErrConcurrentMutation) && !strings.Contains(rebuildErr.Error(), `[stage=root_descriptor_system_delta diff=manifest_progress]`)) {
				t.Fatalf("concurrent normalized insert rebuildErr=%v; want concurrent-mutation rejection", rebuildErr)
			}
			if got := buildCalls.Load(); got != 1 {
				t.Fatalf("concurrent normalized insert must NOT retry: buildCalls=%d, want 1", got)
			}
			if gotFrames := countCollectionCommandWALFrames(t, dir); gotFrames != beforeFrames {
				t.Fatalf("concurrent normalized insert appended rebuild frame: before=%d after=%d", beforeFrames, gotFrames)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if rootpublication.StableRelativeNamespaceSupported() {
				gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
				if err != nil || gcStats.Plan.Segments.Unknown != 0 {
					t.Fatalf("ColumnAssetGC after normalized concurrent insert rejection: err=%v unknown_segments=%d", err, gcStats.Plan.Segments.Unknown)
				}
			}
		})
	})
}

func reopenRepoChunks5921TestDB(t *testing.T, dir string) (*backenddb.DB, func() error, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 512
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	db, closeFn, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := NewCollectionManager(db)
	col, err := mgr.OpenCollection("repo_chunks")
	if err != nil {
		_ = closeFn()
		t.Fatalf("reopen OpenCollection(repo_chunks): %v", err)
	}
	return db, closeFn, mgr, col
}

func verifyRepoChunks5921SearchAndGC(t *testing.T, db *backenddb.DB, col *Collection, prefix, survivingID, deletedID string, survivingVec []float32) {
	t.Helper()
	for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedRerank} {
		var buf VectorIndexSearchBuffer
		opts := VectorIndexSearchOptions{
			IndexName: "embedding_graph",
			Query:     survivingVec,
			TopK:      200,
			EfSearch:  256,
			QueryMode: mode,
		}
		if mode == VectorIndexQueryModeQuantizedRerank {
			opts.QuantizedIndexName = "embedding_scalar_u8"
		}
		resp, err := col.SearchVectorIndexWithBuffer(opts, &buf)
		if err != nil {
			t.Fatalf("SearchVectorIndexWithBuffer(mode=%s): %v", mode, err)
		}
		foundSurvivor := false
		for _, item := range resp.Results {
			id := string(item.ID)
			if id == deletedID {
				t.Fatalf("SearchVectorIndexWithBuffer(mode=%s) returned deleted document %q", mode, deletedID)
			}
			if id == survivingID {
				foundSurvivor = true
			}
		}
		if !foundSurvivor {
			t.Fatalf("SearchVectorIndexWithBuffer(mode=%s) omitted surviving document %q (results=%d)", mode, survivingID, len(resp.Results))
		}
	}
	textResp, err := col.SearchText(TextSearchOptions{
		IndexName: "lexical",
		Query:     "quantum " + prefix,
		TopK:      200,
	})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	foundSurvivorText := false
	for _, item := range textResp.Results {
		id := string(item.DocumentID)
		if id == deletedID {
			t.Fatalf("SearchText returned deleted document %q", deletedID)
		}
		if id == survivingID {
			foundSurvivorText = true
		}
	}
	if !foundSurvivorText {
		t.Fatalf("SearchText omitted surviving document %q for prefix %q (results=%d)", survivingID, prefix, len(textResp.Results))
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint before ColumnAssetGC: %v", err)
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatalf("CloseVectorIndexPreparedSearchCache: %v", err)
	}
	gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{DryRun: !rootpublication.StableRelativeNamespaceSupported()})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if gcStats.Plan.Segments.Unknown != 0 {
		t.Fatalf("ColumnAssetGC reported unknown_segments=%d; want 0 (plan=%+v)", gcStats.Plan.Segments.Unknown, gcStats.Plan.Segments)
	}
}

func collectRootValueLogFileIDsFor5921Test(t *testing.T, snap *backenddb.Snapshot, rootID uint64) map[uint32]struct{} {
	t.Helper()
	out := make(map[uint32]struct{})
	if rootID == 0 {
		return out
	}
	it, err := snap.IteratorAtRootWithOptions(rootID, nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorAtRootWithOptions(%d): %v", rootID, err)
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && ptr.FileID != 0 {
			out[ptr.FileID] = struct{}{}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error on root %d: %v", rootID, err)
	}
	return out
}
