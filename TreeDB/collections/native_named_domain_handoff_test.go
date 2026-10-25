package collections

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Use the maintained durable typed asset hook solely to pause the real old
// complete-cut build. Both graph replacements use the ordinary HNSW API and
// canonical durable publication; no graph command or physical root is forged.
func TestNativeHNSWReplacementDuringCapturedBuild(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "graphs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("graphs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`), []byte(`{"embedding":[0.8,0.2]}`)}); err != nil {
		t.Fatal(err)
	}
	indexOptions := vectorIndexOptionsFromDefinition(def)
	idx, err := c.BuildVectorIndex(indexOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "build_pause", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
	}}}); err != nil {
		t.Fatal(err)
	}
	pauseCollection, err := manager.OpenCollection("build_pause")
	if err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var reads []*NativeJSONRead
	defer func() {
		for _, read := range reads {
			if err := read.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	capture := func() *NativeJSONRead {
		t.Helper()
		read, err := c.nativeJSON.Capture("graphs")
		if err != nil {
			t.Fatal(err)
		}
		reads = append(reads, read)
		return read
	}
	image := func(read *NativeJSONRead) map[string]string {
		t.Helper()
		result := map[string]string{}
		var start []byte
		for {
			key, value, found, err := read.namedDomainSeek("vector-index/embedding", start, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				break
			}
			result[string(key)] = string(value)
			start = append(bytes.Clone(key), 0)
		}
		return result
	}
	var finishCheckpoint func()
	defer func() {
		if finishCheckpoint != nil {
			finishCheckpoint()
		}
	}()
	for _, stage := range []string{"full", "empty"} {
		if _, err = idx.SaveSnapshot(); err != nil {
			t.Fatal(err)
		}
		if _, _, err = pauseCollection.InsertTypedBatchWithStats([][]byte{[]byte(stage)}, [][]byte{[]byte(`{"id":"` + stage + `"}`)}, []TypedColumnBatch{{Name: "label", Strings: []string{stage}}}); err != nil {
			t.Fatal(err)
		}
		old := capture()
		oldImage := image(old)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var once, releaseOnce sync.Once
		restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error { once.Do(func() { close(entered); <-release }); return nil })
		go func() { done <- database.Checkpoint() }()
		finishCheckpoint = func() {
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("captured checkpoint did not finish")
			}
			restore()
			finishCheckpoint = nil
		}
		select {
		case <-entered:
		case err := <-done:
			restore()
			finishCheckpoint = nil
			t.Fatalf("checkpoint missed typed preparation: %v", err)
		case <-time.After(15 * time.Second):
			t.Fatal("checkpoint did not reach typed preparation")
		}
		if stage == "full" {
			idx.TombstoneDocumentID([]byte("c"))
		} else {
			idx.TombstoneDocumentID([]byte("a"))
			idx.TombstoneDocumentID([]byte("b"))
		}
		if _, err = idx.SaveSnapshot(); err != nil {
			t.Fatal(err)
		}
		replacement := capture()
		replacementImage := image(replacement)
		if reflect.DeepEqual(oldImage, replacementImage) {
			t.Fatal("replacement did not change saved graph")
		}
		domain, _ := replacement.collection.domains.Get("vector-index/embedding")
		if domain.baseRoot != 0 {
			t.Fatal("replacement has a physical read base")
		}
		finishCheckpoint()
		current := capture()
		handed, _ := current.collection.domains.Get("vector-index/embedding")
		token, ok := current.capture.Snapshot().StateToken()
		if !ok {
			t.Fatal("physical token")
		}
		raw, err := current.capture.Snapshot().GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootPrefix+"graphs/vector-index/embedding"))
		if err != nil {
			t.Fatal(err)
		}
		root, err := decodeRootID(raw)
		if err != nil {
			t.Fatal(err)
		}
		if handed.baseRoot != 0 || handed.catalogRoot != root || handed.catalogRoot == domain.catalogRoot || handed.interpretation != domain.interpretation || handed.sourceGeneration != domain.sourceGeneration {
			t.Fatalf("old build changed replacement or lost catalog provenance: before=%+v after=%+v root=%d", domain, handed, root)
		}
		if !reflect.DeepEqual(image(old), oldImage) || !reflect.DeepEqual(image(replacement), replacementImage) || !reflect.DeepEqual(image(current), replacementImage) {
			t.Fatal("old handoff resurrected captured graph or changed held cuts")
		}
		refreshed, err := prepareNativeEngineBasisRefresh(current.capture.State().(*nativeEngineVersion), current.capture.Snapshot(), current.capture.Snapshot())
		if refreshed != nil {
			if cleanup := finishPrivateNative(refreshed.Release()); cleanup != nil {
				t.Error(cleanup)
			}
		}
		if err != nil {
			t.Fatalf("post-handoff equivalent basis: %v", err)
		}
		rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
		want := 2
		if stage == "empty" {
			want = 0
		}
		if err != nil || len(rows) != want {
			t.Fatalf("replacement search stage=%s rows=%+v err=%v", stage, rows, err)
		}
		if err = database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	budget := reads[len(reads)-1].capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
	for _, read := range reads {
		if err = read.Close(); err != nil {
			t.Fatal(err)
		}
	}
	reads = nil
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("original owner did not release: %+v", stats)
	}
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(handle)
	c, err = manager.OpenCollection("graphs")
	if err != nil {
		t.Fatal(err)
	}
	idx, loaded, err := c.LoadVectorIndexSnapshot(indexOptions)
	if err != nil || !loaded.Loaded {
		t.Fatalf("reopen load: %+v %v", loaded, err)
	}
	rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil || len(rows) != 0 {
		t.Fatalf("reopen resurrected graph: %+v %v", rows, err)
	}
}
