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
// complete-cut build. Both scalar domain replacements use ordinary index DDL
// and canonical durable publication; no command or physical root is forged.
func TestNativeScalarReplacementDuringCapturedBuild(t *testing.T) {
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
	def := IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "graphs", Indexes: []IndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("graphs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte(`{"tenant":"one"}`), []byte(`{"tenant":"one"}`), []byte(`{"tenant":"one"}`)}); err != nil {
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
			key, value, found, err := read.namedDomainSeek("index/tenant", start, nil, false)
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
		// The physical builder scans only pending domains. Seed an actual scalar
		// change after the prior checkpoint, so the paused old build owns this
		// domain and must advance its catalog provenance after replacement.
		changedID := []byte("c")
		if stage == "empty" {
			changedID = []byte("a")
		}
		if _, changed, updateErr := c.Update(changedID, func([]byte) ([]byte, bool, error) {
			return []byte(`{"tenant":"two"}`), true, nil
		}); updateErr != nil || !changed {
			t.Fatalf("seed captured scalar change stage=%s changed=%t err=%v", stage, changed, updateErr)
		}
		if _, _, err = pauseCollection.InsertTypedBatchWithStats([][]byte{[]byte(stage)}, [][]byte{[]byte(`{"id":"` + stage + `"}`)}, []TypedColumnBatch{{Name: "label", Strings: []string{stage}}}); err != nil {
			t.Fatal(err)
		}
		old := capture()
		capturedDomain, pending := old.collection.pending.Get("index/tenant")
		if !pending || capturedDomain == nil || ((capturedDomain.pending == nil || capturedDomain.pending.Len() == 0) && capturedDomain.captured == nil) {
			t.Fatalf("stage=%s old checkpoint has no pending scalar domain", stage)
		}
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
			if err = c.Delete([]byte("c")); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err = c.DeleteBatch([][]byte{[]byte("a"), []byte("b")}); err != nil {
				t.Fatal(err)
			}
		}
		// Adding a scalar index backfills the complete current image and replaces
		// the existing scalar domains while the older build is still paused.
		if _, err = c.CreateIndex(IndexDefinition{Name: "extra_" + stage, Field: "tenant", ValueType: IndexValueString}); err != nil {
			t.Fatal(err)
		}
		replacement := capture()
		replacementImage := image(replacement)
		if reflect.DeepEqual(oldImage, replacementImage) {
			t.Fatal("replacement did not change scalar image")
		}
		domain, _ := replacement.collection.domains.Get("index/tenant")
		if domain.baseRoot != 0 {
			t.Fatal("replacement has a physical read base")
		}
		finishCheckpoint()
		current := capture()
		handed, _ := current.collection.domains.Get("index/tenant")
		token, ok := current.capture.Snapshot().StateToken()
		if !ok {
			t.Fatal("physical token")
		}
		raw, err := current.capture.Snapshot().GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootPrefix+"graphs/index/tenant"))
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
			t.Fatal("old handoff resurrected captured scalar image or changed held cuts")
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
		rows, err := c.FindByIndex("tenant", "one")
		want := 2
		if stage == "empty" {
			want = 0
		}
		if err != nil || len(rows) != want {
			t.Fatalf("replacement scalar read stage=%s rows=%+v err=%v", stage, rows, err)
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
	rows, err := c.FindByIndex("tenant", "one")
	if err != nil || len(rows) != 0 {
		t.Fatalf("reopen resurrected scalar image: %+v %v", rows, err)
	}
}
