package collections

import (
	"fmt"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openNativeProducerAdmissionFixture(t *testing.T, meta *CollectionMeta) (*backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return backend, manager, col
}

// All four original bodies must become ready before any is released to durable
// completion. This catches a collection gate that silently turns a durable
// group into singletons. DDL must still wait through the last admitted body.
func TestNativeScalarProducerCohortRetainsDDLExclusion(t *testing.T) {
	const writers = 4
	backend, _, col := openNativeProducerAdmissionFixture(t, &CollectionMeta{Name: "docs"})
	for i := range writers {
		if _, err := col.Insert([]byte(fmt.Sprintf("row-%d", i)), []byte(`{"marker":0}`)); err != nil {
			t.Fatal(err)
		}
	}
	entered, done := make(chan struct{}, writers), make(chan error, writers)
	var releases [writers]chan struct{}
	var releaseOnce [writers]sync.Once
	for i := range writers {
		releases[i] = make(chan struct{})
	}
	release := func(i int) { releaseOnce[i].Do(func() { close(releases[i]) }) }
	completed := 0
	defer func() {
		for i := range writers {
			release(i)
		}
		for completed < writers {
			select {
			case err := <-done:
				completed++
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("producer did not finish after its callback was released")
				return
			}
		}
	}()
	for i := range writers {
		manager := NewCollectionManager(backend)
		t.Cleanup(func() { _ = manager.Close() })
		other, err := manager.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		go func(i int) {
			_, _, err := other.Update([]byte(fmt.Sprintf("row-%d", i)), func([]byte) ([]byte, bool, error) {
				entered <- struct{}{}
				<-releases[i]
				return []byte(`{"marker":1}`), true, nil
			})
			done <- err
		}(i)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for i := range writers {
		select {
		case <-entered:
		case err := <-done:
			completed++
			t.Fatalf("producer completed before the cohort was ready: %v", err)
		case <-deadline.C:
			t.Fatalf("only %d of %d original producer bodies became ready", i, writers)
		}
	}
	ddlDone := make(chan error, 1)
	go func() {
		_, err := col.CreateIndex(IndexDefinition{Name: "marker", Field: "marker", ValueType: IndexValueInt64})
		ddlDone <- err
	}()
	ddlJoined := false
	defer func() {
		for i := range writers {
			release(i)
		}
		if !ddlJoined {
			select {
			case err := <-ddlDone:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("DDL did not finish after producers were released")
			}
		}
	}()
	for i := 0; i < writers-1; i++ {
		release(i)
		select {
		case err := <-done:
			completed++
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("released producer did not complete")
		}
	}
	coord := col.collectionSchemaCoordinator()
	if coord.vectorDDLMutationMu.TryLock() {
		coord.vectorDDLMutationMu.Unlock()
		t.Fatal("DDL admission passed the last unreleased original producer")
	}
	select {
	case err := <-ddlDone:
		ddlJoined = true
		t.Fatalf("DDL completed before the last original producer: %v", err)
	default:
	}
	release(writers - 1)
	select {
	case err := <-done:
		completed++
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("last producer did not complete")
	}
	select {
	case err := <-ddlDone:
		ddlJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DDL did not follow the completed cohort")
	}
	ids, err := col.FindByIndexValue("marker", int64(1))
	if err != nil || len(ids) != writers {
		t.Fatalf("DDL did not index the complete admitted cohort: ids=%q err=%v", ids, err)
	}
}

// A declaration requires exclusive producer admission even before any live
// carrier is loaded. Reader admission would allow concurrent vector mutation.
func TestNativeDeclaredVectorProducerAdmissionIsExclusive(t *testing.T) {
	_, _, col := openNativeProducerAdmissionFixture(t, &CollectionMeta{
		Name: "vectors",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
		}},
		VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}},
	})
	unlock, _, err := col.prepareNativeProducerMutation()
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	coord := col.collectionSchemaCoordinator()
	if coord.vectorDDLMutationMu.TryRLock() {
		coord.vectorDDLMutationMu.RUnlock()
		t.Fatal("declared vector mutation received shared admission")
	}
}

// Hold real DDL authority across a schema transition while a producer starts
// from a different, stale handle. Its original transaction must open after DDL
// completes and maintain the newly declared scalar index.
func TestNativeProducerAdmissionObservesSchemaAfterConcurrentDDL(t *testing.T) {
	_, manager, col := openNativeProducerAdmissionFixture(t, &CollectionMeta{Name: "docs"})
	if _, err := col.Insert([]byte("row"), []byte(`{"marker":0}`)); err != nil {
		t.Fatal(err)
	}
	stale, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if err := col.nativeJSON.db.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	unlockDDL := col.lockVectorIndexMutation()
	var ddlOnce sync.Once
	releaseDDL := func() { ddlOnce.Do(unlockDDL) }
	defer releaseDDL()
	started, entered, done := make(chan struct{}), make(chan struct{}, 1), make(chan error, 1)
	go func() {
		close(started)
		_, _, err := stale.Update([]byte("row"), func([]byte) ([]byte, bool, error) {
			entered <- struct{}{}
			return []byte(`{"marker":1}`), true, nil
		})
		done <- err
	}()
	<-started
	joined := false
	defer func() {
		releaseDDL()
		if !joined {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("producer did not leave schema transition")
			}
		}
	}()
	// Invoke the exact public CreateIndex publication routine under its retained
	// mutation authority, so no scheduling delay is used to order the schema cut.
	def := IndexDefinition{Name: "marker", Field: "marker", ValueType: IndexValueInt64}
	if _, err := col.nativeJSON.mutateIndexFamilies(col, col.name, &def, nil, false, nil, "", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
		t.Fatal("producer body crossed retained DDL authority")
	default:
	}
	releaseDDL()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("producer did not observe completed DDL")
	}
	ids, err := col.FindByIndexValue("marker", int64(1))
	if err != nil || len(ids) != 1 || string(ids[0]) != "row" {
		t.Fatalf("producer failed to maintain the completed schema: %q %v", ids, err)
	}
	ids, err = col.FindByIndexValue("marker", int64(0))
	if err != nil || len(ids) != 0 {
		t.Fatalf("producer retained stale pre-DDL index state: %q %v", ids, err)
	}
}
