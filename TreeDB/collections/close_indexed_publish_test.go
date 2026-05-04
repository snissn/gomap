package collections

import (
	"bytes"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionManagerCloseWaitsForActiveIndexedPublish(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:     true,
			BufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert buffered indexed batch: %v", err)
	}

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare indexed async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare indexed async publish returned nil work")
	}
	defer collectionTestCloseIndexedFlushWork(work)
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("begin indexed async flush returned false")
	}

	waitEntered := make(chan struct{})
	allowWait := make(chan struct{})
	var waitEnteredOnce sync.Once
	var allowWaitOnce sync.Once
	releaseWait := func() {
		allowWaitOnce.Do(func() {
			close(allowWait)
		})
	}
	defer releaseWait()
	restoreWaitHook := setCollectionWaitIndexedAsyncFlushHookForTest(func() {
		waitEnteredOnce.Do(func() {
			close(waitEntered)
			<-allowWait
		})
	})
	defer restoreWaitHook()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- d.Close()
	}()
	select {
	case <-waitEntered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for Close to reach indexed async flush drain")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before active indexed publish finished: %v", err)
	default:
	}
	releaseWait()

	publishErr := col.publishPreparedIndexedFlush(work)
	col.writeDomain.finishIndexedAsyncFlush(publishErr)
	if publishErr != nil {
		t.Fatalf("publish prepared indexed flush: %v", publishErr)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close db: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for close after active indexed publish finished")
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened email ids=%q want [u1]", ids)
	}
}
