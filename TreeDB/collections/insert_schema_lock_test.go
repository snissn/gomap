package collections

import (
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionInsertFallbackDoesNotReacquireSchemaRead(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *CollectionManager, *Collection) []byte
		stale bool
	}{
		{
			name:  "stale scalar index",
			stale: true,
			setup: func(t *testing.T, manager *CollectionManager, stale *Collection) []byte {
				current, err := manager.OpenCollection("docs")
				if err != nil {
					t.Fatalf("open current collection: %v", err)
				}
				if _, err := current.CreateIndex(IndexDefinition{Name: "by_body", Field: "body", ValueType: IndexValueString}); err != nil {
					t.Fatalf("create index: %v", err)
				}
				return []byte(`{"body":"queued writer"}`)
			},
		},
		{
			name: "bson",
			setup: func(t *testing.T, _ *CollectionManager, _ *Collection) []byte {
				return mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "doc"}, {Key: "body", Value: "queued writer"}})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			t.Cleanup(func() { _ = d.Close() })
			manager := NewCollectionManager(d)
			meta := &CollectionMeta{Name: "docs"}
			if tc.name == "bson" {
				meta.Options.DocumentFormat = DocumentFormatBSON
			}
			if _, err := manager.CreateCollection(meta); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			stale, err := NewCollectionManager(d).OpenCollection("docs")
			if err != nil {
				t.Fatalf("open stale collection: %v", err)
			}
			document := tc.setup(t, manager, stale)
			if tc.stale && len(stale.meta.Indexes) != 0 {
				t.Fatal("test setup refreshed the stale collection metadata")
			}

			domain := stale.writeDomain
			domain.mu.Lock()
			domainLocked := true
			defer func() {
				if domainLocked {
					domain.mu.Unlock()
				}
			}()
			inserted := make(chan error, 1)
			go func() {
				_, err := stale.Insert([]byte("doc"), document)
				inserted <- err
			}()

			coord := stale.collectionSchemaCoordinator()
			waitForLockState(t, func() bool {
				if coord.schemaMu.TryLock() {
					coord.schemaMu.Unlock()
					return false
				}
				return true
			}, "insert to acquire schema read lock")

			writerStarted := make(chan struct{})
			writerAcquired := make(chan struct{})
			go func() {
				close(writerStarted)
				coord.schemaMu.Lock()
				close(writerAcquired)
				coord.schemaMu.Unlock()
			}()
			<-writerStarted
			waitForLockState(t, func() bool {
				if coord.schemaMu.TryRLock() {
					coord.schemaMu.RUnlock()
					return false
				}
				return true
			}, "schema writer to queue")

			domain.mu.Unlock()
			domainLocked = false
			select {
			case err := <-inserted:
				if err != nil {
					t.Fatalf("insert: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("insert deadlocked reacquiring the schema read lock")
			}
			select {
			case <-writerAcquired:
			case <-time.After(time.Second):
				t.Fatal("queued schema writer remained blocked after insert")
			}
		})
	}
}

func TestVectorIndexRebuildDoesNotDeadlockQueuedSchemaWriter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	locked := make(chan struct{})
	release := make(chan struct{})
	restore := setVectorIndexRebuildAfterLockHookForTest(func() {
		close(locked)
		<-release
	})
	t.Cleanup(restore)
	rebuilt := make(chan error, 1)
	go func() { rebuilt <- index.Rebuild() }()
	<-locked

	created := make(chan error, 1)
	go func() {
		_, err := col.CreateIndex(IndexDefinition{Name: "by_body", Field: "body", ValueType: IndexValueString})
		created <- err
	}()
	coord := col.collectionSchemaCoordinator()
	waitForLockState(t, func() bool {
		if coord.schemaMu.TryRLock() {
			coord.schemaMu.RUnlock()
			return false
		}
		return true
	}, "schema writer to queue behind rebuild")

	close(release)
	select {
	case err := <-rebuilt:
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rebuild deadlocked behind queued schema writer")
	}
	select {
	case err := <-created:
		if err != nil {
			t.Fatalf("create index: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("schema writer remained blocked after rebuild")
	}
}

func TestVectorIndexRebuildAcquiresAdmissionBeforeMutation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.Insert([]byte("a"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4,
	})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	peer, err := NewCollectionManager(d).OpenCollection(col.collectionName())
	if err != nil {
		t.Fatalf("open peer collection: %v", err)
	}

	mutation := col.lockMutation()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			mutation.Unlock()
		}
	}()
	rebuilt := make(chan error, 1)
	go func() { rebuilt <- index.Rebuild() }()
	coord := col.collectionSchemaCoordinator()
	waitForLockState(t, func() bool {
		if coord.adHocVectorAdmissionMu.TryRLock() {
			coord.adHocVectorAdmissionMu.RUnlock()
			return false
		}
		return true
	}, "vector rebuild to acquire ad-hoc admission")

	inserted := make(chan error, 1)
	go func() {
		_, err := peer.Insert([]byte("b"), []byte(`{"embedding":[0,1]}`))
		inserted <- err
	}()
	select {
	case err := <-inserted:
		t.Fatalf("insert passed rebuild admission before mutation release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	mutation.Unlock()
	mutationLocked = false
	select {
	case err := <-rebuilt:
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rebuild deadlocked against ordinary writer")
	}
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary writer remained blocked after rebuild")
	}
}

func waitForLockState(t *testing.T, ready func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		runtime.Gosched()
	}
}
