package collections

import (
	"bytes"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestInsertCallerProvidedID_StoresAndReturnsSameID(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("tenant-1-order")
	got, err := col.AllocateID(id)
	if err != nil {
		t.Fatalf("allocate provided id: %v", err)
	}
	if !bytes.Equal(got, id) {
		t.Fatalf("id mismatch: got=%q want=%q", got, id)
	}
}

func TestInsertAutoID_GeneratesMonotonicAndUnique(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeAuto,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	prev := []byte(nil)
	seen := map[string]struct{}{}
	for i := 0; i < 64; i++ {
		id, err := col.AllocateID(nil)
		if err != nil {
			t.Fatalf("allocate id %d: %v", i, err)
		}
		if i > 0 && bytes.Compare(prev, id) >= 0 {
			t.Fatalf("ids not monotonic: prev=%x new=%x", prev, id)
		}
		key := string(id)
		if _, exists := seen[key]; exists {
			t.Fatalf("id duplicate: %x", id)
		}
		seen[key] = struct{}{}
		prev = append(prev[:0], id...)
	}
}

func TestAutoIDPersistenceAcrossCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeAuto,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	first, err := col.AllocateID(nil)
	if err != nil {
		t.Fatalf("allocate first: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	mgr = NewCollectionManager(reopen)
	col, err = mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	second, err := col.AllocateID(nil)
	if err != nil {
		t.Fatalf("allocate second after reopen: %v", err)
	}
	if bytes.Compare(first, second) >= 0 {
		t.Fatalf("expected persisted auto-id to increase across reopen: first=%x second=%x", first, second)
	}
}

func TestInsertRejectsEmptyIDOrNilIDWhenExplicitRequired(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.AllocateID(nil); err == nil {
		t.Fatalf("expected nil id to be rejected")
	}
	if _, err := col.AllocateID([]byte{}); err == nil {
		t.Fatalf("expected empty id to be rejected")
	}
}

func TestAutoID_ConcurrentContention(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeAuto,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const workers = 16
	const perWorker = 16
	var wg sync.WaitGroup
	ids := make(chan []byte, workers*perWorker)
	errs := make(chan error, workers*perWorker)

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				id, err := col.AllocateID(nil)
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)

	for err := range errs {
		t.Fatalf("concurrent id allocation error: %v", err)
	}

	seen := make(map[string]struct{}, workers*perWorker)
	for id := range ids {
		key := string(id)
		if _, exists := seen[key]; exists {
			t.Fatalf("duplicate concurrent id: %x", id)
		}
		seen[key] = struct{}{}
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("expected %d ids, got %d", workers*perWorker, len(seen))
	}
}
