package collections

import (
	"fmt"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestFlushBufferedNoIndexLockFreeFastPath(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := NewCollectionManager(db)

	// 1. Indexed collection: no-index table stays empty, lock-free fast path
	// must not block on domain.mu.Lock() even if a reader holds domain.mu.RLock().
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users_indexed",
		Indexes: []IndexDefinition{{
			Name:      "age",
			Field:     "age",
			ValueType: IndexValueInt64,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection indexed: %v", err)
	}
	indexed, err := mgr.OpenCollection("users_indexed")
	if err != nil {
		t.Fatalf("OpenCollection indexed: %v", err)
	}
	ids := make([][]byte, 200)
	docs := make([][]byte, 200)
	for i := range docs {
		ids[i] = []byte(fmt.Sprintf("u%04d", i))
		docs[i] = []byte(fmt.Sprintf(`{"age":%d,"name":"user-%d"}`, 20+(i%50), i))
	}
	if _, err := indexed.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch indexed: %v", err)
	}
	if err := indexed.Flush(); err != nil {
		t.Fatalf("Flush indexed: %v", err)
	}
	if indexed.writeDomain.hasNoIndexTableWrites.Load() {
		t.Fatalf("indexed collection unexpectedly marked hasNoIndexTableWrites=true")
	}

	// Hold domain.mu.RLock(): if flushBufferedNoIndex tried to take domain.mu.Lock(),
	// this call would deadlock. With the lock-free fast path it returns immediately.
	indexed.writeDomain.mu.RLock()
	err = indexed.flushBufferedNoIndex()
	indexed.writeDomain.mu.RUnlock()
	if err != nil {
		t.Fatalf("flushBufferedNoIndex under RLock: %v", err)
	}

	// Concurrent indexed reads under -race.
	rangeOpts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(25), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(35), Inclusive: true},
		Limit: 10,
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 50; iter++ {
				gotIDs, _, scanErr := indexed.FindByIndexRange("age", rangeOpts)
				if scanErr != nil {
					errs <- scanErr
					return
				}
				if len(gotIDs) == 0 {
					errs <- fmt.Errorf("expected matches in age range")
					return
				}
				count := 0
				_, scanErr = indexed.ScanBorrowedDocumentsByIndexRange("age", rangeOpts, func(_ BorrowedDocumentRecord) (bool, error) {
					count++
					return true, nil
				})
				if scanErr != nil {
					errs <- scanErr
					return
				}
				if count != len(gotIDs) {
					errs <- fmt.Errorf("borrowed count=%d want %d", count, len(gotIDs))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent indexed read: %v", err)
	}

	// 2. No-index collection: buffered write sets hasNoIndexTableWrites,
	// flushBufferedNoIndex flushes it to durable storage and clears the flag.
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "notes_noindex"}); err != nil {
		t.Fatalf("CreateCollection no-index: %v", err)
	}
	noIdx, err := mgr.OpenCollection("notes_noindex")
	if err != nil {
		t.Fatalf("OpenCollection no-index: %v", err)
	}
	if _, err := noIdx.Insert([]byte("n1"), []byte(`{"body":"hello"}`)); err != nil {
		t.Fatalf("Insert no-index: %v", err)
	}
	if !noIdx.writeDomain.hasNoIndexTableWrites.Load() {
		t.Fatalf("expected hasNoIndexTableWrites=true after buffered no-index insert")
	}
	if err := noIdx.flushBufferedNoIndex(); err != nil {
		t.Fatalf("flushBufferedNoIndex: %v", err)
	}
	if noIdx.writeDomain.hasNoIndexTableWrites.Load() {
		t.Fatalf("expected hasNoIndexTableWrites=false after flushBufferedNoIndex")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 3. Reopen and verify persisted visibility.
	reopened, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedMgr := NewCollectionManager(reopened)
	reopenedNoIdx, err := reopenedMgr.OpenCollection("notes_noindex")
	if err != nil {
		t.Fatalf("OpenCollection no-index: %v", err)
	}
	got, err := reopenedNoIdx.Get([]byte("n1"))
	if err != nil {
		t.Fatalf("Get n1 after reopen: %v", err)
	}
	if string(got) != `{"body":"hello"}` {
		t.Fatalf("Get n1=%q want %q", string(got), `{"body":"hello"}`)
	}
}

func BenchmarkCollectionConcurrentIndexedRangeRead(b *testing.B) {
	dir := b.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{{
			Name:      "age",
			Field:     "age",
			ValueType: IndexValueInt64,
		}},
	}); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	const totalDocs = 10000
	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("u%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"age":%d,"email":"user%06d@example.com"}`, 18+(i%60), i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	opts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(30), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(40), Inclusive: true},
		Limit: 10,
	}

	for _, par := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("r%d", par), func(b *testing.B) {
			b.SetParallelism(par)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					count := 0
					_, scanErr := col.ScanBorrowedDocumentsByIndexRange("age", opts, func(_ BorrowedDocumentRecord) (bool, error) {
						count++
						return true, nil
					})
					if scanErr != nil {
						b.Error(scanErr)
						return
					}
					if count != 10 {
						b.Errorf("count=%d want 10", count)
						return
					}
				}
			})
		})
	}
}
