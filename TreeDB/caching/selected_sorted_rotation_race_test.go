package caching

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestSmallSelectedSortedBatchRaceWithIteratorRotation(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const iterations = 2000
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < iterations; i++ {
			key := []byte(fmt.Sprintf("key-%02d", i%64))
			value := []byte(fmt.Sprintf("value-%04d", i))
			batch := db.NewBatchWithSize(1)
			if err := batch.Set(key, value); err != nil {
				_ = batch.Close()
				errCh <- fmt.Errorf("batch set %d: %w", i, err)
				return
			}
			if err := batch.Write(); err != nil {
				_ = batch.Close()
				errCh <- fmt.Errorf("batch write %d: %w", i, err)
				return
			}
			if err := batch.Close(); err != nil {
				errCh <- fmt.Errorf("batch close %d: %w", i, err)
				return
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < iterations; i++ {
			it, err := db.Iterator(nil, nil)
			if err != nil {
				errCh <- fmt.Errorf("iterator %d: %w", i, err)
				return
			}
			for it.Valid() {
				_ = it.Key()
				_ = it.Value()
				it.Next()
			}
			iterErr := it.Error()
			closeErr := it.Close()
			if iterErr != nil || closeErr != nil {
				errCh <- fmt.Errorf("iterator %d drain=%v close=%v", i, iterErr, closeErr)
				return
			}
			runtime.Gosched()
		}
	}()
	close(start)
	workers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	finalKey := []byte(fmt.Sprintf("key-%02d", (iterations-1)%64))
	want := fmt.Sprintf("value-%04d", iterations-1)
	got, err := db.Get(finalKey)
	if err != nil {
		t.Fatalf("Get final key %q: %v", finalKey, err)
	}
	if string(got) != want {
		t.Fatalf("Get final key %q = %q, want %q", finalKey, got, want)
	}
}
