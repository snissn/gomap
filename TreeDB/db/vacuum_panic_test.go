package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestVacuumRaceMissingKey simulates a write that commits while online vacuum is
// in progress. The vacuum recorder must capture the full batch.Entry so vacuum
// never needs to look up the key in a potentially stale snapshot.
func TestVacuumRaceMissingKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir, err := os.MkdirTemp("", "vacuum-race")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Dir:       dir,
		ChunkSize: 64 * 1024 * 1024,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const initialKeys = 1000
	b0 := db.NewBatch()
	for i := 0; i < initialKeys; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i))
		v := []byte(fmt.Sprintf("val-%06d", i))
		if err := b0.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := b0.Write(); err != nil {
		t.Fatal(err)
	}
	if err := b0.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	vacuumErrCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		vacuumErrCh <- db.VacuumIndexOnline(ctx)
	}()

	const newKeysStart = 2000
	const newKeysCount = 5000

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < newKeysCount; i++ {
			k := []byte(fmt.Sprintf("key-%06d", newKeysStart+i))
			v := []byte(fmt.Sprintf("val-%06d", newKeysStart+i))

			b := db.NewBatch()
			if err := b.Set(k, v); err != nil {
				errCh <- err
				return
			}
			if err := b.Write(); err != nil {
				errCh <- err
				return
			}
			_ = b.Close()

			if i%100 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent op error: %v", err)
	}
	if vacuumErr := <-vacuumErrCh; vacuumErr != nil {
		if !errors.Is(vacuumErr, ErrRecoverableRootSetStale) && !errors.Is(vacuumErr, ErrVacuumConcurrentMutation) {
			t.Fatalf("concurrent vacuum: %v", vacuumErr)
		}
		if err := db.VacuumIndexOnline(ctx); err != nil {
			t.Fatalf("post-churn vacuum retry: %v", err)
		}
	}

	missing := 0
	for i := 0; i < newKeysCount; i++ {
		k := []byte(fmt.Sprintf("key-%06d", newKeysStart+i))
		val, err := db.Get(k)
		if err != nil {
			missing++
			continue
		}
		expected := fmt.Sprintf("val-%06d", newKeysStart+i)
		if string(val) != expected {
			t.Errorf("corrupt value for %s: got %q, want %q", k, val, expected)
		}
	}

	if missing > 0 {
		t.Fatalf("consistency failure: %d/%d concurrent keys are missing after vacuum", missing, newKeysCount)
	}
}
