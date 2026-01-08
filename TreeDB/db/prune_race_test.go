package db

import (
	"sync"
	"testing"
)

// This test is primarily intended to be run with -race.
// It validates that calling Prune concurrently with commits does not introduce
// a data race.
func TestPruneConcurrentWithCommits(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true, // isolate explicit Prune calls
		PreferAppendAlloc:      true, // Avoid freelist complexity for this test
		ChunkSize:              1 << 20,
		PruneInterval:          0,
		PruneMaxDuration:       0,
		PruneMaxPages:          0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Drive a lot of commit seq bumps while pruning in parallel.
	const iters = 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iters; i++ {
			// Using Commit directly or via batch to increment seq
			// Since this is the backend DB, we use NewBatch
			b := db.NewBatch()
			if err := b.Set([]byte("k"), []byte("v")); err != nil {
				_ = b.Close()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err := b.Write(); err != nil {
				_ = b.Close()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			_ = b.Close()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iters; i++ {
			db.Prune()
		}
	}()

	close(start)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}
