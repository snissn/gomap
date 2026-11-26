package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativeCommandWALDurablePairAdmissionHighConcurrency(t *testing.T) {
	runNativeCommandWALAdmissionHighConcurrency(t, ProfileCommandWALDurable, 4, 8, 8, 32)
}

func TestNativeCommandWALRelaxedAdmissionHighConcurrency(t *testing.T) {
	// Keep MaxSources > workers so concurrent workers cannot pin more distinct
	// cuts than MaxSources while stressing repeated full accepted-prefix ring
	// refills (8*64 = 512 inserts across a 100-slot ring).
	runNativeCommandWALAdmissionHighConcurrency(t, ProfileCommandWALRelaxed, 10, 10, 8, 64)
}

func runNativeCommandWALAdmissionHighConcurrency(t *testing.T, profile Profile, maxSources, maxGenerations, workers, perWorker int) {
	t.Helper()
	opts := OptionsFor(profile, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 1
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = maxSources
	opts.COWMemtableLimits.MaxGenerations = maxGenerations // maxSources*maxGenerations-slot ring
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1

	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name:    "items",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}); err != nil {
		_ = manager.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("items")
	if err != nil {
		_ = manager.Close()
		_ = database.Close()
		t.Fatal(err)
	}

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := []byte(fmt.Sprintf("w%02d-k%04d", worker, i))
				doc := []byte(fmt.Sprintf(`{"_id":"w%02d-k%04d","n":%d}`, worker, i, i))
				if err := col.NativewireInsertBatchNoResultIDs([][]byte{id}, [][]byte{doc}, false); err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d insert: %w", worker, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		_ = manager.Close()
		_ = database.Close()
		return
	}

	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			id := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
			want := []byte(fmt.Sprintf(`{"_id":"w%02d-k%04d","n":%d}`, w, i, i))
			got, err := col.Get(id)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("readback %q: got=%q want=%q err=%v", id, got, want, err)
			}
		}
	}

	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
}

func TestNativeCommandWALDurablePairAdmissionCancelsSpareOnConflictAndClosed(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 1
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = 2
	opts.COWMemtableLimits.MaxGenerations = 4 // 8-slot accepted-prefix ring (7 ordinary + 1 maintenance)
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1

	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name:    "items",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}); err != nil {
		_ = manager.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("items")
	if err != nil {
		_ = manager.Close()
		_ = database.Close()
		t.Fatal(err)
	}

	// Induce repeated concurrent-modification failures on durable native commits:
	// more than 7 failures would exhaust the 8-slot accepted-prefix ring if any
	// pre-reserved barrier spare leaked.
	nativeMgr, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 20; i++ {
		key := []byte(fmt.Sprintf("conflict-%02d", i))
		txA, err := nativeMgr.Begin("items")
		if err != nil {
			t.Fatalf("iter %d begin A: %v", i, err)
		}
		txB, err := nativeMgr.Begin("items")
		if err != nil {
			_ = txA.Close()
			t.Fatalf("iter %d begin B: %v", i, err)
		}
		docA := []byte(fmt.Sprintf(`{"_id":"conflict-%02d","v":%d}`, i, i*2-1))
		docB := []byte(fmt.Sprintf(`{"_id":"conflict-%02d","v":%d}`, i, i*2))
		if err := txA.InsertBatch([][]byte{key}, [][]byte{docA}); err != nil {
			_ = txA.Close()
			_ = txB.Close()
			t.Fatalf("iter %d stage A: %v", i, err)
		}
		if err := txB.InsertBatch([][]byte{key}, [][]byte{docB}); err != nil {
			_ = txA.Close()
			_ = txB.Close()
			t.Fatalf("iter %d stage B: %v", i, err)
		}
		if err := txA.Commit(); err != nil {
			_ = txB.Close()
			t.Fatalf("iter %d commit A: %v", i, err)
		}
		if err := txB.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
			t.Fatalf("iter %d commit B err=%v, want ErrConcurrentModification", i, err)
		}
	}

	// Verify RegisterNativeCommandWALWithSpare cancels a spare ticket cleanly
	// when invoked on an already-closed completion.
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closedComp := &nativePublicCommandWALCompletion{}
	for i := 0; i < 16; i++ {
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			t.Fatalf("reserve spare %d failed (possible leak): %v", i, err)
		}
		err = closedComp.RegisterNativeCommandWALWithSpare(func(bool) (uint64, error) { return 0, nil }, ticket)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("closed RegisterNativeCommandWALWithSpare err=%v, want ErrClosed", err)
		}
	}
	// Verify all 7 ordinary slots of the 8-slot ring (1 slot is reserved for
	// maintenance) are still available after all cancellations (zero leaks).
	var held []rootpublication.AcceptedTicket
	for i := 0; i < 7; i++ {
		ticket, err := prefix.ReserveAccepted(false)
		if err != nil {
			for _, h := range held {
				_ = h.CancelAccepted()
			}
			t.Fatalf("slot %d of 7 unavailable after cancellations: %v", i, err)
		}
		held = append(held, ticket)
	}
	for _, h := range held {
		if err := h.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
	}

	// Confirm further durable writes still succeed cleanly.
	for i := 0; i < 16; i++ {
		id := []byte(fmt.Sprintf("post-%02d", i))
		doc := []byte(fmt.Sprintf(`{"_id":"post-%02d","ok":true}`, i))
		if err := col.NativewireInsertBatchNoResultIDs([][]byte{id}, [][]byte{doc}, false); err != nil {
			t.Fatalf("post insert %d failed: %v", i, err)
		}
	}

	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
}
