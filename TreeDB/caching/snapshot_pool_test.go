package caching

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func newCachedSnapshotTestDB(t *testing.T) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached open: %v", err)
	}
	return cached, backend
}

func TestAcquireSnapshot_CachedPathAllocsAreBounded(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()

	if err := cached.Set([]byte("alloc-key"), []byte("alloc-value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	warm := cached.AcquireSnapshot()
	if warm == nil {
		t.Fatal("warm AcquireSnapshot=nil")
	}
	if warm.view == nil {
		_ = warm.Close()
		t.Fatal("warm snapshot used backend fast path; want cached path")
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm Close: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		snap := cached.AcquireSnapshot()
		if snap == nil {
			panic("AcquireSnapshot=nil")
		}
		if snap.view == nil {
			panic("AcquireSnapshot did not retain queued cached view")
		}
		if err := snap.Close(); err != nil {
			panic(err)
		}
	})
	// Each acquisition deliberately allocates distinct exported cached and
	// backend handles. Reusing either address would allow a stale pointer to
	// operate on a later snapshot; keep the safety cost bounded to those handles.
	if allocs > 2.1 {
		t.Fatalf("cached AcquireSnapshot allocs/run=%f want <= 2.1", allocs)
	}
}

func TestAcquireSnapshot_CachedPathSnapshotIsolationAcrossAcquisitions(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()

	key := []byte("snapshot-acquisition-isolation")
	if err := cached.Set(key, []byte("old")); err != nil {
		t.Fatalf("Set old: %v", err)
	}
	oldSnap := cached.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatal("old AcquireSnapshot=nil")
	}
	defer func() { _ = oldSnap.Close() }()

	if err := cached.Set(key, []byte("new")); err != nil {
		t.Fatalf("Set new: %v", err)
	}
	newSnap := cached.AcquireSnapshot()
	if newSnap == nil {
		t.Fatal("new AcquireSnapshot=nil")
	}
	defer func() { _ = newSnap.Close() }()

	gotOld, err := oldSnap.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("old snapshot GetAppend: %v", err)
	}
	if !bytes.Equal(gotOld, []byte("old")) {
		t.Fatalf("old snapshot value=%q want old", string(gotOld))
	}
	gotNew, err := newSnap.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("new snapshot GetAppend: %v", err)
	}
	if !bytes.Equal(gotNew, []byte("new")) {
		t.Fatalf("new snapshot value=%q want new", string(gotNew))
	}
}

func TestAcquireSnapshot_CachedPathConcurrentAcquireCloseWithWrites(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()

	key := []byte("snapshot-concurrent-cached-key")
	if err := cached.Set(key, []byte("v00")); err != nil {
		t.Fatalf("Set seed: %v", err)
	}
	seed := cached.AcquireSnapshot()
	if seed == nil {
		t.Fatal("seed AcquireSnapshot=nil")
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}

	var stop atomic.Bool
	errCh := make(chan error, 1)
	publishErr := func(err error) {
		if err == nil {
			return
		}
		if stop.CompareAndSwap(false, true) {
			errCh <- err
		}
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 250 && !stop.Load(); i++ {
				snap := cached.AcquireSnapshot()
				if snap == nil {
					publishErr(fmt.Errorf("worker %d acquire %d: nil snapshot", worker, i))
					return
				}
				got, err := snap.GetAppend(key, nil)
				closeErr := snap.Close()
				if err != nil {
					publishErr(fmt.Errorf("worker %d get %d: %w", worker, i, err))
					return
				}
				if len(got) == 0 {
					publishErr(fmt.Errorf("worker %d get %d: empty value", worker, i))
					return
				}
				if closeErr != nil {
					publishErr(fmt.Errorf("worker %d close %d: %w", worker, i, closeErr))
					return
				}
			}
		}(worker)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 25 && !stop.Load(); i++ {
			value := []byte(fmt.Sprintf("v%02d", i))
			if err := cached.Set(key, value); err != nil {
				publishErr(fmt.Errorf("writer set %d: %w", i, err))
				return
			}
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}
