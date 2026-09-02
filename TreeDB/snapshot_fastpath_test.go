package treedb

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestAcquireSnapshot_BackendFastPathAfterCheckpointIsStable(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("snapshot-fast-path-key")
	if err := d.SetSync(key, []byte("old")); err != nil {
		t.Fatalf("SetSync old: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint old: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if _, ok := snap.(*backenddb.Snapshot); !ok {
		_ = snap.Close()
		t.Fatalf("AcquireSnapshot type=%T want backend fast-path snapshot", snap)
	}

	if err := d.SetSync(key, []byte("new")); err != nil {
		_ = snap.Close()
		t.Fatalf("SetSync new: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = snap.Close()
		t.Fatalf("checkpoint new: %v", err)
	}

	got, err := snap.GetAppend(key, nil)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("old snapshot GetAppend: %v", err)
	}
	if !bytes.Equal(got, []byte("old")) {
		_ = snap.Close()
		t.Fatalf("old snapshot value=%q want old", string(got))
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	next := d.AcquireSnapshot()
	if next == nil {
		t.Fatal("next AcquireSnapshot=nil")
	}
	defer next.Close()
	got, err = next.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("next snapshot GetAppend: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("next snapshot value=%q want new", string(got))
	}
}

func TestAcquireSnapshot_BackendFastPathAllocsAreBounded(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("alloc-key"), []byte("alloc-value")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	warm := d.AcquireSnapshot()
	if warm == nil {
		t.Fatal("warm AcquireSnapshot=nil")
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm Close: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		snap := d.AcquireSnapshot()
		if snap == nil {
			panic("AcquireSnapshot=nil")
		}
		if _, ok := snap.(*backenddb.Snapshot); !ok {
			panic("AcquireSnapshot did not use backend fast path")
		}
		if err := snap.Close(); err != nil {
			panic(err)
		}
	})
	// The backend fast path intentionally allocates one uniquely-addressed
	// exported handle. Reusing its address would let a stale pointer operate on a
	// later snapshot.
	if allocs > 1.1 {
		t.Fatalf("backend fast-path AcquireSnapshot allocs/run=%f want <= 1.1", allocs)
	}
}

func TestAcquireSnapshot_ConcurrentFastPathAcquireCloseWithCheckpoints(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("snapshot-concurrent-key")
	if err := d.SetSync(key, []byte("v00")); err != nil {
		t.Fatalf("SetSync seed: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
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
				snap := d.AcquireSnapshot()
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
			if err := d.SetSync(key, value); err != nil {
				publishErr(fmt.Errorf("writer set %d: %w", i, err))
				return
			}
			if err := d.Checkpoint(); err != nil {
				publishErr(fmt.Errorf("writer checkpoint %d: %w", i, err))
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

func TestAcquireSnapshot_MutableCachedWritesUseCachedSnapshot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("snapshot-mutable-key")
	if err := d.Set(key, []byte("cached")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	if _, ok := snap.(*caching.Snapshot); !ok {
		t.Fatalf("AcquireSnapshot type=%T want cached snapshot while mutable write is pending", snap)
	}
	got, err := snap.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("snapshot GetAppend: %v", err)
	}
	if !bytes.Equal(got, []byte("cached")) {
		t.Fatalf("snapshot value=%q want cached", string(got))
	}
}
