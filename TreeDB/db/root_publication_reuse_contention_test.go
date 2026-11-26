package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
)

func TestDurableRootReuseCapabilityForPublishRespectsActiveReaderPin(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("SetSync v1: %v", err)
	}
	pinned := d.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = pinned.Close() }()
	pinnedSeq := pinned.State().CommitSeq

	for i := 2; i <= 6; i++ {
		if err := d.SetSync([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("SetSync v%d: %v", i, err)
		}
	}

	d.durablePublishMu.Lock()
	cap, err := d.durableRootReuseCapabilityForPublishV1(d.durableRoot)
	d.durablePublishMu.Unlock()
	if err != nil {
		t.Fatalf("durableRootReuseCapabilityForPublishV1: %v", err)
	}
	if cap == (freelist.ReuseCapability{}) {
		t.Fatal("expected non-zero reuse capability")
	}

	// The older pinned snapshot must still read its exact value even after
	// multiple durable seals and concurrent reads.
	got, err := pinned.Get([]byte("k"))
	if err != nil {
		t.Fatalf("pinned.Get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("pinned.Get=%q want %q", got, "v1")
	}
	if gotMin := d.minPinnedSnapshotCommitSeqForDurablePublish(d.state.Load().primaryRetentionCommitV5()); gotMin != pinnedSeq {
		t.Fatalf("minPinnedSnapshotCommitSeqForDurablePublish=%d want %d", gotMin, pinnedSeq)
	}
}

func TestFlushApplyReadContentionDetectorAndConcurrentMixedReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Pure write baseline should not trigger read-contention fallback.
	for i := 0; i < 4; i++ {
		if err := d.Set([]byte(fmt.Sprintf("seed-%04d", i)), []byte("val")); err != nil {
			t.Fatalf("Set seed %d: %v", i, err)
		}
	}
	if d.hasConcurrentReadContentionForFlushApply() {
		t.Fatal("unexpected read-contention detection without concurrent readers")
	}

	// Simulate heavy concurrent reads alongside writes, then verify durability across reopen.
	var stop atomic.Bool
	var wg sync.WaitGroup
	readErrCh := make(chan error, 1)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := d.Get([]byte("seed-0001")); err != nil {
					select {
					case readErrCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	for i := 0; i < 64; i++ {
		b := d.NewBatch()
		for j := 0; j < 16; j++ {
			k := []byte(fmt.Sprintf("mixed-%04d-%04d", i, j))
			v := []byte(fmt.Sprintf("value-%04d-%04d", i, j))
			if err := b.Set(k, v); err != nil {
				t.Fatalf("batch.Set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("batch.Write: %v", err)
		}
		_ = b.Close()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && d.snapshotAcquireEpoch.Load() < 128 {
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-readErrCh:
		t.Fatalf("concurrent reader failed: %v", err)
	default:
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for i := 0; i < 64; i += 8 {
		k := []byte(fmt.Sprintf("mixed-%04d-%04d", i, 7))
		want := fmt.Sprintf("value-%04d-%04d", i, 7)
		got, err := reopened.Get(k)
		if err != nil {
			t.Fatalf("reopened.Get(%q): %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("reopened.Get(%q)=%q want %q", k, got, want)
		}
	}
}
