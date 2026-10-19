package caching

import (
	"errors"
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestConditionalTxnAdaptiveInactiveHistoryEnrollment(t *testing.T) {
	backend := newBlockingWriteBackend()
	cache, err := Open(t.TempDir(), backend, Options{
		MemtableMode: "adaptive", DisableWAL: true, RelaxedSync: true,
		AllowUnsafe: true, FlushThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})
	if cache.cow != nil || !cache.memtableAdaptive {
		t.Fatal("not adaptive routing")
	}
	if err := cache.SetSync([]byte("k"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	start := cache.conditionalSeq.Load()
	wb := cache.NewBatch()
	if err := wb.Set([]byte("k"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	backend.block.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		err := wb.WriteSync()
		if cerr := wb.Close(); err == nil {
			err = cerr
		}
		writeDone <- err
	}()
	released := false
	release := func() {
		if !released {
			close(backend.release)
			released = true
		}
	}
	defer release()
	select {
	case <-backend.entered:
	case err := <-writeDone:
		t.Fatalf("write missed publication gate: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("write did not reach backend")
	}
	// The real backend write has recorded its sequence, but still owns writeMu.
	if got := cache.conditionalSeq.Load(); got != start+1 {
		t.Fatalf("sequence=%d, want %d", got, start+1)
	}
	cache.conditionalOracle.mu.Lock()
	hasHistory := cache.conditionalOracle.history != nil
	cache.conditionalOracle.mu.Unlock()
	if hasHistory {
		t.Fatal("inactive adaptive publication allocated conflict history")
	}
	tx := new(ConditionalTxn)
	beginStarted, beginDone := make(chan struct{}), make(chan error, 1)
	go func() { close(beginStarted); beginDone <- cache.InitConditionalTxnWithSnapshot(tx) }()
	<-beginStarted
	select {
	case err := <-beginDone:
		if err == nil {
			_ = tx.Close()
		}
		t.Fatalf("Begin crossed in-flight publication: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if got := cache.conditionalActiveTxnCount.Load(); got != 0 {
		t.Fatalf("enrolled before publication: %d", got)
	}
	release()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-beginDone; err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if tx.startSeq != start+1 {
		t.Fatalf("opening sequence=%d", tx.startSeq)
	}
	value, err := tx.Get([]byte("k"))
	if err != nil || string(value) != "after" {
		t.Fatalf("opening value=%q err=%v", value, err)
	}
	if err := tx.Set([]byte("inside"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetSync([]byte("k"), []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("active conflict=%v", err)
	}
}

func TestConditionalTxnAdaptiveInactiveHistoryPreservesOtherConsumers(t *testing.T) {
	cache, err := Open(t.TempDir(), newBlockingWriteBackend(), Options{
		MemtableMode: "adaptive", DisableWAL: true, RelaxedSync: true, AllowUnsafe: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := cache.Set([]byte("k"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	a, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, _, err := b.GetVersioned([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("inside"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cache.conditionalActiveTxnCount.Load(); got != 1 {
		t.Fatalf("consumers=%d", got)
	}
	for _, value := range []string{"changed", "before"} {
		if err := cache.Set([]byte("k"), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("ABA conflict=%v", err)
	}
	j := cache.conditionalOracle.history
	if j == nil {
		t.Fatal("active consumer lost history")
	}
	count, bytes, floor := j.State()
	points := cache.conditionalOracleRecordedPoints.Load()
	seq := cache.conditionalSeq.Load()
	if err := cache.Set([]byte("idle"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete([]byte("idle")); err != nil {
		t.Fatal(err)
	}
	afterCount, afterBytes, afterFloor := j.State()
	if cache.conditionalOracle.history != j || count != afterCount || bytes != afterBytes || floor != afterFloor || cache.conditionalOracleRecordedPoints.Load() != points {
		t.Fatal("inactive publications changed retained consumer history")
	}
	if got := cache.conditionalSeq.Load(); got != seq+2 {
		t.Fatalf("inactive sequence=%d, want %d", got, seq+2)
	}
	// A later consumer opens at the advanced sequence, beyond the retained ABA.
	c, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.GetVersioned([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := c.Set([]byte("fresh"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(); err != nil {
		t.Fatalf("later consumer inherited old conflict: %v", err)
	}
}

func TestConditionalTxnAdaptiveActiveHistoryStillExpires(t *testing.T) {
	cache, err := Open(t.TempDir(), newBlockingWriteBackend(), Options{
		MemtableMode: "adaptive", DisableWAL: true, RelaxedSync: true, AllowUnsafe: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if err := tx.Set([]byte("inside"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Exceed the maintained 4096-record FIFO while the original consumer lives.
	for n := 0; n < 4097; n++ {
		if err := cache.Set([]byte(fmt.Sprintf("outside-%04d", n)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); !errors.Is(err, backenddb.ErrTxnTooOld) {
		t.Fatalf("expired consumer=%v", err)
	}
}
