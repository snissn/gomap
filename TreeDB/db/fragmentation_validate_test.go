package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
)

func TestValidateFragmentationReport_EndToEnd(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	valA := bytes.Repeat([]byte("a"), 48)
	valB := bytes.Repeat([]byte("b"), 48)

	// Create enough churn to produce internal pages and a non-trivial freelist.
	//
	// Group each churn phase into one root publication: this test validates
	// internal accounting and invariants, not per-key publication behavior. The
	// durable-root format intentionally uses a synchronous data-before-meta
	// transaction for every published root, including Write, so publishing each
	// key separately would turn this accounting fixture into tens of thousands
	// of filesystem barriers.
	const n = 20000
	insertBatch := d.NewBatch()
	defer insertBatch.Close()
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := insertBatch.Set(k, valA); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := insertBatch.Write(); err != nil {
		t.Fatalf("write insert batch: %v", err)
	}

	deleteBatch := d.NewBatch()
	defer deleteBatch.Close()
	for i := 0; i < n; i += 2 {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := deleteBatch.Delete(k); err != nil {
			t.Fatalf("del: %v", err)
		}
	}
	if err := deleteBatch.Write(); err != nil {
		t.Fatalf("write delete batch: %v", err)
	}

	rewriteBatch := d.NewBatch()
	defer rewriteBatch.Close()
	for i := 1; i < n; i += 2 {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := rewriteBatch.Set(k, valB); err != nil {
			t.Fatalf("set2: %v", err)
		}
	}
	if err := rewriteBatch.Write(); err != nil {
		t.Fatalf("write rewrite batch: %v", err)
	}

	// Advance commit seq enough for KeepRecent=1 pruning to take effect.
	if err := d.Set([]byte{0xFF, 0xFF, 0x00, 0x00}, valA); err != nil {
		t.Fatalf("set3: %v", err)
	}
	if err := d.Set([]byte{0xFF, 0xFF, 0x00, 0x01}, valA); err != nil {
		t.Fatalf("set4: %v", err)
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	if err := ValidateFragmentationReport(rep); err != nil {
		t.Fatalf("ValidateFragmentationReport: %v", err)
	}
}

func TestFragmentationReportWaitsForFreelistUpdate(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	idx := d.idx.Load()
	if idx == nil {
		t.Fatalf("missing index")
	}

	base, err := idx.pager.Alloc(2)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	headID := base
	entryID := base + 1
	if headID == 0 || entryID == 0 {
		t.Fatalf("unexpected page ids: head=%d entry=%d", headID, entryID)
	}

	if err := idx.allocator.Free(headID); err != nil {
		t.Fatalf("free head: %v", err)
	}

	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	freelist.TestHookRetireCOWBeforeUnlock = func() {
		select {
		case reached <- struct{}{}:
		default:
		}
		<-release
	}
	defer func() { freelist.TestHookRetireCOWBeforeUnlock = nil }()

	freeDone := make(chan error, 1)
	go func() {
		freeDone <- idx.allocator.Free(entryID)
	}()

	<-reached

	repDone := make(chan error, 1)
	go func() {
		rep, err := d.FragmentationReport()
		if err == nil {
			err = ValidateFragmentationReport(rep)
		}
		repDone <- err
	}()

	select {
	case err := <-repDone:
		t.Fatalf("expected FragmentationReport to block until freelist update completes, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-freeDone; err != nil {
		t.Fatalf("free entry: %v", err)
	}
	if err := <-repDone; err != nil {
		t.Fatalf("ValidateFragmentationReport: %v", err)
	}
}
