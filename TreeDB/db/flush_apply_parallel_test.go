package db

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func openFlushApplyTestDB(t *testing.T, concurrency int) *DB {
	t.Helper()
	d, err := Open(Options{
		Dir:                   t.TempDir(),
		ChunkSize:             64 * 1024,
		FlushApplyConcurrency: concurrency,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open(concurrency=%d): %v", concurrency, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func putBatch(t *testing.T, d *DB, start, count int, valuePrefix string) {
	t.Helper()
	b := d.NewBatch()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", start+i))
		val := []byte(fmt.Sprintf("%s-%06d", valuePrefix, start+i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set seed %d: %v", start+i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write seed: %v", err)
	}
}

func applyMixedFlushApplyBatch(t *testing.T, d *DB) {
	t.Helper()
	b := d.NewBatch()
	for i := 0; i < 6400; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("upd-%06d", i))); err != nil {
			t.Fatalf("Set update %d: %v", i, err)
		}
	}
	// Duplicate point op proves canonical newest-wins survives the opt-in path.
	if err := b.Set([]byte("key-000200"), []byte("first")); err != nil {
		t.Fatalf("duplicate set first: %v", err)
	}
	if err := b.Set([]byte("key-000200"), []byte("second")); err != nil {
		t.Fatalf("duplicate set second: %v", err)
	}
	for i := 101; i < 900; i += 4 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Delete(key); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	if err := b.DeleteRange([]byte("key-001500"), []byte("key-001650")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	for i := 7000; i < 7600; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("new-%06d", i))); err != nil {
			t.Fatalf("Set new %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write mixed: %v", err)
	}
}

func assertDBsEqualOnRange(t *testing.T, serial, parallel *DB, maxKey int) {
	t.Helper()
	for i := 0; i < maxKey; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		want, err := serial.Get(key)
		if err != nil {
			t.Fatalf("serial Get %q: %v", key, err)
		}
		got, err := parallel.Get(key)
		if err != nil {
			t.Fatalf("parallel Get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q: got %q want %q", key, got, want)
		}
	}
}

func TestFlushApplyConcurrencySerialParallelEquivalenceMixedOps(t *testing.T) {
	serial := openFlushApplyTestDB(t, 0)
	parallel := openFlushApplyTestDB(t, 4)
	putBatch(t, serial, 0, 7000, "base")
	putBatch(t, parallel, 0, 7000, "base")

	applyMixedFlushApplyBatch(t, serial)
	applyMixedFlushApplyBatch(t, parallel)

	assertDBsEqualOnRange(t, serial, parallel, 7800)
	if got, _ := parallel.Get([]byte("key-000200")); string(got) != "second" {
		t.Fatalf("newest duplicate value=%q want second", got)
	}
	if got, _ := parallel.Get([]byte("key-001550")); got != nil {
		t.Fatalf("range-deleted value=%q want nil", got)
	}
	stats := parallel.Stats()
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got == "" || got == "0" {
		t.Fatalf("read-only prepare calls stat=%q want >0", got)
	}
}

func TestFlushApplyConcurrencyUsesBoundedWorkerPoolForPointSpans(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 12000, "base")

	b := d.NewBatch()
	for i := 0; i < 9000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("p-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write point update: %v", err)
	}
	stats := d.Stats()
	if got := stats["treedb.flush_apply.merge_build.internal_parallel_merges_total"]; got == "" || got == "0" {
		t.Fatalf("internal parallel merge stat=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.merge_build.internal_parallel_workers_total"]; got == "" || got == "0" {
		t.Fatalf("internal parallel workers stat=%q want >0", got)
	}
}

func TestFlushApplyRootMismatchRetriesWithoutPublishingAbandonedWork(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")

	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := d.NewBatch()
		if err := other.Set([]byte("key-concurrent"), []byte("concurrent")); err != nil {
			t.Fatalf("concurrent Set: %v", err)
		}
		if err := other.Write(); err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	defer func() { d.testAfterOptimisticApplyHook = nil }()

	b := d.NewBatch()
	for i := 0; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("retry-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write with retry: %v", err)
	}
	if got, err := d.Get([]byte("key-concurrent")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent value got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "retry-000123" {
		t.Fatalf("retried value got=%q err=%v", got, err)
	}
	stats := d.Stats()
	if got := stats["treedb.flush_apply.mismatch_total"]; got == "" || got == "0" {
		t.Fatalf("mismatch stat=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.retry_total"]; got == "" || got == "0" {
		t.Fatalf("retry stat=%q want >0", got)
	}
}

func TestFlushApplyCloseAndCheckpointDrainInProgressApply(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")

	block := make(chan struct{})
	entered := make(chan struct{})
	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		close(entered)
		<-block
	}

	writeDone := make(chan error, 1)
	go func() {
		b := d.NewBatch()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("drain-%06d", i))); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- b.Write()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("apply hook did not run")
	}

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- d.Checkpoint() }()
	select {
	case err := <-checkpointDone:
		t.Fatalf("Checkpoint returned before in-progress apply drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(block)
	if err := <-writeDone; err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := <-checkpointDone; err != nil {
		t.Fatalf("checkpoint after drain: %v", err)
	}

	// Close should also drain an in-progress apply before closing the worker pool
	// and index resources.
	blockClose := make(chan struct{})
	enteredClose := make(chan struct{})
	fired.Store(false)
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		close(enteredClose)
		<-blockClose
	}
	writeDone = make(chan error, 1)
	go func() {
		b := d.NewBatch()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("close-%06d", i))); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- b.Write()
	}()
	select {
	case <-enteredClose:
	case <-time.After(5 * time.Second):
		t.Fatal("close apply hook did not run")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before in-progress apply drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(blockClose)
	if err := <-writeDone; err != nil {
		t.Fatalf("close-drained write: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close after drain: %v", err)
	}
}
