package caching

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestConditionalTxnDirectBypassCommitWaitsForOracleRecord(t *testing.T) {
	backend := newBlockingWriteBackend()
	cache, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	if err := cache.SetSync([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if _, _, err := tx.GetVersioned([]byte("k")); err != nil {
		t.Fatalf("tx.GetVersioned: %v", err)
	}
	if err := tx.Set([]byte("other"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}

	wb := cache.NewBatch()
	if wb == nil {
		t.Fatal("NewBatch returned nil")
	}
	if err := wb.Set([]byte("k"), []byte("outside")); err != nil {
		_ = wb.Close()
		t.Fatalf("batch Set: %v", err)
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

	select {
	case <-backend.entered:
	case err := <-writeDone:
		t.Fatalf("outside write completed before block: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backend write to enter blocked window")
	}

	released := false
	release := func() {
		if !released {
			close(backend.release)
			released = true
		}
	}
	defer release()

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- tx.Commit()
	}()

	select {
	case err := <-commitDone:
		release()
		if writeErr := <-writeDone; writeErr != nil {
			t.Fatalf("outside write: %v", writeErr)
		}
		if !errors.Is(err, backenddb.ErrConcurrentModification) {
			t.Fatalf("tx.Commit completed in backend visibility gap with error=%v, want ErrConcurrentModification after oracle record", err)
		}
		return
	case <-time.After(25 * time.Millisecond):
	}

	release()
	if err := <-writeDone; err != nil {
		t.Fatalf("outside write: %v", err)
	}
	if err := <-commitDone; !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnBackendBypassReadConflictsBeforePublish(t *testing.T) {
	backend := newBlockingWriteBackend()
	cache, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	if err := cache.SetSync([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()

	wb := cache.NewBatch()
	if wb == nil {
		t.Fatal("NewBatch returned nil")
	}
	if err := wb.Set([]byte("k"), []byte("outside")); err != nil {
		_ = wb.Close()
		t.Fatalf("batch Set: %v", err)
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

	select {
	case <-backend.entered:
	case err := <-writeDone:
		t.Fatalf("outside write completed before block: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backend write to enter blocked window")
	}

	released := false
	release := func() {
		if !released {
			close(backend.release)
			released = true
		}
	}
	defer release()

	got, _, err := tx.GetVersioned([]byte("k"))
	if !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("tx.GetVersioned during backend publish gap value=%q error=%v, want ErrConcurrentModification", got, err)
	}

	release()
	if err := <-writeDone; err != nil {
		t.Fatalf("outside write: %v", err)
	}
}

func TestConditionalTxnStagedReadDoesNotExtendReadSet(t *testing.T) {
	cache, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:  true,
		RelaxedSync: true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := tx.Set([]byte("k"), []byte("staged")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	got, _, err := tx.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("tx.GetVersioned staged: %v", err)
	}
	if string(got) != "staged" {
		t.Fatalf("tx.GetVersioned staged value=%q want staged", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}

	stats := cache.Stats()
	if got := stats["treedb.cache.conditional_txn.read_set.samples_total"]; got != "1" {
		t.Fatalf("read_set.samples_total=%q want 1", got)
	}
	if got := stats["treedb.cache.conditional_txn.read_set.entries_total"]; got != "0" {
		t.Fatalf("read_set.entries_total=%q want 0", got)
	}
	if got := stats["treedb.cache.conditional_txn.read_set.max"]; got != "0" {
		t.Fatalf("read_set.max=%q want 0", got)
	}
	if got := stats["treedb.cache.conditional_txn.read_set_entries_total"]; got != "" {
		t.Fatalf("legacy flat read_set_entries_total stat=%q want absent", got)
	}
}

func TestConditionalTxnDirectBypassRecordsPointConflictsPrecisely(t *testing.T) {
	backend := NewMockBackend()
	cache, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	if err := cache.Set([]byte("a"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if _, _, err := tx.GetVersioned([]byte("a")); err != nil {
		t.Fatalf("tx.GetVersioned: %v", err)
	}

	rootMarkersBefore := cache.conditionalOracleRootMarkers.Load()
	recordedPointsBefore := cache.conditionalOracleRecordedPoints.Load()
	wb := cache.NewBatch()
	value := make([]byte, 256)
	for i := range value {
		value[i] = 'v'
	}
	const n = 128
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("z%05d", i))
		if err := wb.Set(key, value); err != nil {
			_ = wb.Close()
			t.Fatalf("direct bypass batch Set %d: %v", i, err)
		}
	}
	if err := wb.writeBypass(false); err != nil {
		_ = wb.Close()
		t.Fatalf("direct bypass batch Write: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("backend batch Close: %v", err)
	}
	if got := cache.conditionalOracleRootMarkers.Load() - rootMarkersBefore; got != 0 {
		t.Fatalf("conditional oracle root markers delta=%d want 0 for precise direct-bypass metadata", got)
	}
	if got := cache.conditionalOracleRecordedPoints.Load() - recordedPointsBefore; got < n {
		t.Fatalf("conditional oracle recorded points delta=%d want at least %d", got, n)
	}

	if err := tx.Set([]byte("a"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit after disjoint streaming write: %v", err)
	}
	got, err := cache.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "inside" {
		t.Fatalf("Get=%q want inside", got)
	}
}

func TestConditionalTxnLegacyBackendBatchRecordsPointConflictsPrecisely(t *testing.T) {
	backend := NewMockBackend()
	cache, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	if err := cache.Set([]byte("a"), []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tx, err := cache.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if _, _, err := tx.GetVersioned([]byte("a")); err != nil {
		t.Fatalf("tx.GetVersioned: %v", err)
	}

	rootMarkersBefore := cache.conditionalOracleRootMarkers.Load()
	recordedPointsBefore := cache.conditionalOracleRecordedPoints.Load()
	wb := cache.NewBatch()
	wb.backend = cache.backend.NewBatch()
	const n = 32
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("z%05d", i))
		if err := wb.Set(key, []byte("outside")); err != nil {
			_ = wb.Close()
			t.Fatalf("backend batch Set %d: %v", i, err)
		}
	}
	if err := wb.Write(); err != nil {
		_ = wb.Close()
		t.Fatalf("backend batch Write: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("backend batch Close: %v", err)
	}
	if got := cache.conditionalOracleRootMarkers.Load() - rootMarkersBefore; got != 0 {
		t.Fatalf("conditional oracle root markers delta=%d want 0 for precise backend-batch metadata", got)
	}
	if got := cache.conditionalOracleRecordedPoints.Load() - recordedPointsBefore; got < n {
		t.Fatalf("conditional oracle recorded points delta=%d want at least %d", got, n)
	}

	if err := tx.Set([]byte("a"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit after disjoint backend batch: %v", err)
	}
	got, err := cache.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "inside" {
		t.Fatalf("Get=%q want inside", got)
	}
}

type blockingWriteBackend struct {
	*MockBackend
	block    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	enterOne sync.Once
}

func newBlockingWriteBackend() *blockingWriteBackend {
	mb := NewMockBackend()
	mb.pointerEntries = make(map[string]page.ValuePtr)
	return &blockingWriteBackend{
		MockBackend: mb,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (b *blockingWriteBackend) NewBatch() batch.Interface {
	return &blockingWriteBatch{parent: b}
}

type blockingWriteBatch struct {
	parent *blockingWriteBackend
	ops    []batch.Entry
}

func (b *blockingWriteBatch) Set(key, value []byte) error {
	if err := b.parent.getSetErr(); err != nil {
		return err
	}
	b.ops = append(b.ops, batch.Entry{
		Type:  batch.OpPut,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	return nil
}

func (b *blockingWriteBatch) Delete(key []byte) error {
	if err := b.parent.getDeleteErr(); err != nil {
		return err
	}
	b.ops = append(b.ops, batch.Entry{
		Type: batch.OpDelete,
		Key:  append([]byte(nil), key...),
	})
	return nil
}

func (b *blockingWriteBatch) DeleteRange(start, end []byte) error {
	b.ops = append(b.ops, batch.Entry{
		Type:  batch.OpDeleteRange,
		Key:   append([]byte(nil), start...),
		Value: append([]byte(nil), end...),
	})
	return nil
}

func (b *blockingWriteBatch) SetOps(ops []batch.Entry) error {
	if err := b.parent.getSetOpsErr(); err != nil {
		return err
	}
	b.ops = appendEntriesForBlockingBatch(b.ops[:0], ops)
	return nil
}

func (b *blockingWriteBatch) Write() error {
	return b.write(false)
}

func (b *blockingWriteBatch) WriteSync() error {
	return b.write(true)
}

func (b *blockingWriteBatch) write(syncWrite bool) error {
	if err := b.parent.getWriteErr(); err != nil {
		return err
	}
	b.parent.applyBlockingBatchOps(b.ops, syncWrite)
	if b.parent.block.Load() {
		b.parent.enterOne.Do(func() {
			close(b.parent.entered)
			<-b.parent.release
		})
	}
	return nil
}

func (b *blockingWriteBatch) Close() error { return nil }

func (b *blockingWriteBatch) Replay(fn func(batch.Entry) error) error {
	for i := range b.ops {
		if err := fn(b.ops[i]); err != nil {
			return err
		}
	}
	return nil
}

func (b *blockingWriteBatch) GetByteSize() (int, error) {
	size := 0
	for i := range b.ops {
		size += len(b.ops[i].Key) + len(b.ops[i].Value)
	}
	return size, nil
}

func (b *blockingWriteBackend) applyBlockingBatchOps(ops []batch.Entry, syncWrite bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastOps = appendEntriesForBlockingBatch(b.lastOps[:0], ops)
	for _, op := range ops {
		switch op.Type {
		case batch.OpDeleteRange:
			for k := range b.data {
				if batch.DeleteRangeContainsKey(batch.DeleteRange{Start: op.Key, End: op.Value}, []byte(k)) {
					delete(b.data, k)
				}
			}
			for k := range b.pointerEntries {
				if batch.DeleteRangeContainsKey(batch.DeleteRange{Start: op.Key, End: op.Value}, []byte(k)) {
					delete(b.pointerEntries, k)
				}
			}
		case batch.OpDelete:
			delete(b.data, string(op.Key))
			delete(b.pointerEntries, string(op.Key))
		case batch.OpPut:
			if op.IsPtr {
				b.pointerEntries[string(op.Key)] = op.ValuePtr
				delete(b.data, string(op.Key))
				continue
			}
			value := append([]byte(nil), op.Value...)
			b.data[string(op.Key)] = value
			delete(b.pointerEntries, string(op.Key))
		}
	}
	b.writeCalls++
	if syncWrite {
		b.writeSyncs++
	}
}

func appendEntriesForBlockingBatch(dst []batch.Entry, src []batch.Entry) []batch.Entry {
	for _, op := range src {
		copied := op
		copied.Key = append([]byte(nil), op.Key...)
		copied.Value = append([]byte(nil), op.Value...)
		dst = append(dst, copied)
	}
	return dst
}
