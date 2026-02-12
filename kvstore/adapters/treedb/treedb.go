package treedbadapter

import (
	"context"
	"errors"
	"runtime"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

// DB adapts TreeDB's public API to kvstore interfaces.
type DB struct {
	DB          *treedb.DB
	NameStr     string
	readWorkers int
}

func Wrap(db *treedb.DB) *DB {
	return &DB{DB: db, NameStr: "TreeDB", readWorkers: resolveReadWorkers(1)}
}

func WrapNamed(db *treedb.DB, name string) *DB {
	return &DB{DB: db, NameStr: name, readWorkers: resolveReadWorkers(1)}
}

func WrapNamedWithReadWorkers(db *treedb.DB, name string, readWorkers int) *DB {
	return &DB{DB: db, NameStr: name, readWorkers: resolveReadWorkers(readWorkers)}
}

func resolveReadWorkers(workers int) int {
	// Kept local to avoid introducing a dependency from storage adapters back to
	// benchmark CLI configuration while preserving a stable standalone API.
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		return 1
	}
	return workers
}

func (d *DB) Name() string {
	if d.NameStr != "" {
		return d.NameStr
	}
	return "TreeDB"
}

func (d *DB) Close() error {
	return d.DB.Close()
}

func (d *DB) Get(key []byte) ([]byte, error) {
	val, err := d.DB.Get(key)
	if errors.Is(err, treedb.ErrClosed) {
		return nil, nil
	}
	return val, err
}

func (d *DB) GetMany(keys [][]byte) ([][]byte, error) {
	vals, err := d.DB.GetMany(keys)
	if errors.Is(err, treedb.ErrClosed) {
		return make([][]byte, len(keys)), nil
	}
	return vals, err
}

func (d *DB) ReadBatch(keys [][]byte) error {
	if d == nil || d.DB == nil {
		return nil
	}
	workers := d.readWorkers
	if len(keys) <= 1 || workers <= 1 {
		snap := d.DB.AcquireSnapshot()
		if snap == nil {
			return nil
		}
		defer func() { _ = snap.Close() }()
		for _, key := range keys {
			_, err := snap.GetUnsafe(key)
			if err == nil || errors.Is(err, treedb.ErrKeyNotFound) {
				continue
			}
			return err
		}
		return nil
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	// Snapshots are immutable point-in-time readers in TreeDB, so concurrent
	// GetUnsafe calls are issued against one snapshot for improved read-bandwidth.
	snap := d.DB.AcquireSnapshot()
	if snap == nil {
		return nil
	}
	defer func() { _ = snap.Close() }()

	chunk := (len(keys) + workers - 1) / workers
	if chunk < 1 {
		chunk = 1
	}
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for w := 0; w < workers; w++ {
		start := w * chunk
		if start >= len(keys) {
			break
		}
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				_, readErr := snap.GetUnsafe(keys[i])
				if readErr == nil || errors.Is(readErr, treedb.ErrKeyNotFound) {
					continue
				}
				errMu.Lock()
				if firstErr == nil {
					firstErr = readErr
				}
				errMu.Unlock()
				return
			}
		}(start, end)
	}
	wg.Wait()
	return firstErr
}

func (d *DB) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	if d == nil || d.DB == nil {
		return nil, kvstore.ErrUnsupported
	}
	snap := d.DB.AcquireSnapshot()
	if snap == nil {
		return nil, kvstore.ErrUnsupported
	}
	return snap, nil
}

func (d *DB) GetUnsafe(key []byte) ([]byte, error) {
	val, err := d.DB.GetUnsafe(key)
	if errors.Is(err, treedb.ErrClosed) {
		return nil, nil
	}
	return val, err
}

func (d *DB) GetAppend(key, dst []byte) ([]byte, error) {
	val, err := d.DB.GetAppend(key, dst)
	if errors.Is(err, treedb.ErrClosed) {
		return dst, nil
	}
	return val, err
}

func (d *DB) Set(key, value []byte) error {
	return d.DB.Set(key, value)
}

func (d *DB) Delete(key []byte) error {
	return d.DB.Delete(key)
}

func (d *DB) Has(key []byte) (bool, error) {
	ok, err := d.DB.Has(key)
	if errors.Is(err, treedb.ErrClosed) {
		return false, nil
	}
	return ok, err
}

func (d *DB) SetSync(key, value []byte) error {
	return d.DB.SetSync(key, value)
}

func (d *DB) DeleteSync(key []byte) error {
	return d.DB.DeleteSync(key)
}

func (d *DB) Stats() map[string]string { return d.DB.Stats() }

func (d *DB) Print() error { return d.DB.Print() }

func (d *DB) Checkpoint() error { return d.DB.Checkpoint() }

func (d *DB) VacuumIndexOnline(ctx context.Context) error { return d.DB.VacuumIndexOnline(ctx) }

func (d *DB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return wrapIterator(it), nil
}

func (d *DB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return wrapIterator(it), nil
}

func (d *DB) NewBatch() (kvstore.Batch, error) {
	b := d.DB.NewBatch()
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	wrapped := &batch{b: b}
	if sv, ok := b.(interface{ SetView(key, value []byte) error }); ok {
		wrapped.setView = sv.SetView
	}
	if dv, ok := b.(interface{ DeleteView(key []byte) error }); ok {
		wrapped.deleteView = dv.DeleteView
	}
	return wrapped, nil
}

func (d *DB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	b := d.DB.NewBatchWithSize(size)
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	wrapped := &batch{b: b}
	if sv, ok := b.(interface{ SetView(key, value []byte) error }); ok {
		wrapped.setView = sv.SetView
	}
	if dv, ok := b.(interface{ DeleteView(key []byte) error }); ok {
		wrapped.deleteView = dv.DeleteView
	}
	return wrapped, nil
}

type batch struct {
	b          treedb.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
}

func (b *batch) Set(key, value []byte) error {
	return b.b.Set(key, value)
}

func (b *batch) Delete(key []byte) error {
	return b.b.Delete(key)
}

// SetView records a Put without copying key/value bytes if supported by the
// underlying TreeDB batch. Callers must treat key/value as immutable until
// Commit/CommitSync/Close.
func (b *batch) SetView(key, value []byte) error {
	if b.setView != nil {
		return b.setView(key, value)
	}
	return b.b.Set(key, value)
}

// DeleteView records a Delete without copying key bytes if supported by the
// underlying TreeDB batch.
func (b *batch) DeleteView(key []byte) error {
	if b.deleteView != nil {
		return b.deleteView(key)
	}
	return b.b.Delete(key)
}

func (b *batch) Commit() error {
	return b.b.Write()
}

func (b *batch) CommitSync() error {
	return b.b.WriteSync()
}

func (b *batch) Close() error { return b.b.Close() }

// Reset is an optional fast-path used by higher-level adapters to recycle batch
// buffers without reallocation. If the underlying TreeDB batch doesn't support
// it, Reset is a no-op and callers can fall back to Close/NewBatch.
func (b *batch) Reset() {
	if b == nil || b.b == nil {
		return
	}
	if r, ok := b.b.(interface{ Reset() }); ok {
		r.Reset()
	}
}

type unsafeKVIterator interface {
	UnsafeKey() []byte
	UnsafeValue() []byte
}

type adapterIterator struct {
	inner  kvstore.Iterator
	unsafe unsafeKVIterator
}

func wrapIterator(inner kvstore.Iterator) kvstore.Iterator {
	if inner == nil {
		return nil
	}
	u, ok := inner.(unsafeKVIterator)
	if !ok {
		// Preserve any optional iterator interfaces when there is no unsafe
		// fast-path to expose.
		return inner
	}
	return &adapterIterator{inner: inner, unsafe: u}
}

func (it *adapterIterator) Valid() bool {
	return it.inner.Valid()
}

func (it *adapterIterator) Next() {
	it.inner.Next()
}

func (it *adapterIterator) Key() []byte {
	if it.unsafe != nil {
		return it.unsafe.UnsafeKey()
	}
	return it.inner.Key()
}

func (it *adapterIterator) Value() []byte {
	if it.unsafe != nil {
		return it.unsafe.UnsafeValue()
	}
	return it.inner.Value()
}

func (it *adapterIterator) KeyCopy(dst []byte) []byte {
	if it.unsafe != nil {
		key := it.unsafe.UnsafeKey()
		if key == nil {
			return nil
		}
		return append(dst[:0], key...)
	}
	return it.inner.KeyCopy(dst)
}

func (it *adapterIterator) ValueCopy(dst []byte) []byte {
	if it.unsafe != nil {
		value := it.unsafe.UnsafeValue()
		if value == nil {
			return nil
		}
		return append(dst[:0], value...)
	}
	return it.inner.ValueCopy(dst)
}

func (it *adapterIterator) DebugStats() (queueLen int, sourcesUsed int) {
	if ds, ok := it.inner.(interface {
		DebugStats() (queueLen int, sourcesUsed int)
	}); ok {
		return ds.DebugStats()
	}
	return 0, 0
}

func (it *adapterIterator) Error() error {
	return it.inner.Error()
}

func (it *adapterIterator) Close() error {
	return it.inner.Close()
}
