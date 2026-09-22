package treedbadapter

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

// DB adapts TreeDB's public API to kvstore interfaces.
type DB struct {
	DB               *treedb.DB
	NameStr          string
	readWorkers      atomic.Int32
	readBatchGetMany func(db *treedb.DB, keys [][]byte) ([][]byte, error)
}

const (
	readBatchGetManyMinKeys      = 32
	readBatchDupHeavyMinKeyCount = 8
)

func Wrap(db *treedb.DB) *DB {
	return wrapNamedWithReadWorkers(db, "TreeDB", runtime.GOMAXPROCS(0))
}

func WrapNamed(db *treedb.DB, name string) *DB {
	return wrapNamedWithReadWorkers(db, name, runtime.GOMAXPROCS(0))
}

func wrapNamedWithReadWorkers(db *treedb.DB, name string, workers int) *DB {
	out := &DB{
		DB:      db,
		NameStr: name,
		readBatchGetMany: func(innerDB *treedb.DB, keys [][]byte) ([][]byte, error) {
			return innerDB.GetMany(keys)
		},
	}
	out.setReadWorkers(workers)
	return out
}

func normalizeReadWorkers(workers int) int {
	if workers < 1 {
		return 1
	}
	if workers > math.MaxInt32 {
		return math.MaxInt32
	}
	return workers
}

func dedupeReadBatchKeys(keys [][]byte) [][]byte {
	if len(keys) < 2 {
		return keys
	}
	seen := make(map[string]struct{}, len(keys))
	unique := make([][]byte, 0, len(keys))
	for _, key := range keys {
		keyID := string(key)
		if _, ok := seen[keyID]; ok {
			continue
		}
		seen[keyID] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}

func shouldReadBatchUseGetMany(db *treedb.DB, totalKeys, uniqueKeys, workers int) bool {
	if db == nil || workers <= 1 || uniqueKeys <= 1 {
		return false
	}
	plannedWorkers, willParallelize := db.GetManyParallelPlan(uniqueKeys)
	if willParallelize && plannedWorkers > workers {
		return false
	}
	if uniqueKeys >= readBatchGetManyMinKeys {
		return true
	}
	return totalKeys >= readBatchDupHeavyMinKeyCount && uniqueKeys*2 <= totalKeys
}

func (d *DB) setReadWorkers(workers int) {
	if d == nil {
		return
	}
	d.readWorkers.Store(int32(normalizeReadWorkers(workers)))
}

// SetReadWorkers configures the adapter's internal batch-read worker count.
func (d *DB) SetReadWorkers(workers int) {
	d.setReadWorkers(workers)
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

func (d *DB) GetManyView(keys [][]byte, fn kvstore.MultiGetViewFunc) error {
	if d == nil || d.DB == nil {
		return kvstore.ErrUnsupported
	}
	if fn == nil {
		return errors.New("treedb adapter: GetManyView nil callback")
	}
	err := d.DB.GetManyView(keys, func(index int, key []byte, value []byte, found bool) error {
		return fn(index, key, value, found)
	})
	if errors.Is(err, treedb.ErrClosed) {
		return nil
	}
	return err
}

func (d *DB) ReadBatch(keys [][]byte) (retErr error) {
	if len(keys) == 0 {
		return nil
	}
	if d == nil || d.DB == nil {
		return kvstore.ErrUnsupported
	}
	workers := int(d.readWorkers.Load())
	if workers < 1 {
		workers = 1
	}
	batchKeys := keys
	if workers > 1 && len(keys) >= readBatchDupHeavyMinKeyCount {
		batchKeys = dedupeReadBatchKeys(keys)
		if shouldReadBatchUseGetMany(d.DB, len(keys), len(batchKeys), workers) {
			// Keep historical post-close behavior for ReadBatch: if no snapshot can
			// be acquired up front, report unsupported instead of ErrClosed.
			probe := d.DB.AcquireSnapshot()
			if probe == nil {
				return kvstore.ErrUnsupported
			}
			if closeErr := probe.Close(); closeErr != nil {
				return closeErr
			}
			getManyFn := d.readBatchGetMany
			if getManyFn == nil {
				getManyFn = func(innerDB *treedb.DB, keys [][]byte) ([][]byte, error) {
					return innerDB.GetMany(keys)
				}
			}
			_, err := getManyFn(d.DB, batchKeys)
			if err == nil || errors.Is(err, treedb.ErrClosed) {
				return nil
			}
			return err
		}
	}
	if len(batchKeys) == 1 || workers <= 1 {
		snap := d.DB.AcquireSnapshot()
		if snap == nil {
			return kvstore.ErrUnsupported
		}
		defer func() {
			if closeErr := snap.Close(); retErr == nil && closeErr != nil {
				retErr = closeErr
			}
		}()
		for _, key := range batchKeys {
			_, err := snap.Has(key)
			if err == nil || errors.Is(err, treedb.ErrClosed) {
				continue
			}
			return err
		}
		return nil
	}
	if workers > len(batchKeys) {
		workers = len(batchKeys)
	}
	// Snapshots are immutable point-in-time readers in TreeDB, so concurrent
	// Has calls are issued against one snapshot for improved read-bandwidth.
	snap := d.DB.AcquireSnapshot()
	if snap == nil {
		return kvstore.ErrUnsupported
	}
	defer func() {
		if closeErr := snap.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()

	chunk := (len(batchKeys) + workers - 1) / workers
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
		if start >= len(batchKeys) {
			break
		}
		end := start + chunk
		if end > len(batchKeys) {
			end = len(batchKeys)
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				_, readErr := snap.Has(batchKeys[i])
				if readErr == nil || errors.Is(readErr, treedb.ErrClosed) {
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

func ignoreClosedWrite(err error) error {
	if errors.Is(err, treedb.ErrClosed) {
		return nil
	}
	return err
}

func (d *DB) Set(key, value []byte) error {
	return ignoreClosedWrite(d.DB.Set(key, value))
}

func (d *DB) Delete(key []byte) error {
	return ignoreClosedWrite(d.DB.Delete(key))
}

func (d *DB) RangeDeleteMode() string {
	return kvstore.RangeDeleteModeNative
}

func (d *DB) Has(key []byte) (bool, error) {
	ok, err := d.DB.Has(key)
	if errors.Is(err, treedb.ErrClosed) {
		return false, nil
	}
	return ok, err
}

func (d *DB) SetSync(key, value []byte) error {
	return ignoreClosedWrite(d.DB.SetSync(key, value))
}

func (d *DB) DeleteSync(key []byte) error {
	return ignoreClosedWrite(d.DB.DeleteSync(key))
}

// Stats is safe to call after Close. The unified-bench harness uses that
// post-close snapshot to capture TreeDB stats after final checkpoint/cleanup
// work has run.
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
	if sv, ok := b.(batchSetViewer); ok {
		wrapped.setView = sv
	}
	if dv, ok := b.(batchDeleteViewer); ok {
		wrapped.deleteView = dv
	}
	return wrapped, nil
}

func (d *DB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	b := d.DB.NewBatchWithSize(size)
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	wrapped := &batch{b: b}
	if sv, ok := b.(batchSetViewer); ok {
		wrapped.setView = sv
	}
	if dv, ok := b.(batchDeleteViewer); ok {
		wrapped.deleteView = dv
	}
	return wrapped, nil
}

type batchSetViewer interface {
	SetView(key, value []byte) error
}

type batchDeleteViewer interface {
	DeleteView(key []byte) error
}

type batch struct {
	b          treedb.Batch
	setView    batchSetViewer
	deleteView batchDeleteViewer
}

func (b *batch) Set(key, value []byte) error {
	return b.b.Set(key, value)
}

func (b *batch) Delete(key []byte) error {
	return b.b.Delete(key)
}

func (b *batch) DeleteRange(start, end []byte) error {
	return b.b.DeleteRange(start, end)
}

// SetView records a Put without copying key/value bytes if supported by the
// underlying TreeDB batch. Callers must treat key/value as immutable until
// Commit/CommitSync/Close.
func (b *batch) SetView(key, value []byte) error {
	if b.setView != nil {
		return b.setView.SetView(key, value)
	}
	return b.b.Set(key, value)
}

// DeleteView records a Delete without copying key bytes if supported by the
// underlying TreeDB batch.
func (b *batch) DeleteView(key []byte) error {
	if b.deleteView != nil {
		return b.deleteView.DeleteView(key)
	}
	return b.b.Delete(key)
}

func (b *batch) Commit() error {
	return ignoreClosedWrite(b.b.Write())
}

func (b *batch) CommitSync() error {
	return ignoreClosedWrite(b.b.WriteSync())
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
