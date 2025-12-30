package treedbadapter

import (
	"github.com/snissn/gomap/kvstore"
	"os"
	"strconv"

	treedb "github.com/snissn/gomap/TreeDB"
)

// allowBatchViews enables the view-based batch fast-path for TreeDB's kvstore adapter.
//
// When enabled, Batch.Set/Delete will avoid copying key/value bytes by delegating to
// the underlying TreeDB batch's SetView/DeleteView methods (if supported). Callers
// MUST treat key/value as immutable until Commit/CommitSync/Close.
//
// This is intentionally opt-in because many callers expect Set/Delete to be safe
// against later buffer reuse/mutation.
var allowBatchViews = func() bool {
	v, ok := os.LookupEnv("GOMAP_TREEDB_BATCH_VIEW")
	if !ok {
		return false
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return v != "" && v != "0"
}()

// DB adapts TreeDB's public API to kvstore interfaces.
type DB struct {
	DB      *treedb.DB
	NameStr string
}

func Wrap(db *treedb.DB) *DB { return &DB{DB: db, NameStr: "TreeDB"} }

func WrapNamed(db *treedb.DB, name string) *DB { return &DB{DB: db, NameStr: name} }

func (d *DB) Name() string {
	if d.NameStr != "" {
		return d.NameStr
	}
	return "TreeDB"
}

func (d *DB) Close() error { return d.DB.Close() }

func (d *DB) Get(key []byte) ([]byte, error) { return d.DB.Get(key) }

func (d *DB) GetUnsafe(key []byte) ([]byte, error) { return d.DB.GetUnsafe(key) }

func (d *DB) GetAppend(key, dst []byte) ([]byte, error) { return d.DB.GetAppend(key, dst) }

func (d *DB) Set(key, value []byte) error { return d.DB.Set(key, value) }

func (d *DB) Delete(key []byte) error { return d.DB.Delete(key) }

func (d *DB) Has(key []byte) (bool, error) { return d.DB.Has(key) }

func (d *DB) SetSync(key, value []byte) error { return d.DB.SetSync(key, value) }

func (d *DB) DeleteSync(key []byte) error { return d.DB.DeleteSync(key) }

func (d *DB) Stats() map[string]string { return d.DB.Stats() }

func (d *DB) Print() error { return d.DB.Print() }

func (d *DB) Checkpoint() error { return d.DB.Checkpoint() }

func (d *DB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	return d.DB.Iterator(start, end)
}

func (d *DB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	return d.DB.ReverseIterator(start, end)
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

type batch struct {
	b          treedb.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
}

func (b *batch) Set(key, value []byte) error {
	if allowBatchViews && b.setView != nil {
		return b.setView(key, value)
	}
	return b.b.Set(key, value)
}

func (b *batch) Delete(key []byte) error {
	if allowBatchViews && b.deleteView != nil {
		return b.deleteView(key)
	}
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

func (b *batch) Commit() error { return b.b.Write() }

func (b *batch) CommitSync() error { return b.b.WriteSync() }

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
