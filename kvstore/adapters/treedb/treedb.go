package treedbadapter

import (
	"errors"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

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

func (d *DB) Get(key []byte) ([]byte, error) {
	val, err := d.DB.Get(key)
	if errors.Is(err, treedb.ErrClosed) {
		return nil, nil
	}
	return val, err
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

func (d *DB) Set(key, value []byte) error { return d.DB.Set(key, value) }

func (d *DB) Delete(key []byte) error { return d.DB.Delete(key) }

func (d *DB) Has(key []byte) (bool, error) {
	ok, err := d.DB.Has(key)
	if errors.Is(err, treedb.ErrClosed) {
		return false, nil
	}
	return ok, err
}

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

func (b *batch) Set(key, value []byte) error { return b.b.Set(key, value) }

func (b *batch) Delete(key []byte) error { return b.b.Delete(key) }

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
