package treedbadapter

import (
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

// DB adapts TreeDB's public API to kvstore interfaces.
type DB struct {
	DB      *treedb.DB
	NameStr string
}

func Wrap(db *treedb.DB) *DB {
	d := &DB{DB: db, NameStr: "TreeDB"}
	if t := getTrace(); t != nil {
		t.registerDB()
	}
	return d
}

func WrapNamed(db *treedb.DB, name string) *DB {
	d := &DB{DB: db, NameStr: name}
	if t := getTrace(); t != nil {
		t.registerDB()
	}
	return d
}

func (d *DB) Name() string {
	if d.NameStr != "" {
		return d.NameStr
	}
	return "TreeDB"
}

func (d *DB) Close() error {
	err := d.DB.Close()
	if t := getTrace(); t != nil {
		t.closeDB()
	}
	return err
}

func (d *DB) Get(key []byte) ([]byte, error) {
	val, err := d.DB.Get(key)
	if t := getTrace(); t != nil {
		valLen := 0
		if val != nil {
			valLen = len(val)
		}
		t.noteOp("get", len(key), valLen)
	}
	return val, err
}

func (d *DB) GetUnsafe(key []byte) ([]byte, error) {
	val, err := d.DB.GetUnsafe(key)
	if t := getTrace(); t != nil {
		valLen := 0
		if val != nil {
			valLen = len(val)
		}
		t.noteOp("get", len(key), valLen)
	}
	return val, err
}

func (d *DB) GetAppend(key, dst []byte) ([]byte, error) {
	val, err := d.DB.GetAppend(key, dst)
	if t := getTrace(); t != nil {
		valLen := 0
		if val != nil {
			valLen = len(val)
		}
		t.noteOp("get", len(key), valLen)
	}
	return val, err
}

func (d *DB) Set(key, value []byte) error {
	if t := getTrace(); t != nil {
		t.noteOp("set", len(key), len(value))
	}
	return d.DB.Set(key, value)
}

func (d *DB) Delete(key []byte) error {
	if t := getTrace(); t != nil {
		t.noteOp("delete", len(key), 0)
	}
	return d.DB.Delete(key)
}

func (d *DB) Has(key []byte) (bool, error) {
	ok, err := d.DB.Has(key)
	if t := getTrace(); t != nil {
		t.noteOp("has", len(key), 0)
	}
	return ok, err
}

func (d *DB) SetSync(key, value []byte) error {
	if t := getTrace(); t != nil {
		t.noteOp("set_sync", len(key), len(value))
	}
	return d.DB.SetSync(key, value)
}

func (d *DB) DeleteSync(key []byte) error {
	if t := getTrace(); t != nil {
		t.noteOp("delete_sync", len(key), 0)
	}
	return d.DB.DeleteSync(key)
}

func (d *DB) Stats() map[string]string { return d.DB.Stats() }

func (d *DB) Print() error { return d.DB.Print() }

func (d *DB) Checkpoint() error { return d.DB.Checkpoint() }

func (d *DB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	if t := getTrace(); t != nil {
		t.noteIterCreate("forward", start, end)
		return &traceIterator{inner: it, tracer: t, kind: "forward", start: time.Now()}, nil
	}
	return it, nil
}

func (d *DB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	if t := getTrace(); t != nil {
		t.noteIterCreate("reverse", start, end)
		return &traceIterator{inner: it, tracer: t, kind: "reverse", start: time.Now()}, nil
	}
	return it, nil
}

func (d *DB) NewBatch() (kvstore.Batch, error) {
	b := d.DB.NewBatch()
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	wrapped := &batch{b: b, tracer: getTrace()}
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
	tracer     *traceLogger
	opCount    int
	byteCount  int
}

func (b *batch) Set(key, value []byte) error {
	b.opCount++
	b.byteCount += len(key) + len(value)
	return b.b.Set(key, value)
}

func (b *batch) Delete(key []byte) error {
	b.opCount++
	b.byteCount += len(key)
	return b.b.Delete(key)
}

// SetView records a Put without copying key/value bytes if supported by the
// underlying TreeDB batch. Callers must treat key/value as immutable until
// Commit/CommitSync/Close.
func (b *batch) SetView(key, value []byte) error {
	b.opCount++
	b.byteCount += len(key) + len(value)
	if b.setView != nil {
		return b.setView(key, value)
	}
	return b.b.Set(key, value)
}

// DeleteView records a Delete without copying key bytes if supported by the
// underlying TreeDB batch.
func (b *batch) DeleteView(key []byte) error {
	b.opCount++
	b.byteCount += len(key)
	if b.deleteView != nil {
		return b.deleteView(key)
	}
	return b.b.Delete(key)
}

func (b *batch) Commit() error {
	err := b.b.Write()
	if b.tracer != nil {
		b.tracer.noteBatchWrite(b.opCount, b.byteCount)
	}
	b.opCount = 0
	b.byteCount = 0
	return err
}

func (b *batch) CommitSync() error {
	err := b.b.WriteSync()
	if b.tracer != nil {
		b.tracer.noteBatchWrite(b.opCount, b.byteCount)
	}
	b.opCount = 0
	b.byteCount = 0
	return err
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
	b.opCount = 0
	b.byteCount = 0
}
