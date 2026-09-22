package treedbtrace

import (
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

// DB wraps another kvstore.DB with trace instrumentation.
type DB struct {
	inner kvstore.DB
	phase *treedb.DB
}

// Wrap creates a traced adapter over TreeDB with default naming.
func Wrap(db *treedb.DB) *DB {
	return WrapNamed(db, "TreeDB")
}

// WrapNamed creates a traced adapter over TreeDB with a custom name.
func WrapNamed(db *treedb.DB, name string) *DB {
	return New(treedbadapter.WrapNamed(db, name))
}

// New wraps an existing kvstore.DB with tracing.
func New(inner kvstore.DB) *DB {
	d := &DB{inner: inner}
	if wrapped, ok := inner.(*treedbadapter.DB); ok && wrapped.DB != nil {
		d.phase = wrapped.DB
		phaseBus.register(wrapped.DB)
	}
	if t := getTrace(); t != nil {
		t.registerDB()
	}
	return d
}

// Inner exposes the wrapped DB for callers that need concrete access.
func (d *DB) Inner() kvstore.DB { return d.inner }

func (d *DB) Name() string { return d.inner.Name() }

func (d *DB) Close() error {
	err := d.inner.Close()
	if d.phase != nil {
		phaseBus.unregister(d.phase)
	}
	if t := getTrace(); t != nil {
		t.closeDB()
	}
	return err
}

func (d *DB) Get(key []byte) ([]byte, error) {
	val, err := d.inner.Get(key)
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
	return d.inner.Set(key, value)
}

func (d *DB) Delete(key []byte) error {
	if t := getTrace(); t != nil {
		t.noteOp("delete", len(key), 0)
	}
	return d.inner.Delete(key)
}

func (d *DB) Has(key []byte) (bool, error) {
	h, ok := d.inner.(kvstore.Haser)
	if !ok {
		return false, kvstore.ErrUnsupported
	}
	okv, err := h.Has(key)
	if t := getTrace(); t != nil {
		t.noteOp("has", len(key), 0)
	}
	return okv, err
}

func (d *DB) SetSync(key, value []byte) error {
	s, ok := d.inner.(kvstore.Syncer)
	if !ok {
		return kvstore.ErrUnsupported
	}
	if t := getTrace(); t != nil {
		t.noteOp("set_sync", len(key), len(value))
	}
	return s.SetSync(key, value)
}

func (d *DB) DeleteSync(key []byte) error {
	s, ok := d.inner.(kvstore.Syncer)
	if !ok {
		return kvstore.ErrUnsupported
	}
	if t := getTrace(); t != nil {
		t.noteOp("delete_sync", len(key), 0)
	}
	return s.DeleteSync(key)
}

func (d *DB) Stats() map[string]string {
	if s, ok := d.inner.(kvstore.StatsProvider); ok {
		return s.Stats()
	}
	return nil
}

func (d *DB) Print() error {
	if p, ok := d.inner.(kvstore.Printer); ok {
		return p.Print()
	}
	return kvstore.ErrUnsupported
}

func (d *DB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	rs, ok := d.inner.(kvstore.RangeScanner)
	if !ok {
		return nil, kvstore.ErrUnsupported
	}
	it, err := rs.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	if t := getTrace(); t != nil {
		iterID := t.nextIterID()
		t.noteIterCreate(iterID, "forward", start, end)
		return &traceIterator{inner: it, tracer: t, iterID: iterID, kind: "forward", start: time.Now()}, nil
	}
	return it, nil
}

func (d *DB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	rs, ok := d.inner.(kvstore.RangeScanner)
	if !ok {
		return nil, kvstore.ErrUnsupported
	}
	it, err := rs.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	if t := getTrace(); t != nil {
		iterID := t.nextIterID()
		t.noteIterCreate(iterID, "reverse", start, end)
		return &traceIterator{inner: it, tracer: t, iterID: iterID, kind: "reverse", start: time.Now()}, nil
	}
	return it, nil
}

func (d *DB) ForEach(fn func(key, value []byte) error) error {
	f, ok := d.inner.(kvstore.ForEacher)
	if !ok {
		return kvstore.ErrUnsupported
	}
	return f.ForEach(fn)
}

func (d *DB) NewBatch() (kvstore.Batch, error) {
	b, ok := d.inner.(kvstore.Batcher)
	if !ok {
		return nil, kvstore.ErrUnsupported
	}
	inner, err := b.NewBatch()
	if err != nil {
		return nil, err
	}
	wrapped := &batch{inner: inner, tracer: getTrace()}
	if sv, ok := inner.(interface{ SetView(key, value []byte) error }); ok {
		wrapped.setView = sv.SetView
	}
	if dv, ok := inner.(interface{ DeleteView(key []byte) error }); ok {
		wrapped.deleteView = dv.DeleteView
	}
	return wrapped, nil
}

type batch struct {
	inner      kvstore.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
	tracer     *traceLogger
	opCount    int
	byteCount  int
}

func (b *batch) Set(key, value []byte) error {
	b.opCount++
	b.byteCount += len(key) + len(value)
	return b.inner.Set(key, value)
}

func (b *batch) Delete(key []byte) error {
	b.opCount++
	b.byteCount += len(key)
	return b.inner.Delete(key)
}

// SetView records a Put without copying key/value bytes if supported by the
// underlying batch. Callers must treat key/value as immutable until Commit/CommitSync/Close.
func (b *batch) SetView(key, value []byte) error {
	b.opCount++
	b.byteCount += len(key) + len(value)
	if b.setView != nil {
		return b.setView(key, value)
	}
	return b.inner.Set(key, value)
}

// DeleteView records a Delete without copying key bytes if supported by the
// underlying batch.
func (b *batch) DeleteView(key []byte) error {
	b.opCount++
	b.byteCount += len(key)
	if b.deleteView != nil {
		return b.deleteView(key)
	}
	return b.inner.Delete(key)
}

func (b *batch) Commit() error {
	err := b.inner.Commit()
	if b.tracer != nil {
		b.tracer.noteBatchWrite(b.opCount, b.byteCount)
	}
	b.opCount = 0
	b.byteCount = 0
	return err
}

func (b *batch) CommitSync() error {
	err := b.inner.CommitSync()
	if b.tracer != nil {
		b.tracer.noteBatchWrite(b.opCount, b.byteCount)
	}
	b.opCount = 0
	b.byteCount = 0
	return err
}

func (b *batch) Close() error { return b.inner.Close() }

// Reset is an optional fast-path used by higher-level adapters to recycle batch
// buffers without reallocation. If the underlying batch doesn't support it,
// Reset is a no-op and callers can fall back to Close/NewBatch.
func (b *batch) Reset() {
	if b == nil || b.inner == nil {
		return
	}
	if r, ok := b.inner.(interface{ Reset() }); ok {
		r.Reset()
	}
	b.opCount = 0
	b.byteCount = 0
}
