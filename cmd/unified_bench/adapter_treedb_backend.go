package main

import (
	"github.com/snissn/gomap/TreeDB/batch"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/kvstore"
)

type treeDBBackendAdapter struct {
	db         *treedbdb.DB
	name       string
	finalStats map[string]string
}

func NewTreeDBBackend(dir string) (kvstore.DB, error) {
	return newTreeDBBackend(dir, false, "TreeDB (backend)")
}

func NewTreeDBBackendCommandWAL(dir string) (kvstore.DB, error) {
	return newTreeDBBackend(dir, true, "TreeDB (backend command_wal_v1)")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newTreeDBBackend(dir string, commandWAL bool, name string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.CommandWAL = commandWAL
	opts.IndexOuterLeavesInValueLog = false
	if opts.ValueLog.PointerThreshold <= 0 {
		opts.ValueLog.PointerThreshold = page.DefaultInlineThreshold
	}
	d, err := treedbdb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &treeDBBackendAdapter{db: d, name: name}, nil
}

func (d *treeDBBackendAdapter) Name() string { return d.name }
func (d *treeDBBackendAdapter) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	d.finalStats = cloneStringMap(d.db.Stats())
	err := d.db.Close()
	d.db = nil
	return err
}

func (d *treeDBBackendAdapter) Get(key []byte) ([]byte, error) {
	return d.db.Get(key)
}

func (d *treeDBBackendAdapter) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	if d == nil || d.db == nil {
		return nil, kvstore.ErrUnsupported
	}
	snap := d.db.AcquireSnapshot()
	if snap == nil {
		return nil, kvstore.ErrUnsupported
	}
	return snap, nil
}

func (d *treeDBBackendAdapter) Set(key, value []byte) error     { return d.db.Set(key, value) }
func (d *treeDBBackendAdapter) Delete(key []byte) error         { return d.db.Delete(key) }
func (d *treeDBBackendAdapter) SetSync(key, value []byte) error { return d.db.SetSync(key, value) }
func (d *treeDBBackendAdapter) DeleteSync(key []byte) error     { return d.db.DeleteSync(key) }
func (d *treeDBBackendAdapter) Stats() map[string]string {
	if d == nil {
		return nil
	}
	if d.db == nil {
		return cloneStringMap(d.finalStats)
	}
	return d.db.Stats()
}
func (d *treeDBBackendAdapter) Print() error                 { return d.db.Print() }
func (d *treeDBBackendAdapter) Has(key []byte) (bool, error) { return d.db.Has(key) }
func (d *treeDBBackendAdapter) Checkpoint() error {
	b := d.db.NewBatch()
	if b == nil {
		return nil
	}
	defer b.Close()
	return b.WriteSync()
}
func (d *treeDBBackendAdapter) Iterator(start, end []byte) (kvstore.Iterator, error) {
	return d.db.Iterator(start, end)
}
func (d *treeDBBackendAdapter) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	return d.db.ReverseIterator(start, end)
}

func (d *treeDBBackendAdapter) NewBatch() (kvstore.Batch, error) {
	b := d.db.NewBatch()
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	return &treeDBBackendBatch{b: b}, nil
}

func (d *treeDBBackendAdapter) NewBatchWithSize(size int) (kvstore.Batch, error) {
	b := d.db.NewBatchWithSize(size)
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	return &treeDBBackendBatch{b: b}, nil
}

type treeDBBackendBatch struct {
	b batch.Interface
}

func (b *treeDBBackendBatch) Set(key, value []byte) error {
	return b.b.Set(key, value)
}

func (b *treeDBBackendBatch) SetView(key, value []byte) error {
	if sv, ok := b.b.(interface{ SetView(key, value []byte) error }); ok {
		return sv.SetView(key, value)
	}
	return b.b.Set(key, value)
}

func (b *treeDBBackendBatch) Delete(key []byte) error {
	return b.b.Delete(key)
}

func (b *treeDBBackendBatch) Commit() error {
	return b.b.Write()
}

func (b *treeDBBackendBatch) CommitSync() error {
	return b.b.WriteSync()
}

func (b *treeDBBackendBatch) Close() error {
	return b.b.Close()
}

func (b *treeDBBackendBatch) Reset() {
	if r, ok := b.b.(interface{ Reset() }); ok {
		r.Reset()
	}
}
