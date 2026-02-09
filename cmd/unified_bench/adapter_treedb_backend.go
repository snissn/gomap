package main

import (
	"github.com/snissn/gomap/TreeDB/batch"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/kvstore"
)

type treeDBBackendAdapter struct {
	db *treedbdb.DB
}

func NewTreeDBBackend(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	if opts.ValueLog.PointerThreshold <= 0 {
		opts.ValueLog.PointerThreshold = page.DefaultInlineThreshold
	}
	d, err := treedbdb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &treeDBBackendAdapter{db: d}, nil
}

func (d *treeDBBackendAdapter) Name() string { return "TreeDB (backend)" }
func (d *treeDBBackendAdapter) Close() error { return d.db.Close() }

func (d *treeDBBackendAdapter) Get(key []byte) ([]byte, error) {
	return d.db.Get(key)
}

func (d *treeDBBackendAdapter) Set(key, value []byte) error     { return d.db.Set(key, value) }
func (d *treeDBBackendAdapter) Delete(key []byte) error         { return d.db.Delete(key) }
func (d *treeDBBackendAdapter) SetSync(key, value []byte) error { return d.db.SetSync(key, value) }
func (d *treeDBBackendAdapter) DeleteSync(key []byte) error     { return d.db.DeleteSync(key) }
func (d *treeDBBackendAdapter) Stats() map[string]string        { return d.db.Stats() }
func (d *treeDBBackendAdapter) Print() error                    { return d.db.Print() }
func (d *treeDBBackendAdapter) Has(key []byte) (bool, error)    { return d.db.Has(key) }
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
