package main

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/kvstore"
)

type treeDBBackendAdapter struct {
	db         *treedbdb.DB
	leafLog    treedbdb.LeafPageLogCloser
	name       string
	finalStats map[string]string
	closed     bool
}

func NewTreeDBBackend(dir string) (kvstore.DB, error) {
	return newTreeDBBackend(dir, false, "TreeDB (backend)")
}

func NewTreeDBBackendCommandWAL(dir string) (kvstore.DB, error) {
	return newTreeDBBackend(dir, true, "TreeDB (backend command_wal_v1)")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newTreeDBBackend(dir string, commandWAL bool, name string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptionsWithConfig(dir, treeDBOptionsBuildConfig{forceWALOn: commandWAL})
	if err != nil {
		return nil, err
	}
	opts.CommandWAL = commandWAL
	if commandWAL && opts.Durability == treedbdb.DurabilityWALOffRelaxed {
		opts.Durability = treedbdb.DurabilityWALOnRelaxed
	}
	opts.CommandWALStatsScan = commandWAL && *treedbCommandWALStatsScan
	if opts.ValueLog.PointerThreshold <= 0 {
		opts.ValueLog.PointerThreshold = page.DefaultInlineThreshold
	}
	if cfg, ok, err := treedbdb.LoadFormatConfig(dir); err != nil {
		return nil, err
	} else if ok {
		cfg.ApplyIndexFormatToOptions(&opts)
	}
	effectiveOuterLeaves := opts.IndexOuterLeavesInValueLog
	d, err := treedbdb.Open(opts)
	if err != nil {
		return nil, err
	}
	adapter := &treeDBBackendAdapter{db: d, name: name}
	if effectiveOuterLeaves {
		leafLog, err := treedbdb.NewStandaloneLeafPageLog(dir, treedbdb.StandaloneLeafPageLogOptions{
			Compression: opts.ValueLog.Compression,
			AutoPolicy:  opts.ValueLog.AutoPolicy,
			BlockCodec:  opts.ValueLog.BlockCodec,
		})
		if err != nil {
			_ = d.Close()
			return nil, err
		}
		d.SetLeafPageLog(leafLog)
		adapter.leafLog = leafLog
	}
	return adapter, nil
}

func (d *treeDBBackendAdapter) Name() string { return d.name }
func (d *treeDBBackendAdapter) Close() error {
	if d == nil || d.closed {
		return nil
	}
	d.finalStats = cloneStringMap(d.db.Stats())
	d.closed = true
	err := d.db.Close()
	if d.leafLog != nil {
		err = errors.Join(err, d.leafLog.Close())
		d.leafLog = nil
	}
	return err
}

func (d *treeDBBackendAdapter) openDB() (*treedbdb.DB, error) {
	if d == nil || d.db == nil || d.closed {
		return nil, treedbdb.ErrClosed
	}
	return d.db, nil
}

func (d *treeDBBackendAdapter) Get(key []byte) ([]byte, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	return db.Get(key)
}

func (d *treeDBBackendAdapter) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return nil, kvstore.ErrUnsupported
	}
	return snap, nil
}

func (d *treeDBBackendAdapter) Set(key, value []byte) error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	return db.Set(key, value)
}

func (d *treeDBBackendAdapter) Delete(key []byte) error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	return db.Delete(key)
}

func (d *treeDBBackendAdapter) SetSync(key, value []byte) error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	return db.SetSync(key, value)
}

func (d *treeDBBackendAdapter) DeleteSync(key []byte) error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	return db.DeleteSync(key)
}

func (d *treeDBBackendAdapter) Stats() map[string]string {
	if d == nil {
		return map[string]string{}
	}
	if d.closed {
		return cloneStringMap(d.finalStats)
	}
	return d.db.Stats()
}
func (d *treeDBBackendAdapter) Print() error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	return db.Print()
}
func (d *treeDBBackendAdapter) Has(key []byte) (bool, error) {
	db, err := d.openDB()
	if err != nil {
		return false, err
	}
	return db.Has(key)
}
func (d *treeDBBackendAdapter) Checkpoint() error {
	db, err := d.openDB()
	if err != nil {
		return err
	}
	b := db.NewBatch()
	if b == nil {
		return nil
	}
	defer b.Close()
	return b.WriteSync()
}
func (d *treeDBBackendAdapter) Iterator(start, end []byte) (kvstore.Iterator, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	return db.Iterator(start, end)
}
func (d *treeDBBackendAdapter) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	return db.ReverseIterator(start, end)
}

func (d *treeDBBackendAdapter) NewBatch() (kvstore.Batch, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	b := db.NewBatch()
	if b == nil {
		return nil, kvstore.ErrUnsupported
	}
	return &treeDBBackendBatch{b: b}, nil
}

func (d *treeDBBackendAdapter) NewBatchWithSize(size int) (kvstore.Batch, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	b := db.NewBatchWithSize(size)
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
