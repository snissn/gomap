package main

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/kvstore"
)

type treeDBBackendAdapter struct {
	mu         sync.RWMutex
	db         *treedbdb.DB
	leafLog    treedbdb.LeafPageLogCloser
	name       string
	finalStats map[string]string
	closed     bool
	closeErr   error
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
	if !commandWAL {
		// This hidden adapter intentionally opens the low-level legacy/raw
		// backend rather than a public cached durability profile. Do not stamp a
		// command-WAL profile onto that internal benchmark path.
		opts.ResolvedProfile = ""
		opts.DeprecatedProfileAlias = ""
		opts.UnsafeBenchmarkProfile = false
	}
	opts.CommandWAL = commandWAL
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
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return d.closeErr
	}
	db := d.db
	leafLog := d.leafLog
	d.finalStats = cloneStringMap(db.Stats())

	err := db.Close()
	if leafLog != nil {
		err = errors.Join(err, leafLog.Close())
	}

	d.db = nil
	d.leafLog = nil
	d.closed = true
	d.closeErr = err
	return err
}

func (d *treeDBBackendAdapter) withDB(fn func(*treedbdb.DB) error) error {
	if d == nil {
		return treedbdb.ErrClosed
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil || d.closed {
		return treedbdb.ErrClosed
	}
	return fn(d.db)
}

func (d *treeDBBackendAdapter) Get(key []byte) ([]byte, error) {
	var value []byte
	err := d.withDB(func(db *treedbdb.DB) error {
		var err error
		value, err = db.Get(key)
		return err
	})
	return value, err
}

func (d *treeDBBackendAdapter) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	var snap kvstore.ReadSnapshot
	err := d.withDB(func(db *treedbdb.DB) error {
		snap = db.AcquireSnapshot()
		if snap == nil {
			return kvstore.ErrUnsupported
		}
		return nil
	})
	return snap, err
}

func (d *treeDBBackendAdapter) Set(key, value []byte) error {
	return d.withDB(func(db *treedbdb.DB) error {
		return db.Set(key, value)
	})
}

func (d *treeDBBackendAdapter) Delete(key []byte) error {
	return d.withDB(func(db *treedbdb.DB) error {
		return db.Delete(key)
	})
}

func (d *treeDBBackendAdapter) SetSync(key, value []byte) error {
	return d.withDB(func(db *treedbdb.DB) error {
		return db.SetSync(key, value)
	})
}

func (d *treeDBBackendAdapter) DeleteSync(key []byte) error {
	return d.withDB(func(db *treedbdb.DB) error {
		return db.DeleteSync(key)
	})
}

func (d *treeDBBackendAdapter) Stats() map[string]string {
	if d == nil {
		return map[string]string{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		stats := cloneStringMap(d.finalStats)
		return stats
	}
	if d.db == nil {
		return map[string]string{}
	}
	return d.db.Stats()
}
func (d *treeDBBackendAdapter) Print() error {
	return d.withDB(func(db *treedbdb.DB) error {
		return db.Print()
	})
}
func (d *treeDBBackendAdapter) Has(key []byte) (bool, error) {
	var ok bool
	err := d.withDB(func(db *treedbdb.DB) error {
		var err error
		ok, err = db.Has(key)
		return err
	})
	return ok, err
}
func (d *treeDBBackendAdapter) Checkpoint() error {
	return d.withDB(func(db *treedbdb.DB) error {
		b := db.NewBatch()
		if b == nil {
			return nil
		}
		defer b.Close()
		return b.WriteSync()
	})
}
func (d *treeDBBackendAdapter) Iterator(start, end []byte) (kvstore.Iterator, error) {
	var iter kvstore.Iterator
	err := d.withDB(func(db *treedbdb.DB) error {
		var err error
		iter, err = db.Iterator(start, end)
		return err
	})
	return iter, err
}
func (d *treeDBBackendAdapter) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	var iter kvstore.Iterator
	err := d.withDB(func(db *treedbdb.DB) error {
		var err error
		iter, err = db.ReverseIterator(start, end)
		return err
	})
	return iter, err
}

func (d *treeDBBackendAdapter) NewBatch() (kvstore.Batch, error) {
	var b batch.Interface
	err := d.withDB(func(db *treedbdb.DB) error {
		b = db.NewBatch()
		if b == nil {
			return kvstore.ErrUnsupported
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &treeDBBackendBatch{b: b}, nil
}

func (d *treeDBBackendAdapter) NewBatchWithSize(size int) (kvstore.Batch, error) {
	var b batch.Interface
	err := d.withDB(func(db *treedbdb.DB) error {
		b = db.NewBatchWithSize(size)
		if b == nil {
			return kvstore.ErrUnsupported
		}
		return nil
	})
	if err != nil {
		return nil, err
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
