package main

import (
	"flag"

	"github.com/snissn/gomap/kvstore"
	"github.com/tidwall/buntdb"
)

var (
	buntdbSyncPolicy = flag.Int("buntdb-sync", 0, "BuntDB: sync policy (0=Never, 1=EverySecond, 2=Always)")
)

func init() {
	RegisterDB("buntdb", NewBuntDB)
}

type BuntWrapper struct {
	db *buntdb.DB
}

func NewBuntDB(dir string) (kvstore.DB, error) {
	// BuntDB is a single file
	db, err := buntdb.Open(dir + "/data.db")
	if err != nil {
		return nil, err
	}
	var policy buntdb.SyncPolicy
	switch *buntdbSyncPolicy {
	case 1:
		policy = buntdb.EverySecond
	case 2:
		policy = buntdb.Always
	default:
		policy = buntdb.Never
	}
	if err := db.SetConfig(buntdb.Config{
		SyncPolicy: policy,
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &BuntWrapper{db: db}, nil
}

func (b *BuntWrapper) Name() string { return "BuntDB" }
func (b *BuntWrapper) Close() error { return b.db.Close() }

func (b *BuntWrapper) Get(key []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(string(key)) // BuntDB uses strings
		if err == buntdb.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		val = []byte(v)
		return nil
	})
	return val, err
}

func (b *BuntWrapper) Set(key, value []byte) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(string(key), string(value), nil)
		return err
	})
}

func (b *BuntWrapper) Delete(key []byte) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(string(key))
		if err == buntdb.ErrNotFound {
			return nil
		}
		return err
	})
}

// BuntDB Batch
type BuntBatch struct {
	tx *buntdb.Tx
}

func (b *BuntBatch) Set(key, value []byte) error {
	_, _, err := b.tx.Set(string(key), string(value), nil)
	return err
}
func (b *BuntBatch) Delete(key []byte) error {
	_, err := b.tx.Delete(string(key))
	if err == buntdb.ErrNotFound {
		return nil
	}
	return err
}
func (b *BuntBatch) Commit() error {
	return b.tx.Commit()
}
func (b *BuntBatch) CommitSync() error {
	return b.tx.Commit()
}
func (b *BuntBatch) Close() error {
	err := b.tx.Rollback()
	if err == buntdb.ErrTxClosed {
		return nil
	}
	return err
}

func (b *BuntWrapper) NewBatch() (kvstore.Batch, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &BuntBatch{tx: tx}, nil
}

// BuntDB Iterator
type BuntIterator struct {
	tx    *buntdb.Tx
	keys  []string
	vals  []string
	idx   int
	valid bool
}

func (it *BuntIterator) Valid() bool   { return it.valid && it.idx < len(it.keys) }
func (it *BuntIterator) Next()         { it.idx++ }
func (it *BuntIterator) Key() []byte   { return []byte(it.keys[it.idx]) }
func (it *BuntIterator) Value() []byte { return []byte(it.vals[it.idx]) }
func (it *BuntIterator) KeyCopy(dst []byte) []byte {
	return append(dst, it.keys[it.idx]...)
}
func (it *BuntIterator) ValueCopy(dst []byte) []byte {
	return append(dst, it.vals[it.idx]...)
}
func (it *BuntIterator) Error() error { return nil }
func (it *BuntIterator) Close() error {
	return nil
}

func (b *BuntWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	var keys, vals []string

	err := b.db.View(func(tx *buntdb.Tx) error {
		var iterErr error
		pivot := ""
		if start != nil {
			pivot = string(start)
		}

		err := tx.AscendGreaterOrEqual("", pivot, func(k, v string) bool {
			if end != nil && k >= string(end) {
				return false
			}
			keys = append(keys, k)
			vals = append(vals, v)
			return true
		})
		if err != nil {
			iterErr = err
		}
		return iterErr
	})

	if err != nil {
		return nil, err
	}

	return &BuntIterator{keys: keys, vals: vals, valid: true}, nil
}

func (b *BuntWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	var keys, vals []string

	err := b.db.View(func(tx *buntdb.Tx) error {
		err := tx.Descend("", func(k, v string) bool {
			if end != nil && k >= string(end) {
				return true // Skip until we are < End
			}
			if start != nil && k < string(start) {
				return false // Stop
			}
			keys = append(keys, k)
			vals = append(vals, v)
			return true
		})
		return err
	})

	if err != nil {
		return nil, err
	}

	return &BuntIterator{keys: keys, vals: vals, valid: true}, nil
}
