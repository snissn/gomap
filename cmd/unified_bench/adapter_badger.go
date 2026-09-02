package main

import (
	"bytes"
	"flag"

	"github.com/dgraph-io/badger/v4"
	"github.com/snissn/gomap/kvstore"
)

var (
	badgerNoSync = flag.Bool("badger-nosync", false, "Badger: disable SyncWrites")
)

func init() {
	RegisterDB("badger", NewBadger)
}

type BadgerBatch struct {
	wb *badger.WriteBatch
}

func (b *BadgerBatch) Set(key, value []byte) error { return b.wb.Set(key, value) }
func (b *BadgerBatch) Delete(key []byte) error     { return b.wb.Delete(key) }
func (b *BadgerBatch) Commit() error               { return b.wb.Flush() }
func (b *BadgerBatch) CommitSync() error           { return b.wb.Flush() }
func (b *BadgerBatch) Close() error                { b.wb.Cancel(); return nil }

type BadgerIterator struct {
	txn     *badger.Txn
	it      *badger.Iterator
	end     []byte
	keyBuf  []byte
	valBuf  []byte
	lastErr error
	reverse bool
}

func (i *BadgerIterator) Valid() bool {
	if !i.it.Valid() {
		return false
	}
	if i.end == nil {
		return true
	}
	if i.reverse {
		return bytes.Compare(i.it.Item().Key(), i.end) >= 0
	}
	return bytes.Compare(i.it.Item().Key(), i.end) < 0
}
func (i *BadgerIterator) Next() { i.it.Next() }
func (i *BadgerIterator) Key() []byte {
	i.keyBuf = i.it.Item().KeyCopy(i.keyBuf[:0])
	return i.keyBuf
}
func (i *BadgerIterator) Value() []byte {
	var err error
	i.valBuf, err = i.it.Item().ValueCopy(i.valBuf[:0])
	if err != nil && i.lastErr == nil {
		i.lastErr = err
	}
	return i.valBuf
}

func (i *BadgerIterator) KeyCopy(dst []byte) []byte   { return append(dst, i.Key()...) }
func (i *BadgerIterator) ValueCopy(dst []byte) []byte { return append(dst, i.Value()...) }

func (i *BadgerIterator) Close() error {
	i.it.Close()
	i.txn.Discard()
	return nil
}
func (i *BadgerIterator) Error() error { return i.lastErr }

type BadgerWrapper struct {
	db *badger.DB
}

func NewBadger(dir string) (kvstore.DB, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	if *badgerNoSync {
		opts.SyncWrites = false
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerWrapper{db: db}, nil
}

func (b *BadgerWrapper) Name() string { return "Badger" }
func (b *BadgerWrapper) Checkpoint() error {
	if b == nil || b.db == nil {
		return nil
	}
	return b.db.Sync()
}
func (b *BadgerWrapper) Set(k, v []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}
func (b *BadgerWrapper) Get(k []byte) ([]byte, error) {
	var out []byte
	err := b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		out, err = it.ValueCopy(nil)
		return err
	})
	return out, err
}
func (b *BadgerWrapper) Delete(k []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}
func (b *BadgerWrapper) Close() error { return b.db.Close() }

func (b *BadgerWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	txn := b.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	if start != nil {
		it.Seek(start)
	} else {
		it.Rewind()
	}

	var endCopy []byte
	if end != nil {
		endCopy = append([]byte(nil), end...)
	}
	return &BadgerIterator{txn: txn, it: it, end: endCopy}, nil
}

func (b *BadgerWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	txn := b.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Reverse = true
	it := txn.NewIterator(opts)

	if end == nil {
		it.Rewind()
	} else {
		it.Seek(end)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), end) >= 0 {
				it.Next()
			}
		} else {
			it.Rewind()
		}
	}

	var startCopy []byte
	if start != nil {
		startCopy = append([]byte(nil), start...)
	}
	return &BadgerIterator{txn: txn, it: it, end: startCopy, reverse: true}, nil
}

func (b *BadgerWrapper) NewBatch() (kvstore.Batch, error) {
	return &BadgerBatch{wb: b.db.NewWriteBatch()}, nil
}
