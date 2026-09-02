//go:build lmdb
// +build lmdb

package main

import (
	"bytes"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/snissn/gomap/kvstore"
)

func init() {
	RegisterDB("lmdb", NewLMDB)
}

type LMDBWrapper struct {
	env *lmdb.Env
	dbi lmdb.DBI
	dir string
}

func NewLMDB(dir string) (kvstore.DB, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err := env.SetMapSize(*lmdbMapSize); err != nil {
		return nil, err
	}
	if err := env.SetMaxDBs(10); err != nil {
		return nil, err
	}
	var flags uint = 0
	if *lmdbNoSync {
		flags |= lmdb.NoSync
	}
	if *lmdbNoMetaSync {
		flags |= lmdb.NoMetaSync
	}
	if *lmdbWriteMap {
		flags |= lmdb.WriteMap
	}

	// LMDB needs the directory to exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	if err := env.Open(dir, flags, 0644); err != nil {
		return nil, err
	}

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("bench", lmdb.Create)
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}

	return &LMDBWrapper{env: env, dbi: dbi, dir: dir}, nil
}

func (l *LMDBWrapper) Name() string { return "LMDB" }

func (l *LMDBWrapper) Close() error {
	l.env.CloseDBI(l.dbi)
	return l.env.Close()
}

func (l *LMDBWrapper) Get(key []byte) ([]byte, error) {
	var val []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		v, err := txn.Get(l.dbi, key)
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		// Must copy because v is invalid after txn closes
		val = append([]byte(nil), v...)
		return nil
	})
	return val, err
}

func (l *LMDBWrapper) Set(key, value []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(l.dbi, key, value, 0)
	})
}

func (l *LMDBWrapper) Delete(key []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		err := txn.Del(l.dbi, key, nil)
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
}

// LMDB Iterator
type LMDBIterator struct {
	txn     *lmdb.Txn
	cursor  *lmdb.Cursor
	end     []byte
	key     []byte
	val     []byte
	valid   bool
	reverse bool
	err     error
}

func (it *LMDBIterator) Valid() bool   { return it.valid }
func (it *LMDBIterator) Key() []byte   { return it.key }
func (it *LMDBIterator) Value() []byte { return it.val }
func (it *LMDBIterator) KeyCopy(dst []byte) []byte {
	return append(dst, it.key...)
}
func (it *LMDBIterator) ValueCopy(dst []byte) []byte {
	return append(dst, it.val...)
}
func (it *LMDBIterator) Error() error { return it.err }

func (it *LMDBIterator) Close() error {
	it.cursor.Close()
	// In LMDB read-only txns must be aborted (or committed, same effect) to release lock
	it.txn.Abort()
	return nil
}

func (it *LMDBIterator) Next() {
	if !it.valid {
		return
	}
	var op uint
	if it.reverse {
		op = lmdb.Prev
	} else {
		op = lmdb.Next
	}
	k, v, err := it.cursor.Get(nil, nil, op)
	if lmdb.IsNotFound(err) {
		it.valid = false
		return
	}
	if err != nil {
		it.err = err
		it.valid = false
		return
	}
	// Check bounds
	if it.end != nil {
		cmp := bytes.Compare(k, it.end)
		if !it.reverse && cmp >= 0 {
			it.valid = false
			return
		}
		if it.reverse && cmp < 0 {
			it.valid = false
			return
		}
	}
	it.key = k
	it.val = v
}

func (l *LMDBWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	txn, err := l.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return nil, err
	}
	cursor, err := txn.OpenCursor(l.dbi)
	if err != nil {
		txn.Abort()
		return nil, err
	}

	it := &LMDBIterator{
		txn:    txn,
		cursor: cursor,
		end:    end,
		valid:  true,
	}

	var k, v []byte
	if start != nil {
		k, v, err = cursor.Get(start, nil, lmdb.SetRange)
	} else {
		k, v, err = cursor.Get(nil, nil, lmdb.First)
	}

	if lmdb.IsNotFound(err) {
		it.valid = false
		err = nil
	}
	if err != nil {
		it.Close()
		return nil, err
	}

	if it.valid && end != nil && bytes.Compare(k, end) >= 0 {
		it.valid = false
	}

	it.key = k
	it.val = v
	return it, nil
}

func (l *LMDBWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	txn, err := l.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return nil, err
	}
	cursor, err := txn.OpenCursor(l.dbi)
	if err != nil {
		txn.Abort()
		return nil, err
	}

	it := &LMDBIterator{
		txn:     txn,
		cursor:  cursor,
		end:     start,
		valid:   true,
		reverse: true,
	}

	var k, v []byte
	target := end

	if target == nil {
		k, v, err = cursor.Get(nil, nil, lmdb.Last)
	} else {
		k, v, err = cursor.Get(target, nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			k, v, err = cursor.Get(nil, nil, lmdb.Last)
		} else if err == nil {
			k, v, err = cursor.Get(nil, nil, lmdb.Prev)
		}
	}

	if lmdb.IsNotFound(err) {
		it.valid = false
		err = nil
	}
	if err != nil {
		it.Close()
		return nil, err
	}

	if it.valid && start != nil {
		if bytes.Compare(k, start) < 0 {
			it.valid = false
		}
	}

	it.key = k
	it.val = v
	return it, nil
}

// LMDB Batch
type LMDBBatch struct {
	txn *lmdb.Txn
	dbi lmdb.DBI
}

func (b *LMDBBatch) Set(key, value []byte) error {
	return b.txn.Put(b.dbi, key, value, 0)
}
func (b *LMDBBatch) Delete(key []byte) error {
	err := b.txn.Del(b.dbi, key, nil)
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}
func (b *LMDBBatch) Commit() error {
	return b.txn.Commit()
}
func (b *LMDBBatch) CommitSync() error {
	return b.txn.Commit()
}
func (b *LMDBBatch) Close() error {
	b.txn.Abort()
	return nil
}

func (l *LMDBWrapper) NewBatch() (kvstore.Batch, error) {
	txn, err := l.env.BeginTxn(nil, 0)
	if err != nil {
		return nil, err
	}
	return &LMDBBatch{txn: txn, dbi: l.dbi}, nil
}
