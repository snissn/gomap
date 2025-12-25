package main

import (
	"bytes"
	"strings"

	"github.com/nutsdb/nutsdb"
	"github.com/snissn/gomap/kvstore"
)

func init() {
	RegisterDB("nutsdb", NewNutsDB)
}

type NutsDBWrapper struct {
	db     *nutsdb.DB
	bucket string
}

func NewNutsDB(dir string) (kvstore.DB, error) {
	opt := nutsdb.DefaultOptions
	opt.Dir = dir
	opt.SyncEnable = false

	db, err := nutsdb.Open(opt)
	if err != nil {
		return nil, err
	}

	bucket := "bench"
	// Ensure bucket exists
	err = db.Update(func(tx *nutsdb.Tx) error {
		// Use BTree engine
		return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		db.Close()
		return nil, err
	}

	return &NutsDBWrapper{db: db, bucket: bucket}, nil
}

func (n *NutsDBWrapper) Name() string { return "NutsDB" }
func (n *NutsDBWrapper) Close() error { return n.db.Close() }

func (n *NutsDBWrapper) Get(key []byte) ([]byte, error) {
	var val []byte
	err := n.db.View(func(tx *nutsdb.Tx) error {
		v, err := tx.Get(n.bucket, key)
		if err != nil {
			return err
		}
		val = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		if err.Error() == "key not found" || err == nutsdb.ErrKeyNotFound || err == nutsdb.ErrBucketNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (n *NutsDBWrapper) Set(key, value []byte) error {
	return n.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(n.bucket, key, value, 0)
	})
}

func (n *NutsDBWrapper) Delete(key []byte) error {
	return n.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(n.bucket, key)
	})
}

// NutsDB Batch
type NutsDBBatch struct {
	tx     *nutsdb.Tx
	bucket string
}

func (b *NutsDBBatch) Set(key, value []byte) error {
	return b.tx.Put(b.bucket, key, value, 0)
}
func (b *NutsDBBatch) Delete(key []byte) error {
	return b.tx.Delete(b.bucket, key)
}
func (b *NutsDBBatch) Commit() error {
	return b.tx.Commit()
}
func (b *NutsDBBatch) CommitSync() error {
	return b.tx.Commit()
}
func (b *NutsDBBatch) Close() error {
	// nutsdb Tx.Rollback is not public? usually Commit handles cleanup.
	// But let's check.
	// In NutsDB, if Commit is not called, changes are not applied.
	// There is no Rollback method on nutsdb.Tx.
	return nil
}

func (n *NutsDBWrapper) NewBatch() (kvstore.Batch, error) {
	tx, err := n.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &NutsDBBatch{tx: tx, bucket: n.bucket}, nil
}

func (n *NutsDBWrapper) ForEach(fn func(k, v []byte) error) error {
	return n.db.View(func(tx *nutsdb.Tx) error {
		keys, values, err := tx.GetAll(n.bucket)
		if err != nil {
			if err == nutsdb.ErrBucketNotFound {
				return nil
			}
			return err
		}
		for i := range keys {
			if err := fn(keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

type NutsIterator struct {
	keys   [][]byte
	values [][]byte
	idx    int
}

func (it *NutsIterator) Valid() bool   { return it.idx < len(it.keys) }
func (it *NutsIterator) Next()         { it.idx++ }
func (it *NutsIterator) Key() []byte   { return it.keys[it.idx] }
func (it *NutsIterator) Value() []byte { return it.values[it.idx] }
func (it *NutsIterator) KeyCopy(dst []byte) []byte {
	return append(dst, it.Key()...)
}
func (it *NutsIterator) ValueCopy(dst []byte) []byte {
	return append(dst, it.Value()...)
}
func (it *NutsIterator) Error() error { return nil }
func (it *NutsIterator) Close() error { return nil }

func (n *NutsDBWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	var keys, values [][]byte
	err := n.db.View(func(tx *nutsdb.Tx) error {
		ks, vs, err := tx.GetAll(n.bucket)
		if err != nil {
			return err
		}
		// Filter manually (inefficient but correct for benchmark interface compliance)
		for i := range ks {
			k := ks[i]
			if start != nil && bytes.Compare(k, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(k, end) >= 0 {
				continue
			}
			keys = append(keys, k)
			values = append(values, vs[i])
		}
		return nil
	})

	if err != nil {
		if err == nutsdb.ErrBucketNotFound {
			return &NutsIterator{}, nil
		}
		return nil, err
	}

	return &NutsIterator{keys: keys, values: values}, nil
}

func (n *NutsDBWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := n.Iterator(start, end)
	if err != nil {
		return nil, err
	}

	nit := it.(*NutsIterator)
	// Reverse in place
	for i, j := 0, len(nit.keys)-1; i < j; i, j = i+1, j-1 {
		nit.keys[i], nit.keys[j] = nit.keys[j], nit.keys[i]
		nit.values[i], nit.values[j] = nit.values[j], nit.values[i]
	}
	return nit, nil
}
