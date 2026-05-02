//go:build tidesdb

package main

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/snissn/gomap/kvstore"
	tidesdb "github.com/tidesdb/tidesdb-go"
)

func init() {
	RegisterDB("tidesdb", NewTidesDB)
}

type tidesWrapper struct {
	db *tidesdb.DB
}

func NewTidesDB(dir string) (kvstore.DB, error) {
	db, err := tidesdb.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open tidesdb: %w", err)
	}
	return &tidesWrapper{db: db}, nil
}

func (t *tidesWrapper) Name() string { return "TidesDB" }
func (t *tidesWrapper) Close() error { return t.db.Close() }

func (t *tidesWrapper) Get(key []byte) ([]byte, error) {
	v, err := t.db.Get(key)
	if err != nil {
		if isTidesNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (t *tidesWrapper) Set(key, value []byte) error {
	return t.db.Put(key, value)
}

func (t *tidesWrapper) Delete(key []byte) error {
	err := t.db.Delete(key)
	if err != nil && !isTidesNotFound(err) {
		return err
	}
	return nil
}

type tidesBatch struct {
	db  *tidesdb.DB
	ops []tidesBatchOp
}
type tidesBatchOp struct {
	del   bool
	key   []byte
	value []byte
}

func (t *tidesWrapper) NewBatch() (kvstore.Batch, error) {
	return &tidesBatch{db: t.db}, nil
}
func (b *tidesBatch) Set(key, value []byte) error {
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, tidesBatchOp{key: k, value: v})
	return nil
}
func (b *tidesBatch) Delete(key []byte) error {
	k := append([]byte(nil), key...)
	b.ops = append(b.ops, tidesBatchOp{del: true, key: k})
	return nil
}
func (b *tidesBatch) Commit() error     { return b.apply() }
func (b *tidesBatch) CommitSync() error { return b.apply() }
func (b *tidesBatch) Close() error      { b.ops = nil; return nil }
func (b *tidesBatch) apply() error {
	for _, op := range b.ops {
		if op.del {
			if err := b.db.Delete(op.key); err != nil && !isTidesNotFound(err) {
				return err
			}
			continue
		}
		if err := b.db.Put(op.key, op.value); err != nil {
			return err
		}
	}
	b.ops = nil
	return nil
}

type tidesIter struct {
	keys [][]byte
	vals [][]byte
	idx  int
}

func (it *tidesIter) Valid() bool { return it.idx < len(it.keys) }
func (it *tidesIter) Next()       { it.idx++ }
func (it *tidesIter) Key() []byte { return it.keys[it.idx] }
func (it *tidesIter) Value() []byte {
	return it.vals[it.idx]
}
func (it *tidesIter) KeyCopy(dst []byte) []byte   { return append(dst, it.keys[it.idx]...) }
func (it *tidesIter) ValueCopy(dst []byte) []byte { return append(dst, it.vals[it.idx]...) }
func (it *tidesIter) Error() error                { return nil }
func (it *tidesIter) Close() error                { return nil }

func (t *tidesWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	pairs, err := t.db.Scan(start, end)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(pairs))
	vals := make([][]byte, 0, len(pairs))
	for _, p := range pairs {
		keys = append(keys, append([]byte(nil), p.Key...))
		vals = append(vals, append([]byte(nil), p.Value...))
	}
	return &tidesIter{keys: keys, vals: vals}, nil
}

func (t *tidesWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := t.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	tit := it.(*tidesIter)
	for i, j := 0, len(tit.keys)-1; i < j; i, j = i+1, j-1 {
		tit.keys[i], tit.keys[j] = tit.keys[j], tit.keys[i]
		tit.vals[i], tit.vals[j] = tit.vals[j], tit.vals[i]
	}
	return tit, nil
}

func isTidesNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, tidesdb.ErrKeyNotFound) {
		return true
	}
	return bytes.Contains([]byte(err.Error()), []byte("not found"))
}
