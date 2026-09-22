package btreeonhashdbadapter

import (
	hashdb "github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
	"github.com/snissn/gomap/kvstore"
)

// DB adapts BTreeOnHashDB to kvstore interfaces and closes the underlying HashDB.
type DB struct {
	Tree    *btreeonhashdb.Tree
	Store   *hashdb.HashDB
	NameStr string
}

func Wrap(store *hashdb.HashDB, tree *btreeonhashdb.Tree) *DB {
	return &DB{Store: store, Tree: tree, NameStr: "BTreeOnHashDB"}
}

func WrapNamed(store *hashdb.HashDB, tree *btreeonhashdb.Tree, name string) *DB {
	return &DB{Store: store, Tree: tree, NameStr: name}
}

func (d *DB) Name() string {
	if d.NameStr != "" {
		return d.NameStr
	}
	return "BTreeOnHashDB"
}

func (d *DB) Close() error { return d.Store.Close() }

func (d *DB) Get(key []byte) ([]byte, error) { return d.Tree.Get(key) }

func (d *DB) Set(key, value []byte) error { return d.Tree.Put(key, value) }

func (d *DB) Delete(key []byte) error { return d.Tree.Delete(key) }

func (d *DB) Checkpoint() error { return d.Store.Sync() }

func (d *DB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.Tree.Range(start, end)
	if err != nil {
		return nil, err
	}
	return &iter{it: it}, nil
}

func (d *DB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.Tree.ReverseRange(start, end)
	if err != nil {
		return nil, err
	}
	return &revIter{it: it}, nil
}

func (d *DB) NewBatch() (kvstore.Batch, error) {
	return &treeBatch{t: d.Tree}, nil
}

type iter struct {
	it *btreeonhashdb.Iter
}

func (i *iter) Valid() bool { return i.it.Valid() }

func (i *iter) Next() { i.it.Next() }

func (i *iter) Key() []byte { return i.it.Key() }

func (i *iter) Value() []byte { return i.it.Value() }

func (i *iter) KeyCopy(dst []byte) []byte { return append(dst, i.Key()...) }

func (i *iter) ValueCopy(dst []byte) []byte { return append(dst, i.Value()...) }

func (i *iter) Error() error { return i.it.Error() }

func (i *iter) Close() error { i.it.Close(); return nil }

type revIter struct {
	it *btreeonhashdb.RevIter
}

func (i *revIter) Valid() bool { return i.it.Valid() }

func (i *revIter) Next() { i.it.Next() }

func (i *revIter) Key() []byte { return i.it.Key() }

func (i *revIter) Value() []byte { return i.it.Value() }

func (i *revIter) KeyCopy(dst []byte) []byte { return append(dst, i.Key()...) }

func (i *revIter) ValueCopy(dst []byte) []byte { return append(dst, i.Value()...) }

func (i *revIter) Error() error { return i.it.Error() }

func (i *revIter) Close() error { i.it.Close(); return nil }

type treeBatch struct {
	t      *btreeonhashdb.Tree
	ops    []op
	closed bool
	chunk  []byte
}

type op struct {
	del bool
	key []byte
	val []byte
}

func (b *treeBatch) add(data []byte) []byte {
	if len(b.chunk)+len(data) > cap(b.chunk) {
		size := 64 * 1024
		if len(data) > size {
			size = len(data)
		}
		b.chunk = make([]byte, 0, size)
	}
	start := len(b.chunk)
	b.chunk = append(b.chunk, data...)
	return b.chunk[start : start+len(data)]
}

func (b *treeBatch) Set(key, value []byte) error {
	if b.closed {
		return nil
	}
	b.ops = append(b.ops, op{key: b.add(key), val: b.add(value)})
	return nil
}

func (b *treeBatch) Delete(key []byte) error {
	if b.closed {
		return nil
	}
	b.ops = append(b.ops, op{del: true, key: b.add(key)})
	return nil
}

func (b *treeBatch) Commit() error {
	if b.closed {
		return nil
	}

	var setKeys [][]byte
	var setVals [][]byte

	flushSets := func() error {
		if len(setKeys) > 0 {
			if err := b.t.PutMany(setKeys, setVals); err != nil {
				return err
			}
			setKeys = nil
			setVals = nil
		}
		return nil
	}

	for _, op := range b.ops {
		if op.del {
			if err := flushSets(); err != nil {
				return err
			}
			if err := b.t.Delete(op.key); err != nil {
				return err
			}
		} else {
			setKeys = append(setKeys, op.key)
			setVals = append(setVals, op.val)
		}
	}
	if err := flushSets(); err != nil {
		return err
	}

	b.ops = nil
	b.chunk = nil
	return nil
}

func (b *treeBatch) CommitSync() error { return kvstore.ErrUnsupported }

func (b *treeBatch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}
