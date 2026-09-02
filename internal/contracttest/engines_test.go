package contracttest

import (
	"errors"

	"github.com/snissn/gomap/HashDB"
	"github.com/snissn/gomap/TreeDB"
)

type kv interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	PutSync(key, value []byte) error
	Delete(key []byte) error
	DeleteSync(key []byte) error
	Close() error
}

type batchKV interface {
	kv
	ApplyBatch(ops []hashdb.BatchOp) error
	ApplyBatchSync(ops []hashdb.BatchOp) error
}

type iterableKV interface {
	kv
	ForEach(fn func(key, value []byte) error) error
}

type engine interface {
	Name() string
	Open(dir string) (kv, error)
}

type engineFunc struct {
	name string
	open func(dir string) (kv, error)
}

func (e engineFunc) Name() string                { return e.name }
func (e engineFunc) Open(dir string) (kv, error) { return e.open(dir) }

type hashdbSingle struct{ db *hashdb.DB }

func (h hashdbSingle) Get(key []byte) ([]byte, error)  { return h.db.Get(key) }
func (h hashdbSingle) Put(key, value []byte) error     { return h.db.Put(key, value) }
func (h hashdbSingle) PutSync(key, value []byte) error { return h.db.PutSync(key, value) }
func (h hashdbSingle) Delete(key []byte) error         { return h.db.Delete(key) }
func (h hashdbSingle) DeleteSync(key []byte) error     { return h.db.DeleteSync(key) }
func (h hashdbSingle) ApplyBatch(ops []hashdb.BatchOp) error {
	return h.db.ApplyBatch(ops)
}
func (h hashdbSingle) ApplyBatchSync(ops []hashdb.BatchOp) error {
	return h.db.ApplyBatchSync(ops)
}
func (h hashdbSingle) ForEach(fn func(key, value []byte) error) error { return h.db.ForEach(fn) }
func (h hashdbSingle) Close() error                                   { return h.db.Close() }

type hashdbSharded struct{ db *hashdb.HashDB }

func (h hashdbSharded) Get(key []byte) ([]byte, error)  { return h.db.Get(key) }
func (h hashdbSharded) Put(key, value []byte) error     { return h.db.Put(key, value) }
func (h hashdbSharded) PutSync(key, value []byte) error { return h.db.PutSync(key, value) }
func (h hashdbSharded) Delete(key []byte) error         { return h.db.Delete(key) }
func (h hashdbSharded) DeleteSync(key []byte) error     { return h.db.DeleteSync(key) }
func (h hashdbSharded) ApplyBatch(ops []hashdb.BatchOp) error {
	return h.db.ApplyBatch(ops)
}
func (h hashdbSharded) ApplyBatchSync(ops []hashdb.BatchOp) error {
	return h.db.ApplyBatchSync(ops)
}
func (h hashdbSharded) ForEach(fn func(key, value []byte) error) error { return h.db.ForEach(fn) }
func (h hashdbSharded) Close() error                                   { return h.db.Close() }

type treedbCached struct{ db *treedb.DB }

func (t treedbCached) Get(key []byte) ([]byte, error)  { return t.db.Get(key) }
func (t treedbCached) Put(key, value []byte) error     { return t.db.Set(key, value) }
func (t treedbCached) PutSync(key, value []byte) error { return t.db.SetSync(key, value) }
func (t treedbCached) Delete(key []byte) error         { return t.db.Delete(key) }
func (t treedbCached) DeleteSync(key []byte) error     { return t.db.DeleteSync(key) }
func (t treedbCached) ApplyBatch(ops []hashdb.BatchOp) error {
	b := t.db.NewBatch()
	if b == nil {
		return errors.New("treedb: batch unavailable (db closed)")
	}
	defer b.Close()
	for _, op := range ops {
		switch op.Type {
		case hashdb.BatchOpPut:
			if err := b.Set(op.Key, op.Value); err != nil {
				return err
			}
		case hashdb.BatchOpDelete:
			if err := b.Delete(op.Key); err != nil {
				return err
			}
		default:
			return errors.New("treedb: unknown batch op")
		}
	}
	return b.Write()
}
func (t treedbCached) ApplyBatchSync(ops []hashdb.BatchOp) error {
	b := t.db.NewBatch()
	if b == nil {
		return errors.New("treedb: batch unavailable (db closed)")
	}
	defer b.Close()
	for _, op := range ops {
		switch op.Type {
		case hashdb.BatchOpPut:
			if err := b.Set(op.Key, op.Value); err != nil {
				return err
			}
		case hashdb.BatchOpDelete:
			if err := b.Delete(op.Key); err != nil {
				return err
			}
		default:
			return errors.New("treedb: unknown batch op")
		}
	}
	return b.WriteSync()
}
func (t treedbCached) ForEach(fn func(key, value []byte) error) error {
	it, err := t.db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Valid() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		if err := fn(k, v); err != nil {
			return err
		}
		it.Next()
	}
	return it.Error()
}
func (t treedbCached) Close() error { return t.db.Close() }

func openEngine(name, dir string) (kv, error) {
	switch name {
	case "hashdb-single":
		db, err := hashdb.OpenSingle(dir)
		if err != nil {
			return nil, err
		}
		return hashdbSingle{db: db}, nil
	case "hashdb-sharded":
		db, err := hashdb.OpenWithShards(dir, 8)
		if err != nil {
			return nil, err
		}
		return hashdbSharded{db: db}, nil
	case "treedb-cached":
		db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
		if err != nil {
			return nil, err
		}
		return treedbCached{db: db}, nil
	default:
		return nil, errors.New("unknown engine: " + name)
	}
}
