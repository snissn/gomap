package hashdbadapter

import (
	"strconv"

	hashdb "github.com/snissn/gomap/HashDB"
	"github.com/snissn/gomap/kvstore"
)

// DB adapts HashDB's sharded engine to kvstore interfaces.
type DB struct {
	DB      *hashdb.HashDB
	NameStr string
}

func Wrap(db *hashdb.HashDB) *DB { return &DB{DB: db, NameStr: "HashDB"} }

func WrapNamed(db *hashdb.HashDB, name string) *DB { return &DB{DB: db, NameStr: name} }

func (d *DB) Name() string {
	if d.NameStr != "" {
		return d.NameStr
	}
	return "HashDB"
}

func (d *DB) Close() error { return d.DB.Close() }

func (d *DB) Get(key []byte) ([]byte, error) { return d.DB.Get(key) }

func (d *DB) Set(key, value []byte) error { return d.DB.Put(key, value) }

func (d *DB) Delete(key []byte) error { return d.DB.Delete(key) }

func (d *DB) SetSync(key, value []byte) error { return d.DB.PutSync(key, value) }

func (d *DB) DeleteSync(key []byte) error { return d.DB.DeleteSync(key) }

// Has is best-effort; HashDB does not currently expose an index-only lookup for the sharded type.
func (d *DB) Has(key []byte) (bool, error) {
	v, err := d.DB.Get(key)
	return v != nil, err
}

func (d *DB) Stats() map[string]string {
	s := d.DB.Stats()
	return map[string]string{
		"KeyCount": strconv.FormatUint(s.KeyCount, 10),
		"Capacity": strconv.FormatUint(s.Capacity, 10),
		"DataSize": strconv.FormatUint(s.DataSize, 10),
		"Segments": strconv.Itoa(s.Segments),
	}
}

func (d *DB) Checkpoint() error { return d.DB.Sync() }

// Compact triggers HashDB slab compaction.
func (d *DB) Compact() error { return d.DB.Compact() }

// Clear removes all keys from the database.
func (d *DB) Clear() error { return d.DB.Clear() }

func (d *DB) ForEach(fn func(key, value []byte) error) error { return d.DB.ForEach(fn) }

func (d *DB) NewBatch() (kvstore.Batch, error) { return &batch{db: d.DB}, nil }

type batch struct {
	db     *hashdb.HashDB
	ops    []hashdb.BatchOp
	closed bool
}

func (b *batch) Set(key, value []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, hashdb.PutOp(k, v))
	return nil
}

func (b *batch) Delete(key []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	b.ops = append(b.ops, hashdb.DeleteOp(k))
	return nil
}

func (b *batch) Commit() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.db.ApplyBatch(ops)
}

func (b *batch) CommitSync() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.db.ApplyBatchSync(ops)
}

func (b *batch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}
