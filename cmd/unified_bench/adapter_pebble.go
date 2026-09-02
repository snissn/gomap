package main

import (
	"bytes"
	"flag"

	"github.com/cockroachdb/pebble"
	"github.com/snissn/gomap/kvstore"
)

var (
	pebbleNoSync  = flag.Bool("pebble-nosync", false, "Pebble: use NoSync for writes")
	pebbleCacheMB = flag.Int("pebble-cache-mb", 1024, "Pebble: block cache size in MiB")
)

func init() {
	RegisterDB("pebble", NewPebble)
}

type PebbleWrapper struct {
	db *pebble.DB
}

func NewPebble(dir string) (kvstore.DB, error) {
	opts := &pebble.Options{
		Cache: pebble.NewCache(int64(*pebbleCacheMB) * 1024 * 1024),
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleWrapper{db: db}, nil
}

func (p *PebbleWrapper) Name() string { return "Pebble" }
func (p *PebbleWrapper) Close() error { return p.db.Close() }

func (p *PebbleWrapper) Get(key []byte) ([]byte, error) {
	val, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ret := append([]byte(nil), val...)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (p *PebbleWrapper) Set(key, value []byte) error {
	opts := pebble.Sync
	if *pebbleNoSync {
		opts = pebble.NoSync
	}
	return p.db.Set(key, value, opts)
}

func (p *PebbleWrapper) Delete(key []byte) error {
	opts := pebble.Sync
	if *pebbleNoSync {
		opts = pebble.NoSync
	}
	return p.db.Delete(key, opts)
}

func (p *PebbleWrapper) RangeDeleteMode() string {
	return kvstore.RangeDeleteModeNative
}

func (p *PebbleWrapper) Checkpoint() error {
	return p.db.Flush()
}

// Pebble Iterator
type PebbleIterator struct {
	iter    *pebble.Iterator
	reverse bool
	end     []byte
}

func (i *PebbleIterator) Valid() bool {
	if !i.iter.Valid() {
		return false
	}
	if i.end == nil {
		return true
	}
	k := i.iter.Key()
	if i.reverse {
		return bytes.Compare(k, i.end) >= 0
	}
	return bytes.Compare(k, i.end) < 0
}
func (i *PebbleIterator) Next() {
	if i.reverse {
		i.iter.Prev()
	} else {
		i.iter.Next()
	}
}
func (i *PebbleIterator) Key() []byte { return i.iter.Key() }
func (i *PebbleIterator) Value() []byte {
	return i.iter.Value()
}
func (i *PebbleIterator) KeyCopy(dst []byte) []byte {
	return append(dst, i.Key()...)
}
func (i *PebbleIterator) ValueCopy(dst []byte) []byte {
	return append(dst, i.Value()...)
}
func (i *PebbleIterator) Close() error { return i.iter.Close() }
func (i *PebbleIterator) Error() error { return i.iter.Error() }

func (p *PebbleWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	opts := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	it, err := p.db.NewIter(opts)
	if err != nil {
		return nil, err
	}
	it.First()
	return &PebbleIterator{iter: it, end: end}, nil
}

func (p *PebbleWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	opts := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	it, err := p.db.NewIter(opts)
	if err != nil {
		return nil, err
	}
	it.Last()
	return &PebbleIterator{iter: it, reverse: true, end: start}, nil
}

// Pebble Batch
type PebbleBatch struct {
	batch *pebble.Batch
}

func (b *PebbleBatch) Set(key, value []byte) error { return b.batch.Set(key, value, nil) }
func (b *PebbleBatch) Delete(key []byte) error     { return b.batch.Delete(key, nil) }
func (b *PebbleBatch) DeleteRange(start, end []byte) error {
	return b.batch.DeleteRange(start, end, nil)
}
func (b *PebbleBatch) Commit() error     { return b.batch.Commit(pebble.NoSync) }
func (b *PebbleBatch) CommitSync() error { return b.batch.Commit(pebble.Sync) }
func (b *PebbleBatch) Close() error      { return b.batch.Close() }

func (p *PebbleWrapper) NewBatch() (kvstore.Batch, error) {
	return &PebbleBatch{batch: p.db.NewBatch()}, nil
}
