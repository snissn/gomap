package main

import (
	"bytes"
	"flag"
	"time"

	"github.com/snissn/gomap/kvstore"
	"go.etcd.io/bbolt"
)

var (
	bboltNoSync = flag.Bool("bbolt-nosync", false, "Bbolt: use NoSync flag")
)

func init() {
	RegisterDB("bbolt", NewBbolt)
}

type BboltWrapper struct {
	db     *bbolt.DB
	bucket []byte
}

func NewBbolt(dir string) (kvstore.DB, error) {
	opts := &bbolt.Options{Timeout: 1 * time.Second}
	if *bboltNoSync {
		opts.NoSync = true
	}
	db, err := bbolt.Open(dir+"/data.db", 0600, opts)
	if err != nil {
		return nil, err
	}

	bucketName := []byte("bench")
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BboltWrapper{db: db, bucket: bucketName}, nil
}

func (b *BboltWrapper) Name() string { return "Bbolt" }
func (b *BboltWrapper) Close() error { return b.db.Close() }

func (b *BboltWrapper) Get(key []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(b.bucket)
		v := b.Get(key)
		if v != nil {
			val = append([]byte(nil), v...)
		}
		return nil
	})
	return val, err
}

func (b *BboltWrapper) Set(key, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(b.bucket).Put(key, value)
	})
}

func (b *BboltWrapper) Delete(key []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(b.bucket).Delete(key)
	})
}

// Bbolt Iterator
type BboltIterator struct {
	tx      *bbolt.Tx
	c       *bbolt.Cursor
	end     []byte
	key     []byte
	val     []byte
	valid   bool
	reverse bool
}

func (it *BboltIterator) Valid() bool   { return it.valid }
func (it *BboltIterator) Key() []byte   { return it.key }
func (it *BboltIterator) Value() []byte { return it.val }
func (it *BboltIterator) KeyCopy(dst []byte) []byte {
	return append(dst, it.key...)
}
func (it *BboltIterator) ValueCopy(dst []byte) []byte {
	return append(dst, it.val...)
}
func (it *BboltIterator) Error() error { return nil }
func (it *BboltIterator) Close() error {
	return it.tx.Rollback()
}

func (it *BboltIterator) Next() {
	if !it.valid {
		return
	}
	var k, v []byte
	if it.reverse {
		k, v = it.c.Prev()
	} else {
		k, v = it.c.Next()
	}
	it.key = k
	it.val = v
	it.valid = k != nil

	if it.valid && it.end != nil {
		cmp := bytes.Compare(k, it.end)
		if !it.reverse && cmp >= 0 {
			it.valid = false
		} else if it.reverse && cmp < 0 {
			it.valid = false
		}
	}
}

func (b *BboltWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	c := tx.Bucket(b.bucket).Cursor()

	var k, v []byte
	if start != nil {
		k, v = c.Seek(start)
	} else {
		k, v = c.First()
	}

	it := &BboltIterator{
		tx:    tx,
		c:     c,
		end:   end,
		key:   k,
		val:   v,
		valid: k != nil,
	}

	if it.valid && end != nil && bytes.Compare(k, end) >= 0 {
		it.valid = false
	}

	return it, nil
}

func (b *BboltWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	c := tx.Bucket(b.bucket).Cursor()

	// For reverse, we want to start from `end` (exclusive) and go down.
	// Bbolt Seek gives >= target.

	var k, v []byte
	target := end
	if target == nil {
		k, v = c.Last()
	} else {
		k, v = c.Seek(target)
		// Seek lands on >= target.
		// If key == target or key > target, we need to go Prev to find < target.
		// If key is nil (end of bucket), we need Last.
		if k == nil {
			k, v = c.Last()
		} else {
			// Found something >= target. Go Prev.
			// Wait, what if we found something but it's AFTER target (gap)?
			// Still, we want the largest key < target.
			// Seek returns key >= target.
			// Prev moves to < target?
			// Yes.
			// Example: keys [10, 20]. Seek(15) -> 20. Prev() -> 10. Correct.
			// Example: keys [10, 20]. Seek(20) -> 20. Prev() -> 10. Correct (end is exclusive).

			// But careful: we must check if we are already at the beginning?
			// If Seek returned first item and it's >= target, then Prev makes it nil (before start).
			// If Seek returned target, Prev makes it < target.

			// Logic: Seek target. Move Prev.
			// Exception: if Seek returned nil, it means everything is < target. So start at Last.
			// wait, Seek returns nil if NO key is >= target.

			// So:
			// If Seek(target) != nil: Move Prev.
			// Else: Move Last.

			// Let's verify.
			k, v = c.Prev()
		}
	}

	it := &BboltIterator{
		tx:      tx,
		c:       c,
		end:     start, // Reverse end is 'start'
		key:     k,
		val:     v,
		valid:   k != nil,
		reverse: true,
	}

	if it.valid && start != nil && bytes.Compare(k, start) < 0 {
		it.valid = false
	}

	return it, nil
}

// Bbolt Batch (Bbolt doesn't have explicit batch, just use a transaction)
type BboltBatch struct {
	tx     *bbolt.Tx
	bucket []byte
	db     *bbolt.DB
}

func (b *BboltBatch) Set(key, value []byte) error {
	return b.tx.Bucket(b.bucket).Put(key, value)
}
func (b *BboltBatch) Delete(key []byte) error {
	return b.tx.Bucket(b.bucket).Delete(key)
}
func (b *BboltBatch) Commit() error {
	return b.tx.Commit()
}
func (b *BboltBatch) CommitSync() error {
	return b.tx.Commit()
}
func (b *BboltBatch) Close() error {
	// Rollback is safe to call even if committed (returns error, which we ignore)
	// But usually we just return nil.
	_ = b.tx.Rollback()
	return nil
}

func (b *BboltWrapper) NewBatch() (kvstore.Batch, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &BboltBatch{tx: tx, bucket: b.bucket, db: b.db}, nil
}
