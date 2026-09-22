package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

func TestLevelDBReverseIterator_CoversAllKeys(t *testing.T) {
	db, err := NewLevelDB(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const n = 100
	var key [8]byte
	val := []byte("v")
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Set(key[:], val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	rs, ok := db.(interface {
		ReverseIterator(start, end []byte) (kvstore.Iterator, error)
	})
	if !ok {
		t.Fatalf("expected LevelDB to implement kvstore.RangeScanner")
	}

	it, err := rs.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	t.Cleanup(func() { _ = it.Close() })

	count := 0
	var prev []byte
	for it.Valid() {
		k := append([]byte(nil), it.Key()...)
		if prev != nil && bytes.Compare(prev, k) <= 0 {
			t.Fatalf("expected descending order, saw %x then %x", prev, k)
		}
		prev = k
		_ = it.Value()
		it.Next()
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if count != n {
		t.Fatalf("expected to visit %d keys, got %d", n, count)
	}
}
