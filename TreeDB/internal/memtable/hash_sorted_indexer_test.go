package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestHashSortedPreferSortedPointProbesForSparseFrozenBatches(t *testing.T) {
	m := NewHashSorted()
	var key [8]byte
	for i := uint64(0); i < 1000; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		m.Set(key[:], []byte("v"))
	}
	m.Freeze()

	binary.BigEndian.PutUint64(key[:], 10)
	first := append([]byte(nil), key[:]...)
	binary.BigEndian.PutUint64(key[:], 900)
	last := append([]byte(nil), key[:]...)
	if !m.PreferSortedPointProbes(first, last, 16) {
		t.Fatal("expected sparse frozen batch to prefer point probes")
	}

	binary.BigEndian.PutUint64(key[:], 10)
	first = append(first[:0], key[:]...)
	binary.BigEndian.PutUint64(key[:], 40)
	last = append(last[:0], key[:]...)
	if m.PreferSortedPointProbes(first, last, 16) {
		t.Fatal("expected dense frozen batch to keep iterator scan")
	}
}

func TestHashSortedIncrementalIndexing_FrozenIteratorSorted(t *testing.T) {
	m := NewHashSorted()

	// Force at least one sealed chunk using long-ish keys so the test stays small.
	const (
		keyLen = 96
		nKeys  = 16384 // ~1.5MiB of key bytes
	)

	keys := make([][]byte, 0, nKeys)
	for i := 0; i < nKeys; i++ {
		k := []byte(fmt.Sprintf("%0*x", keyLen, i))
		keys = append(keys, k)
	}

	// Insert in reverse order to ensure we aren't benefiting from append-only key order.
	for i := len(keys) - 1; i >= 0; i-- {
		m.Set(keys[i], []byte("v"))
	}

	m.Freeze()
	it := m.NewIterator(nil, nil)
	defer it.Close()
	it.Seek(nil)

	var prev []byte
	count := 0
	for it.Valid() {
		k := it.UnsafeKey()
		if prev != nil && bytes.Compare(prev, k) > 0 {
			t.Fatalf("iterator not sorted: prev=%q cur=%q", string(prev), string(k))
		}
		prev = append(prev[:0], k...)
		count++
		it.Next()
	}

	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if count != m.Len() {
		t.Fatalf("iterator count mismatch: got=%d want=%d", count, m.Len())
	}
}

func TestHashSortedIncrementalIndexing_ResetWaitsAndClears(t *testing.T) {
	m := NewHashSorted()

	const (
		keyLen = 96
		nKeys  = 16384 // ensure at least one sealed chunk
	)
	for i := 0; i < nKeys; i++ {
		m.Set([]byte(fmt.Sprintf("%0*x", keyLen, i)), []byte("v"))
	}

	m.Reset()
	if m.Len() != 0 {
		t.Fatalf("expected empty after reset, got %d", m.Len())
	}

	m.Freeze()
	it := m.NewIterator(nil, nil)
	defer it.Close()
	it.Seek(nil)
	if it.Valid() {
		t.Fatalf("expected empty iterator after reset")
	}
}

func TestHashSortedIterator_TombstonesRemainVisible(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("a"), []byte("v"))
	m.Delete([]byte("b"))

	m.Freeze()
	it := m.NewIterator(nil, nil)
	defer it.Close()
	it.Seek(nil)

	if !it.Valid() {
		t.Fatalf("expected iterator valid")
	}
	if got := string(it.UnsafeKey()); got != "a" {
		t.Fatalf("unexpected first key: %q", got)
	}
	if it.IsDeleted() {
		t.Fatalf("unexpected tombstone for key a")
	}
	if got := string(it.UnsafeValue()); got != "v" {
		t.Fatalf("unexpected value for key a: %q", got)
	}

	it.Next()
	if !it.Valid() {
		t.Fatalf("expected iterator valid for key b")
	}
	if got := string(it.UnsafeKey()); got != "b" {
		t.Fatalf("unexpected second key: %q", got)
	}
	if !it.IsDeleted() {
		t.Fatalf("expected tombstone for key b")
	}
	if got := it.UnsafeValue(); got != nil {
		t.Fatalf("expected nil value for tombstone, got %q", string(got))
	}
}

func TestHashSortedFrozenIterator_ReusesSortedKeysBuffer(t *testing.T) {
	m := NewHashSorted()

	// Force multiple sealed chunks using long-ish keys so the test stays small.
	const (
		keyLen = 96
		nKeys  = 25000
	)

	for i := nKeys - 1; i >= 0; i-- {
		m.Set([]byte(fmt.Sprintf("%0*x", keyLen, i)), []byte("v"))
	}

	m.Freeze()
	it := m.NewIterator(nil, nil)
	it.Seek(nil)

	if len(m.sortedKeys) == 0 {
		t.Fatalf("expected non-empty key buffer after freeze")
	}
	before := &m.sortedKeys[0]
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	m.Reset()
	for i := nKeys - 1; i >= 0; i-- {
		m.Set([]byte(fmt.Sprintf("%0*x", keyLen, i)), []byte("v"))
	}

	m.Freeze()
	it2 := m.NewIterator(nil, nil)
	it2.Seek(nil)

	after := &m.sortedKeys[0]
	if before != after {
		t.Fatalf("expected sortedKeys buffer reuse; before=%p after=%p", before, after)
	}
	if err := it2.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
}
