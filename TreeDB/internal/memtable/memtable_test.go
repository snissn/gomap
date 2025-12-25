package memtable

import (
	"fmt"
	"testing"
	"time"
)

func TestMemtableCRUD(t *testing.T) {
	m := New()

	m.Set([]byte("key1"), []byte("val1"))
	m.Set([]byte("key2"), []byte("val2"))

	val, del, ok := m.Get([]byte("key1"))
	if !ok || del || string(val) != "val1" {
		t.Errorf("Get key1 failed")
	}

	m.Delete([]byte("key1"))
	val, del, ok = m.Get([]byte("key1"))
	if !ok || !del {
		t.Errorf("Delete key1 failed, got ok=%v del=%v", ok, del)
	}

	if m.Size() <= 0 {
		t.Errorf("Size should be > 0")
	}
}

func TestMemtableIterator(t *testing.T) {
	m := New()
	for i := 0; i < 10; i++ {
		m.Set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	it := m.NewIterator(nil, nil)
	defer it.Close()
	it.Seek([]byte("k0"))

	count := 0
	for it.Valid() {
		count++
		it.Next()
	}
	if count != 10 {
		t.Errorf("Expected 10 items, got %d", count)
	}

	it.Seek([]byte("k5"))
	if !it.Valid() || string(it.UnsafeKey()) != "k5" {
		t.Errorf("Seek k5 failed")
	}
	it.Next()
	if !it.Valid() || string(it.UnsafeKey()) != "k6" {
		t.Errorf("Next after k5 failed, got %s", string(it.UnsafeKey()))
	}
}

func TestMemtableIteratorBlocksWritesUntilClose(t *testing.T) {
	m := New()
	m.Set([]byte("a"), []byte("1"))

	it := m.NewIterator(nil, nil)

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		m.Set([]byte("b"), []byte("2"))
		close(done)
	}()
	<-started

	select {
	case <-done:
		t.Fatalf("expected writer to block while iterator is open")
	case <-time.After(200 * time.Millisecond):
	}

	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("writer did not proceed after iterator closed")
	}
}
