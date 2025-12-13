package memtable

import (
	"fmt"
	"testing"
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
	
	it := m.NewIterator()
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
