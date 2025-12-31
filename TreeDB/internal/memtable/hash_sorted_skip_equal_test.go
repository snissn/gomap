package memtable

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestHashSorted_SkipEqualOverwriteReusesValue(t *testing.T) {
	m := NewHashSorted()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	v1 := []byte("value-1234")
	m.SetSteal(key, v1)
	got1, deleted1, ok1 := m.Get(key)
	if !ok1 || deleted1 {
		t.Fatalf("expected present key, got ok=%v deleted=%v", ok1, deleted1)
	}
	if !bytes.Equal(got1, v1) {
		t.Fatalf("unexpected value: got %q want %q", got1, v1)
	}
	ptr1 := unsafe.SliceData(got1)

	// Identical overwrite should not allocate/copy a new arena value.
	m.SetSteal(key, v1)
	got2, deleted2, ok2 := m.Get(key)
	if !ok2 || deleted2 {
		t.Fatalf("expected present key after overwrite, got ok=%v deleted=%v", ok2, deleted2)
	}
	if !bytes.Equal(got2, v1) {
		t.Fatalf("unexpected value after overwrite: got %q want %q", got2, v1)
	}
	ptr2 := unsafe.SliceData(got2)
	if ptr2 != ptr1 {
		t.Fatalf("expected value pointer reuse for identical overwrite")
	}

	// A different value (same length) must allocate a new arena value.
	v2 := []byte("value-XXXX")
	m.SetSteal(key, v2)
	got3, deleted3, ok3 := m.Get(key)
	if !ok3 || deleted3 {
		t.Fatalf("expected present key after update, got ok=%v deleted=%v", ok3, deleted3)
	}
	if !bytes.Equal(got3, v2) {
		t.Fatalf("unexpected value after update: got %q want %q", got3, v2)
	}
	ptr3 := unsafe.SliceData(got3)
	if ptr3 == ptr2 {
		t.Fatalf("expected a new value allocation when value bytes differ")
	}
}
