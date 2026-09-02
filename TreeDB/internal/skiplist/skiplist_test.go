package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSkipList_BasicCRUD(t *testing.T) {
	s := New(0)

	// 1. Insert
	s.Put([]byte("key1"), []byte("val1"))
	s.Put([]byte("key2"), []byte("val2"))

	if s.Count() != 2 {
		t.Errorf("Expected count 2, got %d", s.Count())
	}

	// 2. Read
	val, del, found := s.Get([]byte("key1"))
	if !found || del || string(val) != "val1" {
		t.Errorf("Get key1 failed: found=%v del=%v val=%s", found, del, string(val))
	}

	val, del, found = s.Get([]byte("key2"))
	if !found || del || string(val) != "val2" {
		t.Errorf("Get key2 failed: found=%v del=%v val=%s", found, del, string(val))
	}

	// 3. Update (Same Size - Inplace optimization)
	s.Put([]byte("key1"), []byte("val1_upd"))
	val, _, _ = s.Get([]byte("key1"))
	if string(val) != "val1_upd" {
		t.Errorf("Update inplace failed: got %s", string(val))
	}

	// 4. Update (Different Size - New allocation)
	longVal := bytes.Repeat([]byte("L"), 100)
	s.Put([]byte("key1"), longVal)
	val, _, _ = s.Get([]byte("key1"))
	if !bytes.Equal(val, longVal) {
		t.Errorf("Update replace failed")
	}

	// 5. Delete
	s.Delete([]byte("key2"))
	val, del, found = s.Get([]byte("key2"))
	if !found {
		t.Errorf("Expected deleted key to be found (tombstone)")
	}
	if !del {
		t.Errorf("Expected deleted flag to be set")
	}
	if len(val) != 0 {
		t.Errorf("Expected deleted value to be empty")
	}
}

func TestSkipList_ChunkCrossing(t *testing.T) {
	// Chunk size is 65536. We write enough small items to force the allocator
	// to span multiple chunks.
	s := New(0)

	itemCount := 5000
	itemSize := 50 // Approx 50 bytes per item

	for i := 0; i < itemCount; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := fmt.Sprintf("v%04d", i)
		s.Put([]byte(k), []byte(v))
	}

	// We expect multiple chunks to be allocated
	if len(s.chunks) < (itemCount*itemSize)/65536 {
		t.Errorf("Expected multiple chunks, got %d", len(s.chunks))
	}

	// Verify Data Integrity
	for i := 0; i < itemCount; i++ {
		k := fmt.Sprintf("k%04d", i)
		expected := fmt.Sprintf("v%04d", i)
		val, _, found := s.Get([]byte(k))
		if !found || string(val) != expected {
			t.Fatalf("Data corruption at index %d: expected %s, got %s", i, expected, string(val))
		}
	}
}

func TestSkipList_HugeAllocations(t *testing.T) {
	s := New(0)

	// 1. Write Huge Item (> 64KB)
	hugeSize := 100 * 1024 // 100KB
	hugeKey := []byte("huge_key")
	hugeVal := make([]byte, hugeSize)
	rand.Read(hugeVal) // Fill with random data

	s.Put(hugeKey, hugeVal)

	// 2. Write Small Item (Ensure allocator recovers state)
	s.Put([]byte("small"), []byte("small_val"))

	// 3. Verify Huge Item
	val, _, found := s.Get(hugeKey)
	if !found {
		t.Fatal("Huge key not found")
	}
	if !bytes.Equal(val, hugeVal) {
		t.Fatal("Huge value corrupted")
	}

	// 4. Verify Small Item
	val, _, found = s.Get([]byte("small"))
	if !found || string(val) != "small_val" {
		t.Fatal("Small item after huge alloc failed")
	}
}

func TestSkipList_ResetReuse(t *testing.T) {
	s := New(0)

	// Phase 1: Allocate and Fill
	s.Put([]byte("k1"), []byte("v1"))
	initialChunks := len(s.chunks)

	// Phase 2: Reset
	s.Reset()
	if s.Count() != 0 {
		t.Error("Count should be 0 after reset")
	}
	if len(s.chunks) != initialChunks {
		// We expect chunks to be retained, not dropped
		t.Error("Reset should retain allocated chunks")
	}
	_, _, found := s.Get([]byte("k1"))
	if found {
		t.Error("Old key found after reset")
	}

	// Phase 3: Reuse
	// Writing again should overwrite existing memory without new allocs (mostly)
	s.Put([]byte("k1"), []byte("v2")) // Overwrite old spot
	val, _, found := s.Get([]byte("k1"))
	if !found || string(val) != "v2" {
		t.Error("Reuse put failed")
	}
}

func TestSkipList_Iterator(t *testing.T) {
	s := New(0)
	keys := []string{"A", "C", "E", "G"}
	for _, k := range keys {
		s.Put([]byte(k), []byte("val"+k))
	}

	it := s.NewIterator(nil, nil)

	// 1. Sequential Scan
	i := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		if string(it.Key()) != keys[i] {
			t.Errorf("Scan index %d: expected %s, got %s", i, keys[i], string(it.Key()))
		}
		i++
	}
	if i != 4 {
		t.Errorf("Scan did not cover all items")
	}

	// 2. Seek
	it.Seek([]byte("C"))
	if !it.Valid() || string(it.Key()) != "C" {
		t.Error("Seek('C') failed")
	}

	// 3. Seek Between
	it.Seek([]byte("D")) // Should land on E
	if !it.Valid() || string(it.Key()) != "E" {
		t.Error("Seek('D') -> 'E' failed")
	}
}

func TestSkipList_ReverseIteratorBounds(t *testing.T) {
	s := New(0)
	for _, k := range []string{"A", "C", "E", "G"} {
		s.Put([]byte(k), []byte("val"+k))
	}

	it := s.NewReverseIterator([]byte("C"), []byte("G"))
	start, end := it.Domain()
	if string(start) != "C" || string(end) != "G" {
		t.Fatalf("Domain()=(%q,%q) want (%q,%q)", start, end, "C", "G")
	}

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"E", "C"}
	if len(got) != len(want) {
		t.Fatalf("reverse len=%d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reverse[%d]=%q want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestSkipList_ReverseIteratorInvalidStateAccessors(t *testing.T) {
	s := New(0)
	it := s.NewReverseIterator(nil, nil)
	if got := it.UnsafeKey(); got != nil {
		t.Fatalf("UnsafeKey() on invalid iterator = %q, want nil", got)
	}
	if got := it.UnsafeValue(); got != nil {
		t.Fatalf("UnsafeValue() on invalid iterator = %q, want nil", got)
	}
	val, ptr, flags := it.UnsafeEntry()
	if val != nil || ptr != (page.ValuePtr{}) || flags != 0 {
		t.Fatalf("UnsafeEntry() on invalid iterator = (%v, %+v, %#x), want (nil, zero, 0)", val, ptr, flags)
	}
	if it.IsDeleted() {
		t.Fatalf("IsDeleted() on invalid iterator = true, want false")
	}
}

func TestSkipList_RandomStress(t *testing.T) {
	// Fuzz testing with mixed operations to catch boundary/pointer bugs
	s := New(0)
	ref := make(map[string]string)
	rnd := rand.New(rand.NewSource(42))

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("k%d", rnd.Intn(1000))
		op := rnd.Intn(10)

		if op < 7 { // 70% Put
			val := fmt.Sprintf("v%d", i)
			s.Put([]byte(key), []byte(val))
			ref[key] = val
		} else { // 30% Delete
			s.Delete([]byte(key))
			delete(ref, key)
		}
	}

	for k, v := range ref {
		val, del, found := s.Get([]byte(k))
		if !found || del {
			t.Errorf("Key %s missing", k)
		}
		if string(val) != v {
			t.Errorf("Key %s value mismatch", k)
		}
	}
}
