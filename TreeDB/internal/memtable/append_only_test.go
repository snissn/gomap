package memtable

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyNextCapacityGrowthPolicy(t *testing.T) {
	tests := []struct {
		name    string
		current int
		flags   byte
		want    int
	}{
		{name: "below-min", current: appendOnlyMinInitialEntries - 1, flags: 0, want: appendOnlyMinInitialEntries},
		{name: "non-pointer-doubles", current: appendOnlyMinInitialEntries, flags: 0, want: appendOnlyMinInitialEntries * 2},
		{name: "pointer-below-cutoff-quadruples", current: appendOnlyMinInitialEntries, flags: node.FlagPointer, want: appendOnlyMinInitialEntries * 4},
		{name: "pointer-just-below-cutoff-quadruples", current: appendOnlyPointerGrowCutoff - 1, flags: node.FlagPointer, want: (appendOnlyPointerGrowCutoff - 1) * 4},
		{name: "pointer-at-cutoff-doubles", current: appendOnlyPointerGrowCutoff, flags: node.FlagPointer, want: appendOnlyPointerGrowCutoff * 2},
		{name: "pointer-above-cutoff-doubles", current: appendOnlyPointerGrowCutoff + 1, flags: node.FlagPointer, want: (appendOnlyPointerGrowCutoff + 1) * 2},
		{name: "non-pointer-below-cutoff-doubles", current: appendOnlyPointerGrowCutoff - 1, flags: 0, want: (appendOnlyPointerGrowCutoff - 1) * 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := appendOnlyNextCapacity(tc.current, tc.flags); got != tc.want {
				t.Fatalf("appendOnlyNextCapacity(%d, %#x)=%d want=%d", tc.current, tc.flags, got, tc.want)
			}
		})
	}
}

func TestAppendOnlyCRUD(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k1"), []byte("v1b"))
	m.Delete([]byte("k2"))

	val, del, ok := m.Get([]byte("k1"))
	if !ok || del || string(val) != "v1b" {
		t.Fatalf("Get(k1) = (%q,%v,%v), want (v1b,false,true)", string(val), del, ok)
	}

	val, del, ok = m.Get([]byte("k2"))
	if !ok || !del || val != nil {
		t.Fatalf("Get(k2) = (%v,%v,%v), want (nil,true,true)", val, del, ok)
	}
}

func TestAppendOnlyIteratorSortedLatest(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	// Out-of-order + duplicate key forces sort/dedup snapshot path.
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k1"), []byte("v1b"))
	m.Delete([]byte("k2"))

	it := m.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()

	if !it.Valid() || string(it.Key()) != "k1" {
		t.Fatalf("first key = %q, want k1", string(it.Key()))
	}
	if got := string(it.Value()); got != "v1b" {
		t.Fatalf("k1 value = %q, want v1b", got)
	}
	it.Next()

	if !it.Valid() || string(it.Key()) != "k2" {
		t.Fatalf("second key = %q, want k2", string(it.Key()))
	}
	_, _, flags := it.UnsafeEntry()
	if flags&node.FlagTombstone == 0 {
		t.Fatalf("k2 should be tombstone, flags=%d", flags)
	}
	it.Next()

	if it.Valid() {
		t.Fatalf("iterator should be exhausted")
	}
}

func TestAppendOnlyResetClearsLatestIndex(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k2"), []byte("v2"))
	m.Reset()

	if v, del, ok := m.Get([]byte("k1")); ok || del || v != nil {
		t.Fatalf("Get(k1) after reset = (%v,%v,%v), want (nil,false,false)", v, del, ok)
	}

	it := m.NewIterator(nil, nil)
	if it.Valid() {
		t.Fatalf("iterator should be empty after reset")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close after reset: %v", err)
	}

	m.Set([]byte("k1"), []byte("v1b"))
	v, del, ok := m.Get([]byte("k1"))
	if !ok || del || string(v) != "v1b" {
		t.Fatalf("Get(k1) after reset+set = (%q,%v,%v), want (v1b,false,true)", string(v), del, ok)
	}
}

func TestAppendOnlyGetOnly8ByteKeysNoPanic(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	var k2 [8]byte
	var k1 [8]byte
	binary.BigEndian.PutUint64(k2[:], 2)
	binary.BigEndian.PutUint64(k1[:], 1)

	m.Set(k2[:], []byte("v2"))
	m.Set(k1[:], []byte("v1"))
	m.Set(k1[:], []byte("v1b"))

	// Build latest index cache; with only 8-byte keys this populates latest64 but
	// may leave latest nil.
	it := m.NewIterator(nil, nil)
	_ = it.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic for 8-byte-key lookup fast path: %v", r)
		}
	}()

	if v, del, ok := m.Get(k1[:]); !ok || del || string(v) != "v1b" {
		t.Fatalf("Get(k1)=(%q,%v,%v), want (v1b,false,true)", string(v), del, ok)
	}
	if _, _, ok := m.Get([]byte("not-8-byte-key")); ok {
		t.Fatalf("expected non-8-byte key miss")
	}
	if _, _, _, ok := m.GetEntry([]byte("another-miss")); ok {
		t.Fatalf("expected GetEntry miss")
	}
}

func TestAppendOnlyOrderedGetAndGetEntry(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k3"), []byte("v3"))
	m.Delete([]byte("k4"))

	if !m.ordered {
		t.Fatalf("expected ordered table for strictly increasing inserts")
	}

	if v, del, ok := m.Get([]byte("k2")); !ok || del || string(v) != "v2" {
		t.Fatalf("Get(k2)=(%q,%v,%v), want (v2,false,true)", string(v), del, ok)
	}
	if v, del, ok := m.Get([]byte("k4")); !ok || !del || v != nil {
		t.Fatalf("Get(k4)=(%v,%v,%v), want (nil,true,true)", v, del, ok)
	}
	if v, del, ok := m.Get([]byte("k0")); ok || del || v != nil {
		t.Fatalf("Get(k0)=(%v,%v,%v), want (nil,false,false)", v, del, ok)
	}

	v, ptr, flags, ok := m.GetEntry([]byte("k3"))
	if !ok || string(v) != "v3" || flags&node.FlagTombstone != 0 || ptr != (page.ValuePtr{}) {
		t.Fatalf("GetEntry(k3) unexpected result: v=%q ptr=%v flags=%d ok=%v", string(v), ptr, flags, ok)
	}
}

func TestAppendOnlyIteratorUnorderedDoesNotBlockWriter(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered path

	it := m.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()

	done := make(chan struct{})
	go func() {
		m.Set([]byte("k3"), []byte("v3"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("writer blocked while unordered iterator was open")
	}
}
