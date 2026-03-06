package memtable

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestBTreeGetEntry_PointersAndTombstones(t *testing.T) {
	m := NewBTree()

	hitKey := []byte("hit")
	tombstoneKey := []byte("tomb")
	emptyKey := []byte("empty")
	pointerKey := []byte("ptr")
	missKey := []byte("miss")

	m.Set(hitKey, []byte("value-hit"))
	m.Delete(tombstoneKey)
	m.Set(emptyKey, []byte{})
	ptrWant := page.ValuePtr{Offset: 123, Length: 456, FileID: 7}
	m.SetEntry(pointerKey, nil, ptrWant, node.FlagPointer)

	if got, del, ok := m.Get(hitKey); !ok || del || string(got) != "value-hit" {
		t.Fatalf("Get(hit) = (%q,%v,%v), want (value-hit,false,true)", string(got), del, ok)
	}
	if got, del, ok := m.Get(tombstoneKey); !ok || !del || got != nil {
		t.Fatalf("Get(tombstone) = (%v,%v,%v), want (nil,true,true)", got, del, ok)
	}
	if got, del, ok := m.Get(emptyKey); !ok || del || got != nil {
		t.Fatalf("Get(empty) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
	}
	if got, del, ok := m.Get(pointerKey); !ok || del || got != nil {
		t.Fatalf("Get(pointer) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
	}
	if got, del, ok := m.Get(missKey); ok || del || got != nil {
		t.Fatalf("Get(miss) = (%v,%v,%v), want (nil,false,false)", got, del, ok)
	}

	if got, ptr, flags, ok := m.GetEntry(hitKey); !ok || string(got) != "value-hit" || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
		t.Fatalf("GetEntry(hit) = (%q,%+v,%d,%v), want (value-hit,zero,%d,true)", string(got), ptr, flags, ok, node.FlagInline)
	}
	if got, ptr, flags, ok := m.GetEntry(tombstoneKey); !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagTombstone {
		t.Fatalf("GetEntry(tombstone) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagTombstone)
	}
	if got, ptr, flags, ok := m.GetEntry(emptyKey); !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
		t.Fatalf("GetEntry(empty) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagInline)
	}
	if got, ptr, flags, ok := m.GetEntry(pointerKey); !ok || got != nil || ptr != ptrWant || flags != node.FlagPointer {
		t.Fatalf("GetEntry(pointer) = (%v,%+v,%d,%v), want (nil,%+v,%d,true)", got, ptr, flags, ok, ptrWant, node.FlagPointer)
	}
	if got, ptr, flags, ok := m.GetEntry(missKey); ok || got != nil || ptr != (page.ValuePtr{}) || flags != 0 {
		t.Fatalf("GetEntry(miss) = (%v,%+v,%d,%v), want (nil,zero,0,false)", got, ptr, flags, ok)
	}
}

func TestBTreePointerInlineTailRoundTrip(t *testing.T) {
	m := NewBTree()

	key := []byte("ptr-tail")
	ptrWant := page.ValuePtr{Offset: 11, Length: 22, FileID: 3}
	m.SetEntry(key, []byte("tail"), ptrWant, node.FlagPointer)

	if got, del, ok := m.Get(key); !ok || del || string(got) != "tail" {
		t.Fatalf("Get(pointer tail) = (%q,%v,%v), want (tail,false,true)", string(got), del, ok)
	}
	if got, ptr, flags, ok := m.GetEntry(key); !ok || string(got) != "tail" || ptr != ptrWant || flags != node.FlagPointer {
		t.Fatalf("GetEntry(pointer tail) = (%q,%+v,%d,%v), want (tail,%+v,%d,true)", string(got), ptr, flags, ok, ptrWant, node.FlagPointer)
	}
}

func TestBTreeSetEntryPreservesExtraFlagBits(t *testing.T) {
	m := NewBTree()
	ptr := page.ValuePtr{Offset: 7, Length: 11, FileID: 3}
	const extra = byte(0x40)

	m.SetEntry([]byte("ptr"), []byte("tail"), ptr, node.FlagPointer|extra)
	_, gotPtr, flags, ok := m.GetEntry([]byte("ptr"))
	if !ok {
		t.Fatalf("GetEntry(ptr) missing")
	}
	if gotPtr != ptr {
		t.Fatalf("ptr=%+v want=%+v", gotPtr, ptr)
	}
	if flags != node.FlagPointer|extra {
		t.Fatalf("flags=%#x want=%#x", flags, node.FlagPointer|extra)
	}

	m.SetEntrySteal([]byte("del"), nil, page.ValuePtr{}, node.FlagTombstone|extra)
	_, _, flags, ok = m.GetEntry([]byte("del"))
	if !ok {
		t.Fatalf("GetEntry(del) missing")
	}
	if flags != node.FlagTombstone|extra {
		t.Fatalf("flags=%#x want=%#x", flags, node.FlagTombstone|extra)
	}
}
