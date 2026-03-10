package memtable

import (
	"bytes"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
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

func TestBTreePointerEmptySliceCanonicalizesToNil(t *testing.T) {
	ptr := page.ValuePtr{Offset: 21, Length: 34, FileID: 5}

	t.Run("SetEntry", func(t *testing.T) {
		m := NewBTree()
		m.SetEntry([]byte("ptr"), []byte{}, ptr, node.FlagPointer)
		got, del, ok := m.Get([]byte("ptr"))
		if !ok || del || got != nil {
			t.Fatalf("Get(ptr) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
		}
		got, gotPtr, flags, ok := m.GetEntry([]byte("ptr"))
		if !ok || got != nil || gotPtr != ptr || flags != node.FlagPointer {
			t.Fatalf("GetEntry(ptr) = (%v,%+v,%d,%v), want (nil,%+v,%d,true)", got, gotPtr, flags, ok, ptr, node.FlagPointer)
		}
	})

	t.Run("SetEntrySteal", func(t *testing.T) {
		m := NewBTree()
		m.SetEntrySteal([]byte("ptr"), []byte{}, ptr, node.FlagPointer)
		got, del, ok := m.Get([]byte("ptr"))
		if !ok || del || got != nil {
			t.Fatalf("Get(ptr) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
		}
		got, gotPtr, flags, ok := m.GetEntry([]byte("ptr"))
		if !ok || got != nil || gotPtr != ptr || flags != node.FlagPointer {
			t.Fatalf("GetEntry(ptr) = (%v,%+v,%d,%v), want (nil,%+v,%d,true)", got, gotPtr, flags, ok, ptr, node.FlagPointer)
		}
	})

	t.Run("ApplyStealSortedBatch", func(t *testing.T) {
		m := NewBTree()
		m.ApplyStealSortedBatch([]batchpkg.Entry{{
			Type:     batchpkg.OpPut,
			Key:      []byte("ptr"),
			Value:    []byte{},
			ValuePtr: ptr,
			IsPtr:    true,
		}}, nil)
		got, del, ok := m.Get([]byte("ptr"))
		if !ok || del || got != nil {
			t.Fatalf("Get(ptr) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
		}
		got, gotPtr, flags, ok := m.GetEntry([]byte("ptr"))
		if !ok || got != nil || gotPtr != ptr || flags != node.FlagPointer {
			t.Fatalf("GetEntry(ptr) = (%v,%+v,%d,%v), want (nil,%+v,%d,true)", got, gotPtr, flags, ok, ptr, node.FlagPointer)
		}
	})
}

func TestBTreeInlineEmptySliceCanonicalizesToNil(t *testing.T) {
	t.Run("SetEntrySteal", func(t *testing.T) {
		m := NewBTree()
		m.SetEntrySteal([]byte("inline"), []byte{}, page.ValuePtr{}, node.FlagInline)
		got, del, ok := m.Get([]byte("inline"))
		if !ok || del || got != nil {
			t.Fatalf("Get(inline) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
		}
		got, ptr, flags, ok := m.GetEntry([]byte("inline"))
		if !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
			t.Fatalf("GetEntry(inline) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagInline)
		}
	})

	t.Run("ApplyStealSortedBatch", func(t *testing.T) {
		m := NewBTree()
		m.ApplyStealSortedBatch([]batchpkg.Entry{{
			Type:  batchpkg.OpPut,
			Key:   []byte("inline"),
			Value: []byte{},
		}}, nil)
		got, del, ok := m.Get([]byte("inline"))
		if !ok || del || got != nil {
			t.Fatalf("Get(inline) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
		}
		got, ptr, flags, ok := m.GetEntry([]byte("inline"))
		if !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
			t.Fatalf("GetEntry(inline) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagInline)
		}
	})
}

func TestBTreeIteratorUnsafeEntry_ForInlinePointerAndTombstone(t *testing.T) {
	ptrWant := page.ValuePtr{Offset: 99, Length: 123, FileID: 7}
	m := NewBTree()
	m.Set([]byte("a-inline"), []byte("inline"))
	m.SetEntry([]byte("b-pointer"), []byte("tail"), ptrWant, node.FlagPointer)
	m.Delete([]byte("c-tomb"))

	assertIter := func(t *testing.T, name string, it interface {
		Valid() bool
		Next()
		UnsafeKey() []byte
		UnsafeValue() []byte
		UnsafeEntry() ([]byte, page.ValuePtr, byte)
		Close() error
	}, wantKeys []string) {
		t.Helper()
		defer func() {
			if err := it.Close(); err != nil {
				t.Fatalf("%s close: %v", name, err)
			}
		}()

		var gotKeys []string
		for ; it.Valid(); it.Next() {
			key := string(it.UnsafeKey())
			gotKeys = append(gotKeys, key)
			value := it.UnsafeValue()
			entryValue, ptr, flags := it.UnsafeEntry()
			switch key {
			case "a-inline":
				if !bytes.Equal(value, []byte("inline")) || !bytes.Equal(entryValue, []byte("inline")) || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
					t.Fatalf("%s %q = (%q,%q,%+v,%d), want (inline,inline,zero,%d)", name, key, value, entryValue, ptr, flags, node.FlagInline)
				}
			case "b-pointer":
				if !bytes.Equal(value, []byte("tail")) || !bytes.Equal(entryValue, []byte("tail")) || ptr != ptrWant || flags != node.FlagPointer {
					t.Fatalf("%s %q = (%q,%q,%+v,%d), want (tail,tail,%+v,%d)", name, key, value, entryValue, ptr, flags, ptrWant, node.FlagPointer)
				}
			case "c-tomb":
				if value != nil || entryValue != nil || ptr != (page.ValuePtr{}) || flags != node.FlagTombstone {
					t.Fatalf("%s %q = (%v,%v,%+v,%d), want (nil,nil,zero,%d)", name, key, value, entryValue, ptr, flags, node.FlagTombstone)
				}
			default:
				t.Fatalf("%s unexpected key %q", name, key)
			}
		}
		if len(gotKeys) != len(wantKeys) {
			t.Fatalf("%s iterated %v want %v", name, gotKeys, wantKeys)
		}
		for i := range wantKeys {
			if gotKeys[i] != wantKeys[i] {
				t.Fatalf("%s iterated %v want %v", name, gotKeys, wantKeys)
			}
		}
	}

	assertIter(t, "forward", m.NewIterator(nil, nil), []string{"a-inline", "b-pointer", "c-tomb"})
	assertIter(t, "reverse", m.NewReverseIterator(nil, nil), []string{"c-tomb", "b-pointer", "a-inline"})
}
