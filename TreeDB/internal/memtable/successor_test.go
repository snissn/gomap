package memtable

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestTableSeekGE_ModeMatrix(t *testing.T) {
	for _, mode := range []Mode{ModeSkiplist, ModeHashSorted, ModeBTree, ModeAppendOnly} {
		t.Run(mode.String(), func(t *testing.T) {
			mt, err := NewWithCapacityMode(64<<10, mode)
			if err != nil {
				t.Fatalf("NewWithCapacityMode: %v", err)
			}
			revisioned, ok := mt.(RevisionTable)
			if !ok {
				t.Fatal("memtable does not implement RevisionTable")
			}
			revisioned.SetEntryWithRevision([]byte("b"), []byte("bee"), page.ValuePtr{}, node.FlagInline, 7)
			revisioned.SetEntryWithRevision([]byte("d"), nil, page.ValuePtr{}, node.FlagTombstone, 9)
			mt.Freeze()

			seeker, ok := mt.(SuccessorTable)
			if !ok {
				t.Fatal("memtable does not implement SuccessorTable")
			}
			key, val, ptr, flags, revision, found := seeker.SeekGE([]byte("c"), []byte("e"))
			if !found || !bytes.Equal(key, []byte("d")) || val != nil || ptr != (page.ValuePtr{}) || flags&node.FlagTombstone == 0 || revision != 9 {
				t.Fatalf("SeekGE(c,e) = (%q,%q,%+v,%#x,%d,%t), want tombstone d@9", key, val, ptr, flags, revision, found)
			}

			if _, _, _, _, _, found := seeker.SeekGE([]byte("d"), []byte("d")); found {
				t.Fatal("SeekGE accepted an empty range")
			}
			if _, _, _, _, _, found := seeker.SeekGE([]byte("e"), nil); found {
				t.Fatal("SeekGE returned a key past the table maximum")
			}
		})
	}
}

func TestHashSortedSeekGE_ExactDoesNotRebuildSortedKeys(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("a"), []byte("aye"))
	m.Set([]byte("c"), []byte("see"))

	// Establish a sorted index, then invalidate it with an interleaved write.
	if key, _, _, _, _, found := m.SeekGE([]byte("b"), nil); !found || !bytes.Equal(key, []byte("c")) {
		t.Fatalf("SeekGE(b) key=%q found=%t, want c,true", key, found)
	}
	ptr := page.ValuePtr{FileID: 42, Offset: 7, Length: 3}
	m.SetEntryWithRevision([]byte("b"), []byte("raw"), ptr, node.FlagPointer, 11)

	m.mu.RLock()
	if m.sortedValid {
		m.mu.RUnlock()
		t.Fatal("interleaved write did not invalidate sorted keys")
	}
	before := append([]string(nil), m.sortedKeys...)
	m.mu.RUnlock()

	key, value, gotPtr, flags, revision, found := m.SeekGE([]byte("b"), []byte("c"))
	if !found || !bytes.Equal(key, []byte("b")) || !bytes.Equal(value, []byte("raw")) || gotPtr != ptr || flags != node.FlagPointer || revision != 11 {
		t.Fatalf("SeekGE(b,c) = (%q,%q,%+v,%#x,%d,%t), want pointer b@11", key, value, gotPtr, flags, revision, found)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.sortedValid {
		t.Fatal("exact SeekGE rebuilt the invalid sorted-key index")
	}
	if len(m.sortedKeys) != len(before) {
		t.Fatalf("exact SeekGE changed sorted-key length from %d to %d", len(before), len(m.sortedKeys))
	}
	for i := range before {
		if m.sortedKeys[i] != before[i] {
			t.Fatalf("exact SeekGE changed sorted key %d from %q to %q", i, before[i], m.sortedKeys[i])
		}
	}
}
