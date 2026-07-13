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

			key, val, ptr, flags, revision, found := mt.SeekGE([]byte("c"), []byte("e"))
			if !found || !bytes.Equal(key, []byte("d")) || val != nil || ptr != (page.ValuePtr{}) || flags&node.FlagTombstone == 0 || revision != 9 {
				t.Fatalf("SeekGE(c,e) = (%q,%q,%+v,%#x,%d,%t), want tombstone d@9", key, val, ptr, flags, revision, found)
			}

			if _, _, _, _, _, found := mt.SeekGE([]byte("d"), []byte("d")); found {
				t.Fatal("SeekGE accepted an empty range")
			}
			if _, _, _, _, _, found := mt.SeekGE([]byte("e"), nil); found {
				t.Fatal("SeekGE returned a key past the table maximum")
			}
		})
	}
}
