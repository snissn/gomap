package tree

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// A split DATA root exercises strict bounds, crossing back over an empty leaf,
// tombstone metadata and pointer metadata without reading pointer values.
func TestOwnedPredecessorSplitBoundsAndMetadata(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	keys := []string{"a", "m", "z"}
	children := make([]uint64, 3)
	ptr := page.ValuePtr{FileID: 7, Offset: 12, Length: 19}
	for i, k := range keys {
		children[i], err = p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		image, e := p.GetForWrite(children[i])
		if e != nil {
			t.Fatal(e)
		}
		b := node.NewBuilder(image, page.PageTypeLeaf)
		b.SetPageID(children[i])
		if i != 1 {
			flags := byte(node.FlagTombstone)
			if i == 2 {
				flags = node.FlagPointer
			}
			if e = b.AddLeafEntryWithRevision([]byte(k), nil, flags, ptr, page.EntryRevision(i+1)); e != nil {
				t.Fatal(e)
			}
		}
		b.FinishNoNode()
	}
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilder(image, page.PageTypeInternal)
	b.SetPageID(root)
	for i, k := range keys {
		if err = b.AddInternalChild([]byte(k), children[i]); err != nil {
			t.Fatal(err)
		}
	}
	b.FinishNoNode()
	tr := New(p, nil, root)
	var key, leaf [page.PageSize]byte
	var scratch OwnedSuccessorScratch
	for _, tc := range []struct{ start, end, want string }{
		{"", "", "z"}, {"", "z", "a"}, {"", "m", "a"}, {"a", "b", "a"}, {"b", "z", ""}, {"", "a", ""}, {"z", "z", ""},
	} {
		var start, end []byte
		if tc.start != "" {
			start = []byte(tc.start)
		}
		if tc.end != "" {
			end = []byte(tc.end)
		}
		got, ok, e := tr.SeekLTWithFixedScratch(start, end, key[:], leaf[:], &scratch, nil)
		if e != nil || ok != (tc.want != "") || (ok && string(got.Key) != tc.want) {
			t.Fatalf("[%q,%q): key=%q found=%v err=%v", tc.start, tc.end, got.Key, ok, e)
		}
		if ok && tc.want == "a" && (got.Flags&node.FlagTombstone == 0 || got.Revision != 1) {
			t.Fatal("tombstone metadata lost")
		}
		if ok && tc.want == "z" && (got.Flags&node.FlagPointer == 0 || got.ValuePtr != ptr || got.Revision != 3) {
			t.Fatal("pointer metadata lost")
		}
	}
}

func TestOwnedPredecessorExternalLeafBorrowedBuffer(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 1, Offset: 8}
	data := make([]byte, page.PageSize)
	n := node.NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
	n.UpdateChecksum()
	reader := &trackedValueReader{mapValueReader: newMapValueReader()}
	reader.values[ptr.ValuePtr()] = data
	tr, closeTree := newTreeWithLeafLogRoot(t, reader, nil, ptr)
	defer closeTree()
	var key, leaf [page.PageSize]byte
	var scratch OwnedSuccessorScratch
	for _, read := range []func(page.LeafLogPtr, []byte) ([]byte, error){nil, func(_ page.LeafLogPtr, _ []byte) ([]byte, error) { return data, nil }} {
		if _, _, err := tr.SeekLTWithFixedScratch(nil, nil, key[:], leaf[:], &scratch, read); err != ErrOwnedIteratorLeafReader {
			t.Fatalf("unowned buffer accepted: %v", err)
		}
	}
	read := func(_ page.LeafLogPtr, dst []byte) ([]byte, error) {
		copy(dst[:page.PageSize], data)
		return dst[:page.PageSize], nil
	}
	got, ok, err := tr.SeekLTWithFixedScratch(nil, nil, key[:], leaf[:], &scratch, read)
	if err != nil || !ok || string(got.Key) != "k" || string(got.Value) != "v" {
		t.Fatalf("bounded leaf key=%q value=%q found=%v err=%v", got.Key, got.Value, ok, err)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatal("global reader used")
	}
}
