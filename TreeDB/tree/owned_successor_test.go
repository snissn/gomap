package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestOwnedSuccessorExternalLeafRefusesUnownedBuffer(t *testing.T) {
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
		if _, _, e := tr.SeekGEWithFixedScratch([]byte("k"), nil, key[:], leaf[:], &scratch, read); e != ErrOwnedIteratorLeafReader {
			t.Fatalf("unowned read accepted: %v", e)
		}
	}
	read := func(_ page.LeafLogPtr, dst []byte) ([]byte, error) {
		copy(dst[:page.PageSize], data)
		return dst[:page.PageSize], nil
	}
	if got, ok, e := tr.SeekGEWithFixedScratch([]byte("j"), nil, key[:], leaf[:], &scratch, read); e != nil || !ok || string(got.Value) != "v" {
		t.Fatalf("bounded leaf: %+v %v %v", got, ok, e)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatal("global reader used")
	}
}
