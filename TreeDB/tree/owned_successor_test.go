package tree

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

func TestOwnedSuccessorPrimarySelectionAndErrors(t *testing.T) {
	for _, kind := range []string{"put", "absence", "bad_base", "bad_component"} {
		t.Run(kind, func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Alloc(2); err != nil {
				t.Fatal(err)
			}
			makeLeaf := func(keys, values []string) (uint64, []byte) {
				id, e := p.Alloc(1)
				if e != nil {
					t.Fatal(e)
				}
				image, e := p.GetForWrite(id)
				if e != nil {
					t.Fatal(e)
				}
				b := node.NewBuilder(image, page.PageTypeLeaf)
				b.SetPageID(id)
				for i, k := range keys {
					if e = b.AddLeafEntryWithRevision([]byte(k), []byte(values[i]), node.FlagInline, page.ValuePtr{}, 1); e != nil {
						t.Fatal(e)
					}
				}
				b.FinishNoNode()
				return id, image
			}
			baseID, baseImage := makeLeaf([]string{"a", "c"}, []string{"base-a", "base-c"})
			componentID, component := makeLeaf([]string{"c"}, []string{"delta-c"})
			base := node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(baseImage)}
			entry := node.PrimaryDirectoryEntry{Key: []byte("c"), Revision: 1, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(componentID), Digest: sha256.Sum256(component)}}
			if kind == "absence" {
				entry = node.PrimaryDirectoryEntry{Key: []byte("c"), Revision: 1, Kind: node.PrimaryAbsence}
			}
			if kind == "bad_base" {
				base.Digest[0] ^= 1
			}
			if kind == "bad_component" {
				entry.Operand.Digest[0] ^= 1
			}
			root, e := p.Alloc(1)
			if e != nil {
				t.Fatal(e)
			}
			image, e := p.GetForWrite(root)
			if e != nil {
				t.Fatal(e)
			}
			if e = node.EncodePrimaryDirectory(image, root, 1, base, []node.PrimaryDirectoryEntry{entry}); e != nil {
				t.Fatal(e)
			}
			tr := New(p, nil, root)
			var key, leaf [page.PageSize]byte
			var scratch OwnedSuccessorScratch
			scratch.BeginLeafBatch()
			defer scratch.EndLeafBatch()
			got, ok, e := tr.SeekGEWithFixedScratch([]byte("b"), nil, key[:], leaf[:], &scratch, nil)
			if kind == "bad_base" || kind == "bad_component" {
				if e == nil {
					t.Fatal("corrupt operand accepted")
				}
				return
			}
			if scratch.batchLeaf != nil {
				t.Fatal("PRIMARY retained a reusable leaf")
			}
			if e != nil || !ok || string(got.Key) != "c" {
				t.Fatalf("entry=%+v found=%v err=%v", got, ok, e)
			}
			if kind == "absence" {
				if got.Flags&node.FlagTombstone == 0 {
					t.Fatal("absence lost")
				}
			} else if string(got.Value) != "delta-c" {
				t.Fatal("equal-key PRIMARY precedence lost")
			}
			if _, ok, e = tr.SeekGEWithFixedScratch([]byte("d"), nil, key[:], leaf[:], &scratch, nil); e != nil || ok {
				t.Fatalf("absence=%v %v", ok, e)
			}
			if _, ok, e = tr.SeekGEWithFixedScratch([]byte("b"), []byte("c"), key[:], leaf[:], &scratch, nil); e != nil || ok {
				t.Fatalf("exclusive end=%v %v", ok, e)
			}
		})
	}
}

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
