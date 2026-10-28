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
	for _, kind := range []string{"put", "absence", "bad_base", "bad_base_type", "bad_component"} {
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
			if kind == "bad_base_type" {
				n := node.NewNode(baseImage)
				n.SetType(page.PageType(15))
				n.UpdateChecksum()
			}
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
			if kind == "bad_base" || kind == "bad_base_type" || kind == "bad_component" {
				if kind == "bad_base_type" && e != node.ErrPrimaryDirectory {
					t.Fatalf("unsupported PRIMARY base type: %v", e)
				}
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
	reads := 0
	read := func(_ page.LeafLogPtr, dst []byte) ([]byte, error) {
		reads++
		copy(dst[:page.PageSize], data)
		return dst[:page.PageSize], nil
	}
	if got, ok, e := tr.SeekGEWithFixedScratch([]byte("j"), nil, key[:], leaf[:], &scratch, read); e != nil || !ok || string(got.Value) != "v" {
		t.Fatalf("bounded leaf: %+v %v %v", got, ok, e)
	}
	if reads != 1 {
		t.Fatalf("external child read %d times, want one", reads)
	}
	if _, ok, e := tr.SeekGEWithFixedScratch([]byte("j"), []byte("k"), key[:], leaf[:], &scratch, read); e != nil || ok {
		t.Fatalf("exclusive external bound: found=%v err=%v", ok, e)
	}
	data[len(data)-1] ^= 1
	if _, _, e := tr.SeekGEWithFixedScratch([]byte("j"), nil, key[:], leaf[:], &scratch, read); e == nil {
		t.Fatal("corrupt external child accepted after a successful seek")
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatal("global reader used")
	}
}

// The initial view is valid only inside one seek. A later seek must verify
// its root again, and reusing the caller's scratch must not cross tree owners.
func TestOwnedSuccessorCapturedRootsAndCorruption(t *testing.T) {
	for _, mode := range []string{"DATA_leaf", "DATA_internal", "PRIMARY"} {
		t.Run(mode, func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err = p.Alloc(2); err != nil {
				t.Fatal(err)
			}
			alloc := func() (uint64, []byte) {
				id, e := p.Alloc(1)
				if e != nil {
					t.Fatal(e)
				}
				image, e := p.GetForWrite(id)
				if e != nil {
					t.Fatal(e)
				}
				return id, image
			}
			makeLeaf := func(k, v string) (uint64, []byte) {
				id, image := alloc()
				b := node.NewBuilder(image, page.PageTypeLeaf)
				b.SetPageID(id)
				if k != "" {
					if e := b.AddLeafEntryWithRevision([]byte(k), []byte(v), node.FlagInline, page.ValuePtr{}, 7); e != nil {
						t.Fatal(e)
					}
				}
				b.FinishNoNode()
				return id, image
			}
			emptyID, _ := makeLeaf("", "")
			oldLeafID, oldLeaf := makeLeaf("m", "old")
			newLeafID, _ := makeLeaf("m", "new")
			makeRoot := func(leafID uint64) (uint64, []byte, []byte) {
				if mode == "DATA_leaf" {
					image, e := p.Get(leafID)
					if e != nil {
						t.Fatal(e)
					}
					return leafID, image, image
				}
				baseID, baseImage := alloc()
				b := node.NewBuilder(baseImage, page.PageTypeInternal)
				b.SetPageID(baseID)
				if e := b.AddInternalChild([]byte("a"), emptyID); e != nil {
					t.Fatal(e)
				}
				if e := b.AddInternalChild([]byte("m"), leafID); e != nil {
					t.Fatal(e)
				}
				b.FinishNoNode()
				if mode != "PRIMARY" {
					return baseID, baseImage, baseImage
				}
				id, image := alloc()
				base := node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(baseImage)}
				if e := node.EncodePrimaryDirectory(image, id, 1, base, nil); e != nil {
					t.Fatal(e)
				}
				return id, image, baseImage
			}
			oldRootID, oldRoot, oldBase := makeRoot(oldLeafID)
			newRootID, _, _ := makeRoot(newLeafID)
			oldTree, newTree := New(p, nil, oldRootID), New(p, nil, newRootID)
			var key, leaf [page.PageSize]byte
			var scratch OwnedSuccessorScratch
			seek := func(tr *Tree, start, want string) {
				t.Helper()
				got, ok, e := tr.SeekGEWithFixedScratch([]byte(start), nil, key[:], leaf[:], &scratch, nil)
				if e != nil || !ok || string(got.Key) != "m" || string(got.Value) != want || got.Revision != 7 {
					t.Fatalf("seek %q: %+v %v %v", start, got, ok, e)
				}
			}
			// Both direct descent and an empty first child followed by ancestor
			// reload must preserve the captured immutable root's result.
			for _, start := range []string{"a", "m"} {
				seek(oldTree, start, "old")
				seek(newTree, start, "new")
				seek(oldTree, start, "old")
			}
			for _, image := range [][]byte{oldRoot, oldBase, oldLeaf} {
				// Corrupt bytes outside the structural fields without updating CRC.
				image[len(image)-1] ^= 1
				if _, _, e := oldTree.SeekGEWithFixedScratch([]byte("m"), nil, key[:], leaf[:], &scratch, nil); e == nil {
					t.Fatal("corruption accepted after previous successful seek")
				}
				seek(newTree, "m", "new")
				image[len(image)-1] ^= 1
				seek(oldTree, "m", "old")
			}
		})
	}
}

func TestOwnedSuccessorUnsupportedRootType(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(3); err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(2)
	if err != nil {
		t.Fatal(err)
	}
	n := node.NewNode(image)
	n.SetPageID(2)
	n.SetType(page.PageTypeFreelist)
	n.UpdateChecksum()
	tr := New(p, nil, 2)
	var key, leaf [page.PageSize]byte
	var scratch OwnedSuccessorScratch
	if _, _, err = tr.SeekGEWithFixedScratch(nil, nil, key[:], leaf[:], &scratch, nil); err != page.ErrInvalidPageType {
		t.Fatalf("unsupported root: got %v want %v", err, page.ErrInvalidPageType)
	}
}
