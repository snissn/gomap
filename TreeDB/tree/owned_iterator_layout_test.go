package tree

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Representation-only storage selection has no behavioral prechange red.
// These real constructors retain the single-allocation contract; the unchanged
// public workload, rather than a synthetic byte threshold, measures savings.
func TestOwnedProjectionOrdinaryLayoutSingleAllocation(t *testing.T) {
	tr, _, _, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true})
	construct := func() {
		it := tr.OwnedPointerProjectionIterator(nil, nil, read)
		if !it.Valid() || it.Error() != nil {
			panic("ordinary constructor failed")
		}
		if err := it.Close(); err != nil {
			panic(err)
		}
	}
	if got := testing.AllocsPerRun(16, construct); got != 1 {
		t.Fatalf("selected layout has %g allocations, want one", got)
	}
}

func TestOwnedProjectionLayoutsIndependentAndReverse(t *testing.T) {
	for _, options := range []node.BuilderOptions{{EntryRevisions: true}, {LeafPrefixCompression: true, EntryRevisions: true}, {LeafColumnar: true, EntryRevisions: true}, {LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true}} {
		tr, keys, _, read := ownedSeekTree(t, options)
		forward := tr.OwnedPointerProjectionIterator(keys[1], keys[70], read)
		reverse := tr.OwnedReversePointerProjectionIterator(keys[1], keys[70], read)
		for _, i := range []int{7, 31, 69, 3, 48} {
			forward.Seek(keys[i])
			if !forward.Valid() || !bytes.Equal(forward.UnsafeKey(), keys[i]) {
				t.Fatalf("forward %d: %v", i, forward.Error())
			}
			ownedKey := forward.KeyCopy(nil)
			borrowed := forward.UnsafeKey()
			reverse.Seek(keys[70-i])
			if !reverse.Valid() || !bytes.Equal(reverse.UnsafeKey(), keys[70-i]) || !bytes.Equal(borrowed, keys[i]) {
				t.Fatal("live iterators share key storage or reverse seek changed")
			}
			_, ptr, flags, revision := iterator.UnsafeEntryWithRevision(forward)
			if revision != page.EntryRevision(i+1) || (i%3 == 1 && (flags&node.FlagPointer == 0 || ptr.FileID != 7)) || (i%3 == 2 && flags&node.FlagTombstone == 0) {
				t.Fatalf("entry metadata %d: flags=%d ptr=%+v revision=%d", i, flags, ptr, revision)
			}
			forward.Next()
			if !bytes.Equal(ownedKey, keys[i]) {
				t.Fatal("KeyCopy aliased private reconstruction buffer")
			}
		}
		if err := forward.Close(); err != nil {
			t.Fatal(err)
		}
		reverse.Seek(keys[23])
		if !reverse.Valid() || !bytes.Equal(reverse.UnsafeKey(), keys[23]) {
			t.Fatal("closing sibling detached live iterator")
		}
		if err := reverse.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Real PRIMARY with external base leaves and a page-backed overriding component.
// The canonical directory codec permits only page-backed components (or inline
// absence), so the full layout's separate delta page is checked for disjoint
// backing without inventing an unsupported direct LeafLogChildRef component.
func ownedProjectionLayoutPrimary(t *testing.T, corrupt string) (*Tree, [][]byte, func(page.LeafLogPtr, []byte) ([]byte, error)) {
	t.Helper()
	tr, keys, _, baseRead := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true})
	p := tr.pager
	baseImage, err := p.Get(tr.rootPageID)
	if err != nil {
		t.Fatal(err)
	}
	base := node.PrimaryOperand{Ref: page.PageChildRef(tr.rootPageID), Digest: sha256.Sum256(baseImage)}
	componentID, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	component, err := p.GetForWrite(componentID)
	if err != nil {
		t.Fatal(err)
	}
	builder := node.NewBuilder(component, page.PageTypeLeaf)
	builder.SetPageID(componentID)
	if err = builder.AddLeafEntryWithRevision(keys[7], []byte("delta"), node.FlagInline, page.ValuePtr{}, 99); err != nil {
		t.Fatal(err)
	}
	builder.FinishNoNode()
	builder.ReleaseScratch()
	entry := node.PrimaryDirectoryEntry{Key: keys[7], Revision: 99, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(componentID), Digest: sha256.Sum256(component)}}
	if corrupt == "base" {
		base.Digest[0] ^= 1
	}
	if corrupt == "component" {
		entry.Operand.Digest[0] ^= 1
	}
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = node.EncodePrimaryDirectory(image, root, 1, base, []node.PrimaryDirectoryEntry{entry, {Key: keys[17], Revision: 100, Kind: node.PrimaryAbsence, ClassSlot: 1}}); err != nil {
		t.Fatal(err)
	}
	if corrupt == "directory" {
		image[len(image)-1] ^= 1
	}
	tr.SetRoot(root)
	return tr, keys, baseRead
}

func TestOwnedProjectionPrimaryLayoutValidationAndLifetime(t *testing.T) {
	for _, corrupt := range []string{"", "directory", "base", "component"} {
		t.Run("primary/"+corrupt, func(t *testing.T) {
			tr, keys, read := ownedProjectionLayoutPrimary(t, corrupt)
			it := tr.OwnedPointerProjectionIterator(nil, nil, read)
			defer it.Close()
			if it.Error() == nil {
				it.Seek(keys[7])
			}
			if corrupt != "" {
				if it.Valid() || it.Error() == nil {
					t.Fatal("PRIMARY directory/base/component corruption accepted")
				}
				return
			}
			if got := testing.AllocsPerRun(8, func() {
				candidate := tr.OwnedPointerProjectionIterator(nil, nil, read)
				if !candidate.Valid() || candidate.Error() != nil {
					panic("PRIMARY constructor failed")
				}
				if err := candidate.Close(); err != nil {
					panic(err)
				}
			}); got != 1 {
				t.Fatalf("PRIMARY layout has %g allocations, want one", got)
			}
			p, ok := it.(*primaryIterator)
			if !ok || !p.base.owned || cap(p.base.ownedStack) != maxTraversalDepth || cap(p.leafScratch) != page.PageSize || cap(p.base.leafRefScratch) != page.PageSize {
				t.Fatal("full PRIMARY fixed layout or external base ownership lost")
			}
			if &p.base.leafRefScratch[:page.PageSize][0] == &p.leafScratch[0] {
				t.Fatal("PRIMARY delta page aliases live external base page")
			}
			value, _, flags, revision := iterator.UnsafeEntryWithRevision(it)
			if !it.Valid() || string(value) != "delta" || flags != node.FlagInline || revision != 99 {
				t.Fatalf("override: value=%q flags=%d revision=%d err=%v", value, flags, revision, it.Error())
			}
			clone := it.KeyCopy(nil)
			it.Seek(keys[17])
			_, _, flags, revision = iterator.UnsafeEntryWithRevision(it)
			if !it.Valid() || flags&node.FlagTombstone == 0 || revision != 100 {
				t.Fatal("PRIMARY absence lost")
			}
			reverse := tr.OwnedReversePointerProjectionIterator(nil, nil, read)
			defer reverse.Close()
			reverse.Seek(keys[7])
			if !reverse.Valid() || string(reverse.Value()) != "delta" {
				t.Fatalf("PRIMARY reverse override: %v", reverse.Error())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(clone, keys[7]) || !reverse.Valid() {
				t.Fatal("Close released independent key or sibling")
			}
		})
	}
}
