package tree

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOwnedProjectionWorkspaceReuseAndAdmission(t *testing.T) {
	for _, options := range []node.BuilderOptions{{EntryRevisions: true}, {LeafPrefixCompression: true, EntryRevisions: true}, {LeafColumnar: true, EntryRevisions: true}, {LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true}} {
		tr, keys, _, read := ownedSeekTree(t, options)
		var workspace OwnedPointerProjectionWorkspace
		calls := 0
		admit := func(n uint64) error {
			calls++
			if n != OwnedPointerProjectionAllocationSize() {
				t.Fatalf("raw storage charge %d", n)
			}
			return nil
		}
		var previous iterator.UnsafeIterator
		for _, i := range []int{7, 31, 3, 69} {
			it, err := tr.OwnedPointerProjectionIteratorWithWorkspace(keys[1], keys[70], read, &workspace, admit)
			if err != nil {
				t.Fatal(err)
			}
			if previous != nil && it != previous {
				t.Fatal("ordinary backing was replaced")
			}
			it.Seek(keys[i])
			if !it.Valid() || !bytes.Equal(it.UnsafeKey(), keys[i]) {
				t.Fatalf("seek %d: %v", i, it.Error())
			}
			_, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
			if revision != page.EntryRevision(i+1) || (i%3 == 1 && (flags&node.FlagPointer == 0 || ptr.FileID != 7)) || (i%3 == 2 && flags&node.FlagTombstone == 0) {
				t.Fatal("reused entry metadata changed")
			}
			clone := it.KeyCopy(nil)
			if other, err := tr.OwnedPointerProjectionIteratorWithWorkspace(nil, nil, read, &workspace, admit); !errors.Is(err, ErrOwnedProjectionWorkspaceBusy) || other != nil {
				t.Fatal("overlapping workspace loan accepted")
			}
			sibling := tr.OwnedPointerProjectionIterator(nil, nil, read)
			sibling.Seek(keys[48])
			if !bytes.Equal(it.UnsafeKey(), keys[i]) {
				t.Fatal("independent constructor changed borrowed key")
			}
			_ = sibling.Close()
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			workspace.Reset()
			if workspace.InUse() || workspace.ordinary.iterator.tree != nil || workspace.ordinary.iterator.boundedLeafReader != nil {
				t.Fatal("reset retains iterator owner")
			}
			for _, frame := range workspace.ordinary.stack {
				if frame.Node.Data() != nil {
					t.Fatal("reset retains stack page")
				}
			}
			if !bytes.Equal(clone, keys[i]) {
				t.Fatal("owned key changed after reset")
			}
			previous = it
		}
		if calls != 1 || workspace.primary != nil {
			t.Fatalf("ordinary layouts calls=%d full=%v", calls, workspace.primary != nil)
		}
		var blocked OwnedPointerProjectionWorkspace
		sentinel := errors.New("refused")
		if it, err := tr.OwnedPointerProjectionIteratorWithWorkspace(nil, nil, read, &blocked, func(uint64) error { return sentinel }); !errors.Is(err, sentinel) || it != nil || blocked.InUse() || blocked.ordinary != nil {
			t.Fatal("refused construction allocated storage")
		}
	}
}

func TestOwnedProjectionWorkspacePrimaryAuthentication(t *testing.T) {
	var workspace OwnedPointerProjectionWorkspace
	calls := 0
	admit := func(uint64) error { calls++; return nil }
	for _, corrupt := range []string{"", "base", "component", "directory", ""} {
		tr, keys, read := ownedProjectionLayoutPrimary(t, corrupt)
		it, err := tr.OwnedPointerProjectionIteratorWithWorkspace(nil, nil, read, &workspace, admit)
		if err != nil {
			t.Fatal(err)
		}
		if it.Error() == nil {
			it.Seek(keys[7])
		}
		if corrupt != "" {
			if it.Valid() || it.Error() == nil {
				t.Fatal("cached workspace bypassed original PRIMARY authentication")
			}
		} else {
			value, _, flags, rev := iterator.UnsafeEntryWithRevision(it)
			if !it.Valid() || string(value) != "delta" || flags != node.FlagInline || rev != 99 {
				t.Fatalf("PRIMARY override: %v", it.Error())
			}
			it.Seek(keys[17])
			_, _, flags, rev = iterator.UnsafeEntryWithRevision(it)
			if !it.Valid() || flags&node.FlagTombstone == 0 || rev != 100 {
				t.Fatal("PRIMARY absence lost")
			}
		}
		_ = it.Close()
		workspace.Reset()
		p := &workspace.primary.primary
		if p.base != nil || p.readLeaf != nil || p.leafScratch != nil || p.start != nil || p.end != nil || p.delta.tree != nil || workspace.primary.iterator.tree != nil {
			t.Fatal("PRIMARY reset retained owner/delta/callback")
		}
		for _, frame := range workspace.primary.stack {
			if frame.Node.Data() != nil {
				t.Fatal("PRIMARY reset retained page")
			}
		}
	}
	if calls < 1 || calls > 2 || workspace.primary == nil {
		t.Fatalf("actual retained layouts=%d", calls)
	}
}
