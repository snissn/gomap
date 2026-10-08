package zipper

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// This fixture issues disjoint real logical IDs without changing pager storage.
// Production uses the guarded intrinsic COW allocator, not this fixture.
type stagedPagerTestAllocator struct{ next uint64 }

func (a *stagedPagerTestAllocator) Alloc(uint64) (uint64, error) {
	id := a.next
	a.next++
	return id, nil
}

func TestPreparedOwnedPagerApplyStagesBeforeStorage(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	root := newParallelApplyEmptyLeafRoot(t, z)
	old, err := p.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(old)
	count := p.PageCount()
	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer b.Close()
	if err := b.Set([]byte("key"), []byte("staged")); err != nil {
		t.Fatal(err)
	}
	var charged uint64
	w, err := NewPreparedOwnedWorkspace(1, 4, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(z, root, b.SortedEntries()); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(16); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitRetirementScratch(); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.EnablePagerStaging(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	a := &stagedPagerTestAllocator{next: count}
	z.allocator = a
	newRoot, _, _, err := z.Apply(root, b)
	if err != nil {
		t.Fatal(err)
	}
	if newRoot != count || p.PageCount() != count || !bytes.Equal(old, before) {
		t.Fatal("Apply changed installed pager bytes, count, or reserved identity")
	}
	if _, err := p.Get(newRoot); err == nil {
		t.Fatal("staged root was physically published")
	}
	if err := w.InstallStagedPagerImages(p); !errors.Is(err, ErrPreparedOwnedWorkspace) || w.pagerInstallStarted {
		t.Fatalf("unprepared growth admitted or consumed install: %v", err)
	}
	if _, _, _, err := z.Apply(root, b); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("second Apply admitted")
	}
	foreign, _ := newParallelApplyTestZipper(t)
	if err := foreign.Truncate(a.next); err != nil {
		t.Fatal(err)
	}
	foreignPage, err := foreign.Get(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	foreignBefore := bytes.Clone(foreignPage)
	if err := w.InstallStagedPagerImages(foreign); !errors.Is(err, ErrPreparedOwnedWorkspace) || w.pagerInstallStarted || !bytes.Equal(foreignPage, foreignBefore) {
		t.Fatal("foreign pager changed or consumed real install authority")
	}
	// The concrete WAL/packet consumer performs admitted growth first.
	if err := p.Truncate(a.next); err != nil {
		t.Fatal(err)
	}
	if err := w.InstallStagedPagerImages(p); err != nil {
		t.Fatal(err)
	}
	installed, err := p.Get(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	image, found := w.pagerImage(newRoot)
	installedNode := node.NewNodeView(installed)
	if !found || !bytes.Equal(installed, image[:]) || installedNode.Count() != 1 {
		t.Fatal("installer changed same-Apply image")
	}
	if err := w.InstallStagedPagerImages(p); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("second install admitted")
	}
	if w.BackingBytes() != charged {
		t.Fatal("owned staging birth uncharged")
	}
	w.Close()
	if w.pagerImages != nil || w.pages != nil {
		t.Fatal("closed owner retained images")
	}
}

func TestPreparedOwnedPagerStagingDenialAndSharedOutputQuota(t *testing.T) {
	deny := false
	sentinel := errors.New("deny staging birth")
	w, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error {
		if deny {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(&Zipper{}, 1, []batch.Entry{{Type: batch.OpPut, Key: []byte("k")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	deny = true
	if err := w.EnablePagerStaging(); !errors.Is(err, sentinel) || w.pagerImages != nil || w.pagerStaging {
		t.Fatal("denied table birth retained staging authority")
	}
	deny = false
	if err := w.EnablePagerStaging(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.NewOutputPage(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.newPagerImage(2); !errors.Is(err, ErrPreparedOwnedWorkspace) || len(w.pagerImages) != 0 {
		t.Fatal("internal output bypassed leaf output quota")
	}
}

func TestPreparedOwnedPagerStagesSplitAndInternalRetirement(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	left := newParallelApplyEmptyLeafRoot(t, z)
	right := newParallelApplyEmptyLeafRoot(t, z)
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	rootData, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	builder := node.NewBuilder(rootData, page.PageTypeInternal)
	builder.SetPageID(root)
	if err := builder.AddInternalChildRef(nil, page.PageChildRef(left)); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddInternalChildRef([]byte("m"), page.PageChildRef(right)); err != nil {
		t.Fatal(err)
	}
	builder.FinishNoNode()
	builder.ReleaseScratch()
	oldRoot := bytes.Clone(rootData)
	leftData, err := p.Get(left)
	if err != nil {
		t.Fatal(err)
	}
	oldLeft := bytes.Clone(leftData)
	count := p.PageCount()
	b := batch.New(panicValueReader{}, 2048)
	defer b.Close()
	value := bytes.Repeat([]byte("v"), 1700)
	for _, key := range []string{"a1", "a2", "a3"} {
		if err := b.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	w, err := NewPreparedOwnedWorkspace(8, 8, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(z, root, b.SortedEntries()); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(16); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitRetirementScratch(); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.EnablePagerStaging(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	a := &stagedPagerTestAllocator{next: count}
	z.allocator = a
	newRoot, retired, _, err := z.Apply(root, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.pagerImages) < 3 || p.PageCount() != count || !bytes.Equal(rootData, oldRoot) || !bytes.Equal(leftData, oldLeft) {
		t.Fatal("split/internal Apply changed old storage or omitted owned images")
	}
	rootRetired, leafRetired := false, false
	for _, id := range retired {
		rootRetired = rootRetired || id == root
		leafRetired = leafRetired || id == left
	}
	if !rootRetired || !leafRetired {
		t.Fatal("same Apply lost old internal/leaf retirement")
	}
	if err := p.Truncate(a.next); err != nil {
		t.Fatal(err)
	}
	if err := w.InstallStagedPagerImages(p); err != nil {
		t.Fatal(err)
	}
	data, err := p.Get(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	parent := node.NewNodeView(data)
	if parent.Type() != page.PageTypeInternal || parent.Count() < 3 {
		t.Fatal("staged split was not installed under same internal root")
	}
	seen := 0
	for i := uint16(0); i < parent.Count(); i++ {
		_, child, err := parent.GetInternalEntryRefView(i)
		if err != nil || child.Kind != page.ChildRefPage {
			t.Fatalf("child ref: %v", err)
		}
		data, err := p.Get(child.Page)
		if err != nil {
			t.Fatal(err)
		}
		leaf := node.NewNodeView(data)
		for j := uint16(0); j < leaf.Count(); j++ {
			key, got, _, _, err := leaf.GetLeafEntryView(j)
			if err != nil || len(key) != 2 || key[0] != 'a' || !bytes.Equal(got, value) {
				t.Fatalf("installed row: %q/%v", key, err)
			}
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("installed rows=%d", seen)
	}
}
