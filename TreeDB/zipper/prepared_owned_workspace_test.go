package zipper

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func ownedWorkspaceTestLeaf(t *testing.T, data []byte, value string) {
	t.Helper()
	b := node.NewBuilder(data, page.PageTypeLeaf)
	if err := b.AddLeafEntry([]byte("key"), []byte(value), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
}

func TestPreparedOwnedWorkspaceFrozenImagesAndBackingAdmission(t *testing.T) {
	var charged uint64
	reserve := func(n uint64) error { charged += n; return nil }
	w, err := NewPreparedOwnedWorkspace(1, 1, reserve)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	source := make([]byte, page.PageSize)
	ownedWorkspaceTestLeaf(t, source, "original")
	ptr := page.ValuePtr{FileID: 2, Offset: 64, Length: 100}
	loads := 0
	load := func(dst []byte) error { loads++; copy(dst, source); return nil }
	if err := w.CaptureOld(ptr, load); err != nil {
		t.Fatal(err)
	}
	frozen := bytes.Clone(source)
	clear(source)
	if err := w.CaptureOld(ptr, load); err != nil || loads != 1 {
		t.Fatalf("duplicate reload %d %v", loads, err)
	}
	if err := w.CaptureOld(page.ValuePtr{FileID: 2, Offset: 128}, load); !errors.Is(err, ErrPreparedOwnedWorkspace) || loads != 1 {
		t.Fatalf("excess read %d %v", loads, err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	got, err := w.ReadUnsafe(ptr)
	if err != nil || !bytes.Equal(got, frozen) {
		t.Fatalf("frozen image changed %v", err)
	}
	if _, err := w.ReadUnsafe(page.ValuePtr{FileID: 2, Offset: 64, Length: 101}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("partial pointer identity accepted %v", err)
	}
	output, err := w.NewOutputPage()
	if err != nil {
		t.Fatal(err)
	}
	ownedWorkspaceTestLeaf(t, output, "new")
	outputPtr := page.ValuePtr{FileID: 3, Offset: 64, Length: 101}
	if err := w.RememberOutput(outputPtr, output); err != nil {
		t.Fatal(err)
	}
	if err := w.RememberOutput(page.ValuePtr{FileID: 3, Offset: 128}, output); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("reused page admitted %v", err)
	}
	if _, err := w.NewOutputPage(); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("excess output %v", err)
	}
	if got, err := w.ReadUnsafe(outputPtr); err != nil || &got[0] != &output[0] {
		t.Fatalf("output was copied/fell back %v", err)
	}
	if w.BackingBytes() != charged {
		t.Fatalf("backing %d charged %d", w.BackingBytes(), charged)
	}
	w.Close()
	if _, err := w.ReadUnsafe(ptr); !errors.Is(err, ErrPreparedOwnedWorkspace) || w.old != nil || w.output != nil || w.pages != nil {
		t.Fatalf("closed owner retains authority %v", err)
	}
}

func TestPreparedOwnedWorkspaceDenialBeforeLoadAndAppend(t *testing.T) {
	sentinel := errors.New("credit denied")
	deny := false
	if _, err := NewPreparedOwnedWorkspace(1, 1, func(uint64) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	w, err := NewPreparedOwnedWorkspace(1, 1, func(uint64) error {
		if deny {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	deny = true
	loaded := false
	if err := w.CaptureOld(page.ValuePtr{FileID: 1, Offset: 64}, func([]byte) error { loaded = true; return nil }); !errors.Is(err, sentinel) || loaded {
		t.Fatalf("load before charge %v %v", loaded, err)
	}

	w2, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	z := New(nil, &outputLimitTestAllocator{})
	log := &outputLimitTestLeafLog{}
	z.SetLeafPageLog(log)
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("key")}}
	if err := w2.BindRoot(z, 1, ops); err != nil {
		t.Fatal(err)
	}
	if err := w2.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w2); err != nil {
		t.Fatal(err)
	}
	borrowed := make([]byte, page.PageSize)
	ownedWorkspaceTestLeaf(t, borrowed, "borrowed")
	var metrics adaptive.Metrics
	if _, err := z.persistLeafPageData(borrowed, &metrics); !errors.Is(err, ErrPreparedOwnedWorkspace) || log.calls != 0 {
		t.Fatalf("unowned append %d %v", log.calls, err)
	}
	output, err := w2.NewOutputPage()
	if err != nil {
		t.Fatal(err)
	}
	ownedWorkspaceTestLeaf(t, output, "owned")
	ref, err := z.persistLeafPageData(output, &metrics)
	if err != nil || log.calls != 1 {
		t.Fatalf("owned append %d %v", log.calls, err)
	}
	if got, err := w2.ReadUnsafe(ref.Log.ValuePtr()); err != nil || &got[0] != &output[0] {
		t.Fatalf("output binding %v", err)
	}
}

func TestPreparedOwnedWorkspaceExactRootOptionsAndPointShape(t *testing.T) {
	z := New(nil, nil)
	z.maintenanceOpsPerCoalesce = 400000
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("a")}, {Type: batch.OpPut, Key: []byte("b")}}
	w, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(z, 7, ops); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	ops[0].Key[0] = 'z'
	canonical := []batch.Entry{{Type: batch.OpDelete, Key: []byte("a")}, {Type: batch.OpPut, Key: []byte("b")}}
	if err := w.validateRoot(z, 7, canonical, nil); err != nil {
		t.Fatalf("owned keys changed %v", err)
	}
	if err := w.validateRoot(z, 8, canonical, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("rebound root accepted")
	}
	if err := w.validateRoot(z, 7, ops, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("rebound key accepted")
	}
	canonical[0].Type = batch.OpPut
	if err := w.validateRoot(z, 7, canonical, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("rebound type accepted")
	}
	canonical[0].Type = batch.OpDelete
	z.maintenanceOpsPerCoalesce--
	if err := w.validateRoot(z, 7, canonical, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("rebound maintenance accepted")
	}
	z.maintenanceOpsPerCoalesce = 1
	w2, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if err := w2.BindRoot(z, 7, canonical); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("computed B>1 accepted")
	}
}

func TestPreparedOwnedWorkspaceConsumerAuthority(t *testing.T) {
	z := New(nil, nil)
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("long-enough-key")}}
	w, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(z, 7, ops); err != nil {
		t.Fatal(err)
	}
	if cap(w.ops[0].key) != len(ops[0].Key) {
		t.Fatal("unaccounted key backing")
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := z.Apply(7, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("empty apply bypassed authority")
	}
	clone := z.CloneWithAllocator(nil)
	if clone.preparedOwned != w {
		t.Fatal("clone dropped owner")
	}
	if err := w.validateRoot(clone, 7, ops, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := z.ApplyWithOptions(7, nil, ApplyOptions{SpanNativeApply: true}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("span-native bypass")
	}
	if _, err := z.ApplyWithOptions(7, nil, ApplyOptions{ParallelApplyConcurrency: 2}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("parallel bypass")
	}
	z.SetLeafPageReader(nil)
	if err := w.validateRoot(z, 7, ops, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("reader rebound")
	}
}
func TestPreparedOwnedWorkspaceFailedLoadRetained(t *testing.T) {
	w, err := NewPreparedOwnedWorkspace(1, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("bad record")
	if err := w.CaptureOld(page.ValuePtr{FileID: 1, Offset: 64}, func([]byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if len(w.old) != 0 || len(w.oldPages) != 1 {
		t.Fatal("failed load ownership")
	}
	w.Close()
	if w.oldPages != nil {
		t.Fatal("failed load retained after close")
	}
}

func TestPreparedOwnedWorkspaceLocalWorkGlobalCapsAndCleanup(t *testing.T) {
	charged := uint64(0)
	w, err := NewPreparedOwnedWorkspace(2, 1, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Seal(); err != nil {
		t.Fatal(err)
	}
	a, err := w.newWorkBuffers(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.newWorkBuffers(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	a.children = a.children[:1]
	a.children[0].key = []byte("held")
	a.entries = a.entries[:2]
	a.entries[1].key = []byte("pruned tail")
	if _, err = w.newWorkBuffers(1, 0); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("per-parent child budget reset")
	}
	if _, err = w.newWorkBuffers(0, 1); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("per-parent entry budget reset")
	}
	if cap(a.children) != 1 || cap(a.entries) != 2 || cap(b.entries) != 1 {
		t.Fatal("unaccounted capacity")
	}
	if w.BackingBytes() != charged {
		t.Fatal("unaccounted work backing")
	}
	w.Close()
	if a.children != nil || a.entries != nil || b.entries != nil || w.work != nil {
		t.Fatal("work aliases survive close")
	}
}

func TestPreparedOwnedWorkspaceOneApplyAuthority(t *testing.T) {
	w, err := NewPreparedOwnedWorkspace(1, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	z := &Zipper{maintenanceOpsPerCoalesce: 400000}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("k")}}
	if err := w.BindRoot(z, 7, ops); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := w.validateRoot(z, 7, ops, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("reused Apply authority: %v", err)
	}
}

func TestPreparedOwnedWorkspaceGlobalBuilderBirthCredit(t *testing.T) {
	var charged uint64
	w, err := NewPreparedOwnedWorkspace(0, 2, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	z := &Zipper{indexInternalBaseDelta: true, adaptiveLeafEncoding: true}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("key")}}
	if err := w.BindRoot(z, 0, ops); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(10); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := z.SetPreparedOwnedWorkspace(w); err != nil {
		t.Fatal(err)
	}
	if _, err := z.newApplyBuilder(make([]byte, page.PageSize), page.PageTypeInternal, nil, true, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("builder before Apply: %v", err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	var first *node.Builder
	for i := 0; i < 2; i++ {
		before := charged
		b, err := z.newApplyBuilder(make([]byte, page.PageSize), page.PageTypeInternal, nil, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !b.OwnedScratch() || charged <= before {
			t.Fatal("builder bypassed request credit")
		}
		if err := b.AddInternalChild([]byte("key"), 1); err != nil {
			t.Fatal(err)
		}
		releasePooledBuilder(b)
		if i == 0 {
			first = b
		}
	}
	before := charged
	if _, err := z.newApplyBuilder(make([]byte, page.PageSize), page.PageTypeInternal, nil, true, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) || charged != before {
		t.Fatalf("per-parent builder quota reset: %v", err)
	}
	if w.BackingBytes() != charged {
		t.Fatal("actual backing ledger diverged")
	}
	w.Close()
	first.ResetWithOptions(make([]byte, page.PageSize), page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	if err := first.AddInternalChild([]byte("key"), 2); !errors.Is(err, node.ErrOwnedBuilderScratch) {
		t.Fatalf("closed builder regained pool: %v", err)
	}
}

func TestPreparedOwnedWorkspaceAdaptiveScratchNoUnsortedFallback(t *testing.T) {
	w, err := NewPreparedOwnedWorkspace(0, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	z := &Zipper{adaptiveLeafEncoding: true, maintenanceOpsPerCoalesce: 400000}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}, {Type: batch.OpDelete, Key: []byte("b")}}
	if err := w.BindRoot(z, 0, ops); err != nil {
		t.Fatal(err)
	}
	if _, err := w.leafOptions(z, ops, false); err != nil {
		t.Fatal(err)
	}
	for _, e := range w.heuristics {
		if e.Key != nil {
			t.Fatal("heuristic source alias retained")
		}
	}
	if _, err := w.leafOptions(z, []batch.Entry{ops[1], ops[0]}, false); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("generic sorted-copy fallback: %v", err)
	}
}

func TestPreparedOwnedWorkspaceGlobalOldSeparatorCredit(t *testing.T) {
	var charged uint64
	w, err := NewPreparedOwnedWorkspace(3, 1, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	z := &Zipper{}
	if err := w.BindRoot(z, 0, []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(7); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	a, err := w.newInternalKeyArena(2)
	if err != nil || cap(a) != 14 {
		t.Fatalf("exact arena %d %v", cap(a), err)
	}
	a = a[:cap(a)]
	copy(a, []byte("source aliases"))
	if _, err := w.newInternalKeyArena(1); err != nil {
		t.Fatal(err)
	}
	before := charged
	if _, err := w.newInternalKeyArena(1); !errors.Is(err, ErrPreparedOwnedWorkspace) || charged != before {
		t.Fatalf("old-child quota reset: %v", err)
	}
	if w.BackingBytes() != charged {
		t.Fatal("actual key backing uncharged")
	}
	w.Close()
	for _, v := range a {
		if v != 0 {
			t.Fatal("closed key bytes retained")
		}
	}
}

func TestPreparedOwnedWorkspaceFixedDecodedKeyScratch(t *testing.T) {
	w, err := NewPreparedOwnedWorkspace(1, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.BindRoot(&Zipper{}, 0, []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(3); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	scratch, err := w.newNodeKeyScratch()
	if err != nil || len(scratch) != 3 || cap(scratch) != 3 {
		t.Fatalf("fixed scratch %d/%d %v", len(scratch), cap(scratch), err)
	}
	if _, err := w.newNodeKeyScratch(); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("per-node quota reset: %v", err)
	}
	data := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(data, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	if err := b.AddInternalChild([]byte("long-key"), 1); err != nil {
		t.Fatal(err)
	}
	n := b.Finish()
	defer b.ReleaseScratch()
	n.SetFixedKeyScratch(scratch)
	if _, _, err := n.GetInternalEntryRefView(0); !errors.Is(err, node.ErrCorruptedNode) {
		t.Fatalf("decoded key silently grew scratch: %v", err)
	}
	if cap(n.TakeKeyScratch()) != 3 {
		t.Fatal("fixed decoded scratch grew")
	}
}

func TestPreparedOwnedWorkspaceInterleavedSplitLifetimes(t *testing.T) {
	var charged uint64
	w, err := NewPreparedOwnedWorkspace(0, 3, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BindRoot(&Zipper{}, 0, []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(7); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	parent, child := w.splitList(), w.splitList()
	key := []byte("parent")
	if err := parent.append(Split{Key: key, Ref: page.PageChildRef(1)}); err != nil {
		t.Fatal(err)
	}
	if err := child.append(Split{Key: []byte("child"), Ref: page.PageChildRef(2)}); err != nil {
		t.Fatal(err)
	}
	if err := parent.append(Split{Key: []byte("tail"), Ref: page.PageChildRef(3)}); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	if err := child.setLastRef(page.PageChildRef(4)); err != nil {
		t.Fatal(err)
	}
	cs, err := child.finish()
	if err != nil {
		t.Fatal(err)
	}
	ps, err := parent.finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 2 || string(ps[0].Key) != "parent" || string(ps[1].Key) != "tail" || len(cs) != 1 || cs[0].Ref.Page != 4 {
		t.Fatal("recursive split segment aliased or lost ordering")
	}
	before := charged
	extra := w.splitList()
	if err := extra.append(Split{Key: []byte("extra")}); !errors.Is(err, ErrPreparedOwnedWorkspace) || charged != before {
		t.Fatalf("global birth quota reset: %v", err)
	}
	if _, err := parent.finish(); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("same births materialized twice: %v", err)
	}
	if w.BackingBytes() != charged {
		t.Fatal("split backing uncharged")
	}
	w.Close()
	if ps[0].Key != nil || cs[0].Key != nil {
		t.Fatal("closed arrays retained split aliases")
	}
}

func TestPreparedOwnedWorkspaceRetirementAndRootInputOwnership(t *testing.T) {
	w, err := NewPreparedOwnedWorkspace(1, 2, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BindRoot(&Zipper{}, 0, []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitBuilderScratch(3); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitRetirementScratch(); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := w.beginApply(); err != nil {
		t.Fatal(err)
	}
	z := &Zipper{preparedOwned: w}
	retired := w.retired
	if err := z.appendRetired(&retired, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	if err := z.appendRetired(&retired, 4); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("retirement backing grew: %v", err)
	}
	foreign := make([]uint64, 0, cap(retired))
	if err := z.appendRetired(&foreign, 1); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("foreign retirement owner: %v", err)
	}
	source := []Split{{Key: []byte("key"), Ref: page.PageChildRef(2)}}
	initial, err := w.rootInput(page.PageChildRef(1), source)
	if err != nil || len(initial) != 2 || cap(initial) != 2 {
		t.Fatalf("root input %d/%d %v", len(initial), cap(initial), err)
	}
	source[0] = Split{}
	if initial[1].Ref.Page != 2 {
		t.Fatal("root input borrowed array")
	}
	if _, err := w.rootInput(page.PageChildRef(1), nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("root quota reset: %v", err)
	}
	w.Close()
	if retired[0] != 0 || initial[1].Key != nil {
		t.Fatal("close retained root/retirement aliases")
	}
}

func TestPreparedOwnedWorkspaceDistinctBirthClassesAndPartialRefusal(t *testing.T) {
	var charges []uint64
	w, err := NewPreparedOwnedWorkspace(3, 7, func(n uint64) error { charges = append(charges, n); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	raws := []uint64{uint64(unsafe.Sizeof(*w)), uint64(cap(w.old)) * uint64(unsafe.Sizeof(preparedOwnedLeafImage{})),
		uint64(cap(w.oldPages)) * uint64(unsafe.Sizeof((*[page.PageSize]byte)(nil))),
		uint64(cap(w.output)) * uint64(unsafe.Sizeof(preparedOwnedLeafImage{})),
		uint64(cap(w.pages)) * uint64(unsafe.Sizeof((*[page.PageSize]byte)(nil))),
		uint64(cap(w.pruneHeaders)) * uint64(unsafe.Sizeof(preparedOwnedPruneHeader{}))}
	if len(charges) != len(raws) {
		t.Fatalf("birth count=%d want=%d", len(charges), len(raws))
	}
	var total uint64
	for i, raw := range raws {
		want, err := allocclass.ClassBytes(raw, i != len(raws)-1)
		if err != nil || charges[i] != want {
			t.Fatalf("birth%d charge=%d want=%d err=%v", i, charges[i], want, err)
		}
		total += want
	}
	if w.BackingBytes() != total {
		t.Fatal("distinct backing classes not cumulative")
	}
	denied := errors.New("class credit denied")
	for fail := 1; fail <= len(charges); fail++ {
		calls := 0
		owner, err := NewPreparedOwnedWorkspace(3, 7, func(n uint64) error {
			calls++
			if calls == fail {
				return denied
			}
			return nil
		})
		if owner != nil || !errors.Is(err, denied) || calls != fail {
			t.Fatalf("constructor denial%d owner=%v calls=%d err=%v", fail, owner, calls, err)
		}
	}
	// A denial after earlier birth charges cannot expose partial root keys or
	// refund the preceding credit; no key copy is installed until all succeed.
	before := w.BackingBytes()
	accepted := uint64(0)
	calls := 0
	w.reserve = func(n uint64) error {
		calls++
		if calls == 4 {
			return denied
		}
		accepted += n
		return nil
	}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("a")}, {Type: batch.OpPut, Key: []byte("b")}}
	if err := w.BindRoot(&Zipper{}, 0, ops); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if w.bound || w.ops != nil || w.heuristics != nil || w.BackingBytes() != before+accepted || accepted == 0 {
		t.Fatal("partial root assets exposed or preceding debit refunded")
	}
}

type pruneHeaderTestReader struct{ calls int }

func (r *pruneHeaderTestReader) ReadUnsafe(page.ValuePtr) ([]byte, error) {
	r.calls++
	return nil, errors.New("header probe reached generic reader")
}

func TestPreparedOwnedWorkspacePruneHeaderAuthority(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	} // reserve metadata page zero
	z.maintenanceOpsPerCoalesce = 400000
	root := newParallelApplyEmptyLeafRoot(t, z)
	ref := page.PageChildRef(root)
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 64, Length: 128}
	log, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		t.Fatal(err)
	}
	logRef := page.LeafLogChildRef(log)
	var charges []uint64
	w, err := NewPreparedOwnedWorkspace(2, 4, func(n uint64) error { charges = append(charges, n); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	digest := [32]byte{1, 2, 3}
	if err := w.CapturePruneHeader(ref, page.PageTypeLeaf, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := w.CapturePruneHeader(ref, page.PageTypeLeaf, 0, digest); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatalf("duplicate accepted: %v", err)
	}
	if err := w.CapturePruneHeader(logRef, page.PageTypeLeaf, 37, digest); err != nil {
		t.Fatal(err)
	}
	before := w.BackingBytes()
	loaded := false
	if err := w.CaptureOld(ptr, func([]byte) error { loaded = true; return nil }); !errors.Is(err, ErrPreparedOwnedWorkspace) || loaded || w.BackingBytes() != before {
		t.Fatalf("header acquired full image: %v loaded=%v", err, loaded)
	}
	if _, _, _, err := w.ProbePruneHeader(ref); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("unsealed probe")
	}
	if err := w.CapturePruneHeader(page.PageChildRef(root+1), page.PageTypeLeaf, 0, digest); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("capacity expanded")
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ref       page.ChildRef
		count     uint16
		fromPager bool
	}{{ref, 0, true}, {logRef, 37, false}} {
		typ, count, fromPager, err := w.ProbePruneHeader(tc.ref)
		if err != nil || typ != page.PageTypeLeaf || count != tc.count || fromPager != tc.fromPager {
			t.Fatalf("facts %v/%d/%v/%v", typ, count, fromPager, err)
		}
	}
	changed := logRef
	changed.Log.RecordLengthHint++
	if _, _, _, err := w.ProbePruneHeader(changed); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("partial ref identity accepted")
	}
	reader := &pruneHeaderTestReader{}
	z.SetLeafPageReader(reader)
	z.preparedOwned = w
	for _, r := range []page.ChildRef{ref, logRef} {
		if _, _, _, _, _, err := z.loadNodeRef(r, nil); !errors.Is(err, ErrPreparedOwnedWorkspace) {
			t.Fatalf("header full-load admitted %v", err)
		}
	}
	if reader.calls != 0 {
		t.Fatal("header accessed generic reader")
	}
	// The pager page really exists; refusing its full load above is a distinct
	// authority check, rather than an invalid-page error.
	if _, err := p.Get(root); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, _, _, err := w.ProbePruneHeader(ref); !errors.Is(err, ErrPreparedOwnedWorkspace) || w.pruneHeaders != nil {
		t.Fatal("closed header authority")
	}
}

func TestPreparedOwnedWorkspacePruneHeaderShapeAndB1(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	} // reserve metadata page zero
	z.maintenanceOpsPerCoalesce = 400000
	root := newParallelApplyEmptyLeafRoot(t, z)
	w, err := NewPreparedOwnedWorkspace(3, 4, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.CapturePruneHeader(page.ChildRef{}, page.PageTypeLeaf, 0, [32]byte{}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("zero pager ref")
	}
	malformed := page.PageChildRef(root)
	malformed.Log.Offset = 9
	if err := w.CapturePruneHeader(malformed, page.PageTypeLeaf, 0, [32]byte{}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("mixed ref")
	}
	if err := w.CapturePruneHeader(page.PageChildRef(root), page.PageType(255), 0, [32]byte{}); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("invalid type")
	}
	if err := w.BindRoot(z, root, []batch.Entry{{Type: batch.OpDelete, Key: []byte("z")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AdmitRetirementScratch(); err != nil {
		t.Fatal(err)
	}
	empty := page.PageChildRef(root)
	internal := page.PageChildRef(root + 1000) // Deliberately absent: probe is scalar.
	if err := w.CapturePruneHeader(empty, page.PageTypeLeaf, 0, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	if err := w.CapturePruneHeader(internal, page.PageTypeInternal, 1, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	z.preparedOwned = w
	budget := &maintenanceBudget{remaining: 1}
	entries := []internalEntry{{child: internal}, {child: empty}, {child: internal}}
	var metrics adaptive.Metrics
	out, retired, err := z.coalesceLeafChildren(entries, budget, &metrics, nil)
	if err != nil || len(out) != 2 || len(retired) != 1 || retired[0] != root || budget.remaining != 0 {
		t.Fatalf("B1 out=%v retired=%v budget=%v err=%v", out, retired, budget, err)
	}
	if metrics.ZipperNodeLoads != 0 || metrics.ZipperPagerNodeBytesRead != 0 {
		t.Fatal("scalar probes reported physical loads")
	}
	if _, err := p.Get(internal.Page); err == nil {
		t.Fatal("fixture absent pager unexpectedly present")
	}
	if _, _, _, err := w.ProbePruneHeader(page.PageChildRef(root + 2000)); !errors.Is(err, ErrPreparedOwnedWorkspace) {
		t.Fatal("unknown header accepted")
	}
}
