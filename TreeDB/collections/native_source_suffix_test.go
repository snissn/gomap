package collections

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func nativeTestAppendSource(t *testing.T, budget *memtable.COWBudget, older *nativePendingSources, key, value string, release func()) *nativePendingSources {
	t.Helper()
	writer, err := budget.NewNativeCOWWriter(nativePendingSourceCharge())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := writer.Prepare([]memtable.COWMutation{{Key: []byte(key), Value: []byte(value)}}, memtable.COWPrepareOptions{ResourceSlots: 1, ResourceBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.AttachResources([]memtable.COWResourceID{{Kind: 1, ID: 1}}, release); err != nil {
		t.Fatal(err)
	}
	root := prepared.Publish()
	prepared = nil
	retirement := writer.Close()
	writer = nil
	retirement.Drain()
	source, err := newNativePendingSource(root)
	if err != nil {
		t.Fatal(err)
	}
	retirement = root.Release()
	root = nil
	retirement.Drain()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	list, err := newNativePendingSources(source, older, admit)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func TestNativeSourceSuffixDropsOnlyCapturedPrefixAndSharesOriginalSlots(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var released [3]atomic.Int32
	oldest := nativeTestAppendSource(t, budget, nil, "old", "A", func() { released[0].Add(1) })
	boundary := oldest.source.root
	// This is an original native source-owner edge, shared by read versions.
	oldest.retain()
	oldView := oldest
	middle := nativeTestAppendSource(t, budget, oldest, "middle", "B", func() { released[1].Add(1) })
	oldest = nil
	latest := nativeTestAppendSource(t, budget, middle, "new", "C", func() { released[2].Add(1) })
	middle = nil
	latest.retain()
	newerView := latest
	before := budget.Stats()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	suffix, scratch, err := prepareNativeSourceSuffix(latest, boundary, admit)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Stats().Sources != before.Sources || suffix.source != latest.source || suffix.older.source != latest.older.source || suffix.older.older != nil {
		t.Fatal("suffix copied source ownership or retained prefix")
	}
	scratch.Complete()
	done := latest.release()
	latest = nil
	done.Complete()
	if released[0].Load() != 0 || budget.Stats().Sources != 3 {
		t.Fatal("read view lost original prefix")
	}
	done = newerView.release()
	newerView = nil
	done.Complete()
	if released[0].Load() != 0 || budget.Stats().Sources != 3 {
		t.Fatal("old native source edge lost")
	}
	done = oldView.release()
	oldView = nil
	boundary = nil
	if released[0].Load() != 1 || budget.Stats().Sources != 3 {
		t.Fatal("source slot refunded before original receipt completion")
	}
	done.Complete()
	if budget.Stats().Sources != 2 {
		t.Fatal(budget.Stats())
	}
	if _, ok := suffix.get([]byte("old")); ok {
		t.Fatal("accepted prefix leaked into suffix")
	}
	if row, ok := suffix.get([]byte("middle")); !ok || row.Value != "B" {
		t.Fatal(row, ok)
	}
	budget.Close()
	done = suffix.release()
	suffix = nil
	done.Complete()
	if released[1].Load() != 1 || released[2].Load() != 1 || budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeSourceSuffixFailureReturnsExactPartialWithoutInlineDestruction(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var released atomic.Int32
	first := nativeTestAppendSource(t, budget, nil, "a", "A", func() { released.Add(1) })
	boundary := first.source.root
	second := nativeTestAppendSource(t, budget, first, "b", "B", func() { released.Add(1) })
	first = nil
	latest := nativeTestAppendSource(t, budget, second, "c", "C", func() { released.Add(1) })
	second = nil
	refusal := errors.New("original suffix position admission refused")
	calls := 0
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) {
		calls++
		if calls == 3 {
			return nil, refusal
		}
		return budget.AdmitNativeBytes(n)
	}
	partial, scratch, err := prepareNativeSourceSuffix(latest, boundary, admit)
	if err != refusal || partial == nil || partial.source != latest.older.source || partial.older != nil {
		t.Fatal("lost original admitted partial", err)
	}
	if released.Load() != 0 || budget.Stats().Sources != 3 {
		t.Fatal("failure destroyed original shared resources")
	}
	done := partial.release()
	partial = nil
	done.Complete()
	scratch.Complete()
	budget.Close()
	done = latest.release()
	latest = nil
	boundary = nil
	done.Complete()
	if released.Load() != 3 || budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
