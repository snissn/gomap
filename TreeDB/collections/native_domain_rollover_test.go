package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestNativeDomainRolloverSourceBoundsViewsAndSuffixGeneration(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxSources = 2
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	old, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("old"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	// An already prepared old-basis private fork may finish or cancel after the
	// physical rollover. It cannot mutate the original captured root.
	fork, err := memtable.ForkCOWWriter(old.pending)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := fork.Prepare([]memtable.COWMutation{{Key: []byte("key"), Value: []byte("private")}}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := prepareNativeDomainRollover(old, admit)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.pending != nil || rotated.captured == nil || budget.Stats().Sources != 1 {
		t.Fatal(budget.Stats())
	}
	nextRoot := prepared.Publish()
	prepared = nil
	retirement := fork.Close()
	fork = nil
	retirement.Drain()
	if row, _ := rotated.captured.get([]byte("key")); row.Value != "old" {
		t.Fatal(row)
	}
	retirement = nextRoot.Release()
	nextRoot = nil
	retirement.Drain()
	repeated, err := prepareNativeDomainRollover(rotated, admit)
	if err != nil {
		t.Fatal(err)
	}
	if repeated != rotated || budget.Stats().Sources != 1 {
		t.Fatal("no-op created another source")
	}
	done, err := (nativeDomainLifetime{}).Release(repeated)
	repeated = nil
	done.Complete()
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := prepareNativeDomainUpdate(rotated, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("new"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := suffix.pending.Get([]byte("key")); row.Value != "new" {
		t.Fatal(row)
	}
	if suffix.pending == old.pending || suffix.captured != rotated.captured {
		t.Fatal("suffix did not preserve captured ownership")
	}
	before := budget.Stats()
	// The fresh active suffix owns its physical slot before accepted writes;
	// capturing it consumes no additional source capacity.
	secondCaptured, err := prepareNativeDomainRollover(suffix, admit)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Stats().Sources != 2 {
		t.Fatal(budget.Stats())
	}
	refused, err := prepareNativeDomainUpdate(nil, "other", 0, 1, 0, []memtable.COWMutation{{Key: []byte("other"), Value: []byte("refused")}}, budget)
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("prewrite active source cap: %v", err)
	}
	if refused != nil {
		t.Fatal("active source refusal produced state")
	}
	if budget.Stats().Sources != before.Sources {
		t.Fatal("capture/refusal changed original source slots", budget.Stats())
	}
	// A view independently keeps the exact captured root after domains and the
	// original budget close. Its original generation and source slot both survive until that last view.
	view, err := rotated.captured.source.root.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	budget.Close()
	for _, d := range []*nativeDomainState{old, rotated, suffix, secondCaptured} {
		done, err = (nativeDomainLifetime{}).Release(d)
		if err != nil {
			t.Fatal(err)
		}
		d = nil
		done.Complete()
	}
	old, rotated, suffix, secondCaptured = nil, nil, nil, nil
	if budget.Stats().Sources != 1 || budget.Stats().TotalBytes == 0 {
		t.Fatal(budget.Stats())
	}
	if row, ok, err := view.Get([]byte("key")); err != nil || !ok || row.Value != "old" {
		t.Fatal(row, ok, err)
	}
	view.Close()
	view = nil
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeDomainCanceledRolloverKeepsOriginalSlotAndPreservesForkCancellation(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	old, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("old")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := memtable.ForkCOWWriter(old.pending)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := fork.Prepare([]memtable.COWMutation{{Key: []byte("key"), Value: []byte("cancel")}}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	refusal := errors.New("original metadata admission refused")
	if _, err := prepareNativeDomainRollover(old, func(uint64) (cowbtree.OwnedMapLease, error) { return nil, refusal }); err != refusal {
		t.Fatal(err)
	}
	after := budget.Stats()
	if after.Sources != before.Sources || after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes {
		t.Fatal(before, after)
	}
	calls := 0
	partial, err := prepareNativeDomainRollover(old, func(n uint64) (cowbtree.OwnedMapLease, error) {
		calls++
		if calls == 2 {
			return nil, refusal
		}
		return budget.AdmitNativeBytes(n)
	})
	if err != refusal || partial == nil || partial.privateSource == nil {
		t.Fatal("missing original partial source", err)
	}
	done, err := (nativeDomainLifetime{}).Release(partial)
	partial = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	if budget.Stats().Sources != before.Sources {
		t.Fatal("cancel changed active source slot", budget.Stats())
	}
	rotated, err := prepareNativeDomainRollover(old, func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) })
	if err != nil {
		t.Fatal("cancel lost original capture claim", err)
	}
	done, err = (nativeDomainLifetime{}).Release(rotated)
	rotated = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	retirement := prepared.Cancel()
	prepared = nil
	retirement.Drain()
	retirement = fork.Close()
	fork = nil
	retirement.Drain()
	if row, _ := old.pending.Get([]byte("key")); row.Value != "old" {
		t.Fatal(row)
	}
	budget.Close()
	done, err = (nativeDomainLifetime{}).Release(old)
	old = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
