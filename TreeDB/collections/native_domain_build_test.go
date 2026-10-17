package collections

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type nativeDomainBuildControl struct {
	group         *backenddb.RootPublicationBuildGroup
	calls, failAt int
	failure       error
	maxRows       int
}

func (c *nativeDomainBuildControl) BuildNativeOrderedRoot(input backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error) {
	c.calls++
	c.maxRows = max(c.maxRows, input.Delta.Len())
	if c.calls == c.failAt {
		return 0, c.failure
	}
	return c.group.BuildNativeOrderedRoot(input)
}

func (c *nativeDomainBuildControl) BuildNativeSystemDelta(delta *batch.Batch) error {
	c.calls++
	c.maxRows = max(c.maxRows, delta.Len())
	if c.calls == c.failAt {
		return c.failure
	}
	return c.group.BuildNativeSystemDelta(delta)
}

func TestNativeDomainBuildCapturedSourcesUsesBoundedOriginalScratch(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := budget.ReserveMaintenance(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return maintenance.AdmitNativeBytes(n) }
	entries := make([]memtable.COWMutation, 600)
	for i := range entries {
		entries[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%04d", i)), Value: []byte("original"), Flags: node.FlagInline, Revision: 11}
	}
	first, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, entries, budget)
	if err != nil {
		t.Fatal(err)
	}
	entries = nil
	rotated, err := prepareNativeDomainRollover(first, admit)
	if err != nil {
		t.Fatal(err)
	}
	second, err := prepareNativeDomainUpdate(rotated, "primary", 0, 1, 0, []memtable.COWMutation{
		{Key: []byte("0000"), Value: []byte("second"), Flags: node.FlagInline, Revision: 22},
		{Key: []byte("0010"), Flags: node.FlagTombstone, Revision: 22},
		{Key: []byte("new"), Value: []byte("second"), Flags: node.FlagInline, Revision: 22},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	secondRotation, err := prepareNativeDomainRollover(second, admit)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := prepareNativeDomainUpdate(secondRotation, "primary", 0, 1, 0, []memtable.COWMutation{
		{Key: []byte("0000"), Value: []byte("latest"), Flags: node.FlagInline, Revision: 33},
		{Key: []byte("0011"), Flags: node.FlagTombstone, Revision: 33},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	work, err := newNativeDomainBuild(latest, budget.Limits().MaxSources, admit)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range []**nativeDomainState{&first, &rotated, &second, &secondRotation, &latest} {
		done, err := (nativeDomainLifetime{}).Release(*slot)
		*slot = nil
		if err != nil {
			t.Fatal(err)
		}
		done.Complete()
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes - header)
	if err != nil {
		t.Fatal(err)
	}
	budget.Close()
	maintenance.Close()
	before := budget.Stats()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed, ResolvedProfile: backenddb.ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	basis := database.AcquireSnapshot()
	defer basis.Close()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	control := &nativeDomainBuildControl{group: group}
	root, err := work.materialize(control)
	if err != nil {
		t.Fatal(err)
	}
	if control.calls != 5 || control.maxRows != nativeBuildBatchEntries || work.delta.Len() != 0 || len(work.roots) != 0 || !work.complete || before != budget.Stats() {
		t.Fatal("build exceeded bounded scratch or acquired producer bytes", control.calls, control.maxRows, before, budget.Stats())
	}
	for _, check := range []struct {
		key, value string
		revision   page.EntryRevision
	}{{"0000", "latest", 33}, {"0599", "original", 11}, {"new", "second", 22}} {
		entry, err := basis.GetEntryAtRoot(root, []byte(check.key))
		if err != nil || string(entry.Value) != check.value || entry.Revision != check.revision {
			t.Fatal(check, entry, err)
		}
	}
	for _, key := range []string{"0010", "0011"} {
		if _, err := basis.GetAtRoot(root, []byte(key)); err == nil {
			t.Fatal("newer deletion was lost", key)
		}
	}
	if _, err := basis.Get([]byte("0000")); err == nil {
		t.Fatal("private domain build leaked into captured raw root")
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	control.group = nil
	control = nil
	group = nil
	done, err := work.finish()
	work = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	filler.Close()
	filler = nil
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}

func TestNativeDomainBuildFirstPartialFailureIsSticky(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	entries := make([]memtable.COWMutation, nativeBuildBatchEntries+1)
	for i := range entries {
		entries[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%04d", i)), Value: []byte("value"), Flags: node.FlagInline}
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, entries, budget)
	if err != nil {
		t.Fatal(err)
	}
	entries = nil
	work, err := newNativeDomainBuild(domain, budget.Limits().MaxSources, admit)
	if err != nil {
		t.Fatal(err)
	}
	done, err := (nativeDomainLifetime{}).Release(domain)
	domain = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed, ResolvedProfile: backenddb.ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	basis := database.AcquireSnapshot()
	defer basis.Close()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	failure := errors.New("second bounded native root build failed")
	control := &nativeDomainBuildControl{group: group, failAt: 2, failure: failure}
	if _, err := work.materialize(control); err != failure || control.calls != 2 {
		t.Fatal(err, control.calls)
	}
	if work.root == 0 {
		t.Fatal("failure erased original first bounded-root identity")
	}
	before := budget.Stats()
	if _, err := work.materialize(control); err != failure || control.calls != 2 {
		t.Fatal("failed original materialization replayed", err, control.calls)
	}
	if _, err := work.finish(); err != failure || work.source == nil || work.delta == nil || work.refund == nil {
		t.Fatal("failed original lost its custody", err)
	}
	budget.Close()
	if got := budget.Stats(); got.Sources != before.Sources || got.HistoryBytes != before.HistoryBytes || got.ExternalBytes != before.ExternalBytes {
		t.Fatal("Close destructed failed original native custody", before, got)
	}
	// Deliberately parked, exactly as original semantic Work on terminal error;
	// no test teardown retries its destructor or invents replacement custody.
}
