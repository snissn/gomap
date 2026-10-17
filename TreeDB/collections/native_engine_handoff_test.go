package collections

import (
	"errors"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This construction capability is private to the original handoff call. Child
// leases contain only the same budget/charge, never this reservation or wrapper.
type nativeHandoffTestAllocator struct {
	backenddb.NativeSemanticAllocator
	parent *memtable.COWExternalLease
	floor  uint64
}

func (a *nativeHandoffTestAllocator) AdmitNativeBytes(n uint64) (*memtable.COWExternalLease, error) {
	return a.parent.SplitNativeBytes(n, a.floor)
}

type nativeHandoffRefusalAllocator struct {
	nativeHandoffTestAllocator
	remaining int
	failure   error
}

func (a *nativeHandoffRefusalAllocator) AdmitNativeBytes(n uint64) (*memtable.COWExternalLease, error) {
	if a.remaining == 0 {
		return nil, a.failure
	}
	a.remaining--
	return a.nativeHandoffTestAllocator.AdmitNativeBytes(n)
}

func TestNativeEngineHandoffPreservesSuffixFromOriginalPreacceptedPromise(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 64<<20, 64<<20, 64<<20
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := budget.ReserveMaintenance(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema, err := newNativeSchemaState(&CollectionMeta{Name: "collection"}, admit)
	if err != nil {
		t.Fatal(err)
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("captured")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	captured := nativeTestEngine(t, budget, nil, domain, schema)
	domain, schema = nil, nil
	captured.maintenance = maintenance // private fixture configuration
	active, err := prepareNativeEngineRollover(captured)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := active.collections.Get(1)
	d, _ := c.domains.Get("primary")
	domain, err = prepareNativeDomainUpdate(d, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("suffix-one")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	suffix := nativeTestEngine(t, budget, active, domain, c.schema)
	domain = nil
	suffix.maintenance = maintenance
	secondActive, err := prepareNativeEngineRollover(suffix)
	if err != nil {
		t.Fatal(err)
	}
	c, _ = secondActive.collections.Get(1)
	d, _ = c.domains.Get("primary")
	domain, err = prepareNativeDomainUpdate(d, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("other"), Value: []byte("suffix-two")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	latest := nativeTestEngine(t, budget, secondActive, domain, c.schema)
	domain = nil
	latest.maintenance = maintenance
	c, _ = captured.collections.Get(1)
	d, _ = c.domains.Get("primary")
	built := []nativeBuiltDomain{{collection: 1, interpretation: 1, name: "primary", root: 900, boundary: d.pending}}
	promiseBytes, count, touched, err := nativeHandoffCapacity(captured)
	if err != nil || count != 1 || touched != 1 || promiseBytes == 0 {
		t.Fatal(promiseBytes, count, touched, err)
	}
	floor := memtable.COWAllocationCharge(327)
	promise, err := maintenance.AdmitNativeBytes(floor + promiseBytes)
	if err != nil {
		t.Fatal(err)
	}
	failedPromise, err := maintenance.AdmitNativeBytes(floor + promiseBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Exhaust both fresh admission lanes AFTER promising the accepted handoff.
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := budget.AcquireExternal(limits.MaxTotalBytes - budget.Stats().TotalBytes - header)
	if err != nil {
		t.Fatal(err)
	}
	maintenance.Close()
	beforeFailure := budget.Stats()
	failure := errors.New("original handoff source-position allocation failed")
	refusal := &nativeHandoffRefusalAllocator{
		nativeHandoffTestAllocator: nativeHandoffTestAllocator{NativeSemanticAllocator: maintenance, parent: failedPromise, floor: floor},
		remaining:                  8, failure: failure,
	}
	partial, err := prepareNativeEngineHandoff(latest, built, count, refusal)
	if err != failure || partial == nil || len(partial.preparation.ownedDomains) != 1 || partial.preparation.ownedDomains[0].pending == nil {
		t.Fatal("late failure lost original partial pending edge", err)
	}
	// This is the exact private original, released only off-gate. The public
	// wrapper must instead park it on its sticky build ledger until shutdown.
	nativeTestFinishEngine(t, &partial)
	refusal.parent = nil
	refusal = nil
	failedPromise.Close()
	failedPromise = nil
	if got := budget.Stats(); got.TotalBytes != beforeFailure.TotalBytes || got.Sources != beforeFailure.Sources {
		t.Fatal("partial failure changed the original physical source owners", beforeFailure, got)
	}
	allocation := &nativeHandoffTestAllocator{NativeSemanticAllocator: maintenance, parent: promise, floor: floor}
	nextLatest, err := prepareNativeEngineHandoff(latest, built, count, allocation)
	if err != nil {
		t.Fatal("first successor exceeded its original promise", err)
	}
	nextFrozen, err := prepareNativeEngineHandoff(captured, built, count, allocation)
	if err != nil {
		t.Fatal("second successor exceeded its original promise", err)
	}
	allocation.parent = nil
	allocation = nil
	promise.Close()
	promise = nil
	c, _ = nextLatest.collections.Get(1)
	d, _ = c.domains.Get("primary")
	if d.baseRoot != 900 || d.pending == nil || d.captured == nil || d.captured.older != nil {
		t.Fatal("handoff lost suffix or retained accepted prefix")
	}
	if row, ok := d.pending.Get([]byte("other")); !ok || row.Value != "suffix-two" {
		t.Fatal(row, ok)
	}
	if row, ok := d.captured.get([]byte("key")); !ok || row.Value != "suffix-one" {
		t.Fatal(row, ok)
	}
	c, _ = nextFrozen.collections.Get(1)
	d, _ = c.domains.Get("primary")
	if d.baseRoot != 900 || d.pending != nil || d.captured != nil || c.pending.Len() != 0 || nextFrozen.pending.Len() != 0 {
		t.Fatal("frozen successor still owns materialized prefix")
	}
	c, d = nil, nil
	clear(built)
	built = nil
	filler.Close()
	filler = nil
	nativeTestFinishEngine(t, &captured)
	nativeTestFinishEngine(t, &active)
	nativeTestFinishEngine(t, &suffix)
	nativeTestFinishEngine(t, &secondActive)
	nativeTestFinishEngine(t, &latest)
	if budget.Stats().Sources != 2 {
		t.Fatal("accepted prefix source did not retire independently", budget.Stats())
	}
	nativeTestFinishEngine(t, &nextFrozen)
	budget.Close()
	nativeTestFinishEngine(t, &nextLatest)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}
