package collections

import (
	"errors"
	"strconv"
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

func TestNativeEngineEmptyCapturedDomainKeepsNewerCapturedSources(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema, err := newNativeSchemaState(&CollectionMeta{Name: "collection"}, admit)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := newNativeDomainState("primary", 0, 1, 0, nil, admit)
	if err != nil {
		t.Fatal(err)
	}
	captured := nativeTestEngine(t, budget, nil, empty, schema)
	empty = nil
	c, _ := captured.collections.Get(1)
	d, _ := c.domains.Get("primary")
	suffix, err := prepareNativeDomainUpdate(d, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("new"), Value: []byte("suffix")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	updated := nativeTestEngine(t, budget, captured, suffix, schema)
	suffix, schema = nil, nil
	latest, err := prepareNativeEngineRollover(updated)
	if err != nil {
		t.Fatal(err)
	}
	built := []nativeBuiltDomain{{collection: 1, interpretation: 1, name: "primary", empty: true}}
	before := budget.Stats()
	built[0].root = 1
	if next, err := prepareNativeEngineHandoff(latest, built, 1, budget); next != nil || err != backenddb.ErrNativeSemanticUnsupported || before != budget.Stats() {
		t.Fatal("invalid empty proof changed ownership", next, err, before, budget.Stats())
	}
	built[0].root = 0
	nextLatest, err := prepareNativeEngineHandoff(latest, built, 1, budget)
	if err != nil {
		t.Fatal(err)
	}
	nextFrozen, err := prepareNativeEngineHandoff(captured, built, 1, budget)
	if err != nil {
		t.Fatal(err)
	}
	c, _ = nextLatest.collections.Get(1)
	d, _ = c.domains.Get("primary")
	if d.pending != nil || d.captured == nil || c.pending.Len() != 1 || nextLatest.pending.Len() != 1 {
		t.Fatal("empty captured prefix consumed newer source")
	}
	if row, ok := d.captured.get([]byte("new")); !ok || row.Value != "suffix" {
		t.Fatal(row, ok)
	}
	c, _ = nextFrozen.collections.Get(1)
	d, _ = c.domains.Get("primary")
	if d.pending != nil || d.captured != nil || c.pending.Len() != 0 || nextFrozen.pending.Len() != 0 {
		t.Fatal("empty frozen successor retains nonexistent physical work")
	}
	c, d = nil, nil
	clear(built)
	built = nil
	nativeTestFinishEngine(t, &captured)
	nativeTestFinishEngine(t, &updated)
	nativeTestFinishEngine(t, &latest)
	nativeTestFinishEngine(t, &nextFrozen)
	budget.Close()
	if budget.Stats().Sources != 1 {
		t.Fatal("newer source was not owned by successor", budget.Stats())
	}
	nativeTestFinishEngine(t, &nextLatest)
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
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

func TestNativeEngineDefaultSourceBoundCanFinishBothHandoffs(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := budget.ReserveMaintenance(min(limits.MaxInFlightBytes, limits.MaxTotalBytes, limits.MaxRetiredBytes) / 2)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	names, err := cowbtree.NewOwnedMapBuilder[string, *nativeCollectionState](nil, limits.MaxSources, nativeCollectionLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := cowbtree.NewOwnedMapBuilder[uint64, *nativeCollectionState](nil, limits.MaxSources, nativeCollectionLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := cowbtree.NewOwnedMapBuilder[uint64, *nativeCollectionState](nil, limits.MaxSources, nativeCollectionLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	originals := make([]*nativeCollectionState, 0, limits.MaxSources)
	built := make([]nativeBuiltDomain, 0, limits.MaxSources)
	for i := 1; i <= limits.MaxSources; i++ {
		id := uint64(i)
		name := "collection-" + strconv.Itoa(i)
		schema, err := newNativeSchemaState(&CollectionMeta{Name: name}, admit)
		if err != nil {
			t.Fatal(err)
		}
		domain, err := prepareNativeDomainUpdate(nil, "primary", id, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("captured")}}, budget)
		if err != nil {
			t.Fatal(err)
		}
		domains := nativeTestDomainDirectory(t, nil, domain, admit)
		pendingDomains := nativeTestDomainDirectory(t, nil, domain, admit)
		collection, err := newNativeCollectionState(id, schema, domains, pendingDomains, admit)
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, collection)
		built = append(built, nativeBuiltDomain{collection: id, interpretation: 1, name: domain.name, oldBaseRoot: id, root: id + 1000, boundary: domain.pending})
		if err = names.Set(name, collection); err != nil {
			t.Fatal(err)
		}
		if err = ids.Set(id, collection); err != nil {
			t.Fatal(err)
		}
		if err = pending.Set(id, collection); err != nil {
			t.Fatal(err)
		}
		done, err := (nativeDomainLifetime{}).Release(domain)
		domain, schema, collection = nil, nil, nil
		if err != nil {
			t.Fatal(err)
		}
		done.Complete()
	}
	captured, err := newNativeEngineVersion(names.Seal(), ids.Seal(), pending.Seal(), uint64(limits.MaxSources+1), budget, maintenance)
	names, ids, pending = nil, nil, nil
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range originals {
		done, err := (nativeCollectionLifetime{}).Release(c)
		originals[i], c = nil, nil
		if err != nil {
			t.Fatal(err)
		}
		done.Complete()
	}
	originals = nil
	active, err := prepareNativeEngineRollover(captured)
	if err != nil {
		t.Fatal(err)
	}
	promiseBytes, count, touched, err := nativeHandoffCapacity(captured)
	if err != nil || count != limits.MaxSources || touched != limits.MaxSources {
		t.Fatal(promiseBytes, count, touched, err)
	}
	// Preserve a separate original build-ledger floor. Default capacity must
	// finish this complete finite prefix without increasing any configured cap.
	floor := uint64(1 << 20)
	promise, err := maintenance.AdmitNativeBytes(floor + promiseBytes)
	if err != nil {
		t.Fatalf("default %d-source prefix cannot promise handoff (%d bytes): %v", limits.MaxSources, promiseBytes, err)
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := budget.AcquireExternal(limits.MaxInFlightBytes - budget.Stats().ReservedBytes - header)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := budget.AdmitNativeBytes(1); !errors.Is(err, memtable.ErrCOWCapacity) || lease != nil {
		t.Fatal("producer admission is not exhausted", err)
	}
	maintenance.Close()
	allocation := &nativeHandoffTestAllocator{NativeSemanticAllocator: maintenance, parent: promise, floor: floor}
	nextLatest, err := prepareNativeEngineHandoff(active, built, count, allocation)
	if err != nil {
		t.Fatal("default latest successor exceeded original promise", err)
	}
	nextFrozen, err := prepareNativeEngineHandoff(captured, built, count, allocation)
	if err != nil {
		t.Fatal("default frozen successor exceeded original promise", err)
	}
	allocation.parent = nil
	allocation = nil
	promise.Close()
	promise = nil
	if nextLatest.pending.Len() != 0 || nextFrozen.pending.Len() != 0 {
		t.Fatal("accepted pending directory was not emptied")
	}
	verify := func(e *nativeEngineVersion) {
		e.collections.Scan(func(id uint64, c *nativeCollectionState) bool {
			d, _ := c.domains.Get("primary")
			if d.baseRoot != id+1000 || d.pending != nil || d.captured != nil {
				t.Fatal("incomplete default successor", id)
			}
			return true
		})
	}
	verify(nextLatest)
	verify(nextFrozen)
	verify = nil
	clear(built)
	built = nil
	filler.Close()
	filler = nil
	nativeTestFinishEngine(t, &captured)
	nativeTestFinishEngine(t, &active)
	if budget.Stats().Sources != 0 {
		t.Fatal("default captured sources survived their last original owners", budget.Stats())
	}
	budget.Close()
	nativeTestFinishEngine(t, &nextLatest)
	nativeTestFinishEngine(t, &nextFrozen)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}
