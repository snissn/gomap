package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeTestDomainDirectory(t *testing.T, base *cowbtree.OwnedMap[string, *nativeDomainState], domain *nativeDomainState, admit cowbtree.OwnedMapAdmit) *cowbtree.OwnedMap[string, *nativeDomainState] {
	t.Helper()
	count := 0
	if domain != nil {
		count = 1
	}
	builder, err := cowbtree.NewOwnedMapBuilder(base, count, nativeDomainLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	if domain != nil {
		if err = builder.Set(domain.name, domain); err != nil {
			t.Fatal(err)
		}
	}
	return builder.Seal()
}
func nativeTestCollectionDirectory(t *testing.T, base *cowbtree.OwnedMap[uint64, *nativeCollectionState], collection *nativeCollectionState, admit cowbtree.OwnedMapAdmit) *cowbtree.OwnedMap[uint64, *nativeCollectionState] {
	t.Helper()
	builder, err := cowbtree.NewOwnedMapBuilder(base, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Set(collection.incarnation, collection); err != nil {
		t.Fatal(err)
	}
	return builder.Seal()
}
func nativeTestNameDirectory(t *testing.T, base *cowbtree.OwnedMap[string, *nativeCollectionState], collection *nativeCollectionState, admit cowbtree.OwnedMapAdmit) *cowbtree.OwnedMap[string, *nativeCollectionState] {
	t.Helper()
	builder, err := cowbtree.NewOwnedMapBuilder(base, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Set(collection.schema.meta.Name, collection); err != nil {
		t.Fatal(err)
	}
	return builder.Seal()
}
func nativeTestEngine(t *testing.T, budget *memtable.COWBudget, base *nativeEngineVersion, domain *nativeDomainState, schema *nativeSchemaState) *nativeEngineVersion {
	t.Helper()
	admit := func(bytes uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(bytes) }
	var oldCollection *nativeCollectionState
	if base != nil {
		oldCollection, _ = base.collections.Get(1)
	}
	var oldDomains, oldPending *cowbtree.OwnedMap[string, *nativeDomainState]
	if oldCollection != nil {
		oldDomains, oldPending = oldCollection.domains, oldCollection.pending
		schema.retain()
	}
	domains := nativeTestDomainDirectory(t, oldDomains, domain, admit)
	pending := nativeTestDomainDirectory(t, oldPending, domain, admit)
	collection, err := newNativeCollectionState(1, schema, domains, pending, admit)
	if err != nil {
		t.Fatal(err)
	}
	var oldNames *cowbtree.OwnedMap[string, *nativeCollectionState]
	var oldCollections, oldPendingCollections *cowbtree.OwnedMap[uint64, *nativeCollectionState]
	if base != nil {
		oldNames, oldCollections, oldPendingCollections = base.names, base.collections, base.pending
	}
	names := nativeTestNameDirectory(t, oldNames, collection, admit)
	collections := nativeTestCollectionDirectory(t, oldCollections, collection, admit)
	pendingCollections := nativeTestCollectionDirectory(t, oldPendingCollections, collection, admit)
	engine, err := newNativeEngineVersion(names, collections, pendingCollections, 2, budget)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := (nativeCollectionLifetime{}).Release(collection)
	collection = nil
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	completion, err = (nativeDomainLifetime{}).Release(domain)
	domain = nil
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	return engine
}

func TestNativeEngineOriginalCleanupDetachesNestedGraphBeforeRefund(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(bytes uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(bytes) }
	schema, err := newNativeSchemaState(&CollectionMeta{Name: "collection"}, admit)
	if err != nil {
		t.Fatal(err)
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 7, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("old"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	old := nativeTestEngine(t, budget, nil, domain, schema)
	oldCollection, _ := old.collections.Get(1)
	oldDomain, _ := oldCollection.domains.Get("primary")
	successorDomain, err := prepareNativeDomainUpdate(oldDomain, "primary", 7, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("new"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	next := nativeTestEngine(t, budget, old, successorDomain, schema)
	schema = nil
	domain = nil
	successorDomain = nil
	nextCollection, _ := next.collections.Get(1)
	if oldCollection.schema != nextCollection.schema {
		t.Fatal("ordinary update copied schema")
	}
	nextDomain, _ := nextCollection.domains.Get("primary")
	if got, _ := oldDomain.pending.Get([]byte("key")); got.Value != "old" {
		t.Fatal(got)
	}
	if got, _ := nextDomain.pending.Get([]byte("key")); got.Value != "new" {
		t.Fatal(got)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		next.Retain()
		if next.Release() != nil {
			panic("not last owner")
		}
	}); allocations != 0 {
		t.Fatalf("engine retain/release allocated %g", allocations)
	}
	before := budget.Stats()
	var oldWork *rootpublication.SemanticCleanup
	if allocations := testing.AllocsPerRun(1, func() {
		// AllocsPerRun invokes a warmup and a measured call: retain a test edge.
		old.Retain()
		oldWork = old.Release()
	}); allocations != 0 || oldWork != nil {
		t.Fatal("nonfinal release allocated or queued", allocations)
	}
	oldWork = old.Release()
	if oldWork != &old.cleanup || oldWork.Work != old {
		t.Fatal("last release replaced original record")
	}
	if budget.Stats() != before || old.names == nil || oldDomain.pending == nil {
		t.Fatal("last release performed physical destruction")
	}
	if !oldWork.EnqueueOnce() || oldWork.EnqueueOnce() {
		t.Fatal("original queue transfer not once")
	}
	completion, err := oldWork.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if old.names != nil || old.collections != nil || old.pending != nil || old.refund != nil || oldWork.Work != nil {
		t.Fatal("completion escaped before graph detach")
	}
	oldWork = nil
	old = nil
	oldCollection = nil
	oldDomain = nil
	completion.Complete()
	if got, _ := nextDomain.pending.Get([]byte("key")); got.Value != "new" {
		t.Fatal("old worker lost successor root", got)
	}
	budget.Close()
	nextWork := next.Release()
	completion, err = nextWork.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("containing engine refunded before worker aliases detach")
	}
	if next.names != nil || next.collections != nil || next.pending != nil || next.allocator != nil || next.refund != nil || nextWork.Work != nil {
		t.Fatal("terminal graph aliases survived")
	}
	nextDomain = nil
	nextCollection = nil
	next = nil
	nextWork = nil
	completion.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
