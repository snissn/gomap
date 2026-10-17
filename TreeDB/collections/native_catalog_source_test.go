package collections

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeTestCatalogEngine(t *testing.T, maxSources int) (*nativeEngineVersion, *memtable.COWBudget, *memtable.COWBudget) {
	t.Helper()
	limits := memtable.DefaultCOWLimits()
	limits.MaxSources = maxSources
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
	base := nativeTestEngine(t, budget, nil, nil, schema)
	base.maintenance = maintenance
	return base, budget, maintenance
}

func TestNativeEngineCatalogOnlyCaptureBuildAndSuffixUseOriginalPromise(t *testing.T) {
	base, budget, maintenance := nativeTestCatalogEngine(t, 32)
	rows := make([]memtable.COWMutation, 600)
	for i := range rows {
		rows[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%s%04d", systemCollectionMetaPrefix, i)), Value: []byte("captured"), Flags: node.FlagInline, Revision: 11}
	}
	captured, err := prepareNativeEngineCatalogUpdate(base, rows, 3)
	rows = nil
	if err != nil || budget.Stats().Sources != 1 || captured.nextIncarnation != 3 {
		t.Fatal(err, budget.Stats())
	}
	active, err := prepareNativeEngineRollover(captured)
	if err != nil || active.catalog.pending != nil || active.catalog.captured == nil {
		t.Fatal("catalog source was not rotated", err)
	}
	before := budget.Stats()
	noop, err := prepareNativeEngineRollover(active)
	if err != nil || noop != active || budget.Stats() != before || noop.Release() != nil {
		t.Fatal("no-op checkpoint added source or metadata", err, budget.Stats())
	}
	noop = nil
	suffix, err := prepareNativeEngineCatalogUpdate(active, []memtable.COWMutation{
		{Key: []byte(systemCollectionMetaPrefix + "0000"), Value: []byte("newer"), Flags: node.FlagInline, Revision: 12},
		{Key: []byte(systemCollectionMetaPrefix + "0001"), Flags: node.FlagTombstone, Revision: 12},
	}, 4)
	if err != nil {
		t.Fatal(err)
	}
	secondActive, err := prepareNativeEngineRollover(suffix)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := prepareNativeEngineCatalogUpdate(secondActive, []memtable.COWMutation{{Key: []byte(systemCollectionDocumentGenerationPrefix + "collection"), Value: encodeRootID(15), Flags: node.FlagInline, Revision: 15}}, 5)
	if err != nil || budget.Stats().Sources != 3 {
		t.Fatal(err, budget.Stats())
	}
	build, err := prepareNativeEngineBuild(captured)
	if err != nil || build.catalog == nil || len(build.domains) != 0 || len(build.built) != 0 {
		t.Fatal("catalog-only prefix was not admitted as actual physical work", err)
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes - header)
	if err != nil {
		t.Fatal(err)
	}
	budget.Close()
	maintenance.Close()
	before = budget.Stats()
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
	if err := build.materializeDomains(control); err != nil || control.calls != 3 || control.maxRows != nativeBuildBatchEntries || budget.Stats() != before {
		t.Fatal("catalog traversal exceeded preaccepted scratch", err, control.calls, control.maxRows, budget.Stats())
	}
	if group.Accepted() {
		t.Fatal("private catalog chunks published a prefix")
	}
	final := database.NewPhysicalBatch().(*backenddb.Batch)
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	if err := final.Write(); err != nil {
		t.Fatal(err)
	}
	final.Close()
	final = nil
	if !group.Accepted() {
		t.Fatal("catalog-only physical group was not accepted")
	}
	accepted := database.AcquireSnapshot()
	defer accepted.Close()
	state, ok := accepted.StateToken()
	if !ok {
		t.Fatal("missing exact accepted state")
	}
	for _, key := range []string{systemCollectionMetaPrefix + "0000", systemCollectionMetaPrefix + "0001", systemCollectionMetaPrefix + "0599"} {
		entry, err := accepted.GetEntryAtRoot(state.SystemRootPageID, []byte(key))
		if err != nil || string(entry.Value) != "captured" || entry.Revision != 11 {
			t.Fatal("captured system prefix borrowed future metadata", key, entry, err)
		}
		if raw, err := database.Get([]byte(key)); err != nil || raw != nil {
			t.Fatal("catalog mutation entered raw user namespace", raw, err)
		}
	}
	nextLatest, err := build.prepareHandoff(latest)
	if err != nil {
		t.Fatal("latest catalog suffix exceeded original promise", err)
	}
	nextFrozen, err := build.prepareHandoff(captured)
	if err != nil {
		t.Fatal("frozen catalog successor exceeded original promise", err)
	}
	if nextLatest.nextIncarnation != 5 || nextLatest.catalog.pending != latest.catalog.pending || nextLatest.catalog.captured == nil || nextLatest.catalog.captured.older != nil || nativeCatalogHasPending(nextFrozen.catalog) {
		t.Fatal("catalog handoff consumed a newer suffix or kept captured prefix")
	}
	if row, ok := nextLatest.catalog.captured.get([]byte(systemCollectionMetaPrefix + "0000")); !ok || row.Value != "newer" || row.Revision != 12 {
		t.Fatal("captured suffix identity was lost", row, ok)
	}
	if row, ok := nextLatest.catalog.captured.get([]byte(systemCollectionMetaPrefix + "0001")); !ok || row.Flags&node.FlagTombstone == 0 {
		t.Fatal("newer catalog deletion was lost", row, ok)
	}
	build = nil
	filler.Close()
	filler = nil
	for _, slot := range []**nativeEngineVersion{&base, &captured, &active, &suffix, &secondActive, &latest, &nextFrozen} {
		nativeTestFinishEngine(t, slot)
	}
	if budget.Stats().Sources != 2 {
		t.Fatal("consumed catalog prefix retained its source", budget.Stats())
	}
	nativeTestFinishEngine(t, &nextLatest)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}

func TestNativeEngineCatalogSourceRefusesBeforeMutationAndCancelKeepsOriginalSlot(t *testing.T) {
	base, budget, maintenance := nativeTestCatalogEngine(t, 1)
	first, err := prepareNativeEngineCatalogUpdate(base, []memtable.COWMutation{{Key: []byte(systemCollectionMetaPrefix + "one"), Value: []byte("old")}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	canceled, err := prepareNativeEngineRollover(first)
	if err != nil {
		t.Fatal(err)
	}
	nativeTestFinishEngine(t, &canceled)
	if before != budget.Stats() || budget.Stats().Sources != 1 {
		t.Fatal("canceling catalog capture changed original active source", before, budget.Stats())
	}
	active, err := prepareNativeEngineRollover(first)
	if err != nil {
		t.Fatal(err)
	}
	before = budget.Stats()
	partial, err := prepareNativeEngineCatalogUpdate(active, []memtable.COWMutation{{Key: []byte(systemCollectionMetaPrefix + "two"), Value: []byte("new")}}, 4)
	if !errors.Is(err, memtable.ErrCOWCapacity) || partial == nil || partial.catalog != nil {
		t.Fatal("catalog write exceeded shared source cap", err, budget.Stats())
	}
	nativeTestFinishEngine(t, &partial)
	after := budget.Stats()
	if after.PeakBytes < before.PeakBytes {
		t.Fatal("source refusal reduced monotone peak", before, after)
	}
	before.PeakBytes = after.PeakBytes
	if before != after {
		t.Fatal("source refusal left partial charge", before, after)
	}
	if row, ok := first.catalog.pending.Get([]byte(systemCollectionMetaPrefix + "one")); !ok || row.Value != "old" {
		t.Fatal("source refusal changed captured metadata", row, ok)
	}
	budget.Close()
	maintenance.Close()
	nativeTestFinishEngine(t, &base)
	nativeTestFinishEngine(t, &active)
	if budget.Stats().Sources != 1 {
		t.Fatal("old pre-rotation native view lost source", budget.Stats())
	}
	nativeTestFinishEngine(t, &first)
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeEngineCatalogBuildFailureKeepsOriginalPartialAfterClose(t *testing.T) {
	base, budget, maintenance := nativeTestCatalogEngine(t, 2)
	rows := make([]memtable.COWMutation, nativeBuildBatchEntries+1)
	for i := range rows {
		rows[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%s%04d", systemCollectionMetaPrefix, i)), Value: []byte("captured")}
	}
	captured, err := prepareNativeEngineCatalogUpdate(base, rows, 3)
	rows = nil
	if err != nil {
		t.Fatal(err)
	}
	build, err := prepareNativeEngineBuild(captured)
	if err != nil {
		t.Fatal(err)
	}
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
	failure := errors.New("original second system chunk failure")
	control := &nativeDomainBuildControl{group: group, failAt: 2, failure: failure}
	if err := build.materializeDomains(control); err != failure || build.catalog.failure != failure || group.Accepted() {
		t.Fatal("original catalog failure/partial was not retained", err)
	}
	before := budget.Stats()
	if err := build.materializeDomains(control); err != failure || control.calls != 2 || budget.Stats() != before {
		t.Fatal("failed original catalog replayed", err)
	}
	work := captured.Release()
	if _, err := work.Finish(); err != failure || captured.build != build || captured.catalog == nil || captured.collections == nil {
		t.Fatal("failed system build lost original source/directory custody", err)
	}
	budget.Close()
	maintenance.Close()
	if budget.Stats() != before {
		t.Fatal("Close refunded exact original failed system build", before, budget.Stats())
	}
	// Deliberately preserve this original terminal Work; no alternate cleanup.
	_ = base
}
