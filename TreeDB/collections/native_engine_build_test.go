package collections

import (
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"testing"
	"unsafe"
)

func nativeTestBuildEngine(t *testing.T, rows int) (*nativeEngineVersion, *memtable.COWBudget, *memtable.COWBudget) {
	t.Helper()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
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
	entries := make([]memtable.COWMutation, rows)
	for i := range entries {
		entries[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%04d", i)), Value: []byte("captured"), Flags: node.FlagInline, Revision: 17}
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, entries, budget)
	if err != nil {
		t.Fatal(err)
	}
	engine := nativeTestEngine(t, budget, nil, domain, schema)
	domain, schema, entries = nil, nil, nil
	engine.maintenance = maintenance
	return engine, budget, maintenance
}

func TestNativeEngineBuildOwnsSparseWorkAndBothSuccessors(t *testing.T) {
	captured, budget, maintenance := nativeTestBuildEngine(t, 600)
	c, _ := captured.collections.Get(1)
	secondary, err := prepareNativeDomainUpdate(nil, "secondary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("index"), Value: []byte("0000"), Flags: node.FlagInline, Revision: 17}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	withSecondary := nativeTestEngine(t, budget, captured, secondary, c.schema)
	secondary = nil
	c = nil
	withSecondary.maintenance = maintenance
	nativeTestFinishEngine(t, &captured)
	captured = withSecondary
	withSecondary = nil
	metadata, err := encodeCollectionMeta(CollectionMeta{Name: "collection"})
	if err != nil {
		t.Fatal(err)
	}
	withCatalog, err := prepareNativeEngineCatalogUpdate(captured, []memtable.COWMutation{{Key: []byte(systemCollectionMetaKey("collection")), Value: metadata, Flags: node.FlagInline, Revision: 17}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	metadata = nil
	nativeTestFinishEngine(t, &captured)
	captured = withCatalog
	withCatalog = nil
	active, err := prepareNativeEngineRollover(captured)
	if err != nil {
		t.Fatal(err)
	}
	c, _ = active.collections.Get(1)
	d, _ := c.domains.Get("primary")
	suffix, err := prepareNativeDomainUpdate(d, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("0000"), Value: []byte("newer"), Flags: node.FlagInline, Revision: 18}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	latest := nativeTestEngine(t, budget, active, suffix, c.schema)
	latest.maintenance = maintenance
	suffix, c, d = nil, nil, nil
	build, err := prepareNativeEngineBuild(captured)
	if err != nil || build != captured.build || len(build.domains) != 2 || len(build.built) != 2 {
		t.Fatal("original captured state did not own the sparse build", err)
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
	if err := build.materializeDomains(control); err != nil || control.calls != 6 || !build.complete || before != budget.Stats() {
		t.Fatal("physical domain build exceeded its original reservation", err, control.calls, before, budget.Stats())
	}
	// Catalog metadata and physical root descriptors were built in that SAME
	// group; only this final physical batch installs the complete result.
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
		t.Fatal("physical group did not accept both native roots")
	}
	accepted := database.AcquireSnapshot()
	defer accepted.Close()
	state, ok := accepted.StateToken()
	if !ok {
		t.Fatal("missing exact accepted state")
	}
	metaEntry, err := accepted.GetEntryAtRoot(state.SystemRootPageID, []byte(systemCollectionMetaKey("collection")))
	if err != nil || metaEntry.Revision != 17 {
		t.Fatal("same group lost captured catalog metadata", metaEntry, err)
	}
	if meta, err := decodeCollectionMeta(metaEntry.Value); err != nil || meta.Name != "collection" {
		t.Fatal("captured schema was not accepted with its physical roots", meta, err)
	}
	for _, result := range build.built {
		rootEntry, err := accepted.GetEntryAtRoot(state.SystemRootPageID, []byte(systemCollectionRootKey("collection/"+result.name)))
		if err != nil {
			t.Fatal(err)
		}
		if root, err := decodeRootID(rootEntry.Value); err != nil || root != result.root {
			t.Fatal("physical root descriptor not installed with schema", result.name, root, err)
		}
		key, value := "0000", "captured"
		if result.name == "secondary" {
			key, value = "index", "0000"
		}
		entry, err := accepted.GetEntryAtRoot(result.root, []byte(key))
		if err != nil || string(entry.Value) != value || entry.Revision != 17 {
			t.Fatal("accepted root did not preserve the captured source", result.name, entry, err)
		}
	}
	nextLatest, err := build.prepareHandoff(latest)
	if err != nil {
		t.Fatal("first handoff exceeded original maintenance promise", err)
	}
	nextFrozen, err := build.prepareHandoff(captured)
	if err != nil {
		t.Fatal("second handoff exceeded original maintenance promise", err)
	}
	if extra, err := build.prepareHandoff(latest); extra != nil || err != backenddb.ErrNativeSemanticUnsupported {
		t.Fatal("original two-construction bound was not enforced", err)
	}
	c, _ = nextLatest.collections.Get(1)
	d, _ = c.domains.Get("primary")
	if row, ok := d.pending.Get([]byte("0000")); !ok || row.Value != "newer" || row.Revision != 18 || d.captured != nil {
		t.Fatal("handoff lost a newer pending effect", row, ok)
	}
	if nextFrozen.pending.Len() != 0 {
		t.Fatal("frozen successor retained physically materialized work")
	}
	build, c, d = nil, nil, nil
	filler.Close()
	filler = nil
	nativeTestFinishEngine(t, &captured)
	nativeTestFinishEngine(t, &active)
	nativeTestFinishEngine(t, &latest)
	if got := budget.Stats(); got.Sources != 1 {
		t.Fatal("original build ledger retained a consumed source", got)
	}
	nativeTestFinishEngine(t, &nextFrozen)
	nativeTestFinishEngine(t, &nextLatest)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}

func TestNativeEngineBuildUntouchedRefusalThenOriginalFailure(t *testing.T) {
	captured, budget, maintenance := nativeTestBuildEngine(t, nativeBuildBatchEntries+1)
	captured.maintenance = &nativeRolloverAdmissionControl{COWBudget: maintenance, remaining: 0, err: memtable.ErrCOWCapacity}
	before := budget.Stats()
	if build, err := prepareNativeEngineBuild(captured); build != nil || captured.build != nil || before != budget.Stats() {
		t.Fatal("untouched refusal changed the original captured state", err)
	} else if _, ok := err.(backenddb.NativeSemanticBuildRetryableRefusal); !ok {
		t.Fatal("untouched admission did not prove retry eligibility", err)
	}
	captured.maintenance = maintenance
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
	failure := errors.New("original engine second physical chunk failure")
	control := &nativeDomainBuildControl{group: group, failAt: 2, failure: failure}
	if err := build.materializeDomains(control); err != failure || build.domains[0].root == 0 {
		t.Fatal("partial original root/failure was lost", err)
	}
	before = budget.Stats()
	if same, err := prepareNativeEngineBuild(captured); same != build || err != failure || before != budget.Stats() {
		t.Fatal("captured build failure replaced its original ledger", err)
	}
	if err := build.materializeDomains(control); err != failure || control.calls != 2 {
		t.Fatal("terminal physical materialization replayed", err, control.calls)
	}
	if next, err := build.prepareHandoff(captured); next != nil || err != failure {
		t.Fatal("terminal original attempted handoff", err)
	}
	work := captured.Release()
	if _, err := work.Finish(); err != failure || captured.build != build || captured.names == nil || captured.collections == nil || captured.pending == nil {
		t.Fatal("failed original cleanup dropped borrowed directory edges", err)
	}
	budget.Close()
	maintenance.Close()
	if got := budget.Stats(); got.Sources != before.Sources || got.HistoryBytes != before.HistoryBytes || got.ExternalBytes != before.ExternalBytes {
		t.Fatal("Close refunded original terminal build custody", before, got)
	}
	// The original failed semantic Work remains parked. Do not replay cleanup
	// or invent an alternate test destructor for its exact retained resources.
}

func TestNativeEngineBuildCapacityRejectsOverflow(t *testing.T) {
	for _, input := range []struct {
		domains, sources int
		handoff          uint64
	}{{-1, 1, 0}, {1, 0, 0}, {int(^uint(0) >> 1), 1, 0}, {1, 1, ^uint64(0)}} {
		if total, floor, err := nativeEngineBuildCapacity(input.domains, input.sources, input.handoff); err == nil || total != 0 || floor != 0 {
			t.Fatal(input, total, floor, err)
		}
	}
}

func TestNativeEnginePartialOwnsSealedEdgesAfterOpeningClose(t *testing.T) {
	opening, budget, maintenance := nativeTestBuildEngine(t, 1)
	failure := errors.New("private collection-directory allocation failed")
	// Header + name directory succeed; collection-directory admission fails.
	// The original name root was borrowed until the deferred private Seal.
	opening.maintenance = &nativeRolloverAdmissionControl{COWBudget: maintenance, remaining: 2, err: failure}
	partial, err := prepareNativeEngineRollover(opening)
	if err != failure || partial == nil || partial.names == nil || partial.preparation.names != nil {
		t.Fatal("partial returned a borrowed builder instead of an owned root", err)
	}
	nativeTestFinishEngine(t, &opening)
	budget.Close()
	maintenance.Close()
	c, ok := partial.names.Get("collection")
	if !ok {
		t.Fatal("opening cleanup cleared the partial's original shared node")
	}
	d, ok := c.domains.Get("primary")
	if !ok {
		t.Fatal("opening cleanup cleared the partial's actual descriptor edge")
	}
	if row, ok := d.pending.Get([]byte("0000")); !ok || row.Value != "captured" || budget.Stats().Sources != 1 {
		t.Fatal("opening Close refunded the partial's original source", row, ok, budget.Stats())
	}
	c, d = nil, nil
	nativeTestFinishEngine(t, &partial)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}

func TestNativeEngineRootDescriptorFailureKeepsOriginalBuiltDomain(t *testing.T) {
	captured, budget, maintenance := nativeTestBuildEngine(t, 1)
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
	failure := errors.New("original root descriptor chunk failed")
	control := &nativeDomainBuildControl{group: group, failAt: 2, failure: failure}
	if err := build.materializeDomains(control); err != failure || !build.domains[0].complete || build.built[0].root == 0 || build.domains[0].delta.Len() != 0 || group.Accepted() {
		t.Fatal("late metadata failure lost original built root or exposed partial", err)
	}
	before := budget.Stats()
	if err := build.materializeDomains(control); err != failure || control.calls != 2 {
		t.Fatal("late metadata failure replayed original physical work", err)
	}
	work := captured.Release()
	if _, err := work.Finish(); err != failure || captured.build != build || build.built[0].rootKey == "" || captured.collections == nil {
		t.Fatal("late metadata failure lost original descriptor/source custody", err)
	}
	budget.Close()
	maintenance.Close()
	if got := budget.Stats(); got.ExternalBytes != before.ExternalBytes || got.HistoryBytes != before.HistoryBytes || got.Sources != before.Sources {
		t.Fatal("Close dropped original metadata failure custody", before, got)
	}
}
