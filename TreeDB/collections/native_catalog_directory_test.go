package collections

import (
	"errors"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeTestEmptyCollection(t *testing.T, budget *memtable.COWBudget, id uint64, name string, old *nativeCollectionState) *nativeCollectionState {
	t.Helper()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema, err := newNativeSchemaState(&CollectionMeta{Name: name}, admit)
	if err != nil {
		t.Fatal(err)
	}
	var domains, pending *cowbtree.OwnedMap[string, *nativeDomainState]
	if old == nil {
		domains = nativeTestDomainDirectory(t, nil, nil, admit)
		pending = nativeTestDomainDirectory(t, nil, nil, admit)
	} else {
		domains, pending = old.domains, old.pending
		domains.Retain()
		pending.Retain()
	}
	c, err := newNativeCollectionState(id, schema, domains, pending, admit)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func nativeTestCatalogReplacementEffects(t *testing.T, old, replacement *nativeCollectionState, next uint64) []memtable.COWMutation {
	t.Helper()
	counter, err := nativeCatalogIdentityRecord(next, true)
	if err != nil {
		t.Fatal(err)
	}
	effects := []memtable.COWMutation{{Key: []byte(nativeCatalogNextIncarnationKey), Value: counter[:], Flags: node.FlagInline}}
	if old != nil && (replacement == nil || old.schema.meta.Name != replacement.schema.meta.Name) {
		for _, prefix := range []string{systemCollectionMetaPrefix, nativeCatalogIncarnationPrefix} {
			effects = append(effects, memtable.COWMutation{Key: []byte(prefix + old.schema.meta.Name), Flags: node.FlagTombstone})
		}
	}
	if replacement != nil {
		identity, err := nativeCatalogIdentityRecord(replacement.incarnation, false)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := encodeCollectionMeta(*replacement.schema.meta)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects,
			memtable.COWMutation{Key: []byte(nativeCatalogIncarnationPrefix + replacement.schema.meta.Name), Value: identity[:], Flags: node.FlagInline},
			memtable.COWMutation{Key: []byte(systemCollectionMetaPrefix + replacement.schema.meta.Name), Value: meta, Flags: node.FlagInline})
	}
	sort.Slice(effects, func(i, j int) bool { return string(effects[i].Key) < string(effects[j].Key) })
	return effects
}

func nativeTestReleaseCollection(t *testing.T, slot **nativeCollectionState) {
	t.Helper()
	done, err := (nativeCollectionLifetime{}).Release(*slot)
	*slot = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
}

func TestNativeEngineCatalogDirectoryDropRecreateAndRename(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := budget.ReserveMaintenance(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := newNativeEmptyEngine(budget, maintenance)
	if err != nil || empty.nextIncarnation != 1 || empty.names.Len() != 0 || budget.Stats().Sources != 0 {
		t.Fatal("empty catalog invented identity or physical source", err, budget.Stats())
	}
	c := nativeTestEmptyCollection(t, budget, 1, "original", nil)
	created, err := prepareNativeCatalogReplacement(empty, nil, c, nativeTestCatalogReplacementEffects(t, nil, c, 2))
	nativeTestReleaseCollection(t, &c)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := created.names.Get("original")
	c = nativeTestEmptyCollection(t, budget, 1, "renamed", old)
	renamed, err := prepareNativeCatalogReplacement(created, old, c, nativeTestCatalogReplacementEffects(t, old, c, 2))
	nativeTestReleaseCollection(t, &c)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := renamed.names.Get("original"); exists {
		t.Fatal("rename kept old catalog name")
	}
	rename, _ := renamed.names.Get("renamed")
	if rename.incarnation != old.incarnation || renamed.nextIncarnation != 2 || rename.domains != old.domains {
		t.Fatal("rename replaced logical incarnation or shared domain directory")
	}
	dropped, err := prepareNativeCatalogReplacement(renamed, rename, nil, nativeTestCatalogReplacementEffects(t, rename, nil, 2))
	if err != nil || dropped.names.Len() != 0 || dropped.collections.Len() != 0 || dropped.pending.Len() != 0 || dropped.nextIncarnation != 2 {
		t.Fatal("drop kept catalog authority or reused counter", err)
	}
	c = nativeTestEmptyCollection(t, budget, 2, "original", nil)
	recreated, err := prepareNativeCatalogReplacement(dropped, nil, c, nativeTestCatalogReplacementEffects(t, nil, c, 3))
	nativeTestReleaseCollection(t, &c)
	if err != nil || recreated.nextIncarnation != 3 {
		t.Fatal(err)
	}
	current, _ := recreated.names.Get("original")
	if current.incarnation != 2 || old.incarnation != 1 || created.nextIncarnation != 2 || budget.Stats().Sources != 1 {
		t.Fatal("recreation changed old view or escaped original generation bound", budget.Stats())
	}
	if original, _ := created.names.Get("original"); original != old {
		t.Fatal("old catalog view changed after drop/recreate")
	}
	old, rename, current = nil, nil, nil
	build, err := prepareNativeEngineBuild(recreated)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	database, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, ResolvedProfile: backenddb.ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	basis := database.AcquireSnapshot()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	if err := build.materializeDomains(group); err != nil {
		t.Fatal(err)
	}
	final := database.NewPhysicalBatch().(*backenddb.Batch)
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	if err := final.WriteSync(); err != nil {
		t.Fatal(err)
	}
	final.Close()
	final = nil
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	group = nil
	basis.Close()
	basis = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, ResolvedProfile: backenddb.ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	reopened := database.AcquireSnapshot()
	state, ok := reopened.StateToken()
	if !ok {
		t.Fatal("missing reopened complete physical state")
	}
	for _, check := range []struct {
		key     string
		counter bool
		value   uint64
	}{{nativeCatalogNextIncarnationKey, true, 3}, {nativeCatalogIncarnationPrefix + "original", false, 2}} {
		entry, err := reopened.GetEntryAtRoot(state.SystemRootPageID, []byte(check.key))
		if err != nil {
			t.Fatal(err)
		}
		if value, err := nativeCatalogIdentityValue(entry.Value, check.counter); err != nil || value != check.value {
			t.Fatal("reopen reconstructed identity from wrong authority", check, value, err)
		}
	}
	for _, prefix := range []string{systemCollectionMetaPrefix, nativeCatalogIncarnationPrefix} {
		entry, err := reopened.GetEntryAtRoot(state.SystemRootPageID, []byte(prefix+"renamed"))
		if err == nil && entry.Flags&node.FlagTombstone == 0 {
			t.Fatal("reopen resurrected dropped renamed identity", prefix, entry)
		}
	}
	reopened.Close()
	reopened = nil
	build = nil
	for _, slot := range []**nativeEngineVersion{&empty, &created, &renamed, &dropped, &recreated} {
		nativeTestFinishEngine(t, slot)
	}
	budget.Close()
	maintenance.Close()
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}

func TestNativeEngineCatalogDirectoryRefusesWrongIdentityBeforeAdmission(t *testing.T) {
	base, budget, maintenance := nativeTestCatalogEngine(t, 2)
	old, _ := base.names.Get("collection")
	replacement := nativeTestEmptyCollection(t, budget, 1, "renamed", old)
	effects := nativeTestCatalogReplacementEffects(t, old, replacement, base.nextIncarnation)
	for i := range effects {
		if nativeCatalogNameKey(effects[i].Key, nativeCatalogIncarnationPrefix, "renamed") {
			wrong, _ := nativeCatalogIdentityRecord(99, false)
			effects[i].Value = wrong[:]
		}
	}
	before := budget.Stats()
	if next, err := prepareNativeCatalogReplacement(base, old, replacement, effects); next != nil || !errors.Is(err, errNativeCatalogIdentity) || before != budget.Stats() {
		t.Fatal("inconsistent identity reached admission", err, before, budget.Stats())
	}
	old = nil
	nativeTestReleaseCollection(t, &replacement)
	nativeTestFinishEngine(t, &base)
	budget.Close()
	maintenance.Close()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeEngineCatalogIdentityRecordRejectsMalformedAndWrongKind(t *testing.T) {
	if _, err := nativeCatalogIdentityRecord(0, false); err == nil {
		t.Fatal("zero incarnation was encoded")
	}
	for _, counter := range []bool{false, true} {
		record, err := nativeCatalogIdentityRecord(^uint64(0), counter)
		if err != nil {
			t.Fatal(err)
		}
		if value, err := nativeCatalogIdentityValue(record[:], counter); err != nil || value != ^uint64(0) {
			t.Fatal(value, err)
		}
		if _, err := nativeCatalogIdentityValue(record[:], !counter); err == nil {
			t.Fatal("counter and incarnation confused")
		}
		for _, offset := range []int{0, 5, 6, 7} {
			bad := record
			bad[offset]++
			if _, err := nativeCatalogIdentityValue(bad[:], counter); err == nil {
				t.Fatal("unknown identity extension accepted", offset)
			}
		}
		for n := 0; n < len(record); n++ {
			if _, err := nativeCatalogIdentityValue(record[:n], counter); err == nil {
				t.Fatal("truncated identity accepted", n)
			}
		}
	}
}

func TestNativeEngineCatalogDirectoryPartialOwnsEdgesAfterOpeningRelease(t *testing.T) {
	completed := false
	for stop := 0; stop < 20; stop++ {
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
		domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("captured")}}, budget)
		if err != nil {
			t.Fatal(err)
		}
		base := nativeTestEngine(t, budget, nil, domain, schema)
		base.maintenance = maintenance
		domain, schema = nil, nil
		old, _ := base.names.Get("collection")
		replacement := nativeTestEmptyCollection(t, budget, old.incarnation, "renamed", old)
		effects := nativeTestCatalogReplacementEffects(t, old, replacement, base.nextIncarnation)
		refusal := errors.New("original catalog allocation refused")
		control := &nativeRolloverAdmissionControl{COWBudget: budget, remaining: stop, err: refusal}
		base.allocator = control // Configure the private fixture before capture.
		next, err := prepareNativeCatalogReplacement(base, old, replacement, effects)
		if err != nil && err != refusal {
			t.Fatal(stop, err)
		}
		if stop == 0 && next != nil {
			t.Fatal("preallocation refusal returned an owner")
		}
		old = nil
		nativeTestReleaseCollection(t, &replacement)
		nativeTestFinishEngine(t, &base)
		// Incomplete map builders must have sealed all borrowed child/value
		// edges before returning. No opening engine remains to keep them live.
		budget.Close()
		maintenance.Close()
		if next != nil {
			if err == nil {
				if c, ok := next.names.Get("renamed"); !ok || c.incarnation != 1 || c.domains.Len() == 0 {
					t.Fatal("complete directory lost exact shared domains", c, ok)
				}
				completed = true
			}
			nativeTestFinishEngine(t, &next)
		}
		if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
			t.Fatal(stop, budget.Stats(), maintenance.Stats())
		}
		if completed {
			break
		}
	}
	if !completed {
		t.Fatal("fixture never reached complete replacement")
	}
}

func TestNativeEngineCatalogDirectoryCounterExhaustionBeforeAdmission(t *testing.T) {
	base, budget, maintenance := nativeTestCatalogEngine(t, 2)
	base.nextIncarnation = ^uint64(0) // Private fixture represents exhausted persisted counter.
	replacement := nativeTestEmptyCollection(t, budget, ^uint64(0), "new", nil)
	effects := nativeTestCatalogReplacementEffects(t, nil, replacement, ^uint64(0))
	before := budget.Stats()
	if next, err := prepareNativeCatalogReplacement(base, nil, replacement, effects); next != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || before != budget.Stats() {
		t.Fatal("exhausted incarnation counter reached admission", next, err, before, budget.Stats())
	}
	nativeTestReleaseCollection(t, &replacement)
	nativeTestFinishEngine(t, &base)
	budget.Close()
	maintenance.Close()
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}
