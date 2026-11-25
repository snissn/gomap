package collections

import (
	"errors"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type countingFailNativeAllocator struct {
	backenddb.NativeSemanticAllocator
	calls  atomic.Int64
	failAt int64
}

func (a *countingFailNativeAllocator) AdmitNativeBytes(n uint64) (*memtable.COWExternalLease, error) {
	call := a.calls.Add(1)
	if a.failAt > 0 && call == a.failAt {
		return nil, memtable.ErrCOWCapacity
	}
	return a.NativeSemanticAllocator.AdmitNativeBytes(n)
}

func openNativePrepareSingleFixture(tb testing.TB, collections []CollectionMeta) (*treedb.DB, *CollectionManager, []*Collection) {
	tb.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, tb.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var budget nativeHNSWAdmissionBudget
	tb.Cleanup(func() {
		if budget == nil {
			return
		}
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			tb.Errorf("COW budget leaked after Close: %+v", stats)
		}
	})
	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := owner.Close(); err != nil {
			tb.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		tb.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	tb.Cleanup(func() {
		if err := manager.Close(); err != nil {
			tb.Error(err)
		}
	})
	cols := make([]*Collection, len(collections))
	for i := range collections {
		meta := collections[i]
		if _, err := manager.CreateCollection(&meta); err != nil {
			tb.Fatal(err)
		}
		col, err := manager.OpenCollection(meta.Name)
		if err != nil {
			tb.Fatal(err)
		}
		cols[i] = col
	}
	if len(cols) != 0 {
		probe, err := cols[0].nativeJSON.Capture(cols[0].name)
		if err != nil {
			tb.Fatal(err)
		}
		budget = probe.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
		if err := probe.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return owner, manager, cols
}

func assertBudgetNoGrowth(t *testing.T, label string, before, after memtable.COWStats) {
	t.Helper()
	if after.ReservedBytes > before.ReservedBytes || after.ExternalBytes > before.ExternalBytes || after.ExternalLeases > before.ExternalLeases || after.Sources > before.Sources {
		t.Fatalf("%s leaked COW budget: before=%+v after=%+v", label, before, after)
	}
}

func shallowEngineWithAllocator(base *nativeEngineVersion, alloc backenddb.NativeSemanticAllocator) *nativeEngineVersion {
	return &nativeEngineVersion{
		names:           base.names,
		collections:     base.collections,
		pending:         base.pending,
		catalog:         base.catalog,
		inventory:       base.inventory,
		nextIncarnation: base.nextIncarnation,
		allocator:       alloc,
		maintenance:     base.maintenance,
	}
}

func assertPrepareUnwindAtEveryAdmit(t *testing.T, base *nativeEngineVersion, budget nativeHNSWAdmissionBudget, changes []nativeJSONPrimaryChange, named []nativeNamedCollectionChange, minAdmitCalls int64) {
	t.Helper()

	// Measure total AdmitNativeBytes calls on a clean run.
	probeAlloc := &countingFailNativeAllocator{NativeSemanticAllocator: base.allocator}
	probeBase := shallowEngineWithAllocator(base, probeAlloc)
	beforeClean := budget.Stats()
	cleanNext, err := prepareNativeCollectionChanges(probeBase, changes, named, false)
	if err != nil {
		t.Fatalf("clean prepareNativeCollectionChanges failed: %v", err)
	}
	if err := finishPrivateNative(cleanNext.Release()); err != nil {
		t.Fatalf("clean finishPrivateNative failed: %v", err)
	}
	assertBudgetNoGrowth(t, "clean run", beforeClean, budget.Stats())
	totalCalls := probeAlloc.calls.Load()
	if totalCalls < minAdmitCalls {
		t.Fatalf("expected at least %d AdmitNativeBytes calls, got %d", minAdmitCalls, totalCalls)
	}

	// Inject ErrCOWCapacity at every single AdmitNativeBytes site.
	for failAt := int64(1); failAt <= totalCalls; failAt++ {
		alloc := &countingFailNativeAllocator{NativeSemanticAllocator: base.allocator, failAt: failAt}
		failBase := shallowEngineWithAllocator(base, alloc)
		before := budget.Stats()
		next, err := prepareNativeCollectionChanges(failBase, changes, named, false)
		if !errors.Is(err, memtable.ErrCOWCapacity) {
			if next != nil {
				_ = finishPrivateNative(next.Release())
			}
			t.Fatalf("failAt=%d/%d: err=%v, want ErrCOWCapacity", failAt, totalCalls, err)
		}
		if next != nil {
			if ferr := finishPrivateNative(next.Release()); ferr != nil {
				t.Fatalf("failAt=%d/%d: finishPrivateNative failed: %v", failAt, totalCalls, ferr)
			}
		}
		assertBudgetNoGrowth(t, "unwind", before, budget.Stats())
	}
}

func TestPrepareNativeCollectionChangesSingleFastPathUnwindAndParity(t *testing.T) {
	t.Run("SingleCollectionThreeDomainsNonSharedUnwind", func(t *testing.T) {
		owner, _, cols := openNativePrepareSingleFixture(t, []CollectionMeta{
			{
				Name:    "docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
					{Name: "idx_age", Field: "age", ValueType: IndexValueBSONOrderedV2},
				},
			},
		})
		col := cols[0]
		doc0, err := bson.Marshal(bson.D{
			{Key: "email", Value: "a@example.com"},
			{Key: "age", Value: int32(30)},
			{Key: "score", Value: int32(1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.Insert([]byte("doc-0"), doc0); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		// One non-indexed update leaves c.domains (len=3) and c.pending (len=1, "primary").
		if matched, modified, err := col.UpdateBSONSet([]byte("doc-0"), []BSONSetField{bsonSetInt32Field(t, "score", 2)}); err != nil || !matched || !modified {
			t.Fatalf("seed update failed: matched=%t modified=%t err=%v", matched, modified, err)
		}

		updatedDoc, err := bson.Marshal(bson.D{
			{Key: "email", Value: "a@example.com"},
			{Key: "age", Value: int32(30)},
			{Key: "score", Value: int32(3)},
		})
		if err != nil {
			t.Fatal(err)
		}

		probe, err := col.nativeJSON.Capture(col.name)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := probe.Close(); err != nil {
				t.Error(err)
			}
		}()
		base := probe.capture.State().(*nativeEngineVersion)
		budget := probe.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
		if probe.collection.domains.Len() != 3 || probe.collection.pending.Len() != 1 {
			t.Fatalf("unexpected domain counts: domains=%d pending=%d", probe.collection.domains.Len(), probe.collection.pending.Len())
		}

		changes := []nativeJSONPrimaryChange{
			{
				collection: probe.collection,
				entries: []memtable.COWMutation{
					{Key: []byte("doc-0"), Value: updatedDoc, Flags: node.FlagInline},
				},
			},
		}

		// Verify all 7 AdmitNativeBytes sites unwind cleanly:
		// 1: newNativeEnginePreparation, 2: newNativeDomainState,
		// 3: domains, 4: pendingDomains, 5: newNativeCollectionState,
		// 6: names, 7: collections (sharePending=true shares pending with collections).
		assertPrepareUnwindAtEveryAdmit(t, base, budget, changes, nil, 7)

		// Verify prepared successor contents.
		before := budget.Stats()
		next, err := prepareNativeCollectionChanges(base, changes, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if next.pending != next.collections {
			t.Fatal("expected single-collection sharePending to share pending with collections")
		}
		gotByName, ok := next.names.Get("docs")
		if !ok {
			t.Fatal("missing collection in next.names")
		}
		gotByID, ok := next.collections.Get(probe.collection.incarnation)
		if !ok || gotByID != gotByName {
			t.Fatal("mismatch between next.names and next.collections")
		}
		if gotByID.documentGeneration != probe.collection.documentGeneration+1 {
			t.Fatalf("documentGeneration=%d want %d", gotByID.documentGeneration, probe.collection.documentGeneration+1)
		}
		if gotByID.domains.Len() != 3 || gotByID.pending.Len() != 1 {
			t.Fatalf("unexpected replacement domain counts: domains=%d pending=%d", gotByID.domains.Len(), gotByID.pending.Len())
		}
		if gotByID.domains == gotByID.pending {
			t.Fatal("expected non-shared domains and pending when domains=3 and pending=1")
		}
		primDom, ok := gotByID.domains.Get("primary")
		if !ok || primDom == nil {
			t.Fatal("missing primary domain")
		}
		pendDom, ok := gotByID.pending.Get("primary")
		if !ok || pendDom != primDom {
			t.Fatal("primary domain mismatch between domains and pending")
		}
		row, ok := primDom.pending.Get([]byte("doc-0"))
		if !ok || string(row.Value) != string(updatedDoc) {
			t.Fatal("updated document not found in prepared primary domain")
		}
		if err := finishPrivateNative(next.Release()); err != nil {
			t.Fatal(err)
		}
		assertBudgetNoGrowth(t, "single-collection three-domains", before, budget.Stats())
	})

	t.Run("SingleUpdateOnTwoCollectionDBNonSharedPendingUnwind", func(t *testing.T) {
		owner, _, cols := openNativePrepareSingleFixture(t, []CollectionMeta{
			{
				Name:    "c1",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "idx_k", Field: "k", ValueType: IndexValueBSONOrderedV2},
				},
			},
			{
				Name:    "c2",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			},
		})
		doc, err := bson.Marshal(bson.D{{Key: "k", Value: "v1"}, {Key: "n", Value: int32(1)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cols[0].Insert([]byte("d0"), doc); err != nil {
			t.Fatal(err)
		}
		if _, err := cols[1].Insert([]byte("d0"), doc); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		// Update c1 once so base.pending has 1 collection while base.collections has 2 (!sharePending).
		if matched, modified, err := cols[0].UpdateBSONSet([]byte("d0"), []BSONSetField{bsonSetInt32Field(t, "n", 2)}); err != nil || !matched || !modified {
			t.Fatalf("seed update c1 failed: matched=%t modified=%t err=%v", matched, modified, err)
		}

		updatedDoc, err := bson.Marshal(bson.D{{Key: "k", Value: "v1"}, {Key: "n", Value: int32(3)}})
		if err != nil {
			t.Fatal(err)
		}
		probe, err := cols[0].nativeJSON.Capture("c1")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := probe.Close(); err != nil {
				t.Error(err)
			}
		}()
		base := probe.capture.State().(*nativeEngineVersion)
		budget := probe.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
		if base.collections.Len() != 2 || base.pending.Len() != 1 || base.pending == base.collections {
			t.Fatalf("expected 2 collections and 1 non-shared pending: collections=%d pending=%d", base.collections.Len(), base.pending.Len())
		}

		changes := []nativeJSONPrimaryChange{
			{
				collection: probe.collection,
				entries: []memtable.COWMutation{
					{Key: []byte("d0"), Value: updatedDoc, Flags: node.FlagInline},
				},
			},
		}

		// Hits all 8 AdmitNativeBytes sites including distinct base.pending.SetSingle:
		// 1: prep, 2: domainState, 3: domains, 4: pendingDomains,
		// 5: collectionState, 6: names, 7: collections, 8: pending.
		assertPrepareUnwindAtEveryAdmit(t, base, budget, changes, nil, 8)

		before := budget.Stats()
		next, err := prepareNativeCollectionChanges(base, changes, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if next.pending == next.collections {
			t.Fatal("expected distinct pending map when 1 of 2 collections is pending")
		}
		if next.collections.Len() != 2 || next.names.Len() != 2 || next.pending.Len() != 1 {
			t.Fatalf("unexpected map lens: names=%d collections=%d pending=%d", next.names.Len(), next.collections.Len(), next.pending.Len())
		}
		if err := finishPrivateNative(next.Release()); err != nil {
			t.Fatal(err)
		}
		assertBudgetNoGrowth(t, "single-collection three-domains", before, budget.Stats())
	})

	t.Run("SingleCollectionSingleDomainSharedUnwind", func(t *testing.T) {
		owner, _, cols := openNativePrepareSingleFixture(t, []CollectionMeta{
			{
				Name:    "plain",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			},
		})
		doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cols[0].Insert([]byte("d0"), doc); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		updatedDoc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
		if err != nil {
			t.Fatal(err)
		}
		probe, err := cols[0].nativeJSON.Capture("plain")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := probe.Close(); err != nil {
				t.Error(err)
			}
		}()
		base := probe.capture.State().(*nativeEngineVersion)
		budget := probe.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
		changes := []nativeJSONPrimaryChange{
			{
				collection: probe.collection,
				entries: []memtable.COWMutation{
					{Key: []byte("d0"), Value: updatedDoc, Flags: node.FlagInline},
				},
			},
		}
		// Post-checkpoint: shareDomains=true and sharePending=true (6 admit calls).
		assertPrepareUnwindAtEveryAdmit(t, base, budget, changes, nil, 6)

		before := budget.Stats()
		next, err := prepareNativeCollectionChanges(base, changes, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := next.collections.Get(probe.collection.incarnation)
		if !ok || got.domains != got.pending || next.collections != next.pending {
			t.Fatal("expected shared domains and shared pending on 1-collection 1-domain update")
		}
		if err := finishPrivateNative(next.Release()); err != nil {
			t.Fatal(err)
		}
		assertBudgetNoGrowth(t, "single-collection three-domains", before, budget.Stats())
	})

	t.Run("MultiDomainMultiCollectionAndNamedDomainParity", func(t *testing.T) {
		owner, _, cols := openNativePrepareSingleFixture(t, []CollectionMeta{
			{
				Name:    "alpha",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "idx_a", Field: "a", ValueType: IndexValueBSONOrderedV2},
					{Name: "idx_b", Field: "b", ValueType: IndexValueBSONOrderedV2},
				},
			},
			{
				Name:    "beta",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			},
		})
		doc, err := bson.Marshal(bson.D{{Key: "a", Value: "x"}, {Key: "b", Value: int32(1)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cols[0].Insert([]byte("d0"), doc); err != nil {
			t.Fatal(err)
		}
		if _, err := cols[1].Insert([]byte("d0"), doc); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}

		probeA, err := cols[0].nativeJSON.Capture("alpha")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := probeA.Close(); err != nil {
				t.Error(err)
			}
		}()
		base := probeA.capture.State().(*nativeEngineVersion)
		budget := probeA.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
		colA, ok := base.names.Get("alpha")
		if !ok {
			t.Fatal("missing alpha")
		}
		colB, ok := base.names.Get("beta")
		if !ok {
			t.Fatal("missing beta")
		}

		// 1. Multi-domain single-collection change (count == 1, domainCount == 3).
		var extraDomains []nativeJSONDomainChange
		colA.domains.Scan(func(name string, d *nativeDomainState) bool {
			if name != "primary" {
				extraDomains = append(extraDomains, nativeJSONDomainChange{
					name:   name,
					policy: d.storagePolicy,
					entries: []memtable.COWMutation{
						{Key: []byte("k1"), Value: []byte("v1"), Flags: node.FlagInline},
					},
				})
			}
			return true
		})
		if len(extraDomains) != 2 {
			t.Fatalf("expected 2 secondary domains, got %d", len(extraDomains))
		}
		multiDomChanges := []nativeJSONPrimaryChange{
			{
				collection: colA,
				entries:    []memtable.COWMutation{{Key: []byte("d0"), Value: doc, Flags: node.FlagInline}},
				additional: extraDomains,
			},
		}
		assertPrepareUnwindAtEveryAdmit(t, base, budget, multiDomChanges, nil, 7)

		before := budget.Stats()
		nextMultiDom, err := prepareNativeCollectionChanges(base, multiDomChanges, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		gotA, ok := nextMultiDom.collections.Get(colA.incarnation)
		if !ok || gotA.domains.Len() != 3 || gotA.pending.Len() != 3 {
			t.Fatalf("unexpected multi-domain result: ok=%t domains=%d pending=%d", ok, gotA.domains.Len(), gotA.pending.Len())
		}
		if err := finishPrivateNative(nextMultiDom.Release()); err != nil {
			t.Fatal(err)
		}
		if after := budget.Stats(); after != before {
			t.Fatalf("multi-domain COW budget leaked: before=%+v after=%+v", before, after)
		}

		// 2. Multi-collection change (count == 2, domainCount == 1 each).
		multiColChanges := []nativeJSONPrimaryChange{
			{
				collection: colA,
				entries:    []memtable.COWMutation{{Key: []byte("d0"), Value: doc, Flags: node.FlagInline}},
			},
			{
				collection: colB,
				entries:    []memtable.COWMutation{{Key: []byte("d0"), Value: doc, Flags: node.FlagInline}},
			},
		}
		assertPrepareUnwindAtEveryAdmit(t, base, budget, multiColChanges, nil, 8)

		before = budget.Stats()
		nextMultiCol, err := prepareNativeCollectionChanges(base, multiColChanges, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if nextMultiCol.collections.Len() != 2 || nextMultiCol.pending.Len() != 2 {
			t.Fatalf("unexpected multi-collection counts: collections=%d pending=%d", nextMultiCol.collections.Len(), nextMultiCol.pending.Len())
		}
		if err := finishPrivateNative(nextMultiCol.Release()); err != nil {
			t.Fatal(err)
		}
		if after := budget.Stats(); after != before {
			t.Fatalf("multi-collection COW budget leaked: before=%+v after=%+v", before, after)
		}

		// 3. Single named-domain change (count == 1, domainCount == 1, !primary).
		namedChanges := []nativeNamedCollectionChange{
			{
				collection:       colA,
				sourceGeneration: colA.documentGeneration,
				domains: []nativeNamedDomainChange{
					{
						name:                   "custom_aux",
						policy:                 backenddb.OrderedRootStorageDefault,
						entries:                []memtable.COWMutation{{Key: []byte("aux-k"), Value: []byte("aux-v"), Flags: node.FlagInline}},
						replace:                true,
						expectedInterpretation: 0,
					},
				},
			},
		}
		assertPrepareUnwindAtEveryAdmit(t, base, budget, nil, namedChanges, 7)

		before = budget.Stats()
		nextNamed, err := prepareNativeCollectionChanges(base, nil, namedChanges, false)
		if err != nil {
			t.Fatal(err)
		}
		gotNamedA, ok := nextNamed.collections.Get(colA.incarnation)
		if !ok {
			t.Fatal("missing alpha after named domain change")
		}
		if gotNamedA.documentGeneration != colA.documentGeneration {
			t.Fatalf("named change mutated documentGeneration: got %d want %d", gotNamedA.documentGeneration, colA.documentGeneration)
		}
		auxDom, ok := gotNamedA.domains.Get("custom_aux")
		if !ok || auxDom == nil || auxDom.sourceGeneration != colA.documentGeneration {
			t.Fatalf("unexpected custom_aux domain: ok=%t dom=%v", ok, auxDom)
		}
		if err := finishPrivateNative(nextNamed.Release()); err != nil {
			t.Fatal(err)
		}
		if after := budget.Stats(); after != before {
			t.Fatalf("named-domain COW budget leaked: before=%+v after=%+v", before, after)
		}

		// 4. Error paths on count == 1 (nil collection, stale collection).
		before = budget.Stats()
		if badNext, err := prepareNativeCollectionChanges(base, []nativeJSONPrimaryChange{{collection: nil}}, nil, false); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			if badNext != nil {
				_ = finishPrivateNative(badNext.Release())
			}
			t.Fatalf("nil collection err=%v, want ErrNativeSemanticUnsupported", err)
		} else if badNext != nil {
			if ferr := finishPrivateNative(badNext.Release()); ferr != nil {
				t.Fatal(ferr)
			}
		}
		staleCol := &nativeCollectionState{incarnation: colA.incarnation, schema: colA.schema}
		if badNext, err := prepareNativeCollectionChanges(base, []nativeJSONPrimaryChange{{collection: staleCol}}, nil, false); !errors.Is(err, backenddb.ErrConcurrentModification) {
			if badNext != nil {
				_ = finishPrivateNative(badNext.Release())
			}
			t.Fatalf("stale collection err=%v, want ErrConcurrentModification", err)
		} else if badNext != nil {
			if ferr := finishPrivateNative(badNext.Release()); ferr != nil {
				t.Fatal(ferr)
			}
		}
		if after := budget.Stats(); after != before {
			t.Fatalf("error-path COW budget leaked: before=%+v after=%+v", before, after)
		}
	})
}

func BenchmarkPrepareNativeCollectionChangesSingleUpdate(b *testing.B) {
	owner, _, cols := openNativePrepareSingleFixture(b, []CollectionMeta{
		{
			Name:    "users",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			Indexes: []IndexDefinition{
				{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
				{Name: "idx_age", Field: "age", ValueType: IndexValueBSONOrderedV2},
			},
		},
	})
	col := cols[0]
	initialDoc, err := bson.Marshal(bson.D{
		{Key: "email", Value: "alice@example.com"},
		{Key: "age", Value: int32(30)},
		{Key: "score", Value: int32(1)},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := col.Insert([]byte("doc-0"), initialDoc); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	updatedDoc, err := bson.Marshal(bson.D{
		{Key: "email", Value: "alice@example.com"},
		{Key: "age", Value: int32(30)},
		{Key: "score", Value: int32(3)},
	})
	if err != nil {
		b.Fatal(err)
	}

	probe, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := probe.Close(); err != nil {
			b.Error(err)
		}
	}()
	base, ok := probe.capture.State().(*nativeEngineVersion)
	if !ok || base == nil {
		b.Fatal("expected native engine version state")
	}
	changes := []nativeJSONPrimaryChange{
		{
			collection: probe.collection,
			entries: []memtable.COWMutation{
				{Key: []byte("doc-0"), Value: updatedDoc, Flags: node.FlagInline},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next, err := prepareNativeCollectionChanges(base, changes, nil, false)
		if err != nil {
			b.Fatal(err)
		}
		if err := finishPrivateNative(next.Release()); err != nil {
			b.Fatal(err)
		}
	}
}
