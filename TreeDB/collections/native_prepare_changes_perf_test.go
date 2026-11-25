package collections

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type failingNativeDomainLifetime struct {
	err error
}

func (f failingNativeDomainLifetime) Retain(d *nativeDomainState) {
	(nativeDomainLifetime{}).Retain(d)
}

func (f failingNativeDomainLifetime) Release(d *nativeDomainState) (cowbtree.OwnedMapCompletion, error) {
	if f.err != nil {
		return cowbtree.OwnedMapCompletion{}, f.err
	}
	return (nativeDomainLifetime{}).Release(d)
}

func openNativePrepareChangesFixture(tb testing.TB) (*treedb.DB, *CollectionManager, *Collection) {
	tb.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, tb.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
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
	meta := &CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		tb.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		tb.Fatal(err)
	}
	return owner, manager, col
}

func assertNativeEngineRefundIndependent(t *testing.T, e *nativeEngineVersion) {
	t.Helper()
	if e == nil || e.preparation == nil || e.refund == nil {
		t.Fatalf("expected non-nil engine, preparation, and refund")
	}
	holderStart := uintptr(unsafe.Pointer(e))
	holderEnd := holderStart + unsafe.Sizeof(nativeEngineVersionWithPrep{})
	prepAddr := uintptr(unsafe.Pointer(e.preparation))
	refundAddr := uintptr(unsafe.Pointer(e.refund))
	if prepAddr < holderStart || prepAddr >= holderEnd {
		t.Fatalf("expected preparation (%#x) fused inside nativeEngineVersionWithPrep [%#x, %#x)", prepAddr, holderStart, holderEnd)
	}
	if refundAddr >= holderStart && refundAddr < holderEnd {
		t.Fatalf("nativeEngineRefund (%#x) must remain independent outside [%#x, %#x)", refundAddr, holderStart, holderEnd)
	}
	refundType := reflect.TypeOf(nativeEngineRefund{})
	versionType := reflect.TypeOf(nativeEngineVersion{})
	prepType := reflect.TypeOf(nativeEnginePreparation{})
	for i := 0; i < refundType.NumField(); i++ {
		ft := refundType.Field(i).Type
		if ft == reflect.PointerTo(versionType) || ft == reflect.PointerTo(prepType) || ft == versionType || ft == prepType {
			t.Fatalf("nativeEngineRefund field %q must not reference version or preparation: %v", refundType.Field(i).Name, ft)
		}
	}
}

func makeSyntheticOwnedCollection(t *testing.T, base *nativeEngineVersion, schema *nativeSchemaState, inc uint64, domainLifetime cowbtree.OwnedValueLifetime[*nativeDomainState], domain *nativeDomainState) *nativeCollectionState {
	t.Helper()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	domBuilder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 1, domainLifetime, admit)
	if err != nil {
		t.Fatal(err)
	}
	if domain != nil {
		if err := domBuilder.Set(domain.name, domain); err != nil {
			domBuilder.Cancel().Complete()
			t.Fatal(err)
		}
	}
	domains := domBuilder.Seal()
	domains.Retain()
	schema.retain()
	c, err := newNativeCollectionState(inc, 1, schema, domains, domains, admit)
	if err != nil {
		if domains.Release() {
			if done, ferr := domains.Finish(); ferr == nil {
				done.Complete()
			}
		}
		if domains.Release() {
			if done, ferr := domains.Finish(); ferr == nil {
				done.Complete()
			}
		}
		schema.release().Complete()
		t.Fatal(err)
	}
	return c
}

func TestNativeEnginePreparationFusedLifecycleAndCleanup(t *testing.T) {
	owner, _, col := openNativePrepareChangesFixture(t)
	doc, err := bson.Marshal(bson.D{{Key: "name", Value: "alice"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("doc-0"), doc); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
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
	base, ok := probe.capture.State().(*nativeEngineVersion)
	if !ok || base == nil {
		t.Fatal("expected native engine version state")
	}
	budget := probe.capture.NativeAllocator().(nativeHNSWAdmissionBudget)

	t.Run("ChargeDominanceAndEmptyEngine", func(t *testing.T) {
		fusedHeader := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersionWithPrep{})))
		separateHeader := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersion{}))) +
			memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEnginePreparation{})))
		refundCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))
		wantBase := max(fusedHeader, separateHeader) + refundCharge

		charge00, err := nativeEnginePreparationCharge(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if charge00 != wantBase {
			t.Fatalf("charge(0,0)=%d want %d (fused=%d separate=%d refund=%d)", charge00, wantBase, fusedHeader, separateHeader, refundCharge)
		}
		if charge00 < fusedHeader+refundCharge || charge00 < separateHeader+refundCharge {
			t.Fatalf("charge(0,0)=%d fails to dominate both fused and separate paths", charge00)
		}
		charge11, err := nativeEnginePreparationCharge(1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if charge11 != charge00 {
			t.Fatalf("charge(1,1)=%d want %d (inline backing should add zero overflow)", charge11, charge00)
		}
		ptr := uint64(unsafe.Sizeof(uintptr(0)))
		charge14, err := nativeEnginePreparationCharge(1, 4)
		if err != nil {
			t.Fatal(err)
		}
		if want14 := wantBase + memtable.COWAllocationCharge(4*ptr); charge14 != want14 {
			t.Fatalf("charge(1,4)=%d want %d", charge14, want14)
		}
		charge36, err := nativeEnginePreparationCharge(3, 6)
		if err != nil {
			t.Fatal(err)
		}
		if want36 := wantBase + memtable.COWAllocationCharge(3*ptr) + memtable.COWAllocationCharge(6*ptr); charge36 != want36 {
			t.Fatalf("charge(3,6)=%d want %d", charge36, want36)
		}
		if _, err := nativeEnginePreparationCharge(-1, 0); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("negative collectionCount err=%v want ErrCOWCapacity", err)
		}
		if _, err := nativeEnginePreparationCharge(0, -1); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("negative domainCount err=%v want ErrCOWCapacity", err)
		}
		if _, err := nativeEnginePreparationCharge(math.MaxInt, 0); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("overflow collectionCount err=%v want ErrCOWCapacity", err)
		}
		if _, err := nativeEnginePreparationCharge(0, math.MaxInt); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("overflow domainCount err=%v want ErrCOWCapacity", err)
		}

		// Verify newNativeEmptyEngine succeeds under nativeEnginePreparationCharge(0,0) and refunds cleanly.
		before := budget.Stats()
		empty, err := newNativeEmptyEngine(base.allocator, base.maintenance)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishPrivateNative(empty.Release()); err != nil {
			t.Fatal(err)
		}
		after := budget.Stats()
		if after != before {
			t.Fatalf("newNativeEmptyEngine leaked COW budget: before=%+v after=%+v", before, after)
		}
	})

	cases := []struct {
		name               string
		collectionCount    int
		domainCount        int
		expectInlineCol    bool
		expectInlineDomain bool
	}{
		{name: "zero_zero", collectionCount: 0, domainCount: 0, expectInlineCol: true, expectInlineDomain: true},
		{name: "single_collection_1_1", collectionCount: 1, domainCount: 1, expectInlineCol: true, expectInlineDomain: true},
		{name: "multi_domain_1_4", collectionCount: 1, domainCount: 4, expectInlineCol: true, expectInlineDomain: false},
		{name: "multi_collection_3_6", collectionCount: 3, domainCount: 6, expectInlineCol: false, expectInlineDomain: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := budget.Stats()
			next, err := newNativeEnginePreparation(base, tc.collectionCount, tc.domainCount, base.allocator)
			if err != nil {
				t.Fatal(err)
			}
			assertNativeEngineRefundIndependent(t, next)
			p := next.preparation

			if tc.expectInlineCol {
				if cap(p.ownedCollections) != len(p.inlineCollections) {
					t.Fatalf("ownedCollections cap=%d want inline %d", cap(p.ownedCollections), len(p.inlineCollections))
				}
				if unsafe.SliceData(p.ownedCollections) != &p.inlineCollections[0] {
					t.Fatalf("ownedCollections did not use inlineCollections backing")
				}
			} else {
				if cap(p.ownedCollections) != tc.collectionCount {
					t.Fatalf("ownedCollections cap=%d want %d", cap(p.ownedCollections), tc.collectionCount)
				}
				if unsafe.SliceData(p.ownedCollections) == &p.inlineCollections[0] {
					t.Fatalf("ownedCollections unexpectedly pointed to inlineCollections backing")
				}
			}

			if tc.expectInlineDomain {
				if cap(p.ownedDomains) != len(p.inlineDomains) {
					t.Fatalf("ownedDomains cap=%d want inline %d", cap(p.ownedDomains), len(p.inlineDomains))
				}
				if unsafe.SliceData(p.ownedDomains) != &p.inlineDomains[0] {
					t.Fatalf("ownedDomains did not use inlineDomains backing")
				}
			} else {
				if cap(p.ownedDomains) != tc.domainCount {
					t.Fatalf("ownedDomains cap=%d want %d", cap(p.ownedDomains), tc.domainCount)
				}
				if unsafe.SliceData(p.ownedDomains) == &p.inlineDomains[0] {
					t.Fatalf("ownedDomains unexpectedly pointed to inlineDomains backing")
				}
			}

			admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
			p.names, err = cowbtree.NewOwnedMapBuilder(base.names, tc.collectionCount, nativeCollectionLifetime{}, admit)
			if err != nil {
				t.Fatal(err)
			}
			p.collections, err = cowbtree.NewOwnedMapBuilder(base.collections, tc.collectionCount, nativeCollectionLifetime{}, admit)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < tc.domainCount; i++ {
				d, err := newNativeDomainState(fmt.Sprintf("dom-%d", i), 0, 1, backenddb.OrderedRootStorageDefault, nil, admit)
				if err != nil {
					t.Fatal(err)
				}
				p.ownedDomains = append(p.ownedDomains, d)
			}
			for i := 0; i < tc.collectionCount; i++ {
				var dom *nativeDomainState
				if i < len(p.ownedDomains) {
					dom = p.ownedDomains[i]
				}
				c := makeSyntheticOwnedCollection(t, base, probe.collection.schema, uint64(100+i), nativeDomainLifetime{}, dom)
				p.ownedCollections = append(p.ownedCollections, c)
				if err := p.collections.Set(c.incarnation, c); err != nil {
					t.Fatal(err)
				}
			}
			if tc.expectInlineCol && tc.collectionCount == 1 && p.inlineCollections[0] == nil {
				t.Fatal("expected inlineCollections[0] populated before finish")
			}
			if tc.expectInlineDomain && tc.domainCount == 1 && p.inlineDomains[0] == nil {
				t.Fatal("expected inlineDomains[0] populated before finish")
			}

			p.sealPrivateBuilders(next)
			if p.names != nil || p.collections != nil || next.names == nil || next.collections == nil {
				t.Fatal("sealPrivateBuilders did not seal private builders onto next")
			}

			if err := finishPrivateNative(next.Release()); err != nil {
				t.Fatal(err)
			}
			if next.preparation != nil {
				t.Fatal("expected next.preparation cleared after clean finish")
			}
			if p.ownedDomains != nil || p.ownedCollections != nil {
				t.Fatal("expected ownedDomains and ownedCollections slices cleared after clean finish")
			}
			if p.inlineDomains[0] != nil || p.inlineCollections[0] != nil {
				t.Fatalf("expected inline backing cleared after clean finish: domain=%v collection=%v", p.inlineDomains[0], p.inlineCollections[0])
			}
			after := budget.Stats()
			// PeakBytes is a monotonic high-water mark across subtests sharing the same
			// DB budget; verify monotonicity and strict equality on all live counters.
			want := before
			want.PeakBytes = after.PeakBytes
			if after.PeakBytes < before.PeakBytes || after != want {
				t.Fatalf("COW budget mismatch after clean finish: before=%+v after=%+v", before, after)
			}
		})
	}

	t.Run("PartialFailureParkingKeepsInlineElementsParked", func(t *testing.T) {
		isolatedBudget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		syntheticBase := &nativeEngineVersion{
			nextIncarnation: 10,
			allocator:       isolatedBudget,
			maintenance:     isolatedBudget,
		}
		admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return isolatedBudget.AdmitNativeBytes(n) }
		schema, err := newNativeSchemaState(probe.collection.schema.meta, admit)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			schema.release().Complete()
		}()

		syntheticErr := errors.New("synthetic partial cleanup failure")
		next, err := newNativeEnginePreparation(syntheticBase, 1, 1, isolatedBudget)
		if err != nil {
			t.Fatal(err)
		}
		assertNativeEngineRefundIndependent(t, next)
		p := next.preparation

		// Attach one clean domain (inline) and one collection whose pending OwnedMap fails on Release.
		cleanDomain, err := newNativeDomainState("clean", 0, 1, backenddb.OrderedRootStorageDefault, nil, admit)
		if err != nil {
			t.Fatal(err)
		}
		p.ownedDomains = append(p.ownedDomains, cleanDomain)

		failingDomain, err := newNativeDomainState("failing", 0, 1, backenddb.OrderedRootStorageDefault, nil, admit)
		if err != nil {
			t.Fatal(err)
		}
		domBuilder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 1, failingNativeDomainLifetime{err: syntheticErr}, admit)
		if err != nil {
			t.Fatal(err)
		}
		if err := domBuilder.Set(failingDomain.name, failingDomain); err != nil {
			t.Fatal(err)
		}
		failingMap := domBuilder.Seal()
		emptyDomBuilder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 0, nativeDomainLifetime{}, admit)
		if err != nil {
			t.Fatal(err)
		}
		emptyDomains := emptyDomBuilder.Seal()
		// Release the creator's extra ref on failingDomain so only failingMap holds it.
		if done, rerr := (nativeDomainLifetime{}).Release(failingDomain); rerr != nil {
			t.Fatal(rerr)
		} else {
			done.Complete()
		}
		schema.retain()
		failingCol, err := newNativeCollectionState(42, 1, schema, emptyDomains, failingMap, admit)
		if err != nil {
			t.Fatal(err)
		}
		p.ownedCollections = append(p.ownedCollections, failingCol)

		if p.inlineCollections[0] != failingCol || p.inlineDomains[0] != cleanDomain {
			t.Fatal("expected single-entry elements stored in inline backing")
		}

		// FinishSemanticCleanup must fail, park failingCol in ownedCollections[0] / inlineCollections[0],
		// detach cleanDomain from ownedDomains[0], and retain preparation + refund charge.
		beforeFail := isolatedBudget.Stats()
		work := next.Release()
		if work == nil {
			t.Fatal("expected cleanup work on last Release")
		}
		completion, err := work.Finish()
		if !errors.Is(err, syntheticErr) {
			t.Fatalf("Finish err=%v want %v", err, syntheticErr)
		}
		if completion.Refund != nil {
			t.Fatal("expected nil Refund on partial cleanup failure")
		}
		if next.preparation != p {
			t.Fatal("expected next.preparation to remain parked on partial failure")
		}
		if len(p.ownedCollections) != 1 || p.ownedCollections[0] != failingCol {
			t.Fatalf("failed collection was not parked in ownedCollections: %v", p.ownedCollections)
		}
		if p.inlineCollections[0] != failingCol {
			t.Fatalf("inlineCollections[0] was prematurely cleared on partial failure")
		}
		if len(p.ownedDomains) != 1 || p.ownedDomains[0] != nil || p.inlineDomains[0] != nil {
			t.Fatalf("successful sibling domain should have been detached: owned=%v inline=%v", p.ownedDomains, p.inlineDomains)
		}
		afterFail := isolatedBudget.Stats()
		if afterFail.ReservedBytes == 0 || afterFail.ExternalLeases == 0 {
			t.Fatalf("expected parked failure to remain charged in COW budget: before=%+v after=%+v", beforeFail, afterFail)
		}
		if afterFail.ReservedBytes >= beforeFail.ReservedBytes {
			t.Fatalf("expected successful sibling domain to be refunded while failed graph stayed parked: before=%+v after=%+v", beforeFail, afterFail)
		}
	})
}

func BenchmarkPrepareNativeCollectionChangesSinglePrimary(b *testing.B) {
	owner, _, col := openNativePrepareChangesFixture(b)
	initialDoc, err := bson.Marshal(bson.D{{Key: "name", Value: "alice"}, {Key: "count", Value: int64(1)}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := col.Insert([]byte("doc-0"), initialDoc); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	updatedDoc, err := bson.Marshal(bson.D{{Key: "name", Value: "alice"}, {Key: "count", Value: int64(2)}})
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
