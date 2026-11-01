package collections

import (
	"errors"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeLeafPackGraphRefreshPreservesCompleteSourceAndOldViews(t *testing.T) {
	f := newNativeOrdinaryGraphHandoffFixture(t, 10)
	manifest, _ := f.collection.domains.Get("column/manifest")
	// Include an older input and a newer non-graph header. Physical maintenance
	// must keep both, unlike captured-prefix handoff, which removes a boundary.
	older := &nativeTypedDelta{appliedLSN: 2, buildCapacity: 17}
	older.refs.Store(1)
	f.original.previous = older
	f.original.retain()
	newer := &nativeTypedDelta{previous: f.original, appliedLSN: 5, memberOrdinal: 3, buildCapacity: 29}
	newer.refs.Store(1)
	t.Cleanup(func() { newer.release().Complete() })
	primary := &nativeDomainState{name: "primary", baseRoot: 8, catalogRoot: 8, typed: newer}
	g, err := prepareNativeLeafPackGraphRefresh(f.collection, primary, manifest, 14)
	if err != nil || !g.touches(primary) || g.boundary != older {
		t.Fatalf("manifest-only move did not select primary: plan=%+v error=%v", g, err)
	}
	before := f.budget.Stats()
	for failAt := 0; failAt <= 2; failAt++ {
		calls := 0
		admit := func(n uint64) (cowbtree.OwnedMapLease, error) {
			calls++
			if failAt != 0 && calls == failAt {
				return nil, memtable.ErrCOWCapacity
			}
			return f.admit(n)
		}
		next, err := g.clone(newer, admit)
		if failAt != 0 {
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("failure %d: %v", failAt, err)
			}
		} else {
			if err != nil || next == newer || next.previous == f.original || next.previous.previous != older {
				t.Fatalf("lost original older input: next=%p error=%v", next, err)
			}
			graph := next.previous
			if next.appliedLSN != 5 || next.memberOrdinal != 3 || next.buildCapacity != 29 ||
				graph.data != f.original.data || graph.graphSourceRoot != 14 ||
				graph.appliedLSN != f.original.appliedLSN || graph.memberOrdinal != f.original.memberOrdinal || graph.buildCapacity != f.original.buildCapacity {
				t.Fatal("private headers changed original data or command/capacity promise")
			}
			// This synthetic successive-refresh control advances the effective
			// header again without rewriting the original recipe's root10.
			secondPrimary := &nativeDomainState{name: "primary", typed: next}
			secondManifest := &nativeDomainState{baseRoot: 14, catalogRoot: 14}
			secondPlan, err := prepareNativeLeafPackGraphRefresh(f.collection, secondPrimary, secondManifest, 18)
			if err != nil {
				t.Fatal(err)
			}
			second, err := secondPlan.clone(next, f.admit)
			if err != nil || second.previous.graphSourceRoot != 18 || second.previous.data != graph.data || second.previous.previous != older || graph.graphSourceRoot != 14 {
				second.release().Complete()
				t.Fatalf("successive refresh lost original promise: %v", err)
			}
			second.release().Complete()
		}
		next.release().Complete()
		if after := f.budget.Stats(); !reflect.DeepEqual(before, after) {
			t.Fatalf("partial/success cleanup leaked: before=%+v after=%+v", before, after)
		}
		if newer.previous != f.original || f.original.previous != older || f.original.graphSourceRoot != 10 || f.original.data.graph.sourceRoot != 10 {
			t.Fatal("maintenance mutated held old headers or recipe")
		}
	}
}

func TestNativeLeafPackGraphRefreshRejectsStaleSource(t *testing.T) {
	for _, name := range []string{"effective_root", "unassigned", "schema", "manifest_provenance", "zero_destination"} {
		t.Run(name, func(t *testing.T) {
			f := newNativeOrdinaryGraphHandoffFixture(t, 10)
			manifest, _ := f.collection.domains.Get("column/manifest")
			primary := &nativeDomainState{name: "primary", typed: f.original}
			root := uint64(14)
			switch name {
			case "effective_root":
				f.original.graphSourceRoot = 9
			case "unassigned":
				f.original.appliedLSN = 0
			case "schema":
				meta := f.collection.schema.meta.copy()
				meta.Options.ColumnStore.Columns[0].VectorDims++
				f.collection = &nativeCollectionState{schema: &nativeSchemaState{meta: meta}}
			case "manifest_provenance":
				manifest.baseRoot = 9
			case "zero_destination":
				root = 0
			}
			if _, err := prepareNativeLeafPackGraphRefresh(f.collection, primary, manifest, root); !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) {
				t.Fatalf("accepted %s: %v", name, err)
			}
		})
	}
}

func TestNativeLeafPackGraphRefreshKeepsReplacementAndRequiresProof(t *testing.T) {
	f := newNativeOrdinaryGraphHandoffFixture(t, 10)
	manifest, _ := f.collection.domains.Get("column/manifest")
	primary := &nativeDomainState{name: "primary", typed: f.original}
	for _, baseRoot := range []uint64{0, 10} {
		manifest.baseRoot = baseRoot
		afterRoot := uint64(14)
		if baseRoot != 0 {
			afterRoot = 10
		}
		g, err := prepareNativeLeafPackGraphRefresh(f.collection, primary, manifest, afterRoot)
		if err != nil || g.touches(primary) {
			t.Fatalf("replacement/unchanged basis changed graph: %+v %v", g, err)
		}
	}
	var scratch nativeBasisRefreshScratch
	if g, err := scratch.leafPackGraphRefresh(nil, false); err != nil || g.primary != nil {
		t.Fatalf("generic maintenance attempted graph rebind: %+v %v", g, err)
	}
	e := &nativeEngineVersion{}
	if state, err := e.PrepareLeafPackBasisRefresh(nil, nil, backenddb.LeafGenerationPackTransition{}); state != nil || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) {
		t.Fatalf("zero receipt authorized refresh: state=%v error=%v", state, err)
	}
}
