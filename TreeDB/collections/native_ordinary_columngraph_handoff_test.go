package collections

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// These private-header controls exercise authentication and custody in isolation.
// The public changed-manifest witness supplies the causal RED and real roots.
type nativeOrdinaryGraphHandoffFixture struct {
	collection *nativeCollectionState
	original   *nativeTypedDelta
	successor  *nativeDomainState
	built      []nativeBuiltDomain
	budget     *memtable.COWBudget
	admit      cowbtree.OwnedMapAdmit
}

func newNativeOrdinaryGraphHandoffFixture(t *testing.T, sourceRoot uint64) *nativeOrdinaryGraphHandoffFixture {
	t.Helper()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("private handoff controls retained custody: %+v", stats)
		}
	})
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AcquireExternal(n) }
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphMeta())
	if err != nil {
		t.Fatal(err)
	}
	schema, err := newNativeSchemaState(&meta, admit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { schema.release().Complete() })
	manifest, err := newNativeDomainState("column/manifest", sourceRoot, 1, 0, nil, admit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, err := (nativeDomainLifetime{}).Release(manifest)
		done.Complete()
		if err != nil {
			t.Error(err)
		}
	})
	domains, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 1, nativeDomainLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { domains.Cancel().Complete() }()
	if err := domains.Set(manifest.name, manifest); err != nil {
		t.Fatal(err)
	}
	directory := domains.Seal()
	t.Cleanup(func() {
		done, err := finishNativeDomainDirectory(&directory)
		done.Complete()
		if err != nil {
			t.Error(err)
		}
	})
	collection := &nativeCollectionState{incarnation: 7, documentGeneration: 3, schema: schema, domains: directory}
	codec := nativeOrdinaryGraphPendingCodec(typedcolumn.CompressionNone) + 1
	codec |= codec << 8
	schema.retain()
	recipe := &nativeVectorGraphRecipe{
		schema: schema, def: meta.VectorIndexes[0], ordinary: true,
		sourceRoot: sourceRoot, sourceDocumentGeneration: 3, rows: 256,
		constructionWorkers: 2, pendingSourceCodec: codec,
	}
	data := &nativeTypedRows{graph: recipe}
	data.refs.Store(1)
	original := &nativeTypedDelta{data: data, appliedLSN: 4, memberOrdinal: 2, buildCapacity: 123456, graphSourceRoot: sourceRoot}
	original.refs.Store(1)
	t.Cleanup(func() { original.release().Complete() })
	cloned, err := cloneNativeTypedSuffix(original, nil, admit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cloned.release().Complete() })
	built := []nativeBuiltDomain{
		{collection: 7, name: "column/manifest", interpretation: 1, oldBaseRoot: sourceRoot, oldCatalogRoot: sourceRoot, root: 14, typedMeta: schema.meta, ordinaryGraphSourceCodecs: codec.sourceCodecs()},
		{collection: 7, name: "primary", interpretation: 1, oldBaseRoot: 8, oldCatalogRoot: 8, root: 12, typedMeta: schema.meta},
	}
	return &nativeOrdinaryGraphHandoffFixture{collection: collection, original: original, successor: &nativeDomainState{name: "primary", baseRoot: 12, typed: cloned}, built: built, budget: budget, admit: admit}
}

func TestNativeOrdinaryColumnGraphRecipeAcrossCapturedHandoff(t *testing.T) {
	for _, tc := range []struct {
		name      string
		edit      func(*nativeOrdinaryGraphHandoffFixture)
		wantError bool
		wantRoot  uint64
	}{
		{name: "captured-prefix", wantRoot: 14},
		{name: "same-cut", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].root = 10 }, wantRoot: 10},
		{name: "already-drained", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.successor.typed = nil }},
		{name: "no-new-source", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].ordinaryGraphSourceCodecs = 0 }, wantRoot: 14},
		{name: "graph-only-prefix", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			f.built[0].ordinaryGraphSourceCodecs = 0
			f.built[1].root, f.successor.baseRoot = 8, 8
		}, wantRoot: 14},
		{name: "conservative-promise", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			f.original.data.graph.pendingSourceCodec = 0
			f.built[0].ordinaryGraphSourceCodecs = 0xff
		}, wantRoot: 14},
		{name: "missing-paired-manifest", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].name = "unrelated" }, wantError: true},
		{name: "foreign-collection", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].collection++ }, wantError: true},
		{name: "foreign-build-metadata", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			f.built[0].typedMeta = f.built[0].typedMeta.copy()
		}, wantError: true},
		{name: "wrong-physical-basis", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].oldBaseRoot++ }, wantError: true},
		{name: "wrong-catalog-provenance", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].oldCatalogRoot++ }, wantError: true},
		{name: "wrong-interpretation", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[0].interpretation++ }, wantError: true},
		{name: "stale-effective-basis", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.successor.typed.graphSourceRoot = 9 }, wantError: true},
		{name: "different-physical-schema", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			meta := f.built[0].typedMeta.copy()
			meta.Options.ColumnStore.Columns[0].VectorDims++
			meta.VectorIndexes[0].Dimensions++
			f.built[0].typedMeta, f.built[1].typedMeta = meta, meta
		}, wantError: true},
		{name: "unpromised-source-codec", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			codec := nativeOrdinaryGraphPendingCodec(typedcolumn.CompressionZSTD) + 1
			f.built[0].ordinaryGraphSourceCodecs |= (codec | codec<<8).sourceCodecs()
		}, wantError: true},
		{name: "invalid-codec-promise", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			f.original.data.graph.pendingSourceCodec = 0x0505
		}, wantError: true},
		{name: "physical-rewrite", edit: func(f *nativeOrdinaryGraphHandoffFixture) { f.built[1].physicalOnly = true }, wantError: true},
		{name: "unchanged-physical-maintenance", edit: func(f *nativeOrdinaryGraphHandoffFixture) {
			f.built[1].physicalOnly = true
			f.built[0].root = 10
			f.built[0].ordinaryGraphSourceCodecs = 0
		}, wantRoot: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeOrdinaryGraphHandoffFixture(t, 10)
			if tc.edit != nil {
				tc.edit(f)
			}
			originalData, recipe := f.original.data, f.original.data.graph
			originalSchema := recipe.schema
			prior := f.budget.Stats()
			err := prepareNativeOrdinaryGraphHandoff(f.collection, f.built, &f.built[1], f.successor)
			if tc.wantError {
				if !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) {
					t.Fatalf("missing original basis refusal: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if f.original.graphSourceRoot != 10 || recipe.sourceRoot != 10 || recipe.sourceDocumentGeneration != 3 ||
				f.original.data != originalData || f.original.data.graph != recipe || recipe.schema != originalSchema ||
				f.original.appliedLSN != 4 || f.original.memberOrdinal != 2 || f.original.buildCapacity != 123456 {
				t.Fatal("handoff changed original recipe/header/command/capacity")
			}
			if d := f.successor.typed; d != nil {
				if d == f.original || d.data != originalData || d.appliedLSN != 4 || d.memberOrdinal != 2 || d.buildCapacity != 123456 {
					t.Fatal("successor lost independent header or original data/command promise")
				}
				if !tc.wantError && d.graphSourceRoot != tc.wantRoot {
					t.Fatalf("successor root=%d want=%d", d.graphSourceRoot, tc.wantRoot)
				}
			}
			if after := f.budget.Stats(); !reflect.DeepEqual(after, prior) {
				t.Fatalf("scalar handoff acquired or refunded backing: before=%+v after=%+v", prior, after)
			}
		})
	}
}

func TestNativeOrdinaryGraphHandoffKeepsOriginalPromiseAcrossSuccessors(t *testing.T) {
	f := newNativeOrdinaryGraphHandoffFixture(t, 10)
	first := f.successor.typed
	if err := prepareNativeOrdinaryGraphHandoff(f.collection, f.built, &f.built[1], f.successor); err != nil {
		t.Fatal(err)
	}
	second, err := cloneNativeTypedSuffix(first, nil, f.admit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.release().Complete() })
	manifest, err := newNativeDomainState("column/manifest", 14, 1, 0, nil, f.admit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, err := (nativeDomainLifetime{}).Release(manifest)
		done.Complete()
		if err != nil {
			t.Error(err)
		}
	})
	builder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](f.collection.domains, 1, nativeDomainLifetime{}, f.admit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { builder.Cancel().Complete() }()
	if err := builder.Set(manifest.name, manifest); err != nil {
		t.Fatal(err)
	}
	directory := builder.Seal()
	t.Cleanup(func() {
		done, err := finishNativeDomainDirectory(&directory)
		done.Complete()
		if err != nil {
			t.Error(err)
		}
	})
	successorCollection := &nativeCollectionState{incarnation: f.collection.incarnation, documentGeneration: f.collection.documentGeneration, schema: f.collection.schema, domains: directory}
	f.built[0].oldBaseRoot, f.built[0].oldCatalogRoot, f.built[0].root = 14, 14, 18
	f.built[1].oldBaseRoot, f.built[1].oldCatalogRoot, f.built[1].root = 12, 12, 16
	successor := &nativeDomainState{name: "primary", baseRoot: 16, typed: second}
	if err := prepareNativeOrdinaryGraphHandoff(successorCollection, f.built, &f.built[1], successor); err != nil {
		t.Fatal(err)
	}
	if f.original.graphSourceRoot != 10 || first.graphSourceRoot != 14 || second.graphSourceRoot != 18 ||
		second.data != f.original.data || second.data.graph.sourceRoot != 10 ||
		second.appliedLSN != f.original.appliedLSN || second.memberOrdinal != f.original.memberOrdinal || second.buildCapacity != f.original.buildCapacity {
		t.Fatal("successive handoffs rewrote an older header or replaced original construction ownership")
	}
}

func TestNativeOrdinaryGraphHandoffFromEmptyManifest(t *testing.T) {
	f := newNativeOrdinaryGraphHandoffFixture(t, 0)
	before := f.budget.Stats()
	if err := prepareNativeOrdinaryGraphHandoff(f.collection, f.built, &f.built[1], f.successor); err != nil {
		t.Fatal(err)
	}
	if f.original.graphSourceRoot != 0 || f.original.data.graph.sourceRoot != 0 ||
		f.successor.typed.graphSourceRoot != 14 || f.successor.typed.data != f.original.data ||
		f.successor.typed.appliedLSN != f.original.appliedLSN || f.successor.typed.memberOrdinal != f.original.memberOrdinal ||
		f.successor.typed.buildCapacity != f.original.buildCapacity {
		t.Fatal("first physical manifest changed original graph source or command promise")
	}
	if after := f.budget.Stats(); !reflect.DeepEqual(after, before) {
		t.Fatalf("first manifest handoff changed admission: before=%+v after=%+v", before, after)
	}
}

func TestNativeGraphPreparationRejectsChangedCapturedSources(t *testing.T) {
	for _, mode := range []string{"same-source", "changed-manifest", "changed-documents", "corrupt-recipe-header"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeOrdinaryGraphHandoffFixture(t, 10)
			f.original.appliedLSN, f.original.memberOrdinal = 0, 0
			var want error
			switch mode {
			case "changed-manifest":
				f.original.data.graph.sourceRoot--
				f.original.graphSourceRoot--
				want = errNativeGraphPreparationStale
			case "changed-documents":
				f.original.data.graph.sourceDocumentGeneration--
				want = backenddb.ErrConcurrentModification
			case "corrupt-recipe-header":
				f.original.data.graph.sourceRoot--
				want = backenddb.ErrRootPublicationBasisMismatch
			}
			before := f.budget.Stats()
			sourceRoot := f.original.graphSourceRoot
			err := validateNativeGraphPreparation(f.collection, f.original)
			if !errors.Is(err, want) {
				t.Fatalf("source validation=%v want=%v", err, want)
			}
			if f.original.appliedLSN != 0 || f.original.memberOrdinal != 0 || f.original.buildCapacity != 123456 || f.original.graphSourceRoot != sourceRoot {
				t.Fatal("source validation changed command or construction ownership")
			}
			if after := f.budget.Stats(); !reflect.DeepEqual(after, before) {
				t.Fatalf("source validation changed admission: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestNativeGraphPreparationRefreshRetainsCleanupFailure(t *testing.T) {
	cleanup := errors.New("original cleanup failure")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "exact-stale", err: errNativeGraphPreparationStale, want: true},
		{name: "wrapped-stale", err: fmt.Errorf("original preparation: %w", errNativeGraphPreparationStale), want: true},
		{name: "only-stale-leaves", err: errors.Join(errNativeGraphPreparationStale, fmt.Errorf("original cleanup: %w", errNativeGraphPreparationStale)), want: true},
		{name: "no-error"},
		{name: "same-text-distinct-error", err: errors.New(errNativeGraphPreparationStale.Error())},
		{name: "logical-conflict", err: backenddb.ErrConcurrentModification},
		{name: "terminal-basis-refusal", err: backenddb.ErrRootPublicationBasisMismatch},
		{name: "joined-cleanup", err: errors.Join(errNativeGraphPreparationStale, cleanup)},
		{name: "nested-cleanup", err: fmt.Errorf("original transaction: %w", errors.Join(errNativeGraphPreparationStale, errors.Join(errNativeGraphPreparationStale, cleanup)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nativeGraphPreparationOnlyStale(tc.err); got != tc.want {
				t.Fatalf("refresh classification=%t want=%t err=%v", got, tc.want, tc.err)
			}
			if (tc.name == "joined-cleanup" || tc.name == "nested-cleanup") && !errors.Is(tc.err, cleanup) {
				t.Fatal("original cleanup cause disappeared")
			}
		})
	}
}
