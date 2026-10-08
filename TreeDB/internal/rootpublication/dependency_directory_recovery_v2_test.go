package rootpublication

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestStableMetadataRecoveryRejectsCompleteAdmittedClosure(t *testing.T) {
	finiteMetadataTestPlatform(t)
	for _, mode := range []string{"selected", "flat", "rope", "physical", "pinIndex"} {
		t.Run(mode, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "leaf")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			identity, err := StableIdentityFromFile(file)
			if err != nil {
				t.Fatal(err)
			}
			registry := NewIdentityPinRegistry()
			if err := registry.Observe(identity); err != nil {
				t.Fatal(err)
			}
			defer registry.Unobserve(identity)
			spec := finiteMetadataTestSpec(file)
			spec.PinRegistry = registry
			ordinary, err := NewStableResourceToken(spec)
			if err != nil {
				t.Fatal(err)
			}
			builder := NewStableResourceSetBuilder()
			if err := builder.Add(ordinary); err != nil {
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			releases := 0
			bound := bindPhysicalUnionTestDirectory(t, source, func() { releases++ })
			defer bound.Release()
			directory, err := bound.DependencyDirectoryV2()
			if err != nil {
				t.Fatal(err)
			}
			account := &testStableMetadataAccount{}
			finite, err := NewStableResourceTokenWithMetadataAccount(spec, account)
			if err != nil {
				t.Fatal(err)
			}
			defer finite.Release()
			var selected stableResourceEntry
			source.rangeEntries(func(entry *stableResourceEntry) bool { selected = *entry; return false })
			hidden := stableResourceEntry{token: finite}
			view := stableResourceKindView{logical: &stableResourceLogicalIndexNode{entry: &selected}}
			// Borrowed test-only unknown views retain the actual token owners outside
			// the callback result; Release of ResourceOwnerView consumes no loan.
			admitted := &StableResourceSet{kindViews: newStableKindViewsWith(ResourceOuterLeafLog, view)}
			admitted.owner.Store(uint32(ResourceOwnerView))
			switch mode {
			case "selected":
				selected.token = finite
				// Encoding this invalid descriptor would report a different error.
				selected.resourceID = ""
			case "flat":
				admitted.entries = []stableResourceEntry{hidden}
			case "rope":
				view.root = &stableResourceEntryNode{entries: []stableResourceEntry{hidden}}
			case "physical":
				view.physical = &stableResourcePhysicalIndexNode{entries: []*stableResourceEntry{&hidden}}
			case "pinIndex":
				hidden = stableResourceEntry{pinIndex: newStableTokenTable(finite)}
				view.physical = &stableResourcePhysicalIndexNode{entries: []*stableResourceEntry{&hidden}}
			}
			admitted.kindViews.set(ResourceOuterLeafLog, view)
			// Any ordinary prefix clone must fail distinctly even if subsequent
			// error cleanup balances its registry references.
			registry.mu.Lock()
			registry.states.get(physicalStableIdentity(identity)).retired = true
			registry.mu.Unlock()
			beforeStats, beforeRefs, beforeBytes := registry.Stats(), account.retained(), account.bytes
			beforeDirectoryRefs := directory.refs.Load()
			callbacks := 0
			result, err := RecoverDependencyDirectoryV2(directory, func(DependencyManifestEntryV1) (*StableResourceSet, error) {
				callbacks++
				return admitted, nil
			}, func(*os.File, DependencyManifestEntryV1, StableLogicalObligation) error {
				t.Fatal("unsupported recovery invoked logical validation")
				return nil
			})
			if result != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) || callbacks != 1 {
				t.Fatalf("whole closure refusal: result=%v error=%v callbacks=%d", result, err, callbacks)
			}
			if admitted.ordinaryMetadata || account.retained() != beforeRefs || account.bytes != beforeBytes || !reflect.DeepEqual(registry.Stats(), beforeStats) || directory.refs.Load() != beforeDirectoryRefs || releases != 0 {
				t.Fatal("recovery staged ownership/account/directory changes before refusal")
			}
			finite.Release()
			bound.Release()
			source.Release()
			if account.retained() != 0 || registry.Stats().ActivePins != 0 || releases != 1 {
				t.Fatalf("final cleanup: account=%d registry=%+v directories=%d", account.retained(), registry.Stats(), releases)
			}
		})
	}
}

func TestDependencyDirectoryV2OrdinaryRecoveryBalancesOwnership(t *testing.T) {
	registry := NewIdentityPinRegistry()
	var identity StableIdentity
	token := stableTokenFixture(t, t.TempDir(), "leaf", 1, 4, ReachabilityOuterLeafRawPointer, "1", func(spec *StableResourceSpec) {
		var err error
		identity, err = StableIdentityFromFile(spec.File)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Observe(identity); err != nil {
			t.Fatal(err)
		}
		spec.PinRegistry = registry
	})
	defer registry.Unobserve(identity)
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	releases := 0
	bound := bindPhysicalUnionTestDirectory(t, source, func() { releases++ })
	defer bound.Release()
	directory, err := bound.DependencyDirectoryV2()
	if err != nil {
		t.Fatal(err)
	}
	beforeRefs := directory.refs.Load()
	beforeStats := registry.Stats()
	var admitted *StableResourceSet
	recovered, err := RecoverDependencyDirectoryV2(directory, func(entry DependencyManifestEntryV1) (*StableResourceSet, error) {
		clone, err := token.cloneSharedPinned(entry.LogicalLane, entry.ResourceID, entry.DiagnosticPath, entry.Frontier, entry.Reachability[0], nil, nil)
		if err != nil {
			return nil, err
		}
		builder := NewStableResourceSetBuilder()
		defer builder.Abandon()
		if err := builder.Add(clone); err != nil {
			clone.Release()
			return nil, err
		}
		admitted, err = builder.Freeze()
		return admitted, err
	}, func(*os.File, DependencyManifestEntryV1, StableLogicalObligation) error {
		t.Fatal("physical-only recovery invoked logical validation")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Release()
	if admitted == nil || admitted.Owner() != ResourceOwnerReleased || !reflect.DeepEqual(mustStableResourceDescriptors(t, recovered), mustStableResourceDescriptors(t, bound)) {
		t.Fatal("ordinary recovery lost contents or callback ownership")
	}
	recovered.Release()
	if directory.refs.Load() != beforeRefs || releases != 0 || !reflect.DeepEqual(registry.Stats(), beforeStats) {
		t.Fatal("ordinary recovery leaked directory ownership")
	}
	bound.Release()
	source.Release()
	if releases != 1 || registry.Stats().ActivePins != 0 {
		t.Fatalf("directory releases=%d registry=%+v", releases, registry.Stats())
	}
}
