package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// These fixtures deliberately bypass Add: accounted ordinary-engine storage is
// unsupported. They exercise defense at complete-operation boundaries rather
// than treating a fabricated set as an admitted finite ownership certificate.
func malformedAccountedSetForTest(t *testing.T, token *StableResourceToken) *StableResourceSet {
	t.Helper()
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		t.Fatal(err)
	}
	entry := stableResourceEntry{token: token, logicalLane: token.logicalLane, resourceID: token.resourceID,
		diagnosticPath: token.diagnosticPath, frontier: token.frontier,
		reachability:       newStableReachabilitySet(1, token.reachability),
		logicalObligations: newStableLogicalObligationView(token.logicalObligations)}
	set := &StableResourceSet{entries: []stableResourceEntry{entry}}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set
}

func TestStableMetadataAddRefusesWithoutConsumingOwner(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	token, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	before := account.retained()
	allocations := testing.AllocsPerRun(5, func() {
		if err := builder.Add(token); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
			t.Fatalf("Add: %v", err)
		}
	})
	if allocations != 0 || len(builder.entries) != 0 || account.retained() != before || ResourceOwnerState(token.owner.Load()) != ResourceOwnerToken {
		t.Fatalf("Add staged/refused owner: allocations=%v refs=%v", allocations, account.retained())
	}
}

func TestStableMetadataWholeOperationRefusalBeforeOrdinaryObserve(t *testing.T) {
	finiteMetadataTestPlatform(t)
	for _, accountedFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary-first", true: "accounted-first"}[accountedFirst], func(t *testing.T) {
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
			ordinaryBuilder := NewStableResourceSetBuilder()
			if err := ordinaryBuilder.Add(ordinary); err != nil {
				t.Fatal(err)
			}
			ordinarySet, err := ordinaryBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer ordinarySet.Release()
			account := &testStableMetadataAccount{}
			accounted, err := NewStableResourceTokenWithMetadataAccount(spec, account)
			if err != nil {
				t.Fatal(err)
			}
			accountedSet := malformedAccountedSetForTest(t, accounted)
			accountedSet.kindViews, err = buildStableResourceKindViews(accountedSet.entries)
			if err != nil {
				t.Fatal(err)
			}
			defer accountedSet.Release()
			account.deny = true
			left, right := ordinarySet, accountedSet
			if accountedFirst {
				left, right = right, left
			}
			builder := NewStableResourceSetBuilder()
			// Deliberately malformed destination shares these test fixtures; no claim,
			// transfer or Abandon is performed on its borrowed view roots.
			builder.kindViews = left.kindViews
			builder.ordinaryMetadata = false // fabricated ownership uncertainty
			// Poison Observe so an ordinary-first clone returns a different error even
			// if a failed implementation later balances Unobserve during cleanup.
			registry.mu.Lock()
			registry.states.get(physicalStableIdentity(identity)).retired = true
			registry.mu.Unlock()
			beforeRefs, beforeBytes := account.retained(), account.bytes
			operations := []struct {
				name string
				run  func() error
			}{
				{"Merge", func() error { return builder.Merge(right) }},
				{"Append", func() error {
					_, err := builder.MergeAppendOnlyLogicalObligations(right, StableLogicalObligationMutation{})
					return err
				}},
				{"FlatAppend", func() error {
					_, err := builder.mergeAppendOnlyLogicalObligationsFlat(right, StableLogicalObligationMutation{})
					return err
				}},
				{"Union", func() error { _, err := UnionStableResourceSets(left, right); return err }},
				{"PhysicalClone", func() error { _, err := ClonePhysicalReachabilityUnion(left, right); return err }},
				{"AppendCertificate", func() error {
					_, _, err := CertifyStableLogicalObligationAppendMutation(left, right, StableLogicalObligationMutation{})
					return err
				}},
				{"EntryBatch", func() error {
					_, err := cloneStableResourceEntriesToKindView([]*stableResourceEntry{&left.entries[0], &right.entries[0]})
					return err
				}},
			}
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					allocations := testing.AllocsPerRun(3, func() {
						if err := op.run(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
							t.Fatalf("preflight: %v", err)
						}
					})
					if allocations != 0 {
						t.Fatalf("refusal allocated before full closure validation: %v", allocations)
					}
					if account.retained() != beforeRefs || account.bytes != beforeBytes || left.Owner() != ResourceOwnerBuilder || right.Owner() != ResourceOwnerBuilder {
						t.Fatal("refusal changed input ownership/account")
					}
				})
			}
		})
	}
}

func TestStableMetadataAllGenericBackingExportsRefuse(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	token, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	source := malformedAccountedSetForTest(t, token)
	defer source.Release()
	if token.Kind() != "" || token.LogicalLane() != "" || token.ResourceID() != "" || token.DiagnosticPath() != "" || token.Reachability() != "" || token.Namespace() != nil || token.LogicalObligations() != nil {
		t.Fatal("token getter exported backing")
	}
	identity, generation, digest := token.Identity(), token.Generation(), token.Digest()
	if source.covers(ReachabilityOuterLeafRawPointer) || source.FrontierFor(token.Identity(), token.Generation()) != (DurableFrontier{}) {
		t.Fatal("unsupported source exported coverage/frontier authority")
	}
	if identity == (StableIdentity{}) || generation == 0 {
		t.Fatal("independent scalar identity unavailable")
	}
	invoked := false
	if err := token.WithPinnedFile(func(*os.File) error { invoked = true; return nil }); !errors.Is(err, ErrStableMetadataShapeUnsupported) || invoked {
		t.Fatal("file callback exposed loan")
	}
	if source.PhysicalDescriptors() != nil || source.Tokens() != nil || source.Stats(time.Now()) != nil || source.IdentityPinRegistryStats() != nil {
		t.Fatal("legacy export returned backing")
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{"Descriptors", func() error { _, err := source.Descriptors(); return err }},
		{"Manifest", func() error { _, _, err := source.DependencyManifestV1(); return err }},
		{"DeletionGuard", func() error { return source.DeletionGuard().Check(identity, generation) }},
		{"Walk", func() error {
			return source.WalkLogicalObligations(func(StableResourcePhysicalDescriptor, StableLogicalObligation) error { invoked = true; return nil })
		}},
		{"Directory", func() error { _, err := source.DependencyDirectoryV2(); return err }},
		{"DirectoryBase", func() error { _, err := source.DependencyDirectoryBaseV2(); return err }},
		{"DirectoryBind", func() error { _, err := BindDependencyDirectoryV2(source, &DependencyDirectoryV2{}); return err }},
		{"DirectoryChanges", func() error {
			_, _, err := WalkDependencyDirectoryChangesV2(source, nil, func([]byte, []byte, bool) error { invoked = true; return nil })
			return err
		}},
		{"DirectoryRecords", func() error {
			return WalkDependencyDirectoryRecordsV2(source, func([]byte, []byte) error { invoked = true; return nil })
		}},
		{"Selector", func() error { _, err := CloneStableResourceForSelector(source, StableResourceSelector{}); return err }},
		{"PhysicalSelection", func() error {
			_, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafLog, nil)
			return err
		}},
		{"Requirements", func() error {
			return ValidateStableResourceSetLogicalObligations(source, StableLogicalObligationRequirements{})
		}},
		{"FinalCertificate", func() error {
			_, err := CertifyStableLogicalObligationMutationFinalRequirements(source, StableLogicalObligationMutation{}, StableLogicalObligationRequirements{})
			return err
		}},
		{"RegistrarID", func() error { _, err := stableChildIDForField(source, ReachabilityOuterLeafRawPointer); return err }},
		{"Transfer", func() error { return source.transfer(ResourceOwnerBuilder, ResourceOwnerRecovery) }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			if err := op.run(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
				t.Fatalf("export refusal: %v", err)
			}
		})
	}
	if invoked {
		t.Fatal("refused exporter invoked callback")
	}
	if view, err := UnionStableResourceSets(source); view != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("union escape: %v", err)
	}
	source.Release()
	if account.retained() != 0 {
		t.Fatal("actual release retained account")
	}
	if token.kind != "" || token.logicalLane != "" || token.resourceID != "" || token.diagnosticPath != "" || token.logicalObligations != nil || token.pinned != nil || token.namespace != nil {
		t.Fatal("Release retained owned backing")
	}
	if token.Identity() != identity || token.Generation() != generation || token.Digest() != digest {
		t.Fatal("Release changed independent identity evidence")
	}
	if _, err := source.Descriptors(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal("post-release metadata escaped")
	}
}

func TestStableMetadataNamespaceInheritanceAndSecondaryProvenance(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	proof, err := NewStableNamespaceCreationProofWithMetadataAccount(parent, file, "leaf", account)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	namespace, err := proof.Bind(parent, 1, "leaf", "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	spec := finiteMetadataTestSpec(file)
	spec.Namespace = namespace
	inherited, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer inherited.Release()
	if inherited.metadataAccount != account {
		t.Fatal("namespace owner not inherited")
	}
	other := &testStableMetadataAccount{}
	if token, err := NewStableResourceTokenWithMetadataAccount(spec, other); token != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) || other.bytes != 0 || other.retained() != 0 {
		t.Fatalf("different owner admitted: %v", err)
	}
	ordinary, err := NewStableResourceToken(finiteMetadataTestSpec(file))
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	for _, entry := range []stableResourceEntry{
		{token: ordinary, pins: []*StableResourceToken{ordinary, inherited}},
		{token: &StableResourceToken{namespace: namespace}},
	} {
		set := &StableResourceSet{entries: []stableResourceEntry{entry}}
		set.owner.Store(uint32(ResourceOwnerView))
		if err := set.RequireMetadataExport(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
			t.Fatal("hidden owner provenance accepted")
		}
		if set.PhysicalDescriptors() != nil || !errors.Is(set.DeletionGuard().Check(ordinary.Identity(), ordinary.Generation()), ErrStableMetadataShapeUnsupported) {
			t.Fatal("hidden owner escaped diagnostics/guard")
		}
	}
}

func TestStableMetadataOrdinaryStampCannotLaunderAccountedInputs(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	token, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	source := malformedAccountedSetForTest(t, token)
	defer source.Release()
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if !builder.ordinaryMetadata {
		t.Fatal("ordinary constructor lost provenance stamp")
	}
	if err := builder.Add(token); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("trusted Add laundered owner: %v", err)
	}
	if err := builder.Merge(source); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("trusted Merge laundered child: %v", err)
	}
	if source.ordinaryMetadata {
		t.Fatal("failed preflight stamped unsupported source")
	}
	unknown := &StableResourceSetBuilder{entries: source.entries}
	if _, err := unknown.Freeze(); !errors.Is(err, ErrStableMetadataShapeUnsupported) || unknown.ordinaryMetadata {
		t.Fatalf("unknown Freeze laundered owner: %v", err)
	}
	ordinary, err := NewStableResourceToken(finiteMetadataTestSpec(file))
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	if err := cloneStableResourceEntryIntoBuilder(unknown, &stableResourceEntry{token: ordinary}); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("single-entry clone staged before destination closure: %v", err)
	}
	if err := cloneStableResourceViewsIntoBuilder(unknown, nil); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("view batch staged before destination closure: %v", err)
	}
	shadow := &StableResourceSet{entries: source.entries, kindViews: newStableKindViews(0)}
	if err := shadow.RequireMetadataExport(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal("shadow flat backing bypassed proof")
	}
}

func TestStableMetadataUnknownViewScansEveryBackingRoute(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	token, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	entry := &stableResourceEntry{token: token}
	views := []stableResourceKindView{
		{root: &stableResourceEntryNode{entries: []stableResourceEntry{*entry}}},
		{logical: &stableResourceLogicalIndexNode{entry: entry}},
		{physical: &stableResourcePhysicalIndexNode{entries: []*stableResourceEntry{entry}}},
		{physical: &stableResourcePhysicalIndexNode{entries: []*stableResourceEntry{{pinIndex: newStableTokenTable(token)}}}},
	}
	for i, view := range views {
		set := &StableResourceSet{kindViews: newStableKindViewsWith(ResourceOuterLeafLog, view)}
		if err := set.RequireMetadataExport(); !errors.Is(err, ErrStableMetadataShapeUnsupported) || set.ordinaryMetadata {
			t.Fatalf("backing route %d laundered: %v", i, err)
		}
		if union, err := UnionStableResourceSets(set); union != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) {
			t.Fatalf("union route %d laundered: %v", i, err)
		}
	}
}

func TestStableMetadataOwnedCloneIOAndReleaseSerialize(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &testStableMetadataAccount{}
	source, err := NewStableResourceTokenWithMetadataAccount(finiteMetadataTestSpec(file), account)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := source.cloneSharedPinned("copy", "2", "leaf/copy", source.frontier, source.reachability, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	if account.retained() != 1 {
		t.Fatal("source dropped live clone loan")
	}
	var workers sync.WaitGroup
	workers.Add(2)
	start := make(chan struct{})
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 100; i++ {
			if _, err := clone.ReadAt(nil, 0); err != nil && !errors.Is(err, ErrResourceOwnership) {
				t.Errorf("exact owned IO: %v", err)
				return
			}
			if clone.Kind() != "" || clone.Namespace() != nil {
				t.Error("concurrent getter exported backing")
				return
			}
		}
	}()
	go func() { defer workers.Done(); <-start; clone.Release(); clone.Release() }()
	close(start)
	workers.Wait()
	if account.retained() != 0 || clone.pinned != nil || clone.logicalObligations != nil {
		t.Fatal("final clone release retained backing/owner")
	}
}

func TestStableMetadataOrdinaryNamespaceLoanAndGroupedStagingRefuse(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := os.Create(filepath.Join(dir, "leaf"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	spec := StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "leaf", DiagnosticPath: "leaf"}
	ordinary, err := NewStableNamespaceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	account := &testStableMetadataAccount{}
	resource := finiteMetadataTestSpec(file)
	resource.Namespace = ordinary
	if token, err := NewStableResourceTokenWithMetadataAccount(resource, account); token != nil || !errors.Is(err, ErrStableMetadataShapeUnsupported) || account.bytes != 0 || account.retained() != 0 {
		t.Fatalf("ordinary namespace loan: %v", err)
	}
	finite, err := NewStableNamespaceTokenWithMetadataAccount(spec, account)
	if err != nil {
		t.Fatal(err)
	}
	defer finite.Release()
	before := account.bytes
	if err := StabilizeStableNamespaceTokens(ordinary, finite); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatalf("grouped staging: %v", err)
	}
	if ordinary.state.Load() != namespacePending || account.bytes != before {
		t.Fatal("group refused after ordinary namespace work")
	}
}
