package rootpublication

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
)

// A private staging builder is still the real owner of each retired role.
// Neither its own mutex boundary nor a consumed set shell is an outer boundary.
func TestStableNestedAddCleanupWaitsForOuterGates(t *testing.T) {
	outer := NewStableResourceSetBuilder()
	child := &StableResourceSet{ordinaryMetadata: true}
	child.owner.Store(uint32(ResourceOwnerBuilder))
	staged := NewStableResourceSetBuilder()
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	retiredEnv := &cleanupCustodyEnvironmentV1{ready: true}
	base := cleanupCustodyTokenV1(t, spec, baseEnv)
	retired := cleanupCustodyTokenV1(t, spec, retiredEnv)
	if err := staged.Add(base); err != nil { t.Fatal(err) }
	retiredEnv.callback = func() {
		if !outer.mu.TryLock() { t.Error("nested Add callback retained outer builder gate"); return }
		outer.mu.Unlock()
		if !child.mu.TryLock() { t.Error("nested Add callback retained outer child gate"); return }
		child.mu.Unlock()
		if outer.State() != ResourceOwnerBuilder || child.Len() != 0 { t.Error("callback saw wrong outer ownership") }
		if !errors.Is(staged.AbandonCheckedV1(), ErrStableResourceOperationBusy) { t.Error("reentrant cleanup did not refuse running owner") }
	}
	outer.mu.Lock()
	child.mu.Lock()
	err := staged.addWithCleanupBoundaryV1(retired, true)
	child.mu.Unlock()
	outer.mu.Unlock()
	if err != nil { t.Fatal(err) }
	if retiredEnv.calls != 0 || retired.CleanupCompleteV1() { t.Fatal("nested Add drained before inherited gates dropped") }
	if err = staged.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if retiredEnv.calls != 1 || baseEnv.calls != 1 || !retired.CleanupCompleteV1() { t.Fatal("staged roles were lost or replayed") }
	if err = staged.AbandonCheckedV1(); err != nil || retiredEnv.calls != 1 || baseEnv.calls != 1 { t.Fatal("completed staging cleanup replayed") }
}

func TestStableNestedFlatTransferKeepsDestinationPendingDebt(t *testing.T) {
	outer := NewStableResourceSetBuilder()
	inheritedChild := &StableResourceSet{ordinaryMetadata: true}
	inheritedChild.owner.Store(uint32(ResourceOwnerBuilder))
	staged := NewStableResourceSetBuilder()
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	pending := &cleanupCustodyEnvironmentV1{}
	base := cleanupCustodyTokenV1(t, spec, baseEnv)
	dropped := cleanupCustodyTokenV1(t, spec, pending)
	if err := staged.Add(base); err != nil { t.Fatal(err) }
	childBuilder := NewStableResourceSetBuilder()
	if err := childBuilder.Add(dropped); err != nil { t.Fatal(err) }
	// Match the actual fallback's private entry-only shell; Freeze deliberately
	// includes view/high-water aliases and is not a transferable cleanup shell.
	childBuilder.mu.Lock()
	child := &StableResourceSet{ordinaryMetadata: true, entries: childBuilder.entries}
	child.owner.Store(uint32(ResourceOwnerBuilder))
	childBuilder.entries = nil
	childBuilder.closed = true
	childBuilder.mu.Unlock()
	var err error
	cleanup := stableBuilderAddCleanup{temporary: staged, temporarySet: child}
	pending.callback = func() {
		if !outer.mu.TryLock() { t.Error("flat cleanup retained outer builder gate"); return }
		outer.mu.Unlock()
		if !inheritedChild.mu.TryLock() { t.Error("flat cleanup retained inherited child gate"); return }
		inheritedChild.mu.Unlock()
		if outer.State() != ResourceOwnerBuilder || inheritedChild.Len() != 0 { t.Error("flat cleanup observed invalid outer owner") }
	}
	outer.mu.Lock()
	inheritedChild.mu.Lock()
	_, err = staged.mergeAppendOnlyLogicalObligationsFlatWithCleanupV1(child, StableLogicalObligationMutation{}, &cleanup)
	if err == nil {
		staged.mu.Lock()
		outer.entries = staged.entries
		staged.entries = nil
		staged.indexed = nil
		staged.closed = true
		staged.mu.Unlock()
	}
	inheritedChild.mu.Unlock()
	outer.mu.Unlock()
	if err != nil { t.Fatal(err) }
	if child.Owner() != ResourceOwnerTransferred || cleanup.temporarySet != nil || pending.calls != 0 { t.Fatal("exact shell transfer or deferred boundary failed") }
	if err = cleanup.release(); !errors.Is(err, errCleanupCustodyFixtureV1) { t.Fatalf("pending cleanup: %v", err) }
	if cleanup.temporary != staged || len(outer.entries) != 1 || outer.entries[0].token != base || base.CleanupCompleteV1() || pending.calls != 1 { t.Fatal("posttransfer destination or pending dropped role was lost") }
	pending.ready = true
	if err = cleanup.release(); err != nil { t.Fatal(err) }
	if !cleanup.empty() || pending.calls != 2 || baseEnv.calls != 0 { t.Fatal("pending retry replayed or released destination") }
	if err = outer.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if pending.calls != 2 || baseEnv.calls != 1 { t.Fatal("final destination cleanup replayed dropped role") }
}

func TestStableAppendFallbackPendingOriginalRolesRetryWithoutReplay(t *testing.T) {
	file := operationLifetimeFile(t)
	candidate := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := candidate.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil { t.Fatal(err) }
	}
	spec := StableResourceSpec{Kind: ResourceOuterLeafPack, LogicalLane: "packs", ResourceID: "fallback-pending", Generation: 1,
		DiagnosticPath: "fallback-pending", File: file, Frontier: DurableFrontier{Bytes: 1},
		Digest: sha256.Sum256([]byte("immutable")), Reachability: ReachabilityDictionaryGeneration, ContentSynced: true}
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	pending := &cleanupCustodyEnvironmentV1{}
	base := cleanupCustodyTokenV1(t, spec, baseEnv)
	if err := candidate.Add(base); err != nil { t.Fatal(err) }
	// Cross-kind immutable coalescing is an actual exact-fallback trigger.
	spec.Kind, spec.LogicalLane, spec.Reachability = ResourceDictionary, "dictionary", ReachabilityDictionaryGeneration
	producer := freezeAppendMutationResources(t, cleanupCustodyTokenV1(t, spec, pending))
	pending.callback = func() {
		if !candidate.mu.TryLock() { t.Error("fallback callback retained outer builder gate"); return }
		candidate.mu.Unlock()
		if !producer.mu.TryLock() { t.Error("fallback callback retained outer set gate"); return }
		producer.mu.Unlock()
		if candidate.State() != ResourceOwnerBuilder || producer.Owner() != ResourceOwnerTransferred { t.Error("fallback callback preceded actual adoption") }
		if !errors.Is(candidate.AbandonCheckedV1(), ErrStableResourceOperationBusy) { t.Error("reentrant abandonment ignored running outer owner") }
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{})
	if !errors.Is(err, errCleanupCustodyFixtureV1) || work.AppendOnlyCollisionFallbacks != 1 { t.Fatalf("fallback result %+v %v", work, err) }
	if producer.Owner() != ResourceOwnerTransferred || len(candidate.entries) != stableResourceEntryLinearLookupLimit+1 || baseEnv.calls != 1 || pending.calls != 1 { t.Fatal("fallback lost adopted destination or original cleanup cursor") }
	if _, err = candidate.Freeze(); err == nil { t.Fatal("unfinished original cleanup allowed publication") }
	if err = producer.Release(); err != nil || pending.calls != 1 { t.Fatal("transferred external shell replayed original token") }
	pending.ready = true
	if err = candidate.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if baseEnv.calls != 1 || pending.calls != 2 || candidate.State() != ResourceOwnerReleased { t.Fatal("fallback retry replayed complete original or lost pending debt") }
}

func TestStableAppendFallbackConflictPreservesOriginalCleanupRoles(t *testing.T) {
	file := operationLifetimeFile(t)
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := candidate.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil { t.Fatal(err) }
	}
	spec := StableResourceSpec{Kind: ResourceDictionary, LogicalLane: "dictionary", ResourceID: "fallback-conflict", Generation: 1,
		DiagnosticPath: "fallback-conflict", File: file, Frontier: DurableFrontier{Bytes: 1},
		Digest: sha256.Sum256([]byte("base")), Reachability: ReachabilityDictionaryGeneration, ContentSynced: true}
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	incomingEnv := &cleanupCustodyEnvironmentV1{ready: true}
	if err := candidate.Add(cleanupCustodyTokenV1(t, spec, baseEnv)); err != nil { t.Fatal(err) }
	spec.Digest = sha256.Sum256([]byte("conflicting"))
	incoming := cleanupCustodyTokenV1(t, spec, incomingEnv)
	producer := freezeAppendMutationResources(t, incoming)
	defer producer.Release()
	candidate.mu.Lock()
	if err := candidate.promoteEntriesToViewsLocked(); err != nil { candidate.mu.Unlock(); t.Fatal(err) }
	before := candidate.kindViews.get(ResourceDictionary).root
	candidate.mu.Unlock()
	_, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{})
	if !errors.Is(err, ErrResourceConflict) { t.Fatalf("conflict: %v", err) }
	if producer.Owner() != ResourceOwnerBuilder || candidate.kindViews.get(ResourceDictionary).root != before || incoming.CleanupCompleteV1() || baseEnv.calls != 0 || incomingEnv.calls != 0 { t.Fatal("pretransfer refusal consumed original participants") }
}

func TestStableViewCollisionAddAllowsReentrantAddAfterOuterUnlock(t *testing.T) {
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	retiredEnv := &cleanupCustodyEnvironmentV1{ready: true}
	base := cleanupCustodyTokenV1(t, spec, baseEnv)
	initial := freezeAppendMutationResources(t, base)
	candidate := NewStableResourceSetBuilder()
	if err := candidate.Merge(initial); err != nil { t.Fatal(err) }
	defer candidate.Abandon()
	nestedSpec := operationLifetimeSpec(operationLifetimeFile(t))
	nestedSpec.ResourceID = "nested-after-collision"
	nested, err := NewStableResourceToken(nestedSpec)
	if err != nil { t.Fatal(err) }
	defer nested.Release()
	retiredEnv.callback = func() {
		if !candidate.mu.TryLock() { t.Error("view collision callback retained outer builder gate"); return }
		candidate.mu.Unlock()
		if candidate.State() != ResourceOwnerBuilder { t.Error("collision callback saw wrong destination state") }
		if err := candidate.Add(nested); err != nil { t.Errorf("reentrant Add: %v", err) }
	}
	retired := cleanupCustodyTokenV1(t, spec, retiredEnv)
	if err = candidate.Add(retired); err != nil { t.Fatal(err) }
	if retiredEnv.calls != 1 || baseEnv.calls != 1 || len(candidate.entries) != 2 { t.Fatal("view collision lost reentrant addition or replayed originals") }
	if err = candidate.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if retiredEnv.calls != 1 || baseEnv.calls != 1 || !nested.CleanupCompleteV1() { t.Fatal("collision final cleanup replayed or lost nested owner") }
}

// Public selector success must not lose preborn operation controls at Freeze.
// Each surviving clone owns exactly one original creator edge; discharged
// operation shells retain no extra edge and do not refund cumulative births.
func TestStablePublicSelectorsReleaseEmptyCloneCreatorControls(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil { t.Fatal(err) }
	scope, err := owner.NewOrdinaryScope()
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { scope.ReleaseStableMetadata(); owner.Close() })
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	obligation := appendMutationTestObligation(1)
	spec.Reachability = obligation.Reachability
	spec.LogicalObligations = []StableLogicalObligation{obligation}
	spec.CallbackCreator = scope
	environment := &cleanupCustodyEnvironmentV1{ready: true}
	source := freezeAppendMutationResources(t, cleanupCustodyTokenV1(t, spec, environment))
	t.Cleanup(func() { if err := source.Release(); err != nil { t.Error(err) } })
	selector := StableResourceSelector{Kind: spec.Kind, LogicalLane: spec.LogicalLane, ResourceID: spec.ResourceID, PhysicalGeneration: spec.Generation, Obligation: obligation}
	baseline := scope.Stats()
	for i := 0; i < 8; i++ {
		// Original ordinary Scope remains valid after DB admission closes.
		if i == 4 { owner.Close() }
		before := scope.Stats()
		selected, err := CloneStableResourceForSelector(source, selector)
		if err != nil { t.Fatal(err) }
		if scope.Stats().Retained != baseline.Retained+1 { t.Fatal("selector leaked paused clone/Add creator edges") }
		descriptors := mustStableResourceDescriptors(t, selected)
		if len(descriptors) != 1 || len(descriptors[0].LogicalObligations()) != 1 || descriptors[0].LogicalObligations()[0] != obligation { t.Fatal("selector lost actual logical obligation") }
		if err := selected.Release(); err != nil { t.Fatal(err) }
		if scope.Stats().Retained != baseline.Retained || scope.Stats().Bytes <= before.Bytes { t.Fatal("selector retained shell or refunded cumulative control births") }
		wrong := selector
		wrong.Obligation = appendMutationTestObligation(99)
		beforeRefusal := scope.Stats()
		if result, err := CloneStableResourceForSelector(source, wrong); result != nil || !errors.Is(err, ErrUnresolvedResource) { t.Fatalf("selector refusal: %v %v", result, err) }
		if scope.Stats() != beforeRefusal { t.Fatal("selection refusal changed original creator") }
	}
	for _, clone := range []func() (*StableResourceSet, error){
		func() (*StableResourceSet, error) { return CloneStableResourceSetSelectingPhysicalKind(source, ResourceDictionary, nil) },
		func() (*StableResourceSet, error) { return ClonePhysicalReachabilityUnion(source, source) },
	} {
		selected, err := clone()
		if err != nil { t.Fatal(err) }
		if scope.Stats().Retained != baseline.Retained+1 { t.Fatal("shared clone caller lost control custody at Freeze") }
		if err := selected.Release(); err != nil { t.Fatal(err) }
		if scope.Stats().Retained != baseline.Retained { t.Fatal("shared clone caller leaked operation edge") }
	}
	environment.callback = func() {
		if !source.mu.TryLock() { t.Error("source cleanup retained source gate"); return }
		source.mu.Unlock()
		_ = source.Len()
		if result, err := CloneStableResourceForSelector(source, selector); result != nil || !errors.Is(err, ErrResourceOwnership) { t.Error("reentrant selector admitted releasing original") }
	}
	if err := source.Release(); err != nil { t.Fatal(err) }
	if environment.calls != 1 || scope.Stats().Retained != 1 { t.Fatal("original cleanup or creator discharge lost/replayed") }
}

func TestStableCloneRefusalKeepsUnclaimedRoleUntilOuterUnlock(t *testing.T) {
	owner, _ := residentcredit.NewOrdinary(128 << 20)
	scope, _ := owner.NewOrdinaryScope()
	t.Cleanup(func() { scope.ReleaseStableMetadata(); owner.Close() })
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.CallbackCreator = scope
	environment := &cleanupCustodyEnvironmentV1{ready: true}
	source := freezeAppendMutationResources(t, cleanupCustodyTokenV1(t, spec, environment))
	t.Cleanup(func() { if err := source.Release(); err != nil { t.Error(err) } })
	builder := NewStableResourceSetBuilder()
	builder.closed = true // Exact Add refusal after a real clone constructor.
	baseline := scope.Stats()
	source.mu.Lock()
	entry := source.entries[0]
	err := cloneStableResourceEntryIntoBuilder(builder, &entry)
	if !errors.Is(err, ErrResourceOwnership) { source.mu.Unlock(); t.Fatalf("clone Add refusal: %v", err) }
	if builder.pendingCleanup == nil || builder.pendingCleanup.cleanup.unclaimed == nil || scope.Stats().Retained <= baseline.Retained { source.mu.Unlock(); t.Fatal("failed clone lost actual unclaimed owner/control") }
	if _, err := builder.Freeze(); err == nil { source.mu.Unlock(); t.Fatal("failed clone exposed unfinished role") }
	source.mu.Unlock()
	if err := builder.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if builder.pendingCleanup != nil || scope.Stats().Retained != baseline.Retained || environment.calls != 0 { t.Fatal("unclaimed clone drain lost creator or replayed original callback") }
	if err := builder.AbandonCheckedV1(); err != nil || scope.Stats().Retained != baseline.Retained { t.Fatal("clone rollback replayed") }
}

func TestStablePausedPendingCreatorCannotEscapeFreeze(t *testing.T) {
	owner, _ := residentcredit.NewOrdinary(128 << 20)
	scope, _ := owner.NewOrdinaryScope()
	t.Cleanup(func() { scope.ReleaseStableMetadata(); owner.Close() })
	spec := operationLifetimeSpec(operationLifetimeFile(t))
	spec.CallbackCreator = scope
	baseEnv := &cleanupCustodyEnvironmentV1{ready: true}
	pending := &cleanupCustodyEnvironmentV1{}
	base := cleanupCustodyTokenV1(t, spec, baseEnv)
	retired := cleanupCustodyTokenV1(t, spec, pending)
	builder := NewStableResourceSetBuilder()
	t.Cleanup(func() { pending.ready = true; if err := builder.AbandonCheckedV1(); err != nil { t.Error(err) } })
	if err := builder.Add(base); err != nil { t.Fatal(err) }
	baseline := scope.Stats().Retained
	outer := NewStableResourceSetBuilder()
	pending.callback = func() {
		if !outer.mu.TryLock() { t.Error("pending callback retained inherited gate"); return }
		outer.mu.Unlock()
		_ = outer.State()
		if !errors.Is(builder.AbandonCheckedV1(), ErrStableResourceOperationBusy) { t.Error("running pending cleanup allowed reentrant replay") }
	}
	outer.mu.Lock()
	err := builder.addWithCleanupBoundaryV1(retired, true)
	outer.mu.Unlock()
	if err != nil { t.Fatal(err) }
	if pending.calls != 0 || scope.Stats().Retained != baseline+1 { t.Fatal("paused nonempty owner was scrubbed or called inside inherited gate") }
	if _, err := builder.Freeze(); err == nil { t.Fatal("Freeze discarded paused nonempty creator role") }
	if err := builder.AbandonCheckedV1(); !errors.Is(err, errCleanupCustodyFixtureV1) { t.Fatalf("pending role: %v", err) }
	if scope.Stats().Retained != baseline+1 || baseEnv.calls != 0 || pending.calls != 1 { t.Fatal("Pending role lost creator/destination") }
	pending.ready = true
	if err := builder.AbandonCheckedV1(); err != nil { t.Fatal(err) }
	if scope.Stats().Retained != 1 || baseEnv.calls != 1 || pending.calls != 2 { t.Fatal("pending retry failed exact last edge or replayed complete role") }
	if err := builder.AbandonCheckedV1(); err != nil || pending.calls != 2 || baseEnv.calls != 1 { t.Fatal("completed Pending cleanup replayed") }
}
