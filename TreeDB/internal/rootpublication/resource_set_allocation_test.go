package rootpublication

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"testing"
	"time"
	"unsafe"
)

func TestOwnedSetWrapperFreezeRefusalPreservesSource(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "wrapper.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err = builder.Merge(source); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	budget.cap = before
	result, err := builder.Freeze()
	if !errors.Is(err, retainedalloc.ErrCapacity) || result != nil {
		if result != nil {
			result.Release()
		}
		t.Fatalf("freeze allocated an unadmitted selected wrapper: result=%v error=%v", result != nil, err)
	}
	if owner.Bytes() != before || builder.State() != ResourceOwnerBuilder {
		t.Fatal("refusal changed source ownership/capacity")
	}
	budget.cap = 1 << 20
	result, err = builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if result.pinHighWater != nil {
		t.Fatal("selected wrapper retained an unadmitted high-water map")
	}
	result.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("last wrapper retained %d bytes, want %d", owner.Bytes(), baseline)
	}
}

func TestOwnedBuilderAddCoalescesAndRefusesBeforeTransfer(t *testing.T) {
	var owner, foreign retainedalloc.Owner
	owner.Initialize(0)
	foreign.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "builder.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	capture := func(o *retainedalloc.Owner, bytes uint64) *StableResourceToken {
		token, e := NewStableResourceToken(StableResourceSpec{MetadataOwner: o, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "lane", ResourceID: "index.db", DiagnosticPath: "index.db", Generation: 1, File: file, Frontier: DurableFrontier{Bytes: bytes}, Reachability: ReachabilityIndexFile})
		if e != nil {
			t.Fatal(e)
		}
		return token
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner, ReachabilityIndexFile)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	first := capture(&owner, 4)
	before := owner.Bytes()
	budget.cap = before
	if err = builder.Add(first); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("refused Add=%v", err)
	}
	if ResourceOwnerState(first.owner.Load()) != ResourceOwnerToken || first.released.Load() || owner.Bytes() != before {
		t.Fatal("refused Add consumed input")
	}
	budget.cap = 1 << 20
	if err = builder.Add(first); err != nil {
		t.Fatal(err)
	}
	unsupported := capture(&foreign, 8)
	if err = builder.Add(unsupported); !errors.Is(err, ErrResourceOwnership) || ResourceOwnerState(unsupported.owner.Load()) != ResourceOwnerToken {
		t.Fatalf("foreign Add=%v", err)
	}
	unsupported.Release()
	second := capture(&owner, 8)
	if err = builder.Add(second); err != nil {
		t.Fatal(err)
	}
	result, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	view := result.kindViews.get(ResourceIndex)
	logical := findStableResourceLogical(view.logical, first.logicalKey())
	physical := findStableResourcePhysical(view.physical, first.physicalIdentityKey())
	physicalCount, physicalBytes := result.PhysicalSummary()
	if result.Len() != 1 || physicalCount != 1 || physicalBytes != 8 || logical == nil || logical.frontier.Bytes != 8 || len(physical) != 1 || physical[0] != logical {
		t.Fatalf("actual repeated identity did not coalesce: len=%d physical=%d/%d logical=%v", result.Len(), physicalCount, physicalBytes, logical)
	}
	if view.root.entries[0].frontier.Bytes != 4 || view.root.entries[0].token != first || logical.token != first {
		t.Fatal("coalescing rewrote immutable physical custody")
	}
	if builder.metadata != nil || builder.requiredFields != nil {
		t.Fatal("closed builder retained admitted constructor storage")
	}
	clone, err := CloneStableResourceSetExcludingKinds(result)
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
	if clone.Len() != 1 {
		t.Fatal("wrapper disposal destroyed independent clone")
	}
	clone.Release()
	if owner.Bytes() != baseline || foreign.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("retained owner=%d foreign=%d pins=%d", owner.Bytes(), foreign.Bytes(), registry.Stats().ActivePins)
	}
}

func TestOwnedRopeReleaseUsesAdmittedScratch(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "rope.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	baseline := owner.Bytes()
	leaf := source.kindViews.get(ResourceIndex).root
	var roots [101]*stableResourceEntryNode
	for i := range roots {
		for j := 0; j < 64; j++ {
			if !leaf.retain() {
				t.Fatal("leaf retain")
			}
			roots[i], err = concatOwnedAdmittedStableResourceEntryNodes(leaf, roots[i], &owner)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	next := 0
	allocations := testing.AllocsPerRun(100, func() { roots[next].release(); roots[next] = nil; next++ })
	if allocations != 0 {
		t.Fatalf("release allocated %.0f unadmitted scratch objects", allocations)
	}
	if owner.Bytes() != baseline || source.Len() != 1 || registry.Stats().ActivePins == 0 {
		t.Fatal("release lost independent root or retained scratch")
	}
	source.Release()
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatal("last root cleanup incomplete")
	}
}

func TestOwnedSetRawDiagnosticsCannotEscapeAdmission(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "diagnostics.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	defer source.Release()
	if len(source.PhysicalDescriptors()) != 0 || len(source.Tokens()) != 0 || len(source.Stats(time.Now())) != 0 {
		t.Fatal("selected raw diagnostics escaped their actual allocation owner")
	}
	if values, err := source.Descriptors(); values != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("raw logical diagnostic escape=%v %v", values, err)
	}
}

func TestOwnedSetDiagnosticOwnerRefusalAliasesAndLastCleanup(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "diagnostic-owner.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	before := owner.Bytes()
	budget.cap = before
	if view, e := source.AcquirePhysicalDiagnostics(); view != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("admission refusal=%v %v", view, e)
	}
	if owner.Bytes() != before || registry.Stats().ActivePins != 1 || source.Owner() != ResourceOwnerBuilder {
		t.Fatal("refusal consumed source")
	}
	budget.cap = 1 << 20
	physical, err := source.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.AcquireStats(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(physical.Physical()) != 1 || len(stats.Stats()) != 1 || stats.Stats()[0].ActivePins != 1 || stats.Stats()[0].PinHighWater != 1 {
		t.Fatal("owned view lost exact physical/stat authority")
	}
	source.Release()
	if owner.Bytes() <= baseline || physical.Physical()[0].ResourceID() != "index.db" || registry.Stats().ActivePins != 1 {
		t.Fatal("source disposal stole actual diagnostic custody")
	}
	physical.Close()
	physical.Close()
	if owner.Bytes() <= baseline || registry.Stats().ActivePins != 1 {
		t.Fatal("remaining statistics owner lost physical operation custody")
	}
	stats.Close()
	stats.Close()
	if owner.Bytes() != baseline || registry.Stats().ActivePins != 0 || physical.Physical() != nil || stats.Stats() != nil {
		t.Fatalf("last diagnostic owner retained backing/pins: bytes=%d baseline=%d", owner.Bytes(), baseline)
	}
	if _, err = source.AcquirePhysicalDiagnostics(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released diagnostic resurrected=%v", err)
	}
}

// A real owned token's duplicated OS handle is closed before its actual last
// Close. This tests the original kind/logical/physical/rope disposal graph,
// rather than substituting a cleanup callback or scalar accounting receipt.
func TestOwnedSetActualPhysicalCloseFailureRetainsOriginalGraph(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "real-close.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 7}, nil)
	views := set.kindViews
	view := views.get(ResourceIndex)
	token := view.logical.entry.token
	originalPhysical := token.physicalBacking
	if err = token.pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err = set.transfer(ResourceOwnerBuilder, ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
	counts := map[ResourceKind]uint64{}
	set.adjustActivePinsByKind(counts, true)
	if counts[ResourceIndex] != 1 {
		t.Fatal("pending contribution missing")
	}
	before := owner.Bytes()
	first := set.releaseFromWithActivePins(ResourceOwnerCoordinator, counts)
	if !errors.Is(first, os.ErrClosed) {
		t.Fatalf("actual Close refusal: %v", first)
	}
	if set.Owner() != ResourceOwnerReleased || counts[ResourceIndex] != 0 {
		t.Fatal("Set edge was not consumed exactly once")
	}
	// Entered edges have moved into the SAME original intrusive release chain.
	// Expired public aliases must not keep a second consumable edge.
	if set.kindViews != nil || set.metadata == nil || !originalReleaseChainContainsV6(&set.metadata.release, views) || !originalReleaseChainContainsV6(&set.metadata.release, originalPhysical) || owner.Bytes() == 0 || owner.Bytes() > before || originalPhysical.file == nil || originalPhysical.owner == nil || registry.failedPinned != originalPhysical {
		t.Fatal("failure refunded original wrapper/backing or lost physical custody")
	}
	remaining := owner.Bytes()
	for i := 0; i < 3; i++ {
		if again := set.ReleaseWithError(); again != first {
			t.Fatal("repeated Close replaced first result")
		}
	}
	if owner.Bytes() != remaining || originalPhysical.refs.Load() != 0 || originalPhysical.failedNext == originalPhysical {
		t.Fatal("repeated Close replayed physical cleanup/refund")
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryHandoffActualPhysicalCloseFailureIsSticky(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "handoff-close.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 7}, nil)
	token := set.kindViews.get(ResourceIndex).logical.entry.token
	// This real terminal coordinator transfers its original retained slice/Set.
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishAmbiguous}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err = coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = set.transfer(ResourceOwnerBuilder, ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	coordinator.recoverySets = append(coordinator.recoverySets, set)
	coordinator.mu.Unlock()
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if handoff.metadata != &owner || handoff.metadataCharge == 0 {
		t.Fatal("handoff was not preadmitted on original owner")
	}
	if err = token.pinned.Close(); err != nil {
		t.Fatal(err)
	}
	first := handoff.ReleaseWithError()
	if !errors.Is(first, os.ErrClosed) || handoff.failure.original != handoff || handoff.sets[0] != set {
		t.Fatalf("handoff lost original failed payload: %v", first)
	}
	remaining := owner.Bytes()
	if again := handoff.ReleaseWithError(); again != first || owner.Bytes() != remaining {
		t.Fatal("handoff repeated original cleanup or refund")
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
}

// The diagnostic is the actual final descriptor borrower after source Close.
func TestOwnedDiagnosticActualPhysicalCloseFailureRetainsOriginalPayload(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "diagnostic-real-close.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 7}, nil)
	token := set.kindViews.get(ResourceIndex).logical.entry.token
	originalPhysical := token.physicalBacking
	diagnostic, err := set.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	if err = set.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = token.pinned.Close(); err != nil {
		t.Fatal(err)
	}
	originalBacking := diagnostic.borrow.backing
	first := diagnostic.CloseWithError()
	if !errors.Is(first, os.ErrClosed) || diagnostic.allocation == nil || diagnostic.allocation.failure.original != diagnostic || len(diagnostic.physical) != 1 || diagnostic.borrow.backing != nil || !originalReleaseChainContainsV6(&originalBacking.allocation.release, originalPhysical) || originalPhysical.file == nil || originalPhysical.owner == nil || registry.failedPinned != originalPhysical {
		t.Fatalf("last diagnostic lost exact retained payload: %v", first)
	}
	remaining := owner.Bytes()
	if remaining == 0 || diagnostic.Physical() != nil {
		t.Fatal("failed original payload was refunded or exposed")
	}
	for i := 0; i < 3; i++ {
		if again := diagnostic.CloseWithError(); again != first || owner.Bytes() != remaining {
			t.Fatal("diagnostic retried/refunded actual failure")
		}
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedScopedTokenCallbackActualLastCloseFailure(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "scoped-real-close.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 7}, nil)
	operationErr := errors.New("original callback refusal")
	err = set.WithScopedTokens(func(tokens []*StableResourceToken) error {
		if len(tokens) != 1 {
			t.Fatal("missing actual original token")
		}
		if e := set.ReleaseWithError(); e != nil {
			t.Fatal(e)
		}
		if e := tokens[0].pinned.Close(); e != nil {
			t.Fatal(e)
		}
		return operationErr
	})
	if !errors.Is(err, operationErr) || !errors.Is(err, os.ErrClosed) || owner.Bytes() == 0 || registry.failedPinned == nil {
		t.Fatalf("scoped cleanup erased original operation/physical outcome: %v", err)
	}
	if e := registry.Unobserve(identity); e != nil {
		t.Fatal(e)
	}
}

func TestOwnedBuilderActualLastCloseFailureRetainsOriginalPayload(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "builder-real-close.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 7}, nil)
	token := source.kindViews.get(ResourceIndex).logical.entry.token
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Merge(source); err != nil {
		t.Fatal(err)
	}
	if err = token.pinned.Close(); err != nil {
		t.Fatal(err)
	}
	views := builder.kindViews
	before := owner.Bytes()
	first := builder.AbandonWithError()
	if !errors.Is(first, os.ErrClosed) || builder.metadata == nil || builder.metadata.failure.original != builder || builder.kindViews != views || registry.failedPinned == nil {
		t.Fatalf("builder lost original failure payload: %v", first)
	}
	held := owner.Bytes()
	if held == 0 || held > before {
		t.Fatalf("invalid retained contribution: %d -> %d", before, held)
	}
	for i := 0; i < 3; i++ {
		if again := builder.AbandonWithError(); again != first || owner.Bytes() != held {
			t.Fatal("repeated builder disposal replayed/refunded original refusal")
		}
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
}

// The original operation callback holds its actual last edge while the SAME
// builder is already claimed. Reentrant and concurrent callers must not report
// completion or replay Release while that physical callback is still pending.
type blockingBuilderOperations struct {
	ownedTokenOperationsFixture
	builder *StableResourceSetBuilder
	entered chan error
	resume  chan struct{}
}

func (o *blockingBuilderOperations) Release() (bool, error) {
	if o.refs.Load() == 1 {
		o.entered <- o.builder.AbandonWithError()
		<-o.resume
	}
	return o.ownedTokenOperationsFixture.Release()
}
func TestOwnedBuilderActualCompletionReentrantAndPending(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(blockingBuilderOperations{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	ops := &blockingBuilderOperations{
		ownedTokenOperationsFixture: ownedTokenOperationsFixture{owner: &owner, charge: charge},
		entered:                     make(chan error, 1), resume: make(chan struct{}),
	}
	ops.refs.Store(1)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "builder-pending.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	token, err := NewStableResourceToken(StableResourceSpec{
		MetadataOwner: &owner, OwnedOperations: ops, PinRegistry: registry,
		Kind: ResourceIndex, LogicalLane: "lane", ResourceID: "builder-pending.bin",
		Generation: 1, DiagnosticPath: "builder-pending.bin", File: file,
		Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ops.Release(); err != nil {
		t.Fatal(err)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	ops.builder = builder
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	complete := make(chan error, 1)
	go func() { complete <- builder.AbandonWithError() }()
	select {
	case err = <-ops.entered:
		if !errors.Is(err, ErrResourceOwnership) {
			t.Fatalf("reentrant close pretended completion: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original release callback never entered")
	}
	held := owner.Bytes()
	if held == 0 || builder.metadata == nil || ops.disposed.Load() {
		t.Fatal("pending original payload refunded")
	}
	for i := 0; i < 3; i++ {
		if err = builder.AbandonWithError(); !errors.Is(err, ErrResourceOwnership) || owner.Bytes() != held {
			t.Fatalf("pending duplicate replay/refund: %v", err)
		}
	}
	close(ops.resume)
	select {
	case err = <-complete:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original callback completion deadlocked")
	}
	if err = builder.AbandonWithError(); err != nil || owner.Bytes() != 0 || !ops.disposed.Load() || ops.releaseCalls.Load() != 2 {
		t.Fatalf("actual final completion failed: %v bytes=%d calls=%d", err, owner.Bytes(), ops.releaseCalls.Load())
	}
}

// The original admitted frames retain every consumed-but-unfinished edge after
// a physical error. Inspect that exact chain, rather than expired public views.
func originalReleaseChainContainsV6(root *resourceReleaseFrameV6, value any) bool {
	for frame := root.current; frame != nil; frame = frame.parent {
		if frame.value == value {
			return true
		}
	}
	return false
}
